"""Publish a finished pipeline run: history, snapshot, deltas, Teams.

This is the internal replacement for the `update-sku-data` n8n workflow. Same
six moves, in the same order, now in-process:

1. append every row to the history workbook (append-only audit trail)
2. read the previous snapshot for this report type
3. compute the day-over-day deltas
4. upsert the new snapshot, matched on `sku_channel_id`
5. post the per-channel summary
6. post the anomaly report

Two things the workflow could not do, added here:

- **Idempotency.** A re-run for a report date already published is refused
  (override with `force=True`). Without the guard a second run duplicates every
  history row and reports deltas of zero, because it compares today against
  today.
- **Dry run.** `dry_run=True` reads the snapshot and builds both reports, but
  writes nothing to the workbooks and posts nothing live — the HTML lands in
  `output/`. This is what `main.py --test` uses.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from src import settings
from src.integrations import microsoft_auth
from src.integrations.excel import ExcelWorkbook
from src.integrations.graph import GraphClient, GraphError
from src.reporting import cards, html as html_builders
from src.reporting.deltas import compute_deltas, parse_date
from src.reporting.transports import FileTransport, Report, TeamsTransport, build_transport

logger = logging.getLogger(__name__)

SNAPSHOT_SHEETS = {
    "inventory": (settings.INVENTORY_WORKBOOK_ID, "raw_inventory"),
    "sales": (settings.INVENTORY_WORKBOOK_ID, "raw_sales"),
}
HISTORY_SHEETS = {
    "inventory": (settings.HISTORY_WORKBOOK_ID, "inventory"),
    "sales": (settings.HISTORY_WORKBOOK_ID, "sales"),
}

LEDGER_FILENAME = "publish_ledger.json"


@dataclass
class PublishResult:
    report_type: str
    report_date: date | None = None
    history_appended: int = 0
    snapshot_updated: int = 0
    snapshot_inserted: int = 0
    messages_sent: int = 0
    skipped_reason: str | None = None
    errors: list[str] = field(default_factory=list)

    @property
    def published(self) -> bool:
        return self.skipped_reason is None and not self.errors


class PublishLedger:
    """Local record of what has already been published, keyed by date."""

    def __init__(self, path: Path | None = None):
        self.path = path or (settings.OUTPUT_DIR / LEDGER_FILENAME)

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            logger.warning("⚠️  %s was malformed — starting a fresh ledger.", self.path.name)
            return {}

    def get(self, report_type: str, report_date: date | None) -> dict | None:
        if report_date is None:
            return None
        return self._read().get(f"{report_type}:{report_date.isoformat()}")

    def record(self, result: PublishResult) -> None:
        if result.report_date is None:
            return
        ledger = self._read()
        ledger[f"{result.report_type}:{result.report_date.isoformat()}"] = {
            "published_at": datetime.now().isoformat(timespec="seconds"),
            **asdict(result),
            "report_date": result.report_date.isoformat(),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(".tmp")
        temp.write_text(json.dumps(ledger, indent=2, sort_keys=True), encoding="utf-8")
        temp.replace(self.path)


class Publisher:
    def __init__(
        self,
        *,
        transport: TeamsTransport | None = None,
        dry_run: bool = False,
        force: bool = False,
        ledger: PublishLedger | None = None,
        client: GraphClient | None = None,
    ):
        self.dry_run = dry_run
        self.force = force
        self.ledger = ledger or PublishLedger()
        self._client = client
        self._transport = FileTransport() if dry_run else transport

    # -- lazily built collaborators ---------------------------------------

    @property
    def client(self) -> GraphClient:
        if self._client is None:
            self._client = GraphClient(token_provider=microsoft_auth.acquire_app_token)
        return self._client

    @property
    def transport(self) -> TeamsTransport:
        if self._transport is None:
            self._transport = build_transport()
        return self._transport

    def workbook(self, item_id: str) -> ExcelWorkbook:
        return ExcelWorkbook(self.client, item_id, settings.MS_MAILBOX_ADDRESS)

    # -- public entry point ------------------------------------------------

    def publish(
        self,
        report_type: str,
        validated_data: Iterable[Any],
        status_summary: dict[str, Any] | None = None,
    ) -> PublishResult:
        result = PublishResult(report_type=report_type)

        rows = [item.model_dump(mode="json", by_alias=True) for item in validated_data]
        if not rows:
            logger.warning("⚠️  Nothing to publish for %s — no rows.", report_type)
            result.skipped_reason = "no rows"
            return result

        result.report_date = _max_report_date(rows)
        logger.info(
            "📤 Publishing %s report for %s (%s rows)%s",
            report_type, result.report_date, len(rows), " [DRY RUN]" if self.dry_run else "",
        )

        if not self.dry_run and not microsoft_auth.credentials_available():
            logger.warning(
                "⚠️  Microsoft Graph credentials are not configured — skipping the "
                "%s publish. Set MS_TENANT_ID / MS_CLIENT_ID / MS_CLIENT_SECRET in .env.",
                report_type,
            )
            result.skipped_reason = "microsoft credentials missing"
            return result

        previous = self.ledger.get(report_type, result.report_date)
        if previous and not self.force and not self.dry_run:
            logger.warning(
                "⏭️  %s for %s was already published at %s — skipping. "
                "Use --force-publish to publish again.",
                report_type, result.report_date, previous.get("published_at", "?"),
            )
            result.skipped_reason = f"already published at {previous.get('published_at', '?')}"
            return result

        snapshot = self.workbook(SNAPSHOT_SHEETS[report_type][0]).worksheet(
            SNAPSHOT_SHEETS[report_type][1]
        )
        history = self.workbook(HISTORY_SHEETS[report_type][0]).worksheet(
            HISTORY_SHEETS[report_type][1]
        )

        try:
            old_rows = snapshot.read_rows()
        except GraphError as e:
            logger.error("❌ Could not read the previous snapshot: %s", e)
            result.errors.append(f"snapshot read failed: {e}")
            self._notify_error(report_type, "Could not read the previous snapshot from Excel.")
            return result

        delta_rows = compute_deltas(rows, old_rows, report_type)

        if not self.dry_run:
            self._write_history(history, rows, result)
            self._write_snapshot(snapshot, rows, result)

        self._send_reports(report_type, delta_rows, status_summary or {}, result)

        if not self.dry_run and not result.errors:
            self.ledger.record(result)

        logger.info(
            "✅ Publish complete: history +%s, snapshot %s updated / %s inserted, %s message(s).",
            result.history_appended, result.snapshot_updated, result.snapshot_inserted,
            result.messages_sent,
        )
        return result

    # -- steps -------------------------------------------------------------

    def _write_history(self, history, rows: list[dict], result: PublishResult) -> None:
        try:
            result.history_appended = history.append_rows(rows)
        except GraphError as e:
            logger.error("❌ History append failed: %s", e)
            result.errors.append(f"history append failed: {e}")

    def _write_snapshot(self, snapshot, rows: list[dict], result: PublishResult) -> None:
        try:
            upsert = snapshot.upsert_rows("sku_channel_id", rows)
            result.snapshot_updated = upsert.updated
            result.snapshot_inserted = upsert.inserted
        except GraphError as e:
            logger.error("❌ Snapshot upsert failed: %s", e)
            result.errors.append(f"snapshot upsert failed: {e}")

    def _send_reports(
        self,
        report_type: str,
        delta_rows: list[dict],
        status_summary: dict[str, Any],
        result: PublishResult,
    ) -> None:
        """Build both renderings of each report and hand them to the transport."""
        stamp = result.report_date.isoformat() if result.report_date else "undated"
        label = report_type.capitalize()

        if report_type == "sales":
            summary = Report(
                title=f"{label} Summary — {stamp}",
                html=html_builders.build_sales_summary(delta_rows),
                card=cards.build_sales_summary_card(delta_rows),
            )
            anomalies = Report(
                title=f"{label} Anomalies — {stamp}",
                html=html_builders.build_sales_anomalies(delta_rows),
                card=cards.build_sales_anomalies_card(delta_rows),
            )
        else:
            summary = Report(
                title=f"{label} Summary — {stamp}",
                html=html_builders.build_inventory_summary(status_summary),
                card=cards.build_inventory_summary_card(status_summary),
            )
            anomalies = Report(
                title=f"{label} Anomalies — {stamp}",
                html=html_builders.build_inventory_anomalies(delta_rows),
                card=cards.build_inventory_anomalies_card(delta_rows),
            )

        for report in (summary, anomalies):
            if result.messages_sent:
                time.sleep(settings.PUBLISH_SEND_GAP_SECONDS)
            try:
                self.transport.send(report)
                result.messages_sent += 1
            except Exception as e:  # a dead Teams lane must not lose the run
                logger.error("❌ Could not deliver %s: %s", report.title, e)
                result.errors.append(f"{report.title} delivery failed: {e}")

    def _notify_error(self, report_type: str, detail: str) -> None:
        message = f"{report_type} publish aborted: {detail}"
        try:
            self.transport.send(Report(
                title=f"{report_type}-publish-error",
                html=(
                    "<div style='font-family: Arial, sans-serif; padding: 10px; "
                    "border: 1px solid #d32f2f; border-radius: 5px;'>"
                    f"<strong>🚨 SKU data update failed</strong><br>{message}</div>"
                ),
                card=cards.simple_card("🚨 SKU data update failed", message),
            ))
        except Exception as e:
            logger.error("❌ Could not deliver the failure notice either: %s", e)


def _max_report_date(rows: list[dict]) -> date | None:
    dates = [parsed for parsed in (parse_date(row.get("Date")) for row in rows) if parsed]
    return max(dates) if dates else None


def publish(
    report_type: str,
    validated_data: Iterable[Any],
    status_summary: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
    force: bool = False,
) -> PublishResult:
    """Convenience wrapper — one Publisher, one run."""
    return Publisher(dry_run=dry_run, force=force).publish(
        report_type, validated_data, status_summary
    )
