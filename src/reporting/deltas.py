"""Day-over-day deltas for the snapshot tables.

Ported from the two n8n `Get Delta *` code nodes. The shape is the same for
both pipelines: take today's rows, look up yesterday's row by `sku_channel_id`,
and attach the change plus the gap in days between the two reports.

Two deliberate deviations from the n8n original:

- A row with no previous snapshot entry yields zero deltas and
  `Days_Since_Last_Report = 0`, not a skipped row. The n8n merge used an
  "enrich input 2" join, which silently dropped new SKUs.
- The sales lane names its revenue delta `Delta_Revenue`. n8n reused
  `Delta_Inventory` for it — a copy-paste artefact that reads as an inventory
  number in a sales payload. Nothing persists these fields (the sheets hold
  eight and seven columns), so the rename costs nothing.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Iterable

from src.integrations.excel import serial_to_date

logger = logging.getLogger(__name__)

INVENTORY_NUMERIC_FIELDS = ("Units", "Inventory", "Inbound")
SALES_NUMERIC_FIELDS = ("Units", "Revenue")


def parse_date(value: Any) -> date | None:
    """Accept what a sheet or a Pydantic dump actually hands over.

    Excel serials (int/float), ISO strings, `datetime`s and `date`s all arrive
    in practice — the snapshot read gives serials, the pipeline gives
    `date` objects, the JSON output gives `"YYYY-MM-DD"`.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return serial_to_date(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            return serial_to_date(float(text))
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            pass
        for fmt in ("%m/%d/%Y", "%d-%b-%Y", "%b %d, %Y"):
            try:
                return datetime.strptime(text, fmt).date()
            except ValueError:
                continue
        logger.warning("⚠️  Unparseable date value in snapshot: %r", value)
        return None
    return None


def days_between(new: date | None, old: date | None) -> int:
    """Whole days from `old` to `new`; 0 when either side is missing.

    Rounding rather than truncation matches the n8n `Math.round`, which
    absorbed timezone jitter when the two ends carried different clocks.
    """
    if new is None or old is None:
        return 0
    return round((new - old).total_seconds() / 86400) if isinstance(new, datetime) else (new - old).days


def _number(value: Any, *, integer: bool) -> float | int:
    if value is None or value == "":
        return 0 if integer else 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0 if integer else 0.0
    return round(number) if integer else round(number, 2)


def _numeric_fields(report_type: str) -> tuple[str, ...]:
    return SALES_NUMERIC_FIELDS if report_type == "sales" else INVENTORY_NUMERIC_FIELDS


def index_by_key(rows: Iterable[dict[str, Any]], key: str = "sku_channel_id") -> dict[str, dict]:
    """Snapshot rows keyed for lookup. Later duplicates win, matching the sheet."""
    return {str(row.get(key)): row for row in rows if row.get(key) not in (None, "")}


def compute_deltas(
    new_rows: Iterable[dict[str, Any]],
    old_rows: Iterable[dict[str, Any]],
    report_type: str,
) -> list[dict[str, Any]]:
    """Attach `Days_Since_Last_Report` and the `Delta_*` fields to each new row.

    `report_type` selects the numeric field set: inventory tracks
    Units/Inventory/Inbound, sales tracks Units/Revenue.
    """
    previous = index_by_key(old_rows)
    fields = _numeric_fields(report_type)
    integer_fields = {field: field != "Revenue" for field in fields}

    delta_names = {"Units": "Delta_Sold", "Inventory": "Delta_Inventory",
                   "Inbound": "Delta_Inbound", "Revenue": "Delta_Revenue"}

    computed: list[dict[str, Any]] = []
    for row in new_rows:
        enriched = dict(row)
        key = str(row.get("sku_channel_id", ""))
        old = previous.get(key, {})

        new_date = parse_date(row.get("Date"))
        old_date = parse_date(old.get("Date"))
        enriched["Days_Since_Last_Report"] = days_between(new_date, old_date)

        for field in fields:
            current = _number(row.get(field), integer=integer_fields[field])
            prior = _number(old.get(field), integer=integer_fields[field])
            enriched[field] = current
            enriched[delta_names[field]] = round(current - prior, 2) if not integer_fields[field] else current - prior

        computed.append(enriched)

    matched = sum(1 for row in computed if str(row.get("sku_channel_id", "")) in previous)
    logger.info(
        "📐 Deltas computed: %s rows (%s matched a previous snapshot, %s new).",
        len(computed), matched, len(computed) - matched,
    )
    return computed
