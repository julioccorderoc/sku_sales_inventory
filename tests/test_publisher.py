"""Publisher orchestration — idempotency, dry runs, and error containment."""
from datetime import date

import pytest

from src.integrations.excel import UpsertResult
from src.reporting.publisher import (
    PublishLedger,
    PublishResult,
    Publisher,
    _max_report_date,
)
from src.schemas import InventoryItem, SalesRecord


def _inventory_items(count: int = 2) -> list[InventoryItem]:
    return [
        InventoryItem(
            id=f"20260923_FBA_100{index}",
            sku_channel_id=f"FBA_100{index}",
            Date=date(2026, 9, 23),
            SKU=f"100{index}",
            Channel="FBA",
            Units=10,
            Inventory=100,
            Inbound=0,
        )
        for index in range(1, count + 1)
    ]


class FakeWorksheet:
    def __init__(self, rows=None, fail: str | None = None):
        self.rows = rows or []
        self.fail = fail
        self.appended: list[dict] = []
        self.upserted: list[dict] = []

    def read_rows(self):
        if self.fail == "read":
            from src.integrations.graph import GraphError

            raise GraphError("boom")
        return self.rows

    def append_rows(self, rows):
        if self.fail == "append":
            from src.integrations.graph import GraphError

            raise GraphError("boom")
        self.appended.extend(rows)
        return len(rows)

    def upsert_rows(self, key, rows):
        if self.fail == "upsert":
            from src.integrations.graph import GraphError

            raise GraphError("boom")
        self.upserted.extend(rows)
        return UpsertResult(updated=len(rows), inserted=0, total=len(rows))


class FakeTransport:
    def __init__(self, fail_on: str | None = None):
        self.sent: list[tuple[str, str]] = []
        self.cards: list[dict | None] = []
        self.fail_on = fail_on

    def send(self, report):
        if self.fail_on and self.fail_on in report.title:
            raise RuntimeError("transport down")
        self.sent.append((report.title, report.html))
        self.cards.append(report.card)


class FakePublisher(Publisher):
    """Publisher with the two worksheets swapped for in-memory fakes."""

    def __init__(self, snapshot=None, history=None, **kwargs):
        super().__init__(**kwargs)
        self._snapshot = snapshot or FakeWorksheet()
        self._history = history or FakeWorksheet()
        self.workbook_calls: list[str] = []

    def workbook(self, item_id):
        self.workbook_calls.append(item_id)
        return self

    def worksheet(self, name):
        return self._snapshot if name in ("raw_inventory", "raw_sales") else self._history


@pytest.fixture(autouse=True)
def _credentials_present(monkeypatch):
    monkeypatch.setattr(
        "src.reporting.publisher.microsoft_auth.credentials_available", lambda: True
    )


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("src.reporting.publisher.time.sleep", lambda _seconds: None)


class TestPublishHappyPath:
    def test_writes_history_and_snapshot_then_sends_two_reports(self, tmp_path):
        transport = FakeTransport()
        snapshot = FakeWorksheet()
        history = FakeWorksheet()
        publisher = FakePublisher(
            snapshot=snapshot, history=history, transport=transport,
            ledger=PublishLedger(tmp_path / "ledger.json"),
        )

        result = publisher.publish("inventory", _inventory_items(), {"FBA": date(2026, 9, 23)})

        assert result.published
        assert result.report_date == date(2026, 9, 23)
        assert len(history.appended) == 2
        assert len(snapshot.upserted) == 2
        assert [title for title, _ in transport.sent] == [
            "Inventory Summary — 2026-09-23", "Inventory Anomalies — 2026-09-23",
        ]
        assert result.history_appended == 2
        assert result.messages_sent == 2

    def test_ledger_records_the_publication(self, tmp_path):
        ledger = PublishLedger(tmp_path / "ledger.json")
        publisher = FakePublisher(
            transport=FakeTransport(), ledger=ledger,
        )

        publisher.publish("inventory", _inventory_items(), {})

        assert ledger.get("inventory", date(2026, 9, 23))["history_appended"] == 2

    def test_sales_lane_uses_the_sales_sheets(self, tmp_path):
        transport = FakeTransport()
        publisher = FakePublisher(
            transport=transport, ledger=PublishLedger(tmp_path / "ledger.json"),
        )
        items = [
            SalesRecord(
                id="20260923_Amazon_1001", sku_channel_id="Amazon_1001",
                Date=date(2026, 9, 23), SKU="1001", Channel="Amazon",
                Units=10, Revenue=100.0,
            )
        ]

        result = publisher.publish("sales", items, {"Amazon": date(2026, 9, 23)})

        assert result.published
        assert "Sales Summary — 2026-09-23" in [title for title, _ in transport.sent]


class TestIdempotency:
    def test_second_publish_of_the_same_date_is_refused(self, tmp_path):
        ledger = PublishLedger(tmp_path / "ledger.json")
        snapshot = FakeWorksheet()
        first = FakePublisher(snapshot=snapshot, transport=FakeTransport(), ledger=ledger)
        first.publish("inventory", _inventory_items(), {})

        second_transport = FakeTransport()
        second_snapshot = FakeWorksheet()
        second = FakePublisher(
            snapshot=second_snapshot, transport=second_transport, ledger=ledger,
        )
        result = second.publish("inventory", _inventory_items(), {})

        assert result.skipped_reason is not None
        assert "already published" in result.skipped_reason
        assert second_snapshot.upserted == []
        assert second_transport.sent == []

    def test_force_overrides_the_guard(self, tmp_path):
        ledger = PublishLedger(tmp_path / "ledger.json")
        FakePublisher(transport=FakeTransport(), ledger=ledger).publish(
            "inventory", _inventory_items(), {}
        )

        snapshot = FakeWorksheet()
        forced = FakePublisher(
            snapshot=snapshot, transport=FakeTransport(), ledger=ledger, force=True,
        )
        result = forced.publish("inventory", _inventory_items(), {})

        assert result.published
        assert len(snapshot.upserted) == 2

    def test_different_dates_do_not_collide(self, tmp_path):
        ledger = PublishLedger(tmp_path / "ledger.json")
        ledger.record(
            PublishResult(
                report_type="inventory", report_date=date(2026, 9, 22),
                history_appended=1, snapshot_updated=1, messages_sent=2,
            )
        )

        publisher = FakePublisher(transport=FakeTransport(), ledger=ledger)
        result = publisher.publish("inventory", _inventory_items(), {})

        assert result.published


class TestDryRun:
    def test_dry_run_writes_nothing_and_posts_nothing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.settings.OUTPUT_DIR", tmp_path)
        snapshot = FakeWorksheet()
        history = FakeWorksheet()
        publisher = FakePublisher(
            snapshot=snapshot, history=history, transport=FakeTransport(),
            ledger=PublishLedger(tmp_path / "ledger.json"), dry_run=True,
        )

        result = publisher.publish("inventory", _inventory_items(), {"FBA": date(2026, 9, 23)})

        assert history.appended == []
        assert snapshot.upserted == []
        assert result.messages_sent == 2
        assert sorted(p.name for p in tmp_path.glob("*.html")) == [
            "inventory-anomalies-2026-09-23.html", "inventory-summary-2026-09-23.html",
        ]

    def test_dry_run_does_not_touch_the_ledger(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.settings.OUTPUT_DIR", tmp_path)
        ledger = PublishLedger(tmp_path / "ledger.json")
        publisher = FakePublisher(ledger=ledger, dry_run=True)

        publisher.publish("inventory", _inventory_items(), {})

        assert ledger.get("inventory", date(2026, 9, 23)) is None

    def test_dry_run_bypasses_the_already_published_guard(self, tmp_path, monkeypatch):
        monkeypatch.setattr("src.settings.OUTPUT_DIR", tmp_path)
        ledger = PublishLedger(tmp_path / "ledger.json")
        FakePublisher(transport=FakeTransport(), ledger=ledger).publish(
            "inventory", _inventory_items(), {}
        )

        result = FakePublisher(ledger=ledger, dry_run=True).publish(
            "inventory", _inventory_items(), {}
        )

        assert result.published


class TestFailureContainment:
    def test_empty_data_is_skipped(self, tmp_path):
        publisher = FakePublisher(transport=FakeTransport(), ledger=PublishLedger(tmp_path / "l.json"))

        result = publisher.publish("inventory", [], {})

        assert result.skipped_reason == "no rows"
        assert result.messages_sent == 0

    def test_snapshot_read_failure_aborts_before_writing(self, tmp_path):
        transport = FakeTransport()
        publisher = FakePublisher(
            snapshot=FakeWorksheet(fail="read"), history=FakeWorksheet(),
            transport=transport, ledger=PublishLedger(tmp_path / "ledger.json"),
        )

        result = publisher.publish("inventory", _inventory_items(), {})

        assert result.errors and "snapshot read failed" in result.errors[0]
        assert result.history_appended == 0
        # the failure notice is the only thing sent
        assert [title for title, _ in transport.sent] == ["inventory-publish-error"]

    def test_history_failure_does_not_stop_the_reports(self, tmp_path):
        transport = FakeTransport()
        snapshot = FakeWorksheet()
        publisher = FakePublisher(
            snapshot=snapshot, history=FakeWorksheet(fail="append"),
            transport=transport, ledger=PublishLedger(tmp_path / "ledger.json"),
        )

        result = publisher.publish("inventory", _inventory_items(), {})

        assert "history append failed" in result.errors[0]
        assert len(snapshot.upserted) == 2
        assert len(transport.sent) == 2

    def test_failed_publish_is_not_recorded_in_the_ledger(self, tmp_path):
        ledger = PublishLedger(tmp_path / "ledger.json")
        publisher = FakePublisher(
            history=FakeWorksheet(fail="append"), transport=FakeTransport(), ledger=ledger,
        )

        publisher.publish("inventory", _inventory_items(), {})

        assert ledger.get("inventory", date(2026, 9, 23)) is None

    def test_transport_failure_is_recorded_but_does_not_raise(self, tmp_path):
        publisher = FakePublisher(
            transport=FakeTransport(fail_on="Anomalies"),
            ledger=PublishLedger(tmp_path / "ledger.json"),
        )

        result = publisher.publish("inventory", _inventory_items(), {})

        assert result.messages_sent == 1
        assert "delivery failed" in result.errors[0]


class TestMaxReportDate:
    def test_picks_the_latest_date(self):
        rows = [{"Date": "2026-09-21"}, {"Date": "2026-09-23"}, {"Date": 46288}]
        assert _max_report_date(rows) == date(2026, 9, 23)

    def test_ignores_unparseable_dates(self):
        assert _max_report_date([{"Date": ""}, {"Date": None}]) is None
