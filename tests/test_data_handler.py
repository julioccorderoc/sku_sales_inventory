"""
Tests for post_to_webhook retry logic in src/data_handler.py.

Uses mocked requests.post and time.sleep to verify:
- Success on first attempt → single call, no sleep
- 5xx response → retries with exponential backoff, succeeds on retry
- 5xx exhausting all retries → logs error, does not raise
- ConnectionError → retries with exponential backoff
- 4xx response → no retry, logs error immediately
- Exponential backoff delays follow WEBHOOK_RETRY_BACKOFF ** attempt
- No-op when WEBHOOK_URL is unset
"""
import pytest
from unittest.mock import patch, MagicMock
import requests

from src import data_handler


@pytest.fixture
def dummy_records():
    record = MagicMock()
    record.model_dump.return_value = {"id": "test_id", "sku": "1001"}
    return [record]


@pytest.fixture
def dummy_metadata():
    return {"date": "2026-03-03", "totalRecords": 1}


def _ok_response():
    r = MagicMock()
    r.status_code = 200
    r.raise_for_status.return_value = None
    return r


def _5xx_response(code=500):
    r = MagicMock()
    r.status_code = code
    return r


def _4xx_response(code=422):
    r = MagicMock()
    r.status_code = code
    r.raise_for_status.side_effect = requests.exceptions.HTTPError(response=r)
    return r


PATCHES = {
    "url": ("src.data_handler.settings.WEBHOOK_URL", "https://example.com/webhook"),
    "retries": ("src.data_handler.settings.WEBHOOK_MAX_RETRIES", 3),
    "backoff": ("src.data_handler.settings.WEBHOOK_RETRY_BACKOFF", 2.0),
}


class TestPostToWebhookRetry:
    def _run(self, mock_post_side_effect, dummy_records, dummy_metadata):
        """Helper: run post_to_webhook with patched settings and return (mock_post, mock_sleep)."""
        with patch("src.data_handler.requests.post", side_effect=mock_post_side_effect) as mock_post, \
             patch("src.data_handler.time.sleep") as mock_sleep, \
             patch(*PATCHES["url"]), \
             patch(*PATCHES["retries"]), \
             patch(*PATCHES["backoff"]):
            data_handler.post_to_webhook(dummy_records, dummy_metadata)
        return mock_post, mock_sleep

    def test_success_on_first_attempt(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run([_ok_response()], dummy_records, dummy_metadata)
        mock_post.assert_called_once()
        mock_sleep.assert_not_called()

    def test_5xx_retries_then_succeeds(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run(
            [_5xx_response(503), _ok_response()], dummy_records, dummy_metadata
        )
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(2.0)  # backoff ** 1

    def test_5xx_exhausts_all_retries(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run(
            [_5xx_response()] * 3, dummy_records, dummy_metadata
        )
        assert mock_post.call_count == 3

    def test_connection_error_retries_then_succeeds(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run(
            [requests.exceptions.ConnectionError(), _ok_response()],
            dummy_records, dummy_metadata,
        )
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once_with(2.0)

    def test_connection_error_exhausts_all_retries(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run(
            [requests.exceptions.ConnectionError()] * 3, dummy_records, dummy_metadata
        )
        assert mock_post.call_count == 3

    def test_no_retry_on_4xx(self, dummy_records, dummy_metadata):
        mock_post, mock_sleep = self._run([_4xx_response()], dummy_records, dummy_metadata)
        mock_post.assert_called_once()
        mock_sleep.assert_not_called()

    def test_exponential_backoff_delays(self, dummy_records, dummy_metadata):
        """All 3 attempts fail → sleeps after attempt 1 (2s) and attempt 2 (4s), not after 3."""
        mock_post, mock_sleep = self._run(
            [_5xx_response()] * 3, dummy_records, dummy_metadata
        )
        assert mock_sleep.call_count == 2
        calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert calls == [2.0, 4.0]  # 2^1, 2^2

    def test_skips_when_no_webhook_url(self, dummy_records, dummy_metadata):
        with patch("src.data_handler.requests.post") as mock_post, \
             patch("src.data_handler.settings.WEBHOOK_URL", None):
            data_handler.post_to_webhook(dummy_records, dummy_metadata)
        mock_post.assert_not_called()

    def test_does_not_raise_after_exhausted_retries(self, dummy_records, dummy_metadata):
        """Pipeline must not surface an unhandled exception after all retries fail."""
        with patch("src.data_handler.requests.post", return_value=_5xx_response()), \
             patch("src.data_handler.time.sleep"), \
             patch(*PATCHES["url"]), \
             patch(*PATCHES["retries"]), \
             patch(*PATCHES["backoff"]):
            data_handler.post_to_webhook(dummy_records, dummy_metadata)  # must not raise


# ---------------------------------------------------------------------------
# TestLogRunHistory
# ---------------------------------------------------------------------------

from datetime import date as _date  # noqa: E402
from unittest.mock import MagicMock


def _make_inventory_record(report_date=_date(2026, 3, 20), units=10):
    r = MagicMock()
    r.report_date = report_date
    r.units = units
    # inventory records have no .revenue attribute
    del r.revenue
    return r


def _make_sales_record(report_date=_date(2026, 3, 23), units=5, revenue=150.0):
    r = MagicMock()
    r.report_date = report_date
    r.units = units
    r.revenue = revenue
    return r


class TestLogRunHistory:
    def _run(self, tmp_path, validated_data, pipeline, source_files=None):
        with patch("src.data_handler.settings.OUTPUT_DIR", tmp_path):
            data_handler.log_run_history(
                validated_data, pipeline, source_files or []
            )
        return tmp_path / "run_history.csv", tmp_path / "run_history.json"

    # -- CSV: structure --

    def test_creates_csv_with_headers(self, tmp_path):
        csv_path, _ = self._run(tmp_path, [_make_inventory_record()], "inventory")
        assert csv_path.exists()
        rows = csv_path.read_text().splitlines()
        assert rows[0] == "timestamp,pipeline,report_date,total_records,total_units,total_revenue,source_files"
        assert len(rows) == 2  # header + 1 data row

    def test_appends_row_on_subsequent_call(self, tmp_path):
        records = [_make_inventory_record()]
        self._run(tmp_path, records, "inventory")
        self._run(tmp_path, records, "inventory")
        rows = (tmp_path / "run_history.csv").read_text().splitlines()
        assert len(rows) == 3  # header + 2 data rows

    def test_headers_written_only_once(self, tmp_path):
        records = [_make_inventory_record()]
        self._run(tmp_path, records, "inventory")
        self._run(tmp_path, records, "inventory")
        header_count = (tmp_path / "run_history.csv").read_text().count("timestamp,pipeline")
        assert header_count == 1

    # -- Revenue field --

    def test_inventory_run_has_empty_revenue(self, tmp_path):
        csv_path, _ = self._run(tmp_path, [_make_inventory_record()], "inventory")
        import csv as _csv
        with open(csv_path) as f:
            row = list(_csv.DictReader(f))[0]
        assert row["total_revenue"] == ""

    def test_sales_run_has_revenue(self, tmp_path):
        records = [_make_sales_record(revenue=100.0), _make_sales_record(revenue=50.55)]
        csv_path, _ = self._run(tmp_path, records, "sales")
        import csv as _csv
        with open(csv_path) as f:
            row = list(_csv.DictReader(f))[0]
        assert float(row["total_revenue"]) == pytest.approx(150.55)

    # -- Aggregation --

    def test_total_units_and_records(self, tmp_path):
        records = [_make_inventory_record(units=10), _make_inventory_record(units=20)]
        csv_path, _ = self._run(tmp_path, records, "inventory")
        import csv as _csv
        with open(csv_path) as f:
            row = list(_csv.DictReader(f))[0]
        assert int(row["total_records"]) == 2
        assert int(row["total_units"]) == 30

    def test_report_date_is_max(self, tmp_path):
        records = [
            _make_inventory_record(report_date=_date(2026, 3, 18)),
            _make_inventory_record(report_date=_date(2026, 3, 20)),
        ]
        csv_path, _ = self._run(tmp_path, records, "inventory")
        import csv as _csv
        with open(csv_path) as f:
            row = list(_csv.DictReader(f))[0]
        assert row["report_date"] == "2026-03-20"

    def test_source_files_joined(self, tmp_path):
        csv_path, _ = self._run(
            tmp_path,
            [_make_inventory_record()],
            "inventory",
            source_files=["FBA_report_2026-03-20.csv", "AWD_report_2026-03-20.csv"],
        )
        import csv as _csv
        with open(csv_path) as f:
            row = list(_csv.DictReader(f))[0]
        assert row["source_files"] == "FBA_report_2026-03-20.csv, AWD_report_2026-03-20.csv"

    # -- No-op on empty data --

    def test_noop_on_empty_data(self, tmp_path):
        csv_path, json_path = self._run(tmp_path, [], "inventory")
        assert not csv_path.exists()
        assert not json_path.exists()

    # -- JSON: structure --

    def test_creates_json_array(self, tmp_path):
        _, json_path = self._run(tmp_path, [_make_inventory_record()], "inventory")
        import json as _json
        assert json_path.exists()
        data = _json.loads(json_path.read_text())
        assert isinstance(data, list)
        assert len(data) == 1

    def test_json_appends_to_existing(self, tmp_path):
        records = [_make_inventory_record()]
        self._run(tmp_path, records, "inventory")
        self._run(tmp_path, records, "inventory")
        import json as _json
        data = _json.loads((tmp_path / "run_history.json").read_text())
        assert len(data) == 2

    def test_json_recovers_from_malformed_file(self, tmp_path):
        json_path = tmp_path / "run_history.json"
        json_path.write_text("not valid json")
        _, json_path_result = self._run(tmp_path, [_make_inventory_record()], "inventory")
        import json as _json
        data = _json.loads(json_path_result.read_text())
        assert len(data) == 1  # fresh start after malformed file
