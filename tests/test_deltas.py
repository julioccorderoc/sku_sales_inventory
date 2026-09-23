"""Delta computation — the port of the two n8n `Get Delta *` code nodes."""
from datetime import date

import pytest

from src.reporting.deltas import (
    compute_deltas,
    days_between,
    index_by_key,
    parse_date,
)


class TestParseDate:
    def test_excel_serial(self):
        assert parse_date(46288) == date(2026, 9, 23)
        assert parse_date(45908) == date(2025, 9, 8)

    def test_serial_as_string(self):
        assert parse_date("46288") == date(2026, 9, 23)

    def test_iso_string(self):
        assert parse_date("2026-09-23") == date(2026, 9, 23)
        assert parse_date("2026-09-23T00:00:00Z") == date(2026, 9, 23)

    def test_date_object_passthrough(self):
        assert parse_date(date(2026, 1, 5)) == date(2026, 1, 5)

    def test_us_style_string(self):
        assert parse_date("09/23/2026") == date(2026, 9, 23)

    @pytest.mark.parametrize("value", [None, "", "   ", "not-a-date", True])
    def test_unparseable_returns_none(self, value):
        assert parse_date(value) is None


class TestDaysBetween:
    def test_whole_days(self):
        assert days_between(date(2026, 9, 23), date(2026, 9, 22)) == 1
        assert days_between(date(2026, 9, 23), date(2026, 7, 20)) == 65

    def test_missing_side_is_zero(self):
        assert days_between(date(2026, 9, 23), None) == 0
        assert days_between(None, date(2026, 9, 23)) == 0
        assert days_between(None, None) == 0

    def test_same_day_is_zero(self):
        assert days_between(date(2026, 9, 23), date(2026, 9, 23)) == 0


class TestIndexByKey:
    def test_skips_blank_keys(self):
        rows = [
            {"sku_channel_id": "FBA_1001", "Units": 1},
            {"sku_channel_id": "", "Units": 2},
            {"sku_channel_id": None, "Units": 3},
        ]
        assert list(index_by_key(rows)) == ["FBA_1001"]


class TestComputeDeltasInventory:
    def test_matched_row_reports_every_delta(self):
        new = [{
            "id": "20260923_FBA_1001", "sku_channel_id": "FBA_1001",
            "Date": "2026-09-23", "SKU": "1001", "Channel": "FBA",
            "Units": 381, "Inventory": 1947, "Inbound": 0,
        }]
        old = [{
            "sku_channel_id": "FBA_1001", "Date": 46287,
            "Units": 400, "Inventory": 2000, "Inbound": 50,
        }]

        row = compute_deltas(new, old, "inventory")[0]

        assert row["Days_Since_Last_Report"] == 1
        assert row["Delta_Sold"] == -19
        assert row["Delta_Inventory"] == -53
        assert row["Delta_Inbound"] == -50

    def test_unmatched_row_is_kept_and_counts_its_whole_value_as_the_delta(self):
        """The n8n enrich-join dropped these; a new SKU must still be reported.

        With no prior snapshot every value is new, so the delta equals the
        current figure — the same arithmetic n8n ran (`|| 0` on a missing key).
        """
        new = [{
            "sku_channel_id": "FBA_9999", "Date": "2026-09-23",
            "SKU": "9999", "Channel": "FBA", "Units": 5, "Inventory": 5, "Inbound": 0,
        }]

        row = compute_deltas(new, [], "inventory")[0]

        assert row["Days_Since_Last_Report"] == 0
        assert row["Delta_Sold"] == 5
        assert row["Delta_Inventory"] == 5

    def test_old_serial_dates_are_understood(self):
        new = [{"sku_channel_id": "FBA_1001", "Date": "2026-09-23", "Units": 1,
                "Inventory": 1, "Inbound": 0}]
        old = [{"sku_channel_id": "FBA_1001", "Date": 46223, "Units": 1,
                "Inventory": 1, "Inbound": 0}]

        # 46223 = 2026-07-20 → 65 days before 2026-09-23
        assert compute_deltas(new, old, "inventory")[0]["Days_Since_Last_Report"] == 65

    def test_float_units_from_the_sheet_are_coerced_to_int(self):
        new = [{"sku_channel_id": "FBA_1001", "Date": "2026-09-23", "Units": 10.0,
                "Inventory": 20.0, "Inbound": 0.0}]
        old = [{"sku_channel_id": "FBA_1001", "Date": 46287, "Units": 4.0,
                "Inventory": 20.0, "Inbound": 0.0}]

        row = compute_deltas(new, old, "inventory")[0]

        assert row["Units"] == 10
        assert row["Delta_Sold"] == 6
        assert isinstance(row["Delta_Sold"], int)


class TestComputeDeltasSales:
    def test_revenue_delta_is_named_delta_revenue(self):
        new = [{"sku_channel_id": "Amazon_1001", "Date": "2026-09-23", "SKU": "1001",
                "Channel": "Amazon", "Units": 57, "Revenue": 1010.36}]
        old = [{"sku_channel_id": "Amazon_1001", "Date": 46223, "Units": 40,
                "Revenue": 800.0}]

        row = compute_deltas(new, old, "sales")[0]

        assert row["Delta_Sold"] == 17
        assert row["Delta_Revenue"] == pytest.approx(210.36)
        assert "Delta_Inventory" not in row
        assert row["Days_Since_Last_Report"] == 65

    def test_missing_numeric_values_default_to_zero(self):
        new = [{"sku_channel_id": "Amazon_1001", "Date": "2026-09-23",
                "Units": None, "Revenue": ""}]

        row = compute_deltas(new, [], "sales")[0]

        assert row["Units"] == 0
        assert row["Revenue"] == 0.0
