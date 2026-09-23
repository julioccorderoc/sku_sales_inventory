"""HTML report builders — behaviour ported from the four n8n Code nodes."""
from datetime import date

from src.reporting.html import (
    build_inventory_anomalies,
    build_inventory_summary,
    build_sales_anomalies,
    build_sales_summary,
    format_long_date,
)


class TestFormatLongDate:
    def test_date_object(self):
        assert format_long_date(date(2026, 9, 23)) == "September 23, 2026"

    def test_iso_string(self):
        assert format_long_date("2026-09-23") == "September 23, 2026"


class TestInventorySummary:
    def test_updated_and_skipped_lines(self):
        html = build_inventory_summary({"FBA": date(2026, 9, 23), "AWD": None})

        assert "📊 Inventory Update Summary" in html
        assert "✅ <strong>FBA:</strong> Updated with data from <strong>September 23, 2026</strong>" in html
        assert "⚠️ <strong>AWD:</strong> Update SKIPPED (report file not found)" in html

    def test_channel_order_is_preserved(self):
        html = build_inventory_summary({"FBA": None, "AWD": None, "DTC": None})
        assert html.index("FBA") < html.index("AWD") < html.index("DTC")

    def test_empty_summary_still_renders(self):
        assert "<ul>" in build_inventory_summary({})


def _sales_row(channel, sku, units, revenue, delta_units=0, delta_revenue=0.0):
    return {
        "Channel": channel, "SKU": sku, "Units": units, "Revenue": revenue,
        "Delta_Sold": delta_units, "Delta_Revenue": delta_revenue,
    }


class TestSalesSummary:
    def test_tiktok_shop_is_excluded_from_totals_and_channels(self):
        rows = [
            _sales_row("Amazon", "1001", 10, 100.0),
            _sales_row("TikTok Shop", "1001", 999, 9999.0),
        ]

        html = build_sales_summary(rows)

        assert "Channels: Amazon" in html
        assert "TikTok Shop" not in html
        assert "$100.00" in html
        assert "$9,999.00" not in html

    def test_bundles_are_split_from_normal_totals(self):
        rows = [
            _sales_row("Shopify", "1001", 10, 100.0),
            _sales_row("Shopify", "Bundles", 3, 45.0),
        ]

        html = build_sales_summary(rows)

        assert "<strong>Total Revenue:</strong> $100.00" in html
        assert "<strong>Total Bundle Revenue:</strong> $45.00" in html
        assert "<strong>Total Bundle Units:</strong> 3" in html

    def test_net_change_uses_signed_money(self):
        rows = [_sales_row("Amazon", "1001", 110, 150.0, delta_units=10, delta_revenue=50.0)]

        html = build_sales_summary(rows)

        assert "+$50.00" in html
        assert "+10" in html
        assert "⬆️ 50.0%" in html

    def test_zero_change_reads_as_zero(self):
        rows = [_sales_row("Amazon", "1001", 10, 100.0, delta_units=0, delta_revenue=0.0)]

        html = build_sales_summary(rows)

        assert "$0.00" in html
        assert "0.0%" in html


def _inventory_row(channel, sku, units, inventory, delta_units, delta_inventory, days=1):
    return {
        "Channel": channel, "SKU": sku, "Units": units, "Inventory": inventory,
        "Delta_Sold": delta_units, "Delta_Inventory": delta_inventory,
        "Days_Since_Last_Report": days,
    }


class TestInventoryAnomalies:
    def test_no_anomalies_message(self):
        rows = [_inventory_row("FBA", "1001", 100, 500, 0, 0)]

        html = build_inventory_anomalies(rows)

        assert "No anomalies detected" in html
        assert "🚨" not in html

    def test_zero_sales_is_flagged(self):
        rows = [_inventory_row("FBA", "1001", 0, 500, -100, 0)]

        html = build_inventory_anomalies(rows)

        assert "⚠️ Zero Sales" in html
        assert "Daily Anomalies Report" in html

    def test_sales_swing_above_threshold(self):
        rows = [_inventory_row("FBA", "1001", 200, 500, 100, 0)]

        html = build_inventory_anomalies(rows)

        assert "📈 Sales Swing (100.0%)" in html

    def test_small_swing_is_not_flagged(self):
        rows = [_inventory_row("FBA", "1001", 102, 500, 2, 0)]

        assert "No anomalies detected" in build_inventory_anomalies(rows)

    def test_low_volume_is_ignored(self):
        rows = [_inventory_row("FBA", "1001", 0, 500, -5, 0)]

        assert "No anomalies detected" in build_inventory_anomalies(rows)

    def test_reserve_and_awd_never_raise_sales_alerts(self):
        rows = [
            _inventory_row("Reserve", "1001", 0, 500, -100, 0),
            _inventory_row("AWD", "1001", 0, 500, -100, 0),
        ]

        assert "No anomalies detected" in build_inventory_anomalies(rows)

    def test_two_pack_only_alerts_on_fba(self):
        on_fba = [_inventory_row("FBA", "10012P", 0, 500, -100, 0)]
        on_dtc = [_inventory_row("DTC", "10012P", 0, 500, -100, 0)]

        assert "⚠️ Zero Sales" in build_inventory_anomalies(on_fba)
        assert "No anomalies detected" in build_inventory_anomalies(on_dtc)

    def test_inventory_anomaly_reports_expected_and_real(self):
        rows = [_inventory_row("FBA", "1001", 100, 100, 0, -400)]

        html = build_inventory_anomalies(rows)

        assert "📦⬇️ Inv Anomaly" in html
        assert "Real: 100" in html

    def test_unknown_channel_lands_in_other(self):
        rows = [_inventory_row("NewChannel", "1001", 0, 500, -100, 0)]

        html = build_inventory_anomalies(rows)

        assert ">Other<" in html
        assert "NewChannel" not in html


def _sales_delta_row(channel, sku, units, revenue, delta_units, delta_revenue):
    return {
        "Channel": channel, "SKU": sku, "Units": units, "Revenue": revenue,
        "Delta_Sold": delta_units, "Delta_Revenue": delta_revenue,
    }


class TestSalesAnomalies:
    def test_no_anomalies_message(self):
        rows = [_sales_delta_row("Amazon", "1001", 100, 1000.0, 0, 0.0)]

        html = build_sales_anomalies(rows)

        assert "No anomalies detected" in html
        assert "Sales Anomalies Report" not in html

    def test_revenue_swing(self):
        rows = [_sales_delta_row("Amazon", "1001", 0, 0.0, -100, -500.0)]

        html = build_sales_anomalies(rows)

        assert "🔻 Rev Swing (-100.0%)" in html
        assert "⚠️ Zero Sales" in html

    def test_unit_swing_when_revenue_is_flat(self):
        rows = [_sales_delta_row("Shopify", "1001", 200, 1000.0, 100, 0.0)]

        html = build_sales_anomalies(rows)

        assert "📦⬆️ Unit Swing (100.0%)" in html

    def test_low_volume_is_skipped(self):
        rows = [_sales_delta_row("Amazon", "1001", 0, 0.0, -5, -50.0)]

        assert "No anomalies detected" in build_sales_anomalies(rows)

    def test_small_revenue_base_is_skipped(self):
        """Under $100 of prior revenue the swing is noise, even at -33%."""
        rows = [_sales_delta_row("Amazon", "1001", 50, 20.0, 0, -10.0)]

        assert "No anomalies detected" in build_sales_anomalies(rows)

    def test_tiktok_shop_is_excluded_from_anomaly_groups(self):
        rows = [
            _sales_delta_row("TikTok Shop", "1001", 0, 0.0, -100, -500.0),
            _sales_delta_row("TikTok Shopify", "1001", 0, 0.0, -100, -500.0),
        ]

        html = build_sales_anomalies(rows)

        assert "TikTok Shopify</h3>" in html
        assert "TikTok Shop</h3>" not in html

    def test_tiktok_comparison_table_lists_mismatches(self):
        rows = [
            _sales_delta_row("TikTok Shop", "1001", 5, 50.0, 0, 0.0),
            _sales_delta_row("TikTok Shopify", "1001", 2, 20.0, 0, 0.0),
        ]

        html = build_sales_anomalies(rows)

        assert "⚖️ TikTok Comparison (Shop vs Shopify)" in html
        assert "⬆️ $30.00" in html
        assert "⬆️ 3" in html

    def test_tiktok_table_hidden_when_no_tiktok_rows(self):
        rows = [_sales_delta_row("Amazon", "1001", 100, 1000.0, 0, 0.0)]

        assert "TikTok Comparison" not in build_sales_anomalies(rows)

    def test_tiktok_rows_with_no_revenue_are_dropped(self):
        rows = [
            _sales_delta_row("TikTok Shop", "1001", 0, 0.0, 0, 0.0),
            _sales_delta_row("TikTok Shopify", "1001", 0, 0.0, 0, 0.0),
        ]

        html = build_sales_anomalies(rows)

        assert "⚖️ TikTok Comparison" in html
        assert "<strong>1001</strong>" not in html
