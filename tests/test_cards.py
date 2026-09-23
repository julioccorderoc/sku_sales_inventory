"""Adaptive Card renderers — tables, ranking, and the caps that bound size."""
import json
from datetime import date

from src.reporting.cards import (
    CARD_VERSION,
    MAX_ROWS_PER_CHANNEL,
    MAX_TOTAL_ROWS,
    build_inventory_anomalies_card,
    build_inventory_summary_card,
    build_sales_anomalies_card,
    build_sales_summary_card,
    simple_card,
)


def _tables(card: dict) -> list[dict]:
    return [block for block in card["body"] if block.get("type") == "Table"]


def _texts(card: dict) -> list[str]:
    return [block["text"] for block in card["body"] if block.get("type") == "TextBlock"]


def _cell(row: dict, index: int) -> str:
    return row["cells"][index]["items"][0]["text"]


def _rows(table: dict) -> list[dict]:
    return table["rows"]


def _inventory_row(channel, sku, units, inventory, delta_units, delta_inventory, days=1):
    return {
        "Channel": channel, "SKU": sku, "Units": units, "Inventory": inventory,
        "Delta_Sold": delta_units, "Delta_Inventory": delta_inventory,
        "Days_Since_Last_Report": days,
    }


def _sales_row(channel, sku, units, revenue, delta_units, delta_revenue):
    return {
        "Channel": channel, "SKU": sku, "Units": units, "Revenue": revenue,
        "Delta_Sold": delta_units, "Delta_Revenue": delta_revenue,
    }


class TestCardEnvelope:
    def test_card_declares_the_table_capable_version(self):
        card = simple_card("Title", "Body")

        assert card["type"] == "AdaptiveCard"
        assert card["version"] == CARD_VERSION == "1.5"

    def test_simple_card_carries_both_lines(self):
        card = simple_card("Heading", "The message")

        assert _texts(card) == ["Heading", "The message"]


class TestInventorySummaryCard:
    def test_lists_every_channel_with_its_date(self):
        card = build_inventory_summary_card({"FBA": date(2026, 9, 23), "AWD": None})

        table = _tables(card)[0]
        assert table["showGridLines"] is True
        assert table["firstRowAsHeader"] is True

        rows = _rows(table)
        assert _cell(rows[0], 0) == "Channel"  # header row
        assert _cell(rows[1], 0) == "✅ FBA"
        assert _cell(rows[1], 1) == "23 Sep 2026"
        assert _cell(rows[2], 0) == "⚠️ AWD"
        assert _cell(rows[2], 1) == "report file not found"

    def test_subtitle_counts_skips(self):
        card = build_inventory_summary_card({"FBA": date(2026, 9, 23), "AWD": None})

        assert "2 channels · 1 updated, 1 skipped" in _texts(card)

    def test_all_updated_reads_cleanly(self):
        card = build_inventory_summary_card({"FBA": date(2026, 9, 23)})

        assert "1 channels · all updated" in _texts(card)


class TestSalesSummaryCard:
    def test_metrics_are_rows_with_net_change(self):
        card = build_sales_summary_card([
            _sales_row("Amazon", "1001", 110, 150.0, 10, 50.0),
        ])

        rows = _rows(_tables(card)[0])
        assert [_cell(rows[0], index) for index in range(3)] == ["Metric", "Value", "Net change"]
        assert _cell(rows[1], 0) == "Total revenue"
        assert _cell(rows[1], 1) == "$150.00"
        assert "+$50.00" in _cell(rows[1], 2)
        assert "⬆️ 50.0%" in _cell(rows[1], 2)

    def test_tiktok_shop_is_excluded(self):
        card = build_sales_summary_card([
            _sales_row("Amazon", "1001", 10, 100.0, 0, 0.0),
            _sales_row("TikTok Shop", "1001", 999, 9999.0, 0, 0.0),
        ])

        assert "$9,999.00" not in json.dumps(card)
        assert "Channels: Amazon" in _texts(card)

    def test_bundles_are_a_separate_row(self):
        card = build_sales_summary_card([
            _sales_row("Shopify", "1001", 10, 100.0, 0, 0.0),
            _sales_row("Shopify", "Bundles", 3, 45.0, 0, 0.0),
        ])

        rows = _rows(_tables(card)[0])
        assert _cell(rows[3], 0) == "Bundle revenue"
        assert _cell(rows[3], 1) == "$45.00"


class TestInventoryAnomaliesCard:
    def test_clean_run_says_so_without_a_table(self):
        card = build_inventory_anomalies_card([_inventory_row("FBA", "1001", 100, 500, 0, 0)])

        assert _tables(card) == []
        assert "✅ Inventory — no anomalies" in _texts(card)

    def test_flagged_rows_render_as_a_table_per_channel(self):
        card = build_inventory_anomalies_card([
            _inventory_row("FBA", "1001", 0, 500, -100, 0),
            _inventory_row("DTC", "2001", 0, 500, -100, 0),
        ])

        tables = _tables(card)
        assert len(tables) == 2
        assert "FBA · 1" in _texts(card)
        assert "DTC · 1" in _texts(card)

        rows = _rows(tables[0])
        assert [_cell(rows[0], index) for index in range(4)] == ["SKU", "Sold", "Stock", "Issue"]
        assert _cell(rows[1], 0) == "1001"
        assert _cell(rows[1], 1) == "0 (-100)"      # current, with the move
        assert _cell(rows[1], 3) == "⚠️ Zero sales"

    def test_headline_counts_rows_and_channels(self):
        card = build_inventory_anomalies_card([
            _inventory_row("FBA", "1001", 0, 500, -100, 0),
            _inventory_row("FBA", "2001", 0, 500, -100, 0),
            _inventory_row("DTC", "3001", 0, 500, -100, 0),
        ])

        assert "3 flagged rows · 2 channels" in _texts(card)

    def test_issue_text_is_the_compact_form(self):
        card = build_inventory_anomalies_card([_inventory_row("FBA", "1001", 100, 100, 0, -400)])

        issue = _cell(_rows(_tables(card)[0])[1], 3)
        assert issue == "📦 exp 497 · real 100"
        assert "Inv Anomaly" not in issue

    def test_rows_are_ranked_worst_first(self):
        card = build_inventory_anomalies_card([
            _inventory_row("FBA", "SMALL", 200, 500, 100, 0),    # +100% swing
            _inventory_row("FBA", "ZERO", 0, 500, -100, 0),      # zero sales
        ])

        rows = _rows(_tables(card)[0])
        assert _cell(rows[1], 0) == "ZERO"

    def test_channel_cap_drops_the_least_severe_and_says_how_many(self):
        rows = [
            _inventory_row("FBA", f"SKU{index:03d}", 0, 500, -100, 0)
            for index in range(MAX_ROWS_PER_CHANNEL + 4)
        ]

        card = build_inventory_anomalies_card(rows)

        table = _tables(card)[0]
        assert len(_rows(table)) == MAX_ROWS_PER_CHANNEL + 1  # + header
        assert f"FBA · {MAX_ROWS_PER_CHANNEL}" in _texts(card)
        assert any("+4 more flagged rows" in text for text in _texts(card))

    def test_total_cap_bounds_a_multi_channel_flood(self):
        rows = [
            _inventory_row(f"CH{channel}", f"SKU{index}", 0, 500, -100, 0)
            for channel in range(8)
            for index in range(MAX_ROWS_PER_CHANNEL)
        ]

        card = build_inventory_anomalies_card(rows)

        shown = sum(len(_rows(table)) - 1 for table in _tables(card))
        assert shown == MAX_TOTAL_ROWS

    def test_payload_stays_inside_the_teams_card_limit(self):
        rows = [
            _inventory_row(f"CH{channel}", f"SKU{index}", 0, 500, -100, -400)
            for channel in range(8)
            for index in range(MAX_ROWS_PER_CHANNEL)
        ]

        size = len(json.dumps({"type": "message", "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": build_inventory_anomalies_card(rows),
        }]}))

        assert size < 24_000, f"card payload is {size:,} bytes"

    def test_columns_are_sized_to_their_content(self):
        card = build_inventory_anomalies_card([
            _inventory_row("FBA", "PH50012P", 5000, 500, -100, 0),
            _inventory_row("FBA", "1001", 0, 500, -100, 0),
        ])

        widths = _tables(card)[0]["columns"]
        # Issue holds the longest text, so it earns the largest weight
        assert widths[3]["width"] > widths[1]["width"] > widths[0]["width"] >= 90

    def test_widths_are_numbers_never_px_strings(self):
        """Teams parses `"96px"` as the number 96 and treats it as a weight,
        while the Adaptive Cards SDK honours it as fixed pixels. A card mixing
        the two collapsed the weighted column to under 1% in Teams. Numbers are
        the only form both renderers agree on."""
        card = build_inventory_anomalies_card([
            _inventory_row("FBA", "1001", 0, 500, -100, 0),
        ])

        for table in _tables(card):
            for column in table["columns"]:
                assert isinstance(column["width"], int), column
        for table in _tables(build_sales_anomalies_card([_sales_row("Amazon", "1001", 0, 0.0, -100, -500.0)])):
            for column in table["columns"]:
                assert isinstance(column["width"], int), column
        for table in _tables(build_inventory_summary_card({"FBA": date(2026, 9, 23)})):
            for column in table["columns"]:
                assert isinstance(column["width"], int), column
        for table in _tables(build_sales_summary_card([_sales_row("Amazon", "1001", 10, 100.0, 0, 0.0)])):
            for column in table["columns"]:
                assert isinstance(column["width"], int), column

    def test_unknown_channel_is_rendered_after_the_target_channels(self):
        card = build_inventory_anomalies_card([
            _inventory_row("Other", "1001", 0, 500, -100, 0),
            _inventory_row("FBA", "2001", 0, 500, -100, 0),
        ])

        headings = [text for text in _texts(card) if "·" in text and "flagged" not in text]
        assert headings == ["FBA · 1", "Other · 1"]


class TestSalesAnomaliesCard:
    def test_revenue_swing_row(self):
        card = build_sales_anomalies_card([
            _sales_row("Amazon", "1001", 0, 0.0, -100, -500.0),
        ])

        rows = _rows(_tables(card)[0])
        assert [_cell(rows[0], index) for index in range(4)] == ["SKU", "Revenue", "Units", "Issue"]
        assert _cell(rows[1], 1) == "$0.00 (-$500.00)"
        assert _cell(rows[1], 3) == "📉 Revenue -100.0%\n⚠️ Zero sales"

    def test_tiktok_reconciliation_is_not_on_the_card(self):
        """It lives in the HTML report; on the card it was noise."""
        card = build_sales_anomalies_card([
            _sales_row("TikTok Shop", "1001", 5, 50.0, 0, 0.0),
            _sales_row("TikTok Shopify", "1001", 2, 20.0, 0, 0.0),
        ])

        assert "TikTok — Shop vs Shopify" not in _texts(card)
        assert "✅ Sales — no anomalies" in _texts(card)

    def test_tiktok_rows_are_still_excluded_from_anomaly_tables(self):
        card = build_sales_anomalies_card([
            _sales_row("TikTok Shop", "1001", 0, 0.0, -100, -500.0),
        ])

        assert _tables(card) == []
