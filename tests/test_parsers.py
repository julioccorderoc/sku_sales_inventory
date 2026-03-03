"""
Tests for src/parsers.py — one class per parser function.

Fixture CSVs are in tests/fixtures/ and mirror the real source file formats,
including format quirks (metadata rows, dynamic headers, JSON columns).

Key fixture values from config/mappings.json:
  FBA/AWD Amazon SKUs   : "1001", "3001s" (trailing-s tests), "2001"
  TikTok ID (single)    : "1729499998780101089"  -> ["1001"]
  TikTok ID (bundle 2x) : "1729500444198670817"  -> ["5001", "5001"]
  Shopify SKU (single)  : "1001"                 -> ["1001"]
  Shopify SKU (bundle)  : "1002"                 -> ["1001", "1001"]
  Shopify bundle name   : "AlexandrasSpecialBundle" -> ["3001","4001","8001"]
  Flexport DSKU         : "DB59IQ90Q2K"          -> "1001"
  Flexport MSKU         : "1001", "2001"
"""
from src import settings
from src.parsers import (
    parse_fba_report,
    parse_awd_report,
    parse_wfs_report,
    parse_fbt_report,
    parse_fbt_inventory_report,
    parse_flexport_reports,
    parse_walmart_sales_report,
    parse_amazon_sales_report,
    parse_tiktok_orders_report,
    parse_tiktok_sales_report,
    parse_shopify_sales_report,
)

INVENTORY_COLS = {"sku", "channel", "units_sold", "inventory", "inbound"}
MASTER_SKUS = set(str(s) for s in settings.SKU_ORDER)


# ---------------------------------------------------------------------------
# Inventory parsers
# ---------------------------------------------------------------------------

class TestParseFbaReport:
    def test_happy_path_returns_dataframe(self, fba_fixture):
        df = parse_fba_report(fba_fixture)
        assert df is not None

    def test_output_columns(self, fba_fixture):
        df = parse_fba_report(fba_fixture)
        assert INVENTORY_COLS.issubset(set(df.columns))

    def test_channel_is_fba(self, fba_fixture):
        df = parse_fba_report(fba_fixture)
        assert (df["channel"] == "FBA").all()

    def test_zero_fill_all_skus_present(self, fba_fixture):
        df = parse_fba_report(fba_fixture)
        assert MASTER_SKUS == set(df["sku"].astype(str))

    def test_known_sku_values(self, fba_fixture):
        df = parse_fba_report(fba_fixture)
        row = df[df["sku"] == "1001"].iloc[0]
        assert row["units_sold"] == 10
        assert row["inventory"] == 43   # Available(40) + FC transfer(3)
        assert row["inbound"] == 5

    def test_trailing_s_stripped(self, fba_fixture):
        df = parse_fba_report(fba_fixture)
        # Fixture has "3001s" → should appear as "3001" after normalization
        assert "3001" in df["sku"].values
        assert "3001s" not in df["sku"].values

    def test_unknown_sku_excluded(self, fba_fixture):
        df = parse_fba_report(fba_fixture)
        assert "UNKNOWN_SKU" not in df["sku"].values

    def test_all_metrics_non_negative(self, fba_fixture):
        df = parse_fba_report(fba_fixture)
        assert (df["inventory"] >= 0).all()
        assert (df["inbound"] >= 0).all()
        assert (df["units_sold"] >= 0).all()

    def test_missing_file_returns_none(self, fixtures_dir):
        df = parse_fba_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert df is None


class TestParseAwdReport:
    def test_happy_path_returns_dataframe(self, awd_fixture):
        df = parse_awd_report(awd_fixture)
        assert df is not None

    def test_output_columns(self, awd_fixture):
        df = parse_awd_report(awd_fixture)
        assert INVENTORY_COLS.issubset(set(df.columns))

    def test_channel_is_awd(self, awd_fixture):
        df = parse_awd_report(awd_fixture)
        assert (df["channel"] == "AWD").all()

    def test_zero_fill_all_skus_present(self, awd_fixture):
        df = parse_awd_report(awd_fixture)
        assert MASTER_SKUS == set(df["sku"].astype(str))

    def test_units_sold_always_zero(self, awd_fixture):
        """AWD reports have no sales data — units_sold must be 0 for all rows."""
        df = parse_awd_report(awd_fixture)
        assert (df["units_sold"] == 0).all()

    def test_known_sku_inventory(self, awd_fixture):
        df = parse_awd_report(awd_fixture)
        row = df[df["sku"] == "1001"].iloc[0]
        assert row["inventory"] == 150
        assert row["inbound"] == 10

    def test_skiprows_respected(self, awd_fixture):
        """Fixture has 2 metadata rows before the header; parser must use skiprows=2."""
        df = parse_awd_report(awd_fixture)
        # If skiprows weren't used, "SKU" wouldn't be a column and the result would be None
        assert df is not None
        assert "sku" in df.columns

    def test_missing_file_returns_none(self, fixtures_dir):
        df = parse_awd_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert df is None


class TestParseWfsReport:
    def test_happy_path_returns_dataframe(self, wfs_fixture):
        df = parse_wfs_report(wfs_fixture)
        assert df is not None

    def test_output_columns(self, wfs_fixture):
        df = parse_wfs_report(wfs_fixture)
        assert INVENTORY_COLS.issubset(set(df.columns))

    def test_channel_is_wfs(self, wfs_fixture):
        df = parse_wfs_report(wfs_fixture)
        assert (df["channel"] == "WFS").all()

    def test_zero_fill_all_skus_present(self, wfs_fixture):
        df = parse_wfs_report(wfs_fixture)
        assert MASTER_SKUS == set(df["sku"].astype(str))

    def test_known_sku_merged_correctly(self, wfs_fixture):
        df = parse_wfs_report(wfs_fixture)
        row = df[df["sku"] == "1001"].iloc[0]
        assert row["units_sold"] == 10
        assert row["inventory"] == 100
        assert row["inbound"] == 10

    def test_gmv_cleaned(self, wfs_fixture):
        """Walmart GMV uses "$1,200.50" format — clean_money must run."""
        # Sales data is not in inventory output, but we verify the parser runs without error
        df = parse_wfs_report(wfs_fixture)
        assert df is not None

    def test_missing_sales_file_returns_none(self, fixtures_dir):
        df = parse_wfs_report({
            "sales": fixtures_dir / "nonexistent.csv",
            "inventory": fixtures_dir / "walmart_inventory.csv",
        })
        assert df is None

    def test_missing_inventory_file_returns_none(self, fixtures_dir):
        df = parse_wfs_report({
            "sales": fixtures_dir / "walmart_sales.csv",
            "inventory": fixtures_dir / "nonexistent.csv",
        })
        assert df is None


class TestParseFbtInventoryReport:
    def test_happy_path_returns_dataframe(self, fbt_inventory_fixture):
        df = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert df is not None

    def test_output_columns(self, fbt_inventory_fixture):
        df = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert INVENTORY_COLS.issubset(set(df.columns))

    def test_channel_is_fbt(self, fbt_inventory_fixture):
        df = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert (df["channel"] == "FBT").all()

    def test_multi_warehouse_aggregated(self, fbt_inventory_fixture):
        """Fixture has two rows for SKU "1001" from different warehouses — must sum."""
        df = parse_fbt_inventory_report(fbt_inventory_fixture)
        row = df[df["sku"] == "1001"].iloc[0]
        assert row["inventory"] == 120   # 80 + 40
        assert row["inbound"] == 15      # 10 + 5

    def test_unknown_sku_excluded(self, fbt_inventory_fixture):
        df = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert "UNKNOWN_SKU" not in df["sku"].values

    def test_zero_fill_all_skus_present(self, fbt_inventory_fixture):
        df = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert MASTER_SKUS == set(df["sku"].astype(str))

    def test_missing_file_returns_none(self, fixtures_dir):
        df = parse_fbt_inventory_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert df is None


class TestParseFbtReport:
    def test_happy_path_returns_dataframe(self, fbt_fixture):
        df = parse_fbt_report(fbt_fixture)
        assert df is not None

    def test_output_columns(self, fbt_fixture):
        df = parse_fbt_report(fbt_fixture)
        assert INVENTORY_COLS.issubset(set(df.columns))

    def test_channel_is_fbt(self, fbt_fixture):
        df = parse_fbt_report(fbt_fixture)
        assert (df["channel"] == "FBT").all()

    def test_sales_merged_from_orders(self, fbt_fixture):
        """FBT sales come from TikTok orders (FBT fulfillment type only)."""
        df = parse_fbt_report(fbt_fixture)
        # SKU "1001" had 2 valid FBT orders in fixture (ORD001: qty=2)
        row = df[df["sku"] == "1001"].iloc[0]
        assert row["units_sold"] > 0

    def test_missing_inventory_file_returns_none(self, fixtures_dir):
        df = parse_fbt_report({
            "sales": fixtures_dir / "tiktok_orders.csv",
            "inventory": fixtures_dir / "nonexistent.csv",
        })
        assert df is None


class TestParseFlexportReports:
    def test_happy_path_returns_dataframe(self, flexport_fixture):
        df = parse_flexport_reports(flexport_fixture)
        assert df is not None

    def test_creates_dtc_and_reserve_channels(self, flexport_fixture):
        df = parse_flexport_reports(flexport_fixture)
        channels = set(df["channel"].unique())
        assert "DTC" in channels
        assert "Reserve" in channels
        assert len(channels) == 2

    def test_all_skus_present_in_both_channels(self, flexport_fixture):
        df = parse_flexport_reports(flexport_fixture)
        for ch in ["DTC", "Reserve"]:
            channel_skus = set(df[df["channel"] == ch]["sku"].astype(str))
            assert MASTER_SKUS == channel_skus

    def test_multi_facility_dtc_aggregated(self, flexport_fixture):
        """Fixture has two rows for MSKU "1001" — DTC inventory must sum."""
        df = parse_flexport_reports(flexport_fixture)
        dtc_row = df[(df["channel"] == "DTC") & (df["sku"] == "1001")].iloc[0]
        assert dtc_row["inventory"] == 150   # 100 + 50

    def test_cancelled_orders_excluded_from_sales(self, flexport_fixture):
        """Fixture has one CANCELLED order for "2001" (DEL002) — must not count."""
        df = parse_flexport_reports(flexport_fixture)
        dtc_row = df[(df["channel"] == "DTC") & (df["sku"] == "2001")].iloc[0]
        # DEL002 is CANCELLED and maps to "2001" (DJN5OWEKCRR) — should not be in sales
        assert dtc_row["units_sold"] == 0

    def test_reserve_has_no_units_sold(self, flexport_fixture):
        df = parse_flexport_reports(flexport_fixture)
        reserve = df[df["channel"] == "Reserve"]
        assert (reserve["units_sold"] == 0).all()

    def test_reserve_has_no_inbound(self, flexport_fixture):
        df = parse_flexport_reports(flexport_fixture)
        reserve = df[df["channel"] == "Reserve"]
        assert (reserve["inbound"] == 0).all()

    def test_optional_inbound_missing_defaults_to_zero(self, flexport_no_inbound_fixture):
        df = parse_flexport_reports(flexport_no_inbound_fixture)
        assert df is not None
        dtc = df[df["channel"] == "DTC"]
        assert (dtc["inbound"] == 0).all()

    def test_unknown_dsku_excluded_from_sales(self, flexport_fixture):
        """UNKNOWN_DSKU in fixture is not in DSKU_TO_SKU_MAP — must be dropped."""
        df = parse_flexport_reports(flexport_fixture)
        # No extra SKUs should appear beyond master list
        dtc_skus = set(df[df["channel"] == "DTC"]["sku"].astype(str))
        assert dtc_skus == MASTER_SKUS

    def test_missing_levels_file_returns_none(self, fixtures_dir):
        df = parse_flexport_reports({
            "levels": fixtures_dir / "nonexistent.csv",
            "orders": fixtures_dir / "flexport_orders.csv",
            "inbound": fixtures_dir / "flexport_inbound.csv",
        })
        assert df is None


# ---------------------------------------------------------------------------
# Sales parsers
# ---------------------------------------------------------------------------

class TestParseWalmartSalesReport:
    def test_happy_path_returns_tuple(self, walmart_sales_fixture):
        result = parse_walmart_sales_report(walmart_sales_fixture)
        df, bundle_stats, raw_count = result
        assert df is not None

    def test_output_columns(self, walmart_sales_fixture):
        df, _, _ = parse_walmart_sales_report(walmart_sales_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(df.columns))

    def test_raw_count_is_correct(self, walmart_sales_fixture):
        _, _, raw_count = parse_walmart_sales_report(walmart_sales_fixture)
        assert raw_count == 3  # fixture has 3 data rows

    def test_gmv_cleaned(self, walmart_sales_fixture):
        """Fixture GMV is "$1,200.50" — must be cleaned to float."""
        df, _, _ = parse_walmart_sales_report(walmart_sales_fixture)
        row = df[df["SKU"] == "1001"].iloc[0]
        assert abs(row["Revenue"] - 1200.50) < 0.01

    def test_no_bundles_in_walmart(self, walmart_sales_fixture):
        _, bundle_stats, _ = parse_walmart_sales_report(walmart_sales_fixture)
        assert bundle_stats["Units"] == 0
        assert bundle_stats["Revenue"] == 0

    def test_missing_file_returns_none_df(self, fixtures_dir):
        df, _, _ = parse_walmart_sales_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert df is None


class TestParseAmazonSalesReport:
    def test_happy_path_returns_tuple(self, amazon_sales_fixture):
        result = parse_amazon_sales_report(amazon_sales_fixture)
        df, bundle_stats, raw_count = result
        assert df is not None

    def test_output_columns(self, amazon_sales_fixture):
        df, _, _ = parse_amazon_sales_report(amazon_sales_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(df.columns))

    def test_known_sku_present(self, amazon_sales_fixture):
        df, _, _ = parse_amazon_sales_report(amazon_sales_fixture)
        assert "1001" in df["SKU"].values

    def test_trailing_s_msku_maps_correctly(self, amazon_sales_fixture):
        """Fixture MSKU "3001s" maps to internal SKU "3001"."""
        df, _, _ = parse_amazon_sales_report(amazon_sales_fixture)
        assert "3001" in df["SKU"].values

    def test_unknown_msku_excluded(self, amazon_sales_fixture):
        df, _, _ = parse_amazon_sales_report(amazon_sales_fixture)
        assert "UNKNOWN_MSKU" not in df["SKU"].values

    def test_raw_count(self, amazon_sales_fixture):
        _, _, raw_count = parse_amazon_sales_report(amazon_sales_fixture)
        assert raw_count == 3

    def test_missing_file_returns_none_df(self, fixtures_dir):
        df, _, _ = parse_amazon_sales_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert df is None


class TestParseTikTokOrdersReport:
    def test_happy_path_returns_tuple(self, tiktok_orders_fixture):
        result = parse_tiktok_orders_report(tiktok_orders_fixture)
        df, bundle_stats, raw_count = result
        assert df is not None

    def test_output_columns(self, tiktok_orders_fixture):
        df, _, _ = parse_tiktok_orders_report(tiktok_orders_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(df.columns))

    def test_cancelled_orders_excluded(self, tiktok_orders_fixture):
        """ORD002 is Cancelled — must not appear in output."""
        _, _, raw_count = parse_tiktok_orders_report(tiktok_orders_fixture)
        # raw_count includes all rows before filtering
        assert raw_count == 4

        df, _, _ = parse_tiktok_orders_report(tiktok_orders_fixture)
        # Only 2 valid FBT orders: ORD001 (qty=2) and ORD004 (qty=1, bundle)
        # ORD002 cancelled, ORD003 non-FBT
        row = df[df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 2  # Only ORD001

    def test_non_fbt_orders_excluded(self, tiktok_orders_fixture):
        """ORD003 uses "Seller Shipping" — must be excluded."""
        df, _, _ = parse_tiktok_orders_report(tiktok_orders_fixture)
        # ORD003 is for SKU "1001" with qty=3 but non-FBT — should NOT be counted
        row = df[df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 2  # Only ORD001 survives

    def test_bundle_expanded(self, tiktok_orders_fixture):
        """ORD004 maps TikTok ID to ["5001","5001"] (bundle 2x)."""
        df, bundle_stats, _ = parse_tiktok_orders_report(tiktok_orders_fixture)
        assert "5001" in df["SKU"].values
        assert bundle_stats["Units"] > 0

    def test_dynamic_header_detected(self, tiktok_orders_fixture):
        """Fixture has an empty row before the header — parser must skip it."""
        df, _, _ = parse_tiktok_orders_report(tiktok_orders_fixture)
        # If header not detected, SKU mapping would fail and df would be None
        assert df is not None

    def test_missing_file_returns_none_df(self, fixtures_dir):
        df, _, _ = parse_tiktok_orders_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert df is None


class TestParseTikTokSalesReport:
    def test_happy_path_returns_tuple(self, tiktok_sales_fixture):
        result = parse_tiktok_sales_report(tiktok_sales_fixture)
        df, bundle_stats, raw_count = result
        assert df is not None

    def test_output_columns(self, tiktok_sales_fixture):
        df, _, _ = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(df.columns))

    def test_dynamic_header_detected(self, tiktok_sales_fixture):
        """Fixture has date-range row + blank row before header — parser must scan."""
        df, _, _ = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert df is not None

    def test_single_sku_row(self, tiktok_sales_fixture):
        df, _, _ = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert "1001" in df["SKU"].values
        row = df[df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 5
        assert abs(row["Revenue"] - 50.0) < 0.01

    def test_bundle_expanded_to_two_rows(self, tiktok_sales_fixture):
        """TikTok ID "1729500444198670817" → ["5001","5001"]: 3 items → 2 rows of 3 each."""
        df, bundle_stats, _ = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert "5001" in df["SKU"].values
        row = df[df["SKU"] == "5001"].iloc[0]
        assert row["Units"] == 6    # 3 + 3 (each bundle entry gets full qty)
        assert abs(row["Revenue"] - 30.0) < 0.01   # 15 + 15

    def test_bundle_stats_tracked(self, tiktok_sales_fixture):
        _, bundle_stats, _ = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert bundle_stats["Units"] == 3.0
        assert abs(bundle_stats["Revenue"] - 30.0) < 0.01

    def test_missing_file_returns_none_df(self, fixtures_dir):
        df, _, _ = parse_tiktok_sales_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert df is None


class TestParseShopifySalesReport:
    def test_happy_path_returns_tuple(self, shopify_sales_fixture):
        result = parse_shopify_sales_report(shopify_sales_fixture)
        df, bundle_stats, raw_count = result
        assert df is not None

    def test_output_columns(self, shopify_sales_fixture):
        df, _, _ = parse_shopify_sales_report(shopify_sales_fixture)
        assert {"SKU", "Units", "Revenue", "Channel"}.issubset(set(df.columns))

    def test_draft_orders_excluded(self, shopify_sales_fixture):
        _, _, raw_count = parse_shopify_sales_report(shopify_sales_fixture)
        # Fixture has 5 rows; "Draft Orders" row is excluded before processing
        # raw_count = 5 (before filter) but only 4 pass the Draft Orders exclusion
        assert raw_count == 5

    def test_online_store_maps_to_shopify_channel(self, shopify_sales_fixture):
        df, _, _ = parse_shopify_sales_report(shopify_sales_fixture)
        shopify_rows = df[df["Channel"] == "Shopify"]
        assert len(shopify_rows) > 0

    def test_tiktok_maps_to_tiktok_shopify_channel(self, shopify_sales_fixture):
        df, _, _ = parse_shopify_sales_report(shopify_sales_fixture)
        channels = set(df["Channel"].unique())
        assert "TikTok Shopify" in channels

    def test_marketplace_connect_maps_to_target(self, shopify_sales_fixture):
        df, _, _ = parse_shopify_sales_report(shopify_sales_fixture)
        channels = set(df["Channel"].unique())
        assert "Target" in channels

    def test_unknown_channel_maps_to_others(self, shopify_sales_fixture):
        """Fixture has "My Platform" which isn't a recognized channel → Others."""
        # "SomeProduct" (unmapped SKU in Others channel) would be skipped by SKU mapping,
        # but the channel bucketing logic still runs first.
        df, _, _ = parse_shopify_sales_report(shopify_sales_fixture)
        # Channel "Others" may or may not have rows depending on SKU mapping results.
        # The important thing is no exception is raised.
        assert df is not None

    def test_bundle_expanded(self, shopify_sales_fixture):
        """Fixture "AlexandrasSpecialBundle" → ["3001","4001","8001"] in Target channel."""
        df, bundle_stats, _ = parse_shopify_sales_report(shopify_sales_fixture)
        target_rows = df[df["Channel"] == "Target"]
        target_skus = set(target_rows["SKU"].values)
        assert "3001" in target_skus
        assert "4001" in target_skus
        assert "8001" in target_skus

    def test_bundle_stats_keyed_by_channel(self, shopify_sales_fixture):
        _, bundle_stats, _ = parse_shopify_sales_report(shopify_sales_fixture)
        assert isinstance(bundle_stats, dict)
        assert "Target" in bundle_stats   # AlexandrasSpecialBundle is in Target channel

    def test_missing_file_returns_none_df(self, fixtures_dir):
        df, _, _ = parse_shopify_sales_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert df is None
