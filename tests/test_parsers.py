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
    parse_amazon_orders_report,
    parse_tiktok_orders_report,
    parse_tiktok_sales_report,
    parse_tiktok_shop_orders_report,
    parse_shopify_sales_report,
)

# All inventory parsers now return CamelCase columns at the parser boundary
INVENTORY_COLS = {"SKU", "Channel", "Units", "Inventory", "Inbound"}
MASTER_SKUS = set(str(s) for s in settings.SKU_ORDER)


# ---------------------------------------------------------------------------
# Inventory parsers
# ---------------------------------------------------------------------------

class TestParseFbaReport:
    def test_happy_path_returns_parse_result(self, fba_fixture):
        result = parse_fba_report(fba_fixture)
        assert result.df is not None

    def test_output_columns(self, fba_fixture):
        result = parse_fba_report(fba_fixture)
        assert INVENTORY_COLS.issubset(set(result.df.columns))

    def test_channel_is_fba(self, fba_fixture):
        result = parse_fba_report(fba_fixture)
        assert (result.df["Channel"] == "FBA").all()

    def test_zero_fill_all_skus_present(self, fba_fixture):
        result = parse_fba_report(fba_fixture)
        assert MASTER_SKUS == set(result.df["SKU"].astype(str))

    def test_known_sku_values(self, fba_fixture):
        result = parse_fba_report(fba_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 10
        assert row["Inventory"] == 43   # Available(40) + FC transfer(3)
        assert row["Inbound"] == 5

    def test_trailing_s_stripped(self, fba_fixture):
        result = parse_fba_report(fba_fixture)
        # Fixture has "3001s" → should appear as "3001" after normalization
        assert "3001" in result.df["SKU"].values
        assert "3001s" not in result.df["SKU"].values

    def test_unknown_sku_excluded(self, fba_fixture):
        result = parse_fba_report(fba_fixture)
        assert "UNKNOWN_SKU" not in result.df["SKU"].values

    def test_all_metrics_non_negative(self, fba_fixture):
        result = parse_fba_report(fba_fixture)
        assert (result.df["Inventory"] >= 0).all()
        assert (result.df["Inbound"] >= 0).all()
        assert (result.df["Units"] >= 0).all()

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_fba_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseAwdReport:
    def test_happy_path_returns_parse_result(self, awd_fixture):
        result = parse_awd_report(awd_fixture)
        assert result.df is not None

    def test_output_columns(self, awd_fixture):
        result = parse_awd_report(awd_fixture)
        assert INVENTORY_COLS.issubset(set(result.df.columns))

    def test_channel_is_awd(self, awd_fixture):
        result = parse_awd_report(awd_fixture)
        assert (result.df["Channel"] == "AWD").all()

    def test_zero_fill_all_skus_present(self, awd_fixture):
        result = parse_awd_report(awd_fixture)
        assert MASTER_SKUS == set(result.df["SKU"].astype(str))

    def test_units_always_zero(self, awd_fixture):
        """AWD reports have no sales data — Units must be 0 for all rows."""
        result = parse_awd_report(awd_fixture)
        assert (result.df["Units"] == 0).all()

    def test_known_sku_inventory(self, awd_fixture):
        result = parse_awd_report(awd_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Inventory"] == 150
        assert row["Inbound"] == 10

    def test_skiprows_respected(self, awd_fixture):
        """Fixture has 2 metadata rows before the header; parser must use skiprows=2."""
        result = parse_awd_report(awd_fixture)
        assert result.df is not None
        assert "SKU" in result.df.columns

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_awd_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseWfsReport:
    def test_happy_path_returns_parse_result(self, wfs_fixture):
        result = parse_wfs_report(wfs_fixture)
        assert result.df is not None

    def test_output_columns(self, wfs_fixture):
        result = parse_wfs_report(wfs_fixture)
        assert INVENTORY_COLS.issubset(set(result.df.columns))

    def test_channel_is_wfs(self, wfs_fixture):
        result = parse_wfs_report(wfs_fixture)
        assert (result.df["Channel"] == "WFS").all()

    def test_zero_fill_all_skus_present(self, wfs_fixture):
        result = parse_wfs_report(wfs_fixture)
        assert MASTER_SKUS == set(result.df["SKU"].astype(str))

    def test_known_sku_merged_correctly(self, wfs_fixture):
        result = parse_wfs_report(wfs_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 10
        assert row["Inventory"] == 100
        assert row["Inbound"] == 10

    def test_gmv_cleaned(self, wfs_fixture):
        """Walmart GMV uses "$1,200.50" format — clean_money must run."""
        result = parse_wfs_report(wfs_fixture)
        assert result.df is not None

    def test_missing_sales_file_returns_none_df(self, fixtures_dir):
        result = parse_wfs_report({
            "sales": fixtures_dir / "nonexistent.csv",
            "inventory": fixtures_dir / "walmart_inventory.csv",
        })
        assert result.df is None

    def test_missing_inventory_file_returns_none_df(self, fixtures_dir):
        result = parse_wfs_report({
            "sales": fixtures_dir / "walmart_sales.csv",
            "inventory": fixtures_dir / "nonexistent.csv",
        })
        assert result.df is None


class TestParseFbtInventoryReport:
    def test_happy_path_returns_parse_result(self, fbt_inventory_fixture):
        result = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert result.df is not None

    def test_output_columns(self, fbt_inventory_fixture):
        result = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert INVENTORY_COLS.issubset(set(result.df.columns))

    def test_channel_is_fbt(self, fbt_inventory_fixture):
        result = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert (result.df["Channel"] == "FBT").all()

    def test_multi_warehouse_aggregated(self, fbt_inventory_fixture):
        """Fixture has two rows for SKU "1001" from different warehouses — must sum."""
        result = parse_fbt_inventory_report(fbt_inventory_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Inventory"] == 120   # 80 + 40
        assert row["Inbound"] == 15      # 10 + 5

    def test_unknown_sku_excluded(self, fbt_inventory_fixture):
        result = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert "UNKNOWN_SKU" not in result.df["SKU"].values

    def test_zero_fill_all_skus_present(self, fbt_inventory_fixture):
        result = parse_fbt_inventory_report(fbt_inventory_fixture)
        assert MASTER_SKUS == set(result.df["SKU"].astype(str))

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_fbt_inventory_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseFbtReport:
    def test_happy_path_returns_parse_result(self, fbt_fixture):
        result = parse_fbt_report(fbt_fixture)
        assert result.df is not None

    def test_output_columns(self, fbt_fixture):
        result = parse_fbt_report(fbt_fixture)
        assert INVENTORY_COLS.issubset(set(result.df.columns))

    def test_channel_is_fbt(self, fbt_fixture):
        result = parse_fbt_report(fbt_fixture)
        assert (result.df["Channel"] == "FBT").all()

    def test_sales_merged_from_orders(self, fbt_fixture):
        """FBT sales come from TikTok orders (FBT fulfillment type only)."""
        result = parse_fbt_report(fbt_fixture)
        # SKU "1001" had 2 valid FBT orders in fixture (ORD001: qty=2)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] > 0

    def test_missing_inventory_file_returns_none_df(self, fixtures_dir):
        result = parse_fbt_report({
            "sales": fixtures_dir / "tiktok_orders.csv",
            "inventory": fixtures_dir / "nonexistent.csv",
        })
        assert result.df is None


class TestParseFlexportReports:
    def test_happy_path_returns_parse_result(self, flexport_fixture):
        result = parse_flexport_reports(flexport_fixture)
        assert result.df is not None

    def test_creates_dtc_and_reserve_channels(self, flexport_fixture):
        result = parse_flexport_reports(flexport_fixture)
        channels = set(result.df["Channel"].unique())
        assert "DTC" in channels
        assert "Reserve" in channels
        assert len(channels) == 2

    def test_all_skus_present_in_both_channels(self, flexport_fixture):
        result = parse_flexport_reports(flexport_fixture)
        for ch in ["DTC", "Reserve"]:
            channel_skus = set(result.df[result.df["Channel"] == ch]["SKU"].astype(str))
            assert MASTER_SKUS == channel_skus

    def test_multi_facility_dtc_aggregated(self, flexport_fixture):
        """Fixture has two rows for MSKU "1001" — DTC inventory must sum."""
        result = parse_flexport_reports(flexport_fixture)
        dtc_row = result.df[(result.df["Channel"] == "DTC") & (result.df["SKU"] == "1001")].iloc[0]
        assert dtc_row["Inventory"] == 150   # 100 + 50

    def test_cancelled_orders_excluded_from_sales(self, flexport_fixture):
        """Fixture has one CANCELLED order for "2001" (DEL002) — must not count."""
        result = parse_flexport_reports(flexport_fixture)
        dtc_row = result.df[(result.df["Channel"] == "DTC") & (result.df["SKU"] == "2001")].iloc[0]
        assert dtc_row["Units"] == 0

    def test_reserve_has_no_units(self, flexport_fixture):
        result = parse_flexport_reports(flexport_fixture)
        reserve = result.df[result.df["Channel"] == "Reserve"]
        assert (reserve["Units"] == 0).all()

    def test_reserve_has_no_inbound(self, flexport_fixture):
        result = parse_flexport_reports(flexport_fixture)
        reserve = result.df[result.df["Channel"] == "Reserve"]
        assert (reserve["Inbound"] == 0).all()

    def test_optional_inbound_missing_defaults_to_zero(self, flexport_no_inbound_fixture):
        result = parse_flexport_reports(flexport_no_inbound_fixture)
        assert result.df is not None
        dtc = result.df[result.df["Channel"] == "DTC"]
        assert (dtc["Inbound"] == 0).all()

    def test_unknown_dsku_excluded_from_sales(self, flexport_fixture):
        """UNKNOWN_DSKU in fixture is not in DSKU_TO_SKU_MAP — must be dropped."""
        result = parse_flexport_reports(flexport_fixture)
        dtc_skus = set(result.df[result.df["Channel"] == "DTC"]["SKU"].astype(str))
        assert dtc_skus == MASTER_SKUS

    def test_missing_levels_file_returns_none_df(self, fixtures_dir):
        result = parse_flexport_reports({
            "levels": fixtures_dir / "nonexistent.csv",
            "orders": fixtures_dir / "flexport_orders.csv",
            "inbound": fixtures_dir / "flexport_inbound.csv",
        })
        assert result.df is None


# ---------------------------------------------------------------------------
# Sales parsers
# ---------------------------------------------------------------------------

class TestParseWalmartSalesReport:
    def test_happy_path_returns_parse_result(self, walmart_sales_fixture):
        result = parse_walmart_sales_report(walmart_sales_fixture)
        assert result.df is not None

    def test_output_columns(self, walmart_sales_fixture):
        result = parse_walmart_sales_report(walmart_sales_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(result.df.columns))

    def test_raw_count_is_correct(self, walmart_sales_fixture):
        result = parse_walmart_sales_report(walmart_sales_fixture)
        assert result.raw_count == 3  # fixture has 3 data rows

    def test_gmv_cleaned(self, walmart_sales_fixture):
        """Fixture GMV is "$1,200.50" — must be cleaned to float."""
        result = parse_walmart_sales_report(walmart_sales_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert abs(row["Revenue"] - 1200.50) < 0.01

    def test_no_bundles_in_walmart(self, walmart_sales_fixture):
        result = parse_walmart_sales_report(walmart_sales_fixture)
        assert result.bundle_stats["Units"] == 0
        assert result.bundle_stats["Revenue"] == 0

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_walmart_sales_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseAmazonSalesReport:
    def test_happy_path_returns_parse_result(self, amazon_sales_fixture):
        result = parse_amazon_sales_report(amazon_sales_fixture)
        assert result.df is not None

    def test_output_columns(self, amazon_sales_fixture):
        result = parse_amazon_sales_report(amazon_sales_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(result.df.columns))

    def test_known_sku_present(self, amazon_sales_fixture):
        result = parse_amazon_sales_report(amazon_sales_fixture)
        assert "1001" in result.df["SKU"].values

    def test_trailing_s_msku_maps_correctly(self, amazon_sales_fixture):
        """Fixture MSKU "3001s" maps to internal SKU "3001"."""
        result = parse_amazon_sales_report(amazon_sales_fixture)
        assert "3001" in result.df["SKU"].values

    def test_unknown_msku_excluded(self, amazon_sales_fixture):
        result = parse_amazon_sales_report(amazon_sales_fixture)
        assert "UNKNOWN_MSKU" not in result.df["SKU"].values

    def test_raw_count(self, amazon_sales_fixture):
        result = parse_amazon_sales_report(amazon_sales_fixture)
        assert result.raw_count == 3

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_amazon_sales_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseAmazonOrdersReport:
    """
    Tests for parse_amazon_orders_report — EPIC-008 Step 2.

    Fixture: tests/fixtures/amazon_orders.csv
      ORD001  Shipped    Amazon.com      sku=1001      qty=1  rev=24.99   (keep)
      ORD002  Shipped    Amazon.com      sku=3001s     qty=2  rev=65.90   (keep; trailing-s map)
      ORD003  Cancelled  Amazon.com      sku=1001      qty=1  rev=24.99   (excluded: Cancelled)
      ORD004  Pending    Amazon.com      sku=5001s     qty=1  rev=29.99   (excluded: Pending)
      ORD005  Shipped    Non-Amazon US   sku=1001      qty=1  rev=24.99   (excluded: MCF)
      ORD_UNK Shipped    Amazon.com      sku=UNKNOWN   qty=1  rev=10.00   (excluded: unmapped)

    Expected output:
      SKU 1001 : Units=1, Revenue=24.99
      SKU 3001 : Units=2, Revenue=65.90
      bundle_stats : {Units: 0, Revenue: 0}
    """

    def test_happy_path_returns_parse_result(self, amazon_orders_fixture):
        result = parse_amazon_orders_report(amazon_orders_fixture)
        assert result.df is not None

    def test_output_columns(self, amazon_orders_fixture):
        result = parse_amazon_orders_report(amazon_orders_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(result.df.columns))

    def test_raw_count_includes_all_rows(self, amazon_orders_fixture):
        """raw_count is total rows before any filtering (6 in fixture)."""
        result = parse_amazon_orders_report(amazon_orders_fixture)
        assert result.raw_count == 6

    def test_cancelled_orders_excluded(self, amazon_orders_fixture):
        """ORD003 is Cancelled — SKU 1001 should only get 1 unit (ORD001), not 2."""
        result = parse_amazon_orders_report(amazon_orders_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 1

    def test_pending_orders_excluded(self, amazon_orders_fixture):
        """ORD004 is Pending (5001s) — must not appear in output."""
        result = parse_amazon_orders_report(amazon_orders_fixture)
        assert "5001" not in result.df["SKU"].values

    def test_mcf_orders_excluded(self, amazon_orders_fixture):
        """ORD005 is Non-Amazon US (MCF) — must not contribute to SKU 1001."""
        result = parse_amazon_orders_report(amazon_orders_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 1  # ORD001 only; ORD005 (MCF) is excluded

    def test_trailing_s_msku_maps_correctly(self, amazon_orders_fixture):
        """ORD002 sku='3001s' → AMAZON_SKU_MAP → internal SKU '3001'."""
        result = parse_amazon_orders_report(amazon_orders_fixture)
        assert "3001" in result.df["SKU"].values

    def test_unknown_msku_excluded(self, amazon_orders_fixture):
        result = parse_amazon_orders_report(amazon_orders_fixture)
        assert "UNKNOWN_MSKU" not in result.df["SKU"].values

    def test_revenue_correct(self, amazon_orders_fixture):
        result = parse_amazon_orders_report(amazon_orders_fixture)
        row_1001 = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert abs(row_1001["Revenue"] - 24.99) < 0.01
        row_3001 = result.df[result.df["SKU"] == "3001"].iloc[0]
        assert abs(row_3001["Revenue"] - 65.90) < 0.01

    def test_no_bundles_in_fixture(self, amazon_orders_fixture):
        """Fixture has no bundle MSKUs — bundle_stats should be zero."""
        result = parse_amazon_orders_report(amazon_orders_fixture)
        assert result.bundle_stats["Units"] == 0
        assert result.bundle_stats["Revenue"] == 0

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_amazon_orders_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseTikTokOrdersReport:
    def test_happy_path_returns_parse_result(self, tiktok_orders_fixture):
        result = parse_tiktok_orders_report(tiktok_orders_fixture)
        assert result.df is not None

    def test_output_columns(self, tiktok_orders_fixture):
        result = parse_tiktok_orders_report(tiktok_orders_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(result.df.columns))

    def test_cancelled_orders_excluded(self, tiktok_orders_fixture):
        """ORD002 is Cancelled — must not appear in output."""
        result = parse_tiktok_orders_report(tiktok_orders_fixture)
        # raw_count includes all rows before filtering
        assert result.raw_count == 4

        # Only 2 valid FBT orders: ORD001 (qty=2) and ORD004 (qty=1, bundle)
        # ORD002 cancelled, ORD003 non-FBT
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 2  # Only ORD001

    def test_non_fbt_orders_excluded(self, tiktok_orders_fixture):
        """ORD003 uses "Seller Shipping" — must be excluded."""
        result = parse_tiktok_orders_report(tiktok_orders_fixture)
        # ORD003 is for SKU "1001" with qty=3 but non-FBT — should NOT be counted
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 2  # Only ORD001 survives

    def test_bundle_expanded(self, tiktok_orders_fixture):
        """ORD004 maps TikTok ID to ["5001","5001"] (bundle 2x)."""
        result = parse_tiktok_orders_report(tiktok_orders_fixture)
        assert "5001" in result.df["SKU"].values
        assert result.bundle_stats["Units"] > 0

    def test_dynamic_header_detected(self, tiktok_orders_fixture):
        """Fixture has an empty row before the header — parser must skip it."""
        result = parse_tiktok_orders_report(tiktok_orders_fixture)
        assert result.df is not None

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_tiktok_orders_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseTikTokSalesReport:
    def test_happy_path_returns_parse_result(self, tiktok_sales_fixture):
        result = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert result.df is not None

    def test_output_columns(self, tiktok_sales_fixture):
        result = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(result.df.columns))

    def test_dynamic_header_detected(self, tiktok_sales_fixture):
        """Fixture has date-range row + blank row before header — parser must scan."""
        result = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert result.df is not None

    def test_single_sku_row(self, tiktok_sales_fixture):
        result = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert "1001" in result.df["SKU"].values
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 5
        assert abs(row["Revenue"] - 50.0) < 0.01

    def test_bundle_expanded_to_two_rows(self, tiktok_sales_fixture):
        """TikTok ID "1729500444198670817" → ["5001","5001"]: 3 items → 2 rows of 3 each."""
        result = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert "5001" in result.df["SKU"].values
        row = result.df[result.df["SKU"] == "5001"].iloc[0]
        assert row["Units"] == 6    # 3 + 3 (each bundle entry gets full qty)
        assert abs(row["Revenue"] - 30.0) < 0.01   # 15 + 15

    def test_bundle_stats_tracked(self, tiktok_sales_fixture):
        result = parse_tiktok_sales_report(tiktok_sales_fixture)
        assert result.bundle_stats["Units"] == 3.0
        assert abs(result.bundle_stats["Revenue"] - 30.0) < 0.01

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_tiktok_sales_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseShopifySalesReport:
    def test_happy_path_returns_parse_result(self, shopify_sales_fixture):
        result = parse_shopify_sales_report(shopify_sales_fixture)
        assert result.df is not None

    def test_output_columns(self, shopify_sales_fixture):
        result = parse_shopify_sales_report(shopify_sales_fixture)
        assert {"SKU", "Units", "Revenue", "Channel"}.issubset(set(result.df.columns))

    def test_draft_orders_excluded(self, shopify_sales_fixture):
        result = parse_shopify_sales_report(shopify_sales_fixture)
        # Fixture has 5 rows; "Draft Orders" row is excluded before processing
        # raw_count = 5 (before filter) but only 4 pass the Draft Orders exclusion
        assert result.raw_count == 5

    def test_online_store_maps_to_shopify_channel(self, shopify_sales_fixture):
        result = parse_shopify_sales_report(shopify_sales_fixture)
        shopify_rows = result.df[result.df["Channel"] == "Shopify"]
        assert len(shopify_rows) > 0

    def test_tiktok_maps_to_tiktok_shopify_channel(self, shopify_sales_fixture):
        result = parse_shopify_sales_report(shopify_sales_fixture)
        channels = set(result.df["Channel"].unique())
        assert "TikTok Shopify" in channels

    def test_marketplace_connect_maps_to_target(self, shopify_sales_fixture):
        result = parse_shopify_sales_report(shopify_sales_fixture)
        channels = set(result.df["Channel"].unique())
        assert "Target" in channels

    def test_unknown_channel_maps_to_others(self, shopify_sales_fixture):
        """Fixture has "My Platform" which isn't a recognized channel → Others."""
        result = parse_shopify_sales_report(shopify_sales_fixture)
        assert result.df is not None

    def test_bundle_expanded(self, shopify_sales_fixture):
        """Fixture "AlexandrasSpecialBundle" → ["3001","4001","8001"] in Target channel."""
        result = parse_shopify_sales_report(shopify_sales_fixture)
        target_rows = result.df[result.df["Channel"] == "Target"]
        target_skus = set(target_rows["SKU"].values)
        assert "3001" in target_skus
        assert "4001" in target_skus
        assert "8001" in target_skus

    def test_bundle_stats_keyed_by_channel(self, shopify_sales_fixture):
        result = parse_shopify_sales_report(shopify_sales_fixture)
        assert isinstance(result.bundle_stats, dict)
        assert "Target" in result.bundle_stats   # AlexandrasSpecialBundle is in Target channel

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_shopify_sales_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None


class TestParseTikTokShopOrdersReport:
    """
    Tests for parse_tiktok_shop_orders_report — EPIC-008 Step 4.

    Fixture: tests/fixtures/tiktok_shop_orders.csv
      ORD001  Completed  SKU 1729499998780101089 → ["1001"]  qty=2  rev=56.34  FBT
      ORD002  Cancelled  same SKU                 qty=1  rev=28.17  FBT        (excluded)
      ORD003  Completed  same SKU                 qty=3  rev=84.51  Seller Shipping
      ORD004  Completed  SKU 1729500444198670817 → ["5001","5001"]  qty=1  rev=30.00  FBT
      ORD005  Completed  same bundle SKU          qty=2  rev=60.00  Seller Shipping

    Expected after processing:
      SKU 1001 : Units=5, Revenue=140.85
      SKU 5001 : Units=6  (bundle 2x: (1+2)×2), Revenue=90.00
      bundle_stats : {Units: 3, Revenue: 90.0}
    """

    def test_happy_path_returns_parse_result(self, tiktok_shop_orders_fixture):
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        assert result.df is not None

    def test_output_columns(self, tiktok_shop_orders_fixture):
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        assert {"SKU", "Units", "Revenue"}.issubset(set(result.df.columns))

    def test_raw_count_includes_all_rows(self, tiktok_shop_orders_fixture):
        """raw_count reflects total rows before any filtering."""
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        assert result.raw_count == 5

    def test_cancelled_orders_excluded(self, tiktok_shop_orders_fixture):
        """ORD002 (Cancelled) must not contribute — SKU 1001 Units should be 5, not 6."""
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] == 5

    def test_seller_shipping_orders_included(self, tiktok_shop_orders_fixture):
        """ORD003 (Seller Shipping) must count — distinguishes this from parse_tiktok_orders_report."""
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        # FBT only (ORD001) would give 2; adding Seller Shipping (ORD003, qty=3) gives 5
        assert row["Units"] == 5

    def test_fbt_orders_included(self, tiktok_shop_orders_fixture):
        """ORD001 (FBT) also counts — not filtered out."""
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert row["Units"] >= 2  # ORD001 alone contributes 2

    def test_revenue_uses_subtotal_after_discount(self, tiktok_shop_orders_fixture):
        """Revenue = SKU Subtotal After Discount (56.34 + 84.51 = 140.85 for SKU 1001)."""
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        row = result.df[result.df["SKU"] == "1001"].iloc[0]
        assert abs(row["Revenue"] - 140.85) < 0.01

    def test_bundle_expanded(self, tiktok_shop_orders_fixture):
        """ORD004+ORD005 map to ["5001","5001"]: (1+2) qty × 2 bundle entries = 6 units."""
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        assert "5001" in result.df["SKU"].values
        row = result.df[result.df["SKU"] == "5001"].iloc[0]
        assert row["Units"] == 6

    def test_bundle_revenue(self, tiktok_shop_orders_fixture):
        """Bundle revenue: (30+60)=90 total split evenly, then summed → 90.00 for SKU 5001."""
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        row = result.df[result.df["SKU"] == "5001"].iloc[0]
        assert abs(row["Revenue"] - 90.0) < 0.01

    def test_bundle_stats_include_seller_shipping(self, tiktok_shop_orders_fixture):
        """ORD004 (FBT bundle) + ORD005 (Seller Shipping bundle) → bundle_stats Units == 3."""
        result = parse_tiktok_shop_orders_report(tiktok_shop_orders_fixture)
        assert result.bundle_stats["Units"] == 3.0
        assert abs(result.bundle_stats["Revenue"] - 90.0) < 0.01

    def test_missing_file_returns_none_df(self, fixtures_dir):
        result = parse_tiktok_shop_orders_report({"primary": fixtures_dir / "nonexistent.csv"})
        assert result.df is None
