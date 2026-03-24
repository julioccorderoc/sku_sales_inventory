import pandas as pd
import json
from pathlib import Path

from . import settings
from .utils import load_csv, clean_money
from .schemas import ParseResult
import logging

logger = logging.getLogger(__name__)


# --- Helper for Sales Parsing ---
def _process_bundled_row(row, mapping_dict, key_col, qty_col, rev_col, source_name):
    """
    Returns: tuple(list_of_exploded_items, is_bundle_bool)
    """
    source_key = str(row[key_col]).strip()

    if source_key not in mapping_dict:
        # Filter out NaNs or empty strings before alerting
        if source_key and source_key.lower() != "nan":
            logger.warning(
                f"⚠️  ALERT: Unmapped SKU found in {source_name}: '{source_key}'"
            )
        return [], False

    target_skus = mapping_dict[source_key]
    if not target_skus:
        return [], False

    bundle_size = len(target_skus)
    is_bundle = bundle_size > 1

    original_qty = float(row[qty_col] or 0)
    original_rev = clean_money(row[rev_col])

    rev_per_item = original_rev / bundle_size if bundle_size > 0 else 0

    results = []
    for sku in target_skus:
        results.append({"SKU": sku, "Units": original_qty, "Revenue": rev_per_item})
    return results, is_bundle


# --- Parsers ---


def parse_walmart_sales_report(file_paths: dict) -> ParseResult:
    df = load_csv(file_paths["primary"])
    if df is None:
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    raw_count = len(df)
    df = df.rename(columns={"Units_Sold": "Units", "GMV": "Revenue"})
    df["Revenue"] = df["Revenue"].apply(clean_money)

    grouped = df.groupby("SKU")[["Units", "Revenue"]].sum().reset_index()
    return ParseResult(df=grouped, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})


def parse_amazon_sales_report(file_paths: dict) -> ParseResult:
    df = load_csv(file_paths["primary"])
    if df is None:
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    raw_count = len(df)

    expanded_rows = []
    bundle_units = 0
    bundle_rev = 0

    for _, row in df.iterrows():
        if pd.isna(row.get("MSKU")):
            continue
        new_rows, is_bundle = _process_bundled_row(
            row,
            settings.AMAZON_SKU_MAP,
            "MSKU",
            "Net units sold",
            "Net sales",
            "Amazon",
        )
        expanded_rows.extend(new_rows)
        if is_bundle:
            bundle_units += float(row["Net units sold"] or 0)
            bundle_rev += clean_money(row["Net sales"])

    if not expanded_rows:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df_norm = pd.DataFrame(expanded_rows)
    grouped = df_norm.groupby("SKU")[["Units", "Revenue"]].sum().reset_index()
    return ParseResult(df=grouped, raw_count=raw_count, bundle_stats={"Units": bundle_units, "Revenue": bundle_rev})


def parse_tiktok_sales_report(file_paths: dict) -> ParseResult:
    """
    LEGACY — superseded by parse_tiktok_shop_orders_report (EPIC-008).
    No longer registered in SalesPipeline.PARSER_REGISTRY.
    Kept for reference; remove after one reporting cycle confirms parity.

    Robust Parsing for TikTok:
    1. Scan first few lines to find the header row containing "SKU ID".
    2. Determine delimiter (comma or semicolon) from that header.
    3. Load CSV with calculated 'skiprows'.
    """
    path = file_paths["primary"]

    # 1. Scan for Header Row and Delimiter
    header_row_index = None
    delimiter = ","  # Default

    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if "SKU ID" in line:
                    header_row_index = i
                    # Simple heuristic: count occurrences
                    semicolons = line.count(";")
                    commas = line.count(",")
                    if semicolons > commas:
                        delimiter = ";"
                    else:
                        delimiter = ","
                    break

        # If still not found, fallback to legacy hardcoded skip
        if header_row_index is None:
            logger.warning(
                "Could not find 'SKU ID' header in TikTok report. Trying default skiprows=2."
            )
            header_row_index = 2

    except Exception as e:
        logger.error(f"Error scanning TikTok file for header: {e}")
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    # 2. Load with determined parameters
    logger.info(
        f"  > 🔍 Detected Header at Row {header_row_index} using Delimiter '{delimiter}'"
    )
    df = load_csv(path, skiprows=header_row_index, sep=delimiter)

    if df is None:
        logger.warning("  > ⚠️  Could not load TikTok report DataFrame.")
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    # Verify expected column exists
    if "SKU ID" not in df.columns:
        logger.warning(
            f"  > ⚠️  'SKU ID' column missing despite header detection. Columns found: {df.columns.tolist()}"
        )
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    raw_count = len(df)

    expanded_rows = []
    bundle_units = 0
    bundle_rev = 0

    for _, row in df.iterrows():
        if pd.isna(row.get("SKU ID")):
            continue
        new_rows, is_bundle = _process_bundled_row(
            row, settings.TIKTOK_ID_MAP, "SKU ID", "Items sold", "GMV", "TikTok Direct"
        )
        expanded_rows.extend(new_rows)
        if is_bundle:
            bundle_units += float(row["Items sold"] or 0)
            bundle_rev += clean_money(row["GMV"])

    if not expanded_rows:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df_norm = pd.DataFrame(expanded_rows)
    grouped = df_norm.groupby("SKU")[["Units", "Revenue"]].sum().reset_index()
    return ParseResult(df=grouped, raw_count=raw_count, bundle_stats={"Units": bundle_units, "Revenue": bundle_rev})


def parse_tiktok_shop_orders_report(file_paths: dict) -> ParseResult:
    """
    Reads raw TikTok_orders_*.csv and aggregates for the TikTok Shop SALES channel.

    Replaces parse_tiktok_sales_report (EPIC-008 Step 4).

    Key differences from parse_tiktok_orders_report (used for FBT inventory):
      - No fulfillment-type filter: counts Seller Shipping AND FBT orders
      - Revenue = 'SKU Subtotal After Discount' (per-SKU net, excludes shipping/taxes)

    Input file  : TikTok_orders_YYYY-MM-DD.csv  (settings.TIKTOK_ORDERS_PREFIX)
    SKU mapping : SKU ID → settings.TIKTOK_ID_MAP (same bundle expansion logic)
    """
    path = file_paths.get("primary")
    if not path or not path.exists():
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    df = load_csv(path)
    if df is None:
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    raw_count = len(df)

    expected_cols = ["Order Status", "SKU ID", "Quantity", "SKU Subtotal After Discount"]
    present_cols = [c for c in expected_cols if c in df.columns]
    if len(present_cols) < len(expected_cols):
        missing = [c for c in expected_cols if c not in df.columns]
        logger.error(f"❌ TikTok Shop orders: missing columns {missing}. Found: {df.columns.tolist()}")
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df = df[expected_cols].copy()
    df["Order Status"] = df["Order Status"].astype(str).str.strip()

    # Exclude cancelled orders — keep ALL fulfillment types (FBT + Seller Shipping)
    df = df[~df["Order Status"].str.lower().str.contains("cancel", na=False)].copy()
    logger.info(f"TikTok Shop Orders: {len(df)} non-cancelled rows (of {raw_count} total)")

    if df.empty:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    df["SKU Subtotal After Discount"] = df["SKU Subtotal After Discount"].apply(clean_money)
    df["SKU ID"] = df["SKU ID"].astype(str).str.strip()

    grouped_src = df.groupby("SKU ID")[["Quantity", "SKU Subtotal After Discount"]].sum().reset_index()

    expanded_rows = []
    bundle_units = 0.0
    bundle_rev = 0.0

    for _, row in grouped_src.iterrows():
        new_rows, is_bundle = _process_bundled_row(
            row, settings.TIKTOK_ID_MAP, "SKU ID", "Quantity",
            "SKU Subtotal After Discount", "TikTok Shop Orders"
        )
        expanded_rows.extend(new_rows)
        if is_bundle:
            bundle_units += float(row.get("Quantity") or 0)
            bundle_rev += float(row.get("SKU Subtotal After Discount") or 0)

    if not expanded_rows:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df_norm = pd.DataFrame(expanded_rows)
    grouped = df_norm.groupby("SKU")[["Units", "Revenue"]].sum().reset_index()
    return ParseResult(df=grouped, raw_count=raw_count,
                       bundle_stats={"Units": bundle_units, "Revenue": bundle_rev})


# ---------------------------------------------------------------------------
# EPIC-008 Stubs — pending raw order file availability
# Each stub documents exactly what to implement and how to wire it in.
# ---------------------------------------------------------------------------

def parse_shopify_orders_report(file_paths: dict) -> ParseResult:  # noqa: ARG001
    """
    [NOT YET IMPLEMENTED — EPIC-008 Step 1]

    Replaces  : parse_shopify_sales_report + Shopify_sales_*.csv
    Input file: Shopify_orders_YYYY-MM-DD.csv  (settings.SHOPIFY_ORDERS_PREFIX)

    How to download:
      Shopify Admin → Orders → Export → "All orders" as plain CSV.
      Rename to Shopify_orders_YYYY-MM-DD.csv using the export date.

    Key columns in the export:
      Name             — order number (e.g. "#1001")
      Financial Status — filter: keep "paid" rows only
      Lineitem sku     — internal SKU or bundle name → apply SHOPIFY_SKU_MAP
      Lineitem quantity — units
      Lineitem price   — unit price; Revenue = Lineitem price × Lineitem quantity
      Source name      — maps to channel (same logic as parse_shopify_sales_report:
                          "tiktok"→"TikTok Shopify", "Marketplace Connect"→"Target", etc.)

    Implementation steps:
      1. Filter: Financial Status == "paid"
      2. Compute Revenue = Lineitem price × Lineitem quantity per row
      3. Apply channel mapping (same as current parse_shopify_sales_report)
      4. For each row: _process_bundled_row(row, SHOPIFY_SKU_MAP, "Lineitem sku", ...)
      5. Group by SKU + Channel, sum Units and Revenue
      6. Track per-channel bundle_stats (same dict-of-dicts as current parser)

    To wire in, replace the PARSER_REGISTRY entry in SalesPipeline:
      {"channel": "Mixed", "parser": parsers.parse_shopify_orders_report,
       "files": {"primary": settings.SHOPIFY_ORDERS_PREFIX}}

    Also: add tests/fixtures/shopify_orders.csv + TestParseShopifyOrdersReport.
    See docs/epic008_raw_orders.md for a full implementation checklist.
    """
    logger.error("❌ parse_shopify_orders_report is not yet implemented (EPIC-008 Step 1).")
    return ParseResult(df=None, raw_count=0, bundle_stats={})


def parse_amazon_orders_report(file_paths: dict) -> ParseResult:
    """
    Reads raw Amazon_orders_*.{txt,csv} and aggregates for the Amazon SALES channel.

    Replaces parse_amazon_sales_report (EPIC-008 Step 2).

    Download instructions:
      Amazon Seller Central → Orders → Order Reports → "Request Report".
      Select the desired date range and "All statuses". The downloaded file arrives
      with a random numeric name and a .txt extension (tab-separated).
      Rename it to Amazon_orders_YYYY-MM-DD.txt and drop it in input/.

    Filters applied:
      - sales-channel == "Amazon.com"  (excludes MCF / Non-Amazon fulfillment orders)
      - order-status == "Shipped"      (excludes Cancelled and Pending)

    Note on Pending: Pending orders (not yet fulfilled) are intentionally excluded.
    They may still ship or cancel; counting them early would inflate figures inconsistently
    with the "shipped = sold" convention used across all other channels.

    Key columns:
      sales-channel  — filter: keep "Amazon.com" only
      order-status   — filter: keep "Shipped" only
      sku            — MSKU → apply AMAZON_SKU_MAP directly (map has trailing-s variants)
      quantity       — units per line item (already an integer count)
      item-price     — total revenue for the line (unit price × quantity, pre-multiplied)
    """
    path = file_paths.get("primary")
    if not path or not path.exists():
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    # Amazon exports as tab-separated .txt; fixture/fallback files may be .csv
    df = load_csv(path, sep="\t") if path.suffix.lower() == ".txt" else load_csv(path)

    if df is None:
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    raw_count = len(df)

    required_cols = ["sales-channel", "order-status", "sku", "quantity", "item-price"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error(
            f"❌ Amazon Orders: missing columns {missing}. Found: {df.columns.tolist()}"
        )
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    # Filter: Amazon.com marketplace only (exclude MCF / Non-Amazon channels)
    df = df[df["sales-channel"] == "Amazon.com"].copy()
    # Filter: Shipped only (Pending and Cancelled excluded)
    df = df[df["order-status"] == "Shipped"].copy()

    logger.info(f"Amazon Orders: {len(df)} Shipped Amazon.com rows (of {raw_count} total)")

    if df.empty:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0)
    df["item-price"] = df["item-price"].apply(clean_money)
    df["sku"] = df["sku"].astype(str).str.strip()

    # Group by MSKU before mapping — many rows per SKU across multiple orders
    grouped_src = df.groupby("sku")[["quantity", "item-price"]].sum().reset_index()

    expanded_rows = []
    bundle_units = 0.0
    bundle_rev = 0.0

    for _, row in grouped_src.iterrows():
        new_rows, is_bundle = _process_bundled_row(
            row, settings.AMAZON_SKU_MAP, "sku", "quantity", "item-price", "Amazon Orders"
        )
        expanded_rows.extend(new_rows)
        if is_bundle:
            bundle_units += float(row["quantity"] or 0)
            bundle_rev += float(row["item-price"] or 0)

    if not expanded_rows:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df_norm = pd.DataFrame(expanded_rows)
    grouped = df_norm.groupby("SKU")[["Units", "Revenue"]].sum().reset_index()

    logger.info(f"✅ Parsed {path.name} successfully.")
    return ParseResult(
        df=grouped,
        raw_count=raw_count,
        bundle_stats={"Units": bundle_units, "Revenue": bundle_rev},
    )


def parse_walmart_orders_report(file_paths: dict) -> ParseResult:  # noqa: ARG001
    """
    [NOT YET IMPLEMENTED — EPIC-008 Step 3]

    Replaces  : parse_walmart_sales_report + Walmart_sales_*.csv
    Input file: Walmart_orders_YYYY-MM-DD.csv  (settings.WALMART_ORDERS_PREFIX)

    How to download:
      Walmart Seller Center → Orders → Manage Orders → Export → CSV.
      IMPORTANT: Confirm column names before implementing — Walmart's export format
      varies by account type. Download a sample and verify column names first.
      Rename to Walmart_orders_YYYY-MM-DD.csv using the export date.

    Expected columns (verify against actual export before implementing):
      Purchase Order ID — order ID
      Order Date        — order date
      SKU               — seller SKU → direct match to internal SKU (no mapping needed)
      Quantity          — units
      Unit Price        — unit price; Revenue = Quantity × Unit Price
      Order Line Status — filter: keep "Acknowledged", "Shipped", "Delivered"
                         (exclude "Cancelled", "Refunded")

    Implementation steps:
      1. Filter on Order Line Status (exclude cancelled/refunded)
      2. Compute Revenue = Quantity × Unit Price
      3. Group by SKU, sum Units and Revenue
      4. No bundle expansion needed (Walmart SKUs are 1:1 with internal SKUs)
      5. Return bundle_stats = {"Units": 0, "Revenue": 0}

    To wire in, replace the PARSER_REGISTRY entry in SalesPipeline:
      {"channel": "Walmart", "parser": parsers.parse_walmart_orders_report,
       "files": {"primary": settings.WALMART_ORDERS_PREFIX}}

    Also: add tests/fixtures/walmart_orders.csv + TestParseWalmartOrdersReport.
    See docs/epic008_raw_orders.md for a full implementation checklist.
    """
    logger.error("❌ parse_walmart_orders_report is not yet implemented (EPIC-008 Step 3).")
    return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})


def parse_shopify_sales_report(file_paths: dict) -> ParseResult:
    df = load_csv(file_paths["primary"])
    if df is None:
        return ParseResult(df=None, raw_count=0, bundle_stats={})

    raw_count = len(df)

    df = df[df["Sales channel"] != "Draft Orders"]
    df = df[(df["Net sales"] > 0) | (df["Quantity ordered"] > 0)]

    all_expanded = []
    bundle_stats = {
        "Shopify": {"Units": 0.0, "Revenue": 0.0},
        "TikTok Shopify": {"Units": 0.0, "Revenue": 0.0},
        "Target": {"Units": 0.0, "Revenue": 0.0},
        "Others": {"Units": 0.0, "Revenue": 0.0},
    }

    for _, row in df.iterrows():
        c_name = str(row["Sales channel"])
        if c_name == "TikTok":
            bucket = "TikTok Shopify"
        elif c_name == "Marketplace Connect":
            bucket = "Target"
        elif c_name in ["Online Store", "Shop", "Loop Subscriptions"]:
            bucket = "Shopify"
        else:
            bucket = "Others"

        new_rows, is_bundle = _process_bundled_row(
            row,
            settings.SHOPIFY_SKU_MAP,
            "Product variant SKU",
            "Quantity ordered",
            "Net sales",
            f"Shopify ({c_name})",
        )

        for r in new_rows:
            r["Channel"] = bucket  # Tag the channel immediately
            all_expanded.append(r)

        if is_bundle:
            bundle_stats[bucket]["Units"] += float(row["Quantity ordered"] or 0)
            bundle_stats[bucket]["Revenue"] += clean_money(row["Net sales"])

    if not all_expanded:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats=bundle_stats)

    # Group by SKU AND Channel
    df_all = pd.DataFrame(all_expanded)
    grouped = (
        df_all.groupby(["SKU", "Channel"])[["Units", "Revenue"]].sum().reset_index()
    )
    return ParseResult(df=grouped, raw_count=raw_count, bundle_stats=bundle_stats)


def _normalize_and_aggregate_amazon_report(
    df: pd.DataFrame, column_mappings: dict[str, str], source_sku_col: str
) -> pd.DataFrame:
    """
    A robust, reusable helper for parsing Amazon-based reports (FBA, AWD).
    - Filters by the master Amazon SKU list.
    - Normalizes SKUs (removes trailing 's').
    - Aggregates data by summing to prevent duplicates.
    - Renames columns to the standard internal schema (e.g., 'units_sold').
    """
    df_filtered = df[df[source_sku_col].astype(str).isin(settings.AMAZON_SKUs)].copy()

    # Create a temporary DataFrame using the provided column mappings
    temp_df = pd.DataFrame()
    temp_df["sku"] = (
        df_filtered[source_sku_col].astype(str).str.replace(r"s$", "", regex=True)
    )

    for standard_name, source_name in column_mappings.items():
        # Handle cases where inventory is calculated from multiple columns
        if isinstance(source_name, list):
            temp_df[standard_name] = df_filtered[source_name].sum(axis="columns")  # type: ignore
        else:
            temp_df[standard_name] = df_filtered[source_name]

    # --- THIS IS THE CRITICAL FIX ---
    # Group by the normalized 'sku' and sum the values. This collapses duplicates correctly.
    parsed_data = temp_df.groupby("sku").sum().reset_index()
    return parsed_data


def parse_fba_report(file_paths: dict[str, Path]) -> ParseResult:
    """Loads FBA data and transforms it into the standard normalized format."""
    df = load_csv(file_paths["primary"])
    if df is None:
        return ParseResult(df=None)

    raw_count = len(df)

    # Define the specific column mappings for an FBA report
    fba_column_map = {
        "units_sold": "Units Sold Last 30 Days",
        "inventory": ["Available", "FC transfer"],
        "inbound": "Inbound",
    }

    # Use the helper to perform the core logic
    parsed_data = _normalize_and_aggregate_amazon_report(
        df, fba_column_map, source_sku_col="Merchant SKU"
    )

    # Ensure all desired SKUs are present in the output
    full_sku_template = pd.DataFrame({"sku": settings.SKU_ORDER})
    df_normalized = pd.merge(full_sku_template, parsed_data, on="sku", how="left")

    df_normalized["channel"] = "FBA"
    df_normalized = df_normalized.fillna(0)

    # Rename to CamelCase at the parser boundary
    df_normalized = df_normalized.rename(columns={
        "sku": "SKU",
        "channel": "Channel",
        "units_sold": "Units",
        "inventory": "Inventory",
        "inbound": "Inbound",
    })

    logger.info(f"✅ Parsed {file_paths['primary'].name} successfully.")
    return ParseResult(df=df_normalized, raw_count=raw_count)


def parse_fbt_inventory_report(file_paths: dict[str, Path]) -> ParseResult:
    """
    Parses the FBT Inventory report (TikTok Fulfillment).
    Aggregates inventory across multiple locations for each SKU.
    """
    df = load_csv(file_paths["primary"])
    if df is None:
        return ParseResult(df=None)

    # Check if df is empty
    if df.empty:
        return ParseResult(df=None)

    # Rename columns for clarity before processing
    col_map = {
        "Reference code": "sku",
        "Available inventory": "inventory",
        "In Transit: Total Quantity": "inbound",
    }

    # Check if necessary columns exist
    missing_cols = [c for c in col_map.keys() if c not in df.columns]
    if missing_cols:
        logger.error(f"❌ FBT Report missing columns: {missing_cols}")
        return ParseResult(df=None)

    raw_count = len(df)

    # Rename
    df = df.rename(columns=col_map)

    # Convert to numeric, forcing errors to NaN then 0
    df["inventory"] = pd.to_numeric(df["inventory"], errors="coerce").fillna(0)
    df["inbound"] = pd.to_numeric(df["inbound"], errors="coerce").fillna(0)

    # Group by SKU (Reference code) and sum
    grouped = df.groupby("sku")[["inventory", "inbound"]].sum().reset_index()

    # Ensure SKU is string and clean
    grouped["sku"] = grouped["sku"].astype(str).str.strip()

    # Filter/Merge with Master SKU List
    full_sku_template = pd.DataFrame({"sku": settings.SKU_ORDER})
    full_sku_template["sku"] = full_sku_template["sku"].astype(str)

    df_normalized = pd.merge(full_sku_template, grouped, on="sku", how="left")

    df_normalized["channel"] = "FBT"
    df_normalized["units_sold"] = 0
    df_normalized = df_normalized.fillna(0)

    # Rename to CamelCase at the parser boundary
    df_normalized = df_normalized.rename(columns={
        "sku": "SKU",
        "channel": "Channel",
        "units_sold": "Units",
        "inventory": "Inventory",
        "inbound": "Inbound",
    })

    logger.info(f"✅ Parsed {file_paths['primary'].name} successfully.")
    return ParseResult(df=df_normalized, raw_count=raw_count)


def parse_fbt_report(file_paths: dict[str, Path]) -> ParseResult:
    """
    Combined FBT parser that mirrors `parse_wfs_report` structure.
    Expects `file_paths` to contain:
      - 'sales': TikTok Orders file (Fulfillment by TikTok Shop)
      - 'inventory': FBT inventory file

    Returns a ParseResult with CamelCase columns: Channel, SKU, Units, Inventory, Inbound
    """
    # Load inventory portion using existing inventory parser
    inv_path = file_paths.get("inventory")
    if inv_path is None:
        return ParseResult(df=None)
    inv_paths = {"primary": inv_path}
    inventory_result = parse_fbt_inventory_report(inv_paths)
    if inventory_result.df is None:
        return ParseResult(df=None)

    inventory_df = inventory_result.df

    # Load sales portion using existing TikTok orders parser (returns ParseResult)
    tiktok_result = parse_tiktok_orders_report({"primary": file_paths.get("sales")})
    sales_grouped = tiktok_result.df

    # If sales parser returned None, create empty grouped frame
    if sales_grouped is None:
        sales_grouped = pd.DataFrame(columns=["SKU", "Units", "Revenue"])

    # Normalize column names for merge (inventory_df is already CamelCase; tiktok returns CamelCase)
    # Temporarily rename to lowercase for the merge
    inventory_df = inventory_df.rename(columns={
        "SKU": "sku", "Channel": "channel", "Units": "units_sold",
        "Inventory": "inventory", "Inbound": "inbound",
    })

    if "SKU" in sales_grouped.columns:
        sales_grouped = sales_grouped.rename(columns={"SKU": "sku", "Units": "units_sold"})
    else:
        sales_grouped = sales_grouped.rename(lambda c: c.lower(), axis=1)

    # Ensure units_sold exists
    if "units_sold" not in sales_grouped.columns:
        sales_grouped["units_sold"] = 0

    # Merge sales into inventory template
    merged = pd.merge(
        inventory_df,
        sales_grouped[["sku", "units_sold"]],
        on="sku",
        how="left",
    )

    # If units_sold came from inventory_df (was present as 0), prefer sales value
    merged["units_sold"] = (
        merged["units_sold_y"].fillna(merged.get("units_sold_x", 0))
        if "units_sold_y" in merged.columns
        else merged["units_sold"]
    )

    # Clean up any extraneous columns created by merge
    for col in [c for c in merged.columns if c.endswith("_x") or c.endswith("_y")]:
        merged = merged.drop(columns=[col])

    # Ensure types and fillna
    merged["units_sold"] = merged["units_sold"].fillna(0).astype(float)
    merged["inventory"] = merged["inventory"].fillna(0).astype(float)
    merged["inbound"] = merged["inbound"].fillna(0).astype(float)

    merged["channel"] = "FBT"
    merged = merged.fillna(0)

    # Rename to CamelCase at the parser boundary
    merged = merged.rename(columns={
        "sku": "SKU",
        "channel": "Channel",
        "units_sold": "Units",
        "inventory": "Inventory",
        "inbound": "Inbound",
    })

    logger.info("✅ Parsed FBT combined (sales + inventory) reports successfully.")
    return ParseResult(df=merged, raw_count=inventory_result.raw_count)


def parse_awd_report(file_paths: dict[str, Path]) -> ParseResult:
    """Loads and parses the AWD report, transforming it to the standard normalized format."""
    df = load_csv(file_paths["primary"], skiprows=2)
    if df is None:
        return ParseResult(df=None)

    raw_count = len(df)

    # Define the specific column mappings for an AWD report
    awd_column_map = {
        "inventory": "Available in AWD (units)",
        "inbound": "Inbound to AWD (units)",
    }

    # Use the same robust helper function
    parsed_data = _normalize_and_aggregate_amazon_report(
        df, awd_column_map, source_sku_col="SKU"
    )

    # Ensure all desired SKUs are present in the output
    full_sku_template = pd.DataFrame({"sku": settings.SKU_ORDER})
    df_normalized = pd.merge(full_sku_template, parsed_data, on="sku", how="left")

    df_normalized["channel"] = "AWD"
    df_normalized["units_sold"] = 0  # No sales data in this report
    df_normalized = df_normalized.fillna(0)

    # Rename to CamelCase at the parser boundary
    df_normalized = df_normalized.rename(columns={
        "sku": "SKU",
        "channel": "Channel",
        "units_sold": "Units",
        "inventory": "Inventory",
        "inbound": "Inbound",
    })

    logger.info(f"✅ Parsed {file_paths['primary'].name} successfully.")
    return ParseResult(df=df_normalized, raw_count=raw_count)


def parse_flexport_reports(file_paths: dict[str, Path]) -> ParseResult:
    """
    Parses Flexport data using the new 3-file system:
    1. Levels (Inventory): Provides stock levels for DTC and Reserve.
    2. Orders (Sales): Provides raw order data which we aggregate.
    3. Inbound: (Legacy) Provides inbound metrics.
    """
    levels_df = load_csv(file_paths["levels"])
    orders_df = load_csv(file_paths["orders"])
    inbound_df = load_csv(file_paths["inbound"])

    if levels_df is None or orders_df is None:
        return ParseResult(df=None)

    raw_count = len(orders_df)

    # --- Part A: Process Inventory (Levels) ---
    levels_filtered = levels_df[levels_df["MSKU"].isin(settings.SKU_ORDER)].copy()

    cols_to_sum = ["DTC Total Quantity", "RS Total Quantity", "Ops WIP Quantity"]
    for col in cols_to_sum:
        levels_filtered[col] = pd.to_numeric(
            levels_filtered[col], errors="coerce"
        ).fillna(0)

    levels_agg = levels_filtered.groupby("MSKU")[cols_to_sum].sum().reset_index()
    levels_agg = levels_agg.rename(columns={"MSKU": "sku"})

    levels_agg["dtc_inventory"] = levels_agg["DTC Total Quantity"]
    levels_agg["reserve_inventory"] = (
        levels_agg["RS Total Quantity"] - levels_agg["Ops WIP Quantity"]
    )
    levels_agg["reserve_inventory"] = levels_agg["reserve_inventory"].clip(lower=0)

    # --- Part B: Process Sales (Orders) ---
    orders_clean = orders_df[orders_df["Order Status"] != "CANCELLED"].copy()

    orders_clean["Items"] = orders_clean["Items"].apply(
        lambda x: json.loads(x) if isinstance(x, str) else []
    )
    orders_exploded = orders_clean.explode("Items")

    orders_exploded["dsku"] = orders_exploded["Items"].apply(
        lambda x: x.get("dsku") if isinstance(x, dict) else None
    )
    orders_exploded["qty"] = orders_exploded["Items"].apply(
        lambda x: x.get("qty", 0) if isinstance(x, dict) else 0
    )

    orders_exploded["sku"] = orders_exploded["dsku"].map(settings.DSKU_TO_SKU_MAP)

    orders_valid = orders_exploded[orders_exploded["sku"].notna()].copy()

    sales_agg = orders_valid.groupby("sku")["qty"].sum().reset_index()
    sales_agg = sales_agg.rename(columns={"qty": "units_sold"})

    # --- Part C: Process Inbound (Legacy Logic) ---
    inbound_data = pd.DataFrame({"sku": settings.SKU_ORDER, "inbound": 0})

    if inbound_df is not None:
        inbound_filtered = inbound_df[
            inbound_df["MSKU"].isin(settings.SKU_ORDER)
        ].copy()
        inbound_cols = [
            "IN_TRANSIT_WITHIN_DELIVERR_UNDER_60_DAYS",
            "IN_TRANSIT_TO_DELIVERR",
        ]
        for col in inbound_cols:
            inbound_filtered[col] = pd.to_numeric(
                inbound_filtered[col], errors="coerce"
            ).fillna(0)

        inbound_filtered["inbound_calc"] = inbound_filtered[inbound_cols].sum(axis=1)
        inbound_grouped = (
            inbound_filtered.groupby("MSKU")["inbound_calc"].sum().reset_index()
        )
        inbound_grouped = inbound_grouped.rename(
            columns={"MSKU": "sku", "inbound_calc": "inbound"}
        )

        inbound_data = pd.merge(
            pd.DataFrame({"sku": settings.SKU_ORDER}),
            inbound_grouped,
            on="sku",
            how="left",
        ).fillna(0)

    # --- Part D: Merge and Finalize ---

    # 1. DTC DataFrame Construction
    dtc_final = pd.DataFrame({"sku": settings.SKU_ORDER})
    dtc_final = pd.merge(dtc_final, levels_agg[["sku", "dtc_inventory"]], on="sku", how="left")
    dtc_final = pd.merge(dtc_final, sales_agg, on="sku", how="left")
    dtc_final = pd.merge(dtc_final, inbound_data, on="sku", how="left")

    dtc_final["channel"] = "DTC"
    dtc_final = dtc_final.rename(columns={"dtc_inventory": "inventory"})

    # 2. Reserve DataFrame Construction
    reserve_final = pd.DataFrame({"sku": settings.SKU_ORDER})
    reserve_final = pd.merge(
        reserve_final, levels_agg[["sku", "reserve_inventory"]], on="sku", how="left"
    )

    reserve_final["channel"] = "Reserve"
    reserve_final["units_sold"] = 0
    reserve_final["inbound"] = 0
    reserve_final = reserve_final.rename(columns={"reserve_inventory": "inventory"})

    # 3. Concatenate
    df_normalized = pd.concat([dtc_final, reserve_final], ignore_index=True)
    df_normalized = df_normalized.fillna(0)

    # Rename to CamelCase at the parser boundary
    df_normalized = df_normalized.rename(columns={
        "sku": "SKU",
        "channel": "Channel",
        "units_sold": "Units",
        "inventory": "Inventory",
        "inbound": "Inbound",
    })

    logger.info("✅ Parsed Flexport (DTC & Reserve) using new Levels/Orders reports.")
    return ParseResult(df=df_normalized, raw_count=raw_count)


def parse_wfs_report(file_paths: dict[str, Path]) -> ParseResult:
    """
    Loads and merges Walmart Sales and WFS Inventory reports.
    Expects a dict with 'sales' and 'inventory' file paths.
    """
    sales_df = load_csv(file_paths["sales"])
    inventory_df = load_csv(file_paths["inventory"])

    if sales_df is None or inventory_df is None:
        return ParseResult(df=None)

    raw_count = len(sales_df)

    # Step 1: Process the Sales Report
    sales_data = sales_df.rename(columns={"SKU": "sku", "Units_Sold": "units_sold"})
    sales_data = sales_data[["sku", "units_sold"]]
    sales_data = sales_data.groupby("sku").sum().reset_index()

    # Step 2: Process the Inventory Report
    inventory_data = inventory_df.rename(
        columns={
            "SKU": "sku",
            "Available units": "inventory",
            "Inbound units": "inbound",
        }
    )
    inventory_data = inventory_data[["sku", "inventory", "inbound"]]

    # Step 3: Merge sales and inventory data (outer join)
    merged_data = pd.merge(sales_data, inventory_data, on="sku", how="outer")

    # Step 4: Ensure all desired SKUs are present
    full_sku_template = pd.DataFrame({"sku": settings.SKU_ORDER})
    df_normalized = pd.merge(full_sku_template, merged_data, on="sku", how="left")

    # Step 5: Finalize
    df_normalized["channel"] = "WFS"
    df_normalized = df_normalized.fillna(0)

    # Rename to CamelCase at the parser boundary
    df_normalized = df_normalized.rename(columns={
        "sku": "SKU",
        "channel": "Channel",
        "units_sold": "Units",
        "inventory": "Inventory",
        "inbound": "Inbound",
    })

    logger.info("✅ Parsed Walmart/WFS reports successfully.")
    return ParseResult(df=df_normalized, raw_count=raw_count)


def parse_tiktok_orders_report(file_paths: dict) -> ParseResult:
    """
    Placeholder for TikTok Orders report.
    Expected file: TikTok_orders_yyyy_mm_dd.csv
    """
    path = file_paths.get("primary")
    if not path or not path.exists():
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    df = load_csv(path)
    if df is None:
        return ParseResult(df=None, raw_count=0, bundle_stats={"Units": 0, "Revenue": 0})

    # Raw row count before filtering
    raw_count = len(df)

    # Required columns per spec
    expected_cols = [
        "Order Status",
        "SKU ID",
        "Seller SKU",
        "Quantity",
        "Order Amount",
        "Fulfillment Type",
        "Warehouse Name",
    ]

    # Reduce to intersection to avoid KeyErrors if some are missing
    present_cols = [c for c in expected_cols if c in df.columns]
    if not present_cols:
        logger.error(
            f"❌ TikTok Orders report missing expected columns. Available: {df.columns.tolist()}"
        )
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df = df[present_cols].copy()
    logger.info(f"TikTok Orders: Rows after column filter: {len(df)}")

    # Normalize textual columns for robust filtering
    df["Order Status"] = df["Order Status"].astype(str).str.strip()
    if "Fulfillment Type" in df.columns:
        df["Fulfillment Type"] = df["Fulfillment Type"].astype(str).str.strip()
    else:
        df["Fulfillment Type"] = ""

    # 1) Filter out cancelled orders
    df = df[~df["Order Status"].str.lower().str.contains("cancel", na=False)].copy()
    logger.info(f"TikTok Orders: Rows after Order Status filter: {len(df)}")

    # 2) Keep only Fulfillment by TikTok Shop
    df = df[
        df["Fulfillment Type"].str.lower() == "fulfillment by tiktok shop".lower()
    ].copy()
    logger.info(f"TikTok Orders: Rows after Fulfillment Type filter: {len(df)}")

    if df.empty:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    # Ensure numeric conversions
    if "Quantity" in df.columns:
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    else:
        df["Quantity"] = 0

    if "Order Amount" in df.columns:
        df["Order Amount"] = df["Order Amount"].apply(clean_money)
    else:
        df["Order Amount"] = 0

    # Ensure SKU ID is string for mapping
    df["SKU ID"] = df["SKU ID"].astype(str).str.strip()
    grouped_src = df.groupby("SKU ID")[["Quantity", "Order Amount"]].sum().reset_index()
    logger.info(f"TikTok Orders: Grouped SKUs: {grouped_src['SKU ID'].tolist()}")

    # Explode / map to internal SKUs using the same bundle logic
    expanded_rows = []
    bundle_units = 0.0
    bundle_rev = 0.0

    mapped_skus = []
    for _, row in grouped_src.iterrows():
        new_rows, is_bundle = _process_bundled_row(
            row,
            settings.TIKTOK_ID_MAP,
            "SKU ID",
            "Quantity",
            "Order Amount",
            "TikTok Orders",
        )
        expanded_rows.extend(new_rows)
        mapped_skus.extend([r["SKU"] for r in new_rows])
        if is_bundle:
            bundle_units += float(row.get("Quantity") or 0)
            bundle_rev += float(row.get("Order Amount") or 0)
    logger.info(f"TikTok Orders: Mapped internal SKUs: {mapped_skus}")

    if not expanded_rows:
        return ParseResult(df=None, raw_count=raw_count, bundle_stats={"Units": 0, "Revenue": 0})

    df_norm = pd.DataFrame(expanded_rows)
    grouped = df_norm.groupby("SKU")[["Units", "Revenue"]].sum().reset_index()

    return ParseResult(df=grouped, raw_count=raw_count, bundle_stats={"Units": bundle_units, "Revenue": bundle_rev})
