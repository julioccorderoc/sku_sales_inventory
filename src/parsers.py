import pandas as pd
from pathlib import Path

from . import settings
from .utils import load_csv


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


def parse_fba_report(file_paths: dict[str, Path]) -> pd.DataFrame | None:
    """Loads FBA data and transforms it into the standard normalized format."""
    df = load_csv(file_paths["primary"])
    if df is None:
        return None

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

    print(f"✅ Parsed {file_paths['primary'].name} successfully.")
    return df_normalized


def parse_awd_report(file_paths: dict[str, Path]) -> pd.DataFrame | None:
    """Loads and parses the AWD report, transforming it to the standard normalized format."""
    df = load_csv(file_paths["primary"], skiprows=2)
    if df is None:
        return None

    # Define the specific column mappings for an AWD report
    awd_column_map = {
        "inventory": ["Available in AWD (units)", "Reserved in AWD (units)"],
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

    print(f"✅ Parsed {file_paths['primary'].name} successfully.")
    return df_normalized


def parse_flexport_reports(file_paths: dict[str, Path]) -> pd.DataFrame | None:
    """
    A one-to-many parser. Reads Flexport files once and generates data for
    both the 'DTC' and 'Reserve' channels.
    """
    inventory_df = load_csv(file_paths["inventory"])
    inbound_df = load_csv(file_paths["inbound"])

    if inventory_df is None:
        return None

    # --- Part A: Process DTC Data ---
    inv_filtered_dtc = inventory_df[inventory_df["SKU"].isin(settings.SKU_ORDER)].copy()
    dtc_data = (
        inv_filtered_dtc.groupby("SKU")
        .agg(
            units_sold=("Ecom Last 30 Days", "max"),
            inventory=("Available in Ecom", "sum"),
        )
        .reset_index()
        .rename(columns={"SKU": "sku"})
    )
    if inbound_df is not None:
        # (This inbound logic is correct and remains unchanged)
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
        inbound_data = inbound_filtered.rename(columns={"MSKU": "sku"})
        inbound_data["inbound"] = inbound_data[inbound_cols].sum(axis=1)
        inbound_data = inbound_data.groupby("sku")[["inbound"]].sum().reset_index()
        dtc_data = pd.merge(dtc_data, inbound_data, on="sku", how="left")

    # --- Part B: Process Reserve Data ---
    inv_filtered_reserve = inventory_df[
        inventory_df["SKU"].isin(settings.SKU_ORDER)
    ].copy()
    reserve_data = (
        inv_filtered_reserve.groupby("SKU")
        .agg(inventory=("Available in Reserve", "sum"))
        .reset_index()
        .rename(columns={"SKU": "sku"})
    )

    # --- Part C: Correctly Combine and Finalize ---
    # 1. Create a complete template for EACH channel FIRST.
    dtc_template = pd.DataFrame({"sku": settings.SKU_ORDER})
    reserve_template = pd.DataFrame({"sku": settings.SKU_ORDER})

    # 2. Merge the parsed data into the templates.
    dtc_final = pd.merge(dtc_template, dtc_data, on="sku", how="left")
    reserve_final = pd.merge(reserve_template, reserve_data, on="sku", how="left")

    # 3. NOW assign the channel. This overwrites any potential NaNs in the channel column.
    dtc_final["channel"] = "DTC"
    reserve_final["channel"] = "Reserve"

    # 4. Concatenate the two complete, channel-labeled DataFrames.
    df_normalized = pd.concat([dtc_final, reserve_final], ignore_index=True)

    # 5. Finally, fill all remaining numeric NaNs with 0. This is now safe.
    df_normalized = df_normalized.fillna(0)

    print("✅ Parsed Flexport (DTC & Reserve) reports successfully.")
    return df_normalized


# def parse_dtc_report(file_paths: dict[str, Path]) -> pd.DataFrame | None:
#     """
#     Loads and merges DTC Inventory and Inbound reports.
#     Expects a dict with 'inventory' and 'inbound' file paths.
#     """
#     inventory_df = load_csv(file_paths["inventory"])
#     inbound_df = load_csv(file_paths["inbound"])

#     if inventory_df is None or inbound_df is None:
#         return None

#     # --- Step 1: Process the Inventory/Sales Report (mostly old logic) ---
#     inv_filtered = inventory_df[inventory_df["SKU"].isin(settings.SKU_ORDER)].copy()
#     inventory_sales_data = (
#         inv_filtered.groupby("SKU")
#         .agg(
#             units_sold=("Ecom Last 30 Days", "max"),
#             inventory=("Available in Ecom", "sum"),
#         )
#         .reset_index()
#         .rename(columns={"SKU": "sku"})
#     )

#     # --- Step 2: Process the new Inbound Reconciliation Report ---
#     inbound_filtered = inbound_df[inbound_df["MSKU"].isin(settings.SKU_ORDER)].copy()

#     # Define columns for inbound calculation
#     inbound_cols = [
#         "IN_TRANSIT_WITHIN_DELIVERR_UNDER_60_DAYS",
#         "IN_TRANSIT_TO_DELIVERR",
#     ]

#     # Defensively convert to numeric, coercing errors to NaN, then filling with 0
#     for col in inbound_cols:
#         inbound_filtered[col] = pd.to_numeric(
#             inbound_filtered[col], errors="coerce"
#         ).fillna(0)

#     # Calculate the total inbound
#     inbound_filtered["inbound"] = inbound_filtered[inbound_cols].sum(axis=1)

#     # Prepare for merging
#     inbound_data = inbound_filtered.rename(columns={"MSKU": "sku"})[["sku", "inbound"]]
#     # Defensively aggregate in case a SKU appears multiple times
#     inbound_data = inbound_data.groupby("sku").sum().reset_index()

#     # --- Step 3: Merge the two data sources ---
#     # An outer join ensures we keep SKUs that might only appear in one of the files.
#     merged_data = pd.merge(inventory_sales_data, inbound_data, on="sku", how="outer")

#     # --- Step 4: Apply the template to ensure all 32 SKUs are present ---
#     full_sku_template = pd.DataFrame({"sku": settings.SKU_ORDER})
#     df_normalized = pd.merge(full_sku_template, merged_data, on="sku", how="left")

#     # --- Step 5: Finalize the standard format ---
#     df_normalized["channel"] = "DTC"
#     df_normalized = df_normalized.fillna(0)

#     print("✅ Parsed DTC reports successfully.")
#     return df_normalized


def parse_wfs_report(file_paths: dict[str, Path]) -> pd.DataFrame | None:
    """
    Loads and merges Walmart Sales and WFS Inventory reports.
    Expects a dict with 'sales' and 'inventory' file paths.
    """
    sales_df = load_csv(file_paths["sales"])
    inventory_df = load_csv(file_paths["inventory"])

    if sales_df is None or inventory_df is None:
        return None

    # Step 1: Process the Sales Report
    sales_data = sales_df.rename(columns={"SKU": "sku", "Units_Sold": "units_sold"})
    sales_data = sales_data[["sku", "units_sold"]]
    # Defensively aggregate in case a SKU appears multiple times
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

    # Step 3: Merge sales and inventory data.
    # An outer join is crucial to keep SKUs that exist in one report but not the other.
    merged_data = pd.merge(sales_data, inventory_data, on="sku", how="outer")

    # Step 4: Ensure all desired SKUs are present in the final output.
    full_sku_template = pd.DataFrame({"sku": settings.SKU_ORDER})
    df_normalized = pd.merge(full_sku_template, merged_data, on="sku", how="left")

    # Step 5: Finalize the standard format
    df_normalized["channel"] = "WFS"
    df_normalized = df_normalized.fillna(0)  # Fill NaNs for any missing values with 0

    print("✅ Parsed Walmart/WFS reports successfully.")
    return df_normalized
