import pandas as pd
from datetime import date
from pydantic import ValidationError

from src import parsers, data_handler, settings, utils
from src.schemas import InventoryItem

# --- Parser Registry ---
PARSER_REGISTRY = [
    {
        "channel_name": "FBA",
        "parser_func": parsers.parse_fba_report,
        "required_files": {"primary": settings.FBA_FILENAME_PREFIX},
    },
    {
        "channel_name": "Flexport",
        "parser_func": parsers.parse_flexport_reports,
        "required_files": {
            "levels": settings.FLEXPORT_LEVELS_FILENAME_PREFIX,
            "orders": settings.FLEXPORT_ORDERS_FILENAME_PREFIX,
            "inbound": settings.FLEXPORT_INBOUND_FILENAME_PREFIX,
        },
    },
    {
        "channel_name": "AWD",
        "parser_func": parsers.parse_awd_report,
        "required_files": {"primary": settings.AWD_FILENAME_PREFIX},
    },
    {
        "channel_name": "WFS",
        "parser_func": parsers.parse_wfs_report,
        "required_files": {
            "sales": settings.WALMART_SALES_FILENAME_PREFIX,
            "inventory": settings.WFS_INVENTORY_FILENAME_PREFIX,
        },
    },
]


def run_inventory_update():
    print("--- Starting Daily Inventory Report Process ---")

    dataframes = []
    status_summary = {}

    # We need system date for the ID prefix (just like Sales)
    system_date_str = date.today().strftime("%Y%m%d")

    for parser_config in PARSER_REGISTRY:
        source_name = parser_config["channel_name"]
        print(f"\n-- Processing Source: {source_name} --")

        file_paths = {}
        report_dates = {}
        all_files_found = True

        for file_key, prefix in parser_config["required_files"].items():
            found_file_info = utils.find_latest_report(settings.INPUT_DIR, prefix)

            if found_file_info:
                path, report_date = found_file_info
                file_paths[file_key] = path
                report_dates[file_key] = report_date
                print(f"  > Found '{file_key}': {path.name} ({report_date})")
            else:
                if file_key == "inbound" and source_name == "Flexport":
                    print(f"  > INFO: Optional '{file_key}' missing. Continuing.")
                    file_paths[file_key] = None
                else:
                    print(f"  > ERROR: Required '{file_key}' missing.")
                    all_files_found = False
                    break

        if not all_files_found:
            print(f"Skipping source '{source_name}' due to missing files.")
            # Populate summary with None
            if source_name == "Flexport":
                status_summary["DTC"] = None
                status_summary["Reserve"] = None
            else:
                status_summary[source_name] = None
            continue

        # Run Parser
        df = parser_config["parser_func"](file_paths)
        if df is not None and not df.empty:
            # --- 1. Assign Date Column ---
            # (Renamed from 'last_updated' to 'Date')
            primary_date = (
                report_dates.get("levels")
                or report_dates.get("inventory")
                or report_dates.get("primary")
            )
            df["Date"] = primary_date
            dataframes.append(df)

            for ch in df["channel"].unique():
                status_summary[ch] = primary_date

    if not dataframes:
        print("\n❌ No dataframes parsed.")
        data_handler.post_to_webhook([], status_summary, "inventory")
        return

    # --- Combine ---
    print("\nConcatenating reports...")
    combined_df = pd.concat(dataframes, ignore_index=True)

    # --- 1.1 Normalize Data (Zero-Filling) ---
    print("\n--- Normalizing Data (Zero-Filling) ---")
    
    # Identify all channels processed in this run
    processed_channels = combined_df["channel"].unique()
    master_skus = [str(x) for x in settings.SKU_ORDER]
    
    # Get map of Channel -> Date from existing data
    channel_dates = combined_df.groupby("channel")["Date"].first().to_dict()
    
    # Generate template: Channel + SKU -> Date
    template_rows = []
    
    # We want to ensure that for every processed channel, we have an entry for every SKU
    for ch in processed_channels:
        # Default to system date if somehow missing
        date_for_channel = channel_dates.get(ch, date.today())
        
        for sku in master_skus:
            template_rows.append({
                "channel": ch, 
                "sku": sku, 
                "Date": date_for_channel
            })
            
    template_df = pd.DataFrame(template_rows)
    
    # Merge Actual Data into Template
    # We'll Left Merge on [channel, sku, Date] to keep the template shape
    merged_df = pd.merge(
        template_df, 
        combined_df, 
        on=["channel", "sku", "Date"], 
        how="left"
    )
    
    # Fill NuNs with 0 for metrics
    merged_df["units_sold"] = merged_df["units_sold"].fillna(0)
    merged_df["inventory"] = merged_df["inventory"].fillna(0)
    merged_df["inbound"] = merged_df["inbound"].fillna(0)
    
    # Replace combined_df with the normalized version
    combined_df = merged_df

    # --- 2. Generate IDs (New Structure) ---
    print("Generating IDs...")

    # ID: YYYYMMDD_Channel_SKU
    combined_df["id"] = (
        system_date_str
        + "_"
        + combined_df["channel"].astype(str)
        + "_"
        + combined_df["sku"].astype(str)
    )

    # SKU_Channel_ID: Channel_SKU
    combined_df["sku_channel_id"] = (
        combined_df["channel"].astype(str) + "_" + combined_df["sku"].astype(str)
    )

    # --- 3. Filter Columns & Validate ---
    # Get column names from Schema aliases
    final_columns = [
        field.alias or name for name, field in InventoryItem.model_fields.items()
    ]

    # Ensure all columns exist (fill missing with default if needed, though parsers should handle it)
    combined_df = combined_df.rename(
        columns={
            "channel": "Channel",
            "sku": "SKU",
            "units_sold": "Units",
            "inventory": "Inventory",
            "inbound": "Inbound",
        }
    )
    combined_df = combined_df[final_columns]

    try:
        print("Validating data against schema...")
        validated_data = [
            InventoryItem(**row) for row in combined_df.to_dict("records")
        ]
        print("✅ Data validation successful.")
    except ValidationError as e:
        print("❌ Data validation failed!")
        print(e)
        return

    # --- 4. Final Sort & Display ---
    # Create DF from validated data for nice printing
    display_df = pd.DataFrame(
        [item.model_dump(by_alias=True) for item in validated_data]
    )

    display_df["Channel"] = pd.Categorical(
        display_df["Channel"], categories=settings.CHANNEL_ORDER, ordered=True
    )
    display_df["SKU"] = pd.Categorical(
        display_df["SKU"], categories=settings.SKU_ORDER, ordered=True
    )
    display_df = display_df.sort_values(["Channel", "SKU"]).reset_index(drop=True)

    print("\n--- Final Normalized Report ---")
    print(
        display_df.drop(columns=["id", "sku_channel_id"]).to_string()
    )  # Drop IDs just for cleaner terminal print

    # --- 5. Save & Post ---
    print("\n--- Final Status Summary ---")
    for ch in settings.CHANNEL_ORDER:
        if ch not in status_summary:
            status_summary[ch] = None
        date_val = status_summary.get(ch)
        print(f"{ch}: {date_val.isoformat() if date_val else 'No data'}")

    data_handler.save_outputs(validated_data, "inventory_report")

    data_handler.post_to_webhook(
        validated_data=validated_data,
        metadata=status_summary,
        report_type="inventory",
    )

    print("\n--- Process Finished Successfully ---")
