import pandas as pd
from datetime import date
from pydantic import ValidationError

from src import parsers, data_handler, settings, utils
from src.schemas import InventoryItem

# --- Parser Registry ---
# The single source of truth for all our data sources.
# To add a report, just add a new entry here. It's that simple.
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
            "inventory": settings.FLEXPORT_INVENTORY_FILENAME_PREFIX,
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


def run_process():
    """Main orchestration function to run the entire reporting process."""
    print("--- Starting Daily Inventory Report Process ---")
    today_str = utils.get_date_str_for_filename()
    print(f"Processing reports for date string: '{today_str}'")

    dataframes = []
    # --- UPGRADED Orchestration Loop ---
    for parser_config in PARSER_REGISTRY:
        channel = parser_config["channel_name"]
        print(f"\n-- Processing Channel: {channel} --")

        file_paths = {}
        all_files_found = True

        # 1. Gather all required file paths for the current parser
        for file_key, prefix in parser_config["required_files"].items():
            path = settings.INPUT_DIR / f"{prefix}{today_str}.csv"
            if not path.exists():
                print(f"WARNING: Required file not found for {channel}: {path.name}")
                all_files_found = False
                break
            file_paths[file_key] = path

        # 2. If all files are present, run the parser
        if all_files_found:
            df = parser_config["parser_func"](file_paths)
            if df is not None:
                dataframes.append(df)
        else:
            print(f"Skipping channel {channel} due to missing files.")

    # 2. Combine all parsed dataframes by stacking them. This is the new, simpler "merge".
    print(f"Found {len(dataframes)} reports. Concatenating...")
    combined_df = pd.concat(dataframes, ignore_index=True)

    # 3. Create the unique ID and add metadata
    # This is the most efficient place to create the ID, after all rows are combined.
    # We use a vectorized operation which is extremely fast.
    print("Generating unique IDs for each record...")
    combined_df["id"] = combined_df["channel"] + "-" + combined_df["sku"]
    combined_df["last_updated"] = date.today()

    # 4. Reorder columns and ensure structure matches our Pydantic schema
    # The Pydantic model's field order is now the source of truth for our column order.
    final_columns = list(InventoryItem.model_fields.keys())
    combined_df = combined_df.reindex(columns=final_columns).fillna(0)

    # 5. Validate data against our Pydantic schema
    try:
        print("Validating data against schema...")
        validated_data = [
            InventoryItem(**row)  # type: ignore
            for row in combined_df.to_dict("records")
        ]
        print("✅ Data validation successful.")
    except ValidationError as e:
        print("❌ Data validation failed! The data does not match the required schema.")
        print(e)
        return

    # 6. Final Sorting
    combined_df["channel"] = pd.Categorical(
        combined_df["channel"], categories=settings.CHANNEL_ORDER, ordered=True
    )
    combined_df["sku"] = pd.Categorical(
        combined_df["sku"], categories=settings.SKU_ORDER, ordered=True
    )
    combined_df = combined_df.sort_values(["channel", "sku"]).reset_index(drop=True)

    # Re-apply the desired column order after sorting, as sorting can sometimes change it.
    combined_df = combined_df[final_columns]

    print("\n--- Final Normalized Report with IDs ---")
    print(combined_df.to_string())

    # 7. Save outputs and post to webhook
    data_handler.save_outputs(combined_df, validated_data)
    data_handler.post_to_webhook(validated_data)

    print("\n--- Process Finished Successfully ---")


if __name__ == "__main__":
    run_process()
