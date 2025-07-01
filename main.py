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
        "parser_func": parsers.parse_fba_report,
        "filename_prefix": settings.FBA_FILENAME_PREFIX,
    },
    {
        "parser_func": parsers.parse_dtc_report,
        "filename_prefix": settings.DTC_FILENAME_PREFIX,
    },
    {
        "parser_func": parsers.parse_awd_report,
        "filename_prefix": settings.AWD_FILENAME_PREFIX,
    },
    {
        "parser_func": parsers.parse_wfs_report,
        "filename_prefix": settings.WFS_FILENAME_PREFIX,
    },
]


def run_process():
    """Main orchestration function to run the entire reporting process."""
    print("--- Starting Daily Inventory Report Process ---")
    today_str = utils.get_date_str_for_filename()
    print(f"Processing reports for date string: '{today_str}'")

    # 1. Loop through the registry, parse all reports into a list of DataFrames
    dataframes = []
    for parser_config in PARSER_REGISTRY:
        filename = f"{parser_config['filename_prefix']}{today_str}.csv"
        file_path = settings.INPUT_DIR / filename
        df = parser_config["parser_func"](file_path)
        if df is not None:
            dataframes.append(df)

    if not dataframes:
        print("❌ No data found for today. Aborting process.")
        return

    # 2. Combine all parsed dataframes by stacking them. This is the new, simpler "merge".
    print(f"Found {len(dataframes)} reports. Concatenating...")
    combined_df = pd.concat(dataframes, ignore_index=True)

    # 3. Add metadata and ensure the structure matches our Pydantic schema
    combined_df["last_updated"] = date.today()

    final_columns = list(InventoryItem.model_fields.keys())
    combined_df = combined_df.reindex(columns=final_columns).fillna(0)

    # 4. Validate data against our Pydantic schema for ultimate data integrity
    try:
        print("Validating data against schema...")
        validated_data = [
            InventoryItem(**row) for row in combined_df.to_dict("records")
        ]
        print("✅ Data validation successful.")
    except ValidationError as e:
        print("❌ Data validation failed! The data does not match the required schema.")
        print(e)
        return

    # 5. Final Sorting
    combined_df["channel"] = pd.Categorical(
        combined_df["channel"], categories=settings.CHANNEL_ORDER, ordered=True
    )
    combined_df["sku"] = pd.Categorical(
        combined_df["sku"], categories=settings.SKU_ORDER, ordered=True
    )

    # The sort order in the list determines priority: first by channel, then by SKU.
    combined_df = combined_df.sort_values(["channel", "sku"]).reset_index(drop=True)

    print("\n--- Final Normalized Report ---")
    print(combined_df.to_string())

    # 6. Save outputs and post to webhook
    data_handler.save_outputs(combined_df, validated_data)
    data_handler.post_to_webhook(validated_data)

    print("\n--- Process Finished Successfully ---")


if __name__ == "__main__":
    run_process()
