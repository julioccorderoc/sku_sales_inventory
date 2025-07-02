import pandas as pd
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
    """
    Main orchestration function with resilient file finding and status summary.
    It now centrally manages the 'last_updated' date for each record.
    """
    print("--- Starting Daily Inventory Report Process ---")

    dataframes = []
    status_summary = {}  # To hold the final status summary for the webhook

    for parser_config in PARSER_REGISTRY:
        source_name = parser_config["channel_name"]
        print(f"\n-- Processing Source: {source_name} --")

        file_paths = {}
        report_dates = {}  # To temporarily store the date of each file
        all_files_found = True

        for file_key, prefix in parser_config["required_files"].items():
            found_file_info = utils.find_latest_report(settings.INPUT_DIR, prefix)

            if found_file_info:
                path, report_date = found_file_info
                file_paths[file_key] = path
                report_dates[file_key] = report_date
                print(
                    f"  > Found '{file_key}' report: {path.name} (Date: {report_date})"
                )
            else:
                # Handle optional files gracefully (like Flexport Inbound)
                if file_key == "inbound" and source_name == "Flexport":
                    print(
                        f"  > INFO: Optional file not found for '{file_key}' with prefix '{prefix}'. Continuing."
                    )
                    file_paths[file_key] = None
                else:
                    print(
                        f"  > ERROR: Required file not found for '{file_key}' with prefix '{prefix}'"
                    )
                    all_files_found = False
                    break

        if not all_files_found:
            print(f"Skipping source '{source_name}' due to missing required files.")
            # We will populate the status summary with null for the channels this parser would have created.
            if source_name == "Flexport":
                status_summary["DTC"] = None
                status_summary["Reserve"] = None
            else:
                status_summary[source_name] = None
            continue

        # Run the parser. It returns a dataframe WITHOUT the 'last_updated' column.
        df = parser_config["parser_func"](file_paths)
        if df is not None and not df.empty:
            # --- CENTRALLY ASSIGN 'last_updated' DATE ---
            # Determine the primary date for this batch of data.
            # Use the 'inventory' date for multi-file sources, otherwise 'primary'.
            primary_date = report_dates.get("inventory") or report_dates.get("primary")
            df["last_updated"] = primary_date

            dataframes.append(df)

            # --- Populate the status summary ---
            # The date is the same for all channels produced by this parser run.
            for ch in df["channel"].unique():
                status_summary[ch] = primary_date

    if not dataframes:
        print("\n❌ No dataframes were successfully parsed. Aborting process.")
        # We can still send the summary to notify that nothing was updated.
        data_handler.post_to_webhook([], status_summary)
        return

    # --- Combine, Validate, and Sort ---
    print("\nConcatenating reports...")
    combined_df = pd.concat(dataframes, ignore_index=True)

    # Generate the unique ID
    print("Generating unique IDs for each record...")
    combined_df["id"] = (
        combined_df["channel"].astype(str) + "-" + combined_df["sku"].astype(str)
    )

    # Reorder columns to match Pydantic model for validation
    final_columns = list(InventoryItem.model_fields.keys())
    combined_df = combined_df.reindex(columns=final_columns)

    # Validate data
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

    # Final Sorting
    combined_df["channel"] = pd.Categorical(
        combined_df["channel"], categories=settings.CHANNEL_ORDER, ordered=True
    )
    combined_df["sku"] = pd.Categorical(
        combined_df["sku"], categories=settings.SKU_ORDER, ordered=True
    )
    combined_df = combined_df.sort_values(["channel", "sku"]).reset_index(drop=True)
    combined_df = combined_df[final_columns]

    print("\n--- Final Normalized Report ---")
    print(combined_df.to_string())

    # --- Save and Post Final Payload ---
    print("\n--- Final Status Summary ---")
    # Ensure all channels are in the summary, even if their parser failed.
    for ch in settings.CHANNEL_ORDER:
        if ch not in status_summary:
            status_summary[ch] = None

    for channel, date in status_summary.items():
        print(f"{channel}: {date.isoformat() if date else 'No data'}")

    data_handler.save_outputs(combined_df, validated_data)
    data_handler.post_to_webhook(validated_data, status_summary)

    print("\n--- Process Finished Successfully ---")


if __name__ == "__main__":
    run_process()
