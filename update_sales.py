import pandas as pd
from datetime import date
from pydantic import ValidationError

from src import parsers, data_handler, settings, utils
from src.schemas import SalesRecord

SALES_PARSER_REGISTRY = [
    {
        "channel": "Walmart",
        "func": parsers.parse_walmart_sales_report,
        "file": settings.WALMART_SALES_PREFIX,
    },
    {
        "channel": "Amazon",
        "func": parsers.parse_amazon_sales_report,
        "file": settings.AMAZON_SALES_PREFIX,
    },
    {
        "channel": "TikTok Shop",
        "func": parsers.parse_tiktok_sales_report,
        "file": settings.TIKTOK_SALES_PREFIX,
    },
    {
        "channel": "Mixed",
        "func": parsers.parse_shopify_sales_report,
        "file": settings.SHOPIFY_SALES_PREFIX,
    },
]


def run_sales_update():
    print("--- Starting Daily Sales Aggregation (Long Format) ---")

    all_data_frames = []
    bundle_rows = []
    processed_channels = set()

    # We need the system date for the ID generation
    system_date = date.today()
    # system_date_str = system_date.strftime("%Y%m%d")

    # 1. PROCESS SOURCES
    for parser in SALES_PARSER_REGISTRY:
        print(f"\n-- Processing Source: {parser['channel']} --")

        found_info = utils.find_latest_report(settings.INPUT_DIR, parser["file"])
        if not found_info:
            print(f"  > ⚠️  File missing ({parser['file']}). Skipping.")
            continue

        path, file_date = found_info
        print(f"  > Found: {path.name} (File Date: {file_date})")

        # Parse
        df_source, bundle_stats = parser["func"]({"primary": path})

        if df_source is not None and not df_source.empty:
            # Add the File Date to the DataFrame
            df_source["Date"] = file_date

            # A. Handle Standard Parsers
            if parser["channel"] != "Mixed":
                df_source["Channel"] = parser["channel"]
                all_data_frames.append(df_source)
                processed_channels.add(parser["channel"])

                # Bundle Row for this channel (Using File Date)
                bundle_rows.append(
                    {
                        "SKU": "Bundles",
                        "Channel": parser["channel"],
                        "Date": file_date,
                        "Units": bundle_stats["Units"],
                        "Revenue": bundle_stats["Revenue"],
                    }
                )

            # B. Handle Shopify (Mixed)
            else:
                all_data_frames.append(df_source)
                unique_chans = df_source["Channel"].unique()
                processed_channels.update(unique_chans)

                for bucket, stats in bundle_stats.items():
                    if bucket in unique_chans or stats["Units"] > 0:
                        processed_channels.add(bucket)
                        bundle_rows.append(
                            {
                                "SKU": "Bundles",
                                "Channel": bucket,
                                "Date": file_date,
                                "Units": stats["Units"],
                                "Revenue": stats["Revenue"],
                            }
                        )

            print(f"  > Processed {len(df_source)} SKU records.")

    if not all_data_frames:
        print("❌ No data found from any source.")
        return

    # 2. CONCATENATE
    full_df = pd.concat(all_data_frames, ignore_index=True)

    # 3. NORMALIZE (Zero Filling)
    print("\n--- Normalizing Data (Zero-Filling) ---")

    master_skus = [str(x) for x in settings.SKU_ORDER]
    channels_list = list(processed_channels)

    # We cannot simply cross-join everything because different channels might have different Dates.
    # Strategy: For every Channel found, we assume the Date found in `full_df` for that channel
    # is the target date.

    # Get map of Channel -> Date from existing data
    channel_dates = full_df.groupby("Channel")["Date"].first().to_dict()

    # Generate template: Channel + SKU -> Date
    template_rows = []
    for ch in channels_list:
        # Default to system date if channel somehow missing from map (fallback)
        d = channel_dates.get(ch, system_date)
        for sku in master_skus:
            template_rows.append({"Channel": ch, "SKU": sku, "Date": d})

    template_df = pd.DataFrame(template_rows)

    # Merge actual data into template
    merged_df = pd.merge(
        template_df, full_df, on=["SKU", "Channel", "Date"], how="left"
    )
    merged_df = merged_df.fillna(0)

    # 4. ADD BUNDLES
    if bundle_rows:
        bundle_df = pd.DataFrame(bundle_rows)
        final_df = pd.concat([bundle_df, merged_df], ignore_index=True)
    else:
        final_df = merged_df

    # 5. FORMATTING & IDs
    print("--- Generating IDs and Formatting ---")

    # Types
    final_df["Units"] = final_df["Units"].astype(int)
    final_df["Revenue"] = final_df["Revenue"].round(2)

    # ID Generation: YYYYMMDD_Channel_SKU
    # We use %Y%m%d (e.g. 20251219) for the compact ID
    final_df["id"] = (
        system_date.strftime("%Y%m%d")
        + "_"
        + final_df["Channel"].astype(str)
        + "_"
        + final_df["SKU"].astype(str)
    )

    # sku_channel_id: Channel_SKU
    final_df["sku_channel_id"] = (
        final_df["Channel"].astype(str) + "_" + final_df["SKU"].astype(str)
    )

    # Final Column Order
    target_cols = ["id", "sku_channel_id", "Date", "SKU", "Channel", "Units", "Revenue"]
    final_df = final_df[target_cols]

    # 6. VALIDATION
    try:
        print("Validating data against schema...")
        validated_data = [SalesRecord(**row) for row in final_df.to_dict("records")]
        print(f"✅ Data validation successful ({len(validated_data)} records).")
    except ValidationError as e:
        print("❌ Data validation failed!")
        print(e)
        return

    # 7. OUTPUTS & WEBHOOK
    print("\n--- Saving and Sending ---")

    # Save to File (CSV/JSON)
    # The filename will use standard YYYY-MM-DD format
    data_handler.save_outputs(validated_data, "sales_report")

    # Post to Webhook
    data_handler.post_to_webhook(
        validated_data=validated_data,
        metadata={"status": "Sales Updated", "count": len(validated_data)},
        report_type="sales",
    )

    print("\n--- Process Finished Successfully ---")


if __name__ == "__main__":
    run_sales_update()
