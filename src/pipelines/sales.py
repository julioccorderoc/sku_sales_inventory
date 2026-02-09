import logging
import pandas as pd
from datetime import date
from pydantic import ValidationError
from typing import cast

from src import parsers, utils, settings
from src.pipeline import DataPipeline
from src.schemas import SalesRecord

logger = logging.getLogger(__name__)


class SalesPipeline(DataPipeline):
    def __init__(self, test_mode: bool = False):
        # Initialize with empty channels; we will discover them dynamically
        super().__init__("sales", channels=[], test_mode=test_mode)
        self.system_date = date.today()

        # Sales-specific Parsers
        self.PARSER_REGISTRY = [
            {
                "channel": "Walmart",
                "file": settings.WALMART_SALES_PREFIX,
                "func": parsers.parse_walmart_sales_report,
            },
            {
                "channel": "Amazon",
                "file": settings.AMAZON_SALES_PREFIX,
                "func": parsers.parse_amazon_sales_report,
            },
            {
                "channel": "TikTok Shop",
                "file": settings.TIKTOK_SALES_PREFIX,
                "func": parsers.parse_tiktok_sales_report,
            },
            {
                "channel": "Mixed",
                "file": settings.SHOPIFY_SALES_PREFIX,
                "func": parsers.parse_shopify_sales_report,
            },
        ]

    def extract(self) -> pd.DataFrame | None:
        logger.info("--- Starting Sales Aggregation ---")

        all_data_frames = []
        self.bundle_rows = []  # Need to store this for transformation
        self.processed_channels = set()

        # Store raw counts and processed counts for reporting (optional, can log immediately)

        for parser in self.PARSER_REGISTRY:
            logger.info(f"\n-- Processing Source: {parser['channel']} --")

            found_info = utils.find_latest_report(settings.INPUT_DIR, parser["file"])
            if not found_info:
                logger.warning(f"  > ⚠️  File missing ({parser['file']}). Skipping.")
                continue

            path, file_date = found_info
            logger.info(f"  > Found: {path.name} (File Date: {file_date})")

            # Parse
            df_source, bundle_stats, raw_count = parser["func"]({"primary": path})

            if df_source is not None and not df_source.empty:
                # Add the File Date to the DataFrame
                df_source["Date"] = file_date

                # A. Handle Standard Parsers
                if parser["channel"] != "Mixed":
                    df_source["Channel"] = parser["channel"]
                    all_data_frames.append(df_source)
                    self.processed_channels.add(parser["channel"])

                    # Update Status Summary
                    cast(dict, self.status_summary)[parser["channel"]] = file_date

                    # Bundle Row for this channel (Using File Date)
                    self.bundle_rows.append(
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
                    self.processed_channels.update(unique_chans)

                    # Update Status Summary for all found channels
                    for ch in unique_chans:
                        cast(dict, self.status_summary)[ch] = file_date

                    for bucket, stats in bundle_stats.items():
                        if bucket in unique_chans or stats["Units"] > 0:
                            self.processed_channels.add(
                                bucket
                            )  # Ensure implicit channels are added
                            self.bundle_rows.append(
                                {
                                    "SKU": "Bundles",
                                    "Channel": bucket,
                                    "Date": file_date,
                                    "Units": stats["Units"],
                                    "Revenue": stats["Revenue"],
                                }
                            )

                logger.info(f"  > 📊 Stats for {parser['channel']}:")
                logger.info(f"    - Rows Analyzed: {raw_count}")
                logger.info(f"    - Individual SKUs Found: {len(df_source)}")

                # Check for missing SKUs
                present_skus = set(df_source["SKU"].astype(str).unique())
                master_skus = set(settings.SKU_ORDER)
                missing = master_skus - present_skus
                if missing:
                    # For Mixed, this alert is less useful per-SKU, but we keep it for now
                    msg = (
                        f"    - ⚠️  Missing SKUs ({len(missing)}): {', '.join(missing)}"
                    )
                    logger.warning(msg)

            else:
                logger.warning(f"  > ⚠️  No data processed from {parser['file']}.")

        if not all_data_frames:
            return None

        # Update channels list for the status summary
        self.channels = sorted(list(self.processed_channels))

        # Concatenate
        return pd.concat(all_data_frames, ignore_index=True)

    def transform(self, df: pd.DataFrame) -> list[SalesRecord] | None:
        logger.info("\n--- Normalizing Data (Zero-Filling) ---")

        master_skus = [str(x) for x in settings.SKU_ORDER]
        # FORCE: Always utilize the full list of configured sales channels
        channels_list = settings.SALES_CHANNEL_ORDER

        # Get map of Channel -> Date from existing data
        channel_dates = df.groupby("Channel")["Date"].first().to_dict()

        # Generate template: Channel + SKU -> Date
        template_rows = []
        for ch in channels_list:
            # Default to system date if channel missing from map (fallback for zero-filled channels)
            d = channel_dates.get(ch, self.system_date)
            for sku in master_skus:
                template_rows.append({"Channel": ch, "SKU": sku, "Date": d})

        template_df = pd.DataFrame(template_rows)

        # Merge actual data into template
        merged_df = pd.merge(template_df, df, on=["SKU", "Channel", "Date"], how="left")
        merged_df = merged_df.fillna(0)

        # 4. ADD BUNDLES (For ALL Channels)
        # Create a DataFrame from any existing bundle rows
        if self.bundle_rows:
            existing_bundles_df = pd.DataFrame(self.bundle_rows)
        else:
            existing_bundles_df = pd.DataFrame(
                columns=["SKU", "Channel", "Date", "Units", "Revenue"]
            )

        # Create a Template for Bundles to ensure every channel has a Bundle row
        bundle_template_rows = []
        for ch in channels_list:
            d = channel_dates.get(ch, self.system_date)
            bundle_template_rows.append({"SKU": "Bundles", "Channel": ch, "Date": d})

        bundle_template_df = pd.DataFrame(bundle_template_rows)

        # Merge existing bundles into the bundle template
        # Note: Merging on ["SKU", "Channel", "Date"] might miss if dates differ slightly,
        # but here we derived 'd' from the same source, so it should match.
        # If the channel was missing, 'd' is system_date, and existing_bundles_df won't have it, so it will fillna(0).
        final_bundles_df = pd.merge(
            bundle_template_df,
            existing_bundles_df,
            on=["SKU", "Channel", "Date"],
            how="left",
        ).fillna(0)

        # Concatenate Bundles + SKUs
        final_df = pd.concat([final_bundles_df, merged_df], ignore_index=True)

        # 5. FORMATTING & IDs
        logger.info("--- Generating IDs and Formatting ---")

        # Types
        final_df["Units"] = final_df["Units"].astype(int)
        final_df["Revenue"] = final_df["Revenue"].round(2)

        # ID Generation: YYYYMMDD_Channel_SKU
        system_date_str = self.system_date.strftime("%Y%m%d")

        # FIX: Replace spaces with underscores in Channel name
        final_df["id"] = (
            system_date_str
            + "_"
            + final_df["Channel"].astype(str).str.replace(" ", "_")
            + "_"
            + final_df["SKU"].astype(str)
        )

        # sku_channel_id: Channel_SKU
        final_df["sku_channel_id"] = (
            final_df["Channel"].astype(str).str.replace(" ", "_")
            + "_"
            + final_df["SKU"].astype(str)
        )

        # Final Column Order
        target_cols = [
            "id",
            "sku_channel_id",
            "Date",
            "SKU",
            "Channel",
            "Units",
            "Revenue",
        ]
        final_df = final_df[target_cols]

        # 6. VALIDATION
        try:
            logger.info("Validating data against schema...")
            # Ensure each record is a plain dict with string keys so it can be expanded via **
            records = final_df.to_dict("records")
            normalized_records = [
                {str(k): v for k, v in rec.items()} for rec in records
            ]
            validated_data = [SalesRecord(**row) for row in normalized_records]
            logger.info(
                f"✅ Data validation successful ({len(validated_data)} records)."
            )
            return validated_data
        except ValidationError as e:
            logger.error("❌ Data validation failed!")
            logger.error(e)
            return None
