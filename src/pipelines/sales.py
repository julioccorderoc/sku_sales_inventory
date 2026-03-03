import logging
import pandas as pd
from datetime import date
from pydantic import ValidationError
from typing import cast

from src import parsers, utils, settings
from src.pipeline import DataPipeline
from src.schemas import SalesRecord, ExtractResult

logger = logging.getLogger(__name__)


class SalesPipeline(DataPipeline):
    def __init__(self, test_mode: bool = False):
        super().__init__("sales", channels=[], test_mode=test_mode)
        self.system_date = date.today()

        # Standardized registry: every entry uses the same keys
        #   channel   — display name / status key
        #   parser    — callable returning ParseResult
        #   files     — dict of file_key → filename prefix
        self.PARSER_REGISTRY = [
            {
                "channel": "Walmart",
                "parser": parsers.parse_walmart_sales_report,
                "files": {"primary": settings.WALMART_SALES_PREFIX},
            },
            {
                "channel": "Amazon",
                "parser": parsers.parse_amazon_sales_report,
                "files": {"primary": settings.AMAZON_SALES_PREFIX},
            },
            {
                "channel": "TikTok Shop",
                "parser": parsers.parse_tiktok_sales_report,
                "files": {"primary": settings.TIKTOK_SALES_PREFIX},
            },
            {
                "channel": "Mixed",
                "parser": parsers.parse_shopify_sales_report,
                "files": {"primary": settings.SHOPIFY_SALES_PREFIX},
            },
        ]

    def extract(self) -> ExtractResult:
        logger.info("--- Starting Sales Aggregation ---")

        all_data_frames = []
        bundle_rows: list[dict] = []
        processed_channels: set[str] = set()

        for registry_entry in self.PARSER_REGISTRY:
            source_name = registry_entry["channel"]
            logger.info(f"\n-- Processing Source: {source_name} --")

            found_info = utils.find_latest_report(
                settings.INPUT_DIR, registry_entry["files"]["primary"]
            )
            if not found_info:
                logger.warning(
                    f"  > ⚠️  File missing ({registry_entry['files']['primary']}). Skipping."
                )
                continue

            path, file_date = found_info
            logger.info(f"  > Found: {path.name} (File Date: {file_date})")

            # Parse — returns ParseResult
            parse_result = registry_entry["parser"]({"primary": path})
            df_source = parse_result.df
            bundle_stats = parse_result.bundle_stats
            raw_count = parse_result.raw_count

            if df_source is not None and not df_source.empty:
                df_source["Date"] = file_date

                # A. Handle Standard Parsers
                if source_name != "Mixed":
                    df_source["Channel"] = source_name
                    all_data_frames.append(df_source)
                    processed_channels.add(source_name)

                    cast(dict, self.status_summary)[source_name] = file_date

                    bundle_rows.append(
                        {
                            "SKU": "Bundles",
                            "Channel": source_name,
                            "Date": file_date,
                            "Units": bundle_stats.get("Units", 0),
                            "Revenue": bundle_stats.get("Revenue", 0),
                        }
                    )

                # B. Handle Shopify (Mixed)
                else:
                    all_data_frames.append(df_source)
                    unique_chans = df_source["Channel"].unique()
                    processed_channels.update(unique_chans)

                    for ch in unique_chans:
                        cast(dict, self.status_summary)[ch] = file_date

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

                logger.info(f"  > 📊 Stats for {source_name}:")
                logger.info(f"    - Rows Analyzed: {raw_count}")
                logger.info(f"    - Individual SKUs Found: {len(df_source)}")

                present_skus = set(df_source["SKU"].astype(str).unique())
                master_skus = set(str(s) for s in settings.SKU_ORDER)
                missing = master_skus - present_skus
                if missing:
                    logger.warning(
                        f"    - ⚠️  Missing SKUs ({len(missing)}): {', '.join(sorted(missing))}"
                    )

            else:
                logger.warning(
                    f"  > ⚠️  No data processed from {registry_entry['files']['primary']}."
                )

        if not all_data_frames:
            return ExtractResult(df=None, bundle_rows=[])

        self.channels = sorted(list(processed_channels))

        combined = pd.concat(all_data_frames, ignore_index=True)
        return ExtractResult(df=combined, bundle_rows=bundle_rows)

    def transform(self, df: pd.DataFrame, bundle_rows: list[dict]) -> list[SalesRecord] | None:
        logger.info("\n--- Normalizing Data (Zero-Filling) ---")

        master_skus = [str(x) for x in settings.SKU_ORDER]
        # FORCE: Always use the full list of configured sales channels
        channels_list = settings.SALES_CHANNEL_ORDER

        channel_dates = df.groupby("Channel")["Date"].first().to_dict()

        # Generate template: Channel + SKU -> Date
        template_rows = []
        for ch in channels_list:
            d = channel_dates.get(ch, self.system_date)
            for sku in master_skus:
                template_rows.append({"Channel": ch, "SKU": sku, "Date": d})

        template_df = pd.DataFrame(template_rows)

        merged_df = pd.merge(template_df, df, on=["SKU", "Channel", "Date"], how="left")
        merged_df = merged_df.fillna(0).infer_objects(copy=False)

        # --- ADD BUNDLES (for ALL channels) ---
        if bundle_rows:
            existing_bundles_df = pd.DataFrame(bundle_rows)
            existing_bundles_df["Units"] = existing_bundles_df["Units"].astype(float)
            existing_bundles_df["Revenue"] = existing_bundles_df["Revenue"].astype(float)
        else:
            existing_bundles_df = pd.DataFrame({
                "SKU": pd.Series(dtype=str),
                "Channel": pd.Series(dtype=str),
                "Date": pd.Series(dtype=object),
                "Units": pd.Series(dtype=float),
                "Revenue": pd.Series(dtype=float),
            })

        bundle_template_rows = []
        for ch in channels_list:
            d = channel_dates.get(ch, self.system_date)
            bundle_template_rows.append({"SKU": "Bundles", "Channel": ch, "Date": d})

        bundle_template_df = pd.DataFrame(bundle_template_rows)

        final_bundles_df = pd.merge(
            bundle_template_df,
            existing_bundles_df,
            on=["SKU", "Channel", "Date"],
            how="left",
        ).fillna(0.0)

        final_df = pd.concat([final_bundles_df, merged_df], ignore_index=True)

        # --- Formatting & IDs ---
        logger.info("--- Generating IDs and Formatting ---")

        final_df["Units"] = final_df["Units"].astype(int)
        final_df["Revenue"] = final_df["Revenue"].round(2)

        system_date_str = self.system_date.strftime("%Y%m%d")

        final_df["id"] = (
            system_date_str
            + "_"
            + final_df["Channel"].astype(str).str.replace(" ", "_")
            + "_"
            + final_df["SKU"].astype(str)
        )

        final_df["sku_channel_id"] = (
            final_df["Channel"].astype(str).str.replace(" ", "_")
            + "_"
            + final_df["SKU"].astype(str)
        )

        target_cols = ["id", "sku_channel_id", "Date", "SKU", "Channel", "Units", "Revenue"]
        final_df = final_df[target_cols]

        # --- Validation ---
        try:
            logger.info("Validating data against schema...")
            records = final_df.to_dict("records")
            normalized_records = [{str(k): v for k, v in rec.items()} for rec in records]
            validated_data = [SalesRecord(**row) for row in normalized_records]
            logger.info(
                f"✅ Data validation successful ({len(validated_data)} records)."
            )
            return validated_data
        except ValidationError as e:
            logger.error("❌ Data validation failed!")
            logger.error(e)
            return None
