import logging
import pandas as pd
from datetime import date
from pydantic import ValidationError

from src import parsers, utils, settings
from src.pipeline import DataPipeline
from src.schemas import InventoryItem, ExtractResult

logger = logging.getLogger(__name__)


class InventoryPipeline(DataPipeline):
    def __init__(self, test_mode: bool = False):
        super().__init__("inventory", test_mode=test_mode)
        self.system_date = date.today()

        # Standardized registry: every entry uses the same keys
        #   channel   — display name / status key
        #   parser    — callable returning ParseResult
        #   files     — dict of file_key → filename prefix
        self.PARSER_REGISTRY = [
            {
                "channel": "FBA",
                "parser": parsers.parse_fba_report,
                "files": {"primary": settings.FBA_FILENAME_PREFIX},
            },
            {
                "channel": "Flexport",
                "parser": parsers.parse_flexport_reports,
                "files": {
                    "levels": settings.FLEXPORT_LEVELS_FILENAME_PREFIX,
                    "orders": settings.FLEXPORT_ORDERS_FILENAME_PREFIX,
                    "inbound": settings.FLEXPORT_INBOUND_FILENAME_PREFIX,
                },
            },
            {
                "channel": "AWD",
                "parser": parsers.parse_awd_report,
                "files": {"primary": settings.AWD_FILENAME_PREFIX},
            },
            {
                "channel": "WFS",
                "parser": parsers.parse_wfs_report,
                "files": {
                    "sales": settings.WALMART_SALES_FILENAME_PREFIX,
                    "inventory": settings.WFS_INVENTORY_FILENAME_PREFIX,
                },
            },
            {
                "channel": "FBT",
                "parser": parsers.parse_fbt_report,
                "files": {
                    "sales": settings.TIKTOK_ORDERS_PREFIX,
                    "inventory": settings.FBT_INVENTORY_FILENAME_PREFIX,
                },
            },
        ]

    def extract(self) -> ExtractResult:
        logger.info("--- Starting Inventory Report Process ---")

        dataframes = []

        for registry_entry in self.PARSER_REGISTRY:
            source_name = registry_entry["channel"]
            logger.info(f"\n-- Processing Source: {source_name} --")

            file_paths = {}
            report_dates = {}
            all_files_found = True

            for file_key, prefix in registry_entry["files"].items():
                found_file_info = utils.find_latest_report(settings.INPUT_DIR, prefix)

                if found_file_info:
                    path, report_date = found_file_info
                    file_paths[file_key] = path
                    report_dates[file_key] = report_date
                    logger.info(f"  > Found '{file_key}': {path.name} ({report_date})")
                else:
                    if file_key == "inbound" and source_name == "Flexport":
                        logger.info(
                            f"  > INFO: Optional '{file_key}' missing. Continuing."
                        )
                        file_paths[file_key] = None
                    else:
                        logger.error(f"  > ERROR: Required '{file_key}' missing.")
                        all_files_found = False
                        break

            if not all_files_found:
                logger.warning(f"Skipping source '{source_name}' due to missing files.")
                if source_name == "Flexport":
                    self.status_summary["DTC"] = None
                    self.status_summary["Reserve"] = None
                else:
                    self.status_summary[source_name] = None
                continue

            # Run Parser — returns ParseResult
            parse_result = registry_entry["parser"](file_paths)

            if parse_result.df is not None and not parse_result.df.empty:
                df = parse_result.df

                primary_date = (
                    report_dates.get("levels")
                    or report_dates.get("inventory")
                    or report_dates.get("primary")
                )
                df["Date"] = primary_date
                dataframes.append(df)

                for ch in df["Channel"].unique():
                    self.status_summary[ch] = primary_date

                # Stats logging — mirrors the visibility already in SalesPipeline
                logger.info(f"  > 📊 Stats for {source_name}:")
                logger.info(f"    - Rows Analyzed: {parse_result.raw_count}")
                logger.info(f"    - SKU-Channel Rows Found: {len(df)}")

                present_skus = set(df["SKU"].astype(str).unique())
                master_skus_set = set(str(s) for s in settings.SKU_ORDER)
                missing = master_skus_set - present_skus
                if missing:
                    logger.warning(
                        f"    - ⚠️  Missing SKUs ({len(missing)}): {', '.join(sorted(missing))}"
                    )
            else:
                logger.warning(f"  > ⚠️  No data processed for '{source_name}'.")
                if source_name == "Flexport":
                    self.status_summary["DTC"] = None
                    self.status_summary["Reserve"] = None
                else:
                    self.status_summary[source_name] = None

        if not dataframes:
            return ExtractResult(df=None)

        logger.info("\nConcatenating reports...")
        combined = pd.concat(dataframes, ignore_index=True)
        return ExtractResult(df=combined)

    def transform(self, df: pd.DataFrame, bundle_rows: list[dict]) -> list[InventoryItem] | None:  # noqa: ARG002
        logger.info("\n--- Normalizing Data (Zero-Filling) ---")

        # Force the full configured channel list — same policy as SalesPipeline
        channels_list = settings.CHANNEL_ORDER
        master_skus = [str(x) for x in settings.SKU_ORDER]

        # Get map of Channel -> Date from existing data
        channel_dates = df.groupby("Channel")["Date"].first().to_dict()

        # Generate template: every channel × every SKU
        template_rows = []
        for ch in channels_list:
            date_for_channel = channel_dates.get(ch, date.today())
            for sku in master_skus:
                template_rows.append({"Channel": ch, "SKU": sku, "Date": date_for_channel})

        template_df = pd.DataFrame(template_rows)

        # Merge actual data into template
        merged_df = pd.merge(template_df, df, on=["Channel", "SKU", "Date"], how="left")

        # Fill NaNs with 0 for metrics
        merged_df["Units"] = merged_df["Units"].fillna(0)
        merged_df["Inventory"] = merged_df["Inventory"].fillna(0)
        merged_df["Inbound"] = merged_df["Inbound"].fillna(0)

        df = merged_df

        # --- Generate IDs ---
        logger.info("Generating IDs...")
        system_date_str = self.system_date.strftime("%Y%m%d")

        df["id"] = (
            system_date_str
            + "_"
            + df["Channel"].astype(str).str.replace(" ", "_")
            + "_"
            + df["SKU"].astype(str)
        )

        df["sku_channel_id"] = (
            df["Channel"].astype(str).str.replace(" ", "_")
            + "_"
            + df["SKU"].astype(str)
        )

        # --- Filter Columns & Validate ---
        final_columns = [
            field.alias or name for name, field in InventoryItem.model_fields.items()
        ]
        df = df[final_columns]

        try:
            logger.info("Validating data against schema...")
            validated_data = [
                InventoryItem(**{str(k): v for k, v in row.items()}) for row in df.to_dict("records")
            ]
            logger.info("✅ Data validation successful.")
        except ValidationError as e:
            logger.error("❌ Data validation failed!")
            logger.error(e)
            return None

        return validated_data
