import logging
import pandas as pd
from datetime import date
from pydantic import ValidationError

from src import parsers, utils, settings
from src.pipeline import DataPipeline
from src.schemas import InventoryItem

logger = logging.getLogger(__name__)


class InventoryPipeline(DataPipeline):
    def __init__(self):
        super().__init__("inventory")
        self.system_date = date.today()

        # Inventory-specific Parsers
        self.PARSER_REGISTRY = [
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

    def extract(self) -> pd.DataFrame | None:
        logger.info("--- Starting Inventory Report Process ---")

        dataframes = []
        
        for parser_config in self.PARSER_REGISTRY:
            source_name = parser_config["channel_name"]
            logger.info(f"\n-- Processing Source: {source_name} --")

            file_paths = {}
            report_dates = {}
            all_files_found = True

            for file_key, prefix in parser_config["required_files"].items():
                found_file_info = utils.find_latest_report(settings.INPUT_DIR, prefix)

                if found_file_info:
                    path, report_date = found_file_info
                    file_paths[file_key] = path
                    report_dates[file_key] = report_date
                    logger.info(f"  > Found '{file_key}': {path.name} ({report_date})")
                else:
                    if file_key == "inbound" and source_name == "Flexport":
                        logger.info(f"  > INFO: Optional '{file_key}' missing. Continuing.")
                        file_paths[file_key] = None
                    else:
                        logger.error(f"  > ERROR: Required '{file_key}' missing.")
                        all_files_found = False
                        break

            if not all_files_found:
                logger.warning(f"Skipping source '{source_name}' due to missing files.")
                # Populate summary with None
                if source_name == "Flexport":
                    self.status_summary["DTC"] = None
                    self.status_summary["Reserve"] = None
                else:
                    self.status_summary[source_name] = None
                continue

            # Run Parser
            df = parser_config["parser_func"](file_paths)
            if df is not None and not df.empty:
                # --- 1. Assign Date Column ---
                primary_date = (
                    report_dates.get("levels")
                    or report_dates.get("inventory")
                    or report_dates.get("primary")
                )
                df["Date"] = primary_date
                dataframes.append(df)

                for ch in df["channel"].unique():
                    self.status_summary[ch] = primary_date

        if not dataframes:
            return None

        # --- Combine ---
        logger.info("\nConcatenating reports...")
        return pd.concat(dataframes, ignore_index=True)

    def transform(self, combined_df: pd.DataFrame) -> list[InventoryItem] | None:
        logger.info("\n--- Normalizing Data (Zero-Filling) ---")
        
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
        
        combined_df = merged_df

        # --- 2. Generate IDs (New Structure) ---
        logger.info("Generating IDs...")
        system_date_str = self.system_date.strftime("%Y%m%d")

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
            logger.info("Validating data against schema...")
            validated_data = [
                InventoryItem(**row) for row in combined_df.to_dict("records")
            ]
            logger.info("✅ Data validation successful.")
        except ValidationError as e:
            logger.error("❌ Data validation failed!")
            logger.error(e)
            return None

        # --- 4. Sort (Optional, for presentation consistency) ---
        # Note: The DataPipeline.load method will save this data. 
        # Sorting there or here is fine, but lets do it here to ensure saved CSV is clean.
        # However, listing comprehension loses dataframe structure.
        # It's better to trust the saved order or standard CSV opening.
        # But let's stick to the previous behavior of saving what we validated.
        return validated_data
