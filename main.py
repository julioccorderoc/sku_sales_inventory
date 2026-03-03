import argparse
import glob
import re
import time
from datetime import datetime

import pandas as pd

from src import settings
from src.logger import setup_logger
from src.pipelines.inventory import InventoryPipeline
from src.pipelines.sales import SalesPipeline

# Set up global logger
logger = setup_logger()


def run_combine_inventory():
    """Combine all historical inventory_report_*.csv files in OUTPUT_DIR into a single CSV."""
    pattern = str(settings.OUTPUT_DIR / "inventory_report_*.csv")
    files = glob.glob(pattern)
    date_pattern = re.compile(r"inventory_report_(\d{4}-\d{2}-\d{2})\.csv")

    logger.info(f"📦 Found {len(files)} inventory report file(s) to combine.")

    df_list = []
    for file in files:
        match = date_pattern.search(file)
        if not match:
            continue

        report_date_str = match.group(1)

        try:
            df = pd.read_csv(file)
        except Exception as e:
            logger.error(f"❌ Error reading {file}: {e}")
            continue

        # Normalize headers — handles legacy column names from older report formats
        rename_map = {
            "Units Sold": "Units",
            "Last Updated": "Date",
            "units_sold": "Units",
            "last_updated": "Date",
            "inventory": "Inventory",
            "inbound": "Inbound",
            "sku": "SKU",
            "channel": "Channel",
            "id": "old_id",  # Rename old ID to avoid conflicts during ID regeneration
        }
        df = df.rename(columns=rename_map)

        # Always use the filename date as the source of truth
        df["Date"] = report_date_str

        required_cols = ["SKU", "Channel", "Units", "Inventory", "Inbound"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            logger.warning(f"⚠️  Skipping {file} — Missing columns: {missing}")
            continue

        # Regenerate IDs to match current schema
        date_id_part = report_date_str.replace("-", "")
        df["id"] = (
            date_id_part + "_" + df["Channel"].astype(str) + "_" + df["SKU"].astype(str)
        )
        df["sku_channel_id"] = df["Channel"].astype(str) + "_" + df["SKU"].astype(str)

        for col in ["Units", "Inventory", "Inbound"]:
            df[col] = df[col].fillna(0).astype(int)

        final_cols = ["id", "sku_channel_id", "Date", "SKU", "Channel", "Units", "Inventory", "Inbound"]
        df = df[final_cols]
        df_list.append(df)

    if not df_list:
        logger.warning("⚠️  No matching inventory_report CSV files found.")
        return

    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df = combined_df.sort_values(by=["Date", "Channel", "SKU"])

    output_file = settings.OUTPUT_DIR / f"{settings.COMBINED_FILENAME_BASE}_report.csv"
    combined_df.to_csv(output_file, index=False)

    logger.info(f"✅ Combined report saved to: {output_file}")
    logger.info(f"📊 Total records: {len(combined_df)}")


def run_master_pipeline():
    parser = argparse.ArgumentParser(description="Run Sales and Inventory Pipeline")
    parser.add_argument(
        "--test", "-t", action="store_true", help="Run in test mode (no webhook)"
    )
    parser.add_argument(
        "--combine", "-c", action="store_true",
        help="Combine historical inventory reports into a single file",
    )
    args = parser.parse_args()

    test_mode = args.test

    start_time = time.time()
    logger.info(
        f"🚀 STARTING MASTER PIPELINE | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    if test_mode:
        logger.info("🧪 RUNNING IN TEST MODE")
    logger.info("=" * 60)

    if args.combine:
        try:
            run_combine_inventory()
        except Exception as e:
            logger.error(f"\n❌ CRITICAL ERROR in Combine Inventory: {e}", exc_info=True)
    else:
        # --- STEP 1: INVENTORY UPDATE ---
        try:
            inventory_job = InventoryPipeline(test_mode=test_mode)
            inventory_job.run()
        except Exception as e:
            logger.error(f"\n❌ CRITICAL ERROR in Inventory Process: {e}", exc_info=True)

        logger.info("\n" + "=" * 60)

        # --- STEP 2: SALES AGGREGATION ---
        try:
            sales_job = SalesPipeline(test_mode=test_mode)
            sales_job.run()
        except Exception as e:
            logger.error(f"\n❌ CRITICAL ERROR in Sales Process: {e}", exc_info=True)

    # --- SUMMARY ---
    elapsed = time.time() - start_time
    logger.info("\n" + "=" * 60)
    logger.info(f"✅ MASTER PIPELINE FINISHED in {elapsed:.2f} seconds.")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_master_pipeline()
