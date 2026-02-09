import argparse
import time
from datetime import datetime

from src.logger import setup_logger
from src.pipelines.inventory import InventoryPipeline
from src.pipelines.sales import SalesPipeline

# Set up global logger
logger = setup_logger()


def run_master_pipeline():
    parser = argparse.ArgumentParser(description="Run Sales and Inventory Pipeline")
    parser.add_argument(
        "--test", "-t", action="store_true", help="Run in test mode (no webhook)"
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
