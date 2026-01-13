import time
from datetime import datetime

from update_inventory import run_inventory_update as run_inventory_job
from update_sales import run_sales_update as run_sales_job


def run_master_pipeline():
    start_time = time.time()
    print(
        f"🚀 STARTING MASTER PIPELINE | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    print("=" * 60)

    # --- STEP 1: INVENTORY UPDATE ---
    print("\n📦 STEP 1/2: INVENTORY REPORT")
    print("-" * 30)
    try:
        run_inventory_job()
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR in Inventory Process: {e}")

    print("\n" + "=" * 60)

    # --- STEP 2: SALES AGGREGATION ---
    print("\n💰 STEP 2/2: SALES REPORT")
    print("-" * 30)
    try:
        run_sales_job()
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR in Sales Process: {e}")

    # --- SUMMARY ---
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"✅ MASTER PIPELINE FINISHED in {elapsed:.2f} seconds.")
    print("=" * 60)


if __name__ == "__main__":
    run_master_pipeline()
