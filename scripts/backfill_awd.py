"""One-off recovery: backfill AWD inventory for a single report date.

Used when the main pipeline skipped AWD (e.g. case-mismatched prefix bug
on 2026-05-28). Produces an AWD-only payload — 32 SKUs x 1 channel —
and optionally POSTs to the same webhook the main pipeline uses.

Usage:
    uv run python scripts/backfill_awd.py --report-date 2026-05-27 --system-date 2026-05-28 --test
    uv run python scripts/backfill_awd.py --report-date 2026-05-27 --system-date 2026-05-28
"""
import argparse
import sys
from datetime import date
from pathlib import Path

from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import data_handler, parsers, settings  # noqa: E402
from src.logger import setup_logger  # noqa: E402
from src.schemas import InventoryItem  # noqa: E402


logger = setup_logger()


def find_awd_file(report_date: date) -> Path:
    """Locate the AWD file for a date, accepting either casing of 'Report'."""
    target = report_date.isoformat()
    for prefix in ("AWD_Report_", "AWD_report_"):
        candidate = settings.INPUT_DIR / f"{prefix}{target}.csv"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"No AWD file found for {target} in {settings.INPUT_DIR}")


def main() -> int:
    arg_parser = argparse.ArgumentParser(description="Backfill AWD inventory for one date")
    arg_parser.add_argument("--report-date", required=True, help="Date on the AWD file (YYYY-MM-DD)")
    arg_parser.add_argument(
        "--system-date",
        required=True,
        help="System date for ID prefix — use the date the original run executed so IDs match the batch",
    )
    arg_parser.add_argument("--test", action="store_true", help="Skip webhook POST (still writes local CSV/JSON)")
    args = arg_parser.parse_args()

    report_date = date.fromisoformat(args.report_date)
    system_date = date.fromisoformat(args.system_date)

    awd_file = find_awd_file(report_date)
    logger.info(f"📄 AWD source file: {awd_file.name}")

    parse_result = parsers.parse_awd_report({"primary": awd_file})
    if parse_result.df is None or parse_result.df.empty:
        logger.error("❌ Parser returned no data — aborting.")
        return 1

    df = parse_result.df.copy()
    df["Date"] = report_date

    system_date_str = system_date.strftime("%Y%m%d")
    df["id"] = (
        system_date_str + "_" + df["Channel"].astype(str).str.replace(" ", "_") + "_" + df["SKU"].astype(str)
    )
    df["sku_channel_id"] = df["Channel"].astype(str).str.replace(" ", "_") + "_" + df["SKU"].astype(str)

    for col in ("Units", "Inventory", "Inbound"):
        df[col] = df[col].fillna(0).clip(lower=0).astype(int)

    final_columns = [field.alias or name for name, field in InventoryItem.model_fields.items()]
    df = df[final_columns]

    try:
        validated = [InventoryItem(**{str(k): v for k, v in row.items()}) for row in df.to_dict("records")]
    except ValidationError as e:
        logger.error("❌ Validation failed:")
        logger.error(e)
        return 1

    logger.info(f"✅ Validated {len(validated)} AWD rows for report date {report_date}")
    logger.info(f"   ID prefix: {system_date_str}_AWD_<SKU>")

    total_inventory = sum(item.inventory for item in validated)
    total_inbound = sum(item.inbound for item in validated)
    logger.info(f"   Totals — Inventory: {total_inventory}, Inbound: {total_inbound}")

    data_handler.save_outputs(validated, f"inventory_awd_backfill_{report_date.isoformat()}")

    if args.test:
        logger.info("🧪 Test mode: skipping webhook POST.")
        return 0

    metadata = {ch: None for ch in settings.CHANNEL_ORDER}
    metadata["AWD"] = report_date

    data_handler.post_to_webhook(
        validated_data=validated,
        metadata=metadata,
        report_type="inventory",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
