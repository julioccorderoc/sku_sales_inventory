import csv
import pandas as pd
import requests
import json
import logging
import time
from typing import Any
from datetime import date, datetime

from . import settings

logger = logging.getLogger(__name__)


def save_outputs(validated_data: list[Any], filename_base: str):
    """
    Saves validated data to CSV and JSON.
    Generates the CSV directly from the Pydantic models to ensure column names (aliases) match.
    """
    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Use standard ISO format YYYY-MM-DD for filenames
    date_suffix = date.today().isoformat()

    csv_path = settings.OUTPUT_DIR / f"{filename_base}_{date_suffix}.csv"
    json_path = settings.OUTPUT_DIR / f"{filename_base}_{date_suffix}.json"

    # 1. Convert Pydantic models to a list of dicts (using aliases)
    # mode='json' converts date objects to "YYYY-MM-DD" strings automatically
    data_dicts = [
        item.model_dump(by_alias=True, mode="json") for item in validated_data
    ]

    if not data_dicts:
        logger.warning("⚠️ No data to save.")
        return

    # 2. Save CSV
    df = pd.DataFrame(data_dicts)
    df.to_csv(csv_path, index=False)
    logger.info(f"📁 CSV saved to: {csv_path}")

    # 3. Save JSON (Conditionally)
    if settings.SAVE_JSON_OUTPUT:
        with open(json_path, "w") as f:
            json.dump(data_dicts, f, indent=2)
        logger.info(f"📁 JSON saved to: {json_path}")


def post_to_webhook(
    validated_data: list[Any], metadata: dict[str, Any], report_type: str = "inventory"
):
    """
    Posts data to the webhook with a discriminator field ('reportType').
    """
    if not settings.WEBHOOK_URL:
        logger.warning("⚠️ WEBHOOK_URL not set. Skipping webhook post.")
        return

    logger.info(f"🚀 Posting '{report_type}' data to webhook: {settings.WEBHOOK_URL}")

    # 1. Process Metadata (Handle Dates generically)
    # This preserves your original logic for Inventory dates but allows strings for Sales
    clean_meta = {}
    for k, v in metadata.items():
        if isinstance(v, (date, datetime)):
            clean_meta[k] = v.isoformat()
        else:
            clean_meta[k] = v

    # 2. Construct Payload
    payload = {
        "reportType": report_type,
        "reportSummary": clean_meta,
        "reportData": [
            item.model_dump(mode="json", by_alias=True) for item in validated_data
        ],
    }

    for attempt in range(1, settings.WEBHOOK_MAX_RETRIES + 1):
        try:
            response = requests.post(settings.WEBHOOK_URL, json=payload, timeout=60)
        except requests.exceptions.ConnectionError as e:
            if attempt < settings.WEBHOOK_MAX_RETRIES:
                delay = settings.WEBHOOK_RETRY_BACKOFF ** attempt
                logger.warning(f"⚠️  Connection error on attempt {attempt}/{settings.WEBHOOK_MAX_RETRIES}. Retrying in {delay:.0f}s...")
                time.sleep(delay)
                continue
            logger.error(f"❌ Webhook connection failed after {settings.WEBHOOK_MAX_RETRIES} attempts: {e}")
            return
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error posting to webhook: {e}")
            return

        if response.status_code >= 500:
            if attempt < settings.WEBHOOK_MAX_RETRIES:
                delay = settings.WEBHOOK_RETRY_BACKOFF ** attempt
                logger.warning(f"⚠️  Webhook returned HTTP {response.status_code} on attempt {attempt}/{settings.WEBHOOK_MAX_RETRIES}. Retrying in {delay:.0f}s...")
                time.sleep(delay)
                continue
            logger.error(f"❌ Webhook failed after {settings.WEBHOOK_MAX_RETRIES} attempts. Final status: {response.status_code}")
            return

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ Error posting to webhook: {e}")
            return

        logger.info(f"✅ {report_type.capitalize()} data successfully posted.")
        return


_HISTORY_FIELDNAMES = [
    "timestamp", "pipeline", "report_date",
    "total_records", "total_units", "total_revenue", "source_files",
]


def log_run_history(validated_data: list[Any], pipeline: str, source_files: list[str]):
    """
    Appends a metadata summary row to output/run_history.csv and output/run_history.json
    after each pipeline run. Both files stay in sync:
      - CSV  → Excel-compatible, one appended row per run
      - JSON → programmatic lookups (idempotency checks, anomaly detection)
    """
    if not validated_data:
        logger.warning("⚠️ No data to log to run history.")
        return

    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    report_dates = [item.report_date for item in validated_data if hasattr(item, "report_date")]
    report_date_str = max(report_dates).isoformat() if report_dates else ""

    total_units = sum(getattr(item, "units", 0) for item in validated_data)
    raw_revenue = sum(getattr(item, "revenue", 0.0) for item in validated_data)
    total_revenue: float | str = round(raw_revenue, 2) if pipeline == "sales" else ""

    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "pipeline": pipeline,
        "report_date": report_date_str,
        "total_records": len(validated_data),
        "total_units": total_units,
        "total_revenue": total_revenue,
        "source_files": ", ".join(source_files),
    }

    # --- CSV (append) ---
    csv_path = settings.OUTPUT_DIR / "run_history.csv"
    write_header = not csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_HISTORY_FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

    # --- JSON (read-rewrite array) ---
    json_path = settings.OUTPUT_DIR / "run_history.json"
    history: list[dict] = []
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                history = json.load(f)
        except (json.JSONDecodeError, ValueError):
            logger.warning("⚠️ run_history.json was malformed — starting fresh.")
    history.append(row)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)

    logger.info(f"📋 Run history logged → {csv_path.name} + {json_path.name}")
