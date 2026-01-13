import pandas as pd
import requests
import json
from typing import Any
from datetime import date, datetime

from . import settings


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
        print("⚠️ No data to save.")
        return

    # 2. Save CSV
    df = pd.DataFrame(data_dicts)
    df.to_csv(csv_path, index=False)
    print(f"📁 CSV saved to: {csv_path}")

    # 3. Save JSON (Conditionally)
    if settings.SAVE_JSON_OUTPUT:
        with open(json_path, "w") as f:
            json.dump(data_dicts, f, indent=2)
        print(f"📁 JSON saved to: {json_path}")


def post_to_webhook(
    validated_data: list[Any], metadata: dict[str, Any], report_type: str = "inventory"
):
    """
    Posts data to the webhook with a discriminator field ('reportType').
    """
    if not settings.WEBHOOK_URL:
        print("⚠️ WEBHOOK_URL not set. Skipping webhook post.")
        return

    print(f"🚀 Posting '{report_type}' data to webhook: {settings.WEBHOOK_URL}")

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

    try:
        response = requests.post(settings.WEBHOOK_URL, json=payload, timeout=60)
        response.raise_for_status()
        print(f"✅ {report_type.capitalize()} data successfully posted.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error posting to webhook: {e}")
