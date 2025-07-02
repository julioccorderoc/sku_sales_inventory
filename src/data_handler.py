import pandas as pd
import requests
import json
from typing import Optional
from datetime import date

from . import settings
from . import utils
from .schemas import InventoryItem


def save_outputs(df: pd.DataFrame, validated_data: list[InventoryItem]):
    """Saves the final data to CSV and conditionally to JSON, with dated filenames."""
    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    date_suffix = utils.get_date_suffix_for_filename()

    csv_path = (
        settings.OUTPUT_DIR / f"{settings.COMBINED_FILENAME_BASE}_{date_suffix}.csv"
    )
    json_path = (
        settings.OUTPUT_DIR / f"{settings.COMBINED_FILENAME_BASE}_{date_suffix}.json"
    )

    csv_columns = {
        field: info.alias for field, info in InventoryItem.model_fields.items()
    }
    df_for_csv = df.rename(columns=csv_columns)
    df_for_csv.to_csv(csv_path, index=False)
    print(f"✅ Normalized report saved to: {csv_path}")

    # --- MODIFIED: Conditionally save to JSON ---
    if settings.SAVE_JSON_OUTPUT:
        with open(json_path, "w") as f:
            json_data = [item.model_dump(by_alias=True) for item in validated_data]
            json.dump(json_data, f, indent=2, default=str)
        print(f"✅ JSON output saved to: {json_path}")
    else:
        print("INFO: Skipping JSON file save as per configuration.")


def post_to_webhook(
    validated_data: list[InventoryItem], status_summary: dict[str, Optional[date]]
):
    """
    Posts the validated data AND the status summary to the webhook.
    """
    if not settings.WEBHOOK_URL:
        print("⚠️ WEBHOOK_URL not set. Skipping webhook post.")
        return

    print(f"🚀 Posting data and summary to webhook: {settings.WEBHOOK_URL}")

    payload = {
        "reportData": [
            item.model_dump(mode="json", by_alias=True) for item in validated_data
        ],
        "statusSummary": {
            channel: dt.isoformat() if dt else None
            for channel, dt in status_summary.items()
        },
    }

    try:
        response = requests.post(settings.WEBHOOK_URL, json=payload, timeout=15)
        response.raise_for_status()
        print("✅ Data and summary successfully posted to webhook.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error posting to webhook: {e}")
