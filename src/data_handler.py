import pandas as pd
import requests
import json
from typing import List

from . import settings
from .schemas import InventoryItem


def save_outputs(df: pd.DataFrame, validated_data: List[InventoryItem]):
    """Saves the final data to both CSV and JSON formats."""
    settings.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = settings.OUTPUT_DIR / f"{settings.COMBINED_FILENAME_BASE}.csv"
    json_path = settings.OUTPUT_DIR / f"{settings.COMBINED_FILENAME_BASE}.json"

    # Save to CSV - use the Pydantic aliases for user-friendly column headers
    csv_columns = {
        field: info.alias for field, info in InventoryItem.model_fields.items()
    }
    df_for_csv = df.rename(columns=csv_columns)
    df_for_csv.to_csv(csv_path, index=False)
    print(f"✅ Combined report saved to: {csv_path}")

    # Save to JSON from the validated Pydantic models for guaranteed schema correctness
    with open(json_path, "w") as f:
        # model_dump will respect the field aliases
        json_data = [item.model_dump(by_alias=True) for item in validated_data]
        json.dump(json_data, f, indent=4, default=str)
    print(f"✅ Combined report saved to: {json_path}")


def post_to_webhook(validated_data: List[InventoryItem]):
    """Posts the validated list of Pydantic models to the webhook."""
    if not settings.WEBHOOK_URL:
        print("⚠️ WEBHOOK_URL not set. Skipping webhook post.")
        return

    print(f"🚀 Posting data to webhook: {settings.WEBHOOK_URL}")

    # Use model_dump(by_alias=True) to create a list of dicts with the correct JSON keys
    json_payload = [
        item.model_dump(mode="json", by_alias=True) for item in validated_data
    ]
    payload = {"reportData": json_payload}

    try:
        response = requests.post(settings.WEBHOOK_URL, json=payload, timeout=15)
        response.raise_for_status()
        print("✅ Data successfully posted to webhook.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error posting to webhook: {e}")
