import os
from pathlib import Path
from dotenv import load_dotenv

# --- Base Directory ---
BASE_DIR = Path(__file__).resolve().parent.parent

# --- Load Environment Variables ---
load_dotenv(BASE_DIR / ".env")

# --- Path Configuration ---
# Use Path objects for robust, OS-agnostic path handling.
INPUT_DIR = BASE_DIR / os.getenv("INPUT_DIR", "input")
OUTPUT_DIR = BASE_DIR / os.getenv("OUTPUT_DIR", "output")

# --- Filename Configuration ---
FBA_FILENAME_PREFIX = os.getenv("FBA_FILENAME_PREFIX", "FBA_report_")
FLEXPORT_LEVELS_FILENAME_PREFIX = os.getenv(
    "FLEXPORT_LEVELS_FILENAME_PREFIX", "Flexport_levels_"
)
FLEXPORT_ORDERS_FILENAME_PREFIX = os.getenv(
    "FLEXPORT_ORDERS_FILENAME_PREFIX", "Flexport_orders_"
)
FLEXPORT_INBOUND_FILENAME_PREFIX = os.getenv(
    "FLEXPORT_INBOUND_FILENAME_PREFIX", "Flexport_inbound_"
)
AWD_FILENAME_PREFIX = os.getenv("AWD_FILENAME_PREFIX", "AWD_report_")
WFS_INVENTORY_FILENAME_PREFIX = os.getenv(
    "WFS_INVENTORY_FILENAME_PREFIX", "Walmart_inventory_"
)
WALMART_SALES_FILENAME_PREFIX = os.getenv(
    "WALMART_SALES_FILENAME_PREFIX", "Walmart_sales_"
)
COMBINED_FILENAME_BASE = os.getenv("COMBINED_FILENAME", "combined_inventory")

WALMART_SALES_PREFIX = "Walmart_sales_"
TIKTOK_SALES_PREFIX = "TikTok_sales_"
AMAZON_SALES_PREFIX = "Amazon_sales_"
SHOPIFY_SALES_PREFIX = "Shopify_sales_"

# --- Webhook ---
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
# --- Output Configuration ---
SAVE_JSON_OUTPUT = os.getenv("SAVE_JSON_OUTPUT", "true").lower() == "true"

# --- Mappings (Loaded from config/mappings.json) ---
import json

MAPPINGS_FILE = BASE_DIR / "config" / "mappings.json"

try:
    with open(MAPPINGS_FILE, "r", encoding="utf-8") as f:
        _mappings = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError(f"Configuration file missing: {MAPPINGS_FILE}")
except json.JSONDecodeError as e:
    raise ValueError(f"Invalid JSON in configuration file: {e}")

DSKU_TO_SKU_MAP = _mappings.get("DSKU_TO_SKU_MAP", {})
TIKTOK_ID_MAP = _mappings.get("TIKTOK_ID_MAP", {})
SHOPIFY_SKU_MAP = _mappings.get("SHOPIFY_SKU_MAP", {})
AMAZON_SKU_MAP = _mappings.get("AMAZON_SKU_MAP", {})
CHANNEL_ORDER = _mappings.get("CHANNEL_ORDER", [])
SALES_CHANNEL_ORDER = _mappings.get("SALES_CHANNEL_ORDER", [])
SKU_ORDER = _mappings.get("SKU_ORDER", [])
AMAZON_SKUs = _mappings.get("AMAZON_SKUs", [])
