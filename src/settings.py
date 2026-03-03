import os
from pathlib import Path
import json
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
FBT_INVENTORY_FILENAME_PREFIX = os.getenv(
    "FBT_INVENTORY_FILENAME_PREFIX", "FBT_inventory_"
)
WALMART_SALES_FILENAME_PREFIX = os.getenv(
    "WALMART_SALES_FILENAME_PREFIX", "Walmart_sales_"
)
COMBINED_FILENAME_BASE = os.getenv("COMBINED_FILENAME", "combined_inventory")

WALMART_SALES_PREFIX = "Walmart_sales_"
TIKTOK_SALES_PREFIX = "TikTok_sales_"
TIKTOK_ORDERS_PREFIX = "TikTok_orders_"
AMAZON_SALES_PREFIX = "Amazon_sales_"
SHOPIFY_SALES_PREFIX = "Shopify_sales_"

# --- Webhook ---
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
# --- Output Configuration ---
SAVE_JSON_OUTPUT = os.getenv("SAVE_JSON_OUTPUT", "true").lower() == "true"

def _load_json(path: "Path") -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file missing: {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in configuration file {path}: {e}")


_flexport = _load_json(BASE_DIR / "config" / "flexport_map.json")
_tiktok   = _load_json(BASE_DIR / "config" / "tiktok_map.json")
_shopify  = _load_json(BASE_DIR / "config" / "shopify_map.json")
_amazon   = _load_json(BASE_DIR / "config" / "amazon_map.json")
_catalog  = _load_json(BASE_DIR / "config" / "catalog.json")

DSKU_TO_SKU_MAP     = _flexport.get("DSKU_TO_SKU_MAP", {})
TIKTOK_ID_MAP       = _tiktok.get("TIKTOK_ID_MAP", {})
SHOPIFY_SKU_MAP     = _shopify.get("SHOPIFY_SKU_MAP", {})
AMAZON_SKU_MAP      = _amazon.get("AMAZON_SKU_MAP", {})
CHANNEL_ORDER       = _catalog.get("CHANNEL_ORDER", [])
SALES_CHANNEL_ORDER = _catalog.get("SALES_CHANNEL_ORDER", [])
SKU_ORDER           = _catalog.get("SKU_ORDER", [])
AMAZON_SKUs         = _catalog.get("AMAZON_SKUs", [])
