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
AWD_FILENAME_PREFIX = os.getenv("AWD_FILENAME_PREFIX", "AWD_Report_")
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
TIKTOK_SALES_PREFIX = "TikTok_sales_"   # Legacy — superseded by TIKTOK_ORDERS_PREFIX (EPIC-008)
TIKTOK_ORDERS_PREFIX = "TikTok_orders_"
AMAZON_SALES_PREFIX = "Amazon_sales_"
SHOPIFY_SALES_PREFIX = "Shopify_sales_"

# --- EPIC-008: Raw Order File Prefixes ---
# Wire each constant into SalesPipeline.PARSER_REGISTRY as channels are migrated.
# See docs/epic008_raw_orders.md for download instructions and implementation checklists.
SHOPIFY_ORDERS_PREFIX = "Shopify_orders_"   # Step 1 — not yet active
AMAZON_ORDERS_PREFIX  = "Amazon_orders_"    # Step 2 — not yet active
WALMART_ORDERS_PREFIX = "Walmart_orders_"   # Step 3 — not yet active
# TikTok Shop already uses TIKTOK_ORDERS_PREFIX above   — Step 4 complete (EPIC-008)

# --- Webhook (legacy n8n lane — OFF by default) ---
# The n8n workflow was replaced by src/reporting/publisher.py. Leave this false
# unless you deliberately want both lanes writing to the same workbooks.
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_ENABLED = os.getenv("WEBHOOK_ENABLED", "false").lower() == "true"
WEBHOOK_MAX_RETRIES = int(os.getenv("WEBHOOK_MAX_RETRIES", "3"))
# Base for exponential backoff: delay = WEBHOOK_RETRY_BACKOFF ** attempt (2s, 4s, 8s)
WEBHOOK_RETRY_BACKOFF = float(os.getenv("WEBHOOK_RETRY_BACKOFF", "2.0"))

# --- Publishing (internal replacement for the n8n workflow) ---
PUBLISH_ENABLED = os.getenv("PUBLISH_ENABLED", "true").lower() == "true"

# Microsoft Graph — the app registration shared with the supply_chain_agent.
# App-only (client credentials) carries Files.ReadWrite.All for the workbooks;
# the delegated refresh token carries ChatMessage.Send for the chat transport.
MS_TENANT_ID = os.getenv("MS_TENANT_ID")
MS_CLIENT_ID = os.getenv("MS_CLIENT_ID")
MS_CLIENT_SECRET = os.getenv("MS_CLIENT_SECRET")
MS_REFRESH_TOKEN = os.getenv("MS_REFRESH_TOKEN")
MS_MAILBOX_ADDRESS = os.getenv("MS_MAILBOX_ADDRESS", "julio@naturalcurelabs.com")

# Workbook driveItem ids (same values the n8n workflow used).
INVENTORY_WORKBOOK_ID = os.getenv(
    "MS_INVENTORY_WORKBOOK_ID", "016XA7EZ36EKRNDCJNUZA3JXJTLH4YB44K"
)
HISTORY_WORKBOOK_ID = os.getenv(
    "MS_HISTORY_WORKBOOK_ID", "016XA7EZ74NOVOHL26LZGIUGKTW25ZWYLA"
)

# Teams delivery: webhook | graph_chat | file | none
TEAMS_TRANSPORT = os.getenv("TEAMS_TRANSPORT", "file")
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")
# The Power Automate action's bound field: message | adaptive_card | text
TEAMS_WEBHOOK_PAYLOAD = os.getenv("TEAMS_WEBHOOK_PAYLOAD", "message")
TEAMS_CHAT_ID = os.getenv("TEAMS_CHAT_ID")
# Seconds between the summary and the anomaly post (keeps Teams ordering sane).
PUBLISH_SEND_GAP_SECONDS = float(os.getenv("PUBLISH_SEND_GAP_SECONDS", "2"))

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
