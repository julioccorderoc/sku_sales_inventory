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
DTC_FILENAME_PREFIX = os.getenv("DTC_FILENAME_PREFIX", "DTC_report_")
AWD_FILENAME_PREFIX = os.getenv("AWD_FILENAME_PREFIX", "AWD_report_")
WFS_FILENAME_PREFIX = os.getenv("WFS_FILENAME_PREFIX", "WFS_report_")
COMBINED_FILENAME_BASE = os.getenv("COMBINED_FILENAME", "combined_inventory")

# --- Webhook ---
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# --- Shared Business Logic ---
# Define the explicit order for channels in the final report.
CHANNEL_ORDER = [
    "DTC",
    "FBA",
    "AWD",
    "WFS",
]

# Define the SKU order in one place so it's consistent everywhere.
SKU_ORDER = [
    "1001",
    "PH1001",
    "10012P",
    "PH10012P",
    "2001",
    "PH2001",
    "20012P",
    "PH20012P",
    "3001",
    "PH3001",
    "30012P",
    "PH30012P",
    "4001",
    "PH4001",
    "40012P",
    "PH40012P",
    "5001",
    "PH5001",
    "50012P",
    "PH50012P",
    "6001",
    "PH6001",
    "60012P",
    "PH60012P",
    "8001",
    "PH8001",
    "80012P",
    "PH80012P",
    "9001",
    "PH9001",
    "90012P",
    "PH90012P",
]

# FBA report has slightly different raw SKU names we need to filter by first.
AMAZON_SKUs = [
    "1001",
    "2001",
    "3001s",
    "4001s",
    "5001s",
    "6001s",
    "8001s",
    "9001",
    "PH1001s",
    "PH2001s",
    "PH3001s",
    "PH4001s",
    "PH5001s",
    "PH6001s",
    "PH8001s",
    "PH9001",
    "10012P",
    "20012P",
    "30012P",
    "40012P",
    "50012P",
    "60012P",
    "80012P",
    "90012P",
    "PH10012P",
    "PH20012P",
    "PH30012P",
    "PH40012P",
    "PH50012P",
    "PH80012P",
    "PH90012P",
]
