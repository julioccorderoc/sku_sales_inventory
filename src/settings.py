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

# --- Mappings ---
DSKU_TO_SKU_MAP = {
    "DB59IQ90Q2K": "1001",
    "DJN5OWEKCRR": "2001",
    "DU100QJK94P": "3001",
    "DLIPFIOQ0VP": "4001",
    "DT0W1B9XFKY": "5001",
    "DYXWQPQDEZO": "6001",
    "D2CN7AA43MT": "8001",
    "DSSMD9US54L": "9001",
    "DK8JVG67AXP": "PH1001",
    "DL68JUL9HET": "PH2001",
    "D2J2GKL0I65": "PH3001",
    "DQZ2GQTDB87": "PH4001",
    "DQY4L26TKHU": "PH5001",
    "DWVISC176TD": "PH6001",
    "DIIETYES2QI": "PH8001",
    "DSAF9XP4WJD": "PH9001",
    "DWEK26BBZJJ": "10012P",
    "D6MLKSVAVKB": "20012P",
    "D7DH8R7K86B": "30012P",
    "D4PP7Y7QWFQ": "40012P",
    "DD48FDEWX7L": "50012P",
    "D5XE5BRXRQV": "60012P",
    "DQESAGVV2D9": "80012P",
    "DHAMPN9SDCE": "90012P",
    "DZ2CZJNPYWX": "PH10012P",
    "DT8BHMKWJWD": "PH20012P",
    "DWVPW2TADRV": "PH30012P",
    "DP4VYHTD277": "PH40012P",
    "DTNBSCVBWTQ": "PH50012P",
    "DWLMK6GHEBH": "PH80012P",
    "D2NZAKEPW6B": "PH90012P",
}

# ---   TikTok Sales Mapping (Variant SKU -> Internal SKU List)   ---
# Note: Bundles are lists of SKUs. Revenue/Units will be distributed.
TIKTOK_ID_MAP = {
    "1729500444198605281": ["5001"],
    "1729500444198670817": ["5001", "5001"],
    "1729500444198736353": ["5001", "5001", "5001"],
    "1729500852410421729": ["PH5001"],
    "1730967972136325601": ["2001", "4001", "3001"],
    "1729500832005591521": ["4001"],
    "1731511743027384801": ["5001", "4001"],
    "1729500852410487265": ["PH5001", "PH5001"],
    "1729500832005657057": ["4001", "4001"],
    "1729500256926732769": ["3001"],
    "1729500832651186657": ["4001", "4001", "4001"],
    "1729500852410552801": ["PH5001", "PH5001", "PH5001"],
    "1730967954029318625": ["2001", "3001", "8001"],
    "1729500256926798305": ["3001", "3001"],
    "1729500256926863841": ["3001", "3001", "3001"],
    "1731511756248093153": ["4001", "3001"],
    "1731531464545898977": ["5001", "PH6001"],
    "1730967973602234849": ["1001", "6001", "4001"],
    "1729499998780101089": ["1001"],
    "1729672391668437473": ["9001", "9001"],
    "1731511728937538017": ["4001", "1001"],
    "1729499998780166625": ["1001", "1001"],
    "1729500399816315361": ["6001"],
    "1731511751922192865": ["5001", "2001"],
    "1729500829969715681": ["PH8001"],
    "1729500829969781217": ["PH8001", "PH8001"],
    "1729500838277321185": ["PH6001"],
    "1729499998780232161": ["1001", "1001", "1001"],
    "1729500439128347105": ["8001"],
    "1729500439128412641": ["8001", "8001"],
    "1729672391668371937": ["9001"],
    "1731531475122098657": ["5001", "PH1001"],
    "1729500399816380897": ["6001", "6001"],
    "1729500842255880673": ["PH3001"],
    "1729500838277386721": ["PH6001", "PH6001"],
    "1729500839146066401": ["PH2001"],
    "1729500839146131937": ["PH2001", "PH2001"],
    "1729500457249509857": ["2001"],
    "1729500804499411425": ["2001", "2001"],
    "1729500457249640929": ["2001", "2001", "2001"],
    "1729500844125032929": ["PH4001", "PH4001", "PH4001"],
    "1729500844124967393": ["PH4001", "PH4001"],
    "1729500844124901857": ["PH4001"],
    "1731511753441513953": ["4001", "2001"],
    "1729500296657801697": ["PH1001"],
    "1731531472205156833": ["4001", "9001"],
    "1729672305039217121": ["PH9001"],
    "1729500439128478177": ["8001", "8001", "8001"]
}


# --- 2. Shopify Mapping (Variant SKU -> Internal SKU List) ---
SHOPIFY_SKU_MAP = {
    "5001": ["5001"],
    "5002": ["5001", "5001"],
    "5003": ["5001", "5001", "5001"],
    "PH5001": ["PH5001"],
    "4001": ["4001"],
    "3001": ["3001"],
    "PH5002": ["PH5001", "PH5001"],
    "2001": ["2001"],
    "8001": ["8001"],
    "1001": ["1001"],
    "9001": ["9001"],
    "4002": ["4001", "4001"],
    "PH5003": ["PH5001", "PH5001", "PH5001"],
    "6001": ["6001"],
    "4003": ["4001", "4001", "4001"],
    "3003": ["3001", "3001", "3001"],
    "3002": ["3001", "3001"],
    "PH8003": ["PH8001", "PH8001", "PH8001"],
    "9003": ["9001", "9001", "9001"],
    "PH3003": ["PH3001", "PH3001", "PH3001"],
    "8003": ["8001", "8001", "8001"],
    "1003": ["1001", "1001", "1001"],
    "PH8001": ["PH8001"],
    "4001-3001": ["4001", "3001"],
    "9002": ["9001", "9001"],
    "1002": ["1001", "1001"],
    "6003": ["6001", "6001", "6001"],
    "PH2001": ["PH2001"],
    "PH8002": ["PH8001", "PH8001"],
    "PH3001": ["PH3001"],
    "8002": ["8001", "8001"],
    "PH6001": ["PH6001"],
    "PH1002": ["PH1001", "PH1001"],
    "PH6002": ["PH6001", "PH6001"],
    "6002": ["6001", "6001"],
    "PH9001": ["PH9001"],
    "PH1001": ["PH1001"],
    "PH4001": ["PH4001"],
    "PH4002": ["PH4001", "PH4001"],
    "PH4003": ["PH4001", "PH4001", "PH4001"],
    "2005": ["2001", "2001"],
    "2003": ["2001", "2001", "2001"],
    "PH2002": ["PH2001", "PH2001"],
    "PH2003": ["PH2001", "PH2001", "PH2001"],
    "PH9002": ["PH9001", "PH9001"],
    "PH9003": ["PH9001", "PH9001", "PH9001"],
    "PH3002": ["PH3001", "PH3001"],
    "PH1003": ["PH1001", "PH1001", "PH1001"],
    "PH6003": ["PH6001", "PH6001", "PH6001"],
    "4001-PH8001": ["4001", "PH8001"],
    "4001-PH9001": ["4001", "PH9001"],
    "4001-2001": ["4001", "2001"],
    "5001-2001": ["5001", "2001"],
    "5001-PH1001": ["5001", "PH1001"],
    "5001-PH6001": ["5001", "PH6001"],
    "4001-1001": ["4001", "1001"],
    "AlexandrasSpecialBundle": ["3001", "4001", "8001"],
    "GetStartedOffer": ["1001"],
    "LysineMonolaurinandCleanLysine": ["5001", "4001"],
    "DigestiveHealthBundle": ["1001", "4001", "6001"],
    "recoverysetbundle": ["2001", "3001", "4001"],
    "CousinsSpecialBundle": ["2001", "5001", "6001"],
    "ImmuneDefenseBundle": ["2001", "3001", "8001"],
    "CreatorEssentialsBundle": ["2001", "5001", "8001"],
    "BlasianBundle": ["2001", "5001", "8001"],
}

AMAZON_SKU_MAP = {
    "1001": ["1001"],
    "10012P": ["10012P"],
    "2001": ["2001"],
    "20012P": ["20012P"],
    "2002": ["2002"],
    "30012P": ["30012P"],
    "3001s": ["3001"],
    "4001": ["4001"],
    "40012P": ["40012P"],
    "4001s": ["4001"],
    "5001": ["5001"],
    "50012P": ["50012P"],
    "5001s": ["5001"],
    "60012P": ["60012P"],
    "6001s": ["6001"],
    "80012P": ["80012P"],
    "8001s": ["8001"],
    "9001": ["9001"],
    "90012P": ["90012P"],
    "PH10012P": ["PH10012P"],
    "PH1001s": ["PH1001"],
    "PH20012P": ["PH20012P"],
    "PH2001s": ["PH2001"],
    "PH30012P": ["PH30012P"],
    "PH3001s": ["PH3001"],
    "PH40012P": ["PH40012P"],
    "PH4001s": ["PH4001"],
    "PH50012P": ["PH50012P"],
    "PH5001s": ["PH5001"],
    "PH6001s": ["PH6001"],
    "PH8001": ["PH8001"],
    "PH80012P": ["PH80012P"],
    "PH8001s": ["PH8001"],
    "PH9001": ["PH9001"],
    "PH90012P": ["PH90012P"],
}





# --- Shared Business Logic ---
# Define the explicit order for channels in the final report.
CHANNEL_ORDER = [
    "FBA",
    "AWD",
    "DTC",
    "Reserve",
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
