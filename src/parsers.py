import pandas as pd
from pathlib import Path
from typing import Optional

from . import settings


def _load_csv(file_path: Path) -> Optional[pd.DataFrame]:
    """Helper function to load a CSV with encoding fallback."""
    try:
        return pd.read_csv(file_path, encoding="utf-8")
    except FileNotFoundError:
        print(f"INFO: Report not found at {file_path}, skipping.")
        return None
    except UnicodeDecodeError:
        print(f"INFO: UTF-8 failed for {file_path.name}, retrying with latin-1.")
        return pd.read_csv(file_path, encoding="latin-1")
    except Exception as e:
        print(f"ERROR: Could not read {file_path.name}. Reason: {e}")
        return None


def parse_fba_report(file_path: Path) -> Optional[pd.DataFrame]:
    """Loads FBA data and transforms it into the standard normalized format."""
    df = _load_csv(file_path)
    if df is None:
        return None

    df_filtered = df[df["Merchant SKU"].astype(str).isin(settings.AMAZON_SKUs)].copy()

    # Step 1: Create a new DataFrame with the standard column names
    df_normalized = pd.DataFrame()
    df_normalized["sku"] = (
        df_filtered["Merchant SKU"].astype(str).str.replace(r"s$", "", regex=True)
    )
    df_normalized["channel"] = "FBA"
    df_normalized["units_sold"] = df_filtered["Units Sold Last 30 Days"]
    df_normalized["inventory"] = df_filtered["Available"] + df_filtered["FC transfer"]
    df_normalized["inbound"] = df_filtered["Inbound"]

    print(f"✅ Parsed {file_path.name} successfully.")
    return df_normalized


def parse_dtc_report(file_path: Path) -> Optional[pd.DataFrame]:
    """Loads Flexport (DTC) data and transforms it into the standard normalized format."""
    df = _load_csv(file_path)
    if df is None:
        return None

    df_filtered = df[df["SKU"].isin(settings.SKU_ORDER)].copy()
    agg = (
        df_filtered.groupby("SKU")
        .agg(
            units_sold=("Ecom Last 30 Days", "max"),
            inventory=("Available in Ecom", "sum"),
        )
        .reset_index()
    )

    # Step 1: Add the channel and rename SKU column
    agg = agg.rename(columns={"SKU": "sku"})
    agg["channel"] = "DTC"

    print(f"✅ Parsed {file_path.name} successfully.")
    return agg


# --- Placeholder Parsers (for future implementation) ---
# By creating these stubs, our main orchestrator already knows about them.
# To enable them, you just need to fill in their logic.


def parse_awd_report(file_path: Path) -> Optional[pd.DataFrame]:
    """Placeholder for parsing the AWD report."""
    print(f"INFO: AWD parser is a placeholder. Skipping {file_path.name}.")
    # To implement:
    # 1. df = _load_csv(file_path)
    # 2. if df is None: return None
    # 3. Create df_normalized with columns: sku, channel, units_sold, inventory, inbound
    # 4. df_normalized['channel'] = 'AWD'
    # 5. return df_normalized
    return None


def parse_wfs_report(file_path: Path) -> Optional[pd.DataFrame]:
    """Placeholder for parsing the Walmart (WFS) report."""
    print(f"INFO: WFS parser is a placeholder. Skipping {file_path.name}.")
    return None
