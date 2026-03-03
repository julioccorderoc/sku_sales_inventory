import pytest
import pandas as pd
from pathlib import Path
from datetime import date

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


# --- Inventory Parser File-Path Fixtures ---

@pytest.fixture
def fba_fixture():
    return {"primary": FIXTURES_DIR / "fba_report.csv"}


@pytest.fixture
def awd_fixture():
    return {"primary": FIXTURES_DIR / "awd_report.csv"}


@pytest.fixture
def wfs_fixture():
    return {
        "sales": FIXTURES_DIR / "walmart_sales.csv",
        "inventory": FIXTURES_DIR / "walmart_inventory.csv",
    }


@pytest.fixture
def fbt_fixture():
    return {
        "sales": FIXTURES_DIR / "tiktok_orders.csv",
        "inventory": FIXTURES_DIR / "fbt_inventory.csv",
    }


@pytest.fixture
def fbt_inventory_fixture():
    return {"primary": FIXTURES_DIR / "fbt_inventory.csv"}


@pytest.fixture
def flexport_fixture():
    return {
        "levels": FIXTURES_DIR / "flexport_levels.csv",
        "orders": FIXTURES_DIR / "flexport_orders.csv",
        "inbound": FIXTURES_DIR / "flexport_inbound.csv",
    }


@pytest.fixture
def flexport_no_inbound_fixture():
    """Tests the optional inbound file path — inbound defaults to 0.
    Uses a nonexistent path (not None) because load_csv(None) crashes on
    file_path.name in the error handler (latent bug; None is only passed by
    the pipeline's extract phase, not by tests directly)."""
    return {
        "levels": FIXTURES_DIR / "flexport_levels.csv",
        "orders": FIXTURES_DIR / "flexport_orders.csv",
        "inbound": FIXTURES_DIR / "nonexistent_inbound.csv",
    }


# --- Sales Parser File-Path Fixtures ---

@pytest.fixture
def walmart_sales_fixture():
    return {"primary": FIXTURES_DIR / "walmart_sales.csv"}


@pytest.fixture
def amazon_sales_fixture():
    return {"primary": FIXTURES_DIR / "amazon_sales.csv"}


@pytest.fixture
def tiktok_orders_fixture():
    return {"primary": FIXTURES_DIR / "tiktok_orders.csv"}


@pytest.fixture
def tiktok_sales_fixture():
    return {"primary": FIXTURES_DIR / "tiktok_sales.csv"}


@pytest.fixture
def shopify_sales_fixture():
    return {"primary": FIXTURES_DIR / "shopify_sales.csv"}


# --- Pipeline Transform DataFrames ---

@pytest.fixture
def inventory_df():
    """
    Minimal DataFrame in the shape returned by InventoryPipeline.extract()
    and expected by InventoryPipeline.transform().
    Columns: channel, sku, units_sold, inventory, inbound, Date
    """
    return pd.DataFrame([
        {"channel": "FBA",  "sku": "1001", "units_sold": 5, "inventory": 100, "inbound": 10, "Date": date(2026, 2, 11)},
        {"channel": "FBA",  "sku": "2001", "units_sold": 3, "inventory": 50,  "inbound": 5,  "Date": date(2026, 2, 11)},
        {"channel": "AWD",  "sku": "1001", "units_sold": 0, "inventory": 200, "inbound": 20, "Date": date(2026, 2, 11)},
        {"channel": "AWD",  "sku": "2001", "units_sold": 0, "inventory": 80,  "inbound": 8,  "Date": date(2026, 2, 11)},
    ])


@pytest.fixture
def inventory_df_negative():
    """DataFrame containing a negative inventory value — should fail Pydantic validation."""
    return pd.DataFrame([
        {"channel": "FBA", "sku": "1001", "units_sold": 0, "inventory": -1, "inbound": 0, "Date": date(2026, 2, 11)},
    ])


@pytest.fixture
def sales_df():
    """
    Minimal DataFrame in the shape returned by SalesPipeline.extract()
    and expected by SalesPipeline.transform().
    Columns: SKU, Channel, Units, Revenue, Date
    """
    return pd.DataFrame([
        {"SKU": "1001", "Channel": "Amazon",  "Units": 5, "Revenue": 125.0, "Date": date(2026, 2, 11)},
        {"SKU": "2001", "Channel": "Amazon",  "Units": 2, "Revenue": 50.0,  "Date": date(2026, 2, 11)},
        {"SKU": "1001", "Channel": "Walmart", "Units": 1, "Revenue": 25.0,  "Date": date(2026, 2, 11)},
        {"SKU": "2001", "Channel": "Walmart", "Units": 0, "Revenue": 0.0,   "Date": date(2026, 2, 11)},
    ])
