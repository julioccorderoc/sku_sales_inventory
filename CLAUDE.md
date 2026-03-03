# CLAUDE.md — Sales & Inventory Intelligence Pipeline

## Project Overview

Automated ETL pipeline for **Natural Cure Labs (NCL)** that aggregates, normalizes, and reports on sales and inventory data across multiple commerce channels. Reads manually-downloaded CSV reports, normalizes them against a master SKU list, and posts results to an n8n webhook for downstream automation.

**Two pipelines run sequentially:**

- **Inventory Pipeline**: Consolidates stock from FBA, AWD, WFS, FBT, DTC (Flexport), and Reserve
- **Sales Pipeline**: Aggregates daily sales from Amazon, Walmart, TikTok Shop, and Shopify

---

## Commands

```bash
# Run the full pipeline (production)
python main.py

# Run in test mode (skips webhook POST, still generates CSV/JSON output)
python main.py --test

# Run individual pipeline in isolation (for debugging)
python -c "from src.pipelines.inventory import InventoryPipeline; InventoryPipeline().run()"
python -c "from src.pipelines.sales import SalesPipeline; SalesPipeline().run()"

# Combine historical inventory reports into a single file
python combine_inventory.py

# Install dependencies (uses uv, not pip)
uv sync
```

---

## Architecture

```
main.py                         # Orchestrates both pipelines
src/
  pipeline.py                   # Abstract DataPipeline base (ETL pattern)
  pipelines/
    inventory.py                # InventoryPipeline (extends DataPipeline)
    sales.py                    # SalesPipeline (extends DataPipeline)
  parsers.py                    # All channel-specific CSV parsers
  schemas.py                    # Pydantic validation models
  settings.py                   # Config loader (env vars + mappings.json)
  data_handler.py               # Output saving + webhook POST
  utils.py                      # Shared helpers (CSV loading, date parsing, clean_money)
  logger.py                     # Console + rotating file logging
config/
  mappings.json                 # Master config: SKU maps, channel order, SKU list
input/                          # Manually downloaded CSVs from seller platforms
output/                         # Generated reports (CSV + JSON pairs)
logs/app.log                    # Rotating log (5MB max, 3 backups)
```

### ETL Flow

Each pipeline follows the abstract `DataPipeline` base class pattern:

1. **Extract** — `_extract()`: Finds the latest report file(s) per channel, returns raw DataFrame
2. **Transform** — `_transform(df)`: Normalizes columns, zero-fills all SKU × channel combinations, validates with Pydantic
3. **Load** — handled by base class: saves CSV/JSON to `output/`, POSTs to webhook

---

## Configuration

### Environment Variables (`.env`)

```ini
# Flexport API
FLEXPORT_API_KEY="..."
FLEXPORT_CODE="..."
FLEXPORT_ACCESS_TOKEN="..."
FLEXPORT_ACCOUNT_ID="..."
BASE_URL="https://logistics-api.flexport.com"

# File paths
INPUT_DIR="input"
OUTPUT_DIR="output"
COMBINED_FILENAME="inventory_report"
SAVE_JSON_OUTPUT="true"

# Webhook
WEBHOOK_URL="https://..."
```

### Config Files — Mappings and Catalog

The `config/` directory contains five JSON files loaded independently by `src/settings.py`:

| File | Key(s) | Purpose |
| ---- | ------ | ------- |
| `config/flexport_map.json` | `DSKU_TO_SKU_MAP` | Flexport DSKU → internal SKU (31 entries) |
| `config/tiktok_map.json` | `TIKTOK_ID_MAP` | TikTok product ID → `[SKU, ...]` (supports bundles with duplication for 2x; 50 entries) |
| `config/shopify_map.json` | `SHOPIFY_SKU_MAP` | Shopify SKU/name → `[SKU, ...]` (80+ entries) |
| `config/amazon_map.json` | `AMAZON_SKU_MAP` | Amazon merchant SKU → `[SKU, ...]` (30+ entries) |
| `config/catalog.json` | `CHANNEL_ORDER`, `SALES_CHANNEL_ORDER`, `SKU_ORDER`, `AMAZON_SKUs` | Master SKU list and channel ordering |

Channel ordering values:

| Key | Values |
|-----|--------|
| `CHANNEL_ORDER` | `["FBA", "AWD", "DTC", "Reserve", "WFS", "FBT"]` |
| `SALES_CHANNEL_ORDER` | `["Amazon", "Walmart", "TikTok Shop", "Shopify", "TikTok Shopify", "Target", "Others"]` |
| `SKU_ORDER` | Master list of 32 internal SKUs (fixed order for output consistency) |

---

## Key Conventions

### Naming

- **DataFrame columns**: CamelCase (`SKU`, `Channel`, `Units`, `Revenue`, `Inventory`, `Inbound`)
- **Python variables/functions**: `snake_case`
- **Constants**: `UPPER_SNAKE_CASE`
- **Classes**: `PascalCase`

### ID Generation

```python
# Unique record ID
id = f"{date}_{channel}_{sku}"          # e.g., "20260226_FBA_1001"

# SKU-channel composite (for merges/lookups)
sku_channel_id = f"{channel}_{sku}"     # e.g., "FBA_1001"

# Spaces in channel names → underscores in IDs
# e.g., "TikTok Shop" → "TikTok_Shop_1001"
```

### Zero-Fill Template Pattern

All outputs guarantee every SKU × channel combination is present, even with zero values:

```python
# Create full template
template = pd.MultiIndex.from_product([channels, skus], names=["Channel", "SKU"])
df_template = pd.DataFrame(index=template).reset_index()
# Merge actual data, fill NaN → 0
result = df_template.merge(actual_data, on=["Channel", "SKU"], how="left").fillna(0)
```

### Bundle Expansion

Bundles are exploded at parse time (before aggregation). A Shopify bundle of `["3001", "4001"]` sold once becomes two rows, one per component SKU.

### Pydantic Validation

All output validated before saving:

- `InventoryItem`: fields `date`, `sku`, `channel`, `units`, `inventory`, `inbound` (all `ge=0`)
- `SalesRecord`: fields `date`, `sku`, `channel`, `units`, `revenue` (all `ge=0`)
- `populate_by_name=True` — both field names and aliases work for construction

### Logging Style

```python
logger.info("✅ Parsed {file.name} successfully.")
logger.warning("⚠️  Missing SKUs ({len(missing)}): {', '.join(missing)}")
logger.error("❌ Data validation failed!")
logger.info("📊 Stats: {raw_count} rows → {len(df)} SKU-channel pairs")
```

---

## Input File Naming Conventions

Files must follow these prefixes for auto-detection (latest by date in filename):

| Source | Prefix Pattern |
|--------|---------------|
| FBA Inventory | `FBA_report_YYYY-MM-DD.csv` |
| AWD Inventory | `AWD_Report_YYYY-MM-DD.csv` |
| WFS Inventory | `Walmart_inventory_YYYY-MM-DD.csv` |
| Walmart Sales | `Walmart_sales_YYYY-MM-DD.csv` |
| FBT Inventory | `FBT_inventory_YYYY-MM-DD.csv` |
| TikTok Orders | `TikTok_orders_YYYY-MM-DD.csv` |
| TikTok Sales | `TikTok_sales_YYYY-MM-DD.csv` |
| Amazon Sales | `Amazon_sales_YYYY-MM-DD.csv` |
| Shopify Sales | `Shopify_sales_YYYY-MM-DD.csv` |
| Flexport Levels | `Flexport_levels_YYYY-MM-DD.csv` |
| Flexport Orders | `Flexport_orders_YYYY-MM-DD.csv` |
| Flexport Inbound | `Flexport_inbound_YYYY-MM-DD.csv` |

The pipeline finds the **most recently dated file** matching each prefix — always place new downloads in `input/`.

---

## Adding a New Channel or SKU

### New SKU

1. Add to `SKU_ORDER` list in `config/catalog.json`
2. Add relevant entries to each applicable channel map (`AMAZON_SKU_MAP`, `TIKTOK_ID_MAP`, etc.)
3. Add to `AMAZON_SKUs` list if sold on Amazon

### New Inventory Channel

All parsers in `src/parsers.py` use the same return contract:

```python
# Parser signature
def parse_my_channel(file_paths: dict[str, Path]) -> ParseResult:
    ...
    # Return CamelCase columns at the parser boundary
    df = df.rename(columns={"sku": "SKU", "channel": "Channel", ...})
    return ParseResult(df=df, raw_count=raw_count)
```

Steps:

1. Write a parser in `src/parsers.py` returning `ParseResult` with `df` columns `[Channel, SKU, Units, Inventory, Inbound]` (CamelCase).

2. Add an entry to `InventoryPipeline.PARSER_REGISTRY` in `src/pipelines/inventory.py` using the standardized shape:

   ```python
   {"channel": "MyChannel", "parser": parsers.parse_my_channel, "files": {"primary": settings.MY_PREFIX}}
   ```

3. Add channel name to `CHANNEL_ORDER` in `config/catalog.json`.

### New Sales Channel

Steps:

1. Write a parser in `src/parsers.py` returning `ParseResult` with `df` columns `[Channel, SKU, Units, Revenue]` and `bundle_stats` dict.

2. Add an entry to `SalesPipeline.PARSER_REGISTRY` in `src/pipelines/sales.py` using the standardized shape:

   ```python
   {"channel": "MyChannel", "parser": parsers.parse_my_channel, "files": {"primary": settings.MY_PREFIX}}
   ```

3. Add channel name to `SALES_CHANNEL_ORDER` in `config/catalog.json`.

---

## Special Parsing Notes

- **Flexport**: Uses 3 files (levels, orders, inbound) and creates two output channels: `DTC` and `Reserve`
- **Shopify**: A single file contains multiple sales platforms (Shopify, TikTok, Target, Others) — the parser splits by `Sales channel` field
- **TikTok Sales**: Has dynamic header rows — the parser scans for the row containing `"SKU ID"` before processing
- **Amazon SKUs**: Trailing `"s"` suffix removed from SKU names during normalization (e.g., `"3001s"` → `"3001"`)
- **CSV Encoding**: All CSV loading falls back from `utf-8-sig` → `latin-1` automatically
- **Report Date**: Extracted from filename, not file content — file naming must be accurate

---

## Technology Stack

- **Python 3.13** (pinned via `.python-version`)
- **uv** — package manager (use `uv sync`, not `pip install`)
- **pandas** — data manipulation
- **pydantic** — schema validation and serialization
- **requests** — webhook HTTP POST
- **python-dotenv** — env variable loading

---

## Output Format

Both pipelines output two files per run to `output/`:

```
output/
  inventory_report_YYYY-MM-DD.csv    # All SKU × channel combinations with inventory data
  inventory_report_YYYY-MM-DD.json   # Same data as JSON array
  sales_report_YYYY-MM-DD.csv        # All SKU × channel combinations with sales data
  sales_report_YYYY-MM-DD.json       # Same data as JSON array
```

The date in the filename is the **processing date** (today's date when the script runs), not the report date.

---

## Webhook Integration

After each pipeline completes, results are POSTed to the n8n webhook defined in `WEBHOOK_URL`:

```json
{
  "reportType": "inventory" | "sales",
  "reportSummary": { "date": "...", "totalRecords": 192, ... },
  "reportData": [ { "id": "...", "sku": "...", ... } ]
}
```

Use `--test` flag to skip the webhook POST during development.

## Before Starting Any Task

1. Check `docs/roadmap.md` — what Epic is active? What is the DoD?
2. Check `.ai/CURRENT_PLAN.MD` — is there in-progress work to continue?
3. Check `.ai/ERRORS.MD` — any known blockers or failure patterns?
4. Read the relevant source files **before** modifying them.
