# EPIC-008 — Raw Order Report Migration

## Overview

This guide covers the phased migration of sales parsers from pre-aggregated platform summary CSVs to raw order-level exports. Raw order files give the pipeline control over aggregation, enable deduplication, and provide the granularity needed for future anomaly detection.

Migration is one channel at a time. **TikTok Shop is already complete.** Shopify, Amazon, and Walmart are scaffolded with stubs and documented here for easy pickup.

---

## Status

| Step | Channel | Status | Parser | Input File Prefix |
|------|---------|--------|--------|-------------------|
| Step 4 | TikTok Shop | ✅ **Complete** | `parse_tiktok_shop_orders_report` | `TikTok_orders_` |
| Step 2 | Amazon | ✅ **Complete** | `parse_amazon_orders_report` | `Amazon_orders_` |
| Step 1 | Shopify | ⏳ Pending — raw file needed | `parse_shopify_orders_report` | `Shopify_orders_` |
| Step 3 | Walmart | ⏳ Pending — raw file needed | `parse_walmart_orders_report` | `Walmart_orders_` |

> TikTok Shop was done first because `TikTok_orders_*.csv` already existed in `input/`.
> Amazon was completed next using the raw Order Report export.

---

## How to Activate a Channel (General Checklist)

For each pending channel, follow these steps once the raw order file is available:

1. Download the raw order export (instructions per channel below)
2. Drop the file in `input/` using the correct filename prefix
3. Implement the parser stub in `src/parsers.py` (the stub already has a full docstring with exact logic)
4. Add the test fixture to `tests/fixtures/` and wire it in `tests/conftest.py`
5. Add a test class to `tests/test_parsers.py`
6. Swap the `PARSER_REGISTRY` entry in `src/pipelines/sales.py`
7. Run `uv run pytest -v` — all tests must pass
8. Run `python main.py --test` and compare `output/run_history.csv` totals against the prior run

---

## Step 1 — Shopify (pending)

### Download instructions

1. Log in to Shopify Admin
2. Navigate to **Orders → All orders**
3. Click **Export** → select **All orders** → format: **Plain CSV file**
4. Rename the downloaded file: `Shopify_orders_YYYY-MM-DD.csv` (use today's date)
5. Drop it in `input/`

### Expected columns (key subset)

| Column | Purpose |
|--------|---------|
| `Name` | Order number (e.g. `#1001`) |
| `Financial Status` | Filter: keep `paid` only |
| `Lineitem sku` | Shopify SKU or bundle name → apply `SHOPIFY_SKU_MAP` |
| `Lineitem quantity` | Units per line item |
| `Lineitem price` | Unit price (Revenue = price × quantity per row) |
| `Source name` | Maps to channel (same logic as current `parse_shopify_sales_report`) |

### Channel mapping (same as current parser)

| Source name | Channel |
|-------------|---------|
| `web` / `online_store` | `Shopify` |
| `tiktok` | `TikTok Shopify` |
| `marketplace_connect` | `Target` |
| anything else | `Others` |

### Implementation checklist

- [ ] Filter: `Financial Status == "paid"`
- [ ] Compute Revenue = `Lineitem price` × `Lineitem quantity` per row
- [ ] Apply channel mapping above
- [ ] For each row call `_process_bundled_row(row, SHOPIFY_SKU_MAP, "Lineitem sku", "Lineitem quantity", "Revenue", "Shopify Orders")`
- [ ] Group by `SKU` + `Channel`, sum `Units` and `Revenue`
- [ ] Track per-channel `bundle_stats` dict (same shape as current `parse_shopify_sales_report`)
- [ ] Return `ParseResult(df=grouped, raw_count=raw_count, bundle_stats=bundle_stats)`

### PARSER_REGISTRY entry to swap in

Replace in `src/pipelines/sales.py`:

```python
# Before (current):
{
    "channel": "Mixed",
    "parser": parsers.parse_shopify_sales_report,
    "files": {"primary": settings.SHOPIFY_SALES_PREFIX},
},

# After (EPIC-008 Step 1):
{
    "channel": "Mixed",
    "parser": parsers.parse_shopify_orders_report,
    "files": {"primary": settings.SHOPIFY_ORDERS_PREFIX},
},
```

### Test fixture

Create `tests/fixtures/shopify_orders.csv` with at least:
- One `paid` row with a known Shopify SKU (maps to internal SKU via `SHOPIFY_SKU_MAP`)
- One `draft`/`pending` row → must be excluded
- One row from each channel source (`web`, `tiktok`, `marketplace_connect`, unknown)
- One bundle row

Add to `tests/conftest.py`:

```python
@pytest.fixture
def shopify_orders_fixture():
    return {"primary": FIXTURES_DIR / "shopify_orders.csv"}
```

Add `TestParseShopifyOrdersReport` to `tests/test_parsers.py` covering:
- Happy path, output columns, raw count
- `draft` orders excluded
- Channel mapping per source name
- Bundle expansion
- Per-channel bundle_stats
- Missing file returns `None` df

---

## Step 2 — Amazon ✅ Complete (2026-03-24)

### Download instructions

1. Log in to Amazon Seller Central
2. Navigate to **Orders → Order Reports → Request Report**
3. Select the desired date range and choose **"All statuses"**
4. Download the report — Amazon delivers it as a **tab-separated `.txt` file** with a random numeric filename (e.g. `153633910055020536.txt`)
5. Rename it to `Amazon_orders_YYYY-MM-DD.txt` using the export date
6. Drop it in `input/`

> **No format conversion needed.** The parser detects the `.txt` extension and reads it as TSV automatically. You only need to rename the file.

### Actual columns (verified against real export)

| Column | Purpose |
|--------|---------|
| `amazon-order-id` | Order ID |
| `sales-channel` | Filter: keep `Amazon.com` only (excludes MCF/Non-Amazon) |
| `order-status` | Filter: keep `Shipped` only |
| `sku` | MSKU → apply `AMAZON_SKU_MAP` directly (map already has trailing-`s` variants as keys) |
| `quantity` | Units per line item |
| `item-price` | Total revenue for the line (unit price × quantity, pre-multiplied by Amazon) |

> **Note on Pending orders:** Pending orders (not yet fulfilled) are excluded. They may still ship or cancel; counting them early would inflate sales figures inconsistently with the "shipped = sold" convention used across all channels.

> **Note on MCF:** The `Non-Amazon US` / `Non-Amazon` rows are Amazon fulfilling Shopify or other external orders. These are excluded via the `sales-channel == "Amazon.com"` filter.

### Implementation (complete)

- [x] Filter: `sales-channel == "Amazon.com"` (exclude MCF)
- [x] Filter: `order-status == "Shipped"` (exclude Cancelled and Pending)
- [x] Group by `sku`, sum `quantity` and `item-price` before mapping
- [x] For each MSKU call `_process_bundled_row(row, AMAZON_SKU_MAP, "sku", "quantity", "item-price", "Amazon Orders")`
- [x] Group by `SKU`, sum `Units` and `Revenue`
- [x] Track `bundle_stats = {"Units": ..., "Revenue": ...}`
- [x] Return `ParseResult(df=grouped, raw_count=raw_count, bundle_stats=bundle_stats)`

### PARSER_REGISTRY entry (active)

```python
{
    "channel": "Amazon",
    "parser": parsers.parse_amazon_orders_report,
    "files": {"primary": settings.AMAZON_ORDERS_PREFIX},
    "extensions": (".txt", ".csv"),   # Amazon downloads as .txt (TSV)
},
```

### Test fixture

`tests/fixtures/amazon_orders.csv` — 6 rows covering all filter cases:
- `ORD001` Shipped Amazon.com, sku=`1001` → keep
- `ORD002` Shipped Amazon.com, sku=`3001s` → maps to `3001` via `AMAZON_SKU_MAP`
- `ORD003` Cancelled → excluded
- `ORD004` Pending → excluded
- `ORD005` Shipped Non-Amazon US (MCF) → excluded
- `ORD_UNK` Shipped Amazon.com, unknown MSKU → excluded with warning

`TestParseAmazonOrdersReport` — 11 tests, all passing.

---

## Step 3 — Walmart (pending)

### Download instructions

1. Log in to Walmart Seller Center
2. Navigate to **Orders → Manage Orders**
3. Click **Export** → select CSV format
4. **Verify the column names against the table below before implementing** — Walmart's export format varies by account type
5. Rename the downloaded file: `Walmart_orders_YYYY-MM-DD.csv` (use the export date)
6. Drop it in `input/`

### Expected columns (verify against actual export — may vary)

| Column | Purpose |
|--------|---------|
| `Purchase Order ID` | Order ID |
| `Order Date` | Order date |
| `SKU` | Seller SKU → direct match to internal SKU (no map needed) |
| `Quantity` | Units |
| `Unit Price` | Unit price (Revenue = Quantity × Unit Price) |
| `Order Line Status` | Filter: keep `Acknowledged`, `Shipped`, `Delivered` only |

> **Note**: If the actual column names differ, update the stub in `src/parsers.py` before implementing.

### Implementation checklist

- [ ] Filter: exclude `Order Line Status` values containing `Cancelled` or `Refunded`
- [ ] Compute Revenue = `Quantity` × `Unit Price` per row
- [ ] Group by `SKU`, sum `Units` and `Revenue`
- [ ] No bundle expansion needed (Walmart SKUs map 1:1 to internal SKUs)
- [ ] Return `bundle_stats = {"Units": 0, "Revenue": 0}`
- [ ] Return `ParseResult(df=grouped, raw_count=raw_count, bundle_stats=bundle_stats)`

### PARSER_REGISTRY entry to swap in

Replace in `src/pipelines/sales.py`:

```python
# Before (current):
{
    "channel": "Walmart",
    "parser": parsers.parse_walmart_sales_report,
    "files": {"primary": settings.WALMART_SALES_PREFIX},
},

# After (EPIC-008 Step 3):
{
    "channel": "Walmart",
    "parser": parsers.parse_walmart_orders_report,
    "files": {"primary": settings.WALMART_ORDERS_PREFIX},
},
```

### Test fixture

Create `tests/fixtures/walmart_orders.csv` with at least:
- One `Shipped` row with a known Walmart SKU
- One `Cancelled` row → must be excluded
- Multiple rows for the same SKU → must aggregate

Add to `tests/conftest.py`:

```python
@pytest.fixture
def walmart_orders_fixture():
    return {"primary": FIXTURES_DIR / "walmart_orders.csv"}
```

Add `TestParseWalmartOrdersReport` to `tests/test_parsers.py` covering:
- Happy path, output columns, raw count
- `Cancelled` orders excluded
- Revenue computed as quantity × unit price
- Multiple rows for same SKU aggregated
- No bundle stats (Units=0, Revenue=0)
- Missing file returns `None` df

---

## Validation (after activating any channel)

After swapping in a new parser and running `uv run pytest -v`:

```bash
# Run in test mode (no webhook POST)
python main.py --test

# Compare totals against the prior run using the same date's input files
cat output/run_history.csv
```

Check `run_history.csv` for the new run row and compare `total_units` and `total_revenue` against the previous row for the same pipeline. Values should be close — significant differences indicate a parsing bug or a difference in how the old vs. new file defines "sold units".

Also check the pipeline log output for any `⚠️ Missing SKUs` warnings — these mean mapped SKUs are present in `SKU_ORDER` but not in the raw order file, which is expected if that SKU had zero sales in the period.

---

## Reference: TikTok Shop (complete)

For comparison when implementing other channels.

**Key differences from `parse_tiktok_orders_report` (FBT inventory parser):**

| | `parse_tiktok_orders_report` (inventory) | `parse_tiktok_shop_orders_report` (sales) |
|--|--|--|
| Fulfillment filter | FBT only | None — all non-cancelled |
| Revenue field | `Order Amount` (includes shipping/taxes) | `SKU Subtotal After Discount` (per-SKU net) |
| Output columns | `SKU`, `Units`, `Inventory`, `Inbound` | `SKU`, `Units`, `Revenue` |
| Bundle expansion | Yes (via `TIKTOK_ID_MAP`) | Yes (via `TIKTOK_ID_MAP`) |

Both parsers read the same `TikTok_orders_*.csv` file — this is intentional and mirrors how `Walmart_sales_*.csv` is shared between the WFS inventory parser and the Walmart sales parser.
