# Current Plan

* **Active Epic:** EPIC-008 — Migrate Sales Parsers to Raw Order Reports (in progress)
* **Last Completed:** EPIC-007 — Run History & Data Continuity (2026-03-24)

## EPIC-008 Progress

### Step 4 — TikTok Shop ✅ Complete (2026-03-24)

`TikTok_orders_*.csv` confirmed as row-per-order (48 cols). The existing `parse_tiktok_orders_report` reads the same file for FBT inventory (filters for FBT fulfillment only). New sales parser keeps all non-cancelled orders regardless of fulfillment type.

**Changes made:**

* `src/parsers.py` — Added `parse_tiktok_shop_orders_report()` (no fulfillment filter, revenue = `SKU Subtotal After Discount`). Marked `parse_tiktok_sales_report` as LEGACY. Added three documented stubs for Steps 1–3.
* `src/pipelines/sales.py` — PARSER_REGISTRY TikTok Shop entry now uses `parse_tiktok_shop_orders_report` + `settings.TIKTOK_ORDERS_PREFIX`.
* `src/settings.py` — Added `SHOPIFY_ORDERS_PREFIX`, `AMAZON_ORDERS_PREFIX`, `WALMART_ORDERS_PREFIX` scaffolding constants.
* `tests/fixtures/tiktok_shop_orders.csv` — New fixture (5 rows: FBT + Seller Shipping + cancelled + bundle cases).
* `tests/conftest.py` — Added `tiktok_shop_orders_fixture`.
* `tests/test_parsers.py` — Added `TestParseTikTokShopOrdersReport` (11 tests, all passing).
* `tests/test_pipelines.py` — Updated outdated `test_negative_inventory_fails_validation` → `test_negative_inventory_clipped_to_zero`.
* `docs/epic008_raw_orders.md` — Migration guide created.
* `docs/roadmap.md` — EPIC-008 status updated.

**Test suite:** 153 passed, 0 failed.

---

### Steps 1–3 — Shopify, Amazon, Walmart ⏳ Pending

Stubs are implemented and documented in `src/parsers.py`. Each stub contains:
- Download instructions for the raw order file
- Expected column names
- Exact implementation steps
- The PARSER_REGISTRY entry to swap in when ready

Full per-channel guide: `docs/epic008_raw_orders.md`

**To activate a channel when its raw order file becomes available:**

1. Download the raw order export and drop in `input/` with the correct prefix
2. Implement the stub (docstring has all the details)
3. Create the test fixture in `tests/fixtures/` and add to `tests/conftest.py`
4. Add a test class to `tests/test_parsers.py`
5. Swap the PARSER_REGISTRY entry in `src/pipelines/sales.py`
6. Run `uv run pytest -v` — all tests must pass
7. Run `python main.py --test` and compare `output/run_history.csv` totals against the prior run
