# ROADMAP

* **Version:** 0.1.0
* **Last Updated:** 2026-03-24
* **Primary Human Owner:** Julio Cordero

## Operating Rules for the Planner Agent

1. You may only move one Epic to `Active` at a time.
2. Before marking an Epic `Complete`, you must verify all its Success Criteria are met in the main branch.
3. Do not parse or extract Epics that depend on incomplete prerequisites.

---

## Epic Ledger

### EPIC-001 — Automated Test Suite

* **Status:** Complete (2026-03-02)
* **Dependencies:** None
* **Business Objective:** Catch parser breakage immediately when seller platforms change their CSV export formats, preventing silent data corruption from reaching the webhook and downstream n8n workflows.
* **Technical Boundary:** Add a `tests/` directory with pytest. Cover all parsers in `src/parsers.py` using fixture CSV files, and the core transform logic in both pipelines. Do not test the webhook POST or file I/O in unit tests — those belong in integration tests.
* **Verification Criteria (Definition of Done):**
  * `pytest` runs successfully with no failures from the project root.
  * Each parser function in `src/parsers.py` has at least one test using a fixture CSV that reflects the real format (including encoding, dynamic headers, bundle rows).
  * The zero-fill template logic in both `InventoryPipeline.transform` and `SalesPipeline.transform` is covered with assertions that every SKU × channel combination is present in the output.
  * Missing-file and bad-data edge cases (empty DataFrame, unrecognized SKU) are tested and confirmed to produce the expected logged warnings rather than exceptions.
  * A `pyproject.toml` test configuration block is added so `uv run pytest` works without manual path setup.

---

### EPIC-002 — Platform API Integration Guide

* **Status:** Complete (2026-03-03)
* **Dependencies:** None
* **Business Objective:** Eliminate the manual CSV download step for all channels by documenting the exact API connection path for each platform, enabling future automation of the extract phase.
* **Technical Boundary:** Produce a single reference document at `docs/api_integrations.md`. No code changes. All five channels currently read from manually-downloaded CSVs — including Flexport, which has API credentials in `.env` but no API integration in the codebase yet. Cover: Flexport (Logistics API, credentials already in `.env`), Amazon (SP-API), Walmart (Seller API), TikTok Shop (Open Platform API), and Shopify (Admin API).
* **Verification Criteria (Definition of Done):**
  * `docs/api_integrations.md` exists and covers all five platforms.
  * For each platform, the document includes: authentication method (OAuth, API key, etc.), the specific endpoint(s) needed to replicate the current CSV export data, required scopes/permissions, known rate limits, and a minimal Python `requests` code example showing the auth flow and a sample API call.
  * For Flexport specifically, the doc maps the existing `.env` credentials (`FLEXPORT_API_KEY`, `FLEXPORT_ACCESS_TOKEN`, `BASE_URL`) to the correct request headers and endpoints, since the groundwork is already laid.
  * The document notes any platforms that require app review or seller approval before API access is granted.
  * The document is reviewed and confirmed accurate against each platform's current developer docs.

---

### EPIC-003 — Split Mappings Configuration

* **Status:** Complete (2026-03-03)
* **Dependencies:** None
* **Business Objective:** Make SKU and channel mappings easier to maintain as the catalog grows, so a new SKU or bundle definition can be added without touching a single large file that mixes all channel concerns.
* **Technical Boundary:** Split `config/mappings.json` into per-channel files. All existing keys stay intact; only the file layout changes. `src/settings.py` is updated to load from the new paths. No pipeline logic changes.
* **Verification Criteria (Definition of Done):**
  * `config/mappings.json` is removed and replaced by individual files, e.g. `config/amazon_map.json`, `config/tiktok_map.json`, `config/shopify_map.json`, `config/flexport_map.json`, and `config/catalog.json` (for `SKU_ORDER`, `AMAZON_SKUs`, `CHANNEL_ORDER`, `SALES_CHANNEL_ORDER`).
  * `src/settings.py` loads each file independently and exposes the same constant names as before (`DSKU_TO_SKU_MAP`, `TIKTOK_ID_MAP`, etc.) so no other file needs to change.
  * The full pipeline runs cleanly with `python main.py --test` and produces identical output to a run against the old single file.
  * CLAUDE.md is updated to reflect the new config structure.

---

### EPIC-004 — Standardize Pipeline Architecture

* **Status:** Complete (2026-03-03)
* **Dependencies:** EPIC-001 (tests must exist before a refactor of this scope)
* **Business Objective:** Reduce cognitive overhead when adding new channels. A developer adding a new inventory source and a new sales channel should follow the exact same pattern and registry contract.
* **Technical Boundary:** Normalize the divergences between `InventoryPipeline` and `SalesPipeline` without changing output schemas or business logic. Specific issues to resolve:
    1. **Registry key names differ** — Inventory uses `channel_name`/`parser_func`/`required_files`; Sales uses `channel`/`func`/`file`. Standardize to a single shape.
    2. **Parser return signature differs** — Inventory parsers return `pd.DataFrame | None`; Sales parsers return a `(df, bundle_stats, raw_count)` tuple. Adopt a consistent return contract (e.g., a `ParseResult` dataclass).
    3. **Stats and missing-SKU logging only exists in Sales** — Move to the shared `extract` loop or base class so Inventory gets the same visibility.
    4. **Column naming in parsers is inconsistent** — Inventory parsers return lowercase snake_case (`channel`, `sku`, `units_sold`); Sales parsers return CamelCase (`Channel`, `SKU`, `Units`). Standardize to one convention at the parser boundary.
    5. **Bundle state leaks from `extract` to `transform` via `self.bundle_rows`** — This breaks the ETL contract where each phase is independent. Bundle data should be returned from `extract` as part of its output (e.g., alongside the main DataFrame) rather than stored as instance state.
    6. **Channel template forcing is inconsistent** — Inventory zero-fills only channels found in data; Sales always forces the full `SALES_CHANNEL_ORDER` list. Decide on one policy and apply it to both (forcing the full configured list is the safer default).
* **Verification Criteria (Definition of Done):**
  * Both pipelines use an identical registry entry shape.
  * All parsers in `src/parsers.py` return the same type.
  * `self.bundle_rows` is removed as instance state; bundle data flows through the ETL return values.
  * Stats logging (rows analyzed, SKUs found, missing SKUs) appears for every channel in both pipelines.
  * CLAUDE.md "Adding a New Channel" section reflects the updated pattern.
  * All EPIC-001 tests pass against the refactored code.

---

### EPIC-005 — Webhook Retry Logic

* **Status:** Complete (2026-03-03)
* **Dependencies:** None
* **Business Objective:** Prevent data loss when the n8n webhook is transiently unreachable, avoiding the need to re-run the entire pipeline because of a momentary network issue.
* **Technical Boundary:** Changes are isolated to `src/data_handler.py`. Add retry with exponential backoff on the `post_to_webhook` function. No changes to pipeline logic or output file generation.
* **Verification Criteria (Definition of Done):**
  * `post_to_webhook` retries on HTTP 5xx responses and connection errors, up to a configurable maximum (default: 3 attempts).
  * Retry delays follow exponential backoff (e.g., 2s, 4s, 8s).
  * After all retries are exhausted, the error is logged clearly with the final status code and the pipeline exits without raising an unhandled exception.
  * A `WEBHOOK_MAX_RETRIES` and `WEBHOOK_RETRY_BACKOFF` setting is added to `src/settings.py` with documented defaults.
  * Retry behavior is covered by a unit test using a mocked `requests.post`.

---

### EPIC-006 — Integrate `combine_inventory.py`

* **Status:** Complete (2026-03-03)
* **Dependencies:** None
* **Business Objective:** Make historical inventory aggregation a first-class operation accessible through `main.py` rather than a disconnected utility script.
* **Technical Boundary:** Fold the logic of `combine_inventory.py` into `main.py` behind a `--combine` flag. The original script can remain in place or be removed once the flag is confirmed working.
* **Verification Criteria (Definition of Done):**
  * `python main.py --combine` produces the same output as running `python combine_inventory.py` directly on the same input set.
  * `--combine` can be run standalone (without also triggering the inventory/sales pipelines) and in combination, e.g., `python main.py --combine --test`.
  * `combine_inventory.py` is either removed from the repo root or clearly marked as deprecated.
  * CLAUDE.md commands section is updated with the `--combine` flag.

---

### EPIC-007 — Run History & Data Continuity

* **Status:** Complete (2026-03-24)
* **Dependencies:** EPIC-006 (combine script must be integrated before history layer is built on top of it)
* **Business Objective:** Enable anomaly detection, idempotent runs, and trend reporting — all of which require a persistent record of prior pipeline runs to compare against. The current workflow saves output data to Excel manually; any solution must integrate with or extend that existing habit.
* **Implementation:** Option 5 — Keep Excel + Add a Lightweight CSV + JSON Log. After every pipeline run, `data_handler.log_run_history()` appends one metadata row to `output/run_history.csv` (Excel-compatible) and updates `output/run_history.json` (programmatic lookups). Schema: `timestamp`, `pipeline`, `report_date`, `total_records`, `total_units`, `total_revenue`, `source_files`. No new dependencies.
* **Verification Criteria (Definition of Done):**
  * `docs/history_options.md` exists and covers all five approaches. ✅
  * Each option assessed against Excel workflow impact, tooling, and all three downstream use cases. ✅
  * Document ends with a clear recommendation. ✅
  * Storage approach chosen and implemented: `output/run_history.csv` + `output/run_history.json` written by `data_handler.log_run_history()`. ✅
  * 12 tests added to `tests/test_data_handler.py::TestLogRunHistory`, all passing. ✅

---

### EPIC-008 — Migrate Sales Parsers to Raw Order Reports

* **Status:** In Progress (2026-03-24) — Steps 2 & 4 complete; Steps 1 & 3 scaffolded pending raw files
* **Dependencies:** EPIC-007 (run history log must exist so each migration step can be validated by comparing aggregated output against the previous baseline)
* **Business Objective:** Replace pre-aggregated platform summary CSVs with raw order-level exports so the pipeline controls its own aggregation logic. This yields consistent calculations across channels, enables deduplication by order ID, supports flexible date-range re-aggregation, and provides the order-level granularity needed for meaningful anomaly detection.
* **Technical Boundary:** Migrate one sales channel at a time. Each step replaces the existing parser for that channel with a new one that reads raw order rows and aggregates them to the same `[Channel, SKU, Units, Revenue]` output schema. The pipeline output format and all downstream consumers (webhook, n8n) must remain unchanged throughout. This epic is intentionally phased — each step is independently shippable and must be validated before the next begins.
* **Implementation Steps:**
    1. **Step 1 — Shopify** *(pending — raw file not yet available)*
        * Stub in `src/parsers.py`: `parse_shopify_orders_report()` — fully documented.
        * See `docs/epic008_raw_orders.md` for download instructions, expected columns, and implementation checklist.
    2. **Step 2 — Amazon** ✅ **Complete (2026-03-24)**
        * `Amazon_orders_*.txt` confirmed as tab-separated, row-per-order-line-item (36 cols).
        * Filters: `sales-channel == "Amazon.com"` (excludes MCF) + `order-status == "Shipped"` (excludes Cancelled/Pending).
        * `find_latest_report` extended to accept `extensions` tuple — Amazon entry uses `(".txt", ".csv")` so `.txt` downloads are found without format conversion.
        * `parse_amazon_sales_report` remains in codebase (kept for reference); no longer in registry.
        * PARSER_REGISTRY updated; 11 tests added and passing.
    3. **Step 3 — Walmart** *(pending — raw file not yet available; confirm export column names before implementing)*
        * Stub in `src/parsers.py`: `parse_walmart_orders_report()` — fully documented.
        * See `docs/epic008_raw_orders.md` for download instructions, expected columns, and implementation checklist.
    4. **Step 4 — TikTok Shop** ✅ **Complete (2026-03-24)**
        * `TikTok_orders_*.csv` confirmed as row-per-order (48 cols).
        * Added `parse_tiktok_shop_orders_report()` — no fulfillment filter, revenue = `SKU Subtotal After Discount`.
        * `parse_tiktok_sales_report` marked LEGACY (kept for reference, no longer in registry).
        * PARSER_REGISTRY updated; 11 tests added and passing.
* **Verification Criteria (Definition of Done):**
  * All four sales channels read raw order-level exports.
  * For each channel, a validation run confirmed that aggregated output on the same input date matched the pre-migration summary-based output within a documented tolerance.
  * No changes to the output schema (`SalesRecord` Pydantic model), column names, or webhook payload.
  * Each channel has an updated fixture CSV reflecting the raw order format and a passing parser test.
  * `CLAUDE.md` input file naming table is updated with the new raw-order filename prefixes.
