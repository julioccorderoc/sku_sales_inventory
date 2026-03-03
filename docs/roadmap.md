# ROADMAP

* **Version:** 0.1.0
* **Last Updated:** 2026-03-02
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

* **Status:** Active (2026-03-03)
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

* **Status:** Pending
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

* **Status:** Pending
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

* **Status:** Pending
* **Dependencies:** EPIC-006 (combine script must be integrated before history layer is built on top of it)
* **Business Objective:** Enable anomaly detection, idempotent runs, and trend reporting — all of which require a persistent record of prior pipeline runs to compare against. The current workflow saves output data to Excel manually; any solution must integrate with or extend that existing habit.
* **Technical Boundary:** Produce a decision document at `docs/history_options.md` — no code. Evaluate the viable storage approaches given the current Excel-based workflow, and provide a clear recommendation. The document should cover at minimum:
    1. **Extend the existing Excel file** — append each run as a new dated sheet or rows; pros: zero new tooling, familiar format; cons: file size growth, no programmatic querying.
    2. **CSV append log** — a running `output/run_history.csv` that accumulates one row per pipeline run; pros: simple, git-diffable, readable in Excel; cons: no structured querying.
    3. **SQLite database** — a local `output/history.db`; pros: queryable, compact, no server required; cons: requires a DB browser tool to inspect manually.
    4. **JSON log file** — a `output/run_history.json` array; pros: human-readable, easy to parse in Python; cons: grows unbounded, no native Excel integration.
    5. **Keep Excel + add a lightweight log** — combine the existing Excel habit for the output data with a small structured log (CSV or JSON) for run metadata only (timestamps, record counts, anomaly flags).
* **Verification Criteria (Definition of Done):**
  * `docs/history_options.md` exists and covers all five approaches above.
  * Each option includes a concrete assessment of: how the current Excel workflow is affected, what tooling (if any) is needed to read it, and whether it supports the three downstream use cases (anomaly detection, idempotency guard, trend comparison).
  * The document ends with a clear recommendation and the reasoning behind it.
  * The document is reviewed and a storage approach is chosen before any implementation begins.
