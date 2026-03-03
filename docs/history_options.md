# Run History & Data Continuity — Options Analysis

* **Status:** Draft for review (EPIC-007)
* **Date:** 2026-03-03
* **Author:** Claude Code (for Julio Cordero / NCL)

## Context

The pipeline currently saves one `inventory_report_YYYY-MM-DD.csv` and one `sales_report_YYYY-MM-DD.csv` per run to `output/`. There is no persistent record of prior runs beyond those files, and no structured way to detect anomalies, guard against double-processing, or track trends over time. The current manual habit is to open the daily CSV in Excel.

Three downstream use cases drive this evaluation:

| Use Case | What it needs |
|---|---|
| **Anomaly detection** | Compare today's totals against a recent baseline (e.g., 7-day average) |
| **Idempotency guard** | Know whether a given date has already been processed before re-running |
| **Trend comparison** | Plot or query per-SKU metrics across many dates |

---

## Option 1 — Extend the Existing Excel File

Append each run's output as a new dated worksheet or as new rows in a running sheet inside a single Excel workbook (e.g., `output/history.xlsx`).

**How the Excel workflow is affected:** This *is* the Excel workflow — no change in tooling. A user opens `history.xlsx` and sees all historical data.

**Tooling required to read it programmatically:** `openpyxl` or `pandas` with `xlsxwriter`/`openpyxl` engine. Both are available but not currently in the dependency list.

| Use Case | Supported? |
|---|---|
| Anomaly detection | Partial — possible in Excel formulas, awkward in Python without adding a dependency |
| Idempotency guard | Weak — requires parsing sheet names or scanning a column for today's date |
| Trend comparison | Yes — native Excel charts; Python requires the extra dep |

**Pros:** Zero new tooling for manual inspection; familiar format.

**Cons:** File grows without bound and eventually becomes slow. Concurrent writes are not safe. Requires adding `openpyxl` to the project. Sheet-per-run layout makes Python queries verbose; row-append layout risks schema drift across old and new runs.

---

## Option 2 — CSV Append Log

A running `output/run_history.csv` that accumulates one summary row per pipeline run. Each row contains run metadata (date, pipeline type, record count, aggregate totals, anomaly flags).

Example row:
```
date,pipeline,total_records,total_units,total_revenue,flagged
2026-03-03,inventory,192,4821,,
2026-03-03,sales,231,1204,87450.00,
```

**How the Excel workflow is affected:** `run_history.csv` opens directly in Excel. The per-day full-detail CSVs remain unchanged.

**Tooling required to read it:** None beyond `pandas`, which is already a project dependency. Excel opens CSVs natively.

| Use Case | Supported? |
|---|---|
| Anomaly detection | Yes — `df.tail(7).mean()` against today's row is a one-liner |
| Idempotency guard | Yes — check if today's date and pipeline are already in the file |
| Trend comparison | Yes — standard pandas/matplotlib on the CSV; also directly chartable in Excel |

**Pros:** No new dependencies. Git-diffable (one line added per run). Simple to implement: append a row in `data_handler.py` after saving outputs. Excel-compatible without conversion. Survives process crashes (rows are committed atomically per run).

**Cons:** Grows unbounded (one row per run per pipeline; at one run/day that is ~730 rows/year — trivially small). No relational querying across runs (no JOIN). Schema changes require a migration comment.

---

## Option 3 — SQLite Database

A local `output/history.db` SQLite database with a `runs` table (run metadata) and optionally a `records` table (full per-SKU data per run).

**How the Excel workflow is affected:** Requires a DB browser tool (e.g., DB Browser for SQLite, TablePlus) or a Python script to export to CSV/Excel. Not natively openable in Excel.

**Tooling required to read it:** `sqlite3` (stdlib — no new dep for Python), but a GUI tool for manual inspection.

| Use Case | Supported? |
|---|---|
| Anomaly detection | Yes — full SQL `SELECT AVG(...) WHERE date > ...` |
| Idempotency guard | Yes — `SELECT 1 WHERE date = ? AND pipeline = ?` |
| Trend comparison | Yes — SQL aggregations; requires export step for Excel |

**Pros:** Compact. Full SQL querying power. `sqlite3` is stdlib. Handles concurrent reads safely.

**Cons:** Not natively inspectable in Excel — breaks the existing manual workflow. Requires a separate viewer tool. Overkill for the current data volume (one summary row per run). Adds operational complexity (DB file must be backed up).

---

## Option 4 — JSON Log File

A `output/run_history.json` array where each element is a run summary object.

```json
[
  {
    "date": "2026-03-03",
    "pipeline": "inventory",
    "total_records": 192,
    "total_units": 4821
  }
]
```

**How the Excel workflow is affected:** Not directly openable in Excel. Requires conversion to CSV first (Power Query can do this, but it's a multi-step setup).

**Tooling required to read it:** `json` (stdlib). Excel requires Power Query or a conversion step.

| Use Case | Supported? |
|---|---|
| Anomaly detection | Yes — in Python; cumbersome in Excel |
| Idempotency guard | Yes — scan for matching date/pipeline entry |
| Trend comparison | Partial — Python only; no native Excel path |

**Pros:** Human-readable. Easy to parse in Python. No schema — flexible structure.

**Cons:** The entire file must be read and re-written to append (or a JSONL format used, which Excel cannot open at all). Not Excel-compatible without conversion. Grows unbounded with no natural size cap. No streaming-safe append.

---

## Option 5 — Keep Excel + Add a Lightweight CSV Log

Preserve the current Excel habit for the full daily output files (open `inventory_report_YYYY-MM-DD.csv` in Excel as before) and add a small `output/run_history.csv` that logs only run *metadata* — not the full record set.

This is a hybrid of the existing workflow and Option 2: the full data lives in per-day CSV files (Excel-compatible), and the history log is a compact metadata-only CSV that is also Excel-compatible.

**How the Excel workflow is affected:** No change to the daily CSV files. `run_history.csv` is an additive file, openable directly in Excel.

**Tooling required to read it:** None — `pandas` is already a dependency.

| Use Case | Supported? |
|---|---|
| Anomaly detection | Yes — compare today's summary row against recent rows |
| Idempotency guard | Yes — check for today's date + pipeline in the log |
| Trend comparison | Yes — plot summary metrics from the log; full detail available in per-day CSVs |

**Pros:** Zero disruption to the existing workflow. No new dependencies. The log stays small (metadata only). Full detail is still accessible per-day in the existing CSV outputs. Easiest to implement.

**Cons:** Trend queries on full per-SKU granularity (e.g., "show me SKU 1001 revenue across 90 days") require reading many individual CSV files — the history log alone won't answer them. That use case is served by `python main.py --combine`, which already exists (EPIC-006).

---

## Recommendation

**Option 5 — Keep Excel + Add a Lightweight CSV Log**

Reasoning:

1. **Zero disruption.** The per-day CSV files that are currently opened in Excel remain unchanged. Nothing is removed or renamed.

2. **No new dependencies.** `pandas` already handles CSV reading/writing. `openpyxl` or a DB browser tool are not needed.

3. **All three use cases are covered at the right granularity.** Anomaly detection and idempotency checks only need summary-level data per run (date, record count, total units/revenue). The metadata log provides exactly that. Full per-SKU trend data is served by `python main.py --combine`, which already produces `combined_inventory_report.csv`.

4. **Simple implementation.** One function call appending a row to `output/run_history.csv` after `save_outputs()` completes in `data_handler.py`. The schema is a handful of columns and is easily extended.

5. **Git-friendly.** One appended line per run is a clean, auditable diff.

### Proposed `run_history.csv` Schema

| Column | Description |
|---|---|
| `timestamp` | ISO datetime of the run (`2026-03-03T14:32:01`) |
| `pipeline` | `"inventory"` or `"sales"` |
| `report_date` | Date extracted from the source files |
| `total_records` | Number of SKU × channel rows written |
| `total_units` | Sum of all `Units` in the output |
| `total_revenue` | Sum of all `Revenue` (sales pipeline only; blank for inventory) |
| `source_files` | Comma-separated list of input filenames used |

This schema is sufficient for anomaly detection (compare `total_units` across recent rows), idempotency guard (check `report_date` + `pipeline`), and trend comparison (plot `total_units` or `total_revenue` over `report_date`).

---

*Decision required before implementation begins (per EPIC-007 DoD).*
