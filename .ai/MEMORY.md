# Project Memory

Stable patterns and conventions confirmed during development. Update when new patterns
are established or existing ones change.

---

## Test Suite (EPIC-001)

* Run tests: `uv run pytest tests/ -v`
* 121 tests across `test_utils.py`, `test_parsers.py`, `test_pipelines.py`
* Fixtures live in `tests/fixtures/` — 12 minimal CSV files, one per parser input
* `conftest.py` provides file-path fixtures and pre-built DataFrames for pipeline tests

### Critical fixture gotcha — SKU dtype mismatch

Fixture CSVs that contain only integer-looking SKUs (e.g., `1001`, `2001`) are read by
pandas as `int64`. `SKU_ORDER` in settings is a list of strings. A `pd.merge` on mixed
`int64`/`object` columns silently produces no matches. **Fix:** always include at least
one string-format SKU (e.g., `PH1001`) in any fixture that feeds a parser using a merge.

### `load_csv(None)` is unsafe (ERR-001)

Never pass `None` to `load_csv`. In tests, use a nonexistent `Path` object to exercise
the "file not found" branch safely.

---

## Parser contracts

* **Inventory parsers** return `pd.DataFrame | None`
* **Sales parsers** return `(df, bundle_stats, raw_count)` tuple — `df` may be `None`
* Column naming at the parser boundary is inconsistent (snake_case for inventory,
  CamelCase for sales) — this is a known divergence, tracked for EPIC-004

---

## Configuration

* All SKU/channel mappings live in `config/mappings.json`
* `SKU_ORDER` is the master list of 32 internal SKUs — all outputs zero-fill to this list
* `CHANNEL_ORDER` and `SALES_CHANNEL_ORDER` define the fixed output ordering
* Trailing `"s"` on Amazon SKUs is stripped at parse time (e.g., `"3001s"` → `"3001"`)

---

## ID format

```python
id             = f"{date}_{channel}_{sku}"     # e.g., "20260226_FBA_1001"
sku_channel_id = f"{channel}_{sku}"            # e.g., "FBA_1001"
```

Spaces in channel names are replaced with underscores in IDs (e.g., `TikTok_Shop_1001`).

---

## Workflow conventions

* Package manager: `uv` — always use `uv sync` / `uv run`, never `pip`
* Dev dependencies install: `uv sync --extra dev`
* Test mode (no webhook POST): `python main.py --test`
* `.ai/CURRENT_PLAN.md` — updated at the start and end of every epic
* `.ai/ERRORS.md` — log bugs as they are discovered; resolve entries when fixed
* `docs/roadmap.md` — update epic status when work starts (`Active`) and finishes (`Complete`)
