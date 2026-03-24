# Known Errors & Bugs

Bugs discovered during development. Each entry includes status and the epic/session it was found in.

---

## Active (Unresolved)

### ERR-002 — `clean_money()` raises `ValueError` on arbitrary non-numeric strings

* **Found:** EPIC-001 test suite
* **Location:** `src/utils.py` → `clean_money()`
* **Symptom:** `clean_money("N/A")` raises `ValueError: could not convert string to float: 'N/A'`.
  The function only handles `None` and empty string gracefully — any other non-numeric
  string (e.g., `"N/A"`, `"-"`, `"—"`) causes an unhandled exception.
* **Root cause:** The function strips `$` and `,`, then passes the result directly to
  `float()` with no try/except.
* **Risk:** Low for now — current CSV sources don't produce these values — but a future
  format change could introduce them silently.
* **Resolution:** Consider adding a `try/except ValueError` fallback to return `0.0`.
  Out of scope until a failing case appears in production.

---

## Resolved

### ERR-001 — `load_csv(None)` crashes in error handler

* **Found:** EPIC-001 test suite
* **Location:** `src/utils.py` → `load_csv()`
* **Resolved:** EPIC-004 — Added `if file_path is None: return None` guard at the top of `load_csv`.

---

### ERR-003 — Pydantic class-based `config` is deprecated

* **Found:** EPIC-001 test suite (warnings)
* **Location:** `src/schemas.py`
* **Resolved:** EPIC-004 — Migrated all three models (`InventoryItem`, `SalesItem`, `SalesRecord`)
  from `class Config: populate_by_name = True` to `model_config = ConfigDict(populate_by_name=True)`.

---

### ERR-004 — pandas `fillna` downcasting deprecated

* **Found:** EPIC-001 test suite (warnings)
* **Location:** `src/pipelines/sales.py`
* **Resolved:** EPIC-004 — The empty-bundle fallback DataFrame now uses explicitly typed columns
  (`pd.Series(dtype=float)`) so the merge result is never object-typed. No downcasting occurs
  and the `FutureWarning` no longer fires.
