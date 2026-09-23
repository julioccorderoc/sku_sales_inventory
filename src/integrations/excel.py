"""Excel workbook access over the Microsoft Graph workbook API.

Four operations, which is all this project needs:

- `read_rows`     — every data row of a worksheet as a list of dicts keyed by
                    the sheet's own header row.
- `append_rows`   — append-only history writes.
- `upsert_rows`   — the snapshot tables: update the row whose key column
                    matches, insert a new row when it does not.
- `used_range`    — the raw grid, for callers that want positions not dicts.

Why the Graph workbook API and not a downloaded .xlsx: these workbooks are
live documents other people and tools read. Editing them in place is the whole
point, and a download-edit-upload cycle would clobber a concurrent edit.

Dates are written as Excel serial numbers, matching what the sheets already
hold — see `to_cell_value`.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Sequence

from src.integrations.graph import GraphClient, GraphError

logger = logging.getLogger(__name__)

EXCEL_EPOCH = date(1899, 12, 30)
"""Excel's day zero. Serial 1 = 1900-01-01 (the 1900 leap-year bug is why the
epoch sits two days before it rather than on 1899-12-31)."""


def date_to_serial(value: date | datetime) -> int:
    """`2026-09-23` → `46288`."""
    if isinstance(value, datetime):
        value = value.date()
    return (value - EXCEL_EPOCH).days


def serial_to_date(serial: float) -> date:
    """`46288` → `2026-09-23`."""
    return EXCEL_EPOCH + timedelta(days=int(serial))


def to_cell_value(value: Any) -> Any:
    """Coerce a Python value into something Graph writes as a real Excel value.

    Dates become serials on purpose: the target columns already hold serials,
    so a number round-trips exactly, where a string would depend on Excel
    deciding to parse it.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return value
    if isinstance(value, (datetime, date)):
        return date_to_serial(value)
    return value


def column_letter(index: int) -> str:
    """1-based column index → A1-notation letters (1 → A, 27 → AA)."""
    letters = ""
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


@dataclass
class UpsertResult:
    updated: int = 0
    inserted: int = 0
    total: int = 0


class Worksheet:
    """One sheet of one workbook, addressed by name."""

    def __init__(self, workbook: "ExcelWorkbook", name: str):
        self._workbook = workbook
        self.name = name

    @property
    def _path(self) -> str:
        escaped = self.name.replace("'", "''")
        return f"{self._workbook.base_path}/worksheets('{escaped}')"

    def used_range(self) -> dict:
        """The populated grid: `values` (2D), `rowCount`, `columnCount`."""
        result = self._workbook.client.get(
            f"{self._path}/usedRange", params={"$select": "values,rowCount,columnCount"}
        )
        return result or {"values": [], "rowCount": 0, "columnCount": 0}

    def headers(self) -> list[str]:
        """Row 1, trimmed to the populated columns."""
        grid = self.used_range()
        values = grid.get("values") or []
        if not values:
            return []
        return [str(cell).strip() if cell is not None else "" for cell in values[0]]

    def read_rows(self) -> list[dict[str, Any]]:
        """Every data row as a dict keyed by header.

        Columns with a blank header are skipped rather than keyed by "" — the
        history sheet carries four trailing blank columns and a `{"": ...}`
        key would collide across them.
        """
        grid = self.used_range()
        values = grid.get("values") or []
        if len(values) < 2:
            return []

        raw_headers = values[0]
        columns = [
            (idx, str(cell).strip())
            for idx, cell in enumerate(raw_headers)
            if cell is not None and str(cell).strip()
        ]

        rows: list[dict[str, Any]] = []
        for raw_row in values[1:]:
            if all(cell is None or cell == "" for cell in raw_row):
                continue
            row: dict[str, Any] = {}
            for idx, header in columns:
                row[header] = raw_row[idx] if idx < len(raw_row) else ""
            rows.append(row)
        return rows

    def write_range(self, start_row: int, values: Sequence[Sequence[Any]]) -> None:
        """PATCH a rectangular block starting at column A of `start_row`."""
        if not values:
            return
        last_row = start_row + len(values) - 1
        width = max(len(row) for row in values)
        address = f"A{start_row}:{column_letter(width)}{last_row}"
        padded = [list(row) + [""] * (width - len(row)) for row in values]
        self._workbook.client.patch(
            f"{self._path}/range(address='{address}')", json_body={"values": padded}
        )

    def append_rows(self, rows: Iterable[dict[str, Any]]) -> int:
        """Append dicts to the bottom of the sheet, mapped onto its headers."""
        rows = list(rows)
        if not rows:
            return 0

        grid = self.used_range()
        headers = [
            str(cell).strip() if cell is not None else "" for cell in (grid.get("values") or [[]])[0]
        ]
        if not headers:
            raise GraphError(f"Worksheet '{self.name}' has no header row to map onto.")

        width = max(len(headers), grid.get("columnCount") or 0)
        values = [self._row_values(headers, row, width) for row in rows]
        self.write_range(_last_data_row(grid.get("values") or []) + 1, values)
        return len(values)

    def upsert_rows(self, key: str, rows: Iterable[dict[str, Any]]) -> UpsertResult:
        """Match on `key`; update the hit, append the miss.

        Matched rows are written back to the exact rows they came from, and
        contiguous matches are batched into one call — the snapshot tables are
        a stable key set, so this is normally a single PATCH.
        """
        rows = list(rows)
        if not rows:
            return UpsertResult()

        grid = self.used_range()
        values = grid.get("values") or []
        if not values:
            raise GraphError(f"Worksheet '{self.name}' is empty; nothing to match on.")

        headers = [str(cell).strip() if cell is not None else "" for cell in values[0]]
        if key not in headers:
            raise GraphError(
                f"Worksheet '{self.name}' has no '{key}' column "
                f"(found: {', '.join(h for h in headers if h)})"
            )
        key_idx = headers.index(key)
        width = max(len(headers), grid.get("columnCount") or 0)

        existing: dict[str, int] = {}
        for offset, raw_row in enumerate(values[1:], start=2):
            if key_idx < len(raw_row) and raw_row[key_idx] not in (None, ""):
                existing[str(raw_row[key_idx])] = offset

        result = UpsertResult(total=len(rows))
        updates: dict[int, list[Any]] = {}
        appends: list[list[Any]] = []

        for row in rows:
            row_key = str(row.get(key, ""))
            target = existing.get(row_key)
            mapped = self._row_values(headers, row, width)
            if target is None:
                appends.append(mapped)
            else:
                updates[target] = mapped

        for block in _contiguous_blocks(sorted(updates)):
            self.write_range(block[0], [updates[row] for row in block])

        result.updated = len(updates)
        if appends:
            self.write_range(_last_data_row(values) + 1, appends)
            result.inserted = len(appends)
        return result

    @staticmethod
    def _row_values(headers: list[str], row: dict[str, Any], width: int) -> list[Any]:
        values = [to_cell_value(row.get(header)) if header else "" for header in headers]
        if width > len(values):
            values.extend([""] * (width - len(values)))
        return values


def _contiguous_blocks(sorted_rows: list[int]) -> list[list[int]]:
    """Group ascending row numbers into runs: [2,3,4,9] → [[2,3,4],[9]]."""
    blocks: list[list[int]] = []
    for row in sorted_rows:
        if blocks and row == blocks[-1][-1] + 1:
            blocks[-1].append(row)
        else:
            blocks.append([row])
    return blocks


def _last_data_row(values: list[list[Any]]) -> int:
    """1-based number of the last row holding any value.

    Deliberately not `usedRange.rowCount`: a formatted-but-empty trailing row
    counts as "used" to Excel, and appending below it would leave a silent gap
    of blank rows in the middle of the history sheet.
    """
    last = 0
    for index, row in enumerate(values, start=1):
        if any(cell not in (None, "") for cell in row):
            last = index
    return last


class ExcelWorkbook:
    """A workbook addressed by driveItem id, on a named user's drive.

    The id is the same value the previous n8n workflow used as its workbook
    reference, so no lookup or migration is needed.
    """

    def __init__(self, client: GraphClient, item_id: str, owner: str):
        self.client = client
        self.item_id = item_id
        self.owner = owner

    @property
    def base_path(self) -> str:
        return f"/users/{self.owner}/drive/items/{self.item_id}/workbook"

    def worksheet(self, name: str) -> Worksheet:
        return Worksheet(self, name)

    def worksheet_names(self) -> list[str]:
        listing = self.client.get(f"{self.base_path}/worksheets", params={"$select": "name"})
        return [sheet["name"] for sheet in (listing or {}).get("value", [])]

    def exists(self) -> bool:
        """Resolve the workbook itself — a cheap credentials + id smoke test."""
        return self.client.get_optional(f"/users/{self.owner}/drive/items/{self.item_id}") is not None
