"""Excel worksheet access — row mapping and upsert mechanics, no network."""
from datetime import date

import pytest

from src.integrations.excel import (
    ExcelWorkbook,
    GraphError,
    UpsertResult,
    Worksheet,
    column_letter,
    date_to_serial,
    serial_to_date,
    to_cell_value,
)


class TestSerialConversion:
    def test_known_serial(self):
        assert date_to_serial(date(2026, 9, 23)) == 46288
        assert serial_to_date(46288) == date(2026, 9, 23)

    def test_round_trip(self):
        for day in (date(2025, 9, 8), date(2026, 1, 14), date(2026, 7, 20)):
            assert serial_to_date(date_to_serial(day)) == day

    def test_cell_value_converts_dates(self):
        assert to_cell_value(date(2026, 9, 23)) == 46288
        assert to_cell_value(None) == ""
        assert to_cell_value("1001") == "1001"
        assert to_cell_value(12.5) == 12.5


class TestColumnLetter:
    @pytest.mark.parametrize(
        "index,expected",
        [(1, "A"), (2, "B"), (8, "H"), (26, "Z"), (27, "AA"), (28, "AB"), (52, "AZ"), (53, "BA")],
    )
    def test_letters(self, index, expected):
        assert column_letter(index) == expected


class FakeGraphClient:
    """Records every call and answers GETs from a path-substring map."""

    def __init__(self, responses: dict[str, dict] | None = None):
        self.responses = responses or {}
        self.calls: list[tuple] = []

    def _lookup(self, path: str) -> dict:
        for fragment, payload in self.responses.items():
            if fragment in path:
                return payload
        return {}

    def get(self, path, params=None):
        self.calls.append(("GET", path, params))
        return self._lookup(path)

    def get_optional(self, path, params=None):
        return self.get(path, params)

    def post(self, path, json_body=None):
        self.calls.append(("POST", path, json_body))
        return {}

    def patch(self, path, json_body=None):
        self.calls.append(("PATCH", path, json_body))
        return {}


def _worksheet(grid: dict, client: FakeGraphClient | None = None) -> tuple[Worksheet, FakeGraphClient]:
    client = client or FakeGraphClient({"usedRange": grid})
    return Worksheet(ExcelWorkbook(client, "ITEM", "owner@example.com"), "raw_inventory"), client


HEADERS = ["id", "sku_channel_id", "Date", "SKU", "Channel", "Units", "Inventory", "Inbound"]


class TestReadRows:
    def test_rows_are_keyed_by_header(self):
        worksheet, _ = _worksheet({
            "values": [HEADERS, ["20260923_FBA_1001", "FBA_1001", 46288, "1001", "FBA", 381, 1947, 0]],
            "rowCount": 2, "columnCount": 8,
        })

        rows = worksheet.read_rows()

        assert rows == [{
            "id": "20260923_FBA_1001", "sku_channel_id": "FBA_1001", "Date": 46288,
            "SKU": "1001", "Channel": "FBA", "Units": 381, "Inventory": 1947, "Inbound": 0,
        }]

    def test_blank_header_columns_are_skipped(self):
        worksheet, _ = _worksheet({
            "values": [HEADERS + ["", ""], ["a", "FBA_1001", 46288, "1001", "FBA", 1, 2, 3, "", ""]],
            "rowCount": 2, "columnCount": 10,
        })

        row = worksheet.read_rows()[0]

        assert "" not in row
        assert row["Inbound"] == 3

    def test_blank_rows_are_dropped(self):
        worksheet, _ = _worksheet({
            "values": [HEADERS, [""] * 8, ["a", "FBA_1001", 46288, "1001", "FBA", 1, 2, 3]],
            "rowCount": 3, "columnCount": 8,
        })

        assert len(worksheet.read_rows()) == 1

    def test_header_only_sheet_has_no_rows(self):
        worksheet, _ = _worksheet({"values": [HEADERS], "rowCount": 1, "columnCount": 8})

        assert worksheet.read_rows() == []


class TestAppendRows:
    def test_appends_below_the_last_row(self):
        worksheet, client = _worksheet({
            "values": [HEADERS, ["a", "FBA_1001", 46288, "1001", "FBA", 1, 2, 3]],
            "rowCount": 2, "columnCount": 8,
        })

        appended = worksheet.append_rows([{
            "id": "b", "sku_channel_id": "FBA_1002", "Date": date(2026, 9, 23),
            "SKU": "1002", "Channel": "FBA", "Units": 5, "Inventory": 6, "Inbound": 7,
        }])

        assert appended == 1
        method, path, body = client.calls[-1]
        assert method == "PATCH"
        assert "range(address='A3:H3')" in path
        assert body["values"] == [["b", "FBA_1002", 46288, "1002", "FBA", 5, 6, 7]]

    def test_missing_fields_become_blank_cells(self):
        worksheet, client = _worksheet({
            "values": [HEADERS, ["a", "FBA_1001", 46288, "1001", "FBA", 1, 2, 3]],
            "rowCount": 2, "columnCount": 8,
        })

        worksheet.append_rows([{"sku_channel_id": "FBA_1002"}])

        assert client.calls[-1][2]["values"] == [["", "FBA_1002", "", "", "", "", "", ""]]

    def test_no_rows_means_no_call(self):
        worksheet, client = _worksheet({
            "values": [HEADERS], "rowCount": 1, "columnCount": 8,
        })

        assert worksheet.append_rows([]) == 0
        assert client.calls == []

    def test_trailing_formatted_blank_rows_are_not_skipped_over(self):
        """`usedRange` counts a formatted empty row as used; appending below one
        would leave a silent gap in the history."""
        worksheet, client = _worksheet({
            "values": [
                HEADERS,
                ["a", "FBA_1001", 46288, "1001", "FBA", 1, 2, 3],
                [""] * 8,
                [""] * 8,
            ],
            "rowCount": 4, "columnCount": 8,
        })

        worksheet.append_rows([{"sku_channel_id": "FBA_1002"}])

        assert "range(address='A3:H3')" in client.calls[-1][1]


class TestUpsertRows:
    def _sheet(self):
        return _worksheet({
            "values": [
                HEADERS,
                ["a", "FBA_1001", 46288, "1001", "FBA", 1, 2, 3],
                ["b", "FBA_1002", 46288, "1002", "FBA", 4, 5, 6],
            ],
            "rowCount": 3, "columnCount": 8,
        })

    def test_matched_row_updates_in_place(self):
        worksheet, client = self._sheet()

        result = worksheet.upsert_rows("sku_channel_id", [
            {"sku_channel_id": "FBA_1001", "Units": 99},
        ])

        assert result == UpsertResult(updated=1, inserted=0, total=1)
        method, path, body = client.calls[-1]
        assert "range(address='A2:H2')" in path
        assert body["values"][0][5] == 99

    def test_unmatched_row_is_appended(self):
        worksheet, client = self._sheet()

        result = worksheet.upsert_rows("sku_channel_id", [
            {"sku_channel_id": "FBA_9999", "Units": 7},
        ])

        assert result == UpsertResult(updated=0, inserted=1, total=1)
        method, path, body = client.calls[-1]
        assert "range(address='A4:H4')" in path
        assert body["values"][0][1] == "FBA_9999"

    def test_contiguous_matches_are_batched_into_one_call(self):
        worksheet, client = self._sheet()

        result = worksheet.upsert_rows("sku_channel_id", [
            {"sku_channel_id": "FBA_1001", "Units": 10},
            {"sku_channel_id": "FBA_1002", "Units": 20},
        ])

        assert result.updated == 2
        patches = [call for call in client.calls if call[0] == "PATCH"]
        assert len(patches) == 1
        assert "range(address='A2:H3')" in patches[0][1]
        assert [row[5] for row in patches[0][2]["values"]] == [10, 20]

    def test_non_contiguous_matches_split_into_separate_calls(self):
        worksheet, client = _worksheet({
            "values": [
                HEADERS,
                ["a", "FBA_1001", 46288, "1001", "FBA", 1, 2, 3],
                ["b", "FBA_1002", 46288, "1002", "FBA", 4, 5, 6],
                ["c", "FBA_1003", 46288, "1003", "FBA", 7, 8, 9],
            ],
            "rowCount": 4, "columnCount": 8,
        })

        worksheet.upsert_rows("sku_channel_id", [
            {"sku_channel_id": "FBA_1001", "Units": 10},
            {"sku_channel_id": "FBA_1003", "Units": 30},
        ])

        patches = [call for call in client.calls if call[0] == "PATCH"]
        assert len(patches) == 2
        assert "range(address='A2:H2')" in patches[0][1]
        assert "range(address='A4:H4')" in patches[1][1]

    def test_missing_key_column_raises(self):
        worksheet, _ = self._sheet()

        with pytest.raises(GraphError, match="has no 'nope' column"):
            worksheet.upsert_rows("nope", [{"sku_channel_id": "FBA_1001"}])

    def test_empty_rows_short_circuits(self):
        worksheet, client = self._sheet()

        assert worksheet.upsert_rows("sku_channel_id", []) == UpsertResult()
        assert client.calls == []

    def test_insert_lands_directly_under_the_last_data_row(self):
        worksheet, client = _worksheet({
            "values": [
                HEADERS,
                ["a", "FBA_1001", 46288, "1001", "FBA", 1, 2, 3],
                [""] * 8,
            ],
            "rowCount": 3, "columnCount": 8,
        })

        worksheet.upsert_rows("sku_channel_id", [{"sku_channel_id": "FBA_9999"}])

        assert "range(address='A3:H3')" in client.calls[-1][1]


class TestWorkbookPath:
    def test_paths_use_the_owner_and_item_id(self):
        workbook = ExcelWorkbook(FakeGraphClient(), "ITEM-ID", "julio@example.com")

        assert workbook.base_path == "/users/julio@example.com/drive/items/ITEM-ID/workbook"
        assert "worksheets('raw_sales')" in workbook.worksheet("raw_sales")._path

    def test_apostrophes_in_sheet_names_are_escaped(self):
        workbook = ExcelWorkbook(FakeGraphClient(), "ITEM", "owner")

        assert "worksheets('O''Brien')" in workbook.worksheet("O'Brien")._path
