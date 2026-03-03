"""
Tests for src/utils.py — clean_money, load_csv, find_latest_report.
"""
from datetime import date

from src.utils import clean_money, load_csv, find_latest_report


# ---------------------------------------------------------------------------
# clean_money
# ---------------------------------------------------------------------------

class TestCleanMoney:
    def test_currency_string_with_dollar_and_comma(self):
        assert clean_money("$1,200.50") == 1200.50

    def test_plain_float(self):
        assert clean_money(5.0) == 5.0

    def test_integer(self):
        assert clean_money(10) == 10.0

    def test_plain_string_number(self):
        assert clean_money("29.97") == 29.97

    def test_dollar_sign_only_string(self):
        assert clean_money("$500.00") == 500.0

    def test_empty_string(self):
        assert clean_money("") == 0.0

    def test_empty_string_returns_zero(self):
        """After stripping $ and ,, an empty string returns 0.0."""
        assert clean_money("") == 0.0

    def test_none_value(self):
        assert clean_money(None) == 0.0

    def test_zero(self):
        assert clean_money(0) == 0.0


# ---------------------------------------------------------------------------
# load_csv
# ---------------------------------------------------------------------------

class TestLoadCsv:
    def test_happy_path_utf8(self, tmp_path):
        csv = tmp_path / "test.csv"
        csv.write_text("col1,col2\nval1,val2\n", encoding="utf-8")
        df = load_csv(csv)
        assert df is not None
        assert list(df.columns) == ["col1", "col2"]
        assert len(df) == 1

    def test_missing_file_returns_none(self, tmp_path):
        result = load_csv(tmp_path / "nonexistent.csv")
        assert result is None

    def test_skiprows_skips_metadata(self, tmp_path):
        csv = tmp_path / "with_meta.csv"
        # Two metadata rows, then header, then data
        csv.write_text(
            "Report Type,AWD Inventory Report\n"
            "Generated,2026-02-11\n"
            "name,value\n"
            "foo,1\n",
            encoding="utf-8",
        )
        df = load_csv(csv, skiprows=2)
        assert df is not None
        assert "name" in df.columns
        assert len(df) == 1

    def test_latin1_fallback(self, tmp_path):
        """Files with latin-1 characters (e.g. é) that are invalid UTF-8 load via fallback."""
        latin1_file = tmp_path / "latin1.csv"
        # \xe9 = é in latin-1, not valid as standalone byte in UTF-8
        latin1_file.write_bytes("col1,col2\nval,caf\xe9\n".encode("latin-1"))
        df = load_csv(latin1_file)
        assert df is not None
        assert len(df) == 1


# ---------------------------------------------------------------------------
# find_latest_report
# ---------------------------------------------------------------------------

class TestFindLatestReport:
    def test_returns_most_recent_file(self, tmp_path):
        (tmp_path / "FBA_report_2026-01-01.csv").write_text("col\nval")
        (tmp_path / "FBA_report_2026-02-11.csv").write_text("col\nval")

        result = find_latest_report(tmp_path, "FBA_report_")
        assert result is not None
        path, found_date = result
        assert path.name == "FBA_report_2026-02-11.csv"
        assert found_date == date(2026, 2, 11)

    def test_no_match_returns_none(self, tmp_path):
        result = find_latest_report(tmp_path, "FBA_report_")
        assert result is None

    def test_ignores_wrong_prefix(self, tmp_path):
        (tmp_path / "AWD_report_2026-02-11.csv").write_text("col\nval")
        result = find_latest_report(tmp_path, "FBA_report_")
        assert result is None

    def test_single_file_returned(self, tmp_path):
        (tmp_path / "Walmart_sales_2026-01-15.csv").write_text("col\nval")
        result = find_latest_report(tmp_path, "Walmart_sales_")
        assert result is not None
        _, found_date = result
        assert found_date == date(2026, 1, 15)
