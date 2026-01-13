from datetime import datetime, date
from pathlib import Path
import pandas as pd
import re


def get_date_str_for_filename() -> str:
    """
    Returns the current date formatted as 'monDD', e.g., 'jun30'.
    This creates the dynamic part of our report filenames.
    """
    return datetime.now().strftime("%b%d").lower()


def get_date_suffix_for_filename() -> str:
    """Returns the current date as a YYYY-MM-DD string for filenames."""
    return datetime.now().strftime("%Y-%m-%d")


def load_csv(file_path: Path, skiprows: int = 0) -> pd.DataFrame | None:
    """
    A more robust CSV loader with a multi-stage encoding fallback.
    It will attempt to read a file in the following order:
    1. UTF-8 with BOM support ('utf-8-sig') - The best practice.
    2. Latin-1 - A permissive fallback that never fails but might misinterpret characters.
    """
    try:
        # Attempt 1: Try the most common and correct encoding first.
        return pd.read_csv(file_path, encoding="utf-8-sig", skiprows=skiprows)

    except UnicodeDecodeError:
        try:
            # Attempt 2: Fallback to latin-1. This encoding can read any byte,
            # so it's a very safe fallback to prevent crashes.
            return pd.read_csv(file_path, encoding="latin-1", skiprows=skiprows)
        except Exception as e_latin1:
            # This is a defensive catch-all in case the latin-1 read also fails for
            # a non-encoding reason (e.g., malformed CSV).
            print(
                f"ERROR: Could not read {file_path.name} even with latin-1. Reason: {e_latin1}"
            )
            return None

    except FileNotFoundError:
        # Handle the case where the file doesn't exist separately for a clear message.
        print(f"INFO: Report not found at {file_path}, skipping.")
        return None

    except Exception as e_general:
        # Catch any other unexpected errors during the initial read.
        print(
            f"ERROR: An unexpected error occurred while reading {file_path.name}. Reason: {e_general}"
        )
        return None


def find_latest_report(directory: Path, prefix: str) -> tuple[Path, date] | None:
    """
    Finds the most recent report file for a given prefix in a directory.
    Target format: prefix + "YYYY-MM-DD.csv" (e.g., "AWD_Report_2025-12-15.csv")
    """
    # 1. Optimistic Check: Try to find today's file immediately (O(1) operation)
    today = date.today()
    today_str = today.isoformat()  # Returns 'YYYY-MM-DD'
    today_file = directory / f"{prefix}{today_str}.csv"

    if today_file.exists():
        return (today_file, today)

    # 2. Scanning: If today's file is missing, scan the directory
    # We use re.escape(prefix) to prevent regex injection if prefix contains special chars
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{4}}-\d{{2}}-\d{{2}})\.csv$")
    found_files = []

    for f in directory.iterdir():
        # Skip directories, looking only at files
        if not f.is_file():
            continue

        match = pattern.match(f.name)
        if match:
            date_str = match.group(1)
            try:
                # ISO formats parse reliably without complex format strings
                file_date = date.fromisoformat(date_str)
                found_files.append((f, file_date))
            except ValueError:
                # Malformed date strings (e.g. 2025-13-99) are ignored
                continue

    if not found_files:
        return None

    # 3. Sort by date descending (newest first) and return the winner
    # Logic: Tuple comparison uses the second element (date)
    found_files.sort(key=lambda x: x[1], reverse=True)
    return found_files[0]


def clean_money(val) -> float:
    """Removes $ and , from currency strings."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        clean = val.replace("$", "").replace(",", "").strip()
        return float(clean) if clean else 0.0
    return 0.0
