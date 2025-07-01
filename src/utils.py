from datetime import datetime
from pathlib import Path
import pandas as pd


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
        # This block only runs if the first attempt failed specifically due to encoding.
        print(
            f"INFO: UTF-8 decoding failed for {file_path.name}. Retrying with 'latin-1'."
        )
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
