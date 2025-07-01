from datetime import datetime


def get_date_str_for_filename() -> str:
    """
    Returns the current date formatted as 'monDD', e.g., 'jun30'.
    This creates the dynamic part of our report filenames.
    """
    return datetime.now().strftime("%b%d").lower()
