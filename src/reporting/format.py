"""Number and date formatting shared by the HTML and Adaptive Card renderers.

Kept in one place so a card and its HTML twin can never disagree about what a
number looks like.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any


def as_float(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def as_int(value: Any) -> int:
    return round(as_float(value))


def fmt_money(value: Any) -> str:
    """`1234.5` → `$1,234.50`; negatives keep the sign outside the symbol."""
    number = as_float(value)
    sign = "-" if number < 0 else ""
    return f"{sign}${abs(number):,.2f}"


def fmt_delta_money(value: Any) -> str:
    number = as_float(value)
    if number == 0:
        return "$0.00"
    sign = "+" if number > 0 else "-"
    return f"{sign}${abs(number):,.2f}"


def fmt_num(value: Any) -> str:
    return f"{round(as_float(value)):,}"


def fmt_delta(value: Any) -> str:
    number = round(as_float(value))
    if number == 0:
        return "0"
    return f"{number:+,}"


def pct_and_icon(delta: Any, current: Any) -> str:
    """Reconstruct the prior value from the delta and describe the move."""
    delta_value = as_float(delta)
    current_value = as_float(current)
    previous = current_value - delta_value

    if previous == 0:
        return "0.0%" if delta_value == 0 else "📈 100%"

    pct = delta_value / previous * 100
    icon = "⬆️" if pct >= 0 else "🔻"
    return f"{icon} {abs(pct):.1f}%"


def format_long_date(value: Any) -> str:
    """`2026-09-23` → `September 23, 2026` (matches toLocaleDateString en-US)."""
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value[:10])
        except ValueError:
            return str(value)
    if isinstance(value, date):
        return f"{value.strftime('%B')} {value.day}, {value.year}"
    return str(value)


def format_short_date(value: Any) -> str:
    """`2026-09-23` → `23 Sep 2026` — the card's tighter date."""
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value[:10])
        except ValueError:
            return str(value)
    if isinstance(value, date):
        return f"{value.day} {value.strftime('%b')} {value.year}"
    return str(value)
