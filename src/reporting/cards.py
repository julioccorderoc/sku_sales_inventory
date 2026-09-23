"""Adaptive Card renderers — the Teams-facing view of a report.

Why cards and not HTML: the Teams lane posts through a Power Automate Workflow
whose action renders an Adaptive Card. Cards understand no HTML at all, so the
reports are rebuilt here from the same computed data the HTML builders use —
detection is shared (`anomalies.py`), only the presentation differs.

Two things the old HTML report could not do, and this one does:

- **Real tables.** The `Table` element (Adaptive Cards 1.5) gives the bordered
  grid, verified rendering in this tenant.
- **Summarisation.** A channel with thirty flagged SKUs is not a report, it is a
  wall. Rows are ranked by severity and capped; the count of anything dropped is
  stated rather than silently swallowed.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from src.reporting.anomalies import (
    INVENTORY_TARGET_CHANNELS,
    SALES_EXCLUDE_CHANNELS,
    find_inventory_anomalies,
    find_sales_anomalies,
    group_by_channel,
)
from src.reporting.format import (
    as_float,
    fmt_delta,
    fmt_delta_money,
    fmt_money,
    fmt_num,
    format_short_date,
    pct_and_icon,
)

CARD_VERSION = "1.5"
"""The `Table` element landed in 1.5; everything else here is 1.0-era."""

MAX_ROWS_PER_CHANNEL = 3
"""Top three, ranked by severity — the card is a heads-up, not the ledger.

Everything past the cut is counted in a `+N more` line; the untrimmed report is
always in `output/*.html` and the history workbook.
"""

MAX_TOTAL_ROWS = 21
"""Bounds the payload at seven channels x three rows."""

BUNDLES_SKU = "Bundles"

_CHAR_PX = 8.5
"""Measured on a rendered card at Teams' ~660px post width: 14px Segoe UI runs
about 8.5px a character."""

_CELL_PADDING_PX = 28
_MIN_COLUMN_PX = 60


def _display_units(text: str) -> float:
    """Rough rendered width in characters, counting emoji as double."""
    units = 0.0
    for character in text:
        code = ord(character)
        if code == 0xFE0F:  # variation selector — zero width
            continue
        units += 1.8 if code >= 0x2000 else 1.0
    return units


def _column_weight(*columns: Sequence[str]) -> int:
    """A relative width proportional to the column's widest cell.

    Emitted as a bare **number**, never a `"<n>px"` string, because the two
    renderers disagree about what a px string means: the Adaptive Cards SDK
    honours it as fixed pixels, while Teams parses the number out and treats it
    as a weight. A card mixing `"96px"` and `3` therefore collapsed the weighted
    column to under 1% of the width in Teams while looking correct locally.

    Weights are the one form both read the same way. Measuring the content and
    using the measurement as the weight is how "as wide as its content" is
    expressed without an `"auto"` (the schema has none).
    """
    widest = max(
        (
            _display_units(line)
            for column in columns
            for text in column
            for line in text.split("\n")
        ),
        default=0.0,
    )
    return max(_MIN_COLUMN_PX, round(widest * _CHAR_PX + _CELL_PADDING_PX))


# --- building blocks ----------------------------------------------------------

def _text(
    text: str,
    *,
    size: str | None = None,
    weight: str | None = None,
    color: str | None = None,
    subtle: bool = False,
    wrap: bool = True,
    spacing: str | None = None,
) -> dict[str, Any]:
    block: dict[str, Any] = {"type": "TextBlock", "text": text, "wrap": wrap}
    if size:
        block["size"] = size
    if weight:
        block["weight"] = weight
    if color:
        block["color"] = color
    if subtle:
        block["isSubtle"] = True
    if spacing:
        block["spacing"] = spacing
    return block


def _cell(text: str, *, color: str | None = None, bold: bool = False) -> dict[str, Any]:
    return {
        "type": "TableCell",
        "items": [_text(text, color=color, weight="Bolder" if bold else None)],
    }


def _table(
    headers: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    widths: Sequence[int] | None = None,
) -> dict[str, Any]:
    """A bordered grid. The first row is the header."""
    columns = [{"width": width} for width in (widths or [1] * len(headers))]
    table_rows = [{
        "type": "TableRow",
        "cells": [_cell(str(label), bold=True) for label in headers],
    }]
    for row in rows:
        table_rows.append({
            "type": "TableRow",
            "cells": [
                value if isinstance(value, dict) else _cell(str(value))
                for value in row
            ],
        })
    return {
        "type": "Table",
        "firstRowAsHeader": True,
        "showGridLines": True,
        "columns": columns,
        "rows": table_rows,
    }


def _card(body: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": CARD_VERSION,
        "body": body,
    }


def simple_card(title: str, message: str) -> dict[str, Any]:
    """One-heading, one-paragraph card — the smoke test and failure notices."""
    return _card([
        _text(title, size="Medium", weight="Bolder"),
        _text(message, spacing="Small"),
    ])


def _signed_color(value: float) -> str | None:
    if value < 0:
        return "attention"
    if value > 0:
        return "good"
    return None


def _moved(current: float, delta: float, formatter) -> str:
    """A value and its move as one cell string: `203 (-178)`.

    Collapsing the old report's "Prev" and "Δ" columns into one halves the
    table's width — which is what makes it readable on a phone and keeps the
    card inside Teams' size limit.
    """
    return f"{formatter(current + delta)} ({formatter(delta)})"


# --- 1. inventory summary -----------------------------------------------------

def build_inventory_summary_card(status_summary: Mapping[str, Any]) -> dict[str, Any]:
    entries = list((status_summary or {}).items())
    updated = sum(1 for _, report_date in entries if report_date)
    skipped = len(entries) - updated

    subtitle = f"{len(entries)} channels · " + (
        "all updated" if not skipped else f"{updated} updated, {skipped} skipped"
    )

    channels = [f"{'✅' if report_date else '⚠️'} {channel}" for channel, report_date in entries]
    dates = [
        format_short_date(report_date) if report_date else "report file not found"
        for _, report_date in entries
    ]
    rows = [
        [_cell(channel, bold=True), _cell(report_date)]
        for channel, report_date in zip(channels, dates)
    ]

    return _card([
        _text("📊 Inventory Update Summary", size="Large", weight="Bolder"),
        _text(subtitle, subtle=True, spacing="None"),
        _table(
            ["Channel", "Report date"],
            rows,
            widths=[_column_weight(["Channel", *channels]), _column_weight(["Report date", *dates])],
        ),
    ])


# --- 2. sales summary ---------------------------------------------------------

def build_sales_summary_card(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    channels_found: list[str] = []
    normal = {"units": 0.0, "revenue": 0.0, "delta_units": 0.0, "delta_revenue": 0.0}
    bundles = {"units": 0.0, "revenue": 0.0, "delta_units": 0.0, "delta_revenue": 0.0}

    for row in rows:
        channel = row.get("Channel")
        if channel in SALES_EXCLUDE_CHANNELS:
            continue
        if channel not in channels_found:
            channels_found.append(channel)

        bucket = bundles if str(row.get("SKU")) == BUNDLES_SKU else normal
        bucket["units"] += as_float(row.get("Units"))
        bucket["revenue"] += as_float(row.get("Revenue"))
        bucket["delta_units"] += as_float(row.get("Delta_Sold"))
        bucket["delta_revenue"] += as_float(row.get("Delta_Revenue"))

    def change(delta: float, current: float, formatter) -> str:
        return f"{formatter(delta)} ({pct_and_icon(delta, current)})"

    metrics = ["Total revenue", "Total units", "Bundle revenue", "Bundle units"]
    values = [
        fmt_money(normal["revenue"]), fmt_num(normal["units"]),
        fmt_money(bundles["revenue"]), fmt_num(bundles["units"]),
    ]
    changes = [
        change(normal["delta_revenue"], normal["revenue"], fmt_delta_money),
        change(normal["delta_units"], normal["units"], fmt_delta),
        change(bundles["delta_revenue"], bundles["revenue"], fmt_delta_money),
        change(bundles["delta_units"], bundles["units"], fmt_delta),
    ]
    rows = [
        [_cell(metric, bold=True), _cell(value), _cell(change_text)]
        for metric, value, change_text in zip(metrics, values, changes)
    ]

    return _card([
        _text("📊 Sales Summary", size="Large", weight="Bolder"),
        _text(f"Channels: {', '.join(channels_found) or 'None'}", subtle=True, spacing="None"),
        _table(
            ["Metric", "Value", "Net change"],
            rows,
            widths=[
                _column_weight(["Metric", *metrics]),
                _column_weight(["Value", *values]),
                _column_weight(["Net change", *changes]),
            ],
        ),
    ])


# --- shared: anomaly cards ----------------------------------------------------

def _rank_and_cap(grouped: dict[str, list], label: str) -> tuple[dict[str, list], int]:
    """Worst-first within each channel, capped per channel and in total."""
    ranked = {
        channel: sorted(items, key=lambda finding: finding.severity, reverse=True)
        for channel, items in grouped.items()
    }

    omitted = 0
    for channel, items in ranked.items():
        if len(items) > MAX_ROWS_PER_CHANNEL:
            omitted += len(items) - MAX_ROWS_PER_CHANNEL
            ranked[channel] = items[:MAX_ROWS_PER_CHANNEL]

    shown = sum(len(items) for items in ranked.values())
    if shown > MAX_TOTAL_ROWS:
        for channel in ranked:
            if shown <= MAX_TOTAL_ROWS:
                break
            overflow = min(shown - MAX_TOTAL_ROWS, len(ranked[channel]))
            omitted += overflow
            ranked[channel] = ranked[channel][: len(ranked[channel]) - overflow]
            shown -= overflow

    return ranked, omitted


def _omitted_note(omitted: int) -> list[dict[str, Any]]:
    if not omitted:
        return []
    return [_text(
        f"+{omitted} more flagged row{'s' if omitted != 1 else ''} — full detail in "
        "historical_SKU_data.xlsx",
        subtle=True,
        spacing="Medium",
    )]


def _channel_heading(channel: str, count: int) -> dict[str, Any]:
    return _text(f"{channel} · {count}", size="Medium", weight="Bolder", spacing="Large")


# --- 3. inventory anomalies ---------------------------------------------------

def build_inventory_anomalies_card(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    findings = find_inventory_anomalies(rows)
    if not findings:
        return _card([
            _text("✅ Inventory — no anomalies", size="Large", weight="Bolder", color="good"),
            _text("Every tracked channel moved within its expected range.", spacing="Small"),
        ])

    grouped = group_by_channel(findings)
    ranked, omitted = _rank_and_cap(grouped, "inventory")

    body: list[dict[str, Any]] = [
        _text("🚨 Inventory Anomalies", size="Large", weight="Bolder", color="attention"),
        _text(
            f"{len(findings)} flagged rows · {len(grouped)} channels",
            subtle=True,
            spacing="None",
        ),
    ]

    ordered = [channel for channel in INVENTORY_TARGET_CHANNELS if channel in ranked]
    ordered += [channel for channel in ranked if channel not in INVENTORY_TARGET_CHANNELS]

    for channel in ordered:
        entries = ranked[channel]
        body.append(_channel_heading(channel, len(entries)))

        skus = [entry.sku for entry in entries]
        sold = [_moved(entry.previous_sold, entry.delta_sold, fmt_num) for entry in entries]
        stock = [_moved(entry.previous_inventory, entry.delta_inventory, fmt_num) for entry in entries]
        issues = [entry.reasons_compact for entry in entries]

        body.append(_table(
            ["SKU", "Sold", "Stock", "Issue"],
            [
                [
                    _cell(entry.sku, bold=True),
                    _cell(sold[index], color=_signed_color(entry.delta_sold)),
                    _cell(stock[index], color=_signed_color(entry.delta_inventory)),
                    _cell(issues[index]),
                ]
                for index, entry in enumerate(entries)
            ],
            widths=[
                _column_weight(["SKU", *skus]),
                _column_weight(["Sold", *sold]),
                _column_weight(["Stock", *stock]),
                _column_weight(["Issue", *issues]),
            ],
        ))

    body.extend(_omitted_note(omitted))
    return _card(body)


# --- 4. sales anomalies -------------------------------------------------------

def build_sales_anomalies_card(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Sales anomalies only.

    The TikTok Shop-vs-Shopify reconciliation stays in the HTML report
    (`output/*.html`); it was dropped from the card as noise.
    """
    findings, _tiktok = find_sales_anomalies(rows)
    grouped = group_by_channel(findings)

    body: list[dict[str, Any]] = []

    if not findings:
        body.append(_text("✅ Sales — no anomalies", size="Large", weight="Bolder", color="good"))
        body.append(_text("Revenue and units moved within expected ranges.", spacing="Small"))
    else:
        ranked, omitted = _rank_and_cap(grouped, "sales")
        body.append(_text("🚨 Sales Anomalies", size="Large", weight="Bolder", color="attention"))
        body.append(_text(
            f"{len(findings)} flagged rows · {len(grouped)} channels",
            subtle=True,
            spacing="None",
        ))
        for channel in sorted(ranked):
            entries = ranked[channel]
            body.append(_channel_heading(channel, len(entries)))

            skus = [entry.sku for entry in entries]
            revenue = [
                _moved(entry.previous_revenue, entry.delta_revenue, fmt_money)
                for entry in entries
            ]
            units = [_moved(entry.previous_units, entry.delta_units, fmt_num) for entry in entries]
            issues = [entry.reasons_compact for entry in entries]

            body.append(_table(
                ["SKU", "Revenue", "Units", "Issue"],
                [
                    [
                        _cell(entry.sku, bold=True),
                        _cell(revenue[index], color=_signed_color(entry.delta_revenue)),
                        _cell(units[index], color=_signed_color(entry.delta_units)),
                        _cell(issues[index]),
                    ]
                    for index, entry in enumerate(entries)
                ],
                widths=[
                    _column_weight(["SKU", *skus]),
                    _column_weight(["Revenue", *revenue]),
                    _column_weight(["Units", *units]),
                    _column_weight(["Issue", *issues]),
                ],
            ))
        body.extend(_omitted_note(omitted))

    return _card(body)
