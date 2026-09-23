"""HTML report builders — a direct port of the four n8n Code nodes.

Kept byte-faithful where it matters: the same thresholds, the same emoji, the
same table columns, and the same inline styles, because these strings are what
the Supply Chain channel read every day until the Teams lane moved to Adaptive
Cards (`cards.py`). HTML is still the `file` transport's output and the shape a
`message`-mode webhook would post.

The four builders:

- `build_inventory_summary`   ← Craft Inventory Update
- `build_sales_summary`       ← Craft Sales Update
- `build_inventory_anomalies` ← Craft Inventory Delta
- `build_sales_anomalies`     ← Craft Sales Delta

Detection lives in `anomalies.py`; this module only renders it.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from src.reporting.anomalies import (
    INVENTORY_TARGET_CHANNELS,
    SALES_EXCLUDE_CHANNELS,
    TIKTOK_SHOP_NAME,
    TIKTOK_SHOPIFY_NAME,
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
    format_long_date,
    pct_and_icon,
)

__all__ = [
    "build_inventory_summary",
    "build_sales_summary",
    "build_inventory_anomalies",
    "build_sales_anomalies",
    "format_long_date",
]

BUNDLES_SKU = "Bundles"

_TABLE_STYLE = (
    "width: 100%; border-collapse: collapse; border: 1px solid #ddd; "
    "font-family: Arial, sans-serif; font-size: 14px;"
)
_TABLE_STYLE_SPACED = _TABLE_STYLE + " margin-bottom: 20px;"
_TH_STYLE = "border: 1px solid #ddd; padding: 8px; text-align: right; font-weight: bold; font-size: 16px;"
_TH_STYLE_LEFT = "border: 1px solid #ddd; padding: 8px; text-align: left; font-weight: bold; font-size: 16px;"
_TD_STYLE = "border: 1px solid #ddd; padding: 8px; text-align: right;"
_TD_STYLE_LEFT = "border: 1px solid #ddd; padding: 8px; text-align: left;"

_TABLE_STYLE_DARK = _TABLE_STYLE.replace("#ddd", "#777") + " margin-bottom: 20px;"
_TH_STYLE_DARK = _TH_STYLE.replace("#ddd", "#777")
_TH_STYLE_LEFT_DARK = _TH_STYLE_LEFT.replace("#ddd", "#777")
_TD_STYLE_DARK = _TD_STYLE.replace("#ddd", "#777")
_TD_STYLE_LEFT_DARK = _TD_STYLE_LEFT.replace("#ddd", "#777")


# --- 1. inventory summary -----------------------------------------------------

def build_inventory_summary(status_summary: Mapping[str, Any]) -> str:
    """Per-channel "updated / skipped" list. The date comes from the filename."""
    parts = ["<h3>📊 Inventory Update Summary</h3>", "<hr>", "<ul>"]

    for channel, report_date in (status_summary or {}).items():
        if report_date:
            parts.append(
                f"<li>✅ <strong>{channel}:</strong> Updated with data from "
                f"<strong>{format_long_date(report_date)}</strong></li>"
            )
        else:
            parts.append(
                f"<li>⚠️ <strong>{channel}:</strong> Update SKIPPED "
                f"(report file not found)</li>"
            )

    parts.append("</ul>")
    return "\n".join(parts)


# --- 2. sales summary ---------------------------------------------------------

def build_sales_summary(rows: Iterable[dict[str, Any]]) -> str:
    """Totals card: revenue and units, split normal vs bundle, with net change."""
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

    card_style = "border: 1px solid #777; border-radius: 5px; padding: 15px; margin-bottom: 20px;"
    header_style = (
        "font-size: 18px; font-weight: bold; margin-bottom: 10px; "
        "border-bottom: 2px solid #005a9c; padding-bottom: 5px;"
    )
    list_style = "margin: 0; padding: 0; list-style: none; font-size: 14px;"
    item_style = "margin-bottom: 8px;"

    def line(label: str, value: str, pct: str, delta: str) -> str:
        return (
            f"<li style='{item_style}'><strong>{label}:</strong> {value} "
            f"({pct} Net Change: <strong>{delta}</strong>)</li>"
        )

    html = [f"<div style='{card_style}'>"]
    html.append(f"<div style='{header_style}'>📊 Global Sales Summary</div>")
    html.append(
        "<div style='font-size: 12px; color: #888; margin-bottom: 10px;'>"
        f"Channels: {', '.join(channels_found) or 'None'}</div>"
    )
    html.append(f"<ul style='{list_style}'>")
    html.append(line(
        "Total Revenue", fmt_money(normal["revenue"]),
        pct_and_icon(normal["delta_revenue"], normal["revenue"]),
        fmt_delta_money(normal["delta_revenue"]),
    ))
    html.append(line(
        "Total Units", fmt_num(normal["units"]),
        pct_and_icon(normal["delta_units"], normal["units"]),
        fmt_delta(normal["delta_units"]),
    ))
    html.append('<hr style="border: 0; border-top: 1px solid #555; margin: 10px 0;">')
    html.append(line(
        "Total Bundle Revenue", fmt_money(bundles["revenue"]),
        pct_and_icon(bundles["delta_revenue"], bundles["revenue"]),
        fmt_delta_money(bundles["delta_revenue"]),
    ))
    html.append(line(
        "Total Bundle Units", fmt_num(bundles["units"]),
        pct_and_icon(bundles["delta_units"], bundles["units"]),
        fmt_delta(bundles["delta_units"]),
    ))
    html.append("</ul>")
    html.append("</div>")
    return "\n".join(html)


# --- 3. inventory anomalies ---------------------------------------------------

def build_inventory_anomalies(rows: Iterable[dict[str, Any]]) -> str:
    """Sales swings and stock movements that do not match the run rate."""
    findings = find_inventory_anomalies(rows)
    grouped = group_by_channel(findings)

    buckets: dict[str, list] = {
        channel: list(grouped.get(channel, [])) for channel in INVENTORY_TARGET_CHANNELS
    }
    buckets["Other"] = [
        finding
        for channel, items in grouped.items()
        if channel not in INVENTORY_TARGET_CHANNELS
        for finding in items
    ]

    if not findings:
        return (
            "<div style='font-family: Arial, sans-serif; padding: 10px; border: 1px solid #4CAF50; "
            "background-color: #e8f5e9; color: #2e7d32; border-radius: 5px;'>"
            "✅ <strong>Daily Report:</strong> No anomalies detected. All systems normal.</div>"
        )

    html = ["<h2 style='font-family: Arial, sans-serif; color: #d32f2f;'>🚨 Daily Anomalies Report</h2>"]
    for channel in [*INVENTORY_TARGET_CHANNELS, "Other"]:
        entries = buckets.get(channel) or []
        if not entries:
            continue

        html.append(
            "<h3 style='font-family: Arial, sans-serif; border-bottom: 2px solid #333; "
            f"padding-bottom: 5px; margin-top: 25px;'>{channel}</h3>"
        )
        html.append(f"<table style='{_TABLE_STYLE}'>")
        html.append("<thead><tr>")
        for label, style in [
            ("SKU", _TH_STYLE_LEFT), ("Prev Sold", _TH_STYLE), ("Δ Sold", _TH_STYLE),
            ("Prev Inv", _TH_STYLE), ("Δ Inv", _TH_STYLE), ("Issue", _TH_STYLE_LEFT),
        ]:
            html.append(f"<th style='{style}'>{label}</th>")
        html.append("</tr></thead><tbody>")
        for entry in entries:
            html.append("<tr>")
            html.append(f"<td style='{_TD_STYLE_LEFT}'><strong>{entry.sku}</strong></td>")
            html.append(f"<td style='{_TD_STYLE}'>{fmt_num(entry.previous_sold)}</td>")
            html.append(f"<td style='{_TD_STYLE}'>{fmt_delta(entry.delta_sold)}</td>")
            html.append(f"<td style='{_TD_STYLE}'>{fmt_num(entry.previous_inventory)}</td>")
            html.append(f"<td style='{_TD_STYLE}'>{fmt_delta(entry.delta_inventory)}</td>")
            html.append(f"<td style='{_TD_STYLE_LEFT}'>{entry.reasons_html}</td>")
            html.append("</tr>")
        html.append("</tbody></table>")

    return "\n".join(html)


# --- 4. sales anomalies -------------------------------------------------------

def build_sales_anomalies(rows: Iterable[dict[str, Any]]) -> str:
    """Revenue/unit swings per channel, plus the TikTok Shop-vs-Shopify check."""
    findings, tiktok = find_sales_anomalies(rows)
    grouped = group_by_channel(findings)

    html: list[str] = [_sales_anomaly_table(grouped)]
    html.append(_tiktok_comparison_table(tiktok))
    return "\n".join(part for part in html if part)


def _sales_anomaly_table(groups: dict[str, list]) -> str:
    if not groups:
        return (
            "<div style='font-family: Arial, sans-serif; padding: 10px; border: 1px solid #4CAF50; "
            "border-radius: 5px; margin-bottom: 20px;'>"
            "✅ <strong>Sales Report:</strong> No anomalies detected.</div>"
        )

    html = ["<h2 style='font-family: Arial, sans-serif; color: #d32f2f;'>🚨 Sales Anomalies Report</h2>"]
    for channel in sorted(groups):
        html.append(
            "<h3 style='font-family: Arial, sans-serif; border-bottom: 2px solid #777; "
            f"padding-bottom: 5px; margin-top: 25px;'>{channel}</h3>"
        )
        html.append(f"<table style='{_TABLE_STYLE_SPACED}'>")
        html.append("<thead><tr>")
        for label, style in [
            ("SKU", _TH_STYLE_LEFT_DARK), ("Prev Rev", _TH_STYLE_DARK), ("Δ Rev", _TH_STYLE_DARK),
            ("Prev Units", _TH_STYLE_DARK), ("Δ Units", _TH_STYLE_DARK), ("Issue", _TH_STYLE_LEFT_DARK),
        ]:
            html.append(f"<th style='{style}'>{label}</th>")
        html.append("</tr></thead><tbody>")

        for entry in groups[channel]:
            color = ""
            if entry.delta_revenue < 0:
                color = "color: red;"
            elif entry.delta_revenue > 0:
                color = "color: green;"

            html.append("<tr>")
            html.append(f"<td style='{_TD_STYLE_LEFT_DARK}'><strong>{entry.sku}</strong></td>")
            html.append(f"<td style='{_TD_STYLE_DARK}'>{fmt_money(entry.previous_revenue)}</td>")
            html.append(f"<td style='{_TD_STYLE_DARK}; {color}'>{fmt_delta_money(entry.delta_revenue)}</td>")
            html.append(f"<td style='{_TD_STYLE_DARK}'>{fmt_num(entry.previous_units)}</td>")
            html.append(f"<td style='{_TD_STYLE_DARK}'>{fmt_delta(entry.delta_units)}</td>")
            html.append(f"<td style='{_TD_STYLE_LEFT_DARK}'>{entry.reasons_html}</td>")
            html.append("</tr>")

        html.append("</tbody></table>")
    return "\n".join(html)


def _tiktok_comparison_table(tiktok: dict[str, dict[str, dict[str, float]]]) -> str:
    """Same SKU, two feeds — a mismatch means one of them is wrong."""
    if not tiktok:
        return ""

    html = [
        "<h2 style='font-family: Arial, sans-serif; color: #005a9c; margin-top: 40px;'>"
        "⚖️ TikTok Comparison (Shop vs Shopify)</h2>",
        f"<table style='{_TABLE_STYLE_SPACED}'>",
        "<thead><tr>",
    ]
    for label, style in [
        ("SKU", _TH_STYLE_LEFT_DARK), ("Shop Rev", _TH_STYLE_DARK), ("Shopify Rev", _TH_STYLE_DARK),
        ("Diff ($)", _TH_STYLE_DARK), ("Shop Units", _TH_STYLE_DARK),
        ("Shopify Units", _TH_STYLE_DARK), ("Diff (Units)", _TH_STYLE_DARK),
    ]:
        html.append(f"<th style='{style}'>{label}</th>")
    html.append("</tr></thead><tbody>")

    for sku in sorted(tiktok):
        shop = tiktok[sku].get(TIKTOK_SHOP_NAME, {"rev": 0.0, "units": 0.0})
        shopify = tiktok[sku].get(TIKTOK_SHOPIFY_NAME, {"rev": 0.0, "units": 0.0})

        diff_revenue = shop["rev"] - shopify["rev"]
        diff_units = shop["units"] - shopify["units"]

        if shop["rev"] == 0 and shopify["rev"] == 0:
            continue

        is_mismatch = abs(diff_revenue) > 1 or abs(diff_units) > 0
        row_style = "" if is_mismatch else "opacity: 0.6; color: #666;"

        revenue_cell, revenue_style = _diff_cell(diff_revenue, fmt_money, threshold=1)
        units_cell, units_style = _diff_cell(diff_units, fmt_num, threshold=0)

        html.append(f"<tr style='{row_style}'>")
        html.append(f"<td style='{_TD_STYLE_LEFT_DARK}'><strong>{sku}</strong></td>")
        html.append(f"<td style='{_TD_STYLE_DARK}'>{fmt_money(shop['rev'])}</td>")
        html.append(f"<td style='{_TD_STYLE_DARK}'>{fmt_money(shopify['rev'])}</td>")
        html.append(f"<td style='{revenue_style}'>{revenue_cell}</td>")
        html.append(f"<td style='{_TD_STYLE_DARK}'>{fmt_num(shop['units'])}</td>")
        html.append(f"<td style='{_TD_STYLE_DARK}'>{fmt_num(shopify['units'])}</td>")
        html.append(f"<td style='{units_style}'>{units_cell}</td>")
        html.append("</tr>")

    html.append("</tbody></table>")
    return "\n".join(html)


def _diff_cell(difference: float, formatter, *, threshold: float) -> tuple[str, str]:
    """A diff cell, flagged orange when it is material (revenue > $1, units > 0)."""
    if difference > threshold:
        return f"⬆️ {formatter(difference)}", _TD_STYLE_DARK + " color: orange; font-weight: bold;"
    if difference < -threshold:
        return f"⬇️ {formatter(difference)}", _TD_STYLE_DARK + " color: orange; font-weight: bold;"
    return formatter(difference), _TD_STYLE_DARK
