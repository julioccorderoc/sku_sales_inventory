"""Anomaly detection — one implementation, two renderers.

The thresholds and rules are the ones the n8n `Craft * Delta` nodes used. They
live here rather than inside a renderer so the Teams card and the HTML report
can never disagree about what counts as an anomaly; only the wording differs
(`Issue.to_html` keeps the original phrasing, `Issue.to_compact` is the card's
tighter one).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from src.reporting.format import as_float

# --- inventory thresholds -----------------------------------------------------
INVENTORY_SALES_CHANGE_THRESHOLD = 0.05
INVENTORY_MARGIN_PERCENT = 0.01
INVENTORY_SALES_PERIOD_DAYS = 30
INVENTORY_MIN_VOLUME_THRESHOLD = 10
INVENTORY_TARGET_CHANNELS = ["FBA", "DTC", "WFS", "AWD", "Reserve"]
INVENTORY_NO_SALES_CHANNELS = ["AWD", "Reserve"]

# --- sales thresholds ---------------------------------------------------------
SALES_CHANGE_THRESHOLD = 0.05
SALES_MIN_REVENUE_THRESHOLD = 100
SALES_MIN_VOLUME_THRESHOLD = 10
SALES_EXCLUDE_CHANNELS = ["TikTok Shop"]
TIKTOK_SHOP_NAME = "TikTok Shop"
TIKTOK_SHOPIFY_NAME = "TikTok Shopify"


@dataclass(frozen=True)
class Issue:
    """One thing wrong with one SKU, with both renderings of it."""

    kind: str  # zero_sales | sales_swing | unit_swing | revenue_swing | inventory
    pct: float | None = None  # raw fraction, e.g. -0.467
    expected: float | None = None
    actual: float | None = None

    def to_html(self) -> str:
        """The original n8n wording."""
        if self.kind == "zero_sales":
            return "⚠️ Zero Sales"
        if self.kind == "sales_swing":
            arrow = "📈" if (self.pct or 0) > 0 else "📉"
            return f"{arrow} Sales Swing ({(self.pct or 0) * 100:.1f}%)"
        if self.kind == "revenue_swing":
            arrow = "📈" if (self.pct or 0) > 0 else "🔻"
            return f"{arrow} Rev Swing ({(self.pct or 0) * 100:.1f}%)"
        if self.kind == "unit_swing":
            arrow = "📦⬆️" if (self.pct or 0) > 0 else "📦⬇️"
            return f"{arrow} Unit Swing ({(self.pct or 0) * 100:.1f}%)"
        icon = "📦⬆️" if (self.actual or 0) > (self.expected or 0) else "📦⬇️"
        return f"{icon} Inv Anomaly (Exp: ~{(self.expected or 0):.0f} • Real: {int(self.actual or 0)})"

    def to_compact(self) -> str:
        """The card wording — shorter, signed, no parentheses."""
        if self.kind == "zero_sales":
            return "⚠️ Zero sales"
        if self.kind == "sales_swing":
            return f"{_arrow(self.pct)} Sales {_signed_pct(self.pct)}"
        if self.kind == "revenue_swing":
            return f"{_arrow(self.pct)} Revenue {_signed_pct(self.pct)}"
        if self.kind == "unit_swing":
            return f"{_arrow(self.pct)} Units {_signed_pct(self.pct)}"
        return f"📦 exp {round(self.expected or 0):,} · real {round(self.actual or 0):,}"

    @property
    def severity(self) -> float:
        """Ranking score for the card, which shows the worst rows first.

        Continuous measures are capped at 1.0 so a 400% swing cannot outrank
        something categorically worse, and a full stop (zero sales) scores
        above all of them — a SKU that stopped selling is the headline, not the
        row with the largest percentage.
        """
        if self.kind == "zero_sales":
            return 2.0
        if self.pct is not None:
            return min(abs(self.pct), 1.0)
        if self.expected:
            return min(abs((self.actual or 0) - self.expected) / self.expected, 1.0)
        return 0.0


def _arrow(pct: float | None) -> str:
    return "📈" if (pct or 0) > 0 else "📉"


def _signed_pct(pct: float | None) -> str:
    return f"{(pct or 0) * 100:+.1f}%"


@dataclass
class InventoryFinding:
    channel: str
    sku: str
    previous_sold: float
    delta_sold: float
    previous_inventory: float
    delta_inventory: float
    issues: list[Issue] = field(default_factory=list)

    @property
    def reasons_html(self) -> str:
        return "<br>".join(issue.to_html() for issue in self.issues)

    @property
    def reasons_compact(self) -> str:
        return "\n".join(issue.to_compact() for issue in self.issues)

    @property
    def severity(self) -> float:
        return max((issue.severity for issue in self.issues), default=0.0)


@dataclass
class SalesFinding:
    channel: str
    sku: str
    previous_revenue: float
    delta_revenue: float
    previous_units: float
    delta_units: float
    issues: list[Issue] = field(default_factory=list)

    @property
    def reasons_html(self) -> str:
        return "<br>".join(issue.to_html() for issue in self.issues)

    @property
    def reasons_compact(self) -> str:
        return "\n".join(issue.to_compact() for issue in self.issues)

    @property
    def severity(self) -> float:
        return max((issue.severity for issue in self.issues), default=0.0)


def find_inventory_anomalies(rows: Iterable[dict[str, Any]]) -> list[InventoryFinding]:
    """Sales swings and stock movements that do not match the run rate."""
    findings: list[InventoryFinding] = []

    for row in rows:
        issues: list[Issue] = []

        current_sold = as_float(row.get("Units"))
        delta_sold = as_float(row.get("Delta_Sold"))
        previous_sold = current_sold - delta_sold

        current_inventory = as_float(row.get("Inventory"))
        delta_inventory = as_float(row.get("Delta_Inventory"))
        previous_inventory = current_inventory - delta_inventory

        days_elapsed = as_float(row.get("Days_Since_Last_Report"))
        channel = row.get("Channel")
        sku = str(row.get("SKU"))
        is_2p = sku.endswith("2P")

        is_sales_channel = channel not in INVENTORY_NO_SALES_CHANNELS
        has_stock_history = previous_inventory > 0
        is_high_volume = previous_sold >= INVENTORY_MIN_VOLUME_THRESHOLD
        is_2p_allowed = (not is_2p) or channel == "FBA"

        if is_sales_channel and has_stock_history and is_high_volume and is_2p_allowed:
            if current_sold == 0:
                issues.append(Issue("zero_sales"))
            if current_sold > 0:
                pct_change = (current_sold - previous_sold) / previous_sold
                if abs(pct_change) > INVENTORY_SALES_CHANGE_THRESHOLD:
                    issues.append(Issue("sales_swing", pct=pct_change))

        daily_sales = current_sold / INVENTORY_SALES_PERIOD_DAYS if is_sales_channel else 0
        expected_inventory = previous_inventory - daily_sales * days_elapsed

        if expected_inventory >= 0:
            margin = previous_inventory * INVENTORY_MARGIN_PERCENT
            if not (expected_inventory - margin <= current_inventory <= expected_inventory + margin):
                if abs(current_inventory - expected_inventory) > 1:
                    issues.append(
                        Issue("inventory", expected=expected_inventory, actual=current_inventory)
                    )

        if issues:
            findings.append(InventoryFinding(
                channel=channel, sku=sku,
                previous_sold=previous_sold, delta_sold=delta_sold,
                previous_inventory=previous_inventory, delta_inventory=delta_inventory,
                issues=issues,
            ))

    return findings


def find_sales_anomalies(
    rows: Iterable[dict[str, Any]],
) -> tuple[list[SalesFinding], dict[str, dict[str, dict[str, float]]]]:
    """Revenue/unit swings per channel, plus the TikTok reconciliation map."""
    findings: list[SalesFinding] = []
    tiktok: dict[str, dict[str, dict[str, float]]] = {}

    for row in rows:
        issues: list[Issue] = []
        channel = row.get("Channel")
        sku = str(row.get("SKU"))

        current_units = as_float(row.get("Units"))
        current_revenue = as_float(row.get("Revenue"))
        delta_units = as_float(row.get("Delta_Sold"))
        delta_revenue = as_float(row.get("Delta_Revenue"))

        previous_units = current_units - delta_units
        previous_revenue = current_revenue - delta_revenue

        if channel in (TIKTOK_SHOP_NAME, TIKTOK_SHOPIFY_NAME):
            tiktok.setdefault(sku, {})[channel] = {"rev": current_revenue, "units": current_units}

        if channel in SALES_EXCLUDE_CHANNELS:
            continue
        if previous_units < SALES_MIN_VOLUME_THRESHOLD:
            continue

        if previous_revenue >= SALES_MIN_REVENUE_THRESHOLD:
            pct_change = delta_revenue / previous_revenue
            if abs(pct_change) > SALES_CHANGE_THRESHOLD:
                issues.append(Issue("revenue_swing", pct=pct_change))

        if previous_units > 0 and current_units == 0:
            issues.append(Issue("zero_sales"))

        if not any(issue.kind == "revenue_swing" for issue in issues):
            pct_change = delta_units / previous_units
            if abs(pct_change) > SALES_CHANGE_THRESHOLD:
                issues.append(Issue("unit_swing", pct=pct_change))

        if issues:
            findings.append(SalesFinding(
                channel=channel, sku=sku,
                previous_revenue=previous_revenue, delta_revenue=delta_revenue,
                previous_units=previous_units, delta_units=delta_units,
                issues=issues,
            ))

    return findings, tiktok


def group_by_channel(findings: Iterable[Any]) -> dict[str, list[Any]]:
    """Findings bucketed by channel, preserving first-seen order."""
    grouped: dict[str, list[Any]] = {}
    for finding in findings:
        grouped.setdefault(finding.channel, []).append(finding)
    return grouped
