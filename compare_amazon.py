"""
compare_amazon.py — Side-by-side comparison of old Amazon summary report vs new raw orders report.

Usage:
    python compare_amazon.py

Both files must be present in input/:
  - Amazon_sales_YYYY-MM-DD.csv    old Business Report (pre-aggregated summary)
  - Amazon_orders_YYYY-MM-DD.txt   new raw Order Report (row-per-order-line-item)

Output:
  Console table showing units and revenue per SKU for each source, with diff column.
  A CSV copy is saved to output/amazon_comparison_YYYY-MM-DD.csv for reference.

Note on date ranges:
  The two files likely cover different periods. This comparison shows methodology
  differences, not necessarily identical time windows. Always check the file dates
  printed at the top of the output before drawing conclusions.
"""

import sys
from datetime import date

from src import settings, parsers, utils


def main() -> None:
    # --- Find files ---
    old_info = utils.find_latest_report(settings.INPUT_DIR, settings.AMAZON_SALES_PREFIX)
    new_info = utils.find_latest_report(
        settings.INPUT_DIR, settings.AMAZON_ORDERS_PREFIX, (".txt", ".csv")
    )

    missing = []
    if not old_info:
        missing.append(f"  Amazon_sales_*.csv   (prefix: {settings.AMAZON_SALES_PREFIX})")
    if not new_info:
        missing.append(f"  Amazon_orders_*.txt  (prefix: {settings.AMAZON_ORDERS_PREFIX})")

    if missing:
        print("❌ Missing input files:")
        for m in missing:
            print(m)
        sys.exit(1)

    old_path, old_date = old_info
    new_path, new_date = new_info

    print()
    print("=" * 70)
    print("  Amazon Parser Comparison")
    print("=" * 70)
    print(f"  OLD  {old_path.name:<44} [{old_date}]")
    print(f"  NEW  {new_path.name:<44} [{new_date}]")
    if old_date != new_date:
        print(f"\n  ⚠️  Different file dates — differences may reflect date range, not methodology.")
    print("=" * 70)

    # --- Run both parsers ---
    old_result = parsers.parse_amazon_sales_report({"primary": old_path})
    new_result = parsers.parse_amazon_orders_report({"primary": new_path})

    if old_result.df is None:
        print("❌ Old parser (parse_amazon_sales_report) returned no data.")
        sys.exit(1)
    if new_result.df is None:
        print("❌ New parser (parse_amazon_orders_report) returned no data.")
        sys.exit(1)

    old_df = old_result.df.set_index("SKU").rename(
        columns={"Units": "Old_Units", "Revenue": "Old_Rev"}
    )
    new_df = new_result.df.set_index("SKU").rename(
        columns={"Units": "New_Units", "Revenue": "New_Rev"}
    )

    # Full outer join so we see SKUs present in only one source
    merged = old_df.join(new_df, how="outer").fillna(0)
    merged["Units_Diff"] = (merged["New_Units"] - merged["Old_Units"]).round(0)
    merged["Rev_Diff"] = (merged["New_Rev"] - merged["Old_Rev"]).round(2)
    merged = merged.sort_values("SKU")

    # --- Print table ---
    header = f"{'SKU':<12}  {'Old Units':>9}  {'New Units':>9}  {'Δ Units':>8}    {'Old Rev':>10}  {'New Rev':>10}  {'Δ Rev':>10}"
    sep = "-" * len(header)
    print()
    print(header)
    print(sep)

    for sku, row in merged.iterrows():
        u_diff = f"{row['Units_Diff']:+.0f}" if row["Units_Diff"] != 0 else "—"
        r_diff = f"{row['Rev_Diff']:+.2f}" if row["Rev_Diff"] != 0 else "—"
        flag = "  ◄" if row["Units_Diff"] != 0 else ""
        print(
            f"{sku:<12}  {row['Old_Units']:>9.0f}  {row['New_Units']:>9.0f}  {u_diff:>8}"
            f"    {row['Old_Rev']:>10.2f}  {row['New_Rev']:>10.2f}  {r_diff:>10}{flag}"
        )

    print(sep)

    old_unit_total = merged["Old_Units"].sum()
    new_unit_total = merged["New_Units"].sum()
    old_rev_total = merged["Old_Rev"].sum()
    new_rev_total = merged["New_Rev"].sum()

    print(
        f"{'TOTAL':<12}  {old_unit_total:>9.0f}  {new_unit_total:>9.0f}  {new_unit_total - old_unit_total:>+8.0f}"
        f"    {old_rev_total:>10.2f}  {new_rev_total:>10.2f}  {new_rev_total - old_rev_total:>+10.2f}"
    )

    # --- Summary stats ---
    print()
    print(f"  Old  {old_result.raw_count:>5} raw rows → {len(old_df):>3} SKUs   "
          f"(bundle units: {old_result.bundle_stats.get('Units', 0):.0f})")
    print(f"  New  {new_result.raw_count:>5} raw rows → {len(new_df):>3} SKUs   "
          f"(bundle units: {new_result.bundle_stats.get('Units', 0):.0f})")

    skus_only_old = set(old_df.index) - set(new_df.index)
    skus_only_new = set(new_df.index) - set(old_df.index)
    if skus_only_old:
        print(f"\n  SKUs in OLD only: {', '.join(sorted(skus_only_old))}")
    if skus_only_new:
        print(f"\n  SKUs in NEW only: {', '.join(sorted(skus_only_new))}")

    diff_count = (merged["Units_Diff"] != 0).sum()
    print()
    if diff_count == 0:
        print("  ✅ No unit differences — parsers agree on all SKUs.")
    else:
        print(f"  ⚠️  {diff_count} SKU(s) with unit differences (marked ◄)")

    # --- Save CSV ---
    settings.OUTPUT_DIR.mkdir(exist_ok=True)
    today_str = date.today().isoformat()
    out_path = settings.OUTPUT_DIR / f"amazon_comparison_{today_str}.csv"

    merged.reset_index().rename(columns={"index": "SKU"}).to_csv(out_path, index=False)
    print(f"\n  💾 Saved: {out_path.name}")
    print()


if __name__ == "__main__":
    main()
