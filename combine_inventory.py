import pandas as pd
import glob
import os
import re

# Folder where your CSV files are stored
folder_path = "output"

# Pattern to match files
pattern = os.path.join(folder_path, "inventory_report_*.csv")

# Find all matching files
files = glob.glob(pattern)

# Prepare a list to hold DataFrames
df_list = []

# Regex to extract date from filename
date_pattern = re.compile(r"inventory_report_(\d{4}-\d{2}-\d{2})\.csv")

print(f"Found {len(files)} files to process.")

for file in files:
    match = date_pattern.search(file)
    if not match:
        continue  # Skip files that don't match the expected pattern

    report_date_str = match.group(1)
    
    # Read CSV
    # Some older files might have different headers or casing, so we handle normalization below
    try:
        df = pd.read_csv(file)
    except Exception as e:
        print(f"❌ Error reading {file}: {e}")
        continue

    # --- NORMALIZE HEADERS ---
    # Map old headers to new schema
    # Old: Units Sold, Last Updated
    # New: Units, Date
    # Also standardize typical casing differences if any exist
    rename_map = {
        "Units Sold": "Units",
        "Last Updated": "Date",
        "units_sold": "Units",
        "last_updated": "Date",
        "inventory": "Inventory",
        "inbound": "Inbound",
        "sku": "SKU",
        "channel": "Channel",
        "id": "old_id" # Rename old ID so we don't conflict when regenerating
    }
    df = df.rename(columns=rename_map)
    
    # Ensure redundant columns don't mess up merge or final usage
    # We strictly use the FILENAME date as the source of truth for the report date
    df["Date"] = report_date_str
    
    # Handle missing columns if older files don't identify slightly differently
    # But based on inspection, they should have Units, Inventory, Inbound
    required_cols = ["SKU", "Channel", "Units", "Inventory", "Inbound"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"⚠️ Skipping {file} - Missing columns: {missing}")
        continue

    # --- REGENERATE IDs ---
    # ID: YYYYMMDD_Channel_SKU
    # sku_channel_id: Channel_SKU
    
    # Clean up Date string for ID generation (remove dashes)
    date_id_part = report_date_str.replace("-", "")
    
    df["id"] = (
        date_id_part
        + "_"
        + df["Channel"].astype(str)
        + "_"
        + df["SKU"].astype(str)
    )
    
    df["sku_channel_id"] = (
        df["Channel"].astype(str) + "_" + df["SKU"].astype(str)
    )

    # Convert numeric columns to ensure type consistency
    for col in ["Units", "Inventory", "Inbound"]:
        df[col] = df[col].fillna(0).astype(int)

    # Filter and Reorder
    final_cols = ["id", "sku_channel_id", "Date", "SKU", "Channel", "Units", "Inventory", "Inbound"]
    df = df[final_cols]

    # Append to list
    df_list.append(df)

# Combine all dataframes
if df_list:
    combined_df = pd.concat(df_list, ignore_index=True)

    # Sort by report date (oldest first), then Channel, then SKU
    combined_df = combined_df.sort_values(by=["Date", "Channel", "SKU"])

    # Export combined CSV
    output_file = os.path.join(folder_path, "combined_inventory_report.csv")
    combined_df.to_csv(output_file, index=False)

    print(f"✅ Combined report saved to: {output_file}")
    print(f"   Total Records: {len(combined_df)}")
    
    # Optional: Preview
    print("\nPreview (First 5 Rows):")
    print(combined_df.head().to_string())

else:
    print("⚠️ No matching CSV files found.")
