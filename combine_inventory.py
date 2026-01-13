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

for file in files:
    match = date_pattern.search(file)
    if not match:
        continue  # Skip files that don't match the expected pattern

    report_date = match.group(1)

    # Read CSV
    df = pd.read_csv(file)

    # Add a column with the report date
    df["Report Date"] = pd.to_datetime(report_date)

    # Append to list
    df_list.append(df)

# Combine all dataframes
if df_list:
    combined_df = pd.concat(df_list, ignore_index=True)

    # Sort by report date
    combined_df = combined_df.sort_values(by="Report Date")

    # Export combined CSV
    output_file = os.path.join(folder_path, "combined_inventory_report.csv")
    combined_df.to_csv(output_file, index=False)

    print(f"✅ Combined report saved to: {output_file}")
else:
    print("⚠️ No matching CSV files found.")
