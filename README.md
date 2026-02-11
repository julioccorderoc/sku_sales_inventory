# Sales & Inventory Intelligence Pipeline

This repository contains the automated pipeline for aggregating, normalizing, and reporting on Sales and Inventory data across multiple channels (Amazon FBA, Walmart WFS, TikTok Shop, Shopify, and Flexport).

## Overview

The pipeline runs two major jobs:

1. **Inventory Report**: Aggregates stock levels from FBA, WFS, AWD, and Flexport (DTC/Reserve).
2. **Sales Report**: Aggregates daily sales from Amazon, Walmart, TikTok, and Shopify.

### Key Features

- **Normalization**: Ensures all master SKUs are present in the output (zero-filling missing data).
- **Validation**: Uses Pydantic models to ensure data integrity.
- **Logging**: Detailed logging to console and `logs/app.log`.
- **Webhooks**: Automatically posts processed data to an n8n webhook.

## Project Structure

```text
.
├── config/             # JSON configuration files
│   └── mappings.json   # SKU mappings and channel order
├── input/              # Place raw CSV reports here
├── logs/               # Log files (rotating)
├── output/             # Processed CSV/JSON reports
├── src/
│   ├── pipelines/      # Core logic (SalesPipeline, InventoryPipeline)
│   ├── ...
├── main.py             # Entry point
└── README.md
```

## Setup

1. **Prerequisites**: [uv](https://github.com/astral-sh/uv) installed
2. **Install Dependencies**:

    ```bash
    uv sync
    ```

3. **Environment Variables**:

    Create a `.env` file in the root directory:

    ```ini
    INPUT_DIR=input
    OUTPUT_DIR=output
    WEBHOOK_URL=https://your-webhook-url.com
    ```

## Usage

1. **Download Reports**: Place the required CSV files in the `input/` folder (see instructions below).
2. **Run Pipeline**:

    ```bash
    uv run main.py
    ```

3. **Check Output**:
    - Processed files will be in `output/`.
    - Logs will be displayed in the terminal and saved to `logs/app.log`.

---

## 📥 Report Download Instructions

> **IMPORTANT**: Files must be named with the format `Prefix_YYYY-MM-DD.csv` matching the **File Date** (usually today's date).
> Example: `Amazon_sales_2025-01-14.csv`

### 1. Amazon (Seller Central)

#### A. Amazon Sales Report

1. **Navigate** to the \'[Selling Economics and Fees](https://sellercentral.amazon.com/sereport)\' report
2. **Configure** the filter to \'Aggregate by Merchant SKU\' and Date Range to **Last 30 Days**, then **Click** Download.
3. **Rename** the file following the convention `Amazon_sales_YYYY-MM-DD.csv`

#### B. FBA Inventory

1. **Navigate** to [**Reports \> Fulfillment \> Restock Inventory**](https://sellercentral.amazon.com/reportcentral/RestockReport/1).
2. **Click** the "Request CSV Download" button.
3. **Rename** the file following the convention `FBA_report_YYYY-MM-DD.csv`

#### C. AWD Inventory

1. **Navigate** to [**AWD \> View Inventory**](https://sellercentral.amazon.com/fba-inventory/gim/inventory-list?ref=asdn_about)
2. **Select** the checkbox for "All eligible FBA SKUs" and **Click** "AWD inventory report"
3. **Rename** the file following the convention `AWD_Report_YYYY-MM-DD.csv`

### 2. Walmart (Seller Center)

#### A. Walmart Sales Report

1. **Navigate** to [**Sales Insights \> Item Sales Report**](https://seller.walmart.com/analytics/sales-insights/item-sales)
2. **Select** **Last 30 Days** and **Click** Download
3. **Retrieve** the generated files from the \'[Report](https://seller.walmart.com/analytics/reports)\' section
4. **Rename** the file following the convention `Walmart_sales_YYYY-MM-DD.csv`

#### B. WFS Inventory

1. **Navigate** to [**WFS \> Inventory**](https://seller.walmart.com/wfs/inventory)
2. **Download** the \'All current items\' file
3. **Rename** the file following the convention `WFS_inventory_YYYY-MM-DD.csv`

### 3. TikTok

#### A. TikTok Sales Report

1. **Navigate** to [**Analytics \> Product Analytics**](https://seller-us.tiktok.com/compass/product-analysis)
2. **Filter** by \'SKU\' and set time range to **Last 30 Days**, then **Download**
3. **Rename** the file following the convention `TikTok_sales_YYYY-MM-DD.csv`

#### B. TikTok Orders Report

1. **Navigate** to [**TikTok Shop \> Orders**](https://seller-us.tiktok.com/order)
2. **Filter** the date range to **Last 30 Days**, then **Download**
3. **Rename** the file following the convention `TikTok_orders_YYYY-MM-DD.csv`

#### C. FBT inventory Report

1. **Navigate** to [**FBT \> Inventory \> Goods Inventory**](https://scm-us.tiktok.com/inventory/list?op_region=US) and **Download** the report
2. **Rename** the file following the convention `FBT_inventory_YYYY-MM-DD.csv`

### 4. Shopify

#### Shopify Sales Report

1. **Navigate** to **[Analytics \> Custom](https://admin.shopify.com/store/naturalcurelabs/analytics/reports/explore?ql=FROM+sales%0A++SHOW+net_sales%2C+quantity_ordered%0A++GROUP+BY+product_variant_sku%2C+sales_channel+WITH+TOTALS%0A++SINCE+-30d+UNTIL+today%0A++ORDER+BY+net_sales+DESC%0AVISUALIZE+net_sales)**
2. **Paste** the SQL Query (check below) into the query editor
3. **Export** the results as \'Separated Values (CSV)\'
4. **Rename** the file following the convention `Shopify_sales_YYYY-MM-DD.csv`

**SQL Query:**

```sql
FROM sales
    SHOW net_sales, quantity_ordered
    GROUP BY product_variant_sku, sales_channel WITH TOTALS
    SINCE -30d UNTIL today
    ORDER BY net_sales DESC
```

### 5. Flexport

#### A. Inventory (Levels)

1. **Navigate** to the [**Reporting tab**](https://portal.flexport.com/reports)
2. **Download** the \'Inventory Levels Report - RS & DTC\' report for inventory data
3. **Rename** the file following the convention `Flexport_levels_YYYY-MM-DD.csv`

#### B. Orders

1. **Navigate** to the [**Reporting tab**](https://portal.flexport.com/reports)
2. **Download** the \'Orders - All Orders\' report for order data, setting the date range to the last 30 days
3. **Rename** the file following the convention `Flexport_orders_YYYY-MM-DD.csv`

#### C. Inbound

1. **Navigate** to the [**Reporting tab**](https://portal.flexport.com/reports)
2. **Download** the \'Inbounds - Inventory Reconciliation Report\' report for inbound data
3. **Rename** the file following the convention `Flexport_inbound_YYYY-MM-DD.csv`
