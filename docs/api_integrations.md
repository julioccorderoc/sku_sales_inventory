# Platform API Integration Guide

**Project:** Natural Cure Labs — SKU Sales & Inventory Intelligence Pipeline
**Purpose:** Reference for automating the extract phase of each pipeline by replacing manually downloaded CSV reports with programmatic API calls.
**Last Updated:** 2026-03-03

---

## Overview

Five platform integrations are documented here. Each section covers the authentication method, the specific endpoints that replicate the current CSV export data, required scopes/permissions, rate limits, a minimal Python `requests` code example, and any platform-specific approval requirements.

| Platform | Current CSVs | Auth Method | App Review Required? |
|---|---|---|---|
| [Flexport](#1-flexport-logistics-api) | Levels, Orders, Inbound | API Key (Bearer token) | No (credentials already in `.env`) |
| [Amazon SP-API](#2-amazon-sp-api) | FBA report, AWD report, Amazon sales | LWA OAuth + AWS SigV4 | No (self-authorization for own seller account) |
| [Walmart Seller API](#3-walmart-seller-api) | Walmart inventory, Walmart sales | OAuth 2.0 Client Credentials | No (self-served via Seller Center) |
| [TikTok Shop](#4-tiktok-shop-open-platform-api) | TikTok sales, TikTok orders, FBT inventory | OAuth 2.0 + HMAC-SHA256 signing | **Yes** — required for advanced scopes |
| [Shopify Admin API](#5-shopify-admin-api) | Shopify sales | Custom app token (`shpat_…`) | No (custom apps on your own store need no review) |

---

## 1. Flexport Logistics API

**Base URL:** `https://logistics-api.flexport.com`
**API Version:** `2025-03` (stable as of early 2026)
**Current CSVs replaced:** `Flexport_levels_*.csv`, `Flexport_orders_*.csv`, `Flexport_inbound_*.csv`

The project already holds Flexport credentials in `.env`. This is the highest-priority integration — the groundwork is fully laid.

### 1.1 Authentication

Flexport uses **API Key Bearer token** authentication. Every request includes:

```
Authorization: Bearer <FLEXPORT_API_KEY>
Content-Type: application/json
```

`.env` credential mapping:

| `.env` Variable | Shape | Where it goes |
|---|---|---|
| `FLEXPORT_API_KEY` | `shltm_…` | `Authorization: Bearer` header on every request |
| `FLEXPORT_ACCESS_TOKEN` | `shltm_…` | Alternative bearer token; use if `FLEXPORT_API_KEY` yields a 401 — try both |
| `FLEXPORT_CODE` | UUID | OAuth `code` param in an initial `/oauth/token` exchange, if required |
| `FLEXPORT_ACCOUNT_ID` | ULID | Likely `X-Account-Id` header or `accountId=` query param for multi-tenant scoping — confirm against the live OpenAPI spec |
| `BASE_URL` | URL | `https://logistics-api.flexport.com` |

> **Verify:** Fetch `https://logistics-api.flexport.com/logistics/api/2025-03/openapi.json` to confirm whether `FLEXPORT_ACCOUNT_ID` is a header or query param, and whether `FLEXPORT_CODE` is needed before API calls or only for the initial OAuth grant.

### 1.2 Endpoints

All three endpoint paths below are confirmed from the project's `.env` (lines 7–9).

#### Inventory (replaces `Flexport_levels_*.csv`)

```
GET /logistics/api/2025-03/products/inventory/all
GET /logistics/api/2025-03/products/{logisticsSku}/inventory
```

The CSV columns used by `parse_flexport_reports()` are `DTC Total Quantity`, `RS Total Quantity`, and `Ops WIP Quantity` (grouped by `MSKU`). The pipeline derives:

```python
dtc_inventory     = sum(DTC_Total_Quantity)          # per MSKU
reserve_inventory = max(0, RS_Total_Quantity - Ops_WIP_Quantity)  # per MSKU
```

The `{logisticsSku}` path parameter is the DSKU (e.g., `DB59IQ90Q2K`), not the internal MSKU.

Expected response shape:

```json
{
  "items": [
    {
      "logisticsSku": "DB59IQ90Q2K",
      "merchantSku":  "1001",
      "dtcTotalQuantity":     100,
      "rsTotalQuantity":       50,
      "opsWipQuantity":        10,
      "inTransitQuantity":      5
    }
  ],
  "pagination": { "cursor": "...", "hasNext": true }
}
```

#### Orders / Sales (replaces `Flexport_orders_*.csv`)

```
GET /logistics/api/2025-03/orders
```

The CSV columns used: `Order Status`, `Items` (JSON array of `{"dsku": "…", "qty": N}`). The pipeline filters out `CANCELLED` orders and aggregates `qty` per internal SKU via `DSKU_TO_SKU_MAP`.

Expected query parameters:

| Parameter | Description |
|---|---|
| `createdAfter` | ISO 8601 start (e.g., `2026-02-01T00:00:00Z`) |
| `createdBefore` | ISO 8601 end |
| `status` | Omit `CANCELLED`; or filter post-fetch |
| `limit` / `cursor` | Pagination |

Expected response shape:

```json
{
  "orders": [
    {
      "createdAt":    "2026-02-11T12:00:00Z",
      "orderStatus":  "FULFILLED",
      "items": [{ "dsku": "DB59IQ90Q2K", "qty": 2 }]
    }
  ],
  "pagination": { "cursor": "...", "hasNext": false }
}
```

> The `Items` JSON array structure in the current CSV appears to be serialized directly from this API response, which is a strong signal that the field names are correct.

#### Inbound (replaces `Flexport_inbound_*.csv`)

```
GET /logistics/api/2025-03/inbounds/shipments/{shipmentId}   ← single shipment
GET /logistics/api/2025-03/inbounds/shipments                ← list (inferred)
```

The pipeline sums two fields: `IN_TRANSIT_WITHIN_DELIVERR_UNDER_60_DAYS + IN_TRANSIT_TO_DELIVERR` per MSKU.

Expected list query params: `status`, `createdAfter`, `createdBefore`, `limit`, `cursor`.

### 1.3 Scopes / Permissions

No separate scope configuration is required beyond the API key already in `.env`. Flexport API keys are scoped at the account level.

### 1.4 Rate Limits

Not published in this project's codebase. Typical Flexport Logistics API limits are **100–300 requests/minute** per API key. The API returns `HTTP 429` with a `Retry-After` header when exceeded. At NCL's scale (32 SKUs), a single paginated call retrieves all inventory.

Implement exponential backoff on 429 responses (consistent with the pattern planned for EPIC-005).

### 1.5 Pagination

Uses **cursor-based pagination**. Each response includes a `pagination` object:

```json
{ "cursor": "<opaque string>", "hasNext": true }
```

Pass `cursor=<value>` as a query parameter on the next call. Stop when `hasNext` is `false`.

### 1.6 Python Example

```python
import os
import requests

FLEXPORT_API_KEY = os.environ["FLEXPORT_API_KEY"]
BASE_URL         = os.environ.get("BASE_URL", "https://logistics-api.flexport.com")
API_VERSION      = "2025-03"


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {FLEXPORT_API_KEY}",
        "Content-Type": "application/json",
    }


def get_all_inventory() -> list[dict]:
    """
    Fetch all Flexport inventory levels.
    Replaces Flexport_levels_YYYY-MM-DD.csv.
    Returns a list of dicts with keys: logisticsSku, merchantSku,
    dtcTotalQuantity, rsTotalQuantity, opsWipQuantity.
    """
    url = f"{BASE_URL}/logistics/api/{API_VERSION}/products/inventory/all"
    items, cursor = [], None

    while True:
        params = {"cursor": cursor} if cursor else {}
        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        items.extend(data.get("items", []))

        pagination = data.get("pagination", {})
        if not pagination.get("hasNext"):
            break
        cursor = pagination["cursor"]

    return items


def get_orders(created_after: str, created_before: str) -> list[dict]:
    """
    Fetch non-cancelled Flexport orders for a date range.
    created_after / created_before: ISO 8601 strings (e.g. '2026-03-01T00:00:00Z').
    Replaces Flexport_orders_YYYY-MM-DD.csv.
    """
    url = f"{BASE_URL}/logistics/api/{API_VERSION}/orders"
    orders, cursor = [], None

    while True:
        params = {
            "createdAfter":  created_after,
            "createdBefore": created_before,
        }
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for order in data.get("orders", []):
            if order.get("orderStatus") != "CANCELLED":
                orders.append(order)

        pagination = data.get("pagination", {})
        if not pagination.get("hasNext"):
            break
        cursor = pagination["cursor"]

    return orders


# --- Usage ---
if __name__ == "__main__":
    inventory = get_all_inventory()
    print(f"Fetched {len(inventory)} inventory records from Flexport.")

    orders = get_orders("2026-03-02T00:00:00Z", "2026-03-03T00:00:00Z")
    print(f"Fetched {len(orders)} orders from Flexport.")
```

### 1.7 Items to Verify Against the Live OpenAPI Spec

1. Whether `FLEXPORT_ACCOUNT_ID` is passed as `X-Account-Id` header or `accountId=` query param.
2. Whether `FLEXPORT_API_KEY` alone is sufficient or if `FLEXPORT_CODE` must be exchanged for an access token first.
3. Exact JSON field names in the inventory response (`camelCase` vs. `snake_case`).
4. Exact path for listing orders (`/orders`, `/fulfillment/orders`, etc.).
5. Confirmed rate limit values per endpoint.

---

## 2. Amazon SP-API

**Base URL (US):** `https://sellingpartnerapi-na.amazon.com`
**Current CSVs replaced:** `FBA_report_*.csv`, `AWD_Report_*.csv`, `Amazon_sales_*.csv`

### 2.1 Authentication

Amazon SP-API uses **two-layer auth**: an LWA (Login with Amazon) access token on every request, plus AWS Signature Version 4 request signing.

#### Credentials Required

| Credential | Where to Get It |
|---|---|
| `client_id` | Seller Central → Apps & Services → Develop Apps → your app |
| `client_secret` | Same location |
| `refresh_token` | Obtained once via OAuth authorization grant for your seller account |
| `aws_access_key_id` | IAM user linked to your SP-API app |
| `aws_secret_access_key` | Same IAM user |
| `aws_role_arn` | IAM role ARN with `SellingPartnerAPI` managed policy attached |

#### Step 1 — Get LWA Access Token (1-hour TTL)

```
POST https://api.amazon.com/auth/o2/token
Content-Type: application/x-www-form-urlencoded

grant_type=refresh_token
&refresh_token=<REFRESH_TOKEN>
&client_id=<CLIENT_ID>
&client_secret=<CLIENT_SECRET>
```

Response: `{ "access_token": "Atza|...", "expires_in": 3600 }`

#### Step 2 — Sign the Request (AWS SigV4)

Every SP-API HTTP call must be signed with AWS Signature Version 4:

1. Call `STS.AssumeRole` with your IAM user credentials + `aws_role_arn` to get temporary AWS credentials.
2. Use `requests-aws4auth` (or equivalent) to sign the request with those credentials.
3. Add `x-amz-access-token: <LWA_access_token>` as a request header.

#### App Registration

For NCL's own seller account, use **self-authorization** (no app review needed):

1. Go to Seller Central → Apps & Services → Develop Apps.
2. Create a developer profile and register a new application.
3. Select the required roles (FBA Inventory, Analytics, Reports).
4. Create an IAM user/role in your AWS account and link it in the Developer Console.
5. Run the OAuth authorization flow once to obtain the `refresh_token`.

**No third-party app review is required** when the developer and seller are the same entity.

### 2.2 Endpoints

#### FBA Inventory (replaces `FBA_report_*.csv`)

```
GET /fba/inventory/v1/summaries
```

CSV columns used: `Merchant SKU`, `Available`, `FC transfer`, `Inbound`.

| Query Parameter | Value |
|---|---|
| `granularityType` | `Marketplace` |
| `granularityId` | `ATVPDKIKX0DER` (US marketplace) |
| `marketplaceIds` | `ATVPDKIKX0DER` |
| `details` | `true` (required for granular sub-quantities) |
| `nextToken` | Pagination cursor from prior response |

Column mapping:

```python
available   = inventoryDetails["fulfillableQuantity"]
fc_transfer = inventoryDetails["reservedQuantity"]["fcProcessingQuantity"]
inbound     = (inventoryDetails["inboundWorkingQuantity"]
             + inventoryDetails["inboundShippedQuantity"]
             + inventoryDetails["inboundReceivingQuantity"])
```

> **Note:** `Units Sold Last 30 Days` in the current FBA CSV is informational and not used by the pipeline's inventory output (`Inventory`, `Inbound`). It can be omitted or sourced from the Sales API if needed.

#### AWD Inventory (replaces `AWD_Report_*.csv`)

```
GET /awd/2024-05-09/inventory
```

| Query Parameter | Value |
|---|---|
| `details` | `SHOW` (for `inventoryDetails` breakdown) |
| `maxResults` | `100` |
| `nextToken` | Pagination cursor |

Column mapping:

```python
available_awd = inventoryDetails["availableQuantity"]["amount"]
inbound_awd   = inventoryDetails["inboundQuantity"]["amount"]
```

#### Amazon Sales (replaces `Amazon_sales_*.csv`)

**Option A — Reports API (recommended for net sales)**

Report type: `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_DATE_GENERAL`

This is an async process:

1. `POST /reports/2021-06-30/reports` — create report with `dataStartTime` / `dataEndTime`
2. Poll `GET /reports/2021-06-30/reports/{reportId}` until `processingStatus == "DONE"`
3. `GET /reports/2021-06-30/documents/{reportDocumentId}` — get download URL
4. Download and decompress (gzip) the flat file

Column mapping from the flat file: `sku` → MSKU, `quantity` → Units, `item-price` → Revenue (sum per SKU, filter for non-cancelled).

**Option B — Sales Analytics API (simpler, ordered figures only)**

```
POST /analytics/2024-11-15/sales/analytics/search
```

```json
{
  "aggregateBy": "SKU",
  "startDate": "2026-02-01",
  "endDate": "2026-02-28",
  "dimensions": ["SKU"],
  "metrics": ["ORDERED_UNITS", "ORDERED_PRODUCT_SALES"],
  "marketplaceId": "ATVPDKIKX0DER"
}
```

Note: Returns *ordered* figures (not net-of-returns). Use Option A for true net sales.

### 2.3 Required Scopes

| API | LWA Scope |
|---|---|
| FBA Inventory | `sellingpartnerapi::inventory` |
| AWD Inventory | `sellingpartnerapi::inventory` |
| Reports | `sellingpartnerapi::reports` |
| Sales Analytics | `sellingpartnerapi::analytics` |

All require **seller-delegated authorization** (not grantless).

### 2.4 Rate Limits

| API | Rate | Burst |
|---|---|---|
| FBA Inventory (`getInventorySummaries`) | 2 req/sec | 2 |
| AWD Inventory (`listInventory`) | 2 req/sec | 2 |
| Reports — create (`createReport`) | 0.0167 req/sec (1/min) | 15 |
| Reports — status poll (`getReport`) | 2 req/sec | 15 |
| Sales Analytics | 0.033 req/sec (~2/min) | 1 |

At NCL's scale (32 SKUs), FBA/AWD inventory fits in a single request. Implement exponential backoff on `HTTP 429` responses; use the `x-amzn-RateLimit-Limit` response header to adjust dynamically.

### 2.5 Python Example — FBA Inventory

```python
"""
Requires: pip install requests boto3 requests-aws4auth python-dotenv
"""
import os
import requests
from requests_aws4auth import AWS4Auth
import boto3

CLIENT_ID         = os.environ["SP_API_CLIENT_ID"]
CLIENT_SECRET     = os.environ["SP_API_CLIENT_SECRET"]
REFRESH_TOKEN     = os.environ["SP_API_REFRESH_TOKEN"]
AWS_ACCESS_KEY_ID = os.environ["SP_API_AWS_ACCESS_KEY_ID"]
AWS_SECRET_KEY    = os.environ["SP_API_AWS_SECRET_ACCESS_KEY"]
ROLE_ARN          = os.environ["SP_API_ROLE_ARN"]
MARKETPLACE_ID    = "ATVPDKIKX0DER"
REGION            = "us-east-1"
SP_API_BASE       = "https://sellingpartnerapi-na.amazon.com"


def get_lwa_token() -> str:
    resp = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type":    "refresh_token",
            "refresh_token": REFRESH_TOKEN,
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_aws_session_creds() -> dict:
    sts = boto3.client(
        "sts",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=REGION,
    )
    return sts.assume_role(
        RoleArn=ROLE_ARN, RoleSessionName="SPAPISession"
    )["Credentials"]


def get_fba_inventory() -> list[dict]:
    """
    Returns a list of dicts with keys matching the current FBA CSV:
    Merchant SKU, Available, FC transfer, Inbound.
    """
    lwa_token = get_lwa_token()
    creds     = get_aws_session_creds()

    auth = AWS4Auth(
        creds["AccessKeyId"],
        creds["SecretAccessKey"],
        REGION,
        "execute-api",
        session_token=creds["SessionToken"],
    )
    headers = {
        "x-amz-access-token": lwa_token,
        "Content-Type": "application/json",
    }
    params = {
        "granularityType": "Marketplace",
        "granularityId":   MARKETPLACE_ID,
        "marketplaceIds":  MARKETPLACE_ID,
        "details":         "true",
    }

    url, summaries = f"{SP_API_BASE}/fba/inventory/v1/summaries", []
    while True:
        resp = requests.get(url, headers=headers, params=params, auth=auth, timeout=15)
        resp.raise_for_status()
        body = resp.json()
        summaries.extend(body.get("payload", {}).get("inventorySummaries", []))

        next_token = body.get("pagination", {}).get("nextToken")
        if not next_token:
            break
        params = {"nextToken": next_token}

    rows = []
    for item in summaries:
        d = item.get("inventoryDetails", {})
        r = d.get("reservedQuantity", {})
        rows.append({
            "Merchant SKU": item.get("sellerSku", ""),
            "Available":    d.get("fulfillableQuantity", 0),
            "FC transfer":  r.get("fcProcessingQuantity", 0),
            "Inbound": (
                d.get("inboundWorkingQuantity", 0)
                + d.get("inboundShippedQuantity", 0)
                + d.get("inboundReceivingQuantity", 0)
            ),
        })
    return rows
```

### 2.6 Required `.env` Additions

```ini
SP_API_CLIENT_ID="amzn1.application-oa2-client...."
SP_API_CLIENT_SECRET="..."
SP_API_REFRESH_TOKEN="Atzr|..."
SP_API_AWS_ACCESS_KEY_ID="AKIA..."
SP_API_AWS_SECRET_ACCESS_KEY="..."
SP_API_ROLE_ARN="arn:aws:iam::123456789012:role/SellingPartnerAPIRole"
```

---

## 3. Walmart Seller API

**Base URL:** `https://marketplace.walmartapis.com`
**Current CSVs replaced:** `Walmart_inventory_*.csv`, `Walmart_sales_*.csv`

### 3.1 Authentication

Walmart uses **OAuth 2.0 — Client Credentials grant** (no user login required).

**Credentials needed:**

- `Client ID` — Seller Center → Settings → API Access
- `Client Secret` — generated alongside Client ID in Seller Center

**Token endpoint:**

```
POST https://marketplace.walmartapis.com/v3/token
```

Required headers on the token request:

| Header | Value |
|---|---|
| `Authorization` | `Basic <base64(client_id:client_secret)>` |
| `WM_SVC.NAME` | `Walmart Marketplace` |
| `WM_QOS.CORRELATION_ID` | Any UUID you generate per request |
| `Content-Type` | `application/x-www-form-urlencoded` |
| `Accept` | `application/json` |

Body: `grant_type=client_credentials`

Response: `{ "access_token": "eyJ...", "expires_in": 900 }`

Tokens expire in **15 minutes**. Cache and refresh before expiry.

**All subsequent API calls require these headers:**

| Header | Value |
|---|---|
| `Authorization` | `Bearer <access_token>` |
| `WM_SEC.ACCESS_TOKEN` | `<access_token>` (send both for compatibility) |
| `WM_SVC.NAME` | `Walmart Marketplace` |
| `WM_QOS.CORRELATION_ID` | UUID per request |
| `Accept` | `application/json` |

### 3.2 Approval / Access Process

- Credentials are **self-served** via Seller Center for active Marketplace sellers — no separate API approval gate.
- **WFS enrollment** is a separate process; NCL must be enrolled in WFS for the WFS inventory endpoint to return data. Confirm enrollment status with your Walmart account manager.
- No explicit OAuth scope strings — access is controlled by which APIs your seller account is enrolled for.

### 3.3 Endpoints

#### WFS Inventory (replaces `Walmart_inventory_*.csv`)

```
GET https://marketplace.walmartapis.com/v3/fulfillment/inventory
```

| Query Parameter | Description |
|---|---|
| `sku` | Filter by a single seller SKU (optional) |
| `limit` | Max records per page (default 10, max 50) |
| `offset` | Pagination offset |

Column mapping:

| CSV Column | API Response Field |
|---|---|
| `SKU` | `sku` |
| `Available units` | `availableToSellQty` |
| `Inbound units` | `inboundQty` |

#### Sales (replaces `Walmart_sales_*.csv`)

Walmart has no dedicated "Sales Report" API equivalent to the downloaded CSV. Use the **Orders API** to aggregate units and GMV by SKU:

```
GET https://marketplace.walmartapis.com/v3/orders
```

| Query Parameter | Description |
|---|---|
| `createdStartDate` | ISO 8601 start (e.g., `2026-03-01T00:00:00Z`) |
| `createdEndDate` | ISO 8601 end |
| `status` | `Shipped` or `Delivered` to exclude cancelled |
| `limit` | Max 200 per page |
| `nextCursor` | Cursor-based pagination |

Aggregate `orderLineQuantity.amount` → `Units_Sold` and `charges.charge[chargeType=PRODUCT].chargeAmount.amount` → `GMV` per SKU.

### 3.4 Rate Limits

| Endpoint | Rate Limit |
|---|---|
| Orders | 20 req/sec |
| Inventory | 20 req/sec |
| Token endpoint | 10 req/min |

Returns `HTTP 429` when exceeded. At NCL's scale (32 SKUs), a single paginated call retrieves all WFS inventory.

### 3.5 Python Example

```python
import base64
import uuid
import requests

CLIENT_ID     = "your_client_id"
CLIENT_SECRET = "your_client_secret"
BASE_URL      = "https://marketplace.walmartapis.com"


def get_walmart_token() -> str:
    credentials = base64.b64encode(
        f"{CLIENT_ID}:{CLIENT_SECRET}".encode()
    ).decode()
    resp = requests.post(
        f"{BASE_URL}/v3/token",
        headers={
            "Authorization": f"Basic {credentials}",
            "WM_SVC.NAME": "Walmart Marketplace",
            "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        data={"grant_type": "client_credentials"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _walmart_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "WM_SEC.ACCESS_TOKEN": token,
        "WM_SVC.NAME": "Walmart Marketplace",
        "WM_QOS.CORRELATION_ID": str(uuid.uuid4()),
        "Accept": "application/json",
    }


def get_wfs_inventory(token: str) -> list[dict]:
    """
    Fetch all WFS inventory.
    Returns list of dicts matching Walmart_inventory_*.csv columns.
    """
    all_items, limit, offset = [], 50, 0

    while True:
        resp = requests.get(
            f"{BASE_URL}/v3/fulfillment/inventory",
            headers=_walmart_headers(token),
            params={"limit": limit, "offset": offset},
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json().get("payload", {})
        items   = payload.get("inventory", [])

        for item in items:
            all_items.append({
                "SKU":             item.get("sku"),
                "Available units": item.get("availableToSellQty", 0),
                "Inbound units":   item.get("inboundQty", 0),
            })

        total = payload.get("totalCount", 0)
        offset += limit
        if offset >= total:
            break

    return all_items


if __name__ == "__main__":
    token     = get_walmart_token()
    inventory = get_wfs_inventory(token)
    print(f"Fetched {len(inventory)} WFS inventory records.")
```

### 3.6 Required `.env` Additions

```ini
WALMART_CLIENT_ID="..."
WALMART_CLIENT_SECRET="..."
```

---

## 4. TikTok Shop Open Platform API

**Base URL:** `https://open-api.tiktokglobalshop.com`
**Sandbox URL:** `https://sandbox-open-api.tiktokglobalshop.com`
**API Version:** Date-versioned paths (e.g., `202309`); verify the current stable version at <https://partner.tiktok.com/document> before implementation.
**Current CSVs replaced:** `TikTok_sales_*.csv`, `TikTok_orders_*.csv`, `FBT_inventory_*.csv`

### 4.1 Authentication

TikTok Shop uses **OAuth 2.0 Authorization Code grant**. A full OAuth token exchange is required for accessing seller data — there is no simple API-key-only flow.

**Credentials needed:**

| Credential | Where Obtained |
|---|---|
| `app_key` | TikTok Shop Developer Portal after app creation |
| `app_secret` | Developer Portal (keep secret; used for signing) |
| `access_token` | OAuth exchange using the authorization code |
| `refresh_token` | Returned alongside `access_token`; lasts 30 days |

**Step 1 — Redirect seller to the TikTok authorization URL:**

```
https://auth.tiktok-shops.com/oauth/authorize?app_key=YOUR_APP_KEY&state=RANDOM_CSRF_TOKEN
```

**Step 2 — Exchange the authorization code for tokens:**

```curl
POST https://auth.tiktok-shops.com/api/v2/token/get
Content-Type: application/x-www-form-urlencoded

app_key=YOUR_APP_KEY&app_secret=YOUR_APP_SECRET
&auth_code=AUTH_CODE&grant_type=authorized_code
```

Response:

```json
{
  "data": {
    "access_token": "TTP_...",
    "access_token_expire_in": 86400,
    "refresh_token": "TTP_...",
    "refresh_token_expire_in": 2592000
  }
}
```

Access tokens expire in **24 hours**; refresh tokens expire in **30 days**.

**Token refresh:**

```
POST https://auth.tiktok-shops.com/api/v2/token/refresh
Content-Type: application/x-www-form-urlencoded

app_key=YOUR_APP_KEY&app_secret=YOUR_APP_SECRET
&refresh_token=CURRENT_REFRESH_TOKEN&grant_type=refresh_token
```

### 4.2 Request Signing (Required on Every API Call)

Every request (except the auth token exchange) requires an **HMAC-SHA256 signature**:

1. Collect all query and body parameters (exclude `sign` and `access_token`).
2. Sort them alphabetically by key.
3. Concatenate as `key1value1key2value2…` (no separators).
4. Wrap between `app_secret`: `{app_secret}{concatenated_params}{app_secret}`.
5. Compute `HMAC-SHA256(app_secret, wrapped_string)` and hex-encode (uppercase).

```python
import hmac
import hashlib

def generate_sign(app_secret: str, params: dict) -> str:
    """HMAC-SHA256 request signature for TikTok Shop API."""
    exclude = {"sign", "access_token"}
    sorted_items = sorted(
        (k, str(v)) for k, v in params.items() if k not in exclude
    )
    param_str = "".join(f"{k}{v}" for k, v in sorted_items)
    base = f"{app_secret}{param_str}{app_secret}"
    return hmac.new(
        app_secret.encode("utf-8"),
        base.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest().upper()
```

### 4.3 App Review and Approval

**App review is required.** Process:

1. Create a developer account at <https://partner.tiktok.com> (business email required).
2. Create an app and select permission scopes.
3. **Basic scopes** (e.g., `order.read`) are often auto-approved for seller-owned apps.
4. **Advanced scopes** (e.g., `fulfillment.inventory.read`) require submitting a use-case justification; review takes 3–14 business days.
5. Use the sandbox environment (`sandbox-open-api.tiktokglobalshop.com`) during development — no review required for sandbox.

Self-developed apps connecting to your own seller account typically receive expedited approval.

### 4.4 Endpoints

#### TikTok Orders (replaces both `TikTok_orders_*.csv` and `TikTok_sales_*.csv`)

```
POST /order/202309/orders/search
```

The `TikTok_sales_*.csv` is a pre-aggregated version of the same orders data. Once the Orders API is live, `parse_tiktok_sales_report()` can be removed — its output is a direct aggregation of the order line items already pulled by this endpoint.

Request body:

```json
{
  "create_time_ge": 1706745600,
  "create_time_lt": 1706832000,
  "page_size": 100,
  "fulfillment_type": "TIKTOK_FULFILLMENT"
}
```

| Parameter | Description |
|---|---|
| `create_time_ge` | Unix timestamp (inclusive lower bound) |
| `create_time_lt` | Unix timestamp (exclusive upper bound) |
| `fulfillment_type` | `"TIKTOK_FULFILLMENT"` — filters to FBT orders only |
| `page_size` | Max 100 |
| `page_token` | Cursor from previous response |

Order status code mapping:

| Code | Meaning | Pipeline action |
|---|---|---|
| 140 | Cancelled | Exclude (mirrors parser's cancel filter) |
| 130, 122, 121 | Completed / Delivered / In transit | Include |

Response structure (key fields):

```json
{
  "data": {
    "orders": [
      {
        "order_id": "576636912421",
        "order_status": 130,
        "fulfillment_type": "TIKTOK_FULFILLMENT",
        "line_items": [
          {
            "sku_id": "1729499998780101089",
            "seller_sku": "NCL-1001",
            "quantity": 2
          }
        ],
        "payment": { "total_amount": "50.00" }
      }
    ],
    "next_page_token": "abc123..."
  }
}
```

Column mapping to current CSV:

| CSV Column | API Field |
|---|---|
| `Order ID` | `order_id` |
| `Order Status` | `order_status` (integer code) |
| `SKU ID` | `line_items[].sku_id` → `TIKTOK_ID_MAP` |
| `Quantity` | `line_items[].quantity` |
| `Order Amount` | `payment.total_amount` |
| `Fulfillment Type` | `fulfillment_type` |

**Required scope:** `order.read`

#### FBT Inventory (replaces `FBT_inventory_*.csv`)

```
GET /fulfillment/202309/inventories
```

| Query Parameter | Description |
|---|---|
| `app_key` | Your app key |
| `access_token` | Seller access token |
| `timestamp` | Unix epoch (seconds) |
| `sign` | HMAC-SHA256 signature |
| `page_size` | Max 100 |
| `page_token` | Pagination cursor |

Response:

```json
{
  "data": {
    "inventories": [
      {
        "seller_sku": "1001",
        "available_quantity": 80,
        "in_transit_quantity": 10
      }
    ],
    "next_page_token": "..."
  }
}
```

Column mapping:

| CSV Column | API Field |
|---|---|
| `Reference code` | `seller_sku` (your internal SKU — confirm in sandbox) |
| `Available inventory` | `available_quantity` |
| `In Transit: Total Quantity` | `in_transit_quantity` |

The existing `groupby("sku")[["inventory", "inbound"]].sum()` logic in the parser handles multi-warehouse aggregation and requires no change.

**Required scope:** `fulfillment.inventory.read` (or `logistics.fulfillment.read` — confirm exact scope name for your API version)

### 4.5 Rate Limits

| Endpoint Category | Rate Limit |
|---|---|
| Orders search / list | 40 req/min |
| FBT Inventory list | 20 req/min |
| Analytics / Reports | 10 req/min |
| Token refresh | 5 req/min |

Returns `HTTP 429` with `Retry-After` header when exceeded. At NCL's scale, rate limits are not a practical concern.

### 4.6 Python Example — Get FBT Orders

```python
import hmac
import hashlib
import time
import requests
from datetime import datetime, timedelta

APP_KEY      = "your_app_key"
APP_SECRET   = "your_app_secret"
ACCESS_TOKEN = "TTP_your_access_token"
BASE_URL     = "https://open-api.tiktokglobalshop.com"


def generate_sign(params: dict) -> str:
    exclude = {"sign", "access_token"}
    sorted_items = sorted(
        (k, str(v)) for k, v in params.items() if k not in exclude
    )
    param_str = "".join(f"{k}{v}" for k, v in sorted_items)
    base = f"{APP_SECRET}{param_str}{APP_SECRET}"
    return hmac.new(
        APP_SECRET.encode("utf-8"),
        base.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest().upper()


def get_fbt_orders(target_date: datetime) -> list[dict]:
    """
    Fetch all non-cancelled FBT orders for target_date.
    Replaces TikTok_orders_YYYY-MM-DD.csv.
    """
    start_ts = int(target_date.replace(hour=0, minute=0, second=0).timestamp())
    end_ts   = int((target_date + timedelta(days=1)).replace(
                   hour=0, minute=0, second=0).timestamp())

    all_orders, page_token = [], None

    while True:
        timestamp   = int(time.time())
        query_params = {
            "app_key":      APP_KEY,
            "timestamp":    timestamp,
            "access_token": ACCESS_TOKEN,
        }
        body = {
            "create_time_ge":  start_ts,
            "create_time_lt":  end_ts,
            "page_size":       100,
            "fulfillment_type": "TIKTOK_FULFILLMENT",
        }
        if page_token:
            body["page_token"] = page_token

        query_params["sign"] = generate_sign({**query_params, **body})

        resp = requests.post(
            f"{BASE_URL}/order/202309/orders/search",
            params=query_params,
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise RuntimeError(f"TikTok API error {data.get('code')}: {data.get('message')}")

        orders     = data["data"].get("orders", [])
        page_token = data["data"].get("next_page_token")

        for order in orders:
            if order.get("order_status") != 140:   # exclude cancelled
                all_orders.append(order)

        if not page_token or not orders:
            break

    return all_orders


if __name__ == "__main__":
    today  = datetime(2026, 3, 2)
    orders = get_fbt_orders(today)
    print(f"Fetched {len(orders)} FBT orders.")
```

### 4.7 Required `.env` Additions

```ini
TIKTOK_APP_KEY="..."
TIKTOK_APP_SECRET="..."
TIKTOK_ACCESS_TOKEN="TTP_..."
TIKTOK_REFRESH_TOKEN="TTP_..."
```

---

## 5. Shopify Admin API

**Base URL:** `https://<your-store>.myshopify.com/admin/api/2025-01`
**API Version:** `2025-01` (last stable quarterly release as of early 2026)
**Current CSVs replaced:** `Shopify_sales_*.csv`

### 5.1 Authentication

Use a **Custom App** installed directly on the NCL Shopify store. This is the correct approach for internal tooling — no OAuth public app flow, no app review.

**How to create the custom app:**

1. Shopify Admin → Settings → Apps and sales channels → Develop apps.
2. Click "Create an app" (e.g., "NCL Sales Pipeline").
3. Under Configuration → Admin API integration, enable the `read_orders` scope.
4. Click Install app; copy the **Admin API access token** (`shpat_…`) — shown only once.
5. Store in `.env` as `SHOPIFY_ADMIN_TOKEN`.

**Every request header:**

```
X-Shopify-Access-Token: shpat_your_token_here
Content-Type: application/json
```

### 5.2 App Review Requirements

**None.** Custom apps installed directly from your own Shopify Admin do not require Shopify app review. App review is only for apps submitted to the public Shopify App Store.

### 5.3 Endpoints

#### Orders (replaces `Shopify_sales_*.csv`)

**Recommended: GraphQL Admin API** (more efficient, preferred for new integrations)

```
POST /admin/api/2025-01/graphql.json
```

**Also supported: REST API**

```
GET /admin/api/2025-01/orders.json
```

**CSV columns used:** `Sales channel`, `Product variant SKU`, `Quantity ordered`, `Net sales`

The parser buckets on `Sales channel`. The API equivalent is the `source_name` (REST) / `sourceName` (GraphQL) field:

| API `source_name` | CSV `Sales channel` | NCL bucket |
|---|---|---|
| `web` | `Online Store` | `Shopify` |
| `shop_app` / `shop` | `Shop` | `Shopify` |
| `subscription_contract` | `Loop Subscriptions` | `Shopify` |
| `tiktok` | `TikTok` | `TikTok Shopify` |
| `marketplace-connect` | `Marketplace Connect` | `Target` |
| Any other value | — | `Others` |
| Draft orders | filtered out by the parser | Never appear in `orders` endpoint |

> **Action required:** Do a one-time audit of actual `source_name` values from live orders (especially Loop Subscriptions and Marketplace Connect) to confirm exact strings before building the bucketing map. The values above match documented Shopify behavior but can vary slightly by app/version.

**REST query parameters:**

| Parameter | Value |
|---|---|
| `status` | `any` (include fulfilled and closed) |
| `created_at_min` | `2026-02-01T00:00:00Z` |
| `created_at_max` | `2026-03-01T00:00:00Z` |
| `fields` | `id,source_name,line_items,financial_status` |
| `limit` | `250` (maximum) |

**Net sales calculation** (API does not pre-compute this):

```python
net_revenue = (quantity * unit_price) - sum(discount_allocations[].amount)
```

Refunds require a separate call to `/orders/{id}/refunds.json` if true net-of-returns is needed. For a daily forward pull, discount-adjusted revenue is a sufficient approximation.

**Required scope:** `read_orders`

**Note on order history:** Add `read_all_orders` scope if you need orders older than 60 days. For daily incremental pulls (previous day only), `read_orders` is sufficient.

### 5.4 Rate Limits

**REST — Leaky Bucket:**

- Bucket size: 40 requests (80 for Shopify Plus)
- Restore rate: 2 requests/second
- Header to monitor: `X-Shopify-Shop-Api-Call-Limit: 12/40`
- Throttled: `HTTP 429` with `Retry-After` header

**GraphQL — Query Cost Points:**

- Bucket size: 1,000 points
- Restore rate: 50 points/second
- Cost reported in the `extensions.cost` field of each response
- Throttled: `HTTP 200` with `errors[].extensions.code == "THROTTLED"`

At NCL's volume (one store's daily orders), neither limit is a practical concern.

### 5.5 Pagination

**REST:** Cursor-based via `Link` header:

```
Link: <https://...orders.json?limit=250&page_info=abc123>; rel="next"
```

Parse the `Link` header for `rel="next"`. When absent, you are on the last page. **Do not re-send filter params on subsequent pages** — only `limit` and `fields` are valid alongside `page_info`.

**GraphQL:** Use `pageInfo.hasNextPage` and `pageInfo.endCursor`. Pass `endCursor` as the `after` argument on the next request.

### 5.6 Python Example — REST

```python
import os
import re
import time
import requests
from datetime import date, timedelta

SHOP_DOMAIN  = "naturalcurelabs.myshopify.com"
API_VERSION  = "2025-01"
ADMIN_TOKEN  = os.environ["SHOPIFY_ADMIN_TOKEN"]
BASE_URL     = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}"
HEADERS      = {
    "X-Shopify-Access-Token": ADMIN_TOKEN,
    "Content-Type": "application/json",
}

SOURCE_NAME_TO_BUCKET = {
    "web":                    "Shopify",
    "shop_app":               "Shopify",
    "shop":                   "Shopify",
    "subscription_contract":  "Shopify",
    "tiktok":                 "TikTok Shopify",
    "marketplace-connect":    "Target",
    "marketplace_connect":    "Target",
}

def bucket_channel(source_name: str | None) -> str:
    if not source_name:
        return "Others"
    return SOURCE_NAME_TO_BUCKET.get(source_name.lower(), "Others")


def fetch_shopify_orders(report_date: date) -> list[dict]:
    """
    Fetch all orders for report_date and return line-item rows
    matching the current Shopify_sales_*.csv column structure:
      Sales channel, Product variant SKU, Quantity ordered, Net sales
    """
    params = {
        "status":          "any",
        "created_at_min":  f"{report_date}T00:00:00Z",
        "created_at_max":  f"{report_date + timedelta(days=1)}T00:00:00Z",
        "fields":          "id,source_name,line_items",
        "limit":           250,
    }

    rows, url, page = [], f"{BASE_URL}/orders.json", 0

    while url:
        page += 1
        resp = requests.get(
            url,
            headers=HEADERS,
            params=params if page == 1 else None,
            timeout=15,
        )
        if resp.status_code == 429:
            time.sleep(float(resp.headers.get("Retry-After", 2.0)))
            continue
        resp.raise_for_status()

        for order in resp.json().get("orders", []):
            channel = bucket_channel(order.get("source_name"))
            for item in order.get("line_items", []):
                qty         = int(item.get("quantity", 0))
                price       = float(item.get("price", 0))
                discounts   = sum(
                    float(d.get("amount", 0))
                    for d in item.get("discount_allocations", [])
                )
                net_revenue = round((qty * price) - discounts, 2)

                if qty == 0 and net_revenue == 0:
                    continue

                rows.append({
                    "Sales channel":        channel,
                    "Product variant SKU":  item.get("sku") or "",
                    "Quantity ordered":     qty,
                    "Net sales":            net_revenue,
                })

        # Parse Link header for next page
        link = resp.headers.get("Link", "")
        url  = None
        for part in link.split(","):
            if 'rel="next"' in part:
                match = re.search(r"<([^>]+)>", part.strip())
                if match:
                    url = match.group(1)
                    break

        params = None
        if url:
            time.sleep(0.25)

    return rows


if __name__ == "__main__":
    import pandas as pd

    yesterday = date.today() - timedelta(days=1)
    rows      = fetch_shopify_orders(yesterday)
    df        = pd.DataFrame(rows)
    # df matches the CSV structure consumed by parse_shopify_sales_report()
    print(df.head())
```

### 5.7 Required `.env` Additions

```ini
SHOPIFY_ADMIN_TOKEN="shpat_..."
SHOPIFY_DOMAIN="naturalcurelabs.myshopify.com"
```

---

## Summary — CSV to API Mapping

| Current CSV File | Platform | Endpoint | Auth |
|---|---|---|---|
| `Flexport_levels_*.csv` | Flexport | `GET /logistics/api/2025-03/products/inventory/all` | Bearer `FLEXPORT_API_KEY` |
| `Flexport_orders_*.csv` | Flexport | `GET /logistics/api/2025-03/orders` (inferred) | Bearer `FLEXPORT_API_KEY` |
| `Flexport_inbound_*.csv` | Flexport | `GET /logistics/api/2025-03/inbounds/shipments` (inferred) | Bearer `FLEXPORT_API_KEY` |
| `FBA_report_*.csv` | Amazon SP-API | `GET /fba/inventory/v1/summaries` | LWA + SigV4 |
| `AWD_Report_*.csv` | Amazon SP-API | `GET /awd/2024-05-09/inventory` | LWA + SigV4 |
| `Amazon_sales_*.csv` | Amazon SP-API | Reports API flat file (async) | LWA + SigV4 |
| `Walmart_inventory_*.csv` | Walmart | `GET /v3/fulfillment/inventory` | OAuth 2.0 client credentials |
| `Walmart_sales_*.csv` | Walmart | `GET /v3/orders` | OAuth 2.0 client credentials |
| `TikTok_orders_*.csv` | TikTok Shop | `POST /order/202309/orders/search` | OAuth 2.0 + HMAC-SHA256 |
| `TikTok_sales_*.csv` | TikTok Shop | Aggregate from orders endpoint (redundant) | OAuth 2.0 + HMAC-SHA256 |
| `FBT_inventory_*.csv` | TikTok Shop | `GET /fulfillment/202309/inventories` | OAuth 2.0 + HMAC-SHA256 |
| `Shopify_sales_*.csv` | Shopify | `GET /admin/api/2025-01/orders.json` | Custom app token |

---

## Implementation Notes

- **Start with Flexport** — credentials are already in `.env`, three endpoint paths are confirmed, and this replaces three CSVs at once. Run `GET /logistics/api/2025-03/products/inventory/all` with `FLEXPORT_API_KEY` first to validate auth.
- **TikTok Shop requires the most lead time** — submit the app for review early. Use the sandbox URL during development.
- **Amazon SP-API requires AWS setup** — allow time to create the IAM user, IAM role, and link them in the SP-API Developer Console before writing any API code.
- **Shopify and Walmart are the simplest** — Shopify custom app tokens are instant; Walmart credentials are self-served from Seller Center.
- **The `TikTok_sales_*.csv` parser can be deprecated** once the Orders API is live — it is a redundant aggregation of the same data.
- **Endpoint paths marked "(inferred)"** in the Flexport section must be validated against the live OpenAPI spec at `https://logistics-api.flexport.com/logistics/api/2025-03/openapi.json` before building the integration.
- **All five integrations should be gated behind a `--live` flag** or similar to preserve the existing CSV-based flow as a fallback during rollout.
