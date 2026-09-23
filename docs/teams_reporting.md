# Internal Publishing — Workbooks & Teams

**Status:** Active. Replaces the n8n workflow `update-sku-data`
(`naturalcurelabs.app.n8n.cloud/webhook/eb9e321c-…`).

The pipeline no longer POSTs a webhook. After each run it publishes in-process:
history workbook, snapshot workbook, deltas, and two Teams messages.

---

## What a run does

| # | Step | Destination |
|---|------|-------------|
| 1 | Append every row (append-only audit trail) | `historical_SKU_data.xlsx` → `inventory` / `sales` |
| 2 | Read the previous snapshot | `Inventory.xlsx` → `raw_inventory` / `raw_sales` |
| 3 | Compute `Days_Since_Last_Report` + `Delta_*` per `sku_channel_id` | in memory |
| 4 | Upsert the new snapshot (match on `sku_channel_id`) | `Inventory.xlsx` → `raw_inventory` / `raw_sales` |
| 5 | Post the per-channel summary | Teams |
| 6 | Post the anomaly report | Teams |

Both workbooks live at
`damonsun-my.sharepoint.com/personal/julio_naturalcurelabs_com` →
`Company/Operations/Supply Chain/`.

Step 3 aborts the run when step 2 fails: without the previous snapshot the
deltas would be fiction, and writing the new snapshot would destroy the state
needed to compute them tomorrow.

---

## Configuration

```ini
# Microsoft Graph — app registration shared with supply_chain_agent
MS_TENANT_ID="…"
MS_CLIENT_ID="…"
MS_CLIENT_SECRET="…"          # app-only grant: workbooks (never expires)
MS_REFRESH_TOKEN="…"          # delegated grant: Teams chat (expires in 90 days)
MS_MAILBOX_ADDRESS="julio@naturalcurelabs.com"

MS_INVENTORY_WORKBOOK_ID="016XA7EZ36EKRNDCJNUZA3JXJTLH4YB44K"
MS_HISTORY_WORKBOOK_ID="016XA7EZ74NOVOHL26LZGIUGKTW25ZWYLA"

PUBLISH_ENABLED="true"        # master switch for the whole publish step
TEAMS_TRANSPORT="file"        # webhook | graph_chat | file | none
TEAMS_WEBHOOK_URL=""
TEAMS_WEBHOOK_PAYLOAD="message"
TEAMS_CHAT_ID=""
PUBLISH_SEND_GAP_SECONDS="2"

WEBHOOK_ENABLED="false"       # legacy n8n lane — leave false
```

`TEAMS_TRANSPORT="file"` writes each report to `output/<name>.html` instead of
posting it. That is the default until a destination is configured, so a run is
never blocked by a missing Teams secret.

---

## Enabling Teams

### Option A — Power Automate Workflows webhook (in use)

No Graph permission, no admin consent, and nothing that expires.

**Configured:** `TEAMS_TRANSPORT="webhook"` + `TEAMS_WEBHOOK_PAYLOAD="adaptive_card"`.

The flow behind `TEAMS_WEBHOOK_URL` is the "Post to a channel when a webhook
request is received" template, whose action posts an **Adaptive Card**. Two
consequences, both already handled:

- The body must be the full card envelope
  (`{"type": "message", "attachments": [{contentType, content}]}`). A plain
  `{"message": …}` is accepted with HTTP 202 and then posts **nothing** — the
  action has no card to render. That is why `adaptive_card` is the mode.
- Cards render no HTML at all. The reports are therefore **rebuilt** for the
  card in `src/reporting/cards.py` from the same computed data the HTML
  builders use — detection is shared (`anomalies.py`), only presentation
  differs. Nothing is converted from HTML.

### Card design

`cards.py` uses the **`Table` element** (Adaptive Cards 1.5), which renders as
the bordered grid — verified in this tenant. Three deliberate choices:

| Choice | Why |
|---|---|
| **Four columns**, not six | `Sold` and `Stock` cells carry the current value *and* its move (`203 (−178)`), so the old report's `Prev` + `Δ` pairs collapse. Halves the table width. |
| **Columns sized to their content** | The `Table` schema takes numbers (relative weights) or `"<n>px"` — there is no `"auto"`. `_column_weight()` measures the text actually being rendered (8.5px a character at 14px Segoe UI, emoji counted double) and uses the measurement as the weight. **Always emit a bare number, never a px string** — see the gotcha below. |
| **Top 3 per channel** | Ranked by severity (`Issue.severity`: zero sales outranks everything, continuous swings cap at 100%). A channel with thirty flagged SKUs is a wall, not a report. |
| **Dropped rows are stated** | `+N more flagged rows — full detail in historical_SKU_data.xlsx`. A silent trim would read as "nothing else happened". |

The TikTok Shop-vs-Shopify reconciliation is **card-only removed** — it was
noise in the channel. It is still in the HTML report (`output/*.html`).

Measured worst case (8 channels, every one at its cap): **~17 KB**, inside
Teams' ~28 KB card limit. A summary card is ~1 KB.

### Gotcha: `"96px"` is not a pixel width in Teams

The Adaptive Cards **SDK** honours a `"<n>px"` column width as fixed pixels.
**Teams parses the number out and treats it as a weight.** A card declaring
`["96px", "164px", "96px", 3]` therefore looked correct in a local SDK render
and collapsed the last column to under 1% of the table in the channel.

Numbers are the only form both renderers read the same way, so every width
`_column_weight()` returns is a bare int. A unit test asserts it.

### Previewing a layout

The SDK is still the fastest way to check a layout before posting — render the
card in a headless browser at the real post width (~660px) and look at it. Just
remember the px-string difference above when reading the result.

The HTML renderers are unchanged and still the `file` transport's output —
`output/*.html` is the full, untrimmed report.

To point it somewhere else, create the workflow from the target channel and
replace `TEAMS_WEBHOOK_URL`. Verify with:

```bash
python main.py --test-teams
```

One clearly-labelled card lands in the channel.

If a future flow uses a "Post message in a chat or channel" action instead
(which takes HTML directly), set `TEAMS_WEBHOOK_PAYLOAD="message"` and the
reports are posted as-is, tables and all.

### Option B — Graph chat message

Works with the credentials already present (`ChatMessage.Send`). It posts to a
**chat**, not the Supply Chain channel — Graph channel messages need
`ChannelMessage.Send`, which this app registration does not have.

```ini
TEAMS_TRANSPORT="graph_chat"
TEAMS_CHAT_ID="19:…@thread.v2"
```

The refresh token behind this lane dies **90 days after issue**
(`MS_REFRESH_TOKEN_ISSUED_AT`, currently 2026-09-15 → ~2026-12-14). Re-mint it
with `supply_chain_agent/scripts/bootstrap_graph_token.py` before then.

---

## Commands

```bash
# Normal run — publishes to the workbooks and posts to Teams
python main.py

# Dry run — reads the snapshot, builds both reports, writes NOTHING to Excel,
# posts nothing live. Reports land in output/*.html.
python main.py --test

# Publish a report date that was already pushed (duplicates the history rows)
python main.py --force-publish

# One-off Teams delivery smoke test
python main.py --test-teams
```

---

## Idempotency

Every publish is recorded in `output/publish_ledger.json` under
`<report_type>:<report_date>`. A second run for the same date is refused:

```
⏭️  inventory for 2026-09-23 was already published at 2026-09-23T14:02:11 — skipping.
```

Without the guard, a re-run would duplicate all ~192 history rows *and* report
deltas of zero (it would compare today against today). `--force-publish`
overrides it. Dry runs never touch the ledger.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Microsoft Graph credentials are not configured` | `MS_*` missing from `.env` | Copy them from `supply_chain_agent/.env` |
| `invalid_grant` / `microsoft_refresh_token_expired` | delegated token past its 90-day cap | Re-mint via `bootstrap_graph_token.py`, update `MS_REFRESH_TOKEN` |
| `invalid_client` | `MS_CLIENT_SECRET` rotated or wrong | Rotate in Entra → Certificates & secrets, update `.env` |
| HTTP 202 but nothing posts | the flow's action expects a card and the body carried none | Set `TEAMS_WEBHOOK_PAYLOAD="adaptive_card"` |
| Card shows no grid, just text | Teams client predates Adaptive Cards 1.5 | `cards.py` would need the `Table` swapped for `ColumnSet` rows |
| Nothing in the channel, run succeeded | `TEAMS_TRANSPORT` still `file` | Check `output/*.html`; set the transport + URL |
| `already published` on a legitimate re-run | ledger guard | `--force-publish` |

Logs go to `logs/app.log` and stdout. The publisher logs one line per step.

---

## What has been verified

Against the live tenant, using a throwaway copy of the history workbook
(deleted afterwards — the live workbooks were never written to):

- reading all four sheets
- appending rows, landing directly under the last data row
- upserting a matched key in place
- inserting a brand-new key
- dates written as Excel serials and rendering as real dates (`23-Sep-2026`)

Against the live Teams destination:

- a smoke card (`main.py --test-teams`)
- a full anomaly report card, rendered from the real builder output

The four HTML builders and the markdown conversion are covered by unit tests
(`tests/test_reporting_html.py`), as are the delta maths
(`tests/test_deltas.py`), the workbook mechanics (`tests/test_excel.py`) and
the publish orchestration (`tests/test_publisher.py`).
