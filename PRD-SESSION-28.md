# PRD — Session 28: System Enhancement & Optimization

**Date:** February 22, 2026
**System State:** 32 agents, 382 routes, 305 tests passing, 25,432 lines of agent code
**Data:** 839 catalog products, 1,474 price history entries, 63 customers, 122 vendors, 38 quotes, 112 emails

---

## Analysis Summary

Full codebase scan identified **6 high-impact work items** across three categories: workflow automation gaps where data exists but isn't connected, data expansion opportunities where manual steps should be automated, and UI/UX improvements where Mike's daily workflow has friction points.

The common thread: the system collects enormous amounts of data but doesn't yet close the loop on several critical paths. Quotes sit in "pending" without aging out. Leads sit in "new" without nurture. Email drafts pile up without bulk action. The fixes below connect these broken feedback loops.

---

## Work Item 1: Quote Lifecycle Automation

**Problem:** 37/38 quotes are stuck in "pending" status indefinitely. There is no expiration, no aging alert, no revision tracking, and no auto-status-update when a buyer replies. The reply_analyzer agent detects win/loss signals but doesn't write back to the quote record.

**Current state:**
- `quotes` table: 38 rows, all "pending" or "sent"
- `reply_analyzer.analyze_reply()` returns `{signal: "win"|"loss", po_number, triggers}` — but nothing consumes this
- `award_monitor` checks SCPRS for competitor wins — but doesn't update quote status
- No `expires_at` field on quotes
- No revision history (quote edited → old version lost)

**Solution:**

1. **Quote expiration engine** — Add `expires_at` column to quotes table (default: 30 days from creation). Background scheduler checks hourly. When expired: auto-set status to "expired", send Mike a notification, optionally trigger a "last chance" follow-up email draft.

2. **Reply → quote status bridge** — When `email_poller` processes a reply that `reply_analyzer` classifies as "win" (confidence > 0.7), auto-update the matching quote to "won" and create an order stub. For "loss" signals, mark quote "lost" and log the reason. For "question" signals, flag for Mike's review and reset the expiration clock.

3. **Quote revision tracking** — Before any quote update, snapshot the current version into a `quote_revisions` table (quote_number, revision_num, snapshot_json, revised_at, reason). Show revision history on the quote detail page with diff view.

4. **Award monitor → quote close** — When `award_monitor` finds a SCPRS PO matching an open quote's agency + items but awarded to a different vendor, auto-close the quote as "lost_to_competitor" with the competitor name and winning price.

**Touches:** `email_poller.py`, `reply_analyzer.py`, `award_monitor.py`, `orchestrator.py`, `db.py` (schema), `routes_intel.py` (quote detail page), `notify_agent.py`

**Estimated complexity:** Medium-high (schema migration + 3 new cross-agent flows)

---

## Work Item 2: Email Outbox Overhaul — Bulk Actions + Failed Retry + Engagement Tracking

**Problem:** 80 email drafts sit unreviewed with no bulk action capability. 16 emails failed to send with no retry mechanism. Zero visibility into whether sent emails were opened or clicked.

**Current state:**
- `email_outbox.json`: 112 entries (80 draft, 16 approved, 16 failed)
- Outbox page shows list but requires one-by-one approve/send
- Failed sends are permanent — no retry
- No open/click tracking pixel or link wrapping
- `follow_up_engine` scans outbox but can't distinguish "sent and ignored" from "sent and opened"

**Solution:**

1. **Bulk outbox actions** — Add "Select All / Deselect All" checkbox to outbox page. "Bulk Approve" button approves all selected drafts. "Bulk Send" sends all approved. "Bulk Delete" removes stale drafts. Filter buttons: "All | Drafts | Approved | Failed | Sent".

2. **Failed email retry** — Add `retry_count` and `last_error` fields to outbox entries. Background scheduler (every 15 min) retries failed emails up to 3 times with exponential backoff (15 min, 1 hour, 4 hours). After 3 failures, mark as "permanently_failed" and notify Mike.

3. **Email engagement tracking** — When sending, insert a 1x1 tracking pixel URL (`/api/email/track/<email_id>/open`) and wrap links with redirect tracking (`/api/email/track/<email_id>/click?url=...`). Log opens/clicks with timestamp and IP to `email_engagement` table. Show open/click status on outbox page with green/yellow/gray indicators. Feed this data into `follow_up_engine` — if opened but no reply after 3 days, trigger a follow-up draft.

4. **Outbox dashboard widget** — Add a summary card to the home page: "📧 Outbox: 80 drafts pending, 3 failed (retrying), 16 sent today, 42% open rate this week."

**Touches:** `email_outreach.py`, `notify_agent.py`, `follow_up_engine.py`, `routes_crm.py` (outbox page), `dashboard.py` (home widget), `db.py` (email_engagement table)

**Estimated complexity:** Medium (bulk UI + retry scheduler + tracking pixel endpoint)

---

## Work Item 3: Lead Nurture Automation + Lead → Customer Conversion

**Problem:** 26/39 leads are stuck in "new" status with no automated nurture sequence. There's no mechanism to convert a qualified lead into a CRM contact/customer. Lead scores are static — they don't recalculate when new intel arrives.

**Current state:**
- `leads.json`: 39 leads (26 new, 13 contacted)
- `lead_gen_agent.score_opportunity()` scores at creation time only
- No drip campaign / nurture sequence
- No lead→customer conversion path
- `growth_agent` and `lead_gen_agent` maintain separate prospect lists (`growth_prospects.json` vs `leads.json`)

**Solution:**

1. **Lead nurture sequences** — Define 3-step nurture sequences per lead type. Step 1 (Day 0): Initial outreach email draft. Step 2 (Day 7): Follow-up with value prop. Step 3 (Day 21): "Checking in" with case study. Each step auto-drafts to outbox. If the lead replies at any step, pause the sequence and alert Mike.

2. **Dynamic lead rescoring** — Add a `rescore_leads()` function that runs daily. Factors: original SCPRS score + new PO activity from that agency (from `scprs_intelligence_engine`) + email engagement (opens/clicks from Work Item 2) + recency of last contact. Surface score changes as notifications: "Lead CDCR-Gloves score increased 62→78 — new PO activity detected."

3. **Lead → customer conversion** — Add a "Convert to Customer" button on lead detail page. Auto-creates a CRM contact record with all lead data pre-filled (name, email, phone, agency, institution). Links the lead record to the new contact ID. Moves lead status to "converted". Triggers a "welcome" email draft.

4. **Unified prospect pipeline** — Merge `growth_prospects.json` into `leads.json` with a `source` field ("scprs_scan", "growth_outreach", "manual", "inbound_inquiry"). Single pipeline view on the Growth page showing all prospects regardless of source, sortable by score, status, and age.

**Touches:** `lead_gen_agent.py`, `growth_agent.py`, `follow_up_engine.py`, `email_outreach.py`, `scprs_intelligence_engine.py`, `routes_crm.py` (conversion UI), `routes_intel.py` (unified pipeline view)

**Estimated complexity:** Medium (nurture scheduler + rescore logic + conversion endpoint + UI merge)

---

## Work Item 4: Revenue Dashboard + Pipeline Forecasting

**Problem:** Revenue tracking shows $0.00 with 0 entries despite 2 orders existing. There's no pipeline-based revenue forecast. No margin tracking per deal. No monthly/quarterly breakdowns. The `$2M goal` referenced in market intelligence has no progress visualization.

**Current state:**
- `revenue_log` table: 14 rows but `intel_revenue.json` shows 0 entries (sync gap)
- `forecasting.py` has `score_quote()` and `score_all_quotes()` but they're not surfaced in any dashboard
- `winning_prices` table: 28 rows (data exists for margin calc)
- `market_intelligence.json` has TAM segments but no actual-vs-target tracking
- No visual revenue chart anywhere in the UI

**Solution:**

1. **Revenue reconciliation** — Sync revenue_log (SQLite) with `intel_revenue.json`. When an order is marked "fulfilled" or an invoice is created in QuickBooks, auto-log revenue. Include: amount, date, agency, institution, quote_number, po_number, margin_pct.

2. **Pipeline forecast engine** — Extend `forecasting.py` with `forecast_pipeline()`. For each open quote, multiply total × win probability (from `predictive_intel`). Sum = weighted pipeline value. Show: "Pipeline: $145K weighted ($320K total) | Won YTD: $85K | Goal: $2M (4.25% achieved)."

3. **Revenue dashboard page** — New `/revenue` page with:
   - Monthly revenue bar chart (actual vs target)
   - Pipeline funnel visualization (leads → quotes → orders → revenue)  
   - Top 5 customers by revenue
   - Margin analysis (avg margin %, lowest-margin deals flagged)
   - Rolling 12-month trend line
   - Goal tracker: progress bar toward $2M with projected hit date

4. **Margin tracking** — On every quote, calculate margin = (sell_price - cost) / sell_price for each line item. Pull cost from `catalog_price_history` or `price_history`. Store on the quote record. Flag quotes with margin < 15% in orange, < 10% in red. Show aggregate margin on revenue dashboard.

**Touches:** `forecasting.py` (expand), `manager_agent.py` (revenue summary), `quickbooks_agent.py` (auto-log), `routes_catalog_finance.py` (new revenue page), `db.py` (margin columns), `predictive_intel.py` (pipeline scoring)

**Estimated complexity:** Medium-high (new page + reconciliation + forecast math + charts)

---

## Work Item 5: Vendor Intelligence + Preferred Vendor Ranking

**Problem:** 115/122 vendors have no email. Zero vendors have websites stored. No vendor performance scoring. No preferred vendor ranking by product category. When building quotes, Mike has no quick way to find the best vendor for a given product.

**Current state:**
- `vendors.json`: 122 vendors, only 7 with email, 7 with phone, 0 with website
- `vendor_registration.json`: 8 registrations (disconnected from vendors.json)
- `vendor_ordering_agent.py` can search Grainger + Amazon but doesn't rank results
- `product_catalog` has 839 products with cost data but no vendor linkage
- `catalog_price_history`: 1,602 price records that could build vendor scorecards

**Solution:**

1. **Vendor enrichment engine** — New background agent that processes vendors with missing data. For each vendor name, search the web for their website, email, phone, and GSA contract status. Store results. Run on a schedule (weekly) and on-demand. Target: get email coverage from 6% to 50%+.

2. **Vendor scorecard** — Score each vendor on: price competitiveness (avg price vs market for same items), delivery reliability (from order tracking data), response time (from email timestamps), catalog breadth (how many of our product categories they cover). Store as `vendor_score` in vendors table. Show on vendor detail page as a radar chart.

3. **Preferred vendor matrix** — For each product category in the catalog, rank vendors by: (a) best price, (b) fastest delivery, (c) reliability. Show as a matrix on the vendor page. When building a quote, auto-suggest the preferred vendor for each line item.

4. **Vendor compare tool** — On the quote builder, when Mike selects a product, show a side-by-side comparison of 2-3 vendors: price, lead time, minimum order, GSA pricing. Pull from `catalog_price_history` and `price_history`.

**Touches:** `vendor_ordering_agent.py` (enrichment), `product_catalog.py` (vendor linkage), `routes_catalog_finance.py` (vendor page + compare tool), `db.py` (vendor_scores table), new `vendor_intelligence.py` agent

**Estimated complexity:** Medium (enrichment agent + scoring logic + matrix UI)

---

## Work Item 6: UI/UX — Mobile Responsive + Dashboard Density + Smart Defaults

**Problem:** The dashboard uses inline styles with no responsive breakpoints — unusable on mobile/tablet. Home page shows a basic brief but doesn't surface actionable items. Several pages load all data without pagination. No dark mode. Polling is 30-second fixed interval with no websocket upgrade path.

**Current state:**
- 11 `@media` refs across entire codebase (minimal responsiveness)
- 5 viewport meta tags (inconsistent)
- 77 modals but no responsive modal sizing
- All tables load full dataset (no server-side pagination)
- Home page brief is text-heavy, not action-oriented
- 30-second polling for real-time updates

**Solution:**

1. **Action-oriented home dashboard** — Redesign the home page around Mike's daily workflow. Replace the text brief with action cards:
   - 🔴 **Urgent** (red): Failed emails needing retry, expiring quotes (< 3 days), stale price checks
   - 🟡 **Action Needed** (yellow): Drafts awaiting approval (count), new leads to review, follow-ups due today
   - 🟢 **Progress** (green): Quotes sent this week, orders fulfilled, revenue this month
   - Each card is clickable → navigates to the relevant page with filters pre-applied

2. **Responsive CSS framework** — Extract all inline styles into a `styles.css` served from `/static/`. Add responsive breakpoints at 768px (tablet) and 480px (mobile). Key changes: nav collapses to hamburger menu, tables become card-based on mobile, modals go full-screen on small screens, font sizes scale down 15%.

3. **Server-side pagination** — Add `?page=1&per_page=25` to all list endpoints. Implement in: quotes list, orders list, outbox, leads, contacts, vendors, price checks, catalog. Show page controls at bottom. Default 25 rows, max 100.

4. **Smart defaults + quick actions** — On every list page, add a "Quick Actions" bar at the top:
   - Quotes page: "New Quote" button, filter by status pills (pending/sent/won/lost/expired)
   - Orders page: filter by status, "Needs Invoice" badge count
   - Contacts page: "Recently Active" toggle, "Missing Email" filter
   - Outbox page: "Approve All Drafts" button, "Retry Failed" button

5. **Keyboard shortcuts expansion** — Add global shortcuts visible via `?` overlay: `N` = new quote, `S` = search, `B` = daily brief, `O` = outbox, `P` = pipeline. Add per-page shortcuts: on quote detail `E` = edit, `A` = approve, `D` = download PDF.

**Touches:** `dashboard.py` (home page), `templates.py` (base template), all `routes_*.py` (pagination), new `static/styles.css`

**Estimated complexity:** High (CSS refactor across all pages + pagination on 8+ endpoints + home redesign)

---

## Priority Ranking

| # | Item | Impact | Effort | Priority |
|---|------|--------|--------|----------|
| 1 | Quote Lifecycle Automation | 🔴 Critical — quotes dead-ending | Medium | **P0** |
| 2 | Email Outbox Overhaul | 🔴 Critical — 80 drafts piling up | Medium | **P0** |
| 4 | Revenue Dashboard + Forecasting | 🟡 High — no revenue visibility | Medium-High | **P1** |
| 3 | Lead Nurture Automation | 🟡 High — 26 leads rotting | Medium | **P1** |
| 5 | Vendor Intelligence | 🟢 Medium — competitive advantage | Medium | **P2** |
| 6 | UI/UX Responsive + Dashboard | 🟢 Medium — daily friction | High | **P2** |

**Recommended execution order:** 1 → 2 → 4 → 3 → 5 → 6

Items 1 and 2 fix broken feedback loops where data is being lost today. Item 4 gives Mike revenue visibility he currently has zero of. Item 3 prevents lead decay. Items 5 and 6 are competitive advantages and polish.

---

## Data Flow After All 6 Items

```
Inbound Email
  → email_poller (classify)
    → Price Check → orchestrator → quote → [NEW: expiration engine]
    → RFQ → quote → send → [NEW: engagement tracking]
    → Reply → reply_analyzer → [NEW: auto-update quote status]
    → Shipping → [NEW: auto-update order tracking]
    → Support → cs_agent → [NEW: convert inquiry to lead]

SCPRS Scanner (scheduled)
  → award_monitor → [NEW: auto-close lost quotes]
  → lead_gen → [NEW: nurture sequence] → email_outreach
  → [NEW: vendor scorecard data]

Quote Lifecycle
  → Created → Sent → [NEW: tracking pixel] → Opened?
    → Yes + Reply "win" → [NEW: auto-create order] → revenue_log
    → Yes + No reply → [NEW: follow-up at Day 3]
    → No open after 7d → [NEW: follow-up escalation]
    → 30d no action → [NEW: auto-expire + notify]

Revenue Flow
  → [NEW: auto-log from QB invoices + won quotes]
  → [NEW: pipeline forecast = Σ(quote_total × win_prob)]
  → [NEW: margin tracking per line item]
  → [NEW: revenue dashboard with goal tracker]
```

---

## Success Metrics

| Metric | Current | Target After Implementation |
|--------|---------|----------------------------|
| Quote conversion rate | 11% (2/18) | 25%+ (with follow-up automation) |
| Quotes stuck in "pending" | 97% (37/38) | < 20% (expiration + auto-close) |
| Email drafts unreviewed | 80 | < 10 at any time (bulk actions) |
| Leads in "new" status | 67% (26/39) | < 25% (nurture sequences) |
| Revenue visibility | $0 tracked | Real-time with forecast |
| Vendor email coverage | 6% (7/122) | 50%+ (enrichment engine) |
| Mobile usability | Broken | Fully responsive |
| Failed emails recovered | 0% | 80%+ (auto-retry) |
