# PRD-v32 — Production Audit + Next Phase
**Date:** 2026-03-03 · **Auditor:** Claude · **Codebase:** Reytech-RFQ v8.0
**Stats:** 101,613 lines · 695 routes (86 pages + 609 APIs) · 46 templates · 16 route modules · 41 agents · 826 commits

---

# PART 1: FULL SYSTEM AUDIT

## 1. Architecture

**Stack:** Python 3.12 / Flask / SQLite (WAL) / Jinja2 / Gunicorn
**Deploy:** Railway auto-deploy from GitHub `main`, persistent volume at `/data`
**Auth:** Session-based with `@auth_required` decorator on 682/695 routes

### Module Map

| Layer | Files | Lines | Purpose |
|-------|-------|-------|---------|
| `src/api/modules/` | 16 route files | 32,444 | All HTTP endpoints |
| `src/agents/` | 41 agent files | 28,109 | External integrations, intelligence |
| `src/api/dashboard.py` | 1 | 4,166 | Blueprint, module loader, core routes |
| `src/core/` | 8 files | 6,891 | DB, security, paths, data access |
| `src/forms/` | 8 files | 6,340 | PDF generation, form filling |
| `src/knowledge/` | 4 files | 2,179 | Pricing oracle, won quotes |
| `src/templates/` | 46 HTML | 13,639 | All UI (+ ~6,805 lines inline JS) |

### Top 5 Largest Files

1. `routes_intel.py` — 6,281 lines (growth, SCPRS, revenue, QB, analytics)
2. `routes_pricecheck.py` — 5,518 lines (price check workflow)
3. `growth_agent.py` — 4,179 lines (104 functions, 3-phase intel engine)
4. `dashboard.py` — 4,166 lines (module loader, core routes)
5. `routes_crm.py` — 3,935 lines (CRM, contacts, activity feed)

---

## 2. Compilation & Syntax Audit

| Check | Result |
|-------|--------|
| Python compilation (105 files) | ✅ All pass |
| Jinja2 template parsing (46 files) | ✅ All pass |
| Growth page render test (full data) | ✅ 84,324 chars rendered |
| Growth page render test (empty data) | ✅ 56,907 chars rendered |

---

## 3. Security Audit

### 🔴 CRITICAL: SQL Injection (16 files, 31 instances)

f-string interpolation in SQL queries. All behind auth, but still exploitable by authenticated users or if auth is bypassed.

| File | Instances |
|------|-----------|
| `src/agents/product_catalog.py` | 7 |
| `src/agents/scprs_universal_pull.py` | 4 |
| `src/core/db.py` | 3 |
| `src/agents/email_lifecycle.py` | 2 |
| `src/agents/award_monitor.py` | 2 |
| `src/api/modules/routes_pricecheck.py` | 2 |
| 10 other files | 1 each |

**Fix:** Convert all to parameterized queries. Pattern: `cursor.execute(f"SELECT * FROM t WHERE x = '{val}'")` → `cursor.execute("SELECT * FROM t WHERE x = ?", (val,))`

### 🟡 WARNING: Unprotected Admin Routes (2)

| Route | File | Risk |
|-------|------|------|
| `/api/email-trace` | `dashboard.py` | Exposes email processing logs |
| `/api/disk-cleanup` | `dashboard.py` | Can delete files |

**Intentionally unprotected (OK):** `/health`, `/api/email/track/*/open`, `/api/email/track/*/click`, `/api/voice/webhook`, `/api/qb/callback`, `/api/v1/rfqs`

### 🔵 INFO: Code Quality

| Issue | Count | Notes |
|-------|-------|-------|
| Bare `except:` clauses | 74 | Should specify exception types |
| POST without CSRF | 230 | Relies on session auth only |
| TODO/FIXME/HACK | 9 | Minor cleanup items |
| `open().read()` without `with` | 2 | In `qa_agent.py` |

---

## 4. Feature Inventory — All Pages

### Core Business Pages (daily use)

| # | Page | Route | Template | Status |
|---|------|-------|----------|--------|
| 1 | Home | `/` | `home.html` | ✅ Production — KPIs, activity feed, manager brief, growth card |
| 2 | Price Checks | `/pricechecks` | `pc_detail.html` | ✅ Production — archive with 4-status workflow |
| 3 | PC Detail | `/pricecheck/<id>` | `pc_detail.html` | ✅ Production — parse, lookup, price, generate (2,458 lines) |
| 4 | Quotes | `/quotes` | `quotes.html` | ✅ Production — KPI cards, status tracking |
| 5 | Quote Detail | `/quote/<qn>` | `quote_detail.html` | ✅ Production — line items, PDF gen |
| 6 | Orders | `/orders` | `orders.html` | ✅ Production — lifecycle, margins, timeline |
| 7 | Order Detail | `/order/<oid>` | `order_detail.html` | ✅ Production — delivery tracking, supplier tags |
| 8 | PO Tracking | `/po-tracking` | `po_tracking.html` | ✅ Production — delivery status dashboard |
| 9 | Follow-Ups | `/follow-ups` | `follow_ups.html` | ✅ Production — cohort analysis, call queue |

### CRM & Intelligence Pages

| # | Page | Route | Template | Status |
|---|------|-------|----------|--------|
| 10 | CRM | `/contacts` | `crm.html` | ✅ Production — 7 KPIs, bulk actions, sortable |
| 11 | Catalog | `/catalog` | `catalog.html` | ✅ Production — supplier pricing, freshness |
| 12 | Analytics | `/analytics` | `analytics.html` | ✅ Production — growth metrics integration |
| 13 | Pipeline | `/pipeline` | `pipeline.html` | ✅ Production — velocity, stale quotes, revenue goals |
| 14 | Search | `/search` | `search.html` | ✅ Production — universal entity search |
| 15 | Vendors | `/vendors` | `vendors.html` | ✅ Production — KPI cards, directory |
| 16 | Revenue | `/revenue` | `revenue.html` | ✅ Production — tracking, goal progress |
| 17 | Competitors | `/competitors` | (inline) | ✅ Production — SCPRS-based competitor intel |

### Growth Engine Pages

| # | Page | Route | Template | Status |
|---|------|-------|----------|--------|
| 18 | Growth Engine | `/growth` | `growth.html` | ✅ Production — V3 with 12 tabs, 3-phase intel |
| 19 | Prospect Detail | `/growth/prospect/<id>` | `prospect_detail.html` | ✅ Production — win prob, workflow, events |
| 20 | Growth Intel | `/growth-intel` | `growth_intel.html` | ✅ Production — intelligence dashboard |
| 21 | SCPRS Intel | `/intel/scprs` | `scprs_intel.html` | ✅ Production — SCPRS data browser |
| 22 | Market Intel | `/intel/market` | `market_intel.html` | ✅ Production — market intelligence |

### Operations Pages

| # | Page | Route | Template | Status |
|---|------|-------|----------|--------|
| 23 | Settings | `/settings` | `settings.html` | ✅ Production — health, storage, nav config |
| 24 | Agents | `/agents` | `agents.html` | ✅ Production — AI agent status, health sweep |
| 25 | Form Filler | `/form-filler` | `form_filler.html` | ✅ Production — 703B/704B/OBS 1600 |
| 26 | Pricing | `/pricing` | `pricing.html` | ✅ Production — pricing intelligence |
| 27 | Shipping | `/shipping` | `shipping.html` | ✅ Production — shipping dashboard |
| 28 | CCHCS Expand | `/cchcs/expansion` | `expand.html` | ✅ Production — facility expansion |
| 29 | Campaigns | `/campaigns` | `voice_campaigns.html` | ✅ Production — voice/SMS campaigns |
| 30 | Audit | `/audit` | `audit.html` | ✅ Production — audit trail |
| 31 | Debug | `/debug` | `debug.html` | ⚠️ Dev only — should be gated |

---

## 5. Growth Engine Feature Matrix

### Implemented (V1–V4)

| Feature | Version | Status | Functions |
|---------|---------|--------|-----------|
| SCPRS History Pull | V1 | ✅ | `pull_reytech_history` |
| Category Buyer Search | V1 | ✅ | `find_category_buyers` |
| Credentials Engine | V1 | ✅ | `get_reytech_credentials` |
| 6 Email Templates | V1 | ✅ | `build_outreach_email`, `EMAIL_TEMPLATES` |
| Follow-Up Cohorts | V1 | ✅ | `get_follow_up_cohorts` |
| Weighted Scoring | V1 | ✅ | `score_prospect_weighted` |
| Win Probability | V2 | ✅ | `get_win_probability` |
| Competitor Intel | V2 | ✅ | `get_competitor_intel` |
| Lost PO Analysis | V2 | ✅ | `get_lost_po_analysis` |
| A/B Testing | V2 | ✅ | `get_ab_stats` |
| KPI Dashboard | V2 | ✅ | `get_growth_kpis` |
| CSV/Excel/PDF Export | V2 | ✅ | 3 export endpoints |
| Workflow Automation | V3 | ✅ | `get_workflows`, `get_workflow_queue` |
| SMS Templates | V3 | ✅ | `SMS_TEMPLATES` |
| Calendar Events | V3 | ✅ | `get_calendar_events`, `get_todays_agenda` |
| Notifications | V3 | ✅ | `get_notifications` |
| Bulk Operations | V3 | ✅ | Bulk status, tag, delete APIs |
| Kanban Board | V3+ | ✅ | `get_kanban_board` |
| Quick Wins | V3+ | ✅ | `get_quick_wins` |
| Daily Brief | V3+ | ✅ | `generate_daily_brief` |
| Agency Intelligence | V3+ | ✅ | `get_agency_intelligence` |
| Campaign Performance | V3+ | ✅ | `get_campaign_performance` |
| Outreach Funnel | V4 | ✅ | `get_outreach_funnel` |
| Data Tools | V4 | ✅ | Auto-tag, dedup, import, price compare |
| **3-Phase Buyer Intel** | **V4** | **✅** | `run_buyer_intelligence` (new) |

### Growth Tabs (12)

Outreach, Follow-Ups, Workflows, Pipeline, Kanban, Quick Wins, Competitors, Lost POs, Calendar, SCPRS, Audit, Tools

---

## 6. Agent Inventory (41 modules)

| Agent | Lines | Purpose | External Dependencies |
|-------|-------|---------|----------------------|
| `growth_agent.py` | 4,179 | Growth intel, SCPRS scraping, outreach | SCPRS (web) |
| `email_poller.py` | 2,526 | Dual Gmail IMAP polling | Gmail IMAP/SMTP |
| `product_catalog.py` | 2,711 | Product catalog CRUD | SQLite |
| `qa_agent.py` | 2,490 | Quality assurance engine | SQLite |
| `scprs_lookup.py` | 683 | FI$Cal SCPRS price scraper | SCPRS (web) |
| `voice_agent.py` | ~900 | Twilio/VAPI voice calls | Twilio, VAPI |
| `sales_intel.py` | 1,164 | Sales intelligence | Internal |
| `email_outreach.py` | ~500 | Growth email sending | Gmail SMTP |
| `item_identifier.py` | ~400 | AI item identification | Anthropic API |
| `product_research.py` | ~600 | Web price research | SerpAPI |
| `tax_agent.py` | ~300 | CDTFA tax rates | CDTFA (web) |
| `notify_agent.py` | ~400 | SMS/email alerts | Twilio, Gmail |
| `quickbooks_agent.py` | ~900 | QuickBooks integration | QB API |

---

## 7. Data Flow Audit

### Primary Data Stores

| Store | Type | Size | Purpose |
|-------|------|------|---------|
| `reytech.db` | SQLite | 1.6 MB | RFQs, products, prices, line items |
| `qa_intelligence.db` | SQLite | 1.2 MB | QA reports, email analysis |
| `quotes_log.json` | JSON | 32 KB | All quotes (37 entries) |
| `crm_activity.json` | JSON | 719 KB | CRM activity log |
| `crm_contacts.json` | JSON | 12 KB | Customer contacts |
| `customers.json` | JSON | 37 KB | Customer records |
| `growth_prospects.json` | JSON | 3 KB | Growth prospects (5 active) |
| `growth_outreach.json` | JSON | 5 KB | Outreach campaign data |
| `vendors.json` | JSON | 38 KB | Vendor directory |
| `product_catalog_import.csv` | CSV | 268 KB | Catalog import data |

### Background Threads

| Thread | Interval | Purpose |
|--------|----------|---------|
| Email Poller 1 | 2 min | `rfq@reytechinc.com` inbox scan |
| Email Poller 2 | 2 min | `mike@reytechinc.com` inbox scan |
| Order Digest | Daily | Pending order summary email |
| SCPRS Pull | Configurable | Price database refresh |
| Growth Intel | On-demand | 3-phase buyer intelligence scrape |

---

## 8. Recent Sprint Summary (March 2026)

50 commits delivering:

| Feature | Commit | Impact |
|---------|--------|--------|
| 3-Phase Buyer Intelligence | `793bd45` | SCPRS scraper for same-agency, cross-sell, medical opportunities |
| Growth Page 500 Fix | `edb52ef` | Defensive error handling, type mismatch fixes, diagnostic page |
| Activity Feed + Follow-Ups | `c499b9a` | Real-time activity widget, enhanced follow-up redesign |
| Ctrl+K Command Palette | `1788a41` | Global keyboard navigation across all pages |
| Manager Brief | `febca46` | Pipeline stats, approval queue, agent status on home |
| System Health Dashboard | `271ceb8` | Settings page with storage, health checks |
| Pipeline Enhancement | `243ce2f` | 6 KPIs, stale quotes, velocity, revenue goal bar |
| CRM Overhaul | `76d6f59` | 7 KPIs, bulk actions, sortable columns |
| Growth V3+ Features | `e2bebd1` | Kanban, quick wins, daily brief, agency intel |
| Growth V2+V3 | `4c28301` | 15 features: analytics, automation, exports, calendar |
| CRM Production | `2220161` | 12-feature contact management system |
| PC Status Simplification | `85ae0a4` | 4 statuses: New, Draft, Sent, Not Responding |
| Global Font Floor | `6b0c16a` | 13px minimum across 57 files, 1,364 replacements |
| Orders 10-Feature Sprint | `0a981ab` | Timeline, margins, filters, aging, emails, audit |
| Order Lifecycle | `3339525` | Line-item notifications, daily digest, tracking |

---

# PART 2: PRODUCTION HARDENING (Priority Order)

## P1 — SQL Injection Remediation [CRITICAL]

**Effort:** 2–3 hours | **Risk if skipped:** Data breach via authenticated user

Scope: 31 f-string SQL queries across 16 files. Convert all to parameterized queries.

**Approach:**
1. `grep -rn 'execute(f"' src/ --include="*.py"` to find all instances
2. For each: replace `f"...WHERE x = '{val}'"` with `"...WHERE x = ?", (val,)`
3. Test each query still returns correct results
4. Priority order: `db.py` (3) → `product_catalog.py` (7) → `scprs_universal_pull.py` (4) → rest

## P2 — Auth on Admin Routes [WARNING]

**Effort:** 10 minutes | **Risk if skipped:** Unauthorized cleanup/trace access

Add `@auth_required` to `/api/email-trace` and `/api/disk-cleanup` in `dashboard.py`.

## P3 — Bare Except Cleanup [INFO]

**Effort:** 1 hour | **Risk if skipped:** Silent error swallowing

Replace 74 bare `except:` with specific exception types (`except Exception as e:` at minimum, `except (KeyError, ValueError):` where applicable). Priority: `routes_features.py` (27), `routes_features3.py` (26), `routes_features2.py` (15).

---

# PART 3: NEXT 10 FEATURES

Adhering to CLAUDE.md principles: simplicity first, minimal impact, senior developer standards.

## F1 — Smart Notification Center

**What:** Replace the scattered notification patterns with a unified notification panel in the header bar. Bell icon with unread count. Notifications from: new RFQs arriving, quotes won/lost, orders shipped, follow-ups due, growth prospects responding.

**Why:** Currently notifications go to email/SMS only. No in-app awareness of events while working in the dashboard.

**Scope:**
- New: `src/agents/notification_hub.py` — central notification store (SQLite table)
- New: `/api/notifications` endpoints (list, mark-read, dismiss)
- Edit: `base.html` — bell icon in header with dropdown
- Edit: Existing agents — emit notifications to hub instead of only email/SMS

**Effort:** Medium (3–4 hours)

## F2 — Quote-to-Order Conversion

**What:** One-click conversion of won quotes to orders. Pre-fills order form with quote line items, agency, contact. Tracks which quotes generated orders.

**Why:** Currently manual. Quote wins require re-entering data to create orders.

**Scope:**
- Edit: `quote_detail.html` — "Convert to Order" button
- New: `/api/quote/<qn>/convert-to-order` endpoint
- Edit: `orders.json` creation logic to accept quote data

**Effort:** Small (1–2 hours)

## F3 — Automated Price Check Follow-Up

**What:** When a price check has been in "Sent" status for 3+ days with no response, automatically queue a follow-up email draft. Configurable delay (3/5/7 days).

**Why:** Price checks often stall after sending. Manual follow-up tracking is error-prone.

**Scope:**
- New: Background thread in `follow_up_engine.py` — scan PCs by sent_at date
- New: `/api/pricecheck/<id>/auto-followup` endpoint
- Edit: `follow_ups.html` — show PC follow-ups alongside quote follow-ups
- Edit: Settings page — configurable follow-up delay

**Effort:** Medium (2–3 hours)

## F4 — Supplier Performance Dashboard

**What:** Track supplier performance: response time to quote requests, price competitiveness, delivery reliability, defect rate. Aggregate across all quotes and orders.

**Why:** Reytech works with multiple suppliers. No current way to compare which suppliers are fastest, cheapest, or most reliable.

**Scope:**
- New: `src/agents/supplier_performance.py` — aggregation engine
- New: `/suppliers/performance` page with rankings
- Edit: `vendors.html` — link to performance data
- Data source: quotes_log (response times), orders (delivery), price_checks (pricing)

**Effort:** Medium (3–4 hours)

## F5 — Batch Email Composer

**What:** Select multiple prospects/contacts from CRM or Growth, compose one email template, preview personalization for each recipient, send all at once via Gmail.

**Why:** Current email sending is one-at-a-time. Growth outreach to 50+ prospects needs batch capability.

**Scope:**
- New: `/api/email/batch-compose` endpoint
- New: Batch compose modal in CRM and Growth pages
- Edit: `email_outreach.py` — rate-limited batch sending (1 per 3 seconds)
- Edit: Activity feed — log batch sends

**Effort:** Medium (3–4 hours)

## F6 — Mobile-Responsive Layout

**What:** Make the dashboard usable on phone screens. Currently desktop-only with tiny text and horizontal scroll on mobile.

**Why:** Mike checks the dashboard from his phone frequently. Current layout is unusable below 768px.

**Scope:**
- Edit: `base.html` — responsive meta tag, mobile nav hamburger (partially done)
- Edit: `styles.css` — media queries for key pages (home, PCs, orders, growth)
- Edit: Key templates — stack grids vertically on mobile, larger touch targets
- Priority pages: Home, Price Checks, Orders, Growth

**Effort:** Large (4–6 hours)

## F7 — SCPRS Auto-Refresh Schedule

**What:** Automated daily/weekly SCPRS pull on a configurable schedule. Currently requires manual "Pull History" button click.

**Why:** Stale SCPRS data means missed pricing intel and outdated buyer information.

**Scope:**
- New: Background scheduler thread (similar to email poller)
- Edit: Settings page — SCPRS schedule config (daily/weekly/off, time of day)
- Edit: `growth_agent.py` — schedule-triggered pull vs manual pull
- New: `/api/growth/schedule` endpoint to configure

**Effort:** Small (1–2 hours)

## F8 — Document Version History

**What:** Track all generated documents (quotes, bid packages, forms) with version history. View previous versions, compare changes, restore old versions.

**Why:** Regenerating a quote overwrites the previous PDF. No way to see what was sent originally vs. revised.

**Scope:**
- New: `src/core/doc_versions.py` — version storage (SQLite table: doc_id, version, path, timestamp)
- Edit: Quote/RFQ generation — save version before overwriting
- New: Version history panel on quote/RFQ detail pages
- Edit: `/dl/` download route — support version parameter

**Effort:** Medium (2–3 hours)

## F9 — Win/Loss Analysis Dashboard

**What:** Dedicated analytics page showing why quotes are won or lost. By agency, by product category, by price point, by competitor. Trends over time.

**Why:** Growth Engine has `get_lost_po_analysis()` but no dedicated page for deep analysis. Understanding win/loss patterns drives better pricing.

**Scope:**
- New: `/analytics/win-loss` page
- New: `win_loss.html` template with charts (pie: reasons, bar: by agency, line: trend)
- Data source: `quotes_log.json` status field + `lost_analysis` from growth agent
- Edit: Analytics nav — add win/loss link

**Effort:** Medium (2–3 hours)

## F10 — Webhook Integration Hub

**What:** Configurable outgoing webhooks for key events: new RFQ received, quote sent, order status change, payment received. POST JSON to user-configured URLs (Slack, Zapier, custom).

**Why:** Enables integration with external tools without custom code. Slack notifications for new RFQs is a frequent request.

**Scope:**
- New: `src/core/webhooks.py` — event dispatcher with retry logic
- New: Settings page section — webhook URL config per event type
- Edit: Key event points — emit webhook after RFQ creation, quote send, order update
- New: `/api/webhooks/test` endpoint to verify connectivity

**Effort:** Medium (2–3 hours)

---

# PART 4: TECHNICAL DEBT INVENTORY

| Item | Severity | Files | Effort |
|------|----------|-------|--------|
| f-string SQL injection | Critical | 16 | 2–3h |
| `exec()` module loading | High | `dashboard.py` | 4–6h (Blueprint refactor) |
| Unprotected admin routes | Medium | `dashboard.py` | 10 min |
| Bare except clauses | Low | 6 files | 1h |
| `routes_intel.py` at 6,281 lines | Low | 1 file | 2h (split into 3) |
| Inline JS in templates (~6,805 lines) | Low | 46 files | 8h+ (extract to .js) |
| No automated test suite | Medium | — | 8h+ (pytest setup) |
| No database migrations | Medium | — | 2h (Alembic setup) |
| Duplicate CRM data (contacts + customers) | Low | 2 JSON files | 1h (merge) |

---

# PART 5: METRICS SUMMARY

| Metric | Value |
|--------|-------|
| Total source lines | 101,613 |
| Python files | 105 |
| HTML templates | 46 |
| Total routes | 695 |
| Page routes | 86 |
| API routes | 609 |
| Agent modules | 41 |
| Growth agent functions | 104 |
| Environment variables | 80 |
| Data files | 40 |
| SQLite databases | 2 |
| Background threads | 5 |
| Commits | 826 |
| Auth-protected routes | 682 (98.1%) |
| Compilation errors | 0 |
| Template parse errors | 0 |
