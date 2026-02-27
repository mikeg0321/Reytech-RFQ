# PRD-v31 — Full System Audit + Next 12 Features
**Date:** 2026-02-26 · **Auditor:** Claude · **Codebase:** Reytech-RFQ  
**Stats:** ~86,000 lines Python · 598 routes · 45 templates · 15 route modules · 38 agents/modules · 12+ background threads

---

# PART 1: FULL SYSTEM AUDIT

## 1. Architecture Overview

**Stack:** Python 3 / Flask / SQLite (WAL mode) / Jinja2 / Gunicorn  
**Deploy:** Railway (auto-deploy from GitHub `main`) with persistent volume at `/data`  
**Domain:** `web-production-dcee9.up.railway.app` → `bid.reytechinc.com`

### Module Map
```
app.py                    → Flask factory, startup, polling init
src/api/dashboard.py      → Main Blueprint (3,244 lines), 598 routes via exec()
src/api/modules/          → 15 route files loaded via exec() into dashboard namespace
src/agents/               → 38 agent modules (email, SCPRS, growth, QA, voice, etc.)
src/core/db.py            → SQLite DAL (2,366 lines, 14 tables)
src/core/security.py      → Rate limiting, CSRF, security headers
src/core/paths.py         → Centralized path resolution (Railway volume detection)
src/forms/                → PDF fillers (703B, 704B, Bid Pkg, OBS 1600), quote gen, price check
src/knowledge/            → Pricing oracle, won quotes DB
src/templates/            → 45 Jinja2 HTML templates
```

---

## 2. CRITICAL ISSUES (Fix Immediately)

### 🔴 C1 — Hardcoded Secret Key Fallback
**File:** `app.py:35`  
`app.secret_key = os.environ.get("SECRET_KEY", "reytech-rfq-2026")`  
If `SECRET_KEY` env var is unset, Flask sessions are signed with a public, guessable key. Anyone can forge session cookies.  
**Fix:** Remove the fallback. Crash on startup if SECRET_KEY is missing.

### 🔴 C2 — `exec()` Module Loading Pattern
**File:** `src/api/dashboard.py:2802`  
All 15 route modules are loaded via `exec(compile(_src, module_path, "exec"), globals())`. This:
- Merges all module globals into one namespace (name collision risk)
- Makes static analysis, IDE support, and debugging impossible
- Creates import-time NameError bugs (documented: `_is_price_check` crashes)
- Bypasses Python's module system entirely

**Fix:** Convert to proper Flask Blueprint registration. Each module gets its own Blueprint, registered on the main app.

### 🔴 C3 — No Auth on ~550+ Routes
**File:** Multiple route modules  
Only ~5 routes in `dashboard.py` use `@auth_required`. All 15 route modules loaded via `exec()` have **zero auth decorators**. Every `/api/*` endpoint is publicly accessible.  
The `before_request` handler only logs — it does NOT check auth.  
**Fix:** Move auth check to `before_request` as a global guard, with explicit exemptions for health/static.

### 🔴 C4 — SQL Injection Vectors
**Files:** `src/agents/quickbooks_agent.py:407,465,885`  
String-interpolated SQL: `f"SELECT * FROM PurchaseOrder WHERE MetaData.CreateTime >= '{since}'"` — attacker-controlled `since` values can inject SQL.  
Also in `notify_agent.py`, `vendor_ordering_agent.py` with f-string WHERE clauses.  
**Fix:** Use parameterized queries exclusively. grep and replace all f-string SQL.

### 🔴 C5 — GitHub PAT Tokens in Commit History
Multiple PATs have been shared in chat and likely committed in data files. If any `.json` or config files contain tokens, they're in git history forever.  
**Fix:** Run `git log --all -p | grep github_pat` to check. Use `git filter-branch` or BFG to purge. Rotate ALL tokens.

---

## 3. HIGH-PRIORITY ISSUES

### 🟠 H1 — Dual-Write Data Inconsistency (JSON + SQLite)
**Scope:** Quotes, contacts, orders, RFQs, price checks  
The system writes to BOTH JSON files and SQLite. Some reads come from JSON, others from SQLite. When one write fails silently, data drifts. The JSON files in the repo (`data/price_checks.json: {} empty`, `data/rfqs.json: {} empty`) are stale seeds while real data lives on the Railway volume's `reytech.db`.  
**Fix:** Single source of truth. Migrate all reads/writes to SQLite via `db.py`. Remove JSON file I/O. JSON exports become read-only API endpoints.

### 🟠 H2 — Duplicate Code: Root Files vs src/ Modules
10 Python files exist at BOTH the project root AND inside `src/`:
```
dashboard.py   (root: 3747L)  vs  src/api/dashboard.py   (3244L)
email_poller.py (root: 465L)  vs  src/agents/email_poller.py (2453L)
price_check.py  (root: 919L)  vs  src/forms/price_check.py   (1253L)
quote_generator.py (root: 1050L) vs src/forms/quote_generator.py (1425L)
+ 6 more
```
The try/except import chain attempts root first, then src. Different versions = different behavior depending on import order.  
**Fix:** Delete ALL root-level duplicates. Ensure all imports reference `src.*` exclusively.

### 🟠 H3 — SQLite Connection Management: No Pooling, Inconsistent Patterns
`src/core/db.py` provides a `get_db()` context manager with WAL mode and thread locking. But **84+ direct `sqlite3.connect()` calls** across agents bypass it — no WAL, no timeout, no `finally: conn.close()` in many cases.  
**Fix:** All DB access must go through `get_db()`. Grep and replace direct connects.

### 🟠 H4 — 12+ Background Daemon Threads With No Coordination
Award monitor, follow-up engine, quote lifecycle, email retry, lead nurture, QA monitor, SCPRS scanner, workflow monitor, growth scheduler, stale watcher, vendor ordering — all launch `threading.Thread(daemon=True)` at import time with no:
- Health monitoring or restart logic
- Graceful shutdown
- Error propagation
- Coordination between threads  
If one thread crashes, it dies silently. If two threads write to the same JSON file, data corrupts.  
**Fix:** Centralized scheduler (e.g., APScheduler) with health checks, error logging, and thread registry.

### 🟠 H5 — CSRF Protection Not Applied
`security.py` has CSRF infrastructure but it's opt-in via decorator. Zero routes use `@csrf_protect`. All POST endpoints accept requests from any origin.  
**Fix:** Apply CSRF validation globally in `before_request` for all POST/PUT/DELETE, exempt API-key auth and health endpoints.

---

## 4. MEDIUM ISSUES

### 🟡 M1 — No Input Validation Framework
`_sanitize_input()` exists in dashboard.py but is called inconsistently. No schema validation (Pydantic/Marshmallow) on any API endpoint. Request JSON bodies are consumed directly with `.get()` defaults.

### 🟡 M2 — Error Handling: Silent Failures
7 bare `except: pass` blocks in dashboard.py. Agents use `try/except Exception as e: log.error(...)` but continue processing — failed price lookups, email sends, and DB writes are logged but never surfaced to the user.

### 🟡 M3 — No Database Migrations
Schema changes happen via `CREATE TABLE IF NOT EXISTS` + manual `ALTER TABLE` attempts in `db.py`. No migration tracking = risky schema evolution. Adding a column after production data exists requires careful ALTER TABLE that's not version-controlled.

### 🟡 M4 — Test Coverage Gaps
Tests exist (14 files, ~150KB) but many test against mocked data, not the actual running system. No integration tests for the email→PC→quote pipeline. No tests for the exec()-loaded route modules (they can't be imported normally).

### 🟡 M5 — Logging: Structured but Unsearchable
Good structured logging via `logging_config.py`, but logs go to stdout. No log aggregation, no alerting on errors, no request tracing across background thread boundaries.

### 🟡 M6 — PDF Generation: No Template Versioning
PDF templates in `data/templates/` are binary blobs. If a state agency updates their form, there's no diffing, versioning, or detection of template staleness.

### 🟡 M7 — Email Pipeline: No Idempotency for Re-processes
If `processed_emails.json` is lost (Railway restart), all emails from the last 3 days get re-imported. This creates duplicate PCs/RFQs. The UID tracking needs to be in SQLite on the persistent volume.

---

## 5. DATA SOURCE AUDIT

| Source | Module | Status | Issue |
|--------|--------|--------|-------|
| **Gmail IMAP** | `email_poller.py` | ✅ Working | UID tracking in JSON (should be SQLite) |
| **SCPRS (CaleProcure)** | `scprs_lookup.py`, `scprs_scanner.py`, `scprs_universal_pull.py` | ✅ Working | 3 separate SCPRS modules with overlapping functionality |
| **CDTFA Tax API** | `tax_agent.py` | ✅ Working | PO Box workaround fragile |
| **Amazon/Google Shopping** | `product_research.py` | ⚠️ Partial | Requires SerpApi key, no fallback |
| **QuickBooks** | `quickbooks_agent.py` | ⚠️ Partial | Token refresh implemented, SQL injection in queries |
| **Product Catalog CSV** | `product_catalog.py` | ✅ Working | 842 products, 2109-line CSV import |
| **Won Quotes DB** | `won_quotes_db.py` | ✅ Working | Duplicate module at root + src/ |
| **CRM Contacts** | `db.py contacts table` | ✅ Working | 18 contacts in JSON, unclear if SQLite synced |
| **Twilio Voice** | `voice_agent.py` | ⚠️ Scaffolded | Routes exist, unclear if Twilio connected |
| **Google Drive** | Not found | ❌ Missing | Referenced in past PRDs, never built |

---

## 6. ROUTE TRACE ANALYSIS

### Email → Price Check → Quote Flow
```
1. Gmail IMAP poll (5-min interval, email_poller.py)
   ├── classify: is_price_check_email() | is_rfq_email() | is_purchase_order_email() | is_reply_followup()
   ├── if PC: parse_ams704() → _merge_save_pc() → _auto_price_new_pc()
   │   ├── SCPRS lookup (bulk_lookup)
   │   ├── Catalog match (match_items_batch)
   │   ├── Amazon research (if enabled)
   │   └── Save to JSON + SQLite (dual write)
   ├── if RFQ: parse_rfq_attachments() → save_rfqs() → _trigger_auto_price()
   └── if PO: _extract_po_data() → _create_order_from_po_email()

2. User reviews PC at /pricecheck/{id}
   ├── GET loads from _load_price_checks() (JSON) 
   ├── User adjusts prices → POST save → _save_price_checks() (JSON + SQLite)
   └── "Generate Quote" → generate_quote_from_pc() → PDF created → status=sent

3. Quote delivery
   ├── generate_quote() → fills PDF template → saves to DATA_DIR
   ├── EmailSender sends via Gmail SMTP
   └── Quote status updated in quotes_log.json + SQLite quotes table
```

### Critical Gap: JSON reads, SQLite writes
The `_load_price_checks()` reads from JSON. The `_save_price_checks()` writes to BOTH JSON and SQLite. But if a write to JSON succeeds and SQLite fails (or vice versa), the data sources diverge silently.

---

## 7. DB PERSISTENCE AUDIT

### SQLite Tables (from db.py SCHEMA)
| Table | Purpose | Rows (repo seed) | Persistent? |
|-------|---------|-------------------|-------------|
| quotes | All generated quotes | 37 in JSON | ✅ Volume |
| price_history | Every price found | Unknown | ✅ Volume |
| contacts | CRM contacts | 18 in JSON | ✅ Volume |
| activity_log | CRM interactions | 1101 in JSON | ✅ Volume |
| orders | Won quotes → POs | 2 in JSON | ✅ Volume |
| rfqs | Inbound email RFQs | 0 in JSON | ✅ Volume |
| price_checks | Parsed PCs | 0 in JSON | ✅ Volume |
| leads | Growth/prospecting | Unknown | ✅ Volume |
| email_sent_log | Outbound email log | Unknown | ✅ Volume |
| workflow_runs | Automation runs | Unknown | ✅ Volume |
| sent_documents | Document versions | Unknown | ✅ Volume |
| product_catalog | 842 products | CSV seed | ✅ Volume |
| audit_trail | Security events | Unknown | ✅ Volume |

### Key Risk:
Railway volume is a **single point of failure**. No backup, no replication. If the volume is deleted or corrupted, ALL production data is lost.

---

# PART 2: PRD-v31 — 12 FEATURES

Prioritized by: (1) data integrity/security first, (2) workflow impact, (3) revenue enablement.

---

## Feature 1 — Global Auth Guard + Session Management
**Priority:** P0 · **Effort:** 4h · **Impact:** Closes C3

Move auth from per-route `@auth_required` decorator to `bp.before_request`. Every request must authenticate EXCEPT an explicit allowlist (`/health`, `/static/*`, `/login`).

**Spec:**
- `before_request` checks `request.authorization` (Basic Auth) or session token
- Failed auth → 401 JSON or redirect to `/login`
- Add session timeout (configurable, default 8h)
- Add "remember me" option using signed cookie
- Log all auth failures to `audit_trail`

**Acceptance:** Zero routes accessible without auth. Pen-test confirms.

---

## Feature 2 — Blueprint Refactor: Kill exec()
**Priority:** P0 · **Effort:** 8h · **Impact:** Closes C2

Convert all 15 `exec()`-loaded route modules to proper Flask Blueprints.

**Spec:**
- Each `routes_*.py` becomes a standalone Blueprint
- `dashboard.py` registers them via `app.register_blueprint()`
- Shared utilities (auth, sanitize, data loaders) move to `src/core/utils.py`
- Delete ALL root-level duplicate `.py` files
- All imports become explicit `from src.x import y`

**Acceptance:** `grep -r "exec(" src/` returns zero results. All routes still respond.

---

## Feature 3 — Single Source of Truth: SQLite-Only Data Layer
**Priority:** P0 · **Effort:** 12h · **Impact:** Closes H1

Eliminate all JSON file I/O for stateful data. SQLite becomes the only persistence layer.

**Spec:**
- `_load_price_checks()` → `db.get_all_price_checks()`
- `_save_price_checks()` → `db.upsert_price_check()`
- `load_rfqs()` / `save_rfqs()` → `db.get_rfqs()` / `db.upsert_rfq()`
- Same for quotes, orders, contacts, activity
- Remove all `json.dump()` calls for stateful data
- Add `/api/export/{table}` endpoints for JSON export (read-only)
- Processed email UIDs tracked in `processed_emails` SQLite table (not JSON)

**Acceptance:** `grep -r "json.dump" src/api/` returns zero results for stateful data. Restart does not lose data.

---

## Feature 4 — Centralized Scheduler + Thread Health
**Priority:** P1 · **Effort:** 6h · **Impact:** Closes H4

Replace 12+ ad-hoc daemon threads with a single APScheduler instance.

**Spec:**
- Install `apscheduler` (add to requirements.txt)
- Create `src/core/scheduler.py` — single scheduler instance
- Register all jobs: email poll (5m), award monitor (1h), follow-up scan (1h), quote lifecycle (1h), email retry (15m), lead nurture (daily), QA monitor (15m), SCPRS scanner (schedule)
- Health endpoint: `GET /api/scheduler/status` → lists all jobs, last run time, next run, error count
- Dead job detection: if a job hasn't run in 3x its interval, log CRITICAL and attempt restart
- Graceful shutdown on SIGTERM

**Acceptance:** `GET /api/scheduler/status` returns all 8+ jobs with accurate timing. No orphan threads.

---

## Feature 5 — Automated Database Backups
**Priority:** P1 · **Effort:** 4h · **Impact:** Mitigates volume SPOF

Daily SQLite backup to a durable location.

**Spec:**
- Scheduler job: daily at 2am PST
- `sqlite3 .backup` command to create snapshot
- Rotate: keep last 7 daily + 4 weekly
- Backup stored to: (a) Railway volume `/data/backups/` + (b) email attachment to admin
- `GET /api/admin/backups` → list available backups with sizes
- `POST /api/admin/restore/{filename}` → restore from backup (with confirmation)
- Health alert if latest backup is >36h old

**Acceptance:** 7 daily backup files exist. Restore from 3-day-old backup succeeds with data intact.

---

## Feature 6 — Smart Email Classification v2
**Priority:** P1 · **Effort:** 8h · **Impact:** Closes the "reply vs new PC" pipeline pollution

Replace rule-based email classification with a scoring system.

**Spec:**
- Each email gets scored across 5 dimensions:
  1. **New PC signals:** AMS 704 attachment, "Quote request" subject, new sender
  2. **New RFQ signals:** 703B/704B/Bid Pkg attachments, PR number in subject
  3. **Reply/Follow-up signals:** "RE:" prefix, references active PC, same thread
  4. **PO signals:** PO number, "Purchase Order" subject, fiscal year ref
  5. **CS/Inquiry signals:** question marks, "clarification", no attachments
- Classification: highest-scoring category wins, with confidence score
- If confidence < 0.6, route to "Manual Review" queue with all scores shown
- CS follow-ups get linked to their parent PC/RFQ as a conversation thread
- New table: `email_classifications` with all scores for audit
- Dashboard widget: "Needs Review" count badge on nav

**Acceptance:** Re-classify last 20 emails. Zero mis-routes. CS replies no longer create phantom PCs.

---

## Feature 7 — Margin Optimizer: Smart Pricing Dashboard
**Priority:** P1 · **Effort:** 10h · **Impact:** Direct revenue improvement

Dedicated pricing intelligence page that consolidates all pricing data.

**Spec:**
- Page: `/margins` (already exists as template, needs full build-out)
- Per-item view showing:
  - Current cost (SCPRS / Amazon / Catalog / Manual)
  - Historical win/loss prices (from won_quotes_db)
  - Competitor prices (from SCPRS awards)
  - Recommended sell price (weighted algorithm)
  - Margin % and $ at recommended vs current
- Category roll-up: average margin by product category
- "Margin alert" — items priced below 15% margin highlighted red
- "Should have won" detector: lost quotes where price was within 5% of winner
- Export to CSV for QuickBooks repricing
- Real-time: updates when new won/lost data comes in

**API:**
- `GET /api/margins/summary` — category-level margin stats
- `GET /api/margins/items?category=X` — item-level detail
- `GET /api/margins/should-have-won` — near-miss analysis
- `POST /api/margins/bulk-reprice` — apply recommended prices to catalog

**Acceptance:** Every product in catalog shows margin data. Should-have-won list populated from historical quotes.

---

## Feature 8 — Order Lifecycle + Revenue Tracking
**Priority:** P2 · **Effort:** 8h · **Impact:** Closes the quote-to-cash loop

End-to-end order tracking from PO receipt through delivery and payment.

**Spec:**
- Order statuses: `received` → `processing` → `ordered_from_vendor` → `shipped` → `delivered` → `invoiced` → `paid`
- Each status transition logged with timestamp and actor
- PO detail page (`/orders/{id}`) shows:
  - Original quote, line items, buyer contact
  - Vendor order status (if placed)
  - Shipping tracking (manual entry or auto-detect from email)
  - Invoice # and payment status
- Revenue dashboard integration: `orders` table feeds annual revenue goal
- Auto-detect PO emails and link to existing quotes (by solicitation #, buyer name, or amount match)
- Follow-up automation: if invoice unpaid after 30 days, draft follow-up

**Acceptance:** Order moves through all 7 statuses. Revenue dashboard shows accurate YTD from completed orders.

---

## Feature 9 — Growth Agent: SCPRS Historical Pull + Outreach
**Priority:** P2 · **Effort:** 10h · **Impact:** New business generation

Automate the full growth workflow described in past conversations.

**Spec:**
Phase A — Data Pull:
- Pull all Reytech POs from SCPRS (2022–present)
- Extract: items, prices, quantities, buyer names, agencies, dates
- Categorize items into product groups
- Store in `scprs_reytech_history` table

Phase B — Market Intelligence:
- For each product category, pull ALL state buyers who purchased similar items
- Extract buyer name, email, phone, agency, purchase dates, quantities
- Deduplicate against existing CRM contacts
- Score prospects: recent purchaser + high volume + no Reytech relationship = hot lead

Phase C — Outreach:
- Template-based email campaigns (already partially built in `growth_agent.py`)
- Personalized: "Hi {name}, we noticed {agency} purchased {items} on {date}. Reytech offers competitive pricing on these products..."
- Track: sent, opened, replied, converted
- Drip sequence: initial → 7d follow-up → 14d value-add → 30d final touch

**Acceptance:** 50+ qualified prospects identified from SCPRS data. 10+ outreach emails sent with tracking.

---

## Feature 10 — Unified Search: Full-Text Across All Entities
**Priority:** P2 · **Effort:** 6h · **Impact:** Daily workflow speed

The search bar on the home page currently doesn't hit descriptions or item numbers. Fix and expand.

**Spec:**
- SQLite FTS5 virtual table spanning: quotes, price_checks, rfqs, orders, contacts, catalog
- Search hits: quote numbers, solicitation numbers, item descriptions, part numbers, buyer names, agency names, email addresses
- Results grouped by entity type with relevance scoring
- Quote numbers in results are clickable links to detail pages
- Institution names resolved from shipping address (never show "Unknown")
- Auto-status based on dependencies (e.g., PC with all items priced + generated quote = "sent")
- Search-as-you-type with 300ms debounce
- Keyboard shortcut: `/` to focus search

**Acceptance:** Search "gauze" returns all PCs, RFQs, quotes, and catalog items mentioning gauze. Results link to correct pages.

---

## Feature 11 — Google Drive Integration for Document Archive
**Priority:** P2 · **Effort:** 8h · **Impact:** Document durability

Archive all generated PDFs and received documents to Google Drive.

**Spec:**
- OAuth2 service account for Google Drive API
- Folder structure: `Reytech RFQ / {Year} / {Month} / {Quote#|PC#|RFQ#}`
- Auto-upload on: quote generated, PC received, PO matched, bid package created
- Metadata: quote #, agency, institution, date, total, status
- Shareable links stored in DB for each document
- Admin page: `/settings/drive` — connect, test, view sync status
- Fallback: if Drive upload fails, file stays on Railway volume (already does)

**Acceptance:** Generated quote PDF appears in Google Drive within 60s. Link in quote detail page opens Drive file.

---

## Feature 12 — Settings Dashboard + System Health
**Priority:** P2 · **Effort:** 6h · **Impact:** Operability

Centralized configuration and monitoring page.

**Spec:**
- Page: `/settings` with tabs:
  - **Email:** IMAP/SMTP config, test connection, poll interval
  - **SCPRS:** Credentials, schedule config, last pull stats
  - **QuickBooks:** OAuth status, sync schedule, last sync
  - **Google Drive:** Connection status, sync stats
  - **Pricing:** Default margin %, category overrides, CDTFA config
  - **Notifications:** Email alerts for errors, daily digest toggle
- System Health panel:
  - DB size, table row counts
  - Background job status (from Feature 4 scheduler)
  - Last email poll, last SCPRS pull, last QB sync
  - Disk usage on Railway volume
  - Error count in last 24h
- All settings stored in `system_config` SQLite table (not env vars)
- Changes take effect without redeploy

**Acceptance:** Settings page loads in <2s. Changing poll interval takes effect on next cycle. Health panel shows all green for healthy system.

---

# PART 3: IMPLEMENTATION PRIORITY

| Phase | Features | Effort | Gate |
|-------|----------|--------|------|
| **Sprint 0: Security** | F1 (Auth), C1 (Secret), C4 (SQLi) | 8h | No new features until auth is global |
| **Sprint 1: Foundation** | F2 (Blueprint), F3 (SQLite-only), H2 (dedup) | 24h | Clean architecture before adding features |
| **Sprint 2: Stability** | F4 (Scheduler), F5 (Backups), M7 (Email idempotency) | 14h | Production-grade reliability |
| **Sprint 3: Intelligence** | F6 (Email v2), F7 (Margins), F10 (Search) | 24h | Core workflow improvements |
| **Sprint 4: Growth** | F8 (Orders), F9 (Growth Agent), F11 (Drive) | 26h | Revenue + new business |
| **Sprint 5: Operations** | F12 (Settings), M3 (Migrations), M5 (Logging) | 14h | Self-managing system |

**Total estimated effort:** ~110 hours across 5 sprints

---

# PART 4: QUICK WINS (Can Do Now)

These are 1-2 hour fixes that should happen regardless of sprint planning:

1. **Set SECRET_KEY as required env var** — remove fallback in app.py
2. **Delete root-level duplicate .py files** — reduces confusion immediately
3. **Move processed_emails tracking to SQLite** — prevents re-import on restart
4. **Add `conn.close()` to all direct sqlite3.connect calls** — prevent connection leaks
5. **Remove f-string SQL in quickbooks_agent.py** — 3 injection points
6. **Add before_request auth check** — 10-line change, closes 550+ unprotected routes

---

*Generated: 2026-02-26 | Full codebase audit of Reytech-RFQ*
