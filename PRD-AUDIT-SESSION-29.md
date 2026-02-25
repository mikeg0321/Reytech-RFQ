# Reytech RFQ Platform — Comprehensive Audit & PRD v29

**Date:** February 24, 2026  
**Scope:** Full-stack production audit from first principles  
**Codebase:** 64,062 lines Python · 9,275 lines HTML · 453 route endpoints · 25 DB tables

---

## PART 1: COMPREHENSIVE AUDIT

### 1.1 Architecture Overview

| Layer | Technology | Size |
|-------|-----------|------|
| Web Framework | Flask + Gunicorn (2 workers) | 2,664 LOC (dashboard.py) |
| Route Modules | 9 module files loaded dynamically | 19,870 LOC total |
| Data Store (Primary) | JSON files with file locks | 25 JSON files in /data |
| Data Store (Secondary) | SQLite (reytech.db) | 25 tables |
| Templates | Jinja2 HTML | 37 templates |
| Agents | Background processors | 37 agent modules |
| Auth | HTTP Basic Auth | Single user |
| Hosting | Railway (persistent volume) | 2 Gunicorn workers |

### 1.2 Security Audit

#### ✅ PASSING

| Check | Status | Details |
|-------|--------|---------|
| Auth on routes | ✅ | 448 routes have `@auth_required`; 5 properly exempt (health, OAuth callback, webhooks, email tracking) |
| Path traversal | ✅ | File downloads use `os.path.basename()` to strip directory components |
| SQL injection | ✅ | All user-facing queries use parameterized `?` placeholders; f-string SQL limited to code-controlled table/column names with whitelist |
| Rate limiting | ✅ | Token-bucket limiter with tiers: auth (6/min), API (60/min), heavy (12/min) |
| Security headers | ✅ | X-Content-Type-Options, X-Frame-Options, X-XSS-Protection, Referrer-Policy |
| Input sanitization | ✅ | `_sanitize_input()` used on search queries; `_safe_filename()` on uploads |
| Audit trail | ✅ | `audit_trail` SQLite table logs rate limits, CSRF failures, security events |
| Atomic writes | ✅ | JSON saves use temp file → fsync → `os.replace()` (POSIX atomic) |
| Webhook auth | ✅ | Voice webhook validates `VAPI_WEBHOOK_SECRET` when configured |

#### ⚠️ WARNINGS

| Issue | Severity | Status |
|-------|----------|--------|
| Default password "changeme" | HIGH | **FIXED** — Startup warning added; recommend env var enforcement |
| No CSRF tokens on HTML forms | MEDIUM | CSRF middleware exists but forms don't include tokens; mitigated by Origin header check for same-origin requests |
| 27 `\|safe` template usages | MEDIUM | XSS surface — most are server-generated HTML/JSON, not user input; review each usage |
| No Content-Security-Policy header | LOW | Would prevent XSS from inline scripts; complex to add given current inline JS pattern |
| HTTP Basic Auth (no sessions) | LOW | Credentials sent on every request; adequate for single-user internal tool behind HTTPS |
| No request signing for inter-service calls | LOW | Background threads call routes internally; no auth header on these |

#### 🔴 CRITICAL FINDINGS (FIXED THIS SESSION)

1. **Health Check Missing** → Added `/health` endpoint (DB + disk checks, no auth)
2. **110 Swallowed Exceptions** → Fixed 22 critical `except: pass` → `log.debug()` in dashboard.py and routes_rfq.py
3. **RFQ Status Bug: "PRICED" with $0.00** → Now checks actual price data before labeling

### 1.3 Data Flow Audit

#### Primary Data Store: JSON Files (25 files)

```
load_rfqs() → read rfqs.json (with 2s TTL cache)
  ↓ modify in memory
save_rfqs() → atomic write to rfqs.json (temp → fsync → replace)
```

**Race Condition Risk:** With 2 Gunicorn workers, Worker A and Worker B can both `load_rfqs()`, make independent changes, and one `save_rfqs()` overwrites the other. The file lock on `_save_price_checks()` uses `fcntl.LOCK_EX` but only during the write — the read-modify-write cycle is NOT atomic.

**Impact:** Low in practice (single-user app, sequential requests), but a ticking time bomb if usage scales.

**Recommendation:** Migrate primary store to SQLite with proper transactions, or add read-write locks around the full load→modify→save cycle.

#### Dual-Store Pattern (JSON + SQLite)

The app maintains BOTH JSON files and SQLite tables for the same entities. This creates:
- **Sync drift:** SQLite may not reflect latest JSON state and vice versa
- **Boot recovery:** `_restore_from_sqlite()` repopulates JSON from SQLite on cold start
- **Write amplification:** Many paths write to both stores

**Recommendation:** Consolidate to SQLite as single source of truth with JSON as export/cache only.

#### Data Flow: Email → RFQ → PC → Quote → Sent

```
Email Poller (background thread)
  → process_rfq_email() 
    → parse_rfq_attachments() → identify 703B/704B
    → create RFQ record in rfqs.json
    → create PC record in price_checks.json  
    → auto_price: SCPRS + Amazon + Catalog lookup
    → set status: 'priced' if prices found, 'draft' if not
    → log CRM activity
    → send notification

User Interaction:
  → /rfq/<id> detail page
  → edit prices, save → /rfq/<id>/update
  → preview quote → showRfqPreview() (JS)
  → generate package → /rfq/<id>/generate-package
  → send → /rfq/<id>/send
  → mark won/lost → status terminal
```

### 1.4 Performance Audit

| Issue | Impact | Severity |
|-------|--------|----------|
| `load_rfqs()` called 99 times across routes | Entire JSON loaded from disk per request (mitigated by 2s TTL cache) | MEDIUM |
| pc_detail.html is 2,423 lines with 323 inline styles | Slow initial render, hard to maintain | MEDIUM |
| No pagination on home queue or archive | All records loaded into DOM | LOW |
| No CDN for static assets | JS/CSS served by Gunicorn workers | LOW |
| 2 Gunicorn workers only | Limited concurrent request handling | LOW |
| No database connection pooling | New SQLite connection per query | LOW |

### 1.5 Code Quality Audit

| Metric | Value | Assessment |
|--------|-------|------------|
| Total Python LOC | 64,062 | Large monolith |
| dashboard.py | 2,664 lines | God module — should be split |
| Largest route module | routes_intel.py (5,151 lines) | Too large |
| Largest template | pc_detail.html (2,423 lines) | Component extraction needed |
| Inline styles | 323 in pc_detail.html alone | Should extract to CSS |
| Inline event handlers | 62 in pc_detail.html | Should use addEventListener |
| Test coverage | 4,095 lines across 14 test files | ~6% code coverage — needs expansion |
| Exception handling | 110 swallowed exceptions (22 fixed) | 88 remaining across other modules |

### 1.6 Logging Audit

| Component | Status |
|-----------|--------|
| Structured JSON logging | ✅ Configured via `logging_config.py` |
| Request logging | ⚠️ No access log middleware |
| Error alerting | ⚠️ Notifications exist but no external alerting (PagerDuty, Slack webhook) |
| Log rotation | ✅ RotatingFileHandler configured |
| Audit trail | ✅ SQLite `audit_trail` table |
| Trace system | ✅ Custom `Trace` class with step tracking |

### 1.7 UI/UX Audit

| Area | Finding | Priority |
|------|---------|----------|
| RFQ auto-import feedback | **FIXED** — Added lookup results banner showing SCPRS/Amazon/Catalog hit counts |
| Status accuracy | **FIXED** — "PRICED" only when items actually have prices; "DRAFT" otherwise |
| Requestor linkage | **FIXED** — Name links to CRM profile, email to mailto: |
| Print preview | **FIXED** — Opens clean window instead of printing full dark UI |
| Mobile responsiveness | ⚠️ Tables overflow on mobile; no responsive breakpoints |
| Keyboard navigation | ⚠️ No focus management, tab traps in modals |
| Loading states | ⚠️ No skeleton loaders; SCPRS/Amazon lookups show no progress indicator |
| Error messages | ⚠️ Flash messages disappear; no persistent error banner |
| Accessibility | ⚠️ Missing ARIA labels, contrast issues on dark theme |
| Form validation | ⚠️ Client-side only; no HTML5 required/pattern attributes |

---

## PART 2: FIXES APPLIED THIS SESSION

| # | Fix | Commit | Impact |
|---|-----|--------|--------|
| 1 | RFQ status: DRAFT when $0.00 instead of PRICED | 312f187 | Prevents misleading status |
| 2 | Auto-lookup results banner (SCPRS/Amazon/Catalog counts) | 312f187 | Clear confirmation of what ran |
| 3 | Requestor name → CRM profile link | 312f187 | One-click buyer history |
| 4 | Email → mailto: link | 312f187 | One-click outreach |
| 5 | Print preview → clean popup window | 312f187 | Proper printing |
| 6 | /health endpoint | 592040c | Railway monitoring |
| 7 | 22 swallowed exceptions → logged | 592040c | Debuggability |
| 8 | Default password startup warning | 592040c | Security awareness |

---

## PART 3: PRD — 15 ENHANCEMENTS

### Enhancement 1: Unified Data Layer — SQLite as Single Source of Truth

**Problem:** Dual JSON + SQLite stores create sync drift, race conditions with 2 workers, and complex boot recovery logic.

**Solution:**
- Migrate `rfqs.json` and `price_checks.json` to SQLite tables with proper schemas
- Use SQLite WAL mode for concurrent reads
- Wrap all read-modify-write cycles in transactions
- Remove JSON file I/O from hot paths
- Keep JSON export as backup/debug only

**Acceptance Criteria:**
- All CRUD operations go through SQLite with transactions
- No more `load_rfqs()` / `save_rfqs()` pattern
- Boot time < 2 seconds (no JSON restore needed)
- Zero data loss on concurrent requests

---

### Enhancement 2: Request Middleware — Access Logging + Timing

**Problem:** No visibility into which routes are hit, how long they take, or who's accessing what.

**Solution:**
- `@app.before_request` / `@app.after_request` middleware logging: method, path, status, duration_ms, IP
- Structured JSON log lines for parsing
- Slow request warning threshold (>2s)
- Request ID propagation through log context

**Acceptance Criteria:**
- Every request logged with timing
- Slow requests (>2s) logged at WARNING
- Request ID visible in all downstream logs

---

### Enhancement 3: Smart Loading States + Progress Tracking

**Problem:** SCPRS/Amazon/Catalog lookups run silently — no feedback during 5-30 second operations.

**Solution:**
- Server-Sent Events (SSE) endpoint `/api/progress/<task_id>` for long-running operations
- Inline progress bar on RFQ detail during auto-lookup
- Step-by-step feedback: "Checking SCPRS... ✓ 2/3 found → Checking Amazon... → Checking Catalog..."
- Skeleton loaders for card sections that load async

**Acceptance Criteria:**
- User sees real-time progress during lookups
- Each step shows ✓/✗ result as it completes
- No more "was anything happening?" confusion

---

### Enhancement 4: Remaining Swallowed Exception Cleanup

**Problem:** 88 remaining `except Exception: pass` patterns across 7 route modules silently hide failures.

**Solution:**
- Convert all to `log.debug()` at minimum
- Upgrade to `log.warning()` for any exception in a critical data path (save, generate, send)
- Add Sentry-style error tracking with context (request path, user, data state)
- Create `/api/admin/errors` endpoint showing recent logged exceptions

**Acceptance Criteria:**
- Zero bare `except: pass` patterns remain
- All exceptions in critical paths logged at WARNING+
- Admin can review recent errors without SSH

---

### Enhancement 5: CSRF Token Integration on All Forms

**Problem:** CSRF middleware exists but no forms include the token. Currently mitigated by Origin header check, but not defense-in-depth.

**Solution:**
- Add `{{ csrf_token() }}` hidden field to all POST forms in templates
- Update JavaScript AJAX calls to include X-CSRF-Token header
- Enable strict CSRF validation (remove Origin bypass)
- Add CSRF token refresh on session expiry

**Acceptance Criteria:**
- All 19 POST forms include CSRF token
- All AJAX POST/PUT/DELETE calls include X-CSRF-Token
- CSRF validation enforced on all state-changing requests

---

### Enhancement 6: Buyer Intelligence Dashboard (CRM Profile Page)

**Problem:** Requestor name links to contacts page but there's no unified buyer profile showing RFQ history, win/loss record, spend patterns, and communication history.

**Solution:**
- `/buyer/<email>` profile page aggregating:
  - All RFQs from this buyer (with status, revenue)
  - Win rate, average margin, total spend
  - Communication timeline (emails sent/received)
  - Price sensitivity analysis (how often they pick lowest bid)
  - Institution budget cycle timing
- Quick actions: send email, create follow-up, export history

**Acceptance Criteria:**
- One-click from any RFQ/PC to buyer profile
- Win rate and spend totals calculated in real-time
- Communication history shows full email thread

---

### Enhancement 7: Automated Price Intelligence — Margin Optimizer

**Problem:** User manually decides bid prices by eyeballing SCPRS, Amazon, cost, and hoping for a good margin. No data-driven recommendation.

**Solution:**
- `recommended_price` auto-calculation engine:
  - If SCPRS exists: undercut by configurable % (default 2%)
  - If won quotes exist for same/similar item: use historical winning price adjusted for inflation
  - If Amazon exists but no SCPRS: markup from Amazon wholesale
  - Confidence score (high/medium/low) based on data freshness
- One-click "Apply All Recommended" button
- Margin visualization: green (>20%), yellow (10-20%), red (<10%)

**Acceptance Criteria:**
- Every line item shows recommended price with reasoning
- One-click applies all recommendations
- Historical win data improves recommendations over time

---

### Enhancement 8: Email Send Integration from Detail Page

**Problem:** After generating a quote/704, user must manually open email, attach PDF, compose message, and send. No tracking of what was sent.

**Solution:**
- "📧 Send Quote" button on RFQ/PC detail that:
  - Pre-fills To (requestor email), Subject (Re: Solicitation #X)
  - Attaches generated PDF automatically
  - Uses professional email template with Reytech branding
  - Sends via Gmail API (already configured)
  - Records in `email_log` + `sent_documents` tables
  - Auto-transitions status to "sent"
- Template editor for customizing email body per-institution

**Acceptance Criteria:**
- One-click send from detail page
- PDF attached automatically
- Email logged with open/click tracking
- Status auto-transitions to "sent"

---

### Enhancement 9: Mobile-Responsive Layout

**Problem:** 323 inline styles, tables overflow on mobile, no responsive breakpoints. Field crews can't review RFQs on phones.

**Solution:**
- Extract inline styles to CSS utility classes
- Add responsive breakpoints: cards stack vertically, tables scroll horizontally
- Collapsible sections for line items on mobile
- Touch-friendly input sizes (min 44px tap targets)
- PWA manifest for home screen installation

**Acceptance Criteria:**
- RFQ detail usable on 375px screen width
- All inputs are touch-friendly
- Key actions (save, preview, send) accessible without scrolling
- Lighthouse mobile score > 80

---

### Enhancement 10: Pipeline Analytics Dashboard

**Problem:** Home page shows basic KPIs but no trend analysis, conversion funnel, or forecasting.

**Solution:**
- `/analytics` page with:
  - Conversion funnel: Email → Parsed → Priced → Sent → Won (with drop-off %)
  - Revenue trend chart (daily/weekly/monthly)
  - Win rate by institution, buyer, product category
  - Average time-to-quote metric
  - Pipeline forecast: expected revenue from pending quotes
  - Competitor win/loss analysis
- Date range picker for filtering
- Export to CSV/PDF

**Acceptance Criteria:**
- Funnel shows real conversion rates
- Revenue trend is accurate to DB
- Win rate breakdowns help prioritize effort
- Time-to-quote drives process improvement

---

### Enhancement 11: Bulk Operations on Queue

**Problem:** Each RFQ/PC must be opened individually. No way to bulk-price, bulk-dismiss, or bulk-assign.

**Solution:**
- Checkbox column on home queue
- Bulk actions toolbar: "Mark Selected as..." (dismiss, archive, assign)
- Bulk quick-markup: select all → apply +20% markup to all items
- Bulk SCPRS/Amazon lookup: run pricing on all selected RFQs at once
- Keyboard shortcuts: Ctrl+A (select all), Delete (dismiss selected)

**Acceptance Criteria:**
- Multi-select with shift+click range
- Bulk actions execute in background with progress
- Keyboard shortcuts documented in help modal

---

### Enhancement 12: Duplicate Detection & Revision Linking

**Problem:** Same solicitation may arrive via email multiple times (amendments, corrections). Currently creates duplicate entries.

**Solution:**
- On import: check if solicitation number already exists
- If duplicate detected:
  - Show "Amendment detected" banner
  - Diff line items against previous version
  - Link as revision (not new RFQ)
  - Preserve pricing from previous version
- Revision history view showing all versions of an RFQ
- Amendment change highlighting (red=removed, green=added items)

**Acceptance Criteria:**
- Duplicate solicitation numbers auto-linked
- Amendment diffs visible at a glance
- Previous pricing preserved on amendments

---

### Enhancement 13: Webhook/API for External Integrations

**Problem:** All interactions are through the web UI. No way for external tools (QuickBooks, Slack, email bots) to push/pull data programmatically.

**Solution:**
- REST API with token auth (separate from Basic Auth):
  - `GET /api/v1/rfqs` — list with filtering
  - `GET /api/v1/rfqs/<id>` — detail
  - `POST /api/v1/rfqs/<id>/price` — submit pricing
  - `GET /api/v1/stats` — dashboard KPIs
- Outbound webhooks on status changes (configurable URL)
- Slack integration: new RFQ notification, quick-price from Slack
- API key management page

**Acceptance Criteria:**
- API returns JSON with consistent schema
- Token auth with per-key rate limits
- Webhook fires on: new_rfq, priced, sent, won, lost
- Slack bot responds to `/price <solicitation#>` command

---

### Enhancement 14: Test Coverage Expansion

**Problem:** 4,095 lines of tests covering ~6% of 64,062 LOC. Critical paths (PDF generation, auto-pricing, email import) have no automated tests.

**Solution:**
- Integration tests for critical paths:
  - Email import → RFQ creation → auto-price → generate → send
  - PC upload → parse → catalog match → fill 704 → download
  - Status transitions (full lifecycle)
- Unit tests for:
  - `_sanitize_input()` edge cases
  - `parse_rfq_attachments()` with malformed PDFs
  - Price calculation logic
  - MFG# extraction regex
- Load test: 50 concurrent requests against JSON store
- CI pipeline: run tests on every push

**Acceptance Criteria:**
- Critical path coverage > 80%
- Tests run in < 60 seconds
- CI blocks deploy on test failure
- PDF generation tests use mock PDFs

---

### Enhancement 15: Configuration Management UI

**Problem:** Markup percentages, SCPRS undercut %, email templates, auto-pricing rules are hardcoded or spread across multiple files.

**Solution:**
- `/settings` page with sections:
  - **Pricing Rules:** default markup %, SCPRS undercut %, minimum margin
  - **Email Templates:** editable templates for quote send, follow-up, won notification
  - **Auto-Import Rules:** which solicitations to auto-process vs queue for review
  - **Notification Preferences:** what triggers alerts (new RFQ, price found, deadline approaching)
  - **User Profile:** company info, signature, logo
- Settings stored in SQLite `config` table
- Hot-reload: changes take effect immediately without restart

**Acceptance Criteria:**
- All configurable values editable through UI
- Changes persist across deploys (SQLite on persistent volume)
- Validation prevents invalid values (negative markup, etc.)
- Audit log records who changed what setting

---

## PART 4: PRIORITY MATRIX

| # | Enhancement | Effort | Impact | Priority |
|---|------------|--------|--------|----------|
| 1 | SQLite as single source of truth | HIGH | HIGH | P0 — Architectural debt |
| 4 | Exception cleanup (88 remaining) | LOW | HIGH | P0 — Reliability |
| 2 | Request middleware logging | LOW | MEDIUM | P1 — Observability |
| 5 | CSRF token integration | LOW | MEDIUM | P1 — Security |
| 3 | Smart loading states | MEDIUM | HIGH | P1 — UX |
| 7 | Margin optimizer | MEDIUM | HIGH | P1 — Revenue |
| 8 | Email send integration | MEDIUM | HIGH | P1 — Workflow |
| 6 | Buyer intelligence dashboard | MEDIUM | MEDIUM | P2 — Intelligence |
| 10 | Pipeline analytics | MEDIUM | MEDIUM | P2 — Strategy |
| 14 | Test coverage expansion | HIGH | HIGH | P2 — Quality |
| 12 | Duplicate detection | MEDIUM | MEDIUM | P2 — Data quality |
| 11 | Bulk operations | MEDIUM | MEDIUM | P3 — Efficiency |
| 9 | Mobile responsive | HIGH | LOW | P3 — Accessibility |
| 13 | External API/webhooks | HIGH | MEDIUM | P3 — Integration |
| 15 | Configuration UI | MEDIUM | LOW | P3 — Maintainability |

---

## PART 5: LESSONS CAPTURED

```
L57: Default passwords must trigger loud startup warnings (and ideally block production boot)
L58: Health check endpoints are mandatory for any Railway/container deployment
L59: except: pass is tech debt that compounds — always log at minimum debug level
L60: Dual data stores (JSON + SQLite) create sync drift; pick one source of truth
L61: 2 Gunicorn workers + JSON file store = potential race condition on concurrent writes
L62: CSRF middleware is useless without tokens in forms — defense must be end-to-end
L63: Status labels must reflect actual data state, not pipeline stage assumption
L64: Auto-import results need explicit UI confirmation — users need to know what ran
L65: Print from web app should open clean popup window, never window.print() on dark UI
L66: Buyer names and emails should always link to actionable destinations (CRM, mailto)
```
