# PRD-v31 Implementation Plan
**Started:** 2026-02-26 · **Approach:** One sprint at a time, verify before moving on

---

## SPRINT 0: SECURITY (P0 — No features until this is done) ✅ COMPLETE

### S0.1 — Fix Hardcoded Secret Key (C1) ✅ 
- [x] Remove fallback `"reytech-rfq-2026"` from app.py — RuntimeError if missing
- [x] Verify: app refuses to start without SECRET_KEY

### S0.2 — Global Auth Guard (F1 + C3) ✅ 
- [x] `bp.before_request` at dashboard.py:92 checks Basic Auth on ALL routes
- [x] Allowlist: `/health`, `/static/*`, `/api/health`, `/api/email/track/*`, `/favicon.ico`, `/login`, `/api/qb/callback`, `/api/voice/webhook`, `/api/build`
- [x] Rate limits auth attempts (429 on abuse)
- [x] Logs auth failures to audit_trail
- [x] CSRF origin check on POST/PUT/DELETE
- [x] All 15 exec'd route modules inherit guard via Blueprint

### S0.3 — Fix SQL Injection (C4) ✅ 
- [x] quickbooks_agent.py:407,470,893 — already mitigated with regex date validation
- [x] award_monitor.py:252 — parameterized LIKE clauses (was f-string interpolation)
- [x] award_monitor.py:374 — parameterized LIKE clauses (was f-string interpolation)
- [x] scprs_universal_pull.py:256 — parameterized LIKE search (was f-string interpolation)
- [x] scprs_universal_pull.py:572 — parameterized agency_code filter (was f-string in 4 queries)
- [x] Verified: grep for f-string LIKE/WHERE injection returns 0 hits
- [x] Remaining f-string SQL (dal.py, db.py) uses internal table/column names — acceptable

### S0.4 — Verification ✅ 
- [x] All 89 Python files compile clean
- [x] No f-string SQL injection in execute() calls with user data
- [x] Global auth guard covers all 598 routes via before_request
- [x] SECRET_KEY required (no fallback)

---

## SPRINT 1: FOUNDATION — EXECUTION PLAN
**Strategy:** Prioritize by production risk. DB consolidation (thread safety) > Email idempotency (data loss) > Blueprint cleanup (maintainability)

### S1.1 — Delete Root-Level Duplicate Files (H2) ✅
- [x] Deleted 13 root-level files (10,584 lines removed)
- [x] Updated 7 src/ files to remove try/except root fallback imports
- [x] dashboard.py: Removed 32 lines of fallback import chains
- [x] Verified: all imports reference src.* paths, 89+ files compile clean

### S1.2 — Blueprint Refactor: Incremental exec() Cleanup (F2) — DEFERRED
**Rationale:** The exec/importlib pattern works reliably today. Security risk (C3) is fully mitigated by global auth guard. A 15-module Blueprint split carries high regression risk (~598 routes affected) with low immediate value. Route modules have 0 own imports — all depend on dashboard globals. Moving bp definition creates circular import cascade.
**When to tackle:** Sprint 5 (Operations) alongside M5 Structured Logging, when system is stable enough for deep refactor.
**Prep done:**
- [x] Mapped all 15 modules' dependency on dashboard globals (bp, auth_required)
- [x] Documented: routes_agents, routes_catalog_finance, routes_orders_full, routes_rfq, routes_voice_contacts have 0 own imports
- [x] L12 lesson documented: exec'd modules can't be imported normally

### S1.3 — SQLite-Only: Fix Worst Dual-Write Issues (F3 + H1 + M7) ✅
- [x] Email poller: _load_processed reads from BOTH JSON + SQLite (union of UIDs)
- [x] Email poller: _save_processed writes to BOTH JSON + SQLite
- [x] Auto-creates processed_emails table on first use
- [x] Recovers UIDs from SQLite if JSON is lost/corrupt (Railway restart safe)
- [ ] DEFERRED: Full JSON→SQLite migration for price_checks, rfqs (low risk, high effort)

### S1.4 — Consolidate DB Access (H3) ✅
- [x] award_monitor.py: Converted 4 `conn = get_db()` to `with get_db() as conn:`
- [x] scprs_universal_pull.py: Converted local get_db() to @contextmanager, fixed 5 call sites
- [x] Early return paths in run_universal_pull now close connections
- [ ] DEFERRED: Route modules (20+ direct connects) and db.py internals (40+ calls) — lower priority, no thread safety risk

### S1.5 — Verification ✅
- [x] Full compile check (90 files clean)
- [x] Startup test: app creates successfully, all 598 routes registered
- [x] Key routes verified: /, /health, /pricechecks, /quotes
- [x] Fixed _load_route_module Python 3.12+ compat (save/restore module identity)
- [x] Git log review — clean commit history
- [x] Push to production

## SPRINT 2: STABILITY ✅ COMPLETE

### S2.1 — Centralized Scheduler (F4) ✅
- [x] Created src/core/scheduler.py — job registry, heartbeat tracking, dead job detection
- [x] 8 background jobs registered: email-poller, award-monitor, follow-up-engine, quote-lifecycle, email-retry, lead-nurture, qa-monitor, growth-agent
- [x] Heartbeats wired into: email_poll_loop, award_monitor._monitor_loop, follow_up_engine._loop, quote_lifecycle._run_lifecycle_check
- [x] Dead job detection: log CRITICAL if 3x interval missed
- [x] GET /api/scheduler/status — full job dashboard with dead_count

### S2.2 — Automated Database Backups (F5) ✅
- [x] sqlite3 .backup API for consistent snapshots
- [x] Daily backup thread (24h interval, first run after 60s)
- [x] Rotation: 7 daily + 4 weekly
- [x] GET /api/admin/backups — list with sizes
- [x] POST /api/admin/backup-now — trigger immediate backup
- [x] backup_health() — alerts if latest >36h old

### S2.3 — Verification ✅
- [x] 91 files compile clean
- [x] App starts: 601 routes registered (3 new scheduler/backup endpoints)
- [x] Auth guard covers new endpoints
- [x] Push to production

## SPRINT 3: INTELLIGENCE ✅ COMPLETE

### S3.1 — Unified Full-Text Search (F10) ✅
- [x] Added price_checks search via SQLite (parameterized LIKE across id, requestor, agency, items)
- [x] Added products/catalog search (name, mfg_number, category)
- [x] Results include: quotes, contacts, intel_buyers, orders, rfqs, price_checks, products (7 entity types)
- [x] Updated breakdown to include new entity types

### S3.2 — Smart Email Classification v2 (F6) ✅
- [x] Created src/agents/email_classifier.py — 5-dimension scoring system
- [x] Dimensions: new_pc, new_rfq, reply_followup, po_award, cs_inquiry
- [x] Confidence = margin between top two scores; needs_review if < 0.15
- [x] email_classifications table for audit trail (auto-created)
- [x] GET /api/email/review-queue — low-confidence classifications
- [x] POST /api/email/classify-test — test classification on sample text

### S3.3 — Margin Optimizer Dashboard (F7) ✅
- [x] Created src/knowledge/margin_optimizer.py
- [x] Overall stats: win rate, avg margin, won revenue
- [x] Low-margin alert: items < 15% margin from recent quotes
- [x] "Should have won" detector: lost quotes within 5% of competitor price (via SCPRS notes)
- [x] Price source breakdown and category margins
- [x] GET /api/margins/summary — full dashboard data
- [x] GET /api/margins/item?description= — per-item pricing intelligence

### S3.4 — Verification ✅
- [x] 93 files compile clean
- [x] App starts: 605 routes (4 new Sprint 3 endpoints)
- [x] Push to production

## SPRINT 4: GROWTH ✅ COMPLETE

### S4.1 — Order Lifecycle + Revenue Tracking (F8) ✅
- [x] Created src/core/order_lifecycle.py — status transitions with audit trail
- [x] ORDER_STATUSES: received→processing→ordered_from_vendor→shipped→delivered→invoiced→paid
- [x] transition_order(): validates status, logs to order_status_log table, updates timestamps
- [x] get_order_detail(): full order with lifecycle timeline
- [x] get_revenue_ytd(): aggregates revenue_log + orders + won quotes, by month/agency/source
- [x] POST /api/orders/<id>/transition — status change endpoint
- [x] GET /api/orders/<id>/detail — order + lifecycle history
- [x] GET /api/revenue/ytd — YTD revenue dashboard data
- [x] GET /api/orders/unpaid — flag invoices older than N days

### S4.2 — Growth Agent: Prospect Scoring (F9) ✅
- [x] Created src/agents/prospect_scorer.py — 4-dimension scoring system
- [x] Dimensions: volume (30%), recency (25%), match (25%), gap (20%)
- [x] Deprioritizes existing customers (0.5x) and recently contacted (0.7x)
- [x] Includes buyer contacts from SCPRS PO data
- [x] GET /api/growth/prospects — scored + ranked prospect list with contacts

### S4.3 — Google Drive Integration (F11) — DEFERRED
**Rationale:** Requires OAuth2 service account configuration on Google Cloud Console + Railway env vars. Cannot be done in this session. Documented for manual setup.

### S4.4 — Verification ✅
- [x] All files compile clean
- [x] App starts: 610 routes (5 new Sprint 4 endpoints)
- [x] Push to production

## SPRINT 5: OPERATIONS
### S5.1 — Settings Dashboard + System Health (F12)
### S5.2 — Database Migrations Framework (M3)
### S5.3 — Structured Logging + Alerting (M5)
### S5.4 — Verification

---

## REVIEW LOG
