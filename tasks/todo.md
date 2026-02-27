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

### S1.2 — Blueprint Refactor: Shared Infrastructure Extraction (F2) — ✅ COMPLETE (Sprint 11)
**Completed:** Extracted bp + auth + rate limiting into src/api/shared.py. Added explicit imports
to all 15 route modules. _load_route_module kept as safety net for dashboard-specific functions.
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
- [x] DONE: JSON→SQLite dual-write for rfqs (price_checks already had it)
- [x] DONE: SQLite restore for load_rfqs when JSON is empty (post-deploy recovery)

### S1.4 — Consolidate DB Access (H3) ✅
- [x] award_monitor.py: Converted 4 `conn = get_db()` to `with get_db() as conn:`
- [x] scprs_universal_pull.py: Converted local get_db() to @contextmanager, fixed 5 call sites
- [x] Early return paths in run_universal_pull now close connections
- [x] DONE: Route modules rfq.db direct connects migrated to reytech.db (19 refs in routes_features + routes_features2)

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

## SPRINT 5: OPERATIONS ✅ COMPLETE

### S5.1 — Settings Dashboard + System Health (F12) ✅
- [x] GET /api/system/health — unified health: DB (tables, size), scheduler (dead jobs), backups (age), schema (version)
- [x] Aggregates existing /api/system/dashboard, scheduler, backup health into single endpoint
- [x] Returns status: "ok" | "degraded" with per-check details

### S5.2 — Database Migrations Framework (M3) ✅
- [x] Created src/core/migrations.py — versioned schema migrations
- [x] 5 initial migrations: order_status_log, processed_emails, email_classifications, backup_log, scheduler_heartbeats
- [x] run_migrations() called at app startup — safe to run repeatedly (idempotent)
- [x] schema_migrations table tracks applied versions
- [x] GET /api/system/migrations — status + history
- [x] POST /api/system/migrations/run — trigger migrations manually

### S5.3 — Structured Logging + Alerting (M5) ✅
- [x] Created src/core/structured_log.py — JSON formatter for Railway
- [x] Production: single-line JSON with ts, level, logger, msg, error trace
- [x] Development: human-readable format with timestamps
- [x] Auto-detects RAILWAY_ENVIRONMENT for format selection
- [x] Quiets noisy libraries (urllib3, werkzeug, httpx)
- [x] Wired into app startup via setup_structured_logging()

### S5.4 — Verification ✅
- [x] 97 files compile clean
- [x] App starts: 613 routes (3 new Sprint 5 endpoints)
- [x] Migrations run on startup
- [x] Push to production

---

## SPRINT 6: HARDENING ✅ COMPLETE

### S6.1 — DB Consolidation: Route Modules (S1.4 deferred) ✅
- [x] routes_crm.py: Converted 3 direct sqlite3.connect(reytech.db) → get_db() context manager
- [x] routes_growth_intel.py: Converted 1 direct connect → get_db()
- [x] routes_rfq.py: Converted 1 health check connect → get_db()
- [x] 0 remaining reytech.db direct connects in route modules
- [x] 22 rfq.db direct connects deferred (legacy DB, needs data migration)

### S6.2 — Input Validation Framework (M1) ✅
- [x] Created src/core/validators.py
- [x] validate_required, validate_optional, validate_email, validate_number
- [x] validate_enum, validate_id, validate_date, sanitize_string
- [x] ValidationError exception class for clean 400 responses

### S6.3 — Error Handler Framework (M2) ✅
- [x] Created src/core/error_handler.py
- [x] safe_call(): replaces silent `except: pass` with logged failures
- [x] safe_route: decorator for Flask routes — catches unhandled exceptions → JSON 500
- [x] safe_background: decorator for background threads — logs CRITICAL on crash
- [x] log_error(): standardized exception logging with context

### S6.4 — Verification ✅
- [x] 99 files compile clean
- [x] App starts: 613 routes
- [x] Push to production

## SPRINT 7: TEST COVERAGE + CONNECTION SAFETY ✅ COMPLETE

### S7.1 — Fix Fallback get_db() Patterns ✅
- [x] award_monitor.py: Converted raw-connection fallback to @contextmanager
- [x] security.py: Added try/finally + timeout to _log_audit_internal
- [x] pricing_intel.py, won_quotes_db.py, cchcs_intel_puller.py: Audited — all conn.close() present
- [x] 0 connection leaks remaining in agent files

### S7.2 — Sprint Smoke Tests (M4) ✅
- [x] Created tests/test_sprints.py — 17 tests covering all Sprint 0-6 features
- [x] Auth guard: anon gets 401, auth gets 200, /health is public
- [x] Scheduler, backups, email classifier, margins, revenue, prospects, system health
- [x] Startup integrity: route count > 500, critical routes exist
- [x] Fixed conftest: monkeypatch DASH_USER/DASH_PASS at module level
- [x] 16/17 pass (1 known exec module limitation in search route)

### S7.3 — Verification ✅
- [x] 99 files compile clean
- [x] Push to production

## SPRINT 8: DATA QUALITY + OBSERVABILITY ✅ COMPLETE

### S8.1 — Data Integrity Checker ✅
- [x] Created src/core/data_integrity.py — 6 cross-table consistency checks
- [x] Checks: orphaned order→quote refs, duplicate quote numbers, missing order items,
      stale pending quotes (>90d), revenue log consistency, table health
- [x] GET /api/system/integrity — run all checks, returns pass/fail with details

### S8.2 — System Preflight + Route Map ✅
- [x] GET /api/system/preflight — combined health + integrity + schema + route count
- [x] GET /api/system/routes — auto-generated API documentation (all routes with methods)
- [x] Fixed: backup_health import (was referencing nonexistent db_backup module)
- [x] Fixed: INTEL_AVAILABLE NameError — pre-defined default in dashboard.py before route module loading

### S8.3 — Verification ✅
- [x] 100 files compile clean
- [x] App starts: 616 routes (3 new Sprint 8 endpoints)
- [x] 20/20 sprint smoke tests pass
- [x] Pre-deploy check passes
- [x] Push to production

## SPRINT 11: BLUEPRINT REFACTOR (S1.2) ✅ COMPLETE

### S11.1 — Extract shared infrastructure ✅
- [x] Created src/api/shared.py — Blueprint, auth_required, check_auth, rate limiting,
      CSRF guard, request timing (all moved from dashboard.py)
- [x] dashboard.py now imports from shared.py (removed ~100 lines of duplicate code)
- [x] No circular imports — shared.py has no dashboard dependencies

### S11.2 — Explicit imports in route modules ✅
- [x] All 15 route modules now have explicit imports:
      from flask import request, jsonify, Response, ...
      from src.api.shared import bp, auth_required
      from src.core.paths import DATA_DIR, ...
      from src.core.db import get_db
      from src.api.render import render_page
- [x] _load_route_module injection still works as safety net for dashboard-specific
      functions (load_rfqs, save_rfqs, POLL_STATUS, etc.)
- [x] Modules are now self-documenting — core dependencies visible at top of file

### S11.3 — Test infrastructure updates ✅
- [x] conftest.py: patches both dashboard + shared module auth
- [x] test_critical_paths.py: patches both modules
- [x] 39/39 tests pass, pre-deploy check clean

## SPRINT 10: PDF VERSIONING + FINAL HARDENING ✅ COMPLETE

### S10.1 — PDF Template Versioning (M6) ✅
- [x] Created src/forms/pdf_versioning.py — version registry + generation audit log
- [x] Template versions: quote v2.1, invoice v1.1, price_check v1.0
- [x] stamp_pdf_metadata() wired into quote_generator + invoice_generator
- [x] Migration v6: pdf_generation_log table with indexes
- [x] GET /api/system/pdf-versions — template versions + generation stats

### S10.2 — Verification ✅
- [x] 101 files compile clean
- [x] App starts: 617 routes
- [x] 39/39 tests pass
- [x] Push to production

## SPRINT 13: DATA TRACING + QA PROCEDURES ✅ COMPLETE

### S13.1 — Blueprint Refactor (shared.py) ✅
- [x] Extracted bp, auth_required, check_auth, rate limiting into src/api/shared.py
- [x] dashboard.py imports from shared.py (no more duplicate definitions)
- [x] Fixed conftest autouse fixture to accept any valid credentials
- [x] All 43 tests pass with refactored auth

### S13.2 — Data Lineage Tracer ✅
- [x] Created src/core/data_tracer.py — traces documents through full pipeline
- [x] Supports: RFQ → Price Check → Quote → Order → Revenue → PDF
- [x] Auto-detects document type from ID format
- [x] GET /api/system/trace/{doc_id}?type= — per-document lineage
- [x] GET /api/system/pipeline — conversion rates and stage counts

### S13.3 — QA Dashboard + Procedures ✅
- [x] GET /api/system/qa — combined health+integrity+pipeline+schema+routes+PDF versions
- [x] Created docs/QA_PROCEDURES.md — comprehensive QA runbook
- [x] Manual checklist: auth, pipeline, API endpoints, tracing, DB health
- [x] Deployment procedure with pre/post verification steps
- [x] Regression prevention guide with known failure patterns

### S13.4 — Verification ✅
- [x] 103 files compile clean
- [x] 621 routes (4 new: trace, pipeline, qa, shared.py auth)
- [x] 43/43 tests pass (4 new tracing/QA tests)
- [x] Pre-deploy check passes
- [x] Push to production

## REVIEW LOG
