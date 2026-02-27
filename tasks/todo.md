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

### S1.2 — Blueprint Refactor: Incremental exec() Cleanup (F2)
**Approach:** Full 15-module Blueprint split is too risky in one pass. Instead:
- [ ] Add explicit imports to each route module (remove dependency on globals injection)
- [ ] Simplify _load_route_module: stop injecting entire globals()
- [ ] Keep single `bp` Blueprint shared across modules (multi-Blueprint is Sprint 5 nice-to-have)
- [ ] Verify: all routes still respond, no 500s, no NameErrors in logs

### S1.3 — SQLite-Only: Fix Worst Dual-Write Issues (F3 + H1 + M7)
**Approach:** Full JSON removal is too large. Fix the 3 highest-risk patterns:
- [ ] Move processed_emails UID tracking from JSON to SQLite (prevents re-import on restart — M7)
- [ ] Audit and fix price_checks.json dual-write (L1: thread safety in background agents)
- [ ] Identify remaining JSON writes; document but defer low-risk ones
- [ ] Verify: email poller doesn't re-process after Railway restart

### S1.4 — Consolidate DB Access (H3) — IN PROGRESS
- [x] award_monitor.py: Converted 4 `conn = get_db()` to `with get_db() as conn:` (proper commit/close)
- [ ] scprs_universal_pull.py: Wrap 5 get_db() calls in try/finally conn.close()
- [ ] Agent files with fallback get_db(): add conn.close() in finally blocks
- [ ] Route modules: Add conn.close() to critical paths
- **Note:** 84+ total calls. Agent files (background threads) are highest priority.

### S1.5 — Verification
- [ ] Full compile check (89+ files)
- [ ] Startup test (app creates successfully with all modules loaded)
- [ ] Route smoke test via validate_build.py
- [ ] Git log review — clean commit history
- [ ] Push to production

## SPRINT 2: STABILITY
### S2.1 — Centralized Scheduler (F4)
### S2.2 — Automated Database Backups (F5)
### S2.3 — Verification

## SPRINT 3: INTELLIGENCE
### S3.1 — Smart Email Classification v2 (F6)
### S3.2 — Margin Optimizer Dashboard (F7)
### S3.3 — Unified Full-Text Search (F10)
### S3.4 — Verification

## SPRINT 4: GROWTH
### S4.1 — Order Lifecycle + Revenue Tracking (F8)
### S4.2 — Growth Agent: SCPRS Historical Pull + Outreach (F9)
### S4.3 — Google Drive Integration (F11)
### S4.4 — Verification

## SPRINT 5: OPERATIONS
### S5.1 — Settings Dashboard + System Health (F12)
### S5.2 — Database Migrations Framework (M3)
### S5.3 — Structured Logging + Alerting (M5)
### S5.4 — Verification

---

## REVIEW LOG
