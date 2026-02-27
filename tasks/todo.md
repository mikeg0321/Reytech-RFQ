# PRD-v31 Implementation Plan
**Started:** 2026-02-26 · **Approach:** One sprint at a time, verify before moving on

---

## SPRINT 0: SECURITY (P0 — No features until this is done)

### S0.1 — Fix Hardcoded Secret Key (C1) ✅ ALREADY DONE
- [x] Remove fallback `"reytech-rfq-2026"` from app.py — already fixed in repo
- [x] Crash on startup if SECRET_KEY env var is missing — RuntimeError raised
- [x] Verify: app refuses to start without SECRET_KEY — confirmed

### S0.2 — Global Auth Guard (F1 + C3)
- [ ] Move auth check from per-route `@auth_required` to `bp.before_request`
- [ ] Explicit allowlist: `/health`, `/static/*`, `/api/health`
- [ ] All other routes require Basic Auth
- [ ] Log auth failures to audit_trail
- [ ] Remove individual `@auth_required` decorators (now redundant)
- [ ] Verify: unauthenticated GET to `/api/qb/customer-health` returns 401

### S0.3 — Fix SQL Injection (C4)
- [ ] quickbooks_agent.py:407 — parameterize
- [ ] quickbooks_agent.py:465 — parameterize
- [ ] quickbooks_agent.py:885 — parameterize
- [ ] Audit all other f-string SQL across agents
- [ ] Verify: grep for f-string SQL returns 0 hits in execute() calls

### S0.4 — Verification
- [ ] Run existing tests
- [ ] Manual route spot-check
- [ ] No startup crashes with SECRET_KEY set
- [ ] Update this file with results

---

## SPRINT 1: FOUNDATION
### S1.1 — Delete Root-Level Duplicate Files (H2)
### S1.2 — Blueprint Refactor: Kill exec() (F2)
### S1.3 — SQLite-Only Data Layer (F3)
### S1.4 — Consolidate DB Access (H3)
### S1.5 — Verification

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
