# PRD-v31 Sprint Execution Plan
**Started:** 2026-02-26 · **Method:** Systematic, one sprint at a time

---

## SPRINT 0: Security (P0) — Fix before anything else

### S0.1 — Remove hardcoded SECRET_KEY fallback
- [ ] `app.py:35` — crash on startup if SECRET_KEY env var missing
- [ ] Verify: app refuses to boot without SECRET_KEY set

### S0.2 — Global auth guard via before_request
- [ ] Add auth check in `bp.before_request` in dashboard.py
- [ ] Allowlist: `/health`, `/static/*`, `/api/health`, `/login`, `/favicon.ico`
- [ ] All other routes require Basic Auth (existing check_auth logic)
- [ ] Verify: unauthenticated curl to `/api/qb/customer-health` returns 401
- [ ] Verify: authenticated curl to same endpoint returns 200

### S0.3 — Fix SQL injection vectors
- [ ] `src/agents/quickbooks_agent.py` — parameterize all f-string SQL
- [ ] `src/agents/notify_agent.py` — parameterize WHERE clause construction
- [ ] `src/agents/vendor_ordering_agent.py` — parameterize WHERE clause
- [ ] `src/agents/email_lifecycle.py` — parameterize DELETE IN clause
- [ ] Verify: grep for f-string SQL returns zero hits in those files

### S0.4 — Fix CSRF for state-changing endpoints
- [ ] Apply CSRF globally for POST/PUT/DELETE in before_request
- [ ] Exempt: API endpoints with Authorization header, AJAX same-origin, JSON content-type from same host
- [ ] Verify: POST without token from cross-origin returns 403

---

## SPRINT 1: Foundation — Clean architecture

### S1.1 — Delete root-level duplicate Python files
- [ ] Remove 10 root-level duplicates
- [ ] Update all try/except import chains to use src.* only
- [ ] Verify: app boots cleanly

### S1.2 — Blueprint refactor: kill exec()
- [ ] Each routes_*.py becomes properly importable
- [ ] Remove exec() loading
- [ ] Shared utilities → src/core/utils.py
- [ ] Verify: zero exec() calls, all routes respond

### S1.3 — SQLite-only data layer
- [ ] All JSON read/write for stateful data → db.py
- [ ] processed_emails → SQLite
- [ ] Verify: no data loss on restart

---

## SPRINT 2: Stability
### S2.1 — Centralized scheduler
### S2.2 — Automated database backups
### S2.3 — Email idempotency

## SPRINT 3: Intelligence
### S3.1 — Smart email classification v2
### S3.2 — Margin optimizer dashboard
### S3.3 — Unified FTS5 search

## SPRINT 4: Growth
### S4.1 — Order lifecycle
### S4.2 — Growth agent SCPRS pull + outreach
### S4.3 — Google Drive integration

## SPRINT 5: Operations
### S5.1 — Settings dashboard
### S5.2 — Migration framework
### S5.3 — Structured logging

---

## Review Section
(Updated after each sprint completion)
