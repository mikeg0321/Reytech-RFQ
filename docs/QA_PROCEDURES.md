# Reytech RFQ — QA Procedures

## Overview

This document defines the Quality Assurance procedures for the Reytech RFQ Dashboard.
All procedures should be executed before production deployments and periodically during operation.

---

## 1. Automated Test Suite

### Quick Smoke Tests (< 30 sec)
```bash
make test-quick   # or: SECRET_KEY=test python -m pytest tests/test_sprints.py -v
```
**Pass criteria:** 21/21 tests pass (auth, scheduling, search, classification, margins, orders, revenue, prospects, system health, migrations, integrity, preflight, routes, PDF versions)

### Full Test Suite (< 60 sec)
```bash
make test         # or: SECRET_KEY=test python -m pytest tests/ -v
```
**Pass criteria:** 39/39 tests pass (smoke + critical paths + margin optimizer + duplicate detection)

### Pre-Deploy Check (< 15 sec)
```bash
make check        # or: SECRET_KEY=test python tests/pre_deploy_check.py
```
**Validates:**
- All Python files compile (syntax check)
- Core module imports succeed
- Dashboard blueprint loads
- Jinja2 templates compile
- No duplicate endpoint function names
- Route decorators use @bp.route (not @app.route)
- render_page() variables exist in scope

---

## 2. Manual QA Checklist

### 2.1 Authentication
- [ ] GET / without credentials → 401 "Login Required"
- [ ] GET / with correct credentials → 200 dashboard
- [ ] GET /health without credentials → 200 (public endpoint)
- [ ] POST /api/settings with wrong credentials → 401
- [ ] 60+ rapid auth failures → 429 rate limited

### 2.2 Core Pipeline
- [ ] Upload PDF price check → parsed and items extracted
- [ ] Price check items → auto-priced from Amazon/SCPRS/catalog
- [ ] Generate quote from price check → PDF created with correct layout
- [ ] Quote shows in /quotes list with correct total
- [ ] Quote status transitions: draft → sent → won/lost
- [ ] Won quote → order created via /api/orders/{id}/transition
- [ ] Order lifecycle: new → confirmed → shipped → delivered → invoiced

### 2.3 API Endpoints (hit each, verify JSON response)
```
GET  /api/system/health        → {status, checks: {database, scheduler, backups}}
GET  /api/system/preflight     → {status, checks: {database, schema, integrity, routes}}
GET  /api/system/qa            → {sections: {database, integrity, pipeline, schema, routes, pdf_templates}}
GET  /api/system/integrity     → {ok, total_checks, passed, failed, checks[]}
GET  /api/system/trace/{id}    → {pipeline, timeline, stages_found}
GET  /api/system/pipeline      → {rfqs, price_checks, quotes, orders, conversion}
GET  /api/system/routes        → {total, api_routes[], page_routes[]}
GET  /api/system/migrations    → {current_version, up_to_date, applied[]}
GET  /api/system/pdf-versions  → {templates, registry}
GET  /api/scheduler/status     → {jobs, next_runs}
GET  /api/admin/backups        → {backups[], latest}
GET  /api/search?q=test        → {results[]}
GET  /api/email/review-queue   → {queue[]}
GET  /api/margins/summary      → {categories[], alerts[]}
GET  /api/margins/item?desc=X  → {pricing}
GET  /api/revenue/ytd          → {total, monthly[]}
GET  /api/orders/unpaid?days=30 → {invoices[]}
GET  /api/growth/prospects      → {prospects[]}
POST /api/email/classify-test   → {classification, confidence}
```

### 2.4 Data Tracing
- [ ] `GET /api/system/trace/R26Q14` → traces quote through pipeline
- [ ] Pipeline shows: source RFQ → quote → order → revenue (where applicable)
- [ ] Timeline events are chronologically ordered
- [ ] `GET /api/system/pipeline` → shows totals and conversion rates

### 2.5 Database Health
- [ ] All expected tables exist (quotes, contacts, orders, rfqs, revenue_log, etc.)
- [ ] No orphaned order→quote references
- [ ] No duplicate quote numbers
- [ ] Schema migrations up to date (version 6)
- [ ] PDF generation log records template versions

---

## 3. Production Monitoring

### 3.1 Health Check (run every 5 minutes)
```
GET /health → 200
GET /api/system/health → {status: "ok"}
```

### 3.2 QA Dashboard (run daily)
```
GET /api/system/qa → check all sections are ok
```

### 3.3 Integrity Sweep (run weekly)
```
GET /api/system/integrity → all checks pass
```

### 3.4 Key Metrics to Watch
- Route count > 600 (module load failures drop this)
- Database table count stable
- No stale quotes older than 90 days piling up
- Revenue log matches order totals (within tolerance)
- Scheduler heartbeats updating (no stuck jobs)

---

## 4. Deployment Procedure

### Pre-Deploy
1. `make check` — all pre-deploy checks pass
2. `make test` — 39/39 tests pass
3. `make lint` — all files compile clean
4. Review `git diff` — no unintended changes

### Deploy
```bash
git add -A
git commit -m "descriptive message"
git push origin main
```
Railway auto-deploys from main branch.

### Post-Deploy Verify
1. Hit `/health` — 200 OK
2. Hit `/api/system/preflight` — all checks pass
3. Hit `/api/system/qa` — full QA dashboard green
4. Spot-check a known quote trace: `/api/system/trace/{known_quote}`

---

## 5. Regression Prevention

### What Breaks Most Often
1. **Auth failures in tests** — conftest must patch both `dashboard.*` and `shared.*`
2. **Module load failures** — exec'd modules depend on dashboard globals, new imports can fail
3. **Connection leaks** — any new `get_db()` call in agents must use context manager
4. **Cross-module globals** — INTEL_AVAILABLE etc. must be pre-defined before route module loading

### Safeguards in Place
- Global auth guard (before_request) on all routes
- CSRF origin check on POST/PUT/DELETE
- Rate limiting (60 auth attempts/min, 300 requests/min)
- Input validation framework (validators.py)
- Error handler with logging (error_handler.py)
- Connection timeout (30s) on all SQLite connections
- Structured logging for all requests (route, method, status, duration)
- Schema migrations auto-applied on startup
- Pre-deploy check catches syntax, import, and template errors

---

## 6. Test File Reference

| File | Tests | Coverage |
|------|-------|----------|
| tests/test_sprints.py | 21 | Auth, scheduler, backups, search, classifier, margins, orders, revenue, prospects, health, migrations, integrity, preflight, routes, PDF versions |
| tests/test_critical_paths.py | 18 | Auth, API v1, bulk actions, settings, margin optimizer algorithms, duplicate detection |
| tests/conftest.py | — | Shared fixtures: app, auth_client, anon_client, temp_data_dir, seed helpers |
| tests/pre_deploy_check.py | 8 | Syntax, imports, blueprint, templates, globals, routes, endpoints |

**Total automated checks: 47** (39 pytest + 8 pre-deploy)
