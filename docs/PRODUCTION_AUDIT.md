# Reytech RFQ Dashboard — Full Production Audit Report
## Date: March 6, 2026

---

## SECTION 1: DATA ROUTE CATALOG

**Total routes: 724** (636 API + 88 pages)

| Category | Routes | Notes |
|----------|--------|-------|
| Price Check | 45 | Core workflow — PC detail, save, generate, lookup |
| Quotes | 24 | Lifecycle, pipeline, revisions |
| CRM / Contacts | 19 | CRUD, activity logging, agencies |
| Intelligence / SCPRS | 65 | Backfill, competitors, buyers, search |
| Orders | 39 | Full CRUD, tracking, attachments |
| Catalog | 37 | Products, pricing, suppliers, AI matching |
| Communications | 33 | Email, SMS, voice, templates |
| QA / Monitoring | 22 | Health checks, regression tracking |
| Growth | 65 | Prospects, outreach, campaigns |
| RFQ | 31 | Detail, files, generation |
| Admin / System | 39 | Backups, cleanup, settings |
| Other API | 272 | Legacy/utility endpoints |
| Pages | 33 | HTML page routes |

**Auth coverage: 715/724 (99.7%)**
9 intentionally public: health check, email tracking pixels, voice webhook, QuickBooks callback, v1 API (has internal auth)

---

## SECTION 2: FUNCTIONALITY CHECKS

| Check | Status | Detail |
|-------|--------|--------|
| SQL Injection vectors | ⚠️ WARN | 40 f-string queries (behind auth) |
| XSS via \|safe filter | ⚠️ WARN | 51 uses (many necessary for HTML rendering) |
| CSRF protection | ⚠️ WARN | No tokens (mitigated by Basic Auth) |
| Auth coverage | ✅ PASS | 99.7% coverage |
| Rate limiting | ✅ PASS | Global + per-IP |
| Quotes JSON↔DB sync | ✅ PASS | 9 quotes in sync |
| Empty tables | ⚠️ WARN | 46 empty (schema created, no data yet) |
| Bare except clauses | ✅ PASS | 8 remaining (improved from 74) |
| POST error handling | ⚠️ WARN | 153 POST routes without explicit try/except |
| Background threads | ⚠️ WARN | 17 threads (~136MB stack) |
| Database indexes | ✅ PASS | 111 indexes |
| SQLite WAL mode | ✅ PASS | WAL enabled |
| Database size | ✅ PASS | 1.7MB local |
| Template compilation | ✅ PASS | 51/51 clean |
| Jinja-in-JS injection | ✅ PASS | 0 remaining (was 97) |

**Score: 9 PASS, 6 WARN, 0 FAIL**

---

## SECTION 3: TEST CASES

| ID | Test | Input | Expected |
|----|------|-------|----------|
| TC-01 | Save with apostrophe | desc="O'Brien's gloves" | Saves without XSS |
| TC-02 | Save after deploy | JSON empty | Recovers from SQLite |
| TC-03 | Amazon preserves MFG# | Paste URL with existing MFG# | MFG# unchanged, ASIN in desc |
| TC-04 | Markup Apply All | Change 25→30% | All rows update |
| TC-05 | Buffer all costs | Click +15% | All costs buffered |
| TC-06 | Status on re-fill | Re-generate sent PC | Status stays "sent" |
| TC-07 | Overdue highlighting | PC past due date | Red border + indicator |
| TC-08 | Bulk archive | Select 3, archive | All 3 archived |
| TC-09 | SCPRS confidence | MFG# = "x-small" | Not 100% match |
| TC-10 | Source badge | Walmart URL | Badge says "Walmart" |
| TC-11 | Revision on save | Save prices | Previous state saved |
| TC-12 | Merge items | 3→1 via ⬆ button | Combined description |
| TC-13 | Unauthed access | GET /api/v1/rfqs | Internal auth check |
| TC-14 | VACUUM | /api/disk-cleanup?action=vacuum | DB shrinks |
| TC-15 | Buyers page | After SCPRS backfill | Buyers with emails |

---

## SECTION 4: RECOMMENDATIONS

### Critical
1. **Fix 40 f-string SQL queries** — Convert to parameterized. Files: db.py, product_catalog.py, scprs_universal_pull.py
2. **Consolidate 724→~300 routes** — Many legacy endpoints unused. Audit access logs and deprecate.

### High
3. **Reduce threads 17→8** — Merge duplicate schedulers
4. **Add CSRF tokens** — Defense-in-depth beyond Basic Auth
5. **Fix IMAP leak** — Reuse connections instead of creating fresh each poll

### Medium
6. **Add POST mutation logging** — Audit trail for data changes
7. **Standardize API versioning** — Currently mixed /api/ and /api/v1/
8. **Add OpenAPI spec** — Auto-generate from route decorators

### Low
9. **Convert bare except clauses** — Specify exception types
10. **Add health check dashboard** — Monitor thread count, DB size, queue lengths

---

## OVERALL GRADE: B+

### What's working well:
- 99.7% auth coverage
- Zero Jinja injection vectors
- WAL mode + automatic backups
- Rate limiting
- Comprehensive error handling on critical paths
- Smart Save workflow
- PC revision system
- Buyer outreach pipeline

### What needs attention:
- SQL injection vectors (behind auth but still risky)
- Thread count and memory pressure
- Route consolidation (too many legacy endpoints)
- IMAP connection management
