# Review — `/intel/scprs` (SCPRS Market Intelligence) — 2026-04-23

**Scope:** `@bp.route("/intel/scprs")` + its template, the engine(s) behind it, and all related API endpoints.
**Approach:** Same retrospective lens used for the full-app review. Code-level audit + SQL reproduction. Live Chrome verification blocked by Basic Auth; see §8.
**Verdict:** **One P0 live bug making the dashboard render empty for all users,** plus architectural debt that mirrors the app-wide patterns (module sprawl, parallel engines, silent-swallow, identity confusion, test-data bleed).

---

## 1. Surface area

| Concern | File | LOC |
|---|---|---|
| Page route | `src/api/modules/routes_intel.py:862-908` | ~50 |
| Template | `src/templates/scprs_intel.html` | 206 |
| "Universal" engine | `src/agents/scprs_universal_pull.py` | 660 |
| "Intelligence" engine | `src/agents/scprs_intelligence_engine.py` | 1,445 |
| Lookup / scraper | `src/agents/scprs_lookup.py` | 1,141 |
| Browser scraper | `src/agents/scprs_browser.py` | 1,005 |
| Public search | `src/agents/scprs_public_search.py` | 478 |
| Universal scanner | `src/agents/scprs_scanner.py` | 304 |
| Orchestrator | `src/agents/scprs_orchestrator.py` | 155 |
| Scraper client | `src/agents/scprs_scraper_client.py` | 156 |
| Connector | `src/agents/connectors/ca_scprs.py` | 230 |
| Scheduler | `src/core/scprs_schedule.py` | 355 |
| **Total SCPRS code** | | **~8,670** |
| SCPRS API routes | across `routes_intel.py`, `routes_intel_ops.py`, `routes_crm.py` | 15 endpoints |

That's ~8.7k LOC of SCPRS code across **12 files** feeding one dashboard.

---

## 2. 🔴 P0 LIVE BUG — SQL syntax error silently blanks the entire dashboard

### The bug

`src/agents/scprs_universal_pull.py:585` and `:597`:

```python
totals = conn.execute("""
    SELECT COUNT(DISTINCT p.po_number) as po_count,
           SUM(p.grand_total) as total_spend,
           ...
    FROM scprs_po_master p WHERE 1=1 " + where + "
""", agency_params).fetchone()
```

The `" + where + "` is **inside a single triple-quoted Python string, not a concatenation.** The intent was `""" + where + """` (terminate, concat, restart). As written, `" + where + "` is literal text sent to SQLite, which raises `sqlite3.OperationalError: near "...": syntax error` every time.

### Reproduced locally

```
python -c "import sqlite3; c=sqlite3.connect(':memory:');
c.execute('CREATE TABLE scprs_po_master (po_number TEXT, agency_code TEXT)');
c.execute('SELECT COUNT(*) FROM scprs_po_master WHERE 1=1 \" + where + \"').fetchone()"
→ ERR: OperationalError near "" + where + "": syntax error
```

### Why nobody noticed

`routes_intel.py:867-874`:

```python
try:
    from src.agents.scprs_universal_pull import get_universal_intelligence, get_pull_status
    intel = get_universal_intelligence()
    status = get_pull_status()
except Exception as e:
    intel = {"summary": {}, "gap_items": [], "win_back": [], "by_agency": [], ...}
    status = {...}
```

Broad `except Exception` swallows the SQL error and substitutes empty defaults. The page renders with zeros and "Pull data →" placeholders in every table. This is the exact failure-shape covered in `feedback_production_ready_definition.md`: compile passes, HTTP 200, but **display ≠ persisted.**

### Blame trail

`git blame` shows the break landed in commit **`8fe34398f` — "Fix ALL audit findings: 0 SQL injection, 0 bare excepts, 0 IMAP leak" on 2026-03-07.** The parameterization refactor left behind placeholder concat text. **~7 weeks of broken dashboard.** The irony — a "fix all audit findings" sweep introduced the bug that the bare-except rule would have caught if the dashboard didn't have its own bare-except at the page level.

### Fix

```python
# Replace both sites:
totals = conn.execute(f"""
    SELECT COUNT(DISTINCT p.po_number) as po_count, ...
    FROM scprs_po_master p WHERE 1=1 {where}
""", agency_params).fetchone()
```

Safe because `where` is a hardcoded constant (`"AND p.agency_code=?" | ""`), not user input. Or use actual string concat:

```python
SQL = ("SELECT ... FROM scprs_po_master p WHERE 1=1 " + where)
totals = conn.execute(SQL, agency_params).fetchone()
```

### Regression guard

Add `tests/test_scprs_universal_pull.py`:
```python
def test_get_universal_intelligence_returns_non_empty_shape(tmp_db_with_scprs_fixture):
    r = get_universal_intelligence()
    assert "totals" in r and "by_agency" in r and "gap_items" in r
    # If SQL is broken, function raises → test fails before even asserting shape
```

And a general rule — add to the CI lint: `grep -Pn '""" *\+ *\w+ *\+ *"""' src/` and `grep -Pn '" \+ \w+ \+ "' src/**/*.py` to catch this broken pattern class.

---

## 3. 🟠 P1 — Architectural

### 3a. Two "engines" for the same dashboard

Both `scprs_universal_pull.py` (660 LOC, "ONE engine that does everything") and `scprs_intelligence_engine.py` (1,445 LOC) exist. The page route uses `universal_pull`. Other SCPRS API routes (`routes_intel.py:62, 76, 121, 340, 364, 454, 472`) import from `scprs_intelligence_engine`. Two overlapping code paths that read/write overlapping tables, with no clear seam between them. This is the **parallel-write-path** failure shape from the app-wide review — same category as Orders V2 and the PC/RFQ fork.

**Pick one, deprecate the other.** If the universal pull is the canonical engine, move `backfill_historical`, `get_engine_status`, `run_po_award_monitor`, `get_competitor_intelligence`, `search_scprs_data` into it (or into a `scprs/` package split by concern), and delete `scprs_intelligence_engine.py`. Every day both live, the next audit finds another divergence.

### 3b. 15 SCPRS API endpoints across 3 route files

```
routes_intel.py:      /api/intel/scprs/pull-all, /engine-status, /test-connection,
                      /po-monitor, /scprs-health, /backfill, /test-pull, /scprs-search,
                      /pull, /status, /intelligence, /close-lost
routes_intel_ops.py:  /api/intel/scprs-test
routes_crm.py:        /api/intel/scprs/test, /scprs/pull-now
```

Pull (3 variants), test (3 variants), status (2). Nobody can answer "which endpoint is canonical?" without reading all three files. This is the `src/agents/` grab-bag problem on a smaller scale — flagged in the full-app review (Part A §5 module sprawl) and still growing here.

**Action:** collapse to one router (`src/api/modules/routes_scprs.py`) with a clear surface: `pull`, `status`, `intelligence`, `search`, `close-lost`, `backfill`. Delete everything else.

### 3c. `cchcs_supplier_map` table — named after one agency, stores all agencies

`scprs_universal_pull.py:183-190, 501-528` defines and writes `cchcs_supplier_map` despite storing suppliers across all 15 tracked agencies. Exact shape of `feedback_canonical_not_verbatim.md` — identity naming that contradicts the data. Any new engineer reading this in 6 months will misuse it.

**Action:** rename to `scprs_supplier_map` via migration. Keep a view `cchcs_supplier_map` pointing at the new table for a release.

### 3d. Idempotency lie in a comment

Line 428: `# Store line items — idempotent via UNIQUE(po_id, line_num).`
Schema (lines 167-176): **no UNIQUE constraint on `(po_id, line_num)`.** `INSERT OR REPLACE` without a uniqueness target behaves like `INSERT` — duplicates accumulate on every re-pull of the same PO. Over time, `scprs_po_lines` bloats and all aggregates (gap, win-back, by-agency totals) inflate.

**Action:** add `UNIQUE(po_id, line_num)` in a migration, then dedup existing rows. Also add `UNIQUE(po_number)` in a non-autoincrement way — it already exists on `scprs_po_master`, good.

### 3e. `is_test` column missing on SCPRS tables

`scprs_po_master` and `scprs_po_lines` have no `is_test` column. But `check_quotes_against_scprs:222` filters quotes by `is_test = 0` — meaning test SCPRS data could still feed into real-quote close-lost decisions. Same shape as the CR-5 / AN-P0 bugs from the 2026-04-22 audit batch. Test SCPRS data can pollute real price_history too (lines 452-460 insert unconditionally).

**Action:** add `is_test INTEGER DEFAULT 0 NOT NULL` to both tables. Filter everywhere. Add to the CI check you're building for `is_test` on every table.

### 3f. Process-local pull status — broken under multi-worker gunicorn

Lines 321-322:
```python
_pull_lock = threading.Lock()  # declared but never acquired anywhere
_pull_status = {"running": False, "progress": "", "last_result": None}
```

`_pull_status` is module-global state. Under gunicorn with N workers, worker A's `/status` endpoint cannot see the running pull in worker B. Symptoms: polling shows `running=False` while a pull is still grinding in a sibling worker; UI reloads at false "done"; user hits "Pull P0 Now" a second time → two concurrent SCPRS sessions hammer `fiscal.ca.gov`.

`_pull_lock` is declared but never acquired — actually dead code.

**Action:** move pull status to the database (`scprs_pull_status` table or columns on `scprs_pull_log`). Acquire an advisory SQLite lock (or a DB flag) before starting a pull; refuse if held.

### 3g. `_upsert_supplier` crashes on totals for re-found POs

Lines 422-427:
```python
if supplier:
    _upsert_supplier(conn, supplier, po.get("supplier_id",""),
                     po.get("grand_total", 0), category, ...)
```

Called **only in the `if not exists:` branch** — good for idempotency of the master row. But `_upsert_supplier` itself always increments `total_po_value` and `po_count` by 1 regardless. So supplier totals are correct *the first time* a PO is seen and stable after — good. But if the same PO has multiple line_items in multiple categories, `_upsert_supplier` isn't called per line — it's called once per PO with a single `category`, dropping the other categories. Category breadth is under-reported per supplier.

**Action:** move `_upsert_supplier` outside the `if not exists` block? No — then totals inflate. Instead, track all categories of a PO's lines and pass the set to `_upsert_supplier`. Or restructure: supplier map is a projection — rebuild it from `scprs_po_lines` on demand, not incrementally.

### 3h. SCPRS feeds `price_history` — CLAUDE.md violation risk

Lines 452-460 insert into `price_history` with `source="scprs_market"` whenever Reytech sells the item. CLAUDE.md §Pricing Guard Rails is explicit: **"SCPRS Prices Are NOT Supplier Costs."** The insert itself is tagged with a source, but unless every downstream consumer of `price_history` filters by `source`, these will leak into cost-basis calculations. `feedback_scprs_prices.md` and the CP-2 PR #376 / #416 per-unit work exists precisely because this keeps going wrong.

**Action:** audit every `SELECT ... FROM price_history` call site and assert `source` discrimination. If a caller is doing "give me the best cost basis for item X," `source='scprs_market'` must be filtered OUT, not averaged in.

---

## 4. 🟡 P2 — SQL and data shape

### 4a. Fragile supplier exclusion

`check_quotes_against_scprs:258-259`:
```sql
AND p.supplier NOT LIKE '%Reytech%'
AND p.supplier NOT LIKE '%Rey Tech%'
```

Misses `Reytech, Inc.`, `REYTECH INCORPORATED`, `Rey-Tech`, typos. The state procurement site does have supplier_id as a canonical key — use `supplier_id != <REYTECH_SUPPLIER_ID>` instead, fall back to LIKE only if the id is missing.

### 4b. String-concat LIKE in close-lost

`check_quotes_against_scprs:260-271` builds a dynamic OR-chain of `LOWER(l.description) LIKE ?` terms from `items_text.split(" | ")[:3]`. Only the first 3 items are checked — quotes with >3 items silently miss auto-close detection. Long items_text strings get truncated with no log. Also, `[:3]` + `len(term) > 4` filter drops common short MFG#s.

**Action:** loop over ALL items (with a sane cap of 50 or so and a log for overflow), and use a real full-text-search index (FTS5) instead of LIKE chains.

### 4c. Agency resolution is substring match on free-form text

`_dept_name_to_agency:146-151` does `dept_name.upper()` substring match against split names. Will miss `Dept of Corrections and Rehabilitation` → CDCR, `California Correctional Health Care Services` → CCHCS because the matcher splits the configured name on `" / "` and expects a part to appear verbatim in the SCPRS string.

**Action:** build a resolver table (`scprs_agency_aliases`) with canonical code + known aliases, populated from observed `dept_name` values. Same pattern `institution_resolver.py` uses. This is the canonical-identity-at-ingest rule.

### 4d. Description dedup by `LOWER(description)` in gap/win-back

`get_universal_intelligence:612, 626`:
```sql
GROUP BY LOWER(l.description)
```

Same item with trailing space, different casing, or trivial typo = separate gap item → inflated gap count. Once data scale grows this fragments the top-N tables.

**Action:** normalize descriptions at ingest (collapse whitespace, strip non-printables, canonicalize `&`/`and`), store `description_normalized`, GROUP BY that.

### 4e. Search terms hardcoded in a 40-entry tuple

Lines 75-120 define product search terms inline. Adding/removing a term = code change + deploy. The CLAUDE.md rule "Catalog = bible" suggests this should live in the catalog (or a `scprs_search_terms` table) with categories, priority, and an active flag.

### 4f. `time.sleep(1.2)` between term searches

40 terms × 1.2s = 48s of idle. No adaptive backoff. If fiscal.ca.gov throttles (429), no response-aware slowdown. If it's fast, wasted wall-clock. **Action:** respect `Retry-After` and use exponential backoff; cap concurrent requests to 1 but remove fixed sleep.

### 4g. Date-window uses naive local datetime

Line 361: `from_date = (datetime.now() - timedelta(days=365)).strftime("%m/%d/%Y")` — `datetime.now()` is naive. Server runs UTC (Railway). CLAUDE.md rule: always PST. This date determines what gets pulled; off-by-hours near midnight. Minor, but drifts over TZ boundaries.

---

## 5. 🟡 P2 — UI / Template

### 5a. Broken aria-label mid-edit

`scprs_intel.html:19`: `aria-label="Running if running els"` — placeholder text shipped. Screen readers announce garbage.

### 5b. `alert()` for Close-Lost result

Line 193: `alert(d.auto_closed+' quotes auto-closed lost')` — terminal UX. If Mike presses Enter twice after a pull, two alerts stack; if another worker auto-closed more quotes in parallel, he gets a blocking dialog. Use a toast/status banner.

### 5c. `location.reload()` on poll completion

Line 200: `if(d.pos_stored > 0) { location.reload(); }` — destroys any scroll position, loses any state the user had (filter, scroll to bottom of 40-row table). Re-render the sections via `fetch('/api/intel/scprs/intelligence')` and replace the table bodies.

### 5d. No error surface when a pull fails

`_pull_status["last_result"]["error"]` is populated (line 543) but the template never reads it. A failed pull (session timeout, fiscal.ca.gov down) looks identical to a successful pull from the UI. The `status.get("progress")` banner might say "Done — 0 new POs" and look healthy.

**Action:** surface `status.last_result.error` as a red banner. Show last successful pull datetime vs last attempt datetime.

### 5e. No freshness banner on `/intel/scprs`

PR #237 added "freshness banner" to some SCPRS surfaces. The main dashboard has no "data is N hours old" indicator. `status.last_pull.pulled_at` is available but unused in the template.

### 5f. Null-through-default gotcha

Pattern across template: `item.get("description","")[:50]`. Safe if key is missing; **unsafe if the key exists with value `None`** — then `None[:50]` raises `TypeError`. SQLite can return NULL for these fields. Jinja2 will `str(None)` if we do `|string` but `[:50]` is a slice on the raw Python object. Should be `(item.get("description") or "")[:50]` or a Jinja filter.

### 5g. Polling timer never stops on page hidden

`setTimeout(pollStatus, 6000)` runs forever while a pull is active, even in a background tab. Use `Page Visibility API` to pause when hidden; use `EventSource` / SSE for a real-time stream instead of 6s polling.

### 5h. `no_data` detection uses `pos == 0` only

Line 890: `no_data = pos == 0`. Doesn't distinguish "never pulled" from "pulled but SQL broken" — both show the "Pull P0 Now to start" blue banner, misleading the user in exactly the P0-bug case above. Distinguish "no pull log entries" vs "POs stored = 0 but pulls succeeded."

---

## 6. 🟡 P2 — Process / Observability

### 6a. No test for the dashboard-level happy path

`tests/test_scprs_*.py` — the memory index references the Apr-19 SCPRS audit closure (PR #237) but I see no integration test that loads `/intel/scprs`, seeds a minimal SCPRS fixture, and asserts that `gap_items|length > 0` in the rendered HTML. That single test would have caught the P0 on day one.

### 6b. No `/health/scprs` route

The pattern is `/health/quoting`, `/health/oracle`, etc. per the audit history. No `/health/scprs` that reports: last pull time, rows pulled, rows stored, last error, staleness threshold, fiscal.ca.gov reachability. Add it — it's the observability rule from the app-wide review Part B §3.2.

### 6c. Scheduled pull failure not alerted

`core/scprs_schedule.py` (355 LOC, not fully read but referenced) runs the Monday+Wednesday 7am PST pull. If that pull fails (SQL error, session break, network), there's no alert path. The user finds out when the dashboard has stale numbers days later.

### 6d. No rate limit on `/api/intel/scprs/pull`

Anyone authenticated can POST-spam the pull endpoint. `_pull_status` check is best-effort (see 3f multi-worker issue). A bored tab left open re-triggering via a JS bug could overload fiscal.ca.gov from Reytech's IP and get them blocked. Memory: SY-3 VACUUM rate-limit work from 2026-04-22 established the rate-limit + single-flight pattern — apply the same here.

### 6e. No tracking of per-term success rate

`scprs_pull_log` captures counts per run. But there's no aggregate for "nitrile gloves has returned zero hits for 4 pulls in a row" — a signal the scraper is broken for that term but fine for others. Add a `/health/scprs/terms` view.

### 6f. Module layout

11 SCPRS-related files in `src/agents/` + 1 in `src/agents/connectors/` + 1 in `src/core/`. Promote to `src/scprs/` package with sub-modules: `scrape/` (session, browser, public_search), `ingest/` (universal_pull, orchestrator), `intelligence/` (gaps, win-back, supplier map), `schedule.py`. Collapses the grab-bag.

---

## 7. Enhancement backlog — prioritized

### P0 — ship today

1. **Fix `get_universal_intelligence` SQL** (§2). One-line change per site; pairs with a regression test asserting the function returns non-empty shape for seeded data. Without this the entire dashboard is cosmetic.
2. **Remove the bare `except Exception` from `page_intel_scprs`** or at least log + surface `error` to the template. Silent swallow masked this for 7 weeks.

### P1 — ship this week

3. **Unify the two engines** (§3a). Pick `universal_pull` or `intelligence_engine`, migrate callers, delete the other.
4. **Collapse 15 endpoints to 6** in a new `routes_scprs.py` (§3b).
5. **Add `UNIQUE(po_id, line_num)` + dedup existing rows** (§3d). Backfill required.
6. **Add `is_test` to `scprs_po_master` / `scprs_po_lines`** and filter everywhere (§3e).
7. **Move pull status to DB** + advisory lock so multi-worker status is correct and pulls can't overlap (§3f).
8. **Audit `price_history` consumers** for `source='scprs_market'` filter discipline (§3h).
9. **Golden-path test**: `test_intel_scprs_page_renders_with_seeded_data.py`. Seeds 3 POs, 10 line items, loads `/intel/scprs`, asserts the 5 KPI cards populate and at least one row shows in each of the 3 tables.
10. **Add `/health/scprs`** with last_pull, rows, error, staleness, reachability.

### P2 — backlog

11. Rename `cchcs_supplier_map` → `scprs_supplier_map` (migration).
12. Move search-terms to a table or the catalog (§4e).
13. Agency resolver table with aliases (§4c).
14. FTS5 on `scprs_po_lines.description` + normalized-description column (§4d, §4b).
15. Adaptive backoff for scraper; respect `Retry-After` (§4f).
16. PST-consistent date handling (§4g).
17. Replace `alert()` and `location.reload()` in template; SSE/visibility-aware polling (§5b, §5c, §5g).
18. Surface pull errors + freshness banner (§5d, §5e).
19. Rate-limit `/api/intel/scprs/pull` (§6d).
20. Promote SCPRS files to `src/scprs/` package (§6f).
21. Fix aria-label placeholder (§5a).
22. Null-safe Jinja filters for description/status_notes (§5f).

---

## 8. Live verification (2026-04-23, after Mike authenticated Chrome)

### Page render
- Title `SCPRS Intelligence — Reytech` loads.
- Blue banner displayed: **"No data yet — click Pull P0 Now to start"** (`no_data_banner: true` in DOM).
- All three tables (Win-Back, Gap Items, By Agency) show one row with `Pull data →` placeholder.
- Console: clean, zero errors. Failure is server-side and silently substituted.
- Screenshot saved at `docs/screenshots/intel_scprs_2026_04_23_live.png`.

### API smoking gun

```
GET /api/intel/scprs/intelligence
→ {"ok": false, "error": "near \"\" + where + \"\": syntax error"}

GET /api/intel/scprs/status
→ {"ok": true, "pos_stored": 36367, "lines_stored": 109709,
   "agencies_seen": 135, "last_pull": null, "running": false}

GET /api/intel/scprs/engine-status   ← uses the OTHER engine (scprs_intelligence_engine.py)
→ {"ok": true, "total_line_items": 109709, "total_gap_items": 14,
   "quotes_auto_closed": 7, "by_agency": [
       {"agency_key": null,    "pos": 35501},
       {"agency_key": "",      "pos": 455},
       {"agency_key": "CCHCS", "pos": 411}]}
```

### What this proves

1. **The P0 SQL bug is live.** The API literally returns the syntax error string — no inference required.
2. **The DB has 36,367 POs and 109,709 line items already pulled.** The data exists. The dashboard simply cannot read it because the `WHERE 1=1 " + where + "` SQL fails on every call.
3. **The other engine works.** `engine-status` returns real numbers from `scprs_intelligence_engine.py`. This *confirms* §3a — two engines compute overlapping intelligence, and the one wired to the page is the broken one. Mike has had working SCPRS intel pulling for 7 weeks; he just hasn't seen any of it.
4. **`last_pull: null` despite 36k POs** = `scprs_pull_log` is empty. Engine #2 doesn't write the pull log. Engine #1 (broken read path) is the only writer. So even if §2 is fixed, the freshness banner (§5e) will show "never pulled" until a manual pull runs.
5. **`agencies_seen: 135` vs `ALL_AGENCIES` whitelist of 15** = engine #2 captured data for 9× more dept_codes than universal_pull tracks. `_dept_name_to_agency` returns `None` for most, so `agency_code` is just the raw dept_code. The whitelist is not a real constraint — it's a label.
6. **`quotes_auto_closed: 7`** — the engine has been auto-closing real quotes for weeks. Whether those closes are correct is a separate question (see §3a, §4a, §4b risks); they're certainly happening without UI visibility.

### Headline

**The most important business intelligence dashboard in the app has been showing "No data yet" for ~7 weeks while the database silently accumulated $108k+ line items and auto-closed 7 real quotes.** This is the canonical "display ≠ persisted ≠ delivered" failure (`feedback_production_ready_definition.md`), masked by a bare `except Exception` (CLAUDE.md anti-pattern), introduced by a refactor that claimed to "fix all SQL injection findings" without integration-testing the result. Every architectural pattern called out in the new-project guidance doc applies to this single page.

Fix priority: §2 P0 today, then §3a engine unification, then §6a integration test that would have caught this in CI.

---

## Cross-reference to the app-wide review

This module exhibits **every** pattern I flagged in Part A of `C:\Users\mikeg\NEW_PROJECT_CLAUDE_GUIDANCE.md`:

| Pattern | Evidence here |
|---|---|
| A1. Free-form identity | `cchcs_supplier_map` name; `_dept_name_to_agency` substring matching |
| A2. Parallel write paths | Two engines (`universal_pull` vs `intelligence_engine`) |
| A3. Broad-except as architecture | `page_intel_scprs:871` hid the P0 for 7 weeks |
| A4. `is_test` retrofit | SCPRS tables missing `is_test` entirely |
| A5. Module sprawl | 12 files, 15 endpoints, 8.7k LOC for one dashboard |
| A6. Display ≠ persisted | HTTP 200 + empty tables = the exact shape |
| A7. Deploy footgun | None on this module specifically |
| A8. Mocked/absent tests | No golden test catching the P0 |
| A9. Output-layer patches | The "Fix ALL SQL injection" commit patched strings without integration-testing |
| A10. Claude collab bugs | A parameterization refactor broke working SQL and merged anyway |

The pattern count alone justifies promoting SCPRS into its own package with first-class tests before adding another feature to it.
