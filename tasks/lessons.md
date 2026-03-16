# MANDATORY: READ tasks/CC_IMPLEMENTATION_RULES.md BEFORE EVERY TASK
# That file contains hardcoded rules for wiring, testing, data paths,
# and verification that MUST be followed. No exceptions.
# Failure to follow those rules has caused repeated production failures.

# Lessons Learned — Reytech RFQ

## Session 2026-02-21

### L1: JSON file writes in threaded code cause data loss
**Pattern**: Background threads (auto_price_pipeline) doing read-modify-write on price_checks.json 
overwrite PCs created by the email poller thread between the read and write.
**Rule**: Always use file locking (fcntl) on shared JSON files. For single-record updates, 
use atomic merge-save (_merge_save_pc) instead of full-file overwrite.

### L2: Pipeline metrics must reflect actual workflow, not just one data source
**Pattern**: Funnel only counted rfqs.json — ignored price_checks.json which is 80% of work.
Labels like "Pending" and "Sent" were meaningless without context.
**Rule**: When building dashboards, trace every metric back to its data source. 
Ask "does this number match what the user sees on screen?" If the PC table shows 6 items 
but pipeline says 0, something is disconnected.

### L3: QuickBooks hierarchical names need parsing, not truncation
**Pattern**: `name[:50]` cut off "CA Correctional Health Care Services:Pelican Bay State Prison".
The real fix is parsing `Parent:Child` into just the child name.
**Rule**: Never truncate display data. Parse it properly. If the source format is known, 
write a parser. Also filter out person-name entries (Lorelei) that look like facilities.

### L4: "+Target" button that creates a dead-end artifact is worse than nothing
**Pattern**: Old +Target created a price check with hardcoded items and no email/outreach.
It looked like it did something but had zero path to revenue.
**Rule**: Every user action should move toward a business outcome. If "Target" means 
"I want to sell to this facility", the action should draft an outreach email AND create a PC.

### L5: Header with 17+ nav items in a single flex row will always overflow
**Pattern**: Icons clipped off the left edge, wrapping looked cluttered.
**Rule**: When nav items exceed ~8, use a two-row layout: logo+actions on row 1, 
scrollable nav on row 2. Add overflow-x:auto with thin scrollbar.

### L6: Always add diagnostic endpoints when debugging persistence
**Pattern**: PC data vanishing on deploy with no way to check what's on the volume.
**Rule**: Add /api/debug/<resource> endpoints and BOOT log lines that confirm 
data survived deploy. "BOOT PC CHECK: N price checks in /path (size=X bytes)"

### L7: Never put dict comprehensions inside Python f-strings
**Pattern**: `{_json.dumps([{{"name": f["raw_name"]}} for f in list])}` crashes with 
`TypeError: unhashable type 'dict'`. The `{{}}` escaping conflicts with dict literals.
**Rule**: Pre-compute any JSON containing dicts BEFORE the f-string. Assign to a 
variable, then reference the variable: `{_precomputed_json}`.

## L8: Rate limiter in tests
- Flask security middleware rate limiter is a module-level dict
- It accumulates across pytest tests, causing 429s after first few tests
- Fix: clear `dashboard._rate_limiter` in conftest fixture after create_app()

## L9: price_checks.json must be {} not []
- _load_price_checks() returns dict {pcid: pc_data}
- Writing [] causes AttributeError: 'list' object has no attribute 'items'
- award_monitor.py, funnel stats, and PC routes all call .items() on it

## L10: Pre-compute HTML in f-strings for complex templates
- Nested f-strings with conditionals and list comprehensions are error-prone
- Pattern: build HTML chunks as variables BEFORE the main f-string
- Avoids quote escaping hell and compile errors

## L11: Jinja2 auto-escaping breaks inline HTML in {{ }}
**Pattern**: `{{ "<button onclick='fn()'>Click</button>" if cond else "" }}` renders
as escaped text (`&lt;button...`) because Jinja2 auto-escapes all `{{ }}` output.
**Rule**: Never put HTML strings in `{{ }}`. Use `{% if %}` blocks instead:
`{% if cond %}<button onclick="fn()">Click</button>{% endif %}`
Or use `{{ html_var | safe }}` for Python-built HTML passed to template.

## L12: Route modules loaded via exec() can't be imported
**Pattern**: `from src.api.modules.routes_crm import _load_customers` fails with
`NameError: name 'bp' is not defined` because route modules are exec'd into
dashboard.py's globals, not loaded as importable Python modules.
**Rule**: Within route modules, just call shared functions directly (e.g. `_load_customers()`)
— they're already in the global scope at request time. Never use `from src.api.modules...`.

## L13: Client-side JS fetches are unreliable on Railway
**Pattern**: fetch() calls to own API endpoints time out on Railway cold starts
(8s timeout, retry+1500ms backoff still not enough). CRM card shows "Loading..." forever.
**Rule**: For data needed at page load, compute it server-side in the route handler and
pass it as template context. Use `{{ data_json | safe }}` in template JS. Eliminates
network roundtrips, auth issues, and cold-start race conditions.

## L14: Systematic |safe audit for ALL templates
**Pattern**: Any `{{ var_html }}` without `| safe` renders as escaped text.
This bug appeared on 8+ pages because HTML was built in Python route handlers
and passed to Jinja2 templates without `| safe`.
**Rule**: After any template change, run: `grep -rn '{{ [a-z_]*_html }}' src/templates/ | grep -v safe`
Also audit: `_rows`, `_section`, `_box`, `_panel` suffix variables.
For inline HTML conditionals, NEVER use `{{ "<html>" if cond else "" }}` — always
use `{% if cond %}<html>{% endif %}` instead.

## L15: _header()/_page_footer()/_wrap_page() were removed
**Pattern**: Legacy pages built full HTML in Python and used `_header(title)`
and `_page_footer()` to wrap with nav/footer. These were removed when
render_page() was introduced but ~8 routes still called them → 500 errors.
**Rule**: Use `render_page("generic.html", page_title=title, content=html_str)`
for pages that build HTML in Python. The `_wrap_page` shim in dashboard.py
delegates to this. Always test every page after refactoring rendering.

## L16: QB product names are part numbers, not descriptions
**Pattern**: QB "Product/Service Name" field contains part numbers like "00300504-6".
The actual product name is in the "Sales Description" field (multiline).
**Rule**: When importing QB data, use `_make_product_name(description)` as the product name,
store the QB name as `mfg_number`. Handle name uniqueness collisions (Foley Catheter in
multiple sizes = same description). Append [part#] on collision.

## L17: Auto-fix hooks need idempotency guards
**Pattern**: Sprint 1 fixes (name fixing, pricing) should run on deploy but not on every restart.
**Rule**: Use a measurable signal (e.g., "count products with NULL recommended_price > 50")
to decide if fixes need to run. Avoids re-processing and keeps startup fast.

## L18: Shared QB emails are NOT facility-specific contacts
**Pattern**: QuickBooks customer records often have one email (e.g., timothy.anderson@cdcr.ca.gov) 
copied across 33 facilities. That person is a central billing contact, NOT a buyer at each facility.
**Rule**: Count emails across facilities. If email appears on 3+ facilities → tag as "CENTRAL" and 
show separately. Never display as a facility-specific buyer. Real buyer contacts come from:
1. SCPRS PO data (buyer_name/buyer_email per PO)
2. Price Check requestor fields
3. CRM manual entries
The hierarchy: SCPRS Buyer > PC Requestor > CRM > QB Billing > Central/Shared

## L19: Use Anthropic API + web_search as universal price finder
**Pattern**: SerpApi costs $50/mo and requires a separate key. The Anthropic API with 
web_search tool is more powerful (Claude understands product descriptions, searches any 
site) and uses the same API key already on Railway.
**Rule**: Default to Claude Haiku + web_search for product pricing. Falls back gracefully.
Pipeline: Catalog (local) → SCPRS (local) → Claude Web Search (API) → Manual.
The web search module caches results for 7 days to avoid redundant API calls.

## L20: Protect all IIFEs and DOM operations in script blocks with try/catch
**Pattern**: An unprotected `recalcPC()` or `getElementById()` crash at the top of a
<script> block kills ALL function definitions below it. Every button handler becomes
undefined, the page looks functional but nothing works.
**Rule**: Wrap ALL early-executing code (IIFEs, DOM queries, recalc calls) in try/catch.
Function definitions below are never guarded — they rely on not being blocked.

## L21: Monolithic script blocks are fragile — split by concern
**Pattern**: 2000 lines in one <script> tag. One bad regex or template injection = 
entire block fails to parse = zero functions defined = every button dead.
**Rule**: Split by concern into independent <script> blocks. CRM rendering, auto-pricing,
and core functions each get their own block. Cross-block references use 
`window.X || fallback` pattern. Data injected via `<script type="application/json">` 
data islands (never inline in JS). Node --check validates each block at build time.

## L22: Consolidate table columns — 14 is too many
**Pattern**: SCPRS$, Amazon$, Source as 3 separate columns wastes space and overflows.
**Rule**: Merge related data into smart composite columns. "Sources" column shows all
price sources as clickable chips with cost comparison. Preferred suppliers (★) float
to top if within 10% of cheapest. Each chip links to source (internal/external).

## L23: Every pricing discovery must write back to product DB
**Pattern**: URL paste, web search, SCPRS match, and catalog match all found pricing
data but stored it only on the PC item's JSON dict — never wrote back to product_catalog
or product_suppliers tables. Data disappeared after the session.
**Rule**: Every pricing path must do 3 things:
1. Set pricing on the PC item (for immediate display)
2. Find-or-create a catalog product (for future matching)
3. Record the supplier + price in product_suppliers (for intelligence)
Write-backs use try/except so failures don't break the main flow.

## L24: Propagate part numbers from every match source
**Pattern**: SCPRS matches often have item_numbers from POs. Catalog matches have
mfg_numbers. Web search finds part_numbers. None of these flowed back to the item's
item_number field, so the MFG# column stayed blank.
**Rule**: After every match (SCPRS, catalog, web), check if item.item_number is empty.
If so, copy the best available part number from the match result.
Priority: mfg_number > part_number > sku > asin > sequential.

## L25: 704 ITEM field is just a row number — real part numbers are elsewhere
**Pattern**: The AMS 704 "ITEM" column contains sequential row numbers (1, 2, 3).
Real MFG/part/reference numbers are in:
1. SUBSTITUTED ITEM column ("Include manufacturer, part number, and/or reference number")
2. Embedded in the DESCRIPTION field (e.g. "MFG#: ABC-123")
3. Occasionally the ITEM field has a real code (alphanumeric, not sequential)
**Rule**: Parse ALL three sources. Priority: substituted > description > item_number.
Use regex patterns for labeled formats (MFG#, SKU:, Part:, Ref#) and structural
patterns (dash-separated codes, alphanumeric combos). Filter out false positives
(UOMs, common words, row numbers 1-50). Store as both `mfg_number` and `item_number`.

## L26: DB migrations must run BEFORE index creation
**Pattern**: `init_catalog_db()` ran CREATE INDEX on `search_tokens` column BEFORE
the ALTER TABLE loop that adds that column. On fresh DBs, CREATE TABLE includes the column
so indexes work. On existing DBs, the index fails → exception propagates → migration
loop never runs → column never gets added → all queries referencing it fail forever.
**Rule**: In any DB init function, always order: 1) CREATE TABLE, 2) ALTER TABLE migrations,
3) verify columns exist, 4) CREATE INDEX. Wrap index creation in try/except so one
index failure doesn't block others. Log confirmation of critical columns after migration.

## L27: Pricing source URLs should auto-populate item_link
**Pattern**: Amazon lookup stores URL in `pricing.amazon_url` but not in `item_link`.
User sees "$5.99 Amazon" in Sources column but ITEM LINK field stays empty.
After page reload, the URL is lost from the user's perspective.
**Rule**: After any pricing lookup (Amazon, web search, SCPRS), if `item_link` is empty,
copy the best source URL into it. This ensures the URL persists across page loads
and flows through to catalog enrichment on save.

## L28: Form fields need onchange/onblur handlers for dependent actions
**Pattern**: MFG# input was a passive text field. User types a part number, tabs away,
nothing happens. Expected behavior: catalog lookup triggers automatically.
**Rule**: Any form field that could trigger a lookup or computation needs an event handler.
MFG# → catalog search. Description → catalog match. URL paste → price lookup.
Don't rely on the user clicking a separate button when the trigger data is right there.

## L29: Never replace pc["items"] — always merge
**Pattern**: Amazon lookup did `pc["items"] = parsed.get("line_items", [])`, wiping
user-edited fields (item_link, notes, is_substitute, vendor_cost) that existed on
the old items but not in the freshly-parsed line_items.
**Rule**: When a lookup/enrichment adds data to items, MERGE into existing items
using `.update()` on the pricing dict. Never replace the items list wholesale.
Only the initial parse and explicit re-parse should replace items.

## L30: pc["parsed"] may not exist — use _sync_pc_items()
**Pattern**: `pc["parsed"]["line_items"] = items` crashes with KeyError when
pc["parsed"] doesn't exist. This happens after SQLite restore (which only stores
items, not the full parsed dict) or manual PC creation.
**Rule**: Never write to pc["parsed"] directly. Use `_sync_pc_items(pc, items)`
which creates pc["parsed"] if missing and preserves the header dict if present.
All 5+ save paths must use this helper.

## L31: fill_ams704 skips items without row_index or unit_price
**Pattern**: Items added via "Add Row" in the UI had no row_index, so fill_ams704
skipped them entirely. Items without any price also get skipped silently.
**Rule**: New items must get row_index = len(items)+1. fill_ams704 defaults
row_index to item position if missing. Always log what fill_ams704 skips so
silent data loss is visible in Railway logs.

## L32: request.json can silently return None — use get_json(force=True)
**Pattern**: Flask's `request.json` returns None if Content-Type isn't exactly
"application/json" (e.g. browser adds charset). Save handler got empty dict,
iterated nothing, saved unchanged items, returned {"ok": true} — looked like
success but nothing persisted.
**Rule**: Use `request.get_json(force=True, silent=True)` for all POST handlers.
Log when body is empty. Never return ok:true unless actual changes were made.

## L33: Add diagnostic endpoints for critical flows
**Pattern**: Four separate failures (save, PDF, catalog, descriptions) with no
visibility into what's happening on Railway. Debugging was impossible without
logs showing what data entered each function.
**Rule**: Critical flows (save-prices, fill_ams704, catalog enrichment) must
log their inputs, outputs, and skip reasons. Add /diagnose endpoints that
check data integrity (row_index, pricing, parsed dict, catalog DB state).

## L21: Python 3.12+ importlib _check_name_wrapper breaks globals injection
**Pattern**: _load_route_module injects dashboard globals into route modules via
`mod.__dict__.update(_shared)`. This overwrites `__name__`, `__spec__`, `__file__`
with dashboard's values. Python 3.12 added strict name checking in
`_check_name_wrapper` → "loader cannot handle" error.
**Rule**: When injecting globals into a module before exec_module(), save and
restore `__name__`, `__spec__`, `__file__` after the update. The loader
checks these against the spec and will reject mismatches.

## L34: Schema migrations must be idempotent and append-only
**Pattern**: SQLite has no ALTER TABLE DROP COLUMN (until 3.35). Tables created
by multiple modules (processed_emails in email_poller + migrations) can collide.
**Rule**: Always use CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS in
migrations. Never modify existing migrations — only append new ones. Track
applied versions in schema_migrations table. Run on every startup.

## L35: Structured logging format must match Railway expectations
**Pattern**: Railway log aggregation works best with single-line JSON. Multi-line
Python tracebacks break log parsing and search.
**Rule**: In production, format logs as single-line JSON with: ts, level, logger,
msg, error.type, error.message. Keep traceback to last 3 frames. Use
RAILWAY_ENVIRONMENT env var to auto-detect production.

## L36: rfq.db vs reytech.db — legacy database split
**Pattern**: Route modules (routes_features, routes_features2) use rfq.db while
agents and core modules use reytech.db. These are DIFFERENT databases with
different schemas. Cannot simply swap sqlite3.connect(rfq.db) with get_db().
**Rule**: Before replacing a direct DB connect, verify which database file it
connects to. rfq.db direct connects need a full data migration to reytech.db
before they can use get_db(). Don't break things by assuming all SQLite paths
point to the same file.

## L37: with-block indentation cascade in exec'd modules
**Pattern**: Adding `with get_db() as conn:` to an existing function requires
re-indenting ALL subsequent lines by 4 spaces. In a 100-line function loaded
via exec(), this is high-risk for indentation errors.
**Rule**: For large functions, use bash/python scripts to add indentation
programmatically rather than manual str_replace. Always compile-check after.

## L38: Cross-module globals in exec'd route modules
**Pattern**: routes_crm references INTEL_AVAILABLE which is defined in routes_intel.
Since routes_crm loads before routes_intel, the variable doesn't exist yet.
Module loading order: routes_rfq → routes_agents → routes_pricecheck →
routes_crm → routes_intel → ...
**Rule**: Pre-define default values for cross-module globals in dashboard.py
BEFORE the route module loading loop. E.g. `INTEL_AVAILABLE = False` as a safe
default that gets overwritten when the defining module loads.

## L39: Test fixtures must patch module-level globals, not just env vars
**Pattern**: Setting `os.environ["DASH_PASS"]` doesn't work if the dashboard
module already read the value at import time into `DASH_PASS = os.environ.get(...)`.
**Rule**: For test auth to work reliably:
1. Set env vars (monkeypatch.setenv)
2. Also monkeypatch the module-level globals (dashboard.DASH_USER, dashboard.DASH_PASS)
3. Also monkeypatch check_auth() function directly for belt-and-suspenders reliability
4. Each test file that defines its own `app` fixture must do ALL three.

## L40: exec'd module functions can't be imported directly in tests
**Pattern**: `from src.api.modules.routes_analytics import _compute_recommended_price`
fails because executing the module triggers @bp.route decorators that need `bp`
in the module namespace, which only exists when loaded via _load_route_module().
**Rule**: To test utility functions from exec'd modules, access them through
dashboard globals after app creation: `getattr(dash, '_compute_recommended_price')`.
Use pytest.skip() if the function isn't available rather than crashing.

## L41: Safe Blueprint refactor — extract, don't restructure
**Pattern**: Moving bp+auth to shared.py and adding explicit imports to modules
is safe and testable. Full Blueprint-per-module restructuring is high risk.
**Rule**: For exec'd/injected modules, add explicit imports ALONGSIDE injection.
The injection becomes a safety net, the imports become documentation. Test patch
both the old location (dashboard) AND the new location (shared) since Python
module references aren't magically updated.

## L41: Forwarded emails — nested PDFs not detected by _get_pdf_names

**Bug:** CalVet forwarded RFQ silently dropped. `_get_pdf_names()` only handled
`isinstance(payload, list)` for `message/rfc822` parts, but Python's email lib
sometimes returns a single Message object (not wrapped in a list). Result:
`pdf_names = []` → self-email filter sees `has_pdfs=False` → drops the forward.

Meanwhile `_extract_forwarded_attachments()` handled BOTH cases correctly with
`elif hasattr(payload, 'walk')`. The two functions were inconsistent.

**Fix:** (1) Unified `_get_pdf_names` to handle both list and single-message
payloads. (2) Self-email filter now lets clear forwards through (Fwd: subject
+ forwarded body markers) even if no top-level PDFs detected, since
`_extract_forwarded_attachments` will find nested PDFs during processing.

---

## Session 2026-03-14 — Architectural Layers 1-4

## Lesson L42 [2026-03-14]
**Mistake:** check_routes.py flagged 12 "duplicate" routes that were actually different HTTP methods on the same path (GET /api/customers vs POST /api/customers). Investigation wasted time before discovering they were false positives.
**Pattern:** Tool gives false signal → developer acts on it without verifying
**Rule:** When a lint/check tool reports violations, verify one by hand before batch-fixing — the tool itself may have a bug (check_routes.py didn't parse HTTP methods).

## Lesson L43 [2026-03-14]
**Mistake:** Consolidating routes_features*.py into domain modules introduced a function name collision — `api_revenue_goal()` existed in both routes_orders_full.py (from features.py) and routes_prd28.py (original). Flask threw AssertionError on blueprint registration: "View function mapping is overwriting an existing endpoint".
**Pattern:** Name collision across exec'd modules — function names are global
**Rule:** Before moving a route function to a new module, grep for its function name across ALL route modules; rename with a domain prefix if it collides (e.g., `api_pipeline_revenue_goal`).

## Lesson L44 [2026-03-14]
**Mistake:** Legacy /api/v1/ routes existed in routes_analytics.py (with a custom `_api_auth()` function) and conflicted with new routes_v1.py. Flask refused to register the blueprint.
**Pattern:** Stale code in large files — features added months ago and forgotten
**Rule:** Before adding new routes to a namespace, grep for that prefix (`/api/v1/`) across ALL route files; remove or rename stale implementations first.

## Lesson L45 [2026-03-14]
**Mistake:** 26 route handlers did DB access and file I/O with no try/except, meaning any exception returned a raw 500 page instead of a JSON error. Four more caught exceptions but didn't log them — silent failures in production.
**Pattern:** Inconsistent error handling — some routes careful, most not
**Rule:** Every route that does I/O must either use `@safe_route` decorator or have an outer try/except that calls `log.error()` with route name and input params before returning a JSON error.

## Lesson L46 [2026-03-14]
**Mistake:** pytest hung indefinitely because `app.py` module-level `create_app()` boots background agents (email poller, IMAP connect, scheduler threads) that block or loop forever. Tests never got to run.
**Pattern:** Module-level side effects in application factory
**Rule:** Every background thread start must be gated behind an env var check (`ENABLE_BACKGROUND_AGENTS != false`). Test fixtures must set this to `false` before importing the app. Never connect to external services (IMAP, APIs) at import time.

## Lesson L47 [2026-03-14]
**Mistake:** DAL test runs created records (T1, DT1 with status "sent") in the production database that then failed the data integrity check ("sent RFQs with all-zero prices"). Test data leaked into production DB.
**Pattern:** Tests using production database instead of isolated test DB
**Rule:** DAL tests run via `pytest` should use the conftest.py temp_data_dir fixture, not the real data directory. Direct `python -c` test runs that call `init_db()` operate on the real DB — clean up test records afterwards or use a separate test DB path.

## Lesson L48 [2026-03-14]
**Mistake:** unittest.mock `patch("src.agents.notify_agent.Client")` failed because `Client` (from twilio) is imported locally inside the function, not at module level. The mock target didn't exist on the module.
**Pattern:** Mock path doesn't match actual import location
**Rule:** When the function-under-test does `from twilio.rest import Client` locally, mock the source: `patch("twilio.rest.Client")`. When it's a module-level import, mock on the module: `patch("mymodule.Client")`. Always match the mock path to where Python resolves the name at call time.

## Lesson L49 [2026-03-14]
**Mistake:** First implementation of API key auth used `Authorization: Bearer <token>` with DB-backed keys (generate, validate, revoke). The actual spec required a simpler `X-API-Key` header checked against an env var. Had to rewrite shared.py.
**Pattern:** Over-engineering before confirming requirements
**Rule:** Implement the simplest auth that meets the stated requirement. Env-var API key → X-API-Key header check is 10 lines. DB-backed key management can be added later when there are multiple API consumers.

## Lesson L50 [2026-03-14]
**Mistake:** The JSON→SQLite read migration for `_load_price_checks()` broke indentation — the old `except Exception: data = {}` block lost its indent level when the surrounding code was restructured, causing a compile error.
**Pattern:** Manual code restructuring around try/except blocks corrupts indentation
**Rule:** After restructuring any try/except block, always compile-check immediately (`py_compile`) before making more edits. The indentation-sensitive nature of Python means even one-space errors silently change control flow or fail to compile.

## Lesson L51 [2026-03-14]
**Mistake:** DAL snapshot tests failed because `agent_snapshots` table didn't exist in the test DB — `init_snapshots()` was never called during test setup. The table is created by a separate module, not by `init_db()`.
**Pattern:** Infrastructure tables created outside the main SCHEMA string don't exist in fresh DBs
**Rule:** Any DAL function that depends on a table outside the main SCHEMA must call `init_X()` (idempotent) on first use, or the table must be added to the main SCHEMA. Don't assume test DBs have all tables.

## Lesson L52 [2026-03-14]
**Mistake:** When splitting routes_intel.py (7.1K lines), growth routes referenced `GROWTH_AVAILABLE` and `get_prospect` which are imported at the TOP of routes_intel.py. The new split module needs its own copy of those imports, but the old file also needs to keep them because non-growth routes use them too.
**Pattern:** Shared imports in exec'd modules must be duplicated in both files after split
**Rule:** When splitting an exec'd route module, identify which top-level imports are used by BOTH the kept and extracted code. Duplicate those imports in both files — exec'd modules don't share imports, they share the dashboard.py global namespace only AFTER both have been loaded.

## Lesson L53 [2026-03-14]
**Mistake:** SQL JOINs between scprs_po_master and scprs_po_lines failed with "ambiguous column name: po_number" because both tables have a `po_number` column. Two separate queries had the same bug — wasn't caught until runtime.
**Pattern:** Ambiguous column names in JOINs between tables with overlapping schemas
**Rule:** When writing SQL JOINs, ALWAYS prefix every column with its table alias (`m.po_number` not `po_number`), especially when both tables share column names. Test every new SQL query with actual data before committing.

---

## Debugging Laws

### Law 1: Working Code Is Sacred
If an endpoint or function works and returns the correct result, READ ITS SOURCE CODE IMMEDIATELY.
Do not hypothesize why it works. Do not build a "better" version. Copy it exactly, line by line.

"Working code is the ground truth. Everything else is a guess."

Applied: If debug-detail returns PDL_DVW=True, open debug-detail, read every line, copy into get_detail().
Do this BEFORE any other debugging step.

### Law 2: Log Before You Fix
Before changing any logic, add logging to show exactly what is happening. Diagnose first, fix second.
Never fix blind.

### Law 3: One Change Per Deploy
Each deploy changes exactly one thing. If it doesn't fix the bug, revert and try something else.
Multiple simultaneous changes make root cause impossible to find.

### Law 4: Mutex Before Threads
Any endpoint that spawns a background thread must check a lock first.
Two threads touching the same session = guaranteed corruption.

### Law 5: Compare Working vs Broken
When X works and Y doesn't, diff them immediately.
Don't fix Y until you know every difference between X and Y.

### Law 6: One URL Scheme — Verify Every Endpoint
When a URL bug is fixed (psp->psc, psfpd1->psfpd1_1, etc.), immediately audit EVERY other URL in the codebase and apply the same fix.

Never assume a URL fix is isolated. PeopleSoft uses the same scheme everywhere. If one endpoint was wrong, they're all wrong.

Checklist when fixing any URL:
  `grep -r "psp/" src/` — should return 0 results
  All SCPRS URLs must use: `psc/psfpd1/`
  (psfpd1_1 is DEAD — confirmed 2026-03-15)

"Fix the URL in one place, audit every URL."

### Law 7: PeopleSoft Needs a Real Browser
HTTP scraping cannot execute PeopleSoft's JavaScript modals. The click POST
returns a 553KB page with PO numbers but no line items. Only a headless
browser (Playwright/Chromium) can click the PO link, wait for the JS modal
to render, catch the popup window, and extract ZZ_SCPR_PDL_DVW line items.

Applied: 6 hours of HTTP debugging (wrong ICAction, wrong URL, wrong fields,
stale state) — all failed. Playwright solved it in one deploy.

### Law 8: Wire Schedulers in dashboard.py, Not Just app.py
Background schedulers must be in the `if ENABLE_BACKGROUND_AGENTS` block
in `dashboard.py`, not just in `app.py` deferred init. The deferred init
runs once but may not persist through Railway worker restarts. Dashboard.py
is the canonical scheduler location.

### Law 9: Price From the Ceiling Down
When you have cost + competitor data, price 2% UNDER the competitor average
(ceiling), not at a fixed markup above cost (floor). Every dollar between
cost and competitor avg is YOUR margin. A 30% markup that leaves $1,071 on
the table is worse than a 45% markup that captures it.

### Law 10: Pre-load State Before Long Jobs
Any exhaustive scrape or batch job should load existing data into a skip-set
before starting. The 2AM run pre-loads 14K+ existing PO numbers so it only
fetches new ones. Without this, you re-scrape everything every night.

### Law 11: Scheduler Wiring Law (The "Plugged In" Rule)
Any function that runs on a schedule MUST have THREE things:
1. The function itself
2. A startup call in dashboard.py background agents section
3. A manual trigger endpoint (/api/v1/{feature}/fire-now)

Before committing any scheduler:
- [ ] Function exists
- [ ] Imported + called in dashboard.py scheduler section
- [ ] Manual trigger endpoint exists
- [ ] Tested manually and confirmed in logs

If you build it but don't wire it, it doesn't exist.

This law exists because on 2026-03-15 we built
schedule_full_fiscal_scrape() and schedule_system_audit() —
both compiled clean but neither was called on startup.
The 2AM scrape never fired. NEVER AGAIN.

### Law 12: The Wiring Verification Law
Before committing ANY code, run these verification commands:

1. Every new function must have >=1 caller outside its own file:
   `grep -rn "function_name" src/ | grep -v "def function_name" | grep -v __pycache__`

2. Every new table must have >=1 INSERT and >=1 SELECT:
   `grep -rn "INSERT.*table_name" src/ | wc -l` (must be > 0)
   `grep -rn "SELECT.*table_name\|FROM table_name" src/ | wc -l` (must be > 0)

3. Every new endpoint must be referenced by UI or another function:
   `grep -rn "/api/v1/endpoint" src/ | grep -v "@bp.route" | wc -l` (must be > 0)

4. Every new scheduler must be called in dashboard.py:
   `grep -rn "schedule_function" src/api/dashboard.py | grep -v "def " | wc -l` (must be > 0)

If ANY of these return 0, THE FEATURE IS NOT WIRED IN. Fix before committing.

See tasks/CC_IMPLEMENTATION_RULES.md for the complete rule set.

### Law 13: The UOM Normalization Law
Never compare, average, or recommend prices without first
normalizing all prices to the same unit of measure.

$652/case != $8/box != $0.065/glove

Parse pack sizes from descriptions before ANY price math.
If UOM cannot be determined, exclude the price from averages.

### Law 14: GET Handlers Are Read-Only
A GET route handler must NEVER call save_rfqs(), save_price_checks(),
db.execute("UPDATE"), db.execute("INSERT"), or any other write operation.

GET = read. POST = write. No exceptions.

On 2026-03-16, a memory auto-fill feature was added to the RFQ detail
GET handler that modified line items and called save_rfqs(). During
6 rapid emergency deploys to fix a crash, the corrupted data was saved
to disk, wiping line items from active RFQs.

### Law 15: Never Deploy Enrichment Into Core Render Paths
New enrichment/intelligence code must NEVER be added directly into
page render handlers (detail, list, etc). Instead:
1. Add enrichment as a SEPARATE endpoint (/api/v1/rfq/{id}/enrich)
2. Call it asynchronously from the frontend
3. The main page renders IMMEDIATELY with whatever data exists
4. Enrichment populates additional fields via AJAX after page load

This way: if enrichment crashes, the page still works.

### Law 16: Atomic Writes Need Rollback
Before any save_rfqs() call, snapshot the current state:
    backup = json.dumps(rfqs[rid])
    try:
        # modifications
        save_rfqs(rfqs)
    except:
        rfqs[rid] = json.loads(backup)  # rollback

### Law 17: Field Name Consistency — One Name, Everywhere
The same data must use the same field name in:
- DB schema (CREATE TABLE)
- JSON files
- Python dicts
- Jinja2 templates
- JavaScript

On 2026-03-16, SQLite stored items as "items" but templates
read "line_items". Data appeared lost but was there all along.
Before adding ANY field, grep the entire codebase for existing
names: `grep -rn "line_items\|\"items\"" src/`

### Law 19: Boot Recovery Is Mandatory
Every deploy restarts the container. JSON files on the volume
can become stale, empty, or corrupted. SQLite is the source
of truth. On EVERY boot:
1. Check if rfqs.json is empty but SQLite has data → rebuild
2. Check if price_checks.json is empty but SQLite has data → rebuild
3. Log the recovery

This law exists because PCs were wiped on 3+ deploys before
boot recovery was added. 2026-03-16.

### Law 20: The "Zero Callers" Check
Before committing ANY new utility function, run:
  `grep -rn "function_name" src/ | grep -v "def function_name" | grep -v __pycache__`
If it returns NOTHING, the function is NOT WIRED.

safe_save_json was requested 3 times and committed 3 times.
Each time the file was created but callers were not verified.
NEVER commit a function with zero callers.

### Law 21: SQLite Is Source of Truth, JSON Is Cache
SQLite DB persists across deploys on Railway volume.
JSON files are a READ CACHE that gets rebuilt from SQLite.
When they disagree, SQLite wins.

Write path: Always SQLite first, then JSON cache.
Read path: DAL (SQLite) first, JSON fallback only if DAL empty.
Boot path: If JSON empty but SQLite has data, rebuild JSON.

### Law 22: Never Delete Business Data
PCs, RFQs, quotes, and orders must NEVER be deleted by
automated code. Use status fields instead:
- "converted" — PC was linked to an RFQ
- "dismissed" — user explicitly dismissed
- "archived" — past retention period

`grep -rn "del pcs\[|del rfqs\[|del orders\[" src/`
must return ZERO results. On 2026-03-16, cross-queue cleanup
was deleting PCs that matched an RFQ's solicitation number.
When the linker failed (bugs 1-4), PC was deleted AND not
linked. User lost all pricing data with no recovery path.

### Law 23: Matching Must Be Fuzzy
Names, descriptions, and identifiers from different sources
(email vs PDF vs database) will NEVER be exactly identical.
All matching code must use:
- SequenceMatcher with threshold >=0.6 for descriptions
- Set intersection of normalized identifiers for people
- Both name AND email checked for requestor matching
- Case-insensitive, punctuation-tolerant comparison

Exact string matching (==, set intersection of truncated
strings) will fail in production. Always.

### Law 24: Critical Paths Need Automated Tests
Before committing changes to _link_rfq_to_pc, save_rfqs,
_save_price_checks, or any data pipeline function, run
the test script and show the output. All tests must pass.
