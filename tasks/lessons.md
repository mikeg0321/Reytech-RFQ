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
