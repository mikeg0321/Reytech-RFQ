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
