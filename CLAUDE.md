# CLAUDE.md — Reytech RFQ Project Rules

## System Context

**What this is:** End-to-end RFQ automation + business intelligence for Reytech Inc., a California SB/DVBE government reseller. 90K+ lines, 955 routes, 50 templates, deployed on Railway.

**Stack:** Python 3.12 / Flask / SQLite (WAL mode) / Jinja2 / Gunicorn. No frontend framework — all server-rendered HTML with inline JS.

**Deploy:** Push to `main` → Railway auto-deploys. Persistent volume at `/data`. Domain: `web-production-dcee9.up.railway.app`.

**Module loading:** Route modules in `src/api/modules/` are loaded via `exec()` into `dashboard.py` namespace. This means all modules share globals. Be aware of name collisions.

## Workflow Orchestration

### 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately — don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Verification Before Done
- Never mark a task complete without proving it works
- **Always compile-check** Python: `python -c "import py_compile; py_compile.compile('file.py', doraise=True)"`
- **Always render-test** templates with all required variables after changes
- Test with realistic data structures — production data may differ from dev assumptions
- Ask yourself: "Would a staff engineer approve this?"

### 3. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests — then resolve them
- Trace the full call chain: route → function → template → data structure
- Check for type mismatches (dict vs list, missing keys, None values)

### 4. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- Skip this for simple, obvious fixes — don't over-engineer
- Challenge your own work before presenting it

## Code Patterns

### Route Pattern
```python
@bp.route("/api/example", methods=["POST"])
@auth_required
def api_example():
    """Docstring with purpose."""
    try:
        # business logic
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        log.error("Example error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
```

### Template Variable Safety
Always use `|default()` for any variable that might not exist:
```jinja2
{{ value|default(0) }}
{{ obj.key|default('fallback') }}
{% for item in items|default([]) %}
```

### Defensive Data Loading
```python
try:
    data = some_function()
    if not isinstance(data, dict):
        data = {}
except Exception as e:
    log.error("Load error: %s", e)
    data = {}
data.setdefault("required_key", default_value)
```

### Growth Agent Functions
All in `src/agents/growth_agent.py` (104 functions). Key patterns:
- `_load_json(path)` / `_save_json(path, data)` for all JSON file I/O
- `_load_prospects_list()` returns list of prospect dicts
- Status dicts (`PULL_STATUS`, `BUYER_STATUS`, `INTEL_STATUS`) for long-running ops
- Thread-based async for SCPRS scraping — poll status endpoints for progress

## Known Issues (Production Audit — last audited 2026-03-23)

### Resolved
- **SQL Injection (was Critical):** All f-string SQL instances audited — all interpolate
  hardcoded constants, table names from allowlists, or dynamic `LIKE ?` placeholder
  counts. No user input reaches SQL strings. Not injection vectors.
- **Bare `except:` clauses:** All 5 replaced with specific exception types (0 remaining).
- **Duplicate routes:** `/api/pc/convert-to-rfq` and `/api/pricecheck/download` duplicates
  removed. Kept the more thorough implementations.
- **Orphaned templates:** 4 dead templates removed (expand, growth_intel, growth, crm).

### Warning — Unprotected Routes
13 routes lack `@auth_required`. Most are intentional (health check, webhooks, email
tracking pixels). Monitor for new unprotected admin routes.

### Info — Code Quality
- 2 TODO comments remaining (QB line-item search, RFQ Undefined values)
- 230 POST endpoints rely on session auth only (no explicit CSRF tokens)

## File Layout Rules

- **Routes:** `src/api/modules/routes_*.py` — one file per domain area
- **Agents:** `src/agents/*.py` — one file per external integration or intelligence engine
- **Templates:** `src/templates/*.html` — extends `base.html`, uses `render_page()`
- **Data:** `data/*.json` and `data/*.db` — persisted on Railway volume
- **Forms:** `src/forms/*.py` — PDF generation and form filling

## Testing Checklist

Before pushing any change:
1. `python -c "py_compile.compile('changed_file.py', doraise=True)"` for each modified Python file
2. If template changed: render test with all required variables (check for `UndefinedError`)
3. If route changed: verify `@auth_required` decorator is present
4. If data structure changed: check all templates that consume it for type assumptions
5. `git diff --stat` to verify only intended files are modified

## Form Filling Guard Rails (CRITICAL — Production Incidents 2026-03-26)

### Package Generation
- **CCHCS package = 703B/C + 704B + Bid Package + Quote ONLY.** DVBE 843, seller's permit, CalRecycle are INSIDE the bid package. Never generate standalone.
- **Optional forms are OPTIONAL.** Never auto-include based on item count or heuristics. Only generate if user explicitly checks them.
- **703C vs 703B:** If buyer provides 703C template, use `fill_703c()`. Never include both.
- **Before changing `agency_config.py` required_forms:** Verify the form isn't already inside the bid package PDF.

### Signature Placement
- **Generic fields (Signature1, Signature):** Only sign if in the lower 40% of the page. Certification sigs are always at the bottom.
- **Never double-sign:** If PDF has `/Sig` form field, `fill_and_sign_pdf` handles it. `_703b_overlay_signature` only runs when NO `/Sig` field exists.
- **New forms:** Use form-specific field names in `SIGN_FIELDS` (e.g., `Signature_formname`), not generic names.

### Quote Counter
- **Stored counter is authoritative.** Scans of existing quotes NEVER override a manual set.
- **Max jump = 5.** Counter blocked if it tries to jump more than 5 from last known value.
- **No nested DB connections inside `BEGIN IMMEDIATE`.** Use single connection with direct SQL. Nested connections cause cascading locks (2+ minute hangs).
- **`set_quote_counter()` must update `quote_counter_last_good`.**

### PC → RFQ Workflow
- **704 (PC)** = market test. Buyer's descriptions unchanged. Only pricing added.
- **704B (RFQ)** = Reytech's response. Use catalog descriptions, proper MFG#, ASIN in description.
- **PC pricing is authoritative** for that quote. Catalog pricing may be older.
- **Match items by description** (should be near-identical), positional fallback.
- **Never import PC items into RFQ.** RFQ items from 704B are authoritative.
- **Catalog provides enrichment** (URLs, ASIN, supplier) but NOT pricing.
- **Cross-queue dedup:** If PC exists for an email, don't also create an RFQ.

### 703C Form Filling
- Read actual PDF field names before filling. Detect prefix (703B_, 703C_, or none).
- Log field names for debugging: `print(f"703C fields: {sorted(field_names)}")`

## JavaScript Guard Rails (CRITICAL — Production Incidents 2026-03-31)

### DOM Access Must Be Null-Safe
Every `document.getElementById()` or `querySelector()` call in inline JS MUST
use null checks. The exec() module loading means elements may not exist on all
page variants (manual PC vs parsed PC vs RFQ).
```javascript
// WRONG — kills autosave silently if element missing:
data['tax_enabled'] = document.getElementById('taxToggle').checked;

// RIGHT:
var el = document.getElementById('taxToggle');
data['tax_enabled'] = el ? el.checked : false;
```

### Autosave Must Never Die Silently
- Wrap `collectPrices()` in try-catch inside `doPcAutosave()`
- Log errors to console so they're visible in DevTools
- Never let a single failed save kill the autosave timer
- The autosave timer re-triggers on next user input (change/input events)

### Inline Event Handlers in innerHTML Are Fragile
Never use complex JS in `onkeydown="..."` inside dynamically inserted HTML.
Quote escaping breaks silently. Use `addEventListener` after DOM insertion:
```javascript
// WRONG — nested quotes break:
html += '<input onkeydown="if(event.key===\'Enter\'){...}">';

// RIGHT — attach after insertion:
element.innerHTML = html;
var input = document.getElementById('myInput');
if (input) input.addEventListener('keydown', function(e) { ... });
```

## Pricing Guard Rails (CRITICAL — Production Incidents 2026-03-31)

### SCPRS Prices Are NOT Supplier Costs
SCPRS prices are what the STATE paid another vendor. They are reference
ceilings for your bid price, NEVER your cost basis.
```python
# WRONG — uses SCPRS as cost:
unit_cost = p.get("unit_cost") or amazon_price or scprs_price or 0

# RIGHT — only real supplier costs:
unit_cost = (p.get("unit_cost") or p.get("catalog_cost")
             or p.get("web_cost") or item.get("vendor_cost") or 0)
```

### Amazon Prices Are NOT Supplier Costs
Amazon retail prices are reference data for comparison. Never use as your
wholesale cost. The app marks Amazon data with ASIN badges — informational only.

### Cost Sanity Guardrail (3x Rule)
If unit_cost is >3x the SCPRS or catalog reference price, it's almost certainly
a bad scrape (wrong product matched on Amazon). Auto-correct to the reference
price and show a warning badge.

### S&S Worldwide Pricing
- S&S is Cloudflare-blocked — cannot scrape prices directly
- ALWAYS keep the S&S URL (never override with Amazon link)
- Use LIST price (non-discount) as cost basis — discounts expire in 45-day window
- When price unavailable: show quick-entry field, not $0.00 silently

### Catalog Match Threshold
Token matching threshold = 0.50 (raised from 0.35 after cross-category garbage
matches — shoes matching medical items). Final output filter also 0.50.
Never lower these without testing cross-category accuracy.

## PDF Parsing Guard Rails (Production Incidents 2026-03-31)

### Multi-Page AMS 704 Forms
PDF form fields use `_2`, `_3`, `_4` suffixes for pages 2-4 (e.g., `QTYRow1_2`
for page 2 item 1). Always scan field names for these suffixes.

### MFG# Extraction Patterns
Must handle: `W12919` (single letter + digits), `FN4368` (2 letter + digits),
`NL304` (2 letter + digits), `16753` (pure 5+ digit codes after " - ").
The `_PN_PATTERNS` list in `price_check.py` covers all these.

### Never Merge Items With Their Own Line Number
If a PDF row has its own `item_number` (distinct line # on the form), NEVER
merge it as a continuation row — even if qty=1 and uom=EA.

### Re-Index After Merge
After continuation merges remove rows, re-index items sequentially (1, 2, 3...)
not the original PDF row numbers (1, 3, 5...).

### Re-Parse Clears Enrichment
When re-parsing from PDF, clear `enrichment_status` and `enrichment_summary`.
Old enrichment data doesn't apply to new item set.

## Agency & Institution Rules

### We Only Sell in CA
Every institution maps to a known CA agency. Default to CDCR (most common),
never "DEFAULT". Use `institution_resolver.resolve()` first, keyword fallback
second.

### Institution Resolver Returns Lowercase
The resolver returns `"cchcs"`, `"cdcr"`, etc. UI expects `"CCHCS"`, `"CDCR"`.
Always normalize: `agency_map.get(agency.lower(), agency.upper())`.

## PC → RFQ Conversion (Updated 2026-03-31)

### Conversion = deepcopy, Not Field Remapping
PC → RFQ conversion is a `copy.deepcopy(pc)` + status change + audit log.
**No field-by-field remapping.** Same items, same prices, same data.
The old approach caused 4 bugs (empty MFG#, 0.00 bid price, "unknown" PC link,
empty subtotals) because field names differed between PC and RFQ schemas.

## Date/Time Rules

### All Dates Must Be PST/PDT
Server runs UTC (Railway). Use `_pst_now()` for any user-facing date:
- AMS 704 signature date
- Price Check expires date (45 days from PST today)
- Quote dates, due dates

### PDF Preview Must Be Inline
Use `?inline=1` query parameter on download URLs for iframe preview.
Without it, browser downloads the PDF instead of rendering it.

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.
- **Defensive Programming**: Every data access should handle None, wrong type, missing keys.
- **Production First**: This is a live business system. Every commit deploys automatically.
- **Never Die Silently**: Errors must be logged. Autosave must never stop. Data loss is unacceptable.
- **Prices Have Roles**: SCPRS = ceiling, Amazon = reference, Catalog = cost, S&S = cost.
- **Test With Real Numbers**: Before pushing ANY calculation change, manually verify: input × formula = expected output. "40.0% markup on $82.24 = ?" must equal $115.14, not $411.20. Compile-check is not enough.
- **Test All UI States**: Every new UI element must be tested in all states: empty, filled, error, re-run. If a field appears conditionally, test the condition being true AND false. Never mix.
