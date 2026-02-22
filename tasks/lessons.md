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
