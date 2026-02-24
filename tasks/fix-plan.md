# Fix Plan: Catalog + Persistence Issues

## Problems
1. **`no such column: search_tokens`** — DB migration not running on Railway volume
2. **MFG# doesn't trigger catalog lookup** — no onchange handler 
3. **URLs not persisting** — need to verify save flow
4. **Data not persisting to DB** — need to trace save → JSON → catalog write-back

## Root Cause Analysis

### P1: search_tokens column missing
- `init_catalog_db()` uses ALTER TABLE ADD COLUMN in a loop
- The column genuinely doesn't exist on Railway's persistent DB
- My try/except in match_item catches the error but returns it as string
- The endpoint try/except passes it to frontend as {"ok": false, "error": "..."}
- Need: Force the migration to run at startup AND before first match

### P2: MFG# field is passive
- `itemnum_{idx}` input has no onchange/onblur handler for catalog lookup
- Should trigger a focused search when user types a part number

### P3: URLs not persisting  
- Need to trace: save-prices endpoint → `link_` field handling → JSON write

### P4: Data persistence
- L23 says every discovery must write back to product DB
- save-prices writes to price_checks.json but catalog enrichment may be failing

## Fix Plan
1. Force `init_catalog_db()` at module import in routes_catalog_finance.py
2. Add startup log to confirm search_tokens column exists
3. Add MFG# onchange handler to trigger catalog search
4. Trace and fix URL/link persistence in save flow
5. Verify catalog write-back on save
