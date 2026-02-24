# Fix Plan: End-to-End Trace

## What's STILL broken (user report):
1. PDF not updating with descriptions
2. Pricing not persisting on Save  
3. Catalog not matching
4. Descriptions not coming over to PDF

## Hypothesis: I've been fixing symptoms, not the root cause.
## Action: Write a test that simulates the EXACT flow and find where data drops.

## Flow to trace:
1. PC loads → items come from JSON file
2. User edits description/cost/price in UI
3. User clicks Save → collectPrices() → POST /save-prices
4. Backend parses fields → writes to items → _save_price_checks()
5. User clicks Save & Fill 704 → GET /generate
6. Backend reads PC → calls fill_ams704() → writes PDF

## Key questions:
- Does _save_price_checks actually persist to the JSON file on Railway volume?
- Does the generate endpoint re-read items correctly after save?
- Is row_index set correctly so fill_ams704 finds the right row?
- Is the field_id pattern correct for this specific PDF form?
