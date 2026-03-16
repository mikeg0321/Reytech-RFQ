# CC Implementation Rules — Reytech RFQ

## READ THIS BEFORE EVERY TASK. NO EXCEPTIONS.

These rules exist because of repeated production failures caused by
building features that compile clean but aren't connected to anything.

---

## Rule 1: Every Function Must Be Wired
If you write a function, it MUST be called from at least one other file.
Verify: `grep -rn "function_name" src/ | grep -v "def function_name" | grep -v __pycache__`
If result is empty, the function doesn't exist from the app's perspective.

## Rule 2: Every Table Must Be Read AND Written
If you create a table, there MUST be at least one INSERT and one SELECT.
Verify: `grep -rn "table_name" src/ --include="*.py" | grep -v "CREATE TABLE"`
If no INSERT or no SELECT, the table is dead.

## Rule 3: Every Endpoint Must Be Reachable
If you create an endpoint, it MUST be called by UI, another endpoint, or a scheduled job.
Verify: `grep -rn "/api/v1/endpoint" src/ | grep -v "@bp.route"`
If result is empty, nobody can reach it except by manually typing the URL.

## Rule 4: Every Scheduler Must Start on Boot
If you create a scheduled function, it MUST be called in the dashboard.py
background agents section AND have a manual trigger endpoint.
Verify: `grep -rn "schedule_function" src/api/dashboard.py`

## Rule 5: py_compile After Every File Change
Run `python -c "import py_compile; py_compile.compile('file.py', doraise=True)"`
after EVERY file change. Do not batch compile — compile as you go.

## Rule 6: UOM Normalize Before Price Math
NEVER compare, average, or recommend prices without normalizing to per-unit first.
$652/case of 1000 = $0.652/unit. $8.44/box of 100 = $0.0844/unit.
Use _normalize_to_per_unit() from pricing_oracle_v2.py.

## Rule 7: Test Connection Points
After wiring, verify with grep that the function/table/endpoint is actually
referenced from where it should be. Count the references. If count is 0, it's broken.

## Rule 8: One Change Per Deploy for Debugging
When debugging, change ONE thing, deploy, test. Multiple changes make root cause
impossible to find. This saved us 6 hours on the SCPRS detail page bug.

## Rule 9: Read Working Code Before Building New Code
If something already works (even partially), READ ITS SOURCE first.
Copy working patterns. Don't invent new approaches for solved problems.

## Rule 10: Data Path Verification
For every data flow (email → parse → store → display), verify each step:
1. Data enters the system (log it)
2. Data is stored (query the table)
3. Data is retrieved (test the endpoint)
4. Data is displayed (check the template)
If any step returns empty, the pipe is broken.

---

## Verification Checklist (Run Before Every Commit)

```bash
# 1. All changed files compile
python -c "import py_compile; py_compile.compile('changed_file.py', doraise=True)"

# 2. New functions are called
grep -rn "new_function" src/ | grep -v "def new_function" | grep -v __pycache__ | wc -l
# Must be > 0

# 3. New tables are used
grep -rn "new_table" src/ --include="*.py" | grep -v "CREATE TABLE" | wc -l
# Must be > 0

# 4. New schedulers are wired
grep -rn "new_scheduler" src/api/dashboard.py | wc -l
# Must be > 0

# 5. Smoke test passes
python scripts/smoke_test.py
```

---

## Known Data Paths (verify these work)

| Source | Storage | Endpoint | UI |
|--------|---------|----------|----|
| Email → PDF parse | price_checks.pc_data | /api/v1/pricing/lookup | pc_detail.html |
| SCPRS scrape | scprs_po_master + scprs_po_lines | /api/v1/harvest/fiscal-scrape-status | (API only) |
| Catalog | scprs_catalog | /api/v1/quote/catalog-search | (API only) |
| Buyers | scprs_buyers | /api/v1/buyers/prospects | (API only) |
| Item mappings | item_mappings | /api/v1/pricing/confirm-item | (API only) |
| Supplier costs | supplier_costs | /api/v1/pricing/lock-cost | (API only) |
| Usage tracking | usage_events | /api/v1/usage/stats | (API only) |

---

## Common Failure Modes

1. **Built but not wired**: Function exists, compiles, but nobody calls it.
   Fix: Add the import + call in dashboard.py or the relevant route handler.

2. **Wrong URL scheme**: PeopleSoft uses psc/psfpd1, not psp/psfpd1_1.
   Fix: grep for all URL variants and standardize.

3. **UOM mismatch**: $652/case compared to $8/box = insane recommendations.
   Fix: Always normalize to per-unit before any price math.

4. **JSON schema mismatch**: Code queries `quote_items` table but items are
   stored as JSON in `price_checks.pc_data` or `quotes.items_detail`.
   Fix: Read the actual schema before writing queries.

5. **Scheduler not firing**: Function scheduled but not called on boot.
   Fix: Add to dashboard.py background agents section + add manual trigger endpoint.
