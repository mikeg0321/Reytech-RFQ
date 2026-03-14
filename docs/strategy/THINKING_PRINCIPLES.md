# How to Think About Every Decision in This Codebase
*Reference before every sprint, every prompt, every architectural choice*

---

## The Business Reality
- Reytech does $500K+/year in government procurement
- 1 person (Mike) handles everything manually today
- Goal: 10x revenue with same headcount via automation
- Every government procurement transaction is public data
- The competitor who harvests and uses this data wins

---

## The Three Questions to Ask Before Every Decision

**1. Does this make the system smarter over time?**
Features that learn (oracle, win/loss feedback, buyer patterns) 
compound in value. Features that don't learn are just tools.
Prefer decisions that create a feedback loop.

**2. Can an external agent call this without knowing internals?**
Every agent entry point should accept a plain dict, return a plain dict.
Every API response should be {ok, data, error}.
Every route should work with X-API-Key, not just browser sessions.
If an LLM tool can't call it cleanly, it's not done.

**3. Will this still be the right decision when there are 
   10 tenants and 5 states of data?**
tenant_id on every table. state + source_system on every harvest row.
Configuration over hardcoding. DAL over raw SQL.
The white-label fork is a configuration change, not a rewrite.

---

## Data Principles

**Public procurement data is a permanent moat.**
SCPRS, USASpending, state portals — all public, all free.
4 years of historical data exists right now.
Every competitor who doesn't harvest this is flying blind.
Storage is cheap. Rebuild cost is enormous.

**Validate against business reality before building intelligence.**
6 Reytech wins ≠ $500K revenue → something is missing.
Always cross-check DB numbers against what Mike knows is true.
Wrong data in = wrong confidence scores = wrong bids = lost deals.

**Confidence over certainty.**
The oracle should be honest about what it doesn't know.
insufficient_data is a valid and important output.
A low-confidence override costs more than a cautious recommendation.

---

## Architecture Principles

**The fork strategy: configuration not code.**
Primary codebase = Reytech optimized.
White-label = different tenant_id, different product catalog,
same engine, same intelligence, same oracle.
One bug fix propagates everywhere.
One oracle improvement benefits every tenant.

**Layer by layer, gate by gate.**
Never start a new layer until QA gates are green.
smoke_test.py + check_routes.py + data_integrity.py before every push.
A clean codebase compounds. Technical debt compounds faster.

**The DAL is the contract.**
All reads and writes go through dal.py for core entities.
No raw SQL in route files. No JSON reads for entities in the DB.
An external agent calling the API should get the same data 
as a human looking at the dashboard.

---

## Intelligence Architecture

**The harvest → oracle → pipeline loop:**
```
SCPRS + USASpending + state data
        ↓ harvest (weekly)
  scprs_po_master + scprs_po_lines
        ↓ reprocess
  won_quotes_kb + vendor_intel + buyer_intel + competitors
        ↓ oracle
  pricing recommendations with confidence scores
        ↓ pipeline  
  RFQ priced → draft sent → award tracked
        ↓ win/loss recorded
  won_quotes_kb updated (closes the loop)
```

Every step in this loop improves the next iteration.
The system gets smarter with every RFQ processed.

**Confidence scoring rules (non-negotiable):**
- high: 10+ agency-specific matches → auto-override price
- medium: 5+ matches OR agency-specific → override if cheaper
- low: 2-4 matches → show as reference, do not override
- insufficient: <2 matches → do not use

**Price recommendation formula:**
```
base = agency_avg if agency_specific else all_agency_avg
recommended = base * 0.97  (3% under market to win)
floor = competitor_low * 1.02  (never below competitor + 2%)
final = max(recommended, floor)
```

---

## Pull Health Contract

Every harvest run is graded A/B/C/F.
F = skip intelligence reprocess, keep previous data, alert Mike.
C = alert (degraded) but reprocess.
A/B = healthy, proceed normally.

Health checks:
1. Row count >= minimum per agency
2. Required fields < 10% NULL
3. Date range valid (no all-same, no future)
4. Values > 0 on 80%+ of rows
5. At least 1 new row inserted

**Never rebuild intelligence tables from bad data.**
Use temp tables → validate → swap. 
If validation fails: restore from snapshot.

---

## The Lessons That Cost the Most (from tasks/lessons.md)

- Always validate row counts against known business reality
- request.json silently returns None — use get_json(force=True)
- rfq.db ≠ reytech.db — check which DB before migrating
- Never rebuild intelligence tables from a degraded harvest
- Confidence scores are only honest if the data is complete
- An oracle built on 6 wins when there are 500 is dangerous
- No hardcoded agency lists. Ever. Dynamic discovery > hardcoded configuration.
  If you're typing agency names into Python code, something is wrong.
- Connector registry = configuration not code. Adding a new data source = one DB row + one adapter.
  Activating a scaffolded connector = one UPDATE query, zero deploys.
