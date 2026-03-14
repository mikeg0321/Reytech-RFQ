# Phase 2 — Pricing Oracle + Buyer Intelligence
*Send to Claude Code after harvest remediation confirms Reytech 
wins reflect $500K+ actual revenue*

---

## Pre-flight checklist (verify before pasting prompt)
- [ ] won_quotes_kb has Reytech wins reflecting ~$500K
- [ ] All 8+ CA agencies in scprs_po_master
- [ ] Date range covers 2024 + 2025 minimum
- [ ] smoke_test.py green
- [ ] check_routes.py 0 duplicates
- [ ] data_integrity.py all PASS

---

## Prompt

```
Read AUDIT.md, tasks/lessons.md, and 
docs/strategy/INTELLIGENCE_ROADMAP.md before starting.
Harvest remediation is complete. Reytech wins in won_quotes_kb
now reflect actual business history.
This is Phase 2: build the pricing oracle and buyer intelligence
engine on top of verified data.
Reference INTELLIGENCE_ROADMAP.md for confidence scoring rules,
price recommendation formula, and business context.
All prior guardrails apply.

═══════════════════════════════════════════════════════════════
PHASE 2 — PRICING ORACLE + BUYER INTELLIGENCE ENGINE
Goal: turn verified won_quotes_kb records and buyer_intel 
profiles into a pricing engine that recommends win prices 
with confidence scores, and a buyer engine that surfaces 
new RFQ opportunities before they hit the inbox.
Every recommendation must be explainable. Every confidence 
score must be honest. Wrong confident = worse than uncertain.
═══════════════════════════════════════════════════════════════

── STEP 0: Data verification before building ────────────────
Run this first. Do not write any code until output confirms
data is sufficient for oracle to be meaningful.

python3 -c "
import sqlite3
conn = sqlite3.connect('data/reytech.db')

print('=== WON_QUOTES_KB COVERAGE ===')
r = conn.execute('''
    SELECT 
        COUNT(*) as total_items,
        SUM(CASE WHEN reytech_won=1 THEN 1 ELSE 0 END) as reytech_wins,
        SUM(CASE WHEN reytech_won=1 THEN winning_price ELSE 0 END) as reytech_value,
        COUNT(DISTINCT agency) as agencies,
        COUNT(DISTINCT state) as states,
        COUNT(CASE WHEN winning_price > 0 THEN 1 END) as priced_items
    FROM won_quotes_kb
''').fetchone()
print(f'Total items: {r[0]}')
print(f'Reytech wins: {r[1]} worth \${r[2]:,.2f}')
print(f'Agencies: {r[3]}, States: {r[4]}')
print(f'Items with price data: {r[5]}')

print()
print('=== CONFIDENCE DISTRIBUTION PREVIEW ===')
rows = conn.execute('''
    SELECT agency, 
           COUNT(*) as samples,
           CASE 
             WHEN COUNT(*) >= 10 THEN \"high\"
             WHEN COUNT(*) >= 5  THEN \"medium\"
             WHEN COUNT(*) >= 2  THEN \"low\"
             ELSE \"insufficient\"
           END as confidence_tier
    FROM won_quotes_kb
    WHERE winning_price > 0
    GROUP BY agency
    ORDER BY samples DESC LIMIT 15
''').fetchall()
for r in rows: print(r)

print()
print('=== BUYER INTEL COVERAGE ===')
r = conn.execute('''
    SELECT COUNT(*) as buyers,
           COUNT(DISTINCT agency) as agencies,
           COUNT(DISTINCT state) as states,
           SUM(total_spend) as total_spend,
           COUNT(CASE WHEN rfq_count=0 THEN 1 END) as no_rfq_yet
    FROM buyer_intel
''').fetchone()
print(f'Buyers: {r[0]}, Agencies: {r[1]}, States: {r[2]}')
print(f'Total spend tracked: \${r[3] or 0:,.2f}')
print(f'Buyers with no Reytech RFQ: {r[4]} (opportunities)')
"

If reytech_value < 400000 STOP and run harvest remediation.
If priced_items < 1000 STOP and run harvest remediation.
If no_rfq_yet < 50 STOP and run harvest remediation.
Only proceed when numbers reflect real market data.

── STEP 1: Pricing oracle ────────────────────────────────────
Create or extend src/knowledge/pricing_oracle.py

Implement these functions with exact signatures:

  get_win_price(
      item_description: str,
      agency: str = None,
      nsn: str = None,
      mfg_number: str = None,
      tenant_id: str = 'reytech'
  ) -> dict:
  """
  Input: item description + optional agency/NSN/MFG
  Output: {
    recommended_price: float,
    confidence: float (0.0-1.0),
    confidence_label: 'high|medium|low|insufficient_data',
    price_range: {min: float, max: float, avg: float, stddev: float},
    sample_size: int,
    agency_specific: bool,
    competitor_low: float or null,
    reytech_historical: float or null,
    win_rate_at_price: float or null,
    reasoning: str  (plain English, 1-2 sentences)
  }
  Side effects: none (read-only)
  """

  Matching priority (in order):
  1. Exact NSN match in won_quotes_kb
  2. Exact MFG number match
  3. Description keyword match:
     - Extract 3-5 meaningful words (strip UOMs, numbers, 
       common words: the/a/and/or/for/with/of)
     - Match ALL extracted words (AND logic, not OR)
     - If <3 results: relax to ANY 3 words (OR logic)

  Agency filtering:
  - If agency provided: filter to agency first
  - If agency results >= 3: use agency_specific=True
  - If agency results < 3: expand to all agencies, 
    agency_specific=False

  Confidence scoring (from INTELLIGENCE_ROADMAP.md):
  - sample_size >= 10 AND agency_specific → high (0.85-1.0)
  - sample_size >= 5 OR agency_specific   → medium (0.6-0.84)
  - sample_size >= 2                      → low (0.3-0.59)
  - sample_size < 2                       → insufficient (0.0)

  Price recommendation formula (from INTELLIGENCE_ROADMAP.md):
  base = agency_avg if agency_specific else all_agency_avg
  recommended = base * 0.97
  if competitor_low: floor = competitor_low * 1.02
  final = max(recommended, floor) if floor else recommended

  reasoning examples:
  "12 CalVet transactions. Avg win: $44.20, range $38-$51.
   Competitor low: $41.50. Recommend $42.87 (3% under market)."
  "2 matches found (low confidence). Avg $89.00 across 
   all agencies. Recommend verifying before submitting."

  get_agency_profile(agency: str, 
                     state: str = 'CA',
                     tenant_id: str = 'reytech') -> dict:
  """
  Input: agency name
  Output: {
    agency: str,
    state: str,
    total_spend: float,
    top_items: [{description, avg_price, frequency, 
                 last_purchased}],
    top_vendors: [{vendor, win_count, total_value, 
                   market_share_pct}],
    reytech_wins: int,
    reytech_value: float,
    price_sensitivity: 'high|medium|low',
    buying_cycles: [int],  # months 1-12
    buyer_contacts: [{name, email, items_purchased}]
  }
  Side effects: none
  """

  price_sensitivity logic:
  - stddev/avg > 0.3 → high (wide price variance, shop around)
  - stddev/avg > 0.15 → medium
  - else → low (consistent pricing, loyalty buyer)

  get_competitor_analysis(
      item_description: str,
      agency: str = None
  ) -> dict:
  """
  Output: {
    competitors: [{
      vendor_name: str,
      win_count: int,
      avg_price: float,
      price_vs_reytech: float,  # % diff, negative = cheaper
      agencies: [str],
      weakness: str or null  # where they lose
    }],
    reytech_position: 'price_leader|competitive|expensive|unknown',
    market_share_estimate: float or null,
    recommendation: str
  }
  """

DAL functions to add in src/core/dal.py:
  get_won_quotes_by_item(description, agency=None, 
                          nsn=None, limit=50,
                          tenant_id='reytech') -> list
  get_competitor_by_item(description, agency=None,
                          tenant_id='reytech') -> list
  get_agency_profile_data(agency, state='CA',
                           tenant_id='reytech') -> dict
  get_buyer_contacts(agency, tenant_id='reytech') -> list

All DAL functions: parameterized queries, tenant_id filter,
try/except with log.error(), return [] or {} on error.

Tests in tests/test_pricing_oracle.py:
  - get_win_price returns dict with all 9 required keys
  - confidence='insufficient_data' when 0 matches found
  - confidence='high' when seeded with 10+ agency matches
  - recommended_price is below avg (3% discount applied)
  - competitor_low sets floor correctly
  - get_agency_profile returns valid dict for unknown agency
  - reasoning is non-empty string in all cases

Commit: "feat(oracle): pricing oracle with confidence 
         scoring and competitor analysis"

── STEP 2: Wire oracle into PC auto-pricing ─────────────────
In routes_pricecheck.py, find where items get auto-priced.
After existing sources run (catalog, SCPRS lookup, Amazon):

  from src.knowledge.pricing_oracle import get_win_price

  oracle = get_win_price(
      item_description=item.get('description', ''),
      agency=pc.get('institution'),
      nsn=item.get('nsn'),
      mfg_number=item.get('item_number')
  )

  # Always store oracle data (for display)
  item['oracle_price'] = oracle['recommended_price']
  item['oracle_confidence'] = oracle['confidence_label']
  item['oracle_confidence_score'] = oracle['confidence']
  item['oracle_reasoning'] = oracle['reasoning']
  item['oracle_sample_size'] = oracle['sample_size']
  item['competitor_low'] = oracle['competitor_low']

  # Only override recommended_price when warranted
  # (from INTELLIGENCE_ROADMAP.md confidence rules)
  if oracle['confidence_label'] in ('high', 'medium'):
      existing = float(item.get('pricing', {})
                      .get('recommended_price') or 0)
      if (oracle['recommended_price'] > 0 and 
          (existing == 0 or 
           oracle['recommended_price'] < existing)):
          item['pricing']['recommended_price'] = \
              oracle['recommended_price']
          item['pricing']['price_source'] = \
              f"oracle_{oracle['confidence_label']}"

  # Never override for low/insufficient confidence
  # existing source is more reliable than thin data

In src/templates/pc_detail.html, per line item add:
  Confidence badge (colored, right of price field):
    green  = high   → "✓ High confidence (N matches)"
    yellow = medium → "~ Medium confidence (N matches)"  
    gray   = low    → "↓ Low confidence (N matches)"
    none   = insufficient → don't show badge

  Competitor context (below price, muted text):
    "Competitor low: $X.XX" if competitor_low exists
    Only show when confidence is medium or high

  Oracle tooltip on badge hover:
    Show oracle['reasoning'] as tooltip text

  Price history toggle (collapsed by default):
    "▸ History" link → fetch 
    /api/v1/pc/{pc_id}/item/{item_number}/history
    → render: Date | Agency | Price | Won/Lost | Vendor

Commit: "feat(pc): oracle enrichment on PC pricing 
         with confidence badges and competitor context"

── STEP 3: Buyer intelligence engine ────────────────────────
Create src/agents/buyer_intel_agent.py

  find_opportunity_buyers(
      product_catalog_items: list = None,
      min_spend: float = 5000,
      tenant_id: str = 'reytech'
  ) -> list[dict]:
  """
  Finds buyers who purchase items Reytech sells 
  but have NOT sent Reytech an RFQ.
  Input: optional item list (defaults to product_catalog)
  Output: sorted list of opportunity dicts
  Each dict: {
    buyer_name: str,
    buyer_email: str,
    agency: str,
    state: str,
    matching_items: [str],
    estimated_annual_spend: float,
    last_purchase: str,
    rfq_count: int,  # how many RFQs already sent to Reytech
    reytech_customer: bool,
    priority_score: float,  # 0-100
    outreach_reason: str   # plain English, 1 sentence
  }
  Side effects: none
  """

  Priority score formula:
    base = 0
    if estimated_annual_spend > 50000: base += 40
    elif estimated_annual_spend > 10000: base += 30
    elif estimated_annual_spend > 5000:  base += 20
    if last_purchase within 90 days:  base += 25
    elif last_purchase within 180 days: base += 15
    if len(matching_items) > 5:  base += 20
    elif len(matching_items) > 2: base += 10
    if reytech_customer:         base += 15  # cross-sell
    if rfq_count == 0:           base += 10  # net new
    return min(base, 100)

  outreach_reason examples:
  "Purchased X-Restraint packages 3x from competitor 
   last year ($12K). No Reytech RFQ on file."
  "CalVet Fresno buyer — active in 6 Reytech categories,
   $45K annual spend, last purchase 60 days ago."

  enrich_rfq_with_buyer_intel(
      rfq_data: dict,
      tenant_id: str = 'reytech'
  ) -> dict:
  """
  Enriches incoming RFQ with buyer context.
  Called automatically on RFQ creation.
  Input: rfq_data dict
  Output: rfq_data with buyer_intel key added:
    rfq_data['buyer_intel'] = {
      known_buyer: bool,
      historical_purchases: [{item, date, price, vendor}],
      estimated_annual_spend: float,
      agency_profile: {from get_agency_profile},
      competitive_notes: str,
      opportunity_score: float
    }
  Side effects: none (does not save, caller saves)
  """

Wire enrich_rfq_with_buyer_intel() into RFQ creation in
src/api/dashboard.py — after RFQ is saved, before 
returning success:
  try:
      from src.agents.buyer_intel_agent import \
          enrich_rfq_with_buyer_intel
      rfq_data = enrich_rfq_with_buyer_intel(rfq_data)
      # re-save with enrichment
      dal.save_rfq(rfq_data)
  except Exception as e:
      log.warning('Buyer intel enrichment failed: %s', e)
      # never block RFQ creation

Add API endpoint to routes_v1.py:
  GET /api/v1/growth/opportunities
    Query params: limit (default 20), min_score (default 30)
    Returns: api_response(find_opportunity_buyers()[:limit])
    Auth: X-API-Key or Basic Auth

Add to home.html — "Growth" card (below existing queues):
  Title: "Growth Opportunities"
  Show top 3 buyers by priority_score
  Each row: agency | items match count | est. spend | score
  "View all" link → /intelligence/dashboard#opportunities
  Refresh: pulled from /api/v1/growth/opportunities on load
  If 0 results: "No opportunities found — harvest may need 
  to run" with link to trigger harvest

Tests in tests/test_buyer_intel.py:
  - find_opportunity_buyers returns list with required keys
  - priority_score between 0-100
  - enrich_rfq_with_buyer_intel adds buyer_intel key
  - known_buyer=True when email matches buyer_intel
  - Does not raise when buyer not found (graceful)

Commit: "feat(intel): buyer intelligence engine + 
         growth opportunities feed on home dashboard"

── STEP 4: Agency profile page ──────────────────────────────
Add GET /agency/<agency_name> → 
    src/templates/agency_profile.html (already exists)

Populate with data from get_agency_profile():
  Header: agency name, state, total spend tracked
  Section 1 — Reytech relationship
    Wins, total value, win rate vs. competitors
  Section 2 — Top items purchased
    Table: item, avg price, frequency, last purchased
  Section 3 — Buyer contacts
    Table: name, email, items they buy, last purchase
    "Outreach" button per contact → existing outreach flow
  Section 4 — Competitors at this agency
    Table: vendor, win count, avg price, market share
  Section 5 — Buying calendar
    Which months have historically high procurement
    (helps Mike plan proactive outreach)

Link agency name on RFQ detail page → agency profile page.
Link agency name on PC detail page → agency profile page.

Commit: "feat(ui): agency profile page with intel, 
         buyers, competitors, and buying calendar"

── STEP 5: SCPRS ongoing sync update ────────────────────────
Update the weekly sync (already scheduled from Phase 1)
to also trigger intelligence reprocessing after successful pull:

In src/core/scheduler.py, after ca_scprs weekly run:
  1. run_ca_harvest() 
  2. if health_grade in ('A','B'):
       safe_reprocess()  # rebuild intel tables
       backup_to_drive()
  3. if health_grade == 'C':
       alert (degraded) but still reprocess
  4. if health_grade == 'F':
       alert (failed), skip reprocess, keep old intel

This ensures intelligence tables are always fresh
and never rebuilt from bad data.

Commit: "feat(scheduler): post-harvest intel reprocess 
         wired to health grade"

── STEP 6: QA gate ──────────────────────────────────────────
python scripts/smoke_test.py           — must be green
python scripts/check_routes.py         — 0 duplicates
python scripts/data_integrity.py       — all PASS

Verify oracle works against real data:
python3 -c "
import json, sqlite3
from src.knowledge.pricing_oracle import (
    get_win_price, get_agency_profile, 
    get_competitor_analysis
)

conn = sqlite3.connect('data/reytech.db')

# Pick the item with most historical data
item = conn.execute('''
    SELECT item_description, agency, COUNT(*) as n
    FROM won_quotes_kb 
    WHERE winning_price > 0
    GROUP BY item_description, agency
    ORDER BY n DESC LIMIT 1
''').fetchone()

if item:
    print(f'Testing oracle on: {item[0]} @ {item[1]}')
    result = get_win_price(item[0], agency=item[1])
    print(json.dumps(result, indent=2, default=str))
    
    profile = get_agency_profile(item[1])
    print(f'Agency top vendors: {len(profile[\"top_vendors\"])}')
    print(f'Buyer contacts: {len(profile[\"buyer_contacts\"])}')
    
    comp = get_competitor_analysis(item[0], item[1])
    print(f'Competitors found: {len(comp[\"competitors\"])}')
    print(f'Reytech position: {comp[\"reytech_position\"]}')
else:
    print('ERROR: No items in won_quotes_kb — run harvest first')
"

Verify growth opportunities:
python3 -c "
from src.agents.buyer_intel_agent import find_opportunity_buyers
opps = find_opportunity_buyers(min_spend=1000)
print(f'Opportunities found: {len(opps)}')
if opps:
    top = opps[0]
    print(f'Top: {top[\"buyer_name\"]} @ {top[\"agency\"]}')
    print(f'  Score: {top[\"priority_score\"]}')
    print(f'  Spend: \${top[\"estimated_annual_spend\"]:,.2f}')
    print(f'  Reason: {top[\"outreach_reason\"]}')
"

Update AUDIT.md — add Phase 2 Completion section:
  - Oracle confidence distribution
    (what % of catalog items have high/medium/low/insufficient)
  - Number of opportunity buyers found
  - Top 3 agencies by opportunity score
  - Sample oracle recommendation (item, price, confidence)
  - Competitor count tracked

Update docs/strategy/INTELLIGENCE_ROADMAP.md:
  - Mark Phase 2 complete
  - Add actual confidence distribution numbers
  - Note any items where oracle had insufficient data

Update tasks/lessons.md with new lessons.

git add -A
git commit -m "chore: Phase 2 complete — pricing oracle, confidence 
               scoring, buyer intelligence, growth opportunities,
               agency profiles, QA green"
git push origin main

═══════════════════════════════════════════════════════════════
DEFINITION OF DONE — PHASE 2
═══════════════════════════════════════════════════════════════
□ get_win_price() returns 9-key dict with honest confidence
□ Confidence scores match INTELLIGENCE_ROADMAP.md thresholds
□ Oracle only overrides price on medium/high confidence
□ Confidence badges visible on PC detail per line item
□ Competitor context shown on medium/high confidence items
□ find_opportunity_buyers() returns ranked list 0-100 scores
□ New RFQs auto-enriched with buyer_intel on creation
□ GET /api/v1/growth/opportunities returns top opportunities
□ Growth card on home dashboard (top 3)
□ Agency profile page live with intel, buyers, competitors
□ Weekly sync triggers safe_reprocess after healthy harvest
□ All tests pass, smoke green, 0 duplicate routes
□ Oracle verified against real won_quotes_kb data
□ AUDIT.md + INTELLIGENCE_ROADMAP.md + lessons.md updated
□ Pushed to main

═══════════════════════════════════════════════════════════════
PHASE 3 PREVIEW
═══════════════════════════════════════════════════════════════
Phase 3 = automated pipeline (RFQ in → oracle prices → 
          buyer context → draft sent → award tracked → 
          win/loss feeds oracle back), Layer 5 resumes
          (audit trail, rollback, god module split),
          proactive outreach agent (surfaces opportunities
          before RFQs arrive).
```
