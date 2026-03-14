# Phase 3 — Tenant Profile + Automated Pipeline
*Send to Claude Code after Phase 2 is complete and QA green*

---

## Where this fits
Phase 2 built the pricing oracle and buyer intelligence.
Phase 3 closes the loop: automated pipeline + tenant identity.

Sequence:
  Phase 2 → oracle prices RFQs with confidence scores
  Phase 3 → pipeline automates the flow + tenant profile
             makes the system multi-tenant ready
  Phase 4 → proactive outreach, growth engine at scale

---

## Pre-flight checklist (verify before pasting prompt)
- [ ] Phase 2 complete and pushed
- [ ] Pricing oracle returning recommendations with confidence scores
- [ ] Buyer intelligence surfacing opportunities on home dashboard
- [ ] smoke_test.py ALL green
- [ ] check_routes.py 0 duplicates
- [ ] data_integrity.py ALL PASS
- [ ] INTELLIGENCE_ROADMAP.md updated with Phase 2 numbers

---

## Prompt

```
Read AUDIT.md, tasks/lessons.md, and all files in
docs/strategy/ before starting.
Phase 2 is complete. Pricing oracle and buyer intelligence
are live. Phase 3 has two parts:

PART A — Tenant Profile System
  Identity layer for the platform. Reytech is tenant 1.
  Makes vendor search, certifications, and harvest
  tenant-aware. Foundation for future multi-tenant.

PART B — Automated Pipeline
  Close the RFQ lifecycle loop. RFQ arrives → oracle
  prices it → draft sent → award tracked → win/loss
  feeds back into oracle. Same human time, 10x throughput.

All prior guardrails apply. Reference THINKING_PRINCIPLES.md
before every architectural decision.

═══════════════════════════════════════════════════════════════
PHASE 3 — TENANT PROFILE + AUTOMATED PIPELINE
═══════════════════════════════════════════════════════════════

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART A: TENANT PROFILE SYSTEM
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

── STEP A1: tenant_profiles table ───────────────────────────
Add to migrations.py as new version:

  tenant_profiles:
    tenant_id TEXT PRIMARY KEY,

    -- Legal identity
    legal_name TEXT NOT NULL,
    dba_names TEXT,           -- JSON array of aliases
    entity_number TEXT,       -- CA SOS entity number
    entity_type TEXT,         -- S-Corp, LLC, etc.
    state_of_formation TEXT,
    formation_date TEXT,
    status TEXT DEFAULT 'active',

    -- Contact
    website TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,

    -- Vendor search terms
    -- How to find THIS company in SCPRS and USASpending
    vendor_search_names TEXT, -- JSON array
    vendor_codes TEXT,        -- JSON array of SCPRS codes

    -- Procurement certifications
    -- JSON array of cert objects:
    -- [{type, number, state, expiry, active, notes}]
    certifications TEXT,

    -- NAICS codes this business operates in
    naics_codes TEXT,         -- JSON array of strings

    -- Compliance tracking
    statement_of_info_due TEXT,   -- date, alert if overdue
    licenses_json TEXT,           -- JSON array of licenses

    -- Platform settings
    notify_phone TEXT,
    notify_email TEXT,
    base_url TEXT,
    api_key_hash TEXT,        -- hashed API key for this tenant

    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))

Seed Reytech's profile on migration:
  INSERT OR IGNORE INTO tenant_profiles (
    tenant_id, legal_name, dba_names, entity_number,
    entity_type, state_of_formation, formation_date,
    website, phone, address, city, state, zip,
    vendor_search_names, certifications, naics_codes,
    statement_of_info_due
  ) VALUES (
    'reytech',
    'REYTECH INC',
    '["Reytech Inc.", "Rey Tech Inc", "Reytech"]',
    '3799353',
    'S-Corp', 'CA', '2015-06-18',
    'https://www.reytechinc.com',
    '949-229-1575',
    '30 Carnoustie Way', 'Trabuco Canyon', 'CA', '92679',
    '["REYTECH INC","reytech inc.","reytech inc",
      "reytech","rey tech inc","rey tech"]',
    '[
      {"type":"MB","number":"2002605","state":"CA",
       "expiry":null,"active":true},
      {"type":"SB","number":"2002605","state":"CA",
       "expiry":null,"active":true},
      {"type":"SB-PW","number":"2002605","state":"CA",
       "expiry":null,"active":true},
      {"type":"DVBE","number":"2002605","state":"CA",
       "expiry":null,"active":true,
       "notes":"Service-Disabled Veteran Business Enterprise"},
      {"type":"SDVOB","number":"221449","state":"NY",
       "expiry":null,"active":true,
       "notes":"NY Service-Disabled Veteran-Owned Business"},
      {"type":"DBE","number":"44511",
       "jurisdiction":"DOT","expiry":null,"active":true,
       "notes":"Disadvantaged Business Enterprise"}
    ]',
    '["339112","339113","423450","423490","339920"]',
    '2024-06-30'
  )

Run migrations and verify:
python3 -c "
from src.core.migrations import run_migrations
run_migrations()
import sqlite3
conn = sqlite3.connect('data/reytech.db')
r = conn.execute(
    'SELECT tenant_id, legal_name, entity_number '
    'FROM tenant_profiles'
).fetchone()
print(f'Tenant: {r}')
certs = conn.execute(
    'SELECT certifications FROM tenant_profiles '
    'WHERE tenant_id=\"reytech\"'
).fetchone()
import json
certs_list = json.loads(certs[0])
print(f'Certifications: {len(certs_list)}')
for c in certs_list:
    print(f'  {c[\"type\"]} #{c[\"number\"]} '
          f'({c.get(\"state\",c.get(\"jurisdiction\",\"\"))})')
"

Commit: "feat(tenant): tenant_profiles table seeded 
         with Reytech identity and certifications"

── STEP A2: Tenant profile DAL ──────────────────────────────
Add to src/core/dal.py:

  def get_tenant_profile(tenant_id: str = 'reytech') -> dict:
      """
      Input: tenant_id
      Output: full tenant profile dict or {}
      Side effects: none
      """

  def get_tenant_vendor_names(
          tenant_id: str = 'reytech') -> list[str]:
      """
      Returns vendor_search_names for this tenant.
      Used by harvest to find company-specific wins.
      Falls back to ['reytech'] if not configured.
      """

  def get_tenant_certifications(
          tenant_id: str = 'reytech') -> list[dict]:
      """Returns active certifications for tenant."""

  def get_tenant_naics_codes(
          tenant_id: str = 'reytech') -> list[str]:
      """Returns NAICS codes for tenant."""

  def check_compliance_alerts(
          tenant_id: str = 'reytech') -> list[dict]:
      """
      Returns list of compliance items needing attention:
      - Statement of Info overdue or due within 60 days
      - Any certification with expiry within 90 days
      - Any active=false certification
      Each alert: {type, message, severity, due_date}
      """

Update pull_orchestrator.py:
  Replace hardcoded REYTECH_VENDOR_NAMES with:
    from src.core.dal import get_tenant_vendor_names
    vendor_names = get_tenant_vendor_names(tenant_id)
  
  Replace hardcoded REYTECH_NAICS_CODES with:
    from src.core.dal import get_tenant_naics_codes
    naics_codes = get_tenant_naics_codes(tenant_id)

This means all future tenants use their own names
and NAICS codes automatically. No code changes needed.

Commit: "feat(tenant): tenant profile DAL — 
         harvest now tenant-aware"

── STEP A3: Compliance alerts on home dashboard ─────────────
In src/api/dashboard.py home route, call
check_compliance_alerts() and pass to template.

In src/templates/home.html, add a compliance banner
at the very top (above all queues) IF alerts exist:

  For each alert with severity='critical':
    Red banner: "⚠️ {message} — {due_date}"

  For each alert with severity='warning':
    Yellow banner: "📋 {message} — {due_date}"

Reytech's Statement of Info was due 06/30/2024.
This should show as a critical alert immediately.
Better late than never — and this prevents future lapses.

Add GET /api/v1/tenant/compliance to routes_v1.py:
  Returns check_compliance_alerts() result.
  Used by /api/v1/health:
    "compliance": {
      "alerts": [...],
      "critical_count": n,
      "warning_count": n
    }

Commit: "feat(tenant): compliance alerts on home 
         dashboard and health endpoint"

── STEP A4: Tenant profile settings page ────────────────────
In src/templates/settings.html, add a new 
"Business Profile" section with:

  Legal Identity (read-only display):
    Legal name, entity number, state, formation date
    
  Certifications table:
    Type | Number | State/Jurisdiction | Expiry | Status
    Each row editable (expiry date + active toggle)
    "Add certification" button
    
  Vendor Search Terms:
    Editable list of how this company appears in SCPRS
    "These names are used to find your wins in procurement
     databases. Add any variations of your company name."
    
  NAICS Codes:
    Editable list with descriptions
    "These codes determine which federal contracts are 
     relevant to your business."
    
  Compliance Tracker:
    Statement of Info due date with alert if overdue
    Link to CA SOS filing portal

All saves go to POST /api/v1/tenant/profile (update only,
tenant_id cannot be changed).

Commit: "feat(settings): business profile section — 
         certifications, vendor names, NAICS, compliance"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PART B: AUTOMATED PIPELINE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

The full RFQ lifecycle in the automated state:

  Email arrives → poller parses → RFQ created
       ↓
  Oracle prices each line item (with confidence)
       ↓
  Buyer intel enriches RFQ (known buyer context)
       ↓
  If all items high/medium confidence AND 
  total value < approval_threshold:
    Auto-draft response, queue for review
  Else:
    Flag for manual pricing review
       ↓
  Mike reviews + approves (or edits)
       ↓
  Response sent → status = 'sent'
       ↓
  Award tracker monitors for outcome
       ↓
  Win: record in won_quotes, feed oracle
  Loss: record loss reason, update competitor intel

── STEP B1: Auto-pricing on RFQ creation ────────────────────
In src/api/dashboard.py, in the RFQ creation flow,
after email parsing and buyer intel enrichment:

  from src.knowledge.pricing_oracle import get_win_price

  def auto_price_rfq(rfq_data: dict,
                      tenant_id: str = 'reytech') -> dict:
      """
      Runs oracle on every line item in an RFQ.
      Updates items with oracle recommendations.
      Sets rfq_data['auto_priced'] = True if all items
      got medium/high confidence recommendations.
      Sets rfq_data['pricing_confidence'] = overall score.
      Never raises — logs and returns rfq_data unchanged
      on any error.
      """
      items = rfq_data.get('line_items', [])
      agency = rfq_data.get('agency', '')
      confidence_scores = []

      for item in items:
          try:
              oracle = get_win_price(
                  item_description=item.get('description',''),
                  agency=agency,
                  nsn=item.get('nsn'),
                  mfg_number=item.get('item_number'),
                  tenant_id=tenant_id
              )
              item['oracle_price'] = oracle['recommended_price']
              item['oracle_confidence'] = oracle['confidence_label']
              item['oracle_confidence_score'] = oracle['confidence']
              item['oracle_reasoning'] = oracle['reasoning']
              item['competitor_low'] = oracle['competitor_low']

              if oracle['confidence_label'] in ('high','medium'):
                  if not item.get('price_per_unit') or \
                     float(item.get('price_per_unit',0)) == 0:
                      item['price_per_unit'] = \
                          oracle['recommended_price']
                      item['pricing_source'] = \
                          f'oracle_{oracle["confidence_label"]}'

              confidence_scores.append(oracle['confidence'])
          except Exception as e:
              log.warning('Oracle failed for item %s: %s',
                          item.get('description','?'), e)

      if confidence_scores:
          avg = sum(confidence_scores) / len(confidence_scores)
          rfq_data['pricing_confidence'] = round(avg, 2)
          rfq_data['auto_priced'] = avg >= 0.6
      else:
          rfq_data['pricing_confidence'] = 0
          rfq_data['auto_priced'] = False

      return rfq_data

Wire auto_price_rfq() into RFQ creation:
  After: enrich_rfq_with_buyer_intel()
  Before: save_rfq()

Commit: "feat(pipeline): auto-pricing on RFQ creation 
         via oracle"

── STEP B2: Approval threshold and auto-draft ───────────────
Add to tenant_profiles table:
  approval_threshold REAL DEFAULT 5000,
  -- RFQs under this value auto-draft if fully priced
  -- RFQs over this value always require manual review

Add to src/core/pipeline.py (create if not exists):

  def should_auto_draft(rfq_data: dict,
                         tenant_id: str = 'reytech'
                         ) -> tuple[bool, str]:
      """
      Determines if an RFQ can be auto-drafted.
      Returns: (should_draft: bool, reason: str)

      Auto-draft conditions (ALL must be true):
        1. rfq_data['auto_priced'] is True
        2. rfq_data['pricing_confidence'] >= 0.6
        3. Total RFQ value <= approval_threshold
        4. No line items with confidence='insufficient_data'
        5. due_date is >= 24 hours away

      If any condition fails: returns (False, reason)
      The reason explains to Mike why manual review needed.
      """

  def create_draft_response(rfq_data: dict,
                             tenant_id: str = 'reytech'
                             ) -> dict:
      """
      Creates a draft response email for an RFQ.
      Uses existing quote_generator + email templates.
      Returns: {
        draft_id: str,
        subject: str,
        body: str,
        attachments: [pdf paths],
        rfq_id: str,
        requires_review: bool
      }
      """

  def process_rfq_pipeline(rfq_data: dict,
                             tenant_id: str = 'reytech'
                             ) -> dict:
      """
      Full pipeline for a single RFQ.
      Called after RFQ is created and saved.
      Sequence:
        1. auto_price_rfq()
        2. enrich_rfq_with_buyer_intel() (if not done)
        3. should_auto_draft()
        4. If auto-draft: create_draft_response()
                          queue for review in outbox
        5. If manual: flag with reason, 
                      add to manual_review queue
        6. fire_webhook('rfq.created', rfq_data)
        7. notify_new_rfq_sms()
        8. record_audit('rfq', id, 'pipeline_processed')
      Returns: enriched rfq_data with pipeline_result key
      """

Wire process_rfq_pipeline() into dashboard.py:
  Replace individual calls to auto_price, buyer_intel, 
  webhook, SMS with single pipeline call.
  Wrap in try/except — pipeline failure must NEVER 
  block RFQ creation.

Commit: "feat(pipeline): auto-draft with approval 
         threshold and pipeline orchestration"

── STEP B3: Manual review queue ─────────────────────────────
RFQs that fail auto-draft go to a manual review queue.
This is distinct from the main RFQ queue — it's 
specifically for pricing that needs human attention.

In src/templates/home.html, add a "Needs Pricing Review"
queue card (between existing PC and RFQ cards):

  Shows RFQs where:
    auto_priced = False OR pricing_confidence < 0.6
  
  Each card shows:
    - Solicitation number + agency
    - Why it needs review (oracle reason)
    - Due date with urgency indicator
    - "Price Now" button → goes to PC detail with 
      oracle suggestions pre-loaded

In routes_rfq.py, add filter for manual review queue:
  GET /rfqs?queue=needs_pricing
  Returns RFQs where auto_priced=False or confidence<0.6

Commit: "feat(pipeline): manual pricing review queue 
         on home dashboard"

── STEP B4: Win/loss feedback loop ──────────────────────────
This is the most important step for long-term intelligence.
Every win and loss must feed back into the oracle.

In routes_rfq.py, when RFQ status changes to 'won':
  from src.knowledge.pricing_oracle import record_win

  record_win(
      rfq_id=rfq_id,
      items=rfq_data['line_items'],
      agency=rfq_data['agency'],
      win_price_total=total_value,
      tenant_id='reytech'
  )

In routes_rfq.py, when RFQ status changes to 'lost':
  from src.knowledge.pricing_oracle import record_loss

  record_loss(
      rfq_id=rfq_id,
      items=rfq_data['line_items'],
      agency=rfq_data['agency'],
      our_price=total_value,
      competitor_name=loss_reason.get('competitor'),
      competitor_price=loss_reason.get('competitor_price'),
      tenant_id='reytech'
  )

Add to src/knowledge/pricing_oracle.py:

  def record_win(rfq_id, items, agency,
                  win_price_total, tenant_id) -> None:
      """
      Records a win in won_quotes and won_quotes_kb.
      Updates vendor_intel for Reytech (win_count++).
      Recalculates confidence scores for affected items.
      Never raises.
      """

  def record_loss(rfq_id, items, agency, our_price,
                   competitor_name=None,
                   competitor_price=None,
                   tenant_id=None) -> None:
      """
      Records a loss in won_quotes_kb 
      (reytech_won=0, competitor info).
      Updates competitor intel if competitor known.
      Never raises.
      """

This closes the intelligence loop:
  Win → oracle gets smarter about this item/agency
  Loss → competitor intel updated, 
          pricing recalibrated for next bid

Commit: "feat(oracle): win/loss feedback loop — 
         oracle improves with every RFQ outcome"

── STEP B5: Pipeline visibility on home dashboard ───────────
In src/templates/home.html, add a pipeline status bar
at the top (below compliance alerts if any):

  Today's pipeline:
  [N RFQs received] → [N auto-priced] → [N in review]
  → [N drafts ready] → [N sent today]

  Each number is a link to the filtered queue.
  Updates every 60 seconds (existing auto-refresh).

This gives Mike a one-glance view of the day's 
pipeline state without opening individual queues.

Commit: "feat(home): pipeline status bar showing 
         daily flow from received to sent"

── STEP B6: Layer 5 resume ──────────────────────────────────
Now that pipeline is in place, resume the layer roadmap.
Reference AUDIT.md for remaining Layer 5 items.

Key remaining items from Layer 5:
  - record_audit() wired into all DAL writes ✓ (done)
  - Rollback via snapshots ✓ (done)
  - routes_intel.py god module split (still needed)
  - request.get_json(force=True) across all POST routes
    (from lesson L32 — silent None returns)

Run routes_intel.py audit:
  wc -l src/api/modules/routes_intel.py
  grep -c "@bp.route" src/api/modules/routes_intel.py

If still > 3000 lines: split into domain files
  routes_crm_contacts.py
  routes_growth.py
  routes_revenue.py
  routes_intel.py (true intel routes only)

After split:
  python scripts/check_routes.py  — 0 duplicates
  python scripts/smoke_test.py    — all green

Commit: "refactor(routes): split routes_intel.py 
         into domain modules"

── STEP C: QA gate ──────────────────────────────────────────
python scripts/smoke_test.py           — ALL green
python scripts/check_routes.py         — 0 duplicates
python scripts/data_integrity.py       — ALL PASS
pytest tests/ -x -q                    — 0 failures

Add integrity checks:
  Check: tenant_profiles has reytech row with 
         certifications JSON valid
  Check: compliance alerts fire for overdue 
         statement_of_info_due
  Check: pipeline processes test RFQ without raising

Verify pipeline end-to-end:
python3 -c "
from src.core.pipeline import process_rfq_pipeline
import json

# Minimal test RFQ
test_rfq = {
    'id': 'test-pipeline-001',
    'solicitation_number': 'TEST-001',
    'agency': 'CalVet',
    'due_date': '2026-04-01',
    'line_items': [{
        'description': 'X-Restraint Package Stryker',
        'qty': 2,
        'uom': 'SET',
        'price_per_unit': 0
    }]
}

result = process_rfq_pipeline(test_rfq)
print('Pipeline result:')
print(f'  auto_priced: {result.get(\"auto_priced\")}')
print(f'  confidence: {result.get(\"pricing_confidence\")}')
items = result.get('line_items', [])
for item in items:
    print(f'  Item: {item.get(\"description\",\"?\")}')
    print(f'    Oracle: \${item.get(\"oracle_price\",0):.2f} '
          f'({item.get(\"oracle_confidence\",\"?\")})')
print(f'  Pipeline: {result.get(\"pipeline_result\",{})}')
"

Update docs/strategy/INTELLIGENCE_ROADMAP.md:
  - Mark Phase 3 complete
  - Update pipeline automation status
  - Note approval_threshold setting
  - Note win/loss feedback loop is live

Update tasks/lessons.md with new lessons.

git add -A
git commit -m "chore: Phase 3 complete — tenant profile,
               certifications, auto-pipeline, win/loss
               feedback loop, compliance alerts, QA green"
git push origin main

═══════════════════════════════════════════════════════════════
DEFINITION OF DONE — PHASE 3
═══════════════════════════════════════════════════════════════

PART A — Tenant Profile:
□ tenant_profiles table with Reytech data seeded
□ All 6 certifications stored (MB/SB/SB-PW/DVBE/SDVOB/DBE)
□ Vendor search names in tenant profile (not hardcoded)
□ NAICS codes in tenant profile (not hardcoded)
□ Compliance alerts on home dashboard
□ Statement of Info overdue alert showing
□ Settings page has Business Profile section
□ /api/v1/tenant/profile endpoint live
□ /api/v1/health includes compliance status
□ Harvest uses tenant profile for vendor names + NAICS

PART B — Automated Pipeline:
□ New RFQs auto-priced by oracle on creation
□ High/medium confidence items get recommended prices
□ RFQs under approval_threshold auto-draft if fully priced
□ Manual review queue shows RFQs needing human pricing
□ Win recorded → oracle win_count updated
□ Loss recorded → competitor intel updated
□ Pipeline status bar on home dashboard
□ routes_intel.py split into domain modules
□ request.get_json(force=True) across all POST routes

QA:
□ smoke_test.py ALL green
□ check_routes.py 0 duplicates
□ data_integrity.py ALL PASS
□ pytest 0 failures
□ Pipeline verified end-to-end with test RFQ
□ INTELLIGENCE_ROADMAP.md updated
□ Pushed to main

═══════════════════════════════════════════════════════════════
PHASE 4 PREVIEW
═══════════════════════════════════════════════════════════════
Phase 4 = Proactive outreach agent (surfaces opportunities
          before RFQs arrive), buying cycle predictions
          (which agencies buy what in which months),
          margin optimization (agency-specific markup
          based on win rate data), QB revenue sync
          verified against oracle win records.
```

---

## Compliance Feature Spec (reference for Step A3 + A4)

### What it tracks
Every item that has a date or expiry tied to Reytech's
ability to bid on contracts:

| Item | Current Value | Alert Window |
|------|--------------|--------------|
| CA Statement of Info | Due 06/30/2024 (OVERDUE) | 60 days before |
| DVBE #2002605 | No expiry on file | Verify annually |
| SB/MB #2002605 | No expiry on file | Verify annually |
| SDVOB NY #221449 | No expiry on file | Verify annually |
| DBE DOT #44511 | No expiry on file | Verify annually |
| Business license renewal | Unknown | 90 days before |
| Insurance certificates | Unknown | 90 days before |

### Alert severity levels
- CRITICAL (red): overdue OR expires within 30 days
  → Show banner on every page, not just settings
  → SMS alert to notify_phone
- WARNING (yellow): expires within 60-90 days
  → Show on home dashboard compliance card
- INFO (gray): upcoming in 90-180 days
  → Show on settings compliance section only

### Why this matters for bids
- Lapsed DVBE = cannot bid on DVBE set-aside contracts
- Lapsed SB = cannot bid on small business set-asides
- Expired DBE = cannot bid on DOT-funded projects
- Missing Statement of Info = CA SOS may suspend entity

### Feature scope (Phase 3 Step A3/A4)
1. Compliance card on home dashboard
   - Shows count of critical/warning/info items
   - Click → expands detail
   - "Mark resolved" button per item

2. Settings > Business Profile > Compliance section
   - Full table of all tracked items
   - Editable expiry dates
   - "Add compliance item" for custom items
     (insurance, bonding, local business licenses)
   - Last verified date per item
   - Link to renewal portal where available

3. Automated reminders
   - Weekly compliance check runs as scheduler job
   - SMS alert when item moves to CRITICAL
   - Never alerts more than once per day per item

4. Compliance health in /api/v1/health
   - Machine-readable for future MCP tool access
   - "compliance_health": "ok|warning|critical"
