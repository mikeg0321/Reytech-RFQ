# Reytech Intelligence Roadmap
*Living document — update after every phase*

---

## End State Vision
Reytech operates as an AI-orchestrated government procurement business.
Every RFQ is priced accurately using historical win data, competitive 
intelligence, and buyer context. Growth opportunities surface automatically 
before RFQs hit the inbox. The system gets smarter with every transaction.

---

## Core Principles (reference before every sprint)

1. **Data first, features second**
   Never build intelligence features on incomplete data.
   Validate harvest counts match known business reality before proceeding.
   Reytech did $500K+ last year — the DB must reflect that before Phase 2.

2. **Every table is tenant-aware**
   All intelligence tables have tenant_id DEFAULT 'reytech'.
   This enables future white-label without schema migration.
   State + source_system on every row enables multi-state expansion.

3. **Confidence over speed**
   Oracle recommends prices with confidence scores.
   Low/insufficient confidence = do not override existing pricing.
   A wrong confident recommendation costs more than a cautious one.

4. **Public data is a permanent moat**
   SCPRS, USASpending, state procurement portals are public and free.
   4 years of historical data exists right now.
   Every competitor who doesn't harvest this is flying blind.
   Storage is cheap. Rebuild cost is enormous.

5. **The fork strategy**
   Primary codebase = Reytech optimized.
   White-label = fork the configuration, not the code.
   tenant_id=1 is Reytech. Future customers get their own tenant.
   One bug fix propagates everywhere.

---

## Phase Status

### ✅ Phase 1 — SCPRS Historical Harvest (COMPLETE — dfc1b2f)
- 2,225 POs pulled (initial — remediation in progress)
- 7 intelligence tables created with tenant_id + state columns
- Harvest runner script (idempotent, --dry-run, --health)
- Health contract: 5 checks, A/B/C/F grading
- Safe upsert: insert/update/skip tracking
- Weekly scheduler: CA Sunday 2am, federal Sunday 4am
- Missed-run detector and SMS alerts
- Google Drive backup after every harvest
- Harvest reliability sprint complete — daea5f7

### ✅ Harvest Remediation (COMPLETE — cbdf379)
Root causes fixed:
  1. supplier_name search added → found 210 Reytech POs ($2.85M)
  2. Connector registry replaces hardcoded agency lists
  3. Dynamic agency discovery → 51 CA agencies in registry
  4. Migrations now run on boot path (not gated by background agents)

Results: 210 Reytech POs, $2,852,494.91 total (was 6 / $166K)

### ✅ Platform Sprint — Connector Registry (COMPLETE — cbdf379)
  - Connector registry: 2 active (CA SCPRS, USASpending), 8 scaffolded
  - BaseConnector interface + CASCPRSConnector + USASpendingConnector
  - PullOrchestrator: unified orchestration, health checks, harvest logging
  - Agency discovery: dynamic via get_all_agencies(), no hardcoded lists
  - API: /api/v1/connectors, /api/v1/agencies, connector health in /api/v1/health
  - Activating a new state = 1 DB row + 1 adapter class. Zero for scaffolded.

### ⏳ Phase 2 — Pricing Oracle + Buyer Intelligence (NEXT)
See PHASE_2_PROMPT.md

### ⏳ Phase 3 — Automated Pipeline
Resume Layer 5+ with intelligence feeding every decision.
RFQ in → oracle prices it → buyer context added → 
draft sent → award tracked → win/loss feeds oracle.

### ⏳ Phase 4 — Growth Engine
Proactive outreach agent.
System identifies upcoming procurement cycles before RFQs arrive.
Buyer engine surfaces net-new opportunities.

---

## Data Architecture

### Pull Frequencies
| Source              | Frequency       | Schedule      | Alert on Skip |
|---------------------|----------------|---------------|---------------|
| California SCPRS    | Weekly         | Sunday 2am    | Yes           |
| Federal USASpending | Weekly         | Sunday 4am    | Yes           |
| State scaffolds     | Monthly        | 1st of month  | No            |
| Intelligence reprocess | Post-harvest | Sunday 6am   | Yes           |
| DB backup           | Daily          | Midnight      | No            |

### Health Contract Thresholds
| Agency   | Min rows/pull | Fail action        |
|----------|---------------|--------------------|
| CCHCS    | 50            | Alert + skip intel |
| CDCR     | 30            | Alert + skip intel |
| CalVet   | 20            | Alert + skip intel |
| DSH      | 10            | Alert + skip intel |
| Default  | 5             | Alert + skip intel |
| Federal  | 100           | Alert + skip intel |

Grading: A=5/5, B=4/5, C=3/5, F=≤2/5
F grade = skip intelligence reprocess for that source

### Intelligence Tables (all have tenant_id, state, source_system)
| Table                | Purpose                        | Feeds              |
|----------------------|--------------------------------|--------------------|
| scprs_po_master      | Raw PO records                 | All intel tables   |
| scprs_po_lines       | Line items per PO              | won_quotes_kb      |
| won_quotes_kb        | Item win prices by agency      | Pricing oracle     |
| vendor_intel         | Competitor profiles            | Competitor analysis|
| buyer_intel          | Who buys what, where           | Growth engine      |
| competitors          | Vendor win rates + weaknesses  | Bid strategy       |
| scprs_awards         | Normalized award records       | Agency profiles    |
| procurement_sources  | Data source registry           | Scheduler          |
| agency_registry      | All agencies + metadata        | Harvest runner     |
| harvest_log          | Pull history + health grades   | Dashboard          |
| connectors           | Data source registry + lifecycle| Orchestrator       |

### Connector Registry Pattern
Agency discovery is dynamic via `CASCPRSConnector.get_all_agencies()`.
No hardcoded agency lists anywhere. Current count: 51 CA agencies.

Adding a new data source = one DB row + one adapter class.
Activating a scaffolded connector = one UPDATE query.
No Python files exist for scaffolded states (TX, FL, NY, WA, AZ).

Active connectors: `ca_scprs` (priority 1), `federal_usaspending` (priority 2)
Scaffolded: `federal_sam`, `tx_esbd`, `fl_mfmp`, `ny_ogs`, `wa_webs`, `az_spo`, `ca_demandstar`, `ca_bonfire`

Federal scope: CA place-of-performance + Reytech NAICS codes only (pricing intelligence, not contracts).

---

## Locked Decisions (do not revisit without business reason)

### Data scope — March 2026
CA fully + federal intelligence only.
- California SCPRS: all agencies, dynamic discovery, 3 years
- Federal USASpending: CA-based locations + Reytech NAICS only
  (pricing intelligence, not contract pursuit)
- Other states: registry entries only, no active code
  Activation = one DB update when business expands

### No hardcoded agency lists — ever
Agency discovery is always dynamic via connector.get_all_agencies().
If you are typing agency names into a list, stop and build
the discovery method instead.

### Connector registry = configuration not code
Adding a data source = one DB row + one adapter class.
Activating a scaffolded source = one SQL UPDATE.
No deployment needed to add a new data source.

### Federal ≠ selling federal
Having federal pricing data makes CA pricing smarter.
Reytech is not pursuing federal contracts until a
deliberate business decision is made to do so.
Data collection and business development are separate decisions.

---

## Pricing Oracle Design

### Confidence Scoring
| Level              | Criteria                              | Action              |
|--------------------|---------------------------------------|---------------------|
| high (0.85-1.0)    | 10+ agency-specific matches           | Auto-override price |
| medium (0.6-0.84)  | 5+ matches OR agency-specific         | Override if cheaper |
| low (0.3-0.59)     | 2-4 matches                           | Show as reference   |
| insufficient (<0.3)| <2 matches                            | Do not use          |

### Price Recommendation Formula
```
base = avg(winning_prices for item+agency)
if agency_specific: use agency avg
else: use all-agency avg
recommended = base * 0.97  (3% under market to win)
floor = competitor_low * 1.02  (never below competitor + 2%)
final = max(recommended, floor)
```

---

## Business Context (reference when making architectural decisions)

**Current state:** ~$500K revenue, 1 person (Mike), manual process
**12-month goal:** 10x revenue with same headcount via automation
**Hiring trigger:** When system is stable enough to delegate operations
**Platform play:** Fork configuration not codebase when first customer emerges
**Key constraint:** Accuracy > speed. A wrong bid costs money. A slow bid costs nothing.

---

## Lessons That Shaped This Architecture

- L36: rfq.db vs reytech.db — verify which DB before migrating
- L32: request.json silently returns None — use get_json(force=True)
- L39: Test fixtures must patch module-level globals not just env vars
- L19: Anthropic API + web_search beats SerpApi for price lookup
- Harvest lesson: Always validate row counts against known business 
  reality (6 wins ≠ $500K revenue → something is missing)

Full lessons: tasks/lessons.md

---

## Future State Scaffolding

### States ready to activate (set status='active' in procurement_sources)
- TX — Texas ESBD (src/agents/states/texas_agent.py)
- FL — MyFlorida MarketPlace (src/agents/states/florida_agent.py)
- NY — NYS Contract Reporter (src/agents/states/new_york_agent.py)
- WA — WEBS (src/agents/states/washington_agent.py)
- AZ — AZ State Procurement (src/agents/states/arizona_agent.py)

### Federal (live)
- USASpending.gov — all federal contract awards
- SAM.gov — future (contract opportunities, not just awards)
- FPDS — future (federal procurement data system)

### White-label trigger conditions
1. First external customer identified
2. tenant_id already on all tables (✅ done)
3. Build permissioning (roles: admin, operator, viewer)
4. Build tenant onboarding flow
5. Fork UI/branding only — engine stays shared

---

## Phase 3 — Tenant Profile + Automated Pipeline
See PHASE_3_PROMPT.md for full prompt.

### What it delivers
- Reytech identity locked in DB (legal name, entity number,
  certifications, NAICS codes, vendor search names)
- Compliance alerts for overdue filings + cert expiry
- New RFQs auto-priced by oracle on arrival
- Approval threshold: small RFQs auto-draft, large go to review
- Win/loss feedback loop — every outcome improves the oracle
- Pipeline status bar on home dashboard
- routes_intel.py god module split

### Reytech Certifications (seeded in tenant_profiles)
| Type   | Number  | Jurisdiction | Notes                          |
|--------|---------|--------------|--------------------------------|
| MB     | 2002605 | CA           | Micro Business                 |
| SB     | 2002605 | CA           | Small Business                 |
| SB-PW  | 2002605 | CA           | Small Business Public Works    |
| DVBE   | 2002605 | CA           | Disabled Veteran Business      |
| SDVOB  | 221449  | NY           | Service-Disabled Veteran       |
| DBE    | 44511   | DOT          | Disadvantaged Business Ent.    |

### Compliance Alert
Statement of Info due: 06/30/2024 — OVERDUE.
Surface as critical alert on home dashboard immediately.

---

## Phase 4 — Proactive Growth + Margin Intelligence
See PHASE_4_PROMPT.md for full prompt.

### What it delivers
- Buying cycle predictions (who buys what, when)
- Proactive outreach targets 60 days ahead
- Margin optimizer per agency (highest price with 60%+ win rate)
- Set-aside opportunity flagging (DVBE/SB eligible contracts missed)
- QB revenue reconciliation against oracle records

---

## Phase Sequence Summary

| Phase | Focus | Prompt File | Status |
|-------|-------|-------------|--------|
| Harvest | Data collection | (in sprint) | 🔄 |
| 1 | SCPRS harvest, tables | (done) | ✅ |
| 2 | Oracle + buyer intel | PHASE_2_PROMPT.md | ⏳ |
| 3 | Tenant profile + pipeline | PHASE_3_PROMPT.md | ⏳ |
| 4 | Growth + margin intel | PHASE_4_PROMPT.md | ⏳ |
| 5 | Multi-user, hiring | (TBD) | ⏳ |

---

## Locked Decisions (do not revisit without business reason)

### Tenant identity — March 2026
All vendor search names, NAICS codes, and certifications
live in tenant_profiles table, not hardcoded anywhere.
Reytech is tenant_id='reytech'. Future tenants get their own row.

### Certification-aware bidding
DVBE and SB certifications create set-aside opportunities.
System flags contracts Reytech is eligible for but hasn't bid on.
Cert expiry tracked with alerts — a lapsed cert = lost revenue.

### Win/loss feedback is mandatory
Every RFQ outcome (won or lost) must be recorded.
The oracle is only as good as its feedback data.
Skipping this step degrades intelligence quality over time.
