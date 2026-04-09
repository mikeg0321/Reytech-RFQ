# Reytech-RFQ System Architecture — North Star

> **Purpose of this document:** Stop the drift. Every feature, every agent, every route
> exists to serve ONE business outcome. If code doesn't trace back to a box on this
> diagram, it's either misplaced or shouldn't exist.

---

## The Business in One Sentence

**Reytech wins government contracts by responding to price checks faster and more
accurately than competitors, then fulfills those orders profitably.**

---

## The Four Domains

Everything in this app belongs to exactly ONE of four domains. If something touches
two domains, it lives in the one where the *decision* happens — the other domain
consumes the result via a clean interface.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        REYTECH-RFQ SYSTEM                          │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────┐│
│  │  1. INTAKE    │→│ 2. PRICING   │→│ 3. QUOTING │→│ 4. FULFILL ││
│  │              │  │  & INTEL     │  │  & SENDING │  │  & LEARN  ││
│  └──────────────┘  └──────────────┘  └────────────┘  └───────────┘│
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                    CATALOG (shared spine)                       ││
│  │  Every domain reads from and writes back to the catalog.       ││
│  │  The catalog is the bible. It gets richer with every quote.    ││
│  └─────────────────────────────────────────────────────────────────┘│
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                    CRM (shared context)                        ││
│  │  Buyer/agency/institution context available to all domains.    ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

---

## Domain 1: INTAKE — "What are they asking for?"

### North Star Outcome
A buyer sends an email with a 704 PDF. Within 60 seconds, the system has:
- Classified the document (PC vs RFQ vs PO vs generic)
- Parsed every line item with identifiers extracted
- Created a PC or RFQ record with status `new`
- Matched to the correct agency/institution
- NOT created duplicates

### Decision Flow
```
Email arrives (Gmail poll)
  │
  ├─ Has PDF attachment?
  │   ├─ Is AMS 704? ──────→ Parse as PRICE CHECK
  │   │   ├─ Multi-PC (bundled)? → Split into bundle, shared bundle_id
  │   │   └─ Single PC → Create one PC record
  │   ├─ Is 704B? ──────────→ Parse as RFQ
  │   ├─ Is Purchase Order? → Create ORDER (domain 4)
  │   └─ Unknown format ───→ Flag for manual review
  │
  ├─ Dedup check: Does PC/RFQ already exist for this email thread?
  │   ├─ Yes → Skip (log duplicate)
  │   └─ No  → Proceed
  │
  └─ Resolve institution: buyer email → agency (CCHCS/CDCR/CalVet/etc.)
```

### What INTAKE owns
| Component | File | Purpose |
|-----------|------|---------|
| Email polling | `email_poller.py` | Gmail API, thread tracking, dedup |
| Document classification | `auto_processor.py` | PDF type detection |
| 704 parsing | `forms/price_check.py` | Line item extraction, identifier parsing |
| RFQ parsing | `forms/generic_rfq_parser.py` | RFQ attachment parsing |
| Multi-PC splitting | `forms/price_check.py:parse_multi_pc()` | Bundle detection |
| Institution resolution | `core/institution_resolver.py` | Email → agency mapping |
| PC/RFQ record creation | `routes_pricecheck.py` (create) | Database writes |
| Bundle management | `core/pc_rfq_linker.py` | Bundle lifecycle |

### What INTAKE does NOT own
- Pricing items (that's Domain 2)
- Generating response PDFs (that's Domain 3)
- Anything about orders or fulfillment
- SCPRS data pulls (that's intelligence, Domain 2)
- Growth/prospecting (not part of the quote pipeline)

### Canonical Status Flow
```
email_received → parsed → items_extracted → [ready_for_pricing]
```

---

## Domain 2: PRICING & INTELLIGENCE — "What should we charge?"

### North Star Outcome
For every line item on a PC/RFQ, the system determines:
1. **What exact product is this?** (identification — confidence score)
2. **What does it cost us?** (supplier cost — from catalog or live lookup)
3. **What should we bid?** (markup strategy — from oracle + win history)

Human review time per PC: **under 2 minutes** for a 10-item PC.

### Decision Flow — Per Item
```
Item from INTAKE (description + identifiers)
  │
  ├─ IDENTIFY (waterfall — stops at first hit):
  │   1. UPC in catalog?           → confidence 0.99
  │   2. MFG# in catalog?         → confidence 0.95
  │   3. Supplier SKU in catalog?  → confidence 0.90
  │   4. ASIN in catalog?         → confidence 0.90
  │   5. Description search        → confidence 0.50-0.85
  │   6. Fuzzy token match         → confidence 0.35-0.50
  │   7. Grok/LLM validation       → confidence varies
  │   8. Manual (flag yellow/red)  → confidence 0.00
  │
  ├─ COST (only from real supplier sources):
  │   Priority: catalog_cost > web_cost > vendor_cost > won_quote_cost
  │   NEVER: SCPRS price (that's what STATE paid, not our cost)
  │   NEVER: Amazon retail (reference only, not wholesale)
  │   Guard: if cost > 3x reference → flag as bad match
  │
  ├─ MARKUP (oracle decision):
  │   ├─ Has SCPRS history? → Price relative to market (undercut ceiling)
  │   ├─ Has win history?   → Use calibrated win curve
  │   ├─ No data?           → Conservative floor markup (20%)
  │   └─ Portfolio pass: balance total quote margin across items
  │
  └─ WRITE BACK TO CATALOG: every lookup enriches the catalog permanently
      (ASIN, UPC, MFG#, supplier SKUs, images, price history)
```

### What PRICING & INTEL owns
| Component | File | Purpose |
|-----------|------|---------|
| Item identification | `agents/item_identifier.py` | Waterfall matching |
| Enrichment pipeline | `agents/pc_enrichment_pipeline.py` | 8-step auto-enrich |
| Pricing oracle | `core/pricing_oracle_v2.py` | Markup strategy, calibration |
| SCPRS intelligence | `agents/scprs_*.py` (all 8) | Market reference data |
| Product catalog | `agents/product_catalog.py` | Catalog CRUD, enrichment |
| Won quotes history | `knowledge/won_quotes_db.py` | Historical pricing |
| Amazon/supplier lookups | `agents/item_link_lookup.py` | Live price discovery |
| Cost reduction | `agents/cost_reduction_agent.py` | Find cheaper suppliers |
| Oracle calibration | V3 in `pricing_oracle_v2.py` | Win-rate self-tuning |

### What PRICING does NOT own
- Parsing PDFs (that's INTAKE)
- Generating filled 704 PDFs (that's QUOTING)
- Sending emails (that's QUOTING)
- Order fulfillment or tracking

### Canonical Status Flow
```
ready_for_pricing → enriching → items_identified → items_priced → [ready_for_review]
```

### SCPRS: The Intelligence Engine (not a separate domain)
SCPRS is a DATA SOURCE for Domain 2. It provides:
- Market ceiling prices (what state paid others)
- Buyer purchasing patterns
- Competitor identification
- Product identification (UPC, MFG# from FI$Cal records)

**8 SCPRS modules exist because the data source is complex, not because they're
separate features.** They should funnel through ONE orchestrator:

```
scprs_universal_pull.py ─── bulk agency pulls
scprs_lookup.py ─────────── single-item price lookup
scprs_browser.py ────────── Selenium automation (detail pages)
scprs_intelligence_engine ─ pattern detection on pulled data
scprs_scanner.py ────────── continuous monitoring
scprs_public_search.py ──── fallback public records
scprs_scraper_client.py ── remote scraper wrapper
connectors/ca_scprs.py ──── connector interface

All → feed into: product_catalog + pricing_oracle_v2
```

---

## Domain 3: QUOTING & SENDING — "Package it and send it"

### North Star Outcome
One click generates a complete, accurate, agency-compliant response package
and sends it to the buyer. The quote looks professional. The forms are correctly
filled. The signature is in the right place.

### Decision Flow
```
User clicks "Generate" on a priced PC/RFQ
  │
  ├─ Which agency? → Determines required forms
  │   ├─ CCHCS: 703B/C + 704B + Bid Package + Quote
  │   ├─ CDCR:  703B/C + 704B + Bid Package + Quote
  │   └─ Other: 704 + Quote (minimal)
  │
  ├─ Generate each document:
  │   ├─ Fill AMS 704 with priced items (overlay for overflow pages)
  │   ├─ Fill compliance forms (703B/C, bid package)
  │   ├─ Apply signatures (form-specific field placement)
  │   ├─ Generate Reytech quote PDF (quote_generator.py)
  │   └─ Bundle check: if multi-PC → merge into single combined PDF
  │
  ├─ QA gate: form_qa.py validates all fields filled
  │   ├─ Pass → Enable "Finalize" button
  │   └─ Fail → Block finalize, show what's missing
  │
  ├─ User reviews → clicks "Send"
  │   ├─ Email sent via Gmail API (NO app signature — Gmail handles it)
  │   ├─ Quote number assigned (atomic counter)
  │   ├─ Sent documents versioned (pdf_versioning.py)
  │   └─ Status → "sent", follow-up timer starts
  │
  └─ PC → RFQ conversion (if PC becomes formal RFQ):
      ├─ deepcopy PC → RFQ (same items, same prices)
      ├─ Bundle siblings linked
      └─ RFQ gets its own lifecycle from here
```

### What QUOTING owns
| Component | File | Purpose |
|-----------|------|---------|
| 704 filling | `forms/price_check.py:fill_ams704()` | Item grid + overflow |
| Form filling | `forms/reytech_filler_v4.py` | 703B/C, bid package, compliance |
| Quote PDF | `forms/quote_generator.py` | Reytech branded quote |
| Bundle merging | `forms/price_check.py:merge_bundle_pdfs()` | Multi-PC combined PDF |
| Form QA | `forms/form_qa.py` | Field validation, blocking gate |
| Signature overlay | `forms/reytech_filler_v4.py` | Positional signatures |
| Agency config | `core/agency_config.py` | Required forms per agency |
| Email sending | `routes_pricecheck.py`, `routes_rfq.py` | Gmail API dispatch |
| Quote numbering | `forms/quote_generator.py` | Atomic counter |
| PDF versioning | `forms/pdf_versioning.py` | Sent document tracking |
| PC→RFQ conversion | `core/pc_rfq_linker.py` | Lifecycle transition |

### What QUOTING does NOT own
- Determining prices (that's PRICING)
- Parsing incoming documents (that's INTAKE)
- Order creation or tracking (that's FULFILL)

### Canonical Status Flow
```
ready_for_review → generating → qa_check → [approved | qa_failed]
approved → sending → sent → [follow_up_scheduled]
```

---

## Domain 4: FULFILLMENT & LEARNING — "We won. Now deliver and get smarter."

### North Star Outcome
When Reytech wins an award, the system:
1. Creates an order with line items from the winning quote
2. Tracks fulfillment (dropship — no warehouse)
3. Records margins (actual cost vs bid price)
4. **Feeds outcome back into the pricing oracle** (THE FLYWHEEL)

### Decision Flow
```
Award detected (email or manual)
  │
  ├─ Create ORDER from winning quote
  │   ├─ Explode items into order_line_items (normalized)
  │   ├─ Set fulfillment_type = dropship
  │   └─ Link back to source PC/RFQ
  │
  ├─ For each line item:
  │   ├─ Select vendor (lowest cost from catalog)
  │   ├─ Place PO with vendor (manual or auto)
  │   ├─ Track shipment (carrier + tracking#)
  │   └─ Confirm delivery
  │
  ├─ Financial tracking:
  │   ├─ Per-line margin = bid_price - actual_cost
  │   ├─ Order total margin
  │   └─ Revenue recognition
  │
  └─ LEARNING (the flywheel):
      ├─ mark_won() → oracle_calibration updates win curve
      ├─ mark_lost() → oracle adjusts ceiling DOWN
      │   └─ If cost-driven loss → cost_reduction_agent fires
      ├─ Won items → catalog enrichment (verified products)
      ├─ Won prices → won_quotes_db (historical reference)
      └─ Buyer pattern → buyer_intelligence (agency profile)
```

### What FULFILLMENT owns
| Component | File | Purpose |
|-----------|------|---------|
| Order DAL | `core/order_dal.py` | All order CRUD, single source of truth |
| Order routes | `routes_orders_full.py` | Order lifecycle endpoints |
| Award detection | `agents/award_tracker.py` | PO email classification |
| Vendor ordering | `agents/vendor_ordering_agent.py` | Supplier PO placement |
| Delivery tracking | `order_dal.py:delivery_log` | Carrier confirmation |
| Margin calculation | `order_dal.py` | Per-line and total margins |
| Win/loss recording | `core/quote_lifecycle_shared.py` | mark_won/mark_lost |
| Oracle feedback | `core/pricing_oracle_v2.py` (V3) | Calibration updates |
| Cost reduction | `agents/cost_reduction_agent.py` | Post-loss supplier hunt |
| Follow-up engine | `agents/follow_up_engine.py` | Post-send follow-ups |

### Canonical Status Flow
```
sent → [won | lost | expired | no_response]
won → order_created → items_ordered → shipping → delivered → invoiced → paid
lost → loss_recorded → oracle_adjusted → [cost_reduction_triggered?]
```

---

## The Flywheel — Why This All Matters

```
    ┌─────────────────────────────────────────────────┐
    │                                                   │
    ▼                                                   │
  INTAKE ──→ PRICING ──→ QUOTING ──→ FULFILL          │
    │           │            │           │              │
    │           │            │           ├─ win → oracle calibrates UP
    │           │            │           ├─ loss → oracle calibrates DOWN
    │           │            │           └─ items → catalog enriched
    │           │            │                          │
    │           ▼            │                          │
    │      ┌─────────┐      │                          │
    │      │ CATALOG  │◄─────┘──────────────────────────┘
    │      │ (bible)  │
    │      └────┬────┘
    │           │
    │    richer catalog = faster identification
    │    better oracle = better margins
    │           │
    └───────────┘  ← next PC benefits from ALL prior learning
```

**This is the north star.** Every feature should make this flywheel spin faster.
If a feature doesn't feed the flywheel, question whether it belongs.

---

## What Exists OUTSIDE the Core Pipeline

These features are real but are SUPPORT functions, not the pipeline itself.
They should never be built at the expense of pipeline quality.

### Support: Growth & Prospecting
- **Purpose:** Find new buyers and agencies to send quotes to
- **Relationship:** Feeds INTAKE with leads, but doesn't process quotes
- **Files:** `growth_agent.py`, `lead_gen_agent.py`, `lead_nurture_agent.py`
- **Risk:** Has grown to 4,211+ LOC with 104 functions. Disproportionate to usage.

### Support: CRM & Contacts
- **Purpose:** Track buyer relationships, contact info, communication history
- **Relationship:** Provides context to QUOTING (who to send to) and PRICING (buyer patterns)
- **Files:** `routes_crm.py`, contacts/activity_log tables

### Support: Analytics & Reporting
- **Purpose:** Dashboards showing pipeline health, revenue, win rates
- **Relationship:** READ-ONLY consumer of all four domains' data
- **Files:** `routes_analytics.py`, `routes_intel.py`
- **Risk:** 178 routes in routes_intel.py alone. Most are read-only dashboards that
  could be consolidated into fewer pages with tabs.

### Support: Operations
- **Purpose:** QuickBooks sync, backups, system health, settings
- **Files:** `quickbooks_agent.py`, `gdrive.py`, `scheduler.py`, `routes_system.py`

---

## The Overlap Problem — Mapped

Here's where functionality currently bleeds across domain boundaries:

### 1. Pricing logic lives in 4 places
| Location | What it does | Should be |
|----------|-------------|-----------|
| `core/pricing_oracle_v2.py` | Markup strategy, calibration | **AUTHORITATIVE** |
| `knowledge/pricing_oracle.py` | Original pricing engine | **RETIRE** — superseded by V2 |
| `knowledge/pricing_intel.py` | Trend analysis | Fold into V2 or keep as read-only analysis |
| `knowledge/margin_optimizer.py` | Margin math | Fold into V2 |

### 2. SCPRS data accessed through 8 doors
All 8 SCPRS modules should funnel through ONE interface that the pricing oracle
and catalog consume. Today they're called directly from routes, agents, and
intelligence engines independently.

### 3. Order management split across 3 route files
| File | Routes | Should be |
|------|--------|-----------|
| `routes_orders_full.py` | 52 | **AUTHORITATIVE** — all order CRUD |
| `routes_order_tracking.py` | 9 | **MERGE INTO** routes_orders_full |
| `routes_orders_enhance.py` | 12 | **MERGE INTO** routes_orders_full |

### 4. Intelligence/analytics split across 3 route files
| File | Routes | Should be |
|------|--------|-----------|
| `routes_analytics.py` | 80 | **KEEP** — dashboards |
| `routes_intel.py` | 178 | **SPLIT**: SCPRS data endpoints → pricing domain; dashboards → analytics |
| `routes_growth_intel.py` | 11 | **MERGE INTO** routes_analytics or routes_growth_prospects |

### 5. RFQ parsing done twice
| File | What | Should be |
|------|------|-----------|
| `forms/generic_rfq_parser.py` | 973 LOC, full parser | **KEEP** — primary parser |
| `forms/rfq_parser.py` | 241 LOC, metadata only | **MERGE INTO** generic_rfq_parser |

### 6. Product data in two models
| Model | Table | Should be |
|-------|-------|-----------|
| `catalog.py:products` | products | **Legacy** — simple SKU master |
| `product_catalog.py` | product_catalog | **AUTHORITATIVE** — rich catalog |
| | won_quotes | Pricing history (keep separate, feeds oracle) |
| | scprs_catalog | SCPRS reference data (keep separate, cross-refs into catalog) |

---

## Page Inventory — What Users Actually Need

### Essential Pages (the pipeline)
| Page | Domain | Purpose | Status |
|------|--------|---------|--------|
| **Home** | All | Dashboard, recent PCs, alerts | Needs: PCs showing |
| **PC Detail** | Intake+Pricing | View/price a single price check | Working |
| **PC Bundle** | Intake+Quoting | Multi-PC combined view | Working |
| **RFQ Detail** | Quoting | View/finalize/send an RFQ | Working |
| **Quotes DB** | Quoting | All sent quotes, status tracking | Working |
| **Orders** | Fulfillment | Order lifecycle, margins, tracking | Rebuilt V2 |
| **Search** | All | Global lookup | Working |

### Consolidation Targets
| Current Pages | Merge Into | Why |
|---------------|-----------|-----|
| Analytics, Revenue, Win/Loss | **Analytics** (tabbed) | Same data, different views |
| Business Intel, Competitor Intel, Loss Intelligence | **Market Intel** (tabbed) | All SCPRS-derived |
| Vendors, Supplier Performance, Catalog, Pricing Intel | **Catalog** (tabbed) | All product/supplier data |
| Buyers, Prospects, Prospect Detail | **CRM** (tabbed) | All contact management |
| Growth Discovery, Growth Intelligence, Lead Gen | **Growth** (tabbed) | All prospecting |
| PO Tracking, PO Detail | **Orders** (already done) | PO tracking is subset of orders |

### Target: ~12 pages
1. Home
2. PC Queue (list of price checks)
3. PC Detail / Bundle
4. RFQ Detail
5. Quotes DB
6. Orders
7. Catalog (products + suppliers + pricing intel)
8. Analytics (pipeline + revenue + win-loss)
9. Market Intel (SCPRS + competitors + loss patterns)
10. CRM (contacts + buyers + prospects)
11. Growth (leads + nurture + outreach)
12. Settings / Admin

---

## Decision Framework: "Should We Build This?"

Before adding ANY feature, answer these questions:

```
1. Which domain does this belong to?
   □ Intake  □ Pricing  □ Quoting  □ Fulfillment  □ Support

2. Does it make the flywheel spin faster?
   □ Faster identification (catalog enrichment)
   □ Better pricing (oracle accuracy)
   □ Faster quoting (less human review time)
   □ Better fulfillment (margin tracking, learning)
   □ None of the above → QUESTION WHETHER TO BUILD IT

3. Does equivalent functionality already exist?
   □ Check this document's overlap map
   □ grep the codebase for similar function names
   □ If yes → ENHANCE existing, don't duplicate

4. What's the smallest version that works?
   □ Can this be 1 route + 1 template change?
   □ Does this need a new agent, or can an existing agent do it?
   □ Does this need a new table, or can existing tables hold the data?

5. What breaks if this is wrong?
   □ PDF output (HIGH RISK — measure before drawing)
   □ Pricing (HIGH RISK — test with real numbers)
   □ Email sending (HIGH RISK — can't unsend)
   □ Dashboard display (LOW RISK — cosmetic)
```

---

## Current Scale (as of 2026-04-08)

| Metric | Count | Target | Notes |
|--------|-------|--------|-------|
| Python files | 213 | ~150 | Consolidation needed |
| Lines of code | 162K | ~120K | Remove dead code + merge duplicates |
| Routes | 1,030 | ~600 | Many are unused dashboard variants |
| Templates | 60 | ~35 | Page consolidation |
| Database tables | 66 | ~50 | Merge redundant tracking tables |
| Agent modules | 70 | ~40 | Consolidate SCPRS, email, growth |
| Core modules | 44 | ~35 | Merge pricing, merge utilities |

---

## Implementation Priority

### Phase 1: Stop the bleeding (this week)
- [ ] Retire `knowledge/pricing_oracle.py` — ensure V2 is sole authority
- [ ] Merge `routes_order_tracking.py` + `routes_orders_enhance.py` → `routes_orders_full.py`
- [ ] Merge `forms/rfq_parser.py` → `forms/generic_rfq_parser.py`
- [ ] Fix Home page PC display

### Phase 2: Consolidate pages (next sprint)
- [ ] Analytics mega-page with tabs
- [ ] Catalog mega-page with tabs
- [ ] CRM mega-page with tabs
- [ ] Delete orphaned templates

### Phase 3: Unify backends (following sprint)
- [ ] SCPRS orchestrator — single interface for all 8 modules
- [ ] Centralize `_load_json` / `_save_json` / `_get_db` utilities
- [ ] Centralize all `_parse_dollar` implementations
- [ ] Audit and remove dead routes (unused dashboard endpoints)

### Phase 4: Harden the pipeline
- [ ] Post-fill PDF verifier (from feedback_form_filling.md)
- [ ] End-to-end integration test: email → parse → price → generate → send
- [ ] Oracle V5 feedback loop verification (is real data flowing?)

---

## How to Use This Document

1. **Before building:** Check which domain owns it. Read the "Should We Build This?" framework.
2. **During building:** Follow the canonical status flows. Don't invent new statuses.
3. **After building:** Verify it enriches the catalog. Verify it doesn't duplicate existing code.
4. **When confused:** The flywheel diagram is the tiebreaker. Does this make the flywheel spin faster?

---

*Last updated: 2026-04-08. This is the system-of-record for architecture decisions.
Update this document when domains change, not just code.*
