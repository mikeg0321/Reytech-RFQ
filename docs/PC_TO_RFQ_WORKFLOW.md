# PC → RFQ Pricing Continuity & QA/QC Plan

## Current State (What Exists)

| Capability | Status | Location |
|---|---|---|
| Manual PC→RFQ conversion | ✅ Works | `POST /api/pc/<id>/convert-to-rfq` |
| Auto-match PC↔RFQ by sol# or items | ✅ Works | `link_pc_to_rfq()` in routes_analytics |
| Pricing copies on manual convert | ✅ Works | Copies cost, bid, SCPRS, Amazon |
| `price_history` table | ✅ Exists | Records every price observation |
| `products` catalog | ✅ Exists | Auto-ingest on save (just wired) |
| Pricing Oracle (recommended prices) | ✅ Exists | `pricing_oracle.py` + `won_quotes_db.py` |
| Auto-link on RFQ import | ❌ Missing | Email poller doesn't check for matching PCs |
| Price freshness check on conversion | ❌ Missing | Copies stale prices without re-validating |
| QA/QC gate before quote send | ❌ Missing | No price sanity checks |
| Price drift alerts | ❌ Missing | No notification when prices change |
| Audit trail (PC price → RFQ price) | ❌ Partial | `_from_pc` field exists but no diff tracking |

---

## The Workflow (How It Should Work)

```
┌─────────────────────────────────────────────────────────────────┐
│  1. PC ARRIVES (email or manual)                                │
│     → Items parsed → SCPRS + Amazon lookup                     │
│     → Prices saved to price_history                             │
│     → Items auto-ingested to catalog                            │
│     → Quote generated + sent                                    │
├─────────────────────────────────────────────────────────────────┤
│  2. RFQ ARRIVES (same items, formal solicitation)               │
│     → AUTO-MATCH to existing PC by:                             │
│        a) Same solicitation/PC number                           │
│        b) Same requestor + ≥50% item overlap                   │
│        c) Same agency + ≥80% item overlap                      │
│     → PORT PRICING from PC (cost, bid, SCPRS, links)           │
│     → FLAG DIFFERENCES (new items, removed items, qty changes)  │
│     → FRESHNESS CHECK on all prices:                            │
│        - Re-check SCPRS (price may have changed)               │
│        - Re-check price_history for newer data                  │
│        - Flag items where price drifted >5%                     │
├─────────────────────────────────────────────────────────────────┤
│  3. USER REVIEWS (RFQ detail page)                              │
│     → See inherited prices with source: "From PC-2025-0892"    │
│     → See freshness alerts: "⚠️ SCPRS price dropped 12%"       │
│     → See price history: "📊 5 observations, avg $42.80"       │
│     → Adjust as needed                                          │
├─────────────────────────────────────────────────────────────────┤
│  4. QA/QC GATE (before Generate Package)                        │
│     → Check every item has a bid price                          │
│     → Check margin ≥15% (warn if lower)                        │
│     → Check bid vs SCPRS (warn if >10% above competitor)       │
│     → Check cost source is recent (<90 days)                   │
│     → Check no items orphaned (in PC but missing from RFQ)     │
│     → Generate QA report (pass/warn/fail per item)             │
├─────────────────────────────────────────────────────────────────┤
│  5. PACKAGE GENERATED + SENT                                    │
│     → All final prices recorded to price_history                │
│     → Won Quotes KB updated                                     │
│     → Catalog costs updated with actual                         │
│     → Audit log: complete pricing lineage                       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Features to Build (Priority Order)

### P0 — Must Have (blocks quoting accuracy)

**F1: Auto-Link RFQ to PC on Import**
When email poller creates an RFQ, immediately check for matching PCs:
- Match by solicitation number (exact)
- Match by requestor email + ≥50% item description overlap
- Match by agency + institution + ≥80% item overlap
- If match found: set `linked_pc_id`, port all pricing, flag as "From PC"
- Where: `email_poller.py` → after RFQ creation, before save

**F2: Price Port with Diff Detection**
When PC pricing ports to RFQ:
- Copy: supplier_cost, price_per_unit, item_link, item_supplier, SCPRS, Amazon
- Detect changes: items added in RFQ not in PC, items in PC not in RFQ, qty changes
- Store diff: `rfq.pc_diff = {added: [...], removed: [...], qty_changed: [...]}`
- UI: yellow banner on RFQ detail showing what changed

**F3: QA Gate Before Package Generation**
Before `generate-package` runs, validate:
- Every item has bid price > 0 (FAIL if not)
- Every item has supplier cost > 0 (WARN if not — can't calculate margin)
- Margin per item ≥ 15% (WARN if below, show which items)
- Bid price vs SCPRS: within 10% band (WARN if undercutting too much or too high)
- Cost source age < 90 days (WARN if stale)
- Result: popup showing pass/warn/fail with option to proceed or fix
- Where: JS intercept on Generate button → `POST /api/rfq/<id>/qa-check`

### P1 — High Value (pricing intelligence)

**F4: Freshness Re-Check on RFQ Load**
When opening an RFQ that has a linked PC:
- Background: re-run SCPRS lookup for items where last check > 7 days
- Compare new SCPRS price vs ported price
- Show drift: "↑ $3.50 (+8%)" or "↓ $1.20 (-3%)" per item
- Don't auto-change prices — just inform
- Where: async on page load, results via SSE or poll

**F5: Pricing Recommendation Engine (already exists, needs wiring)**
On RFQ detail, show per-item:
- **Recommended**: price that won similar items before
- **Aggressive**: lowest price that still wins (from won_quotes KB)
- **Safe**: price with comfortable margin based on history
- One-click "Apply Recommended" button
- Where: existing `_compute_recommended_price()` → expose in UI

**F6: Price Conflict Resolution**
When PC price ≠ new SCPRS price ≠ Amazon price:
- Show comparison table: PC cost / Current SCPRS / Current Amazon / Catalog typical
- Let user pick which source to use per item
- Record decision in audit log

### P2 — Data Quality (QA/QC)

**F7: Pricing Audit Trail**
For every price change, record:
- What: item description, old price, new price
- Who: user or system (SCPRS/Amazon/auto-lookup)  
- When: timestamp
- Why: source (PC port, SCPRS refresh, manual edit, URL paste)
- Where: new `price_audit` table or entries in `price_history` with `event_type`

**F8: Stale Price Alerts (Dashboard)**
On main dashboard, show warning badge:
- "3 RFQs have stale pricing (>30 days since last check)"
- "2 items in pipeline have SCPRS prices that changed"
- Click → goes to affected RFQ with items highlighted

**F9: Duplicate Item Detection**
When items are imported (PC or RFQ):
- Check if same item was quoted in last 90 days
- Show: "You quoted this to CalVet 45 days ago at $42.80 (won)"
- Prevents accidentally quoting same item at different prices to same agency

### P3 — Automation

**F10: Auto-Price New RFQs**
When RFQ imports and items match catalog:
- Auto-fill supplier_cost from catalog.typical_cost
- Auto-fill bid price from pricing oracle recommendation
- Auto-fill SCPRS from most recent price_history
- Set status to "auto-priced" (distinct from "priced" = human verified)
- Still requires human review before generate

**F11: Margin Guardrails**
Configurable rules:
- Minimum margin: 15% (default, configurable per agency)
- Maximum discount vs SCPRS: 5% (don't undercut too much — looks suspicious)
- Cost must have source (no blind manual entry without URL/SCPRS backing)
- Auto-apply on save, warnings if violated

---

## Data Persistence Requirements

All pricing data MUST persist across:
- App restarts (Railway redeploy) → SQLite on persistent volume ✅
- JSON ↔ SQLite sync → dual-write already exists ✅
- Price changes → `price_history` table (every observation) ✅
- Catalog growth → `products` table (auto-ingest on save) ✅ (just wired)
- PC→RFQ lineage → `linked_pc_id` / `linked_rfq_id` fields ✅

**New persistence needed:**
- `price_audit` entries (who changed what when)
- `rfq.pc_diff` (what changed between PC and RFQ versions)
- QA check results (stored on RFQ, not ephemeral)

---

## Implementation Order

| Sprint | Features | Impact |
|---|---|---|
| **Now** | F1 (auto-link) + F3 (QA gate) | Prevents bad quotes going out |
| **Next** | F2 (price port + diff) + F4 (freshness check) | Pricing continuity |
| **Soon** | F5 (recommendations in UI) + F6 (conflict resolution) | Intelligent pricing |
| **Later** | F7-F9 (audit trail, alerts, dedup) | Data quality |
| **Future** | F10-F11 (auto-price, guardrails) | Full automation |

---

## Key Principle

> Every price observation, from any source, at any time, must be recorded.
> Every pricing decision must be traceable back to its source.
> No quote should go out without passing QA checks.
> The system gets smarter with every quote — pricing history IS the competitive advantage.
