# PC → RFQ Pricing Continuity — Implementation Plan
**Started:** 2026-02-28 · **Approach:** One feature at a time, verify before moving on

## F1: Auto-Link RFQ to PC on Import ✅ (Step 2: 3de901d)
- [x] In dashboard.py, after RFQ creation: match to existing PCs by sol#, requestor, items
- [x] Port pricing from matched PC (cost, bid, SCPRS, Amazon, item_link, supplier)
- [x] Store `linked_pc_id`, `linked_pc_number`, `pc_diff` on RFQ
- [x] Don't delete linked PCs — mark as converted_to_rfq instead

## F2: Price Port with Diff Detection ✅ (Step 3: 25bc78f)
- [x] When porting: detect added items, removed items, qty changes vs PC
- [x] Store diff on RFQ: `pc_diff = {added: [], removed: [], qty_changed: []}`
- [x] UI: blue banner on RFQ detail showing linked PC with diff counts
- [x] Per-item "_from_pc" marker for ported items

## F3: QA Gate Before Package Generation ✅ (Step 4: 0585675)
- [x] POST /api/rfq/<id>/qa-check — validate all items before generate
- [x] Checks: bid price > 0, cost > 0, margin >= 15%, bid vs SCPRS within 10%, cost age < 90d, qty > 0
- [x] Return per-item pass/warn/fail with reasons
- [x] JS intercept on Generate button → show QA popup → proceed or fix
- [x] Shows PC diff warnings in popup

## F4: Freshness Re-Check on RFQ Load ✅ (Step 5: a2da2ba)
- [x] price-intel endpoint returns freshness data (days_old, stale flag, drift)
- [x] Compare current SCPRS vs ported price, show drift per item
- [x] Drift indicators in 📊 column
- [x] Stale warning when price data > 30 days old

## F5: Pricing Recommendations in UI ✅ (Step 6)
- [x] _recommend_price() computes recommended/aggressive/safe from SCPRS/Amazon/cost
- [x] price-intel endpoint includes recommendation tiers
- [x] Clickable tiers in price intel popup
- [x] "Apply Recommended" button auto-fills all empty bid prices

## F6: Price Audit Trail ✅ (Steps 1+5: ec07755, a2da2ba)
- [x] price_audit table via migration v7
- [x] record_price_audit() + get_price_audit() in db.py
- [x] Audit log displayed in price intel popup

## F7: Dashboard Alerts — DEFERRED (lower priority)
- [ ] Stale pricing badge on dashboard
- [ ] Price drift badge on dashboard
