# PC → RFQ Pricing Continuity — COMPLETE
**Started:** 2026-02-28 · **All 11 features shipped**

## F1: Auto-Link RFQ to PC on Import ✅ (3de901d)
- [x] Match PCs by sol#, requestor+items, agency+items
- [x] Port pricing (cost, bid, SCPRS, Amazon, link, supplier, MFG#)
- [x] Mark PC as converted_to_rfq (don't delete)

## F2: Price Port with Diff Detection ✅ (25bc78f)
- [x] Detect added/removed/qty-changed items vs PC
- [x] Blue banner on RFQ detail with diff counts + View PC link

## F3: QA Gate Before Package Generation ✅ (0585675)
- [x] 6 checks: bid>0, cost>0, margin≥15%, SCPRS compare, freshness, qty>0
- [x] Popup with per-item pass/warn/fail + PC diff warnings

## F4: Freshness Re-Check ✅ (a2da2ba)
- [x] Drift detection (↑↓%) when SCPRS moved since last check
- [x] Stale warning (⏰) when price data >30 days old

## F5: Pricing Recommendations ✅ (3341e9e)
- [x] 3 tiers: aggressive (-7%), recommended (-2%), safe (+5%)
- [x] Clickable in popup + "Apply Recommended" bulk button

## F6: Price Conflict Resolution ✅ (5c3646f)
- [x] All sources shown per item (Cost, SCPRS, Amazon, Catalog)
- [x] Click source → apply as cost + PC origin tracking

## F7: Pricing Audit Trail ✅ (ec07755)
- [x] price_audit table + record_audit() + get_audit_trail()
- [x] Audit log in intel popup

## F8: Stale Price Alerts ✅ (5c3646f)
- [x] GET /api/pricing-alerts (stale, unpriced, drift)
- [x] Red badge in header nav on every page

## F9: Duplicate Item Detection ✅ (5c3646f)
- [x] Recent quotes shown in intel popup (price, agency, quote#, date)

## F10: Auto-Price New RFQs ✅ (5c3646f)
- [x] Catalog match → fill typical_cost/list_price
- [x] History match → fill avg price
- [x] status="auto_priced" for human review gate

## F11: Margin Guardrails ✅ (5c3646f)
- [x] MARGIN_RULES config (15% min, 5% critical, SCPRS bands)
- [x] Real-time warnings on autosave (floating bar)
- [x] Price audits recorded on every manual edit
