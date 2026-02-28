# PC → RFQ Pricing Continuity — Step-by-Step Build
**Started:** 2026-02-28
**Rule:** One feature per commit. Verify compiles + works. Minimal impact.

---

## Step 1: F6 — Price Audit Trail (Foundation)
Everything else depends on tracking price changes. Build the table first.
- [ ] Add `price_audit` table to migrations (idempotent)
- [ ] `record_audit(item_desc, old_price, new_price, source, actor, rfq_id)` in db.py
- [ ] `get_audit_trail(description, part_number)` in db.py
- [ ] Compile check + verify table creates on startup
- [ ] Commit

## Step 2: F1 — Auto-Link RFQ to PC on Import
When email poller creates RFQ, find matching PC and port pricing.
- [ ] In `process_rfq_email()` (dashboard.py): after RFQ creation, before cross-queue cleanup
- [ ] Match by: sol# (exact), requestor+50% items, agency+80% items
- [ ] Port: cost, bid, SCPRS, Amazon, item_link, MFG#, supplier
- [ ] Store: `linked_pc_id`, `linked_pc_number`, `linked_pc_match_reason`
- [ ] Mark PC as `converted_to_rfq=True` (don't delete — history is valuable)
- [ ] Record ported prices to `price_audit` as source="pc_port"
- [ ] Cross-queue cleanup only removes unlinked PCs
- [ ] Compile check
- [ ] Commit

## Step 3: F2 — Price Port Diff Detection
Track what changed between PC and RFQ versions.
- [ ] During F1 port: compute diff (added, removed, qty_changed items)
- [ ] Store `pc_diff` dict on RFQ
- [ ] Add yellow banner to rfq_detail.html showing diff
- [ ] Compile check + verify template renders
- [ ] Commit

## Step 4: F3 — QA Gate Before Generate
Validate before package generation.
- [ ] `GET /api/rfq/<id>/qa-check` endpoint in routes_rfq.py
- [ ] Checks: bid>0 (FAIL), cost>0 (WARN), margin>=15% (WARN), bid vs SCPRS (WARN), freshness (WARN)
- [ ] Returns per-item pass/warn/fail
- [ ] JS: `runQaCheck()` intercepts Generate button -> fetch QA -> show popup
- [ ] Popup: summary + per-item results + "Proceed Anyway" / "Fix Issues" buttons
- [ ] Compile check + verify endpoint returns correct JSON
- [ ] Commit

## Step 5: F4 — Freshness Re-Check
When RFQ has linked PC and prices are old, flag staleness.
- [ ] On RFQ detail load: check price_history for newer data than what's on the item
- [ ] API: include `freshness` field in price-intel response
- [ ] UI: show drift indicator per item (up/down with percentage)
- [ ] Compile check
- [ ] Commit

## Step 6: F5 — Pricing Recommendations in UI
Surface existing oracle in the UI.
- [ ] Wire `_compute_recommended_price()` into price-intel API
- [ ] Show recommended/aggressive/safe tiers in price intel popup
- [ ] "Apply Recommended" button fills bid column
- [ ] Compile check
- [ ] Commit

## Step 7: F7 — Dashboard Alerts
Surface stale/drifted pricing on main dashboard.
- [ ] Add stale pricing count to dashboard data
- [ ] Add price drift count
- [ ] Show badges in header
- [ ] Compile check
- [ ] Commit

## Step 8: Wire All Recording Points
Ensure every price observation flows to price_history + catalog.
- [ ] autosave endpoint -> record_price + auto_ingest
- [ ] SCPRS lookup on RFQ -> record_price
- [ ] Amazon lookup -> record_price
- [ ] URL paste -> record_price
- [ ] Save Pricing -> record_price + auto_ingest
- [ ] Compile check
- [ ] Commit

## Step 9: Integration Test
- [ ] Verify full flow: PC with prices -> RFQ arrives -> pricing ports -> QA check -> generate
- [ ] Verify price_history grows with each action
- [ ] Verify catalog grows with new items
- [ ] Verify audit trail tracks all changes
- [ ] Push all to production
