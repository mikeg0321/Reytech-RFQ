# Reytech RFQ — Task Tracker

## Current Session (2026-02-21)

### Completed
- [x] Fix PC persistence: file locking + atomic merge saves
- [x] Fix header UI: two-row layout with scrollable nav
- [x] Fix pipeline funnel: include PCs, clear stage labels
- [x] Rebuild Facility Expansion: smart names, email drafts, bulk targeting
- [x] Create CLAUDE.md, lessons.md, todo.md

### Review
- PC locking: Tested _merge_save_pc, _load/_save with fcntl. Compiles. Pushed.
- Header: Both home page and _header() use new layout. Verified via f-string test.
- Pipeline: API returns combined PC+RFQ+quote stages. Legacy fields kept for compat.
- Expansion: Name parser tested against all QB entries. Person-names filtered. Compiles.

---

## Backlog — Needs User Credentials

- [ ] Set GMAIL_ADDRESS + GMAIL_PASSWORD on Railway → unlocks email polling, auto-drafts, outreach
- [ ] Set QB_CLIENT_ID/SECRET/REALM_ID/tokens on Railway → unlocks PO creation, invoice sync
- [ ] Set VAPI_API_KEY on Railway → unlocks voice outreach campaigns

## Backlog — Can Build Anytime

- [ ] Split dashboard.py (10.5K lines) into focused modules
- [ ] Run full pytest suite against production, fix failures
- [ ] Auto follow-up scheduler: N days after outreach, auto-draft follow-up
- [ ] Facility Expansion → Campaign pipeline: track opens/responses per target
- [ ] Margin calculator standalone page (currently inline only)
- [ ] Supplier lookup UI improvements
