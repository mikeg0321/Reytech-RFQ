# Reytech RFQ — Task Tracker

## Active Sprint (2026-02-21)

### Task 1: Split routes_intel.py (10,745 lines → modules) ✅
- [x] Analyze function groups and dependencies
- [x] Split into: routes_orders_full, routes_voice_contacts, routes_catalog_finance
- [x] Update imports in dashboard.py (8 modules now)
- [x] Verify all routes still work (39/39 pass)
- [x] Audit: 0 regressions

### Task 2: Run + fix test suite ✅
- [x] Install pytest
- [x] Run full suite, capture failures (71 initially)
- [x] Fix each failure (api_health, rate limiter, surrogate encoding, etc)
- [x] All tests green: 305 pass, 0 fail

### Task 3: Follow-up automation engine ✅
- [x] Auto-create follow-up drafts at Day 3/7/14 after outreach
- [x] Track which outreach got responses vs ghosted
- [x] Surface "needs follow-up" in daily brief
- [x] Background scheduler (hourly)
- [x] 3 API routes: /api/follow-ups/scan, /summary, /status

### Task 4: Daily briefing + push notification ✅
- [x] Morning brief page: /brief — PCs needing price, aging quotes, stale outreach
- [x] SMS push notification via Twilio REST API
- [x] SMS preview on page with send button
- [x] Nav link added to header

### Task 5: Data quality dedup pass ✅
- [x] Analyzed all data files — 0 actual duplicates
- [x] Fixed 1 email case normalization
- [x] New API: /api/data/quality — auto-fixes + reports coverage
- [x] Customers: 63, CRM: 18, Vendors: 122

### Task 6: Keyboard shortcuts ✅ (pre-existing)
- [x] Global shortcuts: h n q o p g b c i v d / s ?
- [x] Help overlay on ? key
- [x] Input field guards (skip when typing)
- [x] Present on all pages (home + _page_footer)

---

## Completed (2026-02-21)
- [x] Fix PC persistence: file locking + atomic merge saves
- [x] Fix header UI: two-row layout with scrollable nav
- [x] Fix pipeline funnel: include PCs, clear stage labels
- [x] Rebuild Facility Expansion: smart names, email drafts, bulk targeting
- [x] Create CLAUDE.md, lessons.md, todo.md
- [x] Fix Expansion page crash (f-string + dict comprehension)
- [x] Audit: 99/100 A+
