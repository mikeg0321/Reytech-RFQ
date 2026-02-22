# Reytech RFQ — Task Tracker

## Active Sprint (2026-02-21)

### Task 1: Split routes_intel.py (10,745 lines → modules)
- [ ] Analyze function groups and dependencies
- [ ] Split into: routes_intel_core, routes_intel_scprs, routes_intel_growth, routes_intel_funnel
- [ ] Update imports in dashboard.py
- [ ] Verify all routes still work
- [ ] Audit: 0 regressions

### Task 2: Run + fix test suite
- [ ] Install pytest
- [ ] Run full suite, capture failures
- [ ] Fix each failure
- [ ] All tests green

### Task 3: Follow-up automation engine
- [ ] Auto-create follow-up drafts N days after outreach
- [ ] Track which outreach got responses vs ghosted
- [ ] Surface "needs follow-up" in daily brief

### Task 4: Daily briefing + push notification
- [ ] Morning brief page: PCs needing price, aging quotes, stale outreach
- [ ] SMS/push notification with summary (Twilio or native)
- [ ] Auto-send at configurable time

### Task 5: Data quality dedup pass
- [ ] Dedup customers.json (parent/child overlap)
- [ ] Dedup CRM contacts
- [ ] Flag and merge

### Task 6: Keyboard shortcuts
- [ ] Global shortcuts: N=new quote, P=price checks, /=search
- [ ] Add to all pages via shared JS

---

## Completed (2026-02-21)
- [x] Fix PC persistence: file locking + atomic merge saves
- [x] Fix header UI: two-row layout with scrollable nav
- [x] Fix pipeline funnel: include PCs, clear stage labels
- [x] Rebuild Facility Expansion: smart names, email drafts, bulk targeting
- [x] Create CLAUDE.md, lessons.md, todo.md
- [x] Fix Expansion page crash (f-string + dict comprehension)
- [x] Audit: 99/100 A+
