# Reytech RFQ — Task Tracker

## Active Sprint (2026-02-22) — PRD-28 WI-1 + WI-3 Integration

### GAP ANALYSIS
Agents exist. Routes exist. 45 tests pass. DB schema ready.
BUT: No cross-agent wiring. The agents are islands — nothing calls them.

### WI-1: Quote Lifecycle — Wire Into Live Workflow
- [ ] **1a** email_poller reply detection → run reply_analyzer → call process_reply_signal()
- [ ] **1b** award_monitor → call close_lost_to_competitor() on quote when loss detected
- [ ] **1c** Home page action dashboard → show expiring quotes in Urgent/Action cards
- [ ] **1d** Quote detail page → show revision history + status timeline
- [ ] **1e** Verify end-to-end

### WI-3: Lead Nurture — Wire Into Live Workflow
- [ ] **3a** lead_gen_agent → auto-start nurture on new lead creation
- [ ] **3b** scprs_intelligence_engine → rescore leads after new SCPRS data
- [ ] **3c** Prospect detail page → "Convert to Customer" button
- [ ] **3d** Growth page → nurture status badges on lead cards
- [ ] **3e** Verify end-to-end

## Execution: one sprint each, push after each
