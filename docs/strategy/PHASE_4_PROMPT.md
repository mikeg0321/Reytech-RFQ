# Phase 4 — Proactive Growth + Margin Intelligence
*Send to Claude Code after Phase 3 is complete*

---

## Pre-flight checklist
- [ ] Phase 3 complete and pushed
- [ ] Tenant profile seeded with Reytech certifications
- [ ] Automated pipeline running (new RFQs auto-priced)
- [ ] Phase 2B win/loss feedback loop recording outcomes
- [ ] won_quotes_kb has reytech_won + price_delta populated
- [ ] Daily 6am harvest running (award tracker active)
- [ ] smoke_test.py ALL green
- [ ] data_integrity.py ALL PASS

---

## What Phase 4 achieves
At this point the system reacts to RFQs when they arrive.
Phase 4 makes it proactive — it knows who is about to
buy before they send the RFQ. This is the growth engine.

---

## Prompt

```
Read AUDIT.md, tasks/lessons.md, and all files in
docs/strategy/ before starting.
Phase 3 complete. Pipeline is automated.
Phase 4 is the growth and margin intelligence layer.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PHASE 4 — PROACTIVE GROWTH + MARGIN INTELLIGENCE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

── STEP 1: Buying cycle prediction ──────────────────────────
Build src/agents/buying_cycle_agent.py

  analyze_buying_cycles(
      agency: str = None,
      tenant_id: str = 'reytech'
  ) -> list[dict]:
  """
  Analyzes historical PO dates to find when agencies
  predictably buy specific items.

  Uses scprs_po_master award_date + agency + item data.
  Groups by: agency → item category → month of year.
  Finds months with 2x or more average transaction volume.

  Output per agency/item: {
    agency: str,
    item_category: str,
    peak_months: [int],     # months 1-12
    avg_transactions: float,
    peak_transactions: float,
    confidence: float,
    next_predicted_purchase: str,  # ISO date
    days_until_next: int,
    recommended_outreach_date: str  # 30 days before
  }
  """

  get_upcoming_opportunities(
      days_ahead: int = 60,
      tenant_id: str = 'reytech'
  ) -> list[dict]:
  """
  Returns opportunities predicted to procure in
  the next N days based on buying cycle analysis.
  Sorted by: (estimated_value * confidence) DESC
  Each item includes buyer_intel contacts to reach.
  """

Schedule weekly: rebuild buying cycle predictions
every Sunday after harvest completes.

── STEP 2: Proactive outreach agent ─────────────────────────
Extend src/agents/buyer_intel_agent.py:

  generate_outreach_targets(
      days_ahead: int = 60,
      min_value: float = 5000,
      tenant_id: str = 'reytech'
  ) -> list[dict]:
  """
  Combines:
    1. find_opportunity_buyers() — buyers with no RFQ yet
    2. get_upcoming_opportunities() — buyers predicted
       to procure soon
  Returns unified ranked list of outreach targets with:
    - contact info from buyer_intel
    - predicted items and value
    - recommended outreach message angle
    - certification match (DVBE/SB eligible?)
  """

Add to home dashboard — "Outreach This Week" card:
  Top 5 targets from generate_outreach_targets(days_ahead=14)
  Each shows: agency, contact, items, est. value, cert match
  "Reach Out" button → pre-fills outreach email template
  Refreshes weekly after buying cycle update

── STEP 3: Margin optimization by agency ────────────────────
Foundation: Phase 2B win/loss feedback data.
The margin optimizer depends on the closed-loop feedback
system from Phase 2B (see INTELLIGENCE_ROADMAP.md).
won_quotes_kb.reytech_won + price_delta accumulated during
Phase 2B provides the win rate data at each price point.
Without this data, the optimizer has no signal.

Build src/knowledge/margin_optimizer.py

  get_optimal_margin(
      item_description: str,
      agency: str,
      base_cost: float,
      tenant_id: str = 'reytech'
  ) -> dict:
  """
  Recommends the optimal markup % for an item at a
  specific agency based on win rate data.

  Uses Phase 2B win/loss feedback (reytech_won, price_delta,
  winning_vendor) accumulated in won_quotes_kb to calculate
  win rates at different price points per agency.

  Logic:
  - Pull win/loss history for this item + agency
  - Calculate win rate at different price points
  - Find the highest price with >= 60% win rate
  - Compare against competitor_low (never < floor)

  Output: {
    recommended_margin_pct: float,
    expected_win_rate: float,
    price_at_margin: float,
    competitor_low: float,
    reasoning: str,
    confidence: 'high|medium|low'
  }
  """

Wire into PC auto-pricing:
  After oracle prices an item, if win/loss history exists,
  apply get_optimal_margin() to fine-tune markup.

Add margin intelligence section to agency profile page:
  "At this agency, your optimal margin is X% 
   (Y% win rate). Competitor low: $Z."

── STEP 4: Certification-based opportunity flagging ─────────
Reytech holds DVBE and SB certifications.
Set-aside contracts are only open to certified vendors.
These represent lower-competition opportunities.

In buyer_intel_agent.py:
  def flag_set_aside_opportunities(
          tenant_id: str = 'reytech'
  ) -> list[dict]:
      """
      Finds POs in scprs_po_master that were set-aside
      (DVBE, SB, SDVOB) where Reytech's certifications
      qualify but Reytech did not win.
      These are missed opportunities.

      Also finds upcoming agencies with high DVBE/SB
      spend where Reytech hasn't bid.
      """

Add "Set-Aside Opportunities" section to intelligence
dashboard — shows contracts Reytech was eligible for
but didn't bid on, ranked by value.

── STEP 5: QB revenue reconciliation ────────────────────────
QB has actual invoiced/paid revenue.
Oracle has win predictions and recorded wins.
These should match. Gaps indicate data quality issues.

In src/agents/quickbooks_agent.py, add:
  def reconcile_with_oracle(
          period_days: int = 90,
          tenant_id: str = 'reytech'
  ) -> dict:
      """
      Compares QB invoiced revenue against
      oracle won_quotes records for same period.
      Returns: {
        qb_revenue: float,
        oracle_revenue: float,
        delta: float,
        unmatched_qb: [...],   # QB revenue not in oracle
        unmatched_oracle: [...] # Oracle wins not in QB
      }
      """

Surface on QB dashboard page.
If delta > 10%: show warning with detail.

── STEP 6: QA gate ──────────────────────────────────────────
python scripts/smoke_test.py    — ALL green
python scripts/check_routes.py  — 0 duplicates
python scripts/data_integrity.py — ALL PASS

Add integrity checks:
  Check: buying_cycles data exists after analysis
  Check: set-aside opportunities flagged
  Check: QB reconciliation delta < 20%

Update INTELLIGENCE_ROADMAP.md — Phase 4 complete.
Update tasks/lessons.md.

git add -A
git commit -m "chore: Phase 4 complete — buying cycles,
               proactive outreach, margin optimizer,
               set-aside opportunities, QB reconciliation"
git push origin main

═══════════════════════════════════════════════════════════════
DEFINITION OF DONE — PHASE 4
═══════════════════════════════════════════════════════════════
□ Buying cycle predictions built from historical data
□ Upcoming opportunities surfaced 60 days ahead
□ Proactive outreach targets on home dashboard
□ Margin optimizer tuned per agency/item
□ Set-aside opportunities flagged (DVBE/SB)
□ QB revenue reconciled against oracle records
□ All QA gates green
□ Pushed to main

═══════════════════════════════════════════════════════════════
PHASE 5 PREVIEW
═══════════════════════════════════════════════════════════════
Phase 5 = multi-user with role-based access (operator
          vs admin), hiring enablement (hand off operations
          without losing control), second Railway worker
          for background agents, expand to DemandStar
          local government connector.
```
