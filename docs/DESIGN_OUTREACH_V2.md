# /outreach/next V2 — Public-Sector Procurement Lens

**Context correction (2026-04-24):** V1 shipped with SaaS-sales framing (drip campaigns, open/click signals, "fatigued buyers"). That model is wrong for this app. Reytech operates in California public-sector procurement — buyers are **bound by procurement rules**, not by persuasion. The KPI is not "response rate." It's **"Reytech is on this agency's RFQ distribution list when the next solicitation drops."**

Everything downstream of that framing changes.

## The actual problem

Public-sector procurement is a closed-world problem, not an open-world prospecting problem:

- We **know** who buys what (`scprs_po_master` + `scprs_po_lines`, 7 weeks of state-wide history).
- We **know** when they buy (start_date / end_date — contract cycles are predictable per agency × category).
- We **know** what they pay (unit_price, line_total by supplier).
- We **know** the incumbent supplier and contract expiry.
- We **know** the fiscal-year cadence (CA FY = Jul 1 → Jun 30; Q1 = PO-heavy, Q4 = spend-down).
- We **know** the legal small-business preferences (Reytech is CA SB/MB; some RFQs must go to SB).

What we **don't know** — and what V2 has to fix:

- Is Reytech registered in this agency's buyer/vendor database to *receive* the RFQ when it posts?
- When the current contract expires, which buyers are cutting the next RFP?
- Does Reytech have a credible capability story in this agency's category (recent wins they can verify)?
- Are our SB/MB/DVBE/OSDS certifications current and on file with the agency?
- Does the right procurement officer at that agency know Reytech exists as an eligible bidder?

The unit of work is **capability registration + maintenance**, not prospecting.

## What V2 is not

Crossing these out because they're dead weight in this domain:

- ❌ **Open-rate optimization.** Procurement officers aren't A/B-testable. They scan formal solicitations and vendor-list requests; they don't respond to "7 tricks to sell more gloves" subject lines.
- ❌ **Engagement scoring (engaged/warm/cold/fatigued).** A buyer is either on the RFQ distribution list or not. "Cold" doesn't mean they're unreachable — it means we're not registered. Different fix, different lever.
- ❌ **Drip sequences with branches.** Procurement doesn't read nurture emails. One clear capability statement + SB cert docs lands; a 7-touch sequence gets marked as spam.
- ❌ **Per-variant response-rate learning.** Sample size is tiny (4-8 touches/quarter per agency), noise dominates, and the winning copy is determined by procurement-regulation compliance, not subject-line cleverness.
- ❌ **Channel routing via "fatigue signal."** Phone calls to procurement officers are welcome when you have a specific question (contract expiry? RFQ timeline?), not for generic check-ins.

## What V2 IS

A **targeting board** that answers, for every high-spend CA agency in Reytech's categories:

1. Are we **on the RFQ distribution list** for this agency's category?
2. What's the **next solicitation window** (from contract expiry + fiscal cadence)?
3. Do we have a **capability credit** with this agency (past Reytech win they can verify)?
4. Are our **certifications current** (SB / MB / DVBE / OSDS) and on file?
5. What's the **right next touch** — RFQ-list inclusion request, capability one-pager, cert renewal confirmation, or rebid-window memo?

Every row produces a standardized procurement-appropriate action, not a custom sales email.

## Data connection points — public-sector lens

Ranked by how much each moves the "Reytech gets the RFQ" outcome.

| # | Signal | Source | Why it matters in procurement |
|---|---|---|---|
| 1 | **Contract expiry + rebid window** | `scprs_po_master.end_date` | 60-90d before a contract expires, the agency issues the successor RFQ. Missing this window = missing the whole cycle. |
| 2 | **Is Reytech registered as a bidder?** | Cal eProcure + per-agency buyer lists (new data — see "What we need to capture") | #1 determinant of whether we'll receive the RFQ at all. Nothing else matters until this is Yes. |
| 3 | **SB/MB/DVBE set-aside applicability** | `agency_config` + `quotes.items_text` category | Some solicitations are legally restricted to certified SBs. Reytech's cert is a legal advantage, not a soft pitch. |
| 4 | **Reytech's win history at this agency** | `won_quotes_kb`, `quotes.status='won'` | Procurement officers trust "you've delivered $X to CDCR across N POs" more than any capability statement. |
| 5 | **Agency × category spend velocity** | `scprs_po_master` × `scprs_po_lines` histogram | "CalFire issues medical-supply RFQs every ~60d, last 2026-03-15, next ~May 15" — so we register TODAY. |
| 6 | **Cert expiry dates** | `data/certifications.json` or new table | Expired certs remove legal preferences. Silent cert lapse = we lose set-asides invisibly. |
| 7 | **Contract vehicles** (IDIQ / multi-year master contracts) | `scprs_po_master.acq_type` | Missing a multi-year master = 3+ years of lost RFQs. Very high leverage, narrow window to get on. |
| 8 | **Fiscal-year cadence** | Derived from SCPRS `pulled_at` × `start_date` | July surge + April spend-down — timing outreach to fiscal rhythms, not calendar rhythms. |
| 9 | **Incumbent performance signals** | `scprs_po_lines` + any late-delivery / cancel data | Weak incumbents (missed delivery, partial ships) = agencies looking for alternatives. |
| 10 | **Lost-bid memory** | `quotes.status='closed_lost' + status_notes` | We bid, we lost. WHY? At what price? When does their award contract expire and we can re-compete? |
| 11 | **Small-business matchmaker events** | Calendar (new data — CA annual events) | Procurement officers attend these. Post-event capability emails land 10× better. |
| 12 | **Correct contact = procurement officer, not buyer** | `scprs_po_master.buyer_name/email` is often ordering clerk, not PO | Registration requests must go to the agency's procurement-officer role, not whoever signed the last PO. |

**Top 3 to ship first:**
1. Contract-expiry rebid-window surveillance (#1)
2. Registration-status tracker (#2) — requires new data capture
3. Win-history cite + SB-set-aside flag (#3 + #4) — inline credibility

## What we need to CAPTURE that we don't have today

V1's signals are all derivable from existing data. V2's hardest step is **adding the registration-status layer** — the single data gap that gates everything else.

Required new data:

- **`agency_vendor_registry`** table (or per-agency JSON):
  - Per `(dept_code, category_cluster)`: is Reytech on the RFQ distribution list?
  - Last registration/confirmation date.
  - Registration expiry (some agencies require annual re-registration).
  - Link to the agency's supplier portal (Cal eProcure, DGS, CDCR supplier DB, etc.).
  - The procurement-officer contact (role, not name).
- **`reytech_certifications`** table:
  - SB / MB / DVBE / OSDS cert number, issue_date, expiry_date, renewal_url.
- **`bid_memory`** table:
  - Every RFQ we received: date, agency, category, bid/no-bid, if bid → our price, win/loss, winning supplier, winning price.
  - This is the backbone of all future rebid-window predictions.

These are small tables (~hundreds of rows total), all operator-curated initially with agent-assist later.

## Agentic loops — what actually closes the loop in procurement

Replacing the SaaS-funnel loops with procurement-appropriate ones:

### 1. Rebid-Window Surveillance Agent (HIGHEST VALUE)

- Daily: scans `scprs_po_master.end_date` for **competitor** contracts expiring in 30/60/90d.
- For each expiring contract, checks `agency_vendor_registry`: are we on the list to receive the successor RFQ?
- If yes → queues a "rebid window open" reminder on the card.
- If no → queues a **registration request** action: "Contact agency procurement office, request inclusion on [category] RFQ distribution list. Cite: Reytech SB cert #, recent [agency-similar] wins."
- Prevents the "I missed the RFQ" shape entirely.

### 2. Registration-Gap Agent (HIGHEST LEVERAGE)

- For every CA agency in the top-20 by category spend, verifies registration status against `agency_vendor_registry`.
- Surfaces gaps: "You're NOT on CDPH's medical-supply bidder list. They've awarded $X in this category in the last 6 months. Register via [link]."
- One-time action → 3+ years of RFQs received. Highest ROI per hour of operator time in the whole system.

### 3. Fiscal-Cadence Predictor Agent (HIGH VALUE)

- Per `(dept_code, category)`: computes histogram of `start_date` intervals. Output: "next RFQ expected in ~N days, confidence C."
- Surfaces on the card 14d before predicted date: "CalFire wound-care RFQ expected ~May 15; confirm registration is active."
- Real reason to touch the buyer — not a fake reason.

### 4. Cert-Expiry Watchdog Agent (COMPLIANCE-CRITICAL)

- Tracks `reytech_certifications.expiry_date`. At 60 / 30 / 14d pre-expiry, surfaces renewal action.
- Silent cert lapse today = invisible loss of SB set-aside eligibility on every affected bid. This agent prevents that class of loss entirely.

### 5. Bid-History Memory Agent (HIGH VALUE)

- Records every RFQ received + outcome. Joins to `scprs_po_master` post-award (who actually won, at what price).
- Surfaces on card: "You bid this agency's gloves RFQ 2026-01-15 at $8.40/bx; Medline won at $8.12. Their contract expires 2026-07-30 — rebid window opens May 30. Note: aim under $8.12 if we re-bid, or decline."
- This is operator-visible learning, not "variant response rate."

### 6. Capability Credit Assembler (SMALL, EVERY CARD)

- Per prospect agency × category: pulls the highest-confidence Reytech win in the same or adjacent category.
- Inlines one sentence: "Recently delivered [product family] to CDCR (PO R26Q0321, $X, on-time)."
- Procurement-appropriate credibility — no "we'd love to chat."

### 7. Soft-Touch Scheduler (REPLACES DRIP)

- Quarterly: for every registered agency where we haven't received an RFQ in 90+ days, queue a capability refresher (standard template, not A/B variants).
- Annually: SB cert renewal confirmation to every registered agency.
- Post-matchmaker-event: warm follow-up to every procurement officer met.
- These are the ONLY legitimate "outreach" touches in this domain; everything else is response-to-event (rebid window, cert expiry, RFQ received).

## Revised UX for `/outreach/next`

Same page, different primary columns. Each card now shows:

| Field | Source |
|---|---|
| **Agency** | `scprs_po_master.dept_name` |
| **Annual category spend** | Aggregate from `scprs_po_lines` |
| **Registered for this category's RFQ list?** | `agency_vendor_registry` (🟢 yes / 🔴 no / ⚪ unknown) |
| **Current contract expires** | `scprs_po_master.end_date` countdown |
| **Next expected RFQ** | Fiscal-cadence predictor output |
| **Reytech capability credit** | Top 1-2 won quotes in same/adjacent category |
| **SB set-aside applicable** | Match to category + agency history |
| **Cert status for this agency** | `reytech_certifications` ∩ agency requirement |
| **Last touch / last RFQ received** | `bid_memory` + outreach log |

Action buttons change:

- ~~Draft A/B email~~ ✗
- ✅ **Request inclusion on RFQ distribution list** (standard template with SB cert # + capability credit)
- ✅ **Send quarterly capability refresher** (standard template, no variants)
- ✅ **Confirm SB cert on file** (just-the-cert-packet)
- ✅ **Rebid-window memo** (only visible when contract expiry is 30-90d out)

## V2 build order — procurement lens

Ordered by "moves the RFQ-inclusion needle fastest":

- **V2-PR-1: Contract-expiry surveillance + rebid-window badge.** SMALL. Pure SQL on `scprs_po_master.end_date`. Inline countdown. First thing Mike sees on the card.
- **V2-PR-2: `agency_vendor_registry` table + registration-status badge.** SMALL. New table, minimal UI. Operator fills in known registrations manually at first.
- **V2-PR-3: Capability-credit assembler.** SMALL. Joins `won_quotes_kb` to each card's agency + category. One-liner per card.
- **V2-PR-4: Cert-expiry watchdog.** SMALL. New `reytech_certifications` table + daily check + card badge.
- **V2-PR-5: Fiscal-cadence predictor.** **DEFERRED 2026-04-24** per pre-build product-engineer review. Three structural issues block: (1) 7-week SCPRS history is too thin for 60-180d cadence prediction — most predictions land at low-confidence suppressed → invisible block; (2) `start_date` is award date not RFQ-post date — cadence labels would mislead; (3) no prediction writeback = "metric theater" without backtest. Revisit when SCPRS history >6 months AND with persistence/backtest infra (cadence_predictions table + Day-30 backtest endpoint) baked in.
- **V2-PR-6: `bid_memory` + rebid-window memo generator.** MEDIUM. New table, post-RFQ logging, standard-template generator.
- **V2-PR-7: Registration-gap agent — auto-detect where we SHOULD be registered.** MEDIUM. For every top-20 agency, flag the categories where we have capability credit but no registration record.
- **V2-PR-8 (stretch): Standard-template outbox for procurement-appropriate touches.** MEDIUM. Replaces V1's A/B drafts with 4 canned procurement templates (RFQ-list request / capability refresher / cert confirmation / rebid memo).

**Key sequencing insight:** PR-1 is the only one with existing data; PRs 2/4/6 require small new tables. Ship PR-1 alone first (ships in an hour, big immediate value), then do PR-2 (the registration-status layer is the single highest-leverage schema change in the whole app for this use case).

## Day-7 success metric

Not "sends per hour." Not "response rate." The right metric is:

**# CA agencies where Reytech is confirmed-on the RFQ distribution list for ≥1 high-spend category.**

V1 baseline: unknown. V2 target: move this from unknown → tracked and growing. Second-order metric: **# RFQs received per quarter** (before V2: whatever arrives naturally; after V2: the direct consequence of registration work).

If neither moves in 60 days after PRs 1-4 ship, the registration layer isn't actually the bottleneck — at that point reassess. But the prior-art evidence from every CA SB that scales (Grainger, Cardinal, Henry Schein) is that supplier-registration + RFQ-list inclusion IS the bottleneck in public-sector. Bet accordingly.

## What changes in V1 code

`/outreach/next` V1 stays. V2 is additive:

- Card layout shifts: engagement-signal block → registration-status + rebid-window block.
- New helpers per card: `_contract_expiry_for_dept`, `_registration_status`, `_capability_credit`, `_cert_status`, `_fiscal_cadence`.
- Draft buttons replaced with canned-template actions.
- Response-history signal (PR #502) retired or demoted to "touches log" — kept for reference, not for action recommendation.

`prospect_scorer` stays but score weighting changes: recency and gap matter less, **contract expiry proximity + registration-gap matter more**. Registration-gap alone should dominate the top-10 ranking.

## One-paragraph summary

Public-sector procurement is a closed-world problem. We know who buys what at what price. We don't know whether Reytech is on their RFQ list. V2 is a targeting board whose entire job is to **get Reytech on more RFQ distribution lists before the next solicitation window** and to **maintain the certifications + capability credits that win the RFQs once we receive them**. Every V1 UX element that optimizes for "is this email going to get opened" is noise in this domain and gets replaced with "is this agency about to post an RFQ we could have bid on."
