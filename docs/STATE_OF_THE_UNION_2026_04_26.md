# State of the Union — 2026-04-26 (rev 2)

After ~30 PRs and a clean-slate audit + fix cycle, this is what's now
live in the RFQapp, how it's wired together, and what to read first
when planning the next arc.

---

## 🎯 LIVE PROD DATA (after the is_test bug fix landed)

```
OVERALL (all-time, since 2022):
  363 quotes / 84 wins / 257 losses
  Win rate: 23.1%  (target: 30%)
  Revenue: $1,460,822 won / $1,815,773 lost
  Capture: 40% of bid volume converts to revenue

PER AGENCY (top 10):
   81q  CIW                              19w  23.5%  $    40,640
   31q  Veterans Home — Chula Vista      14w  45.2%  $   114,118
   28q  CSP — Sacramento                  2w   7.1%  $    10,393
   21q  Veterans Home — Barstow          13w  61.9%  $   717,122  ← biggest $
   18q  Veterans Home — Fresno            9w  50.0%  $   144,773
   17q  Veterans Home — West L.A.         3w  17.6%  $    91,869
   15q  DSH — Atascadero                  1w   6.7%  $     3,556
   14q  Veterans Home — Yountville        6w  42.9%  $   154,711
   10q  Veterans Home — Redding           3w  30.0%  $    33,895
```

**Strategic intel from the data:**
- Veterans Home market: dominant (5 of 6 facilities ≥30% rate, $1.16M won)
- Prison/CDCR market: weak (CIW 23.5%, CSPS 7.1%, DSH 6.7%)
- Biggest revenue lever: CIW (81 quotes — doubling rate ≈ +20 wins)
- Biggest revenue producer: Barstow ($717K won, 61.9% — protect this)

---

After 4 sessions and 30+ PRs, this is what's now live in the RFQapp,
how it's wired together, and what to read first when planning the next
arc. Replaces the per-session execution logs as the canonical reference.

---

## North Star (unchanged)

**One operator sends one clean quote in <90 seconds. Win rate climbs
to 30%.**

Today: 21% win rate measured against 481 historical outcomes (per the
QuoteWerks + SCPRS-wins backfill). That's the baseline the win-rate
engine compounds on.

---

## What's now LIVE (in dependency order)

### 1. The win-rate engine — closed loop

| Side | What | Where |
|---|---|---|
| **Backfill** | 503 quotes / 102 wins / 379 losses indexed | `scripts/import_quotewerks_export.py`, `scripts/import_scprs_reytech_wins.py` |
| **Calibration** | 488 oracle rows updated by category × agency | `core/oracle_backfill.py:backfill_all`, `core/pricing_oracle_v2.py:calibrate_from_outcome` |
| **Outcome verify** | 77 SCPRS-verified wins joined to QW imports + 25 QW-flagged wins | `core/oracle_backfill.py:verify_quotewerks_outcomes` |
| **Write side (UI)** | Mark Won/Lost on `/quotes/<id>` fires calibration + toast | `templates/base.html:markQuote`, `routes_crm.py:1644` |
| **Read side (API)** | `/api/oracle/item-history?agency=X&description=Y` returns prior bids + oracle markup | `routes_oracle_item_history.py` |
| **Read side (UI)** | "📊 Hist" button on each PC item row → modal | `routes_pricecheck.py:962` button + `base.html:openItemHistory` modal |
| **Aggregation** | `/api/oracle/win-rate-by-agency?days=N&min_quotes=M` rolls up by canonical agency | `routes_oracle_win_rate.py` |
| **Aggregation UI** | Win-Rate Intel widget at top of `/` (home) | `templates/home.html` (top of `block content`) |

### 2. Catalog / supplier intelligence

| What | Where | Status |
|---|---|---|
| McKesson SKU lookup (2,178 rows) | `supplier_skus` table + `routes_supplier_sku_lookup.py` | live, queryable via `/api/catalog/supplier-sku-lookup` |
| Cost-alert scanner (catalog cost change detection) | `cost_alerts` table + `routes_cost_alerts.py` | live; trigger via `POST /api/admin/scan-cost-alerts`, list via `GET /api/admin/cost-alerts` |
| won_quotes_kb (1,260 historical SCPRS competitor wins) | existing | populated; joinback found 0 matches because they're pre-2025 SCPRS scrapes that don't overlap with QuoteWerks 2025 data |

### 3. Architecture cleanup

| What | Status |
|---|---|
| `/agents` page deleted (was performative) | live; redirects 301 → `/health/quoting` |
| `/growth-intel` 404 hyperlinks fixed | live |
| Default top nav locked to 6 KPI pages | live (Home / PCs / Quotes / CRM / Outbox / Analytics) |
| Phase 0.4 quote-status race fence (3 sites patched + atomic helper) | live |
| Phase 0.5 PC cost reset button | live (PR #536) |
| Phase 0.6 phantom-flag re-classification | live |
| Phase 1.3 dead-flag verdicts (7 DEFER-DELETE, 3 KEEP, 2 CONSIDER) | documented in DATA_ARCHITECTURE_MAP.md §5.c |
| Architecture map (DATA_ARCHITECTURE_MAP.md) — 5/12 silos closed | live, updated per PR |

### 4. Deploy infra

| What | Status |
|---|---|
| `dashboard.py` shim (railway dashboard config drift) | live; lets `gunicorn dashboard:app` resolve to `app:app` |
| Railway dashboard override (start command) | **STILL NEEDS MANUAL FIX** by Mike — clear in Railway Settings → Deploy → Start Command, then delete the shim in a follow-up PR |

---

## How the operator-facing flow works now

When Mike opens `/` (home):
1. **Win-Rate Intel widget** at top loads (auto-hides if no data).
2. Headline: "479 quotes across N agencies · won $X / lost $Y".
3. Win rate · wins · losses stat strip.
4. Top 6 agencies tiled — each shows agency name · win-rate% (green ≥30, orange 15–30, red <15) · wins/quotes.

When Mike opens a PC `/pricecheck/<id>`:
1. For each item row, the description cell has 🔍 Amazon and **📊 Hist** buttons.
2. Click 📊 → modal opens with prior bids for `(this PC's institution × this item description)`.
3. Modal shows: win rate · wins · losses · matches · oracle's recommended markup · winning-price stats (yours/competitor's, min/median/max) · recent matches table with quote#, status, our price, date.

When Mike marks a quote Won or Lost on `/quotes/<id>`:
1. Mark Won prompts for PO number.
2. Mark Lost prompts for the winning competitor's price + loss notes.
3. The endpoint fires `pricing_oracle_v2.calibrate_from_outcome(items, outcome, agency, winner_prices, loss_reason)`.
4. Toast confirms "✓ Marked WON — oracle calibrated".
5. Next quote at same agency for same item gets a smarter recommended markup.

---

## Live data snapshot (Phase 0.7d backfill outcome)

```
McKesson SKUs imported:   2,178
SCPRS won POs imported:     112 + 1 manual = 113
QuoteWerks quotes imported: 479 (3,754 line items)
Quotes verified as won:     102 (25 QW DocStatus + 77 SCPRS verified)
Quotes verified as lost:    379
Oracle calibrations:        488
won_quotes_kb rows:       1,260 (competitor intel; 0 matched to QW yet)
```

**Win rate (last 365 days, ≥3 quotes/agency): 21%**

---

## Late-session adds (post-rev-1)

After the original wrap, this session also shipped:

| PR | What |
|---|---|
| #559 | `/api/admin/quotes-diagnostic` — histogram tool |
| #560 | `/api/admin/fix-quotewerks-is-test` — clear is_test on QW imports |
| #561 | Phase 4.3 — cost-alert scanner + triage queue |
| #562 | Original state-of-the-union doc |
| #563 | McKesson SKU autocomplete on MFG# input |
| #564 | Win-Rate widget — 1y/2y/All toggle, default all-time |
| #565 | Sticky "At this agency" intel strip on PC detail |
| #566 | "🏆 Recent wins" preview row + `/api/oracle/recent-wins` |

**Audit:** 84/84 targeted tests green. All 8 endpoints respond 200.
`/agents` redirects 301 → `/health/quoting` correctly.

**The is_test bug was the unlock.** Pre-fix: 19 quotes visible. Post-fix:
363 quotes visible. 19× signal increase.

---

## What's deferred (next-session backlog)

In rough priority order:

1. **Phase 1.6 — per-buyer form profile training.**
   For each agency where Reytech sent ≥5 quotes, generate a buyer-specific
   YAML profile from their actual sent PDFs. The biggest remaining
   "accurate documents per agency" lever. ~1 day per agency per PLATFORM_QUOTING.md.
   Needs: Gmail crawl of sent items OR walk `output/` for Reytech's
   generated PDFs and reverse-engineer the buyer's template from the
   `data/uploads/` inbound RFQs.

2. **Phase 4.5 — win-rate sparkline on /analytics.**
   Show 30-day rolling win-rate trend so Mike sees the engine's effect
   over time. The aggregation endpoint exists; just needs a Chart.js
   line on /analytics.

3. **Phase 4.6 — cost-alert background worker.**
   Today's `routes_cost_alerts.py` ships the scanner + table + triage
   queue, but only triggers on operator-initiated scans. A daily
   background pass would proactively detect cost drift before quoting.

4. **Phase 1.4 — admin namespace move.**
   Move `routes_build_health`, `routes_feature_flags`, `routes_shadow`,
   `routes_quoting_status`, `routes_system` under `/admin/*`. Tedious
   refactor without much KPI value but cleans the operator surface.

5. **Phase 2.5 — historical replay gate.**
   50 random historical quotes through the current pipeline cold; diff
   regenerated PDFs vs originally-sent. Trust check on the engine.

6. **Phase 3 — unify shadow schemas (S3, S4 done, S8/S10/S12 left).**
   Per DATA_ARCHITECTURE_MAP §7. Multi-week.

7. **Mike housekeeping (5 min on his side):**
   Clear Railway dashboard Start Command override in Settings → Deploy.
   Then delete `dashboard.py` shim in a follow-up PR.

8. **The 2022-2024 QuoteWerks export, if it exists somewhere.**
   The Phase 0.7d import pulled 479 quotes (mostly 2025). Mike's
   "since 2022" data presumably exists in an older QuoteWerks backup;
   importing it would 3-5× the calibration sample size.

---

## Reading order for the next session

1. **This doc** — the canonical "what's live" state.
2. `docs/PLAN_ONCE_AND_FOR_ALL.md` — the original 6-phase plan.
3. `docs/PLAN_EXECUTION_LOG_2026_04_25.md` — turn-by-turn log of how we got here.
4. `docs/DATA_ARCHITECTURE_MAP.md` — the silo ledger; pick one and close it.
5. `~/.claude/.../memory/MEMORY.md` — feedback principles + context.

---

## Files Mike should know exist

```
src/api/modules/routes_oracle_item_history.py       — Phase 4.2 read-side API
src/api/modules/routes_oracle_win_rate.py           — Phase 4.4 aggregation API
src/api/modules/routes_cost_alerts.py               — Phase 4.3 cost-drift detection
src/api/modules/routes_supplier_sku_lookup.py       — Phase 1.7 McKesson + future suppliers
src/core/oracle_backfill.py                         — backfill_all, joinback_won_quotes_kb,
                                                       verify_quotewerks_outcomes
scripts/import_quotewerks_export.py                 — QuoteWerks CSV → quotes table
scripts/import_scprs_reytech_wins.py                — SCPRS HTML → scprs_reytech_wins
scripts/import_mckesson_catalog.py                  — McKesson CSV → supplier_skus
src/templates/home.html                             — Win-Rate Intel widget at top
src/templates/base.html                             — markQuote (Mark Won/Lost) +
                                                       openItemHistory (📊 modal)
dashboard.py                                        — Railway shim (delete after dashboard fix)
docs/PLAN_ONCE_AND_FOR_ALL.md                       — master 6-phase plan
docs/PLAN_EXECUTION_LOG_2026_04_25.md               — execution log of 4 sessions
docs/DATA_ARCHITECTURE_MAP.md                       — silos + connectivity ledger
docs/STATE_OF_THE_UNION_2026_04_26.md               — this doc
```

---

*Compiled by Claude (Opus 4.7) on 2026-04-26 after 4 autonomous sessions.
22+ PRs shipped (#537–#561). Win-rate engine end-to-end live with real
historical data. Next session pick item from the deferred backlog above.*
