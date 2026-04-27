# Plan: Once and For All — Make RFQapp Earn

**Created 2026-04-25.** Authored after a full ground-truth pass (DATA_ARCHITECTURE_MAP, route triage, live verification of /agents and /growth-intel). This doc supersedes any "next session" notes that aren't on this list.

---

## 0. The North Star

**One operator sends one clean quote in under 90 seconds. Win rate climbs to 30%.**

That's it. Every PR either advances that number or it doesn't ship. We have shipped 535+ PRs and built dashboards, oracles, agents, intel surfaces — and Mike still missed quotes this week because the *core path* wobbled. The gap isn't features; it's that we kept building *adjacent* to the path instead of locking *the path*.

This plan does three things, in order:
1. **Lock the golden path.** Quote works, every time, with a regression net under it.
2. **Cut the slop.** Pages that don't move quotes get deleted or moved to `/admin`. No more decoration.
3. **Build the win-rate engine.** Once #1 and #2 are true, the win-rate work has a stable floor to compound on.

Anything that doesn't fit those three buckets is deferred or killed.

---

## 1. The Honest Diagnosis (so we stop repeating it)

| Failure pattern | Evidence | Root cause |
|---|---|---|
| "Built but never connected" | DATA_ARCHITECTURE_MAP §5.c — 8 LIVE-OFF flags with shipped code | No "flip-or-delete" deadline on any flag |
| "Multiple pages do the same thing" | `/agents`, `/growth-intel`, `routes_growth_prospects.py`, `routes_intelligence.py` (partial), `routes_cchcs_packet.py` (superseded) | Each new feature got a new page; nothing was retired |
| "Inaccurate data + 404s" | `/growth-intel`: `/growth` link 404s in `base.html:319`; price alerts table empty until manual rebuild | Features ship without an "is this populated?" probe |
| "Quoting fails for basic items" | `feedback_quoting_core_repeats_failing` — three-strikes triggered 2026-04-24 | Per-symptom fixes without a golden E2E that runs on every push (now exists: `tests/test_golden_path.py` PR #523) |
| "Shipping PRs ≠ KPI" | `feedback_volume_vs_outcome` | Cadence rewards motion, not outcome — no KPI gate on merges |

The architecture is fine. The wiring discipline is the problem. The next sections are about wiring discipline.

---

## 2. Phase 0 — Stop the Bleeding (this week, ≤3 days)

These are already shipped or one PR away. Verify they hold, then we stop touching them.

| # | Item | Status | Owner action |
|---|---|---|---|
| 0.1 | Fail-closed quote validator (no $0 PDF reaches operator) | ✅ PR #525 | Verify in prod: try to render a PC with one $0 item; expect refusal |
| 0.2 | 1-item Barstow golden E2E on every push | ✅ PR #523 + #532 tripwire | Confirm `make ship` runs it; if green stays green, we're protected |
| 0.3 | Catalog-first cost cascade (no Amazon/SCPRS as cost basis) | ✅ PRs #524/#528/#529/#530/#531 | Verify: 5 fresh PCs, all show `cost_source` chip, no "needs_lookup" leaks |
| 0.4 | Quote-status race (S4) — operator manual sets get overwritten by background tracker | ✅ DONE | `set_quote_status_atomic()` helper in `core/quote_lifecycle_shared.py:22`; all 7 background-agent writers (award_tracker ×3, email_poller, scprs_intelligence_engine, scprs_universal_pull, revenue_engine) already have `WHERE status = 'sent'` (or equiv) conditional WHERE. Regression test: `tests/test_quote_status_race_fence.py` (8 tests, all passing). |
| 0.5 | Pre-Phase-1 PC cost reset (the in-flight gap) | ✅ DONE 2026-04-27 | PC detail surfaces a "Pre-Phase-1 PC — Refresh Costs" banner when `_items_with_chip == 0`. Click POSTs to existing `/api/pricecheck/<pcid>/lookup-costs` (idempotent tier-cascade). 15s reload. No new backend code needed — endpoint and worker already existed. |
| 0.6 | Phantom flags purged from memory | ✅ DONE 2026-04-27 | Verified via prod `/api/admin/flags`: `ingest.ghost_quarantine_enabled`, `ingest.legacy_fallback_loud`, `quote.block_unresolved_ship_to` are all real and `true`. No phantom flags to purge — memory entry is accurate. |
| 0.7 | **Run the oracle backfill against 4 yrs of historical quotes** | ✅ DONE 2026-04-27 | First prod run via `POST /api/oracle/backfill-all`: **488 calibrations written** (102 won + 379 lost from quotes table). Pricing engine confidence jumped from `none` to `high`. KB joinback returned 0 matches — SCPRS KB rows don't link to Reytech bids; deferred. `/health/quoting` widget for "last run" deferred. |

**Phase 0 success gate:** Mike sends 3 quotes in a row with zero hand-correction **AND** oracle priors are visible on `/health/quoting` with non-zero counts. If either fails, Phase 1 is blocked.

---

## 3. Phase 1 — Kill the Slop (this week + next, ~5 PRs)

This is the cathartic phase. Every PR deletes or hides. Net code shrinks.

### 3.1 Delete `/agents` (`routes_agents.py`)
- The page is performative — `/api/agents/health-sweep`, `/api/agents/batch-test` are diagnostics, not workflow.
- **Action:** Move the API endpoints under `/health/quoting` as a "Diagnostics" panel. Delete `routes_agents.py` and `templates/agents.html`. Remove nav link.
- **Verify:** Chrome-MCP, confirm `/health/quoting` shows the rolled-up status; `/agents` returns 404; nav is shorter.

### 3.2 Repair or hide `/growth-intel` (`routes_growth_intel.py`)
Two specific bugs Mike named:
- **404 on top hyperlink:** `base.html:319` command palette references `/growth` (no such route). **Fix:** change `u:'/growth'` → `u:'/growth-intel'` (one-line patch).
- **Inaccurate data:** Price alerts require a manual "Rebuild Catalog" click that no one does. **Fix:** schedule `_rebuild_catalog_from_history()` as a daily background worker; surface "last rebuild: Xh ago" badge.
- **Outreach send doesn't log activity:** the modal calls `/api/outreach/send` but the response never appends to `activity_log`. **Fix:** wrap the send in `core/dal.log_outreach_send()`.
- **OR:** if the page still feels low-value after these fixes, move the entire route to `/admin/growth-intelligence`, drop it from main nav, and keep the work-in-progress where it can't mislead operators.

**Decision rule:** If after the three fixes the page can't show *one piece of accurate, actionable data* on a fresh load, hide it. Mike judges in Chrome.

### 3.3 LIVE-OFF Flag Verdict Sprint (one PR, one day)
For each flag in DATA_ARCHITECTURE_MAP §5.c, pick one of three outcomes. No flag survives this PR in limbo state.

| Flag | Verdict (recommended) | Reasoning |
|---|---|---|
| `unspsc_enrichment` | **Delete** — 0 callers using the classification today | Build later only if a buyer demand signal arrives |
| `outbox.send_approved_enabled` | **Delete** — outbox already manual-send | Auto-send risk > value at our volume |
| `ingest.classifier_v2_enabled` | **Flip ON, remove flag** — already in prod path per memory | Confirm on `/admin/flags` first |
| `bid_scoring` | **Delete** — unbuilt | Re-greenfield if ever needed |
| `compliance_matrix` | **Delete** — unbuilt | RFQ checklist work has not landed |
| `docling_intake` | **Delete** — vision parser is canonical | Don't keep two intake paths |
| `nl_query_enabled` | **Delete** — speculative | |
| `orders_v2.poller_unified` | **Keep, prep to flip in Phase 3** | This is the S3 lever |
| `rfq.require_profile_match` | **Delete** — rules already enforced upstream | |
| `quote_model_v2_enabled` | **Keep, gated on Phase 3 S12** | Promotion blocked on shadow telemetry consumer |
| `rfq.readback_verifier` | **Delete** — unbuilt | |
| `rfq.orchestrator_pipeline` | **Keep, gated on V2 promotion** | |

Default verdict on anything ambiguous = **delete**. Code we don't run is code we re-fight every audit.

### 3.4 Move admin clutter to `/admin/*`
Migrate to a new `routes_admin.py` blueprint nested at `/admin`:
- `routes_build_health.py` → `/admin/build`
- `routes_feature_flags.py` → `/admin/flags`
- `routes_shadow.py` → `/admin/shadow`
- `routes_quoting_status.py` → `/admin/quoting`
- `routes_system.py` → `/admin/system`
- `routes_classifier_debug.py` → **delete**
- `routes_cchcs_packet.py` → **delete** (superseded by `routes_rfq_gen.py` + `agency_config.py`)
- `routes_locked_costs.py` → **delete** (planned, not adopted)
- `routes_utilization.py` → **audit usage**, default delete
- `routes_v1.py` → **deprecate banner now**, delete after 30 days

### 3.5 Per-buyer form profile training (Phase 1.6)
17 generic form profiles exist (`src/forms/profiles/*.yaml`) but none are buyer-specific. For each agency where Reytech has sent ≥5 quotes (CDCR, CCHCS, CalVet/Barstow, DSH, DGS), run `src/agents/form_profiler.py` against the actual sent PDFs in `output/` + Gmail history. Generate buyer-specific YAML profiles. Result: when a CDCR-Folsom RFQ arrives, dispatcher uses *their* profile (with their checkbox layout, their signature row, their tax line), not the generic 703B fallback. Hand-edits drop to near-zero per quote.

### 3.6 McKesson catalog import (Phase 1.7)
If a McKesson item DB exists as a CSV/file outside the repo, one-shot import into `product_catalog` with `supplier='mckesson'`. Cost-cascade order becomes: **catalog (McKesson preferred) → web_cost → vendor_cost → SCPRS reference (ceiling, never cost)**. Closes a major asset gap — McKesson is a real wholesale-cost source unlike Amazon retail. Mike provides the file location; I run the import + write tests + add a "supplier: mckesson" filter to `/catalog`.

### 3.7 Lock the operator nav to 6 pages
The only top-level nav items: **Home · PCs · Quotes · CRM · Outbox · Analytics**.

Everything else (search, deadlines sidebar, outreach card, growth-intel if surviving) is a sidebar widget or a CRM tab — not a top-level entry. Remove the rest from `templates/base.html` nav.

**Phase 1 success gate:**
- App boots with ≤30 route modules (down from 36).
- Top nav has 6 items.
- Mike pulls a fresh checkout and can't find a single dead link or empty page.
- Code line count is *down*, not up.

---

## 4. Phase 2 — Lock the Golden Path (concurrent with Phase 1, ~1 week)

These are the *quoting-specific* hardening items. Every one of them is a regression net for the KPI.

### 4.1 Operator-KPI telemetry
A single SQLite event: `operator_quote_sent` with `(quote_id, ts, time_to_send_seconds, item_count, agency_key)`. Surfaced on `/analytics` as **"Quotes sent this week × median time-to-send"**. This is the first chart Mike checks every morning. If `time_to_send_seconds > 90` for a 1-item quote, that's a red flag we investigate.

### 4.2 S2 follow-up: drop `_FACILITY_ADDRESSES` parallel dict — ✅ CLOSED 2026-04-27
`core/institution_resolver.py` no longer carries a parallel facility-address dict. Audit found the only reader was `get_ship_to_address` in the same file (zero external callers — `ship_to_resolver` had migrated to `quote_contract.ship_to_for_text` on 2026-04-25). Both deleted. Canonical `FacilityRecord` already held `address_line1`/`address_line2` for every facility; the pre-existing 5-test cross-source consistency suite confirmed parity before deletion (then itself removed as vestigial). Replaced with an absence-guard ratchet in `test_ship_to_resolver_canonical.py`.

### 4.3 `/health/quoting` becomes the operator's ONLY ops page
Roll up: DB health, email poll lag, Gmail send health, golden-path test status, last 5 quotes' `cost_source` chips, `/agents` health endpoints (from 3.1), drift counters from S3 prep.

If something is broken, it's visible here. If it's not visible here, it's not breaking quotes.

### 4.4 Historical replay gate (Phase 2.5)
Take 50 random quotes Mike actually sent successfully 2022-2025 (across 5+ agencies, mixed item counts). Run each through the current pipeline cold, regenerate the PDF, diff against the originally-sent PDF. **Expectation:** every regeneration matches within tolerance (price exact, items exact, signature/date placement within 5pt, totals exact). Any divergence = bug we fix before declaring Phase 2 done. This is the trust check — it proves the app handles your actual historical data, not just synthetic fixtures.

Output: `tests/test_historical_replay.py` runs nightly; results posted to `/health/quoting`.

### 4.5 Pre-push hook teeth
The `make ship` pre-push hook already runs the golden test + chrome-verified gate. Add:
- `tests/test_route_module_registration.py` (already in PR #532)
- `tests/test_url_map_floor.py` (already in PR #532, 1200-rule floor)
- **NEW:** `tests/test_dead_route_audit.py` — fails if any route in `_ROUTE_MODULES` was deleted in Phase 1 isn't also removed from `base.html` nav.

**Phase 2 success gate:** Three weeks pass with zero golden-path test failures on `main`. Median time-to-send for a 1-item quote drops below 90 seconds.

---

## 5. Phase 3 — Unify the Shadow Schemas (3–4 weeks, sustained)

The remaining silos from DATA_ARCHITECTURE_MAP §7. Sequenced per p-eng review.

| Step | Silo | PR shape | Soak gate |
|---|---|---|---|
| 5.1 | S3-prep | UNIQUE on `orders.po_number` (dedupe first), drift counter on `/health/quoting` | 14-day soak, zero divergence |
| 5.2 | S3-flip | Flip `orders_v2.poller_unified` ON; `record_po()` helper in `core/order_dal.py` | 100 PO writes, zero divergence |
| 5.3 | S3-cleanup | Drop `purchase_orders` shadow schema | After 5.2 gate passes |
| 5.4 | S8 | Stop `data_json` blob writes on V2 orders; drop `order_dal.py:96-100` fallback | One-week prod observation |
| 5.5 | S10 | Background workers gated by feature flag at start-of-interval, not just env var | Post-soak |
| 5.6 | S12 | Surface shadow telemetry consumer for `quote_model_v2_shadow`; OR kill V2 promotion plan and delete shadow code | Decision PR — either flip or kill |

Don't combine these into one mega-PR. Each one ships behind its own gate, soaks, then the next one starts.

**Phase 3 success gate:** DATA_ARCHITECTURE_MAP §7 silo table reads "12 of 12 closed" — and stays that way for 30 days.

---

## 6. Phase 4 — The Win-Rate Engine (the actual revenue lever)

*This is what Mike means by "iterative intelligence." It only works if Phases 0–2 are solid.*

### 6.1 Outcome-driven oracle calibration
- Every quote marked **won** or **lost** triggers `pricing_oracle_v2.calibrate_from_outcome(quote_id, outcome, gap_pct)`.
- Wired today in code (DATA_ARCHITECTURE_MAP §8) but only fires when operator hits `/api/rfq/<rid>/outcome` directly. No UI surfaces the call.
- **Action:** `/quotes/<id>` page gets a 3-button "Mark Won / Mark Lost / Mark Cancelled" widget. Mark-lost prompts for the winning price (paste from buyer's "you lost to $X" email or SCPRS award). Calibration fires automatically.

### 6.2 Buyer-product pricing memory
Replace `/growth-intel`'s broken price-alerts panel with one focused view:
- Pick a buyer (e.g., "CDCR Folsom") and a product (catalog row).
- Show: last 5 quotes for that buyer-product, our price, win/loss, winning price (if known), our oracle's current recommendation, and **delta vs. our last winning bid for that buyer**.
- This is the "should I bid $X or $X-5?" decision support Mike has been asking for.

### 6.3 Cost-alert background worker
- Daily cron: scrape Amazon/Grainger/S&S for every catalog row that's been quoted in the last 30 days.
- If a cost moves >10%, write a row to `cost_alerts`; `/health/quoting` shows the count; Mike clicks to triage.
- No manual rebuild button. Fully automated. Visible only when there's actual signal.

### 6.4 Win-rate dashboard (replaces 4-feature `/growth-intel`)
One chart on `/analytics`: rolling 30-day win rate by agency. Sparkline below: same metric over the last 6 months. **The number we are paid to move.**

**Phase 4 success gate:** Win rate visible, calibration loop closed. Then we measure whether the rate moves. That's the experiment Phase 4 enables.

---

## 7. Phase 5 — Volume Multipliers (only after Phase 4 win-rate moves)

Don't start any of this until win-rate has moved. Volume amplifies whatever rate you have — if it's bad, we just lose faster.

- **Multi-PC bundle send** — partial in memory, finish per `project_multi_pc_bundle.md`.
- **Auto-quote review queue** — high-confidence single-item PCs auto-fill a draft, queue for operator one-click send. Never auto-send.
- **Buyer email polling SLA** — every email triaged in <5 min during business hours; `/health/quoting` flags lag.
- **Outreach v2** — only after win-rate work proves the upstream funnel.

---

## 8. What We Explicitly Stop Building

| Stop | Why |
|---|---|
| Any new route module that isn't on the golden path or rolling up to `/health/quoting` | We have 36 modules. Adding more is a tax on every audit. |
| Any new feature flag that doesn't have a flip-or-kill date in the same PR | Phase 1 cost us a week to audit 12 dead flags. Don't repeat the mistake. |
| Any "intelligence" page without a populated-data probe | `/growth-intel` is the cautionary tale. Empty pages mislead operators. |
| Multiple paths to the same outcome | One ingest. One pricing engine. One quote model. The plural existence of any of these is the bug. |
| New PRs while three-strikes is active on the quoting core | If a quote-core fix fails 3x, Phase 0 reopens; nothing else lands until it's green. |

---

## 9. Operating Rhythm

- **Every PR must answer:** does this advance "operator sends one clean quote in <90s"? If not, justify in the PR body or close it.
- **Every Friday:** 5-minute look at `/health/quoting` + win-rate sparkline. That's the weekly review.
- **Every silo PR:** updates `docs/DATA_ARCHITECTURE_MAP.md` §7 in the same commit. The map is the running ledger.
- **Three-strikes rule:** still active. If a quote-core bug needs 3 fixes, stop and recommend a fresh session with full audit.

---

## 10. What "Done" Looks Like

We say this plan is done when, on a single weekday:
1. Mike receives an RFQ email at 9:00 AM.
2. By 9:01:30, a draft quote with correct `cost_source` chips is on screen.
3. He hits send by 9:02:00.
4. The quote enters `/quotes` with `status=sent`, telemetry logs `time_to_send=85s`.
5. Two days later the buyer marks it won. Mike clicks **Mark Won**.
6. The oracle calibrates. The next CDCR-Folsom quote on the same product reflects the new winning-price floor.
7. The 30-day win-rate sparkline ticks up.

That's the closed loop. Everything in this plan is in service of it.

---

*Authored by Claude (Opus 4.7) on 2026-04-25 after a full ground-truth audit of `docs/DATA_ARCHITECTURE_MAP.md`, route inventory across 36 modules, and verification of `/agents` + `/growth-intel`. Updates and amendments belong in this file — don't fork it.*
