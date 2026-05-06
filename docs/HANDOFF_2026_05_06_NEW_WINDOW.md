# Handoff — 2026-05-06 substrate session, fresh window pickup

This window has run long. Mike asked for a handoff so a fresh window can resume cleanly without context drift.

## What shipped (10 PRs total)

All auto-merged or auto-merging. **Theme**: substrate fixes that close failure classes, not symptoms.

| PR  | Status | Removes |
|-----|--------|---------|
| #777 | ✅ MERGED | URL paste — 4 contamination sources (host-gate `_extract_asin`, kill catalog write-back from lookup, tighten Claude hallucination guard, `_origDesc` capture before mutation) |
| #778 | ✅ MERGED | Autosave RMW race (Mike P0 RFQ a5b09b56 — work overwritten on refresh) |
| #779 | ✅ MERGED | 8 mark-* RMW handlers (mark-won/lost/sent, convert-to-rfq, bundle-converts) |
| #780 | ✅ MERGED | Audit doc closeout (P0 + P1 #9-#12 already shipped, marked CLOSED) |
| #781 | ✅ MERGED | Step 4b cost overwrite — auto source confidence never licenses overwriting operator cost |
| #782 | ✅ MERGED | CCHCS package rules pinned (CLAUDE.md → executable tests) |
| #783 | ✅ MERGED | Step 4b URL overwrite — sibling fix to #781 for `item_link` field |
| #784 | ✅ MERGED | RMW race lint ratchet (95 violations frozen as backlog, CI fails on new) + 3 hot-path handler fixes |
| #785 | 🟡 auto-merging | Home = Urgent only + cross-table staleness fix in `get_expiring_soon` |
| #786 | 🟡 auto-merging | RMW batch 1 — 6 hot-path handlers (lookup, rescan_mfg, rename, reparse, quick_price_save, bulk-scrape, bulk-paste) |

**RMW backlog: 95 → 85.** Lint ratchet test (`tests/test_rmw_race_lint.py`) enforces: no new violations may be added; fixes must remove their entry from `KNOWN_VIOLATIONS` to count.

## Two substrate rules, now both executable

1. **Auto sources never override operator data.** Any path that fills `cost` / `price` / `markup` / `item_link` must gate on `if not <field>:` — never on confidence comparisons. Pinned by `tests/test_enrichment_respects_operator_cost.py` + `tests/test_enrich_step4b_no_link_overwrite.py`.

2. **Load + mutate + save must be atomic.** Wrap the sequence in `with _save_pcs_lock:` / `with _save_rfqs_lock:` (RLock — re-entrant). 11 handlers fixed; 85 remain on the backlog. Pinned by `tests/test_rmw_race_lint.py`.

## What Mike rescoped this morning

> "how could i be worried about growth when i'm struggling to send a basic quote in right now"

**Killed from scope:**
- Find buyers / growth route cleanup (5,159 LOC, 60+ orphan endpoints) — DEFERRED until quoting flow is reliable
- Action Items widget at top of home — DELETED (Mike: "ignored because of stale data")
- Action Needed card — DELETED ("important but I do step A in the app directly")
- Progress card — DELETED (duplicates Revenue/Manager card KPIs)

**Kept + made accurate:**
- Urgent card — kept, plus the actual staleness root cause fixed (`api_rfq_mark_won` writes `rfqs.status` only; `quotes.status` stays at 'sent'; `get_expiring_soon` now cross-checks the rfqs table to drop terminal-RFQ quotes from the expiring list).

## What's left for the new window

Mike wants to do these sequentially, with feedback at each step:

### 1. RMW batches 2–N (85 violations remaining)

**Batch plan:** ~12 handlers per PR, grouped by file for tight blast radius. Mike OK'd:
- Mechanical wrapper-rename application (no per-handler review)
- Spot-check one handler per batch (read the wrapped function, verify the `with` covers all return paths)
- Smoke after each batch: `pytest tests/test_pc_generation.py tests/test_rfq_generation.py tests/test_golden_path.py`
- Lint ratchet enforces decrement (the `KNOWN_VIOLATIONS` set in `tests/test_rmw_race_lint.py` is the single source of truth for the backlog)

**Suggested batch grouping (rough):**
- Batch 2: `routes_pricecheck.py` remaining (~9 handlers)
- Batch 3: `routes_pricecheck_admin.py` (~13 handlers)
- Batch 4: `routes_pricecheck_pricing.py` + `routes_pricecheck_v2.py` (~6 handlers)
- Batch 5: `routes_rfq.py` remaining (~9 handlers)
- Batch 6: `routes_rfq_admin.py` (~16 handlers)
- Batch 7: `routes_rfq_gen.py` (~11 handlers)
- Batch 8: `routes_analytics.py` remaining (~9) + `routes_cchcs_packet.py` (~2)

### 2. P1 #8 — resolver consolidation (architecturally high-stakes)

**Mike has NOT answered the grill questions yet.** The new window should ask first:
- Which resolver wins: `institution_resolver` (lowercase keys) or `facility_registry`?
- When neither finds a match, default to: `cchcs` (current), `unknown` (force review), or last-known-good?
- One PR or N (PR-1 introduce unified, PR-2..N migrate writers, PR-final delete deprecated)?

This is **multi-PR by definition**. Don't autonomous-ship without Mike's call on those three.

### 3. Whatever Mike puts on the table next

Drop the previous "find buyers" item. He's explicit: quoting reliability first.

## Drift-prevention rules for the new window

From `feedback_no_patching_slop_kpi_architecture.md`:
- When a bug surfaces on a surface I touched in the last ~7d → STOP, audit, name the substrate issue, propose ONE substrate fix, ask Mike before shipping.
- 8-PR sessions look productive but are overhead if none remove a failure class.

From `feedback_one_task_at_a_time_no_drift.md`:
- When live-quoting bugs arrive mid-PR: park current work, fix the new one, don't fan out into parallel investigations.

From this morning:
- **Growth = scope creep until basic quote-sending is reliable.** Don't bring it up.

## Memory pointers

- `project_substrate_session_2026_05_06_overnight.md` — full session narrative
- `project_url_paste_substrate_2026_05_06.md` — PR #777 detail
- `feedback_no_patching_slop_kpi_architecture.md` — Mike's anti-patching directive
- `tests/test_rmw_race_lint.py` — the backlog (single source of truth)

## How to resume

Open a new Claude window in this worktree (`C:/Users/mikeg/rfq-pr5-url-paste-substrate`). Tell it:

> "Read `docs/HANDOFF_2026_05_06_NEW_WINDOW.md`. Pick up from RMW batch 2 unless Mike redirects."

The new window will have fresh context and the lint ratchet to keep itself honest.
