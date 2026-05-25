# Active Workstreams

One repo, one main. Track in-progress work here. Every context window reads
this before starting and updates it on branch create / PR / merge / abandon.

> **Den collapsed 2026-05-21 — Job #0.** 138 working directories → 1 canonical
> repo (`C:\Users\mikeg\rfq-spine-sequential-numbering`) + `Reytech-RFQ`.
> 1,666 branches → 58 (1,608 confirmed-merged branches pruned). The pre-Job-#0
> branch table — dozens of merged/abandoned rows back to 2026-04 — was stale
> cruft and has been removed. Audit trail of exactly what was deleted:
> `_diag/job0_branch_killlist.txt` and `_diag/job0_worktrees_removed.txt`.

## Operating model

`CLAUDE.md §0` is the law. Worktrees only — never new clones — capped at 10
live. `make worktree name=feat/topic` to create; `make worktree-list` to audit.
Only the Architect authorizes substrate / schema / migration changes (LAW 4).

## Current branches

| Branch | Status | Notes |
|--------|--------|-------|
| `main` | canonical | Never pushed to directly. |
| `feat/job1-pr3-forms-render` | Job #1 PR-3 | `src/spine/forms_render.py` — the Format-B CCHCS adapter (703B/703C + 704B + Bid Package) + format-aware /forms routes. |
| `fix/flatten-regen-appearance-before-bake` | PR-10, shipping | `src/spine/flatten.py` — regenerate widget appearances (`fitz.Widget.update()`) BEFORE `bake()`. Fixes Demidenko PC comb-spacing + clipping caught 2026-05-23. Worktree: `rfq-spine-sequential-numbering`. |
| `fix/liveness-scprs-source-canonical-writer` | PR A of liveness silent-failure triage 2026-05-25 | SCPRS liveness check + `scprs_browser` writer. (1) `liveness_checks.py` — new `_multi_source_freshness` primitive; SCPRS check reads `MAX(scprs_po_master.pulled_at, scraped_at)`. (2) `scprs_browser._store_results` — writes both columns. Closes the false-alarm class: scheduled browser scrape wrote `scraped_at`, check only read `pulled_at`. Same shape as PR #1076 substrate-sourcing fix. 13/13 tests green. Worktree: `rfq-liveness-scprs-source-canonical-writer`. |
| `fix/email-poller-restartable` | PR B of liveness silent-failure triage 2026-05-25 | `routes_crm.start_polling` — register the `email-poller` thread with `register_restartable` so the job watchdog revives a dead thread. Closes the silent-death class: 4 days silent inbound with 3 unread CDCR RFQs in the sales inbox 2026-05-21→25. Mirrors the pattern used by the 4 other daemons in `dashboard.py` (follow-up-engine, award-tracker, oracle-weekly-report, cross-sell-weekly-digest). 3/3 new tests + scheduler substrate verified. Worktree: `rfq-email-poller-restartable`. |
| `fix/liveness-quotes-spine-aware` | PR C of liveness silent-failure triage 2026-05-25 | `liveness_checks.py` — new `_quote_ingestion_freshness()` helper reads MAX(created_at) across legacy `quotes` AND Spine `spine_quotes`. Closes the false-alarm class for the Quote ingestion check: per §0 LAW 1 the Spine is canonical, new quotes flow through `spine_quotes`, so reading only `quotes.created_at` triggered an 11d-silent alarm while spine_quotes was current. Collapses to single-table read when legacy `quotes` is DELETED at the end of the Spine migration. 11/11 tests green. Worktree: `rfq-liveness-quotes-spine-aware`. |
| `fix/oracle-substrate-telegram` | Active | Mr. Wolf pass on the 2026-05-25 empty `[ORACLE] Weekly Intelligence` screenshot (0/0/0 KPIs above a 100+-sample calibration table = two substrate tables on one card). Three changes in `src/agents/`: (1) `notify_agent.py` — new `_send_telegram` + MarkdownV2 escaper + `TELEGRAM_*` env vars + `CHANNEL_MAP` reorganized into three tiers (ACTIONABLE sms/email/bell, REPORTS telegram/bell, INTELLIGENCE telegram+email/bell); (2) `oracle_weekly_report.py` — KPI #3 re-sourced from `winning_prices.COUNT(*)` (win-only path, empty in prod) → `SUM(oracle_calibration.sample_size)` and relabeled "Calibration Samples"; (3) `award_tracker.py` — fires `award_tracker_idle` (daily-bucketed) when run_award_check sees 0 eligible but >0 recent quotes (exposes silent Mark-Sent break). Removed explicit `channels=["email"]` from oracle + cross_sell so routing follows the new map. **Patch 4 (Mark-Sent invariant) deferred** — write seams are scattered across dashboard/routes_pc/routes_rfq + Spine `/send-prep`, not in `set_quote_status_atomic`; Architect-level call on where invariant belongs. Idle alarm covers same break. 24 new tests + 286/286 critical green. **Operator todo before merge:** BotFather → `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` + `TELEGRAM_ENABLED=1` on Railway. Channel silently degrades if missing — bell still fires + the three INTELLIGENCE-tier events keep email as backup. Worktree: `Reytech-RFQ` (main checkout). |

## Preserved branches — await Closer triage

Job #0 kept 28 branches that have unmerged commits and no merged PR, plus the
6 explicitly preserved in Step 1. They are NOT abandoned and NOT confirmed
live — the Closer triages each (live vs dead) before Job #1. Full list:
`_diag/job0_branches_kept.txt`. Known callouts: `feat/spine-shadow-ingest-and-bidpkg`
— Architect flagged as possibly superseded by PR #1033; `feat/spine-signature-overlay`
— memory says do NOT ship.

## Coordination rules

1. Read this file before creating a branch.
2. One window = one branch = one PR. Never push directly to `main`.
3. Update the table on branch create / PR / merge / abandon.
4. `make ship` is the only way to push.

## Conflict zones

Frequently-edited files — if two windows need the same one, sequence the work:
- `src/api/dashboard.py`, `src/api/data_layer.py`
- `src/api/modules/routes_rfq.py`, `src/api/modules/routes_pricecheck.py`
- `src/spine/model.py` (Architect-only), `src/spine/SPINE_CHARTER.md`
- `CLAUDE.md` (coordinate edits)
