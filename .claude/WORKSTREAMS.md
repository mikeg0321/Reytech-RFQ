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
| `feat/wolfpack-operating-model` | PR #1061 | CLAUDE.md §0 + the four pack agents. |
| `feat/job0-convergence-ratchet` | Job #0 ship | convergence_baseline.json + LAW 3 ratchet tests + this file. |

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
