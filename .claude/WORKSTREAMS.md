# Active Workstreams

Track all in-progress work across Claude Code context windows.
**Every context window MUST read this before starting work and update it when creating/finishing branches.**

## Current Branches

| Branch | Context | Status | Description | Started |
|--------|---------|--------|-------------|---------|
| fix/ci-staging-pipeline | Window 1 | Active | Fix CI pre-deploy, add staging gate, branch protection | 2026-04-11 |

## Stale / Abandoned Branches (cleanup needed)

| Branch | Status | Notes |
|--------|--------|-------|
| feat/platform-upgrade | Abandoned | 0 commits ahead of main, content pushed directly to main instead |
| feat/north-star-p1 | Abandoned | 0 commits ahead, 19 behind — empty branch |
| feat/oracle-v5-qa-ux | Abandoned | 0 commits ahead, 4 behind — empty branch |
| feat/v5-remaining | Stale | 0 ahead, 3 behind — had uncommitted work, now stashed |
| feat/pricing-v2-migration | Stale | 1 commit ahead, 30 behind — too far diverged to merge |
| feat/v5-cost-triggers | Abandoned | 0 commits ahead — content pushed directly to main |
| feat/repo-hardening | Stale | 1 commit ahead (docling feature flag) — needs PR or abandon |

## Coordination Rules

1. **Check this file FIRST** before creating a new branch
2. If your work overlaps with an active branch, coordinate — don't create a parallel branch
3. Update this table when you: create a branch, open a PR, merge, or abandon
4. Mark branches as `Merged` or `Abandoned` (don't delete rows — keep history for the session)
5. **One window = one branch = one PR.** Never push directly to main.

## Recently Completed

| Branch | Merged | Description |
|--------|--------|-------------|
| `feat/final-cleanup` | 2026-04-11 | Delete stubs, growth tabs, 12 new tests (PR #9) |
| `feat/data-layer-refactor` | 2026-04-11 | Move data access from dashboard.py to data_layer.py (PR #8) |
| `feat/intelligence-layer` | 2026-04-11 | UNSPSC, COO, Docling, NL Query, Compliance Matrix (PR #7) |
| `feat/arch-gaps` | 2026-04-11 | Task consumer, crash recovery, durability, logging (PR #6) |
| `feat/pipeline-v2-feedback` | 2026-04-10 | Oracle V5 + V2 pipeline feedback loops (PR #5) |
| `feat/golden-path-expansion` | 2026-04-10 | Golden path: RFQ conversion + package gen tests (PR #4) |
| `feat/golden-path-test` | 2026-04-10 | Golden path E2E pricing accuracy test (PR #3) |
| `fix/test-schema-sync` | 2026-04-10 | Test schema sync (PR #2) |
| `fix/qa-delivery-and-quote-number` | 2026-04-10 | QA panel fixes (PR #1) |

## Conflict Zones

Files that are frequently edited and likely to cause merge conflicts:
- `src/api/dashboard.py` — main blueprint, 5000+ lines
- `src/api/data_layer.py` — extracted data access functions
- `src/api/modules/routes_pricecheck.py` — PC workflow
- `src/api/modules/routes_rfq.py` — RFQ workflow
- `src/api/modules/routes_catalog_finance.py` — catalog + finance
- `src/forms/price_check.py` — PDF generation
- `src/core/dal.py` — data access layer
- `.github/workflows/ci.yml` — CI pipeline
- `CLAUDE.md` — project rules (coordinate edits)

If two windows need to touch the same conflict zone file, one should finish first.
