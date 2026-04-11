# Active Workstreams

Track all in-progress work across Claude Code context windows.
**Every context window MUST read this before starting work and update it when creating/finishing branches.**

## Current Branches

| Branch | Context | Status | Description | Started |
|--------|---------|--------|-------------|---------|
| (none active) | — | — | All PRs merged | — |

## Coordination Rules

1. **Check this file FIRST** before creating a new branch
2. If your work overlaps with an active branch, coordinate — don't create a parallel branch
3. Update this table when you: create a branch, open a PR, merge, or abandon
4. Mark branches as `Merged` or `Abandoned` (don't delete rows — keep history for the session)

## Recently Completed

| Branch | Merged | Description |
|--------|--------|-------------|
| `feat/golden-path-expansion` | 2026-04-10 | Golden path: RFQ conversion + package gen tests (PR #4) |
| `feat/pipeline-v2-feedback` | 2026-04-10 | Oracle V5 + V2 pipeline feedback loops (PR #5) |
| `feat/golden-path-test` | 2026-04-10 | Golden path E2E pricing accuracy test (PR #3) |
| `fix/test-schema-sync` | 2026-04-10 | Test schema sync (PR #2) |
| `fix/qa-delivery-and-quote-number` | 2026-04-10 | QA panel fixes (PR #1) |
| `phase-14-agents` | 2026-04-09 | Extended agent fleet |
| `phase-13-agents` | 2026-04-08 | Agent infrastructure |

## Conflict Zones

Files that are frequently edited and likely to cause merge conflicts:
- `src/api/dashboard.py` — main blueprint, 6000+ lines
- `src/api/modules/routes_pricecheck.py` — PC workflow
- `src/api/modules/routes_rfq.py` — RFQ workflow
- `src/forms/price_check.py` — PDF generation
- `src/agents/growth_agent.py` — 104 functions, frequently extended
- `src/core/dal.py` — data access layer
- `CLAUDE.md` — project rules (coordinate edits)

If two windows need to touch the same conflict zone file, one should finish first.
