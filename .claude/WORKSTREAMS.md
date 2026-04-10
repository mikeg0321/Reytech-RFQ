# Active Workstreams

Track all in-progress work across Claude Code context windows.
**Every context window MUST read this before starting work and update it when creating/finishing branches.**

## Current Branches

| Branch | Context | Status | Description | Started |
|--------|---------|--------|-------------|---------|
| `fix/quote-counter-agency-package` | — | Open PR | Quote counter + agency config fixes | 2026-04-08 |
| `tech-debt-qa-agent` | — | WIP | QA agent + LangGraph orchestrator | 2026-04-06 |

## Coordination Rules

1. **Check this file FIRST** before creating a new branch
2. If your work overlaps with an active branch, coordinate — don't create a parallel branch
3. Update this table when you: create a branch, open a PR, merge, or abandon
4. Mark branches as `Merged` or `Abandoned` (don't delete rows — keep history for the session)

## Recently Completed

| Branch | Merged | Description |
|--------|--------|-------------|
| `phase-14-agents` | 2026-04-09 | Extended agent fleet |
| `phase-13-agents` | 2026-04-08 | Agent infrastructure |

## Conflict Zones

Files that are frequently edited and likely to cause merge conflicts:
- `src/api/dashboard.py` — main blueprint, 4000+ lines
- `src/api/modules/routes_pricecheck.py` — PC workflow
- `src/api/modules/routes_rfq.py` — RFQ workflow
- `src/forms/fill_ams704.py` — PDF generation
- `src/agents/growth_agent.py` — 104 functions, frequently extended
- `src/core/dal.py` — data access layer
- `CLAUDE.md` — project rules (coordinate edits)

If two windows need to touch the same conflict zone file, one should finish first.
