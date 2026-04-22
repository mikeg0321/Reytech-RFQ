# Active Workstreams

Track all in-progress work across Claude Code context windows.
**Every context window MUST read this before starting work and update it when creating/finishing branches.**

## Current Branches

> **Worktree column is MANDATORY.** Every parallel window must own a distinct
> working directory. See `CLAUDE.md → Worktrees Are Required for Parallel Windows`.
> Use `make worktree name=feat/topic` to create one; `make worktree-list` to audit.

| Branch | Context | Worktree | Status | Description | Started |
|--------|---------|----------|--------|-------------|---------|
| fix/ci-staging-pipeline | Window 1 | `C:\Users\mikeg\Reytech-RFQ` (main checkout) | Active | Fix CI pre-deploy, add staging gate, branch protection | 2026-04-11 |
| fix/amazon-garbage-title-filter | Window 2 | `C:\Users\mikeg\rfq-amazon-garbage-title-filter` | Active | Filter garbage "Amazon.com" titles so Claude tier fires + bump max_tokens | 2026-04-15 |
| feat/quote-engine-unified | (merged) | `C:\Users\mikeg\rfq-quote-engine-unified` | Merged PR #135 | Unified quote_engine orchestrator + boot profile validator + parse_engine fix | 2026-04-17 |
| feat/simple-submit-quote-engine | (merged) | `C:\Users\mikeg\rfq-simple-submit-quote-engine` | Merged PR #136 | Migrate routes_simple_submit.py to call quote_engine.draft (Phase 3 first route) | 2026-04-18 |
| feat/quote-counter-unique | (merged) | `C:\Users\mikeg\rfq-quote-counter-unique` | Merged PR #137 | Quote-counter audit + UNIQUE constraint installer + JSON drift fix | 2026-04-18 |
| feat/deadline-alert-snooze | (merged) | `C:\Users\mikeg\rfq-deadline-alert-snooze` | Merged PR #138 | Deadline-alert snooze: localStorage persistence + Pause 30m / Pause 1h / Bypass today | 2026-04-18 |
| fix/health-startup-shadow | (merged) | `C:\Users\mikeg\rfq-health-startup-shadow` | Merged PR #139 | Delete dashboard.py shadow of /api/health/startup so app.py rich handler wins | 2026-04-18 |
| feat/704b-profile | (merged) | `C:\Users\mikeg\rfq-704b-profile` | Merged PR #140 | Add 704b_reytech_standard profile + blank fixture | 2026-04-18 |
| feat/strict-boot-validator | (merged) | `C:\Users\mikeg\rfq-strict-boot-validator` | Merged PR #141 | Flip boot validator to strict — bad profile blocks app boot in prod | 2026-04-18 |
| feat/703a-profile | (merged) | `C:\Users\mikeg\rfq-703a-profile` | Merged PR #142 | Add 703a_reytech_standard profile + blank fixture | 2026-04-18 |
| feat/golden-test-fixture | (merged) | `C:\Users\mikeg\rfq-golden-test-fixture` | Merged PR #143 | Test0321/R26Q0321 golden fixture + 28 real CCHCS items + seed script | 2026-04-18 |
| feat/quote-orchestrator | (merged) | `C:\Users\mikeg\rfq-quote-orchestrator` | Merged (platform PR) | Platform build: QuoteOrchestrator state machine + FormProfiler agent + ComplianceValidator + /quoting/status dashboard + playbook (`docs/PLATFORM_QUOTING.md`). | 2026-04-18 |
| feat/calvet-r25q86-proof | (merged) | `C:\Users\mikeg\rfq-calvet-r25q86-proof` | Merged PR #156 | CalVet R25Q86 E2E proof + new pass_through and generated fill modes + sellers_permit_reytech and quote_reytech_letterhead profiles. | 2026-04-19 |
| feat/ui-tier1-status-overhaul | (other window) | `C:\Users\mikeg\rfq-ui-tier1` | Active | UI Tier 1 from Grok audit: live status auto-refresh + timeline stepper polish + override+retry modal + new POST /api/quoting/retry. Touches `quoting_status.html`, `quoting_status_detail.html`, `routes_quoting_status.py`. | 2026-04-19 |
| feat/manual-submit-emergency | (merged) | `C:\Users\mikeg\rfq-manual-submit-emergency` | Merged PR #239 | B1: 704 Rebuild Phase 0 — POST /rfq/<rid>/manual-submit emergency route. | 2026-04-19 |
| feat/rfq-contract-builder | This window | `C:\Users\mikeg\rfq-rfq-contract-builder` | Active | Unified Contract Builder: single dropzone on RFQ detail auto-classifies uploads → 703B/704B/bidpkg template slots, email screenshots, or attachments. New `src/forms/form_classifier.py` + `/api/rfq/<rid>/contract-upload` route + dropzone block in `rfq_detail.html`. | 2026-04-20 |
| chore/deploy-speedup-ignore-sleep | (merged) | `C:\Users\mikeg\rfq-deploy-speedup-ignore-sleep` | Merged PR #413 | Shave `make promote` time: .dockerignore backups + /version poll replaces sleep 90. | 2026-04-22 |
| chore/deploy-serialize-await-idle | This window | `C:\Users\mikeg\rfq-deploy-serialize-await-idle` | Active | Structural fix for burst-merge preemption: `scripts/await_deploy_idle.sh` + `make await-idle` + opt-in `serial=1` on `make ship`. Every introspection failure exits 0 so the release pipeline cannot be broken by this tool. | 2026-04-22 |

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
| `chore/worktree-protocol` | 2026-04-12 | Require git worktrees for parallel Claude windows; `make worktree` / `worktree-remove` / `worktree-list` (PR #24) |
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
