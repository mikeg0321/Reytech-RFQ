# Active Workstreams

Track all in-progress work across Claude Code context windows.
**Every context window MUST read this before starting work and update it when creating/finishing branches.**

## Current Branches

> **Worktree column is MANDATORY.** Every parallel window must own a distinct
> working directory. See `CLAUDE.md → Worktrees Are Required for Parallel Windows`.
> Use `make worktree name=feat/topic` to create one; `make worktree-list` to audit.

| Branch | Context | Worktree | Status | Description | Started |
|--------|---------|----------|--------|-------------|---------|
| fix/bid-recurrence-detection | (auto session) | `C:\Users\mikeg\rfq-bid-recurrence-detection` | Active | Mike's 2026-05-05 ask: same institution + same item descriptions + same qty = bid recurrence. New `src/core/bid_recurrence.py` matcher + chip on PC detail page surfacing prior PCs at the same institution that ≥75% match by qty+description. Read-side only; at-ingest persistence is a follow-up PR. 23 tests. | 2026-05-05 |
| fix/rfq-misclassified-as-pc | (auto session) | `C:\Users\mikeg\rfq-rfq-misclassified-as-pc` | Active PR #736 | P0: RFQ "RFQ - Informal Competitive - Attachment 1-_ 10838974.pdf" auto-classified as PC because `dashboard.py:_is_pc_filename` field-set heuristic matched on CCHCS-flavored RFQ form fields. Adds early hard-reject for filenames starting with rfq/solicitation/informal-competitive. 3 source-level guard tests. | 2026-05-05 |
| chore/workstreams-sync | (auto session) | `C:\Users\mikeg\rfq-workstreams-sync` | Merged PR #735 | Doc-only sync to mark #733/#734 as merged. | 2026-05-05 |
| fix/asin-recycle-detection | (auto session) | `C:\Users\mikeg\rfq-asin-recycle-detection` | Merged PR #734 | Surface #6: Amazon ASIN recycling — `product_research._cache_store` flags `recycled_suspected` when an `asin:<ASIN>` cache write has token-overlap < 0.30 with the previous title; PC QA emits a soft WARNING for items whose ASIN is flagged. 18 tests. | 2026-05-05 |
| fix/auto-pc-display-name-from-attachment | (auto session) | `C:\Users\mikeg\rfq-auto-pc-display-name-from-attachment` | Merged PR #733 | Surface #17 follow-on: queue list shows attachment title for AUTO_<hex> PCs/RFQs (read-side override in `normalize_queue_item`; record's pc_number left intact for routing/identity). 9 source-level guard tests. | 2026-05-05 |
| fix/price-history-band-click-target | (auto session) | `C:\Users\mikeg\rfq-price-history-band-click-target` | Merged PR #728 | Surface #14: PRICE HISTORY INTELLIGENCE band whole-band click target. | 2026-05-04 |
| fix/ship-to-autofill-facility-registry | (auto session) | `C:\Users\mikeg\rfq-ship-to-autofill-facility-registry` | Merged PR #726 | Surface #15: auto-fill ship_to from facility_registry at ingest. | 2026-05-04 |
| fix/kill-profit-floor | (auto session) | `C:\Users\mikeg\rfq-kill-profit-floor` | Merged PR #724 | Surface #16: kill profit floor entirely. | 2026-05-04 |
| fix/pc-status-flip-on-generate | (auto session) | `C:\Users\mikeg\rfq-pc-status-flip-on-generate` | Merged PR #725 | Surfaces #11+#13: post-generate status flip from "draft"→"completed". | 2026-05-04 |
| fix/pc-name-from-attachment-filename | (auto session) | `C:\Users\mikeg\rfq-pc-name-from-attachment-filename` | Merged PR #727 | Surface #17: PC + RFQ name cascade falls back to attachment filename before AUTO_<hash>. | 2026-05-04 |
| fix/mark-sent-on-pc-detail | (auto session) | `C:\Users\mikeg\rfq-mark-sent-on-pc-detail` | Active | Surface #12: PC detail page Mark Sent Manually escape valve. Adds modal + More-dropdown entry gated by `_pc_allow_mark_sent` (any non-sent, non-terminal status). Mirrors RFQ Bundle-5 PR-5b. POSTs to existing `/api/pricecheck/<pcid>/mark-sent-manually`. + 7 source-level guard tests. | 2026-05-04 |
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
| feat/route-module-load-gate | (auto session) | (worktree removed) | Merged PR #532 | DATA_ARCHITECTURE_MAP §1 silos S1+S11: boot WARNING + hard CI test on disk-vs-`_ROUTE_MODULES` drift; URL-map floor tripwire in golden path. Deployed 2026-04-25 commit `17f7d2c6`; prod boot log clean (36 modules loaded, 0 drift). | 2026-04-25 |
| chore/architecture-map-doc | (auto session) | (worktree removed) | Merged PR #533 | Doc-only PR: landed `docs/DATA_ARCHITECTURE_MAP.md`. Deployed 2026-04-25 commit `3ffba64`. |
| chore/delete-vestigial-award-queue | (auto session) | (worktree removed) | Merged PR #534 | DATA_ARCHITECTURE_MAP §7 silo S7 closed. Deployed 2026-04-25 commit `7ece31c`. Migration 29 dropped 32 prod orphan rows. |
| chore/delete-facility-db-tombstone | (auto session) | `C:\Users\mikeg\Documents\rfq-delete-facility-db-tombstone` | Active | DATA_ARCHITECTURE_MAP §7 silo S2: surgical delete of `quote_generator.FACILITY_DB` (~40-row dict with stale audit-W data — CSP-SAC at "300 Prison Road" instead of "100"), `ZIP_TO_FACILITY` (dead constant), `_lookup_facility_legacy` (zero callers, embedded `_CITY_MAP`). `core/facility_registry.FACILITIES_BY_CODE` becomes sole source. ~100 LOC delete + 3 absence-guard tests. Closes the TODO at `quote_contract.py:61-62`. | 2026-04-25 |
| chore/deploy-speedup-ignore-sleep | (merged) | `C:\Users\mikeg\rfq-deploy-speedup-ignore-sleep` | Merged PR #413 | Shave `make promote` time: .dockerignore backups + /version poll replaces sleep 90. | 2026-04-22 |
| chore/deploy-serialize-await-idle | (prior window) | `C:\Users\mikeg\rfq-deploy-serialize-await-idle` | Active | Structural fix for burst-merge preemption: `scripts/await_deploy_idle.sh` + `make await-idle` + opt-in `serial=1` on `make ship`. Every introspection failure exits 0 so the release pipeline cannot be broken by this tool. | 2026-04-22 |
| fix/scprs-dashboard-sql-syntax | This window | `C:\Users\mikeg\Reytech-RFQ` (main checkout) | Merged PR #484 | P0: `/intel/scprs` empty for 7 weeks — `" + where + "` literal in 4 SQL strings in `scprs_universal_pull.py`. Convert to f-strings + 4 regression tests (shape, filter, empty-db, injection-lock). | 2026-04-23 |
| fix/scprs-dedup-and-error-surface | This window | `C:\Users\mikeg\Reytech-RFQ` (main checkout) | Merged PR #488 | Follow-up to #484: §3d migration 22 dedups `scprs_po_lines` on (po_id, line_num) + adds UNIQUE INDEX (re-pull dupes inflated 7-week totals). §2 replaces bare-except in `page_intel_scprs` with `log.exception` + red error banner that suppresses the misleading "no data" banner. 7 new tests + Chrome-verified error-state. Deployed in commit 746c9c14 (preempted by in-flight #489 → rolled forward into 7c117eac). | 2026-04-23 |
| fix/scprs-is-test-isolation | This window | `C:\Users\mikeg\Reytech-RFQ` (main checkout) | Active | §3e is_test on SCPRS tables: migration 23 adds `is_test INTEGER NOT NULL DEFAULT 0` to `scprs_po_master` + `scprs_po_lines` (idempotent, fresh-install-safe). 13 read sites filtered: 4 in `scprs_universal_pull` (status, intel x4, auto-close-lost), 2 in `pricing_oracle_v2` (search_po_lines, get_cross_sell), 5 in `routes_intel` (System Health card), 2 in `routes_search` (global search), 2 in `quote_intelligence` (competitor_prices, reytech_prices). 8 new tests including the headline "synthetic test PO can NEVER auto-close a real quote" + inverse positive. 91/91 SCPRS suite green. | 2026-04-23 |
| feat/bundle-4-lpa-fill-engine | Window 1 | `C:\Users\mikeg\rfq-bundle-4-lpa-fill-engine` | Merged PR #447 | Bundle-4 PR-4a: fill_cchcs_it_rfq() + 703b-slot fingerprint dispatcher. Closes audit item M. Touches `src/forms/reytech_filler_v4.py` + `src/api/modules/routes_rfq_gen.py`. | 2026-04-22 |
| feat/bundle-4-lpa-classifier-shape | Window 1 | `C:\Users\mikeg\rfq-bundle-4-lpa-classifier-shape` | Merged PR #448 | Bundle-4 PR-4b: classifier shape=cchcs_it_rfq + LPA body keywords + template fingerprint. Touches `src/core/request_classifier.py` + `src/core/ingest_pipeline.py`. Confidence gate 0.70 → operator review lane below. | 2026-04-22 |
| feat/item-z-fillable-quote | Window 1 | `C:\Users\mikeg\rfq-item-z-fillable-quote` | Merged PR #450 | Item Z (release valve): Reytech-owned fillable Quote PDF + /rfq/<rid>/submit-edited-quote + 30d audit log of operator edits. Touches `src/forms/quote_generator.py` + new route in `src/api/modules/routes_rfq_gen.py`. | 2026-04-22 |
| (reserved for Window 1) | Window 1 | `C:\Users\mikeg\rfq-bundle-1-facility-registry` | Reserved | Bundle-1: canonical facility registry, resolver rewrite, unified tax pipeline, quote_generator canonical write-back. Touches `src/core/institution_resolver.py`, `src/core/agency_config.py`, `src/forms/quote_generator.py`, `src/api/modules/routes_rfq.py`, `src/api/modules/routes_pricecheck.py`. **HOLDING on DB migration until Mike greenlights.** | 2026-04-22 |
| feat/bundle-5-sent-status | Window 2 | `C:\Users\mikeg\rfq-bundle-5-sent-status` | In review (PR #449) | Bundle-5 PR-5a+5b: sent-status hygiene audit + gated `scripts/backfill_sent_status.py` (`make run-backfill-sent-status`) + /api/{rfq,pricecheck}/<id>/mark-sent-manually endpoints + RFQ detail modal (sent_to, sent_at, attachment, notes) + post-send line-item readonly lock (RFQ + PC). Touches `routes_rfq_admin.py`, `routes_pricecheck_pricing.py`, `rfq_detail.html`, `pc_detail.html`, `data_layer.py`. 28 tests. | 2026-04-22 |
| feat/bundle-6-linker-pricing-copy | Window 2 | `C:\Users\mikeg\rfq-bundle-6-linker-pricing-copy` | In review (PR #451) | Bundle-6 PR-6a+6b: `_copy_pc_pricing_to_rfq()` post-link hook in `ingest_pipeline.py` copies PC `item['pricing']` onto RFQ items by desc match (idempotent, merge-not-clobber); "Pricing copied from PC #X" banner on RFQ detail; RFQ items table MFG# moved LEFT of Description; KPI strip expanded from 4 → 6 cells with Subtotal + Tax (rate label) + Total + Profit. Touches `src/core/ingest_pipeline.py`, `src/templates/rfq_detail.html`. 15 tests. | 2026-04-23 |
| fix/calvet-barstow-tax | This window | `C:\Users\mikeg\rfq-calvet-barstow-tax` | Active | Global tax fix: `FacilityRecord.tax_rate` (operator-verified canonical) — Barstow stamped 8.75% (CDTFA misses district add-on). Resolver short-circuits to canonical rate before CDTFA. PC `/api/pricecheck/<id>/lookup-tax-rate` migrated onto `tax_resolver.resolve_tax` (closes audit Y for PC). Touches `src/core/facility_registry.py`, `src/core/tax_resolver.py`, `src/api/modules/routes_pricecheck.py`, `tests/test_tax_resolver.py`. 4 new regressions. | 2026-04-23 |
| feat/drop-facility-addresses-parallel-dict | (auto session) | `C:\Users\mikeg\rfq-drop-facility-addresses-parallel-dict` | Active | Plan §4.2 / S2 follow-up: delete `institution_resolver._FACILITY_ADDRESSES` parallel dict + dead `get_ship_to_address()` (zero external callers — `ship_to_resolver` migrated to `quote_contract.ship_to_for_text` on 2026-04-25). Canonical `FacilityRecord` already holds every address; consistency tests confirm parity. Also deletes vestigial `tests/test_institution_resolver_canonical_consistency.py` and adds absence-guard ratchets to `tests/test_ship_to_resolver_canonical.py`. Touches `src/core/institution_resolver.py`, `src/core/quote_contract.py` (docstring milestone), `src/core/ship_to_resolver.py` (comment). | 2026-04-27 |
| fix/quote-agency-first-and-race-fence | This window | `C:\Users\mikeg\rfq-quote-agency-first-and-race-fence` | Active | Fix-B + Fix-C + golden E2E from 2026-04-24 product-engineer review. Fix-B: `generate_quote_from_pc` / `generate_quote_from_rfq` consult `facility_registry.resolve_by_agency_key()` BEFORE text-based `_lookup_facility` chain (closes f81c4e9b → Calipatria mis-render). Fix-C: Convert / Reclassify JS awaits `_flushPcAutosave()` before POST (drains in-flight save) + server stamps `last_save_at` / `last_save_seq` for observability. 13 new E2E regression tests pin the fix. Touches `src/core/facility_registry.py`, `src/forms/quote_generator.py`, `src/templates/pc_detail.html`, `src/api/modules/routes_pricecheck.py`, `src/api/modules/routes_analytics.py`, `tests/test_quote_package_consistency_e2e.py`. | 2026-04-24 |
| fix/errorhandler-passthrough-http | (auto session) | `C:\Users\mikeg\rfq-errorhandler-passthrough-http` | Active | 2026-05-01 ghost-arc follow-up: `app.errorhandler(Exception)` masks `HTTPException` subclasses (405/403/400/etc.) as synthesized 500s. Fix re-raises HTTPException so Flask renders the correct 4xx natively; specific handlers (404, 413, 500) still win. Browser users never hit this; scripted/curl clients did (POST /generate-package → 302 → curl -L re-POSTs to GET-only /review-package → MethodNotAllowed → masked as 500). Touches `app.py` only + new `tests/test_app_errorhandler.py` (5 tests). | 2026-05-01 |
| feat/review-package-alignment | (auto session) | `C:\Users\mikeg\rfq-review-package-alignment` | Active | **PR-A of the global send-flow fix.** Mike: "go through regenerate and approval, then figure out what to do next? makes no sense." Replaces 3 scattered banners on `/review-package` with one alignment rollup (5 checks: forms-on-disk / QA / source-validation / buyer-agency / items-priced) → green "READY TO SEND" or red issue list. New items-alignment table (buyer-asked vs your 704B, with "no source captured" banner when parsed-buyer-items absent — flushable wipes case). Promotes Deliverables list to filenames+sizes+QA-pill (the "ESPECIALLY package to make sure i returned all forms" piece). Removes Force Approve. Touches `src/api/review_alignment.py` (new pure-logic module), `src/api/modules/routes_rfq.py`, `src/templates/rfq_review.html`, `tests/test_review_alignment.py` (18) + `tests/test_review_package_route.py` (4). PR-B (draft+preview send flow) follows. | 2026-05-01 |

### Window 2 (recommended lanes, zero overlap)
- `feat/bundle-5-sent-status` — send/mark-sent/archive audit + "Mark as sent manually" UI button + backfill (gated). Touches send paths, `rfq_detail.html` button, new endpoint. **PR #449 armed.**
- `feat/bundle-6-linker-pricing-copy` — post-link hook: copy PC item['pricing'] → RFQ on match + RFQ UI parity (MFG# left, summary stack). Touches `src/core/ingest_pipeline.py` (ONLY the `_run_triangulated_linker` tail, not ingest-start) + `src/templates/rfq_detail.html`. **PR #451 armed.**
- Pre-2026-04-22 tech debt: DB bloat vacuum (`project_db_bloat_findings_2026_04_20`), `TestEndToEndCchcsGolden` (`project_ci_gate_failures_2026_04_20`), 704 rebuild handoff (`project_704_rebuild_handoff`).

**Window 2 stay-out files:** `src/forms/quote_generator.py`, `src/forms/reytech_filler_v4.py`, `src/api/modules/routes_rfq_gen.py`, `src/core/request_classifier.py`, `src/core/institution_resolver.py`, `src/core/agency_config.py`. Window 1 is touching these in the Bundle-1/4/Z track.

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
