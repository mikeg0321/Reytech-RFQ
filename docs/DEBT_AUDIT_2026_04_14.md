# Tech Debt Audit — 2026-04-14

*Snapshot of the Reytech-RFQ codebase at session end. Not a plan, just a
map of where debt lives so future sessions can prioritize targeted fixes.*

## Headline numbers

| Metric | Value |
|---|---|
| Python files in `src/` | 210 |
| Total LOC (`src/**/*.py`) | 169,193 |
| Templates (`src/templates/*.html`) | 54 |
| Largest template | `pc_detail.html` (4,843 lines) |
| Second largest | `rfq_detail.html` (3,265 lines) |
| Full regression test suite | 368 tests passing |
| `except Exception: pass` blocks | **189** (in 49 files) |
| Real `TODO` / `FIXME` markers | 1 (1 in quickbooks_agent) |

## The real debt: silent exception swallowing

**189** `except Exception: pass` blocks across 49 files. These are the
highest-risk debt class because:
1. They hide real bugs (the exception happened, nothing told us)
2. They mask refactor regressions (tests don't fail but behavior changes)
3. They make production incidents harder to diagnose (no log, no trace)
4. They make it hard to distinguish "expected failure" from "unknown bug"

### Top 10 offenders (worth targeted cleanup)

| Count | File | Context |
|---|---|---|
| 35 | `src/api/modules/routes_v1.py` | API v1 surface — suspicious, user-facing |
| 12 | `src/agents/pc_enrichment_pipeline.py` | Pricing pipeline — each one could hide a pricing miss |
| 12 | `src/forms/cchcs_packet_filler.py` | Most are defensive around pypdf quirks — low-risk |
| 10 | `src/agents/scprs_browser.py` | SCPRS scraping — needs graceful fallback, lower priority |
| 8 | `src/core/agent_context.py` | Context wiring — investigate |
| 6 | `src/agents/quote_reprocessor.py` | Quote regen — risky |
| 6 | `src/agents/system_auditor.py` | System audit — low-risk |
| 5 | `src/agents/scprs_public_search.py` | SCPRS scraping — same as above |
| 5 | `src/forms/cchcs_attachment_fillers.py` | Defensive, low-risk |
| 4 | `src/agents/quote_lifecycle.py` | Quote lifecycle — risky |

### Recommended remediation ordering

1. **`routes_v1.py` (35 blocks)** — user-facing API, highest business impact if a silent failure reaches external clients. One focused audit session.
2. **`pc_enrichment_pipeline.py` (12 blocks)** — each pass block in the pricing layer could hide a pricing hole. These should log at minimum.
3. **`quote_reprocessor.py` + `quote_lifecycle.py` (10 combined)** — quote regen is revenue-impacting, silent failures there mean wrong quotes ship.
4. Everything else is lower-priority. `cchcs_packet_filler.py` / `cchcs_attachment_fillers.py` blocks are mostly defensive around pypdf edge cases and fine as-is.

**Remediation pattern:**

```python
# BEFORE
try:
    risky_op()
except Exception:
    pass

# AFTER (minimum acceptable)
try:
    risky_op()
except Exception as e:
    log.debug("suppressed in X: %s", e)
```

Add log.debug at minimum so tracebacks surface during incidents. For
routes_v1 + pricing-layer blocks, graduate to `log.warning` so they
show up in prod alerts.

## Oversized modules

| Lines | File | Risk |
|---|---|---|
| 6,216 | `routes_pricecheck_admin.py` | Largest non-CSS file. Split candidates: cleanup, convert-to-rfq, recall. |
| 4,843 | `templates/pc_detail.html` | Partially shared with RFQ detail (queue tables are unified; detail not). Phase 5 deferred. |
| 3,438 | `routes_pricecheck.py` | Main PC route. Classifier integration adds a shortcut; full extraction is next session. |
| 3,359 | `routes_rfq_admin.py` | Mirror of pricecheck_admin. Same split candidates. |
| 3,265 | `templates/rfq_detail.html` | See pc_detail.html. |
| 3,240 | `routes_rfq.py` | Main RFQ route. |
| 2,928 | `routes_rfq_gen.py` | Package generation. |
| 2,003 | `routes_pricecheck_gen.py` | PC package gen. |
| 1,275 | `routes_pricecheck_pricing.py` | PC pricing. |

**Pattern**: PC and RFQ are parallel at every layer. The 5-phase unified
ingest refactor (shipped as PR #47) addresses the ingest side. The
remaining work is the generate + detail-template side, which is the
explicit follow-up from that refactor.

## Real `# TODO` markers

| File:Line | TODO | Priority |
|---|---|---|
| `src/agents/quickbooks_agent.py:523` | line-item search requires full PO refetch | low — QB integration is ops-side |

Everything else marked "TODO" / "FIXME" / "XXX" in grep output is
embedded in format-string examples (e.g. `"PREQ1234567"` or
`"B0XXXXXXXX"`) rather than actual code-debt markers.

## Known deferred items (from memory + PR descriptions)

### Shipped PRs today had explicit "not in this PR" notes
- **PR #40** — CCHCS packet: no UI button (shipped in #41), no email poller hook (deferred), no backfill (shipped in #41)
- **PR #41** — CCHCS follow-ups: completed #40's deferrals
- **PR #42** — Overlay infrastructure: DOCX calibration (shipped in #43)
- **PR #43** — DOCX 704 locked in — no deferrals
- **PR #44** — P0 resilience: Item A (skip CI for hotfix) rejected by me, not built
- **PR #45** — Pricing/email validators: Phase 3 UI modal deferred
- **PR #46** — Package completeness gate: no deferrals
- **PR #47** — Unified ingest 5 phases: template button parity (trimmed scope), old route deletion (risky, deferred), email poller integration (risky, deferred)

### From `project_rfq_remaining_deliverables.md` (recently refreshed)

1. ~~DOCX 704 overlay positioning~~ — RESOLVED in PR #43
2. **Grok LLM validator Phase 2** — Phase 2 kill switch shipped in PR #45. Phase 3 review UX color coding still pending.
3. **Review UX color coding (Pricing Pipeline Phase 3)** — depends on #2 confidence tiers being stable
4. **Supplier SKU reverse lookup follow-through** — Phase 1 already done per pricing pipeline PRD
5. **Email-as-Contract requirements extraction Phase 3 UI** — backend shipped in PR #45, frontend not yet
6. **Orders V2 cleanup** — PO→orders merge already shipped; data_json drop needs zero-read audit
7. **Golden path expansion** — already extended to cover CCHCS in PR #43
8. **Bulk scrape progress UI verify** — one-shot verification task

## Security / vulnerability baseline

Per `remote: GitHub found 20 vulnerabilities on mikeg0321/Reytech-RFQ's default branch (1 high, 16 moderate, 3 low)` output from recent push — Dependabot is flagging 20 issues. **Not audited in this report.** Recommend a session dedicated to dependency updates + Dependabot triage.

## Recommended 3-PR cleanup cycle

**Cleanup PR 1 (~3 hr):** `routes_v1.py` pass blocks → `log.debug` conversion. 35 blocks in one file, one PR, one review surface.

**Cleanup PR 2 (~2 hr):** `pc_enrichment_pipeline.py` + `quote_reprocessor.py` + `quote_lifecycle.py` pass blocks. These touch pricing + revenue — need careful review.

**Cleanup PR 3 (~4-6 hr):** Dependabot dependency updates — triage the 20 vulnerabilities, update requirements.txt, run full regression.

**Not recommended this cycle:**
- Splitting oversized modules — too risky without the classifier refactor being fully stable
- Template unification — explicit PR #47 follow-up, handle as its own session
- New feature work — let the current shipped work bed in first

---

*Generated from a single-session audit. Rerun quarterly or after any
major refactor to spot drift.*
