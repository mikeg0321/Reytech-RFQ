# Creepy Crawler — Site Audit Dossier

> A running record of automated site/codebase sweeps. Newest sweep on top.
> Each sweep: boot the app, crawl every no-arg GET route, reconcile against
> §0 governance, then **independently verify** every sub-agent claim before
> it lands here. Hype is removed; only verified findings are recorded.
>
> **For:** the Architect ("Mr. Wolf"). Findings tagged BLOCKER/HIGH that
> touch `model.py`, schema, `SPINE_CHARTER.md`, new `src/spine/` modules, or
> any new substrate are **Architect-authorized work (LAW 4)** — the crawler
> reports them, it does not fix them.

---

## Sweep 2026-05-30 — `f9ee506` (branch `claude/creepycrawler-site-audit-Yv2QI`)

**Method.** Booted the Flask app from a clean checkout (Python 3.11 locally;
prod is 3.12), ran migrations (57 applied → schema v52), enumerated the live
`url_map` (**1,073 routes registered**), and crawled every no-arg GET route
with dev Basic-Auth. Two read-only audit agents swept governance + churn in
parallel; **every** agent claim below was re-verified by hand against
`file:line` before being recorded.

**Reconciled against `a2a8e8c` (PR #1272), 2026-05-30.** The sweep base
`f9ee506` was ~12 commits behind main; Job #1 (J1-1…J1-5b) landed during the
sweep. Findings below are re-scored against current main: O2 is partly
resolved + one claim retracted (see row), O3 still holds (ratchet unmoved),
F1 remains unique to this branch. The remaining open items (O1, O3, O4, O5,
O6, O7) all reproduce on `a2a8e8c`.

**Crawl health (no-arg GET, n≈500):**
`200×476 · 204×1 · 301×1 · 302×10 · 400×16 · 404×2 · 410×3 · 500×24 · 503×2`.
Of the 24 `500`s, **23 are environmental** (fresh test DB lacks SCPRS/connector
tables: `scprs_po_master`, `connectors`, `scprs_buyers`, `supplier_costs`,
`usage_events`, `task_queue`, `scprs_catalog`, `scprs_po_lines`) — they do **not**
reproduce in prod where those tables exist. **One is a real code bug** (fixed
this sweep, below). Auth gate is clean: only `/health`, `/ping`, `/version`
answer `200` unauthenticated.

### Fixed in this sweep
| # | Sev | Finding | Evidence | Status |
|---|-----|---------|----------|--------|
| F1 | HIGH | `/api/vendor/performance` 500'd on **every** call — `NameError: name 'defaultdict' is not defined`. The handler used `defaultdict` but `routes_crm.py` never imported it (injected-globals fallback covers `os`/`json` but not `defaultdict`). Route was dead in prod. | `src/api/modules/routes_crm.py:4683` (use) vs missing import | **FIXED** — added `from collections import defaultdict`; route now `200`. Regression test `tests/test_vendor_performance_import.py` added & green. |

### Open findings (Architect-authorized — not fixed here)
| # | Sev | Finding | Evidence | Recommended next step |
|---|-----|---------|----------|----------------------|
| O1 | HIGH | **Shadowed duplicate route handler.** `POST /api/pricecheck/<pcid>/auto-price` is registered **twice in the same file** — one handler is dead/shadowed and which one serves is registration-order-dependent. Sits inside the Oracle/pricing churn hotspot (O5), so editing the dead copy looks effective but changes nothing. | `routes_pricecheck_admin.py:703` (`api_pricecheck_auto_price`) **and** `:5619` (`api_pc_auto_price`), both `POST`, same path | Decide the canonical handler, delete the other, add a `test_no_duplicate_route_rules` guard. Conflict-zone file → sequence the edit. |
| O2 | HIGH→**PARTLY RESOLVED on `a2a8e8c`** | **Job #1 (CCHCS migration) is NOT done** per LAW 2 — but it moved hard between the sweep base (`f9ee506`) and current main. **Resolved since sweep:** the legacy `/rfq/<rid>/generate` route is **DELETED** (`69214e9`), and CCHCS bill-to + form-set readers are repointed to the Spine (`e65d886`, `d7fb617`, `caafdf7`). **Retracted:** "delete `src/spine/agency_forms/` renderers" — §0 was corrected by PR (`f17e8b9`, LAW 7): those modules (`std_204`, `dvbe_843`, `darfur`, `calrecycle_74`, `std_1000`, `cuf`, `_identity`, `_template_resolver`, `FORM_REGISTRY`) are **load-bearing**, only the `cchcs_*.py` shims were retired and they're already gone. The sweep flagged them only because the base checkout predated that §0 fix. **Still open:** the `cchcs` agency-config entry persists as the legacy-keeper. | `src/core/agency_config.py:193` (entry still present on main); resolved-by commits above | Delete the `cchcs` config entry once the legacy `generate-quote`/`generate-package` write paths are gone; commit visible in `git log`. |
| O3 | HIGH | **Convergence ratchet is flat — still `9/3/2` on `a2a8e8c`.** The sharpest remaining point: a large amount of J1 *repointing* merged (J1-1…J1-5b) but `convergence_baseline.json` has **not** dropped, because repointing readers to the Spine ≠ deleting the legacy substrate (LAW 2: "Routes repointed is not done"). The pack checkpoint **2026-06-20** requires substrate **and** dir counts to DROP or "the pack model has failed." | `tests/spine/convergence_baseline.json` on `origin/main` (writers 9 / substrates 3 / dirs 2); §0 "Pack checkpoint — 2026-06-20" | Cross the deletion line: remove a legacy quote-write path / substrate file and ratchet `substrates 3→2`. This is now the gating item — repointing is largely done, deletion is not. |
| O4 | MED | **Send-gate enforcement may be a placeholder.** The per-attachment **disposition manifest required by LAW 6 DOES exist** (records `parsed_items` / `distribution_list` / `sibling_attachment` / `*_parse_failed` for every attachment) — the static agent's "manifest missing" was a **false positive** (it grepped only `spine_bridge`/`spine` and missed `core/`). The genuine open question is whether the send-gate that must *refuse* on an unaccounted attachment is wired or still future. | manifest: `src/core/ingest_pipeline.py:388-446`, stashed at `:2401`/`:2578`; gate: `routes_spine.py:618` comment "the future /send-prep gating" | Architect confirm whether `/send-prep` gating enforces manifest completeness (LAW 6 "teeth"). If placeholder, wire `test_ingest_reads_all_attachments` to fail the build on an unaccounted attachment. |
| O5 | MED | **Oracle cost-matching is a fragility hotspot.** 6 fixes in 30 days on the locked-cost / cost-sanity guards (line-number collisions, SCPRS cap, cross-category) — correct individually, but accreting overlapping guards rather than one validator. | commits `78814cc 349e2b0 ff6233f e882d22 d4f35fb 0bee070`; `src/core/pricing_oracle_v2.py` | Consolidate the overlapping guards into one `_validate_cost_sanity()` (catalog ≤ price ≤ scprs_cap, no cross-category, line-precise); retire guards as it proves out. |
| O6 | MED | **Python 3.12 is a hard floor with no CI below it.** 5 route modules use f-string-with-backslash (PEP 701, 3.12+ only); they `SyntaxError` and silently fail to load on 3.11 (`dashboard.py` logs "Failed to load route module" and continues — the routes simply vanish). Prod is 3.12 so this is latent, but a 3.11 runner or a contributor on 3.11 loses whole route modules with no test failure. | `routes_catalog_finance.py:458`, `routes_intel.py:1903`, `routes_orders_full.py:566`, `routes_pricecheck.py:1088`, `routes_voice_contacts.py:568` | Pin `python_requires>=3.12` and assert it at boot; OR make `_load_route_module` failures fail loudly in CI instead of degrading silently. |
| O7 | LOW | **Overdue dead-code deadline.** Dual-read safety net flagged "remove after 2026-04-21" is **~39 days overdue**. | `src/core/flags.py:~130` | Delete the fallback (the boot migration already copies legacy rows forward) or re-justify with a new date. |

### Verified GREEN (no action)
- **Pricing guard rails intact:** SCPRS/Amazon never used as cost basis (`auto_processor.py:446`); 3× cost-sanity refuse (`product_catalog.py:3650`); catalog match threshold **0.65** (`product_catalog.py:2846`); `_detect_pg1_rows` used, no hardcoded `row=8` (`price_check.py:4282`); quote-counter max-jump **5** (`spine/db.py:1008`).
- **Recent incidents defended by tests:** Coleman `10842771` distribution-list (`test_coleman_10842771_canary.py`, `test_ingest_reads_all_attachments.py`) and the 2026-04-03 multi-page 704 (`test_multipage_704.py`).
- **Churn discipline:** 50 commits/30d, **0 reverts** in the last 200 — the revert-spiral that motivated §0 is not currently active.
- **Den is collapsed (Job #0):** 1 canonical repo, 1 worktree, ~4 branches. LAW 5 cap respected.

### Sub-agent claims the crawler **overturned** (recorded so they aren't re-raised)
- ❌ "24 duplicate route paths (BLOCKER)" → **1** real conflict (O1); the other 24 are legal HTTP-method splits on a shared path.
- ❌ "LAW 6 disposition manifest missing (HIGH)" → **exists** (O4); the manifest is implemented in `core/ingest_pipeline.py`.

### Top 5 next steps (ranked)
1. **Land the Job #1 deletion PRs (O2 → O3)** before 2026-06-15 — the only thing that moves the 2026-06-20 ratchet. Delivery, not correctness.
2. **Kill the shadowed `auto-price` duplicate (O1)** and add a `test_no_duplicate_route_rules` guard so it can't recur.
3. **Architect to confirm the send-gate (O4)** — manifest exists; verify it actually *blocks*.
4. **Unify the Oracle cost guards (O5)** to stop the 6-fixes/30-day bleed.
5. **Pin/assert Python 3.12 (O6)** so a 3.11 runner can't silently drop route modules.
