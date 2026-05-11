# KPI Dashboard — Scope (Tier 3d)

**Status:** scoped 2026-05-11, ~1 week fresh-session build.
**Why:** every accuracy/substrate claim post-2026-05-08 is faith-based until a dashboard surfaces the underlying counters. The QA agent has 5 unacknowledged regressions and no UI surface. Mike asked for "build remaining" and the KPI gate keeps reappearing.
**Predecessor audit:** `docs/AUDIT_DEEP_E2E_2026_05_07_v2.md §Tier 3d`.

---

## Why a dashboard now

1. **The drift catch-vs-prevent gap is invisible.** `assert_subtotal_invariant` logs `PRICING-DRIFT` to Railway but nobody scrapes those logs. Without a counter, a regression goes silent for days.
2. **Quote.set_price adoption is 1/N today.** No way to see whether substrate rollout PRs are actually changing the call ratio.
3. **Pending queue depth is `/health` only.** `active_pcs:27, active_rfqs:8` tells operator nothing about funnel position, win rate by buyer, or per-form failure rate.
4. **`feedback_no_patching_slop_kpi_architecture`** explicitly demands KPI-shaped substrate. The audit-v2 status block defers this as ~1 week fresh-session — that fresh session is the next pick.

---

## Surfaces (what to render)

### Strip 1 — Pricing accuracy (lives or dies on PRICING-DRIFT count)
- **`PRICING-DRIFT` 24h count** — sourced from Railway log scrape into a new `metrics_pricing_drift` SQLite table. Target = 0.
- **Stale `unit_price` records** — gauge: `is_unit_price_stale(item)` true count across all active PCs + RFQs. Updated nightly by `scripts/backfill_unit_price.py --dry-run --emit-metric`.
- **Renderer-mode breakdown** — count of `PRICING-DRIFT` log lines grouped by `context=` prefix (`fill_ams704`, `quote_generator`, `fill_704b`).

### Strip 2 — Canonical-write adoption (Quote.set_price ratio)
- **Atomic status-flip ratio** — count of `set_quote_status_atomic` calls vs raw `UPDATE quotes SET status` calls, per 24h. Target ≥ 95% atomic after PR-η lands.
- **Quote.set_price call ratio** — count of `Quote.set_price` calls vs direct `pc["items"][i]["pricing"]` mutations, per 24h. Target ≥ 80% after PR-ε rollout. Instrumented via call-site decorator.
- **`quote_model_v2_enabled` flag state** — boolean badge. Re-flip date when toggled.

### Strip 3 — Pipeline funnel (where the queue is stuck)
- **PC → quoted → sent → won** — count per stage, last 30 days. Click-through to filtered queue.
- **Per-buyer win rate** — top 10 buyers by RFQ volume, win % column. Source: `dal.get_win_rate(buyer_email)`.
- **Per-form failure rate** — count of PCs/RFQs that crashed `fill_ams704` / `quote_generator` / `fill_704b`, last 30 days. Source: new `form_fill_audit` table.

### Strip 4 — Substrate health (audit-v2 residuals)
- **Phantom-import baseline** — `tools/lint_phantom_imports.py` count. Currently 0; gauge to keep it there.
- **`audit_trail` writers active** — count of distinct `actor` values writing in 24h. Pre-#854 was 1 of 3; now should be 3 of 3.
- **Daemon heartbeat freshness** — every daemon registered with the scheduler last heartbeat age. Anything > 3× interval = red.

### Strip 5 — Orphan / data-integrity (clears the read-side debt)
- **Orphan-orders count** — `quotes` rows with no matching `orders.quote_number`. Click-through to `link_orphan_orders.py --dry-run` output.
- **`won_quotes` table integrity** — count of rows with NULL `description` or `unit_price` (S-13 follow-on).
- **`quotes` ON CONFLICT field-drop count** — instrument `db.py:2333` UPDATE to log when COALESCE protected a field. Trend should fall to 0 as upstream callers stop sending empty values.

---

## Data sources

| Surface | Source | Rate | New code? |
|---|---|---|---|
| PRICING-DRIFT 24h | Railway log scrape → `metrics_pricing_drift` | hourly cron | yes — new table + scraper |
| Stale unit_price | `scripts/backfill_unit_price.py --dry-run --emit-metric` | nightly | extend existing script |
| Status-flip ratio | new structured-log line at every `set_quote_status_atomic` + raw UPDATE | per-call | yes — instrumentation |
| Quote.set_price ratio | same shape, instrument the method | per-call | yes — decorator |
| Funnel counts | `dal.get_pipeline_counts()` (exists) + new per-stage breakdown | on-demand | extend DAL |
| Per-buyer win rate | `dal.get_win_rate()` (exists) | on-demand | none |
| Form failure rate | new `form_fill_audit` table; `try/except` around fill_* writes row | per-call | yes — new table |
| Phantom-import baseline | `tools/lint_phantom_imports.py --count` | per-deploy | none (extend exit-code) |
| audit_trail writers | `SELECT COUNT(DISTINCT actor) FROM audit_trail WHERE created_at > ?` | hourly | none |
| Daemon heartbeats | `src/core/scheduler.py` (exists) | on-demand | none |
| Orphan-orders | `link_orphan_orders.py --dry-run` (exists) | nightly | extend to emit JSON |
| won_quotes integrity | `SELECT COUNT(*) FROM won_quotes WHERE description IS NULL OR unit_price IS NULL` | hourly | none |
| ON CONFLICT field-drop | structured-log at the COALESCE site in `db.py` | per-call | yes — instrumentation |

---

## URL surface

- `GET /kpi` — top-level dashboard (5 strips).
- `GET /kpi/pricing-drift` — drill into the `PRICING-DRIFT` log timeline with `context` filter.
- `GET /kpi/funnel?stage=quoted` — drill into the PCs/RFQs at one funnel stage.
- `GET /kpi/orphan-orders` — paginated orphan list with `Link to quote` action per row.
- `GET /api/v1/kpi/snapshot` — JSON for the strips, refreshed every 60s by the page.

All routes `@auth_required`. CSV export from each list view.

---

## Implementation order (1 week)

| Day | Deliverable |
|---|---|
| 1 | New `metrics_pricing_drift` table + scraper cron. `data_integrity.py` check 11 (stale unit_price count). |
| 2 | Strip 1 + Strip 2 rendering. Instrumentation for status-flip and Quote.set_price ratios. |
| 3 | Strip 3 — funnel queries + per-buyer win rate. New `form_fill_audit` table. |
| 4 | Strip 4 — substrate health. Drill-throughs. |
| 5 | Strip 5 — orphan-orders integration + extending existing scripts to emit metrics. |
| 6 | E2E styling — `.kpi-strip`/`.card-tight` reuse from PR #846. Chrome-MCP verify per CLAUDE.md. |
| 7 | Tests: every metric query has a fixture-backed pin; render snapshot under 4 viewport widths. Promotion to prod with smoke. |

---

## Out of scope (do NOT include)

- Real-time WebSocket updates (60s poll is fine).
- Per-day historical sparklines beyond 30 days (rolling-window view; archive older into a separate page).
- Operator self-serve metric definitions (no DSL; metric set is fixed in code).
- Cross-tenant filtering (single-tenant app).

---

## Success criteria (KPI for the KPI dashboard)

- `PRICING-DRIFT` 24h count = 0 sustained for 7 days post-launch (proves PR-α/β + heal worked).
- Orphan-orders count trending to 0 (proves PR-θ runs are clearing).
- Atomic status-flip ratio ≥ 95% (proves PR-η rollout works).
- Mike's first comment when opening it is a number, not a question. "27 stale PCs, 3 orphan orders, 1 drift event today" beats "looks busy."
