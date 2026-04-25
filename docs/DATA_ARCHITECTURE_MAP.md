# Data Architecture & Connectivity Map

*Created 2026-04-25 from a full-app connectivity audit. Companion to `ARCHITECTURE.md` (top-level flow) and `SYSTEM_ARCHITECTURE.md` (north-star / aspirational domains). This doc is about **wiring**: who calls whom, what's connected vs. siloed, where features live but never run.*

> **Why this exists:** The recurring complaint — "things get built but never connected." This map traces every layer end-to-end, lists what's actually wired vs. defined-but-inert, and names the single chokepoints that decide whether a new feature is reachable or silently dead.

---

## 1. The Wiring Spine — One Blueprint, One Load List

The entire app's HTTP surface goes through a single Flask Blueprint.

| Spine element | File:line | Notes |
|---|---|---|
| Single `bp = Blueprint("dashboard", ...)` | `src/api/shared.py:22` | Every route attaches here |
| `app.register_blueprint(bp)` | `app.py:184` | Only one registration in the entire app |
| Module loader `_load_route_module()` | `src/api/dashboard.py:5219` | importlib + globals injection (not exec, despite legacy comment) |
| **The chokepoint: `_ROUTE_MODULES` list** | `src/api/dashboard.py:5239–5276` | **36 modules hardcoded here (was 35 at audit time)** |
| Load loop with silent-error fallback | `src/api/dashboard.py:5278–5283` | A failed module logs an error and the app keeps booting → 404s in prod, no crash signal |
| **✅ Drift detector** | `src/api/dashboard.py:5288 _audit_route_module_registration()` | PR #532: WARNING when disk ≠ list. CI test `tests/test_route_module_registration.py` blocks pushes that introduce drift |

**Connectivity rule confirmed by audit:** all 36 `routes_*.py` files in `src/api/modules/` are listed in `_ROUTE_MODULES` and load successfully. Prod boot log on `17f7d2c6`: *"Dashboard: 36 route modules loaded, 1197 deferred fns"*.

### ✅ Silo risk #1 — CLOSED 2026-04-25 via PR #532

A new `routes_*.py` file dropped into `src/api/modules/` previously did **nothing** unless its name was added to `_ROUTE_MODULES`. PR #532 (commit `17f7d2c6`) added three layers:

1. **Boot WARNING** in `dashboard.py:_audit_route_module_registration()` — scans `src/api/modules/routes_*.py` after the load loop and logs a clear WARNING on disk-vs-list mismatch (no crash in prod; let the app keep serving).
2. **Hard CI gate** in `tests/test_route_module_registration.py` — asserts `set(on_disk) == set(_ROUTE_MODULES)`. Pre-push hook runs this; pushes that introduce drift are blocked.
3. **URL-map floor tripwire** in `tests/test_golden_path.py::TestRouteSurfaceTripwire` — asserts ≥ 1,200 rules (current ~1,223). Catches the case where a module loads but registers nothing.

Atomicity rule: renaming or removing a route module MUST update `_ROUTE_MODULES` in the same commit. The CI test enforces this by design.

Prod boot log on `17f7d2c6` confirmed the detector running cleanly: *"Dashboard: 36 route modules loaded, 1197 deferred fns"* — zero DRIFT warnings = healthy state.

---

## 2. Request → Response Flow (high-level)

```
HTTP request
   │
   ▼
app.py:295  _check_degraded_mode()         ← DB health gate; 503 if DB down
   │
   ▼
shared.py   @auth_required  (Basic Auth)   ← 13 routes intentionally exempt (webhooks, /ping)
   │
   ▼
shared.py   bp dispatch                    ← single Blueprint, 1,223 routes
   │
   ├── routes_pricecheck*.py   (PC domain — 169 routes across 5 files)
   ├── routes_rfq*.py          (RFQ domain — 113 routes across 3 files)
   ├── routes_orders_full.py / routes_order_tracking.py  (orders — see §4.b)
   ├── routes_intel*.py        (intel/SCPRS — 179 routes across 2 files)
   ├── routes_analytics.py     (dashboards — 81 routes)
   ├── routes_v1.py            (MCP-ready external API — 127 routes)
   └── … 24 more modules
   │
   ▼
service / agent / DAL layer
   │
   ▼
SQLite (WAL mode, /data/reytech.db on Railway)
   │
   ▼
app.py:327  _optimize_response()  (gzip + cache headers)
```

For the *business* flow (intake → pricing → quoting → fulfillment), see `ARCHITECTURE.md` and `SYSTEM_ARCHITECTURE.md` §"Four Domains". Those are accurate; this doc does not duplicate them.

---

## 3. Database — Writer/Reader Connectivity Table

Tables ordered by connectivity health. **Status legend:**
- ✅ HEALTHY — bidirectional, single canonical writer, multiple consumers
- ⚠️ FRAGMENTED — multiple writers, no serialization, divergence risk
- 🛑 WRITE-ONLY — written but no production read path (data going nowhere)
- 👻 SHADOW — V1/V2 parallel schema with best-effort mirror writes

| Table | Status | Writers | Readers | Notes |
|---|---|---|---|---|
| `quotes` | ✅ HEALTHY | dashboard, award_tracker, email_poller, quote_reprocessor, routes_v1 | dashboard, routes_pricecheck_admin, revenue_engine, multiple dashboards | **Status field is FRAGMENTED — see §3.b** |
| `price_checks` | ✅ HEALTHY | email_poller, quote_reprocessor, routes_pricecheck_admin, dashboard delete | dashboard PC pages, pc_enrichment_pipeline, forms/*.py | OK |
| `rfqs` | ✅ HEALTHY | email_poller, routes_rfq_admin | dashboard, generic_rfq_parser consumers | OK |
| `price_history` | ✅ HEALTHY | every pricing lookup | pricing_oracle_v2, freshness checks | Append-only by design |
| `contacts` | ✅ HEALTHY | core/dal.py upsert_contact, email_poller | routes_crm, dashboard | OK |
| `orders` + `order_line_items` | 👻 SHADOW | core/order_dal.py (V2 normalized) | dashboard, order_dal | V2 path is canonical; legacy `data_json` blob still written, fallback-read path silently masks corruption (`order_dal.py:96–100`) |
| `purchase_orders` + `po_line_items` | 👻 SHADOW | routes_order_tracking.py:306, 579, 798 (best-effort mirror) | routes_order_tracking dashboards | **No FK to `orders`. Reconciliation code at line 863 proves divergence is known.** See §3.a |
| `agency_registry` | ⚠️ PARTIAL | `connectors/ca_scprs.py:177`, `core/ca_agencies.py:82` (INSERT OR IGNORE) | `routes_v1.py:565,569` only | Read by external API only — **internal code uses `FACILITY_DB` instead**. See §6 |
| ~~`award_check_queue`~~ | ✅ DROPPED 2026-04-25 | — | — | **CLOSED — migration 29 dropped the table.** Was vestigial design from 2026-03-16 (queue-driven adaptive checker); superseded by `award_tracker.py` direct-iteration design (calls `scprs_schedule.should_check_record` per row at read-time). Same schedule, different consumer pattern. 32 prod orphan rows reclaimed. |
| `qa_runs` | ✅ HEALTHY (recently) | `agents/qa_agent.py:2176` | `routes_analytics.py:4716`, `routes_crm.py:3989, 4012` | Earlier audit claimed write-only — that's wrong; analytics + CRM both read it |
| `qa_reports.json` (file, not table) | ✅ HEALTHY | qa_agent | routes_analytics:4055 | File-based, excluded from gdrive backup intentionally |
| `leads` | ⚠️ MOSTLY-WRITE | growth agents | rare CRM reads | Most leads written, rarely read into UI — confirm operator value before continuing to invest |
| `activity_log` | ✅ HEALTHY | dashboard contact actions | routes_crm | OK |

### 3.a — `purchase_orders` shadow schema (highest-risk fragment)

`src/api/modules/routes_order_tracking.py` defines an entire parallel order schema (`purchase_orders`, `po_line_items`) at lines 39 and 64, separate from the V2 `orders`/`order_line_items` tables managed by `core/order_dal.py`.

- Write order: legacy first, V2 mirror best-effort (lines 306–311, 579–613, 798–842)
- Failure mode encoded in code: *"legacy write succeeded; V2 mirror may have failed"*
- No foreign key links `purchase_orders.po_number` to `orders.po_number`
- Reconciliation check at line 863 exists because the team knows these can drift

**Real writer count (verified 2026-04-25 product-engineer review):**
- `purchase_orders` writes: **4 sites** — `routes_order_tracking.py:643` (insert), `:398/:446/:788` (status updates)
- `orders` writes: **9 sites** — `routes_order_tracking.py:551, 593`; `core/dal.py:1130, 1169`; `core/order_dal.py:319, 704, 912`; `core/po_email_v2.py:103`; `core/db.py:2838, 2959, 3017`; `agents/quote_lifecycle.py:332`; `routes_pricecheck_admin.py:347`

**Operator-visible symptom:** PO appears in PO-tracking dashboard with one status; the same order in the Orders V2 dashboard shows a different status. Rare today because both writes usually succeed; will bite the moment one path errors.

**Fix shape — corrected after p-eng review (do NOT skip these prerequisites):**
1. **`orders.po_number` is NOT UNIQUE today** — SQLite FK requires the parent column have PRIMARY KEY or UNIQUE. Naïve `FOREIGN KEY (po_number) REFERENCES orders(po_number)` will fail at migration time. **Add UNIQUE on `orders.po_number` first** (which itself requires deduping any existing duplicates — soak a separate PR before the FK PR).
2. **`core/po_email_v2.py:103` already implements a single-writer-to-`orders` path** behind existing flag `orders_v2.poller_unified` (default OFF per §5.c). Don't invent a *second* flag — finish flipping the existing one. The remaining S3 work is "drain & flip `orders_v2.poller_unified`," not "build a new unified writer."
3. Add `record_po(...)` to `core/order_dal.py` (same module as existing writers; don't fork the DAL) that atomically writes both tables in one transaction, with the legacy path inside `try/except` so the new writer is primary and legacy is the fallback.
4. Delete `purchase_orders` shadow schema only after the FK soak proves zero divergence.

**Sequence (per p-eng):** PR-1 = `record_po()` + drift counter on `/health/quoting` (one PR, both safe-by-default OFF). PR-2 = UNIQUE on `orders.po_number` + 14-day soak. PR-3 = FK constraint + delete legacy path. Threshold: drift events, not calendar — *"100 PO writes with zero divergence"* is the gate, not 30 days.

### 3.b — Quote status race condition

Four writers, no lock, last-write-wins:
- `agents/award_tracker.py:337, 525, 678` — background award detection
- `agents/email_poller.py:2231` — email-triggered status update
- `agents/quote_lifecycle.py:235, 320` — lifecycle transitions
- `api/dashboard.py:3808, 4009` — manual operator action

**Risk:** background job overwrites a manual operator action made 200ms earlier. Move all status writes through a single `mark_quote_status(quote_id, new_status, source, expected_prev=None)` helper in `core/quote_lifecycle_shared.py` with `WHERE status = expected_prev` to make updates conditional.

---

## 4. Background Workers — Trigger Catalog

All confirmed live. Source: `app.py:401–434`, `dashboard.py:5466+`.

| Worker | File:line | Trigger | Writes | Flag-gated? |
|---|---|---|---|---|
| `email-poller` | `app.py:403` | every 300s | DB (price_checks, rfqs, email_rejections) | env `ENABLE_EMAIL_POLLING` only |
| `award-tracker` | `app.py:403` | every 3600s | DB (awards, contacts, quote status) | env `ENABLE_BACKGROUND_AGENTS` only |
| `follow-up-engine` | `app.py:404` | every 3600s | DB, email outbox | env only |
| `quote-lifecycle` | `app.py:404` | every 3600s | DB (quote_status) | env only |
| `email-retry` | `app.py:405` | every 900s | email outbox | env only |
| `lead-nurture` | `app.py:405` | every 86400s (daily) | DB, email outbox | env only |
| `qa-monitor` | `app.py:406` | every 900s | DB, audit logs | env only |
| `growth-agent` | `app.py:406` | every 86400s | DB (prospects) | env only |
| `fiscal-exhaustive-scrape` | `app.py:426` | cron 2 AM PST | SCPRS tables | env only |
| `system-auditor` | `app.py:434` | cron 5:30 AM PST | DB (audit tables) | env only |
| `backup-scheduler` | `app.py:401` | startup | `/data/backups/hourly/*.db` | none |
| `task-consumer` | `app.py:419` | startup | task queue DB | none |
| `ops-monitor` | `app.py:487` | startup | ops_logs, backups | none |
| `watchdog` | `app.py:409` | every 300s | jobs registry | none |
| `pc-enrichment` | `agents/pc_enrichment_pipeline.py:1040` | on PC creation | DB (enrichment_status) | flag `unspsc_enrichment` (default OFF) |
| Boot health checks | `dashboard.py:5443` | startup + 10s | logs only | env `TESTING` |

**Connectivity note:** Background workers are gated by **env vars only**, not feature flags. The runtime flag admin endpoint (`/api/admin/flags`, `routes_feature_flags.py`) cannot disable a running worker. If you need a kill switch, add an env var or refactor the worker to read its flag at the start of each interval.

---

## 5. Feature Flag Inventory — Current State

Source: full grep of flag-checking patterns across `src/`.

### 5.a — LIVE-ON (in production paths today)

| Flag | File | Default | Effect |
|---|---|---|---|
| `pricing.grok_validator_enabled` | `agents/product_validator.py` | `TRUE` | Grok LLM fallback when confidence < 0.75 |
| `pricing_v2` | `routes_pricecheck_pricing.py` | `TRUE` | V2 pricing pipeline live |
| `oracle.volume_aware` | `core/pricing_oracle_v2.py` | `TRUE` | Volume-aware oracle live |
| `pipeline.confidence_threshold` | `routes_pricecheck.py` | `0.75` | Threshold (not boolean) |
| `pipeline.delivery_threshold` | `forms/document_pipeline.py` | `70` | Threshold (not boolean) |

### 5.b — SHADOW (observe-only, dual-running)

| Flag | File | Default | Notes |
|---|---|---|---|
| `quote_model_v2_shadow` | `forms/shadow_mode.py` | `TRUE` | V2 quote model runs alongside V1; V1 is still authoritative. **No telemetry consumer found** — verify shadow observations are being read before next session of V2 work |
| `rfq.do_not_send_list` | `core/quote_validator.py` | `""` (empty) | Empty list = no-op |

### 5.c — 🚨 LIVE-OFF (shipped code, default-off, no flip in progress)

These are the inert features Mike asked about. Each represents engineering effort that doesn't currently affect production behavior.

| Flag | File | What's gated | Recommended action |
|---|---|---|---|
| `unspsc_enrichment` | `agents/pc_enrichment_pipeline.py` | UNSPSC product classification on PC enrichment | Flip ON or delete |
| `outbox.send_approved_enabled` | `routes_growth_prospects.py` | Auto-send approved growth outbox messages | Decide policy or delete |
| `ingest.classifier_v2_enabled` | `routes_health.py` + `core/request_classifier.py` | Classifier V2 (the unified-ingest refactor PR #47) | **Memory says it was flipped 2026-04-14 — verify in prod admin and remove flag if stable** |
| `bid_scoring` | `routes_intelligence.py` | Bid intelligence endpoint | Build or delete |
| `compliance_matrix` | `routes_intelligence.py`, `routes_rfq.py` | RFQ compliance matrix endpoint | Build or delete |
| `docling_intake` | `routes_intelligence.py`, `routes_rfq.py` | Docling-based document intake | Build or delete |
| `nl_query_enabled` | `routes_intelligence.py`, `routes_rfq.py` | Natural-language query | Build or delete |
| `orders_v2.poller_unified` | `routes_order_tracking.py` | Unified V2 order poller (replaces legacy) | Flip ON to begin draining shadow schema in §3.a |
| `rfq.require_profile_match` | `routes_rfq_gen.py` | RFQ profile validation gate | Decide if needed |
| `quote_model_v2_enabled` | `core/quote_adapter.py` | Promote V2 quote model from shadow to live | Tied to shadow telemetry — confirm before flip |
| `rfq.readback_verifier` | `core/quote_orchestrator.py` | Quote readback verification | Build or delete |
| `rfq.orchestrator_pipeline` | `core/quote_orchestrator.py` | Orchestrator pipeline path | Tied to V2 promotion |

### 5.d — 👻 PHANTOM flags (referenced in memory, NOT in code)

These flags are mentioned in `~/.claude/.../memory/project_rfq_session_2026_04_23_complete.md` as "pending flips after 48h shadow telemetry" but **zero references exist anywhere in `src/`**:

- `ingest.ghost_quarantine_enabled`
- `quote.block_unresolved_ship_to`

Either the gating code was never merged, was reverted, or was renamed. **Update the memory entry** to reflect actual state — leaving it as-is means the next session will plan around features that don't exist.

The same memory mentions a *T1 declarative pipeline observer* on 4 RFQ routes; no observer code matching that description was found in this audit.

---

## 6. Identity / Canonical-Source Map

### Facility / agency identity

The de-facto canonical facility registry is a **module-level dict** inside a PDF-generation file:

| Source | File:line | Used by |
|---|---|---|
| `src/core/facility_registry.py` (`FACILITIES_BY_CODE`) | sole canonical source as of 2026-04-25 | quote_generator (via `_lookup_facility` → `resolve()`), institution_resolver, tax_resolver |
| ~~`FACILITY_DB` in `quote_generator.py`~~ | DELETED 2026-04-25 (S2 PR) | — |
| ~~`_lookup_facility_legacy` + `_CITY_MAP`~~ | DELETED 2026-04-25 (S2 PR) | — (was zero callers) |
| `agency_registry` table | populated by `core/ca_agencies.py:82`, `connectors/ca_scprs.py:177`; read by `routes_v1.py:565,569` | External MCP API only — agency-level data, distinct from facility-level. Not a facility-data duplicate (S9 framing corrected). |
| Per-module agency-name normalizers (different concern) | `quote_generator.py:1697` (`_parent_agency_map`), `:1772` (`_agency_map`) | Lowercase→Capital agency-name lookup, NOT facility data. Could be folded into a tiny helper but separate concern. |
| Per-module email-domain → agency maps (different concern) | `dashboard.py:906`, `routes_pricecheck_admin.py:1927` | EMAIL DOMAIN mapping (e.g. `cdcr.ca.gov` → `CDCR`), NOT facility data. Folding these would be a separate refactor. |
| `core/institution_resolver.py:_FACILITY_ADDRESSES` | actively read at lines 163–167 | **Next-up S2 follow-up.** Parallel-universe dict that should fold into `FacilityRecord.mailing_address`. Needs its own PR with Chrome verify since 3 active read sites. |

**Status as of 2026-04-25:** S2 closed by the surgical-delete PR. `FACILITY_DB` had stale audit-W data (CSP-SAC at "300 Prison Road") that would have silently regressed the audit-W fix if anyone read it directly. `_lookup_facility_legacy` had zero callers per grep. Both gone; `core/facility_registry.py` is the sole source. Three absence-guard tests added to `tests/test_quote_gen_canonical_facility.py`.

### ✅ Silo risk #2 — CLOSED 2026-04-25 (surgical delete)

What was deleted from `src/forms/quote_generator.py`:
- `FACILITY_DB` constant (~40 facility entries) — duplicate of `FACILITIES_BY_CODE` with stale audit-W data
- `ZIP_TO_FACILITY` constant — built from `FACILITY_DB`, never read in the live path
- `_lookup_facility_legacy(text)` function — zero callers; its docstring already said *"Normal operation never reaches this function."*
- Embedded `_CITY_MAP` inside the function — the thirty-row city-fallback dict

Live consumer path (unchanged): `_lookup_facility(text)` → `_contract_resolve_facility(text)` → `facility_registry.resolve(text)` → `_registry_record_to_legacy_dict(rec)` → renderer-friendly dict.

**Future S2 follow-up (not in this PR):** migrate `core/institution_resolver.py:_FACILITY_ADDRESSES` (3 active read sites at lines 163–167) into `FacilityRecord.mailing_address` fields on the canonical registry. Needs Chrome verify per `feedback_workflow_ui_chrome_verify`.

### Other identity facts (confirmed correct, not silos)

- Reytech canonical sender = `Michael Guadan` + `sales@reytechinc.com` — used everywhere, see `feedback`/`project_reytech_canonical_identity.md`.
- Quote counter = single authority via `set_quote_counter()` with `quote_counter_last_good`. See CLAUDE.md "Quote Counter" rules.

---

## 7. Verified Silos & Disconnects (Action Backlog)

These are the items most likely to surprise you with *"I built that — why isn't it doing anything?"* All file:line refs verified during this audit.

| # | Silo | Where | Severity | Fix shape |
|---|---|---|---|---|
| ✅ S1 | New `routes_*.py` files inert unless added to `_ROUTE_MODULES` | `dashboard.py:5239–5285` | **CLOSED PR #532 (2026-04-25)** | Boot WARNING + CI test + URL-map tripwire (§1) |
| ✅ S2 | `FACILITY_DB` lives in `quote_generator.py`, not a registry module | (deleted) | **CLOSED 2026-04-25** — surgical-delete PR removed `FACILITY_DB`, `ZIP_TO_FACILITY`, and `_lookup_facility_legacy` (zero callers, stale audit-W data). `core/facility_registry.py:FACILITIES_BY_CODE` is sole source. Follow-up still owed: migrate `institution_resolver._FACILITY_ADDRESSES` (3 active read sites) into `FacilityRecord.mailing_address`. |
| S3 | `purchase_orders` + `po_line_items` shadow schema | `routes_order_tracking.py:39, 64` | High — **PRD reworked 2026-04-25 (see §3.a)** | (1) UNIQUE on `orders.po_number` first — FK can't be added without it. (2) Finish flipping existing flag `orders_v2.poller_unified`, don't invent a second flag. (3) `record_po()` in `core/order_dal.py`, not a new module. 9 `orders` writers + 4 `purchase_orders` writers must be reconciled |
| S4 | Quote status writes from 4 sources, no lock | see §3.b | Medium-High | Centralize in `quote_lifecycle_shared.mark_quote_status()` |
| S5 | 12 LIVE-OFF flags with shipped code | §5.c | Medium (tech debt) | Audit each: flip, build, or delete |
| ✅ S6 | Phantom flags from memory don't exist in code | §5.d | **CLOSED 2026-04-25** — `project_arch_silos_2026_04_25.md` notes the two phantom flags in this entry; future sessions won't plan around them |
| ✅ S7 | `award_check_queue` written, never read | (table dropped) | **CLOSED 2026-04-25** — migration 29 + write site removed from `post_send_pipeline.on_quote_sent`. Bundled tombstone `get_sent_quotes_dashboard()` + dead route `/api/v1/quotes/sent-tracker` removed in same PR. Investigation confirmed `award_tracker.py` is the live consumer using the same `scprs_schedule` helper. |
| S8 | `data_json` blob still written for V2 orders, fallback-read masks corruption | `order_dal.py:96–100`, `db.py:2771` | Medium | Finish V2 phase 4 — stop blob writes, drop fallback path |
| ⏸ S9 | `agency_registry` only read by external API | `routes_v1.py:565,569` | Re-framed 2026-04-25: agency_registry is agency-level data, NOT a facility duplicate. Original framing conflated agency vs facility layers. The actual gap is institution_resolver._FACILITY_ADDRESSES (parallel facility-address dict), tracked as the S2 follow-up. Leaving S9 open as "internal code could optionally consume agency_registry" but it's now Low severity, not Medium. |
| S10 | Background workers gated by env vars, not feature flags | §4 | Low | Add flag check at top of each worker interval |
| ✅ S11 | Module-load failure logs but doesn't crash | `dashboard.py:5278` | **CLOSED PR #532 (2026-04-25)** — symmetry enforced via CI test; boot WARNING surfaces drift |
| S12 | `quote_model_v2_shadow` shadow telemetry has no consumer surfaced in audit | `forms/shadow_mode.py` | Medium | Verify the dashboard reading shadow diffs exists; if not, the V2 promotion can't be safely flipped |

**Status summary as of 2026-04-25:** 5 of 12 closed (S1, S2, S6, S7, S11). S9 re-framed (Low). Recommended next sequence per product-engineer reviews: **S2 follow-up** (institution_resolver._FACILITY_ADDRESSES → FacilityRecord.mailing_address; needs Chrome verify) → **S3 with corrected PRD** (UNIQUE first, finish existing flag) → S4/S5/S8/S10/S12 in any order.

---

## 8. Golden-Path Connectivity Trace

The single end-to-end test that proves connectivity: **incoming buyer email → sent quote → mark won → oracle calibrates**.

```
Gmail poll                                  agents/email_poller.py:2231
  ↓ writes
price_checks / rfqs row                     core/db.py:425, 507
  ↓ triggers
process_buyer_request()                     core/ingest_pipeline.py
  ↓ classifies via
classify_request()                          core/request_classifier.py    [⚠ flag: ingest.classifier_v2_enabled — verify ON]
  ↓ enriches via
pc_enrichment_pipeline.run()                agents/pc_enrichment_pipeline.py:1040
  ↓ prices via
pricing_oracle_v2.recommend()               core/pricing_oracle_v2.py     [LIVE]
  ↓ fills via
fill_ams704() / cchcs_packet_filler.fill()  forms/price_check.py / forms/cchcs_packet_filler.py
  ↓ identity from
FACILITY_DB                                 forms/quote_generator.py:127  [🚨 wrong module — S2]
  ↓ QA-gates via
form_qa.run_form_qa()                       forms/form_qa.py
  ↓ operator clicks Send
gmail_api.send_message()                    src/integrations/gmail_api.py
  ↓ writes status
quotes.status = 'sent'                      one of 4 writers — RACE       [⚠ S4]
  ↓ on award detection
award_tracker.mark_won()                    agents/award_tracker.py:525   [overlaps with operator manual mark]
  ↓ calibrates
pricing_oracle_v2.calibrate_from_outcome()  core/pricing_oracle_v2.py     [LIVE]
  ↓ feeds back into next enrichment
```

**Gaps that break this trace today:**
- S4 (quote status race) can drop the operator's manual won-status under a background overwrite, breaking calibration's input.
- S8 (data_json fallback) can return stale items if blob and normalized rows diverge after a partial write.
- S11 (silent module-load failure) can silently 404 the quote-detail page if `routes_pricecheck_pricing` fails to load on boot.

A single integration test that exercises this whole path (fixtures already exist for most of it — see `tests/test_golden_path.py`) is the regression net for the entire architecture map.

---

## 9. How to Keep This Map Alive

This document goes stale the moment someone adds a new route or table without updating it. Three lightweight mechanisms keep it honest:

1. **✅ Boot detector + CI test (S1 + S11)** — shipped 2026-04-25 PR #532. `dashboard.py:_audit_route_module_registration()` warns at boot; `tests/test_route_module_registration.py` blocks pushes that introduce disk/list drift; `tests/test_golden_path.py::TestRouteSurfaceTripwire` catches the URL surface shrinking.
2. **WORKSTREAMS row** — when a PR adds a new route module, table, background worker, or feature flag, add a one-liner to `.claude/WORKSTREAMS.md` flagging "DATA_ARCHITECTURE_MAP.md needs update". A pre-merge checklist item.
3. **After every silo PR** — update §7 of this doc to mark the silo CLOSED and add a one-line summary in §1's spine table or the relevant section. The map is the running ledger; let it stay accurate.
4. **Yearly re-audit** — re-run the four parallel agent passes that produced this doc (routes / data layer / background+flags / orphan hunt). Cross-check against this map; reconcile drift.

---

## 10. Cross-References

- `docs/ARCHITECTURE.md` — top-level pipeline flow (intake → enrichment → fill → QA → send → calibrate). Stays accurate; this doc complements rather than replaces.
- `docs/SYSTEM_ARCHITECTURE.md` — north-star "Four Domains" model + flywheel. Aspirational; this doc is descriptive of current state. The deltas between them ARE the work backlog.
- `docs/PC_TO_RFQ_WORKFLOW.md` — pricing continuity plan (mostly forward-looking). Sections F1–F11 there are still mostly unbuilt — overlap with §5.c LIVE-OFF flags.
- `CLAUDE.md` — guard rails (form filling, signatures, pricing roles, JS null-safety). Operational rules; doesn't duplicate this map.

---

*Audit method: four parallel `Explore` agent passes (routes, data layer, background+flags, orphan hunt) on 2026-04-25, then direct verification of every file:line cited above before inclusion. Two early agent claims (auto-price endpoint duplicate; `agency_registry` unused) were dropped after grep showed they were wrong — only verified facts kept.*
