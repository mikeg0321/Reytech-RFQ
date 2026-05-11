# Reytech RFQ — Deep E2E Audit, Re-run (v2)

**Date:** 2026-05-07 (afternoon — after PRs #821-#827 shipped overnight)
**Predecessor:** `docs/AUDIT_DEEP_E2E_2026_05_07.md` (the morning version, before #821-#827)
**Method:** verify each shipped class on prod, then re-spawn five parallel deep-readers (no recency bias, every line evaluated for usability + data connection, lens "does this serve win-rate × volume").
**Prod SHA verified:** `ec46fd53` = PR #827 = origin/main. Local was 7 commits behind; all reads done via `git show origin/main:`.
**Live log evidence pull:** Railway, 2026-05-07T15:25-15:27Z.

---

> ## 📌 Status update (2026-05-10 / 2026-05-11)
>
> This doc is the **canonical RFQapp audit**. Earlier `docs/AUDIT_*` files
> (2026-05-06, the morning E2E, FULL, HANDOFF, HOME_QUOTING_GROWTH variants)
> are superseded; the 2026-05-06 doc was removed from git in this PR.
>
> **Tier 0/1/2/3 status after 2026-05-08 → 2026-05-10 session:**
>
> | Findings | Status |
> |---|---|
> | Tier 0a (QB OAuth CSRF) | ✅ shipped #828 |
> | Tier 0b (7-col INSERT) | ✅ shipped #828 |
> | Tier 1a (inline pricing panel) | ✅ shipped #830 |
> | Tier 1b Phase 1 (Quote.set_price bid_price param) | ✅ shipped #831 |
> | Tier 1b Phase 2 (50-site rollout) | ⏰ soak-gated to 2026-06-07; durable remote routine `trig_01DfmmCPah1MoLrwVTh8Gj4x` armed |
> | Tier 1c (status taxonomy) | ✅ shipped #832 + #840 |
> | Tier 1d (with_retry substrate) | ✅ shipped #833-#839 |
> | Tier 2a (audit_trail unification) | ✅ shipped #854 + #858 hotfix |
> | Tier 2b/c/d/e | ✅ shipped #836/837/842/847 |
> | Tier 3a/b/c | ✅ shipped #841-#848 |
> | Tier 3d (KPI dashboard) | ⏰ explicit fresh-session, ~1 week scope |
> | S-1 .. S-15 substrate items | ✅ all shipped #850-#857 (S-11 phase 2 in #859) |
>
> **Additionally shipped 2026-05-11 (renderer-accuracy + lint substrate):**
>
> | PR | Subject |
> |---|---|
> | #874 | Renderers derive subtotal from subtotal_of(); fill_704b gains the missing assert_subtotal_invariant call |
> | #875 | `tools/lint_quote_status_writes.py` — blocks new raw `UPDATE quotes SET status` writers (9 exempt baseline files documented for PR-η migration) |
> | #876 | Adapter render-divergence test (precondition for re-enabling `quote_model_v2_enabled`) + `scripts/data_integrity.py` check 11 (stale unit_price gauge) |
>
> **New findings tonight's deep audit surfaced (post-v2 still open):**
>
> - **Renderer-read drift class** — pre-#874, all 4 renderers used `extension = unit_price * qty` (raw) and accumulated subtotal in-loop. Cortech-mattress $148.96 under-quote class. **CLOSED #874.**
> - **fill_704b missing invariant** — 704B RFQ-response PDF (customer-facing) had NO `assert_subtotal_invariant` call. PR #849 covered PC + quote PDF, missed this. **CLOSED #874.**
> - **UI/PDF predicate divergence** — `pc_detail.html` JS subtotal filtered on `bid` checkbox; `simple_submit.html` filtered on nothing. PDFs filtered on `no_bid` field. **CLOSED in branch `fix/ui-subtotal-predicate-no-bid` (commit ce3717d1) — push-deferred for Mike's morning chrome-verify per CLAUDE.md.**
> - **Quote.set_price adoption is 1/N sites** — Tier 1b Phase 2 status was "soak-gated to 2026-06-07 with routine armed" but only `quote_engine.apply_oracle_pricing` calls it. The 14-site rollout queue from `project_build_remaining_queue_2026_05_08.md` is the real Tier 1b Phase 2 — needs explicit start, not auto-fire. **OPEN — PR-ε scaffolding.**
> - **`quote_model_v2_enabled` flag still OFF** since 2026-05-05 incident. Unblocking PR #826 shipped 2026-05-07; flag never flipped. Render-divergence test in #876 closes the precondition; flag flip itself is a separate operator action. **OPEN — PR-ζ flip deferred.**
> - **13 raw status writers** bypass `set_quote_status_atomic` (audit §S-12). Lint in #875 blocks new ones; baseline migration is PR-η Phase 2. **PARTIAL — substrate locked, rollout deferred.**
> - **canonical_unit_price treats markup_pct=0.0 as missing** (Python falsy `or` chain). Rare in prod (give-away/sample items use `no_bid=True`), but a real follow-up. **OPEN — low priority.**
>
> **Additionally shipped 2026-05-10 (post-audit follow-on):**
>
> | PR | Subject |
> |---|---|
> | #859 | S-11 phase 2 — last 3 dashboard.py daemons emit heartbeat |
> | #860 | /health active_pcs sentinel typo |
> | #861 | 5 silent COUNT(*) sentinels now log.warning |
> | #862-#864 | Phantom-import lint substrate + 18 drains |
> | #865 | Voice campaigns FEATURE DELETED (half-built) |
> | #866-#867 | Async-queue wrapper + GC bug fix |
> | #868 | SCPRS Mon/Wed scheduler real-name fix |
> | #869 | LangGraph orchestrator FEATURE DELETED (half-built) |
> | #870-#872 | Final phantom-drain (down to 0 baseline) |
>
> Phantom-import lint substrate at `tools/lint_phantom_imports.py` keeps
> the gain locked. Pre-push hook enforces.

---

## §1. Are all 7 classes still closed on prod?

| PR | Claim | Evidence | Status |
|---|---|---|---|
| #821 | Buyer-reply diff panel + extract-diff endpoint | `routes_rfq.py:4525` route + `rfq_detail.html:1336-1455` panel; auth+safe_route gates intact | ✓ closed |
| #822 | Auto-pricing TP/FP telemetry | `src/agents/auto_pricing_tp_fp.py` exists; endpoints at `routes_rfq.py:4615/4687`; `auto_priced_value/at/source` stamps at `4004-4006` | ✓ closed (with caveats — see §3) |
| #823 B-1 | `order_dal._item_status` isinstance guard | `order_dal.py:1118-1124` has `if not isinstance(item, dict): return "pending"`. Live log: dashboard /init returned 200 in 4016ms, no `'str' object` error | ✓ closed |
| #823 B-2 | Invoice poller `_get_email_config` restored | `invoice_processor.py:274` `def _get_email_config()` defined; live log 15:25:46Z `Found QB invoice email: #26-003` | ✓ closed (one new failure shape — see §3) |
| #823 B-3 | Gmail `_with_gmail_retry` on `list_message_ids` + `get_raw_message` | `gmail_api.py:175` defines helper; wraps L213, L238. Live log 15:25:45Z `Gmail get_raw_message transient error (attempt 2/3): [SSL] record layer failure — retry in 1.0s` — actually firing in prod | ✓ closed |
| #824 | pypdf padding-error WARN→ERROR | `structured_log.py:103` `logging.getLogger("pypdf._crypt_providers._cryptography").setLevel(...)`. Live log: 0 padding warnings in 2-min window (was hundreds before) | ✓ closed |
| #825 | DROP TRIGGER preamble removed from `railway.toml` | `railway.toml` startCommand is now just `gunicorn ...` (no DROP TRIGGER). 11/11 startup checks passed; boot 28.4s. Issue #415 closed at 14:27:27Z | ✓ closed |
| #826 | `Quote.from_legacy_dict` reads canonical `items` first | `quote_model.py:421-440` resolver picks `items_alias` first; logs WARN on mismatch. 30-line block with explicit doc comment | ✓ closed |
| #827 | `_with_gmail_retry` extended to `get_message_metadata` | `gmail_api.py:263` wrapped via `_with_gmail_retry(... op="get_message_metadata")` | ✓ closed |

**All 7 closed.** Gmail retry actually firing in prod was the most reassuring single line of evidence — the helper is doing live work right now.

---

## §2. Memory drift found

The audit memory `project_full_audit_2026_05_07.md` is now partially stale:

- "B-1/B-2/B-3 live fixes (0.5d)" — **CLOSED** (PR #823). Update.
- "pypdf padding-error log noise — 1 line in structured_log.py" — **CLOSED** (PR #824). Update.
- "Awaiting Mike's pick" — restated below in §4 with refreshed scope, since some picks have shifted.
- DB bloat 548MB > threshold — **STILL BLEEDING.** Live log 15:25:55Z still shows 548.4MB. Memory note "Tracked-not-P0" and `db_bloat_diagnostic.py exists, never run with retention proposal" still accurate.
- "QA agent 5 unacknowledged regressions; no UI surface" — **STILL BLEEDING.** Live log 15:25:13Z `[WARN] QA REGRESSION: 5 unacknowledged score drops`.
- The 10 substrate classes (C-1..C-10):
  - C-10 log signal-to-noise — **closed by #824** (one line away).
  - C-1 parallel-implementation — **untouched** at substrate; C-2 dual-source-of-truth — **untouched**; C-3 status drift — **WORSENED** (re-audit found a 4th and 5th status whitelist; see §3); C-4 silent-overflow — partially addressed by PR #801 (form-capacity registry); C-5 RMW race — **8/8 batches done** but mainline `update()` form-post still unwrapped (see §3); C-6 thread-blind dedup — **substrate gap discovered** (see §3); C-7 logs-not-metrics — untouched; C-8 home-page widget dump — untouched; C-9 pricing-without-data — untouched.

**Memory entry `project_audit_b_fixes_2026_05_07.md`** reads accurately. Confirms #823 closed B-1/B-2/B-3.

---

## §3. New findings the re-audit surfaced (not in the prior audit)

These are new evidence, not restatements. Cited file:line via `git show origin/main:` reads.

### S-1 (P0 security) · QuickBooks OAuth has no real CSRF defense

`src/api/modules/routes_intel_ops.py:1066` — the OAuth `state` parameter is hardcoded `state=reytech`. The callback at L1080+ never reads `state` back to validate. **Trivially CSRF-able**: an attacker can construct a QB OAuth URL with `code=<their_attacker_realm_code>` and trick Mike into clicking; Reytech's callback gladly swaps the code, saves attacker tokens to `data/qb_tokens.json`, and `os.environ["QB_REFRESH_TOKEN"]` is rewritten in-process to the attacker's realm. Mike's invoice tracking silently rebinds to attacker's books — any `quote_won` auto-creates a PO in the wrong realm.

**This wasn't in the prior audit.** Highest blast-radius single finding.

### S-2 (P0 architecture) · `_save_single_rfq` *still* doesn't write 7 schema columns

`src/api/data_layer.py:286-311` — INSERT enumerates 22 columns + `data_json`. **Missing from the column list:** `email_thread_id`, `email_message_id`, `original_sender`, `gmail_draft_id`, `gmail_message_ids`, `gmail_thread_duplicate_of`, `requirements_json`. Each is declared by `_migrate_columns` (`db.py:1696-1751`).

**`_save_single_pc` (`data_layer.py:475-500`) has the same omission.**

The data IS captured — into the `data_json` blob. But the *dedicated SQL columns* that PRs #808-#812 (thread-aware-ingest schema) and #815/#820 (observed-sends) and #821 (buyer-reply diff) all rely on are populated only by:
- `_migrate_columns` at boot (default `''`)
- `scripts/backfill_email_thread_id.py` (one-time backfill, exposed via `routes_rfq.py:2987`)

**The thread-aware substrate work shipped overnight is built on a foundation that the primary writer doesn't update.** New RFQs go in with `email_thread_id=''` until backfill runs. The prior audit (morning) flagged this as **P0-5** and said "any future SQL-side match returns zero rows". The shipped-overnight PRs masked the gap with backfill scripts but did not close it. **This is the single most important finding of this re-audit.**

### S-3 (P0 ops) · 5 Twilio implementations with two divergent env-var conventions

Yesterday's count was 4. Today it's **5**, with TWO env-var schemes:

| File:line | Function | Env-var convention |
|---|---|---|
| `routes_crm.py:4369` | `_send_sms` | `TWILIO_ACCOUNT_SID`/`TWILIO_AUTH_TOKEN`/`TWILIO_FROM_NUMBER` (Twilio-official names) |
| `due_date_reminder.py:151` | `_send_sms_reminder` | same official names |
| `notify_agent.py:271` | `_send_alert_sms` | `TWILIO_SID`/`TWILIO_TOKEN`/`TWILIO_FROM` (short names) |
| `notify_agent.py:977` | `notify_new_rfq` | short names |
| `growth_agent.py:3300` | `send_sms_outreach` | short names |

If a fresh-install operator sets the official Twilio names, **`notify_agent` alerts and growth SMS silently no-op.** Half the alert surface depends on which env-var convention got set first.

### S-4 (P1 architecture) · `audit_trail` table has THREE incompatible schemas, two writers silently no-op in prod

Re-confirmed; this finding was in yesterday's morning audit. Re-stated here because nothing shipped overnight closed it. Two of three audit-write pipelines (`security.py:170` + `routes_catalog_finance.py:2000` + `startup_checks.py:275`) raise `OperationalError`, get swallowed by surrounding `except Exception: log.debug(...)`, and silently lose data. The `/api/audit` UI page is permanently empty.

### S-5 (P0 architecture) · `rfqs.json` is `{}` on disk but 30 readers consult it

`src/api/modules/routes_orders_full.py:104-119` defines `_load_rfqs_from_json()` which reads `rfqs.json` directly. Called from **12 distinct routes** in that file, plus `email_poller.py:891`, `manager_agent.py` (3 sites), `qa_agent.py:624`, `workflow_tester.py` (4 sites), `agents/drive_backup.py:169` (lists as essential!).

Every one silently sees `{}` → "no RFQ found" → falls through to a wrong default (manager brief misses RFQs, orphan-order link backfill misses links, drive-backup omits "essential" data). This is the architectural shape behind the 67-orphan-orders count Mike's been chasing.

### S-6 (P1 ops) · `_with_gmail_retry` transient-error list is fragile substring match

`gmail_api.py:165-167` — match against `str(err)`: `IncompleteRead, record layer failure, Connection reset, Connection aborted, EOF occurred, TimeoutError`. **Misses:**

- `429` (rate limit) — treated as terminal.
- `googleapiclient.errors.HttpError` 5xx (`503 Service Unavailable`) — treated as terminal even though it's the canonical "retry me" response.
- `socket.timeout` — surfaces as `"Read timed out"` from `requests`/`urllib3`, not matched.
- `httplib2.ServerNotFoundError` (DNS flap), `BrokenPipeError`, `ssl.SSLEOFError`.

A substring-match against a `str(exception)` is fragile across googleapiclient version bumps. PR #823 closed the obvious `IncompleteRead`/SSL transients; the next class of transient (429 storm during a busy email burst) will silently skip messages.

### S-7 (P0 cost) · No retry / no daily budget for Anthropic in `buyer_reply_diff` (PR #821 path)

`src/agents/buyer_reply_diff.py:312-321` — outer `try/except Exception` maps everything (including `anthropic.RateLimitError`) to `(_empty_diff(), f"LLM call failed: {type(e).__name__}: {e}")`. Logged at DEBUG only. **A 429 storm during a buyer-reply burst silently degrades every diff to empty.** Operator sees "no changes detected" alongside `LLM call failed: RateLimitError` in skipped_reason — no operator alert, no automatic retry.

Plus: **no result caching.** The diff isn't persisted to `reply["_extracted_diff"]`. Operator clicking "Extract changes" 100× = 100 paid Claude calls. `src/core/api_quota.py` exists and is wired into `item_identifier.py` only — not into any of the ~15 other Anthropic callers. **No daily $ cap on Claude.**

### S-8 (P1 ops) · PR #822 telemetry has no scan-id dedup; no per-RFQ tp_rate writeback

`routes_rfq.py:4615-4694` — scan walks all RFQs, appends per-record JSONL rows in `"a"` mode, no atomic across the scan, no `(scan_id, record_id)` dedup. Re-running scan (which the operator might do) **double-counts**. Every row has `_scanned_at` but `summarise_jsonl` doesn't dedup on it.

The PR's docstring says it stamps `r["tp_rate"]` per RFQ. **It does not.** The code only appends to JSONL. UI panels that expect `r.tp_rate` see empty.

### S-9 (P1 architecture) · 3 PC-side status whitelists with diverging members

`routes_pricecheck.py:387` accepts `completed`, `pending_award`, `expired`, `converted`, `parsed`, `priced`, `ready` (15 strings).
`routes_pricecheck_admin.py:5783` accepts `parsed, draft, priced, ready, sent, won, lost, expired, no_response, new` (10 strings).
`routes_pricecheck_pricing.py:356` accepts `not_responding, dismissed, archived, duplicate, no_response, won, lost` (7 strings).

The string `completed` is accepted by 1, rejected by 2. `pending_award` is accepted by 1, rejected by 2. `not_responding` is in canonical_state but not QuoteStatus enum. **An operator hitting the "wrong" PC status endpoint silently 400s or coerces to a different status** — no central document of which endpoint accepts which strings.

### S-10 (P1 ops) · `api_resend_package` + `/rfq/<rid>/send` have no idempotency key

`routes_rfq.py:3338` (`api_resend_package`) and `routes_rfq.py:3402` (synchronous send) — both call `gmail_api.send_message` directly. Gmail send is NOT retried at the SDK layer (PR #827 explicit). But: **operator double-click sends the same email twice**, because there's no client-side idempotency token, no server-side "already sent in last N seconds" check, no `gmail_draft_id` reservation. The buyer receives two PDFs.

### S-11 (P1 architecture) · 6 daemon `while True` loops invisible to scheduler watchdog

`scprs-export-watcher` (line 226), `scprs-fiscal-scrape` (line 606), `gdrive-worker` (line 639), `utilization-flusher` (line 86), `error-handler` (line 109), plus 3 unnamed loops in `dashboard.py` (lines 6133, 6156, 6189). **None call `heartbeat()`.** A silent crash in `gdrive-worker` accumulates Drive tasks forever; nothing alerts.

### S-12 (P1 architecture) · `mark_won` is a 7-block non-atomic write

`src/core/quote_lifecycle_shared.py:107-225` — yesterday counted 5 separate `with get_db() as conn:` blocks; re-count today found **7**. A crash between block 2 and 6 leaves revenue logged but no calibration, no activity_log row, no award_tracker_log entry. The status flip itself is atomic (`set_quote_status_atomic`); the broader transition is not.

### S-13 (P1 architecture) · `won_quotes` table created twice with conflicting nullability

`db.py:963` creates with `description TEXT` (nullable). `won_quotes_db.py:81` creates with `description TEXT NOT NULL`. Whichever runs first wins; subsequent INSERT into the wrong shape can throw at runtime (and is caught by surrounding `try/except`).

### S-14 (P1 architecture) · `quotes` ON CONFLICT DROPs 12 lifecycle fields

`db.py:2278-2308` — the UPDATE SET clause omits: `is_test, sent_at, source, source_pc_id, source_rfq_id, created_at, expires_at, closed_by_agent, close_reason, revision_count, win_probability, last_follow_up, follow_up_count`. A re-import or retry that hits an existing quote silently drops these fields on update. `is_test`/`created_at` omission is documented intent; the others are bugs.

### S-15 (P0/P1 substrate) · 14 routes_orders_full sites have empty `quote_number` paths

The 67-orphan-orders symptom traces to: `order_dal.py:448` defaults to empty string, ON CONFLICT clobbers existing `quote_number` with new (possibly empty) value. SCHEMA `db.py:334` declares `quote_number TEXT` with no `NOT NULL`, no `CHECK`, no FK to `quotes(quote_number)`. **5 distinct sites can write empty `quote_number`** including `routes_orders_full.py:3568` (reorder constructor literally `"quote_number": ""`).

### Cross-cutting count refreshes (vs prior audit)

- Templates audit found ~80 findings (P0: 13, P1: 23, P2: 44). 184 hardcoded `/api/...` URLs (was 62 yesterday — yesterday's count was scoped narrower). 39 `prompt()`/`confirm()` chains. 232 `getElementById` in `rfq_detail.html` alone, ~50 unguarded.
- Routes audit found ~50 save sites still bypassing `Quote.set_price/transition`. 8/8 RMW batches done but mainline `update()` form-post still unwrapped (`routes_rfq.py:3508`). 7 fire-and-forget threads with in-memory POLL_STATUS that vanishes on gunicorn worker recycle.
- Persistence audit found `audit_trail` 3 schemas (still); `won_quotes` 2 schemas (new finding); `email_log` likely the 548MB bloat dominant table; VACUUM disabled; **0 retention crons across 6+ monotonically-growing tables.**
- External audit found **PR #823 added a 7th retry implementation; consolidated none.** No `external_call.with_retry()` substrate.

---

## §4. Recommended next pick — Mike chooses, I do not start

The pick options have shifted. Some are new (post-#821-#827 evidence). Some are sharper after deep re-read. **Effort estimates are with worktree + tests + chrome-verify per CLAUDE.md.**

Ordered by KPI cost ÷ effort, highest-leverage first:

### Tier 0 — Security / data-loss critical

| # | Item | KPI cost | Effort | Risk | Why now |
|---|---|---|---|---|---|
| **0a** | **S-1: QuickBooks OAuth CSRF fix** — replace hardcoded `state=reytech` with random session-token, validate on callback | "attacker rebinds Reytech to wrong QB realm; every won quote auto-creates PO in attacker's books" | 1-2 hours | low | Single-point security gap, no operator visible defense. Should ship without further conversation. |
| **0b** | **S-2: `_save_single_rfq`/`_save_single_pc` 7-column INSERT fix** — add `email_thread_id`, `email_message_id`, `original_sender`, `gmail_draft_id`, `gmail_message_ids`, `gmail_thread_duplicate_of`, `requirements_json` to the INSERT lists | "thread-aware-ingest substrate (PRs #808-#821) silently builds on backfill-only data; new RFQs land with empty thread-id and the SQL-side match never works" | 2-4 hours | low | The 7-PR overnight ship has a hidden write-side gap. Closing it would actually make the thread-aware-ingest work as intended without a perpetual backfill cron. |

### Tier 1 — Substrate (KPI-shaping)

| # | Item | KPI cost | Effort | Risk |
|---|---|---|---|---|
| 1a | **PR-D1 inline pricing intelligence panel on `rfq_detail.html` / `pc_detail.html`** — `last_won_for_buyer(email, desc, pn)` + `scprs_ceiling_for_item(desc, pn)` per line item | "pricing decisions made without the data that should drive them; queries already exist at routes_growth_intel.py:1202 but are 2 clicks deep" | 2 days | low |
| 1b | **PR-1 `Quote.set_price()` / `Quote.transition()` unification** — first add `bid_price` parameter to `Quote.set_price` (re-audit confirmed it can't represent "preserve operator bid"), then route the 3 reconcile-aware sites through it. After 30 days holding, fan to the other 50 save sites | "100% of pricing/status writes bypass the canonical model; PR #765 reconcile is wired into 3 of ~17 sites only" | 1 day | low |
| 1c | **Status taxonomy unification (S-9)** — collapse the 5 inline whitelists to one `is_valid_status_for(record_type, status)` predicate; add lint banning `status IN (` in route files | "`completed`/`pending_award`/`ready_to_send` escape both enums; PC-side has 3 divergent endpoints" | 1 day | low |
| 1d | **`external_call.with_retry()` substrate** (subsumes B-3 + S-6 + S-7) — single helper for Gmail/Drive/SCPRS/Twilio/QB/Anthropic; per-service transient list; unified metrics emission; 429-aware; subsumes the 7 ad-hoc retry implementations | "PR #823 was a 7th retry implementation, not a consolidation; without it, every future external API needs its own retry" | 3 days | low |

### Tier 2 — Data hygiene (P1)

| # | Item | KPI cost | Effort | Risk |
|---|---|---|---|---|
| 2a | **`audit_trail` schema reconciliation (S-4)** — pick one shape, ALTER existing rows, retire two parallel writers | "audit-trail UI permanently empty; security audits silently lose 2/3 of writes" | 1 day | low |
| 2b | **`rfqs.json` cleanup (S-5)** — make `_load_rfqs_from_json()` read SQLite or be deleted; switch 14 `routes_orders_full` callers; delete the JSON file | "30+ readers see empty data; manager brief, orphan-link, drive-backup all silently miss" | 4 hours | low |
| 2c | **`api_resend_package` idempotency (S-10)** — 60-second "already sent" guard; client-side idempotency token | "operator double-click sends 2 PDFs to buyer" | 1 day | low |
| 2d | **DB retention cron** — 90-day on `email_log`, `utilization_events`, `audit_trail`, `recommendation_audit`, `lifecycle_events`, `processed_emails`; weekly incremental VACUUM | "548MB > 500MB threshold; bloat grows ~5MB/day; eventual disk-full" | 1 day | low |
| 2e | **Twilio consolidation (S-3)** — one `_send_sms()` helper with one env-var convention; 5 callers collapse to 1 | "half the alert surface silently no-ops if operator uses official Twilio env-var names" | 1 day | low |

### Tier 3 — UX + observability (P2 but valuable)

| # | Item | KPI cost | Effort | Risk |
|---|---|---|---|---|
| 3a | **PR-2 home slim + canonical_state filters** — delete 21 of 27 home widgets; replace inline filters with `is_active_queue()` | "operator scan time inflated by accretion; widgets disagree mathematically" | 2 days | low |
| 3b | **Buyer-reply diff caching (S-7)** — persist `reply["_extracted_diff"]` after first call; skip re-call when present | "100 panel clicks = 100 paid Claude calls; no daily budget cap" | 4 hours | low |
| 3c | **Anthropic `api_quota` rollup** — wire `api_quota.can_call("claude")` into all 15+ Anthropic callers; daily $ cap | "no enforced daily Claude spend cap; runaway scenarios possible" | 1 day | low |
| 3d | **Metrics layer + KPI dashboard** — quote pipeline funnel, per-buyer win rate, per-form failure rate. Plus retry-counters from S-6 to detect Gmail degradation | "every future improvement is opinion without measurement; QA agent has 5 unacknowledged regressions and no UI surface" | 1 week | low |

---

## §5. My recommendation

**Ship Tier 0a + 0b together as a single PR.** Both are surface-scope, low-risk, high-leverage:
- 0a closes a real security gap that nobody has noticed.
- 0b closes the substrate gap that the 7-PR overnight ship was masking with backfill scripts. **Until 0b ships, the thread-aware-ingest work cannot be trusted to work without manual backfill.**

After Tier 0, I'd ship Tier 1a (PR-D1 inline pricing panel) for the highest win-rate lift. Tier 1b/c/d are substrate changes; recommend serializing one at a time per `feedback_no_patching_slop_kpi_architecture` (one substrate fix, hold 30 days, observe before next).

I have not opened a worktree, branch, or PR. **Pick one and tell me.**

---

*— end re-audit v2 —*
