# PRD — Orders V2 Email Poller Migration

**Status:** Spec approved 2026-04-17, implementation in progress
**Branch:** `feat/orders-v2-poller-migration`
**Worktree:** `../rfq-orders-v2-poller-migration`
**Owner:** Mike + Claude (this session)

## 1. Problem

The PO email poller (`_poll_po_inbox` / `_process_po_email` in
`src/api/modules/routes_order_tracking.py`) is the last live writer to the
legacy `purchase_orders` + `po_line_items` + `po_emails` + `po_status_history`
tables. Every other read/write path on the user side has migrated to V2
(`orders` + `order_line_items` via `order_dal`). The boot-time merge in
`db._fix_data_on_boot` Fix 6 keeps the V2 tables in sync with the legacy ones,
but only at process start — POs that arrive between deploys live solely in the
legacy schema until the next reboot.

This is a split-brain. We cannot drop the legacy tables until the poller
writes through `order_dal`.

## 2. Goal

Single PR that cuts the poller over to `order_dal`. After this ships and
soaks for ≥7 days with zero writes to the legacy tables, the next PR drops
those tables.

## 3. Scope

**In scope (this PR):**
- Rewrite `_process_po_email` end-to-end against `order_dal`.
- Match incoming PO numbers to `orders` (V2) rows, not `purchase_orders`.
- Apply tracking + status updates via `order_dal.update_line_status` and
  `order_dal.transition_order` / `order_dal.compute_order_status`.
- Record inbound email events in `order_audit_log` (replaces `po_emails`).
- Feature flag `orders_v2.poller_unified` — default OFF, flip ON via
  `/api/admin/flags` after observing one clean poll cycle.
- Per-branch unit tests (`tests/test_orders_v2_poller.py`).

**Out of scope (future PRs):**
- Migrating the user CRUD endpoints in `routes_order_tracking.py`
  (`/api/po/create`, `/api/po/<po_id>/update-item`, `/api/po/<po_id>/update-status`).
- Deleting `/po-tracking-legacy` and `/po-tracking/<po_id>` views.
- Dropping the four legacy tables (separate PR after 7-day zero-write).

## 4. Non-goals

- No dual-write. Hard cut. When the flag is ON, the legacy
  `purchase_orders` / `po_line_items` / `po_status_history` paths receive
  zero new rows from the poller. Boot migration plus the existing data is
  the safety net; we are not rebuilding parity logic at write time.
- No backfill of historical email logs. `po_emails` rows that exist today
  stay where they are; new inbound events go to `order_audit_log`.
- No new email-parser features (still 5 status keywords + tracking regex).

## 5. User-visible behavior

User-visible delta: **nothing** when working as designed. The Orders V2
detail page already shows tracking, status, timeline. The new poller writes
into the same tables that page reads from, so the user sees their POs
update in real time instead of waiting for the next deploy's boot migration.

If we cut over and the new poller is broken, the `/orders` view stops
auto-updating from email. We catch that within one polling cycle (5 min)
because the runtime feature-flag console (`/api/admin/flags`) lets us
flip the flag back to OFF in <1 minute and the legacy code path resumes.

## 6. Decisions (locked with Mike)

| Question | Decision | Rationale |
|---|---|---|
| One PR or many? | **One PR** | "One strike" — Mike. Doesn't affect quote-out-the-door so blast radius is acceptable. |
| Which step first? | **Poller** | Stops the bleeding. CRUD endpoints are user-triggered and easy to migrate later. |
| Dual-write or hard cut? | **Hard cut** | Mike: "hard cut". Avoids parity logic complexity. FF is the rollback. |
| Feature flag? | **Yes** — `orders_v2.poller_unified`, default OFF | Explicit Mike approval. Sub-1-min rollback if the new path misbehaves. |
| Per-branch parity tests or rewrite all 6 at once? | **Rewrite** | Mike: "do rewrite". Tests cover all branches but the implementation lands as one function. |

## 7. Implementation plan

### 7.1 New code

**`_process_po_email_v2(subject, sender, body, email_uid)`** — sibling to the
existing `_process_po_email`. Writes through `order_dal`. Same return shape
(`{"matched": bool, "po_id": str|None, "updates": [...]}`) so the caller in
`_poll_po_inbox` is one-line gated.

**Match logic:**
- Parse PO numbers from `subject + body` via existing `_extract_po_numbers`.
- For each PO# found: `SELECT id FROM orders WHERE po_number = ? LIMIT 1`.
  Skip if no row (preserves legacy "silent skip on unknown PO" behavior —
  and matches what the boot migration produced: every legacy `purchase_orders`
  row has an `orders` row with id `ORD-PO-<po_number>`).

**Apply updates:**
- `tracking` update → for each `order_line_items` row where
  `sourcing_status` ∈ ('pending', 'ordered'):
  - `order_dal.update_line_status(order_id, line_db_id, "tracking_number", ..., actor="email_poller")`
  - `order_dal.update_line_status(order_id, line_db_id, "carrier", ..., actor="email_poller")`
  - `order_dal.update_line_status(order_id, line_db_id, "sourcing_status", "shipped", actor="email_poller")`
  - `order_dal.update_line_status(order_id, line_db_id, "ship_date", today, actor="email_poller")`
- `status_change` update — map legacy → V2:
  - `shipped` → `transition_order(order_id, "shipped", actor="email_poller")` +
    update each line's `sourcing_status` to `shipped`.
  - `delivered` → `transition_order(order_id, "delivered", ...)` +
    each line's `sourcing_status` to `delivered`.
  - `backordered` → each line's `sourcing_status` to `backordered`,
    then `compute_order_status` to roll up.
  - `invoiced` → `transition_order(order_id, "invoiced", ...)`.
  - `confirmed` → `transition_order(order_id, "sourcing", ...)` (V2 doesn't
    have "confirmed" — `sourcing` is the closest mapped equivalent).
- After all updates: `compute_order_status(order_id, actor="email_poller")`.

**Email log:** instead of `INSERT INTO po_emails`, write a single
`order_audit_log` row:
```sql
INSERT INTO order_audit_log (order_id, action, actor, details, created_at)
VALUES (?, 'inbound_email', 'email_poller', ?, ?)
```
where `details` = JSON of `{subject, sender, body_preview, parsed_updates}`.

### 7.2 Gate

```python
def _process_po_email(subject, sender, body, email_uid):
    from src.core.flags import get_flag
    if get_flag("orders_v2.poller_unified", False):
        return _process_po_email_v2(subject, sender, body, email_uid)
    return _process_po_email_legacy(subject, sender, body, email_uid)
```

The legacy implementation keeps its current name-suffixed alias
`_process_po_email_legacy` so it remains test-callable for regression and
for instant rollback via the FF.

### 7.3 Migration / rollout

1. Ship PR with flag default OFF. Production behavior unchanged.
2. After deploy, flip on via:
   ```bash
   curl -X POST .../api/admin/flags \
     -d '{"key":"orders_v2.poller_unified","value":"true",
          "updated_by":"mike","description":"V2 poller cutover"}'
   ```
3. Wait one polling cycle (5 min). Verify in `/orders` that any inbound
   email matched a known PO produced an audit log entry and an orders
   table update.
4. If broken, DELETE the flag (instant rollback).
5. Soak 7 days with zero-write to legacy tables, then PR #2: drop tables.

## 8. Tests

`tests/test_orders_v2_poller.py`:

| Test | Scenario | Assertion |
|---|---|---|
| `test_unknown_po_silently_skipped` | Email with PO# that doesn't match any order | `matched=False`, no DB writes |
| `test_tracking_number_applied` | Email "Tracking: 1Z999AA10123456784" | All pending lines get tracking + carrier + ship_date + status=shipped |
| `test_shipped_keyword_transitions_order` | Email body "Your order has shipped" | `orders.status` = "shipped", all lines `sourcing_status` = "shipped" |
| `test_delivered_keyword_transitions_order` | Email body "Package delivered" | `orders.status` = "delivered" |
| `test_backorder_keyword_marks_lines` | Email body "Item backordered" | All lines `sourcing_status` = "backordered" |
| `test_invoiced_keyword_transitions_order` | Email body "Invoice attached" | `orders.status` = "invoiced" |
| `test_confirmed_keyword_maps_to_sourcing` | Email body "Order confirmed" | `orders.status` = "sourcing" |
| `test_audit_log_records_email` | Any matched email | `order_audit_log` has `action='inbound_email'` row |
| `test_flag_off_uses_legacy_path` | FF off, poll an email | Writes go to `purchase_orders`/`po_line_items`, not V2 |
| `test_flag_on_uses_v2_path` | FF on, poll same email | Writes go to V2, none to legacy |

All tests use the existing test sandbox fixtures (isolated temp DB, no
external IMAP — `_process_po_email_v2` takes the parsed strings directly).

## 9. Risk assessment

| Risk | Mitigation |
|---|---|
| New code writes wrong order_id | Match by `po_number` exactly; same regex as legacy. Test covers unknown PO. |
| Status keyword matches multiple statuses in one email | Same as legacy — last write wins. We're not changing parser semantics, only sink. |
| `transition_order` rejects unknown status | Mapped legacy status → V2 status table in implementation. "confirmed" → "sourcing" is the only fuzzy mapping. |
| `order_audit_log` write fails silently | order_dal already wraps DB writes; check Mike's existing telemetry on `/api/admin/flags` errors. |
| Flag flip doesn't propagate fast enough | flags.py has 60s TTL — worst-case 60s lag. Acceptable for a 5-min polling cycle. |
| FF off path breaks during PR review | Legacy code is renamed only — same body, same SQL, same tables. Diff should show ~0 logic changes in legacy fn. |

## 10. Acceptance

- [ ] `tests/test_orders_v2_poller.py` — 10 tests pass.
- [ ] Existing `tests/test_orders_v2_po_merge.py` still passes (boot
      migration regression guard).
- [ ] Full sandbox suite green.
- [ ] PR description includes the flip-flag curl command.
- [ ] MEMORY.md `project_orders_v2_po_merge_audit.md` updated post-merge to
      record poller migration done; next step is CRUD migration.
