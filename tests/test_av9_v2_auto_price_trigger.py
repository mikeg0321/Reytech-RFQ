"""PR-AV9 (AV-9) — WITHDRAWN 2026-05-26 (substrate-singleness fix).

Original purpose: pin the V2 ingest path's `_trigger_auto_price` call so
that V2-extracted RFQs reached pricing parity with the legacy path.

Why withdrawn: the "parity" itself was a bug. `_trigger_auto_price`
spawns an `auto_rfq_*` PC for every RFQ-class email — doubling each
inbound on /home (PC panel + RFQ panel render the same email twice).
Mike caught it 2026-05-26: 5 ghost PCs visible on prod
(`auto_rfq_d877/f11c/cc83/0124/89bb`), each one a duplicate of a
real `rfq_*` row. That's the substrate-singleness defect class
(`[[feedback-kpi-substrate-singleness]]`) — 4th instance in 5 days
(after Oracle KPI #1076, SCPRS liveness #1086, Quote ingestion
liveness #1088).

The fix is at the SUBSTRATE: RFQ-class records live in `rfqs` only.
The PC substrate is reserved for the `is_price_check_email` early-
detect branch in `email_poller.py` — true PC-class emails (704
worksheets, AMS price-check headers). Both `_trigger_auto_price`
callsites in `dashboard.process_rfq_email` (the V2 branch at L~2271
and the legacy tail at L~3595) have been REMOVED.

What these tests pin now:
  - `_trigger_auto_price` itself remains in dashboard.py (in case a
    future caller wants it for a true PC path), and its idempotency
    + empty-line-items + already-priced safety nets all still hold.
  - The V2 success branch does NOT call `_trigger_auto_price` —
    source-grep guard against accidental re-introduction.
  - The legacy `process_rfq_email` tail does NOT call
    `_trigger_auto_price` — same guard at the other seam.

CCHCS auto-pricing is now Spine's responsibility (`spine/auto_pricer.py`
per §0 LAW 1). Non-CCHCS legacy RFQs price via the editor's manual
flow until those agencies migrate to Spine.
"""
from __future__ import annotations


def test_trigger_auto_price_function_still_exists():
    """The function isn't deleted (in case a true PC path wants it
    later) — only the RFQ-class callsites are removed. Calling it on
    an empty record is still safe."""
    from src.api.dashboard import _trigger_auto_price
    # No-op on empty line_items
    _trigger_auto_price({"id": "rfq_empty", "line_items": [], "solicitation_number": "TEST"})


def test_trigger_auto_price_still_idempotent():
    """Idempotency safety net still holds even though no caller is
    expected to hit this today — defensive against future reuse."""
    from src.api.dashboard import _trigger_auto_price
    rfq = {
        "id": "rfq_already_priced",
        "line_items": [{"description": "A", "qty": 1}],
        "solicitation_number": "TEST",
        "auto_price_pc_id": "pc_existing_abc123",
    }
    _trigger_auto_price(rfq)
    assert rfq["auto_price_pc_id"] == "pc_existing_abc123"


def test_v2_success_branch_does_NOT_call_trigger_auto_price():
    """Source-grep guard: the V2 success branch must NOT invoke
    `_trigger_auto_price`. Re-introducing it would re-create the
    `auto_rfq_*` ghost-PC bug Mike caught 2026-05-26.

    Find the branch by its sentinel string (`_v2_result.record_type == "rfq"`)
    and assert no `_trigger_auto_price` call sits in the window
    between that sentinel and the branch's `return _new_rfq`.
    """
    import inspect
    from src.api import dashboard
    src = inspect.getsource(dashboard)
    idx_branch = src.find('_v2_result.record_type == "rfq"')
    assert idx_branch != -1, "V2 success branch sentinel missing"
    # Window from the branch sentinel to the next `return _new_rfq`
    rest = src[idx_branch:]
    idx_return = rest.find("return _new_rfq")
    assert idx_return != -1, "branch return statement missing"
    branch_body = rest[:idx_return]
    assert "_trigger_auto_price(_new_rfq)" not in branch_body, (
        "_trigger_auto_price re-introduced in V2 success branch — "
        "this is the substrate-singleness regression that spawned 5 "
        "ghost auto_rfq_* PCs on prod 2026-05-26. RFQ-class records "
        "live in rfqs only; PCs are for the early-detect path."
    )


def test_legacy_process_rfq_email_tail_does_NOT_call_trigger_auto_price():
    """Same guard for the legacy tail callsite at the end of
    `process_rfq_email`. The function-end window between the F10
    auto-price block's closing `except Exception as _ap_e:` and the
    final `return rfq_data` must not contain `_trigger_auto_price(rfq_data)`.
    """
    import inspect
    from src.api import dashboard
    src = inspect.getsource(dashboard.process_rfq_email)
    # The function body itself shouldn't contain the line. (It also
    # shouldn't contain it on any path — process_rfq_email is the
    # RFQ-class entry point; calling auto-price here always doubles.)
    assert "_trigger_auto_price(rfq_data)" not in src, (
        "_trigger_auto_price(rfq_data) re-introduced in process_rfq_email — "
        "this is the substrate-singleness regression. process_rfq_email "
        "handles RFQ-class records only; spawning a PC here doubles "
        "every inbound on /home (PC panel + RFQ panel)."
    )
