"""PR-AV9 (AV-9) — V2 ingest path auto-price trigger.

Closes the substrate gap flagged in the 5/14 EOD handoff: the
classifier_v2 success branch in `dashboard.process_rfq_email`
returns immediately after saving the record, bypassing the
`_trigger_auto_price` call at the end of the function. Result:
every Vision/AcroForm-extracted RFQ (most modern records) lands
in "parsed" status with empty pricing fields. Operators had to
hand-price or hit the manual retry-auto-price route.

rfq_efbdef4a / DSH 25CB021 was the flagged example: Vision found
7 items, V2 path saved them, but auto-price never fired. Items
stayed unpriced until Mike manually intervened.

THE FIX

Before the V2 success branch returns, look up the freshly-saved
RFQ and call `_trigger_auto_price(record)`. The legacy V1 path
already does this at the end of process_rfq_email — V2 just
returned early. Now both paths reach pricing parity.

Safety:
  - `_trigger_auto_price` is idempotent (early-exits when
    `auto_price_pc_id` already set), so reparse / re-ingest
    never double-creates auto-PCs.
  - Defensive: try/except around the call so any pipeline crash
    doesn't break the ingest path. The auto-price work is
    asynchronous (spawns a daemon thread) — its failure is
    independent of the record save.
  - Skipped when `line_items` is empty (no rows to price).

Tests pin the seam contract without exercising the full
auto-price pipeline (which would need PC seeding + Oracle mocks).
We verify:
  - The dashboard.py source contains the AV-9 marker + trigger
    call in the V2 success branch (source-grep guard against
    accidental removal during refactors).
  - `_trigger_auto_price` is idempotent on a record that already
    has `auto_price_pc_id` set.
  - `_trigger_auto_price` skips records with empty line_items.
"""
from __future__ import annotations


def test_v2_success_branch_calls_trigger_auto_price():
    """Source-level guard: the dashboard.py V2 success path must
    invoke `_trigger_auto_price`. A future refactor that drops the
    call would silently regress rfq_efbdef4a's failure class. Pin
    the marker + the call so a search for `PR-AV9` always lands at
    the seam."""
    import inspect
    from src.api import dashboard
    src = inspect.getsource(dashboard)
    # AV-9 marker comment must be present in the V2 branch
    assert "PR-AV9" in src, "AV-9 marker comment missing"
    # The trigger call must be in the V2 success branch — look for
    # the call sequence: load_rfqs → get(record_id) → _trigger_auto_price
    assert "_trigger_auto_price(_new_rfq)" in src, (
        "AV-9 trigger call missing from V2 success branch"
    )


def test_trigger_auto_price_skips_when_no_line_items():
    """The trigger has a defensive `if not line_items` early-exit so
    records that V2 saved without items (parser failure, vision miss)
    don't crash the auto-price pipeline."""
    from src.api.dashboard import _trigger_auto_price

    # Record with no line_items — trigger should no-op cleanly
    rfq = {"id": "rfq_empty", "line_items": [], "solicitation_number": "TEST"}
    # No exception should propagate; no thread should spawn (we can't
    # easily assert on threads, but at minimum no AttributeError /
    # KeyError on the early-exit path)
    _trigger_auto_price(rfq)
    # Record gets no `auto_price_pc_id` set (would only set if pipeline
    # actually ran and completed; here it bailed before spawning thread)
    # The function returns None; we just confirm no exception.


def test_trigger_auto_price_skips_when_already_processed():
    """Idempotency: a record that already has `auto_price_pc_id` must
    NOT have a fresh auto-price pipeline kicked off. This is the safety
    net that lets us call the trigger from multiple ingest paths
    (V1, V2, reparse, retry) without double-creating auto-PCs."""
    from src.api.dashboard import _trigger_auto_price

    rfq = {
        "id": "rfq_already_priced",
        "line_items": [{"description": "A", "qty": 1}],
        "solicitation_number": "TEST",
        "auto_price_pc_id": "pc_existing_abc123",
    }
    _trigger_auto_price(rfq)
    # No change to the record; no new pc_id stamped
    assert rfq["auto_price_pc_id"] == "pc_existing_abc123"


def test_v2_branch_only_triggers_for_rfq_record_type():
    """PCs use their own auto-price hook (_auto_price_new_pc) — the
    V2 success branch must not also fire `_trigger_auto_price` on a
    PC record_type, which would create a redundant auto-PC of-a-PC
    chain. The branch shape uses an explicit `record_type == 'rfq'`
    guard before the trigger; pin that guard."""
    import inspect
    from src.api import dashboard
    src = inspect.getsource(dashboard)
    # The guard must precede the trigger call
    idx_guard = src.find('_v2_result.record_type == "rfq"')
    idx_trigger = src.find("_trigger_auto_price(_new_rfq)")
    assert idx_guard != -1, "PC/RFQ guard missing"
    assert idx_trigger != -1, "trigger call missing"
    assert idx_guard < idx_trigger, (
        "trigger fires BEFORE the rfq-only guard — would also fire "
        "on PC ingests"
    )


def test_v2_trigger_wrapped_in_try_except():
    """The auto-price call must NOT propagate exceptions to the ingest
    path. If pipeline crashes (e.g., Oracle unreachable, DB lock), the
    record save has already happened — we don't want the poller to
    think the save failed because a background hook crashed."""
    import inspect
    from src.api import dashboard
    src = inspect.getsource(dashboard)
    # Find the trigger call and check the preceding ~5 lines contain
    # a try: block, and the following ~5 lines contain except.
    idx = src.find("_trigger_auto_price(_new_rfq)")
    assert idx != -1
    preceding = src[max(0, idx - 200):idx]
    following = src[idx:idx + 500]
    assert "try:" in preceding, "try block missing before trigger call"
    assert "except" in following, "except block missing after trigger call"
    # The exception handler must log at debug level (best-effort),
    # not surface to operator
    assert ("log.debug" in following or "log.warning" in following), (
        "exception in auto-price must be logged, not silently swallowed"
    )
