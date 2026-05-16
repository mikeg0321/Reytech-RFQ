"""The Spine — DB layer behavior tests.

Covers atomic write, append-only event log, single-writer invariant
(enforced by test_spine_architecture), and the terminal-sent guard.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.spine.db import (
    init_db,
    read_quote,
    read_event_log,
    iter_quote_ids,
    write_quote,
)
from src.spine.model import (
    Quote,
    LineItem,
    QuoteStatus,
    SpineValidationError,
)


def _line(line_no: int = 1, **overrides) -> LineItem:
    base = dict(
        line_no=line_no,
        description=f"item {line_no}",
        qty=2,
        uom="EA",
        cost_cents=5000,
        cost_source_url="https://supplier.example.com/sku",
        cost_validated_at=datetime.now(timezone.utc) - timedelta(days=1),
        unit_price_cents=6750,
    )
    base.update(overrides)
    return LineItem(**base)


def _quote(quote_id: str = "Q-db-001", *, status: QuoteStatus = QuoteStatus.PARSED) -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="SATF",
        solicitation_number="10847262",
        line_items=[_line(1), _line(2)],
        tax_rate_bps=825,
        status=status,
    )


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    p = tmp_path / "spine_test.db"
    init_db(p)
    return p


def test_init_db_is_idempotent(tmp_path):
    p = tmp_path / "spine.db"
    init_db(p)
    init_db(p)
    init_db(p)
    # No exception = pass.


def test_write_and_read_round_trip(db_path):
    q = _quote()
    write_quote(db_path, q, actor="operator")
    loaded = read_quote(db_path, q.quote_id)
    assert loaded is not None
    assert loaded.quote_id == q.quote_id
    assert loaded.subtotal_cents == q.subtotal_cents
    assert loaded.line_items[0].unit_price_cents == q.line_items[0].unit_price_cents


def test_read_missing_returns_none(db_path):
    assert read_quote(db_path, "nonexistent") is None


def test_event_log_appends_on_every_write(db_path):
    q = _quote()
    write_quote(db_path, q, actor="ingest", note="initial parse")
    q2 = q.with_status(QuoteStatus.PRICED)
    write_quote(db_path, q2, actor="operator", note="priced")
    q3 = q2.with_status(QuoteStatus.FINALIZED)
    write_quote(db_path, q3, actor="operator", note="finalized")

    log = read_event_log(db_path, q.quote_id)
    assert len(log) == 3
    assert log[0]["status"] == "parsed"
    assert log[0]["note"] == "initial parse"
    assert log[1]["status"] == "priced"
    assert log[2]["status"] == "finalized"
    assert log[2]["actor"] == "operator"


def test_write_requires_non_empty_actor(db_path):
    q = _quote()
    with pytest.raises(SpineValidationError, match="actor"):
        write_quote(db_path, q, actor="")
    with pytest.raises(SpineValidationError, match="actor"):
        write_quote(db_path, q, actor="   ")


def test_sent_quotes_are_immutable(db_path):
    items = [_line(1)]
    q = Quote(
        quote_id="Q-sent-001",
        agency="CCHCS",
        facility="SATF",
        solicitation_number="X",
        line_items=items,
        tax_rate_bps=825,
        status=QuoteStatus.SENT,
    )
    write_quote(db_path, q, actor="operator")
    with pytest.raises(SpineValidationError, match="terminal"):
        write_quote(db_path, q, actor="operator", note="trying to mutate sent")


def test_iter_quote_ids_returns_recent_first(db_path):
    import time

    write_quote(db_path, _quote("Q-001"), actor="ingest")
    time.sleep(0.01)
    write_quote(db_path, _quote("Q-002"), actor="ingest")
    time.sleep(0.01)
    write_quote(db_path, _quote("Q-003"), actor="ingest")

    ids = list(iter_quote_ids(db_path))
    assert ids == ["Q-003", "Q-002", "Q-001"]


def test_validation_on_load(db_path):
    """A row in the DB that no longer satisfies the model must raise."""
    import json
    import sqlite3

    # Manually write a malformed state to bypass the writer.
    bad = {
        "quote_id": "Q-bad",
        "agency": "CCHCS",
        "facility": "SATF",
        "solicitation_number": "x",
        "line_items": [],  # min_length=1 violated.
        "tax_rate_bps": 825,
        "status": "parsed",
        "created_at": "2026-05-15T00:00:00+00:00",
        "updated_at": "2026-05-15T00:00:00+00:00",
    }
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO spine_quotes (quote_id, state_json, event_log, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ("Q-bad", json.dumps(bad), json.dumps([]), "x", "x"),
    )
    conn.commit()
    conn.close()

    with pytest.raises(Exception):  # ValidationError from Pydantic.
        read_quote(db_path, "Q-bad")


def test_computed_fields_not_persisted_in_state_json(db_path):
    """subtotal_cents / tax_cents / total_cents must not appear in
    the persisted JSON. They are derived on every read, never stored.

    This closes the silent-mutation class at the persistence layer:
    the on-disk state cannot diverge from the model's computed values
    because the computed values are not on disk.
    """
    import json
    import sqlite3

    q = _quote()
    write_quote(db_path, q, actor="operator")

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT state_json FROM spine_quotes WHERE quote_id = ?", (q.quote_id,)
    ).fetchone()
    conn.close()

    state = json.loads(row[0])
    # None of the computed fields are in the persisted JSON.
    assert "subtotal_cents" not in state
    assert "tax_cents" not in state
    assert "total_cents" not in state
    for li in state["line_items"]:
        assert "extension_cents" not in li
        assert "markup_pct_display" not in li


def test_hand_injected_computed_field_in_db_raises_on_load(db_path):
    """If someone hand-edits the stored JSON to inject a fake computed
    field, the next load RAISES via extra='forbid'.

    This is the stricter consequence of the no-persist rule: the model
    refuses to load a tampered row. Better to raise loudly than to
    silently ignore the lie.
    """
    import json
    import sqlite3

    q = _quote()
    write_quote(db_path, q, actor="operator")

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT state_json FROM spine_quotes WHERE quote_id = ?", (q.quote_id,)
    ).fetchone()
    state = json.loads(row[0])
    state["subtotal_cents"] = 999_999  # hand-inject a lie.
    conn.execute(
        "UPDATE spine_quotes SET state_json = ? WHERE quote_id = ?",
        (json.dumps(state), q.quote_id),
    )
    conn.commit()
    conn.close()

    with pytest.raises(Exception):  # ValidationError — extra forbidden.
        read_quote(db_path, q.quote_id)
