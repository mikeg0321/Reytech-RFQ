"""Pin: POST /spine/quotes/<quote_id>/carry-forward-prices wires the
pure carry_forward_costs auto-pricer to an operator action.

Chrome MCP audit 2026-05-26 next-window priority: shortens per-ship
time on the 3 overdue Job #1 RFQs by letting the operator one-click
the validated-cost carry from a linked PC instead of re-typing.

Tests pin:
  1. 404 when target quote doesn't exist.
  2. 409 + error="no_linked_pc" when target has no quote_links.
  3. Auto-picks highest-confidence link when from_pc_id omitted.
  4. Honors explicit from_pc_id when provided.
  5. wrote=False + summary when no lines need carry (idempotent on
     already-priced target).
  6. wrote=True + summary on actual carry; line cost on the persisted
     quote matches the source's cost_cents.
  7. self-link refused (400).
"""
from __future__ import annotations

from datetime import datetime, timezone


def _make_quote(quote_id, line_items):
    """Build a minimal Quote model + persist via write_quote."""
    from src.spine.model import Quote, LineItem
    lis = []
    for li in line_items:
        lis.append(LineItem(
            line_no=li["line_no"],
            description=li["description"],
            mfg_number=li.get("mfg_number"),
            qty=li.get("qty", li.get("quantity", 1)),
            uom=li.get("uom", "EA"),
            cost_cents=li.get("cost_cents", 0),
            unit_price_cents=li.get("unit_price_cents", 0),
            cost_source_url=li.get("cost_source_url"),
            cost_validated_at=li.get("cost_validated_at"),
            cost_hand_validated_note=li.get("cost_hand_validated_note"),
        ))
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="CHCF",
        solicitation_number="TEST-SOL-1",
        tax_rate_bps=775,
        line_items=lis,
        status="parsed",
        created_at=datetime.now(timezone.utc),
        updated_at=datetime.now(timezone.utc),
    )


def _seed(db_path, quote_id, line_items, actor="test"):
    from src.spine.db import write_quote, init_db
    init_db(db_path)
    q = _make_quote(quote_id, line_items)
    return write_quote(db_path, q, actor=actor)


def _link(db_path, from_id, to_id, confidence=0.95):
    from src.spine.db import write_quote_link
    return write_quote_link(
        db_path, from_quote_id=from_id, to_quote_id=to_id,
        match_method="test_link", confidence=confidence,
        evidence={}, actor="test",
    )


def _client(db_path, monkeypatch):
    """Build a Flask test client wired to the spine blueprint with
    db_path pointed at our isolated tmp SQLite. Returns (app, client)."""
    from flask import Flask
    from src.api.modules.routes_spine import make_spine_blueprint
    app = Flask(__name__)
    app.config["TESTING"] = True
    bp = make_spine_blueprint(db_path)
    app.register_blueprint(bp)
    return app, app.test_client()


# ─── 404 + 409 paths ─────────────────────────────────────────────────


def test_404_when_target_quote_missing(tmp_path, monkeypatch):
    db = str(tmp_path / "spine.db")
    from src.spine.db import init_db
    init_db(db)
    _, c = _client(db, monkeypatch)
    r = c.post("/spine/quotes/q-does-not-exist/carry-forward-prices",
               json={})
    assert r.status_code == 404
    data = r.get_json()
    assert data["ok"] is False
    assert data["error"] == "not_found"


def test_409_when_no_linked_pc(tmp_path, monkeypatch):
    db = str(tmp_path / "spine.db")
    _seed(db, "rfq-1", [{"line_no": 1, "description": "x", "mfg_number": "M1"}])
    _, c = _client(db, monkeypatch)

    r = c.post("/spine/quotes/rfq-1/carry-forward-prices", json={})
    assert r.status_code == 409
    data = r.get_json()
    assert data["ok"] is False
    assert data["error"] == "no_linked_pc"


def test_400_when_self_link_explicit(tmp_path, monkeypatch):
    """Explicit from_pc_id == quote_id is refused before any read."""
    db = str(tmp_path / "spine.db")
    _seed(db, "rfq-2", [{"line_no": 1, "description": "x", "mfg_number": "M1"}])
    _, c = _client(db, monkeypatch)

    r = c.post("/spine/quotes/rfq-2/carry-forward-prices",
               json={"from_pc_id": "rfq-2"})
    assert r.status_code == 400
    data = r.get_json()
    assert data["error"] == "self_link"


# ─── Happy path: auto-pick + explicit ────────────────────────────────


def test_auto_picks_highest_confidence_link(tmp_path, monkeypatch):
    """When body omits from_pc_id, the endpoint reads find_links_from
    and uses the top-confidence entry. Higher confidence wins."""
    db = str(tmp_path / "spine.db")
    # Two PCs as potential sources.
    _seed(db, "pc-high", [{
        "line_no": 1, "description": "gloves", "mfg_number": "GLV-1",
        "cost_cents": 7500, "cost_validated_at": datetime.now(timezone.utc),
    }])
    _seed(db, "pc-low", [{
        "line_no": 1, "description": "gloves", "mfg_number": "GLV-1",
        "cost_cents": 5000, "cost_validated_at": datetime.now(timezone.utc),
    }])
    _seed(db, "rfq-target", [
        {"line_no": 1, "description": "gloves", "mfg_number": "GLV-1"},
    ])
    _link(db, "rfq-target", "pc-low", confidence=0.50)
    _link(db, "rfq-target", "pc-high", confidence=0.99)

    _, c = _client(db, monkeypatch)
    r = c.post("/spine/quotes/rfq-target/carry-forward-prices", json={})
    assert r.status_code == 200, r.get_json()
    data = r.get_json()
    assert data["ok"] is True
    # pc-high (confidence 0.99) wins over pc-low.
    assert data["source_quote_id"] == "pc-high"
    # And the cost carried is 7500 (from pc-high), not 5000.
    assert data["wrote"] is True
    assert data["summary"]["carried"][0]["cost_cents"] == 7500


def test_honors_explicit_from_pc_id(tmp_path, monkeypatch):
    """Explicit from_pc_id overrides the auto-pick."""
    db = str(tmp_path / "spine.db")
    _seed(db, "pc-A", [{
        "line_no": 1, "description": "widget", "mfg_number": "W-1",
        "cost_cents": 4000, "cost_validated_at": datetime.now(timezone.utc),
    }])
    _seed(db, "pc-B", [{
        "line_no": 1, "description": "widget", "mfg_number": "W-1",
        "cost_cents": 6500, "cost_validated_at": datetime.now(timezone.utc),
    }])
    _seed(db, "rfq-X", [{"line_no": 1, "description": "widget",
                         "mfg_number": "W-1"}])
    # Only pc-A is linked at high confidence; caller asks for pc-B.
    _link(db, "rfq-X", "pc-A", confidence=0.99)

    _, c = _client(db, monkeypatch)
    r = c.post("/spine/quotes/rfq-X/carry-forward-prices",
               json={"from_pc_id": "pc-B"})
    assert r.status_code == 200
    data = r.get_json()
    assert data["source_quote_id"] == "pc-B"
    assert data["summary"]["carried"][0]["cost_cents"] == 6500


# ─── No-op + persistence checks ──────────────────────────────────────


def test_no_write_when_nothing_to_carry(tmp_path, monkeypatch):
    """Target already priced + source MFG# doesn't match → carried=[]
    → wrote=False so we don't touch the event log for a no-op."""
    db = str(tmp_path / "spine.db")
    _seed(db, "pc-src", [{
        "line_no": 1, "description": "scope", "mfg_number": "DIFFERENT",
        "cost_cents": 1000, "cost_validated_at": datetime.now(timezone.utc),
    }])
    _seed(db, "rfq-priced", [
        {"line_no": 1, "description": "scope", "mfg_number": "ALREADY-PRICED",
         "cost_cents": 9999,
         "cost_validated_at": datetime.now(timezone.utc)},
    ])
    _link(db, "rfq-priced", "pc-src", confidence=0.9)

    _, c = _client(db, monkeypatch)
    r = c.post("/spine/quotes/rfq-priced/carry-forward-prices", json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert data["wrote"] is False
    assert data["summary"]["carried"] == []


def test_writes_persist_to_db(tmp_path, monkeypatch):
    """After carry+write, re-reading the quote shows the carried cost
    on the target line — proves the write went through, not just the
    summary."""
    db = str(tmp_path / "spine.db")
    _seed(db, "pc-src2", [{
        "line_no": 1, "description": "swab", "mfg_number": "SW-9",
        "cost_cents": 350, "cost_validated_at": datetime.now(timezone.utc),
    }])
    _seed(db, "rfq-tgt2", [
        {"line_no": 1, "description": "swab", "mfg_number": "SW-9"},
    ])
    _link(db, "rfq-tgt2", "pc-src2", confidence=0.99)

    _, c = _client(db, monkeypatch)
    r = c.post("/spine/quotes/rfq-tgt2/carry-forward-prices", json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data["wrote"] is True

    # Re-read the target and confirm cost landed.
    from src.spine.db import read_quote
    persisted = read_quote(db, "rfq-tgt2")
    assert persisted is not None
    assert persisted.line_items[0].cost_cents == 350
