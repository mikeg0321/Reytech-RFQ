"""Tests for the oracle-suggestions endpoint + fixture proxy.

Oracle SUGGESTS, operator DECIDES, substrate STORES only operator-
typed values. These tests prove:

1. The endpoint returns the documented JSON shape.
2. The shape is stable across multiple quote shapes (1 line, many lines).
3. The fixture proxy never touches the spine_quotes table.
4. The substrate has no oracle_suggested_* fields (architectural test).

PR-O4 will swap the proxy body to call the real parent-repo oracle;
the JSON contract proved by these tests is the surface that swap
must honor.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import LineItem, Quote, QuoteStatus, init_db, write_quote


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _line(n: int = 1, **kw) -> LineItem:
    base = dict(
        line_no=n,
        description=f"oracle item {n}",
        mfg_number=f"MFG-{n:03d}",
        qty=2,
        uom="EA",
        cost_cents=5000,
        cost_source_url="https://supplier.example.com/sku",
        cost_validated_at=_fresh_ts(),
        unit_price_cents=6750,
    )
    base.update(kw)
    return LineItem(**base)


def _quote(quote_id: str = "Q-oracle-001", n_lines: int = 1) -> Quote:
    return Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="oracle test",
        solicitation_number="ORACLE1",
        line_items=[_line(i + 1) for i in range(n_lines)],
        tax_rate_bps=775,
        status=QuoteStatus.PARSED,
    )


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_oracle.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    bp = make_spine_blueprint(db_path, auth_decorator=None)
    app.register_blueprint(bp)
    return app.test_client()


def test_oracle_endpoint_returns_404_for_unknown_quote(client):
    r = client.get("/spine/quotes/Q-no-such-thing/oracle-suggestions")
    assert r.status_code == 404


def test_oracle_endpoint_returns_documented_shape(client, db_path):
    q = _quote("Q-oracle-shape", n_lines=3)
    write_quote(db_path, q, actor="seed")
    r = client.get("/spine/quotes/Q-oracle-shape/oracle-suggestions")
    assert r.status_code == 200
    body = r.json
    # Top-level
    assert body["quote_id"] == "Q-oracle-shape"
    assert "oracle_version" in body
    assert "generated_at" in body
    assert isinstance(body["lines"], list)
    assert len(body["lines"]) == 3
    # Per-line shape
    line = body["lines"][0]
    required = {
        "line_no", "suggested_cost_cents", "suggested_unit_price_cents",
        "price_basis", "competitor_unit_price_cents", "competitor_vendor",
        "competitor_delta_pct", "confidence", "freshness_days",
        "cost_sources",
    }
    assert required.issubset(line.keys()), f"missing: {required - line.keys()}"
    # Confidence values are constrained
    for ln in body["lines"]:
        assert ln["confidence"] in ("high", "medium", "low")
        assert ln["freshness_days"] >= 0


def test_oracle_endpoint_does_not_mutate_substrate(client, db_path):
    """The endpoint is read-only. Calling it must not write any rows
    to spine_quotes or spine_quote_snapshots."""
    import sqlite3
    q = _quote("Q-oracle-readonly")
    write_quote(db_path, q, actor="seed")

    conn = sqlite3.connect(db_path)
    quotes_before = conn.execute("SELECT updated_at FROM spine_quotes WHERE quote_id=?",
                                   ("Q-oracle-readonly",)).fetchone()[0]
    snaps_before = conn.execute("SELECT COUNT(*) FROM spine_quote_snapshots").fetchone()[0]
    conn.close()

    # Call the endpoint 5 times to be sure.
    for _ in range(5):
        r = client.get("/spine/quotes/Q-oracle-readonly/oracle-suggestions")
        assert r.status_code == 200

    conn = sqlite3.connect(db_path)
    quotes_after = conn.execute("SELECT updated_at FROM spine_quotes WHERE quote_id=?",
                                  ("Q-oracle-readonly",)).fetchone()[0]
    snaps_after = conn.execute("SELECT COUNT(*) FROM spine_quote_snapshots").fetchone()[0]
    conn.close()
    assert quotes_after == quotes_before, "endpoint must not mutate spine_quotes"
    assert snaps_after == snaps_before == 0, "endpoint must not write snapshots"


def test_oracle_suggestions_are_deterministic_for_same_quote_shape(client, db_path):
    """Fixture proxy is deterministic so the UI's 'Refresh' button
    doesn't flicker values randomly. PR-O4 (real oracle) will replace
    this with cached-with-freshness; the determinism property is
    preserved across the swap by spec."""
    q = _quote("Q-oracle-det")
    write_quote(db_path, q, actor="seed")
    r1 = client.get("/spine/quotes/Q-oracle-det/oracle-suggestions").json
    r2 = client.get("/spine/quotes/Q-oracle-det/oracle-suggestions").json
    # generated_at differs by call time, so compare the line array.
    assert r1["lines"] == r2["lines"]


def test_oracle_no_substrate_fields_named_oracle():
    """Architectural test: no Spine model field name contains 'oracle'.
    Oracle is a SUGGESTER; the substrate has no representation of
    oracle state. Any future field named oracle_* on Quote or LineItem
    is a substrate violation.
    """
    from src.spine.model import Quote, LineItem
    for model in (Quote, LineItem):
        for field_name in model.model_fields.keys():
            assert "oracle" not in field_name.lower(), (
                f"{model.__name__}.{field_name}: oracle state must not "
                "be persisted in the Spine substrate; oracle is a "
                "read-only suggester."
            )
