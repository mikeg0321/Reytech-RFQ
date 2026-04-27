"""Tests for `_build_recent_quotes_cost_source_card` — the /health/quoting
card that surfaces pricing-pipeline health by chipping each recent quote's
items[].pricing.cost_source mix.

Plan §4.3 sub-3 lever: pairs with the §4.1 KPI surface. Operator quote
SEND telemetry (operator_quote_sent, PR #608) tells us "how fast"; this
card tells us "from where" — the cost source of each item that went out.
A healthy quote is operator/catalog dominant. A sick pipeline shows up as
amazon/scprs reliance or needs_lookup gaps.

The card joins:
  operator_quote_sent (KPI table, rows-of-truth for sent quotes)
  → source PC (price_checks.items[].pricing.cost_source) or
    source RFQ (rfqs.items[].pricing.cost_source).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest


def _build(limit=5):
    from src.api.modules.routes_health import _build_recent_quotes_cost_source_card
    return _build_recent_quotes_cost_source_card(limit=limit)


def _ensure_kpi_table(conn):
    """The conftest's init_db() runs the SCHEMA block but not the
    migrations module. operator_quote_sent lives in migration #34, so
    create it inline for tests."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS operator_quote_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id TEXT NOT NULL,
            quote_type TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            started_at TEXT,
            time_to_send_seconds INTEGER,
            item_count INTEGER DEFAULT 0,
            agency_key TEXT,
            quote_total REAL DEFAULT 0
        )
    """)


def _seed_kpi_row(conn, quote_id: str, quote_type: str, sent_at: str,
                  item_count: int = 1, agency_key: str = "CDCR",
                  quote_total: float = 100.0):
    conn.execute("""
        INSERT INTO operator_quote_sent
        (quote_id, quote_type, sent_at, started_at,
         time_to_send_seconds, item_count, agency_key, quote_total)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (quote_id, quote_type, sent_at, sent_at, 30,
          item_count, agency_key, quote_total))


def _seed_pc(conn, pc_id: str, items: list):
    """Insert a PC row whose items[].pricing.cost_source is what the card reads.
    Mirror of the get_pc() data shape — the items column carries the
    JSON list directly."""
    pc_data = {"items": items}
    conn.execute("""
        INSERT INTO price_checks (id, created_at, status, items, pc_data)
        VALUES (?, ?, ?, ?, ?)
    """, (pc_id, datetime.now().isoformat(), "sent",
          json.dumps(items), json.dumps(pc_data)))


def _seed_rfq(conn, rfq_id: str, items: list):
    """Insert into rfqs (uses received_at not created_at; data_json carries
    the items[] like get_rfq() expects)."""
    data = {"items": items}
    conn.execute("""
        INSERT INTO rfqs (id, received_at, status, items, data_json)
        VALUES (?, ?, ?, ?, ?)
    """, (rfq_id, datetime.now().isoformat(), "sent",
          json.dumps(items), json.dumps(data)))


def _wipe(conn):
    """Reset tables this test cares about so leftover seeds from conftest
    don't interfere. Ensure the KPI table exists (not in SCHEMA, lives in
    migration #34)."""
    _ensure_kpi_table(conn)
    for tbl in ("operator_quote_sent", "price_checks", "rfqs"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _iso_ago(seconds: int) -> str:
    return (datetime.now() - timedelta(seconds=seconds)).isoformat()


# ── Empty / missing data ────────────────────────────────────────────────


def test_returns_empty_quotes_when_no_kpi_rows():
    from src.core.db import get_db
    with get_db() as conn:
        _wipe(conn)
    out = _build()
    assert out["ok"] is True
    assert out["quotes"] == []
    assert out["totals"] == {
        "operator": 0, "catalog": 0, "amazon": 0,
        "scprs": 0, "needs_lookup": 0, "unknown": 0,
    }


def test_marks_missing_source_when_pc_not_found():
    """KPI row exists but the source PC was deleted — card must show the
    quote with a missing_source flag rather than crashing or hiding it."""
    from src.core.db import get_db
    with get_db() as conn:
        _wipe(conn)
        _seed_kpi_row(conn, "ghost-pc-id", "pc", _iso_ago(60), item_count=3)
        conn.commit()
    out = _build()
    assert len(out["quotes"]) == 1
    q = out["quotes"][0]
    assert q["missing_source"] is True
    assert q["item_count"] == 3
    assert sum(q["chips"].values()) == 0


# ── Per-quote chip aggregation ──────────────────────────────────────────


def test_chips_aggregate_cost_source_buckets_for_a_pc():
    from src.core.db import get_db
    with get_db() as conn:
        _wipe(conn)
        _seed_pc(conn, "pc1", [
            {"description": "A", "pricing": {"cost_source": "operator"}},
            {"description": "B", "pricing": {"cost_source": "catalog"}},
            {"description": "C", "pricing": {"cost_source": "catalog_confirmed"}},
            {"description": "D", "pricing": {"cost_source": "amazon_scrape"}},
            {"description": "E", "pricing": {"cost_source": "scprs"}},
            {"description": "F", "pricing": {"cost_source": "needs_lookup"}},
        ])
        _seed_kpi_row(conn, "pc1", "pc", _iso_ago(60), item_count=6)
        conn.commit()
    out = _build()
    assert len(out["quotes"]) == 1
    q = out["quotes"][0]
    assert q["chips"] == {
        "operator": 1, "catalog": 2, "amazon": 1,
        "scprs": 1, "needs_lookup": 1, "unknown": 0,
    }


def test_unknown_bucket_catches_new_pipeline_values():
    """A future scraper might write a cost_source we don't know about.
    The card must NOT silently drop the item — bucket it as 'unknown'
    so an unrecognized value shows up as a grey chip."""
    from src.core.db import get_db
    with get_db() as conn:
        _wipe(conn)
        _seed_pc(conn, "pc-future", [
            {"description": "X", "pricing": {"cost_source": "some_new_scraper"}},
            {"description": "Y", "pricing": {"cost_source": ""}},
        ])
        _seed_kpi_row(conn, "pc-future", "pc", _iso_ago(60), item_count=2)
        conn.commit()
    out = _build()
    chips = out["quotes"][0]["chips"]
    assert chips["unknown"] == 1
    assert chips["needs_lookup"] == 1  # empty string falls back to needs_lookup


def test_handles_rfq_quote_type():
    from src.core.db import get_db
    with get_db() as conn:
        _wipe(conn)
        _seed_rfq(conn, "rfq1", [
            {"description": "A", "pricing": {"cost_source": "catalog"}},
            {"description": "B", "pricing": {"cost_source": "operator"}},
        ])
        _seed_kpi_row(conn, "rfq1", "rfq", _iso_ago(60), item_count=2)
        conn.commit()
    out = _build()
    assert len(out["quotes"]) == 1
    q = out["quotes"][0]
    assert q["quote_type"] == "rfq"
    assert q["chips"]["catalog"] == 1
    assert q["chips"]["operator"] == 1


def test_handles_missing_pricing_field_on_item():
    """Legacy items may lack a 'pricing' subdict entirely. They should land
    in 'needs_lookup' rather than crashing the card or being silently
    dropped."""
    from src.core.db import get_db
    with get_db() as conn:
        _wipe(conn)
        _seed_pc(conn, "pc-legacy", [
            {"description": "A"},  # no pricing
            {"description": "B", "pricing": {}},  # empty pricing
            {"description": "C", "pricing": None},
        ])
        _seed_kpi_row(conn, "pc-legacy", "pc", _iso_ago(60), item_count=3)
        conn.commit()
    out = _build()
    assert out["quotes"][0]["chips"]["needs_lookup"] == 3


# ── Limit + ordering ────────────────────────────────────────────────────


def test_limit_applies_with_most_recent_first():
    from src.core.db import get_db
    with get_db() as conn:
        _wipe(conn)
        # Seed 7 quotes, each 1 item operator-typed
        for i in range(7):
            _seed_pc(conn, f"pc{i}", [
                {"description": "A", "pricing": {"cost_source": "operator"}},
            ])
            _seed_kpi_row(conn, f"pc{i}", "pc",
                          _iso_ago(seconds=(7 - i) * 60), item_count=1)
        conn.commit()
    out = _build(limit=5)
    assert len(out["quotes"]) == 5
    # Most recent (pc6, 60s ago) first; pc2 (5*60s ago) last
    assert out["quotes"][0]["quote_id"] == "pc6"
    assert out["quotes"][-1]["quote_id"] == "pc2"


def test_totals_sum_across_returned_quotes():
    from src.core.db import get_db
    with get_db() as conn:
        _wipe(conn)
        _seed_pc(conn, "pc1", [
            {"description": "A", "pricing": {"cost_source": "operator"}},
            {"description": "B", "pricing": {"cost_source": "catalog"}},
        ])
        _seed_pc(conn, "pc2", [
            {"description": "C", "pricing": {"cost_source": "amazon"}},
            {"description": "D", "pricing": {"cost_source": "amazon"}},
            {"description": "E", "pricing": {"cost_source": "needs_lookup"}},
        ])
        _seed_kpi_row(conn, "pc1", "pc", _iso_ago(60), item_count=2)
        _seed_kpi_row(conn, "pc2", "pc", _iso_ago(120), item_count=3)
        conn.commit()
    out = _build()
    assert out["totals"]["operator"] == 1
    assert out["totals"]["catalog"] == 1
    assert out["totals"]["amazon"] == 2
    assert out["totals"]["needs_lookup"] == 1


# ── Schema tolerance ────────────────────────────────────────────────────


def test_safe_default_when_db_query_raises(monkeypatch):
    from src.core import db as _db
    class _Boom:
        def __enter__(self): raise RuntimeError("db unavailable")
        def __exit__(self, *a): return False
    monkeypatch.setattr(_db, "get_db", lambda: _Boom())
    out = _build()
    assert out["ok"] is False
    assert out["quotes"] == []


# ── Bucket helper ───────────────────────────────────────────────────────


def test_bucket_helper_normalizes_aliases():
    from src.api.modules.routes_health import _bucket_cost_source
    assert _bucket_cost_source("operator") == "operator"
    assert _bucket_cost_source("OPERATOR") == "operator"
    assert _bucket_cost_source("catalog") == "catalog"
    assert _bucket_cost_source("catalog_confirmed") == "catalog"
    assert _bucket_cost_source("amazon_scrape") == "amazon"
    assert _bucket_cost_source("scprs_scrape") == "scprs"
    assert _bucket_cost_source("legacy_unknown") == "needs_lookup"
    assert _bucket_cost_source(None) == "needs_lookup"
    assert _bucket_cost_source("") == "needs_lookup"
    assert _bucket_cost_source("totally-new-source") == "unknown"
