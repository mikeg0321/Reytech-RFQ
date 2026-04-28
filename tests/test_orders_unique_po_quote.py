"""Tests for the partial UNIQUE index on orders(po_number, quote_number)
+ the duplicate_orders surface card.

This is S3-prep PR-2 corrected scope. The original plan (UNIQUE on
po_number alone) would have failed against legitimate multi-quote
POs verified on prod via PR #632. The corrected constraint is
compound + partial:

    CREATE UNIQUE INDEX idx_orders_po_quote
    ON orders(po_number, quote_number)
    WHERE po_number != '' AND quote_number != ''

  - Compound: same po_number with DIFFERENT quote_numbers stays
    legal (one buyer PO covering N awarded quotes — verified real
    on prod, e.g., PO 0000053217 spans 7 quotes).
  - Partial: empty/NULL values don't trigger the constraint, so
    "no PO yet" rows can co-exist freely until the buyer PO
    email arrives.

These tests lock:
  - The migration creates the index when no offenders exist
  - The migration is idempotent (re-running is a no-op)
  - INSERT of a true duplicate raises IntegrityError once the
    index is live
  - Different quote_numbers under the same po_number stay legal
  - Empty po_number repeats are allowed (partial WHERE clause)
  - The card surfaces the index status + offending pairs
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

import pytest


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("orders", "quotes"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    # Also drop the index so each test starts from a known state —
    # the migration recreates it.
    try:
        conn.execute("DROP INDEX IF EXISTS idx_orders_po_quote")
    except Exception:
        pass
    conn.commit()


def _seed_order(conn, *, order_id, quote_number="", po_number="",
                total=100.0, agency="CDCR", is_test=0):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, agency, "",
          total, "open", "[]", when, when, is_test))


def _migrate():
    from src.core.db import _migrate_columns
    _migrate_columns()


def _index_exists(conn):
    r = conn.execute("""
        SELECT name FROM sqlite_master
        WHERE type='index' AND name='idx_orders_po_quote'
    """).fetchone()
    return bool(r)


def _build_card():
    from src.api.modules.routes_health import _build_duplicate_orders_card
    return _build_duplicate_orders_card()


# ── Migration: index creation ───────────────────────────────────────────


def test_migration_creates_index_on_clean_db():
    with _conn() as c:
        _wipe(c)
    _migrate()
    with _conn() as c:
        assert _index_exists(c)


def test_migration_is_idempotent():
    """Re-running the migration must NOT error — IF NOT EXISTS
    handles it. The index lands once and stays."""
    with _conn() as c:
        _wipe(c)
    _migrate()
    _migrate()
    _migrate()
    with _conn() as c:
        assert _index_exists(c)


def test_migration_skips_index_when_existing_dups_block_it():
    """If existing data has same-PO + same-quote on 2+ rows, the
    index can't land. Migration must catch IntegrityError and not
    crash the boot — the card surfaces the offenders so the
    operator can merge them."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="dup1", quote_number="QDUP",
                    po_number="PO-DUP")
        _seed_order(c, order_id="dup2", quote_number="QDUP",
                    po_number="PO-DUP")
        c.commit()
    _migrate()  # must not raise
    with _conn() as c:
        # Index did NOT land (existing duplicates block it).
        assert not _index_exists(c)


# ── Constraint behavior once index is live ─────────────────────────────


def test_index_blocks_true_duplicate_insert():
    """Once the partial UNIQUE index is live, inserting a second
    row with the same (po_number, quote_number) must raise
    IntegrityError — that's the whole point. Operator-typo
    double-writes get rejected at the DB layer instead of silently
    creating phantom orders."""
    with _conn() as c:
        _wipe(c)
    _migrate()
    with _conn() as c:
        _seed_order(c, order_id="ord1", quote_number="QX",
                    po_number="PO-X")
        c.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _seed_order(c, order_id="ord2", quote_number="QX",
                        po_number="PO-X")
            c.commit()


def test_index_allows_multi_quote_po():
    """Same po_number + DIFFERENT quote_numbers must stay legal.
    PO 0000053217 spanning 7 quotes is a real prod pattern."""
    with _conn() as c:
        _wipe(c)
    _migrate()
    with _conn() as c:
        for i in range(5):
            _seed_order(c, order_id=f"ord{i}", quote_number=f"Q{i}",
                        po_number="PO-MULTI")
        c.commit()
        n = c.execute(
            "SELECT COUNT(*) AS n FROM orders WHERE po_number='PO-MULTI'"
        ).fetchone()["n"]
    assert n == 5


def test_index_allows_repeated_empty_po_number():
    """Partial WHERE clause means empty po_number rows don't
    trigger the constraint. Multiple "no PO yet" rows can share
    the same quote_number until the PO email arrives."""
    with _conn() as c:
        _wipe(c)
    _migrate()
    with _conn() as c:
        _seed_order(c, order_id="np1", quote_number="QY", po_number="")
        _seed_order(c, order_id="np2", quote_number="QY", po_number="")
        c.commit()
        n = c.execute(
            "SELECT COUNT(*) AS n FROM orders "
            "WHERE quote_number='QY' AND COALESCE(po_number,'')=''"
        ).fetchone()["n"]
    assert n == 2


# ── Card builder ────────────────────────────────────────────────────────


def test_card_healthy_when_no_dups_and_index_active():
    with _conn() as c:
        _wipe(c)
    _migrate()
    out = _build_card()
    assert out["status"] == "healthy"
    assert out["duplicate_pairs"] == 0
    assert out["index_active"] is True


def test_card_warn_when_no_dups_but_index_not_landed():
    """Fresh state: no rows, no index — that's "warn" (not an
    error, but not healthy either; constraint will land on next
    full migration run)."""
    with _conn() as c:
        _wipe(c)
    out = _build_card()
    assert out["status"] == "warn"
    assert out["duplicate_pairs"] == 0
    assert out["index_active"] is False


def test_card_error_when_dups_present():
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="d1", quote_number="QD",
                    po_number="PO-D")
        _seed_order(c, order_id="d2", quote_number="QD",
                    po_number="PO-D")
        c.commit()
    _migrate()  # index won't land
    out = _build_card()
    assert out["status"] == "error"
    assert out["duplicate_pairs"] == 1
    assert out["duplicate_rows_total"] == 2
    assert out["index_active"] is False
    sample = out["samples"][0]
    assert sample["po_number"] == "PO-D"
    assert sample["quote_number"] == "QD"
    assert sample["count"] == 2
    assert set(sample["order_ids"]) == {"d1", "d2"}


def test_card_excludes_test_orders():
    """is_test=1 rows mustn't trigger the duplicate signal — they're
    test fixtures, not write-path bugs."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="t1", quote_number="QT",
                    po_number="PO-T", is_test=1)
        _seed_order(c, order_id="t2", quote_number="QT",
                    po_number="PO-T", is_test=1)
        c.commit()
    out = _build_card()
    assert out["duplicate_pairs"] == 0


def test_card_excludes_empty_po_or_quote():
    """No-PO-yet rows aren't dups for this signal — they're
    counted by orders_drift.orders_no_po instead."""
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="e1", quote_number="QE", po_number="")
        _seed_order(c, order_id="e2", quote_number="QE", po_number="")
        c.commit()
    out = _build_card()
    assert out["duplicate_pairs"] == 0


def test_card_caps_samples_at_20():
    with _conn() as c:
        _wipe(c)
        for i in range(25):
            for j in range(2):
                _seed_order(c, order_id=f"d{i}-{j}",
                            quote_number=f"Q{i}",
                            po_number=f"PO-{i}")
        c.commit()
    out = _build_card()
    assert out["duplicate_pairs"] == 20  # samples capped
    # Total rows includes ALL dup rows seen by COUNT, but samples
    # only the top 20 groups.


# ── /health/quoting integration ─────────────────────────────────────────


def test_health_quoting_json_includes_duplicate_orders(auth_client):
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "duplicate_orders" in data
    du = data["duplicate_orders"]
    for k in ("status", "duplicate_pairs", "duplicate_rows_total",
              "index_active", "samples"):
        assert k in du


def test_health_quoting_html_renders_duplicate_orders_card(auth_client):
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    assert "Duplicate orders" in body
    assert "CONSTRAINT" in body
