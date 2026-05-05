"""Mike P1 2026-05-06 (Audit P1 #10): 3x typo guard on operator catalog
cost overwrite.

Background: routes_pricecheck:2272-2277 calls
`enrich_catalog_product(pid, cost=_cost, cost_source="operator", ...)` to
write operator-confirmed costs into product_catalog. The cost_source='operator'
path always overwrites (per the existing rule that operator-confirmed truth
dominates Amazon ghosts).

Pre-fix: NO upper-bound check. Operator typo (e.g. $2500 instead of $25,
extra zero) corrupts catalog cost permanently. Every future quote pulls
$2500 cost → $4000 quoted → never wins.

Post-fix: if existing best_cost > 0 and new cost > 3 * existing → REFUSE,
log warning, record an enrich_error row. Operator must re-enter to confirm.

3x is the canonical guardrail (CLAUDE.md Cost Sanity Guardrail). It catches
the extra-digit typo (10x, 100x) without rejecting legitimate moderate price
moves (1.5x, 2x).
"""
import sqlite3
import os
import tempfile
import pytest


@pytest.fixture
def isolated_catalog(monkeypatch, tmp_path):
    """Spin up a temp catalog DB and patch _get_conn / DATA_DIR to use it."""
    from src.agents import product_catalog as pc
    db_path = str(tmp_path / "catalog.db")

    def _get_conn():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    monkeypatch.setattr(pc, "_get_conn", _get_conn)
    # Initialize the schema
    pc.init_catalog_db()
    return pc, db_path


def _seed_product(db_path, product_id, best_cost):
    """Seed a row in product_catalog with the given best_cost."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO product_catalog (id, name, description, best_cost, cost) "
        "VALUES (?, ?, 'Test product', ?, ?)",
        (product_id, f"Test {product_id}", best_cost, best_cost)
    )
    conn.commit()
    conn.close()


def test_3x_increase_refused_for_operator_cost(isolated_catalog):
    pc, db_path = isolated_catalog
    _seed_product(db_path, 1001, 25.00)

    # Operator types $2500 (extra zero) — should be refused.
    pc.enrich_catalog_product(
        1001,
        cost=2500.00,
        cost_source="operator",
        cost_source_url="",
        cost_accepted_by_quote_id="pc-test",
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT best_cost FROM product_catalog WHERE id=?",
                       (1001,)).fetchone()
    conn.close()
    assert row[0] == 25.00, (
        f"3x typo guard failed — best_cost should still be 25.00 but is {row[0]}. "
        "Operator typo ($2500 vs $25) corrupted the catalog cost."
    )


def test_under_3x_increase_accepted(isolated_catalog):
    """A legitimate 2x price hike should still be accepted."""
    pc, db_path = isolated_catalog
    _seed_product(db_path, 1002, 100.00)

    pc.enrich_catalog_product(
        1002,
        cost=200.00,  # 2x — under threshold
        cost_source="operator",
        cost_source_url="",
        cost_accepted_by_quote_id="pc-test",
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT best_cost FROM product_catalog WHERE id=?",
                       (1002,)).fetchone()
    conn.close()
    assert row[0] == 200.00, (
        f"2x increase wrongly refused — best_cost should be 200.00 but is {row[0]}."
    )


def test_first_cost_write_skips_guard(isolated_catalog):
    """If existing best_cost is 0/NULL, no comparison applies — write proceeds."""
    pc, db_path = isolated_catalog
    _seed_product(db_path, 1003, 0)  # empty cost

    pc.enrich_catalog_product(
        1003,
        cost=500.00,
        cost_source="operator",
        cost_source_url="",
        cost_accepted_by_quote_id="pc-test",
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT best_cost FROM product_catalog WHERE id=?",
                       (1003,)).fetchone()
    conn.close()
    assert row[0] == 500.00, (
        "First cost write (existing=0) should not trigger 3x guard"
    )


def test_decrease_always_accepted(isolated_catalog):
    """A cost DECREASE is never a typo — accept any value below existing."""
    pc, db_path = isolated_catalog
    _seed_product(db_path, 1004, 100.00)

    pc.enrich_catalog_product(
        1004,
        cost=10.00,  # 10x DECREASE — should be accepted (it's not >3x)
        cost_source="operator",
        cost_source_url="",
        cost_accepted_by_quote_id="pc-test",
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT best_cost FROM product_catalog WHERE id=?",
                       (1004,)).fetchone()
    conn.close()
    assert row[0] == 10.00


def test_exactly_3x_boundary_accepted(isolated_catalog):
    """Exactly 3x should still be accepted — only STRICTLY greater is refused."""
    pc, db_path = isolated_catalog
    _seed_product(db_path, 1005, 100.00)

    pc.enrich_catalog_product(
        1005,
        cost=300.00,  # exactly 3x
        cost_source="operator",
        cost_source_url="",
        cost_accepted_by_quote_id="pc-test",
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT best_cost FROM product_catalog WHERE id=?",
                       (1005,)).fetchone()
    conn.close()
    assert row[0] == 300.00


def test_amazon_scrape_still_refused_unchanged(isolated_catalog):
    """The pre-existing Amazon/SCPRS refusal must still hold."""
    pc, db_path = isolated_catalog
    _seed_product(db_path, 1006, 50.00)

    pc.enrich_catalog_product(
        1006,
        cost=25.00,
        cost_source="amazon_scrape",  # always refused
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT best_cost FROM product_catalog WHERE id=?",
                       (1006,)).fetchone()
    conn.close()
    assert row[0] == 50.00, (
        "Amazon scrape source should still be refused (existing rule)"
    )
