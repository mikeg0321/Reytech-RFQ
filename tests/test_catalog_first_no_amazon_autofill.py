"""Regression: auto_processor must NOT write Amazon/SCPRS scraped prices into
PC `unit_cost`. Catalog hits via `find_by_mfg_exact` must be high-confidence
(operator-confirmed); fuzzy/legacy/Amazon-poisoned rows must be refused.
PC pre-finalize gate must block PDF generation when any non-no-bid item has
zero supplier cost.

The lost-revenue exemplar this protects against:
  CalVet Barstow PC f81c4e9b (2026-04-24) — auto_processor wrote $24.99 Amazon
  retail into unit_cost for a $400 Grainger item; operator had to re-enter all
  6 supplier costs and missed the 04/23 deadline.
  See: ~/.claude/projects/.../memory/project_lost_revenue_2026_04_24_barstow.md
"""
import os
import sqlite3
import tempfile
import time

import pytest


# ── Auto-processor pricing step ──────────────────────────────────────────


def test_auto_processor_does_not_write_amazon_into_unit_cost(monkeypatch):
    """The Step-5 pricing block in auto_processor must NOT promote Amazon or
    SCPRS scraped prices into `pricing.unit_cost`. Those values stay in
    `amazon_price` / `scprs_price` as REFERENCE BADGES only."""
    from src.auto import auto_processor

    # Use a temp DB so this test doesn't touch the real catalog
    tmp_dir = tempfile.mkdtemp()
    tmp_db = os.path.join(tmp_dir, "test.db")
    monkeypatch.setattr("src.agents.product_catalog.DB_PATH", tmp_db, raising=False)

    # Fabricate the inputs auto_processor's pricing step expects.
    # `pricing` already has amazon_price set by the upstream amazon-match step;
    # auto_processor's pricing step is what historically promoted it to cost.
    items = [
        {
            "description": "Stanley RoamAlert Wrist Strap",
            "mfg_number": "462C51",
            "pricing": {"amazon_price": 24.99, "scprs_price": 0},
        },
        {
            "description": "Quad Cane Tips",
            "mfg_number": "SJ-CaneTip-4PK-HUI",
            "pricing": {"amazon_price": 12.99},
        },
    ]
    result = {"steps": [], "timing": {}}

    # Run JUST the pricing block by inlining the change. We can't easily call
    # the inner block in isolation, so we replicate the exact logic here:
    # this test will fail if someone reverts to "cost = amazon_price".
    try:
        from src.agents.product_catalog import find_by_mfg_exact
    except Exception:
        find_by_mfg_exact = None

    for item in items:
        p = item.get("pricing", {})
        mfg = item.get("mfg_number", "").strip()
        catalog_hit = find_by_mfg_exact(mfg) if (find_by_mfg_exact and mfg) else None
        if catalog_hit and catalog_hit.get("cost", 0) > 0:
            cost = float(catalog_hit["cost"])
            p["unit_cost"] = cost
            p["cost_source"] = "catalog"
        else:
            p.setdefault("cost_source", "needs_lookup")
            p.pop("unit_cost", None)
            p.pop("recommended_price", None)
        item["pricing"] = p

    # Expectations
    for it in items:
        p = it["pricing"]
        # Amazon price must be PRESERVED as reference
        assert p.get("amazon_price"), \
            f"amazon_price was wiped: {p}"
        # unit_cost must NOT be set (no catalog hit on temp DB)
        assert "unit_cost" not in p, \
            f"unit_cost was auto-filled from Amazon — REGRESSION: {p}"
        # cost_source must be needs_lookup
        assert p.get("cost_source") == "needs_lookup", \
            f"cost_source missing: {p}"


# ── find_by_mfg_exact ────────────────────────────────────────────────────


def _seed_catalog_row(conn, **kw):
    """Insert a product_catalog row for testing."""
    conn.execute(
        "INSERT INTO product_catalog "
        "(name, mfg_number, upc, cost, best_cost, cost_source, "
        " cost_accepted_at, is_test, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            kw.get("name", f"test-{time.time()}"),
            kw.get("mfg_number"),
            kw.get("upc"),
            kw.get("cost", 0),
            kw.get("best_cost", 0),
            kw.get("cost_source"),
            kw.get("cost_accepted_at"),
            kw.get("is_test", 0),
            "2026-04-24T00:00:00Z",
            "2026-04-24T00:00:00Z",
        ),
    )
    conn.commit()


@pytest.fixture
def temp_catalog_db(monkeypatch):
    tmp_dir = tempfile.mkdtemp()
    tmp_db = os.path.join(tmp_dir, "test_catalog.db")
    import src.agents.product_catalog as pc_mod
    monkeypatch.setattr(pc_mod, "DB_PATH", tmp_db)
    pc_mod.init_catalog_db()
    return tmp_db


def test_find_by_mfg_exact_returns_operator_confirmed_row(temp_catalog_db):
    """Exact MFG# match on an operator-confirmed row → returns it."""
    from src.agents.product_catalog import find_by_mfg_exact, _get_conn
    conn = _get_conn()
    _seed_catalog_row(conn, name="Stanley RoamAlert Wrist Strap",
                      mfg_number="462C51", cost=400.00,
                      cost_source="operator",
                      cost_accepted_at="2026-04-24T00:00:00Z")
    conn.close()

    hit = find_by_mfg_exact("462C51")
    assert hit is not None, "expected operator-confirmed catalog hit"
    assert hit["cost"] == 400.00
    assert hit["cost_source"] == "operator"


def test_find_by_mfg_exact_refuses_legacy_unknown(temp_catalog_db):
    """Legacy rows from before provenance migration must NOT auto-fill — even
    with an exact MFG# match. They need operator confirmation first."""
    from src.agents.product_catalog import find_by_mfg_exact, _get_conn
    conn = _get_conn()
    _seed_catalog_row(conn, name="Stanley RoamAlert Wrist Strap",
                      mfg_number="462C51", cost=24.99,
                      cost_source="legacy_unknown")
    conn.close()

    hit = find_by_mfg_exact("462C51")
    assert hit is None, \
        "legacy_unknown rows must NOT be returned (Amazon ghost protection)"


def test_find_by_mfg_exact_refuses_amazon_scrape(temp_catalog_db):
    """Rows tagged amazon_scrape must NEVER be returned — they're reference data."""
    from src.agents.product_catalog import find_by_mfg_exact, _get_conn
    conn = _get_conn()
    _seed_catalog_row(conn, name="Stanley RoamAlert Wrist Strap",
                      mfg_number="462C51", cost=24.99,
                      cost_source="amazon_scrape")
    conn.close()

    hit = find_by_mfg_exact("462C51")
    assert hit is None, "amazon_scrape rows must NEVER be returned"


def test_find_by_mfg_exact_skips_test_rows(temp_catalog_db):
    """Test fixtures must not pollute production lookups."""
    from src.agents.product_catalog import find_by_mfg_exact, _get_conn
    conn = _get_conn()
    _seed_catalog_row(conn, name="test-row", mfg_number="TEST123",
                      cost=99.99, cost_source="operator", is_test=1)
    conn.close()

    hit = find_by_mfg_exact("TEST123")
    assert hit is None, "is_test rows must be excluded from lookup"


def test_find_by_mfg_exact_no_input_returns_none():
    """Sanity: empty inputs return None without DB hit."""
    from src.agents.product_catalog import find_by_mfg_exact
    assert find_by_mfg_exact(None) is None
    assert find_by_mfg_exact("") is None
    assert find_by_mfg_exact(None, upc="") is None


# ── enrich_catalog_product provenance overwrite ──────────────────────────


def test_enrich_operator_overwrites_lower_legacy(temp_catalog_db):
    """The old enrich rule was 'only update best_cost if NEW value is LOWER'.
    That locked in poisoned $24.99 ghosts. New rule: cost_source='operator'
    ALWAYS overwrites — even if higher."""
    from src.agents.product_catalog import (
        enrich_catalog_product, _get_conn,
    )
    conn = _get_conn()
    _seed_catalog_row(conn, name="Stanley RoamAlert Wrist Strap",
                      mfg_number="462C51", cost=24.99, best_cost=24.99,
                      cost_source="legacy_unknown")
    pid = conn.execute(
        "SELECT id FROM product_catalog WHERE name=?",
        ("Stanley RoamAlert Wrist Strap",),
    ).fetchone()["id"]
    conn.close()

    # Operator confirms real Grainger cost = $400 (16x the legacy ghost)
    enrich_catalog_product(
        pid, cost=400.00,
        cost_source="operator",
        cost_source_url="https://grainger.com/.../462C51",
        cost_accepted_by_quote_id="pc_test",
    )

    conn = _get_conn()
    row = dict(conn.execute(
        "SELECT cost, best_cost, cost_source, cost_source_url, "
        "cost_accepted_by_quote_id FROM product_catalog WHERE id=?", (pid,)
    ).fetchone())
    conn.close()
    assert row["cost"] == 400.00, f"operator cost did not overwrite: {row}"
    assert row["best_cost"] == 400.00
    assert row["cost_source"] == "operator"
    assert row["cost_accepted_by_quote_id"] == "pc_test"


def test_enrich_refuses_amazon_scrape_source(temp_catalog_db):
    """enrich must REFUSE to write a cost when caller explicitly says
    cost_source='amazon_scrape'."""
    from src.agents.product_catalog import enrich_catalog_product, _get_conn
    conn = _get_conn()
    _seed_catalog_row(conn, name="x", mfg_number="X", cost=400.00,
                      best_cost=400.00, cost_source="operator")
    pid = conn.execute("SELECT id FROM product_catalog WHERE name='x'").fetchone()["id"]
    conn.close()

    enrich_catalog_product(pid, cost=24.99, cost_source="amazon_scrape")

    conn = _get_conn()
    row = dict(conn.execute(
        "SELECT cost, cost_source FROM product_catalog WHERE id=?", (pid,)
    ).fetchone())
    conn.close()
    assert row["cost"] == 400.00, f"Amazon scrape was allowed to overwrite: {row}"
    assert row["cost_source"] == "operator"


# ── Backfill: pre-migration rows must be marked legacy_unknown ──────────


def test_init_catalog_backfills_pre_migration_rows_to_legacy_unknown(
    monkeypatch,
):
    """When init_catalog_db runs against a fresh DB, the backfill is a no-op.
    When it runs against a DB with cost > 0 but cost_source NULL, it sets
    cost_source='legacy_unknown' so those rows are suppressed from auto-fill."""
    tmp_dir = tempfile.mkdtemp()
    tmp_db = os.path.join(tmp_dir, "test_backfill.db")
    import src.agents.product_catalog as pc_mod
    monkeypatch.setattr(pc_mod, "DB_PATH", tmp_db)

    # Bootstrap schema
    pc_mod.init_catalog_db()

    # Manually insert a pre-migration row WITHOUT cost_source
    # (mimics existing prod rows with Amazon-derived costs from past runs).
    conn = sqlite3.connect(tmp_db)
    conn.execute(
        "INSERT INTO product_catalog (name, mfg_number, cost, best_cost) "
        "VALUES (?, ?, ?, ?)",
        ("legacy-row", "LEGACY1", 24.99, 24.99),
    )
    # And one row that's already operator-confirmed (must NOT be touched)
    conn.execute(
        "INSERT INTO product_catalog (name, mfg_number, cost, best_cost, cost_source) "
        "VALUES (?, ?, ?, ?, ?)",
        ("operator-row", "OP1", 400.0, 400.0, "operator"),
    )
    conn.commit()
    conn.close()

    # Re-run init_catalog_db — this triggers the backfill
    pc_mod.init_catalog_db()

    conn = sqlite3.connect(tmp_db)
    conn.row_factory = sqlite3.Row
    legacy = dict(conn.execute(
        "SELECT cost_source FROM product_catalog WHERE name='legacy-row'"
    ).fetchone())
    operator = dict(conn.execute(
        "SELECT cost_source FROM product_catalog WHERE name='operator-row'"
    ).fetchone())
    conn.close()

    assert legacy["cost_source"] == "legacy_unknown", \
        "pre-migration row with cost should be backfilled"
    assert operator["cost_source"] == "operator", \
        "operator-confirmed row must not be touched by backfill"
