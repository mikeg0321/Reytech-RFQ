"""Mike P0 2026-05-05 (Phase 1.5): boot backfill that scrubs the junk
sku/mfg_number rows PR #741 left behind.

PR #741 (`_is_real_part_number` + `match_item` gate + `add_to_catalog`
sanitization) closed the WRITE path: new pollution is impossible. But
two known polluted rows existed on prod when #741 shipped:

  id=840  sku="1"  mfg="1"  name="Engraved two line name tag"
  id=841  sku="2"  mfg="2"  name="Copy paper"

Until those rows are scrubbed, match_item Strategy 1 still has a path
to surface the wrong product on any line whose placeholder pn happens
to equal an existing junk sku — even though Strategy 1 itself now
refuses to USE the buyer's junk pn. PR #741's gate closes one direction;
this backfill closes the other.

`cleanup_polluted_catalog_rows()` runs from `app.py _deferred_init()`
on every boot. It NULLs out sku/mfg_number on rows whose values fail
`_is_real_part_number` (1-3 chars). Idempotent: re-runs are safe.
"""
import pytest


@pytest.fixture
def tmp_catalog(tmp_path, monkeypatch):
    monkeypatch.setenv("PRODUCT_CATALOG_DB", str(tmp_path / "catalog.db"))
    from src.agents import product_catalog
    monkeypatch.setattr(product_catalog, "DB_PATH", str(tmp_path / "catalog.db"))
    product_catalog.init_catalog_db()
    return product_catalog


# ── Cleanup scrubs the exact prod scenario ─────────────────────────


def test_cleanup_clears_known_polluted_rows(tmp_catalog):
    """Reproduce the prod scenario: rows id=840 sku=1 / id=841 sku=2.
    cleanup_polluted_catalog_rows must NULL out sku/mfg_number on both."""
    pc = tmp_catalog
    conn = pc._get_conn()
    conn.execute(
        "INSERT INTO product_catalog (name, sku, mfg_number, description, "
        " sell_price, cost, is_test) VALUES (?, ?, ?, ?, ?, ?, 0)",
        ("Engraved two line name tag, black/white", "1", "1",
         "Engraved two line name tag", 5.00, 3.00),
    )
    conn.execute(
        "INSERT INTO product_catalog (name, sku, mfg_number, description, "
        " sell_price, cost, is_test) VALUES (?, ?, ?, ?, ?, ?, 0)",
        ("Copy paper, 8.5x11, white, 500 sheet ream", "2", "2",
         "Copy paper", 4.50, 3.00),
    )
    conn.commit()
    conn.close()

    result = pc.cleanup_polluted_catalog_rows()
    assert result["rows_touched"] == 2
    assert result["sku_cleared"] == 2
    assert result["mfg_cleared"] == 2

    conn = pc._get_conn()
    rows = conn.execute(
        "SELECT name, sku, mfg_number FROM product_catalog ORDER BY name"
    ).fetchall()
    conn.close()
    for r in rows:
        assert r["sku"] == "" or r["sku"] is None, (
            f"Row {r['name']!r} should have sku NULLed, got {r['sku']!r}"
        )
        assert r["mfg_number"] == "" or r["mfg_number"] is None, (
            f"Row {r['name']!r} should have mfg NULLed, got {r['mfg_number']!r}"
        )


# ── Cleanup leaves real pns alone ──────────────────────────────────


def test_cleanup_preserves_real_part_numbers(tmp_catalog):
    """The cleanup must not touch real pns — only junk placeholders."""
    pc = tmp_catalog
    conn = pc._get_conn()
    real_rows = [
        ("Echo Dot 5th Gen", "B0CHH87PT2", "B0CHH87PT2"),
        ("Engraved nameplate", "AB-12345", "AB-12345"),
        ("Dell laptop charger", "008R13041", "008R13041"),
        ("Glove nitrile L", "GLV-NIT-L-100", "GLV-NIT-L-100"),
    ]
    for name, sku, mfg in real_rows:
        conn.execute(
            "INSERT INTO product_catalog (name, sku, mfg_number, "
            " sell_price, cost, is_test) VALUES (?, ?, ?, ?, ?, 0)",
            (name, sku, mfg, 50.00, 30.00),
        )
    conn.commit()
    conn.close()

    result = pc.cleanup_polluted_catalog_rows()
    assert result["rows_touched"] == 0, (
        "Real pns must not be touched"
    )

    conn = pc._get_conn()
    rows = {r["name"]: r for r in conn.execute(
        "SELECT name, sku, mfg_number FROM product_catalog"
    ).fetchall()}
    conn.close()
    for name, sku, mfg in real_rows:
        assert rows[name]["sku"] == sku
        assert rows[name]["mfg_number"] == mfg


# ── Cleanup handles partial pollution (one field junk, other real) ──


def test_cleanup_clears_only_the_junk_field(tmp_catalog):
    """If sku is junk but mfg is real (or vice versa), only clear the
    junk field — preserve the real one."""
    pc = tmp_catalog
    conn = pc._get_conn()
    conn.execute(
        "INSERT INTO product_catalog (name, sku, mfg_number, "
        " sell_price, cost, is_test) VALUES (?, ?, ?, ?, ?, 0)",
        ("Mixed-pollution row A", "1", "B0CHH87PT2", 50.00, 30.00),
    )
    conn.execute(
        "INSERT INTO product_catalog (name, sku, mfg_number, "
        " sell_price, cost, is_test) VALUES (?, ?, ?, ?, ?, 0)",
        ("Mixed-pollution row B", "AB-12345", "1", 50.00, 30.00),
    )
    conn.commit()
    conn.close()

    pc.cleanup_polluted_catalog_rows()

    conn = pc._get_conn()
    rows = {r["name"]: r for r in conn.execute(
        "SELECT name, sku, mfg_number FROM product_catalog"
    ).fetchall()}
    conn.close()
    assert rows["Mixed-pollution row A"]["sku"] == ""
    assert rows["Mixed-pollution row A"]["mfg_number"] == "B0CHH87PT2"
    assert rows["Mixed-pollution row B"]["sku"] == "AB-12345"
    assert rows["Mixed-pollution row B"]["mfg_number"] == ""


# ── Idempotence — re-runs are safe ─────────────────────────────────


def test_cleanup_is_idempotent(tmp_catalog):
    """The backfill runs on every boot. Re-runs must not double-count
    or re-touch already-cleaned rows."""
    pc = tmp_catalog
    conn = pc._get_conn()
    conn.execute(
        "INSERT INTO product_catalog (name, sku, mfg_number, "
        " sell_price, cost, is_test) VALUES (?, ?, ?, ?, ?, 0)",
        ("polluted row", "1", "1", 5.00, 3.00),
    )
    conn.commit()
    conn.close()

    first = pc.cleanup_polluted_catalog_rows()
    assert first["rows_touched"] == 1

    # Re-run: nothing left to clean
    second = pc.cleanup_polluted_catalog_rows()
    assert second["rows_touched"] == 0
    assert second["sku_cleared"] == 0
    assert second["mfg_cleared"] == 0


# ── Empty catalog — no-op ──────────────────────────────────────────


def test_cleanup_on_empty_catalog_returns_zero(tmp_catalog):
    """Cleanup on a fresh catalog returns zero counts, no errors."""
    pc = tmp_catalog
    result = pc.cleanup_polluted_catalog_rows()
    assert result == {"rows_touched": 0, "sku_cleared": 0, "mfg_cleared": 0}


# ── Boot wiring — function is called from _deferred_init ───────────


def test_cleanup_is_wired_into_deferred_init():
    """The backfill must run on every boot. Pin the import + call site
    in app.py _deferred_init() so it doesn't get accidentally removed."""
    from pathlib import Path
    body = (Path(__file__).resolve().parent.parent / "app.py").read_text(encoding="utf-8")
    # Import call from product_catalog
    assert "from src.agents.product_catalog import cleanup_polluted_catalog_rows" in body, (
        "app.py _deferred_init must import cleanup_polluted_catalog_rows"
    )
    # Function invocation
    assert "cleanup_polluted_catalog_rows()" in body, (
        "app.py _deferred_init must call cleanup_polluted_catalog_rows()"
    )


# ── 1-3 char threshold matches _is_real_part_number ────────────────


def test_cleanup_threshold_matches_helper():
    """The backfill's SQL `LENGTH(TRIM(...)) BETWEEN 1 AND 3` must
    match `_is_real_part_number`'s `len(s) >= 4` rule. Drift between
    them re-opens the loophole."""
    from src.agents.product_catalog import _is_real_part_number
    # 1-3 char strings: backfill clears them, helper says junk
    for junk in ["1", "12", "123", "AB", "X", "abc"]:
        assert _is_real_part_number(junk) is False
    # 4+ char strings: backfill leaves them, helper says real
    for real in ["1234", "ABCD", "AB-12"]:
        assert _is_real_part_number(real) is True
