"""
Regression tests for §3e: is_test isolation on SCPRS tables.

Why this exists:
  Until migration 23, scprs_po_master and scprs_po_lines had no is_test
  column. Test/synthetic data feeding either table would silently flow
  into:
    - operator-visible KPI cards (POs, line items, gap_spend, win_back)
    - check_quotes_against_scprs auto-close-lost decisions on REAL quotes
    - pricing_oracle_v2._search_po_lines weighted-blend averages

  Same shape as CR-5 / AN-P0 / RE-AUDIT-5 from earlier audits — every
  operationally significant table needs an is_test flag and every read
  site that drives operator decisions must filter is_test=0.

  These tests prove:
    1. Migration 23 adds is_test column with default 0 to both tables.
    2. _ensure_schema on a fresh DB installs the column.
    3. get_pull_status counts only is_test=0 rows.
    4. get_universal_intelligence (totals, by_agency, gaps, win_back) all
       filter is_test=0.
    5. check_quotes_against_scprs ignores is_test SCPRS POs — proves
       a synthetic test PO can NEVER auto-close a real quote.
    6. _search_po_lines (oracle) ignores is_test SCPRS data — proves
       test rows can't pollute the weighted-blend average.
"""
import os
import sqlite3

import pytest


@pytest.fixture
def scprs_db(tmp_path, monkeypatch):
    """Fresh SCPRS DB via _ensure_schema (which now includes is_test)."""
    data_dir = tmp_path / "scprs_iso_3e"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    db_path = str(data_dir / "reytech.db")
    # Stub quotes table joined by check_quotes_against_scprs.
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS quotes ("
        "id INTEGER PRIMARY KEY, quote_number TEXT, agency TEXT, "
        "institution TEXT, status TEXT, status_notes TEXT, "
        "total REAL, created_at TEXT, items_text TEXT, items_detail TEXT, "
        "is_test INTEGER DEFAULT 0, updated_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS price_history ("
        "id INTEGER PRIMARY KEY, found_at TEXT, description TEXT, "
        "unit_price REAL, quantity REAL, source TEXT, agency TEXT, "
        "quote_number TEXT, notes TEXT)"
    )
    conn.commit()
    conn.close()
    return sup, db_path


def _seed_pos(conn, rows):
    """rows: list of (po_number, dept_code, agency_code, supplier, total,
    is_test, lines) where lines is [(desc, qty, price, line_total, sells,
    opp, line_is_test), ...]."""
    for po_num, dept_code, agency_code, supplier, total, is_test, lines in rows:
        cur = conn.execute(
            "INSERT INTO scprs_po_master "
            "(po_number, dept_code, dept_name, agency_code, supplier, "
            "grand_total, start_date, is_test) VALUES (?,?,?,?,?,?,?,?)",
            (po_num, dept_code, dept_code, agency_code, supplier, total,
             "2026-01-15", is_test),
        )
        po_id = cur.lastrowid
        for j, (desc, qty, price, line_total, sells, opp, line_is_test) in enumerate(lines):
            conn.execute(
                "INSERT INTO scprs_po_lines "
                "(po_id, po_number, line_num, item_id, description, quantity, "
                "unit_price, line_total, reytech_sells, opportunity_flag, is_test) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (po_id, po_num, j, "", desc, qty, price, line_total,
                 sells, opp, line_is_test),
            )


def test_ensure_schema_installs_is_test_column(scprs_db):
    sup, db_path = scprs_db
    conn = sqlite3.connect(db_path)
    master_cols = {r[1] for r in conn.execute("PRAGMA table_info(scprs_po_master)").fetchall()}
    lines_cols = {r[1] for r in conn.execute("PRAGMA table_info(scprs_po_lines)").fetchall()}
    assert "is_test" in master_cols
    assert "is_test" in lines_cols
    # Default is 0 — a row inserted without specifying is_test should be is_test=0.
    conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, supplier) "
        "VALUES ('PO-DEFAULT', '4700', 'CCHCS', 'TestSupplier')"
    )
    conn.commit()
    val = conn.execute(
        "SELECT is_test FROM scprs_po_master WHERE po_number='PO-DEFAULT'"
    ).fetchone()[0]
    assert val == 0
    conn.close()


def test_migration_23_adds_is_test_to_existing_tables(tmp_path):
    """Production-like scenario: tables exist WITHOUT is_test (pre-migration
    schema). Migration must ADD COLUMN idempotently."""
    db_path = str(tmp_path / "scprs_pre23" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    # Pre-migration schema (no is_test).
    conn.executescript("""
        CREATE TABLE scprs_po_master (id INTEGER PRIMARY KEY, po_number TEXT);
        CREATE TABLE scprs_po_lines (id INTEGER PRIMARY KEY, po_id INTEGER);
    """)
    conn.commit()

    from src.core.migrations import _run_migration_23
    _run_migration_23(conn)
    conn.commit()

    master_cols = {r[1] for r in conn.execute("PRAGMA table_info(scprs_po_master)").fetchall()}
    lines_cols = {r[1] for r in conn.execute("PRAGMA table_info(scprs_po_lines)").fetchall()}
    assert "is_test" in master_cols
    assert "is_test" in lines_cols

    # Re-running is a no-op (idempotent — column already exists).
    _run_migration_23(conn)
    conn.commit()
    conn.close()


def test_migration_23_safe_on_fresh_db_without_scprs_tables(tmp_path):
    """No SCPRS tables yet → migration must skip cleanly."""
    db_path = str(tmp_path / "scprs_clean" / "reytech.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    from src.core.migrations import _run_migration_23
    _run_migration_23(conn)  # Must not raise.
    has = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name IN ('scprs_po_master','scprs_po_lines')"
    ).fetchone()
    assert has is None
    conn.close()


def test_get_pull_status_excludes_is_test_rows(scprs_db):
    sup, db_path = scprs_db
    conn = sqlite3.connect(db_path)
    _seed_pos(conn, [
        ("PO-REAL-1", "4700", "4700", "Medline", 5000.0, 0, [
            ("nitrile gloves", 50, 8.00, 400.0, 1, "WIN_BACK", 0),
        ]),
        ("PO-TEST-1", "4700", "4700", "TestVendor", 9999.0, 1, [
            ("ghost item", 100, 1.00, 100.0, 0, "GAP_ITEM", 1),
        ]),
        ("PO-TEST-2", "5225", "5225", "TestVendor2", 8888.0, 1, [
            ("another ghost", 50, 2.00, 100.0, 0, "GAP_ITEM", 1),
        ]),
    ])
    conn.commit()
    conn.close()

    status = sup.get_pull_status()
    # Only the 1 real PO counts.
    assert status["pos_stored"] == 1
    assert status["lines_stored"] == 1
    assert status["agencies_seen"] == 1


def test_get_universal_intelligence_excludes_is_test_rows(scprs_db):
    sup, db_path = scprs_db
    conn = sqlite3.connect(db_path)
    _seed_pos(conn, [
        ("PO-REAL", "4700", "4700", "Medline", 5000.0, 0, [
            ("nitrile gloves", 50, 8.00, 400.0, 1, "WIN_BACK", 0),
            ("real gap item", 10, 5.00, 50.0, 0, "GAP_ITEM", 0),
        ]),
        # Test PO with HUGE total — would inflate aggregates if leaked.
        ("PO-TEST", "4700", "4700", "GhostVendor", 1_000_000.0, 1, [
            ("synthetic gloves", 999, 100.00, 99_900.0, 1, "WIN_BACK", 1),
            ("synthetic gap", 500, 50.00, 25_000.0, 0, "GAP_ITEM", 1),
        ]),
    ])
    conn.commit()
    conn.close()

    intel = sup.get_universal_intelligence()
    # Totals only count the real PO (COUNT/SUM use DISTINCT, no JOIN inflation).
    assert intel["totals"]["po_count"] == 1
    assert intel["totals"]["total_spend"] == 5000.0
    # by_agency: $1M test ghost MUST be excluded. (Note: by_agency joins
    # po_master x po_lines, so SUM(grand_total) inflates by line count
    # — a separate pre-existing bug in the same family as AN-3, not in
    # scope for this PR. We just assert the ghost can never reach the
    # aggregate, regardless of inflation factor.)
    by_agency = {row["dept_code"]: row for row in intel["by_agency"]}
    assert "4700" in by_agency
    assert by_agency["4700"]["total_spend"] < 100_000  # < ghost's $1M, < $1M+$5K
    assert by_agency["4700"]["po_count"] == 1  # 1 real PO, ghost excluded
    # gap_items don't include the ghost.
    descs = {g["description"] for g in intel["gap_items"]}
    assert "real gap item" in descs
    assert "synthetic gap" not in descs
    # win_back doesn't include the ghost.
    wb = {w["description"] for w in intel["win_back"]}
    assert "nitrile gloves" in wb
    assert "synthetic gloves" not in wb


def test_check_quotes_against_scprs_ignores_is_test_pos(scprs_db):
    """The headline risk this PR closes: a synthetic test PO must NEVER
    cause a real Reytech quote to be auto-closed-lost."""
    sup, db_path = scprs_db
    conn = sqlite3.connect(db_path)
    # Seed a real Reytech open quote.
    conn.execute(
        "INSERT INTO quotes (quote_number, agency, institution, status, total, "
        "created_at, items_text, items_detail, is_test) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        ("R26Q9999", "CCHCS", "CCHCS", "sent", 800.0, "2026-01-10",
         "nitrile exam gloves medium box of 100", "[]", 0)
    )
    # Seed a TEST SCPRS PO matching the real quote's items.
    _seed_pos(conn, [
        ("PO-TEST-CLOSE", "4700", "4700", "GhostMedline", 5000.0, 1, [
            ("nitrile exam gloves medium", 50, 8.00, 400.0, 1, "WIN_BACK", 1),
        ]),
    ])
    conn.commit()
    conn.close()

    result = sup.check_quotes_against_scprs()
    # The test PO must NOT trigger an auto-close on the real quote.
    assert result["auto_closed"] == 0

    # Verify the real quote is still 'sent', not 'closed_lost'.
    conn = sqlite3.connect(db_path)
    status = conn.execute(
        "SELECT status FROM quotes WHERE quote_number='R26Q9999'"
    ).fetchone()[0]
    conn.close()
    assert status == "sent"


def test_check_quotes_against_scprs_does_close_on_real_pos(scprs_db):
    """Inverse — when the SCPRS PO is real (is_test=0), the real quote
    SHOULD be auto-closed-lost. Proves the filter isn't accidentally
    blocking the correct close path."""
    sup, db_path = scprs_db
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO quotes (quote_number, agency, institution, status, total, "
        "created_at, items_text, items_detail, is_test) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        ("R26Q8888", "CCHCS", "CCHCS", "sent", 800.0, "2026-01-10",
         "nitrile exam", "[]", 0)
    )
    _seed_pos(conn, [
        ("PO-REAL-CLOSE", "4700", "4700", "RealMedline", 5000.0, 0, [
            ("nitrile exam gloves medium", 50, 8.00, 400.0, 1, "WIN_BACK", 0),
        ]),
    ])
    conn.commit()
    conn.close()

    result = sup.check_quotes_against_scprs()
    assert result["auto_closed"] == 1


def test_oracle_search_po_lines_excludes_is_test(scprs_db, tmp_path, monkeypatch):
    """pricing_oracle_v2._search_po_lines must NOT use test SCPRS rows
    for weighted-blend averaging."""
    sup, db_path = scprs_db
    # Seed: 1 real row at $8/unit + 5 test rows at $100/unit (would skew
    # the average dramatically if leaked).
    conn = sqlite3.connect(db_path)
    _seed_pos(conn, [
        ("PO-REAL", "4700", "4700", "Medline", 400.0, 0, [
            ("nitrile exam gloves medium 100ct", 50, 8.00, 400.0, 1, "WIN_BACK", 0),
        ]),
        ("PO-TEST-1", "4700", "4700", "Ghost", 10000.0, 1, [
            ("nitrile exam gloves medium 100ct", 100, 100.00, 10000.0, 1, "WIN_BACK", 1),
        ]),
        ("PO-TEST-2", "4700", "4700", "Ghost", 10000.0, 1, [
            ("nitrile exam gloves medium 100ct", 100, 100.00, 10000.0, 1, "WIN_BACK", 1),
        ]),
    ])
    conn.commit()

    # Point pricing_oracle_v2 at this DB.
    import src.core.pricing_oracle_v2 as oracle
    # _search_po_lines accepts a db handle directly.
    rows = oracle._search_po_lines(conn, "nitrile exam gloves medium 100ct", item_number="")
    conn.close()

    # Only the real row contributes to the price comp pool.
    assert len(rows) == 1
    assert rows[0]["price"] == 8.00
    assert rows[0]["supplier"] == "Medline"
