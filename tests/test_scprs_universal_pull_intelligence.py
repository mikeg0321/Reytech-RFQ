"""
Regression test for scprs_universal_pull.get_universal_intelligence.

Why this test exists:
  Commit 8fe34398f (2026-03-07, "Fix ALL audit findings: 0 SQL injection")
  left literal `" + where + "` text inside four triple-quoted SQL strings in
  get_universal_intelligence. Every call raised
      sqlite3.OperationalError: near "...": syntax error
  but the page route's bare `except Exception` swallowed it, rendering the
  /intel/scprs dashboard empty for ~7 weeks while the DB silently
  accumulated 36k POs / 109k line items / 7 auto-closed quotes.

  This test seeds a minimal fixture, calls get_universal_intelligence, and
  asserts the function returns a populated, well-shaped dict — both
  unfiltered and filtered by agency_code. Either bug shape — broken SQL,
  silent-empty defaults, or filter-not-applied — fails the test.
"""
import os
import sqlite3

import pytest


@pytest.fixture
def scprs_db(tmp_path, monkeypatch):
    """Redirect scprs_universal_pull at an isolated SQLite DB.

    Uses a dedicated subdirectory (NOT the autouse `data/` dir) so the
    dashboard's init_db doesn't pre-create a stale schema before our
    agent's _ensure_schema() can run (real schema-drift issue — see
    docs/REVIEW_INTEL_SCPRS_2026_04_23.md §3).
    """
    data_dir = tmp_path / "scprs_isolated"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    # Stub the quotes table joined by the auto-closed query.
    db_path = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS quotes ("
        "id INTEGER PRIMARY KEY, quote_number TEXT, agency TEXT, "
        "status TEXT, status_notes TEXT, updated_at TEXT)"
    )
    conn.commit()
    conn.close()
    return sup, db_path


def _seed(db_path, rows):
    """rows: list of (po_number, dept_code, agency_code, supplier, grand_total, lines)."""
    conn = sqlite3.connect(db_path)
    for po_num, dept_code, agency_code, supplier, total, lines in rows:
        cur = conn.execute(
            "INSERT INTO scprs_po_master "
            "(po_number, dept_code, dept_name, agency_code, supplier, grand_total) "
            "VALUES (?,?,?,?,?,?)",
            (po_num, dept_code, dept_code, agency_code, supplier, total),
        )
        po_id = cur.lastrowid
        for j, (desc, qty, unit_price, line_total, sells, opp) in enumerate(lines):
            conn.execute(
                "INSERT INTO scprs_po_lines "
                "(po_id, po_number, line_num, description, quantity, unit_price, "
                "line_total, reytech_sells, opportunity_flag) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (po_id, po_num, j, desc, qty, unit_price, line_total, sells, opp),
            )
    conn.commit()
    conn.close()


def test_intelligence_unfiltered_returns_populated_shape(scprs_db):
    sup, db_path = scprs_db
    _seed(db_path, [
        ("PO-CCHCS-1", "4700", "4700", "Medline Industries", 5000.0, [
            ("nitrile exam gloves M box 100", 50, 8.00, 400.0, 1, "WIN_BACK"),
            ("ABD pads 5x9 sterile",          20, 0.40,   8.0, 0, "GAP_ITEM"),
        ]),
        ("PO-CDCR-1", "5225", "5225", "Cardinal Health",     3200.0, [
            ("nitrile exam gloves L box 100", 30, 8.50, 255.0, 1, "WIN_BACK"),
            ("disinfectant wipes",            10, 5.00,  50.0, 0, "GAP_ITEM"),
        ]),
    ])

    intel = sup.get_universal_intelligence()

    # Shape — these keys MUST be present even when SQL fails silently.
    # The pre-fix bug returned empty defaults; this test fails if the
    # function regresses to that shape.
    assert set(intel.keys()) >= {
        "totals", "by_agency", "gap_items", "win_back",
        "competitors", "auto_closed_quotes", "summary",
    }

    # Totals — proves the FROM scprs_po_master query parses & returns rows.
    assert intel["totals"]["po_count"] == 2
    assert intel["totals"]["total_spend"] == 8200.0
    assert intel["totals"]["agency_count"] == 2

    # By-agency — proves the JOIN scprs_po_lines query works.
    by_agency = {row["dept_code"]: row for row in intel["by_agency"]}
    assert set(by_agency) == {"4700", "5225"}

    # Gap items — proves opportunity_flag='GAP_ITEM' query works.
    gap_descs = {g["description"] for g in intel["gap_items"]}
    assert "ABD pads 5x9 sterile" in gap_descs
    assert "disinfectant wipes" in gap_descs

    # Win-back — proves opportunity_flag='WIN_BACK' query works.
    wb_descs = {w["description"] for w in intel["win_back"]}
    assert "nitrile exam gloves M box 100" in wb_descs
    assert "nitrile exam gloves L box 100" in wb_descs

    # Summary — proves the totals aggregator runs.
    assert intel["summary"]["total_market_spend"] == 8200.0
    assert intel["summary"]["agencies_tracked"] == 2


def test_intelligence_agency_filter_actually_filters(scprs_db):
    """
    Proves the `where` clause substitution works AND filters correctly.
    Pre-fix, the literal `" + where + "` text broke the SQL regardless of
    filter — a regression that returns unfiltered totals would also pass
    test #1, so this is the second guard.
    """
    sup, db_path = scprs_db
    _seed(db_path, [
        ("PO-CCHCS-1", "4700", "4700", "Medline", 5000.0, [
            ("nitrile gloves", 50, 8.00, 400.0, 1, "WIN_BACK"),
        ]),
        ("PO-CDCR-1", "5225", "5225", "Cardinal", 3200.0, [
            ("disinfectant wipes", 10, 5.00, 50.0, 0, "GAP_ITEM"),
        ]),
    ])

    intel_cchcs = sup.get_universal_intelligence(agency_code="4700")
    assert intel_cchcs["totals"]["po_count"] == 1
    assert intel_cchcs["totals"]["total_spend"] == 5000.0
    assert {a["dept_code"] for a in intel_cchcs["by_agency"]} == {"4700"}

    intel_cdcr = sup.get_universal_intelligence(agency_code="5225")
    assert intel_cdcr["totals"]["po_count"] == 1
    assert intel_cdcr["totals"]["total_spend"] == 3200.0
    assert {a["dept_code"] for a in intel_cdcr["by_agency"]} == {"5225"}


def test_intelligence_does_not_raise_on_empty_db(scprs_db):
    """Empty DB should return empty-but-valid shape, not raise."""
    sup, _ = scprs_db
    intel = sup.get_universal_intelligence()
    assert intel["totals"].get("po_count", 0) == 0
    assert intel["by_agency"] == []
    assert intel["gap_items"] == []
    assert intel["win_back"] == []
    assert intel["summary"]["total_market_spend"] == 0


def test_intelligence_agency_code_is_parameterized_not_inlined(scprs_db):
    """
    Locks the parameterization forever. A future "fix all SQL injection"
    sweep that inlines `agency_code` into the f-string instead of binding
    it via `?` would make this test fail (a SQLi payload would either
    return all rows or raise). Treated as a literal value: 0 rows.
    """
    sup, db_path = scprs_db
    _seed(db_path, [
        ("PO-CCHCS-1", "4700", "4700", "Medline", 5000.0, [
            ("nitrile gloves", 50, 8.00, 400.0, 1, "WIN_BACK"),
        ]),
    ])

    intel = sup.get_universal_intelligence(agency_code="4700' OR 1=1--")
    assert intel["totals"].get("po_count", 0) == 0
    assert intel["by_agency"] == []
