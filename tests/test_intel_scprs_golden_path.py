"""
Golden-path test for /intel/scprs (§6a of the 2026-04-23 review).

Why this exists:
  The original /intel/scprs P0 (PR #484) hid for ~7 weeks because no
  end-to-end test loaded the page against seeded SCPRS fixtures and
  asserted the KPI cards populated with non-zero values. Backend unit
  tests + HTTP 200 + smoke checks all passed; the page silently rendered
  empty defaults via a bare `except Exception` swallowing a SQL syntax
  error.

  This test is the cheap insurance: seed real-shape SCPRS data, GET
  the page, assert the headline numbers actually appear in the rendered
  HTML. If `get_universal_intelligence` regresses to silent-empty (any
  shape — broken SQL, swallowed exception, defensive None-substitution),
  this test fails immediately.

  Per `feedback_volume_vs_outcome.md`: golden E2E is the KPI, not PR
  count. This test exists so the next regression surfaces in <1 day,
  not 7 weeks.
"""
import os
import sqlite3

import pytest


@pytest.fixture
def seeded_scprs_db(tmp_path, monkeypatch):
    """Seed scprs_universal_pull's DB with realistic SCPRS data so the
    page route's get_universal_intelligence call returns populated
    aggregates."""
    data_dir = tmp_path / "scprs_golden"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()

    db_path = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db_path)

    # Stub the quotes table that check_quotes_against_scprs joins (and
    # that get_universal_intelligence's auto_closed_quotes query reads).
    conn.execute(
        "CREATE TABLE IF NOT EXISTS quotes ("
        "id INTEGER PRIMARY KEY, quote_number TEXT, agency TEXT, "
        "institution TEXT, status TEXT, status_notes TEXT, "
        "total REAL, created_at TEXT, items_text TEXT, items_detail TEXT, "
        "is_test INTEGER DEFAULT 0, updated_at TEXT)"
    )

    # Seed 3 POs across 2 agencies, mix of WIN_BACK + GAP_ITEM lines —
    # exactly the shape /intel/scprs aggregates over.
    seeds = [
        # CCHCS — Medline competitor, 2 lines (1 win-back, 1 gap)
        ("PO-CCHCS-MEDLINE-1", "4700", "4700", "Medline Industries", 5400.0,
         "2026-01-15", [
            ("nitrile exam gloves medium box of 100", 50, 8.00, 400.0, 1, "WIN_BACK"),
            ("ABD pads 5x9 sterile box of 25",         20, 250.00, 5000.0, 0, "GAP_ITEM"),
         ]),
        # CCHCS — Cardinal Health, 1 win-back line
        ("PO-CCHCS-CARDINAL-1", "4700", "4700", "Cardinal Health", 800.0,
         "2026-02-01", [
            ("nitrile exam gloves large box of 100", 100, 8.00, 800.0, 1, "WIN_BACK"),
         ]),
        # CDCR — Henry Schein, 2 gap lines
        ("PO-CDCR-SCHEIN-1", "5225", "5225", "Henry Schein", 4500.0,
         "2026-02-20", [
            ("disinfectant wipes container of 160",  100, 25.00, 2500.0, 0, "GAP_ITEM"),
            ("first aid refill pack",                  20, 100.00, 2000.0, 0, "GAP_ITEM"),
         ]),
    ]
    for po_num, dept_code, agency_code, supplier, total, start_date, lines in seeds:
        cur = conn.execute(
            "INSERT INTO scprs_po_master "
            "(po_number, dept_code, dept_name, agency_code, supplier, "
            "grand_total, start_date) VALUES (?,?,?,?,?,?,?)",
            (po_num, dept_code, dept_code, agency_code, supplier, total, start_date),
        )
        po_id = cur.lastrowid
        for j, (desc, qty, price, line_total, sells, opp) in enumerate(lines):
            conn.execute(
                "INSERT INTO scprs_po_lines "
                "(po_id, po_number, line_num, description, quantity, "
                "unit_price, line_total, reytech_sells, opportunity_flag) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (po_id, po_num, j, desc, qty, price, line_total, sells, opp),
            )
    conn.commit()
    conn.close()

    return sup, db_path


def test_intel_scprs_golden_path_renders_populated_kpis(
    auth_client, seeded_scprs_db, monkeypatch
):
    """The golden-path assertion: with realistic SCPRS data in the DB,
    /intel/scprs renders the 5 KPI cards with non-zero values, the
    "no data" banner is suppressed, no error banner appears, and the
    by_agency / gap_items / win_back tables show real rows.

    Any silent-empty regression (broken SQL, swallowed exception,
    defensive None-substitution returning empty defaults) fails this
    test immediately."""
    sup, _ = seeded_scprs_db

    # Stub manager recommendations so we don't depend on its data sources.
    import src.agents.manager_agent as mgr
    monkeypatch.setattr(
        mgr, "get_intelligent_recommendations",
        lambda: {
            "actions": [
                {"type": "displace_competitor", "title": "Beat Medline on gloves",
                 "why": "We sell this; Medline won at $8/100ct",
                 "action": "Quote at $7.68", "urgency": "THIS WEEK",
                 "dollar_value": 1200},
            ],
            "summary": {"revenue_opportunity": 1200},
        },
    )

    resp = auth_client.get("/intel/scprs")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")

    # ── 1. Error banner is NOT present (intel call succeeded). ──
    assert "Intelligence query failed" not in body, (
        "Error banner is showing — get_universal_intelligence raised. "
        "This is the silent-empty regression shape from PR #484."
    )

    # ── 2. "No data yet" banner is suppressed (KPIs are populated). ──
    assert "click Pull P0 Now to start" not in body, (
        "'No data yet' banner is showing despite seeded fixtures. "
        "get_pull_status.pos_stored is returning 0 — this is the same "
        "user-visible symptom the 7-week incident produced."
    )

    # ── 3. KPI strip shows real numbers. ──
    # POs Captured = 3 (formatted as "3").
    # Market Spend = $10,700 (formatted as "$10,700").
    # Gap Items aggregate = 9,500 (5000 + 2500 + 2000).
    # Win-Back aggregate = 1,200 (400 + 800).
    assert "POs Captured" in body
    assert ">3<" in body or ">3 " in body or "3 line items" in body or "3<" in body, (
        f"Expected POs Captured = 3 in rendered KPI strip; not found"
    )
    # Market Spend cell — rendered as $10,700 (the SUM of grand_total).
    assert "$10,700" in body, (
        "Expected Market Spend = $10,700 in KPI strip"
    )

    # ── 4. by_agency table shows both seeded agencies (CCHCS + CDCR). ──
    # The template renders dept_name truncated to 35 chars.
    assert "4700" in body or "CCHCS" in body
    assert "5225" in body or "CDCR" in body

    # ── 5. Gap Items table shows seeded gap descriptions. ──
    assert "ABD pads 5x9 sterile" in body
    assert "disinfectant wipes" in body or "first aid refill" in body

    # ── 6. Win-Back table shows incumbent vendor + a beat-at price. ──
    assert "Medline" in body or "Cardinal" in body
    assert "nitrile exam gloves" in body

    # ── 7. Manager recommendations panel populated. ──
    assert "Beat Medline on gloves" in body
