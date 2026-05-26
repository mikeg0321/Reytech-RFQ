"""Pin: /api/intel/pricing-impact-report flags quotes priced above SCPRS
ceilings during the SCPRS-silent window.

Chrome MCP audit 2026-05-26 anomaly #9 Phase 2: for the 25-day window
where SCPRS data went stale, our bids priced against ceiling data
that hadn't moved. This endpoint compares sent quotes against the
now-fresh SCPRS catalog and flags lines where our unit price was
≥ 15% above the state-paid ceiling.

Tests pin:
  1. Endpoint accepts from_date / to_date / threshold_pct.
  2. Quotes outside the window are not in the response.
  3. Quotes with status not in {sent,won,lost} are excluded.
  4. Lines without SCPRS match are counted but not flagged.
  5. Lines within threshold are counted but not flagged.
  6. Lines above threshold are flagged with correct gap math.
  7. Revenue-at-risk aggregates correctly.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta


def _seed_quote(conn, quote_number, sent_at, status="sent",
                line_items=None, agency="CCHCS", total=0):
    items_json = json.dumps(line_items or [])
    conn.execute("DELETE FROM quotes WHERE quote_number=?", (quote_number,))
    conn.execute(
        "INSERT INTO quotes "
        "(quote_number, created_at, agency, institution, total, "
        " items_detail, status, sent_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (quote_number, sent_at, agency, "CHCF", total, items_json,
         status, sent_at),
    )


def _seed_scprs_catalog(conn, description, unit_price, qty=1,
                       mfg_number="", supplier="ACME CORP",
                       times_seen=5, last_date="2026-04-01"):
    """Seed scprs_catalog so _search_scprs_catalog can find a match."""
    # Ensure table exists with the columns the helper queries.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scprs_catalog (
            id INTEGER PRIMARY KEY,
            description TEXT, mfg_number TEXT, last_unit_price REAL,
            last_quantity REAL, last_uom TEXT, last_supplier TEXT,
            last_department TEXT, last_date TEXT, times_seen INTEGER
        )
    """)
    conn.execute(
        "INSERT INTO scprs_catalog "
        "(description, mfg_number, last_unit_price, last_quantity, "
        " last_uom, last_supplier, last_department, last_date, times_seen) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (description, mfg_number.lower(), unit_price, qty, "EA",
         supplier, "CDCR", last_date, times_seen),
    )


def _purge(conn):
    conn.execute("DELETE FROM quotes")
    try:
        conn.execute("DELETE FROM scprs_catalog")
    except Exception:
        pass


# ─── Endpoint shape ──────────────────────────────────────────────────


def test_endpoint_returns_summary_and_flagged_quotes(auth_client):
    from src.core.db import get_db
    with get_db() as conn:
        _purge(conn)

    r = auth_client.get(
        "/api/intel/pricing-impact-report?from_date=2026-05-01&to_date=2026-05-26"
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["ok"] is True
    assert "summary" in data
    assert "flagged_quotes" in data
    assert isinstance(data["flagged_quotes"], list)
    for k in ("from_date", "to_date", "threshold_pct",
              "quotes_in_window", "quotes_flagged",
              "lines_total", "lines_with_scprs_match",
              "lines_flagged", "total_revenue_at_risk"):
        assert k in data["summary"], f"missing summary field {k}"


def test_quotes_outside_window_excluded(auth_client):
    from src.core.db import get_db
    with get_db() as conn:
        _purge(conn)
        _seed_quote(conn, "Q-OLD", "2026-04-01T10:00:00",
                    line_items=[{"description": "gloves", "unit_price": 50, "quantity": 1}])
        _seed_quote(conn, "Q-IN", "2026-05-10T10:00:00",
                    line_items=[{"description": "gloves", "unit_price": 50, "quantity": 1}])

    r = auth_client.get(
        "/api/intel/pricing-impact-report?from_date=2026-05-01&to_date=2026-05-26"
    )
    data = r.get_json()
    assert data["summary"]["quotes_in_window"] == 1, (
        "out-of-window quote was included"
    )


def test_non_sent_status_excluded(auth_client):
    """Drafts, void, dismissed quotes should not appear."""
    from src.core.db import get_db
    with get_db() as conn:
        _purge(conn)
        _seed_quote(conn, "Q-DRAFT", "2026-05-10T10:00:00", status="draft",
                    line_items=[{"description": "gloves", "unit_price": 50, "quantity": 1}])
        _seed_quote(conn, "Q-SENT", "2026-05-10T10:00:00", status="sent",
                    line_items=[{"description": "gloves", "unit_price": 50, "quantity": 1}])
        _seed_quote(conn, "Q-VOID", "2026-05-10T10:00:00", status="void",
                    line_items=[{"description": "gloves", "unit_price": 50, "quantity": 1}])

    r = auth_client.get(
        "/api/intel/pricing-impact-report?from_date=2026-05-01&to_date=2026-05-26"
    )
    data = r.get_json()
    assert data["summary"]["quotes_in_window"] == 1


def test_flags_line_above_threshold(auth_client):
    """Our unit_price=$100, SCPRS last_unit_price=$50 → 100% gap, flagged."""
    from src.core.db import get_db
    with get_db() as conn:
        _purge(conn)
        _seed_scprs_catalog(
            conn,
            description="nitrile examination gloves medium",
            unit_price=50.0, qty=1, times_seen=10,
        )
        _seed_quote(conn, "Q-OVERPRICED", "2026-05-10T10:00:00", status="sent",
                    line_items=[{
                        "description": "nitrile examination gloves medium",
                        "unit_price": 100.0,
                        "quantity": 5,
                    }])

    r = auth_client.get(
        "/api/intel/pricing-impact-report?from_date=2026-05-01&to_date=2026-05-26"
    )
    data = r.get_json()
    assert data["summary"]["quotes_flagged"] == 1
    assert data["summary"]["lines_flagged"] == 1
    assert data["summary"]["total_revenue_at_risk"] == 500.0  # 100*5

    fq = data["flagged_quotes"][0]
    assert fq["quote_number"] == "Q-OVERPRICED"
    assert fq["max_gap_pct"] == 100.0
    line = fq["flagged_lines"][0]
    assert line["our_unit_price"] == 100.0
    assert line["scprs_unit_price"] == 50.0
    assert line["gap_pct"] == 100.0


def test_within_threshold_not_flagged(auth_client):
    """Our $54, SCPRS $50 → 8% gap, BELOW the 15% threshold, not flagged."""
    from src.core.db import get_db
    with get_db() as conn:
        _purge(conn)
        _seed_scprs_catalog(
            conn,
            description="nitrile examination gloves medium",
            unit_price=50.0, qty=1, times_seen=10,
        )
        _seed_quote(conn, "Q-FAIR", "2026-05-10T10:00:00", status="sent",
                    line_items=[{
                        "description": "nitrile examination gloves medium",
                        "unit_price": 54.0,
                        "quantity": 1,
                    }])

    r = auth_client.get(
        "/api/intel/pricing-impact-report?from_date=2026-05-01&to_date=2026-05-26"
    )
    data = r.get_json()
    assert data["summary"]["lines_with_scprs_match"] == 1
    assert data["summary"]["lines_flagged"] == 0
    assert data["summary"]["quotes_flagged"] == 0


def test_no_scprs_match_counted_not_flagged(auth_client):
    """A line item with no SCPRS match advances lines_total but not
    lines_with_scprs_match — distinguishable in the summary."""
    from src.core.db import get_db
    with get_db() as conn:
        _purge(conn)
        # No scprs_catalog rows seeded.
        _seed_quote(conn, "Q-UNK", "2026-05-10T10:00:00", status="sent",
                    line_items=[{
                        "description": "exotic widget xyz",
                        "unit_price": 99.0,
                        "quantity": 1,
                    }])

    r = auth_client.get(
        "/api/intel/pricing-impact-report?from_date=2026-05-01&to_date=2026-05-26"
    )
    data = r.get_json()
    assert data["summary"]["lines_total"] == 1
    assert data["summary"]["lines_with_scprs_match"] == 0
    assert data["summary"]["lines_flagged"] == 0


def test_threshold_param_overrides_default(auth_client):
    """A higher threshold lets a 50%-gap line slip through unflagged."""
    from src.core.db import get_db
    with get_db() as conn:
        _purge(conn)
        _seed_scprs_catalog(
            conn, description="widget alpha",
            unit_price=50.0, qty=1, times_seen=10,
        )
        _seed_quote(conn, "Q-MID", "2026-05-10T10:00:00", status="sent",
                    line_items=[{
                        "description": "widget alpha",
                        "unit_price": 75.0,  # 50% gap
                        "quantity": 1,
                    }])

    # Default 15% threshold → flagged
    r = auth_client.get(
        "/api/intel/pricing-impact-report?from_date=2026-05-01&to_date=2026-05-26"
    )
    assert r.get_json()["summary"]["lines_flagged"] == 1

    # 75% threshold → not flagged (50% < 75%)
    r2 = auth_client.get(
        "/api/intel/pricing-impact-report?from_date=2026-05-01&to_date=2026-05-26&threshold_pct=75"
    )
    assert r2.get_json()["summary"]["lines_flagged"] == 0
