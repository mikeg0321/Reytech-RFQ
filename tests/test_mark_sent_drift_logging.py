"""PR-U — Mark-Sent drift logging fix.

The 2026-05-13 quoting walkthrough audit found that drift logging only
fired on `/api/pricecheck/<id>/send-quote` (Mike doesn't use), while
the canonical operator path is `/api/pricecheck/<id>/mark-sent` +
`/mark-sent-manually` — neither of which called log_operator_drift or
log_operator_drift_shadow. Result: every week of operator activity
left operator_drift_line empty, so PR-S auto-recommendations had near-
zero input.

Pinned guarantees:
  1. `fire_drift_logs_on_send` wraps both log calls, never raises.
  2. /api/pricecheck/<id>/mark-sent fires drift logs after the status
     flip.
  3. /api/pricecheck/<id>/mark-sent-manually fires drift logs too.
  4. A log failure does NOT block the mark-sent — the status flip
     succeeds regardless.
"""
from __future__ import annotations

import os
import sqlite3
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _seed_pc_with_oracle_audit(temp_data_dir, pcid="pc_drift_test"):
    """Seed a PC with one priced item that has oracle_audit, so
    log_operator_drift actually writes a row."""
    from src.api.data_layer import _save_single_pc
    pc = {
        "id": pcid,
        "pc_number": "DRIFT-TEST",
        "status": "draft",
        "agency": "cchcs",
        "items": [{
            "description": "Test item",
            "qty": 1,
            "quantity": 1,
            "unit_price": 100.0,
            "unit_cost": 60.0,
            "vendor_cost": 60.0,
            "item_number": "TEST-1",
            "oracle_audit": {
                "rec_price": 95.0,
                "rec_pre_cap_price": 120.0,
                "caps_applied": [{
                    "source": "scprs_rollup",
                    "cap_price": 95.0,
                    "delta_pct": 26.3,
                }],
                "scprs_rollup": {
                    "count": 8,
                    "p50": 90.0,
                    "p75": 95.0,
                    "p90": 105.0,
                    "match_key": "TEST-1",
                    "match_key_type": "mfg",
                },
                "oracle_version": "v2.1",
            },
        }],
    }
    _save_single_pc(pcid, pc)
    return pcid


# ── Helper ────────────────────────────────────────────────────────


def test_fire_drift_logs_on_send_returns_dict(temp_data_dir):
    """The helper must never raise. Empty PC dict → ok=False return."""
    from src.core.operator_kpi import fire_drift_logs_on_send
    r = fire_drift_logs_on_send("", "pc", {})
    assert r.get("ok") is False


def test_fire_drift_logs_on_send_writes_rows(temp_data_dir):
    """With a PC that has oracle_audit on its items, the helper writes
    drift rows to operator_drift_line."""
    from src.core.migrations import run_migrations
    run_migrations()
    pcid = _seed_pc_with_oracle_audit(temp_data_dir)
    from src.api.data_layer import _load_price_checks
    pc = _load_price_checks()[pcid]

    from src.core.operator_kpi import fire_drift_logs_on_send
    result = fire_drift_logs_on_send(pcid, "pc", pc)

    assert result["drift"] is not None
    assert result["drift"].get("ok") is True
    assert result["drift"].get("rows_logged") >= 1

    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT quote_id, sent_price, rec_price FROM operator_drift_line "
        "WHERE quote_id=?", (pcid,)
    ).fetchall()
    conn.close()
    assert len(rows) >= 1
    assert rows[0][0] == pcid


# ── /api/pricecheck/<id>/mark-sent ────────────────────────────────


def test_mark_sent_endpoint_fires_drift_log(client, temp_data_dir):
    """Hitting the actual mark-sent route must populate operator_drift_line.
    Pre-PR-U this was empty no matter how many Mark-Sent clicks fired."""
    from src.core.migrations import run_migrations
    run_migrations()
    pcid = _seed_pc_with_oracle_audit(temp_data_dir, pcid="pc_endpoint")

    resp = client.post(
        f"/api/pricecheck/{pcid}/mark-sent",
        json={"notes": "test mark-sent"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert data["status"] == "sent"

    # Assert the drift table got a row
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    n = conn.execute(
        "SELECT COUNT(*) FROM operator_drift_line WHERE quote_id=?",
        (pcid,),
    ).fetchone()[0]
    conn.close()
    assert n >= 1, "mark-sent must fire operator_drift_line write"


def test_mark_sent_endpoint_fires_shadow_log(client, temp_data_dir):
    """Same path also writes operator_drift_shadow for PR-J shadow eval."""
    from src.core.migrations import run_migrations
    run_migrations()
    pcid = _seed_pc_with_oracle_audit(temp_data_dir, pcid="pc_shadow_endpoint")

    resp = client.post(f"/api/pricecheck/{pcid}/mark-sent", json={})
    assert resp.status_code == 200

    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    n = conn.execute(
        "SELECT COUNT(*) FROM operator_drift_shadow WHERE quote_id=?",
        (pcid,),
    ).fetchone()[0]
    conn.close()
    assert n >= 1, "mark-sent must fire operator_drift_shadow write"


# ── /api/pricecheck/<id>/mark-sent-manually ───────────────────────


def test_mark_sent_manually_fires_drift_log(client, temp_data_dir):
    """The manual-sent path (out-of-band email) must also log drift."""
    from src.core.migrations import run_migrations
    run_migrations()
    pcid = _seed_pc_with_oracle_audit(temp_data_dir, pcid="pc_manual")

    resp = client.post(
        f"/api/pricecheck/{pcid}/mark-sent-manually",
        json={"sent_to": "buyer@test.gov",
              "sent_at": "2026-05-13T10:00:00",
              "notes": "out-of-band send"},
    )
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True

    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    n = conn.execute(
        "SELECT COUNT(*) FROM operator_drift_line WHERE quote_id=?",
        (pcid,),
    ).fetchone()[0]
    conn.close()
    assert n >= 1, "mark-sent-manually must fire operator_drift_line write"


# ── Failure isolation ─────────────────────────────────────────────


def test_mark_sent_succeeds_even_if_drift_log_crashes(client, temp_data_dir,
                                                       monkeypatch):
    """A drift-log failure must not block the mark-sent flip — the
    status write is the source of truth."""
    from src.core.migrations import run_migrations
    run_migrations()
    pcid = _seed_pc_with_oracle_audit(temp_data_dir, pcid="pc_crash")

    import src.core.operator_kpi as okpi

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated drift logger crash")

    monkeypatch.setattr(okpi, "fire_drift_logs_on_send", _boom)

    resp = client.post(f"/api/pricecheck/{pcid}/mark-sent", json={})
    assert resp.status_code == 200
    assert resp.get_json()["ok"] is True

    # PC should be marked sent regardless
    from src.api.data_layer import _load_price_checks
    pc = _load_price_checks()[pcid]
    assert pc["status"] == "sent"
