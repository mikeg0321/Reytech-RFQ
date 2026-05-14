"""PR-AR — drift-audit synthesizer + retroactive backfill.

PR-AQ confirmed 0/235 sent items carry `oracle_audit`. The canonical
writer (`_build_oracle_audit`) only fires in pc_enrichment_pipeline +
pc_rfq_reprice_adapter — neither runs on the operator's actual quote-
pricing path (autosave / URL-paste / manual). The Oracle DID suggest
a price somewhere along the way and got persisted as
`pricing.recommended_price` / `oracle_price` etc., but the audit
envelope wasn't written.

PR-AR fixes this by:
  1. `_synthesize_oracle_audit(item, snapshot_at)` builds a minimal
     audit envelope from existing Oracle-suggested fields.
  2. `log_operator_drift` calls the synthesizer when oracle_audit
     is absent — converting `skipped_no_audit` → logged row.
  3. POST /api/admin/heal-drift-backfill retroactively logs drift
     for already-sent records.

Tests pin:
  1. Synthesizer: returns dict when pricing.recommended_price set.
  2. Synthesizer: returns dict when pricing.oracle_price set.
  3. Synthesizer: returns dict when item.oracle_price set (top-level).
  4. Synthesizer: returns None when none of the three are set.
  5. log_operator_drift now produces rows from items WITHOUT
     oracle_audit but WITH pricing.recommended_price.
  6. Heal backfill dry_run reports counts without inserting.
  7. Heal backfill real-run inserts rows.
"""
from __future__ import annotations


# ── Synthesizer unit tests ──────────────────────────────────────────


def test_synthesize_audit_from_recommended_price():
    """pricing.recommended_price > 0 yields a synthesized audit."""
    from src.core.operator_kpi import _synthesize_oracle_audit

    item = {
        "description": "Widget",
        "unit_price": 100.0,
        "pricing": {"recommended_price": 85.0},
    }
    audit = _synthesize_oracle_audit(item, "2026-05-14T20:30:00")
    assert audit is not None
    assert audit["rec_price"] == 85.0
    assert audit["oracle_version"] == "synthesized_at_log"
    assert audit["caps_applied"] == []
    assert audit["snapshot_at"] == "2026-05-14T20:30:00"


def test_synthesize_audit_from_pricing_oracle_price():
    """pricing.oracle_price > 0 yields a synthesized audit when
    pricing.recommended_price is absent."""
    from src.core.operator_kpi import _synthesize_oracle_audit

    item = {
        "description": "Widget",
        "unit_price": 100.0,
        "pricing": {"oracle_price": 90.0},
    }
    audit = _synthesize_oracle_audit(item, "2026-05-14T20:30:00")
    assert audit is not None
    assert audit["rec_price"] == 90.0


def test_synthesize_audit_from_top_level_oracle_price():
    """item.oracle_price (top-level) yields a synthesized audit."""
    from src.core.operator_kpi import _synthesize_oracle_audit

    item = {
        "description": "Widget",
        "unit_price": 100.0,
        "oracle_price": 80.0,
    }
    audit = _synthesize_oracle_audit(item, "2026-05-14T20:30:00")
    assert audit is not None
    assert audit["rec_price"] == 80.0


def test_synthesize_audit_returns_none_when_no_oracle_suggestion():
    """No Oracle-suggested fields → None. Drift logger falls through
    to skipped_no_audit (we don't fabricate drift from nothing)."""
    from src.core.operator_kpi import _synthesize_oracle_audit

    item = {
        "description": "Widget",
        "unit_price": 100.0,
        # No pricing.recommended_price, no oracle_price anywhere
    }
    assert _synthesize_oracle_audit(item, "2026-05-14T20:30:00") is None


def test_synthesize_audit_returns_none_on_non_positive():
    """Oracle-suggested price of 0 or negative is not a valid signal."""
    from src.core.operator_kpi import _synthesize_oracle_audit

    item = {
        "description": "Widget",
        "unit_price": 100.0,
        "pricing": {"recommended_price": 0},
    }
    assert _synthesize_oracle_audit(item, "2026-05-14T20:30:00") is None


# ── Integration: log_operator_drift uses synthesizer ────────────────


def test_log_operator_drift_synthesizes_when_audit_missing(client, temp_data_dir):
    """An item WITHOUT oracle_audit but WITH pricing.recommended_price
    now produces a logged row (was skipped_no_audit pre-PR-AR)."""
    from src.core.operator_kpi import log_operator_drift

    items = [
        {
            "description": "Widget A",
            "item_number": "WID-001",
            "unit_price": 100.0,
            "pricing": {"recommended_price": 85.0},
            # NO oracle_audit
        },
    ]
    result = log_operator_drift(
        quote_id="test_synth_pos",
        quote_type="pc",
        items=items,
        agency_key="cchcs",
    )
    assert result["ok"] is True
    assert result["rows_logged"] == 1
    assert result["synthesized_audits"] == 1
    assert result["skipped_no_audit"] == 0


def test_log_operator_drift_still_skips_when_no_oracle_data(client, temp_data_dir):
    """An item with neither oracle_audit nor any Oracle-suggested
    price still skips — synthesizer doesn't fabricate."""
    from src.core.operator_kpi import log_operator_drift

    items = [
        {
            "description": "Widget A",
            "unit_price": 100.0,
            # Nothing to synthesize from
        },
    ]
    result = log_operator_drift(
        quote_id="test_synth_neg",
        quote_type="pc",
        items=items,
        agency_key="cchcs",
    )
    assert result["rows_logged"] == 0
    assert result["skipped_no_audit"] == 1
    assert result["synthesized_audits"] == 0


# ── Heal backfill route ─────────────────────────────────────────────


def test_heal_drift_backfill_dry_run(client, temp_data_dir):
    """dry_run=true reports counts but doesn't insert."""
    from src.api.dashboard import _save_single_pc

    pc = {
        "id": "pc_ar_dry",
        "status": "sent",
        "pc_number": "TEST-AR-DRY",
        "agency": "cchcs",
        "items": [
            {"description": "A", "unit_price": 100.0,
             "pricing": {"recommended_price": 85.0}},
        ],
    }
    _save_single_pc("pc_ar_dry", pc)

    resp = client.post("/api/admin/heal-drift-backfill",
                       json={"dry_run": True})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["dry_run"] is True
    # Should have found the sent record
    assert body["records_scanned"] >= 1
    # Our test record should be in summary because it has an
    # Oracle-suggested price
    assert "pc_ar_dry" in body["summary"]


def test_heal_drift_backfill_real_inserts(client, temp_data_dir):
    """dry_run=false actually inserts rows."""
    from src.api.dashboard import _save_single_pc
    from src.core.db import get_db

    # Clean slate for this test's rows.
    with get_db() as conn:
        conn.execute("DELETE FROM operator_drift_line WHERE quote_id = ?",
                     ("pc_ar_real",))

    pc = {
        "id": "pc_ar_real",
        "status": "sent",
        "pc_number": "TEST-AR-REAL",
        "agency": "calvet",
        "items": [
            {"description": "A", "unit_price": 50.0,
             "pricing": {"recommended_price": 40.0}},
            {"description": "B", "unit_price": 30.0,
             "pricing": {"recommended_price": 25.0}},
        ],
    }
    _save_single_pc("pc_ar_real", pc)

    resp = client.post("/api/admin/heal-drift-backfill",
                       json={"dry_run": False})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["rows_inserted"] >= 2
    assert body["synthesized_audits_total"] >= 2

    # Verify rows actually landed
    with get_db() as conn:
        rows = conn.execute(
            "SELECT COUNT(*) AS c FROM operator_drift_line "
            "WHERE quote_id = ?",
            ("pc_ar_real",),
        ).fetchone()
        assert rows["c"] >= 2
