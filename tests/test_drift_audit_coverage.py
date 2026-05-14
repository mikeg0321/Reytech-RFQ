"""PR-AQ — drift-logging audit-coverage diagnostic.

PR-AP shipped a diagnostic showing operator_drift_line is empty in
prod. /admin/funnel shows 4 sent PCs in the last 30 days. So
`log_operator_drift` fires on Mark-Sent but produces zero rows.

The function skips lines two ways:
  - `skipped_no_audit`: item has no `oracle_audit` (or empty dict)
  - `skipped_no_price`: item has no positive unit_price/bid_price/
    price_per_unit

This endpoint walks every sent PC/RFQ and reports coverage so the
PR-AR substrate fix targets the right gate.

Tests pin:
  1. Sent record with audit + price on every item → items_would_log
     == items_total. Coverage 100% / 100%.
  2. Sent record with audit MISSING on every item → audit coverage
     0% / price 100% / would_log 0. Identifies the audit-writer gap.
  3. Sent record with price MISSING on every item → audit 100% /
     price 0% / would_log 0. Identifies the price-timing gap.
  4. Non-sent records (parsed/needs_review) excluded from the walk.
"""
from __future__ import annotations


def test_audit_coverage_full(client, temp_data_dir):
    """Every item has both audit and price → would_log == total."""
    from src.api.dashboard import _save_single_pc

    pc = {
        "id": "pc_aq_full",
        "status": "sent",
        "pc_number": "TEST-AQ-FULL",
        "agency": "cchcs",
        "sent_at": "2026-05-13T10:00:00",
        "items": [
            {"description": "Widget A", "qty": 1,
             "unit_price": 10.0, "oracle_audit": {"rec_price": 9.0}},
            {"description": "Widget B", "qty": 2,
             "unit_price": 20.0, "oracle_audit": {"rec_price": 18.0}},
        ],
    }
    _save_single_pc("pc_aq_full", pc)

    resp = client.get("/api/admin/operator-drift-audit-coverage")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    pc_sum = body["pc"]
    assert pc_sum["sent_count"] >= 1
    # Find our specific record
    rec = next((r for r in pc_sum["per_record"] if r["id"] == "pc_aq_full"), None)
    assert rec is not None
    assert rec["items_total"] == 2
    assert rec["items_with_audit"] == 2
    assert rec["items_with_price"] == 2
    assert rec["items_would_log"] == 2


def test_audit_coverage_audit_missing(client, temp_data_dir):
    """Items have price but no oracle_audit → audit-writer gap."""
    from src.api.dashboard import _save_single_pc

    pc = {
        "id": "pc_aq_no_audit",
        "status": "sent",
        "pc_number": "TEST-AQ-NOAUDIT",
        "agency": "cchcs",
        "sent_at": "2026-05-13T10:00:00",
        "items": [
            {"description": "Widget", "qty": 1, "unit_price": 10.0},
            # no oracle_audit at all
        ],
    }
    _save_single_pc("pc_aq_no_audit", pc)

    resp = client.get("/api/admin/operator-drift-audit-coverage")
    assert resp.status_code == 200
    body = resp.get_json()
    rec = next((r for r in body["pc"]["per_record"] if r["id"] == "pc_aq_no_audit"), None)
    assert rec is not None
    assert rec["items_with_audit"] == 0
    assert rec["items_with_price"] == 1
    assert rec["items_would_log"] == 0


def test_audit_coverage_price_missing(client, temp_data_dir):
    """Items have audit but no positive price → price-timing gap."""
    from src.api.dashboard import _save_single_pc

    pc = {
        "id": "pc_aq_no_price",
        "status": "sent",
        "pc_number": "TEST-AQ-NOPRICE",
        "agency": "cchcs",
        "sent_at": "2026-05-13T10:00:00",
        "items": [
            {"description": "Widget", "qty": 1, "unit_price": 0.0,
             "oracle_audit": {"rec_price": 9.0}},
        ],
    }
    _save_single_pc("pc_aq_no_price", pc)

    resp = client.get("/api/admin/operator-drift-audit-coverage")
    assert resp.status_code == 200
    body = resp.get_json()
    rec = next((r for r in body["pc"]["per_record"] if r["id"] == "pc_aq_no_price"), None)
    assert rec is not None
    assert rec["items_with_audit"] == 1
    assert rec["items_with_price"] == 0
    assert rec["items_would_log"] == 0


def test_audit_coverage_excludes_non_sent(client, temp_data_dir):
    """parsed/needs_review/draft records are NOT walked."""
    from src.api.dashboard import _save_single_pc

    pc = {
        "id": "pc_aq_parsed",
        "status": "parsed",  # not sent
        "pc_number": "TEST-AQ-PARSED",
        "agency": "cchcs",
        "items": [
            {"description": "Widget", "qty": 1, "unit_price": 10.0,
             "oracle_audit": {"rec_price": 9.0}},
        ],
    }
    _save_single_pc("pc_aq_parsed", pc)

    resp = client.get("/api/admin/operator-drift-audit-coverage")
    assert resp.status_code == 200
    body = resp.get_json()
    rec = next((r for r in body["pc"]["per_record"] if r["id"] == "pc_aq_parsed"), None)
    assert rec is None  # non-sent records skipped


def test_audit_coverage_aggregate_pct(client, temp_data_dir):
    """Coverage percentages aggregate across PC + RFQ buckets."""
    from src.api.dashboard import _save_single_pc, _save_single_rfq

    # 1 PC with full coverage, 1 RFQ with no audit
    pc = {
        "id": "pc_aq_agg_pc",
        "status": "sent",
        "pc_number": "TEST-AQ-AGG-PC",
        "agency": "cchcs",
        "items": [
            {"description": "A", "qty": 1, "unit_price": 5.0,
             "oracle_audit": {"rec_price": 4.0}},
            {"description": "B", "qty": 1, "unit_price": 7.0,
             "oracle_audit": {"rec_price": 6.0}},
        ],
    }
    _save_single_pc("pc_aq_agg_pc", pc)

    rfq = {
        "id": "rfq_aq_agg_rfq",
        "status": "sent",
        "rfq_number": "TEST-AQ-AGG-RFQ",
        "agency": "calvet",
        "line_items": [
            {"description": "C", "qty": 1, "unit_price": 10.0},
            # no audit
        ],
    }
    _save_single_rfq("rfq_aq_agg_rfq", rfq)

    resp = client.get("/api/admin/operator-drift-audit-coverage")
    body = resp.get_json()
    agg = body["aggregate"]
    # Total items across both: 3 — 2 with audit, 3 with price
    assert agg["items_total"] >= 3
    assert agg["audit_coverage_pct"] is not None
    assert agg["price_coverage_pct"] is not None
