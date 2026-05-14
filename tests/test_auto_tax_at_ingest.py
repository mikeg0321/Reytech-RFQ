"""PR-AI — auto-tax-at-ingest in _create_record.

Mike's 3-month spec: every RFQ produces ONE structured contract
(due_date, buyer, ship_to, tax_rate, agency, sol#, items) ready
before the operator touches the page. Pre-fix every ingested
record landed without `tax_rate` set, surfacing as the ⚠ DEFAULT
jurisdiction warning on every queue row + a manual Verify Tax
click per detail-page load.

Tests pin:
  1. tax_rate is populated as PERCENT (e.g. 8.975) — matches the
     existing autosave conventions at routes_pricecheck.py:2996 +
     routes_rfq.py:3410. Operators see the real local rate the
     moment the record lands.
  2. tax_source / tax_jurisdiction / tax_validated propagate from
     the canonical tax_for_address() facade so the detail-page
     ✅/⚠️ badge can gate correctly without re-lookup.
  3. Short / empty ship_to is skipped (resolved_ship_to gate matches
     the existing >3 chars heuristic).
  4. tax_for_address() raising must NEVER block ingest — record
     still created with zero/empty tax fields and the operator's
     existing Verify Tax button can re-resolve.
  5. tax_for_address() returning a dict with rate=0 (no validation
     succeeded) ALSO doesn't pollute the record — keeps the
     "unresolved" signal that drives the ⚠ DEFAULT badge.

Hermetic: no real CDTFA calls, no real DB. Monkeypatched
tax_for_address + minimal in-memory record creation.
"""
from __future__ import annotations

import pytest


# ── Helpers ─────────────────────────────────────────────────────────


def _make_classification():
    """Minimal RequestClassification stub."""
    from src.core.request_classifier import RequestClassification
    return RequestClassification(
        shape="ams_704_quote",
        agency="cchcs",
        confidence=0.9,
        institution="CSP-SAC",
        solicitation_number="TEST-SOL-001",
    )


def _invoke_create_record(record_type, ship_to_in_header):
    """Run _create_record with a synthesized header carrying ship_to."""
    from src.core.ingest_pipeline import _create_record
    header = {"ship_to": ship_to_in_header}
    items = [{"description": "test item", "quantity": 1, "unit_price": 0}]
    classification = _make_classification()
    rid = _create_record(
        record_type=record_type,
        items=items,
        header=header,
        classification=classification,
        primary_path=None,
        email_subject="test",
        email_sender="test@example.com",
        email_uid="test-uid",
    )
    return rid


def _load_record(rid):
    """Read the just-created record back from the legacy store."""
    if rid.startswith("pc_"):
        from src.api.dashboard import _load_price_checks
        return _load_price_checks().get(rid)
    if rid.startswith("rfq_"):
        from src.api.dashboard import load_rfqs
        return load_rfqs().get(rid)
    raise AssertionError(f"unrecognized record_type prefix: {rid!r}")


# ── Happy path ───────────────────────────────────────────────────────


def test_auto_tax_at_ingest_stamps_percent_format(temp_data_dir, monkeypatch):
    """tax_for_address returns rate=0.08975 (decimal) → record stores
    tax_rate=8.975 (percent), matching autosave convention."""
    monkeypatch.setattr(
        "src.core.quote_contract.tax_for_address",
        lambda addr: {
            "rate": 0.08975, "rate_bps": 897, "jurisdiction": "COALINGA",
            "source": "cdtfa_api", "validated": True, "facility_code": "",
        },
    )
    rid = _invoke_create_record("pc", "100 Prison Rd, Coalinga, CA 93210")
    pc = _load_record(rid)
    assert pc is not None
    assert pc.get("tax_rate") == 8.975
    assert pc.get("tax_jurisdiction") == "COALINGA"
    assert pc.get("tax_source") == "cdtfa_api"
    assert pc.get("tax_validated") is True


def test_auto_tax_at_ingest_works_for_rfq_too(temp_data_dir, monkeypatch):
    """Same hook fires for record_type='rfq'."""
    monkeypatch.setattr(
        "src.core.quote_contract.tax_for_address",
        lambda addr: {
            "rate": 0.0725, "rate_bps": 725, "jurisdiction": "BARSTOW",
            "source": "fallback_table", "validated": True, "facility_code": "",
        },
    )
    rid = _invoke_create_record("rfq", "100 E Veterans Pkwy, Barstow, CA 92311")
    rfq = _load_record(rid)
    assert rfq is not None
    assert rfq.get("tax_rate") == 7.25
    assert rfq.get("tax_jurisdiction") == "BARSTOW"


# ── Defensive paths ─────────────────────────────────────────────────


def test_auto_tax_skipped_when_ship_to_too_short(temp_data_dir, monkeypatch):
    """ship_to length <=3 chars (AND no canonical fallback) → no lookup
    attempted; fields stay at zero/empty so the existing ⚠ DEFAULT
    operator path still applies."""
    _called = []

    def _spy(addr):
        _called.append(addr)
        return {"rate": 0.08975, "jurisdiction": "X", "source": "Y", "validated": True}

    monkeypatch.setattr("src.core.quote_contract.tax_for_address", _spy)
    # Block the facility_registry canonical-ship-to fallback so
    # resolved_ship_to stays at the header's short value.
    monkeypatch.setattr("src.core.facility_registry.resolve", lambda _x: None)
    rid = _invoke_create_record("pc", "CA")  # 2 chars — too short
    pc = _load_record(rid)
    assert pc is not None
    # Lookup never called (short address gated upstream)
    assert _called == []
    # Tax fields default to unresolved
    assert pc.get("tax_rate") == 0
    assert pc.get("tax_source") == ""
    assert pc.get("tax_validated") is False


def test_auto_tax_resolver_exception_does_not_block_ingest(temp_data_dir, monkeypatch):
    """A raise from tax_for_address must NEVER cause the ingest record
    creation to fail. The operator's existing Verify Tax button can
    re-resolve at detail-page time."""
    def _boom(addr):
        raise RuntimeError("simulated CDTFA outage")

    monkeypatch.setattr("src.core.quote_contract.tax_for_address", _boom)
    rid = _invoke_create_record("pc", "100 Prison Rd, Coalinga, CA 93210")
    pc = _load_record(rid)
    assert pc is not None
    assert pc.get("status") in ("parsed", "needs_review")  # ingest completed
    # Fields default to unresolved — operator can Verify Tax
    assert pc.get("tax_rate") == 0
    assert pc.get("tax_source") == ""


def test_auto_tax_zero_rate_keeps_unresolved_signal(temp_data_dir, monkeypatch):
    """tax_for_address can legitimately return rate=0.0 when address is
    out-of-state or unparseable. Keep the unresolved signal so the
    operator's ⚠ DEFAULT badge still surfaces — don't pollute the
    record with a 0% tax rate that looks valid."""
    monkeypatch.setattr(
        "src.core.quote_contract.tax_for_address",
        lambda addr: {
            "rate": 0.0, "rate_bps": 0, "jurisdiction": "",
            "source": "", "validated": False, "facility_code": "",
        },
    )
    rid = _invoke_create_record("pc", "Unknown Address Outside California")
    pc = _load_record(rid)
    assert pc is not None
    assert pc.get("tax_rate") == 0
    assert pc.get("tax_validated") is False
