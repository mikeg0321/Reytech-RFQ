"""Tests for PR-A: AMS 704 ingest drift fixes.

Triggered by 2026-05-03 prod finding (Mike: "this is a good example
of things not working as expected"). Two CDCR PCs from the same buyer
parsed from AMS 704 PDFs landed with three independent drift surfaces:

  3. agency='CSP-SAC' on one, 'CSP-Sacramento' on the other (same
     facility, two labels) → no canonicalization step.
  5. contact_email='' but pc_data.requestor_email populated → schema
     name mismatch, downstream buyer-rollup surfaces miss the row.
  6. received_at='' → ingest used datetime.now() but never persisted it
     and never accepted email arrival time from the poller.

These tests pin the fixes at the architecture layer (`_create_record`)
so future regressions surface as red tests, not silent prod drift.
"""
from __future__ import annotations

import pytest

from src.core.ingest_pipeline import _create_record
from src.core.request_classifier import RequestClassification


def _classification(institution: str = "", agency: str = "cchcs") -> RequestClassification:
    return RequestClassification(
        shape="pc_704_pdf_fillable",
        agency=agency,
        agency_name="CCHCS",
        is_quote_only=True,
        institution=institution,
    )


# ─── Drift #3: facility_registry canonicalization ─────────────────────

class TestCanonicalizeInstitution:
    """Both 'CSP-SAC' and 'CSP-Sacramento' must collapse to one canonical
    code via facility_registry.resolve. Buyer free-text labels never reach
    the persisted row for facilities the registry recognizes."""

    @pytest.mark.parametrize("buyer_label", [
        # The two labels that landed on prod 2026-05-03 (pc_177b18e6 +
        # pc_08583a68 — same buyer, two requests, drift surfaced):
        "CSP-SAC",
        "CSP-Sacramento",
        # Plus other aliases registered in facility_registry:
        "csp-sac",
        "California State Prison Sacramento",
        "ca state prison sacramento",
        "new folsom",
        "csp sac",
    ])
    def test_csp_sac_variants_canonicalize(self, temp_data_dir, buyer_label):
        """Every alias the registry knows for CSP-SAC must persist as 'CSP-SAC'."""
        rid = _create_record(
            record_type="pc",
            items=[{"description": "test", "qty": 1}],
            header={"institution": buyer_label},
            classification=_classification(institution=buyer_label),
            primary_path=None,
            email_subject="Test",
            email_sender="buyer@cdcr.ca.gov",
            email_uid="test-uid-canon",
        )
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(rid)
        assert pc is not None, f"PC not loaded for {buyer_label}"
        assert pc["institution"] == "CSP-SAC", (
            f"buyer label {buyer_label!r} did not canonicalize "
            f"(got {pc['institution']!r})"
        )

    def test_unknown_facility_falls_through(self, temp_data_dir):
        """When facility_registry can't resolve, keep the raw label —
        don't drop the field. The drift card surfaces unrecognized
        labels for operator review."""
        rid = _create_record(
            record_type="pc",
            items=[{"description": "x", "qty": 1}],
            header={"institution": "Unknown Facility XYZ"},
            classification=_classification(institution="Unknown Facility XYZ"),
            primary_path=None,
            email_subject="t",
            email_sender="u@cdcr.ca.gov",
            email_uid="test-uid-unknown",
        )
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(rid)
        assert pc is not None
        # Falls through to raw label rather than empty string.
        assert pc["institution"] == "Unknown Facility XYZ"


# ─── Drift #5: contact_email mirror ──────────────────────────────────

class TestContactEmailMirror:
    """contact_email is the canonical column quote-keyed buyer rollups
    read from. PC ingest historically only set requestor_email, leaving
    contact_email empty (PR #621-era gap). Mirror at write time."""

    def test_pc_contact_email_mirrors_email_sender(self, temp_data_dir):
        rid = _create_record(
            record_type="pc",
            items=[{"description": "x", "qty": 1}],
            header={},
            classification=_classification(),
            primary_path=None,
            email_subject="t",
            email_sender="valentina.demidenko@cdcr.ca.gov",
            email_uid="test-uid-mirror-pc",
        )
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(rid)
        assert pc is not None
        assert pc["contact_email"] == "valentina.demidenko@cdcr.ca.gov"
        assert pc["requestor_email"] == "valentina.demidenko@cdcr.ca.gov"

    def test_rfq_contact_email_mirrors_email_sender(self, temp_data_dir):
        rid = _create_record(
            record_type="rfq",
            items=[{"description": "x", "qty": 1}],
            header={},
            classification=_classification(),
            primary_path=None,
            email_subject="t",
            email_sender="keith.alsing@calvet.ca.gov",
            email_uid="test-uid-mirror-rfq",
        )
        from src.api.dashboard import load_rfqs
        rfq = load_rfqs().get(rid)
        assert rfq is not None
        assert rfq["contact_email"] == "keith.alsing@calvet.ca.gov"
        assert rfq["requestor_email"] == "keith.alsing@calvet.ca.gov"

    def test_empty_email_sender_yields_empty_mirror(self, temp_data_dir):
        """Don't fabricate a mirror when sender is empty — preserve the
        empty-string sentinel so health-card drift detection still flags
        rows that arrived without a sender."""
        rid = _create_record(
            record_type="pc",
            items=[{"description": "x", "qty": 1}],
            header={},
            classification=_classification(),
            primary_path=None,
            email_subject="t",
            email_sender="",
            email_uid="test-uid-empty-sender",
        )
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(rid)
        assert pc is not None
        assert pc["contact_email"] == ""
        assert pc["requestor_email"] == ""


# ─── Drift #6: received_at from email arrival time ───────────────────

class TestReceivedAtPrecedence:
    """received_at must reflect when the buyer's email actually arrived
    (Gmail Date header) when the poller knows it. Falls back to ingest
    time when not provided (manual upload, re-parse, external API)."""

    def test_received_at_uses_email_received_at_when_provided(self, temp_data_dir):
        rfc822 = "Tue, 23 Apr 2024 14:30:00 -0700"
        rid = _create_record(
            record_type="pc",
            items=[{"description": "x", "qty": 1}],
            header={},
            classification=_classification(),
            primary_path=None,
            email_subject="t",
            email_sender="b@cdcr.ca.gov",
            email_uid="test-uid-recv-1",
            email_received_at=rfc822,
        )
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(rid)
        assert pc is not None
        assert pc["received_at"] == rfc822

    def test_received_at_falls_back_to_now_when_empty(self, temp_data_dir):
        """Manual upload / reparse path doesn't set email_received_at —
        ingest time is the closest signal we have."""
        rid = _create_record(
            record_type="pc",
            items=[{"description": "x", "qty": 1}],
            header={},
            classification=_classification(),
            primary_path=None,
            email_subject="t",
            email_sender="b@cdcr.ca.gov",
            email_uid="test-uid-recv-2",
        )
        from src.api.dashboard import _load_price_checks
        pc = _load_price_checks().get(rid)
        assert pc is not None
        # Not empty — falls back to a real ISO timestamp.
        assert pc["received_at"]
        assert pc["received_at"] != ""
        # And it equals created_at when no email date was supplied.
        assert pc["received_at"] == pc["created_at"]
