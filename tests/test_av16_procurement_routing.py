"""PR-AV16 (AV-16) — procurement-routing table.

Closes AV-16 from the 5/14 EOD substrate backlog. For PREQ 10847262
specifically, the handoff noted:

  "Buyer was Mohammad Chechi but send goes to a different CCHCS
   procurement address ('won't go to Mohammed, other procurement
   people handle it') — Mike will change recipient at draft time."

Mike has been manually changing the recipient on every CCHCS quote
draft. The send-to address is a fixed agency procurement inbox,
not the individual buyer who emailed the RFQ in.

THE FIX

1. FacilityRecord gains two new fields:
     procurement_email: str = ""
     procurement_email_cc: Tuple[str, ...] = ()
   Both default empty — populating them is an operator data
   action (or follow-up PR with real addresses). Empty defaults
   preserve current behavior 100%.

2. draft_builder.build_recipients gains a procurement-routing
   lookup BEFORE the existing buyer-driven chain. When the
   resolved facility has a procurement_email set, that becomes
   the canonical TO and the original buyer moves to CC so they
   stay in the loop without being the primary recipient.

   Resolver:
     - First check rfq.institution as a canonical facility code
     - Fall back to resolve(rfq.ship_to / agency / requestor_name)
       for legacy records that don't carry institution

3. Existing buyer-driven path (original_sender → requestor_email
   → email_sender) is unchanged when procurement_email is empty.

Tests pin:
  - FacilityRecord exposes the two new fields with empty defaults
  - Empty procurement_email → falls through to buyer (no regression)
  - Populated procurement_email → procurement is TO, buyer is CC
  - procurement_email_cc additionally lands in CC
  - Same-email dedupe (procurement == buyer → no double-add)
  - Existing cc_emails on the RFQ are preserved + merged
  - Resolver errors don't break the draft (try/except wrap)
  - Source-grep guards for the wire-up
"""
from __future__ import annotations


# ── FacilityRecord schema contract ──────────────────────────────────────────


def test_facility_record_has_procurement_fields():
    """Pin the new schema fields with their defaults."""
    from src.core.facility_registry import FacilityRecord
    fac = FacilityRecord(
        code="TEST",
        canonical_name="Test Facility",
        address_line1="100 Test Ln",
        address_line2="Test, CA 90000",
        zip="90000",
        parent_agency="TEST",
        parent_agency_full="Test Agency",
    )
    assert hasattr(fac, "procurement_email")
    assert hasattr(fac, "procurement_email_cc")
    assert fac.procurement_email == ""
    assert fac.procurement_email_cc == ()


# ── Empty-procurement fallthrough (no regression) ──────────────────────────


def test_empty_procurement_email_falls_through_to_buyer():
    """When procurement_email is empty on every reachable facility,
    build_recipients returns the buyer-driven chain unchanged."""
    from src.api.draft_builder import build_recipients
    rfq = {
        "id": "rfq_test",
        "institution": "",  # unresolved
        "original_sender": "buyer@agency.gov",
        "requestor_email": "buyer@agency.gov",
    }
    to, cc = build_recipients(rfq)
    assert to == "buyer@agency.gov"
    assert cc == ""


def test_no_institution_no_resolver_match_no_crash():
    """Defensive: an RFQ with no institution + free-text fields that
    don't resolve must not crash. Falls through to buyer."""
    from src.api.draft_builder import build_recipients
    rfq = {
        "id": "rfq_test",
        "institution": "Unknown Facility 9999",
        "agency": "",
        "ship_to": "",
        "original_sender": "x@y.com",
    }
    to, cc = build_recipients(rfq)
    assert to == "x@y.com"


# ── Populated procurement routing ──────────────────────────────────────────


def test_populated_procurement_routes_to_procurement_buyer_to_cc(monkeypatch):
    """When the resolved facility has procurement_email set:
       TO = facility.procurement_email
       CC = original_sender (buyer moves to CC so they stay informed)
    """
    from src.api import draft_builder
    from src.core.facility_registry import FacilityRecord

    fake = FacilityRecord(
        code="SATF",
        canonical_name="SATF",
        address_line1="900 Quebec Avenue",
        address_line2="Corcoran, CA 93212",
        zip="93212",
        parent_agency="CCHCS",
        parent_agency_full="CCHCS",
        procurement_email="cchcs-procurement@cdcr.ca.gov",
    )

    def _fake_get(code):
        return fake if code and code.upper() == "SATF" else None

    monkeypatch.setattr(
        "src.core.facility_registry.get", _fake_get,
    )

    rfq = {
        "id": "rfq_test",
        "institution": "SATF",
        "original_sender": "mchechi@cdcr.ca.gov",
        "requestor_email": "mchechi@cdcr.ca.gov",
    }
    to, cc = draft_builder.build_recipients(rfq)
    assert to == "cchcs-procurement@cdcr.ca.gov"
    assert "mchechi@cdcr.ca.gov" in cc


def test_procurement_cc_additions_land_in_cc(monkeypatch):
    """procurement_email_cc tuple values appear in the CC list."""
    from src.api import draft_builder
    from src.core.facility_registry import FacilityRecord

    fake = FacilityRecord(
        code="SATF",
        canonical_name="SATF",
        address_line1="x",
        address_line2="y",
        zip="93212",
        parent_agency="CCHCS",
        parent_agency_full="CCHCS",
        procurement_email="proc@cdcr.ca.gov",
        procurement_email_cc=("supervisor@cdcr.ca.gov",
                              "auditor@cdcr.ca.gov"),
    )
    monkeypatch.setattr(
        "src.core.facility_registry.get",
        lambda code: fake if code and code.upper() == "SATF" else None,
    )

    rfq = {
        "id": "rfq_test",
        "institution": "SATF",
        "original_sender": "buyer@cdcr.ca.gov",
    }
    to, cc = draft_builder.build_recipients(rfq)
    assert to == "proc@cdcr.ca.gov"
    assert "supervisor@cdcr.ca.gov" in cc
    assert "auditor@cdcr.ca.gov" in cc
    assert "buyer@cdcr.ca.gov" in cc


def test_procurement_dedupe_when_buyer_equals_procurement(monkeypatch):
    """Same address can't appear both TO and CC — dedupe by case-
    insensitive equality."""
    from src.api import draft_builder
    from src.core.facility_registry import FacilityRecord

    fake = FacilityRecord(
        code="SATF",
        canonical_name="SATF",
        address_line1="x", address_line2="y", zip="93212",
        parent_agency="CCHCS", parent_agency_full="CCHCS",
        procurement_email="same@cdcr.ca.gov",
    )
    monkeypatch.setattr(
        "src.core.facility_registry.get",
        lambda code: fake if code and code.upper() == "SATF" else None,
    )

    rfq = {
        "id": "rfq_test",
        "institution": "SATF",
        "original_sender": "SAME@cdcr.ca.gov",  # case differs
    }
    to, cc = draft_builder.build_recipients(rfq)
    assert to == "same@cdcr.ca.gov"
    # buyer did NOT get added to CC (same as TO, case-insensitive)
    assert "same@cdcr.ca.gov" not in cc.lower() or cc == ""


def test_existing_cc_emails_preserved(monkeypatch):
    """An RFQ that carries cc_emails on it (e.g., set by operator)
    must still see those addresses in the final CC list."""
    from src.api import draft_builder
    from src.core.facility_registry import FacilityRecord

    fake = FacilityRecord(
        code="SATF",
        canonical_name="SATF",
        address_line1="x", address_line2="y", zip="93212",
        parent_agency="CCHCS", parent_agency_full="CCHCS",
        procurement_email="proc@cdcr.ca.gov",
    )
    monkeypatch.setattr(
        "src.core.facility_registry.get",
        lambda code: fake if code and code.upper() == "SATF" else None,
    )

    rfq = {
        "id": "rfq_test",
        "institution": "SATF",
        "original_sender": "buyer@cdcr.ca.gov",
        "cc_emails": "watcher@reytechinc.com",
    }
    to, cc = draft_builder.build_recipients(rfq)
    assert to == "proc@cdcr.ca.gov"
    assert "watcher@reytechinc.com" in cc
    assert "buyer@cdcr.ca.gov" in cc


# ── Source-grep guards ─────────────────────────────────────────────────────


def test_draft_builder_source_contains_procurement_marker():
    """Refactor guard: the AV-16 wire-up in draft_builder.py must
    remain. If a future refactor removes it, this test fires."""
    import inspect
    from src.api import draft_builder
    src = inspect.getsource(draft_builder.build_recipients)
    assert "PR-AV16" in src
    assert "procurement_email" in src


def test_draft_builder_wrapped_in_try_except():
    """A facility_registry crash must NOT break draft creation. The
    first PR-AV16 marker is in the docstring; the wire-up (and the
    try/except wrapping it) lives below the function body. Search
    from the SECOND marker — the implementation block."""
    import inspect
    from src.api import draft_builder
    src = inspect.getsource(draft_builder.build_recipients)
    # Skip the docstring marker, find the implementation marker
    first = src.find("PR-AV16")
    assert first != -1
    second = src.find("PR-AV16", first + 1)
    assert second != -1, "PR-AV16 implementation marker missing"
    section = src[second:second + 2000]
    assert "try:" in section
    assert "except" in section
    assert "log.debug" in section


def test_facility_registry_source_contains_av16_marker():
    """Schema change in FacilityRecord is marked with PR-AV16."""
    import inspect
    from src.core import facility_registry
    src = inspect.getsource(facility_registry.FacilityRecord)
    assert "PR-AV16" in src
    assert "procurement_email" in src
