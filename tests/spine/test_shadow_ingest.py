"""Tests for shadow_ingest_to_spine — best-effort parent→Spine wire-up.

Architectural invariants:
- Flag-gated: SPINE_SHADOW_INGEST_ENABLED off → no DB write, returns
  reason='flag_off'.
- Best-effort: ANY internal exception is swallowed and surfaced via
  the return dict; never raises into the parent pipeline.
- Tax resolver wrapping: legacy resolve_tax returns float decimal;
  helper converts to integer bps.
- On success: BOTH spine_email_contracts AND spine_quotes have a new
  row pointing to the same rfq_id, and they round-trip via
  find_contract_for_quote.
- Re-running shadow ingest on the same record produces a new
  contract_id (immutable history).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from src.spine import (
    init_db,
    read_email_contract,
    read_quote,
    find_contract_for_quote,
)
from src.spine_bridge.shadow_ingest import shadow_ingest_to_spine


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


class _Classification:
    """Stand-in for src.core.request_classifier.RequestClassification."""
    def __init__(self, agency="CCHCS", solicitation_number="PREQ 10847262",
                 institution="SATF Corcoran",
                 producer_signature="vision-v3"):
        self.agency = agency
        self.agency_name = agency
        self.solicitation_number = solicitation_number
        self.institution = institution
        self.producer_signature = producer_signature


def _header() -> dict:
    return {
        "buyer_name": "Robert Buyer",
        "institution": "SATF Corcoran",
        "ship_to": "CA Substance Abuse TF, 900 Quebec Ave, Corcoran CA 93212",
        "due_date": "2026-05-20",
    }


def _items() -> list[dict]:
    return [
        {"description": "GLOVES, NITRILE, LARGE, 100/BX",
         "item_number": "MK-2103L", "qty": 10, "uom": "BX"},
        {"description": "MASKS, SURGICAL, 50/BX",
         "item_number": "PRM-1820", "qty": 12, "uom": "BX"},
    ]


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_shadow.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def flag_on(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SPINE_SHADOW_INGEST_ENABLED", "1")
    yield


@pytest.fixture
def flag_off(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("SPINE_SHADOW_INGEST_ENABLED", raising=False)
    yield


@pytest.fixture
def patch_tax(monkeypatch: pytest.MonkeyPatch):
    """Make resolve_tax return a fixed 8.25% rate."""
    def _fake(addr, force_live=False):
        return {"ok": True, "rate": 0.0825, "jurisdiction": "X",
                "source": "test", "validated": True}
    import src.core.tax_resolver as tr
    monkeypatch.setattr(tr, "resolve_tax", _fake)
    yield


# ──────────────────────────────────────────────────────────────────────
# Flag gate
# ──────────────────────────────────────────────────────────────────────


def test_flag_off_returns_flag_off_and_writes_nothing(flag_off, db_path):
    result = shadow_ingest_to_spine(
        record_id="rfq_shadow_001",
        record_type="rfq",
        classification=_Classification(),
        header=_header(),
        items=_items(),
        db_path=db_path,
    )
    assert result["ok"] is False
    assert result["reason"] == "flag_off"
    assert result["contract_id"] is None
    # DB has no contract for this rfq.
    assert find_contract_for_quote(db_path, "rfq_shadow_001") is None
    assert read_quote(db_path, "rfq_shadow_001") is None


# ──────────────────────────────────────────────────────────────────────
# Happy path — both records written
# ──────────────────────────────────────────────────────────────────────


def test_flag_on_writes_both_contract_and_quote(flag_on, patch_tax, db_path):
    result = shadow_ingest_to_spine(
        record_id="rfq_shadow_002",
        record_type="rfq",
        classification=_Classification(),
        header=_header(),
        items=_items(),
        email_subject="RFQ for Examination Supplies",
        email_sender="rbuyer@cchcs.ca.gov",
        gmail_thread_id="thread-abc",
        gmail_message_id="msg-001",
        db_path=db_path,
    )
    assert result["ok"] is True
    assert result["contract_id"] is not None
    assert result["contract_id"].startswith("contract_rfq_shadow_002_")
    assert result["quote_id"] == "rfq_shadow_002"
    assert result["reason"] == "shadow_written"

    # Round-trip both records.
    c = read_email_contract(db_path, result["contract_id"])
    assert c is not None
    assert c.solicitation_number == "10847262"  # PREQ stripped
    assert c.tax_rate_bps == 825
    assert c.buyer_email == "rbuyer@cchcs.ca.gov"
    assert c.source_thread_id == "thread-abc"

    q = read_quote(db_path, "rfq_shadow_002")
    assert q is not None
    assert q.solicitation_number == "10847262"

    # Find-contract-for-quote ties them together.
    found = find_contract_for_quote(db_path, "rfq_shadow_002")
    assert found is not None
    assert found.contract_id == result["contract_id"]


# ──────────────────────────────────────────────────────────────────────
# Reissue produces new contract_id (no upsert)
# ──────────────────────────────────────────────────────────────────────


def test_reingest_creates_second_contract_row(flag_on, patch_tax, db_path):
    """Two ingests of the same RFQ → two distinct contract rows.
    Required for the rebid pattern; substrate stores HISTORY, not state."""
    first = shadow_ingest_to_spine(
        record_id="rfq_reissue", record_type="rfq",
        classification=_Classification(), header=_header(),
        items=_items(), db_path=db_path,
    )
    # Add a tiny delay so the epoch-second timestamp differs.
    import time; time.sleep(1.05)
    second = shadow_ingest_to_spine(
        record_id="rfq_reissue", record_type="rfq",
        classification=_Classification(), header=_header(),
        items=_items(), db_path=db_path,
    )
    assert first["ok"] and second["ok"]
    assert first["contract_id"] != second["contract_id"]


# ──────────────────────────────────────────────────────────────────────
# Failure surfaces — never raises
# ──────────────────────────────────────────────────────────────────────


def test_unsupported_agency_returns_reason_no_raise(flag_on, patch_tax, db_path):
    """CalVet (non-CCHCS) is rejected by the Spine v1 ingest. Helper
    surfaces 'ingest_rejected' but does NOT raise."""
    result = shadow_ingest_to_spine(
        record_id="rfq_calvet", record_type="rfq",
        classification=_Classification(agency="CalVet"),
        header=_header(), items=_items(), db_path=db_path,
    )
    assert result["ok"] is False
    assert result["reason"] == "ingest_rejected"
    assert any("agency" in (i["field"] or "") for i in result["issues"])


def test_tax_resolver_failure_returns_reason_no_raise(flag_on, monkeypatch, db_path):
    """If resolve_tax raises, the helper's wrapper returns None, the
    ingest then fails with tax_rate_bps issue. Never bubbles."""
    def _crashing(addr, force_live=False):
        raise RuntimeError("CDTFA network timeout")
    import src.core.tax_resolver as tr
    monkeypatch.setattr(tr, "resolve_tax", _crashing)

    result = shadow_ingest_to_spine(
        record_id="rfq_taxfail", record_type="rfq",
        classification=_Classification(), header=_header(),
        items=_items(), db_path=db_path,
    )
    assert result["ok"] is False
    assert result["reason"] == "ingest_rejected"
    assert any("tax_rate_bps" in (i["field"] or "") for i in result["issues"])


def test_persist_failure_returns_reason_no_raise(flag_on, patch_tax, db_path, monkeypatch):
    """write_email_contract raising is caught + surfaced as
    persist_failed; never bubbles."""
    import src.spine_bridge.shadow_ingest as si

    real_write = si.__dict__.get("write_email_contract")
    # Patch the import inside the function — use a module-level shim.
    def _boom(*a, **kw):
        raise RuntimeError("disk full")
    monkeypatch.setattr("src.spine.write_email_contract", _boom)

    result = shadow_ingest_to_spine(
        record_id="rfq_persistfail", record_type="rfq",
        classification=_Classification(), header=_header(),
        items=_items(), db_path=db_path,
    )
    assert result["ok"] is False
    assert result["reason"].startswith("persist_failed:")


def test_no_record_id_returns_reason_no_raise(flag_on, patch_tax, db_path):
    result = shadow_ingest_to_spine(
        record_id="", record_type="rfq",
        classification=_Classification(), header=_header(),
        items=_items(), db_path=db_path,
    )
    assert result["ok"] is False
    assert result["reason"] == "no_record_id"


# ──────────────────────────────────────────────────────────────────────
# Field projection — buyer/email/thread carry through
# ──────────────────────────────────────────────────────────────────────


def test_buyer_email_taken_from_email_sender(flag_on, patch_tax, db_path):
    result = shadow_ingest_to_spine(
        record_id="rfq_buyer", record_type="rfq",
        classification=_Classification(), header=_header(),
        items=_items(),
        email_sender="procurement@cchcs.ca.gov",
        db_path=db_path,
    )
    assert result["ok"]
    c = read_email_contract(db_path, result["contract_id"])
    assert c.buyer_email == "procurement@cchcs.ca.gov"


def test_rfq_title_from_email_subject(flag_on, patch_tax, db_path):
    result = shadow_ingest_to_spine(
        record_id="rfq_subj", record_type="rfq",
        classification=_Classification(), header=_header(),
        items=_items(),
        email_subject="Bid Request — Examination Gloves",
        db_path=db_path,
    )
    assert result["ok"]
    c = read_email_contract(db_path, result["contract_id"])
    assert c.rfq_title == "Bid Request — Examination Gloves"


def test_classification_passed_as_dict_works(flag_on, patch_tax, db_path):
    """Caller can pass classification as either dataclass or dict."""
    result = shadow_ingest_to_spine(
        record_id="rfq_dictcls", record_type="rfq",
        classification={
            "agency": "CCHCS",
            "solicitation_number": "10848888",
            "institution": "CCWF",
            "producer_signature": "vision-v3",
        },
        header=_header(), items=_items(), db_path=db_path,
    )
    assert result["ok"]
    c = read_email_contract(db_path, result["contract_id"])
    assert c.solicitation_number == "10848888"


# ──────────────────────────────────────────────────────────────────────
# Boolean flag parsing
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("value,expected_on", [
    ("1", True),
    ("true", True),
    ("True", True),
    ("yes", True),
    ("on", True),
    ("0", False),
    ("false", False),
    ("", False),
    ("no", False),
])
def test_flag_parsing(monkeypatch, db_path, patch_tax, value, expected_on):
    monkeypatch.setenv("SPINE_SHADOW_INGEST_ENABLED", value)
    result = shadow_ingest_to_spine(
        record_id=f"rfq_flag_{value or 'empty'}", record_type="rfq",
        classification=_Classification(), header=_header(),
        items=_items(), db_path=db_path,
    )
    if expected_on:
        assert result["ok"] is True
    else:
        assert result["reason"] == "flag_off"


# ──────────────────────────────────────────────────────────────────────
# Auto-link PC predecessor (PR #1042)
# ──────────────────────────────────────────────────────────────────────


def _items_overlap_with(_items_func) -> list[dict]:
    """Return _items() with one extra row so a second ingest's MFG#s
    overlap with the first by 2/3. Combined with same sol# → auto-link."""
    base = _items_func()
    return base + [
        {"description": "WIPES, ALCOHOL, 200/BX",
         "item_number": "PRM-2200", "qty": 5, "uom": "BX"},
    ]


def test_auto_link_fires_when_second_ingest_matches_first(
    flag_on, patch_tax, db_path
):
    """First ingest seeds a PC predecessor. Second ingest of an RFQ
    with identical sol# + overlapping MFG#s gets an auto-link row."""
    from src.spine import find_links_from

    # 1. Seed predecessor (treat as the "PC").
    first = shadow_ingest_to_spine(
        record_id="pc_predecessor_001",
        record_type="pc",
        classification=_Classification(solicitation_number="10847262"),
        header=_header(),
        items=_items(),
        db_path=db_path,
    )
    assert first["ok"] is True
    assert first.get("auto_link") is None  # no prior quotes to link to

    # 2. Ingest the rebid RFQ — same sol#, same facility, same items.
    second = shadow_ingest_to_spine(
        record_id="rfq_rebid_001",
        record_type="rfq",
        classification=_Classification(solicitation_number="10847262"),
        header=_header(),
        items=_items(),
        db_path=db_path,
    )
    assert second["ok"] is True
    link = second.get("auto_link")
    assert link is not None, "expected auto_link metadata on second ingest"
    assert link["to_quote_id"] == "pc_predecessor_001"
    assert link["confidence"] >= 0.50
    assert link["match_method"] == "auto_mfg_desc"
    assert link["candidates_considered"] == 1

    # 3. The link row is queryable from the DB.
    links = find_links_from(db_path, "rfq_rebid_001")
    assert len(links) == 1
    assert links[0]["to_quote_id"] == "pc_predecessor_001"
    assert links[0]["actor"] == "spine_auto_linker"


def test_no_auto_link_when_no_prior_quotes(flag_on, patch_tax, db_path):
    """First ingest into a fresh DB: matcher runs but finds no candidates."""
    from src.spine import find_links_from

    result = shadow_ingest_to_spine(
        record_id="rfq_solo_001",
        record_type="rfq",
        classification=_Classification(),
        header=_header(),
        items=_items(),
        db_path=db_path,
    )
    assert result["ok"] is True
    assert result.get("auto_link") is None
    assert find_links_from(db_path, "rfq_solo_001") == []


def test_no_auto_link_when_candidate_below_threshold(
    flag_on, patch_tax, db_path
):
    """Unrelated sol# + zero MFG# overlap → no link created."""
    from src.spine import find_links_from

    shadow_ingest_to_spine(
        record_id="pc_unrelated_001",
        record_type="pc",
        classification=_Classification(solicitation_number="SOL-AAA"),
        header=_header(),
        items=[{"description": "ALPHA WIDGET", "item_number": "ALPHA-1",
                "qty": 1, "uom": "EA"}],
        db_path=db_path,
    )
    result = shadow_ingest_to_spine(
        record_id="rfq_different_001",
        record_type="rfq",
        classification=_Classification(solicitation_number="SOL-BBB"),
        header=_header(),
        items=[{"description": "OMEGA WIDGET", "item_number": "OMEGA-1",
                "qty": 1, "uom": "EA"}],
        db_path=db_path,
    )
    assert result["ok"] is True
    assert result.get("auto_link") is None
    assert find_links_from(db_path, "rfq_different_001") == []


def test_auto_link_failure_does_not_fail_ingest(
    flag_on, patch_tax, db_path, monkeypatch
):
    """The quote is the load-bearing record; a matcher / link-writer
    exception MUST NOT cause ingest to report failure. The error is
    surfaced via the return dict instead."""
    # Sabotage the matcher to raise.
    import src.spine_bridge.shadow_ingest as si

    def _boom(*a, **kw):
        raise RuntimeError("matcher exploded")

    monkeypatch.setattr(si, "_maybe_write_auto_link", _boom)

    result = shadow_ingest_to_spine(
        record_id="rfq_resilient_001",
        record_type="rfq",
        classification=_Classification(),
        header=_header(),
        items=_items(),
        db_path=db_path,
    )
    assert result["ok"] is True  # ingest still succeeded
    assert result["quote_id"] == "rfq_resilient_001"
    assert "matcher exploded" in result.get("auto_link_error", "")
