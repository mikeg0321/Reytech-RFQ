"""EmailContract gains required_forms + response_due + response_packaging
+ parse_confidence (Phase 1 of PR #1034).

Closes the "we shipped a different form set than the email asked for"
class structurally: every downstream renderer iterates this list, and
the /package output-vs-contract gate (Phase 2) refuses ship on divergence.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.spine import (
    ALL_FORM_CODES,
    CCHCS_DEFAULT_REQUIRED_FORMS,
    ContractLineItem,
    EmailContract,
)


def _make(**over) -> EmailContract:
    base = dict(
        contract_id="contract_sac_1747000000",
        rfq_id="rfq_sac_test",
        source_email_id="msgSAC",
        source_thread_id="threadSAC",
        buyer_name="Argarin, Marc",
        buyer_email="argarin@cchcs.ca.gov",
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10847457",
        line_items=[
            ContractLineItem(line_no=1, description="N95 masks, box of 20",
                             qty=50, uom="BX"),
        ],
    )
    base.update(over)
    return EmailContract(**base)


# ── Defaults preserve existing fixtures ──────────────────────────────


def test_default_required_forms_is_cchcs_standard_four():
    c = _make()
    assert c.required_forms == CCHCS_DEFAULT_REQUIRED_FORMS
    assert c.required_forms == ["703b", "704b", "bidpkg", "quote"]


def test_default_response_packaging_is_separate_pdfs():
    c = _make()
    assert c.response_packaging == "separate_pdfs"


def test_default_parse_confidence_is_high():
    c = _make()
    assert c.parse_confidence == "high"


def test_default_response_due_is_none():
    c = _make()
    assert c.response_due is None


# ── FormCode literal completeness ────────────────────────────────────


def test_all_form_codes_includes_cchcs_default_set():
    for code in CCHCS_DEFAULT_REQUIRED_FORMS:
        assert code in ALL_FORM_CODES, (
            f"CCHCS default form {code!r} missing from FormCode literal"
        )


def test_all_form_codes_covers_the_known_set():
    expected_minimum = {
        "703b", "703c", "704b", "704c",
        "bidpkg", "quote",
        "calrecycle_74", "std_204", "std_1000",
        "dvbe_843", "darfur", "cuf",
    }
    assert expected_minimum.issubset(set(ALL_FORM_CODES))


# ── Required-forms validation ────────────────────────────────────────


def test_required_forms_accepts_subset_of_form_codes():
    c = _make(required_forms=["703b", "704b", "bidpkg", "quote"])
    assert c.required_forms == ["703b", "704b", "bidpkg", "quote"]


def test_required_forms_accepts_alternates_703c_and_704c():
    c = _make(required_forms=["703c", "704c", "bidpkg", "quote"])
    assert c.required_forms == ["703c", "704c", "bidpkg", "quote"]


def test_required_forms_rejects_unknown_code():
    with pytest.raises(Exception):
        _make(required_forms=["703b", "bogus_form"])


def test_required_forms_rejects_empty_string_member():
    with pytest.raises(Exception):
        _make(required_forms=["", "704b"])


# ── Packaging + confidence validation ────────────────────────────────


@pytest.mark.parametrize("packaging", ["single_pdf", "separate_pdfs", "either"])
def test_response_packaging_round_trips(packaging):
    c = _make(response_packaging=packaging)
    assert c.response_packaging == packaging


def test_response_packaging_rejects_unknown():
    with pytest.raises(Exception):
        _make(response_packaging="zip_archive")


@pytest.mark.parametrize("conf", ["high", "medium", "low"])
def test_parse_confidence_round_trips(conf):
    c = _make(parse_confidence=conf)
    assert c.parse_confidence == conf


def test_parse_confidence_rejects_unknown():
    with pytest.raises(Exception):
        _make(parse_confidence="excellent")


# ── extra='forbid' invariant ─────────────────────────────────────────


def test_extra_forbid_rejects_alias_creep():
    with pytest.raises(Exception):
        _make(required_form_set=["703b"])  # close-but-wrong name


# ── Response_due round-trips ────────────────────────────────────────


def test_response_due_round_trips():
    due = datetime(2026, 5, 18, 17, 0, tzinfo=timezone.utc)
    c = _make(response_due=due)
    assert c.response_due == due
