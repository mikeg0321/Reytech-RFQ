"""PR-2 (Job #1) — CCHCS response-format classification at shadow ingest.

The Spine must render the buyer's CCHCS response in the format they
asked for. These tests pin the classifier (`classify_cchcs_response_
format`) and the contract-dict wiring (`_build_contract_dict` emits
`attachment_refs` / `required_forms` / `response_packaging`).
"""
from __future__ import annotations

from src.spine_bridge.shadow_ingest import (
    _build_contract_dict,
    classify_cchcs_response_format,
)


# ── classify_cchcs_response_format — the pure classifier ──────────────


def test_format_a_non_cloud_packet():
    """Classifier shape 'cchcs_packet' -> single_pdf (Format A)."""
    packaging, forms = classify_cchcs_response_format(
        {"shape": "cchcs_packet"}, ["/tmp/cchcs_packet_preq.pdf"],
    )
    assert packaging == "single_pdf"
    assert forms == ["703b", "704b", "bidpkg", "quote"]


def test_format_b_standalone_default_703b():
    """No packet shape -> separate_pdfs, 703B (Format B, common case)."""
    packaging, forms = classify_cchcs_response_format(
        {"shape": "ams_704"},
        ["/tmp/AMS_703B.pdf", "/tmp/AMS_704B.pdf", "/tmp/Bid_Package.pdf"],
    )
    assert packaging == "separate_pdfs"
    assert forms == ["703b", "704b", "bidpkg", "quote"]


def test_format_b_detects_703c_from_filename():
    """A 703C attachment -> required_forms uses 703c, never 703b."""
    packaging, forms = classify_cchcs_response_format(
        {"shape": "ams_704"},
        ["/tmp/AMS 703C.pdf", "/tmp/AMS_704B.pdf", "/tmp/CDCR_Bid_Package.pdf"],
    )
    assert packaging == "separate_pdfs"
    assert forms[0] == "703c"
    assert "703b" not in forms


def test_classifier_reads_object_classification():
    """Classifier reads .shape off a non-dict classification object."""
    class _C:
        shape = "cchcs_packet"

    packaging, _ = classify_cchcs_response_format(_C(), [])
    assert packaging == "single_pdf"


def test_classifier_null_inputs_safe_default():
    """Null inputs fall back to the safe Format-B default."""
    packaging, forms = classify_cchcs_response_format(None, None)
    assert packaging == "separate_pdfs"
    assert forms == ["703b", "704b", "bidpkg", "quote"]


# ── _build_contract_dict — the wiring ─────────────────────────────────


def _contract(**over):
    base = dict(
        record_id="rfq_test1",
        record_type="rfq",
        classification={"shape": "ams_704", "agency": "CCHCS"},
        header={"buyer_name": "Jane Buyer", "due_date": "2026-06-01"},
        items=[{"description": "Widget", "qty": 5, "uom": "EA"}],
        email_subject="CCHCS RFQ",
        email_sender="buyer@cdcr.ca.gov",
        gmail_thread_id="t1",
        gmail_message_id="m1",
        email_received_at="2026-05-21",
        files=["/tmp/AMS_703B.pdf", "/tmp/AMS_704B.pdf"],
    )
    base.update(over)
    return _build_contract_dict(**base)


def test_contract_dict_emits_attachment_refs():
    d = _contract()
    assert d["attachment_refs"] == ["/tmp/AMS_703B.pdf", "/tmp/AMS_704B.pdf"]


def test_contract_dict_emits_required_forms_and_packaging():
    d = _contract()
    assert d["response_packaging"] == "separate_pdfs"
    assert d["required_forms"] == ["703b", "704b", "bidpkg", "quote"]


def test_contract_dict_packet_shape_is_single_pdf():
    d = _contract(
        classification={"shape": "cchcs_packet", "agency": "CCHCS"},
        files=["/tmp/cchcs_packet.pdf"],
    )
    assert d["response_packaging"] == "single_pdf"


def test_contract_dict_no_files_empty_attachment_refs():
    d = _contract(files=None)
    assert d["attachment_refs"] == []
