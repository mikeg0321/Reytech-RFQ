"""Contract tests for src/agents/observed_send.py.

Pins the match-resolution behavior of the observed-send detector
(PR-G1 of the post-quote queue item 23 arc).

Match-signal priority is load-bearing — if a future change reorders
the signals, the confidence scores will silently shift and downstream
auto-attach gates will misfire. These tests freeze the priority:

  1. quote_number in subject (R##Q##)            confidence 0.95
  2. Gmail threadId == record.email_thread_id    confidence 0.90
  3. solicitation_number in subject (unique)     confidence 0.75
  4. solicitation_number ambiguous (multi-hit)   confidence 0.40

Plus the cheap pre-filter (subject doesn't look like a quote send →
skip without scanning record indices).
"""
from __future__ import annotations

import importlib
import sys
from unittest.mock import patch

import pytest


@pytest.fixture
def det():
    if "src.agents.observed_send" in sys.modules:
        del sys.modules["src.agents.observed_send"]
    return importlib.import_module("src.agents.observed_send")


# ─── unit helpers ─────────────────────────────────────────────────────


def test_extract_quote_numbers_finds_canonical_form(det):
    s = "Reytech Quote R26Q40 — Stuff2Color Love Letters"
    assert det._extract_quote_numbers(s) == ["R26Q40"]


def test_extract_quote_numbers_dedupes(det):
    s = "Quote R26Q40 ref R26Q40 again r26q40 lower"
    assert det._extract_quote_numbers(s) == ["R26Q40"]


def test_extract_solicitation_numbers_picks_8_digit(det):
    s = "RFQ #10838974 — please respond"
    out = det._extract_solicitation_numbers(s)
    assert "10838974" in out


def test_looks_like_quote_subject_positive(det):
    assert det._looks_like_quote_subject("Reytech Quote R26Q40")
    assert det._looks_like_quote_subject("Quote R26Q40 Hospital Order")
    assert det._looks_like_quote_subject("Your quote is ready")


def test_looks_like_quote_subject_negative(det):
    assert not det._looks_like_quote_subject("FW: Lunch order")
    assert not det._looks_like_quote_subject("Vacation auto-reply")
    assert not det._looks_like_quote_subject("")


def test_record_already_has_message_list(det):
    record = {"gmail_message_ids": ["msg_a", "msg_b"]}
    assert det._record_already_has_message(record, "msg_a") is True
    assert det._record_already_has_message(record, "msg_c") is False


def test_record_already_has_message_legacy_field(det):
    record = {"sent_gmail_message_id": "msg_legacy"}
    assert det._record_already_has_message(record, "msg_legacy") is True


# ─── match resolution priority ───────────────────────────────────────


def _meta(**kw):
    base = {
        "gmail_id": "msg_test",
        "subject": "",
        "thread_id": "",
        "to": "",
        "date": "",
    }
    base.update(kw)
    return base


def test_match_quote_number_beats_thread_id(det):
    """Two signals collide — quote_number wins (it's the highest
    confidence). Both fire only if they point at the same record;
    the test sets up a deliberate split to prove priority."""
    rfqs = {
        "rfq_qn":  {"reytech_quote_number": "R26Q40"},
        "rfq_tid": {"email_thread_id": "thread_X"},
    }
    pcs = {}
    idx_qn = det._index_records_by_quote_number(rfqs, pcs)
    idx_tid = det._index_records_by_thread_id(rfqs, pcs)
    idx_sol = det._index_records_by_solicitation(rfqs, pcs)

    meta = _meta(subject="Reytech Quote R26Q40 — please review",
                 thread_id="thread_X")
    out = det._match_one_message(
        meta, rfqs=rfqs, pcs=pcs,
        idx_qn=idx_qn, idx_tid=idx_tid, idx_sol=idx_sol)

    assert out["match_signal"] == "quote_number"
    assert out["matched_record_id"] == "rfq_qn"
    assert out["confidence"] == 0.95


def test_match_thread_id_when_no_quote_number(det):
    rfqs = {"rfq_alpha": {"email_thread_id": "thread_X"}}
    pcs = {}
    idx_qn = det._index_records_by_quote_number(rfqs, pcs)
    idx_tid = det._index_records_by_thread_id(rfqs, pcs)
    idx_sol = det._index_records_by_solicitation(rfqs, pcs)

    meta = _meta(subject="Quote — please review",
                 thread_id="thread_X")
    out = det._match_one_message(
        meta, rfqs=rfqs, pcs=pcs,
        idx_qn=idx_qn, idx_tid=idx_tid, idx_sol=idx_sol)

    assert out["match_signal"] == "thread_id"
    assert out["matched_record_id"] == "rfq_alpha"
    assert out["confidence"] == 0.90


def test_match_solicitation_number_unique(det):
    rfqs = {"rfq_sol": {"solicitation_number": "10838974"}}
    pcs = {}
    idx_qn = det._index_records_by_quote_number(rfqs, pcs)
    idx_tid = det._index_records_by_thread_id(rfqs, pcs)
    idx_sol = det._index_records_by_solicitation(rfqs, pcs)

    meta = _meta(subject="Your quote — RFQ #10838974")
    out = det._match_one_message(
        meta, rfqs=rfqs, pcs=pcs,
        idx_qn=idx_qn, idx_tid=idx_tid, idx_sol=idx_sol)

    assert out["match_signal"] == "solicitation_number"
    assert out["matched_record_id"] == "rfq_sol"
    assert out["confidence"] == 0.75


def test_match_solicitation_number_ambiguous_flagged(det):
    """Same sol number across multiple records — flag low confidence
    so the operator UI can show a warning instead of auto-attaching."""
    rfqs = {
        "rfq_a": {"solicitation_number": "10838974"},
        "rfq_b": {"solicitation_number": "10838974"},
    }
    pcs = {}
    idx_qn = det._index_records_by_quote_number(rfqs, pcs)
    idx_tid = det._index_records_by_thread_id(rfqs, pcs)
    idx_sol = det._index_records_by_solicitation(rfqs, pcs)

    meta = _meta(subject="Quote — RFQ #10838974")
    out = det._match_one_message(
        meta, rfqs=rfqs, pcs=pcs,
        idx_qn=idx_qn, idx_tid=idx_tid, idx_sol=idx_sol)

    assert out["match_signal"] == "solicitation_number_ambiguous"
    assert out["confidence"] == 0.40
    assert "ambiguous_candidates" in out


def test_no_signal_returns_none(det):
    rfqs = {"rfq_x": {"reytech_quote_number": "R26Q99"}}
    pcs = {}
    idx_qn = det._index_records_by_quote_number(rfqs, pcs)
    idx_tid = det._index_records_by_thread_id(rfqs, pcs)
    idx_sol = det._index_records_by_solicitation(rfqs, pcs)

    meta = _meta(subject="Reytech Quote R26Q40 (different number)")
    out = det._match_one_message(
        meta, rfqs=rfqs, pcs=pcs,
        idx_qn=idx_qn, idx_tid=idx_tid, idx_sol=idx_sol)

    assert out is None


# ─── detect_observed_sends end-to-end ────────────────────────────────


def _setup_gmail_mocks(metadata_by_id: dict, list_ids: list = None):
    """Returns context-manager kwargs ready for `with patch.multiple`."""
    if list_ids is None:
        list_ids = list(metadata_by_id.keys())

    def _list_message_ids(_service, query="", max_results=500):
        return list_ids

    def _get_message_metadata(_service, msg_id):
        meta = metadata_by_id.get(msg_id, {})
        meta.setdefault("gmail_id", msg_id)
        return meta

    return _list_message_ids, _get_message_metadata


def test_detect_returns_match_for_quote_number_send(det):
    rfqs = {"rfq_alpha": {"reytech_quote_number": "R26Q40",
                          "gmail_message_ids": []}}
    pcs = {}
    list_fn, meta_fn = _setup_gmail_mocks({
        "msg_outbound_1": {
            "subject": "Reytech Quote R26Q40 — please review",
            "thread_id": "thread_zzz",
            "to": "buyer@cchcs.ca.gov",
            "date": "Tue, 06 May 2026 15:30:00 -0700",
        },
    })
    with patch("src.core.gmail_api.list_message_ids",
               side_effect=list_fn), \
         patch("src.core.gmail_api.get_message_metadata",
               side_effect=meta_fn):
        result = det.detect_observed_sends(
            rfqs=rfqs, pcs=pcs, gmail_service="fake",
            since_days=7, max_messages=10,
        )

    assert result["ok"] is True
    assert result["scanned"] == 1
    assert len(result["matches"]) == 1
    m = result["matches"][0]
    assert m["matched_record_id"] == "rfq_alpha"
    assert m["match_signal"] == "quote_number"
    assert m["already_attached"] is False


def test_detect_flags_already_attached_message(det):
    """Message-id already in record.gmail_message_ids → already_attached=True.
    The match still appears in matches[] (operator may want to verify)
    but downstream "this is a missed send" UI should suppress it."""
    rfqs = {"rfq_alpha": {
        "reytech_quote_number": "R26Q40",
        "gmail_message_ids": ["msg_outbound_1"],
    }}
    pcs = {}
    list_fn, meta_fn = _setup_gmail_mocks({
        "msg_outbound_1": {
            "subject": "Reytech Quote R26Q40",
            "thread_id": "thread_zzz",
        },
    })
    with patch("src.core.gmail_api.list_message_ids",
               side_effect=list_fn), \
         patch("src.core.gmail_api.get_message_metadata",
               side_effect=meta_fn):
        result = det.detect_observed_sends(
            rfqs=rfqs, pcs=pcs, gmail_service="fake")

    assert result["matches"][0]["already_attached"] is True


def test_detect_skips_non_quote_subjects(det):
    rfqs, pcs = {}, {}
    list_fn, meta_fn = _setup_gmail_mocks({
        "msg_lunch":   {"subject": "Lunch tomorrow?"},
        "msg_vacation": {"subject": "Out of office until Monday"},
    })
    with patch("src.core.gmail_api.list_message_ids",
               side_effect=list_fn), \
         patch("src.core.gmail_api.get_message_metadata",
               side_effect=meta_fn):
        result = det.detect_observed_sends(
            rfqs=rfqs, pcs=pcs, gmail_service="fake")

    assert result["scanned"] == 2
    assert result["skipped_non_quote"] == 2
    assert result["matches"] == []
    assert result["unmatched"] == []


def test_detect_unmatched_when_quote_number_unknown(det):
    """Subject looks like a quote send, but no record carries that
    quote number — list it as unmatched so operator can investigate
    (often a manually-typed quote number that drifted from the system)."""
    rfqs = {"rfq_alpha": {"reytech_quote_number": "R26Q1"}}
    pcs = {}
    list_fn, meta_fn = _setup_gmail_mocks({
        "msg_outbound": {
            "subject": "Reytech Quote R26Q999 — orphan",
            "thread_id": "thread_unknown",
        },
    })
    with patch("src.core.gmail_api.list_message_ids",
               side_effect=list_fn), \
         patch("src.core.gmail_api.get_message_metadata",
               side_effect=meta_fn):
        result = det.detect_observed_sends(
            rfqs=rfqs, pcs=pcs, gmail_service="fake")

    assert len(result["unmatched"]) == 1
    assert result["unmatched"][0]["match_signal"] == "no_match"


def test_detect_handles_empty_sent_folder(det):
    rfqs, pcs = {}, {}
    list_fn, meta_fn = _setup_gmail_mocks({})
    with patch("src.core.gmail_api.list_message_ids",
               side_effect=list_fn), \
         patch("src.core.gmail_api.get_message_metadata",
               side_effect=meta_fn):
        result = det.detect_observed_sends(
            rfqs=rfqs, pcs=pcs, gmail_service="fake")

    assert result["ok"] is True
    assert result["scanned"] == 0
    assert result["matches"] == []
    assert result["unmatched"] == []


def test_detect_returns_error_when_gmail_list_fails(det):
    rfqs, pcs = {}, {}
    with patch("src.core.gmail_api.list_message_ids",
               side_effect=RuntimeError("rate limit")):
        result = det.detect_observed_sends(
            rfqs=rfqs, pcs=pcs, gmail_service="fake")

    assert result["ok"] is False
    assert "list_message_ids failed" in result["error"]


def test_detect_routes_pc_match(det):
    """PCs match the same way as RFQs — verify both kinds wire."""
    rfqs = {}
    pcs = {"pc_beta": {"reytech_quote_number": "R26Q42"}}
    list_fn, meta_fn = _setup_gmail_mocks({
        "msg_outbound": {
            "subject": "Reytech Quote R26Q42 (price check)",
        },
    })
    with patch("src.core.gmail_api.list_message_ids",
               side_effect=list_fn), \
         patch("src.core.gmail_api.get_message_metadata",
               side_effect=meta_fn):
        result = det.detect_observed_sends(
            rfqs=rfqs, pcs=pcs, gmail_service="fake")

    assert result["matches"][0]["matched_record_kind"] == "pc"
    assert result["matches"][0]["matched_record_id"] == "pc_beta"
