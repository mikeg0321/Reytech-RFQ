"""Smoke guards for scripts/ingest_inbox_rfqs.py.

The script's job is to hand a hand-picked list of inbox RFQ emails to
`process_rfq_email`. End-to-end tests would require mocking the Gmail
API + email message bytes + dashboard globals, which is out of scope for
a one-off. These tests cover the parts that can drift silently:

  - Targets list is shaped as (label, query, agency) triples and queries
    are scoped `in:inbox` (so we never accidentally pull archived mail).
  - `_identify_form` routes the specific PDF names we expect from the 5
    current inbox RFQs to the right template slot.
  - `_build_rfq_info` produces a dict whose keys are the ones
    `process_rfq_email` dedupes and routes on.
"""
from __future__ import annotations

import importlib
import sys
import os
import email as email_pkg

import pytest


@pytest.fixture(scope="module")
def ingest_mod():
    # scripts/ is not on sys.path by default — load by file path
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    scripts_dir = os.path.join(root, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    if "ingest_inbox_rfqs" in sys.modules:
        del sys.modules["ingest_inbox_rfqs"]
    return importlib.import_module("ingest_inbox_rfqs")


def test_targets_shape(ingest_mod):
    assert len(ingest_mod.TARGETS) == 5, "Expected 5 hand-picked inbox RFQs"
    for entry in ingest_mod.TARGETS:
        assert len(entry) == 3, f"target tuple must be (label, query, agency): {entry}"
        label, query, agency = entry
        assert isinstance(label, str) and label
        assert isinstance(query, str) and "in:inbox" in query, (
            f"target {label} must search in:inbox, got {query!r}"
        )
        assert isinstance(agency, str) and agency


def test_targets_cover_expected_solicitations(ingest_mod):
    queries = " | ".join(q for _, q, _ in ingest_mod.TARGETS)
    for sol in ("20026", "10844466", "10843164", "10840486", "10837703"):
        assert sol in queries, f"target list missing solicitation {sol}"


def test_identify_form_routes_known_pdfs(ingest_mod):
    f = ingest_mod._identify_form
    assert f("AMS 703B Reytech.pdf") == "703b"
    assert f("ams_703a_blank.pdf") == "703a"
    assert f("CCHCS 704B.pdf") == "704b"
    assert f("Fair and Reasonable 703C.pdf") == "703c"
    assert f("Bid_Package_12345.pdf") == "bidpkg"
    assert f("random_attachment.pdf") == "unknown"


def test_build_rfq_info_mirrors_poller_shape(ingest_mod):
    raw = (
        b"From: buyer@cchcs.ca.gov\r\n"
        b"Subject: RFQ 10840486 Test\r\n"
        b"Date: Mon, 20 Apr 2026 09:00:00 -0700\r\n"
        b"Message-ID: <abc@cchcs>\r\n"
        b"Content-Type: text/plain\r\n"
        b"\r\n"
        b"Please quote the attached items.\r\n"
    )
    msg = email_pkg.message_from_bytes(raw)
    info = ingest_mod._build_rfq_info(msg, msg_id="gmailid123", attachments=[],
                                      rfq_dir="/tmp/whatever")

    # These keys are read by process_rfq_email — all must be present.
    required = {
        "id", "email_uid", "message_id", "subject", "sender",
        "sender_email", "date", "solicitation_hint", "attachments",
        "body_text", "body_preview",
    }
    missing = required - set(info)
    assert not missing, f"rfq_info missing keys: {missing}"

    assert info["email_uid"] == "gmailid123"
    assert info["subject"] == "RFQ 10840486 Test"
    assert info["sender_email"] == "buyer@cchcs.ca.gov"
    assert info["solicitation_hint"] == "10840486", (
        "solicitation hint should be pulled from subject"
    )
    assert "Please quote" in info["body_text"]


def test_build_rfq_info_handles_five_digit_solicitation(ingest_mod):
    raw = (
        b"From: dvbe@example.ca.gov\r\n"
        b"Subject: Fwd: RFQ SNF Residents CA DVBE #20026\r\n"
        b"Content-Type: text/plain\r\n\r\nBody\r\n"
    )
    msg = email_pkg.message_from_bytes(raw)
    info = ingest_mod._build_rfq_info(msg, "gmailid456", [], "/tmp/x")
    assert info["solicitation_hint"] == "20026"


# ── Corpus fallback ──────────────────────────────────────────────────
#
# The corpus-first path lets us ingest when Gmail auth isn't available
# locally (and on future runs where we've already mined the thread). We
# only test the pure-python helpers — actual dashboard.process_rfq_email
# integration is covered by the smoke layer above.

import json  # noqa: E402
import shutil  # noqa: E402


def _write_corpus(tmp_path, inbox, records):
    cdir = tmp_path / "email_corpus"
    cdir.mkdir(exist_ok=True)
    jsonl = cdir / f"{inbox}_corpus.jsonl"
    with open(jsonl, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return cdir


def test_subject_matches_requires_all_tokens(ingest_mod):
    m = ingest_mod._subject_matches
    assert m("RFQ 10840486 CCHCS", "in:inbox subject:10840486") is True
    assert m("RFQ 10840486 CCHCS",
             "in:inbox subject:10840486 subject:CCHCS") is True
    # missing second token
    assert m("RFQ 10840486",
             "in:inbox subject:10840486 subject:CCHCS") is False
    # case-insensitive
    assert m("rfq SNF residents 20026",
             "in:inbox subject:20026 subject:SNF") is True
    # no subject tokens → can't match (prevents accidental fuzzy hits)
    assert m("RFQ anything", "in:inbox") is False


def test_find_in_corpus_returns_newest(ingest_mod, tmp_path):
    records = [
        {
            "gmail_id": "aaa111",
            "subject": "RFQ 10840486 CCHCS older",
            "parsed_date": "2026-03-01T10:00:00",
            "from_addr": "old@cchcs.ca.gov",
        },
        {
            "gmail_id": "bbb222",
            "subject": "RFQ 10840486 CCHCS newer",
            "parsed_date": "2026-04-18T10:00:00",
            "from_addr": "new@cchcs.ca.gov",
        },
        {
            "gmail_id": "ccc333",
            "subject": "Unrelated thread",
            "parsed_date": "2026-04-19T10:00:00",
            "from_addr": "misc@example.com",
        },
    ]
    corpus = _write_corpus(tmp_path, "sales", records)
    hit = ingest_mod.find_in_corpus(
        "in:inbox subject:10840486", str(corpus), inbox_hint="sales")
    assert hit is not None
    assert hit["gmail_id"] == "bbb222"
    assert hit["_inbox"] == "sales"


def test_find_in_corpus_returns_none_when_missing(ingest_mod, tmp_path):
    corpus = _write_corpus(tmp_path, "sales", [
        {"gmail_id": "x", "subject": "other", "parsed_date": "2026-01-01"}
    ])
    assert ingest_mod.find_in_corpus(
        "in:inbox subject:99999999", str(corpus)) is None

    # Missing corpus dir is fine — we just return None.
    assert ingest_mod.find_in_corpus(
        "in:inbox subject:anything", str(tmp_path / "does_not_exist")) is None


def test_build_info_from_corpus_copies_pdf_attachments(ingest_mod, tmp_path):
    # Lay out a corpus attachment dir as the miner would have written it.
    gmail_id = "dead1234beef5678"
    inbox = "sales"
    corpus_dir = tmp_path / "email_corpus"
    att_src = corpus_dir / "attachments" / inbox / gmail_id[:16]
    att_src.mkdir(parents=True)
    (att_src / "AMS 703B Reytech.pdf").write_bytes(b"%PDF-1.4 fake\n")
    (att_src / "random_image.png").write_bytes(b"\x89PNG fake")  # ignored

    record = {
        "gmail_id": gmail_id,
        "subject": "RFQ 10840486 CCHCS Test",
        "from_addr": "buyer@cchcs.ca.gov",
        "from_email": "buyer@cchcs.ca.gov",
        "date_str": "Mon, 20 Apr 2026 09:00:00 -0700",
        "body_preview": "Please quote.",
        "message_id": "<msg@cchcs>",
        "_inbox": inbox,
        "_attachment_dir": str(att_src),
    }
    rfq_dir = tmp_path / "uploads" / "rfq_x"
    info = ingest_mod._build_info_from_corpus(record, str(rfq_dir))

    assert info["email_uid"] == gmail_id
    assert info["subject"] == "RFQ 10840486 CCHCS Test"
    assert info["sender_email"] == "buyer@cchcs.ca.gov"
    assert info["solicitation_hint"] == "10840486"
    # Only the PDF was copied; the PNG was ignored.
    assert len(info["attachments"]) == 1
    att = info["attachments"][0]
    assert att["type"] == "703b"
    assert os.path.exists(att["path"])
    # Safe filename was used (spaces preserved, special chars scrubbed).
    assert att["filename"].endswith(".pdf")
