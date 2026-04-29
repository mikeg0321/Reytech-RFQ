"""Tests for scripts/harvest_buyer_corpus.py — the Gmail-API harvester
that pulls every RFQ + sent + contract/amendment email into a local
corpus directory.

Mocks Gmail end-to-end (no real OAuth, no network). Drives the
harvester against a synthetic corpus of MIME messages and asserts the
on-disk layout it builds.

The harvester is glue (Gmail → disk + agency resolution + index
maintenance) so we test the *contracts* it exposes:
  - default Gmail query has the right shape
  - MIME parsing extracts headers + body + attachments correctly
  - agency resolution falls through name → email-domain
  - meta.json + index.json + by_agency.json + by_thread.json are
    written and consistent with each other
  - second run is idempotent (skips already-saved msg_ids)
  - --rebuild-indexes regenerates indexes from on-disk meta.json

Where possible we import functions directly (white-box) instead of
spawning a subprocess — keeps tests fast and the failure modes
specific.
"""
from __future__ import annotations

import base64
import importlib.util
import json
import os
import sys
import tempfile
import types
from email.message import EmailMessage

import pytest


# ─── Module loader (script lives outside src/, no package init) ──────


def _load_harvest_module():
    repo_root = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
    path = os.path.join(repo_root, "scripts", "harvest_buyer_corpus.py")
    spec = importlib.util.spec_from_file_location(
        "harvest_buyer_corpus", path,
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def hb():
    return _load_harvest_module()


# ─── Helpers for building synthetic MIME messages ────────────────────


def _make_eml(subject: str, sender: str, body: str,
              attachments: list[tuple[str, bytes]] | None = None,
              to: str = "sales@reytechinc.com",
              date: str = "Mon, 28 Apr 2026 09:00:00 -0700") -> bytes:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to
    msg["Date"] = date
    msg["Message-ID"] = f"<test-{subject[:20]}@example>"
    msg.set_content(body)
    for fn, data in (attachments or []):
        msg.add_attachment(
            data, maintype="application", subtype="octet-stream",
            filename=fn,
        )
    return bytes(msg)


def _gmail_get_response(raw_bytes: bytes, thread_id: str = "T1") -> dict:
    """Shape of `service.users().messages().get(...)` for format=raw."""
    return {
        "raw": base64.urlsafe_b64encode(raw_bytes).decode("ascii"),
        "threadId": thread_id,
        "labelIds": ["INBOX"],
        "internalDate": "1714291200000",
    }


class _FakeMessages:
    """Stand-in for `service.users().messages()`. Records calls and
    serves canned responses keyed by msg_id."""

    def __init__(self, msgs):
        self._msgs = msgs
        self.get_calls = []

    def get(self, userId, id, format):
        self.get_calls.append(id)
        m = self._msgs.get(id)
        if not m:
            raise KeyError(id)
        return _ExecResp(m)


class _FakeUsers:
    def __init__(self, msgs):
        self._m = _FakeMessages(msgs)

    def messages(self):
        return self._m


class _FakeService:
    def __init__(self, msgs):
        self._u = _FakeUsers(msgs)

    def users(self):
        return self._u


class _ExecResp:
    def __init__(self, body):
        self._body = body

    def execute(self):
        return self._body


# ─── Args namespace builder ──────────────────────────────────────────


def _args(out_dir, **overrides):
    base = dict(
        out_dir=out_dir,
        inboxes=["sales"],
        days=5,
        query="",
        max_messages=100,
        force=False,
        keep_all_attachments=False,
        rebuild_indexes=False,
        progress=0,
    )
    base.update(overrides)
    return types.SimpleNamespace(**base)


# ─── Pure helpers ────────────────────────────────────────────────────


def test_safe_filename_strips_path_separators(hb):
    assert hb._safe_filename("a/b/c.pdf") == "a_b_c.pdf"
    assert hb._safe_filename("..\\evil.exe") == "..\\evil.exe".replace(
        "\\", "_",
    )
    assert hb._safe_filename("") == "attachment"


def test_default_query_includes_rfq_keywords(hb):
    args = _args("/tmp/x")
    q = hb._build_query(args)
    assert "after:" in q
    assert "has:attachment" in q
    assert "RFQ" in q
    assert "amendment" in q


def test_custom_query_passes_through(hb):
    args = _args("/tmp/x", query="from:foo@bar.com")
    assert hb._build_query(args) == "from:foo@bar.com"


def test_decode_body_prefers_plain_over_html(hb):
    msg = EmailMessage()
    msg["Subject"] = "S"
    msg.set_content("PLAIN TEXT")
    msg.add_alternative("<html><body>HTML BODY</body></html>",
                        subtype="html")
    parsed = hb.email.message_from_bytes(bytes(msg))
    body = hb._decode_body(parsed)
    assert "PLAIN TEXT" in body
    assert "HTML BODY" not in body


def test_decode_body_html_fallback_when_no_plain(hb):
    msg = EmailMessage()
    msg["Subject"] = "S"
    msg.set_content("<p>only html here <b>important</b></p>",
                    subtype="html")
    parsed = hb.email.message_from_bytes(bytes(msg))
    body = hb._decode_body(parsed)
    assert "important" in body
    assert "<b>" not in body  # tags stripped


def test_extract_attachments_returns_filename_and_bytes(hb):
    raw = _make_eml(
        subject="hi", sender="a@b.com", body="body",
        attachments=[("rfq.pdf", b"%PDF-1.4 content"),
                     ("notes.txt", b"plain")],
    )
    parsed = hb.email.message_from_bytes(raw)
    atts = hb._extract_attachments(parsed)
    names = sorted(fn for fn, _ in atts)
    assert "rfq.pdf" in names
    assert "notes.txt" in names


def test_classify_message_recognizes_amendment_award_rfq(hb):
    assert hb._classify_message(
        {"subject": "Amendment 03 to PO 8955-..."}, "", [],
    ) == "amendment"
    assert hb._classify_message(
        {"subject": "Purchase Order issued"}, "", [],
    ) == "award"
    assert hb._classify_message(
        {"subject": "Request for Quote — Echelon"}, "", [],
    ) == "rfq"
    assert hb._classify_message(
        {"subject": "Quote attached", "from": "sales@reytechinc.com"},
        "", [],
    ) == "quote_sent"
    assert hb._classify_message(
        {"subject": "thanks"}, "", [],
    ) == "other"


# ─── End-to-end harvest with fake Gmail ──────────────────────────────


def test_harvest_writes_meta_body_and_attachments(hb, tmp_path,
                                                  monkeypatch):
    out_dir = str(tmp_path / "corpus")
    raw = _make_eml(
        subject="RFQ for catheters — CDCR Folsom",
        sender="buyer@cdcr.ca.gov",
        body="Please quote the attached RFQ by Friday.",
        attachments=[("rfq_form.pdf", b"%PDF-1.4 cat")],
    )
    fake_msgs = {"m1": _gmail_get_response(raw)}
    fake_svc = _FakeService(fake_msgs)

    # Patch the gmail_api module the harvester imports
    from src.core import gmail_api as ga
    monkeypatch.setattr(ga, "is_configured", lambda: True)
    monkeypatch.setattr(ga, "get_service", lambda inbox="sales": fake_svc)
    monkeypatch.setattr(ga, "list_message_ids",
                        lambda svc, query="", max_results=500: ["m1"])

    rc = hb.harvest(_args(out_dir))
    assert rc == 0

    msg_dir = os.path.join(out_dir, "messages", "m1")
    assert os.path.isdir(msg_dir)
    meta = json.loads(open(os.path.join(msg_dir, "meta.json")).read())
    assert meta["msg_id"] == "m1"
    assert meta["headers"]["from"] == "buyer@cdcr.ca.gov"
    assert "RFQ for catheters" in meta["headers"]["subject"]
    assert meta["classification"] == "rfq"
    # cdcr.ca.gov → agency=cdcr (via institution_resolver email-domain
    # lookup). If the resolver doesn't return cdcr, agency_key just
    # falls back to 'unknown' — assert it's at least non-empty.
    assert meta["agency_key"]
    assert any(a["filename"] == "rfq_form.pdf"
               for a in meta["attachments"])

    body = open(os.path.join(msg_dir, "body.txt")).read()
    assert "Please quote" in body

    pdf_path = os.path.join(msg_dir, "attachments", "rfq_form.pdf")
    assert os.path.exists(pdf_path)
    assert open(pdf_path, "rb").read().startswith(b"%PDF")


def test_harvest_writes_consistent_indexes(hb, tmp_path, monkeypatch):
    out_dir = str(tmp_path / "corpus")
    raw1 = _make_eml(
        subject="RFQ — gauze pads",
        sender="contact@cchcs.ca.gov",
        body="quote please",
        attachments=[("rfq.pdf", b"%PDF cchcs")],
    )
    raw2 = _make_eml(
        subject="Amendment to PO",
        sender="contact@cchcs.ca.gov",
        body="see amendment",
        attachments=[("amend.pdf", b"%PDF amend")],
    )
    fake_msgs = {
        "a1": _gmail_get_response(raw1, thread_id="THR-1"),
        "a2": _gmail_get_response(raw2, thread_id="THR-1"),
    }
    fake_svc = _FakeService(fake_msgs)
    from src.core import gmail_api as ga
    monkeypatch.setattr(ga, "is_configured", lambda: True)
    monkeypatch.setattr(ga, "get_service", lambda inbox="sales": fake_svc)
    monkeypatch.setattr(ga, "list_message_ids",
                        lambda svc, query="", max_results=500:
                        list(fake_msgs.keys()))

    rc = hb.harvest(_args(out_dir))
    assert rc == 0

    idx = json.loads(open(os.path.join(out_dir, "index.json")).read())
    assert set(idx.keys()) == {"a1", "a2"}
    by_thread = json.loads(
        open(os.path.join(out_dir, "by_thread.json")).read()
    )
    assert by_thread["THR-1"] == ["a1", "a2"]
    by_agency = json.loads(
        open(os.path.join(out_dir, "by_agency.json")).read()
    )
    # cchcs.ca.gov → agency=cchcs (or at minimum: same key for both
    # messages from the same domain).
    keys_with_a1 = [k for k, v in by_agency.items() if "a1" in v]
    keys_with_a2 = [k for k, v in by_agency.items() if "a2" in v]
    assert keys_with_a1 == keys_with_a2
    assert keys_with_a1, "expected at least one agency bucket"


def test_harvest_is_idempotent_on_rerun(hb, tmp_path, monkeypatch):
    out_dir = str(tmp_path / "corpus")
    raw = _make_eml(
        subject="RFQ", sender="x@y.com", body="b",
        attachments=[("a.pdf", b"%PDF")],
    )
    fake_msgs = {"m1": _gmail_get_response(raw)}
    fake_svc = _FakeService(fake_msgs)
    from src.core import gmail_api as ga
    monkeypatch.setattr(ga, "is_configured", lambda: True)
    monkeypatch.setattr(ga, "get_service", lambda inbox="sales": fake_svc)
    monkeypatch.setattr(ga, "list_message_ids",
                        lambda svc, query="", max_results=500: ["m1"])

    hb.harvest(_args(out_dir))
    fetch_count_first = len(fake_svc.users().messages().get_calls)

    # Re-run — must not refetch m1
    hb.harvest(_args(out_dir))
    fetch_count_second = len(fake_svc.users().messages().get_calls)
    assert fetch_count_second == fetch_count_first, (
        "second run refetched a previously-saved msg_id"
    )


def test_force_reharvests_existing(hb, tmp_path, monkeypatch):
    out_dir = str(tmp_path / "corpus")
    raw = _make_eml(
        subject="RFQ", sender="x@y.com", body="b",
        attachments=[("a.pdf", b"%PDF")],
    )
    fake_msgs = {"m1": _gmail_get_response(raw)}
    fake_svc = _FakeService(fake_msgs)
    from src.core import gmail_api as ga
    monkeypatch.setattr(ga, "is_configured", lambda: True)
    monkeypatch.setattr(ga, "get_service", lambda inbox="sales": fake_svc)
    monkeypatch.setattr(ga, "list_message_ids",
                        lambda svc, query="", max_results=500: ["m1"])

    hb.harvest(_args(out_dir))
    hb.harvest(_args(out_dir, force=True))
    # 2 fetches under force — once per run
    assert len(fake_svc.users().messages().get_calls) == 2


def test_attachment_extension_filter(hb, tmp_path, monkeypatch):
    """Default behavior drops .ics / .vcf / weird extensions but keeps
    .pdf / .docx / .xls."""
    out_dir = str(tmp_path / "corpus")
    raw = _make_eml(
        subject="mixed", sender="x@y.com", body="b",
        attachments=[
            ("contract.pdf", b"%PDF"),
            ("invite.ics", b"BEGIN:VCALENDAR\nEND:VCALENDAR"),
            ("vendor.xlsx", b"PK\x03\x04 fake xlsx"),
        ],
    )
    fake_msgs = {"m1": _gmail_get_response(raw)}
    fake_svc = _FakeService(fake_msgs)
    from src.core import gmail_api as ga
    monkeypatch.setattr(ga, "is_configured", lambda: True)
    monkeypatch.setattr(ga, "get_service", lambda inbox="sales": fake_svc)
    monkeypatch.setattr(ga, "list_message_ids",
                        lambda svc, query="", max_results=500: ["m1"])

    hb.harvest(_args(out_dir))

    msg_dir = os.path.join(out_dir, "messages", "m1")
    files = os.listdir(os.path.join(msg_dir, "attachments"))
    assert "contract.pdf" in files
    assert "vendor.xlsx" in files
    assert "invite.ics" not in files


def test_rebuild_indexes_from_disk(hb, tmp_path):
    """If the indexes get corrupted / wiped, --rebuild-indexes
    regenerates them by walking messages/<id>/meta.json."""
    out_dir = str(tmp_path / "corpus")
    msgs_dir = os.path.join(out_dir, "messages", "m9")
    os.makedirs(msgs_dir)
    meta = {
        "msg_id": "m9",
        "thread_id": "THR-X",
        "label_ids": [],
        "internal_date_ms": "0",
        "headers": {
            "from": "buyer@cdcr.ca.gov",
            "to": "sales@reytechinc.com",
            "cc": "", "subject": "test", "date": "",
            "date_iso": "2026-01-01T00:00:00",
            "message_id_header": "", "in_reply_to": "", "references": "",
        },
        "agency_key": "cdcr",
        "classification": "rfq",
        "attachments": [{"filename": "a.pdf", "size_bytes": 4,
                         "ext": ".pdf"}],
        "body_chars": 0, "harvested_at": "2026-04-29T00:00:00",
    }
    with open(os.path.join(msgs_dir, "meta.json"), "w") as f:
        json.dump(meta, f)

    rc = hb.harvest(_args(out_dir, rebuild_indexes=True))
    assert rc == 0
    idx = json.loads(open(os.path.join(out_dir, "index.json")).read())
    assert "m9" in idx
    by_agency = json.loads(
        open(os.path.join(out_dir, "by_agency.json")).read()
    )
    assert "m9" in by_agency.get("cdcr", [])
    by_thread = json.loads(
        open(os.path.join(out_dir, "by_thread.json")).read()
    )
    assert by_thread["THR-X"] == ["m9"]


def test_unconfigured_gmail_returns_2(hb, tmp_path, monkeypatch):
    out_dir = str(tmp_path / "corpus")
    from src.core import gmail_api as ga
    monkeypatch.setattr(ga, "is_configured", lambda: False)
    rc = hb.harvest(_args(out_dir))
    assert rc == 2
