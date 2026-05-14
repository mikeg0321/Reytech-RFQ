"""PR-AO — attachment-deadline extraction tier.

The four ⚠ DEFAULT records on /home (pc_93edc64e, pc_5728f934,
rfq_0ebe242f, rfq_b57f85f7) all had:
  - email body with no parsable deadline
  - email subject with no parsable deadline
  - a PDF attachment whose cover page stated "Due By: 5/13/26"
    in the text

`apply_default_if_missing` runs subject → body → default and stops.
The attachment PDF text was never scanned. PR-AO adds the missing
tier between `email` and `default`:
  - `extract_deadline_from_pdf` — pdfplumber + existing regex
  - `apply_attachment_if_default` — upgrades `default` → `attachment`
  - `/api/admin/heal-due-dates` — retroactive sweep for old records

Tests pin:
  1. pdfplumber extracts text from a synthetic PDF and the regex
     hits "Due By: 05/13/2026" / "Due Date: 5/13/26".
  2. `apply_attachment_if_default` upgrades a record when source is
     `default` AND a PDF yields a deadline.
  3. `apply_attachment_if_default` is a no-op when source is anything
     else (header / subject / email / attachment) — we never
     DOWNGRADE a higher-trust source.
  4. `apply_attachment_if_default` is a no-op when no PDF yields a
     deadline (record's default anchor stays put).
  5. The heal route's outer filter scans `due_date_source==default`
     records, walks their `rfq_files` BLOBs, upgrades, and re-runs
     are idempotent (records_upgraded converges to 0 on a 2nd call).
  6. Heal route dry_run=true returns the same `records_upgraded`
     count but does NOT mutate.
"""
from __future__ import annotations

import os
import tempfile

import pytest


def _write_pdf_with_text(text: str) -> str:
    """Synthesize a 1-page PDF containing the given text in body."""
    from reportlab.pdfgen import canvas

    fd, path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    c = canvas.Canvas(path)
    # Lay out a few lines so pdfplumber sees real text, not whitespace
    y = 750
    for line in text.split("\n"):
        c.drawString(72, y, line)
        y -= 20
    c.showPage()
    c.save()
    return path


# ── 1. extract_deadline_from_pdf ────────────────────────────────────


def test_extract_deadline_from_pdf_labeled_pattern():
    """A PDF cover page with 'Due Date: 5/13/2026' is extracted."""
    from src.core.attachment_deadline import extract_deadline_from_pdf

    path = _write_pdf_with_text(
        "REQUEST FOR QUOTATION\n"
        "Solicitation Number: 25CB021\n"
        "Due Date: 5/13/2026\n"
        "Please respond to all line items below.\n"
    )
    try:
        date_iso, _time = extract_deadline_from_pdf(path)
        assert date_iso == "2026-05-13"
    finally:
        os.unlink(path)


def test_extract_deadline_from_pdf_no_deadline_returns_none():
    """A PDF with no due-date text returns (None, None)."""
    from src.core.attachment_deadline import extract_deadline_from_pdf

    path = _write_pdf_with_text(
        "REQUEST FOR QUOTATION\n"
        "Solicitation Number: 25CB021\n"
        "Please respond with pricing and lead time.\n"
    )
    try:
        date_iso, _time = extract_deadline_from_pdf(path)
        assert date_iso is None
    finally:
        os.unlink(path)


def test_extract_deadline_from_pdf_missing_path_returns_none():
    """A non-existent path is handled gracefully."""
    from src.core.attachment_deadline import extract_deadline_from_pdf

    date_iso, _time = extract_deadline_from_pdf("/nonexistent/path.pdf")
    assert date_iso is None


# ── 2. apply_attachment_if_default ──────────────────────────────────


def test_apply_attachment_if_default_upgrades_default():
    """Record stamped `default` + PDF with deadline → source upgraded."""
    from src.core.deadline_defaults import apply_attachment_if_default

    path = _write_pdf_with_text("Due Date: 5/13/2026\n")
    try:
        doc = {
            "due_date": "05/16/2026",  # the default anchor
            "due_date_source": "default",
        }
        result = apply_attachment_if_default(doc, [path])
        assert result == "attachment"
        assert doc["due_date"] == "2026-05-13"
        assert doc["due_date_source"] == "attachment"
        assert doc["due_date_attachment"] == os.path.basename(path)
    finally:
        os.unlink(path)


def test_apply_attachment_if_default_noop_when_source_is_email():
    """Already upgraded to email-source — never downgrade."""
    from src.core.deadline_defaults import apply_attachment_if_default

    path = _write_pdf_with_text("Due Date: 5/13/2026\n")
    try:
        doc = {
            "due_date": "2026-05-15",  # buyer-stated, body-source
            "due_date_source": "email",
        }
        result = apply_attachment_if_default(doc, [path])
        assert result is None
        # Source untouched.
        assert doc["due_date"] == "2026-05-15"
        assert doc["due_date_source"] == "email"
        assert "due_date_attachment" not in doc
    finally:
        os.unlink(path)


def test_apply_attachment_if_default_noop_when_pdf_has_no_deadline():
    """PDF without a parsable date — default anchor stays."""
    from src.core.deadline_defaults import apply_attachment_if_default

    path = _write_pdf_with_text("Vendor pricing only please.\n")
    try:
        doc = {
            "due_date": "05/16/2026",
            "due_date_source": "default",
        }
        result = apply_attachment_if_default(doc, [path])
        assert result is None
        assert doc["due_date_source"] == "default"
        assert doc["due_date"] == "05/16/2026"
    finally:
        os.unlink(path)


def test_apply_attachment_if_default_first_hit_wins():
    """Multiple PDFs — the first one yielding a date wins; later
    files aren't scanned."""
    from src.core.deadline_defaults import apply_attachment_if_default

    p1 = _write_pdf_with_text("Due Date: 5/13/2026\n")
    p2 = _write_pdf_with_text("Due Date: 5/20/2026\n")
    try:
        doc = {"due_date": "05/16/2026", "due_date_source": "default"}
        result = apply_attachment_if_default(doc, [p1, p2])
        assert result == "attachment"
        # First file (p1)'s date wins.
        assert doc["due_date"] == "2026-05-13"
        assert doc["due_date_attachment"] == os.path.basename(p1)
    finally:
        os.unlink(p1)
        os.unlink(p2)


# ── 3. Heal route ───────────────────────────────────────────────────


@pytest.fixture
def _rfq_files_table(temp_data_dir):
    """The test temp DB doesn't auto-create rfq_files; we use it here."""
    from src.api.dashboard import _init_rfq_files_table
    _init_rfq_files_table()
    yield


def _make_buyer_pdf_in_rfq_files(client, rfq_id: str, deadline_text: str):
    """Helper: write a PDF directly to rfq_files BLOB for a record."""
    from src.api.dashboard import save_rfq_file

    path = _write_pdf_with_text(deadline_text)
    try:
        with open(path, "rb") as fh:
            data = fh.read()
        save_rfq_file(
            rfq_id,
            "buyer_rfq.pdf",
            "application/pdf",
            data,
            category="buyer_attachment",
            uploaded_by="test",
        )
    finally:
        os.unlink(path)


def test_heal_due_dates_upgrades_default_records(client, temp_data_dir, _rfq_files_table):
    """A PC stamped `default` with a buyer_attachment PDF containing
    a deadline is upgraded by the heal route."""
    from src.api.dashboard import _save_single_pc, _load_price_checks

    pcid = "pc_ao_test_pos"
    pc = {
        "id": pcid,
        "pc_number": "TEST-AO-PC",
        "status": "parsed",
        "agency": "cchcs",
        "due_date": "05/16/2026",
        "due_time": "02:00 PM",
        "due_date_source": "default",
        "items": [],
    }
    _save_single_pc(pcid, pc)
    _make_buyer_pdf_in_rfq_files(client, pcid, "Due Date: 5/13/2026\n")

    resp = client.post(
        "/api/admin/heal-due-dates",
        json={"dry_run": False},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["records_upgraded"] >= 1
    assert pcid in body["summary"]
    assert body["summary"][pcid]["date"] == "2026-05-13"

    # Verify persisted.
    pcs = _load_price_checks()
    assert pcs[pcid]["due_date"] == "2026-05-13"
    assert pcs[pcid]["due_date_source"] == "attachment"
    assert pcs[pcid]["due_date_attachment"] == "buyer_rfq.pdf"


def test_heal_due_dates_is_idempotent(client, temp_data_dir, _rfq_files_table):
    """A second heal pass on an already-upgraded record reports 0
    upgraded — apply_attachment_if_default short-circuits when
    source != 'default'."""
    from src.api.dashboard import _save_single_pc

    pcid = "pc_ao_test_idem"
    pc = {
        "id": pcid,
        "pc_number": "TEST-AO-IDEM",
        "status": "parsed",
        "agency": "cchcs",
        "due_date": "05/16/2026",
        "due_date_source": "default",
        "items": [],
    }
    _save_single_pc(pcid, pc)
    _make_buyer_pdf_in_rfq_files(client, pcid, "Due Date: 5/13/2026\n")

    # First pass: upgrades.
    r1 = client.post(
        "/api/admin/heal-due-dates",
        json={"dry_run": False},
    )
    assert r1.status_code == 200
    assert r1.get_json()["records_upgraded"] >= 1

    # Second pass: target record's source is now 'attachment' →
    # outer filter (`due_date_source == "default"`) excludes it.
    r2 = client.post(
        "/api/admin/heal-due-dates",
        json={"dry_run": False},
    )
    assert r2.status_code == 200
    body2 = r2.get_json()
    assert pcid not in body2.get("summary", {})


def test_heal_due_dates_dry_run_does_not_mutate(client, temp_data_dir, _rfq_files_table):
    """dry_run=true reports counts but doesn't write."""
    from src.api.dashboard import _save_single_pc, _load_price_checks

    pcid = "pc_ao_test_dry"
    pc = {
        "id": pcid,
        "pc_number": "TEST-AO-DRY",
        "status": "parsed",
        "agency": "cchcs",
        "due_date": "05/16/2026",
        "due_date_source": "default",
        "items": [],
    }
    _save_single_pc(pcid, pc)
    _make_buyer_pdf_in_rfq_files(client, pcid, "Due Date: 5/13/2026\n")

    resp = client.post(
        "/api/admin/heal-due-dates",
        json={"dry_run": True},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["dry_run"] is True
    # Summary should still include the target so operator can see
    # what WOULD upgrade — but the persisted record is unchanged.
    assert pcid in body["summary"]

    pcs = _load_price_checks()
    assert pcs[pcid]["due_date_source"] == "default"
    assert pcs[pcid]["due_date"] == "05/16/2026"


def test_heal_due_dates_skips_records_without_buyer_attachment(client, temp_data_dir, _rfq_files_table):
    """A `default`-source record with no rfq_files entry stays default —
    nothing to scan."""
    from src.api.dashboard import _save_single_pc, _load_price_checks

    pcid = "pc_ao_test_noattach"
    pc = {
        "id": pcid,
        "pc_number": "TEST-AO-NOATT",
        "status": "parsed",
        "agency": "cchcs",
        "due_date": "05/16/2026",
        "due_date_source": "default",
        "items": [],
    }
    _save_single_pc(pcid, pc)
    # NO _make_buyer_pdf_in_rfq_files — record has no attachments.

    resp = client.post(
        "/api/admin/heal-due-dates",
        json={"dry_run": False},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    assert pcid not in body.get("summary", {})

    pcs = _load_price_checks()
    assert pcs[pcid]["due_date_source"] == "default"
