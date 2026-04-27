"""Phase 1.6 PR3c: buyer_template_capture tests + endpoint smoke."""

import io
import os
import sqlite3
from unittest.mock import patch

import pytest


def _ensure_table(db_path):
    """Create buyer_template_candidates if not present (test isolation)."""
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS buyer_template_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fingerprint TEXT NOT NULL,
            agency_key TEXT NOT NULL DEFAULT '',
            form_type_guess TEXT NOT NULL DEFAULT '',
            sample_filename TEXT NOT NULL DEFAULT '',
            sample_quote_id TEXT NOT NULL DEFAULT '',
            sample_quote_type TEXT NOT NULL DEFAULT '',
            field_count INTEGER NOT NULL DEFAULT 0,
            page_count INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            seen_count INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'candidate',
            promoted_profile_id TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT ''
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_buyer_template_candidates_dedup
            ON buyer_template_candidates(fingerprint, agency_key);
    """)
    conn.commit()
    conn.close()


def _make_acroform_pdf(path, fields=None):
    """Build a tiny PDF with optional AcroForm text fields."""
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
    except ImportError:
        pytest.skip("reportlab not available")

    fields = fields or ["Name", "Email", "Date"]
    c = canvas.Canvas(str(path), pagesize=letter)
    c.acroForm.textfield(name=fields[0], x=72, y=720, width=200, height=20)
    if len(fields) > 1:
        c.acroForm.textfield(name=fields[1], x=72, y=690, width=200, height=20)
    if len(fields) > 2:
        c.acroForm.textfield(name=fields[2], x=72, y=660, width=120, height=20)
    c.save()


# ─── Module-level unit tests (don't touch DB) ─────────────────────────────

class TestGuessFormType:
    def test_703b_filename_match(self):
        from src.agents.buyer_template_capture import _guess_form_type
        assert _guess_form_type("703B_quote_worksheet.pdf") == "703b"
        assert _guess_form_type("AMS 703-b Folsom.pdf") == "703b"

    def test_dvbe_match(self):
        from src.agents.buyer_template_capture import _guess_form_type
        assert _guess_form_type("DVBE_843.pdf") == "dvbe843"
        assert _guess_form_type("std 843 declaration.pdf") == "dvbe843"

    def test_no_match_returns_empty(self):
        from src.agents.buyer_template_capture import _guess_form_type
        assert _guess_form_type("random_attachment.pdf") == ""
        assert _guess_form_type("") == ""


class TestIsPdf:
    def test_extension_match(self):
        from src.agents.buyer_template_capture import _is_pdf
        assert _is_pdf({"filename": "x.pdf"}) is True
        assert _is_pdf({"filename": "x.docx"}) is False


# ─── Fingerprint + register integration ───────────────────────────────────

class TestRegisterAttachment:
    def test_skipped_for_non_pdf(self):
        from src.agents.buyer_template_capture import register_attachment
        r = register_attachment("PC-1", "pc",
                                 {"filename": "notes.docx", "file_path": "/x"})
        assert r["status"] == "skipped_no_pdf"

    def test_skipped_when_no_acroform_fields(self, tmp_path):
        from src.agents.buyer_template_capture import register_attachment
        # Build a flat PDF (no AcroForm)
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
        except ImportError:
            pytest.skip("reportlab")
        flat = tmp_path / "flat.pdf"
        c = canvas.Canvas(str(flat), pagesize=letter)
        c.drawString(72, 720, "no fields here")
        c.save()
        r = register_attachment("PC-1", "pc",
                                 {"filename": "flat.pdf",
                                  "file_path": str(flat),
                                  "file_type": "pdf"})
        assert r["status"] == "skipped_no_fingerprint"

    def test_new_candidate_inserted(self, tmp_path, app):
        # Mock the fingerprint helper rather than building real AcroForm PDFs
        # — reportlab + AcroForm has page-flush quirks not worth fighting in a
        # logic test
        from src.agents.buyer_template_capture import register_attachment
        from src.core.db import DB_PATH
        _ensure_table(DB_PATH)

        pdf = tmp_path / "703B_buyer.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%mock\n")

        with patch("src.agents.buyer_template_capture._fingerprint_attachment",
                   return_value=("a"*64, 8, 2)):
            r = register_attachment("PC-1", "pc",
                                    {"filename": "703B_buyer.pdf",
                                     "file_path": str(pdf),
                                     "file_type": "pdf"},
                                    agency_key="cdcr_folsom")
        assert r["ok"] is True
        assert r["status"] == "new_candidate"
        assert r["form_type_guess"] == "703b"
        assert len(r["fingerprint"]) == 16

    def test_dedup_increments_seen_count(self, tmp_path, app):
        from src.agents.buyer_template_capture import register_attachment
        from src.core.db import DB_PATH
        _ensure_table(DB_PATH)

        pdf = tmp_path / "703B_buyer.pdf"
        pdf.write_bytes(b"%PDF-1.4\n%mock\n")

        att = {"filename": "703B_buyer.pdf",
               "file_path": str(pdf), "file_type": "pdf"}
        with patch("src.agents.buyer_template_capture._fingerprint_attachment",
                   return_value=("b"*64, 5, 1)):
            r1 = register_attachment("PC-1", "pc", att,
                                     agency_key="cdcr_folsom")
            r2 = register_attachment("PC-2", "pc", att,
                                     agency_key="cdcr_folsom")
        assert r1["status"] == "new_candidate"
        assert r2["status"] == "existing_candidate"
        assert r1["candidate_id"] == r2["candidate_id"]

    def test_matched_profile_skips_candidate(self, tmp_path, app):
        from src.agents.buyer_template_capture import register_attachment
        from src.core.db import DB_PATH
        _ensure_table(DB_PATH)

        pdf = tmp_path / "x.pdf"
        pdf.write_bytes(b"%PDF-1.4\n")
        with patch("src.agents.buyer_template_capture._fingerprint_attachment",
                   return_value=("c"*64, 3, 1)), \
             patch("src.agents.buyer_template_capture._match_profile_by_fingerprint",
                   return_value="703b_reytech_standard"):
            r = register_attachment("PC-9", "pc",
                                    {"filename": "x.pdf",
                                     "file_path": str(pdf), "file_type": "pdf"})
        assert r["status"] == "matched_profile"
        assert r["profile_id"] == "703b_reytech_standard"


# ─── Endpoint smoke tests ─────────────────────────────────────────────────

class TestBuyerTemplatesEndpoint:
    def test_list_empty_returns_zero(self, client, app):
        from src.core.db import DB_PATH
        _ensure_table(DB_PATH)
        # Wipe in case other tests left rows
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM buyer_template_candidates")
        conn.commit(); conn.close()

        r = client.get("/api/buyer-templates/candidates")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["count"] == 0
        assert d["candidates"] == []

    def test_scan_unknown_quote_returns_404(self, client):
        r = client.post("/api/buyer-templates/scan/pc/no_such_quote")
        assert r.status_code == 404
        assert r.get_json()["ok"] is False

    def test_scan_invalid_type(self, client):
        r = client.post("/api/buyer-templates/scan/bogus/abc")
        assert r.status_code == 400

    def test_lookup_unknown_fingerprint(self, client, app):
        from src.core.db import DB_PATH
        _ensure_table(DB_PATH)
        r = client.get("/api/buyer-templates/lookup/" + "0" * 64)
        assert r.status_code == 404
