"""Phase 1.6 Enhancement B: candidate-badge surface tests.

When the buyer attaches a blank PDF whose fingerprint isn't in any
FormProfile, the panel renders a "🆕 NEW VARIANT" badge. The fill_plan
builder must populate `candidate_id`, `candidate_fingerprint`, and
`candidate_seen_count` on the matching FillPlanItem.

Tests that mock buyer_template_capture._fingerprint_attachment are
gated on that module being importable — PR3c may not be merged yet
on a given branch.
"""

import json
import sqlite3
from unittest.mock import patch

import pytest

from src.agents.fill_plan_builder import (
    build_fill_plan,
    _candidate_for_attachment,
)
from src.forms.profile_registry import FormProfile, FieldMapping


def _has_buyer_template_capture():
    try:
        import src.agents.buyer_template_capture  # noqa: F401
        return True
    except ImportError:
        return False


_BTC_AVAILABLE = _has_buyer_template_capture()


def _ensure_candidates_table(db_path):
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
    """)
    conn.commit()
    conn.close()


def _profile(pid, form_type="703b", agency_match=None,
             fields=None, signature_field="Sig"):
    return FormProfile(
        id=pid, form_type=form_type, blank_pdf="",
        fill_mode="acroform", fingerprint="fp_" + pid,
        agency_match=list(agency_match or []),
        fields=list(fields or []),
        signature_field=signature_field,
    )


class TestCandidateForAttachment:
    def test_returns_zeros_when_no_attachments(self, app):
        r = _candidate_for_attachment("703b", "cdcr_folsom", [], "")
        assert r == (0, "", 0)

    def test_returns_zeros_when_filename_not_in_attachments(self, app):
        r = _candidate_for_attachment(
            "703b", "cdcr_folsom",
            [{"filename": "other.pdf", "file_path": "/x"}],
            "703B_buyer.pdf",
        )
        assert r == (0, "", 0)

    @pytest.mark.skipif(not _BTC_AVAILABLE,
                        reason="buyer_template_capture not on this branch yet")
    def test_returns_candidate_when_fingerprint_matches(self, app, tmp_path):
        from src.core.db import DB_PATH
        _ensure_candidates_table(DB_PATH)

        # Insert a candidate
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            """INSERT INTO buyer_template_candidates
                (fingerprint, agency_key, form_type_guess, sample_filename, seen_count)
               VALUES (?, ?, ?, ?, ?)""",
            ("a" * 64, "cdcr_folsom", "703b", "buyer.pdf", 5),
        )
        conn.commit()
        conn.close()

        pdf = tmp_path / "buyer.pdf"
        pdf.write_bytes(b"%PDF-1.4\n")
        attached = [{"filename": "buyer.pdf", "file_path": str(pdf),
                     "file_type": "pdf"}]

        with patch("src.agents.buyer_template_capture._fingerprint_attachment",
                   return_value=("a" * 64, 5, 2)):
            r = _candidate_for_attachment(
                "703b", "cdcr_folsom", attached, "buyer.pdf",
            )
        cand_id, fp, seen = r
        assert cand_id > 0
        assert fp == "a" * 16
        assert seen == 5

    def test_returns_zeros_when_table_missing(self, app):
        # Even if buyer_template_capture import fails, function returns zeros
        with patch.dict("sys.modules",
                        {"src.agents.buyer_template_capture": None}):
            r = _candidate_for_attachment("703b", "cdcr_folsom",
                                           [{"filename": "x.pdf"}], "x.pdf")
        assert r == (0, "", 0)


class TestFillPlanItemCandidateFields:
    def test_candidate_fields_default_zero(self, app):
        std = _profile("703b_std", "703b",
                       fields=[FieldMapping(semantic="vendor.name",
                                             pdf_field="V"),
                               FieldMapping(semantic="items[0].unit_price",
                                             pdf_field="P")])
        quote = {
            "id": "PC-1",
            "agency": "CDCR Folsom",
            "institution": "CDCR Folsom",
            "requirements_json": json.dumps({"forms_required": ["703b"]}),
            "source_file": "",
        }
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={"703b_std": std}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cdcr_folsom",
                                 {"name": "CDCR Folsom", "required_forms": []})), \
             patch("src.agents.fill_plan_builder._list_attachments",
                   return_value=[]):
            plan = build_fill_plan("PC-1", "pc", quote_data=quote)
        item = next(it for it in plan.items if it.form_id == "703b")
        assert item.candidate_id == 0
        assert item.candidate_fingerprint == ""
        assert item.candidate_seen_count == 0

    @pytest.mark.skipif(not _BTC_AVAILABLE,
                        reason="buyer_template_capture not on this branch yet")
    def test_candidate_populated_when_attachment_has_unknown_fingerprint(
            self, app, tmp_path):
        from src.core.db import DB_PATH
        _ensure_candidates_table(DB_PATH)

        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            """INSERT INTO buyer_template_candidates
                (fingerprint, agency_key, form_type_guess, sample_filename, seen_count)
               VALUES (?, ?, ?, ?, ?)""",
            ("b" * 64, "cdcr_folsom", "703b", "703B_unknown_variant.pdf", 3),
        )
        conn.commit()
        conn.close()

        pdf = tmp_path / "703B_unknown_variant.pdf"
        pdf.write_bytes(b"%PDF-1.4\n")
        attached = [{"filename": "703B_unknown_variant.pdf",
                     "file_path": str(pdf), "file_type": "pdf"}]

        std = _profile("703b_std", "703b",
                       fields=[FieldMapping(semantic="vendor.name",
                                             pdf_field="V"),
                               FieldMapping(semantic="items[0].unit_price",
                                             pdf_field="P")])
        quote = {
            "id": "PC-2", "agency": "CDCR Folsom",
            "institution": "CDCR Folsom",
            "requirements_json": json.dumps({"forms_required": ["703b"]}),
            "source_file": str(pdf),
        }

        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={"703b_std": std}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cdcr_folsom",
                                 {"name": "CDCR Folsom", "required_forms": []})), \
             patch("src.agents.fill_plan_builder._list_attachments",
                   return_value=attached), \
             patch("src.agents.buyer_template_capture._fingerprint_attachment",
                   return_value=("b" * 64, 3, 2)):
            plan = build_fill_plan("PC-2", "pc", quote_data=quote)

        item = next(it for it in plan.items if it.form_id == "703b")
        # Should have buyer_template_filename + candidate metadata
        assert item.buyer_template_filename == "703B_unknown_variant.pdf"
        assert item.candidate_id > 0
        assert item.candidate_fingerprint == "b" * 16
        assert item.candidate_seen_count == 3

    def test_serializes_to_json(self, app):
        std = _profile("703b_std", "703b")
        quote = {
            "id": "PC-3", "agency": "CDCR Folsom",
            "institution": "CDCR Folsom",
            "requirements_json": json.dumps({"forms_required": ["703b"]}),
            "source_file": "",
        }
        with patch("src.agents.fill_plan_builder._load_profiles_safe",
                   return_value={"703b_std": std}), \
             patch("src.agents.fill_plan_builder._resolve_agency",
                   return_value=("cdcr_folsom",
                                 {"name": "CDCR Folsom", "required_forms": []})), \
             patch("src.agents.fill_plan_builder._list_attachments",
                   return_value=[]):
            plan = build_fill_plan("PC-3", "pc", quote_data=quote)
        rt = json.loads(json.dumps(plan.to_dict()))
        for it in rt["items"]:
            assert "candidate_id" in it
            assert "candidate_fingerprint" in it
            assert "candidate_seen_count" in it
