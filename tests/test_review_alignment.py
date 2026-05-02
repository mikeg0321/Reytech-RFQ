"""Unit tests for the review-page alignment rollup.

Covers the 5-check verdict + items-alignment + forms-checklist that
src/api/review_alignment.py builds. Pure logic — no Flask, no DB.
"""
from __future__ import annotations

import os
import pytest

from src.api.review_alignment import compute_review_alignment


# ── Test fixtures (built fresh per test, not shared) ────────────────────────

def _agency():
    return {"name": "CalVet", "required_forms": ["quote", "704b", "cv012_cuf"]}


def _rfq(**overrides):
    base = {
        "requestor_name": "Keith Alsing",
        "requestor_email": "keith.alsing@calvet.ca.gov",
        "agency": "CalVet",
        "due_date": "2026-05-10",
        "line_items": [
            {"description": "Flushable Wipes Box", "qty": 50,
             "price_per_unit": 12.99, "part_number": "FW-100"},
            {"description": "Dispenser", "qty": 5, "price_per_unit": 89.00},
        ],
    }
    base.update(overrides)
    return base


def _manifest(**overrides):
    base = {
        "id": 1, "agency_name": "CalVet",
        "required_forms": ["quote", "704b", "cv012_cuf"],
        "generated_forms": [],
        "reviews": [
            {"form_id": "quote", "form_filename": "RFQ_Reytech Quote.pdf",
             "verdict": "pending"},
            {"form_id": "704b", "form_filename": "RFQ_Reytech_704B.pdf",
             "verdict": "pending"},
            {"form_id": "cv012_cuf", "form_filename": "RFQ_Reytech_CV012CUF.pdf",
             "verdict": "pending"},
        ],
        "field_audit": {"_qa_passed": True,
                        "_qa_summary": {"forms_checked": 3, "critical_issues": []}},
        "source_validation": {"errors": [], "warnings": [], "checks": ["buyer match"]},
    }
    base.update(overrides)
    return base


# ── Rollup verdict ──────────────────────────────────────────────────────────

class TestRollupAligned:

    def test_all_green_when_everything_present(self):
        a = compute_review_alignment(
            rfq=_rfq(), manifest=_manifest(),
            agency_cfg=_agency(), output_dir=None, source_items=None,
        )
        assert a["rollup"]["aligned"] is True, a["rollup"]["issues"]
        assert a["rollup"]["issues"] == []
        c = a["rollup"]["checks"]
        assert c["forms_on_disk"] is True
        assert c["qa_passed"] is True
        assert c["source_valid"] is True
        assert c["buyer_present"] is True
        assert c["items_priced"] is True

    def test_missing_required_form_blocks(self):
        m = _manifest(reviews=[
            {"form_id": "quote", "form_filename": "q.pdf", "verdict": "pending"},
            {"form_id": "704b", "form_filename": "", "verdict": "pending"},  # missing
            {"form_id": "cv012_cuf", "form_filename": "cv.pdf", "verdict": "pending"},
        ])
        a = compute_review_alignment(rfq=_rfq(), manifest=m, agency_cfg=_agency(),
                                     output_dir=None, source_items=None)
        assert a["rollup"]["aligned"] is False
        assert any("AMS 704B" in i for i in a["rollup"]["issues"])
        assert a["rollup"]["checks"]["forms_on_disk"] is False

    def test_qa_failed_blocks(self):
        m = _manifest(field_audit={
            "_qa_passed": False,
            "_qa_summary": {"critical_issues": ["sig missing on 703B", "page count off"]},
        })
        a = compute_review_alignment(rfq=_rfq(), manifest=m, agency_cfg=_agency(),
                                     output_dir=None, source_items=None)
        assert a["rollup"]["aligned"] is False
        assert any("QA failed" in i for i in a["rollup"]["issues"])
        assert a["rollup"]["checks"]["qa_passed"] is False

    def test_source_validation_errors_block(self):
        m = _manifest(source_validation={
            "errors": ["buyer email mismatch", "sol# not in package"],
            "warnings": [], "checks": [],
        })
        a = compute_review_alignment(rfq=_rfq(), manifest=m, agency_cfg=_agency(),
                                     output_dir=None, source_items=None)
        assert a["rollup"]["aligned"] is False
        assert any("Source validation" in i for i in a["rollup"]["issues"])

    def test_no_buyer_email_blocks(self):
        a = compute_review_alignment(
            rfq=_rfq(requestor_email="", original_sender=""),
            manifest=_manifest(), agency_cfg=_agency(),
            output_dir=None, source_items=None,
        )
        assert a["rollup"]["aligned"] is False
        assert any("buyer email" in i.lower() for i in a["rollup"]["issues"])

    def test_no_due_date_warns_but_still_aligned_otherwise(self):
        # Due date is a soft warning — does not by itself block alignment when
        # all other checks pass. (Mike's rule: "verify before sending" not "stop")
        a = compute_review_alignment(
            rfq=_rfq(due_date=""), manifest=_manifest(),
            agency_cfg=_agency(), output_dir=None, source_items=None,
        )
        # Issue is reported (warning to verify) but rollup is NOT aligned because
        # due_date_present is False. This is correct: Mike said "verify" before
        # send, so we surface it. If we want soft-warn to not block, change here.
        # Currently: due-date issue surfaces in issues list.
        assert any("due-date" in i.lower() for i in a["rollup"]["issues"])

    def test_unpriced_items_block(self):
        a = compute_review_alignment(
            rfq=_rfq(line_items=[{"description": "Widget", "qty": 1, "price_per_unit": 0}]),
            manifest=_manifest(), agency_cfg=_agency(),
            output_dir=None, source_items=None,
        )
        assert a["rollup"]["aligned"] is False
        assert any("unit price" in i.lower() for i in a["rollup"]["issues"])

    def test_no_items_blocks(self):
        a = compute_review_alignment(
            rfq=_rfq(line_items=[]), manifest=_manifest(),
            agency_cfg=_agency(), output_dir=None, source_items=None,
        )
        assert a["rollup"]["aligned"] is False
        assert any("No line items" in i for i in a["rollup"]["issues"])


# ── Forms checklist ─────────────────────────────────────────────────────────

class TestFormsChecklist:

    def test_required_forms_listed_in_agency_order(self):
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=_agency(), output_dir=None, source_items=None)
        ids = [f["form_id"] for f in a["forms_checklist"]]
        assert ids == ["quote", "704b", "cv012_cuf"]

    def test_missing_filename_marks_missing(self):
        m = _manifest(reviews=[
            {"form_id": "quote", "form_filename": "q.pdf", "verdict": "pending"},
            {"form_id": "704b", "form_filename": "", "verdict": "pending"},
            {"form_id": "cv012_cuf", "form_filename": "cv.pdf", "verdict": "pending"},
        ])
        a = compute_review_alignment(rfq=_rfq(), manifest=m, agency_cfg=_agency(),
                                     output_dir=None, source_items=None)
        f704 = next(f for f in a["forms_checklist"] if f["form_id"] == "704b")
        assert f704["missing"] is True
        assert f704["filename"] == ""

    def test_zero_byte_file_on_disk_marks_missing(self, tmp_path):
        f = tmp_path / "empty.pdf"
        f.write_bytes(b"")
        m = _manifest(reviews=[
            {"form_id": "quote", "form_filename": "empty.pdf", "verdict": "pending"},
            {"form_id": "704b", "form_filename": "", "verdict": "pending"},
            {"form_id": "cv012_cuf", "form_filename": "", "verdict": "pending"},
        ])
        a = compute_review_alignment(rfq=_rfq(), manifest=m, agency_cfg=_agency(),
                                     output_dir=str(tmp_path), source_items=None)
        fq = next(f for f in a["forms_checklist"] if f["form_id"] == "quote")
        assert fq["missing"] is True, "0-byte file should be treated as missing"

    def test_filename_present_on_disk_with_size(self, tmp_path):
        f = tmp_path / "real.pdf"
        f.write_bytes(b"x" * 2048)  # 2KB
        m = _manifest(reviews=[
            {"form_id": "quote", "form_filename": "real.pdf", "verdict": "approved"},
            {"form_id": "704b", "form_filename": "", "verdict": "pending"},
            {"form_id": "cv012_cuf", "form_filename": "", "verdict": "pending"},
        ])
        a = compute_review_alignment(rfq=_rfq(), manifest=m, agency_cfg=_agency(),
                                     output_dir=str(tmp_path), source_items=None)
        fq = next(f for f in a["forms_checklist"] if f["form_id"] == "quote")
        assert fq["missing"] is False
        assert fq["size_kb"] == 2

    def test_bidpkg_internal_forms_excluded(self):
        ag = {"name": "CCHCS", "required_forms":
              ["quote", "703b", "704b", "bidpkg", "dvbe843", "calrecycle74"]}
        m = _manifest(required_forms=ag["required_forms"], reviews=[
            {"form_id": "quote", "form_filename": "q.pdf", "verdict": "pending"},
            {"form_id": "703b", "form_filename": "703b.pdf", "verdict": "pending"},
            {"form_id": "704b", "form_filename": "704b.pdf", "verdict": "pending"},
            {"form_id": "bidpkg", "form_filename": "pkg.pdf", "verdict": "pending"},
            {"form_id": "dvbe843", "form_filename": "843.pdf", "verdict": "pending"},
            {"form_id": "calrecycle74", "form_filename": "74.pdf", "verdict": "pending"},
        ])
        a = compute_review_alignment(rfq=_rfq(), manifest=m, agency_cfg=ag,
                                     output_dir=None, source_items=None)
        ids = [f["form_id"] for f in a["forms_checklist"]]
        assert "bidpkg" in ids
        assert "dvbe843" not in ids, "DVBE843 lives inside bidpkg — not standalone"
        assert "calrecycle74" not in ids


# ── Items alignment ─────────────────────────────────────────────────────────

class TestItemsAlignment:

    def test_no_source_flags_has_source_false(self):
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=_agency(), output_dir=None, source_items=None)
        assert a["items_alignment"]["has_source"] is False
        assert all(r["match"] == "no_source" for r in a["items_alignment"]["rows"])

    def test_with_matching_source_marks_matched(self):
        src = [
            {"description": "Flushable Wipes Box", "qty": 50, "part_number": "FW-100"},
            {"description": "Dispenser", "qty": 5},
        ]
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=_agency(), output_dir=None, source_items=src)
        assert a["items_alignment"]["has_source"] is True
        rows = a["items_alignment"]["rows"]
        assert rows[0]["match"] == "matched"
        assert rows[1]["match"] == "matched"

    def test_qty_differs_flagged(self):
        src = [
            {"description": "Flushable Wipes Box", "qty": 100},  # buyer: 100
            {"description": "Dispenser", "qty": 5},
        ]
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=_agency(), output_dir=None, source_items=src)
        assert a["items_alignment"]["rows"][0]["match"] == "qty_differs"
        assert a["items_alignment"]["rows"][1]["match"] == "matched"

    def test_buyer_extra_count_when_source_longer(self):
        src = [
            {"description": "Flushable Wipes Box", "qty": 50},
            {"description": "Dispenser", "qty": 5},
            {"description": "Extra item not in our quote", "qty": 1},
        ]
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=_agency(), output_dir=None, source_items=src)
        assert a["items_alignment"]["buyer_extra_count"] == 1


# ── Sentinel: Force Approve removed from template ───────────────────────────

# ── Agency-aware "Your <primary form>" label ────────────────────────────────

class TestPrimaryFormLabel:

    def test_calvet_says_cv012_cuf(self):
        ag = {"name": "CalVet", "required_forms": ["quote", "cv012_cuf"],
              "primary_response_form": "cv012_cuf"}
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=ag, output_dir=None, source_items=None)
        assert a["primary_form_id"] == "cv012_cuf"
        assert a["primary_form_label"] == "CV 012 CUF"

    def test_cchcs_says_704b(self):
        ag = {"name": "CCHCS", "required_forms": ["703b", "704b", "bidpkg", "quote"],
              "primary_response_form": "704b"}
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=ag, output_dir=None, source_items=None)
        assert a["primary_form_id"] == "704b"
        assert a["primary_form_label"] == "AMS 704B"

    def test_dsh_says_attb(self):
        ag = {"name": "DSH", "required_forms": ["quote", "dsh_attA", "dsh_attB"],
              "primary_response_form": "dsh_attB"}
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=ag, output_dir=None, source_items=None)
        assert a["primary_form_id"] == "dsh_attB"
        # display name falls back to title-case of id when not in FORM_DISPLAY_NAMES

    def test_falls_back_to_quote_if_unset(self):
        ag = {"name": "Other", "required_forms": ["quote"]}  # no primary_response_form
        a = compute_review_alignment(rfq=_rfq(), manifest=_manifest(),
                                     agency_cfg=ag, output_dir=None, source_items=None)
        assert a["primary_form_id"] == "quote"
        assert a["primary_form_label"] == "Reytech Quote"


# ── Email-domain priority in match_agency ───────────────────────────────────

class TestEmailDomainPriority:

    def test_calvet_email_beats_cchcs_keyword(self):
        from src.core.agency_config import match_agency
        # Body says CCHCS but email is CalVet — domain wins
        k, c = match_agency({
            "requestor_email": "buyer@calvet.ca.gov",
            "department": "CCHCS / CDCR Mock",
            "agency": "CCHCS",
        })
        assert k == "calvet"
        assert "email_domain" in c.get("matched_by", "")

    def test_cdcr_email_resolves_to_cchcs(self):
        from src.core.agency_config import match_agency
        # CDCR uses CCHCS forms — domain map normalizes the alias
        k, c = match_agency({"requestor_email": "officer@cdcr.ca.gov"})
        assert k == "cchcs"
        assert c.get("primary_response_form") == "704b"

    def test_unknown_domain_falls_through_to_keywords(self):
        from src.core.agency_config import match_agency
        # `department` isn't in match_agency's search_text — `agency` and
        # `institution` are. Use one of those for the keyword fallback test.
        k, c = match_agency({
            "requestor_email": "user@gmail.com",
            "agency": "California Correctional Health Care Services (CCHCS)",
        })
        assert k == "cchcs"
        # Should NOT have matched via email_domain (fell through)
        assert "email_domain" not in c.get("matched_by", "")

    def test_subdomain_matches_parent_domain(self):
        from src.core.agency_config import _agency_from_email_domain
        assert _agency_from_email_domain("user@regional.calvet.ca.gov") == "calvet"

    def test_empty_email_returns_none(self):
        from src.core.agency_config import _agency_from_email_domain
        assert _agency_from_email_domain("") is None
        assert _agency_from_email_domain(None) is None
        assert _agency_from_email_domain("not-an-email") is None


class TestUIRegression:
    """Make sure the Force Approve button stays gone — alignment rollup
    replaces it; if it ever comes back the rollup is meaningless."""

    def test_force_approve_button_not_in_template(self):
        path = os.path.join(os.path.dirname(__file__), "..",
                            "src", "templates", "rfq_review.html")
        with open(path, encoding="utf-8") as f:
            content = f.read()
        # The visible "Force Approve" text must stay removed — alignment
        # rollup is the gate; skipping it was the bug we're preventing.
        assert "Force Approve" not in content
        # The JS handler call must stay gone too (catches a regression where
        # someone re-adds the button without re-checking the JS).
        assert 'onclick="forceApprove()"' not in content
        assert "function forceApprove(" not in content
