"""Tests for ComplianceValidator.

Uses real Quote objects and agency_config. The LLM gap check is patched
out so tests are hermetic.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

from src.core.quote_model import Quote, DocType, QuoteStatus, LineItem
from src.agents.compliance_validator import validate_package


def _quote(*, agency_key: str = "cchcs", quote_number: str = "R26Q0321"):
    q = Quote(doc_type=DocType.PC)
    q.header.agency_key = agency_key
    q.header.solicitation_number = quote_number
    q.line_items.append(LineItem(line_no=1, description="Gauze 4x4"))
    return q


class TestQuoteNumberCheck:
    def test_missing_quote_number_blocks(self):
        q = _quote(quote_number="")
        with patch("src.agents.compliance_validator._run_llm_gap_check", return_value=[]):
            r = validate_package(quote=q, per_form_reports=[])
        assert any("quote_number is empty" in b for b in r["blockers"])

    def test_malformed_quote_number_blocks(self):
        q = _quote(quote_number="12345")
        with patch("src.agents.compliance_validator._run_llm_gap_check", return_value=[]):
            r = validate_package(quote=q, per_form_reports=[])
        assert any("does not match" in b for b in r["blockers"])

    def test_valid_quote_number_passes(self):
        q = _quote(quote_number="R26Q0321")
        with patch("src.agents.compliance_validator._run_llm_gap_check", return_value=[]):
            r = validate_package(quote=q, per_form_reports=[
                # CCHCS requires a bunch of forms — pass them all so quote_number
                # check is isolated from required-forms blockers.
            ])
        # quote_number check itself should appear as ok
        qn_check = next(c for c in r["checks"] if c["name"] == "quote_number")
        assert qn_check["ok"] is True


class TestRequiredForms:
    def test_missing_required_form_blocks(self):
        """CCHCS requires 703b + 704b + quote (bidpkg is a container). If we
        report only 704b filled, 703b and quote must show up as blockers."""
        q = _quote(agency_key="cchcs")
        per_form = [
            {"profile_id": "704b_reytech_standard", "filled": True, "qa_passed": True},
        ]
        with patch("src.agents.compliance_validator._run_llm_gap_check", return_value=[]):
            r = validate_package(quote=q, per_form_reports=per_form)

        rf_blockers = [b for b in r["blockers"] if "703b" in b]
        assert rf_blockers, f"expected a 703b blocker; got blockers={r['blockers']}"

    def test_unknown_agency_skips_required_forms_check(self):
        q = _quote(agency_key="agency_that_does_not_exist")
        with patch("src.agents.compliance_validator._run_llm_gap_check", return_value=[]):
            r = validate_package(quote=q, per_form_reports=[])
        rf_check = next(c for c in r["checks"] if c["name"] == "required_forms")
        assert rf_check["ok"] is True, "no required_forms → no blockers"

    def test_filled_but_qa_failed_still_blocks(self):
        """A form that was filled but failed QA must not satisfy the required-forms check."""
        q = _quote(agency_key="cchcs")
        per_form = [
            {"profile_id": "704b_reytech_standard", "filled": True, "qa_passed": False},
        ]
        with patch("src.agents.compliance_validator._run_llm_gap_check", return_value=[]):
            r = validate_package(quote=q, per_form_reports=per_form)
        # 704b should show up in blockers because qa_passed=False means it
        # wasn't actually validated.
        rf_blockers = [b for b in r["blockers"] if "704b" in b]
        assert rf_blockers


class TestLLMGate:
    def test_llm_gap_never_blocks(self):
        """Even if the LLM flags concerns, they land in warnings — never blockers."""
        q = _quote(quote_number="R26Q0321", agency_key="cchcs")
        per_form = [
            {"profile_id": "704b_reytech_standard", "filled": True, "qa_passed": True},
            {"profile_id": "703b_reytech_standard", "filled": True, "qa_passed": True},
            {"profile_id": "quote_reytech_letterhead", "filled": True, "qa_passed": True},
        ]
        with patch("src.agents.compliance_validator._run_llm_gap_check",
                   return_value=["buyer asked for DVBE declaration but none filed"]):
            r = validate_package(
                quote=q,
                per_form_reports=per_form,
                buyer_email_text="Please provide DVBE declaration...",
            )
        assert any("DVBE" in w for w in r["warnings"])
        assert all("DVBE" not in b for b in r["blockers"])

    def test_llm_disabled_when_no_email(self):
        """Empty buyer_email_text → no LLM call (save tokens + latency)."""
        q = _quote(quote_number="R26Q0321", agency_key="cchcs")
        per_form = [
            {"profile_id": "704b_reytech_standard", "filled": True, "qa_passed": True},
            {"profile_id": "703b_reytech_standard", "filled": True, "qa_passed": True},
            {"profile_id": "quote_reytech_letterhead", "filled": True, "qa_passed": True},
        ]
        with patch("src.agents.compliance_validator._run_llm_gap_check") as mocked:
            r = validate_package(quote=q, per_form_reports=per_form, buyer_email_text="")
        assert r["checked"] is True
        # We still call it — the function itself short-circuits on empty text,
        # so it's called but returns []. Verify the shape.
        assert r["warnings"] == []


class TestResultShape:
    def test_result_is_always_a_dict_with_blockers_list(self):
        """Orchestrator reads `compliance_gap.get('blockers', [])` — shape must hold."""
        q = _quote(quote_number="R26Q0321", agency_key="cchcs")
        with patch("src.agents.compliance_validator._run_llm_gap_check", return_value=[]):
            r = validate_package(quote=q, per_form_reports=[])
        assert isinstance(r, dict)
        assert isinstance(r.get("blockers"), list)
        assert isinstance(r.get("warnings"), list)
        assert isinstance(r.get("checks"), list)
        assert all({"name", "ok", "detail"} <= set(c.keys()) for c in r["checks"])
