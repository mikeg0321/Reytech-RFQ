"""Tests that compliance_validator._check_required_forms uses try_import
and surfaces import failures as SkipReasons (PR #183).

Before: two `try: from ... except: return []` paths silently bypassed
the required-forms check. After: try_import returns a SkipReason that
validate_package() propagates in its response dict, and the orchestrator
routes it via result.add_skip() into result.blockers.

The end-to-end contract verified here:
  1. validate_package() returns `skips: list[SkipReason]` in its response
  2. When the agency_config import fails, the SkipReason has BLOCKER severity
  3. The orchestrator consumes compliance_gap['skips'] and calls add_skip()
  4. Operator sees "src.core.agency_config: ImportError..." in result.blockers
"""
from __future__ import annotations

import importlib
from decimal import Decimal
from unittest.mock import patch

from src.agents.compliance_validator import _check_required_forms, validate_package
from src.core.dependency_check import Severity, SkipReason
from src.core.quote_model import Quote, DocType, LineItem, QuoteStatus
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    OrchestratorResult,
)


def _quote_with_agency() -> Quote:
    q = Quote(
        doc_type=DocType.PC,
        line_items=[LineItem(line_no=1, description="Gauze 4x4", qty=10, unit_cost=Decimal("2.00"))],
        status=QuoteStatus.PRICED,
    )
    q.header.agency_key = "cchcs"
    q.header.solicitation_number = "R26Q0042"
    q.buyer.requestor_email = "buyer@cchcs.ca.gov"
    return q


def _per_form_clean() -> list[dict]:
    return [
        {"profile_id": "704b_reytech_standard", "filled": True, "qa_passed": True, "bytes": 12345},
        {"profile_id": "703bc_reytech_standard", "filled": True, "qa_passed": True, "bytes": 12345},
        {"profile_id": "quote_reytech_letterhead", "filled": True, "qa_passed": True, "bytes": 12345},
    ]


def _patch_import_failure(*, fail_module: str):
    """Force `importlib.import_module(fail_module)` to raise.

    Patches at the importlib.import_module boundary because that is what
    `try_import` uses; patching builtins.__import__ would not take effect
    when the target module is already cached in sys.modules.
    """
    real_import_module = importlib.import_module

    def fake(name, package=None):
        if name == fail_module or name.startswith(fail_module + "."):
            raise ImportError(f"forced test failure: {fail_module} unavailable")
        return real_import_module(name, package)

    return patch.object(importlib, "import_module", side_effect=fake)


class TestCheckRequiredFormsReturnsSkipsTuple:
    def test_signature_returns_tuple_of_blockers_and_skips(self):
        """The function now returns (blockers, skips) — both lists."""
        result = _check_required_forms(_quote_with_agency(), _per_form_clean())
        assert isinstance(result, tuple) and len(result) == 2, result
        blockers, skips = result
        assert isinstance(blockers, list)
        assert isinstance(skips, list)

    def test_clean_run_returns_no_skips(self):
        blockers, skips = _check_required_forms(_quote_with_agency(), _per_form_clean())
        assert skips == [], skips

    def test_agency_config_import_failure_emits_blocker_skip(self):
        with _patch_import_failure(fail_module="src.core.agency_config"):
            blockers, skips = _check_required_forms(
                _quote_with_agency(), _per_form_clean(),
            )
        assert any(
            s.severity is Severity.BLOCKER
            and "agency_config" in s.name
            and "import" in s.reason.lower()
            for s in skips
        ), skips

    def test_form_map_import_failure_emits_blocker_skip(self):
        with _patch_import_failure(fail_module="src.core.quote_orchestrator"):
            blockers, skips = _check_required_forms(
                _quote_with_agency(), _per_form_clean(),
            )
        assert any(
            s.severity is Severity.BLOCKER
            and ("quote_orchestrator" in s.name or "_FORM_ID_TO_PROFILE_ID" in s.name)
            for s in skips
        ), skips


class TestValidatePackagePropagatesSkips:
    def test_validate_package_includes_skips_in_response(self):
        with _patch_import_failure(fail_module="src.core.agency_config"):
            result = validate_package(
                quote=_quote_with_agency(),
                per_form_reports=_per_form_clean(),
                buyer_email_text="",
            )
        assert "skips" in result, result.keys()
        assert any(
            isinstance(s, SkipReason)
            and s.severity is Severity.BLOCKER
            and "agency_config" in s.name
            for s in result["skips"]
        ), result["skips"]


class TestOrchestratorRoutesComplianceSkips:
    def test_qa_pass_refuses_when_compliance_emits_blocker_skip(self):
        """End-to-end: when validate_package's skips contain a BLOCKER,
        the orchestrator must route it via add_skip() so the operator
        sees the import-failure name in result.blockers (not a buried
        empty-default in compliance_report)."""
        from unittest.mock import patch as _patch

        class _Profile:
            id = "704b_reytech_standard"

        class _QAReport:
            passed = True
            warnings = []
            errors = []

        class _Draft:
            profile_id = "704b_reytech_standard"
            pdf_bytes = b"%PDF-1.4 fake"
            qa_report = _QAReport()

        quote = _quote_with_agency()
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        # Agency_config import fails inside _check_required_forms → BLOCKER skip
        with _patch_import_failure(fail_module="src.core.agency_config"), \
             _patch("src.core.quote_engine.draft", return_value=_Draft()):
            attempt = orch._try_advance(
                quote, "qa_pass", QuoteRequest(target_stage="qa_pass"),
                [_Profile()], result,
            )

        assert attempt.outcome == "error", (attempt.outcome, attempt.reasons)
        # The compliance skip must have routed into result.blockers, not just
        # buried in compliance_report.
        assert any(
            "agency_config" in b for b in result.blockers
        ), result.blockers
        # And the structured skip must be retained on result.skips.
        assert any(
            s.severity is Severity.BLOCKER and "agency_config" in s.name
            for s in result.skips
        ), result.skips
        # Quote status must NOT have transitioned.
        assert quote.status == QuoteStatus.PRICED
