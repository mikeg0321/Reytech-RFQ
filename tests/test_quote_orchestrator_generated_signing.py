"""Tests that the generated stage refuses to advance when finalize() reports
the package is not OK — most importantly, when signing failed.

quote_engine.finalize() does this on signing failure:
    result.warnings.append(f"Signing failed: {e}")
    result.ok = False
... but result.merged_pdf still contains the UNSIGNED bytes. The
orchestrator's previous strict-completeness gate only checked pkg.errors,
missing artifacts, and merged_pdf truthiness — none of which fire on a
signing failure. So the orchestrator silently advanced to GENERATED, the
sent stage emailed the unsigned PDF, and the operator never knew the
package was missing the signature gate.

The fix: also block on `not pkg.ok`, surfacing pkg.warnings so the
operator sees WHY (e.g., the signing exception message).
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

from src.core.quote_model import Quote, QuoteStatus, DocType, LineItem
from src.core.quote_orchestrator import (
    QuoteOrchestrator,
    QuoteRequest,
    OrchestratorResult,
)


class _FakeProfile:
    def __init__(self, pid: str):
        self.id = pid


class _FakeArtifact:
    def __init__(self, profile_id: str, pdf_bytes: bytes):
        self.profile_id = profile_id
        self.pdf_bytes = pdf_bytes


class _FakePackage:
    def __init__(self, *, ok=True, artifacts=None, merged_pdf=b"%PDF-1.4 fake",
                 errors=None, warnings=None):
        self.ok = ok
        self.artifacts = artifacts or []
        self.merged_pdf = merged_pdf
        self.errors = errors or []
        self.warnings = warnings or []


def _qa_passed_quote() -> Quote:
    q = Quote(
        doc_type=DocType.PC,
        line_items=[LineItem(line_no=1, description="Gauze 4x4", qty=10,
                             unit_cost=Decimal("2.00"))],
        status=QuoteStatus.QA_PASS,
    )
    q.header.agency_key = "cchcs"
    q.header.solicitation_number = "R26Q0042"
    q.buyer.requestor_email = "buyer@cchcs.ca.gov"
    return q


class TestGeneratedChecksPackageOk:
    def test_signing_failure_blocks_generated(self):
        """The dangerous case: finalize() returns ok=False with merged_pdf
        still populated (unsigned bytes). Orchestrator must block, not
        send the unsigned PDF."""
        quote = _qa_passed_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        # Realistic shape: artifacts present, merged_pdf bytes present
        # (unsigned), errors empty, but ok=False because signing raised.
        pkg = _FakePackage(
            ok=False,
            artifacts=[_FakeArtifact("704b_reytech_standard", b"%PDF-1.4 unsigned")],
            merged_pdf=b"%PDF-1.4 unsigned-merged",
            warnings=["Signing failed: PIL.UnidentifiedImageError: cannot identify image"],
        )
        with patch("src.core.quote_engine.finalize", return_value=pkg):
            attempt = orch._try_advance(
                quote, "generated",
                QuoteRequest(target_stage="generated"),
                profiles, result,
            )

        assert attempt.outcome == "error", (
            f"expected error, got {attempt.outcome}: {attempt.reasons}"
        )
        assert any("signing failed" in r.lower() or "package not ok" in r.lower()
                   for r in attempt.reasons), attempt.reasons
        assert quote.status == QuoteStatus.QA_PASS

    def test_ok_true_advances(self):
        """Sanity check — pkg.ok=True still advances normally."""
        quote = _qa_passed_quote()
        profiles = [_FakeProfile("704b_reytech_standard")]
        result = OrchestratorResult(quote=quote)
        orch = QuoteOrchestrator(persist_audit=False)

        pkg = _FakePackage(
            ok=True,
            artifacts=[_FakeArtifact("704b_reytech_standard", b"%PDF-1.4 signed")],
            merged_pdf=b"%PDF-1.4 signed-merged",
        )
        with patch("src.core.quote_engine.finalize", return_value=pkg):
            attempt = orch._try_advance(
                quote, "generated",
                QuoteRequest(target_stage="generated"),
                profiles, result,
            )

        assert attempt.outcome == "advanced", f"reasons: {attempt.reasons}"
        assert quote.status == QuoteStatus.GENERATED
