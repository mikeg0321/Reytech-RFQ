"""Package Engine — orchestrates per-profile fills + merge into a complete bid package.

Single entry point: assemble(quote, profiles) → PackageResult.
Replaces the 2000-line generate_rfq_package monolith with ~150 lines.
Drive upload, email draft, and activity logging are pluggable post-hooks.

Usage:
    from src.forms.package_engine import assemble

    result = assemble(quote, [profile_704, profile_703b, profile_quote])
    if result.ok:
        for artifact in result.artifacts:
            print(f"  {artifact.name}: {len(artifact.pdf_bytes)} bytes")
"""
import io
import logging
import os
from dataclasses import dataclass, field
from typing import Callable, Optional

from src.core.quote_model import Quote
from src.forms.fill_engine import fill
from src.forms.qa_engine import validate, ValidationReport
from src.forms.profile_registry import FormProfile

log = logging.getLogger(__name__)


@dataclass
class Artifact:
    """Single generated PDF in the package."""
    name: str
    profile_id: str
    pdf_bytes: bytes
    qa_report: Optional[ValidationReport] = None

    @property
    def ok(self) -> bool:
        return self.qa_report.passed if self.qa_report else True


@dataclass
class PackageResult:
    """Result of assembling a complete bid package."""
    ok: bool
    artifacts: list[Artifact] = field(default_factory=list)
    merged_pdf: Optional[bytes] = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        passed = sum(1 for a in self.artifacts if a.ok)
        total = len(self.artifacts)
        return f"{passed}/{total} forms passed QA — {'PASS' if self.ok else 'FAIL'}"


# Post-assembly hooks — called after all forms are generated
PostHook = Callable[[Quote, PackageResult], None]
_post_hooks: list[PostHook] = []


def register_post_hook(hook: PostHook):
    """Register a function to run after package assembly (Drive upload, email, logging)."""
    _post_hooks.append(hook)


def assemble(
    quote: Quote,
    profiles: list[FormProfile],
    run_qa: bool = True,
    merge: bool = True,
) -> PackageResult:
    """Assemble a complete bid package from a Quote + list of profiles.

    For each profile:
      1. Fill the form using fill_engine
      2. QA the output using qa_engine (optional)
      3. Collect as an Artifact

    Then optionally merge all artifacts into one combined PDF.

    Args:
        quote: The canonical Quote object
        profiles: List of FormProfiles to fill (e.g., [704a, 703b, quote_pdf])
        run_qa: Whether to QA each filled form (default True)
        merge: Whether to merge all artifacts into one PDF (default True)

    Returns:
        PackageResult with individual artifacts + optional merged PDF
    """
    result = PackageResult(ok=True)

    for profile in profiles:
        try:
            # Fill
            pdf_bytes = fill(quote, profile)

            # QA
            qa_report = None
            if run_qa:
                try:
                    qa_report = validate(pdf_bytes, quote, profile)
                    if not qa_report.passed:
                        result.warnings.append(
                            f"{profile.id}: QA flagged {len(qa_report.issues)} issues "
                            f"({qa_report.match_rate}% match)"
                        )
                except Exception as qa_err:
                    log.warning("QA failed for %s: %s", profile.id, qa_err)
                    result.warnings.append(f"{profile.id}: QA skipped ({qa_err})")

            artifact = Artifact(
                name=f"{profile.id}.pdf",
                profile_id=profile.id,
                pdf_bytes=pdf_bytes,
                qa_report=qa_report,
            )
            result.artifacts.append(artifact)

            log.info("package_engine: %s filled (%d bytes, QA=%s)",
                     profile.id, len(pdf_bytes),
                     qa_report.summary if qa_report else "skipped")

        except Exception as e:
            log.error("package_engine: %s fill FAILED: %s", profile.id, e, exc_info=True)
            result.errors.append(f"{profile.id}: {e}")
            result.ok = False

    # Merge all artifacts into one PDF
    if merge and len(result.artifacts) > 1:
        try:
            result.merged_pdf = _merge_pdfs(result.artifacts)
            log.info("package_engine: merged %d artifacts (%d bytes)",
                     len(result.artifacts), len(result.merged_pdf))
        except Exception as e:
            log.error("package_engine: merge failed: %s", e)
            result.warnings.append(f"Merge failed: {e}")
    elif len(result.artifacts) == 1:
        result.merged_pdf = result.artifacts[0].pdf_bytes

    # Any fill errors = overall failure
    if result.errors:
        result.ok = False

    # Run post-hooks (Drive upload, email draft, activity logging)
    for hook in _post_hooks:
        try:
            hook(quote, result)
        except Exception as e:
            log.warning("Post-hook failed: %s", e)

    log.info("package_engine: %s", result.summary)
    return result


def _merge_pdfs(artifacts: list[Artifact]) -> bytes:
    """Merge multiple PDF artifacts into one combined PDF."""
    from pypdf import PdfWriter, PdfReader

    writer = PdfWriter()
    for artifact in artifacts:
        reader = PdfReader(io.BytesIO(artifact.pdf_bytes))
        for page in reader.pages:
            writer.add_page(page)

    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()
