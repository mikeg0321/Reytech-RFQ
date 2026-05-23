"""Visual-fidelity QA — catch what the math/identity/coverage Inspector can't.

Per ``CLAUDE.md §0`` the Inspector role owns "BOTH gates: the Chrome
walkthrough AND the math reconciliation." ``inspector.py`` is the math
half. This module is the visual half — the gate that would have caught
the 2026-05-23 Demidenko PC bug (`30 Carnoustie Way Trabuco Ca`
clipped, `s a l e s @ r e y t e c h i n c .` comb-spaced) BEFORE
Mike's eyes caught it post-ship.

Why a separate module — not a check inside Inspector
----------------------------------------------------
The math/identity/coverage Inspector verifies **values are correct**
(the sol# is the right sol#; subtotal reconciles). This module
verifies **the rendered page looks right** (the value isn't clipped;
the characters aren't fragmented; the signature isn't missing).
Different failure mode, different probes — keep them composable.

What it catches today (Tier 1 — deterministic, no LLM)
------------------------------------------------------
- ``cid_glyph_artifacts`` — runs of ``(cid:0)``-prefixed glyphs in the
  extracted text. fitz emits these when a baked appearance stream uses
  a font subset whose ToUnicode map is missing; this is the signature
  of the PR-10 stale-/AP bug.
- ``comb_class_spacing`` — extracted-text runs of single-char tokens
  separated by single spaces (e.g. ``s a l e s @ r e y t e c h``).
  This is what the eye sees as wide letter-spacing; it indicates the
  text was rendered with one Tj per character instead of one Tj per
  word, the classic "comb field" or "stale appearance fallback" shape.
- ``empty_required_field`` — pre-flatten: an AcroForm widget marked
  required (``/Ff`` flag bit 2 set) with no ``/V``. Post-flatten this
  is undetectable from bytes alone — that's task #20 v2 territory.

Tier 2 (extension point — not built in this PR)
-----------------------------------------------
- Vision classifier: rasterize each page and ask a multimodal model
  to spot overlaps, missing-signature, off-by-one alignment, etc.
- ``inspect_pdf_visual(.., vision_classifier=callable)`` accepts a
  callable that takes a list of page-PNG bytes and returns extra
  ``VisualIssue`` records. The default is None (Tier 1 only).

Severity model
--------------
Matches ``inspector.py``: ``ok | warning | blocking``. Default for
every detector here is ``warning`` in this first ship — advisory
only, so live sends don't suddenly start failing on a marginal
finding. The send-prep route surfaces visual findings alongside the
math Inspector; flip-to-blocking happens after Mike's first week of
operator feedback (recorded in the memory referenced below).

Linked
------
- ``project_flatten_regen_appearance_before_bake_2026_05_23`` —
  the memory documenting the bug class this gate exists to catch.
- ``src/spine/inspector.py`` — the math/identity sibling.
- ``src/spine/flatten.py`` — Tier-1 detectors run on flat OR
  editable bytes; the gate is upstream of flatten in send-prep.
"""
from __future__ import annotations

import io
import logging
import re
from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field

log = logging.getLogger("reytech.spine.visual_qa")


VisualIssueKind = Literal[
    "cid_glyph_artifacts",
    "comb_class_spacing",
    "empty_required_field",
    "vision_finding",  # reserved for Tier-2
]
Severity = Literal["ok", "warning", "blocking"]


class VisualIssue(BaseModel):
    """One visual-fidelity finding from the QA pass."""

    model_config = ConfigDict(extra="forbid")

    kind: VisualIssueKind
    severity: Severity = "warning"
    page: int = Field(..., ge=1)
    location: str = ""  # field name / region label / page coords
    message: str
    evidence: str = ""  # the offending text snippet, if any


class VisualQAReport(BaseModel):
    """The full visual-QA pass result. ``ok`` is True iff zero
    blocking issues; warning issues do not flip ``ok``."""

    model_config = ConfigDict(extra="forbid")

    ok: bool
    pdf_pages: int = Field(..., ge=0)
    issues: list[VisualIssue] = Field(default_factory=list)
    detectors_run: list[str] = Field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "blocking")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")


# ──────────────────────────────────────────────────────────────────────
# Tier-1 detectors — pure functions on bytes. No external services.
# ──────────────────────────────────────────────────────────────────────


# Pattern: 4+ single-char tokens separated by single spaces.
# Matches `s a l e s @ r e y t e c h` (the PR-10 signature) but
# does NOT match normal text like `a b` (too short — false-positive
# prone, every "I am" hits a 2-char run) or `Net 45` (multi-char tokens).
# The 4-char minimum was chosen to balance recall vs precision on the
# Demidenko PC fixture: short words don't fire, comb-class runs do.
_COMB_RUN = re.compile(r"(?:(?<=^)|(?<=\s))(?:\S \S \S \S(?:\s+\S)*)")

# Pattern: a (cid:N) glyph reference. fitz emits these when the font
# subset's ToUnicode map can't resolve the glyph — the post-bake-with-
# stale-/AP signature.
_CID_GLYPH = re.compile(r"\(cid:\d+\)")


def _detect_cid_glyphs(pdf_bytes: bytes) -> list[VisualIssue]:
    """Per-page scan for (cid:N) runs in extracted text."""
    issues: list[VisualIssue] = []
    try:
        import pdfplumber
    except ImportError:  # pragma: no cover - dep is in the stack
        log.debug("pdfplumber unavailable — skipping cid-glyph detector")
        return issues
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for pg_idx, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                # Count glyph hits; surface ONE issue per page (not one
                # per hit) with the count as evidence to keep the report
                # operator-readable.
                hits = _CID_GLYPH.findall(text)
                if len(hits) >= 3:  # ≥3 to avoid one-off false positives
                    issues.append(VisualIssue(
                        kind="cid_glyph_artifacts",
                        severity="warning",
                        page=pg_idx,
                        location=f"page {pg_idx}",
                        message=(
                            f"{len(hits)} (cid:N) glyphs in extracted "
                            f"text — likely stale appearance baked into "
                            f"flatten output (PR-10 bug class)."
                        ),
                        evidence=text[:240],
                    ))
    except Exception as e:  # pragma: no cover - defensive
        log.debug("cid-glyph detector failed: %s", e)
    return issues


def _detect_comb_spacing(pdf_bytes: bytes) -> list[VisualIssue]:
    """Per-page scan for `X X X X` runs of single-char tokens."""
    issues: list[VisualIssue] = []
    try:
        import pdfplumber
    except ImportError:  # pragma: no cover
        return issues
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for pg_idx, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                # Scan each line independently — a stray run on one
                # line shouldn't poison the whole document.
                for ln in text.splitlines():
                    m = _COMB_RUN.search(ln)
                    if m:
                        run = m.group(0)
                        # Require the run to be at least 8 chars wide
                        # (4 chars + 3 spaces = 7; we want a hard line
                        # at "this is unambiguous comb-class").
                        if len(run) >= 8:
                            issues.append(VisualIssue(
                                kind="comb_class_spacing",
                                severity="warning",
                                page=pg_idx,
                                location=f"page {pg_idx}",
                                message=(
                                    "Single-character spaced run "
                                    "detected — page text rendered with "
                                    "wide letter-spacing (likely stale "
                                    "/AP or comb-field defect)."
                                ),
                                evidence=run[:120],
                            ))
                            break  # one issue per page is enough
    except Exception as e:  # pragma: no cover
        log.debug("comb-spacing detector failed: %s", e)
    return issues


def _detect_empty_required_fields(pdf_bytes: bytes) -> list[VisualIssue]:
    """Pre-flatten only: list any AcroForm widget tagged required
    (``/Ff`` bit 2 set, value ``2``) with no ``/V``. Post-flatten the
    fields are gone; this detector returns ``[]`` then.
    """
    issues: list[VisualIssue] = []
    try:
        from pypdf import PdfReader
    except ImportError:  # pragma: no cover - dep is in the stack
        return issues
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        fields = reader.get_fields() or {}
        for name, fld in fields.items():
            ff = fld.get("/Ff") or 0
            try:
                ff_int = int(ff)
            except Exception:
                ff_int = 0
            # /Ff bit 2 (value 2) = Required.
            if not (ff_int & 2):
                continue
            v = fld.get("/V")
            if v is None or (isinstance(v, str) and v.strip() == ""):
                issues.append(VisualIssue(
                    kind="empty_required_field",
                    severity="warning",
                    page=1,  # AcroForm dict doesn't directly carry page index
                    location=name or "<unnamed>",
                    message=(
                        f"Required AcroForm field {name!r} has no value "
                        f"— operator must fill before send."
                    ),
                    evidence="",
                ))
    except Exception as e:  # pragma: no cover
        log.debug("empty-required-field detector failed: %s", e)
    return issues


# ──────────────────────────────────────────────────────────────────────
# Public entry point.
# ──────────────────────────────────────────────────────────────────────


def inspect_pdf_visual(
    pdf_bytes: bytes,
    *,
    vision_classifier: Callable[[list[bytes]], list[VisualIssue]] | None = None,
) -> VisualQAReport:
    """Run the visual-QA pass on a PDF and return a structured report.

    ``pdf_bytes`` empty or non-PDF → ``ok=True`` with zero issues (the
    upstream Inspector / send-prep handles "nothing to send" separately).

    ``vision_classifier`` (Tier-2 extension): optional callable that
    receives a list of per-page PNG bytes and returns extra issues to
    merge. Default ``None`` keeps the Tier-1 deterministic surface.

    Severity policy in this first ship: every Tier-1 finding is
    ``warning``. The send-prep route surfaces these alongside the
    math Inspector but does NOT block the send on a warning alone.
    Flip-to-blocking is a separate PR after operator feedback.
    """
    detectors_run = ["cid_glyph_artifacts", "comb_class_spacing",
                     "empty_required_field"]

    if not pdf_bytes or pdf_bytes[:5] != b"%PDF-":
        return VisualQAReport(ok=True, pdf_pages=0, issues=[],
                              detectors_run=detectors_run)

    # Page count for the report header.
    pdf_pages = 0
    try:
        from pypdf import PdfReader
        pdf_pages = len(PdfReader(io.BytesIO(pdf_bytes)).pages)
    except Exception:  # pragma: no cover
        pass

    all_issues: list[VisualIssue] = []
    all_issues.extend(_detect_cid_glyphs(pdf_bytes))
    all_issues.extend(_detect_comb_spacing(pdf_bytes))
    all_issues.extend(_detect_empty_required_fields(pdf_bytes))

    if vision_classifier is not None:
        try:
            # Rasterize per-page PNG bytes for the classifier.
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            page_pngs = [pg.get_pixmap(dpi=220).tobytes("png") for pg in doc]
            doc.close()
            extra = vision_classifier(page_pngs) or []
            for issue in extra:
                if not isinstance(issue, VisualIssue):
                    log.warning("vision_classifier returned non-VisualIssue %r",
                                type(issue))
                    continue
                all_issues.append(issue)
            detectors_run.append("vision_classifier")
        except Exception as e:  # pragma: no cover - defensive
            log.warning("vision_classifier failed (%s) — Tier-1 only", e)

    blocking_count = sum(1 for i in all_issues if i.severity == "blocking")
    return VisualQAReport(
        ok=(blocking_count == 0),
        pdf_pages=pdf_pages,
        issues=all_issues,
        detectors_run=detectors_run,
    )


__all__ = [
    "VisualIssue",
    "VisualQAReport",
    "inspect_pdf_visual",
]
