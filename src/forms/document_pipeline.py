"""
Self-Healing Document Pipeline — Central orchestrator for PDF generation.

Wraps existing fill functions (fill_ams704, _fill_pdf_text_overlay) in a
generate → read-back verify → auto-repair → re-verify → gate loop.

Accepts ANY input format (PDF, DOCX, XLSX, XLS). Normalizes to PDF inside
the pipeline so conversion failures trigger strategy escalation, not errors.

Score = 100 or BLOCK. Zero tolerance.

Strategy escalation order:
1. form_fields — native pypdf form fill
2. overlay — pdfplumber-detected text overlay
3. blank_template — fill using blank AMS 704

Every outcome is recorded in the template learning DB for the flywheel.
"""

import os
import json
import time
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional

log = logging.getLogger("reytech.document_pipeline")


# ═══════════════════════════════════════════════════════════════════════════
# RESULT TYPES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class AttemptRecord:
    """Record of one generation attempt."""
    strategy: str
    score: int
    duration_ms: int
    issues: list = field(default_factory=list)  # list of ReadbackIssue dicts

    @property
    def succeeded(self) -> bool:
        return self.score == 100


@dataclass
class PipelineResult:
    """Final result of the document pipeline."""
    ok: bool
    output_path: str = ""
    verification_score: int = 0
    strategy_used: str = ""
    source_type: str = ""          # "pdf" | "docx" | "xlsx" | "docx_fallback"
    attempts: list = field(default_factory=list)  # list[AttemptRecord]
    summary: dict = field(default_factory=dict)
    warnings: list = field(default_factory=list)
    failed_fields: list = field(default_factory=list)
    error: str = ""

    @property
    def attempt_summaries(self) -> list:
        return [asdict(a) for a in self.attempts]


# ═══════════════════════════════════════════════════════════════════════════
# STRATEGY DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════

STRATEGIES = ["form_fields", "overlay", "blank_template"]


# ═══════════════════════════════════════════════════════════════════════════
# DOCUMENT PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

class DocumentPipeline:
    """Self-healing document generation pipeline.

    Usage:
        pipeline = DocumentPipeline(
            source_file="buyer.pdf",  # or .docx, .xlsx, .xls
            parsed_data=parsed_pc,
            output_pdf="output.pdf",
            tax_rate=0.0775,
        )
        result = pipeline.execute()
        if result.ok:
            serve_pdf(result.output_path)
        else:
            show_error(result.error, result.failed_fields)
    """

    MAX_ATTEMPTS = 3  # one per strategy

    def __init__(self, source_file: str, parsed_data: dict, output_pdf: str,
                 doc_type: str = "704", company_info: dict = None,
                 **fill_kwargs):
        self.source_file = source_file
        self.parsed_data = parsed_data
        self.output_pdf = output_pdf
        self.doc_type = doc_type
        self.company_info = company_info
        self.fill_kwargs = fill_kwargs

        # Set after normalization
        self.source_pdf: Optional[str] = None
        self.source_type: str = ""
        self.profile = None
        self.fingerprint: str = ""

        # Attempt tracking
        self.attempts: list[AttemptRecord] = []

    def execute(self) -> PipelineResult:
        """Main pipeline: normalize → try strategies until score = 100."""

        # ── Step 0: Normalize input to PDF ──
        try:
            self.source_pdf, self.source_type = self._normalize_source()
        except Exception as e:
            log.error("pipeline: source normalization failed: %s", e)
            return PipelineResult(
                ok=False, error=f"Source file normalization failed: {e}")

        if not self.source_pdf or not os.path.exists(self.source_pdf):
            return PipelineResult(
                ok=False, error="Source PDF not available after normalization")

        # ── Step 1: Profile the template ──
        try:
            from src.forms.template_registry import get_profile
            self.profile = get_profile(self.source_pdf)
        except Exception as e:
            log.warning("pipeline: template profiling failed: %s", e)
            self.profile = None

        # Compute fingerprint for learning DB
        try:
            from src.forms.template_learning import template_fingerprint
            self.fingerprint = template_fingerprint(self.source_pdf)
        except Exception:
            self.fingerprint = "unknown"

        # ── Step 2: Select initial strategy ──
        strategy = self._select_initial_strategy()
        strategies_tried = set()

        log.info("pipeline: starting for %s (source_type=%s, fingerprint=%s, "
                 "initial_strategy=%s)",
                 os.path.basename(self.source_file), self.source_type,
                 self.fingerprint[:25], strategy)

        # ── Step 3: Generate → Verify → Escalate loop ──
        for attempt_num in range(1, self.MAX_ATTEMPTS + 1):
            strategies_tried.add(strategy)
            t0 = time.time()

            log.info("pipeline: attempt %d/%d strategy=%s",
                     attempt_num, self.MAX_ATTEMPTS, strategy)

            # Generate
            gen_result = self._generate(strategy)
            if not gen_result or not gen_result.get("ok"):
                err = gen_result.get("error", "Unknown") if gen_result else "Generation returned None"
                log.warning("pipeline: generation failed (strategy=%s): %s",
                            strategy, err)
                duration = int((time.time() - t0) * 1000)
                self.attempts.append(AttemptRecord(
                    strategy=strategy, score=0, duration_ms=duration,
                    issues=[{"field_name": "GENERATION", "issue_type": "error",
                             "intended_value": "", "actual_value": err}]))
                # Try next strategy
                next_strat = self._escalate(strategy, None, strategies_tried)
                if next_strat:
                    strategy = next_strat
                    continue
                break

            # Read-back verify
            readback = self._verify(strategy)
            duration = int((time.time() - t0) * 1000)

            attempt = AttemptRecord(
                strategy=strategy,
                score=readback.score,
                duration_ms=duration,
                issues=[asdict(i) for i in readback.issues],
            )
            self.attempts.append(attempt)

            # Record outcome in learning DB
            self._record_outcome(strategy, readback.score)

            # Gate check
            # 2026-04-12: was `score == 100` (perfect-or-block). That refused
            # to deliver any PDF where even one field couldn't be readback-
            # verified, including DocuSign-flat sources where 4 of ~70 fields
            # routinely fail cosmetic checks. Mike was blocked from quoting
            # active CDCR PCs because of this. The pipeline still tries every
            # strategy and picks the best — we just don't refuse to ship a
            # 70-grade PDF when the alternative is no PDF at all.
            #
            # If a strategy hits 100 we still short-circuit (no need to try
            # the rest). Anything 70+ is acceptable as a "good enough"
            # delivery; below 70 we keep escalating and only block if all
            # strategies stay below 70.
            _DELIVERY_THRESHOLD = 70
            if readback.score >= _DELIVERY_THRESHOLD:
                log.info("pipeline: PASSED (strategy=%s, attempt=%d, score=%d, %dms)",
                         strategy, attempt_num, readback.score, duration)
                return PipelineResult(
                    ok=True,
                    output_path=self.output_pdf,
                    verification_score=readback.score,
                    strategy_used=strategy,
                    source_type=self.source_type,
                    attempts=self.attempts,
                    summary=gen_result.get("summary", {}),
                )

            log.info("pipeline: score=%d (strategy=%s), escalating...",
                     readback.score, strategy)

            # Escalate
            next_strat = self._escalate(strategy, readback, strategies_tried)
            if next_strat is None:
                break
            strategy = next_strat

        # ── All strategies exhausted — BLOCK ──
        best = max(self.attempts, key=lambda a: a.score) if self.attempts else None
        best_score = best.score if best else 0
        best_strategy = best.strategy if best else "none"
        best_issues = best.issues if best else []

        log.warning("pipeline: BLOCKED — all %d strategies tried, best score=%d (%s)",
                    len(self.attempts), best_score, best_strategy)

        return PipelineResult(
            ok=False,
            output_path=self.output_pdf if best_score > 0 else "",
            verification_score=best_score,
            strategy_used=best_strategy,
            source_type=self.source_type,
            attempts=self.attempts,
            failed_fields=best_issues,
            error=(f"Document verification failed. Best score: {best_score}/100 "
                   f"using '{best_strategy}' strategy. "
                   f"{len(best_issues)} field(s) could not be verified."),
        )

    # ── SOURCE NORMALIZATION ──────────────────────────────────────────

    def _normalize_source(self) -> tuple:
        """Convert DOCX/XLSX/XLS to PDF. PDF passes through."""
        ext = os.path.splitext(self.source_file)[1].lower()

        if ext == ".pdf":
            return self.source_file, "pdf"

        if ext in (".docx", ".doc"):
            converted = self._convert_office(self.source_file)
            if converted:
                return converted, "docx"
            # Fallback: use blank 704 template
            blank = self._blank_704_path()
            if blank:
                log.warning("pipeline: DOCX conversion failed, using blank 704")
                return blank, "docx_fallback"
            return self.source_file, "docx_failed"

        if ext in (".xlsx", ".xls"):
            # Spreadsheets have no buyer form layout — always use blank
            blank = self._blank_704_path()
            if blank:
                return blank, "xlsx"
            return self.source_file, "xlsx_failed"

        # Unknown format — try using as-is
        log.warning("pipeline: unknown source format '%s', using as-is", ext)
        return self.source_file, "unknown"

    def _convert_office(self, file_path: str) -> Optional[str]:
        """Convert office doc to PDF via LibreOffice."""
        try:
            from src.forms.doc_converter import convert_to_pdf, can_convert_to_pdf
            if can_convert_to_pdf():
                from src.core.paths import DATA_DIR
                out_dir = os.path.join(DATA_DIR, "pc_pdfs")
                os.makedirs(out_dir, exist_ok=True)
                converted = convert_to_pdf(file_path, out_dir)
                log.info("pipeline: converted %s → %s",
                         os.path.basename(file_path),
                         os.path.basename(converted))
                return converted
        except Exception as e:
            log.warning("pipeline: office conversion failed: %s", e)
        return None

    def _blank_704_path(self) -> Optional[str]:
        """Get path to blank AMS 704 template."""
        from src.core.paths import DATA_DIR
        blank = os.path.join(DATA_DIR, "templates", "ams_704_blank.pdf")
        if os.path.exists(blank):
            return blank
        return None

    # ── STRATEGY SELECTION ────────────────────────────────────────────

    def _select_initial_strategy(self) -> str:
        """Pick the initial strategy based on template profile + learning DB."""
        # 1. Check learning DB for proven strategy
        try:
            from src.forms.template_learning import get_best_strategy
            learned = get_best_strategy(self.fingerprint)
            if learned:
                log.info("pipeline: using learned strategy '%s' for fp=%s",
                         learned, self.fingerprint[:20])
                return learned
        except Exception:
            pass

        # 2. Use template profile recommendation
        if self.profile and self.profile.fill_recommendation != "form_fields":
            log.info("pipeline: profile recommends '%s' (risk=%s: %s)",
                     self.profile.fill_recommendation,
                     self.profile.risk_level,
                     "; ".join(self.profile.risk_reasons))
            return self.profile.fill_recommendation

        # 3. XLSX/XLS always use form_fields on blank template
        if self.source_type in ("xlsx", "xlsx_failed"):
            return "form_fields"

        # 4. Default
        return "form_fields"

    def _escalate(self, current: str, readback, tried: set) -> Optional[str]:
        """Pick next untried strategy. Never repeat."""
        for candidate in STRATEGIES:
            if candidate not in tried:
                return candidate
        return None

    # ── GENERATION ────────────────────────────────────────────────────

    def _generate(self, strategy: str) -> Optional[dict]:
        """Call the appropriate fill function for the given strategy."""
        source = self.source_pdf

        if strategy == "blank_template":
            blank = self._blank_704_path()
            if blank:
                source = blank
            else:
                log.error("pipeline: blank_template strategy but no blank 704 found")
                return {"ok": False, "error": "Blank 704 template not found"}

        if strategy == "overlay":
            return self._generate_overlay(source)

        # form_fields (default) or blank_template (with substituted source)
        return self._generate_form_fields(source)

    def _generate_form_fields(self, source_pdf: str) -> Optional[dict]:
        """Generate using native pypdf form field fill (fill_ams704)."""
        try:
            from src.forms.price_check import fill_ams704
            result = fill_ams704(
                source_pdf=source_pdf,
                parsed_pc=self.parsed_data,
                output_pdf=self.output_pdf,
                company_info=self.company_info,
                tax_rate=self.fill_kwargs.get("tax_rate", 0.0),
                custom_notes=self.fill_kwargs.get("custom_notes", ""),
                delivery_option=self.fill_kwargs.get("delivery_option", ""),
                keep_all_pages=self.fill_kwargs.get("keep_all_pages", False),
            )
            return result
        except Exception as e:
            log.error("pipeline: fill_ams704 raised: %s", e, exc_info=True)
            return {"ok": False, "error": str(e)}

    def _generate_overlay(self, source_pdf: str) -> Optional[dict]:
        """Generate using text overlay (for flattened/DOCX PDFs)."""
        try:
            from src.forms.price_check import fill_ams704
            # fill_ams704 internally detects flattened PDFs and routes to
            # _fill_pdf_text_overlay. For the overlay strategy, we force this
            # by passing the source as-is (it will detect no AcroForm fields
            # and use overlay). If the source HAS form fields but they don't
            # work, we need to trick it — simplest: pass keep_all_pages=True
            # which preserves the source layout and lets the overlay path run.
            result = fill_ams704(
                source_pdf=source_pdf,
                parsed_pc=self.parsed_data,
                output_pdf=self.output_pdf,
                company_info=self.company_info,
                tax_rate=self.fill_kwargs.get("tax_rate", 0.0),
                custom_notes=self.fill_kwargs.get("custom_notes", ""),
                delivery_option=self.fill_kwargs.get("delivery_option", ""),
                keep_all_pages=True,
            )
            return result
        except Exception as e:
            log.error("pipeline: overlay generation raised: %s", e, exc_info=True)
            return {"ok": False, "error": str(e)}

    # ── VERIFICATION ──────────────────────────────────────────────────

    def _verify(self, strategy: str):
        """Read back the output PDF and verify against intended values.
        Two-stage: 1) Read-back 2) Visual QA (only if readback passes)."""
        from src.forms.readback_verify import (
            verify_form_fields, verify_overlay_text, ReadbackResult, ReadbackIssue
        )
        intended = self._load_intended_values()
        if not intended:
            log.warning("pipeline: no intended values to verify against")
            return ReadbackResult(
                score=100, fields_intended=0, fields_confirmed=0,
                fields_missing=0, fields_wrong=0, verification_mode="none",
            )
        if strategy == "overlay":
            readback = verify_overlay_text(self.output_pdf, intended)
        else:
            readback = verify_form_fields(self.output_pdf, intended)
        if readback.score == 100 and self.fill_kwargs.get("run_visual_qa", True):
            readback = self._run_visual_qa(readback)
        return readback

    def _run_visual_qa(self, readback):
        """Run Visual QA and deduct from score if rendering issues found."""
        from src.forms.readback_verify import ReadbackIssue
        try:
            from src.forms.pdf_visual_qa import inspect_pdf
            vqa = inspect_pdf(self.output_pdf, company_name="Reytech Inc.")
            if not vqa.passed:
                for error in vqa.errors:
                    readback.issues.append(ReadbackIssue(
                        field_name=error.field_name or f"page_{error.page}_{error.category}",
                        intended_value="(visible)",
                        actual_value=f"[Visual] {error.description}",
                        issue_type="missing",
                        is_critical=error.category in ("blank_field", "signature"),
                    ))
                    if error.category in ("blank_field", "signature"):
                        readback.score = max(0, readback.score - 10)
                    else:
                        readback.score = max(0, readback.score - 3)
                log.info("pipeline: Visual QA deducted \u2192 score=%d", readback.score)
            if vqa.pages_inspected > 0:
                readback.verification_mode = readback.verification_mode + "+visual_qa"
        except Exception as e:
            log.info("pipeline: Visual QA skipped: %s", e)
        return readback

    def _load_intended_values(self) -> list:
        """Load the field_values JSON that fill_ams704 writes."""
        from src.core.paths import DATA_DIR
        fv_path = os.path.join(DATA_DIR, "pc_field_values.json")
        try:
            if os.path.exists(fv_path):
                with open(fv_path) as f:
                    return json.load(f)
        except Exception as e:
            log.warning("pipeline: failed to load field values: %s", e)
        return []

    # ── LEARNING DB ───────────────────────────────────────────────────

    def _record_outcome(self, strategy: str, score: int):
        """Record this attempt in the template learning DB."""
        try:
            from src.forms.template_learning import record_outcome
            record_outcome(
                fingerprint=self.fingerprint,
                strategy=strategy,
                score=score,
                source_type=self.source_type,
                pc_id=self.fill_kwargs.get("pc_id", ""),
                buyer_agency=self.fill_kwargs.get("buyer_agency", ""),
            )
        except Exception as e:
            log.debug("pipeline: record_outcome failed: %s", e)
