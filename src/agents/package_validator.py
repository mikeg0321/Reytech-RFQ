"""PR-P — generate-then-pause package validator.

Mike's hard rule: "Do not auto ship or email quotes, generate end to
end, validate and check all forms for accuracy using vision."

The existing generate path (routes_pricecheck.py:3870) already calls
`pdf_visual_qa.inspect_pdf` on the 704 PDF — but the results are
returned in the JSON response as transient `qa_warnings` and then lost.
Reloading the PC detail page shows no record of whether the package
was validated.

PR-P closes that gap:
  1. `validate_pc_package(pc_id)` runs vision over EVERY generated PDF
     on the PC (704, quote, packet) and persists the report on the PC
     dict under `vision_validation`.
  2. The PC detail page renders the persisted report so the operator
     can see "9/9 forms passed, 0 errors, 2 warnings" before deciding
     whether to click Send.
  3. Operator can re-run validation any time via the panel button —
     no need to regenerate the whole package.

Standing constraint preserved: this module does NOT click Send. It
validates that the package LOOKS correct; the operator decides whether
to send it.

Returns the same persisted dict shape every time so the template can
render unconditionally:

    {
      "ok": bool,
      "validated_at": iso timestamp,
      "pc_id": str,
      "files": [
        {"name": "AMS_704B_…pdf", "passed": True, "pages_inspected": 2,
         "errors": [{"category": "blank_field", "description": "…",
                     "field_name": "…", "page": 1}],
         "warnings": [...]},
        ...
      ],
      "overall_passed": bool,
      "total_errors": int,
      "total_warnings": int,
      "summary_line": "9/9 forms passed, 0 errors, 2 warnings",
      "skipped_reason": str or "",   # populated when validator couldn't run
    }
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger("reytech.package_validator")


def _collect_package_pdfs(pc: Dict[str, Any]) -> List[str]:
    """Find every generated PDF associated with a PC.

    Mirrors the resolution logic in `routes_pricecheck.pricecheck_detail()`
    (lines ~1352-1395) but returns absolute paths instead of download URLs.

    Returns paths in priority order — main forms first, then sidecars.
    Skips paths that no longer exist on disk (e.g. lost to a redeploy
    on git-tracked DATA_DIR).
    """
    paths: List[str] = []
    seen: set = set()

    def _add(p: Optional[str]):
        if not p:
            return
        p = str(p)
        if p in seen:
            return
        if not os.path.exists(p):
            return
        seen.add(p)
        paths.append(p)

    # CCHCS packet (when this PC ran the packet generator)
    _add(pc.get("cchcs_packet_output_pdf"))
    # 704 form (primary form-fill output)
    for key in ("output_pdf", "original_pdf"):
        _add(pc.get(key))
    # Reytech-signed quote
    _add(pc.get("reytech_quote_pdf"))
    # Any sibling forms tracked under generic keys
    for key in ("dvbe_843_pdf", "calrecycle74_pdf", "std204_pdf",
                "ams708_pdf", "darfur_pdf"):
        _add(pc.get(key))

    return paths


def validate_pc_package(
    pcid: str,
    persist: bool = True,
    company_name: str = "Reytech Inc.",
) -> Dict[str, Any]:
    """Run vision validation over every PDF in this PC's package and
    persist the report.

    Always returns the same shape (see module docstring) — never raises.

    When `persist=False`, the validation runs but the PC dict isn't
    rewritten. Useful for ad-hoc previews from admin tools without
    bumping the PC's updated_at.

    When `persist=True` (default), the result lands on `pc.vision_validation`
    via `_save_single_pc` so the PC detail page can render it after a
    page reload.
    """
    try:
        from src.api.data_layer import _load_price_checks, _save_single_pc
    except Exception as e:
        return _skip_result(pcid, f"data_layer import failed: {e}")

    pc = _load_price_checks().get(pcid)
    if not isinstance(pc, dict):
        return _skip_result(pcid, f"pc not found: {pcid}")

    pdf_paths = _collect_package_pdfs(pc)
    if not pdf_paths:
        report = _skip_result(
            pcid,
            "no generated PDFs found — run Generate first then re-validate",
        )
        if persist:
            try:
                pc["vision_validation"] = report
                _save_single_pc(pcid, pc)
            except Exception as _se:
                log.warning("persist skipped (no PDFs): %s", _se)
        return report

    files: List[Dict[str, Any]] = []
    try:
        from src.forms.pdf_visual_qa import inspect_package
        results = inspect_package(pdf_paths, company_name=company_name)
    except Exception as e:
        log.error("inspect_package failed: %s", e, exc_info=True)
        return _skip_result(pcid, f"vision call failed: {e}")

    for fname, r in results.items():
        files.append({
            "name": fname,
            "passed": bool(getattr(r, "passed", True)),
            "pages_inspected": int(getattr(r, "pages_inspected", 0) or 0),
            "errors": [
                {
                    "category": getattr(i, "category", ""),
                    "description": getattr(i, "description", ""),
                    "field_name": getattr(i, "field_name", ""),
                    "page": int(getattr(i, "page", 0) or 0),
                }
                for i in getattr(r, "errors", [])
            ],
            "warnings": [
                {
                    "category": getattr(i, "category", ""),
                    "description": getattr(i, "description", ""),
                    "field_name": getattr(i, "field_name", ""),
                    "page": int(getattr(i, "page", 0) or 0),
                }
                for i in getattr(r, "warnings", [])
            ],
        })

    total_errors = sum(len(f["errors"]) for f in files)
    total_warnings = sum(len(f["warnings"]) for f in files)
    passed_count = sum(1 for f in files if f["passed"])
    overall_passed = all(f["passed"] for f in files) if files else False

    report = {
        "ok": True,
        "validated_at": datetime.now().isoformat(),
        "pc_id": pcid,
        "files": files,
        "overall_passed": overall_passed,
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "summary_line": (
            f"{passed_count}/{len(files)} forms passed, "
            f"{total_errors} errors, {total_warnings} warnings"
        ),
        "skipped_reason": "",
    }

    if persist:
        try:
            pc["vision_validation"] = report
            _save_single_pc(pcid, pc)
        except Exception as _se:
            log.warning("persist vision_validation failed: %s", _se)

    log.info("validate_pc_package %s: %s", pcid, report["summary_line"])
    return report


def _skip_result(pcid: str, reason: str) -> Dict[str, Any]:
    """Stable empty report when the validator can't run (no PDFs / no
    Vision key / pc not found). Same shape as a successful run so the
    template doesn't need to handle two structures."""
    return {
        "ok": False,
        "validated_at": datetime.now().isoformat(),
        "pc_id": pcid,
        "files": [],
        "overall_passed": False,
        "total_errors": 0,
        "total_warnings": 0,
        "summary_line": f"Validation skipped: {reason}",
        "skipped_reason": reason,
    }
