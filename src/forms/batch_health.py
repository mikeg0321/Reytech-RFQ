"""
Batch Health Check — Validate ALL existing PC outputs.

Runs read-back verification on every PC that has a generated output PDF.
Reports which PCs need regeneration and why. Optionally auto-regenerates
failing PCs through the DocumentPipeline.
"""

import os
import json
import time
import logging

log = logging.getLogger("reytech.batch_health")


def run_health_check(pc_ids: list = None, auto_regenerate: bool = False,
                     max_items: int = 100) -> dict:
    """Validate existing PC outputs via read-back verification.

    For each PC with an output_pdf:
    1. Check file exists and is valid PDF
    2. Run read-back verification
    3. Optionally auto-regenerate via DocumentPipeline

    Args:
        pc_ids: Specific PC IDs to check. None = all PCs with output PDFs.
        auto_regenerate: If True, re-generate PCs that fail verification.
        max_items: Maximum PCs to process (avoid timeout).

    Returns: {
        "total_checked": int,
        "passed": int,
        "failed": int,
        "missing": int,
        "regenerated": int,
        "duration_ms": int,
        "details": {pcid: {"score": int, "issues": [...], "regenerated": bool}}
    }
    """
    from src.core.paths import DATA_DIR

    t0 = time.time()
    pcs_path = os.path.join(DATA_DIR, "pcs.json")

    try:
        with open(pcs_path) as f:
            all_pcs = json.load(f)
    except Exception as e:
        log.warning("batch_health: failed to load pcs.json: %s", e)
        return {"total_checked": 0, "passed": 0, "failed": 0,
                "missing": 0, "regenerated": 0, "error": str(e)}

    # Filter to PCs with output PDFs
    candidates = {}
    for pcid, pc in all_pcs.items():
        if pc_ids and pcid not in pc_ids:
            continue
        output = pc.get("output_pdf", "")
        if output:
            candidates[pcid] = pc

    if not candidates:
        return {"total_checked": 0, "passed": 0, "failed": 0,
                "missing": 0, "regenerated": 0, "details": {}}

    # Limit to max_items
    check_ids = list(candidates.keys())[:max_items]

    passed = 0
    failed = 0
    missing = 0
    regenerated = 0
    details = {}

    for pcid in check_ids:
        pc = candidates[pcid]
        output_path = pc.get("output_pdf", "")
        detail = {"score": 0, "issues": [], "regenerated": False}

        # Check 1: File exists
        if not output_path or not os.path.exists(output_path):
            missing += 1
            detail["score"] = 0
            detail["issues"] = [{"field_name": "FILE",
                                 "issue_type": "missing",
                                 "intended_value": output_path,
                                 "actual_value": ""}]
            details[pcid] = detail
            continue

        # Check 2: File is valid PDF (not empty, has PDF header)
        try:
            with open(output_path, "rb") as f:
                header = f.read(5)
            if header != b"%PDF-":
                detail["score"] = 0
                detail["issues"] = [{"field_name": "FILE_FORMAT",
                                     "issue_type": "wrong_value",
                                     "intended_value": "%PDF-",
                                     "actual_value": header.decode("ascii", errors="replace")}]
                failed += 1
                details[pcid] = detail
                continue
        except Exception as e:
            detail["score"] = 0
            detail["issues"] = [{"field_name": "FILE_READ",
                                 "issue_type": "error",
                                 "intended_value": "",
                                 "actual_value": str(e)}]
            failed += 1
            details[pcid] = detail
            continue

        # Check 3: Read-back verification
        try:
            from src.forms.readback_verify import verify_form_fields
            # Try to load intended field values
            fv_path = os.path.join(DATA_DIR, "pc_field_values.json")
            intended = []
            if os.path.exists(fv_path):
                try:
                    with open(fv_path) as f:
                        intended = json.load(f)
                except Exception as _e:
                    log.debug('suppressed in run_health_check: %s', _e)

            if intended:
                result = verify_form_fields(output_path, intended)
                detail["score"] = result.score
                detail["issues"] = [
                    {"field_name": i.field_name,
                     "issue_type": i.issue_type,
                     "intended_value": i.intended_value,
                     "actual_value": i.actual_value}
                    for i in result.issues
                ]
            else:
                # No intended values — do basic structural check
                from pypdf import PdfReader
                reader = PdfReader(output_path)
                fields = reader.get_fields() or {}
                # Check that COMPANY NAME is filled
                company = ""
                for fname, fobj in fields.items():
                    if "COMPANY" in fname.upper():
                        if isinstance(fobj, dict):
                            company = str(fobj.get("/V", "")).strip()
                        else:
                            company = str(fobj).strip()
                        break

                if company:
                    detail["score"] = 100  # Basic check passed
                else:
                    detail["score"] = 50
                    detail["issues"] = [{"field_name": "COMPANY NAME",
                                         "issue_type": "missing",
                                         "intended_value": "Reytech Inc.",
                                         "actual_value": ""}]
        except Exception as e:
            detail["score"] = 0
            detail["issues"] = [{"field_name": "VERIFICATION",
                                 "issue_type": "error",
                                 "intended_value": "",
                                 "actual_value": str(e)}]

        if detail["score"] == 100:
            passed += 1
        else:
            failed += 1

        # Auto-regenerate if requested
        if auto_regenerate and detail["score"] < 100:
            try:
                regen_result = _regenerate_pc(pcid, pc)
                if regen_result:
                    detail["regenerated"] = True
                    detail["new_score"] = regen_result.get("score", 0)
                    regenerated += 1
            except Exception as e:
                log.warning("batch_health: regen failed for %s: %s", pcid, e)

        details[pcid] = detail

    duration = int((time.time() - t0) * 1000)
    report = {
        "total_checked": len(check_ids),
        "passed": passed,
        "failed": failed,
        "missing": missing,
        "regenerated": regenerated,
        "duration_ms": duration,
        "details": details,
    }
    log.info("batch_health: checked %d PCs — %d passed, %d failed, %d missing "
             "(%dms)", len(check_ids), passed, failed, missing, duration)
    return report


def _regenerate_pc(pcid: str, pc: dict) -> dict:
    """Re-generate a single PC through the DocumentPipeline."""
    from src.forms.document_pipeline import DocumentPipeline

    source = pc.get("source_pdf", "")
    output = pc.get("output_pdf", "")
    parsed = pc.get("parsed", {"line_items": pc.get("items", [])})

    if not source or not os.path.exists(source):
        return {"ok": False, "error": "Source PDF not found"}

    pipeline = DocumentPipeline(
        source_file=source,
        parsed_data=parsed,
        output_pdf=output,
        tax_rate=pc.get("tax_rate", 0.0) if pc.get("tax_enabled") else 0.0,
        custom_notes=pc.get("custom_notes", ""),
        delivery_option=pc.get("delivery_option", ""),
    )
    result = pipeline.execute()
    if result.ok:
        pc["output_pdf"] = result.output_path
        pc["verification_score"] = result.verification_score
        pc["generation_strategy"] = result.strategy_used
        pc["generation_attempts"] = len(result.attempts)
        try:
            from src.core.paths import DATA_DIR
            pcs_path = os.path.join(DATA_DIR, "pcs.json")
            with open(pcs_path) as f:
                all_pcs = json.load(f)
            all_pcs[pcid] = pc
            with open(pcs_path, "w") as f:
                json.dump(all_pcs, f, indent=2, default=str)
            log.info("batch_health: regen %s saved (score=%d)", pcid, result.verification_score)
        except Exception as e:
            log.warning("batch_health: regen save failed for %s: %s", pcid, e)
    return {"ok": result.ok, "score": result.verification_score}


def get_health_summary() -> dict:
    """Quick summary by verification_score bucket without full re-verification.

    Reads stored verification_score from PC records (set during generation).

    Returns: {
        "excellent": int,   # score = 100
        "unverified": int,  # no score stored (pre-pipeline PCs)
        "total": int,
    }
    """
    from src.core.paths import DATA_DIR

    pcs_path = os.path.join(DATA_DIR, "pcs.json")
    try:
        with open(pcs_path) as f:
            all_pcs = json.load(f)
    except Exception:
        return {"excellent": 0, "unverified": 0, "total": 0}

    excellent = 0
    unverified = 0
    total = 0

    for pcid, pc in all_pcs.items():
        if not pc.get("output_pdf"):
            continue
        total += 1
        score = pc.get("verification_score")
        if score == 100:
            excellent += 1
        elif score is None:
            unverified += 1

    return {"excellent": excellent, "unverified": unverified, "total": total}
