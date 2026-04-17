"""PC Generate V2 — unified quoting pipeline via Quote model + fill engine.

Built per user rule: two quoting modes (PC for CCHCS, RFQ for other agencies).
This module handles the PC half end-to-end using the phase-0 through phase-3
rewrite (Quote model, fill engine, profile registry) instead of the 2000-line
legacy monolith.

Endpoint: POST /api/pricecheck/<pcid>/generate-v2
Contract:
  - Input: pcid (existing Price Check in SQLite/JSON)
  - Output: { ok, output_path, download_url, version: "v2", engine: "fill_engine",
              profile_id, byte_count, warnings: [] }
  - On failure: 500 + { ok: False, error, stage } so the operator knows WHERE
    in the pipeline it failed (load, adapt, profile, fill, write).

This endpoint does NOT replace the legacy POST /pricecheck/<pcid>/generate
route. Both co-exist. The operator chooses which to call. Once V2 is proven
on real data, the UI can flip to calling V2 and the legacy route becomes the
fallback button.

Design rules honored from the user's domain context:
  - PC output uses the 704a_reytech_standard profile (AMS 704 blank).
    If a buyer sends a customized variant, the fingerprint matcher picks the
    right profile — but today only 704a exists, so this is the single path.
    Divergence from buyer's PDF layout on customized variants is a known gap;
    see CLASS A in the shadow-divergence audit.
  - No tax applied (PC rule).
  - Filename follows RFQ_Package_<agency>_Reytech convention (memory feedback).
"""
import logging
import os
from datetime import datetime, timezone, timedelta

from flask import request, jsonify

from src.api.shared import bp, auth_required
from src.core.error_handler import safe_route
from src.core.paths import DATA_DIR, OUTPUT_DIR

log = logging.getLogger(__name__)
_PST = timezone(timedelta(hours=-8))


@bp.route("/api/pricecheck/<pcid>/generate-v2", methods=["POST"])
@auth_required
@safe_route
def pc_generate_v2(pcid):
    """Generate a PC package via the V2 pipeline (Quote model + fill engine).

    Returns JSON so the caller can display the download path and byte count.
    Never overlaps with the legacy generate route — new filename suffix (_v2)
    prevents clobbering the legacy output.
    """
    # ── Stage 1: Load the PC dict ──
    try:
        from src.api.data_layer import _load_price_checks
        pcs = _load_price_checks()
        pc = pcs.get(pcid)
        if not pc:
            return jsonify({
                "ok": False,
                "error": f"PC {pcid} not found",
                "stage": "load",
            }), 404
    except Exception as e:
        log.exception("V2 PC load failed for %s", pcid)
        return jsonify({"ok": False, "error": str(e), "stage": "load"}), 500

    # ── Stage 2: Adapt dict → Quote (pydantic model with validation) ──
    try:
        from src.core.quote_model import Quote
        quote = Quote.from_legacy_dict(pc, doc_type="pc")
    except Exception as e:
        log.exception("V2 PC Quote.from_legacy_dict failed for %s", pcid)
        return jsonify({
            "ok": False,
            "error": f"Could not adapt PC dict to Quote model: {e}",
            "stage": "adapt",
        }), 500

    # ── Stage 3: Resolve profile (704a for standard AMS 704) ──
    try:
        from src.forms.profile_registry import load_profiles, match_profile
        profiles = load_profiles()

        profile = None
        # Prefer fingerprint match on buyer's source PDF if we have one.
        # Falls back to 704a_reytech_standard (only profile today).
        source_pdf = pc.get("source_pdf") or ""
        if source_pdf and os.path.exists(source_pdf):
            try:
                profile = match_profile(source_pdf, profiles)
            except Exception as _pe:
                log.debug("V2 profile fingerprint match failed, falling back: %s", _pe)
                profile = None

        if profile is None:
            profile = profiles.get("704a_reytech_standard")

        if profile is None:
            return jsonify({
                "ok": False,
                "error": "No profile available — 704a_reytech_standard not loaded",
                "stage": "profile",
            }), 500
    except Exception as e:
        log.exception("V2 PC profile resolve failed for %s", pcid)
        return jsonify({
            "ok": False,
            "error": f"Profile resolution failed: {e}",
            "stage": "profile",
        }), 500

    # ── Stage 4: Fill (profile-driven AcroForm fill via fill_engine) ──
    try:
        from src.forms.fill_engine import fill
        pdf_bytes = fill(quote, profile)
    except Exception as e:
        log.exception("V2 PC fill_engine failed for %s (profile=%s)",
                      pcid, getattr(profile, "id", "?"))
        return jsonify({
            "ok": False,
            "error": f"Fill engine failed: {e}",
            "stage": "fill",
            "profile_id": getattr(profile, "id", ""),
        }), 500

    if not pdf_bytes:
        return jsonify({
            "ok": False,
            "error": "Fill engine returned empty bytes",
            "stage": "fill",
            "profile_id": getattr(profile, "id", ""),
        }), 500

    # ── Stage 5: Write PDF to disk ──
    try:
        pc_num = pc.get("pc_number", "") or pcid
        # Memory convention: RFQ_Package_<agency>_Reytech for bid packages.
        # For single-form PC outputs, match legacy naming minus the _Package
        # segment but keep the _v2 suffix so it never collides with legacy.
        safe_pc_num = "".join(c if c.isalnum() or c in "-_" else "_"
                              for c in str(pc_num))[:60]
        filename = f"PC_{safe_pc_num}_{pcid}_Reytech_v2.pdf"

        out_dir = os.path.join(OUTPUT_DIR, pcid)
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, filename)

        with open(output_path, "wb") as f:
            f.write(pdf_bytes)

    except Exception as e:
        log.exception("V2 PC write failed for %s", pcid)
        return jsonify({
            "ok": False,
            "error": f"Could not write output PDF: {e}",
            "stage": "write",
        }), 500

    # ── Stage 6: Record the generation on the PC for audit trail ──
    # Non-fatal: if this fails, the PDF is already written and served.
    try:
        from src.api.data_layer import _save_single_pc
        pc.setdefault("v2_generations", []).append({
            "at": datetime.now(_PST).isoformat(),
            "output_path": output_path,
            "byte_count": len(pdf_bytes),
            "profile_id": profile.id,
            "engine": "fill_engine",
        })
        # raise_on_error=False: audit write failure shouldn't fail the V2
        # generate. The PDF is on disk; operator has the response path.
        _save_single_pc(pcid, pc, raise_on_error=False)
    except Exception as _audit_e:
        log.warning("V2 PC audit write failed for %s (non-fatal): %s",
                    pcid, _audit_e)

    log.info(
        "V2 PC generate: pcid=%s profile=%s bytes=%d path=%s",
        pcid, profile.id, len(pdf_bytes), output_path,
    )

    return jsonify({
        "ok": True,
        "version": "v2",
        "engine": "fill_engine",
        "profile_id": profile.id,
        "output_path": output_path,
        "byte_count": len(pdf_bytes),
        "download_url": f"/dl/{pcid}/{os.path.basename(output_path)}?inline=1",
        "stage": "done",
    })
