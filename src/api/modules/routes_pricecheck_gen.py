# routes_pricecheck_gen.py — Bundle generation, email pipeline, SCPRS, polling
# Split from routes_pricecheck.py — Multi-PC Bundle + Email/SCPRS/Polling routes

from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.error_handler import safe_route
from src.core.security import rate_limit
from flask import redirect, flash, send_file, session
from src.core.paths import DATA_DIR, OUTPUT_DIR, UPLOAD_DIR
from src.core.db import get_db
from src.api.render import render_page
import os
import json
from datetime import datetime, timedelta, timezone

# ═══════════════════════════════════════════════════════════════════════════════
# ── Multi-PC Bundle: Generate, Send, View ─────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

def _load_bundle_pcs(bundle_id):
    """Load all PCs belonging to a bundle, sorted by page_start."""
    pcs = _load_price_checks()
    bundle_pcs = []
    for pcid, pc in pcs.items():
        if pc.get("bundle_id") == bundle_id:
            pc["id"] = pcid
            bundle_pcs.append(pc)
    bundle_pcs.sort(key=lambda p: int(p.get("page_start", 0)))
    return bundle_pcs


@bp.route("/api/pricecheck/bundle/<bundle_id>/generate", methods=["POST"])
@auth_required
def api_bundle_generate(bundle_id):
    """Generate combined PDF for a multi-PC bundle. All PCs filled, merged into one response."""
    try:
        bundle_pcs = _load_bundle_pcs(bundle_id)
        if not bundle_pcs:
            return jsonify({"ok": False, "error": f"No PCs found for bundle {bundle_id}"})

        force = request.args.get("force") == "1" or (request.get_json(force=True, silent=True) or {}).get("force")

        # Check pricing completeness
        ready = []
        not_ready = []
        for pc in bundle_pcs:
            items = pc.get("items", [])
            priced = sum(1 for it in items if it.get("unit_price") or it.get("no_bid"))
            if priced >= len(items) and len(items) > 0:
                ready.append(pc["id"])
            else:
                not_ready.append({"pc_id": pc["id"], "pc_number": pc.get("pc_number", ""),
                                  "priced": priced, "total": len(items)})

        if not_ready and not force:
            return jsonify({
                "ok": False, "partial": True,
                "ready": ready, "not_ready": not_ready,
                "message": f"{len(ready)} of {len(bundle_pcs)} PCs fully priced. Send force=true to generate anyway.",
            })

        # Generate each PC's individual PDF
        pc_outputs = []
        errors = []
        for pc in bundle_pcs:
            pcid = pc["id"]
            result = _generate_pc_pdf(pcid)
            if result.get("ok"):
                pc_outputs.append({
                    "pc_id": pcid,
                    "page_start": int(pc.get("page_start", 0)),
                    "page_end": int(pc.get("page_end", 0)),
                    "output_pdf": result["output_path"],
                    "summary": result.get("summary", {}),
                })
            else:
                errors.append({"pc_id": pcid, "error": result.get("error", "Unknown")})
                log.error("BUNDLE %s: PC %s generate failed: %s", bundle_id, pcid, result.get("error"))

        if not pc_outputs:
            return jsonify({"ok": False, "error": "All PC generations failed", "errors": errors})

        # Merge into combined PDF
        source_pdf = bundle_pcs[0].get("source_pdf", "")
        non_pc_pages = bundle_pcs[0].get("bundle_non_pc_pages", [])

        # Build safe output filename
        _inst = bundle_pcs[0].get("institution", "").replace(" ", "_")[:20]
        _date = datetime.now().strftime("%Y%m%d")
        bundle_output = os.path.join(DATA_DIR, f"Bundle_{_inst}_{_date}_{bundle_id}_Reytech.pdf")

        from src.forms.price_check import merge_bundle_pdfs
        merge_result = merge_bundle_pdfs(source_pdf, pc_outputs, non_pc_pages, bundle_output)

        if not merge_result.get("ok"):
            return jsonify({"ok": False, "error": merge_result.get("error", "Merge failed")})

        # Store bundle output path on each PC
        for pc in bundle_pcs:
            pc["bundle_output_pdf"] = bundle_output
            _save_single_pc(pc["id"], pc)

        # Aggregate summary
        total_items = sum(s.get("summary", {}).get("items_total", 0) for s in pc_outputs)
        total_priced = sum(s.get("summary", {}).get("items_priced", 0) for s in pc_outputs)
        grand_total = sum(s.get("summary", {}).get("total", 0) for s in pc_outputs)

        log.info("BUNDLE %s: generated combined PDF — %d PCs, %d/%d items priced, total=$%.2f, pages=%d",
                 bundle_id, len(pc_outputs), total_priced, total_items, grand_total,
                 merge_result.get("page_count", 0))

        resp = {
            "ok": True,
            "download": f"/api/pricecheck/download/{os.path.basename(bundle_output)}",
            "bundle_id": bundle_id,
            "pcs_generated": len(pc_outputs),
            "pcs_failed": len(errors),
            "page_count": merge_result.get("page_count", 0),
            "summary": {
                "items_total": total_items,
                "items_priced": total_priced,
                "grand_total": grand_total,
            },
        }
        if errors:
            resp["errors"] = errors
        return jsonify(resp)

    except Exception as e:
        log.error("BUNDLE GENERATE %s CRASHED: %s", bundle_id, e, exc_info=True)
        return jsonify({"ok": False, "error": f"Server error: {e}"})


@bp.route("/api/pricecheck/bundle/<bundle_id>/send", methods=["POST"])
@auth_required
def api_bundle_send(bundle_id):
    """Send the combined bundle PDF via email. Marks all PCs as sent."""
    try:
        bundle_pcs = _load_bundle_pcs(bundle_id)
        if not bundle_pcs:
            return jsonify({"ok": False, "error": f"No PCs found for bundle {bundle_id}"})

        data = request.get_json(force=True, silent=True) or {}
        to_email = data.get("to") or bundle_pcs[0].get("requestor_email", "")
        # Stricter than just "@" — catch typo'd domains, missing TLDs, etc.
        # Audit 2026-04-27 P0 #4.
        try:
            from src.core.validators import validate_email, ValidationError
            to_email = validate_email(to_email or "")
            if not to_email:
                raise ValidationError("recipient email is empty")
        except ValidationError as ve:
            return jsonify({"ok": False, "error": f"Invalid recipient: {ve}"}), 400

        # Find bundle PDF
        bundle_pdf = bundle_pcs[0].get("bundle_output_pdf", "")
        if not bundle_pdf or not os.path.exists(bundle_pdf):
            return jsonify({"ok": False, "error": "Bundle PDF not found — generate first"})

        # Build email
        source_name = bundle_pcs[0].get("multi_pc_source", "Quote")
        # Strip .pdf extension and add _Reytech
        attach_name = re.sub(r'\.pdf$', '', source_name, flags=re.IGNORECASE) + "_Reytech.pdf"
        pc_numbers = [pc.get("pc_number", "") for pc in bundle_pcs if pc.get("pc_number")]
        subject = data.get("subject") or f"Price Quotes — {', '.join(pc_numbers) if pc_numbers else bundle_id}"
        body_text = data.get("body") or (
            f"Please find attached our price quotes for the following Price Checks:\n"
            + "\n".join(f"  - {pc.get('pc_number', pc['id'])}" for pc in bundle_pcs)
            + "\n\nThank you,\nReytech Inc."
        )

        # Send via Gmail API (OAuth refresh token — replaces smtplib.SMTP_SSL
        # + GMAIL_PASSWORD app-password. Same pattern as the IN-5 migration
        # of send_quote_email in routes_analytics.py.)
        from src.core import gmail_api
        if not gmail_api.is_configured():
            return jsonify({"ok": False, "error": "Gmail API not configured"}), 400

        # gmail_api.send_message derives the attachment filename from
        # os.path.basename(path). The bundle PDF on disk has an internal
        # name (e.g. bundle_<id>.pdf); we want the buyer to see the
        # Reytech-branded attach_name. Copy to a temp file with the
        # desired filename before sending.
        import shutil, tempfile
        tmp_dir = tempfile.mkdtemp(prefix="bundle_send_")
        named_pdf = os.path.join(tmp_dir, attach_name)
        try:
            shutil.copy(bundle_pdf, named_pdf)
            service = gmail_api.get_send_service()
            gmail_api.send_message(
                service,
                to=to_email,
                subject=subject,
                body_plain=body_text,
                attachments=[named_pdf],
            )
        finally:
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception as _cleanup_err:
                log.debug("bundle send tmpdir cleanup: %s", _cleanup_err)

        # Mark all PCs as sent
        now_iso = datetime.now().isoformat()
        for pc in bundle_pcs:
            pc["status"] = "sent"
            pc["sent_at"] = now_iso
            pc["sent_to"] = to_email
            _save_single_pc(pc["id"], pc)
            try:
                _log_crm_activity(pc["id"], "bundle_quote_sent",
                    f"Bundle quote sent to {to_email} (bundle {bundle_id})",
                    actor="user")
            except Exception as _e:
                log.debug("suppressed: %s", _e)

        log.info("BUNDLE SEND %s: sent to %s (%d PCs)", bundle_id, to_email, len(bundle_pcs))
        return jsonify({"ok": True, "sent_to": to_email, "pcs_sent": len(bundle_pcs)})

    except Exception as e:
        log.error("BUNDLE SEND %s: %s", bundle_id, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:200]})


@bp.route("/api/pricecheck/bundle/<bundle_id>/convert-each-to-rfq", methods=["POST"])
@auth_required
def api_bundle_convert_each(bundle_id):
    """Convert each PC in a bundle into its own RFQ. All RFQs get bundle_id for sibling awareness."""
    try:
        # Hold both locks for the bundle conversion. Without these, a parallel
        # PC autosave or concurrent click could race the read of
        # `converted_to_rfq` and create duplicate RFQs for the same PC.
        from src.api.data_layer import _save_pcs_lock, _save_rfqs_lock
        with _save_pcs_lock, _save_rfqs_lock:
            bundle_pcs = _load_bundle_pcs(bundle_id)
            if not bundle_pcs:
                return jsonify({"ok": False, "error": f"No PCs found for bundle {bundle_id}"})

            from src.api.modules.routes_analytics import _convert_single_pc_to_rfq
            from src.api.dashboard import _save_single_rfq

            created = []
            now = datetime.now().isoformat()

            for pc in bundle_pcs:
                pcid = pc["id"]
                if pc.get("converted_to_rfq"):
                    created.append({"pc_id": pcid, "rfq_id": pc.get("linked_rfq_id", ""),
                                    "skipped": True, "reason": "already converted"})
                    continue

                rfq_id, rfq_data, files_copied = _convert_single_pc_to_rfq(pcid, pc)
                _save_single_rfq(rfq_id, rfq_data)

                # Update PC with link
                pc["linked_rfq_id"] = rfq_id
                pc["linked_rfq_at"] = now
                pc["converted_to_rfq"] = True
                _save_single_pc(pc["id"], pc)

                created.append({"pc_id": pcid, "rfq_id": rfq_id,
                                "items": len(rfq_data.get("line_items", [])),
                                "url": f"/rfq/{rfq_id}"})

            # Cross-reference sibling RFQ IDs on each created RFQ
            rfq_ids = [c["rfq_id"] for c in created if not c.get("skipped") and c.get("rfq_id")]
            if len(rfq_ids) > 1:
                from src.api.dashboard import load_rfqs
                rfqs = load_rfqs()
                for rid in rfq_ids:
                    rfq = rfqs.get(rid)
                    if rfq:
                        rfq["sibling_rfq_ids"] = [r for r in rfq_ids if r != rid]
                        _save_single_rfq(rid, rfq)

        log.info("BUNDLE %s: converted %d PCs to separate RFQs", bundle_id, len(created))
        return jsonify({"ok": True, "created": created, "total": len(created)})

    except Exception as e:
        log.error("BUNDLE CONVERT-EACH %s: %s", bundle_id, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:200]})


@bp.route("/api/pricecheck/bundle/<bundle_id>/convert-to-rfq", methods=["POST"])
@auth_required
def api_bundle_convert_single(bundle_id):
    """Convert all PCs in a bundle into ONE combined RFQ with all items."""
    try:
        # Hold both locks — same race shape as PR #778, two-store variant.
        from src.api.data_layer import _save_pcs_lock, _save_rfqs_lock
        with _save_pcs_lock, _save_rfqs_lock:
            bundle_pcs = _load_bundle_pcs(bundle_id)
            if not bundle_pcs:
                return jsonify({"ok": False, "error": f"No PCs found for bundle {bundle_id}"})

            from src.core.pc_rfq_linker import auto_link_rfq_to_bundle
            from src.api.dashboard import _save_single_rfq
            import uuid as _uuid

            rfq_id = str(_uuid.uuid4())[:8]
            now = datetime.now().isoformat()

            # Build RFQ from first PC's metadata
            first_pc = bundle_pcs[0]
            rfq_data = {
                "id": rfq_id,
                "solicitation_number": first_pc.get("pc_number", ""),
                "status": "new",
                "source": "bundle_conversion",
                "requestor_name": first_pc.get("requestor", ""),
                "requestor_email": first_pc.get("requestor_email", ""),
                "department": first_pc.get("institution", ""),
                "delivery_location": first_pc.get("ship_to", ""),
                "due_date": first_pc.get("due_date", ""),
                "line_items": [],  # will be populated by auto_link_rfq_to_bundle
                "created_at": now,
                "bundle_id": bundle_id,
            }

            # Import items from ALL bundle PCs
            pc_tuples = [(pc["id"], pc) for pc in bundle_pcs]
            imported = auto_link_rfq_to_bundle(rfq_data, pc_tuples)

            # Check if any items got priced
            if any(li.get("price_per_unit") for li in rfq_data.get("line_items", [])):
                rfq_data["status"] = "priced"

            _save_single_rfq(rfq_id, rfq_data)

            # Mark all PCs as converted
            for pc in bundle_pcs:
                pc["linked_rfq_id"] = rfq_id
                pc["linked_rfq_at"] = now
                pc["converted_to_rfq"] = True
                _save_single_pc(pc["id"], pc)

        log.info("BUNDLE %s: converted to single RFQ %s with %d items from %d PCs",
                 bundle_id, rfq_id, len(rfq_data.get("line_items", [])), len(bundle_pcs))
        return jsonify({"ok": True, "rfq_id": rfq_id,
                        "items": len(rfq_data.get("line_items", [])),
                        "pcs": len(bundle_pcs), "url": f"/rfq/{rfq_id}"})

    except Exception as e:
        log.error("BUNDLE CONVERT-SINGLE %s: %s", bundle_id, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)[:200]})


@bp.route("/pricecheck/bundle/<bundle_id>")
@auth_required
def pricecheck_bundle_view(bundle_id):
    """Bundle detail page — shows all PCs, progress, generate/send buttons."""
    bundle_pcs = _load_bundle_pcs(bundle_id)
    if not bundle_pcs:
        return "Bundle not found", 404

    # Aggregate stats
    total_items = 0
    total_priced = 0
    grand_total = 0.0
    for pc in bundle_pcs:
        items = pc.get("items", [])
        total_items += len(items)
        total_priced += sum(1 for it in items if it.get("unit_price") or it.get("no_bid"))
        grand_total += sum(float(it.get("unit_price", 0) or 0) * int(it.get("qty", 1) or 1)
                          for it in items if it.get("unit_price"))

    source_file = bundle_pcs[0].get("multi_pc_source", "")
    bundle_pdf = bundle_pcs[0].get("bundle_output_pdf", "")
    bundle_pdf_exists = bool(bundle_pdf and os.path.exists(bundle_pdf))
    requestor = bundle_pcs[0].get("requestor", "")
    requestor_email = bundle_pcs[0].get("requestor_email", "")
    institution = bundle_pcs[0].get("institution", "")

    return render_page("pc_bundle.html",
        bundle_id=bundle_id,
        bundle_pcs=bundle_pcs,
        source_file=source_file,
        institution=institution,
        requestor=requestor,
        requestor_email=requestor_email,
        total_items=total_items,
        total_priced=total_priced,
        grand_total=grand_total,
        bundle_pdf_exists=bundle_pdf_exists,
        bundle_pdf_name=os.path.basename(bundle_pdf) if bundle_pdf else "",
    )


@bp.route("/api/pricecheck/multi-upload", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_pc_multi_upload():
    """Upload multiple separate AMS 704 files (PDF or office docs). Creates one PC per file, bundled."""
    files = request.files.getlist("files")
    if not files or not any(f.filename for f in files):
        return jsonify({"ok": False, "error": "No files uploaded"})

    # Shared buyer info from form fields
    requestor = request.form.get("requestor", "").strip()
    requestor_email = request.form.get("requestor_email", "").strip()
    institution = request.form.get("institution", "").strip()
    due_date = request.form.get("due_date", "").strip()

    import uuid as _uuid
    import shutil as _shutil
    from src.forms.price_check import parse_ams704
    from src.api.dashboard import _save_single_pc, DATA_DIR

    bundle_id = f"bnd_{_uuid.uuid4().hex[:8]}" if len(files) > 1 else ""
    created_pcs = []
    by_institution = {}

    for f in files:
        if not f.filename:
            continue
        safe_name = f.filename.replace("..", "").replace("/", "_").replace("\\", "_")
        pc_id = f"pc_{_uuid.uuid4().hex[:8]}"

        # Save file
        upload_dir = os.path.join(DATA_DIR, "pc_pdfs")
        os.makedirs(upload_dir, exist_ok=True)
        pc_file = os.path.join(upload_dir, f"{pc_id}_{safe_name}")
        f.save(pc_file)

        # Parse — PDF uses AMS 704 parser, office docs use doc_converter
        try:
            from src.forms.doc_converter import is_office_doc as _is_office
            if _is_office(pc_file):
                from src.forms.doc_converter import extract_text as _extr, parse_items_from_text as _parse_txt
                _doc_text = _extr(pc_file)
                parsed = {}
                try:
                    from src.forms.vision_parser import parse_from_text as _ai_parse, is_available as _ai_ok
                    if _ai_ok():
                        parsed = _ai_parse(_doc_text, source_path=pc_file) or {}
                except Exception as _e:
                    log.debug("suppressed: %s", _e)
                if not parsed.get("line_items"):
                    _fb = _parse_txt(_doc_text)
                    parsed = {"line_items": _fb or [], "header": {}, "parse_method": "regex_fallback"}
            else:
                parsed = parse_ams704(pc_file)
        except Exception as e:
            log.error("multi-upload parse error for %s: %s", safe_name, e)
            parsed = {"error": str(e), "line_items": [], "header": {}}

        items = parsed.get("line_items", [])
        header = parsed.get("header", {})

        # Derive PC name from filename
        import re as _re_fn
        _name = os.path.splitext(safe_name)[0]
        _name = _re_fn.sub(r'^AMS\s*704\s*(?:Price\s*Check\s*)?(?:Worksheet)?\s*[-_\s]*', '', _name, flags=_re_fn.IGNORECASE)
        _name = _re_fn.sub(r'\s*\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\s*$', '', _name)
        pc_name = _name.strip() or os.path.splitext(safe_name)[0]

        # Use shared buyer info, fallback to PDF header
        _inst = institution or header.get("institution", "")
        _due = due_date or header.get("due_date", "")
        _req = requestor or header.get("requestor", "")

        pc = {
            "id": pc_id, "pc_number": pc_name,
            "institution": _inst, "due_date": _due,
            "requestor": _req,
            "requestor_email": requestor_email,
            "requestor_name": _req,
            "ship_to": header.get("ship_to", ""),
            "items": items, "source_pdf": pc_file,
            "status": "parsed" if items else "new",
            "parsed": parsed,
            "parse_quality": parsed.get("parse_quality", {}),
            "created_at": datetime.now().isoformat(),
            "source": "manual_multi_upload",
            "reytech_quote_number": "", "linked_quote_number": "",
            "bundle_id": bundle_id,
            "bundle_total_pcs": len(files) if bundle_id else 0,
        }
        _save_single_pc(pc_id, pc)

        # Persist to DB for deploy resilience
        try:
            from src.core.dal import save_rfq_file
            with open(pc_file, "rb") as _pf:
                save_rfq_file(pc_id, safe_name, "application/pdf", _pf.read(),
                              category="source", uploaded_by="manual_upload")
        except Exception as _e:
            log.debug("suppressed: %s", _e)

        # Auto-enrich
        try:
            from src.agents.pc_enrichment_pipeline import enrich_pc_background
            enrich_pc_background(pc_id)
        except Exception as _ee:
            log.warning("multi-upload enrich %s: %s", pc_id, _ee)

        pc_info = {
            "pc_id": pc_id, "pc_number": pc_name,
            "institution": _inst, "requestor": _req,
            "items": len(items), "url": f"/pricecheck/{pc_id}",
        }
        created_pcs.append(pc_info)
        by_institution.setdefault(_inst or "Unknown", []).append(pc_info)

    bundle_url = f"/pricecheck/bundle/{bundle_id}" if bundle_id else ""
    return jsonify({
        "ok": True,
        "total": len(created_pcs),
        "bundle_id": bundle_id,
        "bundle_url": bundle_url,
        "pcs": created_pcs,
        "by_institution": by_institution,
        "source_file": "Multiple files",
    })


@bp.route("/api/pricecheck/create-manual", methods=["POST"])
@auth_required
@safe_route
def api_pc_create_manual():
    """Create a Price Check manually from the dashboard."""
    data = request.get_json(force=True, silent=True) or {}
    sol = data.get("solicitation_number", "").strip()
    inst = data.get("institution", "").strip()
    if not sol and not inst:
        return jsonify({"ok": False, "error": "solicitation_number or institution required"})

    import uuid
    pcid = "pc_" + uuid.uuid4().hex[:8]

    pc = {
        "id": pcid,
        "pc_number": sol or inst,
        "solicitation_number": sol,
        "institution": inst,
        "requestor": data.get("requestor", ""),
        "buyer": data.get("requestor", ""),
        "due_date": data.get("due_date", ""),
        "status": "new",
        "source": "manual",
        "created_at": datetime.now().isoformat(),
        "items": [],
    }

    try:
        _save_single_pc(pcid, pc)
    except Exception as e:
        log.error("create-manual save failed: %s", e)
        return jsonify({"ok": False, "error": f"Save failed: {e}"}), 500

    return jsonify({"ok": True, "pc_id": pcid, "sol": sol or inst})


@bp.route("/api/resync")
@auth_required
@safe_route
def api_resync():
    """Re-import emails WITHOUT destroying user work.
    
    PRESERVES:
    - RFQs with terminal status (sent, won, lost, generated, draft)
    - All price checks (PCs persist until explicitly dismissed)
    - All user-set pricing, notes, quote numbers
    
    CLEARS:
    - RFQs with status 'new' or 'parse_error' (stale imports)
    - Processed email UID list (so missed emails get re-imported)
    """
    log.info("Smart resync triggered — preserving terminal statuses")
    
    try:
        # ── 1. Snapshot what we want to keep ──
        rfqs = load_rfqs()
        pcs_before = _load_price_checks()
        pc_count = len(pcs_before)
        
        TERMINAL_STATUSES = {"sent", "not_responding", "draft", "dismissed", "archived"}
        
        # Keep RFQs with terminal status — keyed by BOTH id and email_uid
        kept_rfqs = {}           # id → full rfq data (preserved)
        kept_by_uid = set()      # email_uids we're keeping (skip on re-import)
        kept_by_sol = set()      # solicitation numbers we're keeping
        cleared_count = 0
        
        for rid, r in rfqs.items():
            status = (r.get("status") or "new").lower()
            if status in TERMINAL_STATUSES:
                kept_rfqs[rid] = r
                uid = r.get("email_uid")
                if uid:
                    kept_by_uid.add(uid)
                sol = r.get("solicitation_number", "")
                if sol and sol != "unknown":
                    kept_by_sol.add(sol.strip())
            else:
                cleared_count += 1
        
        # Also build set of PC email_uids to skip (don't re-create PCs that already exist)
        # BUT: exclude parse_error PCs with 0 items — those need re-processing after fixes
        pc_uids = set()
        for pc in pcs_before.values():
            uid = pc.get("email_uid")
            if uid:
                # Skip broken PCs — they should be re-imported after a fix
                if pc.get("status") == "parse_error" and not pc.get("items"):
                    continue
                pc_uids.add(uid)
        
        log.info("Resync: keeping %d terminal RFQs, clearing %d stale, %d PCs preserved",
                 len(kept_rfqs), cleared_count, pc_count)
        
        # ── 2. Save only the kept RFQs ──
        save_rfqs(kept_rfqs)
        
        # ── 3. Clear processed UIDs completely ──
        # The dedup logic in process_rfq_email handles duplicates:
        #   - email_uid match → skip
        #   - solicitation_number match → skip or link as amendment
        # So we don't need to pre-seed. This ensures emails that failed
        # processing before (e.g. after a bug fix) get a fresh chance.
        proc_file = os.path.join(DATA_DIR, "processed_emails.json")
        if os.path.exists(proc_file):
            os.remove(proc_file)
        # Also clear SQLite processed_emails + fingerprints tables
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM processed_emails")
                try:
                    conn.execute("DELETE FROM email_fingerprints")
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        log.info("Resync: cleared processed_emails (JSON + SQLite + fingerprints)")
        
        # ── 4. Reset poller + re-poll ──
        global _shared_poller
        _shared_poller = None
        imported = _safe_do_poll_check()
        
        # ── 5. Report ──
        rfqs_after = load_rfqs()
        pcs_after = _load_price_checks()
        
        log.info("Resync complete: %d new imported, %d preserved, %d total RFQs, %d PCs",
                 len(imported), len(kept_rfqs), len(rfqs_after), len(pcs_after))
        
        return jsonify({
            "ok": True,
            "cleared": cleared_count,
            "found": len(imported),
            "preserved": len(kept_rfqs),
            "total_rfqs": len(rfqs_after),
            "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
            "pcs_preserved": pc_count,
            "pcs_total": len(pcs_after),
            "last_check": POLL_STATUS.get("last_check"),
        })
    except Exception as e:
        log.error("Resync failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "found": 0, "error": str(e)})


def _remove_processed_uid(uid):
    """Remove a single UID from processed_emails.json."""
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if not os.path.exists(proc_file):
        return
    try:
        with open(proc_file) as f:
            processed = json.load(f)
        if isinstance(processed, list) and uid in processed:
            processed.remove(uid)
            with open(proc_file, "w") as f:
                json.dump(processed, f)
            log.info(f"Removed UID {uid} from processed list")
        elif isinstance(processed, dict) and uid in processed:
            del processed[uid]
            with open(proc_file, "w") as f:
                json.dump(processed, f)
    except Exception as e:
        log.error(f"Error removing UID: {e}")


@bp.route("/api/email-debug")
@auth_required
@safe_route
def api_email_debug():
    """Diagnostic: show processed email count, poller state, recent traces."""
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    proc_count = 0
    try:
        if os.path.exists(proc_file):
            with open(proc_file) as f:
                proc_data = json.load(f)
                proc_count = len(proc_data) if isinstance(proc_data, (list, dict)) else 0
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    
    # Get poller diagnostics
    diag = {}
    global _shared_poller
    if _shared_poller and hasattr(_shared_poller, '_diag'):
        diag = _shared_poller._diag.copy()
        diag.pop("subjects_seen", None)  # too verbose
    
    traces = POLL_STATUS.get("_email_traces", [])[-10:]
    
    return jsonify({
        "ok": True,
        "processed_count": proc_count,
        "poll_status": {
            "running": POLL_STATUS.get("running"),
            "last_check": POLL_STATUS.get("last_check"),
            "emails_found": POLL_STATUS.get("emails_found"),
            "error": POLL_STATUS.get("error"),
        },
        "poller_diag": diag,
        "recent_traces": traces,
    })


@bp.route("/api/email-rejections")
@auth_required
@safe_route
def api_email_rejections():
    """Audit log: show emails that were blocked/skipped by the filtering pipeline.

    Use this to tune filters — see what's being caught and what's slipping through.
    Query params:
      ?reason=blocklist|marketing|low_score  (filter by rejection reason)
      ?limit=50  (max results, default 50)
      ?since=2026-04-01  (filter by date)
    """
    from flask import request as req
    reason_filter = req.args.get("reason", "")
    limit = min(int(req.args.get("limit", 50)), 500)
    since = req.args.get("since", "")

    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS email_rejections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_uid TEXT, sender TEXT, subject TEXT,
                reason TEXT, details TEXT, rejected_at TEXT
            )""")

            query = "SELECT email_uid, sender, subject, reason, details, rejected_at FROM email_rejections"
            params = []
            clauses = []

            if reason_filter:
                clauses.append("reason = ?")
                params.append(reason_filter)
            if since:
                clauses.append("rejected_at >= ?")
                params.append(since)

            if clauses:
                query += " WHERE " + " AND ".join(clauses)
            query += " ORDER BY rejected_at DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()

            # Also get summary counts by reason
            summary = conn.execute(
                "SELECT reason, COUNT(*) as cnt FROM email_rejections "
                "GROUP BY reason ORDER BY cnt DESC"
            ).fetchall()

        rejections = [{
            "uid": r[0], "sender": r[1], "subject": r[2],
            "reason": r[3], "details": r[4], "rejected_at": r[5],
        } for r in rows]

        return jsonify({
            "ok": True,
            "count": len(rejections),
            "rejections": rejections,
            "summary": {r[0]: r[1] for r in summary},
        })
    except Exception as e:
        log.error("Email rejections endpoint error: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/force-reprocess", methods=["POST"])
@auth_required
@safe_route
def api_force_reprocess():
    """Nuclear option: clear ALL processed UIDs and re-poll. Use when
    a specific email isn't being picked up despite code fixes.

    UX Audit 2026-04-14 §9.3:
      - The UI button was removed from the header. A single misclick
        wiped processed-email state with no confirmation.
      - GET requests are rejected — this endpoint is destructive and
        must not fire on any drive-by GET from a crawler, a pasted
        URL, or a misbehaving browser extension.
      - POST requests must include a body parameter
        `confirm=wipe_all` to actually run. Anything else returns 400.

    To invoke from a script:
        curl -u user:pass -X POST /api/force-reprocess \\
             -H 'Content-Type: application/json' \\
             -d '{"confirm":"wipe_all"}'
    """
    # Explicit confirmation gate
    confirm = ""
    try:
        data = request.get_json(silent=True) or {}
        confirm = (data.get("confirm") or "").strip()
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    if confirm != "wipe_all":
        return jsonify({
            "ok": False,
            "error": "BLOCKED: destructive action requires "
                     "{\"confirm\": \"wipe_all\"} in POST body. "
                     "This endpoint wipes processed-email state.",
            "blocked_reason": "ux_audit_p0_3",
        }), 400

    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    old_count = 0

    try:
        if os.path.exists(proc_file):
            with open(proc_file) as f:
                old_data = json.load(f)
                old_count = len(old_data) if isinstance(old_data, (list, dict)) else 0
            os.remove(proc_file)
            log.info("Force-reprocess: cleared %d processed UIDs from JSON", old_count)
    except Exception as e:
        log.error("Force-reprocess clear failed: %s", e)

    # Also clear SQLite processed_emails table (poller loads from both)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM processed_emails")
            try:
                conn.execute("DELETE FROM email_fingerprints")
            except Exception as _e:
                log.debug("suppressed: %s", _e)
        log.info("Force-reprocess: cleared SQLite processed_emails + fingerprints")
    except Exception as _db_e:
        log.warning("Force-reprocess: SQLite clear failed: %s", _db_e)

    # Reset poller: clear in-memory set + null the instance
    # Must clear in-memory set directly because global may not propagate
    # across exec() module boundary to dashboard.py's _shared_poller
    global _shared_poller
    if _shared_poller and hasattr(_shared_poller, '_processed'):
        old_count = max(old_count, len(_shared_poller._processed))
        _shared_poller._processed.clear()
        log.info("Force-reprocess: cleared %d in-memory processed UIDs", old_count)
    _shared_poller = None
    # Also clear via dashboard module directly
    try:
        import sys as _sys
        _dash = _sys.modules.get('src.api.dashboard')
        if _dash and hasattr(_dash, '_shared_poller') and _dash._shared_poller:
            if hasattr(_dash._shared_poller, '_processed'):
                _dash._shared_poller._processed.clear()
            _dash._shared_poller = None
            log.info("Force-reprocess: cleared dashboard._shared_poller")
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    
    # Re-poll
    try:
        imported = _safe_do_poll_check()
        return jsonify({
            "ok": True,
            "cleared_uids": old_count,
            "found": len(imported),
            "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/force-recapture", methods=["GET", "POST"])
@auth_required
@safe_route
def api_force_recapture():
    """Delete a specific RFQ/PC by keyword match, clear its UID, and re-poll.
    
    POST body: {"match": "calvet"} or {"rfq_id": "exact_id"}
    Searches solicitation_number, email_sender, email_subject, agency.
    """
    data = request.get_json(force=True, silent=True) or {}
    match_kw = (data.get("match") or "").lower().strip()
    exact_id = data.get("rfq_id", "").strip()
    
    if not match_kw and not exact_id:
        return jsonify({"ok": False, "error": "Provide 'match' keyword or 'rfq_id'"})
    
    removed_rfqs = []
    removed_pcs = []
    cleared_uids = []
    
    # ── Remove matching RFQs ──
    rfqs = load_rfqs()
    to_remove = []
    for rid, r in rfqs.items():
        if exact_id and rid == exact_id:
            to_remove.append(rid)
        elif match_kw:
            searchable = " ".join([
                r.get("solicitation_number", ""),
                r.get("email_sender", ""),
                r.get("email_subject", ""),
                r.get("agency", ""),
                r.get("agency_name", ""),
                r.get("requestor_email", ""),
            ]).lower()
            if match_kw in searchable:
                to_remove.append(rid)
    
    for rid in to_remove:
        r = rfqs.pop(rid)
        uid = r.get("email_uid")
        if uid:
            cleared_uids.append(uid)
        removed_rfqs.append({
            "id": rid,
            "sol": r.get("solicitation_number", "?"),
            "sender": r.get("email_sender", "?"),
            "items": len(r.get("line_items", [])),
        })
        log.info("Force-recapture: removed RFQ %s (sol=%s)", rid, r.get("solicitation_number", "?"))
    
    if to_remove:
        save_rfqs(rfqs)
    
    # ── Remove matching PCs ──
    pcs = _load_price_checks()
    pc_remove = []
    for pid, pc in pcs.items():
        if exact_id and pid == exact_id:
            pc_remove.append(pid)
        elif match_kw:
            searchable = " ".join([
                pc.get("pc_number", ""),
                pc.get("email_subject", ""),
                pc.get("requestor", ""),
                str(pc.get("institution", "")),
            ]).lower()
            if match_kw in searchable:
                pc_remove.append(pid)
    
    for pid in pc_remove:
        pc = pcs.pop(pid)
        uid = pc.get("email_uid")
        if uid:
            cleared_uids.append(uid)
        removed_pcs.append({"id": pid, "pc_number": pc.get("pc_number", "?")})
        log.info("Force-recapture: removed PC %s", pid)
    
    if pc_remove:
        _save_price_checks(pcs)
    
    # ── Clear UIDs from processed list ──
    if cleared_uids:
        proc_file = os.path.join(DATA_DIR, "processed_emails.json")
        try:
            if os.path.exists(proc_file):
                with open(proc_file) as f:
                    processed = json.load(f)
                if isinstance(processed, list):
                    before = len(processed)
                    processed = [u for u in processed if u not in cleared_uids]
                    with open(proc_file, "w") as f:
                        json.dump(processed, f)
                    log.info("Cleared %d UIDs from processed list", before - len(processed))
        except Exception as e:
            log.warning("UID clearing failed: %s", e)
    
    if not removed_rfqs and not removed_pcs:
        return jsonify({"ok": False, "error": f"No matches found for '{match_kw or exact_id}'"})
    
    # ── Reset poller and re-poll ──
    global _shared_poller
    _shared_poller = None
    
    try:
        imported = _safe_do_poll_check()
    except Exception as e:
        imported = []
        log.error("Re-poll failed: %s", e)
    
    return jsonify({
        "ok": True,
        "removed_rfqs": removed_rfqs,
        "removed_pcs": removed_pcs,
        "cleared_uids": len(cleared_uids),
        "reimported": len(imported),
        "new_rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
    })


@bp.route("/api/clear-queue", methods=["POST"])
@auth_required
@safe_route
def api_clear_queue():
    """Clear all RFQs from the queue (POST only — destructive operation)."""
    rfqs = load_rfqs()
    count = len(rfqs)
    if not count:
        return jsonify({"ok": True, "message": "Queue already empty"})
    rfqs.clear()
    save_rfqs(rfqs)
    log.warning("Queue cleared: %d RFQs removed by user", count)
    return jsonify({"ok": True, "message": f"Queue cleared ({count} RFQs removed)"})


@bp.route("/dl/<rid>/<fname>")
@auth_required
@safe_page
def download(rid, fname):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    sol = r["solicitation_number"] if r else rid
    safe = os.path.basename(fname)
    inline = request.args.get("inline") == "1"
    
    # Search filesystem — targeted directories only (no full os.walk)
    for search_dir in [
        os.path.join(OUTPUT_DIR, sol),
        os.path.join(OUTPUT_DIR, rid),
        os.path.join(DATA_DIR, "output", sol),
        os.path.join(DATA_DIR, "output", rid),
        os.path.join(DATA_DIR, "outputs"),
        OUTPUT_DIR,
    ]:
        candidate = os.path.join(search_dir, safe)
        if os.path.exists(candidate):
            return send_file(candidate, as_attachment=not inline, download_name=safe)

    # Fallback: check DB (rfq_files table — survives redeploys)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT data, filename FROM rfq_files WHERE (rfq_id=? OR rfq_id=?) AND filename=? ORDER BY id DESC LIMIT 1",
                (rid, sol, safe)).fetchone()
            if not row:
                row = conn.execute(
                    "SELECT data, filename FROM rfq_files WHERE filename=? ORDER BY id DESC LIMIT 1",
                    (safe,)).fetchone()
            if row and row["data"]:
                restore_dir = os.path.join(OUTPUT_DIR, sol or rid, "_restored")
                os.makedirs(restore_dir, exist_ok=True)
                restore_path = os.path.join(restore_dir, safe)
                with open(restore_path, "wb") as _fw:
                    _fw.write(row["data"])
                return send_file(restore_path, as_attachment=not inline, download_name=safe)
    except Exception as _e:
        log.debug("DB file lookup failed for %s: %s", safe, _e)
    
    flash("File not found", "error")
    return redirect(f"/rfq/{rid}")


@bp.route("/api/scprs/<rid>")
@auth_required
@safe_route
def api_scprs(rid):
    """SCPRS lookup API endpoint — batch search, single session."""
    log.info("SCPRS lookup requested for RFQ %s", rid)
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return jsonify({"error": "not found"})
    
    items = r.get("line_items", [])
    if not items:
        return jsonify({"results": [], "errors": ["No line items"]})
    
    results = []
    errors = []
    
    try:
        from src.agents.scprs_lookup import (
            _get_session, _build_search_terms, _find_best_line_match,
            _load_db, save_price
        )
        
        # Step 1: Try local DB first for each item
        db = _load_db()
        items_needing_search = []
        
        for i, item in enumerate(items):
            item_num = item.get("item_number", "")
            desc = item.get("description", "")
            search_terms = _build_search_terms(item_num, desc)
            
            # Check local DB
            local_hit = None
            if item_num and item_num.strip() in db:
                e = db[item_num.strip()]
                local_hit = {
                    "price": e["price"], "source": "local_db",
                    "date": e.get("date", ""), "confidence": "high",
                    "vendor": e.get("vendor", ""), "searched": search_terms,
                }
            
            if not local_hit and desc:
                dl = desc.lower().split("\n")[0].strip()
                for key, entry in db.items():
                    ed = (entry.get("description", "") or "").lower()
                    wa, wb = set(dl.split()), set(ed.split())
                    if wa and wb and len(wa & wb) / max(len(wa), len(wb)) > 0.5:
                        local_hit = {
                            "price": entry["price"], "source": "local_db_fuzzy",
                            "date": entry.get("date", ""), "confidence": "medium",
                            "vendor": entry.get("vendor", ""), "searched": search_terms,
                        }
                        break
            
            if local_hit:
                results.append(local_hit)
            else:
                results.append(None)  # placeholder
                items_needing_search.append((i, item_num, desc, search_terms))
        
        # Step 2: Batch SCPRS live search — ONE session for all items
        if items_needing_search:
            session = _get_session()
            if not session.initialized:
                session.init_session()
            
            if session.initialized:
                for idx, item_num, desc, search_terms in items_needing_search:
                    best_result = None
                    
                    for term in search_terms[:2]:  # Max 2 terms per item
                        try:
                            search_results = session.search(description=term)
                            if not search_results:
                                continue
                            
                            import time
                            time.sleep(0.3)
                            
                            # Sort by most recent
                            from datetime import datetime, timedelta
                            cutoff = datetime.now() - timedelta(days=548)
                            recent = [sr for sr in search_results
                                     if sr.get("start_date_parsed") and sr["start_date_parsed"] >= cutoff]
                            cands = sorted(recent or search_results,
                                          key=lambda x: x.get("start_date_parsed") or datetime.min,
                                          reverse=True)
                            
                            # Try detail page on top candidate
                            for c in cands[:2]:
                                try:
                                    if c.get("_results_html"):
                                        detail = session.get_detail(
                                            c["_results_html"], c["_row_index"],
                                            c.get("_click_action"))
                                        time.sleep(0.3)
                                        
                                        if detail and detail.get("line_items"):
                                            line = _find_best_line_match(
                                                detail["line_items"], item_num, desc)
                                            if line and line.get("unit_price_num"):
                                                best_result = {
                                                    "price": line["unit_price_num"],
                                                    "unit_price": line["unit_price_num"],
                                                    "quantity": line.get("quantity_num"),
                                                    "source": "fiscal_scprs",
                                                    "date": c.get("start_date", ""),
                                                    "confidence": "high",
                                                    "vendor": c.get("supplier_name", ""),
                                                    "po_number": c.get("po_number", ""),
                                                    "department": c.get("dept", ""),
                                                    "searched": search_terms,
                                                }
                                                break
                                        
                                        # Re-init session after detail (state is fragile)
                                        try:
                                            session.init_session()
                                        except Exception as _e:
                                            log.debug("Suppressed: %s", _e)
                                except Exception as _de:
                                    log.debug("Detail attempt: %s", _de)
                            
                            if best_result:
                                break
                            
                            # Fallback: use search-level data (PO total + vendor)
                            if not best_result and cands:
                                c = cands[0]
                                gt = c.get("grand_total_num", 0)
                                if gt and gt > 0:
                                    best_result = {
                                        "price": gt,
                                        "source": "fiscal_scprs_summary",
                                        "date": c.get("start_date", ""),
                                        "confidence": "low",
                                        "vendor": c.get("supplier_name", ""),
                                        "po_number": c.get("po_number", ""),
                                        "department": c.get("dept", ""),
                                        "first_item": c.get("first_item", ""),
                                        "note": "PO total (not unit price)",
                                        "searched": search_terms,
                                    }
                                    break
                            
                        except Exception as _se:
                            log.warning("SCPRS search '%s': %s", term, _se)
                            # Try to recover session
                            try:
                                session.init_session()
                            except Exception as _e:
                                log.debug("Suppressed: %s", _e)
                    
                    if best_result:
                        results[idx] = best_result
                        # Cache for future lookups
                        if best_result.get("price") and best_result.get("source") != "fiscal_scprs_summary":
                            try:
                                save_price(
                                    item_number=item_num or "",
                                    description=desc or "",
                                    price=best_result["price"],
                                    vendor=best_result.get("vendor", ""),
                                    unit_price=best_result.get("unit_price"),
                                    quantity=best_result.get("quantity"),
                                    po_number=best_result.get("po_number", ""),
                                    source="fiscal_scprs"
                                )
                            except Exception as _e:
                                log.debug("Suppressed: %s", _e)
                    else:
                        results[idx] = {
                            "price": None,
                            "note": "No SCPRS data found",
                            "item_number": item_num,
                            "description": (desc or "")[:80],
                            "searched": search_terms,
                        }
            else:
                errors.append("SCPRS session init failed")
                for idx, item_num, desc, search_terms in items_needing_search:
                    results[idx] = {
                        "price": None,
                        "error": "SCPRS session init failed",
                        "item_number": item_num,
                        "searched": search_terms,
                    }
    
    except Exception as e:
        import traceback
        errors.append(str(e))
        log.error("SCPRS batch lookup: %s", e, exc_info=True)
    
    # Fill any remaining None slots
    for i in range(len(results)):
        if results[i] is None:
            results[i] = {"price": None, "note": "Lookup skipped"}
    
    # Auto-ingest results to catalog + KB
    for i, res in enumerate(results):
        if not res or not res.get("price"):
            continue
        item = items[i] if i < len(items) else {}
        item_num = item.get("item_number", "")
        desc = item.get("description", "")
        
        if PRICING_ORACLE_AVAILABLE:
            try:
                ingest_scprs_result(
                    po_number=res.get("po_number", ""),
                    item_number=item_num, description=desc,
                    unit_price=res["price"], quantity=1,
                    supplier=res.get("vendor", ""),
                    department=res.get("department", ""),
                    award_date=res.get("date", ""),
                    source=res.get("source", "scprs_live"),
                )
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
        
        try:
            from src.core.db import record_price as _rp_scprs
            _rp_scprs(
                description=desc, unit_price=res["price"],
                source="scprs_live", part_number=item_num,
                source_id=res.get("po_number", ""),
                agency=res.get("department", ""),
                notes=f"SCPRS vendor: {res.get('vendor', '')}"
            )
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        
        try:
            from src.agents.product_catalog import add_to_catalog, init_catalog_db
            init_catalog_db()
            add_to_catalog(
                description=desc, part_number=item_num,
                cost=float(res["price"]), sell_price=0,
                source="scprs_live",
                supplier_name=res.get("vendor", ""),
            )
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    
    found = sum(1 for r in results if r and r.get("price"))
    log.info("SCPRS batch: %d/%d prices found for RFQ %s", found, len(items), rid)
    return jsonify({"results": results, "errors": errors if errors else None})


@bp.route("/api/scprs-test")
@auth_required
@safe_route
def api_scprs_test():
    """SCPRS search test — ?q=stryker+xpr"""
    q = (request.args.get("q", "") or "").strip()
    if not q:
        return jsonify({"error": "Missing required parameter: q"}), 400
    try:
        from src.agents.scprs_lookup import test_search
        return jsonify(test_search(q))
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/scprs-bulk/<rid>")
@auth_required
@safe_route
def api_scprs_bulk(rid):
    """Bulk SCPRS search — one session, searches each RFQ item, returns summary table.
    
    Hit: /api/scprs-bulk/{rfq_id}
    Returns clean JSON with per-item SCPRS results (PO#, vendor, total, date).
    """
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"error": "RFQ not found"})
    
    items = r.get("line_items", [])
    if not items:
        return jsonify({"error": "No line items"})
    
    try:
        from src.agents.scprs_lookup import _get_session, _build_search_terms
        import time
        
        session = _get_session()
        if not session.initialized:
            if not session.init_session():
                return jsonify({"error": "SCPRS session init failed"})
        
        results = []
        for i, item in enumerate(items):
            pn = item.get("item_number", "")
            desc = item.get("description", "")
            cost = item.get("supplier_cost", 0)
            terms = _build_search_terms(pn, desc)
            
            # Search with first term (most specific)
            search_results = []
            searched_term = ""
            for term in terms[:2]:
                try:
                    search_results = session.search(description=term)
                    searched_term = term
                    if search_results:
                        break
                    time.sleep(0.3)
                except Exception as e:
                    log.debug("Bulk SCPRS search '%s': %s", term, e)
                    try:
                        session.init_session()
                    except Exception as _e:
                        log.debug("Suppressed: %s", _e)
            
            # Extract best result
            best = None
            for sr in sorted(search_results, 
                           key=lambda x: x.get("start_date_parsed") or __import__("datetime").datetime.min,
                           reverse=True)[:3]:
                gt = sr.get("grand_total_num", 0)
                if gt and gt > 0:
                    best = {
                        "po_number": sr.get("po_number", ""),
                        "vendor": sr.get("supplier_name", ""),
                        "grand_total": sr.get("grand_total", ""),
                        "date": sr.get("start_date", ""),
                        "dept": sr.get("dept", ""),
                        "first_item": sr.get("first_item", ""),
                        "acq_method": sr.get("acq_method", ""),
                    }
                    break
            
            results.append({
                "line": i + 1,
                "part_number": pn,
                "description": (desc or "")[:50],
                "echelon_cost": cost,
                "searched": searched_term,
                "scprs_results_count": len(search_results),
                "best_match": best,
            })
            time.sleep(0.5)  # Be gentle with FI$Cal
        
        return jsonify({
            "rfq": rid,
            "items": len(items),
            "results": results,
        })
    
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/scprs-raw")
@auth_required
@safe_route
def api_scprs_raw():
    """Raw SCPRS debug — shows HTML field IDs found in search results."""
    q = (request.args.get("q", "") or "").strip()
    if not q:
        return jsonify({"error": "Missing required parameter: q"}), 400
    try:
        from src.agents.scprs_lookup import _get_session, _discover_grid_ids, SCPRS_SEARCH_URL, SEARCH_BUTTON, ALL_SEARCH_FIELDS, FIELD_DESCRIPTION
        from bs4 import BeautifulSoup
        
        session = _get_session()
        if not session.initialized:
            session.init_session()
        
        # Load search page
        page = session._load_page(2)
        icsid = session._extract_icsid(page)
        if icsid: session.icsid = icsid
        
        # POST search
        sv = {f: "" for f in ALL_SEARCH_FIELDS}
        sv[FIELD_DESCRIPTION] = q
        fd = session._build_form_data(page, SEARCH_BUTTON, sv)
        r = session.session.post(SCPRS_SEARCH_URL, data=fd, timeout=30)
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
        
        import re
        count = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', html)
        discovered = _discover_grid_ids(soup, "ZZ_SCPR_RD_DVW")
        
        # Sample row 0 values
        row0 = {}
        for suffix in discovered:
            eid = f"ZZ_SCPR_RD_DVW_{suffix}$0"
            el = soup.find(id=eid)
            val = el.get_text(strip=True) if el else None
            row0[eid] = val
        
        # Also check for link-style elements
        link0 = soup.find("a", id="ZZ_SCPR_RD_DVW_CRDMEM_ACCT_NBR$0")
        
        # Broad scan: find ALL element IDs ending in $0
        all_row0_ids = {}
        for el in soup.find_all(id=re.compile(r'\$0$')):
            eid = el.get('id', '')
            if eid and ('SCPR' in eid or 'DVW' in eid or 'RSLT' in eid):
                all_row0_ids[eid] = el.get_text(strip=True)[:80]
        
        # Also discover with correct prefix
        discovered2 = _discover_grid_ids(soup, "ZZ_SCPR_RSLT_VW")
        
        # Table class scan
        tables = [(t.get("class",""), t.get("id",""), len(t.find_all("tr")))
                  for t in soup.find_all("table") if t.get("class")]
        grid_tables = [t for t in tables if "PSLEVEL1GRID" in str(t[0])]
        
        return jsonify({
            "query": q, "status": r.status_code, "size": len(html),
            "result_count": count.group(0) if count else "none",
            "id_discovered_RD_DVW": list(discovered.keys()),
            "id_discovered_RSLT_VW": list(discovered2.keys()),
            "all_row0_ids": all_row0_ids,
            "row0_values": row0,
            "po_link_found": link0.get_text(strip=True) if link0 else None,
            "grid_tables": grid_tables[:5],
        })
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/status")
@auth_required
@safe_route
def api_status():
    # Filter POLL_STATUS to only JSON-serializable values
    safe_poll = {k: v for k, v in POLL_STATUS.items()
                 if isinstance(v, (str, int, float, bool, list, dict, type(None)))}
    return jsonify({
        "poll": safe_poll,
        "scprs_db": get_price_db_stats(),
        "rfqs": len(load_rfqs()),
    })


@bp.route("/api/poll-now")
@auth_required
@safe_route
def api_poll_now():
    """Manual trigger: check email inbox right now."""
    try:
        imported = _safe_do_poll_check()
        return jsonify({
            "ok": True,
            "found": len(imported),
            "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
            "last_check": POLL_STATUS.get("last_check"),
            "error": POLL_STATUS.get("error"),
            "diag": POLL_STATUS.get("_diag", {}),
        })
    except Exception as e:
        import traceback as _tb
        return jsonify({"ok": False, "found": 0, "error": str(e), "traceback": _tb.format_exc()})


@bp.route("/api/poll/reset-processed", methods=["GET", "POST"])
@auth_required
@safe_route
def api_poll_reset_processed():
    """Atomic: clear processed UIDs → immediately re-poll → return results.
    Prevents background thread from re-saving UIDs between reset and poll.
    """
    global _shared_poller
    
    # Step 1: Delete the processed emails file
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    old_count = 0
    try:
        if os.path.exists(proc_file):
            import json as _json2
            with open(proc_file) as f:
                old_count = len(_json2.load(f))
            os.remove(proc_file)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    
    # Step 1b: Clear SQLite processed_emails + fingerprints (prevents recovery)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM processed_emails")
            try:
                conn.execute("DELETE FROM email_fingerprints")
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    
    # Step 2: Kill the shared poller so a fresh one gets created
    _shared_poller = None
    
    # Step 3: Immediately run poll (creates new poller with empty processed set)
    try:
        imported = _safe_do_poll_check()
        return jsonify({
            "ok": True,
            "cleared": old_count,
            "found": len(imported),
            "items": [{"id": r.get("id","?"), "sol": r.get("solicitation_number","?"), 
                       "subject": r.get("email_subject", r.get("subject",""))[:60]}
                      for r in imported],
            "poll_diag": POLL_STATUS.get("_diag", {}),
        })
    except Exception as e:
        return jsonify({"ok": False, "cleared": old_count, "error": str(e)})


@bp.route("/api/diag/inbox-peek")
@auth_required
@safe_route
def api_inbox_peek():
    """Show recent inbox messages + filter decisions via Gmail API — NO processing."""
    try:
        from src.core import gmail_api
        if not gmail_api.is_configured():
            return jsonify({"error": "Gmail API not configured"}), 503

        service = gmail_api.get_service("sales")
        from datetime import datetime, timedelta
        since = (datetime.now() - timedelta(days=7)).strftime("%Y/%m/%d")
        query = f"in:inbox after:{since}"
        ids = gmail_api.list_message_ids(service, query=query, max_results=100)

        # Load processed IDs from JSON
        proc_file = os.path.join(DATA_DIR, "processed_emails.json")
        processed_json = set()
        try:
            if os.path.exists(proc_file):
                import json as _j
                with open(proc_file) as f:
                    processed_json = set(str(x) for x in _j.load(f))
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

        # Load processed IDs from SQLite
        processed_db = set()
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute("SELECT uid FROM processed_emails").fetchall()
                processed_db = set(str(r[0]) for r in rows)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

        emails = []
        for msg_id in ids[-10:]:
            meta = gmail_api.get_message_metadata(service, msg_id)
            subj = meta.get("subject", "")
            sender = meta.get("from", "")
            if "<" in sender:
                sender_email = sender.split("<")[1].split(">")[0].lower()
            else:
                sender_email = sender.lower().strip()

            our_domains = ["reytechinc.com", "reytech.com"]
            is_self = any(sender_email.endswith(f"@{d}") for d in our_domains)
            is_fwd_subj = any(subj.lower().strip().startswith(p) for p in ["fwd:", "fw:"])
            in_json = msg_id in processed_json
            in_db = msg_id in processed_db

            emails.append({
                "uid": msg_id,
                "subject": subj[:80],
                "sender": sender_email,
                "is_self": is_self,
                "is_fwd": is_fwd_subj,
                "in_json": in_json,
                "in_db": in_db,
                "blocked": in_json or in_db,
                "date": meta.get("date", "")[:30],
            })

        return jsonify({
            "ok": True,
            "total_in_window": len(ids),
            "processed_json": sorted(list(processed_json))[:20],
            "processed_db": sorted(list(processed_db))[:20],
            "emails": emails,
        })
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/diag/nuke-and-poll")
@auth_required
@safe_route
def api_nuke_and_poll():
    """Nuclear option: clear ALL dedup layers and re-poll. GET to bypass CSRF."""
    try:
        _DATA_DIR = DATA_DIR
    except NameError:
        from src.core.paths import DATA_DIR as _DATA_DIR
    # CRITICAL: _shared_poller lives in dashboard.py's module globals.
    # `global _shared_poller` here would reference routes_pricecheck's copy,
    # NOT the one that _safe_do_poll_check() reads. Must access directly.
    import src.api.dashboard as _dash
    cleared = {}
    
    # 0. PAUSE background poller to prevent race condition
    POLL_STATUS["paused"] = True
    import time as _time
    _time.sleep(0.5)  # Let any in-flight poll finish
    
    # 0b. Clear in-memory processed set on existing poller
    _old_poller = getattr(_dash, '_shared_poller', None)
    if _old_poller and hasattr(_old_poller, '_processed'):
        cleared["in_memory_cleared"] = len(_old_poller._processed)
        _old_poller._processed.clear()
        try:
            _old_poller._save_processed()
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    
    # Kill dashboard's poller (this is the one _safe_do_poll_check uses)
    _dash._shared_poller = None
    
    # 1. Clear JSON processed file(s) — both inboxes
    for _pf_name in ("processed_emails.json", "processed_emails_mike.json"):
        _pf = os.path.join(_DATA_DIR, _pf_name)
        try:
            if os.path.exists(_pf):
                with open(_pf) as f:
                    old = json.load(f)
                cleared[_pf_name] = len(old) if isinstance(old, list) else 0
            else:
                cleared[_pf_name] = "not found"
            # Write empty list (not delete — prevents re-creation race)
            with open(_pf, "w") as f:
                json.dump([], f)
        except Exception as e:
            cleared[f"{_pf_name}_error"] = str(e)
    
    # 2. Clear SQLite processed_emails
    try:
        from src.core.db import get_db
        with get_db() as conn:
            n = conn.execute("SELECT COUNT(*) FROM processed_emails").fetchone()[0]
            conn.execute("DELETE FROM processed_emails")
            cleared["db_processed"] = n
    except Exception as e:
        cleared["db_error"] = str(e)
    
    # 3. Clear SQLite email_fingerprints
    try:
        from src.core.db import get_db
        with get_db() as conn:
            n = conn.execute("SELECT COUNT(*) FROM email_fingerprints").fetchone()[0]
            conn.execute("DELETE FROM email_fingerprints")
            cleared["db_fingerprints"] = n
    except Exception as e:
        cleared["fp_error"] = str(e)
    
    # 4. Poller already killed above via _dash._shared_poller = None
    cleared["poller"] = "reset"
    
    # 5. Re-poll (with background poller still paused)
    try:
        imported = _safe_do_poll_check()
        # 6. Unpause background poller
        POLL_STATUS["paused"] = False
        return jsonify({
            "ok": True,
            "cleared": cleared,
            "found": len(imported),
            "rfqs": [{"id": r.get("id","?"), "subject": r.get("subject","")[:60]} for r in imported],
            "traces": POLL_STATUS.get("_email_traces", [])[-30:],
            "sales_diag": POLL_STATUS.get("_diag", {}),
            "mike_diag": POLL_STATUS.get("_mike_diag", {}),
        })
    except Exception as e:
        POLL_STATUS["paused"] = False
        return jsonify({"ok": False, "cleared": cleared, "error": str(e)})


@bp.route("/api/diag/find-rfq")
@auth_required
@safe_route
def api_diag_find_rfq():
    """Search all RFQs and PCs for a keyword (sol number, subject, sender).
    Usage: /api/diag/find-rfq?q=10840486
    """
    q = request.args.get("q", "").lower()
    if not q:
        return jsonify({"error": "Pass ?q=keyword"})
    
    rfqs = load_rfqs()
    pcs = _load_price_checks()
    
    rfq_hits = []
    for rid, r in rfqs.items():
        searchable = json.dumps(r, default=str).lower()
        if q in searchable:
            rfq_hits.append({
                "id": rid,
                "sol": r.get("solicitation_number", "?"),
                "status": r.get("status", "?"),
                "subject": r.get("email_subject", "")[:80],
                "sender": r.get("email_sender", r.get("requestor_email", "")),
                "email_uid": r.get("email_uid", "")[:20],
                "created_at": r.get("created_at", ""),
            })
    
    pc_hits = []
    for pid, p in pcs.items():
        searchable = json.dumps(p, default=str).lower()
        if q in searchable:
            pc_hits.append({
                "id": pid,
                "pc_number": p.get("pc_number", "?"),
                "status": p.get("status", "?"),
                "institution": p.get("institution", ""),
                "email_uid": p.get("email_uid", "")[:20],
            })
    
    return jsonify({
        "query": q,
        "rfq_matches": rfq_hits,
        "pc_matches": pc_hits,
        "total_rfqs": len(rfqs),
        "total_pcs": len(pcs),
        "all_rfq_uids": {rid: {"uid": r.get("email_uid", ""), "sol": r.get("solicitation_number", "?")} for rid, r in rfqs.items()},
    })


@bp.route("/api/diag/rfq-inspect/<rid>")
@auth_required
@safe_route
def api_diag_rfq_inspect(rid):
    """Inspect RFQ data + find matching PCs for debugging."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found", "available_ids": list(rfqs.keys())[:20]})
    items = r.get("line_items", [])
    item_summary = []
    for i, item in enumerate(items):
        item_summary.append({
            "idx": i + 1,
            "qty": item.get("qty", ""),
            "description": (item.get("description", "") or "")[:60],
            "item_number": item.get("item_number", "") or item.get("part_number", ""),
            "supplier_cost": item.get("supplier_cost", "") or item.get("vendor_cost", ""),
            "price_per_unit": item.get("price_per_unit", ""),
            "item_link": (item.get("item_link", "") or "")[:50],
            "source_pc": item.get("source_pc", "") or item.get("_from_pc", ""),
        })
    # Find matching PCs
    pcs = _load_price_checks()
    matching_pcs = []
    rfq_inst = (r.get("delivery_location", "") or r.get("institution", "") or r.get("institution_name", "") or "").lower()
    rfq_sol = (r.get("solicitation_number", "") or "").strip()
    for pid, pc in pcs.items():
        pc_inst = (pc.get("institution", "") or "").lower()
        pc_sol = (pc.get("pc_number", "") or "").strip()
        match_reasons = []
        if rfq_sol and pc_sol and rfq_sol == pc_sol:
            match_reasons.append("sol_match")
        if pc_inst and rfq_inst and (pc_inst in rfq_inst or rfq_inst in pc_inst):
            match_reasons.append("inst_match")
        if match_reasons or pc.get("email_uid") == r.get("email_uid"):
            matching_pcs.append({
                "pc_id": pid,
                "pc_number": pc.get("pc_number", ""),
                "institution": pc.get("institution", ""),
                "items": len(pc.get("items", [])),
                "status": pc.get("status", ""),
                "created": pc.get("created_at", "")[:10],
                "match_reasons": match_reasons,
            })
    return jsonify({
        "ok": True,
        "rfq": {
            "id": rid,
            "solicitation_number": r.get("solicitation_number", ""),
            "institution": r.get("delivery_location", "") or r.get("institution", ""),
            "agency": r.get("agency", ""),
            "source": r.get("source", ""),
            "email_subject": r.get("email_subject", ""),
            "email_sender": r.get("email_sender", ""),
            "form_type": r.get("form_type", ""),
            "linked_pc": r.get("linked_pc_id", ""),
            "status": r.get("status", ""),
            "created": r.get("created_at", ""),
            "item_count": len(items),
            "templates": list(r.get("templates", {}).keys()),
        },
        "items": item_summary,
        "matching_pcs": matching_pcs,
        "body_preview": (r.get("body_text", "") or "")[:500],
    })


@bp.route("/api/diag")
@auth_required
@safe_route
def api_diag():
    """Diagnostic endpoint — shows email config, connection test, and inbox status."""
    import traceback
    try:
        return _api_diag_inner()
    except Exception as e:
        log.error("Diagnostics error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500

def _api_diag_inner():
    email_cfg = CONFIG.get("email", {})
    addr = email_cfg.get("email", "NOT SET")
    has_pw = bool(email_cfg.get("email_password"))

    diag = {
        "config": {
            "email_address": addr,
            "has_password": has_pw,
            "password_length": len(email_cfg.get("email_password", "")),
            "backend": "gmail_api",
        },
        "env_vars": {
            "GMAIL_ADDRESS_set": bool(os.environ.get("GMAIL_ADDRESS")),
            "GMAIL_OAUTH_CLIENT_ID_set": bool(os.environ.get("GMAIL_OAUTH_CLIENT_ID")),
            "GMAIL_OAUTH_CLIENT_SECRET_set": bool(os.environ.get("GMAIL_OAUTH_CLIENT_SECRET")),
            "GMAIL_OAUTH_REFRESH_TOKEN_set": bool(os.environ.get("GMAIL_OAUTH_REFRESH_TOKEN")),
            "GMAIL_ADDRESS_value": os.environ.get("GMAIL_ADDRESS", "NOT SET"),
        },
        # Filter out underscore-prefixed keys — they hold live objects like
        # the EmailPoller instance (_poller_instance) which aren't JSON
        # serializable. UI walkthrough on 2026-04-17 caught a 500 firing on
        # every home-page load because of this.
        "poll_status": {k: v for k, v in POLL_STATUS.items() if not k.startswith("_")},
        "connection_test": None,
        "inbox_test": None,
    }

    # Test Gmail API connection
    try:
        from src.core import gmail_api
        if not gmail_api.is_configured():
            diag["connection_test"] = "Gmail API not configured (missing OAuth env vars)"
        else:
            service = gmail_api.get_service("sales")
            diag["connection_test"] = f"Gmail API authenticated as {addr} OK"
            try:
                since_date = (datetime.now() - timedelta(days=3)).strftime("%Y/%m/%d")
                recent_ids = gmail_api.list_message_ids(
                    service, query=f"in:inbox after:{since_date}", max_results=100
                )
                recent_count = len(recent_ids)

                proc_file = os.path.join(DATA_DIR, "processed_emails.json")
                processed_uids = set()
                if os.path.exists(proc_file):
                    try:
                        with open(proc_file) as pf:
                            processed_uids = set(str(x) for x in json.load(pf))
                    except Exception as e:
                        log.debug("Suppressed: %s", e)

                new_to_process = [m for m in recent_ids if m not in processed_uids]

                diag["inbox_test"] = {
                    "recent_3_days": recent_count,
                    "already_processed": recent_count - len(new_to_process),
                    "new_to_process": len(new_to_process),
                }

                if new_to_process:
                    subjects = []
                    for msg_id in new_to_process[:5]:
                        meta = gmail_api.get_message_metadata(service, msg_id)
                        subjects.append(
                            f"Subject: {meta.get('subject', '')}\nFrom: {meta.get('from', '')}"
                        )
                    diag["inbox_test"]["new_email_subjects"] = subjects
            except Exception as e:
                diag["inbox_test"] = f"Gmail API list/metadata failed: {e}"
    except Exception as e:
        diag["connection_test"] = f"Gmail API connect failed: {e}"
        log.error("Gmail API diag test failed: %s", e, exc_info=True)
    
    # Check processed emails file
    proc_file = email_cfg.get("processed_file", os.path.join(DATA_DIR, "processed_emails.json"))
    if os.path.exists(proc_file):
        try:
            with open(proc_file) as f:
                processed = json.load(f)
            diag["processed_emails"] = {"count": len(processed), "ids": processed[-10:] if isinstance(processed, list) else list(processed)[:10]}
        except Exception as e:
            log.debug("Suppressed: %s", e)
            diag["processed_emails"] = "corrupt file"
    else:
        diag["processed_emails"] = "file not found"
    
    # SCPRS diagnostics
    diag["scprs"] = {
        "db_stats": get_price_db_stats(),
        "db_exists": os.path.exists(os.path.join(BASE_DIR, "data", "scprs_prices.json")),
    }
    try:
        from src.agents.scprs_lookup import test_connection
        import threading
        result = [False, "timeout"]
        def _test():
            try:
                result[0], result[1] = test_connection()
            except Exception as ex:
                result[1] = str(ex)
        t = threading.Thread(target=_test, daemon=True)
        t.start()
        t.join(timeout=15)  # Max 15 seconds for connectivity test (may need 2-3 loads)
        diag["scprs"]["fiscal_reachable"] = result[0]
        diag["scprs"]["fiscal_status"] = result[1]
    except Exception as e:
        diag["scprs"]["fiscal_reachable"] = False
        diag["scprs"]["fiscal_error"] = str(e)
    
    return jsonify(diag)


@bp.route("/api/reset-processed")
@auth_required
@safe_route
def api_reset_processed():
    """Clear the processed emails list so all recent emails get re-scanned."""
    global _shared_poller
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if os.path.exists(proc_file):
        os.remove(proc_file)
    _shared_poller = None  # Force new poller instance
    return jsonify({"ok": True, "message": "Processed emails list cleared. Hit Check Now to re-scan."})


