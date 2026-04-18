"""Simple Submit — fast-path quoting for PCs and RFQs.

Routes:
    GET  /simple-submit/pc/<pcid>   — Simple submit page for a Price Check
    GET  /simple-submit/rfq/<rid>   — Simple submit page for an RFQ
    POST /api/simple-submit/generate — Generate filled 704 + quote PDF
    POST /api/simple-submit/download-bundle — Zip all artifacts for manual Gmail send

Auto-fills 90% (Reytech vendor info, buyer info from parser, items from PDF).
Operator reviews pricing, clicks generate, downloads bundle, sends via Gmail.
This is the permanent fallback — even after the full rebuild ships, this button exists.
"""
import copy
import io
import json
import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime, timedelta, timezone

from flask import jsonify, request, send_file

from src.api.shared import bp, auth_required

log = logging.getLogger(__name__)

# PST timezone offset
_PST = timezone(timedelta(hours=-8))


def _pst_now():
    return datetime.now(_PST)


def _load_doc(doc_type, doc_id):
    """Load a PC or RFQ by ID. Returns (doc_dict, error_msg)."""
    if doc_type == "pc":
        from src.api.data_layer import _load_price_checks
        docs = _load_price_checks()
    elif doc_type == "rfq":
        from src.api.data_layer import load_rfqs
        docs = load_rfqs()
    else:
        return None, f"Unknown doc_type: {doc_type}"

    doc = docs.get(doc_id)
    if not doc:
        return None, f"{doc_type.upper()} {doc_id} not found"
    return copy.deepcopy(doc), None


def _get_items(doc):
    """Extract items list from a PC or RFQ dict."""
    return doc.get("line_items") or doc.get("items") or []


def _get_header(doc):
    """Extract header/buyer info from a PC or RFQ dict."""
    h = doc.get("header") or {}
    return {
        "institution": h.get("institution") or doc.get("institution") or "",
        "agency": h.get("agency") or doc.get("agency_name") or doc.get("agency") or "",
        "requestor": h.get("requestor") or doc.get("requestor") or "",
        "phone": h.get("phone") or doc.get("phone") or "",
        "zip_code": h.get("zip_code") or doc.get("delivery_zip") or "",
        "ship_to": h.get("ship_to") or doc.get("ship_to") or doc.get("delivery_location") or "",
        "due_date": h.get("due_date") or doc.get("due_date") or "",
        "due_time": h.get("due_time") or doc.get("due_time") or "",
        "notes": h.get("notes") or doc.get("notes") or "",
        "pc_number": h.get("pc_number") or doc.get("solicitation_number") or doc.get("rfq_number") or "",
    }


def _get_reytech_info():
    """Load Reytech vendor info (same source as fill_ams704)."""
    try:
        from src.forms.price_check import REYTECH_INFO
        return dict(REYTECH_INFO)
    except Exception:
        return {
            "company_name": "Reytech Inc.",
            "representative": "Michael Guadan",
            "address": "30 Carnoustie Way, Trabuco Canyon, CA 92679",
            "phone": "949-229-1575",
            "email": "sales@reytechinc.com",
            "sb_mb": "2002605",
            "dvbe": "2002605",
            "discount": "Included",
            "delivery": "7-14 business days",
        }


def _build_simple_context(doc_type, doc_id, doc):
    """Build template context for simple-submit page."""
    items = _get_items(doc)
    header = _get_header(doc)
    reytech = _get_reytech_info()

    enriched_items = []
    for i, item in enumerate(items):
        pricing = item.get("pricing") or {}
        enriched_items.append({
            "line_no": i + 1,
            "description": item.get("description") or "",
            "qty": item.get("qty") or item.get("quantity") or 1,
            "uom": item.get("uom") or item.get("unit") or "EA",
            "part_number": item.get("part_number") or item.get("mfg_number") or "",
            "unit_cost": pricing.get("unit_cost") or item.get("unit_cost") or 0,
            "markup_pct": pricing.get("markup_pct") or item.get("markup_pct") or 35,
            "unit_price": pricing.get("unit_price") or item.get("unit_price") or 0,
            "extension": pricing.get("extension") or item.get("extension") or 0,
            "source": pricing.get("source") or item.get("price_source") or "",
            "amazon_price": pricing.get("amazon_price") or item.get("amazon_price") or 0,
            "scprs_price": pricing.get("scprs_price") or item.get("scprs_price") or 0,
            "catalog_cost": pricing.get("catalog_cost") or item.get("catalog_cost") or 0,
        })

    source_pdf = doc.get("source_pdf") or doc.get("pdf_path") or ""
    templates = doc.get("templates") or {}

    return {
        "doc_type": doc_type,
        "doc_id": doc_id,
        "header": header,
        "items": enriched_items,
        "reytech": reytech,
        "source_pdf": source_pdf,
        "templates": templates,
        "item_count": len(enriched_items),
        "has_pricing": any(it["unit_cost"] > 0 for it in enriched_items),
        "created_at": doc.get("created_at") or "",
        "status": doc.get("status") or "draft",
    }


# ── Page Routes ──────────────────────────────────────────────────────────────

@bp.route("/simple-submit/pc/<pcid>")
@auth_required
def simple_submit_pc(pcid):
    """Simple submit page for a Price Check."""
    from src.api.render import render_page
    doc, err = _load_doc("pc", pcid)
    if err:
        return render_page("simple_submit.html", active_page="Price Checks",
                           error=err, doc_type="pc", doc_id=pcid,
                           header={}, items=[], reytech=_get_reytech_info(),
                           templates={}, item_count=0, has_pricing=False,
                           source_pdf="", created_at="", status="")
    ctx = _build_simple_context("pc", pcid, doc)
    return render_page("simple_submit.html", active_page="Price Checks", **ctx)


@bp.route("/simple-submit/rfq/<rid>")
@auth_required
def simple_submit_rfq(rid):
    """Simple submit page for an RFQ."""
    from src.api.render import render_page
    doc, err = _load_doc("rfq", rid)
    if err:
        return render_page("simple_submit.html", active_page="RFQs",
                           error=err, doc_type="rfq", doc_id=rid,
                           header={}, items=[], reytech=_get_reytech_info(),
                           templates={}, item_count=0, has_pricing=False,
                           source_pdf="", created_at="", status="")
    ctx = _build_simple_context("rfq", rid, doc)
    return render_page("simple_submit.html", active_page="RFQs", **ctx)


# ── API Routes ───────────────────────────────────────────────────────────────

@bp.route("/api/simple-submit/generate", methods=["POST"])
@auth_required
def api_simple_submit_generate():
    """Generate filled 704 + quote PDF from simple-submit pricing review."""
    try:
        data = request.get_json(force=True)
        doc_type = data.get("doc_type", "")
        doc_id = data.get("doc_id", "")
        items_data = data.get("items", [])
        markup_pct = float(data.get("default_markup", 35))
        tax_rate = float(data.get("tax_rate", 0))
        notes = data.get("notes", "")
        delivery = data.get("delivery", "")

        doc, err = _load_doc(doc_type, doc_id)
        if err:
            return jsonify({"ok": False, "error": err}), 404

        # Update items with operator-reviewed pricing
        existing_items = _get_items(doc)
        for submitted in items_data:
            idx = int(submitted.get("line_no", 0)) - 1
            if 0 <= idx < len(existing_items):
                item = existing_items[idx]
                cost = float(submitted.get("unit_cost", 0))
                mkp = float(submitted.get("markup_pct", markup_pct))
                price = round(cost * (1 + mkp / 100), 2) if cost > 0 else 0

                if "pricing" not in item:
                    item["pricing"] = {}
                item["pricing"]["unit_cost"] = cost
                item["pricing"]["markup_pct"] = mkp
                item["pricing"]["unit_price"] = price
                qty = float(item.get("qty") or item.get("quantity") or 1)
                item["pricing"]["extension"] = round(price * qty, 2)
                item["unit_cost"] = cost
                item["markup_pct"] = mkp
                item["unit_price"] = price
                item["extension"] = round(price * qty, 2)

        # Save updated pricing back
        if doc_type == "pc":
            doc["line_items"] = existing_items
            from src.api.data_layer import _load_price_checks, _save_price_checks
            pcs = _load_price_checks()
            pcs[doc_id] = doc
            _save_price_checks(pcs)
        else:
            doc["line_items"] = existing_items
            doc["items"] = existing_items
            from src.api.data_layer import load_rfqs, save_rfqs
            rfqs = load_rfqs()
            rfqs[doc_id] = doc
            save_rfqs(rfqs)

        results = {"ok": True, "files": []}

        # Generate filled 704 via the unified quote engine (Quote model + profile + QA)
        output_dir = os.path.join("output", "simple_submit", doc_id)
        os.makedirs(output_dir, exist_ok=True)
        output_704 = os.path.join(output_dir, f"704_filled_{doc_id}.pdf")

        try:
            from src.core.quote_engine import draft, ingest

            # Operator-reviewed pricing was already written to doc at lines 194-212;
            # ingest() picks it up via Quote.from_legacy_dict.
            quote, _warnings = ingest(doc, doc_type=doc_type)
            draft_result = draft(quote)

            with open(output_704, "wb") as f:
                f.write(draft_result.pdf_bytes)

            header = _get_header(doc)
            pc_num = header.get("pc_number") or doc_id[:12]
            safe_pc = re.sub(r'[^\w\s\-.]', '', pc_num).strip().replace(' ', '_')
            results["files"].append({
                "type": "704",
                "path": output_704,
                "name": f"{safe_pc}_Reytech.pdf",
            })
            if not draft_result.ok:
                results["704_qa_warnings"] = draft_result.qa_report.summary
            log.info(
                "Simple submit: quote_engine.draft produced %d bytes for %s (profile=%s, QA=%s)",
                len(draft_result.pdf_bytes), doc_id,
                draft_result.profile_id, draft_result.qa_report.summary,
            )
        except Exception as e:
            log.error("Simple submit quote_engine error: %s — falling back to legacy", e, exc_info=True)
            # Fallback to legacy fill_ams704 (kept until shadow-mode parity is confirmed)
            try:
                from src.forms.price_check import fill_ams704
                blank_704 = os.path.join("tests", "fixtures", "ams_704_blank.pdf")
                if not os.path.exists(blank_704):
                    blank_704 = os.path.join("src", "forms", "templates", "ams_704_blank.pdf")
                fill_result = fill_ams704(
                    source_pdf=blank_704, parsed_pc=doc, output_pdf=output_704,
                    tax_rate=tax_rate, custom_notes=notes, delivery_option=delivery,
                )
                if fill_result.get("ok"):
                    results["files"].append({
                        "type": "704",
                        "path": output_704,
                        "name": f"AMS_704_{doc_id}.pdf",
                    })
                    results["704_fallback"] = True
                else:
                    results["704_error"] = fill_result.get("error", "Unknown fill error")
            except Exception as e2:
                log.error("Simple submit legacy fallback also failed: %s", e2)
                results["704_error"] = str(e)

        # Generate Reytech quote PDF
        try:
            from src.forms.quote_generator import generate_quote
            header = _get_header(doc)
            quote_data = {
                "institution": header["institution"],
                "ship_to_name": header["ship_to"],
                "ship_to_address": header["ship_to"],
                "rfq_number": header["pc_number"],
                "solicitation_number": header["pc_number"],
                "line_items": [
                    {
                        "line_number": it.get("line_no", i + 1),
                        "part_number": it.get("part_number") or it.get("mfg_number") or "",
                        "qty": it.get("qty") or it.get("quantity") or 1,
                        "uom": it.get("uom") or it.get("unit") or "EA",
                        "description": it.get("description") or "",
                        "unit_price": (it.get("pricing") or {}).get("unit_price")
                                      or it.get("unit_price") or 0,
                    }
                    for i, it in enumerate(existing_items)
                ],
                "delivery_location": header["ship_to"] or header["zip_code"],
            }

            output_quote = os.path.join(output_dir, f"Quote_{doc_id}.pdf")
            quote_result = generate_quote(
                quote_data=quote_data,
                output_path=output_quote,
                agency=header.get("agency"),
                tax_rate=tax_rate,
                notes=notes,
            )
            if quote_result.get("ok"):
                results["files"].append({
                    "type": "quote",
                    "path": quote_result.get("output", output_quote),
                    "name": f"Reytech_Quote_{quote_result.get('quote_number', doc_id)}.pdf",
                    "quote_number": quote_result.get("quote_number", ""),
                })
            else:
                results["quote_error"] = quote_result.get("error", "Unknown quote error")
        except Exception as e:
            log.error("Simple submit quote generation error: %s", e, exc_info=True)
            results["quote_error"] = str(e)

        return jsonify(results)

    except Exception as e:
        log.error("Simple submit generate error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/simple-submit/download-bundle", methods=["POST"])
@auth_required
def api_simple_submit_download_bundle():
    """Zip all generated files for download + manual Gmail attach."""
    try:
        data = request.get_json(force=True)
        files = data.get("files", [])

        if not files:
            return jsonify({"ok": False, "error": "No files to bundle"}), 400

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in files:
                path = f.get("path", "")
                name = f.get("name", os.path.basename(path))
                if os.path.exists(path):
                    zf.write(path, name)

        buf.seek(0)
        return send_file(
            buf,
            mimetype="application/zip",
            as_attachment=True,
            download_name=f"quote_bundle_{data.get('doc_id', 'unknown')}.zip",
        )

    except Exception as e:
        log.error("Simple submit download bundle error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/simple-submit/download/<path:filepath>")
@auth_required
def api_simple_submit_download(filepath):
    """Serve a single generated file for inline preview or download."""
    safe_path = os.path.normpath(filepath)
    if ".." in safe_path or safe_path.startswith(("/", "\\")):
        return jsonify({"ok": False, "error": "Invalid path"}), 400

    full_path = os.path.join("output", "simple_submit", safe_path)
    if not os.path.exists(full_path):
        return jsonify({"ok": False, "error": "File not found"}), 404

    inline = request.args.get("inline", "0") == "1"
    return send_file(
        full_path,
        mimetype="application/pdf",
        as_attachment=not inline,
    )


@bp.route("/api/simple-submit/approve", methods=["POST"])
@auth_required
def api_simple_submit_approve():
    """Sign a draft 704 PDF — adds PNG signature + date, locks the PDF.

    Called AFTER the operator reviews the draft. This is the final step
    before sending to the buyer.
    """
    try:
        data = request.get_json(force=True)
        doc_id = data.get("doc_id", "")
        file_path = data.get("file_path", "")

        if not file_path or not os.path.exists(file_path):
            # Try to find the 704 in the output directory
            file_path = os.path.join("output", "simple_submit", doc_id,
                                     f"704_filled_{doc_id}.pdf")
        if not os.path.exists(file_path):
            return jsonify({"ok": False, "error": "Draft PDF not found"}), 404

        with open(file_path, "rb") as f:
            draft_bytes = f.read()

        from src.forms.fill_engine import approve_and_sign
        signed_bytes = approve_and_sign(draft_bytes)

        # Save signed version with _SIGNED suffix
        signed_path = file_path.replace(".pdf", "_SIGNED.pdf")
        with open(signed_path, "wb") as f:
            f.write(signed_bytes)

        return jsonify({
            "ok": True,
            "signed_path": signed_path,
            "name": os.path.basename(signed_path),
            "size": len(signed_bytes),
        })

    except Exception as e:
        log.error("Simple submit approve error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
