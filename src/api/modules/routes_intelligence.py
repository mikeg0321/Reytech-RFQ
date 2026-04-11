"""
routes_intelligence.py — Intelligence layer routes.

Features:
- Document intake (upload + parse with docling)
- Natural language query interface
- Compliance matrix extraction
"""
import json
import logging
import os
import tempfile

log = logging.getLogger("reytech.routes_intelligence")


# ═══════════════════════════════════════════════════════════════════════════
# DOCUMENT INTAKE
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/documents/upload", methods=["POST"])
@auth_required
def api_document_upload():
    """Upload and parse a document (PDF, DOCX, XLSX)."""
    try:
        from src.core.feature_flags import get_flag
        if not get_flag("docling_intake", default=False):
            return jsonify({"ok": False, "error": "Document intake is not enabled"}), 403
    except ImportError:
        pass

    try:
        from src.agents.docling_parser import parse_document, save_parsed_document, validate_file

        if "file" not in request.files:
            return jsonify({"ok": False, "error": "No file uploaded"}), 400

        f = request.files["file"]
        if not f.filename:
            return jsonify({"ok": False, "error": "No filename"}), 400

        # Save to temp file
        ext = os.path.splitext(f.filename)[1].lower()
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
            f.save(tmp.name)
            tmp_path = tmp.name

        try:
            # Validate
            ok, err = validate_file(tmp_path)
            if not ok:
                return jsonify({"ok": False, "error": err}), 400

            # Parse
            result = parse_document(tmp_path)
            if not result.get("ok"):
                return jsonify({"ok": False, "error": result.get("error", "Parse failed")}), 500

            # Get optional link IDs
            linked_rfq = request.form.get("rfq_id", "")
            linked_pc = request.form.get("pc_id", "")
            doc_type = request.form.get("doc_type", "unknown")

            # Save to DB
            doc_id = save_parsed_document(
                filename=f.filename,
                markdown=result["markdown"],
                tables=result.get("tables", []),
                metadata=result.get("metadata", {}),
                linked_rfq_id=linked_rfq or None,
                linked_pc_id=linked_pc or None,
                doc_type=doc_type,
                page_count=result.get("page_count", 0),
                duration_ms=result.get("duration_ms", 0),
            )

            return jsonify({
                "ok": True,
                "doc_id": doc_id,
                "filename": f.filename,
                "page_count": result.get("page_count", 0),
                "tables_found": len(result.get("tables", [])),
                "engine": result.get("metadata", {}).get("engine", "unknown"),
                "duration_ms": result.get("duration_ms", 0),
                "preview": result["markdown"][:500],
            })

        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    except Exception as e:
        log.error("Document upload error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/documents", methods=["GET"])
@auth_required
def api_documents_list():
    """List parsed documents."""
    try:
        from src.agents.docling_parser import list_parsed_documents
        limit = request.args.get("limit", 50, type=int)
        doc_type = request.args.get("type", None)
        docs = list_parsed_documents(limit=limit, doc_type=doc_type)
        return jsonify({"ok": True, "documents": docs, "count": len(docs)})
    except Exception as e:
        log.error("Document list error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/documents/<int:doc_id>", methods=["GET"])
@auth_required
def api_document_detail(doc_id):
    """Get a parsed document by ID."""
    try:
        from src.agents.docling_parser import get_parsed_document
        doc = get_parsed_document(doc_id)
        if not doc.get("ok"):
            return jsonify(doc), 404
        return jsonify(doc)
    except Exception as e:
        log.error("Document detail error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/documents/<int:doc_id>", methods=["GET"])
@auth_required
def page_document_detail(doc_id):
    """Rendered HTML page for a parsed document."""
    try:
        from src.agents.docling_parser import get_parsed_document
        doc = get_parsed_document(doc_id)
        if not doc.get("ok"):
            return render_page("_error.html", error="Document not found"), 404

        return render_page("document_view.html",
                           doc=doc,
                           doc_id=doc_id,
                           page_title=f"Document: {doc.get('filename', 'Unknown')}")
    except Exception as e:
        log.error("Document page error: %s", e, exc_info=True)
        return render_page("_error.html", error=str(e)), 500


# ═══════════════════════════════════════════════════════════════════════════
# NATURAL LANGUAGE QUERY
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/v1/search/nl", methods=["POST"])
@auth_required
def api_nl_query():
    """Natural language query interface."""
    try:
        from src.core.feature_flags import get_flag
        if not get_flag("nl_query_enabled", default=False):
            return jsonify({"ok": False, "error": "Natural language query is not enabled"}), 403
    except ImportError:
        pass

    try:
        from src.agents.nl_query_agent import nl_query

        data = request.get_json(silent=True) or {}
        query_text = data.get("query", "").strip()

        if not query_text:
            return jsonify({"ok": False, "error": "No query provided"}), 400

        if len(query_text) > 500:
            return jsonify({"ok": False, "error": "Query too long (max 500 chars)"}), 400

        result = nl_query(query_text)
        return jsonify(result)

    except Exception as e:
        log.error("NL query error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/v1/search/nl/history", methods=["GET"])
@auth_required
def api_nl_query_history():
    """Get recent NL query history."""
    try:
        from src.agents.nl_query_agent import get_query_history
        limit = request.args.get("limit", 20, type=int)
        history = get_query_history(limit=limit)
        return jsonify({"ok": True, "history": history})
    except Exception as e:
        log.error("NL query history error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# COMPLIANCE MATRIX
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/<rfq_id>/compliance/extract", methods=["POST"])
@auth_required
def api_compliance_extract(rfq_id):
    """Extract compliance matrix from an uploaded solicitation PDF."""
    try:
        from src.core.feature_flags import get_flag
        if not get_flag("compliance_matrix", default=False):
            return jsonify({"ok": False, "error": "Compliance matrix is not enabled"}), 403
    except ImportError:
        pass

    try:
        from src.agents.compliance_extractor import extract_compliance_matrix

        # Accept either uploaded file or existing parsed document
        pdf_path = None
        if "file" in request.files:
            f = request.files["file"]
            ext = os.path.splitext(f.filename)[1].lower()
            if ext != ".pdf":
                return jsonify({"ok": False, "error": "Only PDF files supported"}), 400
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                f.save(tmp.name)
                pdf_path = tmp.name
        else:
            data = request.get_json(silent=True) or {}
            pdf_path = data.get("pdf_path", "")

        if not pdf_path:
            return jsonify({"ok": False, "error": "No PDF provided"}), 400

        # Load RFQ data for cross-reference
        rfq_data = {}
        generated_files = []
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute("SELECT * FROM rfqs WHERE id = ?", (rfq_id,)).fetchone()
                if row:
                    rfq_data = dict(row)
            # Try to get generated files from manifest
            import glob
            rfq_dir = os.path.join("data", "rfq_packages", rfq_id)
            if os.path.isdir(rfq_dir):
                generated_files = [os.path.splitext(os.path.basename(f))[0]
                                   for f in glob.glob(os.path.join(rfq_dir, "*.pdf"))]
        except Exception:
            pass

        result = extract_compliance_matrix(pdf_path, rfq_id, rfq_data, generated_files)

        # Cleanup temp file if we created one
        if "file" in request.files:
            try:
                os.unlink(pdf_path)
            except OSError:
                pass

        return jsonify(result)

    except Exception as e:
        log.error("Compliance extract error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfq/<rfq_id>/compliance", methods=["GET"])
@auth_required
def api_compliance_get(rfq_id):
    """Get the compliance matrix for an RFQ."""
    try:
        from src.agents.compliance_extractor import get_compliance_matrix
        matrix = get_compliance_matrix(rfq_id)
        return jsonify(matrix)
    except Exception as e:
        log.error("Compliance get error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# QUOTE EXPIRY
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/quotes/expiring", methods=["GET"])
@auth_required
def api_quotes_expiring():
    """Get quotes expiring within 7 days."""
    try:
        from src.core.db import get_db
        from datetime import datetime, timedelta
        now = datetime.now()
        cutoff = (now + timedelta(days=7)).isoformat()
        with get_db() as conn:
            rows = conn.execute(
                """SELECT quote_number, agency, institution, total, expires_at, sent_at, contact_name, status
                   FROM quotes WHERE expires_at != '' AND expires_at <= ? AND status NOT IN ('won','lost','cancelled','expired')
                   ORDER BY expires_at ASC LIMIT 20""",
                (cutoff,)
            ).fetchall()
        results = []
        for row in rows:
            d = dict(row)
            try:
                exp = datetime.fromisoformat(d["expires_at"])
                d["days_remaining"] = max(0, (exp - now).days)
                d["severity"] = "critical" if d["days_remaining"] <= 3 else "warning"
            except (ValueError, TypeError):
                d["days_remaining"] = -1
                d["severity"] = "unknown"
            results.append(d)
        return jsonify({"ok": True, "expiring": results, "count": len(results)})
    except Exception as e:
        log.error("Expiring quotes error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# BID SCORING
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/pricecheck/<pc_id>/bid-score", methods=["POST"])
@auth_required
def api_bid_score(pc_id):
    """Score a PC for bid/no-bid decision."""
    try:
        from src.core.feature_flags import get_flag
        if not get_flag("bid_scoring", default=False):
            return jsonify({"ok": False, "error": "Bid scoring not enabled"}), 403
    except ImportError:
        pass
    try:
        from src.agents.bid_decision_agent import score_pc
        return jsonify(score_pc(pc_id))
    except Exception as e:
        log.error("Bid score error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pricecheck/<pc_id>/bid-score", methods=["GET"])
@auth_required
def api_bid_score_get(pc_id):
    """Get saved bid score for a PC."""
    try:
        from src.agents.bid_decision_agent import get_bid_score
        return jsonify(get_bid_score(pc_id))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# SHIP-TO AUTO-FILL
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/contacts/<email>/address", methods=["GET"])
@auth_required
def api_contact_address(email):
    """Lookup contact address by email for ship-to auto-fill."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT address, city, state, zip, ship_to_default, agency FROM contacts WHERE buyer_email = ?",
                (email,)
            ).fetchone()
            if not row:
                return jsonify({"ok": False, "error": "Contact not found"}), 404
            d = dict(row)
            d["ok"] = True
            return jsonify(d)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# NL QUERY V2 — SUGGESTIONS
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/v1/search/nl/suggestions", methods=["GET"])
@auth_required
def api_nl_suggestions():
    """Get suggested NL queries."""
    try:
        from src.agents.nl_query_agent import SUGGESTED_QUERIES
        return jsonify({"ok": True, "suggestions": SUGGESTED_QUERIES})
    except Exception:
        return jsonify({"ok": True, "suggestions": []})


# ═══════════════════════════════════════════════════════════════════════════
# UNSPSC V2 — BATCH RETAG
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/catalog/retag-unspsc", methods=["POST"])
@auth_required
def api_retag_unspsc():
    """Batch retag catalog items with UNSPSC codes."""
    try:
        from src.core.feature_flags import get_flag
        if not get_flag("unspsc_enrichment", default=False):
            return jsonify({"ok": False, "error": "UNSPSC enrichment not enabled"}), 403
    except ImportError:
        pass
    try:
        from src.agents.unspsc_classifier import batch_retag_catalog
        data = request.get_json(silent=True) or {}
        limit = data.get("limit", 50)
        result = batch_retag_catalog(limit=limit)
        return jsonify({"ok": True, **result})
    except Exception as e:
        log.error("UNSPSC retag error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════════
# COMPLIANCE V2 — AGENCY TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════

@bp.route("/api/compliance/templates/<agency_key>", methods=["GET"])
@auth_required
def api_compliance_template(agency_key):
    """Get compliance requirement template for an agency."""
    try:
        from src.agents.compliance_extractor import get_agency_template
        return jsonify({"ok": True, "agency": agency_key, "requirements": get_agency_template(agency_key)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/compliance/templates/seed", methods=["POST"])
@auth_required
def api_seed_compliance_templates():
    """Seed default agency compliance templates."""
    try:
        from src.agents.compliance_extractor import seed_agency_templates
        return jsonify({"ok": True, "seeded": seed_agency_templates()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
