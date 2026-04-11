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
