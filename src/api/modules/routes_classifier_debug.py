"""Debug endpoint for the unified ingest classifier.

POST /api/admin/classifier/classify
    multipart file upload → runs classify_request() → returns JSON

Lets Mike (and Claude in future sessions) test arbitrary buyer files
against the classifier without creating a record or going through
the full ingest pipeline. Essential for debugging when a new file
type surfaces in production.

Read-only. No record created. No data modified. Completely safe.
"""
import logging
import os
import tempfile

from flask import jsonify, request

from src.api.shared import bp, auth_required
from src.core.error_handler import safe_route

log = logging.getLogger("reytech")


@bp.route("/api/admin/classifier/classify", methods=["POST"])
@auth_required
@safe_route
def api_classifier_classify():
    """Run the classifier on an uploaded file.

    Form fields:
      file        — the uploaded file (required)
      email_body  — optional email body text for email-only classification
      email_subject — optional subject
      email_sender  — optional from-address

    Returns the full RequestClassification dict. Nothing is persisted.
    """
    from src.core.request_classifier import classify_request

    f = request.files.get("file")
    email_body = request.form.get("email_body", "") or ""
    email_subject = request.form.get("email_subject", "") or ""
    email_sender = request.form.get("email_sender", "") or ""

    tmp_path = None
    files = []
    try:
        if f and f.filename:
            # Write to a temp file for the classifier to read
            suffix = os.path.splitext(f.filename)[1].lower() or ".bin"
            tmp = tempfile.NamedTemporaryFile(
                delete=False, suffix=suffix, prefix="classify_debug_"
            )
            f.save(tmp.name)
            tmp.close()
            tmp_path = tmp.name
            files = [tmp_path]

        classification = classify_request(
            attachments=files,
            email_body=email_body,
            email_subject=email_subject,
            email_sender=email_sender,
        )

        result = classification.to_dict()
        # Add the input echo so the caller can sanity-check what was sent
        result["_input"] = {
            "filename": f.filename if f else None,
            "size_bytes": os.path.getsize(tmp_path) if tmp_path else 0,
            "email_subject": email_subject[:200] if email_subject else "",
            "email_sender": email_sender,
        }
        return jsonify({"ok": True, "classification": result})
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception as _e:
                log.debug("suppressed: %s", _e)
