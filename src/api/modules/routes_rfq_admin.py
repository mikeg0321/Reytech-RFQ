# routes_rfq_admin.py — Admin, Export, Settings, Diagnostics, File Management
# Split from routes_rfq.py — loaded via importlib in dashboard.py

from flask import request, jsonify, Response
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.error_handler import safe_route, safe_page
from src.core.security import rate_limit
from flask import redirect, flash, send_file, session
from src.core.paths import DATA_DIR, OUTPUT_DIR, UPLOAD_DIR
from src.core.db import get_db
from src.api.render import render_page
import os, json, re
from datetime import datetime, timedelta, timezone


# ═══════════════════════════════════════════════════════════════════════
# RFQ File Management — download from DB
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/file/<file_id>")
@auth_required
@safe_page
def rfq_download_file(rid, file_id):
    """Download an RFQ file from the database."""
    _bad = _validate_rid(rid)
    if _bad: return _bad
    f = get_rfq_file(file_id)
    if not f or f.get("rfq_id") != rid:
        flash("File not found", "error")
        return redirect(f"/rfq/{rid}")
    from flask import Response
    # Validate MIME type against allowlist to prevent XSS via stored type
    _allowed_mimes = {"application/pdf", "image/png", "image/jpeg",
                      "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
    _mime = f.get("mime_type", "application/pdf")
    if _mime not in _allowed_mimes:
        _mime = "application/octet-stream"
    _inline = request.args.get("inline") == "1"
    _disp = "inline" if _inline else "attachment"
    return Response(
        f["data"],
        mimetype=_mime,
        headers={"Content-Disposition": f"{_disp}; filename=\"{f['filename']}\""}
    )


@bp.route("/api/rfq/<rid>/files")
@auth_required
@safe_route
def api_rfq_files(rid):
    """List all files for an RFQ."""
    category = request.args.get("category")
    files = list_rfq_files(rid, category=category)
    return jsonify({"ok": True, "files": files, "count": len(files)})


# ═══════════════════════════════════════════════════════════════════════
# RFQ Status Management — reopen, edit, resubmit
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/reopen", methods=["POST"])
@auth_required
@safe_route
def rfq_reopen(rid):
    """Reopen an RFQ for editing. Changes status back to 'ready'."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error")
        return redirect("/")
    
    old_status = r.get("status", "?")
    _transition_status(r, "ready", actor="user", notes=f"Reopened from '{old_status}'")
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "ready")
    except Exception as _e:
        log.debug('suppressed in rfq_reopen: %s', _e)

    _log_rfq_activity(rid, "reopened",
        f"RFQ #{r.get('solicitation_number','?')} reopened for editing (was: {old_status})",
        actor="user", metadata={"old_status": old_status})
    
    flash(f"RFQ reopened for editing (was: {old_status})", "info")
    return redirect(f"/rfq/{rid}")


def _rfq_to_pc_for_qa(rfq: dict) -> dict:
    """Adapter: shape an RFQ dict into the form pc_qa_agent.run_qa expects.

    Delegates to the canonical record-field accessor so PC and RFQ shapes
    both yield correct revenue/profit readings without mutating the source.
    See src/core/record_fields.build_qa_view.
    """
    from src.core.record_fields import build_qa_view
    return build_qa_view(rfq)


@bp.route("/api/rfq/<rid>/qa", methods=["GET", "POST"])
@auth_required
@safe_route
def api_rfq_qa(rid):
    """Run the existing PC QA helper against an RFQ. Same rules — no new
    QA logic — so the RFQ page can hard-block destructive actions until
    blockers clear, mirroring the PC review page gate.
    """
    try:
        rfqs = load_rfqs()
        rfq = rfqs.get(rid)
        if not rfq:
            return jsonify({"ok": False, "error": "RFQ not found"}), 404
        pc_view = _rfq_to_pc_for_qa(rfq)
        # Default to skip-LLM on the RFQ side: the gate is meant to be fast
        # and reproducible. Caller can opt in with ?llm=1.
        use_llm = request.args.get("llm", "0") == "1"
        from src.agents.pc_qa_agent import run_qa
        import copy as _copy
        report = run_qa(_copy.deepcopy(pc_view), use_llm=use_llm)
        return jsonify(report)
    except Exception as e:
        log.error("RFQ QA error for %s: %s", rid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfq/<rid>/update-status", methods=["POST"])
@auth_required
@safe_route
def api_rfq_update_status_json(rid):
    """Update RFQ status via JSON (AJAX)."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    data = request.get_json(force=True, silent=True) or {}
    new_status = data.get("status", "").strip()
    notes = data.get("notes", "").strip()

    valid = {"new", "ready", "generated", "ready_to_send", "sent", "won", "lost", "no_bid", "cancelled"}
    if new_status not in valid:
        return jsonify({"ok": False, "error": f"Invalid status: {new_status}"})

    old_status = r.get("status", "?")
    r["status"] = new_status
    if notes:
        r["status_notes"] = notes
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, new_status)
    except Exception as _e:
        log.debug('suppressed in api_rfq_update_status_json: %s', _e)

    try:
        from src.core.dal import log_lifecycle_event
        log_lifecycle_event("rfq", rid, "status_changed",
            f"Status: {old_status} → {new_status}" + (f" ({notes})" if notes else ""),
            actor="user")
    except Exception as _e:
        log.debug('suppressed in api_rfq_update_status_json: %s', _e)

    return jsonify({"ok": True, "old_status": old_status, "new_status": new_status})


@bp.route("/rfq/<rid>/update-status-form", methods=["POST"])
@auth_required
@safe_route
def rfq_update_status(rid):
    """Change RFQ status to any valid state."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error")
        return redirect("/")
    
    new_status = request.form.get("status", "").strip()
    valid = {"new", "ready", "generated", "ready_to_send", "sent", "won", "lost", "no_bid", "cancelled"}
    if new_status not in valid:
        flash(f"Invalid status: {new_status}", "error")
        return redirect(f"/rfq/{rid}")
    
    old_status = r.get("status", "?")
    notes = request.form.get("notes", "").strip()
    _transition_status(r, new_status, actor="user", notes=notes or f"Changed from {old_status}")
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, new_status)
    except Exception as _e:
        log.debug('suppressed in rfq_update_status: %s', _e)

    _log_rfq_activity(rid, "status_changed",
        f"RFQ #{r.get('solicitation_number','?')} status: {old_status} → {new_status}" + (f" ({notes})" if notes else ""),
        actor="user", metadata={"old_status": old_status, "new_status": new_status, "notes": notes})

    flash(f"Status changed: {old_status} → {new_status}", "success")
    return redirect(f"/rfq/{rid}")


# ═══════════════════════════════════════════════════════════════════════
# RFQ Activity Log
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/<rid>/activity")
@auth_required
@safe_route
def api_rfq_activity(rid):
    """Get activity log for an RFQ."""
    activities = _get_crm_activity(ref_id=rid, limit=50)
    return jsonify({"ok": True, "activities": activities, "count": len(activities)})


# ═══════════════════════════════════════════════════════════════════════
# Email Templates API
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email-templates")
@auth_required
@safe_route
def api_list_email_templates():
    """List email templates, optionally filtered by category."""
    category = request.args.get("category")
    templates = get_email_templates_db(category)
    return jsonify({"ok": True, "templates": templates})


@bp.route("/api/email-templates/<tid>", methods=["GET"])
@auth_required
@safe_route
def api_get_email_template(tid):
    """Get a single email template by ID."""
    templates = get_email_templates_db()
    t = next((t for t in templates if t["id"] == tid), None)
    if not t:
        return jsonify({"ok": False, "error": "Template not found"}), 404
    return jsonify({"ok": True, "template": t})


@bp.route("/api/email-templates", methods=["POST"])
@auth_required
@safe_route
def api_create_email_template():
    """Create or update an email template."""
    data = request.get_json(force=True, silent=True) or request.form
    tid = save_email_template_db(
        data.get("id", ""), data.get("name", ""), data.get("category", "rfq"),
        data.get("subject", ""), data.get("body", ""), int(data.get("is_default", 0)))
    if tid:
        return jsonify({"ok": True, "id": tid})
    return jsonify({"ok": False, "error": "Save failed"}), 500


@bp.route("/api/email-templates/<tid>", methods=["DELETE"])
@auth_required
@safe_route
def api_delete_email_template(tid):
    """Delete an email template."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM email_templates WHERE id = ?", (tid,))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/email-templates/render", methods=["POST"])
@auth_required
@safe_route
def api_render_email_template():
    """Render a template with variables. POST {template_id, variables: {...}}"""
    data = request.get_json(force=True, silent=True) or {}
    tid = data.get("template_id", "")
    variables = data.get("variables", {})
    
    templates = get_email_templates_db()
    t = next((t for t in templates if t["id"] == tid), None)
    if not t:
        return jsonify({"ok": False, "error": "Template not found"}), 404
    
    subject = t["subject"]
    body = t["body"]
    for key, val in variables.items():
        subject = subject.replace("{{" + key + "}}", str(val))
        body = body.replace("{{" + key + "}}", str(val))
    
    return jsonify({"ok": True, "subject": subject, "body": body})


# ═══════════════════════════════════════════════════════════════════════
# PDF Preview from DB
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/preview/<file_id>")
@auth_required
@safe_page
def rfq_preview_pdf(rid, file_id):
    """Serve a PDF for inline preview (Content-Disposition: inline)."""
    f = get_rfq_file(file_id)
    if not f or f.get("rfq_id") != rid:
        return "File not found", 404
    from flask import Response
    return Response(
        f["data"],
        mimetype="application/pdf",
        headers={"Content-Disposition": f"inline; filename=\"{f['filename']}\""}
    )


# ═══════════════════════════════════════════════════════════════════════
# Email Signature — get/save HTML signature for outbound emails
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email-signature")
@auth_required
@safe_route
def get_email_signature():
    """Get current email signature config."""
    email_cfg = CONFIG.get("email", {})
    sig_html = email_cfg.get("signature_html", "")

    # Auto-generate default signature on first load if empty
    if not sig_html:
        sig_html = _build_default_signature()
        CONFIG.setdefault("email", {})["signature_html"] = sig_html

    return jsonify({
        "ok": True,
        "signature_html": sig_html,
        "signature_enabled": email_cfg.get("signature_enabled", True),
    })

@bp.route("/api/email-signature", methods=["POST"])
@auth_required
@safe_route
def save_email_signature():
    """Save email signature HTML."""
    data = request.get_json(force=True)
    sig_html = data.get("signature_html", "")
    
    CONFIG.setdefault("email", {})["signature_html"] = sig_html
    CONFIG["email"]["signature_enabled"] = True
    
    # Persist to config file
    import json as _json
    for cfg_path in [
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "reytech_config.json"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "forms", "reytech_config.json"),
    ]:
        try:
            with open(cfg_path) as f:
                cfg = _json.load(f)
            cfg.setdefault("email", {})["signature_html"] = sig_html
            cfg["email"]["signature_enabled"] = True
            with open(cfg_path, "w") as f:
                _json.dump(cfg, f, indent=2)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    
    return jsonify({"ok": True})


@bp.route("/api/upload-sig-logo", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def upload_sig_logo():
    """Upload a PNG/JPG logo for the email signature. Returns base64 data URI."""
    import base64 as _b64
    if "logo" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400
    f = request.files["logo"]
    if not f.filename:
        return jsonify({"ok": False, "error": "Empty filename"}), 400

    data = f.read()
    if len(data) > 5_000_000:
        return jsonify({"ok": False, "error": "File too large (max 5MB)"}), 400

    fname = f.filename.lower()
    if fname.endswith(".png"):
        mime = "image/png"
    elif fname.endswith((".jpg", ".jpeg")):
        mime = "image/jpeg"
    elif fname.endswith(".gif"):
        mime = "image/gif"
    else:
        return jsonify({"ok": False, "error": "PNG/JPG/GIF only"}), 400

    # Resize for email if large
    try:
        from PIL import Image
        import io as _io
        img = Image.open(_io.BytesIO(data))
        if img.width > 200:
            ratio = 200 / img.width
            img = img.resize((200, int(img.height * ratio)), Image.LANCZOS)
            buf = _io.BytesIO()
            img.save(buf, "PNG", optimize=True)
            data = buf.getvalue()
            mime = "image/png"
    except Exception as _e:
        log.debug('suppressed in upload_sig_logo: %s', _e)

    b64 = _b64.b64encode(data).decode()
    data_uri = f"data:{mime};base64,{b64}"

    # Save to data/ for future use
    try:
        save_path = os.path.join(DATA_DIR, "email_logo.png")
        with open(save_path, "wb") as _fw:
            _fw.write(data)
    except Exception as _e:
        log.debug('suppressed in upload_sig_logo: %s', _e)

    return jsonify({"ok": True, "data_uri": data_uri, "size": len(data)})


def _build_default_signature():
    """Return empty — Gmail auto-appends the canonical signature.

    Previously auto-generated a hardcoded Reytech HTML signature on first
    load of /api/email-signature, which stacked on top of Gmail's own
    auto-sig on every send. Per CLAUDE.md "Gmail Handles Signatures", the
    app must not inject one. Users who want an explicit compose-time sig
    can still save one via POST /api/email-signature (that's a user
    choice, not an app default).
    """
    return ""


# ═══════════════════════════════════════════════════════════════════════
# Enhanced Email Send — DB attachments + email logging + CRM tracking
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/save-draft", methods=["POST"])
@auth_required
@safe_page
def save_gmail_draft(rid):
    """Save email as Gmail draft — user reviews and sends manually from Gmail."""
    from src.api.trace import Trace
    t = Trace("email_draft", rfq_id=rid)

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error")
        return redirect("/")

    to_addr = request.form.get("to", "").strip()
    subject = request.form.get("subject", "").strip()
    body = request.form.get("body", "").strip()
    cc = request.form.get("cc", "").strip()
    attach_ids = [x.strip() for x in request.form.get("attach_files", "").split(",") if x.strip()]

    if not to_addr or not subject:
        flash("Draft requires To and Subject", "error")
        return redirect(f"/rfq/{rid}")

    import tempfile, shutil, imaplib, time as _time
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    tmp_dir = tempfile.mkdtemp(prefix="rfq_draft_")
    try:
        # Build the MIME message
        msg = MIMEMultipart("mixed")
        email_cfg = CONFIG.get("email", {})
        from_name = email_cfg.get("from_name", "Michael Guadan - Reytech Inc.")
        from_addr = email_cfg.get("email", os.environ.get("GMAIL_ADDRESS", "sales@reytechinc.com"))
        password = email_cfg.get("email_password", os.environ.get("GMAIL_PASSWORD", ""))

        msg["From"] = f"{from_name} <{from_addr}>"
        msg["To"] = to_addr
        msg["Subject"] = subject
        if cc:
            msg["Cc"] = cc

        # HTML body with signature
        try:
            from src.core.email_signature import wrap_html_email
            body_html = wrap_html_email(body)
        except Exception:
            body_html = None

        if body_html:
            related = MIMEMultipart("related")
            related.attach(MIMEText(body_html, "html"))
            # Embed logo as CID inline attachment
            try:
                from src.core.paths import DATA_DIR as _dd2
                for _ln in ("reytech_logo_email.png", "email_logo.png", "reytech_logo.png", "logo.png"):
                    _lp = os.path.join(_dd2, _ln)
                    if os.path.exists(_lp):
                        from email.mime.image import MIMEImage
                        with open(_lp, "rb") as _lf2:
                            _lip = MIMEImage(_lf2.read(), _subtype="png")
                        _lip.add_header("Content-ID", "<reytech_logo>")
                        _lip.add_header("Content-Disposition", "inline", filename="reytech_logo.png")
                        related.attach(_lip)
                        break
            except Exception as _e:
                log.debug('suppressed in save_gmail_draft: %s', _e)
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(body, "plain"))
            alt.attach(related)
            msg.attach(alt)
        else:
            msg.attach(MIMEText(body, "plain"))

        # Attach files
        attached = []
        for fid in attach_ids:
            f = get_rfq_file(fid)
            if f and f.get("data"):
                path = os.path.join(tmp_dir, f["filename"])
                with open(path, "wb") as _fw:
                    _fw.write(f["data"])
                with open(path, "rb") as _fr:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(_fr.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={f['filename']}")
                msg.attach(part)
                attached.append(f["filename"])

        # Save to Gmail Drafts via IMAP APPEND
        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(from_addr, password)

        saved = False
        for folder in ['"[Gmail]/Drafts"', "[Gmail]/Drafts", "Drafts", "DRAFTS"]:
            try:
                res = imap.append(folder, "", imaplib.Time2Internaldate(_time.time()), msg.as_bytes())
                if res[0] == "OK":
                    saved = True
                    t.ok("Draft saved", folder=folder, attachments=len(attached))
                    break
            except Exception as _fe:
                log.debug("IMAP draft append %s: %s", folder, _fe)

        if not saved:
            # Auto-detect Drafts folder
            _, folders = imap.list()
            import re as _re
            for _raw in (folders or []):
                _s = _raw.decode() if isinstance(_raw, bytes) else str(_raw)
                if "draft" in _s.lower():
                    _m = _re.search(r'"([^"]+)"\s*$', _s) or _re.search(r'(\S+)$', _s)
                    if _m:
                        try:
                            res = imap.append(_m.group(1), "", imaplib.Time2Internaldate(_time.time()), msg.as_bytes())
                            if res[0] == "OK":
                                saved = True
                                t.ok("Draft saved", folder=_m.group(1))
                                break
                        except Exception as _e:
                            log.debug('suppressed in save_gmail_draft: %s', _e)

        imap.logout()

        if saved:
            flash(f"✅ Draft saved to Gmail — open Gmail to review and send ({len(attached)} attachments)", "success")
        else:
            flash("⚠️ Could not save to Gmail Drafts — check IMAP is enabled in Gmail settings", "error")

    except Exception as e:
        t.fail("Draft save failed", error=str(e))
        flash(f"Draft save failed: {e}", "error")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/send-email", methods=["POST"])
@auth_required
@safe_route
def send_email_enhanced(rid):
    """Send email with editable fields and DB-stored attachments.
    Form fields: to, subject, body, attach_files (comma-separated file IDs)
    """
    from src.api.trace import Trace
    t = Trace("email_send", rfq_id=rid)

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        t.fail("RFQ not found")
        flash("RFQ not found", "error")
        return redirect("/")

    # Validate before sending
    from src.core.quote_validator import validate_ready_to_send
    validation = validate_ready_to_send(r)
    if not validation["ok"]:
        t.fail("Send validation failed", errors=validation["errors"])
        flash(f"Cannot send: {'; '.join(validation['errors'])}", "error")
        return redirect(f"/rfq/{rid}")

    # Get editable fields from form
    to_addr = request.form.get("to", "").strip()
    subject = request.form.get("subject", "").strip()
    body = request.form.get("body", "").strip()
    cc = request.form.get("cc", "").strip()
    bcc = request.form.get("bcc", "").strip()
    attach_ids = [x.strip() for x in request.form.get("attach_files", "").split(",") if x.strip()]
    
    if not to_addr or not subject:
        flash("Email requires To and Subject", "error")
        return redirect(f"/rfq/{rid}")
    
    t.step("Preparing email", to=to_addr, attachments=len(attach_ids))
    
    # Build attachment list from DB files
    import tempfile, shutil
    tmp_dir = tempfile.mkdtemp(prefix="rfq_send_")
    attachment_paths = []
    attachment_names = []
    
    try:
        for fid in attach_ids:
            f = get_rfq_file(fid)
            if f and f.get("data"):
                path = os.path.join(tmp_dir, f["filename"])
                with open(path, "wb") as _fw:
                    _fw.write(f["data"])
                attachment_paths.append(path)
                attachment_names.append(f["filename"])
                t.step(f"Attached: {f['filename']}")
        
        # Also check filesystem for any files not in DB yet
        if not attach_ids and r.get("output_files"):
            out_dir = os.path.join(UPLOAD_DIR, rid)
            for fname in r["output_files"]:
                fpath = os.path.join(out_dir, fname)
                if os.path.exists(fpath):
                    attachment_paths.append(fpath)
                    attachment_names.append(fname)
        
        # Send via SMTP — include HTML signature if enabled
        draft = {
            "to": to_addr,
            "subject": subject,
            "body": body,
            "cc": cc,
            "bcc": bcc,
            "attachments": attachment_paths,
        }
        
        # Threading: if RFQ came from email, reply to that thread
        msg_id = r.get("email_message_id", "")
        if msg_id:
            draft["in_reply_to"] = msg_id
            draft["references"] = msg_id
        
        include_sig = request.form.get("include_signature") == "1"
        email_cfg = CONFIG.get("email", {})
        sig_html = email_cfg.get("signature_html", "")
        
        if include_sig and sig_html:
            # Build HTML body: plain text body + signature
            import html as _html
            body_escaped = _html.escape(body).replace("\n", "<br>")
            draft["body_html"] = f"""<div style="font-family:'Segoe UI',Arial,sans-serif;font-size:14px;color:#222;line-height:1.6">
{body_escaped}
<br><br>
<div style="border-top:1px solid #ddd;padding-top:10px;margin-top:10px">
{sig_html}
</div>
</div>"""
            t.step("HTML signature included")
        
        sender = EmailSender(CONFIG.get("email", {}))
        sender.send(draft)
        
        # Transition status
        _transition_status(r, "sent", actor="user", notes=f"Email sent to {to_addr}")
        r["sent_at"] = datetime.now().isoformat()
        r["draft_email"] = {"to": to_addr, "subject": subject, "body": body, "cc": cc, "bcc": bcc}
        from src.api.dashboard import _save_single_rfq
        _save_single_rfq(rid, r)
        try:
            from src.core.dal import update_rfq_status as _dal_ur
            _dal_ur(rid, "sent")
        except Exception as _e:
            log.debug('suppressed in send_email_enhanced: %s', _e)
        
        # ── Log to email_log table ──
        sol = r.get("solicitation_number", "")
        qn = r.get("reytech_quote_number", "")
        
        # Find contact_id from recipient email
        contact_id = ""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute("SELECT id FROM contacts WHERE buyer_email = ?", (to_addr.lower(),)).fetchone()
                if row:
                    contact_id = row[0]
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        
        email_log_id = log_email_sent_db(
            direction="outbound", sender=sender.email_addr, recipient=to_addr,
            subject=subject, body=body, attachments=attachment_names,
            quote_number=qn, rfq_id=rid, contact_id=contact_id)
        t.step(f"Email logged (id={email_log_id})")
        
        # ── CRM activity: log against quote AND contact ──
        _log_rfq_activity(rid, "email_sent",
            f"Bid response emailed to {to_addr} for Sol #{sol} ({len(attachment_names)} attachments)",
            actor="user", metadata={"to": to_addr, "quote": qn, "files": attachment_names, "email_log_id": email_log_id})
        
        if qn:
            _log_crm_activity(qn, "email_sent",
                f"Quote {qn} emailed to {to_addr} for Sol #{sol}",
                actor="user", metadata={"to": to_addr, "rfq_id": rid})
            if QUOTE_GEN_AVAILABLE:
                update_quote_status(qn, "sent", actor="system")
        
        if contact_id:
            _log_crm_activity(contact_id, "email_sent",
                f"Bid response for Sol #{sol} (Quote {qn}) sent to {to_addr}",
                actor="user", metadata={"rfq_id": rid, "quote": qn, "solicitation": sol})
        
        t.ok("Email sent", to=to_addr, attachments=len(attachment_names))
        flash(f"✅ Email sent to {to_addr} with {len(attachment_names)} attachments", "success")
        
    except Exception as e:
        t.fail("Send failed", error=str(e))
        flash(f"Send failed: {e}. Try 'Open in Mail App' instead.", "error")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    
    return redirect(f"/rfq/{rid}")


# ═══════════════════════════════════════════════════════════════════════
# Email History API (for contact/quote level)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email-history")
@auth_required
@safe_route
def api_email_history():
    """Get email history. Filter by ?rfq_id=, ?quote_number=, ?contact_id="""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            query = "SELECT id, logged_at, direction, sender, recipient, subject, body_preview, attachments_json, quote_number, rfq_id, contact_id, status FROM email_log WHERE 1=1"
            params = []
            for field in ("rfq_id", "quote_number", "contact_id"):
                val = request.args.get(field)
                if val:
                    query += f" AND {field} = ?"
                    params.append(val)
            query += " ORDER BY logged_at DESC LIMIT 50"
            rows = conn.execute(query, params).fetchall()
            return jsonify({"ok": True, "emails": [dict(r) for r in rows], "count": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ═══════════════════════════════════════════════════════════════════════
# OBS 1600 — CA Agricultural Food Product Certification
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/food-classify", methods=["POST"])
@auth_required
@safe_route
def api_food_classify():
    """Classify quote/RFQ items into CDCR food category codes.
    Body: {"items": [{"description": "..."}, ...]}
    Returns classified items with food codes.
    """
    try:
        from src.forms.food_classifier import classify_quote_items
        data = request.get_json(force=True)
        items = data.get("items", [])
        classified = classify_quote_items(items)
        food_count = sum(1 for r in classified if r['is_food'])
        return jsonify({"ok": True, "items": classified, "food_count": food_count,
                        "total_count": len(classified)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfq/<rid>/obs1600", methods=["POST"])
@auth_required
@safe_route
def api_generate_obs1600(rid):
    """Generate filled OBS 1600 food certification form for an RFQ.
    Uses the bid package PDF if available, or a standalone template.
    """
    from src.api.trace import Trace
    t = Trace("obs1600_fill", rfq_id=rid)
    
    try:
        from src.forms.reytech_filler_v4 import load_config, fill_obs1600, get_pst_date
        from src.forms.food_classifier import get_food_items_for_obs1600
        
        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            t.fail("RFQ not found")
            return jsonify({"ok": False, "error": "RFQ not found"}), 404
        
        config = load_config()
        sol = r.get("solicitation_number", "") or "RFQ"
        
        # Get items — try line_items first, then items_detail from quote
        items = r.get("line_items", [])
        if not items:
            items = r.get("items_detail", r.get("items", []))
            if isinstance(items, str):
                import json as _json
                try: items = _json.loads(items)
                except Exception: items = []
        
        # Classify food items
        food_items = get_food_items_for_obs1600(items)
        
        if not food_items:
            t.step("No food items found", item_count=len(items))
            return jsonify({"ok": False, "error": "No food items found in this RFQ. Only food products need the OBS 1600 form.",
                            "items_checked": len(items)}), 400
        
        t.step("Classified food items", food_count=len(food_items),
               items=[f"{fi['description'][:40]} → Code {fi['code']}" for fi in food_items[:5]])
        
        # Output directory
        out_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "output", sol)
        os.makedirs(out_dir, exist_ok=True)
        
        # Find bid package template — use RFQ's stored template paths
        bid_pkg = None
        tmpl = r.get("templates", {})
        
        # Check bid package template from RFQ data
        if tmpl.get("bidpkg") and os.path.exists(tmpl["bidpkg"]):
            bid_pkg = tmpl["bidpkg"]
        
        # Try to restore from DB if not on disk
        if not bid_pkg:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    db_files = conn.execute(
                        "SELECT id, filename, file_type FROM rfq_files WHERE rfq_id=? AND category='template'",
                        (rid,)).fetchall()
                    for db_f in db_files:
                        fname = db_f["filename"].lower()
                        if "bid" in fname or "package" in fname or "form" in fname:
                            full_f_row = conn.execute("SELECT data FROM rfq_files WHERE id=?", (db_f["id"],)).fetchone()
                            if full_f_row and full_f_row["data"]:
                                restore_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "rfq_templates", rid)
                                os.makedirs(restore_dir, exist_ok=True)
                                restore_path = os.path.join(restore_dir, db_f["filename"])
                                with open(restore_path, "wb") as _fw:
                                    _fw.write(full_f_row["data"])
                                bid_pkg = restore_path
                                t.step(f"Restored bid package from DB: {db_f['filename']}")
                                break
            except Exception as db_err:
                t.step(f"DB restore failed: {db_err}")
        
        # Check uploaded files directory
        if not bid_pkg:
            import glob
            for pattern in [f"*{sol}*BID*PACKAGE*", f"*{sol}*bid*pack*", f"*{sol}*form*", f"*{sol}*.pdf"]:
                for search_dir in [os.path.join(DATA_DIR, "uploads"), os.path.join(DATA_DIR, "rfq_templates"), os.path.join(DATA_DIR, "output", sol)]:
                    matches = glob.glob(os.path.join(search_dir, pattern))
                    for m in matches:
                        # Verify it has OBS 1600 fields
                        try:
                            from pypdf import PdfReader
                            _r = PdfReader(m)
                            for page in _r.pages:
                                if "/Annots" in page:
                                    for annot in page["/Annots"]:
                                        obj = annot.get_object()
                                        if "OBS 1600" in str(obj.get("/T", "")):
                                            bid_pkg = m
                                            break
                                if bid_pkg: break
                        except Exception as _e:
                            log.debug('suppressed in api_generate_obs1600: %s', _e)
                        if bid_pkg: break
                    if bid_pkg: break
                if bid_pkg: break
        
        # Fallback: use saved CDCR bid package template
        if not bid_pkg:
            default_tmpl = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "templates", "cdcr_bid_package_template.pdf")
            if os.path.exists(default_tmpl):
                bid_pkg = default_tmpl
                t.step("Using saved CDCR bid package template")
        
        # Build rfq_data for the filler
        rfq_data = {
            "solicitation_number": sol,
            "sign_date": get_pst_date(),
            "line_items": items,
        }
        
        output_path = os.path.join(out_dir, f"{sol}_OBS1600_FoodCert_Reytech.pdf")
        
        if bid_pkg and os.path.exists(bid_pkg):
            # Fill OBS 1600 fields in the existing bid package
            fill_obs1600(bid_pkg, rfq_data, config, output_path, food_items=food_items)
            t.ok(f"Filled from bid package template: {os.path.basename(bid_pkg)}")
        else:
            # Generate standalone OBS 1600 using reportlab
            _generate_standalone_obs1600(food_items, config, rfq_data, output_path)
            t.ok("Generated standalone OBS 1600")
        
        return jsonify({
            "ok": True,
            "file": output_path,
            "filename": os.path.basename(output_path),
            "food_items": food_items,
            "food_count": len(food_items),
            "download_url": f"/api/download/{sol}/{os.path.basename(output_path)}",
        })
        
    except Exception as e:
        import traceback
        t.fail(str(e))
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


def _generate_standalone_obs1600(food_items, config, rfq_data, output_path):
    """Generate a standalone OBS 1600 PDF using reportlab when no template is available."""
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.units import inch
    
    company = config["company"]
    sol = rfq_data.get("solicitation_number", "")
    sign_date = rfq_data.get("sign_date", "")
    
    c = rl_canvas.Canvas(output_path, pagesize=letter)
    w, h = letter
    
    # Header
    c.setFont("Helvetica-Bold", 9)
    c.drawString(0.5*inch, h - 0.5*inch, "California Department of Corrections and Rehabilitation/California Correctional Health Care Services")
    c.setFont("Helvetica", 8)
    c.drawString(0.5*inch, h - 0.65*inch, "Office of Business Services - Non-IT Goods Procurement/Acquisitions Management Section, Procurement Services")
    c.drawString(0.5*inch, h - 0.8*inch, "OBS 1600 (Rev. 1/26)")
    
    # Title
    c.setFont("Helvetica-Bold", 12)
    c.drawCentredString(w/2, h - 1.2*inch, "California-Grown/Produced Agricultural Food Products Vendor Certification")
    
    # Vendor info
    y = h - 1.6*inch
    c.setFont("Helvetica-Bold", 10)
    c.drawString(0.5*inch, y, f"Vendor Name : {company['name']}")
    y -= 0.25*inch
    c.drawString(0.5*inch, y, f"Solicitation # : {sol}")
    
    # Table header
    y -= 0.45*inch
    c.setFont("Helvetica-Bold", 8)
    col_x = [0.5*inch, 1.2*inch, 4.5*inch, 5.2*inch, 6.2*inch]
    headers = ["Quoted Line\nItem #", "Food Product Description", "Code", "CA-Grown\nor Produced\n(Yes/No)", "If Yes, % of\nProduct"]
    
    # Header row background
    c.setFillColorRGB(0.9, 0.9, 0.9)
    c.rect(0.5*inch, y - 0.15*inch, 7*inch, 0.45*inch, fill=True, stroke=True)
    c.setFillColorRGB(0, 0, 0)
    
    for i, hdr in enumerate(headers):
        lines = hdr.split("\n")
        for j, line in enumerate(lines):
            c.drawString(col_x[i] + 0.05*inch, y + 0.2*inch - j*0.12*inch, line)
    
    # Data rows
    y -= 0.35*inch
    c.setFont("Helvetica", 9)
    for item in food_items[:18]:
        y -= 0.28*inch
        c.line(0.5*inch, y - 0.05*inch, 7.5*inch, y - 0.05*inch)
        c.drawString(col_x[0] + 0.1*inch, y + 0.05*inch, str(item.get("line_number", "")))
        c.drawString(col_x[1] + 0.05*inch, y + 0.05*inch, item.get("description", "")[:55])
        c.drawCentredString(col_x[2] + 0.35*inch, y + 0.05*inch, str(item.get("code", "")))
        c.drawCentredString(col_x[3] + 0.4*inch, y + 0.05*inch, item.get("ca_grown", "No"))
        c.drawCentredString(col_x[4] + 0.4*inch, y + 0.05*inch, item.get("pct", "N/A"))
    
    # Fill remaining empty rows
    for _ in range(18 - len(food_items)):
        y -= 0.28*inch
        c.line(0.5*inch, y - 0.05*inch, 7.5*inch, y - 0.05*inch)
    
    # Table border
    table_top = h - 2.1*inch
    c.rect(0.5*inch, y - 0.05*inch, 7*inch, table_top - (y - 0.05*inch))
    
    # Certification text
    y -= 0.45*inch
    c.setFont("Helvetica", 7.5)
    c.drawString(0.5*inch, y, "Pursuant to California Code, Food and Agricultural Code, Section 58595(a), I certify under the laws of the State of California")
    y -= 0.15*inch
    c.drawString(0.5*inch, y, "that the above information is true and correct.")
    
    # Signature block
    y -= 0.4*inch
    c.setFont("Helvetica", 10)
    c.drawString(0.5*inch, y, company["owner"])
    c.drawString(3.2*inch, y, company["title"])
    c.drawString(5.5*inch, y, sign_date)
    
    y -= 0.15*inch
    c.line(0.5*inch, y, 2.8*inch, y)
    c.line(3.2*inch, y, 4.8*inch, y)
    c.line(5.5*inch, y, 7.5*inch, y)
    
    y -= 0.15*inch
    c.setFont("Helvetica-Bold", 8)
    c.drawString(0.5*inch, y, "Print Name")
    c.drawString(2.2*inch, y, "Signature")
    c.drawString(3.2*inch, y, "Title")
    c.drawString(5.5*inch, y, "Date")
    
    c.save()


@bp.route("/api/download/<path:sol>/<filename>")
@auth_required
@safe_route
def api_download_file(sol, filename):
    """Download a generated file."""
    import re as _re
    # Sanitize: block path traversal but preserve spaces
    sol = sol.replace("..", "").replace("/", "").replace("\\", "")
    filename = os.path.basename(filename)
    filepath = os.path.join(OUTPUT_DIR, sol, filename)
    # Backwards compat: Compliance_Forms_ → RFQ_Package_ rename
    if not os.path.exists(filepath) and filename.startswith("Compliance_Forms_"):
        _old_name = filename.replace("Compliance_Forms_", "RFQ_Package_", 1)
        _old_path = os.path.join(OUTPUT_DIR, sol, _old_name)
        if os.path.exists(_old_path):
            filepath = _old_path
            filename = _old_name
    if not os.path.exists(filepath):
        # Fallback: try serving from DB
        # sol might be solicitation number OR rfq_id — try both
        try:
            found_file = None
            # Try sol as rfq_id first
            files = list_rfq_files(sol, category="generated")
            for dbf in files:
                if dbf.get("filename") == filename:
                    found_file = dbf
                    break
            # If not found, search all RFQs by solicitation number
            if not found_file:
                rfqs = load_rfqs()
                for rid, r in rfqs.items():
                    r_sol = (r.get("solicitation_number") or r.get("rfq_number") or "").replace("/", "_")
                    if r_sol == sol or rid == sol:
                        files = list_rfq_files(rid, category="generated")
                        for dbf in files:
                            if dbf.get("filename") == filename:
                                found_file = dbf
                                break
                        if found_file:
                            break
            if found_file:
                full = get_rfq_file(found_file["id"])
                if full and full.get("data"):
                    from flask import Response
                    return Response(full["data"], mimetype="application/pdf",
                                    headers={"Content-Disposition": f'inline; filename="{filename}"'})
        except Exception as _e:
            log.warning("DB download fallback failed for %s/%s: %s", sol, filename, _e)
        return jsonify({"ok": False, "error": "File not found"}), 404
    from flask import send_file
    return send_file(filepath, mimetype="application/pdf", download_name=filename)


# ═══════════════════════════════════════════════════════════════════════
# Fill ALL Bid Package Forms (CUF, Darfur, DVBE, CalRecycle, OBS 1600, etc.)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/<rid>/fill-bid-package", methods=["POST"])
@auth_required
@safe_route
def api_fill_bid_package(rid):
    """Fill ALL forms in the CDCR bid package for an RFQ."""
    from src.api.trace import Trace
    t = Trace("fill_bid_package", rfq_id=rid)
    
    try:
        from src.forms.reytech_filler_v4 import load_config, fill_bid_package, get_pst_date
        
        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            t.fail("RFQ not found")
            return jsonify({"ok": False, "error": "RFQ not found"}), 404
        
        config = load_config()
        sol = r.get("solicitation_number", "") or "RFQ"
        
        # Get items
        items = r.get("line_items", [])
        if not items:
            items = r.get("items_detail", r.get("items", []))
            if isinstance(items, str):
                import json as _json
                try: items = _json.loads(items)
                except Exception: items = []
        
        # Find template
        bid_pkg = None
        tmpl = r.get("templates", {})
        if tmpl.get("bidpkg") and os.path.exists(tmpl["bidpkg"]):
            bid_pkg = tmpl["bidpkg"]
        
        # Fallback to saved template
        if not bid_pkg:
            default_tmpl = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "templates", "cdcr_bid_package_template.pdf")
            if os.path.exists(default_tmpl):
                bid_pkg = default_tmpl
        
        if not bid_pkg:
            t.fail("No bid package template found")
            return jsonify({"ok": False, "error": "No bid package template found. Upload one at /form-filler or place in data/templates/"}), 400
        
        out_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "output", sol)
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, f"{sol}_BidPackage_Reytech.pdf")
        
        rfq_data = {
            "solicitation_number": sol,
            "sign_date": get_pst_date(),
            "line_items": items,
        }
        
        fill_bid_package(bid_pkg, rfq_data, config, output_path)
        
        # Count food items
        from src.forms.food_classifier import get_food_items_for_obs1600
        food_items = get_food_items_for_obs1600(items)
        
        t.ok(f"Filled bid package: {len(items)} items, {len(food_items)} food items")
        
        return jsonify({
            "ok": True,
            "filename": os.path.basename(output_path),
            "download_url": f"/api/download/{sol}/{os.path.basename(output_path)}",
            "total_items": len(items),
            "food_items": len(food_items),
            "forms_filled": ["CUF", "Darfur", "Bidder Declaration", "DVBE", "Drug-Free", "CalRecycle", "OBS 1600"],
        })
    except Exception as e:
        import traceback
        t.fail(str(e))
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/fill-forms", methods=["POST"])
@auth_required
@safe_route
def api_fill_forms_standalone():
    """Standalone form filler — fill bid package from manually entered items.
    Body: {
        "solicitation_number": "...",
        "items": [{"line_number": 1, "description": "..."}],
        "fill_type": "all" | "obs1600_only"
    }
    """
    from src.api.trace import Trace
    t = Trace("fill_forms_standalone")
    
    try:
        from src.forms.reytech_filler_v4 import load_config, fill_bid_package, fill_obs1600, get_pst_date
        from src.forms.food_classifier import get_food_items_for_obs1600
        
        data = request.get_json(force=True)
        sol = data.get("solicitation_number", "STANDALONE")
        items = data.get("items", [])
        fill_type = data.get("fill_type", "all")
        
        if not items:
            return jsonify({"ok": False, "error": "No items provided"}), 400
        
        config = load_config()
        
        # Find template
        bid_pkg = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "templates", "cdcr_bid_package_template.pdf")
        if not os.path.exists(bid_pkg):
            return jsonify({"ok": False, "error": "No bid package template found. Upload cdcr_bid_package_template.pdf to data/templates/"}), 400
        
        out_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "output", sol)
        os.makedirs(out_dir, exist_ok=True)
        
        rfq_data = {
            "solicitation_number": sol,
            "sign_date": get_pst_date(),
            "line_items": items,
        }
        
        food_items = get_food_items_for_obs1600(items)
        
        if fill_type == "obs1600_only":
            output_path = os.path.join(out_dir, f"{sol}_OBS1600_FoodCert_Reytech.pdf")
            fill_obs1600(bid_pkg, rfq_data, config, output_path, food_items=food_items)
        else:
            output_path = os.path.join(out_dir, f"{sol}_BidPackage_Reytech.pdf")
            fill_bid_package(bid_pkg, rfq_data, config, output_path)
        
        t.ok(f"Filled {fill_type}: {sol}, {len(food_items)} food items")
        
        return jsonify({
            "ok": True,
            "filename": os.path.basename(output_path),
            "download_url": f"/api/download/{sol}/{os.path.basename(output_path)}",
            "food_items": food_items,
            "food_count": len(food_items),
            "total_items": len(items),
        })
    except Exception as e:
        import traceback
        t.fail(str(e))
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfq/<rid>/price-intel")
@auth_required
@safe_route
def api_rfq_price_intel(rid):
    """Return pricing intelligence for all items in an RFQ."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False}), 404

    intel = []
    for item in r.get("line_items", []):
        desc = item.get("description", "")
        pn = item.get("item_number", "") or ""
        current_cost = item.get("supplier_cost") or 0
        current_bid = item.get("price_per_unit") or 0
        result = {"description": desc[:60], "part_number": pn}

        # Price history
        try:
            from src.core.db import get_price_history_db
            history = get_price_history_db(
                description=desc[:60] if not pn else "",
                part_number=pn, limit=10
            )
            if history:
                prices = [h["unit_price"] for h in history if h.get("unit_price")]
                result["history"] = {
                    "count": len(history),
                    "avg": round(sum(prices) / len(prices), 2) if prices else 0,
                    "min": round(min(prices), 2) if prices else 0,
                    "max": round(max(prices), 2) if prices else 0,
                    "entries": [{
                        "price": h["unit_price"],
                        "source": h.get("source", ""),
                        "date": h.get("found_at", "")[:10],
                        "quote": h.get("quote_number", ""),
                        "agency": h.get("agency", ""),
                    } for h in history[:5]]
                }

                # Freshness: compare current cost vs most recent history
                latest = history[0]
                latest_price = latest.get("unit_price", 0)
                latest_source = latest.get("source", "")
                latest_date = latest.get("found_at", "")[:10]
                try:
                    from datetime import datetime as _dt
                    days_old = (_dt.now() - _dt.fromisoformat(
                        latest["found_at"][:19])).days
                except Exception:
                    days_old = 999

                drift = None
                if current_cost > 0 and latest_price > 0 and latest_source not in ("rfq_save", "rfq_save_bid"):
                    diff = latest_price - current_cost
                    pct = diff / current_cost * 100
                    if abs(pct) > 3:  # Only flag >3% drift
                        drift = {
                            "direction": "up" if diff > 0 else "down",
                            "amount": round(abs(diff), 2),
                            "pct": round(pct, 1),
                            "new_price": latest_price,
                            "source": latest_source,
                        }

                result["freshness"] = {
                    "days_old": days_old,
                    "stale": days_old > 90,
                    "last_source": latest_source,
                    "last_date": latest_date,
                    "drift": drift,
                }
        except Exception as _e:
            log.debug('suppressed in api_rfq_price_intel: %s', _e)

        # Catalog match
        try:
            from src.core.catalog import search_catalog
            matches = search_catalog(pn or desc[:40], limit=1)
            if matches:
                m = matches[0]
                result["catalog"] = {
                    "sku": m.get("sku", ""),
                    "typical_cost": m.get("typical_cost", 0),
                    "list_price": m.get("list_price", 0),
                    "category": m.get("category", ""),
                }
        except Exception as _e:
            log.debug('suppressed in api_rfq_price_intel: %s', _e)

        # Audit trail
        try:
            from src.core.db import get_audit_trail
            audits = get_audit_trail(
                description=desc[:40],
                rfq_id=r.get("solicitation_number", ""), limit=5)
            if audits:
                result["audit"] = [{
                    "field": a["field_changed"],
                    "old": a.get("old_value"),
                    "new": a.get("new_value"),
                    "source": a.get("source", ""),
                    "ts": a.get("ts", "")[:16],
                } for a in audits]
        except Exception as _e:
            log.debug('suppressed in api_rfq_price_intel: %s', _e)

        # Pricing recommendation
        rec = _recommend_price(item)
        if rec:
            result["recommendation"] = rec

        # Loss intelligence — competitive insights from past losses
        try:
            from src.agents.pricing_feedback import get_pricing_recommendation
            agency = r.get("agency", "")
            loss_rec = get_pricing_recommendation(desc, agency, float(current_cost) if current_cost else 0)
            if loss_rec.get("sources_used", 0) > 0:
                result["loss_intelligence"] = {
                    "competitor_floor": loss_rec.get("competitor_floor"),
                    "suggested_range": loss_rec.get("suggested_range"),
                    "margin_warning": loss_rec.get("margin_warning"),
                    "confidence": loss_rec.get("confidence", 0),
                    "loss_count": loss_rec.get("sources_used", 0),
                }
        except Exception as _e:
            log.debug('suppressed in api_rfq_price_intel: %s', _e)

        # F6: Price conflict resolution — all known sources
        sources = {}
        if item.get("supplier_cost") and item["supplier_cost"] > 0:
            sources["Your Cost"] = round(item["supplier_cost"], 2)
        if item.get("scprs_last_price") and item["scprs_last_price"] > 0:
            sources["SCPRS"] = round(item["scprs_last_price"], 2)
        if item.get("amazon_price") and item["amazon_price"] > 0:
            sources["Amazon"] = round(item["amazon_price"], 2)
        if item.get("price_per_unit") and item["price_per_unit"] > 0:
            sources["Current Bid"] = round(item["price_per_unit"], 2)
        if result.get("catalog") and result["catalog"].get("typical_cost"):
            sources["Catalog"] = round(result["catalog"]["typical_cost"], 2)
        if result.get("catalog") and result["catalog"].get("list_price"):
            sources["Catalog List"] = round(result["catalog"]["list_price"], 2)
        if item.get("_from_pc"):
            sources["_from_pc"] = item["_from_pc"]
        if len(sources) > 1:
            result["sources"] = sources

        # F9: Duplicate item detection — same item quoted recently?
        try:
            from src.core.db import get_price_history_db
            pn = item.get("item_number", "") or ""
            recent = get_price_history_db(
                description=desc[:40] if not pn else "",
                part_number=pn, source="rfq_save_bid", limit=3
            )
            if recent:
                dupes = []
                for rh in recent:
                    dupes.append({
                        "price": rh.get("unit_price", 0),
                        "quote": rh.get("quote_number", ""),
                        "agency": rh.get("agency", ""),
                        "date": rh.get("found_at", "")[:10],
                    })
                if dupes:
                    result["recent_quotes"] = dupes
        except Exception as _e:
            log.debug('suppressed in api_rfq_price_intel: %s', _e)

        intel.append(result)

    return jsonify({"ok": True, "intel": intel})


_pricing_alerts_cache = {"data": None, "ts": 0}

@bp.route("/api/pricing-alerts")
@auth_required
@safe_route
def api_pricing_alerts():
    """F8: Dashboard pricing alerts — stale prices, drift, unpriced items."""
    import time as _time
    global _pricing_alerts_cache
    if _pricing_alerts_cache["data"] and (_time.time() - _pricing_alerts_cache["ts"]) < 120:
        return jsonify(_pricing_alerts_cache["data"])
    from datetime import datetime as _dt, timedelta
    rfqs = load_rfqs()
    stale_rfqs = []
    unpriced_rfqs = []
    drift_items = 0
    now = _dt.now()

    for rid, r in rfqs.items():
        if r.get("status") in ("dismissed", "sent", "won", "lost", "cancelled"):
            continue
        items = r.get("line_items", [])
        if not items:
            continue

        # Check for unpriced items
        unpriced = sum(1 for it in items if not (it.get("price_per_unit") or 0) > 0)
        if unpriced == len(items):
            unpriced_rfqs.append({"id": rid, "sol": r.get("solicitation_number", ""), "items": len(items)})
            continue

        # Check for stale pricing (created > 14 days ago, never regenerated)
        try:
            created = r.get("created_at", "")
            if created:
                age = (now - _dt.fromisoformat(created[:19])).days
                if age > 14 and r.get("status") not in ("generated",):
                    stale_rfqs.append({
                        "id": rid, "sol": r.get("solicitation_number", ""),
                        "age_days": age, "items": len(items),
                    })
        except Exception as _e:
            log.debug('suppressed in api_pricing_alerts: %s', _e)

    # Check price_history for recent drift
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Items with multiple prices where latest differs >10% from previous
            rows = conn.execute("""
                SELECT description, COUNT(*) as cnt,
                       MAX(unit_price) as max_p, MIN(unit_price) as min_p
                FROM price_history
                WHERE found_at > ?
                GROUP BY LOWER(SUBSTR(description, 1, 40))
                HAVING cnt > 1 AND (max_p - min_p) / min_p > 0.10
            """, ((now - timedelta(days=30)).isoformat(),)).fetchall()
            drift_items = len(rows)
    except Exception as _e:
        log.debug('suppressed in api_pricing_alerts: %s', _e)

    total_alerts = len(stale_rfqs) + len(unpriced_rfqs) + (1 if drift_items > 0 else 0)
    _pa_result = {
        "ok": True,
        "total_alerts": total_alerts,
        "stale_rfqs": stale_rfqs,
        "unpriced_rfqs": unpriced_rfqs,
        "drift_items": drift_items,
    }
    _pricing_alerts_cache["data"] = _pa_result
    _pricing_alerts_cache["ts"] = _time.time()
    return jsonify(_pa_result)


@bp.route("/api/rfq/<rid>/qa-check")
@auth_required
@safe_route
def api_rfq_qa_check(rid):
    """QA gate: validate all items before package generation.
    Returns per-item pass/warn/fail with reasons."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False}), 404

    items = r.get("line_items", [])
    results = []
    overall = "pass"
    warnings = 0
    failures = 0

    for i, item in enumerate(items):
        checks = []
        item_status = "pass"
        desc = (item.get("description", "") or "")[:50]
        bid = item.get("price_per_unit") or 0
        cost = item.get("supplier_cost") or 0
        scprs = item.get("scprs_last_price") or 0
        qty = item.get("qty") or 0

        # 1: Bid price > 0
        if not bid or bid <= 0:
            checks.append({"check": "bid_price", "status": "fail", "msg": "No bid price"})
            item_status = "fail"
        else:
            checks.append({"check": "bid_price", "status": "pass", "msg": f"${bid:.2f}"})

        # 2: Supplier cost > 0
        if not cost or cost <= 0:
            checks.append({"check": "cost", "status": "warn", "msg": "No supplier cost"})
            if item_status != "fail":
                item_status = "warn"
        else:
            checks.append({"check": "cost", "status": "pass", "msg": f"${cost:.2f}"})

        # 3: Margin ≥ 15%
        if bid > 0 and cost > 0:
            margin = (bid - cost) / bid * 100
            if margin < 5:
                checks.append({"check": "margin", "status": "fail",
                               "msg": f"{margin:.1f}% — dangerously low"})
                item_status = "fail"
            elif margin < 15:
                checks.append({"check": "margin", "status": "warn",
                               "msg": f"{margin:.1f}% — below 15%"})
                if item_status != "fail":
                    item_status = "warn"
            else:
                checks.append({"check": "margin", "status": "pass",
                               "msg": f"{margin:.1f}%"})

        # 4: Bid vs SCPRS
        if bid > 0 and scprs > 0:
            diff_pct = (bid - scprs) / scprs * 100
            if diff_pct > 10:
                checks.append({"check": "scprs", "status": "warn",
                               "msg": f"{diff_pct:.0f}% above SCPRS ${scprs:.2f}"})
                if item_status != "fail":
                    item_status = "warn"
            elif diff_pct < -15:
                checks.append({"check": "scprs", "status": "warn",
                               "msg": f"{abs(diff_pct):.0f}% below SCPRS — leaving margin?"})
                if item_status != "fail":
                    item_status = "warn"
            else:
                checks.append({"check": "scprs", "status": "pass",
                               "msg": f"OK vs SCPRS ${scprs:.2f}"})

        # 5: Price freshness
        try:
            from src.core.db import get_price_history_db
            pn = item.get("item_number", "") or ""
            history = get_price_history_db(
                description=desc[:40] if not pn else "",
                part_number=pn, limit=1)
            if history:
                from datetime import datetime as _dt
                days = (_dt.now() - _dt.fromisoformat(
                    history[0]["found_at"][:19])).days
                if days > 90:
                    checks.append({"check": "freshness", "status": "warn",
                                   "msg": f"Price data {days}d old"})
                    if item_status != "fail":
                        item_status = "warn"
                else:
                    checks.append({"check": "freshness", "status": "pass",
                                   "msg": f"{days}d ago"})
        except Exception as _e:
            log.debug('suppressed in api_rfq_qa_check: %s', _e)

        # 6: Qty > 0
        if not qty or qty <= 0:
            checks.append({"check": "qty", "status": "warn", "msg": "Qty is 0"})
            if item_status != "fail":
                item_status = "warn"

        if item_status == "fail":
            failures += 1
            if overall != "fail":
                overall = "fail"
        elif item_status == "warn":
            warnings += 1
            if overall == "pass":
                overall = "warn"

        results.append({"idx": i, "description": desc,
                        "status": item_status, "checks": checks})

    # PC diff warnings
    diff_notes = []
    pc_diff = r.get("pc_diff")
    if pc_diff:
        if pc_diff.get("added"):
            diff_notes.append(f"{len(pc_diff['added'])} new items not in Price Check")
        if pc_diff.get("removed"):
            diff_notes.append(f"{len(pc_diff['removed'])} PC items not in RFQ")
        if pc_diff.get("qty_changed"):
            diff_notes.append(f"{len(pc_diff['qty_changed'])} qty changes from PC")

    # Requirements validation (email = contract)
    req_gaps = []
    try:
        _req_json = r.get("requirements_json", "{}")
        if _req_json and _req_json != "{}":
            from src.forms.form_qa import validate_against_requirements
            _gen_files = r.get("output_files", [])
            _vr = validate_against_requirements(_gen_files, _req_json, r)
            req_gaps = _vr.get("gaps", [])
            # Count requirement warnings
            for _gap in req_gaps:
                warnings += 1
                if overall == "pass":
                    overall = "warn"
    except Exception as _e:
        log.debug('suppressed in api_rfq_qa_check: %s', _e)

    return jsonify({"ok": True, "overall": overall, "failures": failures,
                    "warnings": warnings, "total": len(items), "items": results,
                    "linked_pc": r.get("linked_pc_number", ""),
                    "diff_notes": diff_notes,
                    "requirement_gaps": req_gaps})


@bp.route("/form-filler")
@auth_required
@safe_page
def form_filler_page():
    """Standalone form filler page."""
    return render_page("form_filler.html", active_page="Forms")


# ═══════════════════════════════════════════════════════════════════════
# Admin: Nuke & Re-poll RFQ
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/nuke/<rid>", methods=["POST"])
@auth_required
@safe_route
def api_nuke_rfq(rid):
    """Nuclear delete: wipe RFQ from JSON + SQLite + processed UIDs, then re-poll.
    Usage: POST /api/rfq/nuke/<rfq_id>  or  POST /api/rfq/nuke/<solicitation_number>
    """
    import json as _json
    from src.api.dashboard import load_rfqs, save_rfqs

    rfqs = load_rfqs()
    nuked = []

    # Find by ID or by solicitation number
    targets = {}
    for k, v in rfqs.items():
        if k == rid or v.get("solicitation_number", "") == rid or v.get("rfq_number", "") == rid:
            targets[k] = v

    if not targets:
        return jsonify({"ok": False, "error": f"No RFQ found matching '{rid}'"}), 404

    for rfq_id, rfq in targets.items():
        sol = rfq.get("solicitation_number", rfq.get("rfq_number", "?"))
        email_uid = rfq.get("email_uid", "")

        # 1. Remove from JSON
        if rfq_id in rfqs:
            del rfqs[rfq_id]

        # 2. Remove from SQLite (rfqs, rfq_files, email_log, price_checks)
        try:
            with get_db() as conn:
                conn.execute("DELETE FROM rfq_files WHERE rfq_id = ?", (rfq_id,))
                conn.execute("DELETE FROM rfqs WHERE id = ?", (rfq_id,))
                # email_log by rfq_id
                conn.execute("DELETE FROM email_log WHERE rfq_id = ?", (rfq_id,))
                # price_checks by rfq_id
                conn.execute("DELETE FROM price_checks WHERE rfq_id = ?", (rfq_id,))
                conn.commit()
        except Exception as e:
            log.warning("Nuke SQLite cleanup for %s: %s", rfq_id, e)

        # 3. Remove email UID from processed list
        if email_uid:
            try:
                from src.api.modules.routes_pricecheck import _remove_processed_uid
                _remove_processed_uid(email_uid)
            except Exception:
                # Manual fallback
                proc_file = os.path.join(DATA_DIR, "processed_emails.json")
                try:
                    if os.path.exists(proc_file):
                        with open(proc_file) as f:
                            processed = _json.load(f)
                        if isinstance(processed, list) and email_uid in processed:
                            processed.remove(email_uid)
                        elif isinstance(processed, dict) and email_uid in processed:
                            del processed[email_uid]
                        with open(proc_file, "w") as f:
                            _json.dump(processed, f)
                except Exception as _e:
                    log.debug('suppressed in api_nuke_rfq: %s', _e)

        nuked.append({"id": rfq_id, "sol": sol, "uid": email_uid})
        log.info("NUKED RFQ %s (sol=%s, uid=%s)", rfq_id, sol, email_uid)

    save_rfqs(rfqs)

    # 4. Trigger re-poll
    poll_result = None
    try:
        from src.api.modules.routes_pricecheck import do_poll_check
        imported = do_poll_check()
        poll_result = {"found": len(imported), "rfqs": [r.get("solicitation_number", "?") for r in imported]}
    except Exception as e:
        poll_result = {"error": str(e)}

    return jsonify({
        "ok": True,
        "nuked": nuked,
        "poll": poll_result,
    })


@bp.route("/api/rfq/<rid>/clear-quote", methods=["POST", "GET"])
@auth_required
@safe_route
def api_rfq_clear_quote(rid):
    """Clear the quote number on an RFQ so regeneration assigns a new one."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    
    old_qn = r.get("reytech_quote_number", "")
    r["reytech_quote_number"] = ""
    r["linked_quote_number"] = ""
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    return jsonify({"ok": True, "cleared": old_qn, "message": f"Cleared {old_qn}. Regenerate to get a new number."})


@bp.route("/api/rfq/<rid>/set-quote-number", methods=["POST"])
@auth_required
@safe_route
def api_rfq_set_quote_number(rid):
    """Force-set the quote number on an RFQ. Used to fix counter drift."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    data = request.get_json(force=True, silent=True) or {}
    qn = data.get("quote_number", "").strip()
    if not qn:
        return jsonify({"ok": False, "error": "Provide quote_number"})
    old = r.get("reytech_quote_number", "")
    r["reytech_quote_number"] = qn
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    log.info("Force-set quote number on RFQ %s: %s → %s", rid, old, qn)
    return jsonify({"ok": True, "old": old, "new": qn})


@bp.route("/api/rfq/<rid>/revise-quote", methods=["POST"])
@auth_required
@safe_route
def api_rfq_revise_quote(rid):
    """Regenerate ONLY the quote PDF with current pricing — keep all other
    package docs unchanged. Saves revision to quote_revisions table.
    Preserves quote number — never burns a new one."""
    from src.api.trace import Trace
    t = Trace("quote_revision", rfq_id=rid)

    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    locked_qn = r.get("reytech_quote_number", "")
    if not locked_qn:
        return jsonify({"ok": False, "error": "No quote number found — generate the full package first"})

    data = request.get_json(force=True, silent=True) or {}
    reason = str(data.get("reason", "Pricing updated")).strip()[:200] or "Pricing updated"

    sol = r.get("solicitation_number", "") or "RFQ"
    safe_sol = re.sub(r'[^a-zA-Z0-9_-]', '_', sol.strip())
    out_dir = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f"{safe_sol}_Quote_Reytech.pdf")

    # Determine revision number
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT MAX(revision_num) as maxrev FROM quote_revisions WHERE quote_number=?",
                (locked_qn,)).fetchone()
            rev_num = (row["maxrev"] or 0) + 1 if row else 1
    except Exception:
        rev_num = (r.get("quote_revision", 0) or 0) + 1

    t.step("Revising", quote_number=locked_qn, revision=rev_num, reason=reason)

    # Snapshot pricing for diff/history
    snapshot_items = [
        {
            "line_number": it.get("line_number", i+1),
            "description": (it.get("description") or "")[:80],
            "qty": it.get("qty", 0),
            "uom": it.get("uom", ""),
            "supplier_cost": it.get("supplier_cost", 0),
            "price_per_unit": it.get("price_per_unit", 0),
            "markup_pct": it.get("markup_pct", 0),
        }
        for i, it in enumerate(r.get("line_items", []))
    ]

    # Generate quote PDF — same quote number, no new allocation
    result = generate_quote_from_rfq(r, output_path, quote_number=locked_qn)

    if not result.get("ok"):
        t.fail("Quote revision failed", error=result.get("error"))
        return jsonify({"ok": False, "error": result.get("error", "Quote generation failed")})

    new_total = result.get("total", 0)

    # Save revision to DB
    import json as _json_rev
    try:
        with get_db() as conn:
            conn.execute(
                """INSERT INTO quote_revisions
                   (quote_number, revision_num, revised_at, reason, snapshot_json, changed_by)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (locked_qn, rev_num, datetime.now().isoformat(),
                 reason, _json_rev.dumps(snapshot_items), "user"))
            conn.commit()
    except Exception as _dbe:
        log.warning("quote_revisions insert failed: %s", _dbe)

    # Update RFQ record
    r["quote_revision"] = rev_num
    r["quote_revised_at"] = datetime.now().isoformat()
    r["quote_revision_reason"] = reason
    r["pricing_snapshot"] = {
        "snapshot_at": datetime.now().isoformat(),
        "quote_number": locked_qn,
        "revision": rev_num,
        "total": new_total,
        "tax_rate": r.get("tax_rate", 0),
        "items": snapshot_items,
    }
    fname = os.path.basename(output_path)
    if "output_files" not in r:
        r["output_files"] = []
    if fname not in r["output_files"]:
        r["output_files"].append(fname)
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    try:
        from src.core.dal import log_lifecycle_event
        log_lifecycle_event("rfq", rid, "quote_revised",
            f"Quote {locked_qn} Rev {rev_num} — ${new_total:,.2f} — {reason}",
            actor="user",
            detail={"quote_number": locked_qn, "revision": rev_num, "total": new_total, "reason": reason})
    except Exception as _e:
        log.debug('suppressed in api_rfq_revise_quote: %s', _e)

    t.ok("Quote revised", revision=rev_num, total=new_total)
    log.info("Quote %s Rev %d generated for RFQ %s — $%.2f", locked_qn, rev_num, rid, new_total)
    return jsonify({
        "ok": True,
        "quote_number": locked_qn,
        "revision": rev_num,
        "total": new_total,
        "download": f"/api/download/{sol}/{fname}",
    })


@bp.route("/api/rfq/<rid>/revision-history", methods=["GET"])
@auth_required
@safe_route
def api_rfq_revision_history(rid):
    """Return quote revision history for an RFQ."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "not found"})
    locked_qn = r.get("reytech_quote_number", "")
    if not locked_qn:
        return jsonify({"ok": True, "revisions": [], "quote_number": ""})
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT revision_num, revised_at, reason, changed_by, snapshot_json
                   FROM quote_revisions WHERE quote_number=?
                   ORDER BY revision_num DESC LIMIT 20""",
                (locked_qn,)).fetchall()
        import json as _jh
        revisions = []
        for row in rows:
            snap = []
            try:
                snap = _jh.loads(row["snapshot_json"] or "[]")
            except Exception as _e:
                log.debug('suppressed in api_rfq_revision_history: %s', _e)
            total = sum((it.get("price_per_unit", 0) or 0) * (it.get("qty", 0) or 0) for it in snap)
            revisions.append({
                "revision_num": row["revision_num"],
                "revised_at": row["revised_at"],
                "reason": row["reason"],
                "changed_by": row["changed_by"],
                "total": round(total, 2),
                "item_count": len(snap),
            })
        return jsonify({"ok": True, "quote_number": locked_qn, "revisions": revisions})
    except Exception as e:
        log.warning("revision-history for %s: %s", rid, e)
        return jsonify({"ok": True, "revisions": [], "note": str(e)})


@bp.route("/api/rfq/<rid>/revert-pricing", methods=["POST"])
@auth_required
@safe_route
def api_rfq_revert_pricing(rid):
    """Revert line item prices to the last generated quote snapshot."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    snapshot = r.get("pricing_snapshot")
    if not snapshot:
        return jsonify({"ok": False, "error": "No pricing snapshot found. Generate a quote first."})
    snap_items = snapshot.get("items", [])
    if len(snap_items) != len(r.get("line_items", [])):
        return jsonify({"ok": False, "error": "Item count changed since snapshot — cannot auto-revert"})
    for i, snap in enumerate(snap_items):
        item = r["line_items"][i]
        item["supplier_cost"] = snap.get("supplier_cost", 0)
        item["price_per_unit"] = snap.get("price_per_unit", 0)
        item["markup_pct"] = snap.get("markup_pct", 0)
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    log.info("Reverted pricing on RFQ %s to snapshot %s", rid, snapshot.get("quote_number", ""))
    return jsonify({
        "ok": True,
        "reverted_to": snapshot.get("quote_number", ""),
        "snapshot_at": snapshot.get("snapshot_at", ""),
        "items": len(snap_items),
    })


@bp.route("/api/admin/relink-rfq/<rid>", methods=["POST", "GET"])
@auth_required
@safe_route
def api_admin_relink_rfq(rid):
    """Re-run auto-linking on an existing RFQ to find its matching PC.
    GET-accessible for browser use. Example: /api/admin/relink-rfq/abc123
    """
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    _trace = []
    try:
        from src.api.dashboard import _link_rfq_to_pc
        linked = _link_rfq_to_pc(r, _trace)
        if linked:
            from src.api.dashboard import _save_single_rfq
            _save_single_rfq(rid, r)
            return jsonify({
                "ok": True,
                "linked": True,
                "linked_pc_id": r.get("linked_pc_id", ""),
                "linked_pc_number": r.get("linked_pc_number", ""),
                "match_reason": r.get("linked_pc_match_reason", ""),
                "ported_items": r.get("pc_diff", {}).get("ported", 0),
                "trace": _trace,
            })
        return jsonify({"ok": True, "linked": False, "trace": _trace,
                        "message": "No matching PC found for this RFQ"})
    except Exception as e:
        log.error("relink-rfq %s: %s", rid, e, exc_info=True)
        return jsonify({"ok": False, "error": str(e), "trace": _trace})


@bp.route("/api/admin/fix-quote-number/<rid>/<new_qn>/<int:counter_seq>", methods=["POST", "GET"])
@auth_required
@safe_route
def api_admin_fix_quote_number(rid, new_qn, counter_seq):
    """One-shot admin: set RFQ quote number + reset counter. GET-accessible for browser.

    Example: /api/admin/fix-quote-number/cab4bad5/R26Q31/31
    Sets RFQ cab4bad5 to R26Q31, counter to 31 (next = R26Q32).
    """
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    old_qn = r.get("reytech_quote_number", "")
    r["reytech_quote_number"] = new_qn
    # Also update in output files and generated package data
    if r.get("quote_number"):
        r["quote_number"] = new_qn
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    # Reset counter
    try:
        from src.forms.quote_generator import set_quote_counter, peek_next_quote_number
        set_quote_counter(seq=counter_seq)
        nxt = peek_next_quote_number()
    except Exception as e:
        nxt = f"error: {e}"
    log.warning("ADMIN fix-quote: RFQ %s: %s → %s, counter → %d (next: %s)", rid, old_qn, new_qn, counter_seq, nxt)
    return jsonify({
        "ok": True,
        "rfq": rid,
        "old_quote": old_qn,
        "new_quote": new_qn,
        "counter_set_to": counter_seq,
        "next_quote_will_be": nxt,
    })


@bp.route("/api/rfq/<rid>/clear-generated", methods=["POST", "GET"])
@auth_required
@safe_route
def api_rfq_clear_generated(rid):
    """
    Force-clear all generated files for an RFQ from both DB and JSON.
    Resets status to 'ready' so the full generate-package pipeline re-runs cleanly.
    Use this when Railway redeploys cached the old output and Regenerate doesn't help.
    """
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    # Clear DB generated files
    db_deleted = 0
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cur = conn.execute(
                "DELETE FROM rfq_files WHERE rfq_id = ? AND category = 'generated'",
                (rid,)
            )
            db_deleted = cur.rowcount
    except Exception as _e:
        log.warning("clear-generated DB delete failed for %s: %s", rid, _e)

    # Clear disk output files
    sol = r.get("solicitation_number", rid)
    out_dir = os.path.join(OUTPUT_DIR, sol)
    disk_deleted = 0
    if os.path.exists(out_dir):
        try:
            for fname in os.listdir(out_dir):
                fpath = os.path.join(out_dir, fname)
                if os.path.isfile(fpath):
                    os.remove(fpath)
                    disk_deleted += 1
        except Exception as _de:
            log.warning("clear-generated disk delete failed: %s", _de)

    # Reset JSON state
    old_files = r.get("output_files", [])
    r["output_files"] = []
    r.pop("draft_email", None)
    r.pop("generated_at", None)
    _transition_status(r, "ready", actor="user", notes="Cleared generated files for fresh regeneration")
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "ready")
    except Exception as _e:
        log.debug('suppressed in api_rfq_clear_generated: %s', _e)

    msg = f"Cleared {db_deleted} DB files + {disk_deleted} disk files. Status reset to 'ready'. Click Generate Package to rebuild."
    log.info("clear-generated %s: %s", rid, msg)
    return jsonify({"ok": True, "db_deleted": db_deleted, "disk_deleted": disk_deleted,
                    "old_files": old_files, "message": msg})


@bp.route("/api/rfq/<rid>/clean-slate", methods=["POST", "GET"])
@auth_required
@safe_route
def api_rfq_clean_slate(rid):
    """Nuclear clean: keep ONLY line_items with pricing. Clear everything else.
    Use when package is broken — stale templates, wrong forms, old data."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    # Preserve line items with all pricing fields
    items = r.get("line_items", [])
    preserved_items = []
    for it in items:
        preserved_items.append({
            "line_number": it.get("line_number", 0),
            "qty": it.get("qty", 1),
            "uom": it.get("uom", "EA"),
            "description": it.get("description", ""),
            "item_number": it.get("item_number", ""),
            "supplier_cost": it.get("supplier_cost", 0),
            "price_per_unit": it.get("price_per_unit", 0),
            "markup_pct": it.get("markup_pct"),
            "scprs_last_price": it.get("scprs_last_price"),
            "amazon_price": it.get("amazon_price"),
            "item_link": it.get("item_link", ""),
            "item_supplier": it.get("item_supplier", ""),
            "_desc_source": it.get("_desc_source", ""),
        })

    # Preserve core RFQ identity
    sol = r.get("solicitation_number", "")
    identity = {
        "solicitation_number": sol,
        "agency": r.get("agency", ""),
        "requestor_name": r.get("requestor_name", ""),
        "requestor_email": r.get("requestor_email", ""),
        "delivery_location": r.get("delivery_location", ""),
        "due_date": r.get("due_date", ""),
        "institution": r.get("institution", ""),
        "ship_to": r.get("ship_to", ""),
        "created_at": r.get("created_at", ""),
        "source": r.get("source", ""),
        "linked_pc_id": r.get("linked_pc_id", ""),
        "reytech_quote_number": r.get("reytech_quote_number", ""),
    }

    # Clear DB files (generated + templates)
    db_deleted = 0
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cur = conn.execute(
                "DELETE FROM rfq_files WHERE rfq_id = ? AND category IN ('generated', 'template')",
                (rid,)
            )
            db_deleted = cur.rowcount
    except Exception as _e:
        log.warning("clean-slate DB: %s", _e)

    # Clear disk
    disk_deleted = 0
    import shutil as _sh2
    out_dir = os.path.join(OUTPUT_DIR, sol)
    if os.path.exists(out_dir):
        try:
            _sh2.rmtree(out_dir)
            disk_deleted += 1
        except Exception as _e:
            log.debug('suppressed in api_rfq_clean_slate: %s', _e)
    tmpl_dir = os.path.join(DATA_DIR, "rfq_templates", rid)
    if os.path.exists(tmpl_dir):
        try:
            _sh2.rmtree(tmpl_dir)
            disk_deleted += 1
        except Exception as _e:
            log.debug('suppressed in api_rfq_clean_slate: %s', _e)

    # Rebuild RFQ with clean state
    r.clear()
    r.update(identity)
    r["line_items"] = preserved_items
    r["templates"] = {}
    r["output_files"] = []
    r["status"] = "ready"

    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "ready")
    except Exception as _e:
        log.debug('suppressed in api_rfq_clean_slate: %s', _e)

    log.info("clean-slate %s: kept %d items, cleared %d DB + %d disk",
             rid, len(preserved_items), db_deleted, disk_deleted)
    return jsonify({
        "ok": True,
        "items_preserved": len(preserved_items),
        "db_cleared": db_deleted,
        "disk_cleared": disk_deleted,
        "message": f"Clean slate: {len(preserved_items)} items preserved with pricing. All docs/templates cleared. Ready to regenerate.",
    })


@bp.route("/api/rfq/<rid>/debug-pages", methods=["GET"])
@auth_required
@safe_route
def api_rfq_debug_pages(rid):
    """Debug: run page-skip logic against last generated package PDF. Returns per-page decisions."""
    from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason
    from pypdf import PdfReader

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    sol = r.get("solicitation_number", rid)
    pkg_path = os.path.join(OUTPUT_DIR, sol, f"RFQ_Package_{sol}_ReytechInc.pdf")

    if not os.path.exists(pkg_path):
        return jsonify({"ok": False, "error": f"Not found: {pkg_path}"})

    reader = PdfReader(pkg_path)
    pages = []
    for i, page in enumerate(reader.pages):
        try:
            reason = _bidpkg_page_skip_reason(page)
        except Exception as e:
            reason = f"ERROR: {e}"
        text_snip = (page.extract_text() or "")[:80].replace("\n", " ")
        n_fields = len(page.get("/Annots", [])) if "/Annots" in page else 0
        pages.append({"page": i, "decision": "SKIP" if reason else "KEEP",
                      "reason": reason or "", "fields": n_fields, "text": text_snip})

    return jsonify({"ok": True, "total": len(pages),
                    "kept": sum(1 for p in pages if p["decision"] == "KEEP"),
                    "skipped": sum(1 for p in pages if p["decision"] == "SKIP"),
                    "pages": pages})


@bp.route("/api/rfq/<rid>/debug-templates", methods=["GET"])
@auth_required
@safe_route
def api_rfq_debug_templates(rid):
    """Dump all field names from uploaded 703B/704B/bidpkg templates. Use to diagnose fill mismatches."""
    from pypdf import PdfReader

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    result = {}
    tmpl = r.get("templates", {})

    # Also restore from DB if needed
    db_files = list_rfq_files(rid, category="template")
    for db_f in db_files:
        ft = db_f.get("file_type", "").lower().replace("template_", "")
        fname = db_f.get("filename", "").lower()
        ttype = None
        if "703b" in ft or "703b" in fname: ttype = "703b"
        elif "704b" in ft or "704b" in fname: ttype = "704b"
        elif "bid" in ft or "bid" in fname: ttype = "bidpkg"
        if ttype and (ttype not in tmpl or not os.path.exists(tmpl.get(ttype, ""))):
            full_f = get_rfq_file(db_f["id"])
            if full_f and full_f.get("data"):
                restore_dir = os.path.join(DATA_DIR, "rfq_templates", rid)
                os.makedirs(restore_dir, exist_ok=True)
                restore_path = os.path.join(restore_dir, db_f["filename"])
                with open(restore_path, "wb") as _fw:
                    _fw.write(full_f["data"])
                tmpl[ttype] = restore_path

    for tname, tpath in tmpl.items():
        if not os.path.exists(tpath):
            result[tname] = {"error": f"file missing: {tpath}"}
            continue
        try:
            rdr = PdfReader(tpath)
            fields = rdr.get_fields() or {}
            sig_fields = []
            all_pages = []
            for i, pg in enumerate(rdr.pages):
                annots = pg.get("/Annots", [])
                pg_fields = []
                for a in (annots or []):
                    obj = a.get_object() if hasattr(a, "get_object") else a
                    name = str(obj.get("/T", ""))
                    ft_val = str(obj.get("/FT", ""))
                    if ft_val == "/Sig" or "sig" in name.lower():
                        sig_fields.append({"name": name, "ft": ft_val, "page": i})
                    if name:
                        pg_fields.append(name)
                all_pages.append({"page": i, "fields": pg_fields[:10]})
            result[tname] = {
                "path": tpath,
                "pages": len(rdr.pages),
                "total_fields": len(fields),
                "sig_fields": sig_fields,
                "all_field_names": sorted(fields.keys())[:50],
                "pages_preview": all_pages,
            }
        except Exception as e:
            result[tname] = {"error": str(e)}

    return jsonify({"ok": True, "templates": result})


@bp.route("/api/rfq/<rid>/diag-package")
@auth_required
@safe_route
def api_diag_package(rid):
    """Diagnostic: test each form generation step and report what works/fails."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    results = {"rid": rid, "items": len(r.get("line_items", [])), "steps": []}
    
    # Check agency
    try:
        from src.core.agency_config import match_agency
        _agency_key, _agency_cfg = match_agency(r)
        _req = _agency_cfg.get("required_forms", [])
        results["agency"] = _agency_key
        results["required_forms"] = _req
        results["steps"].append({"step": "agency_match", "ok": True, "agency": _agency_key, "forms": _req})
    except Exception as e:
        results["steps"].append({"step": "agency_match", "ok": False, "error": str(e)})
        return jsonify(results)
    
    # Check templates dir
    import os
    tdir = os.path.join(DATA_DIR, "templates")
    if os.path.exists(tdir):
        files = os.listdir(tdir)
        results["steps"].append({"step": "templates_dir", "ok": True, "files": files})
    else:
        results["steps"].append({"step": "templates_dir", "ok": False, "error": f"{tdir} not found"})
    
    # Check quote generator
    try:
        results["steps"].append({"step": "quote_gen_import", "ok": True})
    except Exception as e:
        results["steps"].append({"step": "quote_gen_import", "ok": False, "error": str(e)})
    
    # Check each required form's generator
    form_checks = {
        "calrecycle74": ("src.forms.reytech_filler_v4", "fill_calrecycle_standalone"),
        "std204": ("src.forms.reytech_filler_v4", "fill_std204"),
        "std1000": ("src.forms.reytech_filler_v4", "fill_std1000"),
        "dvbe843": ("src.forms.reytech_filler_v4", "generate_dvbe_843"),
        "bidder_decl": ("src.forms.reytech_filler_v4", "generate_bidder_declaration"),
        "darfur_act": ("src.forms.reytech_filler_v4", "generate_darfur_act"),
        "cv012_cuf": ("src.forms.reytech_filler_v4", "fill_cv012_cuf"),
    }
    for form_id, (mod, func) in form_checks.items():
        if form_id in _req:
            try:
                m = __import__(mod, fromlist=[func])
                fn = getattr(m, func)
                results["steps"].append({"step": f"import_{form_id}", "ok": True, "func": func})
            except Exception as e:
                results["steps"].append({"step": f"import_{form_id}", "ok": False, "error": str(e)})
    
    # Check CONFIG
    try:
        from src.api.modules.routes_rfq import CONFIG
        results["steps"].append({"step": "config", "ok": True, "company": CONFIG.get("company", {}).get("name", "?")})
    except Exception as e:
        results["steps"].append({"step": "config", "ok": False, "error": str(e)})
    
    # Check line items have pricing
    items = r.get("line_items", [])
    priced = sum(1 for i in items if i.get("price_per_unit") and i["price_per_unit"] > 0)
    results["steps"].append({"step": "pricing", "items": len(items), "priced": priced})

    return jsonify(results)


# ══ Consolidated from routes_features*.py ══════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════
# Email Draft Queue Status
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email/queue-status")
@auth_required
@safe_route
def api_email_queue_status():
    """Status of email drafts: pending, approved, sent."""
    outbox_path = os.path.join(DATA_DIR, "outbox.json")
    try:
        with open(outbox_path) as f:
            outbox = json.load(f)
    except Exception:
        outbox = []

    if isinstance(outbox, dict):
        outbox = list(outbox.values())

    draft = [e for e in outbox if (e.get("status") or "").lower() in ("draft", "pending")]
    approved = [e for e in outbox if (e.get("status") or "").lower() == "approved"]
    sent = [e for e in outbox if (e.get("status") or "").lower() == "sent"]

    return jsonify({
        "ok": True,
        "drafts": len(draft),
        "approved": len(approved),
        "sent": len(sent),
        "total": len(outbox),
        "needs_review": len(draft),
        "ready_to_send": len(approved),
        "recent_drafts": [
            {"to": e.get("to", "?"), "subject": e.get("subject", "?")[:50],
             "created": e.get("created", "?"), "type": e.get("type", "?")}
            for e in sorted(draft, key=lambda x: x.get("created", ""), reverse=True)[:5]
        ]
    })


# ═══════════════════════════════════════════════════════════════════════
# RFQs Ready to Quote
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/ready-to-quote")
@auth_required
@safe_route
def api_rfq_ready_to_quote():
    """RFQs that need pricing/quoting — prioritized by deadline."""
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    if not os.path.exists(rfqs_path):
        return jsonify({"ok": True, "rfqs": [], "count": 0})

    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
    except Exception:
        return jsonify({"ok": True, "rfqs": [], "count": 0})

    today = datetime.now().strftime("%Y-%m-%d")
    ready = []

    for rid, r in rfqs.items():
        status = (r.get("status") or "").lower()
        if status in ("new", "draft", "priced", "inbox"):
            due = r.get("due_date") or r.get("deadline") or ""
            sol = r.get("solicitation_number", rid)
            items = r.get("line_items") or r.get("items_detail") or []
            if isinstance(items, str):
                try: items = json.loads(items)
                except Exception: items = []

            overdue = due and due < today
            days_left = None
            if due:
                try:
                    dd = datetime.strptime(due[:10], "%Y-%m-%d")
                    days_left = (dd - datetime.now()).days
                except (ValueError, TypeError) as e:
                    log.debug("due_date parse %r: %s", due, e)

            ready.append({
                "id": rid,
                "solicitation": sol[:30],
                "requestor": r.get("requestor", r.get("buyer_name", "?")),
                "institution": r.get("institution", "?"),
                "status": status.upper(),
                "items": len(items) if isinstance(items, list) else 0,
                "due": due[:10] if due else "TBD",
                "days_left": days_left,
                "overdue": overdue,
                "total": r.get("total_price", 0),
            })

    # Sort: overdue first, then by days_left
    ready.sort(key=lambda x: (not x["overdue"], x["days_left"] if x["days_left"] is not None else 999))

    return jsonify({
        "ok": True,
        "rfqs": ready[:20],
        "count": len(ready),
        "overdue": len([r for r in ready if r["overdue"]]),
        "due_this_week": len([r for r in ready if r.get("days_left") is not None and 0 <= r["days_left"] <= 7])
    })


@bp.route("/api/rfq/<rid>/clean-items", methods=["POST"])
@auth_required
@safe_route
def rfq_clean_items(rid):
    """Remove junk items (legal text, instructions, boilerplate) from an RFQ."""
    from src.api.dashboard import load_rfqs, save_rfqs
    rfqs = load_rfqs()
    rfq = rfqs.get(rid)
    if not rfq:
        return jsonify({"ok": False, "error": "RFQ not found"})

    items = rfq.get("line_items", [])
    original_count = len(items)

    from src.forms.price_check import _filter_junk_items
    cleaned = _filter_junk_items(items)

    rfq["line_items"] = cleaned
    if "parsed" in rfq:
        rfq["parsed"]["line_items"] = cleaned

    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, rfq)

    removed = original_count - len(cleaned)
    return jsonify({"ok": True, "removed": removed, "kept": len(cleaned), "original": original_count})


# ═══════════════════════════════════════════════════════════════════
# Package Manifest + Lifecycle API
# ═══════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/<rid>/manifest")
@auth_required
@safe_route
def api_rfq_manifest(rid):
    """Get the latest package manifest for an RFQ."""
    from src.core.dal import get_latest_manifest
    manifest = get_latest_manifest(rid)
    if not manifest:
        return jsonify({"ok": False, "error": "No package manifest found"})
    return jsonify({"ok": True, "manifest": manifest})


@bp.route("/api/rfq/<rid>/manifest/<int:manifest_id>/review", methods=["POST"])
@auth_required
@safe_route
def api_rfq_review_form(rid, manifest_id):
    """Record a review verdict for a form in the manifest."""
    from src.core.dal import review_form, log_lifecycle_event
    data = request.get_json(force=True, silent=True) or {}
    form_id = data.get("form_id", "")
    verdict = data.get("verdict", "approved")
    notes = data.get("notes", "")
    if not form_id:
        return jsonify({"ok": False, "error": "form_id required"})
    ok = review_form(manifest_id, form_id, verdict, reviewed_by="user", notes=notes)
    if ok:
        log_lifecycle_event("rfq", rid, "form_reviewed",
            f"Form {form_id}: {verdict}" + (f" — {notes}" if notes else ""),
            actor="user", detail={"form_id": form_id, "verdict": verdict, "manifest_id": manifest_id})
    return jsonify({"ok": ok})


@bp.route("/api/rfq/<rid>/manifest/<int:manifest_id>/approve", methods=["POST"])
@auth_required
@safe_route
def api_rfq_approve_package(rid, manifest_id):
    """Approve the entire package (all forms must be reviewed first).
    Pass ?force=1 to skip pending/rejected/QA checks."""
    from src.core.dal import get_package_manifest, update_manifest_status, log_lifecycle_event
    manifest = get_package_manifest(manifest_id)
    if not manifest:
        return jsonify({"ok": False, "error": "Manifest not found"})
    force = request.args.get("force") == "1"
    if not force:
        pending = [r for r in manifest.get("reviews", []) if r.get("verdict") == "pending"]
        if pending:
            return jsonify({"ok": False, "error": f"{len(pending)} forms still pending review",
                            "pending": [r["form_id"] for r in pending]})
        rejected = [r for r in manifest.get("reviews", []) if r.get("verdict") == "rejected"]
        if rejected:
            return jsonify({"ok": False, "error": f"{len(rejected)} forms rejected",
                            "rejected": [r["form_id"] for r in rejected]})
        # Block if Form QA failed
        field_audit = manifest.get("field_audit") or {}
        if isinstance(field_audit, str):
            try:
                field_audit = json.loads(field_audit)
            except Exception:
                field_audit = {}
        if field_audit.get("_qa_passed") is False:
            qa_issues = field_audit.get("_qa_summary", {}).get("critical_issues", [])
            return jsonify({"ok": False,
                            "error": f"Form QA failed with {len(qa_issues)} critical issue(s). Regenerate the package to fix.",
                            "qa_issues": qa_issues[:5]})
    ok = update_manifest_status(manifest_id, "approved")
    if ok:
        log_lifecycle_event("rfq", rid, "package_approved",
            f"Package v{manifest.get('version', '?')} approved ({manifest.get('total_forms', 0)} forms)",
            actor="user", detail={"manifest_id": manifest_id, "version": manifest.get("version")})
        # Update RFQ status to ready_to_send
        try:
            rfqs = load_rfqs()
            r = rfqs.get(rid, {})
            r["status"] = "ready_to_send"
            if not r.get("draft_email"):
                r["draft_email"] = {
                    "to": r.get("requestor_email", ""),
                    "subject": f"Reytech Inc. — RFQ Response #{r.get('solicitation_number', '')}",
                    "body": "Please find attached our bid response package.",
                }
            _save_single_rfq(rid, r)
        except Exception as _e:
            log.warning("approve_package: status update failed: %s", _e)
    return jsonify({"ok": ok, "status": "approved"})


@bp.route("/api/rfq/<rid>/manifest/<int:manifest_id>/remove-form", methods=["POST"])
@auth_required
@safe_route
def api_rfq_remove_form(rid, manifest_id):
    """Remove a form from the package manifest and delete its file."""
    data = request.get_json(force=True, silent=True) or {}
    form_id = data.get("form_id", "")
    if not form_id:
        return jsonify({"ok": False, "error": "form_id required"})

    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Verify manifest belongs to this RFQ
            _owner = conn.execute(
                "SELECT rfq_id FROM package_manifest WHERE id = ?",
                (manifest_id,)).fetchone()
            if not _owner or _owner[0] != rid:
                return jsonify({"ok": False, "error": "Manifest not found for this RFQ"})

            # Get the review record to find the filename
            row = conn.execute(
                "SELECT form_filename FROM package_review WHERE manifest_id = ? AND form_id = ?",
                (manifest_id, form_id)).fetchone()
            filename = row[0] if row else ""

            # Delete the review record
            conn.execute(
                "DELETE FROM package_review WHERE manifest_id = ? AND form_id = ?",
                (manifest_id, form_id))

            # Update the manifest's generated_forms list
            manifest_row = conn.execute(
                "SELECT generated_forms, total_forms FROM package_manifest WHERE id = ?",
                (manifest_id,)).fetchone()
            if manifest_row:
                import json as _json_rm
                gen_forms = _json_rm.loads(manifest_row[0] or "[]")
                gen_forms = [f for f in gen_forms if (f.get("form_id") if isinstance(f, dict) else f) != form_id]
                total = (manifest_row[1] or 0) - 1
                conn.execute(
                    "UPDATE package_manifest SET generated_forms = ?, total_forms = ? WHERE id = ?",
                    (_json_rm.dumps(gen_forms), max(total, 0), manifest_id))

            # Delete the actual file from disk
            if filename:
                rfqs = load_rfqs()
                r = rfqs.get(rid, {})
                sol = r.get("solicitation_number", "") or r.get("rfq_number", "") or "RFQ"
                filepath = os.path.join(OUTPUT_DIR, sol, filename)
                if os.path.exists(filepath):
                    os.remove(filepath)
                    log.info("Removed file %s for form %s", filename, form_id)

                # Remove from rfq output_files list
                out_files = r.get("output_files", [])
                if filename in out_files:
                    out_files.remove(filename)
                    r["output_files"] = out_files
                    from src.api.dashboard import _save_single_rfq
                    _save_single_rfq(rid, r)

            # Log the removal
            from src.core.dal import log_lifecycle_event
            log_lifecycle_event("rfq", rid, "form_removed",
                f"Removed {form_id} from package ({filename})",
                actor="user", detail={"form_id": form_id, "filename": filename, "manifest_id": manifest_id})

        return jsonify({"ok": True, "removed": form_id, "filename": filename})
    except Exception as e:
        log.error("Remove form failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/rfq/<rid>/refill/<form_id>", methods=["POST"])
@auth_required
@safe_route
def api_rfq_refill_form(rid, form_id):
    """Refill a single form with updated data — inline editing from review page.

    Accepts field_overrides in JSON body, merges into RFQ data, regenerates
    only the specified form, runs QA, and resets verdict to pending.
    """
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    data = request.get_json(force=True, silent=True) or {}
    overrides = data.get("field_overrides", {})

    # ── Merge overrides into RFQ ──
    # Line item overrides: [{index: 0, price_per_unit: 123.45}, ...]
    items = r.get("line_items", r.get("items", []))
    for item_override in overrides.get("line_items", []):
        idx = item_override.get("index")
        if idx is not None and 0 <= idx < len(items):
            for k, v in item_override.items():
                if k == "index":
                    continue
                # 704B guard: block buyer-field changes
                if form_id == "704b" and k in ("description", "qty", "uom", "department"):
                    continue
                items[idx][k] = v
    r["line_items"] = items
    r["items"] = items

    # Top-level overrides (solicitation_number, custom_notes, etc.)
    for k, v in overrides.items():
        if k == "line_items":
            continue
        r[k] = v

    # Update sign date
    try:
        from src.forms.reytech_filler_v4 import get_pst_date
        r["sign_date"] = get_pst_date()
    except ImportError:
        from datetime import datetime as _dt
        r["sign_date"] = _dt.now().strftime("%m/%d/%Y")

    # Save updated RFQ
    from src.api.dashboard import _save_single_rfq
    _save_single_rfq(rid, r)

    # ── Find output path and template for this form ──
    sol = r.get("solicitation_number", "") or r.get("rfq_number", "") or "RFQ"
    out_dir = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out_dir, exist_ok=True)
    tmpl = r.get("templates", {})

    # Map form_id → fill function + template + output filename
    TEMPLATE_DIR = os.path.join(DATA_DIR, "templates")
    FORM_MAP = {
        "703b":          {"fn": "fill_703b",    "tmpl_key": "703b",    "filename": f"{sol}_703B_Reytech.pdf"},
        "703c":          {"fn": "fill_703c",    "tmpl_key": "703c",    "filename": f"{sol}_703C_Reytech.pdf"},
        "704b":          {"fn": "fill_704b",    "tmpl_key": "704b",    "filename": f"{sol}_704B_Reytech.pdf"},
        "bidpkg":        {"fn": "fill_bid_package", "tmpl_key": "bidpkg", "filename": f"{sol}_BidPackage_Reytech.pdf"},
        "calrecycle74":  {"fn": "fill_calrecycle_standalone", "tmpl_file": "calrecycle_74_blank.pdf", "filename": f"{sol}_CalRecycle74_Reytech.pdf"},
        "darfur_act":    {"fn": "fill_darfur_standalone", "tmpl_file": "darfur_act_blank.pdf", "filename": f"{sol}_DarfurAct_Reytech.pdf"},
        "darfur":        {"fn": "fill_darfur_standalone", "tmpl_file": "darfur_act_blank.pdf", "filename": f"{sol}_DarfurAct_Reytech.pdf"},  # alias
        "cv012_cuf":     {"fn": "fill_cv012_cuf", "tmpl_file": "cv012_cuf_blank.pdf", "filename": f"{sol}_CV012_CUF_Reytech.pdf"},
        "std204":        {"fn": "fill_std204",  "tmpl_file": "std204_blank.pdf", "filename": f"{sol}_STD204_Reytech.pdf"},
        "std205":        {"fn": "fill_std205",  "tmpl_file": "std205_blank.pdf", "filename": f"{sol}_STD205_Reytech.pdf"},
        "std1000":       {"fn": "fill_std1000", "tmpl_file": "std1000_blank.pdf", "filename": f"{sol}_STD1000_Reytech.pdf"},
        "bidder_decl":   {"fn": "fill_bidder_declaration", "tmpl_file": "bidder_declaration_blank.pdf", "filename": f"{sol}_BidderDecl_Reytech.pdf"},
        "dvbe843":       {"fn": "generate_dvbe_843", "tmpl_file": "dvbe_843_blank.pdf", "filename": f"{sol}_DVBE843_Reytech.pdf", "no_template_arg": True},
        "sellers_permit": {"fn": "_copy_static", "tmpl_file": "sellers_permit_reytech.pdf", "filename": f"{sol}_SellersPermit_Reytech.pdf", "static": True},
    }

    if form_id == "quote":
        # Quote uses a separate generation path
        try:
            from src.forms.quote_generator import generate_quote_from_rfq
            locked_qn = r.get("reytech_quote_number", "")
            _q_output = os.path.join(out_dir, f"{sol}_Quote_Reytech.pdf")
            result = generate_quote_from_rfq(r, _q_output, quote_number=locked_qn)
            if not result.get("ok"):
                return jsonify({"ok": False, "error": f"Quote refill failed: {result.get('error')}"})
        except Exception as e:
            return jsonify({"ok": False, "error": f"Quote refill error: {e}"}), 500
    elif form_id in FORM_MAP:
        fm = FORM_MAP[form_id]
        # Resolve template path
        if "tmpl_key" in fm:
            template_path = tmpl.get(fm["tmpl_key"], "")
        else:
            template_path = os.path.join(TEMPLATE_DIR, fm["tmpl_file"])
        if not template_path or not os.path.exists(template_path):
            return jsonify({"ok": False, "error": f"Template not found for {form_id}"}), 400

        # Call the fill function
        try:
            output_path = os.path.join(out_dir, fm["filename"])
            if fm.get("static"):
                # Static file — just copy the template (e.g., sellers_permit)
                import shutil
                shutil.copy2(template_path, output_path)
            elif fm.get("no_template_arg"):
                # Function generates from scratch (e.g., generate_dvbe_843)
                import src.forms.reytech_filler_v4 as filler
                fill_fn = getattr(filler, fm["fn"])
                fill_fn(r, CONFIG, output_path)
            else:
                import src.forms.reytech_filler_v4 as filler
                fill_fn = getattr(filler, fm["fn"])
                fill_fn(template_path, r, CONFIG, output_path)
        except Exception as e:
            log.error("Refill %s failed: %s", form_id, e, exc_info=True)
            return jsonify({"ok": False, "error": f"Fill failed: {e}"}), 500
    else:
        return jsonify({"ok": False, "error": f"Unknown form_id: {form_id}"}), 400

    # ── Run QA on refilled form ──
    _qa_result = {}
    try:
        from src.forms.form_qa import verify_single_form
        _out_file = os.path.join(out_dir, FORM_MAP.get(form_id, {}).get("filename", ""))
        if form_id == "quote":
            _out_file = os.path.join(out_dir, f"{sol}_Quote_Reytech.pdf")
        _qa_result = verify_single_form(_out_file, form_id, r, CONFIG)
    except Exception as _qe:
        log.debug("Refill QA skipped: %s", _qe)

    # ── Reset verdict to pending ──
    try:
        from src.core.dal import get_latest_manifest, reset_form_verdict
        manifest = get_latest_manifest(rid)
        if manifest:
            reset_form_verdict(manifest["id"], form_id)
    except Exception as _rv:
        log.warning("Verdict reset failed: %s", _rv)

    log.info("REFILL %s/%s: success (overrides: %s)", rid, form_id,
             list(overrides.keys()) if overrides else "none")

    return jsonify({
        "ok": True,
        "form_id": form_id,
        "qa": _qa_result,
    })


@bp.route("/api/rfq/<rid>/timeline")
@auth_required
@safe_route
def api_rfq_timeline(rid):
    """Get the full lifecycle timeline for an RFQ."""
    from src.core.dal import get_lifecycle_events
    events = get_lifecycle_events("rfq", rid, limit=200)
    return jsonify({"ok": True, "events": events, "count": len(events)})


@bp.route("/api/rfq/<rid>/buyer-prefs")
@auth_required
@safe_route
def api_rfq_buyer_prefs(rid):
    """Get buyer preferences for the RFQ's requestor."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    email = r.get("requestor_email", "")
    if not email:
        return jsonify({"ok": True, "preferences": [], "message": "No requestor email"})
    from src.core.dal import get_buyer_preferences
    prefs = get_buyer_preferences(email)
    return jsonify({"ok": True, "preferences": prefs, "buyer_email": email})


@bp.route("/api/rfq/<rid>/download-complete-package")
@auth_required
@safe_route
def api_download_complete_package(rid):
    """Download ALL forms merged into one PDF — quote + compliance."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    sol = r.get("solicitation_number", "") or r.get("rfq_number", "") or "RFQ"
    out_dir = os.path.join(OUTPUT_DIR, sol)
    output_files = r.get("output_files", [])

    if not output_files:
        return jsonify({"ok": False, "error": "No files generated"})

    try:
        from pypdf import PdfReader, PdfWriter
        writer = PdfWriter()
        merged_count = 0

        # Quote first, then all other forms in order
        quote_files = [f for f in output_files if "Quote" in f and "704" not in f.upper()]
        other_files = [f for f in output_files if f not in quote_files]
        ordered = quote_files + other_files

        for f in ordered:
            fpath = os.path.join(out_dir, f)
            if not os.path.exists(fpath):
                continue
            # Skip the merged package file itself (avoid double-counting)
            if "RFQ_Package" in f or "Compliance_Forms" in f:
                continue
            try:
                reader = PdfReader(fpath)
                for page in reader.pages:
                    text = ""
                    try:
                        text = page.extract_text() or ""
                    except Exception as _e:
                        log.debug('suppressed in api_download_complete_package: %s', _e)
                    if text.strip().startswith("Please wait") and len(text.strip()) < 300:
                        continue
                    writer.add_page(page)
                merged_count += 1
            except Exception as _e:
                log.warning("Skip %s in complete package: %s", f, _e)

        if merged_count == 0:
            return jsonify({"ok": False, "error": "No valid PDFs to merge"})

        import io
        buf = io.BytesIO()
        writer.write(buf)
        buf.seek(0)

        _safe_agency = ""
        try:
            from src.core.agency_config import match_agency
            _ak, _ac = match_agency(r)
            _safe_agency = (_ac.get("name", "") or "").replace(" ", "").replace("/", "")[:20]
        except Exception as _e:
            log.debug('suppressed in api_download_complete_package: %s', _e)

        filename = f"Complete_RFQ_{_safe_agency}_{sol}_ReytechInc.pdf" if _safe_agency else f"Complete_RFQ_{sol}_ReytechInc.pdf"

        from flask import send_file
        return send_file(buf, mimetype="application/pdf", as_attachment=True, download_name=filename)
    except Exception as e:
        log.error("Complete package download failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/rfq/<rid>/export-invoice")
@auth_required
@safe_route
def api_export_invoice(rid):
    """Export buyer invoice as Excel for QB entry."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    items = r.get("line_items", r.get("items", []))
    sol = r.get("solicitation_number", "") or r.get("rfq_number", "")
    quote_num = r.get("reytech_quote_number", "")
    buyer = r.get("requestor_name", "")
    agency = r.get("agency", "") or r.get("agency_name", "")
    tax_rate = float(r.get("tax_rate", 0) or 0) / 100
    delivery = r.get("delivery_location", "")

    try:
        import openpyxl
    except ImportError:
        return jsonify({"ok": False, "error": "openpyxl not installed — run: pip install openpyxl"}), 500

    try:
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from io import BytesIO

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Invoice"

        hf = Font(bold=True, size=14)
        sf = Font(size=11, color="333333")
        cf = Font(bold=True, size=10, color="FFFFFF")
        cfill = PatternFill("solid", fgColor="2E75B6")
        mf = '#,##0.00'
        bd = Border(bottom=Side(style='thin', color='CCCCCC'))

        ws.merge_cells('A1:G1')
        ws['A1'] = "REYTECH INC. — INVOICE"
        ws['A1'].font = hf
        ws['A2'] = f"Quote #: {quote_num}"
        ws['A2'].font = sf
        ws['A3'] = f"Bill To: {buyer} — {agency}"
        ws['A3'].font = sf
        ws['A4'] = f"Ship To: {delivery}"
        ws['A4'].font = sf
        ws['A5'] = f"Solicitation #: {sol}"
        ws['A5'].font = sf

        for col, h in enumerate(["#", "Description", "Part #", "QTY", "UOM", "Unit Price", "Subtotal"], 1):
            c = ws.cell(row=7, column=col, value=h)
            c.font = cf
            c.fill = cfill
            c.alignment = Alignment(horizontal='center')

        subtotal = 0
        for idx, item in enumerate(items):
            row = 8 + idx
            qty = int(float(item.get("qty", 1) or 1))
            price = float(item.get("price_per_unit", 0) or 0)
            lt = qty * price
            subtotal += lt
            ws.cell(row=row, column=1, value=idx+1).border = bd
            ws.cell(row=row, column=2, value=(item.get("description", "") or "")[:80]).border = bd
            ws.cell(row=row, column=3, value=item.get("part_number", "") or item.get("item_number", "")).border = bd
            ws.cell(row=row, column=4, value=qty).border = bd
            ws.cell(row=row, column=5, value=item.get("uom", "EA")).border = bd
            ws.cell(row=row, column=6, value=price).number_format = mf
            ws.cell(row=row, column=7, value=lt).number_format = mf

        tr = 8 + len(items) + 1
        tax_amt = subtotal * tax_rate
        ws.cell(row=tr, column=6, value="Subtotal:").font = Font(bold=True)
        ws.cell(row=tr, column=7, value=subtotal).number_format = mf
        ws.cell(row=tr+1, column=6, value=f"Tax ({r.get('tax_rate', 0)}%):").font = Font(bold=True)
        ws.cell(row=tr+1, column=7, value=tax_amt).number_format = mf
        ws.cell(row=tr+2, column=6, value="TOTAL:").font = Font(bold=True, size=12)
        ws.cell(row=tr+2, column=7, value=subtotal + tax_amt).number_format = mf
        ws.cell(row=tr+2, column=7).font = Font(bold=True, size=12)

        ws.column_dimensions['A'].width = 5
        ws.column_dimensions['B'].width = 50
        ws.column_dimensions['C'].width = 15
        ws.column_dimensions['D'].width = 8
        ws.column_dimensions['E'].width = 8
        ws.column_dimensions['F'].width = 14
        ws.column_dimensions['G'].width = 14

        buf = BytesIO()
        wb.save(buf)
        buf.seek(0)

        from flask import send_file
        return send_file(buf, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True, download_name=f"Invoice_{quote_num}_{sol}_Reytech.xlsx")
    except Exception as e:
        log.error("Invoice export: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/rfq/<rid>/export-supplier-po")
@auth_required
@safe_route
def api_export_supplier_po(rid):
    """Export supplier PO as Excel for QB entry."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    items = r.get("line_items", r.get("items", []))
    sol = r.get("solicitation_number", "") or r.get("rfq_number", "")
    quote_num = r.get("reytech_quote_number", "")
    supplier_name = ""
    for item in items:
        sn = item.get("cost_supplier_name", "") or item.get("scprs_supplier", "")
        if sn:
            supplier_name = sn
            break

    try:
        import openpyxl
    except ImportError:
        return jsonify({"ok": False, "error": "openpyxl not installed — run: pip install openpyxl"}), 500

    try:
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from io import BytesIO

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Supplier PO"

        hf = Font(bold=True, size=14)
        sf = Font(size=11, color="333333")
        cf = Font(bold=True, size=10, color="FFFFFF")
        cfill = PatternFill("solid", fgColor="2D6A2E")
        mf = '#,##0.00'
        bd = Border(bottom=Side(style='thin', color='CCCCCC'))

        ws.merge_cells('A1:H1')
        ws['A1'] = "REYTECH INC. — PURCHASE ORDER"
        ws['A1'].font = hf
        ws['A2'] = f"Related Quote #: {quote_num}"
        ws['A2'].font = sf
        ws['A3'] = f"Supplier: {supplier_name}"
        ws['A3'].font = sf
        ws['A4'] = f"Solicitation #: {sol}"
        ws['A4'].font = sf

        for col, h in enumerate(["#", "Description", "Part #", "QTY", "UOM", "Supplier Cost", "Subtotal", "Source"], 1):
            c = ws.cell(row=6, column=col, value=h)
            c.font = cf
            c.fill = cfill
            c.alignment = Alignment(horizontal='center')

        subtotal = 0
        for idx, item in enumerate(items):
            row = 7 + idx
            qty = int(float(item.get("qty", 1) or 1))
            cost = float(item.get("supplier_cost", 0) or item.get("vendor_cost", 0) or 0)
            lt = qty * cost
            subtotal += lt
            source = item.get("cost_source", "")
            sname = item.get("cost_supplier_name", "")
            ws.cell(row=row, column=1, value=idx+1).border = bd
            ws.cell(row=row, column=2, value=(item.get("description", "") or "")[:80]).border = bd
            ws.cell(row=row, column=3, value=item.get("part_number", "") or item.get("item_number", "")).border = bd
            ws.cell(row=row, column=4, value=qty).border = bd
            ws.cell(row=row, column=5, value=item.get("uom", "EA")).border = bd
            ws.cell(row=row, column=6, value=cost).number_format = mf
            ws.cell(row=row, column=7, value=lt).number_format = mf
            ws.cell(row=row, column=8, value=f"{source} — {sname}" if sname else source).border = bd

        tr = 7 + len(items) + 1
        bid_total = sum(int(float(i.get("qty", 1) or 1)) * float(i.get("price_per_unit", 0) or 0) for i in items)
        ws.cell(row=tr, column=6, value="TOTAL:").font = Font(bold=True, size=12)
        ws.cell(row=tr, column=7, value=subtotal).number_format = mf
        ws.cell(row=tr, column=7).font = Font(bold=True, size=12)
        ws.cell(row=tr+2, column=5, value="Supplier Cost:").font = Font(bold=True)
        ws.cell(row=tr+2, column=7, value=subtotal).number_format = mf
        ws.cell(row=tr+3, column=5, value="Bid Total:").font = Font(bold=True)
        ws.cell(row=tr+3, column=7, value=bid_total).number_format = mf
        ws.cell(row=tr+4, column=5, value="Gross Margin:").font = Font(bold=True)
        ws.cell(row=tr+4, column=7, value=bid_total - subtotal).number_format = mf
        ws.cell(row=tr+4, column=7).font = Font(bold=True, color="2D6A2E")

        ws.column_dimensions['A'].width = 5
        ws.column_dimensions['B'].width = 50
        ws.column_dimensions['C'].width = 15
        ws.column_dimensions['D'].width = 8
        ws.column_dimensions['E'].width = 8
        ws.column_dimensions['F'].width = 14
        ws.column_dimensions['G'].width = 14
        ws.column_dimensions['H'].width = 25

        buf = BytesIO()
        wb.save(buf)
        buf.seek(0)

        from flask import send_file
        safe_sup = supplier_name.replace(" ", "")[:20]
        return send_file(buf, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True, download_name=f"SupplierPO_{quote_num}_{sol}_{safe_sup}.xlsx")
    except Exception as e:
        log.error("Supplier PO export: %s", e)
        return jsonify({"ok": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════
# Bookkeeper Export — QBO CSV + deal summary
# ═══════════════════════════════════════════════════════════════════════

def _build_bookkeeper_data(r, rid):
    """Build QBO CSV content and deal summary from an RFQ dict.
    Returns (csv_content, summary, filename)."""
    import csv
    import io

    items = r.get("line_items", r.get("items", []))
    sol = r.get("solicitation_number", "")
    po = r.get("po_number", "")
    agency = r.get("agency_name", r.get("agency", ""))
    institution = r.get("institution", "")
    ship_to = r.get("delivery_location", r.get("ship_to", ""))

    # Build QBO CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Product/Service Name", "Sales Description", "Purchase Description",
        "Sales Price / Rate", "Purchase Cost", "SKU", "Taxable", "Type",
        "Income Account", "Expense Account"
    ])

    subtotal = 0
    cost_total = 0
    bid_count = 0
    for item in items:
        if item.get("no_bid"):
            continue
        bid_count += 1
        desc = (item.get("description", "") or "")[:100]
        mfg = item.get("mfg_number", "") or item.get("part_number", "") or ""
        product_name = mfg if mfg else desc[:40]
        sell_price = (item.get("unit_price")
                      or item.get("price_per_unit")
                      or (item.get("pricing", {}) or {}).get("recommended_price")
                      or 0)
        cost = (item.get("vendor_cost")
                or item.get("supplier_cost")
                or (item.get("pricing", {}) or {}).get("unit_cost")
                or 0)
        qty = item.get("qty", 1) or 1
        uom = (item.get("uom") or "EA").upper()

        subtotal += float(sell_price) * float(qty)
        cost_total += float(cost) * float(qty)

        writer.writerow([
            product_name,
            f"{product_name} {desc[:60]}",
            f"{product_name} {desc[:60]}",
            f"{float(sell_price):.2f}",
            f"{float(cost):.2f}",
            uom,
            "Yes",
            "Non-inventory",
            "Sales of Product Income",
            "Cost of Goods Sold",
        ])

    csv_content = output.getvalue()

    # Deal summary
    tax_rate = 0.0725
    tax = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax, 2)
    profit = round(subtotal - cost_total, 2)
    margin = round(profit / subtotal * 100, 1) if subtotal > 0 else 0

    summary_lines = [
        f"DEAL SUMMARY — PO #{po or 'TBD'}",
        f"Agency: {agency}",
        f"Institution: {institution}",
        f"Ship To: {ship_to}",
        f"Solicitation: {sol}",
        "",
        f"Items: {bid_count}",
        f"Subtotal: ${subtotal:,.2f}",
        f"Sales Tax ({tax_rate * 100:.2f}%): ${tax:,.2f}",
        f"Total: ${total:,.2f}",
        "",
        f"Cost Total: ${cost_total:,.2f}",
        f"Profit: ${profit:,.2f} ({margin}% margin)",
    ]
    summary = "\n".join(summary_lines)
    filename = f"QBO_Import_{po or sol or rid}.csv"

    return csv_content, summary, filename


@bp.route("/api/rfq/<rid>/bookkeeper-export", methods=["POST"])
@auth_required
@safe_route
def api_bookkeeper_export(rid):
    """Generate QBO CSV + deal summary for bookkeeper."""
    try:
        bad = _validate_rid(rid)
        if bad:
            return bad
        from src.api.dashboard import load_rfqs
        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            return jsonify({"ok": False, "error": "RFQ not found"}), 404

        csv_content, summary, filename = _build_bookkeeper_data(r, rid)

        return jsonify({
            "ok": True,
            "csv": csv_content,
            "summary": summary,
            "filename": filename,
        })
    except Exception as e:
        log.error("Bookkeeper export error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfq/<rid>/bookkeeper-csv")
@auth_required
@safe_route
def api_bookkeeper_csv(rid):
    """Download QBO CSV directly."""
    try:
        bad = _validate_rid(rid)
        if bad:
            return bad
        from src.api.dashboard import load_rfqs
        from flask import Response
        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            return jsonify({"ok": False, "error": "RFQ not found"}), 404

        csv_content, _summary, filename = _build_bookkeeper_data(r, rid)

        return Response(
            csv_content,
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        log.error("Bookkeeper CSV download error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfq/<rid>/re-extract-requirements", methods=["POST"])
@auth_required
@safe_route
def api_rfq_re_extract_requirements(rid):
    """Re-run email requirement extraction on demand.

    Used when: user manually uploads new email, forwards changed, or wants
    to refresh extraction after body edits.
    """
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"}), 404

    body_text = r.get("body_text", "") or r.get("body_preview", "") or ""
    subject = r.get("email_subject", "")
    attachments = [{"filename": f} for f in r.get("attachments_raw", [])]

    if not body_text and not subject:
        return jsonify({"ok": False, "error": "No email body or subject to extract from"})

    try:
        import json as _j
        from src.agents.requirement_extractor import extract_requirements
        req = extract_requirements(body_text, subject, attachments)
        if req and req.has_requirements:
            r["requirements_json"] = _j.dumps(req.to_dict(), default=str)
            # Supplement due date if missing
            if r.get("due_date") in ("TBD", "", None) and req.due_date:
                r["due_date"] = req.due_date
            rfqs[rid] = r
            _save_single_rfq(rid, r)
            return jsonify({
                "ok": True,
                "method": req.extraction_method,
                "confidence": req.confidence,
                "forms_found": len(req.forms_required),
                "requirements": req.to_dict(),
            })
        else:
            return jsonify({"ok": True, "method": "none", "forms_found": 0,
                           "message": "No requirements detected"})
    except Exception as e:
        log.error("Re-extract requirements error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
