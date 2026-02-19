"""
email_outreach.py — Email Outreach Agent for Reytech
Phase 14 | Version: 1.0.0

Automates the last mile: quote complete → draft email → approve → send.

Two modes:
  1. PC Outreach: 704 filled → draft buyer email with 704 attached → approve → send
  2. Lead Outreach: Lead gen agent finds opportunity → draft cold email → approve → send

Uses the existing EmailSender from email_poller.py for SMTP.
Adds: LLM-powered personalization, template management, send queue, audit trail.

Pipeline position:
  Parse → Identify → Price → Fill 704 → DRAFT EMAIL → Review → SEND
"""

import json
import os
import time
import logging
import hashlib
from datetime import datetime
from typing import Optional

log = logging.getLogger("outreach")

# ── JSON→SQLite compatibility (Phase 32c migration) ──────────────────────────
try:
    from src.core.db import (
        get_all_customers, get_all_vendors, get_all_price_checks, get_price_check,
        upsert_price_check, get_outbox, upsert_outbox_email, update_outbox_status,
        get_email_templates, upsert_email_template, get_vendor_registrations,
        upsert_vendor_registration, get_market_intelligence, upsert_market_intelligence,
        get_intel_agencies, upsert_intel_agency, get_growth_outreach, save_growth_campaign,
        get_qa_reports, save_qa_report, get_latest_qa_report,
        upsert_customer, upsert_vendor,
    )
    _HAS_DB_DAL = True
except ImportError:
    _HAS_DB_DAL = False
# ─────────────────────────────────────────────────────────────────────────────
# ── Shared DB Context (Anthropic Skills Guide: Pattern 5 — Domain Intelligence) ──
# Gives this agent access to live CRM, quotes, revenue, price history from SQLite.
# Eliminates file loading duplication and ensures consistent ground truth.
try:
    from src.core.agent_context import (
        get_context, format_context_for_agent,
        get_contact_by_agency, get_best_price,
    )
    HAS_AGENT_CTX = True
except ImportError:
    HAS_AGENT_CTX = False
    def get_context(**kw): return {}
    def format_context_for_agent(c, **kw): return ""
    def get_contact_by_agency(a): return []
    def get_best_price(d): return None



try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

OUTBOX_FILE = os.path.join(DATA_DIR, "email_outbox.json")
SENT_LOG_FILE = os.path.join(DATA_DIR, "email_sent_log.json")
MAX_OUTBOX = 200
MAX_SENT_LOG = 5000

# ─── Outbox (draft queue) ───────────────────────────────────────────────────

def _load_outbox() -> list:
    try:
        with open(OUTBOX_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_outbox(outbox: list):
    os.makedirs(DATA_DIR, exist_ok=True)
    if len(outbox) > MAX_OUTBOX:
        outbox = outbox[-MAX_OUTBOX:]
    with open(OUTBOX_FILE, "w") as f:
        json.dump(outbox, f, indent=2)


def _log_sent(entry: dict):
    try:
        with open(SENT_LOG_FILE) as f:
            log_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        log_data = []
    log_data.append(entry)
    if len(log_data) > MAX_SENT_LOG:
        log_data = log_data[-MAX_SENT_LOG:]
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(SENT_LOG_FILE, "w") as f:
        json.dump(log_data, f)


# ─── Email Templates ────────────────────────────────────────────────────────

def _draft_pc_email(pc: dict, quote_number: str = "",
                    pdf_path: str = "") -> dict:
    """Draft a buyer email for a completed Price Check."""
    institution = pc.get("institution", pc.get("parsed", {}).get("header", {}).get("institution", ""))
    requestor = pc.get("requestor", pc.get("parsed", {}).get("header", {}).get("requestor", "Purchasing Department"))
    requestor_email = pc.get("requestor_email", pc.get("parsed", {}).get("header", {}).get("email", ""))
    pc_number = pc.get("pc_number", "")
    due_date = pc.get("due_date", "")

    items = pc.get("items", [])
    item_count = len(items)
    total = sum(
        it.get("pricing", {}).get("recommended_price", 0) * it.get("qty", 0)
        for it in items
    )

    subject = f"Reytech Inc. — Quote {quote_number} — {institution}" if quote_number else f"Reytech Inc. — Price Quote — {institution}"

    body = f"""Dear {requestor},

Thank you for the opportunity to quote on Price Check #{pc_number}. Please find attached our completed AMS 704 with pricing for {item_count} item{'s' if item_count != 1 else ''}.

Quote Summary:
  Quote Number: {quote_number}
  Items: {item_count}
  Total: ${total:,.2f}
  Delivery: {pc.get('delivery_time', '5-7 business days')}
  Pricing Valid: 45 calendar days from {due_date}

All items are quoted F.O.B. Destination, freight prepaid and included.

We appreciate the opportunity to serve {institution} and look forward to your response.

Best regards,
Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com
SB/DVBE Cert #2002605"""

    attachments = []
    if pdf_path and os.path.exists(pdf_path):
        attachments.append(pdf_path)

    return {
        "id": hashlib.md5(f"{pc_number}{time.time()}".encode()).hexdigest()[:12],
        "type": "pc_quote",
        "status": "draft",  # draft → approved → sent → failed
        "to": requestor_email,
        "subject": subject,
        "body": body,
        "attachments": attachments,
        "metadata": {
            "pc_number": pc_number,
            "quote_number": quote_number,
            "institution": institution,
            "requestor": requestor,
            "total": round(total, 2),
            "item_count": item_count,
        },
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "sent_at": None,
        "error": None,
    }


def _draft_lead_email(lead: dict) -> dict:
    """Draft a cold outreach email for a lead from Lead Gen agent."""
    institution = lead.get("institution", "")
    buyer = lead.get("buyer_name", "Purchasing Department")
    buyer_email = lead.get("buyer_email", "")
    po_number = lead.get("po_number", "")
    savings = lead.get("estimated_savings_pct", 0)
    matched_items = lead.get("matched_items", [])

    item_mention = ""
    if matched_items:
        names = [it.get("description", "")[:50] for it in matched_items[:3]]
        item_mention = f", including {', '.join(names)}"

    subject = f"Reytech Inc. — Competitive Pricing Available — {institution}"

    body = f"""Dear {buyer},

I noticed Purchase Order {po_number} for {institution}{item_mention} and wanted to reach out.

Reytech Inc. is a certified Small Business (SB/DVBE) reseller specializing in California state procurement. We have supplied similar items to CDCR, CCHCS, CalVet, and DSH facilities{f' at pricing {savings:.0f}% below current listed rates' if savings > 5 else ' at competitive rates'}.

We would welcome the opportunity to be added to the quote list for this and future Purchase Orders. We typically respond within 24 hours with competitive pricing and can meet all delivery requirements.

Would you be open to a brief conversation about how Reytech can support {institution}?

Best regards,
Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com
SB/DVBE Cert #2002605"""

    return {
        "id": hashlib.md5(f"lead-{po_number}{time.time()}".encode()).hexdigest()[:12],
        "type": "lead_outreach",
        "status": "draft",
        "to": buyer_email,
        "subject": subject,
        "body": body,
        "attachments": [],
        "metadata": {
            "lead_id": lead.get("id", ""),
            "po_number": po_number,
            "institution": institution,
            "buyer_name": buyer,
            "score": lead.get("score", 0),
        },
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "sent_at": None,
        "error": None,
    }


# ─── Public API ──────────────────────────────────────────────────────────────

def draft_for_pc(pc: dict, quote_number: str = "",
                 pdf_path: str = "") -> dict:
    """
    Create a draft email for a completed Price Check.
    Adds to outbox for review/approval.
    """
    email = _draft_pc_email(pc, quote_number, pdf_path)
    outbox = _load_outbox()
    outbox.append(email)
    _save_outbox(outbox)
    log.info("DRAFT PC email: %s → %s (%s)",
             email["metadata"]["pc_number"], email["to"], email["id"])
    return email


def draft_for_lead(lead: dict) -> dict:
    """
    Create a draft outreach email for a lead.
    Adds to outbox for review/approval.
    """
    email = _draft_lead_email(lead)
    outbox = _load_outbox()
    outbox.append(email)
    _save_outbox(outbox)
    log.info("DRAFT lead email: %s → %s (%s)",
             email["metadata"]["po_number"], email["to"], email["id"])
    return email


def get_outbox(status: str = None) -> list:
    """Get outbox items, optionally filtered by status."""
    outbox = _load_outbox()
    if status:
        outbox = [e for e in outbox if e.get("status") == status]
    return sorted(outbox, key=lambda x: x.get("created_at", ""), reverse=True)


def approve_email(email_id: str) -> dict:
    """Mark an email as approved for sending."""
    outbox = _load_outbox()
    for email in outbox:
        if email["id"] == email_id:
            if email["status"] != "draft":
                return {"ok": False, "error": f"Email is {email['status']}, not draft"}
            if not email.get("to"):
                return {"ok": False, "error": "No recipient email address"}
            email["status"] = "approved"
            email["updated_at"] = datetime.now().isoformat()
            _save_outbox(outbox)
            return {"ok": True, "email": email}
    return {"ok": False, "error": "Email not found in outbox"}


def update_draft(email_id: str, updates: dict) -> dict:
    """Edit a draft email (subject, body, to)."""
    outbox = _load_outbox()
    for email in outbox:
        if email["id"] == email_id:
            if email["status"] != "draft":
                return {"ok": False, "error": f"Cannot edit — email is {email['status']}"}
            for field in ("to", "subject", "body"):
                if field in updates:
                    email[field] = updates[field]
            email["updated_at"] = datetime.now().isoformat()
            _save_outbox(outbox)
            return {"ok": True, "email": email}
    return {"ok": False, "error": "Email not found in outbox"}


def send_email(email_id: str) -> dict:
    """
    Send an approved email via SMTP.
    Requires GMAIL_ADDRESS + GMAIL_PASSWORD env vars.
    """
    outbox = _load_outbox()
    target = None
    for email in outbox:
        if email["id"] == email_id:
            target = email
            break
    if not target:
        return {"ok": False, "error": "Email not found in outbox"}
    if target["status"] not in ("approved", "draft"):
        return {"ok": False, "error": f"Email is {target['status']}, cannot send"}
    if not target.get("to"):
        return {"ok": False, "error": "No recipient address"}

    # Import EmailSender
    try:
        from src.agents.email_poller import EmailSender
        config = {
            "email": os.environ.get("GMAIL_ADDRESS", "sales@reytechinc.com"),
            "email_password": os.environ.get("GMAIL_PASSWORD", ""),
        }
        sender = EmailSender(config)
    except ImportError:
        return {"ok": False, "error": "EmailSender not available"}

    try:
        draft = {
            "to": target["to"],
            "subject": target["subject"],
            "body": target["body"],
            "attachments": target.get("attachments", []),
        }
        sender.send(draft)

        # Mark as sent
        target["status"] = "sent"
        target["sent_at"] = datetime.now().isoformat()
        _save_outbox(outbox)

        # Log
        _log_sent({
            "email_id": target["id"],
            "type": target["type"],
            "to": target["to"],
            "subject": target["subject"],
            "sent_at": target["sent_at"],
            "metadata": target.get("metadata", {}),
        })

        log.info("SENT email %s → %s: %s", target["id"], target["to"], target["subject"])
        # 📧 Log to email_log for CS communication history
        try:
            from src.agents.notify_agent import log_email_event
            log_email_event(
                direction="sent",
                sender=os.environ.get("GMAIL_ADDRESS","sales@reytechinc.com"),
                recipient=target.get("to",""),
                subject=target.get("subject",""),
                body_preview=(target.get("body","") or "")[:500],
                full_body=target.get("body",""),
                quote_number=(target.get("metadata",{}) or {}).get("quote_number",""),
                contact_id=target.get("to",""),
                intent=target.get("type","general"),
                status="sent",
            )
        except Exception as _le:
            pass
        return {"ok": True, "email": target}

    except Exception as e:
        target["status"] = "failed"
        target["error"] = str(e)
        target["updated_at"] = datetime.now().isoformat()
        _save_outbox(outbox)
        log.error("SEND FAILED %s: %s", target["id"], e)
        return {"ok": False, "error": str(e)}


def send_approved() -> dict:
    """Send all approved emails in outbox. Returns summary."""
    outbox = _load_outbox()
    approved = [e for e in outbox if e.get("status") == "approved"]
    results = {"sent": 0, "failed": 0, "errors": []}

    for email in approved:
        result = send_email(email["id"])
        if result["ok"]:
            results["sent"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({"id": email["id"], "error": result.get("error")})

    return results


def delete_from_outbox(email_id: str) -> dict:
    """Delete an email from outbox."""
    outbox = _load_outbox()
    before = len(outbox)
    outbox = [e for e in outbox if e["id"] != email_id]
    if len(outbox) == before:
        return {"ok": False, "error": "Email not found"}
    _save_outbox(outbox)
    return {"ok": True}


def get_sent_log(limit: int = 50) -> list:
    """Get the sent email log."""
    try:
        with open(SENT_LOG_FILE) as f:
            data = json.load(f)
        return sorted(data, key=lambda x: x.get("sent_at", ""), reverse=True)[:limit]
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def get_agent_status() -> dict:
    """Agent health status."""
    outbox = _load_outbox()
    by_status = {}
    for e in outbox:
        s = e.get("status", "unknown")
        by_status[s] = by_status.get(s, 0) + 1

    return {
        "agent": "email_outreach",
        "version": "1.0.0",
        "outbox_total": len(outbox),
        "by_status": by_status,
        "gmail_configured": bool(os.environ.get("GMAIL_ADDRESS") and os.environ.get("GMAIL_PASSWORD")),
        "sent_log_count": len(get_sent_log(limit=9999)),
    }
