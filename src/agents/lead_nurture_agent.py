"""
src/agents/lead_nurture_agent.py — Lead Nurture Automation (PRD-28 WI-3)

Closes the lead pipeline gap:
  1. Auto-nurture sequences — 3-step drip per lead type
  2. Dynamic lead rescoring — recalculate based on new activity
  3. Lead → Customer conversion
  4. Unified prospect pipeline (merge growth_prospects into leads)
  
Background scheduler runs daily.
"""

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone

log = logging.getLogger("lead_nurture")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

# ── Config ────────────────────────────────────────────────────────────────────
CHECK_INTERVAL = 86400  # Daily
_scheduler_running = False

# Nurture templates: step_num → (delay_days, subject_template, body_template)
NURTURE_SEQUENCES = {
    "scprs_lead": [
        (0, "California State Pricing — {category}",
         "Hi {name},\n\nI noticed {agency} has been purchasing {category} items through state contracts. "
         "Reytech Medical Supply specializes in competitive pricing for California state agencies.\n\n"
         "We'd love to provide a quick comparison quote. Would you have 5 minutes this week?\n\n"
         "Best,\nMike Garcia\nReytech Medical Supply"),
        (7, "Quick follow-up — State contract pricing for {agency}",
         "Hi {name},\n\nJust following up on my previous note about {category} pricing for {agency}. "
         "We currently serve several CDCR facilities and have strong pricing on your commonly purchased items.\n\n"
         "Happy to send over a no-obligation comparison. Just reply with your current item list.\n\n"
         "Best,\nMike Garcia"),
        (21, "Checking in — {agency} supply needs",
         "Hi {name},\n\nI wanted to check in one more time. If {agency}'s supply needs have changed "
         "or if there's a better time to connect, I'm happy to adjust.\n\n"
         "In the meantime, here's a quick overview of what we offer:\n"
         "• Medical/dental supplies at state-contract pricing\n"
         "• Free delivery to California state facilities\n"
         "• Same-day quote turnaround\n\n"
         "Let me know if I can help.\n\nBest,\nMike Garcia"),
    ],
    "inbound_inquiry": [
        (0, "Thanks for reaching out — {category}",
         "Hi {name},\n\nThank you for your inquiry about {category}. "
         "I'm putting together pricing for you and will have a quote ready within 24 hours.\n\n"
         "Is there anything specific you'd like me to include?\n\nBest,\nMike Garcia"),
        (7, "Your quote is ready — {category}",
         "Hi {name},\n\nJust following up to make sure you received the quote I sent. "
         "Let me know if you have any questions or need any adjustments.\n\n"
         "Best,\nMike Garcia"),
        (21, "Still interested? — {category}",
         "Hi {name},\n\nI wanted to check in on the quote I sent for {category}. "
         "If your needs have changed or you'd like updated pricing, I'm happy to revise.\n\n"
         "Best,\nMike Garcia"),
    ],
    "default": [
        (0, "Introduction — Reytech Medical Supply",
         "Hi {name},\n\nI'm Mike Garcia from Reytech Medical Supply. We specialize in "
         "competitive pricing for California government agencies.\n\n"
         "I'd love to learn more about {agency}'s supply needs. Would you have a few minutes to connect?\n\n"
         "Best,\nMike Garcia"),
        (7, "Following up — Reytech Medical Supply",
         "Hi {name},\n\nJust a quick follow-up on my previous note. "
         "I'd be happy to provide a no-obligation quote comparison for {agency}.\n\n"
         "Best,\nMike Garcia"),
        (21, "One last check-in — {agency}",
         "Hi {name},\n\nJust checking in one more time. If now isn't the right time, "
         "no worries at all — feel free to reach out whenever {agency} has supply needs.\n\n"
         "Best,\nMike Garcia"),
    ],
}


# ── Lead Loading ──────────────────────────────────────────────────────────────

def _load_leads() -> list:
    """Load leads from DB (single source of truth)."""
    try:
        from src.core.dal import get_all_leads
        return get_all_leads()
    except Exception:
        try:
            with open(os.path.join(DATA_DIR, "leads.json")) as f:
                return json.load(f)
        except Exception:
            return []


def _save_leads(leads: list):
    """Save leads to DB (single source of truth)."""
    try:
        from src.core.dal import save_all_leads
        save_all_leads(leads)
    except Exception:
        with open(os.path.join(DATA_DIR, "leads.json"), "w") as f:
            json.dump(leads, f, indent=2, default=str)


# ── Nurture Sequences ─────────────────────────────────────────────────────────

def start_nurture(lead_id: str, sequence_key: str = "") -> dict:
    """Start a nurture sequence for a lead."""
    leads = _load_leads()
    lead = next((l for l in leads if l.get("id") == lead_id), None)
    if not lead:
        return {"ok": False, "error": "lead not found"}

    if lead.get("nurture_active"):
        return {"ok": False, "error": "nurture already active"}

    # Determine sequence
    source = lead.get("source", "")
    if not sequence_key:
        if "scprs" in source.lower() or "scan" in source.lower():
            sequence_key = "scprs_lead"
        elif "inbound" in source.lower() or "email" in source.lower():
            sequence_key = "inbound_inquiry"
        else:
            sequence_key = "default"

    sequence = NURTURE_SEQUENCES.get(sequence_key, NURTURE_SEQUENCES["default"])
    now = datetime.now(timezone.utc)

    # Schedule steps
    steps = []
    for step_num, (delay_days, subject_tpl, body_tpl) in enumerate(sequence):
        scheduled = (now + timedelta(days=delay_days)).isoformat()
        steps.append({
            "step_num": step_num,
            "scheduled_at": scheduled,
            "status": "pending",
            "template_key": sequence_key,
        })

    lead["nurture_active"] = True
    lead["nurture_sequence"] = sequence_key
    lead["nurture_steps"] = steps
    lead["nurture_started_at"] = now.isoformat()

    _save_leads(leads)

    # Also log to DB
    try:
        with get_db() as conn:
            for step in steps:
                conn.execute("""
                    INSERT INTO lead_nurture (lead_id, step_num, scheduled_at, status, template_key)
                    VALUES (?, ?, ?, 'pending', ?)
                """, (lead_id, step["step_num"], step["scheduled_at"], sequence_key))
    except Exception as e:
        log.warning("start_nurture DB: %s", e)

    log.info("Started nurture for lead %s (sequence: %s, %d steps)", lead_id, sequence_key, len(steps))
    return {"ok": True, "steps": len(steps), "sequence": sequence_key}


def process_nurture_queue() -> dict:
    """Check for due nurture steps and create email drafts."""
    now = datetime.now(timezone.utc).isoformat()
    leads = _load_leads()
    drafts_created = 0
    paused = 0

    try:
        from src.core.dal import get_outbox as _dal_ob
        outbox = _dal_ob()
    except Exception:
        outbox = []

    for lead in leads:
        if not lead.get("nurture_active"):
            continue

        steps = lead.get("nurture_steps", [])
        for step in steps:
            if step.get("status") != "pending":
                continue
            if step.get("scheduled_at", "") > now:
                continue

            # Create email draft
            sequence_key = step.get("template_key", "default")
            seq = NURTURE_SEQUENCES.get(sequence_key, NURTURE_SEQUENCES["default"])
            step_num = step.get("step_num", 0)
            if step_num >= len(seq):
                continue

            _, subject_tpl, body_tpl = seq[step_num]
            context = {
                "name": lead.get("contact_name", lead.get("buyer_name", "there")),
                "agency": lead.get("agency", "your agency"),
                "category": lead.get("category", "medical supplies"),
            }

            subject = subject_tpl.format(**context)
            body = body_tpl.format(**context)
            email_id = f"nurture-{lead.get('id', '')}-{step_num}"

            draft = {
                "id": email_id,
                "created_at": now,
                "status": "draft",
                "type": "nurture",
                "to_address": lead.get("email", lead.get("buyer_email", "")),
                "subject": subject,
                "body": body,
                "intent": f"nurture_step_{step_num}",
                "entities": json.dumps({"lead_id": lead.get("id"), "step": step_num}),
            }

            if draft["to_address"]:
                outbox.append(draft)
                step["status"] = "sent"
                step["sent_at"] = now
                step["email_id"] = email_id
                drafts_created += 1
            else:
                step["status"] = "skipped"
                step["skip_reason"] = "no email address"

    _save_leads(leads)
    try:
        from src.core.dal import upsert_outbox_email as _upsert
        for e in outbox:
            if e.get("id"): _upsert(e)
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    if drafts_created:
        log.info("Nurture: created %d email drafts", drafts_created)
    return {"ok": True, "drafts_created": drafts_created, "checked_at": now}


def pause_nurture(lead_id: str, reason: str = "reply_received") -> dict:
    """Pause nurture for a lead (e.g. when they reply)."""
    leads = _load_leads()
    lead = next((l for l in leads if l.get("id") == lead_id), None)
    if not lead:
        return {"ok": False, "error": "lead not found"}

    lead["nurture_active"] = False
    lead["nurture_paused_at"] = datetime.now(timezone.utc).isoformat()
    lead["nurture_pause_reason"] = reason

    # Mark remaining pending steps as paused
    for step in lead.get("nurture_steps", []):
        if step.get("status") == "pending":
            step["status"] = "paused"

    _save_leads(leads)
    log.info("Paused nurture for lead %s: %s", lead_id, reason)
    return {"ok": True}


# ── Dynamic Rescoring ─────────────────────────────────────────────────────────

def rescore_all_leads() -> dict:
    """Recalculate lead scores based on latest activity."""
    leads = _load_leads()
    rescored = 0

    for lead in leads:
        old_score = lead.get("score", 0)
        new_score = _calculate_lead_score(lead)

        if abs(new_score - old_score) >= 5:  # Only update if significant change
            lead["score"] = new_score
            lead["score_updated_at"] = datetime.now(timezone.utc).isoformat()
            lead["score_history"] = lead.get("score_history", [])
            lead["score_history"].append({"score": new_score, "at": lead["score_updated_at"]})
            # Keep only last 10
            lead["score_history"] = lead["score_history"][-10:]
            rescored += 1

            if new_score - old_score >= 15:
                _notify_score_increase(lead, old_score, new_score)

    _save_leads(leads)
    log.info("Rescored leads: %d updated out of %d total", rescored, len(leads))
    return {"ok": True, "rescored": rescored, "total": len(leads)}


def _calculate_lead_score(lead: dict) -> float:
    """Calculate lead score from multiple factors."""
    score = 0.0

    # Base score from original scoring
    score += lead.get("original_score", lead.get("score", 0)) * 0.5

    # Agency value (state agencies = higher)
    agency = (lead.get("agency") or "").upper()
    if any(a in agency for a in ["CDCR", "CCHCS", "CORRECTIONS"]):
        score += 20
    elif any(a in agency for a in ["CALFIRE", "CHP", "HIGHWAY PATROL"]):
        score += 15
    elif any(a in agency for a in ["COUNTY", "CITY"]):
        score += 10

    # Has email → much more actionable
    if lead.get("email") or lead.get("buyer_email"):
        score += 10

    # Engagement signals
    if lead.get("status") == "contacted":
        score += 10
    if lead.get("nurture_active"):
        score += 5

    # Recency — newer leads score higher
    created = lead.get("created_at", "")
    if created:
        try:
            days_old = (datetime.now(timezone.utc) - datetime.fromisoformat(created.replace("Z", "+00:00"))).days
            if days_old < 7:
                score += 15
            elif days_old < 30:
                score += 10
            elif days_old < 90:
                score += 5
            # Old leads lose points
            elif days_old > 180:
                score -= 10
        except Exception as _e:
            log.debug("suppressed: %s", _e)

    # Category match (medical/dental → Reytech's bread and butter)
    category = (lead.get("category") or "").lower()
    if any(c in category for c in ["medical", "dental", "glove", "ppe", "surgical"]):
        score += 10

    return max(0, min(100, round(score, 1)))


def _notify_score_increase(lead: dict, old_score: float, new_score: float):
    """Alert Mike when a lead's score jumps significantly."""
    try:
        from src.agents.notify_agent import send_alert
        send_alert(
            event_type="lead_score_increase",
            title=f"Lead score up: {lead.get('agency', '?')} ({old_score:.0f}→{new_score:.0f})",
            body=f"{lead.get('contact_name', 'Unknown')} at {lead.get('agency', '?')} — "
                 f"score increased from {old_score:.0f} to {new_score:.0f}",
            urgency="info",
            deep_link="/growth"
        )
    except Exception as _e:
        log.debug("suppressed: %s", _e)


# ── Lead → Customer Conversion ────────────────────────────────────────────────

def convert_lead_to_customer(lead_id: str) -> dict:
    """Convert a lead to a CRM contact."""
    leads = _load_leads()
    lead = next((l for l in leads if l.get("id") == lead_id), None)
    if not lead:
        return {"ok": False, "error": "lead not found"}

    now = datetime.now(timezone.utc).isoformat()
    contact_id = f"contact-{uuid.uuid4().hex[:8]}"

    # Create CRM contact
    contact = {
        "id": contact_id,
        "created_at": now,
        "buyer_name": lead.get("contact_name", lead.get("buyer_name", "")),
        "buyer_email": lead.get("email", lead.get("buyer_email", "")),
        "buyer_phone": lead.get("phone", lead.get("buyer_phone", "")),
        "agency": lead.get("agency", ""),
        "title": lead.get("title", ""),
        "department": lead.get("department", ""),
        "notes": f"Converted from lead {lead_id}. Original source: {lead.get('source', 'unknown')}",
        "score": lead.get("score", 0),
        "source": "lead_conversion",
        "converted_from_lead": lead_id,
        "updated_at": now,
    }

    # Save to CRM contacts JSON
    contacts_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(contacts_path) as f:
            contacts = json.load(f)
    except Exception:
        contacts = {}

    contacts[contact_id] = contact
    with open(contacts_path, "w") as f:
        json.dump(contacts, f, indent=2, default=str)

    # Save to DB
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO contacts
                (id, created_at, buyer_name, buyer_email, buyer_phone, agency,
                 title, department, notes, score, source, converted_from_lead, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'lead_conversion', ?, ?)
            """, (contact_id, now, contact["buyer_name"], contact["buyer_email"],
                  contact["buyer_phone"], contact["agency"], contact["title"],
                  contact["department"], contact["notes"], contact["score"],
                  lead_id, now))
    except Exception as e:
        log.warning("convert DB: %s", e)

    # Update lead status
    lead["status"] = "converted"
    lead["converted_at"] = now
    lead["converted_contact_id"] = contact_id
    lead["nurture_active"] = False
    _save_leads(leads)

    log.info("Converted lead %s → contact %s (%s at %s)",
             lead_id, contact_id, contact["buyer_name"], contact["agency"])
    return {"ok": True, "contact_id": contact_id, "contact": contact}


# ── Unified Pipeline View ─────────────────────────────────────────────────────

def get_unified_pipeline() -> dict:
    """Merge leads + growth prospects into a single pipeline view."""
    leads = _load_leads()

    # Also pull growth prospects
    try:
        with open(os.path.join(DATA_DIR, "growth_prospects.json")) as f:
            prospects_data = json.load(f)
            if isinstance(prospects_data, dict):
                prospects = list(prospects_data.values()) if not isinstance(list(prospects_data.values())[0] if prospects_data else {}, list) else []
            else:
                prospects = prospects_data
    except Exception:
        prospects = []

    # Merge (dedup by email)
    seen_emails = {l.get("email", l.get("buyer_email", "")).lower() for l in leads if l.get("email") or l.get("buyer_email")}
    for p in prospects:
        email = (p.get("email") or p.get("buyer_email") or "").lower()
        if email and email not in seen_emails:
            leads.append({
                "id": p.get("id", f"prospect-{uuid.uuid4().hex[:6]}"),
                "contact_name": p.get("name", p.get("buyer_name", "")),
                "email": email,
                "agency": p.get("agency", ""),
                "score": p.get("score", 0),
                "status": p.get("status", "new"),
                "source": "growth_prospect",
                "created_at": p.get("created_at", ""),
            })
            seen_emails.add(email)

    # Sort by score descending
    leads.sort(key=lambda l: l.get("score", 0), reverse=True)

    from collections import Counter
    statuses = Counter(l.get("status", "?") for l in leads)
    sources = Counter(l.get("source", "?") for l in leads)

    return {
        "leads": leads,
        "total": len(leads),
        "by_status": dict(statuses),
        "by_source": dict(sources),
        "avg_score": round(sum(l.get("score", 0) for l in leads) / max(len(leads), 1), 1),
    }


# ── Bulk Nurture Start ────────────────────────────────────────────────────────

def auto_start_nurture_new_leads() -> dict:
    """Start nurture for all 'new' leads that have email and aren't yet nurtured."""
    leads = _load_leads()
    started = 0

    for lead in leads:
        if lead.get("status") != "new":
            continue
        if lead.get("nurture_active"):
            continue
        if not (lead.get("email") or lead.get("buyer_email")):
            continue

        result = start_nurture(lead.get("id", ""))
        if result.get("ok"):
            started += 1

    log.info("Auto-started nurture for %d new leads", started)
    return {"ok": True, "started": started}


# ── Background Scheduler ─────────────────────────────────────────────────────

def _nurture_loop():
    """Daemon loop for daily nurture checks — shutdown-aware."""
    from src.core.scheduler import _shutdown_event, heartbeat
    _shutdown_event.wait(300)  # initial delay for app boot
    while not _shutdown_event.is_set():
        try:
            process_nurture_queue()
            rescore_all_leads()
            heartbeat("lead-nurture", success=True)
        except Exception as e:
            log.error("Lead nurture scheduler: %s", e, exc_info=True)
            heartbeat("lead-nurture", success=False, error=str(e)[:200])
        _shutdown_event.wait(CHECK_INTERVAL)
    log.info("Lead nurture scheduler shutting down")


def start_nurture_scheduler():
    global _scheduler_running
    if _scheduler_running:
        return
    _scheduler_running = True
    threading.Thread(target=_nurture_loop, daemon=True, name="lead-nurture").start()
    log.info("Lead nurture scheduler started (daily)")


# ── Agent Status ──────────────────────────────────────────────────────────────

def get_agent_status() -> dict:
    leads = _load_leads()
    active_nurture = sum(1 for l in leads if l.get("nurture_active"))
    new_leads = sum(1 for l in leads if l.get("status") == "new")
    return {
        "name": "lead_nurture",
        "status": "ok",
        "scheduler_running": _scheduler_running,
        "total_leads": len(leads),
        "new_leads": new_leads,
        "active_nurtures": active_nurture,
    }
