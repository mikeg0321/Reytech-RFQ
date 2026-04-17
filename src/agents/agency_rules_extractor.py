"""Agency Rules Extractor (Phase C).

Pulls buyer emails from sales@reytechinc.com via Gmail API, classifies each
by agency, samples representative threads, and asks Claude to extract
per-agency rules (required forms, delivery/packaging terms, signature
requirements, quote format expectations, past rejection reasons).

Rules are upserted into the `agency_rules` table for consumption by the
Form QA gate (`src/forms/form_qa.py`).

Entry points:
    run_extraction(days=730, agencies=None, dry_run=False, sample_per_agency=30)
    run_for_agency(agency, days=730, sample=30, dry_run=False)

Claude is called with tool-use-forced structured extraction so every
rule has (rule_type, rule_text, confidence, source_email_id).

Rate-limited and cache-friendly — reruns skip already-processed email
threads keyed by gmail message_id.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

log = logging.getLogger("reytech.agency_rules_extractor")


MODEL = "claude-sonnet-4-6"
MAX_EMAIL_CHARS = 8000  # truncate very long bodies before sending to Claude
BATCH_SIZE = 5          # emails per Claude call


AGENCY_KEYWORDS = {
    "cdcr":   ["cdcr", "california department of corrections", "corrections.ca.gov"],
    "cchcs":  ["cchcs", "california correctional health care", "cchcs.ca.gov"],
    "calvet": ["calvet", "veterans home", "ch yountville", "ch chula", "ch barstow",
               "ch fresno", "ch lancaster", "ch redding", "ch ventura", "cdva.ca.gov"],
    "dgs":    ["department of general services", "dgs.ca.gov"],
    "dsh":    ["department of state hospitals", "dsh.ca.gov"],
    "dds":    ["department of developmental services", "dds.ca.gov"],
    "cde":    ["department of education", "cde.ca.gov"],
    "dmv":    ["department of motor vehicles", "dmv.ca.gov"],
    "caltrans": ["caltrans", "department of transportation", "dot.ca.gov"],
}


def _classify_agency(from_addr: str, subject: str, body: str) -> str:
    text = f"{from_addr} {subject} {body[:2000]}".lower()
    for agency, kws in AGENCY_KEYWORDS.items():
        for kw in kws:
            if kw in text:
                return agency
    return ""


def _parse_message(raw_bytes: bytes) -> dict:
    """Parse RFC 2822 bytes into {from, subject, date, body, message_id}."""
    import email
    from email.utils import parsedate_to_datetime
    msg = email.message_from_bytes(raw_bytes)
    body_parts = []
    for part in msg.walk():
        ctype = part.get_content_type()
        disp = str(part.get("Content-Disposition") or "")
        if "attachment" in disp.lower():
            continue
        if ctype == "text/plain":
            try:
                payload = part.get_payload(decode=True)
                if payload:
                    body_parts.append(payload.decode(
                        part.get_content_charset() or "utf-8", errors="replace"))
            except Exception:
                pass
    body = "\n".join(body_parts)
    try:
        dt = parsedate_to_datetime(msg.get("Date", "")) if msg.get("Date") else None
    except Exception:
        dt = None
    return {
        "message_id": msg.get("Message-ID", ""),
        "from": msg.get("From", ""),
        "subject": msg.get("Subject", ""),
        "date": dt.isoformat() if dt else "",
        "body": body[:MAX_EMAIL_CHARS],
    }


def fetch_buyer_emails(days: int = 730,
                       max_messages: int = 2000,
                       inbox: str = "sales") -> List[dict]:
    """Pull inbound emails from last `days` via Gmail API. No outbound.
    Returns list of {message_id, from, subject, date, body, gmail_id}."""
    from src.core.gmail_api import (is_configured, get_service,
                                     list_message_ids, get_raw_message)
    if not is_configured():
        log.warning("Gmail API not configured — fetch_buyer_emails aborted")
        return []

    svc = get_service(inbox)
    since = (datetime.utcnow() - timedelta(days=days)).strftime("%Y/%m/%d")
    # Skip internal reyetech@ chatter and auto-notifications we already filter
    q = f"after:{since} -from:sales@reytechinc.com -category:promotions"
    gmail_ids = list_message_ids(svc, query=q, max_results=max_messages)
    log.info("fetch_buyer_emails: %d gmail ids matched", len(gmail_ids))

    results = []
    for i, gid in enumerate(gmail_ids):
        try:
            raw = get_raw_message(svc, gid)
            parsed = _parse_message(raw)
            parsed["gmail_id"] = gid
            results.append(parsed)
        except Exception as e:
            log.debug("fetch failed for %s: %s", gid, e)
        if (i + 1) % 50 == 0:
            log.info("fetch_buyer_emails: %d/%d", i + 1, len(gmail_ids))
    return results


def _bucket_by_agency(emails: List[dict]) -> Dict[str, List[dict]]:
    buckets: Dict[str, List[dict]] = {}
    for em in emails:
        agency = _classify_agency(em.get("from", ""), em.get("subject", ""),
                                   em.get("body", ""))
        if not agency:
            continue
        buckets.setdefault(agency, []).append(em)
    return buckets


# ── Claude extraction ────────────────────────────────────────────────────

_EXTRACTION_TOOL = {
    "name": "record_agency_rules",
    "description": ("Record one or more rules about how a specific California "
                    "state-agency buyer wants RFQ responses handled. A rule is a "
                    "durable, agency-specific expectation that applies to future "
                    "quotes to this same buyer."),
    "input_schema": {
        "type": "object",
        "properties": {
            "rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "rule_type": {
                            "type": "string",
                            "enum": ["forms", "delivery", "packaging",
                                     "signature", "contact", "quote_format",
                                     "rejection_reason", "misc"],
                            "description": "Category of rule."
                        },
                        "rule_text": {
                            "type": "string",
                            "description": ("Short declarative rule, phrased as "
                                            "instruction to a quoter (≤160 chars). "
                                            "Example: 'Include signed W-9 with every "
                                            "quote submission'.")
                        },
                        "confidence": {
                            "type": "number",
                            "description": "0-1. How durable/generalizable is this rule?"
                        },
                    },
                    "required": ["rule_type", "rule_text", "confidence"],
                }
            }
        },
        "required": ["rules"],
    },
}

_SYSTEM_PROMPT = (
    "You analyze emails from California state-agency buyers to extract durable "
    "RFQ-response rules. Only record rules that apply to FUTURE quotes from the "
    "same agency (not one-off requests). Skip generic politeness, skip "
    "already-known baseline requirements (solicitation #, due date). Focus on "
    "things a vendor would violate by mistake — e.g., 'must be signed in blue ink', "
    "'do not include optional forms unless requested', 'quote valid 60 days minimum'. "
    "If an email shows a buyer correcting a past mistake, extract that as a "
    "rejection_reason rule. Return empty rules array if the email has no durable "
    "guidance."
)


def _extract_rules_batch(agency: str, emails_batch: List[dict]) -> List[dict]:
    """Send one batch to Claude. Returns list of rules with source_email_id
    attached."""
    try:
        import anthropic
    except Exception as e:
        log.error("anthropic SDK unavailable: %s", e)
        return []
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.error("ANTHROPIC_API_KEY missing — cannot extract rules")
        return []
    client = anthropic.Anthropic(api_key=api_key)

    parts = [f"Agency: {agency.upper()}", ""]
    for i, em in enumerate(emails_batch, 1):
        parts.append(f"── Email {i} (id: {em.get('gmail_id','')}) ──")
        parts.append(f"From: {em.get('from','')}")
        parts.append(f"Subject: {em.get('subject','')}")
        parts.append(f"Date: {em.get('date','')}")
        parts.append("")
        parts.append(em.get("body", "")[:MAX_EMAIL_CHARS])
        parts.append("")
    user_block = "\n".join(parts)

    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            system=_SYSTEM_PROMPT,
            tools=[_EXTRACTION_TOOL],
            tool_choice={"type": "tool", "name": "record_agency_rules"},
            messages=[{"role": "user", "content": user_block}],
        )
    except Exception as e:
        log.error("Claude extraction error: %s", e)
        return []

    rules_out: List[dict] = []
    for block in resp.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "record_agency_rules":
            data = block.input or {}
            for rule in data.get("rules", []):
                rules_out.append({
                    "rule_type": rule.get("rule_type", "misc"),
                    "rule_text": (rule.get("rule_text") or "").strip()[:500],
                    "confidence": float(rule.get("confidence") or 0.5),
                    # Cannot reliably attribute to a single email from the batch —
                    # use the whole batch ids so rule lineage is preserved.
                    "source_email_id": ",".join(em.get("gmail_id", "") for em in emails_batch),
                })
    return rules_out


def _already_processed(gmail_ids: List[str]) -> set:
    """Which gmail ids already contributed to a rule? Skip those on rerun."""
    if not gmail_ids:
        return set()
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT source_email_ids FROM agency_rules WHERE source_email_ids != '[]'"
            ).fetchall()
    except Exception:
        return set()
    seen = set()
    for r in rows:
        raw = r[0] if not isinstance(r, dict) else r["source_email_ids"]
        try:
            ids = json.loads(raw or "[]")
            for item in ids:
                if isinstance(item, str):
                    for gid in item.split(","):
                        gid = gid.strip()
                        if gid:
                            seen.add(gid)
        except Exception:
            pass
    return seen & set(gmail_ids)


def run_extraction(days: int = 730,
                   agencies: Optional[List[str]] = None,
                   sample_per_agency: int = 30,
                   dry_run: bool = False) -> dict:
    """End-to-end: fetch emails, bucket by agency, sample, extract, upsert.

    Args:
        days: lookback window
        agencies: restrict to these agency keys (default: all detected)
        sample_per_agency: up to N emails per agency sent to Claude
        dry_run: if True, extract but do NOT upsert

    Returns dict summary.
    """
    t0 = time.time()
    report = {
        "started_at": datetime.utcnow().isoformat(),
        "days": days, "dry_run": dry_run,
        "emails_fetched": 0,
        "agencies": {},
        "rules_upserted": 0,
    }

    emails = fetch_buyer_emails(days=days)
    report["emails_fetched"] = len(emails)
    buckets = _bucket_by_agency(emails)

    if agencies:
        agencies_set = set(a.lower() for a in agencies)
        buckets = {k: v for k, v in buckets.items() if k in agencies_set}

    from src.core.agency_rules import upsert_rule

    for agency, ag_emails in buckets.items():
        # Skip already-processed
        skip = _already_processed([em.get("gmail_id", "") for em in ag_emails])
        fresh = [em for em in ag_emails if em.get("gmail_id") not in skip]
        fresh = fresh[:sample_per_agency]

        agency_report = {
            "total_emails": len(ag_emails),
            "fresh_emails": len(fresh),
            "skipped_processed": len(ag_emails) - len(fresh),
            "rules_extracted": 0,
            "rules_upserted": 0,
        }

        if not fresh:
            report["agencies"][agency] = agency_report
            continue

        # Batch to Claude
        for i in range(0, len(fresh), BATCH_SIZE):
            batch = fresh[i:i + BATCH_SIZE]
            rules = _extract_rules_batch(agency, batch)
            agency_report["rules_extracted"] += len(rules)
            if not dry_run:
                for r in rules:
                    upsert_rule(
                        agency=agency,
                        rule_type=r["rule_type"],
                        rule_text=r["rule_text"],
                        source_email_id=r["source_email_id"],
                        confidence=r["confidence"],
                    )
                    agency_report["rules_upserted"] += 1
                    report["rules_upserted"] += 1

        report["agencies"][agency] = agency_report

    report["duration_sec"] = round(time.time() - t0, 1)
    report["completed_at"] = datetime.utcnow().isoformat()
    return report


def run_for_agency(agency: str, days: int = 730, sample: int = 30,
                   dry_run: bool = False) -> dict:
    return run_extraction(days=days, agencies=[agency],
                          sample_per_agency=sample, dry_run=dry_run)


__all__ = [
    "fetch_buyer_emails",
    "run_extraction",
    "run_for_agency",
    "AGENCY_KEYWORDS",
]
