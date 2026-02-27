"""
Smart Email Classification v2 (F6)

Scoring system that classifies incoming emails across 5 dimensions.
Wraps existing detection logic with confidence scores and audit trail.

Usage:
    from src.agents.email_classifier import classify_email, log_classification
    result = classify_email(subject, body, sender, attachments)
    log_classification(result, email_uid)
"""

import os
import re
import logging
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger("reytech.email_classifier")

# ── Scoring Weights ──────────────────────────────────────────────────────────

# Each dimension gets a score 0.0 - 1.0
# Classification = highest scoring dimension
# Confidence = gap between top two scores

SIGNALS = {
    "new_pc": {
        "attachment_704": 0.40,     # AMS 704 attachment
        "quote_request_subject": 0.25,  # "Quote request" in subject
        "new_sender": 0.15,         # Never seen this sender before
        "item_list_body": 0.10,     # Body contains item descriptions/quantities
        "price_inquiry": 0.10,      # "price", "quote", "pricing" in body
    },
    "new_rfq": {
        "attachment_703b_704b": 0.35,  # 703B/704B/Bid package
        "pr_number_subject": 0.25,     # PR/solicitation number in subject
        "bid_keywords": 0.20,          # "bid", "solicitation", "proposal"
        "formal_language": 0.10,       # Government formal language patterns
        "deadline_mentioned": 0.10,    # Due date / deadline / respond by
    },
    "reply_followup": {
        "re_prefix": 0.30,            # RE: / FW: in subject
        "references_active": 0.30,     # References an active PC/quote number
        "same_thread": 0.20,          # Same sender + recent conversation
        "short_body": 0.10,           # Short body (typical of replies)
        "no_new_attachments": 0.10,   # No new 704/703B attachments
    },
    "po_award": {
        "po_number_found": 0.40,       # PO number detected
        "purchase_order_subject": 0.25, # "Purchase Order" in subject
        "fiscal_year_ref": 0.15,       # Fiscal year reference
        "award_keywords": 0.10,        # "award", "encumbrance"
        "amount_mentioned": 0.10,      # Dollar amount in body
    },
    "cs_inquiry": {
        "question_marks": 0.25,        # Multiple question marks
        "clarification_keywords": 0.25, # "clarify", "question", "status"
        "no_attachments": 0.20,        # No file attachments
        "short_body": 0.15,            # Brief message
        "existing_relationship": 0.15, # Known sender with history
    },
}


def classify_email(subject: str, body: str, sender: str,
                   attachment_names: list = None, known_senders: set = None) -> dict:
    """
    Score email across 5 dimensions and return classification.

    Returns:
        {
            "classification": "new_pc" | "new_rfq" | "reply_followup" | "po_award" | "cs_inquiry",
            "confidence": 0.0-1.0,
            "scores": {"new_pc": 0.65, "new_rfq": 0.10, ...},
            "signals_matched": {"new_pc": ["quote_request_subject", "item_list_body"], ...},
            "needs_review": True/False (confidence < 0.6),
        }
    """
    subject = (subject or "").strip()
    body = (body or "").strip()
    sender = (sender or "").strip().lower()
    attachment_names = attachment_names or []
    known_senders = known_senders or set()

    subj_lower = subject.lower()
    body_lower = body.lower()
    att_lower = [a.lower() for a in attachment_names]
    att_str = " ".join(att_lower)

    scores = {}
    matched = {}

    # ── Score: New PC ────────────────────────────────────────────────────────
    pc_score = 0.0
    pc_signals = []

    if any("704" in a and "703" not in a for a in att_lower):
        pc_score += SIGNALS["new_pc"]["attachment_704"]
        pc_signals.append("attachment_704")

    if any(kw in subj_lower for kw in ("quote request", "price check", "pricing request", "request for quote")):
        pc_score += SIGNALS["new_pc"]["quote_request_subject"]
        pc_signals.append("quote_request_subject")

    if sender not in known_senders and sender:
        pc_score += SIGNALS["new_pc"]["new_sender"]
        pc_signals.append("new_sender")

    # Item list patterns: numbered items, quantities, UOM
    if re.search(r'\d+\s*(ea|each|cs|case|bx|box|pk|pack|dz|dozen)\b', body_lower):
        pc_score += SIGNALS["new_pc"]["item_list_body"]
        pc_signals.append("item_list_body")

    if any(kw in body_lower for kw in ("price", "quote", "pricing", "how much", "cost")):
        pc_score += SIGNALS["new_pc"]["price_inquiry"]
        pc_signals.append("price_inquiry")

    scores["new_pc"] = min(pc_score, 1.0)
    matched["new_pc"] = pc_signals

    # ── Score: New RFQ ───────────────────────────────────────────────────────
    rfq_score = 0.0
    rfq_signals = []

    if any(x in att_str for x in ("703b", "704b", "bid pkg", "bid package", "solicitation")):
        rfq_score += SIGNALS["new_rfq"]["attachment_703b_704b"]
        rfq_signals.append("attachment_703b_704b")

    if re.search(r'(PR|SOL|IFB|RFQ|RFP)[\s#-]*\d{2,}', subject):
        rfq_score += SIGNALS["new_rfq"]["pr_number_subject"]
        rfq_signals.append("pr_number_subject")

    if any(kw in body_lower for kw in ("bid", "solicitation", "proposal", "invitation for bid")):
        rfq_score += SIGNALS["new_rfq"]["bid_keywords"]
        rfq_signals.append("bid_keywords")

    if any(kw in body_lower for kw in ("pursuant to", "in accordance with", "state contract")):
        rfq_score += SIGNALS["new_rfq"]["formal_language"]
        rfq_signals.append("formal_language")

    if re.search(r'(due|deadline|respond by|submit by|close[sd]?)\s*(date|on|by)?:?\s*\d', body_lower):
        rfq_score += SIGNALS["new_rfq"]["deadline_mentioned"]
        rfq_signals.append("deadline_mentioned")

    scores["new_rfq"] = min(rfq_score, 1.0)
    matched["new_rfq"] = rfq_signals

    # ── Score: Reply/Follow-up ───────────────────────────────────────────────
    reply_score = 0.0
    reply_signals = []

    if re.match(r'^(RE|FW|FWD)\s*:', subject, re.I):
        reply_score += SIGNALS["reply_followup"]["re_prefix"]
        reply_signals.append("re_prefix")

    # Check for quote/PC number references
    if re.search(r'R\d{2}Q\d+|PC-\w+|PCID-\w+', subject + " " + body[:500]):
        reply_score += SIGNALS["reply_followup"]["references_active"]
        reply_signals.append("references_active")

    if sender in known_senders:
        reply_score += SIGNALS["reply_followup"]["same_thread"]
        reply_signals.append("same_thread")

    if len(body.split()) < 50:
        reply_score += SIGNALS["reply_followup"]["short_body"]
        reply_signals.append("short_body")

    if not attachment_names or all("image" in a.lower() or "sig" in a.lower() for a in attachment_names):
        reply_score += SIGNALS["reply_followup"]["no_new_attachments"]
        reply_signals.append("no_new_attachments")

    scores["reply_followup"] = min(reply_score, 1.0)
    matched["reply_followup"] = reply_signals

    # ── Score: PO/Award ──────────────────────────────────────────────────────
    po_score = 0.0
    po_signals = []

    if re.search(r'PO[\s#-]*\d{3,}|P\.?O\.?\s*\d{3,}', subject + " " + body[:500]):
        po_score += SIGNALS["po_award"]["po_number_found"]
        po_signals.append("po_number_found")

    if any(kw in subj_lower for kw in ("purchase order", "po #", "po#", "encumbrance")):
        po_score += SIGNALS["po_award"]["purchase_order_subject"]
        po_signals.append("purchase_order_subject")

    if re.search(r'(FY|fiscal year)\s*\d{2,4}', body, re.I):
        po_score += SIGNALS["po_award"]["fiscal_year_ref"]
        po_signals.append("fiscal_year_ref")

    if any(kw in body_lower for kw in ("award", "encumbrance", "obligat")):
        po_score += SIGNALS["po_award"]["award_keywords"]
        po_signals.append("award_keywords")

    if re.search(r'\$[\d,]+\.?\d*', body):
        po_score += SIGNALS["po_award"]["amount_mentioned"]
        po_signals.append("amount_mentioned")

    scores["po_award"] = min(po_score, 1.0)
    matched["po_award"] = po_signals

    # ── Score: CS/Inquiry ────────────────────────────────────────────────────
    cs_score = 0.0
    cs_signals = []

    question_count = body.count("?")
    if question_count >= 2:
        cs_score += SIGNALS["cs_inquiry"]["question_marks"]
        cs_signals.append("question_marks")

    if any(kw in body_lower for kw in ("clarif", "question", "status", "update on", "where is", "when will")):
        cs_score += SIGNALS["cs_inquiry"]["clarification_keywords"]
        cs_signals.append("clarification_keywords")

    if not attachment_names:
        cs_score += SIGNALS["cs_inquiry"]["no_attachments"]
        cs_signals.append("no_attachments")

    if len(body.split()) < 80:
        cs_score += SIGNALS["cs_inquiry"]["short_body"]
        cs_signals.append("short_body")

    if sender in known_senders:
        cs_score += SIGNALS["cs_inquiry"]["existing_relationship"]
        cs_signals.append("existing_relationship")

    scores["cs_inquiry"] = min(cs_score, 1.0)
    matched["cs_inquiry"] = cs_signals

    # ── Determine winner ─────────────────────────────────────────────────────
    sorted_scores = sorted(scores.items(), key=lambda x: -x[1])
    top = sorted_scores[0]
    runner_up = sorted_scores[1] if len(sorted_scores) > 1 else ("none", 0)

    # Confidence = margin between top two
    confidence = round(top[1] - runner_up[1], 3) if top[1] > 0 else 0.0
    # Also factor in absolute score
    if top[1] < 0.2:
        confidence = 0.0  # Too weak to classify

    return {
        "classification": top[0],
        "confidence": confidence,
        "top_score": round(top[1], 3),
        "scores": {k: round(v, 3) for k, v in scores.items()},
        "signals_matched": matched,
        "needs_review": confidence < 0.15 or top[1] < 0.3,
    }


def log_classification(result: dict, email_uid: str, subject: str = "", sender: str = ""):
    """Log classification to SQLite for audit trail."""
    try:
        from src.core.db import get_db
        import json
        now = datetime.now(timezone.utc).isoformat()
        with get_db() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS email_classifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_uid TEXT, classified_at TEXT,
                classification TEXT, confidence REAL, top_score REAL,
                scores_json TEXT, signals_json TEXT,
                needs_review INTEGER, subject TEXT, sender TEXT
            )""")
            conn.execute("""
                INSERT INTO email_classifications
                (email_uid, classified_at, classification, confidence, top_score,
                 scores_json, signals_json, needs_review, subject, sender)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                email_uid, now, result["classification"],
                result["confidence"], result["top_score"],
                json.dumps(result["scores"]),
                json.dumps(result["signals_matched"]),
                1 if result["needs_review"] else 0,
                (subject or "")[:200], (sender or "")[:100],
            ))
    except Exception as e:
        log.debug("Classification log error: %s", e)


def get_review_queue(limit: int = 20) -> list:
    """Get emails needing manual review (low confidence classifications)."""
    try:
        from src.core.db import get_db
        import json
        with get_db() as conn:
            # Ensure table exists
            conn.execute("""CREATE TABLE IF NOT EXISTS email_classifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_uid TEXT, classified_at TEXT,
                classification TEXT, confidence REAL, top_score REAL,
                scores_json TEXT, signals_json TEXT,
                needs_review INTEGER, subject TEXT, sender TEXT
            )""")
            rows = conn.execute("""
                SELECT email_uid, classified_at, classification, confidence,
                       top_score, scores_json, subject, sender
                FROM email_classifications
                WHERE needs_review = 1
                ORDER BY classified_at DESC LIMIT ?
            """, (limit,)).fetchall()
            return [{
                "uid": r[0], "classified_at": r[1], "classification": r[2],
                "confidence": r[3], "top_score": r[4],
                "scores": json.loads(r[5] or "{}"),
                "subject": r[6], "sender": r[7],
            } for r in rows]
    except Exception as e:
        log.debug("Review queue error: %s", e)
        return []
