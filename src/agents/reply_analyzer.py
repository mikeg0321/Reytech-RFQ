"""
reply_analyzer.py — Email Reply Intelligence
Phase 20 | Detects buy signals, rejections, and questions from email replies

When a buyer replies to a quote email, this module analyzes the reply
to detect:
- WIN signals: "approved", "proceed", "PO attached", "we accept"
- LOSS signals: "went with another vendor", "too expensive", "no longer needed"
- QUESTION signals: "can you do better on price?", "what about shipping?"
- TRACKING signals: "shipped", "tracking number"

Integrates with email poller to auto-flag quotes.
"""

import re
import logging
from typing import Optional

log = logging.getLogger("reply_analyzer")

# ── Shared DB Context (Anthropic Skills Guide: Pattern 5 — Domain Intelligence) ──
# Full access to live CRM, quotes, revenue, price history, voice calls from SQLite.
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

# ─── Signal Patterns ────────────────────────────────────────────────────────

WIN_PATTERNS = [
    r"(?:please\s+)?proceed",
    r"(?:we(?:'d| would)\s+like\s+to\s+)?(?:go\s+ahead|move\s+forward|accept)",
    r"approved",
    r"po\s+(?:number|#|attached|enclosed|is)\b",
    r"purchase\s+order\s+(?:number|#|attached|enclosed|is)\b",
    r"award(?:ed|ing)?",
    r"congratulations",
    r"(?:you(?:'ve| have)\s+)?won\s+(?:the|this)",
    r"place\s+(?:the\s+)?order",
    r"we(?:'ll|\s+will)\s+(?:go\s+with|use|choose)\s+(?:you|reytech)",
    r"send\s+(?:us\s+)?(?:the\s+)?invoice",
    r"ship\s+(?:it|them|the\s+order)",
]

LOSS_PATTERNS = [
    r"(?:went|going)\s+with\s+(?:another|different|other)\s+vendor",
    r"(?:too\s+)?(?:expensive|high|pricey|costly)",
    r"(?:no\s+longer|don't|do\s+not)\s+need",
    r"cancel(?:led|ing)?",
    r"(?:not|won't|will\s+not)\s+(?:be\s+)?(?:proceed|moving|going\s+forward)",
    r"(?:decided\s+(?:to\s+)?(?:go|not)|chose\s+(?:another|not\s+to))",
    r"budget\s+(?:cut|reduced|issue|constraint)",
    r"(?:found|got)\s+(?:a\s+)?(?:better|lower|cheaper)\s+(?:price|quote|deal)",
    r"project\s+(?:cancelled|postponed|on\s+hold)",
    r"(?:unable|can't|cannot)\s+(?:to\s+)?(?:approve|fund|proceed)",
]

QUESTION_PATTERNS = [
    r"(?:can\s+you|could\s+you|is\s+it\s+possible\s+to)\s+(?:do\s+better|lower|reduce|match|beat)",
    r"(?:what\s+about|how\s+about|any\s+discount)",
    r"(?:is\s+(?:there|that)\s+)?(?:free\s+shipping|shipping\s+included)",
    r"(?:when\s+(?:can|would)|how\s+(?:soon|quickly))\s+(?:you|it|they)\s+(?:ship|deliver|arrive)",
    r"(?:do\s+you\s+have|is\s+.+\s+in\s+stock|availability)",
    r"\?",
]

PO_NUMBER_PATTERN = re.compile(
    r"(?:PO|purchase\s+order)(?:\s+(?:number|#|no\.?)\s*(?:is|:)?)?\s*[:#-]?\s*([A-Z]*\d+[-A-Z0-9]*)", re.IGNORECASE)

QUOTE_REF_PATTERN = re.compile(
    r"(?:quote|quotation|ref|reference)[\s#:-]*([A-Z0-9-]{3,15})", re.IGNORECASE)


def analyze_reply(subject: str, body: str, sender: str = "") -> dict:
    """
    Analyze an email reply for buy/loss/question signals.
    
    Returns:
        {
            signal: "win" | "loss" | "question" | "neutral",
            confidence: 0-1,
            quote_ref: str,  # detected quote number
            po_number: str,  # detected PO number
            triggers: [...],  # matched patterns
            summary: str,     # human-readable summary
        }
    """
    text = f"{subject}\n{body}".lower()
    text_clean = re.sub(r'\s+', ' ', text)

    triggers = []
    win_score = 0
    loss_score = 0
    question_score = 0

    # Check win patterns
    for pattern in WIN_PATTERNS:
        if re.search(pattern, text_clean, re.IGNORECASE):
            win_score += 1
            triggers.append(f"win: {pattern[:40]}")

    # Check loss patterns
    for pattern in LOSS_PATTERNS:
        if re.search(pattern, text_clean, re.IGNORECASE):
            loss_score += 1
            triggers.append(f"loss: {pattern[:40]}")

    # Check question patterns
    for pattern in QUESTION_PATTERNS:
        if re.search(pattern, text_clean, re.IGNORECASE):
            question_score += 1
            triggers.append(f"question: {pattern[:40]}")

    # Extract references
    po_match = PO_NUMBER_PATTERN.search(f"{subject}\n{body}")
    po_number = po_match.group(1) if po_match else ""

    quote_match = QUOTE_REF_PATTERN.search(f"{subject}\n{body}")
    quote_ref = quote_match.group(1) if quote_match else ""

    # Also check subject for quote number (e.g., "RE: Reytech Quote R26Q1")
    subj_match = re.search(r"R\d{2}Q\d+", subject, re.IGNORECASE)
    if subj_match and not quote_ref:
        quote_ref = subj_match.group(0)

    # Determine signal
    if win_score > loss_score and win_score > question_score:
        signal = "win"
        confidence = min(1.0, win_score * 0.3 + (0.2 if po_number else 0))
    elif loss_score > win_score and loss_score > question_score:
        signal = "loss"
        confidence = min(1.0, loss_score * 0.3)
    elif question_score > 0:
        signal = "question"
        confidence = min(1.0, question_score * 0.2)
    else:
        signal = "neutral"
        confidence = 0.1

    # Boost confidence if PO number found with win signal
    if signal == "win" and po_number:
        confidence = min(1.0, confidence + 0.3)

    # Summary
    summaries = {
        "win": f"Buyer appears ready to proceed" + (f" — PO: {po_number}" if po_number else ""),
        "loss": "Buyer may be declining or going with another vendor",
        "question": "Buyer has questions — follow up needed",
        "neutral": "No clear buy/loss signal detected",
    }

    return {
        "signal": signal,
        "confidence": round(confidence, 2),
        "quote_ref": quote_ref,
        "po_number": po_number,
        "triggers": triggers[:10],
        "win_score": win_score,
        "loss_score": loss_score,
        "question_score": question_score,
        "summary": summaries[signal],
    }


def find_quote_from_reply(subject: str, body: str, sender: str,
                           quotes: list) -> Optional[dict]:
    """
    Try to match a reply email to a specific quote.
    Searches by: quote number in subject, sender email, institution name.
    """
    analysis = analyze_reply(subject, body, sender)
    quote_ref = analysis.get("quote_ref", "")

    # Direct quote number match
    if quote_ref:
        for q in quotes:
            if q.get("quote_number", "").upper() == quote_ref.upper():
                return {**analysis, "matched_quote": q.get("quote_number")}

    # Match by sender email to buyer in recent quotes
    sender_lower = sender.lower()
    if sender_lower:
        for q in sorted(quotes, key=lambda x: x.get("created_at", ""), reverse=True):
            q_email = q.get("buyer_email", "").lower() or q.get("email", "").lower()
            if q_email and q_email == sender_lower:
                return {**analysis, "matched_quote": q.get("quote_number")}

    # Match by institution name in body
    text = f"{subject} {body}".lower()
    for q in sorted(quotes, key=lambda x: x.get("created_at", ""), reverse=True)[:20]:
        inst = q.get("institution", "")
        if inst and inst.lower() in text:
            return {**analysis, "matched_quote": q.get("quote_number")}

    return {**analysis, "matched_quote": None}
