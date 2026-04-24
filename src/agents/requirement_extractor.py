"""
requirement_extractor.py — Extract structured requirements from buyer emails.

The buyer email IS the contract. This module parses the email body + subject
into structured requirements that the QA pipeline validates against.

Architecture:
  1. Try Claude Haiku (fast, cheap structured extraction)
  2. Fall back to regex patterns if API unavailable
  3. Return RFQRequirements dataclass, serializable to JSON
  4. Never raise — always return a result (possibly empty)

Cost: ~$0.001 per extraction (Haiku, small prompt)
"""

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

log = logging.getLogger("reytech.requirement_extractor")

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# Reuse the form patterns from agency_config
try:
    from src.core.agency_config import FORM_TEXT_PATTERNS
except ImportError:
    FORM_TEXT_PATTERNS = {}

# ── Trusted URL domains (never auto-download from untrusted sources) ────────
TRUSTED_URL_DOMAINS = [
    "ca.gov", "sharepoint.com", "onedrive.live.com",
    "drive.google.com", "docs.google.com",
]

# ── API Configuration ───────────────────────────────────────────────────────
_API_TIMEOUT = 10  # seconds — fail fast, don't slow email processing
_MODEL = "claude-haiku-4-5-20251001"


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class RFQRequirements:
    """Structured requirements extracted from a buyer email.

    All fields are optional — extraction may only find some.
    confidence indicates how reliable the extraction is (0.0-1.0).
    """
    forms_required: list = field(default_factory=list)
    due_date: str = ""              # ISO format "2026-04-15"
    due_time: str = ""              # "5:00 PM" or "COB"
    special_instructions: list = field(default_factory=list)
    delivery_location: str = ""
    buyer_name: str = ""
    buyer_email: str = ""
    buyer_phone: str = ""
    solicitation_number: str = ""
    food_items_present: bool = False
    attachment_types: list = field(default_factory=list)
    template_urls: list = field(default_factory=list)
    confidence: float = 0.0
    extraction_method: str = "none"  # "claude" | "regex" | "none"
    raw_form_matches: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "RFQRequirements":
        if not d or not isinstance(d, dict):
            return cls()
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})

    @property
    def has_requirements(self) -> bool:
        return bool(self.forms_required or self.due_date
                    or self.special_instructions or self.solicitation_number)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def extract_requirements(
    email_body: str,
    subject: str = "",
    attachments: list = None,
) -> RFQRequirements:
    """Extract structured requirements from an email.

    Primary: Claude Haiku API (structured JSON extraction).
    Fallback: Regex patterns (offline, fast).

    Args:
        email_body: Full email body text.
        subject: Email subject line.
        attachments: List of attachment dicts [{filename, path, ...}].

    Returns:
        RFQRequirements — never raises, always returns a result.
    """
    if not email_body and not subject:
        return RFQRequirements()

    attachments = attachments or []
    combined_text = f"Subject: {subject}\n\n{email_body}" if subject else email_body

    # Try Claude first
    result = _extract_with_claude(combined_text, attachments)
    if result and result.has_requirements:
        return result

    # Fallback to regex
    result = _extract_with_regex(combined_text, attachments)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# CLAUDE API EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

_EXTRACTION_PROMPT = """You are parsing a buyer email for a California government procurement RFQ/Price Check.
Extract these fields from the email. Return ONLY valid JSON, no other text.

{
  "forms_required": ["std204", "dvbe843", ...],
  "due_date": "YYYY-MM-DD or empty",
  "due_time": "time string or empty",
  "special_instructions": ["instruction 1", ...],
  "delivery_location": "address or empty",
  "buyer_name": "name or empty",
  "buyer_phone": "phone or empty",
  "solicitation_number": "number or empty",
  "food_items_present": false,
  "template_urls": ["url1", ...]
}

Form IDs to use: std204, std205, dvbe843, darfur_act, cv012_cuf, calrecycle74,
bidder_decl, std1000, sellers_permit, w9, obs_1600, 703b, 703c, 704b, quote.

Rules:
- food_items_present = true if email mentions food, agricultural products, OBS 1600, or perishable items
- Only include forms that are explicitly requested, not inferred
- template_urls: only include actual URLs from the email body
- If a field is not found, use empty string or empty list"""


def _get_api_key() -> str:
    for var in ("AGENT_PRICING_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var, "")
        if key:
            return key
    return ""


def _extract_with_claude(text: str, attachments: list) -> Optional[RFQRequirements]:
    """Try Claude Haiku for structured extraction."""
    api_key = _get_api_key()
    if not api_key or not HAS_REQUESTS:
        return None

    try:
        # Include attachment filenames for context
        att_names = [a.get("filename", "") for a in attachments if a.get("filename")]
        user_msg = text
        if att_names:
            user_msg += f"\n\nAttachments: {', '.join(att_names)}"

        request_body = {
            "model": _MODEL,
            "max_tokens": 1024,
            "system": [{"type": "text", "text": _EXTRACTION_PROMPT,
                        "cache_control": {"type": "ephemeral"}}],
            "messages": [{"role": "user", "content": user_msg[:8000]}],
        }
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers, json=request_body, timeout=_API_TIMEOUT,
        )

        if resp.status_code == 429:
            log.debug("Requirement extractor: 429 rate limited, falling back to regex")
            return None

        if resp.status_code != 200:
            log.debug("Requirement extractor: API error %d", resp.status_code)
            return None

        data = resp.json()
        full_text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                full_text += block.get("text", "")

        if not full_text:
            return None

        # Parse JSON from response
        parsed = _parse_json_response(full_text)
        if not parsed:
            return None

        # Classify attachments
        att_types = _classify_attachments(attachments)

        result = RFQRequirements(
            forms_required=parsed.get("forms_required", []),
            due_date=parsed.get("due_date", ""),
            due_time=parsed.get("due_time", ""),
            special_instructions=parsed.get("special_instructions", []),
            delivery_location=parsed.get("delivery_location", ""),
            buyer_name=parsed.get("buyer_name", ""),
            buyer_phone=parsed.get("buyer_phone", ""),
            solicitation_number=parsed.get("solicitation_number", ""),
            food_items_present=bool(parsed.get("food_items_present", False)),
            attachment_types=att_types,
            template_urls=_filter_trusted_urls(parsed.get("template_urls", [])),
            confidence=0.85,
            extraction_method="claude",
        )
        log.info("Extracted requirements via Claude: %d forms, due=%s, conf=%.2f",
                 len(result.forms_required), result.due_date, result.confidence)
        return result

    except requests.exceptions.Timeout:
        log.debug("Requirement extractor: API timeout after %ds", _API_TIMEOUT)
        return None
    except Exception as e:
        log.debug("Requirement extractor Claude error: %s", e)
        return None


def _parse_json_response(text: str) -> Optional[dict]:
    """Extract JSON from Claude's response text."""
    # Try direct parse first
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as _e:
        log.debug("suppressed: %s", _e)

    # Try extracting JSON block from markdown
    m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError as _e:
            log.debug("suppressed: %s", _e)

    # Try finding first { to last }
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError as _e:
            log.debug("suppressed: %s", _e)

    return None


# ═══════════════════════════════════════════════════════════════════════════
# REGEX FALLBACK EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

def _extract_with_regex(text: str, attachments: list) -> RFQRequirements:
    """Extract requirements using pattern matching (offline fallback)."""
    forms = _detect_forms(text)
    due_date = _extract_due_date(text)
    due_time = _extract_due_time(text)
    sol_num = _extract_solicitation_number(text)
    phone = _extract_phone(text)
    food = _detect_food_items(text)
    urls = _extract_urls(text)
    instructions = _extract_special_instructions(text)
    att_types = _classify_attachments(attachments)

    return RFQRequirements(
        forms_required=forms,
        due_date=due_date,
        due_time=due_time,
        special_instructions=instructions,
        solicitation_number=sol_num,
        buyer_phone=phone,
        food_items_present=food,
        attachment_types=att_types,
        template_urls=_filter_trusted_urls(urls),
        confidence=0.60 if forms or due_date else 0.30,
        extraction_method="regex",
        raw_form_matches=forms,
    )


def _detect_forms(text: str) -> list:
    """Detect required forms from text using FORM_TEXT_PATTERNS."""
    if not text:
        return []
    upper = text.upper()
    found = []
    for form_id, patterns in FORM_TEXT_PATTERNS.items():
        for pattern in patterns:
            if pattern.upper() in upper:
                if form_id not in found:
                    found.append(form_id)
                break
    return found


def _parse_numeric_date(raw: str) -> str:
    """Parse `M/D/YY[YY]` or `M-D-YY[YY]` into ISO YYYY-MM-DD. Returns
    "" on parse failure. Normalizes the separator so both / and - work."""
    raw = raw.replace("-", "/").strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _parse_long_date(raw: str) -> str:
    """Parse `Month DD, YYYY` (with or without comma) into ISO. Returns
    "" on parse failure."""
    cleaned = raw.replace(",", "").strip()
    for fmt in ("%B %d %Y", "%b %d %Y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


# Patterns that explicitly LABEL a field as a deadline. These fire on
# common government RFQ formats where the date follows a label rather
# than a verb ("Due Date: 4/24/2026" instead of "due 4/24/2026"). Each
# tuple is (regex, parser_kind) where parser_kind picks numeric vs
# long-form date parsing.
#
# Order matters — most-specific labels first. The "by/due/before" verb
# patterns from the original implementation come AFTER the labeled
# patterns so a labeled date wins over a stray verb match.
_DUE_DATE_LABELED = [
    # "Due Date: 4/24/2026", "Date Due: 04-24-26"
    (re.compile(
        r'(?:Due\s+Date|Date\s+Due|Quote\s+Due|Bid\s+Due|Response\s+Due|'
        r'Bid\s+Open(?:ing)?\s+Date|Closing\s+Date|Submission\s+Deadline|'
        r'Response\s+Required|Required\s+By|Submit\s+By|Reply\s+By|'
        r'Quotes?\s+Due|Bids?\s+Due|Quote\s+Needed|Need\s+Quote\s+By)'
        r'\s*[:\-]?\s*'
        r'(?:end\s+of\s+business\s+)?(?:by\s+)?(?:\w+day,?\s+)?'
        r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
        re.IGNORECASE,
    ), "numeric"),
    # "Due Date: April 24, 2026"
    (re.compile(
        r'(?:Due\s+Date|Date\s+Due|Quote\s+Due|Bid\s+Due|Response\s+Due|'
        r'Bid\s+Open(?:ing)?\s+Date|Closing\s+Date|Submission\s+Deadline|'
        r'Response\s+Required|Required\s+By|Submit\s+By|Reply\s+By|'
        r'Quotes?\s+Due|Bids?\s+Due)'
        r'\s*[:\-]?\s*'
        r'(?:by\s+)?(?:\w+day,?\s+)?'
        r'([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})',
        re.IGNORECASE,
    ), "long"),
    # Bare "Closing 4/24/2026" / "Closes 4/24/2026"
    (re.compile(
        r'\b(?:Closing|Closes|Closed)\s+'
        r'(?:on\s+)?(?:end\s+of\s+business\s+)?(?:\w+day,?\s+)?'
        r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
        re.IGNORECASE,
    ), "numeric"),
]

# Verb-prefixed patterns: "by April 24, 2026", "due 4/24". These were
# the original implementation; preserved verbatim so we don't regress.
_DUE_DATE_VERBS = [
    (re.compile(
        r'(?:by|due|deadline|before|no\s+later\s+than)\s+'
        r'(?:end\s+of\s+business\s+)?(?:close\s+of\s+business\s+)?'
        r'(?:\w+day,?\s+)?'
        r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
        re.IGNORECASE,
    ), "numeric"),
    (re.compile(
        r'(?:by|due|deadline|before|no\s+later\s+than)\s+'
        r'(?:end\s+of\s+business\s+)?(?:close\s+of\s+business\s+)?'
        r'(?:\w+day,?\s+)?'
        r'([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})',
        re.IGNORECASE,
    ), "long"),
]


def _extract_due_date(text: str) -> str:
    """Extract due date from email body. Returns YYYY-MM-DD or "".

    Detection priority:
      1. Labeled patterns ("Due Date:", "Closing Date:", "Submit By:") —
         most specific, least false-positive risk.
      2. Verb-prefixed patterns ("by 4/24", "due April 24, 2026") —
         original behavior, preserved unchanged.

    Numeric formats accept both `/` and `-` separators. Long formats
    accept "April" and abbreviated "Apr" month names. Year is required
    (no inference from current year — too ambiguous for a quote
    deadline that could be in 2025 or 2026).
    """
    if not text:
        return ""

    for pat, kind in _DUE_DATE_LABELED:
        m = pat.search(text)
        if m:
            iso = (_parse_numeric_date(m.group(1)) if kind == "numeric"
                   else _parse_long_date(m.group(1)))
            if iso:
                return iso

    for pat, kind in _DUE_DATE_VERBS:
        m = pat.search(text)
        if m:
            iso = (_parse_numeric_date(m.group(1)) if kind == "numeric"
                   else _parse_long_date(m.group(1)))
            if iso:
                return iso

    return ""


# Time-extraction patterns. Order matters: explicit `at HH:MM` after a
# date wins over bare-time fallback so "due 4/24 at 2:00 PM PST" lands
# the right value.
_DUE_TIME_PATTERNS = [
    # "by 2:00 PM PST", "before 14:00", "at 2:00 PM"
    re.compile(
        r'(?:by|before|at|until|no\s+later\s+than)\s+'
        r'(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?\s*(?:PST|PDT|PT|EST|EDT|ET|CST|CDT|MST|MDT)?)',
        re.IGNORECASE,
    ),
    # "by noon" / "at noon"
    re.compile(r'(?:by|before|at|until)\s+(noon|midnight)', re.IGNORECASE),
    # Standalone "5:00 PM PST" anywhere in body — last resort, no
    # surrounding verb required. Common in form fields like
    # "Quote Due: 4/24/2026 5:00 PM PST".
    re.compile(
        r'\b(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)\s*(?:PST|PDT|PT|EST|EDT|ET|CST|CDT|MST|MDT)?)',
    ),
    # "5pm PST" without colon
    re.compile(
        r'\b(\d{1,2}\s*(?:AM|PM|am|pm)\s*(?:PST|PDT|PT|EST|EDT|ET|CST|CDT|MST|MDT)?)\b',
    ),
]


def _extract_due_time(text: str) -> str:
    """Extract due time from email body. Returns the matched time
    substring (caller's parser handles "12:00 PM PST" forms) or
    "COB" / "EOD" sentinel if only a "close of business" hint exists.

    Returns "" when nothing parseable found.
    """
    if not text:
        return ""
    for pat in _DUE_TIME_PATTERNS:
        m = pat.search(text)
        if m:
            v = m.group(1).strip()
            # Normalize "noon" / "midnight" to a parseable time so the
            # downstream `_parse_due_datetime` can use it.
            lv = v.lower()
            if lv == "noon":
                return "12:00 PM"
            if lv == "midnight":
                return "11:59 PM"
            return v
    if re.search(r'end\s+of\s+business|close\s+of\s+business|\bEOB\b|\bCOB\b|\bEOD\b',
                 text, re.IGNORECASE):
        return "COB"
    return ""


def _extract_solicitation_number(text: str) -> str:
    """Extract solicitation/RFQ number from text."""
    if not text:
        return ""
    # Pattern: "Solicitation #12345" or "RFQ-2026-001" or "RFQ #10838043"
    for pattern in [
        r'(?:solicitation|rfq|bid)\s+(?:number|no\.?|#)\s*:?\s*([A-Z0-9][\w-]{3,20})',
        r'(?:solicitation|rfq|bid)\s*#\s*(\d[\w-]{2,20})',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _extract_phone(text: str) -> str:
    """Extract phone number from email body/signature."""
    if not text:
        return ""
    m = re.search(r'(?:phone|tel|cell|fax)[\s:]*(\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4})', text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def _detect_food_items(text: str) -> bool:
    """Detect if email mentions food items (triggers OBS-1600 requirement)."""
    if not text:
        return False
    food_keywords = [
        "food item", "food product", "agricultural product",
        "perishable", "obs 1600", "obs-1600", "food cert",
        "food service", "nutritional",
    ]
    lower = text.lower()
    return any(kw in lower for kw in food_keywords)


def _extract_urls(text: str) -> list:
    """Extract URLs from email body."""
    if not text:
        return []
    url_pattern = r'https?://[^\s<>"\')\]]+(?:\.[^\s<>"\')\]]+)+'
    return re.findall(url_pattern, text)


def _extract_special_instructions(text: str) -> list:
    """Extract notable instructions from email body."""
    if not text:
        return []
    instructions = []
    # Look for lines that start with instruction-like patterns
    for pattern in [
        r'(?:please|must|required|ensure|note)\s+(.{10,100})',
        r'(?:important|attention|reminder)\s*:?\s*(.{10,100})',
    ]:
        for m in re.finditer(pattern, text, re.IGNORECASE):
            inst = m.group(1).strip().rstrip(".")
            if inst and inst not in instructions:
                instructions.append(inst)
    return instructions[:5]  # Cap at 5


def _filter_trusted_urls(urls: list) -> list:
    """Only keep URLs from trusted domains."""
    trusted = []
    for url in urls:
        if any(domain in url.lower() for domain in TRUSTED_URL_DOMAINS):
            trusted.append(url)
    return trusted


def _classify_attachments(attachments: list) -> list:
    """Classify attachment filenames into form types."""
    types = []
    for att in attachments:
        fname = (att.get("filename") or "").lower()
        if "704" in fname:
            types.append("704")
        elif "703b" in fname or "703-b" in fname:
            types.append("703b")
        elif "703c" in fname or "703-c" in fname:
            types.append("703c")
        elif "bid" in fname and "package" in fname:
            types.append("bidpkg")
        elif fname.endswith(".pdf"):
            types.append("pdf")
        elif fname.endswith(".docx") or fname.endswith(".doc"):
            types.append("docx")
        elif fname.endswith(".xlsx") or fname.endswith(".xls"):
            types.append("excel")
    return types
