"""
Data Contracts — Enforces minimum required fields on every
record type before save. Blocked saves are logged.
"""
import re
import json
import logging
from datetime import datetime

log = logging.getLogger("reytech.contracts")


class ContractViolation(Exception):
    def __init__(self, record_type, record_id, violations):
        self.record_type = record_type
        self.record_id = record_id
        self.violations = violations
        super().__init__(f"{record_type} {record_id}: {violations}")


def validate_quote(q, strict=True):
    """Validate a quote. Returns (is_valid, violations)."""
    violations = []
    qn = q.get("quote_number", "")
    if not qn:
        violations.append("missing quote_number")
    elif not re.match(r"^R\d{2}Q\d+$", qn):
        violations.append(f"invalid format: {qn}")
    if strict:
        items_count = q.get("items_count", 0) or 0
        items = q.get("items_detail", [])
        if not items and items_count == 0:
            violations.append("no items")
        total = q.get("total", 0) or 0
        if total == 0 and q.get("status") != "void":
            violations.append("$0 total")
        if not q.get("source_pc_id") and not q.get("source_rfq_id"):
            log.debug("Quote %s has no source link (PC or RFQ) — allowed but noted", qn)
        if not (q.get("institution") or "").strip():
            violations.append("empty institution")
    is_valid = len(violations) == 0
    if not is_valid:
        log.warning("QUOTE CONTRACT FAIL %s: %s", qn, violations)
    return is_valid, violations


def validate_pc(pc, pc_id=""):
    """Validate a price check. Returns (is_valid, violations)."""
    violations = []
    items = pc.get("items", [])
    if not items:
        pd = pc.get("pc_data", {})
        if isinstance(pd, str):
            try:
                pd = json.loads(pd)
            except Exception:
                pd = {}
        if isinstance(pd, dict):
            items = pd.get("items", [])
    if not items:
        items = pc.get("line_items", [])
    if not items:
        violations.append("no items")
    requestor = (pc.get("requestor") or pc.get("requestor_email") or "").strip()
    if not requestor:
        violations.append("no requestor")
    is_valid = len(violations) == 0
    if not is_valid:
        log.warning("PC CONTRACT FAIL %s: %s", str(pc_id)[:20], violations)
    return is_valid, violations


def validate_rfq(r, rfq_id=""):
    """Validate an RFQ. Returns (is_valid, violations)."""
    violations = []
    items = r.get("line_items", r.get("items", []))
    if not items:
        violations.append("no items")
    is_valid = len(violations) == 0
    if not is_valid:
        log.warning("RFQ CONTRACT FAIL %s: %s", str(rfq_id)[:20], violations)
    return is_valid, violations


def safe_match(a, b, min_length=3):
    """Safe substring match — prevents empty string bugs (Law 29)."""
    a = (a or "").strip()
    b = (b or "").strip()
    if not a or not b:
        return False
    if len(a) < min_length or len(b) < min_length:
        return False
    return a.lower() in b.lower() or b.lower() in a.lower()


def safe_eq(a, b, min_length=1):
    """Safe equality — both must be non-empty."""
    a = (a or "").strip()
    b = (b or "").strip()
    if not a or not b:
        return False
    return a.lower() == b.lower()


_blocked_saves = []


def log_blocked_save(record_type, record_id, violations, caller=""):
    """Log when a save is blocked."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "record_type": record_type,
        "record_id": str(record_id)[:50],
        "violations": violations,
        "caller": caller,
    }
    _blocked_saves.append(entry)
    if len(_blocked_saves) > 200:
        _blocked_saves.pop(0)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("""
                INSERT INTO contract_violations
                (record_type, record_id, violations, caller, created_at)
                VALUES (?,?,?,?,datetime('now'))
            """, (record_type, str(record_id)[:50],
                  json.dumps(violations), caller))
    except Exception:
        pass
    log.warning("BLOCKED SAVE: %s %s — %s", record_type, str(record_id)[:30], violations)


def get_blocked_saves(limit=50):
    return _blocked_saves[-limit:]


# ════════════════════════════════════════════════════════════════
# BUYER NAME RESOLUTION
# ════════════════════════════════════════════════════════════════

def resolve_buyer_name(parsed_name, sender_name, sender_email):
    """Determine real buyer name. Email sender is default unless parsed name is valid."""
    if parsed_name and _is_real_name(parsed_name):
        return parsed_name.strip()
    if sender_name and _is_real_name(sender_name):
        return sender_name.strip()
    if sender_email and "@" in sender_email:
        local = sender_email.split("@")[0]
        parts = re.split(r'[._]', local)
        if len(parts) >= 2:
            name = " ".join(p.capitalize() for p in parts if len(p) >= 2)
            if _is_real_name(name):
                return name
        if len(local) >= 3:
            return local.capitalize()
    return parsed_name or sender_name or ""


_NON_NAMES = [
    "purchase order", "price check", "quote request", "bid package",
    "request for", "solicitation", "department of", "state of california",
    "attention", "procurement", "accounts payable", "general services",
    "tbd", "n/a", "none", "unknown", "test", "see attached",
    "see instructions", "various", "mail room", "receiving dock",
]


def _is_real_name(name):
    """Check if a string is a plausible human name."""
    if not name or not isinstance(name, str):
        return False
    name = name.strip()
    if len(name) < 3 or len(name) > 60:
        return False
    words = name.split()
    if len(words) < 2:
        return False
    for word in words:
        cleaned = word.strip(".,'-")
        if len(cleaned) < 2:
            return False
        if not re.match(r"^[A-Za-z][A-Za-z'\-]+$", cleaned):
            return False
    name_lower = name.lower()
    for non in _NON_NAMES:
        if non in name_lower:
            return False
    return True
