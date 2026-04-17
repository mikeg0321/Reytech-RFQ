"""Agency Rules read API (Phase C).

Canonical reader for per-agency rules extracted from buyer emails.
Form QA and ingest pipelines call these functions to get agency-aware
guidance at runtime.

Writes are handled by src/agents/agency_rules_extractor.py.
"""
from __future__ import annotations

import json
import logging
from typing import List, Optional

log = logging.getLogger("reytech.agency_rules")


RULE_TYPES = {
    "forms",           # required forms / what to include
    "delivery",        # delivery address, terms, freight
    "packaging",       # labeling, packaging requirements
    "signature",       # signature requirements, ink color, title
    "contact",         # which buyer to send to / CC
    "quote_format",    # how the buyer wants the quote formatted
    "rejection_reason",# things that got us rejected before
    "misc",
}


def get_rules_for_agency(agency: str,
                         rule_type: Optional[str] = None,
                         min_confidence: float = 0.5,
                         active_only: bool = True) -> List[dict]:
    """Return active rules for an agency, ranked by confidence + recency.

    Rules table is opt-in: if empty or unavailable, returns []. Callers
    should treat an empty list as 'no agency-specific guidance' and
    fall back to default behavior.
    """
    if not agency:
        return []
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.row_factory = None
            q = """SELECT id, agency, rule_type, rule_text, source_email_ids,
                          confidence, sample_count, first_seen, last_seen, active
                     FROM agency_rules
                    WHERE lower(agency) = lower(?)
                      AND confidence >= ?"""
            params = [agency, min_confidence]
            if active_only:
                q += " AND active = 1"
            if rule_type:
                q += " AND rule_type = ?"
                params.append(rule_type)
            q += " ORDER BY confidence DESC, last_seen DESC"
            rows = conn.execute(q, params).fetchall()
    except Exception as e:
        log.debug("get_rules_for_agency(%s) error: %s", agency, e)
        return []

    out = []
    for r in rows:
        try:
            src_ids = json.loads(r[4] or "[]")
        except Exception:
            src_ids = []
        out.append({
            "id": r[0],
            "agency": r[1],
            "rule_type": r[2],
            "rule_text": r[3],
            "source_email_ids": src_ids,
            "confidence": r[5],
            "sample_count": r[6],
            "first_seen": r[7],
            "last_seen": r[8],
            "active": bool(r[9]),
        })
    return out


def summarize_for_qa(agency: str, min_confidence: float = 0.6) -> dict:
    """Compact shape for Form QA display: bucketed by rule_type."""
    rules = get_rules_for_agency(agency, min_confidence=min_confidence)
    bucketed = {}
    for r in rules:
        bucketed.setdefault(r["rule_type"], []).append(r["rule_text"])
    return {
        "agency": agency,
        "rule_count": len(rules),
        "types": sorted(bucketed.keys()),
        "by_type": bucketed,
    }


def upsert_rule(agency: str, rule_type: str, rule_text: str,
                source_email_id: str = "", confidence: float = 0.5) -> int:
    """Idempotent upsert. Matches on (agency, rule_type, rule_text)
    normalized to lowercase+trimmed. On match, bumps sample_count,
    appends source_email_id, updates last_seen, averages confidence.
    Returns the id."""
    if not agency or not rule_type or not rule_text:
        return 0
    if rule_type not in RULE_TYPES:
        rule_type = "misc"

    from src.core.db import get_db
    rule_key = rule_text.strip().lower()[:500]
    with get_db() as conn:
        existing = conn.execute("""
            SELECT id, source_email_ids, confidence, sample_count
              FROM agency_rules
             WHERE lower(agency) = lower(?)
               AND rule_type = ?
               AND lower(rule_text) = ?
        """, (agency, rule_type, rule_key)).fetchone()

        if existing:
            rid, src_json, conf, n = existing[0], existing[1], existing[2] or 0.5, existing[3] or 1
            try:
                srcs = json.loads(src_json or "[]")
            except Exception:
                srcs = []
            if source_email_id and source_email_id not in srcs:
                srcs.append(source_email_id)
            new_conf = ((conf * n) + confidence) / (n + 1)
            conn.execute("""
                UPDATE agency_rules
                   SET source_email_ids = ?, confidence = ?, sample_count = sample_count + 1,
                       last_seen = datetime('now')
                 WHERE id = ?
            """, (json.dumps(srcs[-50:]), new_conf, rid))
            return rid
        else:
            srcs = [source_email_id] if source_email_id else []
            cur = conn.execute("""
                INSERT INTO agency_rules
                    (agency, rule_type, rule_text, source_email_ids, confidence,
                     sample_count, first_seen, last_seen, active)
                VALUES (?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'), 1)
            """, (agency, rule_type, rule_text, json.dumps(srcs), confidence))
            return cur.lastrowid


def deactivate_rule(rule_id: int) -> bool:
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("UPDATE agency_rules SET active = 0 WHERE id = ?", (rule_id,))
        return True
    except Exception as e:
        log.error("deactivate_rule(%d) error: %s", rule_id, e)
        return False


__all__ = [
    "RULE_TYPES",
    "get_rules_for_agency",
    "summarize_for_qa",
    "upsert_rule",
    "deactivate_rule",
]
