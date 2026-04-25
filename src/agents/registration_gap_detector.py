"""
src/agents/registration_gap_detector.py — V2-PR-7

Two functions populate `agency_vendor_registry` so V2-PR-2's
registration-status pill (and V2-PR-8's templates) operate on real
data instead of all-`unknown`.

  detect_registration_gaps(top_n) — punch list of agencies with high
                                     SCPRS spend that we have NO
                                     registry row for (or 'unknown').

  gmail_bulk_seed_registrations(dry_run, limit, since_days)
    — scans Gmail inbox archive for past RFQ-like emails from CA
      agency domains and bulk-marks `agency_vendor_registry` rows as
      registered (with `source='agent'`, `is_provisional=1` until
      operator confirms — auto-promoted when ≥3 RFQs from same
      domain at high confidence).

Per 2026-04-25 product-engineer pre-build review:
  - Auto-promote at high-confidence + ≥3 RFQs (real automation, not
    just queue-builder)
  - evidence_message_ids JSON column stored for audit/re-run
  - Reuses existing src/core/gmail_api.list_message_ids paged search
    (do NOT fork a new gmail_archive_reader module)
  - Bare @ca.gov / @state.ca.gov skipped as too ambiguous
  - Dedupe by Gmail thread ID, not message count
  - Skip rows where source IN ('operator','imported') — agent never
    overwrites operator truth, but can re-seed its own (idempotent)
  - 200-message cap with cursor persistence (TODO for the cursor
    once we see real pagination need; first cut just caps)
"""
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

log = logging.getLogger("registration_gap_detector")


# Subjects that look like a procurement RFQ being delivered TO us.
# Conservative — false positives here would auto-mark agencies as
# registered that we never actually were on the list for.
_RFQ_SUBJECT_PATTERNS = [
    re.compile(r"\bRFQ\b", re.IGNORECASE),
    re.compile(r"\bRFP\b", re.IGNORECASE),
    re.compile(r"\brequest for (quote|quotation|proposal)\b", re.IGNORECASE),
    re.compile(r"\bsolicitation\b", re.IGNORECASE),
    re.compile(r"\bbid\s*(invitation|notice|opportunity|opening)\b", re.IGNORECASE),
    re.compile(r"\bIFB\b"),  # Invitation for Bid
]

# Domains too generic to seed even at medium confidence.
_AMBIGUOUS_DOMAIN_BLOCKLIST = {"ca.gov", "state.ca.gov"}


def _get_db():
    from src.core.db import get_db
    return get_db()


# ── Detector — punch list of agencies missing from the registry ─────────────

def detect_registration_gaps(top_n: int = 20) -> dict:
    """Top-N CA agencies (by 365d SCPRS line_total) where Reytech is
    NOT registered or has status='unknown'. Operator punch list."""
    out = {"ok": True, "gaps": [], "scanned_top_n": top_n,
           "checked_at": datetime.now(timezone.utc).isoformat()}
    try:
        with _get_db() as conn:
            rows = conn.execute("""
                SELECT p.dept_code, p.dept_name,
                       SUM(l.line_total) AS total_spend,
                       COUNT(DISTINCT p.po_number) AS po_count,
                       MAX(p.start_date) AS last_po_date
                FROM scprs_po_master p
                JOIN scprs_po_lines l ON l.po_id = p.id
                WHERE p.is_test = 0 AND l.is_test = 0
                  AND p.start_date >= date('now', '-365 days')
                GROUP BY p.dept_code
                ORDER BY total_spend DESC
                LIMIT ?
            """, (top_n,)).fetchall()
            for r in rows:
                d = dict(r)
                dept_code = d["dept_code"]
                if not dept_code:
                    continue
                reg = conn.execute(
                    "SELECT status, source, is_provisional "
                    "FROM agency_vendor_registry WHERE dept_code = ?",
                    (dept_code,),
                ).fetchone()
                if reg is None:
                    gap_status = "no_record"
                    current_status = None
                elif reg["status"] in (None, "", "unknown"):
                    gap_status = "unknown"
                    current_status = "unknown"
                elif reg["status"] in ("registered",) and reg["is_provisional"]:
                    gap_status = "provisional"
                    current_status = "registered (provisional)"
                else:
                    continue  # already healthy — no gap
                out["gaps"].append({
                    "dept_code": dept_code,
                    "dept_name": d.get("dept_name") or dept_code,
                    "total_spend": round(d.get("total_spend") or 0, 2),
                    "po_count": d.get("po_count", 0),
                    "last_po_date": (d.get("last_po_date") or "")[:10],
                    "gap_status": gap_status,
                    "current_status": current_status,
                })
    except Exception as e:
        log.exception("detect_registration_gaps failed")
        out["ok"] = False
        out["error"] = f"{type(e).__name__}: {e}"
    return out


# ── Gmail bulk-seed agent ──────────────────────────────────────────────────

def _extract_domain(from_header: str) -> str | None:
    """Pull the domain part out of a `From:` header. Lower-cased.
    Returns None on garbage."""
    if not from_header:
        return None
    m = re.search(r"<([^@>]+@([^>]+))>", from_header)
    if m:
        return (m.group(2) or "").strip().lower()
    m = re.search(r"@([\w\.-]+)", from_header)
    if m:
        return (m.group(1) or "").strip().lower()
    return None


def _is_rfq_subject(subject: str) -> bool:
    if not subject:
        return False
    return any(p.search(subject) for p in _RFQ_SUBJECT_PATTERNS)


def gmail_bulk_seed_registrations(
    dry_run: bool = True,
    limit: int = 200,
    since_days: int = 540,
    inbox_name: str = "sales",
) -> dict:
    """Scan Gmail archive for RFQ messages from known CA agency domains.

    For each (domain → dept_code) mapping with ≥3 distinct RFQ
    THREADS in the last `since_days`, upsert agency_vendor_registry:
      - status='registered'
      - source='agent'
      - is_provisional = 0 if confidence='high' AND thread_count >= 3
                          else 1 (operator-confirmable)
      - evidence_message_ids = JSON of the matching Gmail message IDs
      - notes = "auto-seeded from N RFQ threads since {date}"

    Skips rows where existing source IN ('operator','imported') —
    operator truth is never overwritten. Idempotent on re-run for
    agent-owned rows (re-seeds same row with refreshed evidence).

    Unmapped domains (domain seen ≥2 times but no agency_domain_aliases
    row) are queued in agency_pending_aliases for operator review.

    dry_run=True returns proposed upserts without writing.
    """
    out = {
        "ok": True, "dry_run": dry_run,
        "scanned_messages": 0, "matched_messages": 0,
        "domains_seen": 0, "rows_upserted": 0, "rows_skipped": 0,
        "pending_aliases_queued": 0,
        "proposed": [],
        "errors": [],
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        from src.core import gmail_api
        if not gmail_api.is_configured():
            out["ok"] = False
            out["error"] = "gmail_api not configured"
            return out
        service = gmail_api.get_service(inbox_name=inbox_name)
        # Gmail q= search for RFQ-like emails. Conservative — leans on
        # subject keywords + government tld. Cap by `limit` server-side.
        since_date = (datetime.now() - timedelta(days=since_days)).strftime("%Y/%m/%d")
        query = f'(subject:RFQ OR subject:"request for quote" OR subject:solicitation OR subject:RFP OR subject:IFB) after:{since_date}'
        msg_ids = gmail_api.list_message_ids(service, query=query,
                                              max_results=limit)
        out["scanned_messages"] = len(msg_ids)
    except Exception as e:
        log.exception("gmail_bulk_seed: list_message_ids failed")
        out["ok"] = False
        out["error"] = f"{type(e).__name__}: {e}"
        return out

    # Bucket message IDs by domain → distinct thread IDs.
    by_domain: dict[str, dict] = defaultdict(lambda: {
        "thread_ids": set(), "message_ids": [], "example_subject": "",
    })
    pending_seen: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "example_subject": "",
    })

    for mid in msg_ids:
        try:
            meta = gmail_api.get_message_metadata(service, mid)
        except Exception as e:
            out["errors"].append(f"get_metadata {mid}: {e}")
            continue
        subject = (meta.get("subject") or "").strip()
        sender = (meta.get("from") or "").strip()
        thread_id = meta.get("thread_id") or mid
        if not _is_rfq_subject(subject):
            continue
        domain = _extract_domain(sender)
        if not domain or domain in _AMBIGUOUS_DOMAIN_BLOCKLIST:
            continue
        out["matched_messages"] += 1
        b = by_domain[domain]
        b["thread_ids"].add(thread_id)
        if len(b["message_ids"]) < 50:  # cap evidence list per domain
            b["message_ids"].append(mid)
        if not b["example_subject"]:
            b["example_subject"] = subject[:120]

    out["domains_seen"] = len(by_domain)
    if not by_domain:
        out["finished_at"] = datetime.now(timezone.utc).isoformat()
        return out

    try:
        with _get_db() as conn:
            # Resolve each seen domain via agency_domain_aliases.
            # Unresolved → queue for operator review.
            domains = list(by_domain.keys())
            placeholders = ",".join(["?"] * len(domains))
            alias_rows = conn.execute(
                f"SELECT domain, dept_code, dept_name, confidence, is_active "
                f"FROM agency_domain_aliases WHERE domain IN ({placeholders})",
                domains,
            ).fetchall()
            alias_map = {r["domain"]: dict(r) for r in alias_rows}

            now_iso = datetime.now().isoformat(timespec="seconds")
            for domain, payload in by_domain.items():
                alias = alias_map.get(domain)
                thread_count = len(payload["thread_ids"])
                if alias is None or not alias.get("is_active"):
                    # Queue for operator. Only stash domains seen ≥2
                    # threads to avoid noise from one-off missends.
                    if thread_count < 2:
                        continue
                    if not dry_run:
                        conn.execute(
                            "INSERT INTO agency_pending_aliases "
                            "(domain, seen_count, example_subject) "
                            "VALUES (?, ?, ?) "
                            "ON CONFLICT(domain) DO UPDATE SET "
                            "seen_count = seen_count + excluded.seen_count, "
                            "last_seen = datetime('now'), "
                            "example_subject = COALESCE(NULLIF(example_subject, ''), excluded.example_subject)",
                            (domain, thread_count, payload["example_subject"]),
                        )
                    out["pending_aliases_queued"] += 1
                    continue

                dept_code = alias["dept_code"]
                confidence = alias.get("confidence") or "high"
                # Auto-promote rule: high confidence + ≥3 RFQ threads
                # → operator-equivalent automation (is_provisional=0).
                # Otherwise leave provisional for operator confirm.
                is_provisional = 0 if (confidence == "high" and thread_count >= 3) else 1

                # Skip rows operator already owns.
                existing = conn.execute(
                    "SELECT source FROM agency_vendor_registry WHERE dept_code = ?",
                    (dept_code,),
                ).fetchone()
                if existing and (existing["source"] or "") in ("operator", "imported"):
                    out["rows_skipped"] += 1
                    continue

                proposed = {
                    "dept_code": dept_code,
                    "domain": domain,
                    "thread_count": thread_count,
                    "is_provisional": is_provisional,
                    "evidence_message_ids": payload["message_ids"][:25],
                    "notes": (
                        f"auto-seeded from {thread_count} RFQ threads via "
                        f"{domain} (last {since_days}d)"
                    ),
                }
                out["proposed"].append(proposed)
                if dry_run:
                    continue

                fields = {
                    "status": "registered",
                    "source": "agent",
                    "is_provisional": is_provisional,
                    "notes": proposed["notes"],
                    "evidence_message_ids": json.dumps(proposed["evidence_message_ids"]),
                    "updated_at": now_iso,
                    "updated_by": "agent:gmail_bulk_seed",
                }
                if existing:
                    set_clause = ", ".join(f"{k}=?" for k in fields)
                    conn.execute(
                        f"UPDATE agency_vendor_registry SET {set_clause} "
                        "WHERE dept_code = ?",
                        list(fields.values()) + [dept_code],
                    )
                else:
                    fields["dept_code"] = dept_code
                    fields["created_at"] = now_iso
                    cols = ", ".join(fields.keys())
                    placeholders = ", ".join(["?"] * len(fields))
                    conn.execute(
                        f"INSERT INTO agency_vendor_registry ({cols}) "
                        f"VALUES ({placeholders})",
                        list(fields.values()),
                    )
                out["rows_upserted"] += 1
    except Exception as e:
        log.exception("gmail_bulk_seed: registry write failed")
        out["ok"] = False
        out["error"] = f"{type(e).__name__}: {e}"

    out["finished_at"] = datetime.now(timezone.utc).isoformat()
    return out


# ── Operator confirm/reject ────────────────────────────────────────────────

def confirm_agent_registration(dept_code: str, updated_by: str = "operator") -> dict:
    """Operator-graduates an agent-seeded row: source='operator',
    is_provisional=0. Only works on rows currently source='agent'."""
    out = {"ok": True, "dept_code": dept_code, "graduated": False}
    try:
        with _get_db() as conn:
            row = conn.execute(
                "SELECT source FROM agency_vendor_registry WHERE dept_code = ?",
                (dept_code,),
            ).fetchone()
            if not row:
                out["ok"] = False
                out["error"] = "no row for dept_code"
                return out
            if (row["source"] or "") != "agent":
                out["ok"] = False
                out["error"] = f"source is '{row['source']}', not agent"
                return out
            conn.execute(
                "UPDATE agency_vendor_registry SET source='operator', "
                "is_provisional=0, updated_by=?, updated_at=datetime('now') "
                "WHERE dept_code = ?",
                (updated_by, dept_code),
            )
            out["graduated"] = True
    except Exception as e:
        log.exception("confirm_agent_registration failed")
        out["ok"] = False
        out["error"] = f"{type(e).__name__}: {e}"
    return out


def reject_agent_registration(dept_code: str) -> dict:
    """Operator marks an agent-seeded row as not_registered."""
    out = {"ok": True, "dept_code": dept_code, "rejected": False}
    try:
        with _get_db() as conn:
            row = conn.execute(
                "SELECT source FROM agency_vendor_registry WHERE dept_code = ?",
                (dept_code,),
            ).fetchone()
            if not row:
                out["ok"] = False
                out["error"] = "no row for dept_code"
                return out
            if (row["source"] or "") != "agent":
                out["ok"] = False
                out["error"] = f"source is '{row['source']}', not agent"
                return out
            conn.execute(
                "UPDATE agency_vendor_registry SET status='not_registered', "
                "source='operator', is_provisional=0, "
                "updated_at=datetime('now') WHERE dept_code = ?",
                (dept_code,),
            )
            out["rejected"] = True
    except Exception as e:
        log.exception("reject_agent_registration failed")
        out["ok"] = False
        out["error"] = f"{type(e).__name__}: {e}"
    return out
