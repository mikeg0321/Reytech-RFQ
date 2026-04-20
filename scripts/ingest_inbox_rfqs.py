"""One-off inbox RFQ ingester — 2026-04-20.

Drives a hand-picked list of in-inbox RFQ emails through `process_rfq_email`
so they land in the RFQ queue before `rfq.orchestrator_pipeline` gets
flipped ON in prod. User direction: "quote perfectly for at least one
from each agency/type" — this gives us one from each of the flavors
currently sitting in the inbox (CA DVBE, CDPH AMS 703A, CDCR CIW, CCHCS).

Targets (Gmail subject keywords):
    SNF Residents 20026   → CA DVBE
    PREQ 10844466         → CDPH / AMS 703A
    10843164 CIW          → CDCR CIW
    RFQ #10840486         → CCHCS
    10837703 CIW          → CDCR CIW

Usage:
    python scripts/ingest_inbox_rfqs.py               # ingest all targets
    python scripts/ingest_inbox_rfqs.py --dry-run     # report without writes
    python scripts/ingest_inbox_rfqs.py --inbox mike  # use the second inbox
    python scripts/ingest_inbox_rfqs.py --target 10840486  # single solicitation

The script reuses `process_rfq_email` in dashboard.py — the exact same
entry point the background poller uses — so ingest is identical to what
would happen on the next poll cycle, minus the timing. Dedup inside
`process_rfq_email` protects us from double-creating if one of these has
already been picked up by the poller.
"""
from __future__ import annotations

import argparse
import base64
import email as email_pkg
import logging
import os
import re
import sys
from datetime import datetime
from email.header import decode_header

# Make `src.*` imports work regardless of cwd
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
)
log = logging.getLogger("ingest_inbox_rfqs")


# ── Targets ───────────────────────────────────────────────────────────
#
# Each target: (label, gmail_query, expected_agency_hint). Queries are
# scoped `in:inbox` + a distinctive subject fragment so each one resolves
# to exactly one thread. If Gmail returns multiple hits we take the most
# recent.

TARGETS = [
    ("SNF Residents 20026",
     'in:inbox subject:20026 subject:SNF',
     "ca_dvbe"),
    ("PREQ 10844466",
     'in:inbox subject:10844466',
     "cdph"),
    ("10843164 CIW",
     'in:inbox subject:10843164',
     "cdcr_ciw"),
    ("RFQ 10840486 CCHCS",
     'in:inbox subject:10840486',
     "cchcs"),
    ("10837703 CIW",
     'in:inbox subject:10837703',
     "cdcr_ciw"),
]


# ── Helpers ───────────────────────────────────────────────────────────

def _decode_header(h):
    if not h:
        return ""
    try:
        parts = decode_header(h)
        out = ""
        for content, charset in parts:
            if isinstance(content, bytes):
                out += content.decode(charset or "utf-8", errors="replace")
            else:
                out += content
        return out
    except Exception:
        return str(h)


def _extract_email_addr(sender_str):
    m = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", sender_str or "")
    return m.group(0) if m else (sender_str or "")


def _get_body_text(msg):
    bodies = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    bodies.append(payload.decode("utf-8", errors="replace"))
            elif ctype == "message/rfc822":
                inner = part.get_payload()
                if isinstance(inner, list):
                    for m in inner:
                        bodies.append(_get_body_text(m))
                elif hasattr(inner, "walk"):
                    bodies.append(_get_body_text(inner))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            bodies.append(payload.decode("utf-8", errors="replace"))
    return "\n".join(b for b in bodies if b)


def _save_pdf_attachments(msg, save_dir):
    """Minimal PDF-only saver. Handles top-level parts and forwarded rfc822
    parts. Intentionally ignores zip/office for this one-off."""
    os.makedirs(save_dir, exist_ok=True)
    saved = []

    def _walk(m):
        for part in m.walk():
            ctype = part.get_content_type()
            if ctype == "message/rfc822":
                inner = part.get_payload()
                if isinstance(inner, list):
                    for inner_msg in inner:
                        _walk(inner_msg)
                elif hasattr(inner, "walk"):
                    _walk(inner)
                continue
            if part.get_content_maintype() == "multipart":
                continue
            fname = part.get_filename()
            if not fname:
                continue
            fname = _decode_header(fname) if isinstance(fname, str) else fname
            if not fname.lower().endswith(".pdf"):
                continue
            safe = re.sub(r"[^\w\-_. ()]+", "_", fname)
            fpath = os.path.join(save_dir, safe)
            data = part.get_payload(decode=True)
            if not data:
                continue
            with open(fpath, "wb") as f:
                f.write(data)
            ftype = _identify_form(safe)
            saved.append({"path": fpath, "filename": safe, "type": ftype})
            log.info("  saved %s (%d bytes, type=%s)", safe, len(data), ftype)

    _walk(msg)
    return saved


def _identify_form(filename):
    name_lower = filename.lower().replace(" ", "_").replace("-", "_")
    if "703c" in name_lower or "fair_and_reasonable" in name_lower:
        return "703c"
    if "703b" in name_lower:
        return "703b"
    if "703a" in name_lower:
        return "703a"
    if "704b" in name_lower:
        return "704b"
    if "704" in name_lower:
        return "704"
    if "bid_package" in name_lower or "bidpkg" in name_lower:
        return "bidpkg"
    return "unknown"


# ── Ingest core ───────────────────────────────────────────────────────

def _find_latest_message_id(service, query):
    from src.core.gmail_api import list_message_ids, get_message_metadata
    ids = list_message_ids(service, query=query, max_results=10)
    if not ids:
        return None, []
    # Gmail list returns newest-first already — first hit is the most recent.
    latest = ids[0]
    return latest, ids


def _build_rfq_info(msg, msg_id, attachments, rfq_dir):
    subject = _decode_header(msg.get("Subject", ""))
    sender_raw = _decode_header(msg.get("From", ""))
    sender_email = _extract_email_addr(sender_raw)
    body = _get_body_text(msg)

    # Try to pull a solicitation number hint from the subject
    sol_hint = ""
    m = re.search(r"\b(\d{7,})\b", subject)
    if m:
        sol_hint = m.group(1)
    else:
        m = re.search(r"\b(\d{5})\b", subject)
        if m:
            sol_hint = m.group(1)

    # Mirror the shape email_poller builds at line 2712
    rfq_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + msg_id[:6]
    return {
        "id": rfq_id,
        "email_uid": msg_id,  # Gmail message id acts as our UID
        "message_id": msg.get("Message-ID", ""),
        "subject": subject,
        "sender": sender_raw,
        "sender_email": sender_email,
        "original_sender": sender_email,
        "date": msg.get("Date", ""),
        "solicitation_hint": sol_hint,
        "attachments": attachments,
        "rfq_dir": rfq_dir,
        "body_preview": body[:500] if body else "",
        "body_text": body or "",
    }


def _upload_dir_for(rfq_id):
    base = os.environ.get("UPLOAD_DIR") or os.path.join(_ROOT, "data", "uploads")
    return os.path.join(base, rfq_id)


def ingest_target(service, label, query, expected_agency, dry_run=False):
    log.info("─── %s ───", label)
    log.info("  query: %s", query)
    msg_id, all_hits = _find_latest_message_id(service, query)
    if not msg_id:
        log.warning("  NO MATCH in Gmail — skipping")
        return {"label": label, "status": "no_match", "query": query}
    if len(all_hits) > 1:
        log.info("  %d hits; using most recent %s", len(all_hits), msg_id)

    from src.core.gmail_api import get_raw_message, get_message_metadata

    meta = get_message_metadata(service, msg_id)
    log.info("  subject : %s", meta.get("subject", "")[:90])
    log.info("  from    : %s", meta.get("from", "")[:60])
    log.info("  date    : %s", meta.get("date", ""))

    if dry_run:
        return {
            "label": label,
            "status": "dry_run",
            "msg_id": msg_id,
            "subject": meta.get("subject", ""),
            "sender": meta.get("from", ""),
        }

    raw = get_raw_message(service, msg_id)
    msg = email_pkg.message_from_bytes(raw)

    # Stage attachments
    rfq_id_preview = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + msg_id[:6]
    rfq_dir = _upload_dir_for(rfq_id_preview)
    atts = _save_pdf_attachments(msg, rfq_dir)
    log.info("  attachments: %d PDF(s)", len(atts))

    rfq_info = _build_rfq_info(msg, msg_id, atts, rfq_dir)
    # Ensure the id we used for the upload dir is the id the dict carries
    rfq_info["id"] = rfq_id_preview
    rfq_info["expected_agency"] = expected_agency  # hint for debugging only

    # Hand off to the same entry point the poller uses
    from src.api.dashboard import process_rfq_email
    result = process_rfq_email(rfq_info)
    if result is None:
        log.info("  process_rfq_email returned None — likely dedup (already in queue)")
        return {
            "label": label,
            "status": "skipped_dedup",
            "msg_id": msg_id,
            "attachments": len(atts),
        }
    return {
        "label": label,
        "status": "ingested",
        "msg_id": msg_id,
        "rfq_id": result.get("id") if isinstance(result, dict) else rfq_info["id"],
        "attachments": len(atts),
        "sol_hint": rfq_info["solicitation_hint"],
    }


def main():
    ap = argparse.ArgumentParser(description="Ingest hand-picked inbox RFQs")
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch metadata only; do NOT write to the queue")
    ap.add_argument("--inbox", default="sales",
                    help="Inbox key: 'sales' (default) or 'mike'")
    ap.add_argument("--target", default=None,
                    help="Filter to a single solicitation (substring match on query)")
    args = ap.parse_args()

    from src.core import gmail_api
    if not gmail_api.is_configured():
        log.error("Gmail OAuth is not configured. Set GMAIL_OAUTH_* env vars "
                  "or run scripts/gmail_oauth_setup.py first.")
        return 2

    log.info("Building Gmail service for inbox=%s (dry_run=%s)",
             args.inbox, args.dry_run)
    service = gmail_api.get_service(args.inbox)

    targets = TARGETS
    if args.target:
        needle = args.target.strip()
        targets = [t for t in TARGETS if needle in t[1]]
        if not targets:
            log.error("No target matches --target=%s", needle)
            return 2

    results = []
    for label, query, agency in targets:
        try:
            results.append(ingest_target(
                service, label, query, agency, dry_run=args.dry_run))
        except Exception as e:
            log.exception("  FAILED: %s", e)
            results.append({"label": label, "status": "error", "error": str(e)})

    # ── Summary ──────────────────────────────────────────────────────
    log.info("═" * 64)
    log.info("SUMMARY")
    log.info("═" * 64)
    for r in results:
        log.info("  %-24s  %s", r["label"], r.get("status", "?"))
        if r.get("rfq_id"):
            log.info("       rfq_id=%s  attachments=%s",
                     r["rfq_id"], r.get("attachments", 0))

    ingested = sum(1 for r in results if r.get("status") == "ingested")
    no_match = sum(1 for r in results if r.get("status") == "no_match")
    dedup = sum(1 for r in results if r.get("status") == "skipped_dedup")
    errors = sum(1 for r in results if r.get("status") == "error")
    log.info("ingested=%d  dedup=%d  no_match=%d  errors=%d",
             ingested, dedup, no_match, errors)
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
