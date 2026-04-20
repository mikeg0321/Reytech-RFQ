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
    python scripts/ingest_inbox_rfqs.py --corpus-dir /path/to/email_corpus

Resolution order per target:
    1. Gmail API (if configured) — authoritative; returns raw bytes we parse.
    2. Local email corpus (`tools/mine_email_corpus.py` output) — fallback
       when Gmail auth isn't available or the mailbox moved. Corpus entries
       include gmail_id + already-downloaded attachments, so we can build an
       rfq_info dict without a second API trip.

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
import json
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


# ── Corpus fallback ───────────────────────────────────────────────────
#
# `tools/mine_email_corpus.py` writes one JSONL record per email into
# <corpus_dir>/{sales,mike}_corpus.jsonl and the corresponding raw
# attachments into <corpus_dir>/attachments/<inbox>/<gmail_id[:16]>/.
# When Gmail auth isn't available (e.g. running locally), we can still
# drive the pipeline from that on-disk snapshot.

def _default_corpus_dir():
    """Corpus is mined in a sibling worktree; allow an env override."""
    override = os.environ.get("EMAIL_CORPUS_DIR")
    if override:
        return override
    return os.path.join(_ROOT, "data", "email_corpus")


def _subject_matches(subject, query):
    """Loose match: every `subject:<token>` in the Gmail query must appear
    in the record's subject (case-insensitive, partial ok)."""
    if not subject:
        return False
    subject_lc = subject.lower()
    tokens = re.findall(r"subject:(\S+)", query or "")
    if not tokens:
        return False
    return all(t.lower() in subject_lc for t in tokens)


def find_in_corpus(query, corpus_dir, inbox_hint=None):
    """Search the corpus JSONLs for a record whose subject matches `query`.

    Returns the newest matching record dict (by parsed_date) plus the
    resolved attachment directory, or None if no match.
    """
    if not os.path.isdir(corpus_dir):
        return None

    candidates = []
    # Prefer the hinted inbox first, then the other.
    inbox_order = []
    if inbox_hint in ("sales", "mike"):
        inbox_order.append(inbox_hint)
    for ix in ("sales", "mike"):
        if ix not in inbox_order:
            inbox_order.append(ix)

    for inbox_name in inbox_order:
        jsonl = os.path.join(corpus_dir, f"{inbox_name}_corpus.jsonl")
        if not os.path.exists(jsonl):
            continue
        try:
            with open(jsonl, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if _subject_matches(rec.get("subject", ""), query):
                        rec["_inbox"] = inbox_name
                        candidates.append(rec)
        except OSError:
            continue

    if not candidates:
        return None

    # Newest first.
    candidates.sort(key=lambda r: r.get("parsed_date", ""), reverse=True)
    best = candidates[0]

    gid = best.get("gmail_id", "")
    att_dir = os.path.join(
        corpus_dir, "attachments", best["_inbox"], gid[:16]
    ) if gid else ""
    best["_attachment_dir"] = att_dir if os.path.isdir(att_dir) else ""
    return best


def _build_info_from_corpus(record, rfq_dir):
    """Reconstruct an rfq_info dict from a corpus record + on-disk PDFs."""
    os.makedirs(rfq_dir, exist_ok=True)
    attachments = []
    src_dir = record.get("_attachment_dir", "")
    if src_dir and os.path.isdir(src_dir):
        for fname in os.listdir(src_dir):
            if not fname.lower().endswith(".pdf"):
                continue
            src_path = os.path.join(src_dir, fname)
            safe = re.sub(r"[^\w\-_. ()]+", "_", fname)
            dst_path = os.path.join(rfq_dir, safe)
            # Copy so the RFQ owns its own files (corpus stays immutable).
            try:
                with open(src_path, "rb") as sf, open(dst_path, "wb") as df:
                    df.write(sf.read())
            except OSError as e:
                log.warning("  corpus copy failed %s: %s", fname, e)
                continue
            attachments.append({
                "path": dst_path,
                "filename": safe,
                "type": _identify_form(safe),
            })

    subject = record.get("subject", "")
    sender_raw = record.get("from_addr", "")
    sender_email = record.get("from_email", "") or _extract_email_addr(sender_raw)
    body = record.get("body_preview", "") or ""

    sol_hint = ""
    m = re.search(r"\b(\d{7,})\b", subject)
    if m:
        sol_hint = m.group(1)
    else:
        m = re.search(r"\b(\d{5})\b", subject)
        if m:
            sol_hint = m.group(1)

    gid = record.get("gmail_id", "")
    rfq_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + (gid[:6] or "corpus")
    return {
        "id": rfq_id,
        "email_uid": gid,
        "message_id": record.get("message_id", ""),
        "subject": subject,
        "sender": sender_raw,
        "sender_email": sender_email,
        "original_sender": sender_email,
        "date": record.get("date_str", ""),
        "solicitation_hint": sol_hint,
        "attachments": attachments,
        "rfq_dir": rfq_dir,
        "body_preview": body[:500],
        "body_text": body,
    }


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


def ingest_target(service, label, query, expected_agency, dry_run=False,
                  corpus_dir=None, inbox_hint=None):
    log.info("─── %s ───", label)
    log.info("  query: %s", query)

    # Gmail path — only attempted if we actually have a service handle.
    msg_id = None
    all_hits = []
    if service is not None:
        msg_id, all_hits = _find_latest_message_id(service, query)

    # Corpus fallback — used when Gmail returned nothing OR no service.
    if not msg_id and corpus_dir:
        hit = find_in_corpus(query, corpus_dir, inbox_hint=inbox_hint)
        if hit:
            log.info("  corpus hit: gmail_id=%s subject=%s",
                     hit.get("gmail_id", "")[:16],
                     (hit.get("subject", "") or "")[:80])
            if dry_run:
                return {
                    "label": label,
                    "status": "dry_run_corpus",
                    "msg_id": hit.get("gmail_id", ""),
                    "subject": hit.get("subject", ""),
                    "sender": hit.get("from_addr", ""),
                }
            rfq_id_preview = datetime.now().strftime("%Y%m%d_%H%M%S") + \
                "_" + (hit.get("gmail_id", "") or "corpus")[:6]
            rfq_dir = _upload_dir_for(rfq_id_preview)
            rfq_info = _build_info_from_corpus(hit, rfq_dir)
            rfq_info["id"] = rfq_id_preview
            rfq_info["expected_agency"] = expected_agency
            rfq_info["_source"] = "corpus"

            from src.api.dashboard import process_rfq_email
            result = process_rfq_email(rfq_info)
            if result is None:
                return {
                    "label": label,
                    "status": "skipped_dedup",
                    "source": "corpus",
                    "msg_id": hit.get("gmail_id", ""),
                    "attachments": len(rfq_info["attachments"]),
                }
            return {
                "label": label,
                "status": "ingested",
                "source": "corpus",
                "msg_id": hit.get("gmail_id", ""),
                "rfq_id": result.get("id") if isinstance(result, dict) else rfq_info["id"],
                "attachments": len(rfq_info["attachments"]),
                "sol_hint": rfq_info["solicitation_hint"],
            }

    if not msg_id:
        log.warning("  NO MATCH in Gmail%s — skipping",
                    " or corpus" if corpus_dir else "")
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
    ap.add_argument("--corpus-dir", default=None,
                    help="Path to mined email corpus (fallback when Gmail unavailable). "
                         "Defaults to $EMAIL_CORPUS_DIR or <repo>/data/email_corpus.")
    ap.add_argument("--no-gmail", action="store_true",
                    help="Skip Gmail entirely; ingest from corpus only.")
    args = ap.parse_args()

    corpus_dir = args.corpus_dir or _default_corpus_dir()
    corpus_available = os.path.isdir(corpus_dir)

    from src.core import gmail_api
    service = None
    if not args.no_gmail and gmail_api.is_configured():
        log.info("Building Gmail service for inbox=%s (dry_run=%s)",
                 args.inbox, args.dry_run)
        service = gmail_api.get_service(args.inbox)
    else:
        reason = "disabled via --no-gmail" if args.no_gmail \
            else "Gmail OAuth not configured"
        if corpus_available:
            log.warning("%s — falling back to corpus at %s", reason, corpus_dir)
        else:
            log.error("%s and no usable corpus at %s", reason, corpus_dir)
            return 2

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
                service, label, query, agency,
                dry_run=args.dry_run,
                corpus_dir=corpus_dir if corpus_available else None,
                inbox_hint=args.inbox))
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
