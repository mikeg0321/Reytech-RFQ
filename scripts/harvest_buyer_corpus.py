#!/usr/bin/env python3
"""harvest_buyer_corpus.py — pull all RFQ + sent + contract/amendment
emails (with attachments) from Gmail into a local corpus directory.

Mike's directive 2026-04-29: "you have gmail api, you have access to
every email, RFQ and sent outbox including buyer contract and
amendment emails. Pull a script into a directory file, and query.
More data good."

The corpus this builds is the foundation for:
  - Per-buyer form-profile training (Phase 1.5 — what hand-edits cost)
  - Outcome-driven calibration (Phase 4.6 — match RFQ to award)
  - Buyer-product pricing memory (Phase 4.6.2)

Layout written under `data/buyer_corpus/`:

    messages/<msg_id>/
        meta.json         # headers + agency_resolved + classification
        body.txt          # plain text body (or HTML→text fallback)
        attachments/      # raw file bytes (PDF, DOCX, XLS, etc.)
            <filename>
    index.json            # {msg_id: {date, from, subject, agency_key, ...}}
    by_agency.json        # {agency_key: [msg_id, ...]}
    by_thread.json        # {thread_id: [msg_id, ...]}
    .watermark            # last-completed run timestamp (for resume)

Idempotent + resumable: rerunning skips msg_ids already saved. Set
--clean to rebuild indexes from disk without re-fetching.

Defaults pull *broadly*: every Gmail message in the last 5 years
across both `sales` and `mike` inboxes that has a PDF attachment, OR
matches the RFQ-shape keyword set. Override with --query for ad-hoc
slices.

Usage:
    python scripts/harvest_buyer_corpus.py
        # 5y broad pull, both inboxes, default query
    python scripts/harvest_buyer_corpus.py --inbox mike --since 2024-01-01
    python scripts/harvest_buyer_corpus.py --query 'from:cdcr.ca.gov'
    python scripts/harvest_buyer_corpus.py --rebuild-indexes
"""
from __future__ import annotations

import argparse
import base64
import email
import email.utils
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from email.message import Message
from typing import Any

# Force UTF-8 stdout so unicode glyphs in progress banners (→, ─, …)
# don't crash on Windows cp1252 consoles.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def _utcnow() -> datetime:
    """Naive UTC timestamp; replacement for the deprecated
    `datetime.utcnow()` (Python 3.12+ deprecation, removed 3.14)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)

# Add project root to path so the script runs from anywhere
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

DEFAULT_OUT_DIR = os.path.join(_REPO_ROOT, "data", "buyer_corpus")
DEFAULT_INBOXES = ("sales", "mike")
DEFAULT_DAYS = 5 * 365

# Reytech-owned addresses. Mail from these *to* these is the app's
# own notification stream (CS Draft Ready, Order Digest, 500 Error,
# etc.) — useful as ops telemetry but pollution for buyer corpus
# work. We tag those as `app_internal` so the trainer skips them.
_OWN_ADDRESSES = {
    "sales@reytechinc.com",
    "mike@reytechinc.com",
}

# RFQ-shaped subject keywords. We OR these into a fallback query if
# `has:attachment` is too narrow. Cast a wide net — better to harvest
# extra threads than miss a contract amendment.
RFQ_KEYWORDS = (
    "RFQ", "rfq", "request for quote", "purchase order", "PO ",
    "amendment", "contract", "award", "solicitation", "bid",
)

# File extensions worth saving. Everything else (calendar invites,
# image footers in signatures) is metadata-only.
_KEEP_ATTACHMENT_EXTS = {
    ".pdf", ".docx", ".doc", ".xlsx", ".xls", ".csv",
    ".png", ".jpg", ".jpeg", ".tiff", ".tif",
}


# ─── Filesystem helpers ──────────────────────────────────────────────


def _safe_filename(name: str) -> str:
    """Strip path separators + control chars from an attachment name.
    Drive sometimes hands us names with `/` in them (folder-style).
    """
    name = (name or "attachment").strip()
    name = re.sub(r"[\x00-\x1f]", "", name)
    name = name.replace("/", "_").replace("\\", "_")
    name = re.sub(r"\s+", " ", name)
    if not name:
        name = "attachment"
    return name[:200]


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _write_json_atomic(path: str, data) -> None:
    """Write a JSON file via tmp+rename so a Ctrl-C doesn't corrupt it."""
    _ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, path)


def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


# ─── Gmail message parsing ───────────────────────────────────────────


def _decode_body(msg: Message) -> str:
    """Return the best plain-text body. Prefers text/plain; falls back
    to a stripped HTML body if only text/html is present."""
    plain_parts, html_parts = [], []
    for part in msg.walk():
        ctype = part.get_content_type()
        disp = str(part.get("Content-Disposition") or "").lower()
        if "attachment" in disp:
            continue
        try:
            payload = part.get_payload(decode=True)
        except Exception:
            payload = None
        if not payload:
            continue
        try:
            text = payload.decode(
                part.get_content_charset() or "utf-8",
                errors="replace",
            )
        except Exception:
            continue
        if ctype == "text/plain":
            plain_parts.append(text)
        elif ctype == "text/html":
            html_parts.append(text)
    if plain_parts:
        return "\n".join(plain_parts)
    if html_parts:
        # Crude HTML strip — for corpus/search use, we don't need
        # perfect rendering. Anything wanting structured HTML can
        # re-parse the .eml later if we save it.
        text = re.sub(r"<[^>]+>", " ", "\n".join(html_parts))
        return re.sub(r"\s+", " ", text).strip()
    return ""


def _extract_attachments(msg: Message) -> list[tuple[str, bytes]]:
    """Walk a MIME message, return [(filename, bytes), ...] for parts
    that look like real attachments (filename + non-empty payload)."""
    out = []
    for part in msg.walk():
        if part.is_multipart():
            continue
        disp = str(part.get("Content-Disposition") or "").lower()
        fn = part.get_filename() or ""
        # Some inline images carry no Content-Disposition but do have
        # a filename — keep them too.
        if "attachment" not in disp and not fn:
            continue
        try:
            data = part.get_payload(decode=True)
        except Exception:
            data = None
        if not data:
            continue
        out.append((fn, data))
    return out


def _parse_date_iso(raw: str) -> str:
    if not raw:
        return ""
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        return dt.isoformat() if dt else ""
    except Exception:
        return ""


def _decode_header(raw: str) -> str:
    """Decode RFC 2047 encoded-word headers (`=?utf-8?b?...?=`,
    `=?UTF-8?Q?...?=`) into plain Unicode. Buyers and Reytech's
    own notification emails frequently use Q/B encoding for emoji
    and non-ASCII, and a raw-string compare misses every keyword."""
    if not raw:
        return ""
    try:
        from email.header import decode_header, make_header
        return str(make_header(decode_header(raw)))
    except Exception:
        return raw


def _extract_address(raw_from_header: str) -> str:
    """Pull just the bare email out of an RFC 5322 address. e.g.
    `'Sales Support (Reytech) <sales@reytechinc.com>'` → `sales@reytechinc.com`."""
    if not raw_from_header:
        return ""
    try:
        _name, addr = email.utils.parseaddr(raw_from_header)
        return (addr or "").strip().lower()
    except Exception:
        return ""


# ─── Agency resolution ───────────────────────────────────────────────


# Newsletter / aggregator senders that look like RFQs but aren't real
# buyer mail. Skip agency resolution and tag the message via
# classification (`aggregator`) so the trainer ignores them.
_AGGREGATOR_DOMAINS = (
    "candspublishing", "unisonglobal", "bidnetdirect", "publicpurchase",
    "demandstar", "govbids", "constructconnect", "biddoc", "biddingo",
    "ssgaa.org",
)


def _is_aggregator(from_addr: str) -> bool:
    """True if the sender is a procurement-newsletter aggregator
    rather than an actual buyer."""
    addr = (from_addr or "").lower()
    return any(d in addr for d in _AGGREGATOR_DOMAINS)


# Canonical PO prefix → agency. Mirrors `core/order_dal.extract_canonical_po`
# patterns; if the subject carries `8955-...`, `4440-...`, or `4500...`
# we know the agency without a domain match.
_PO_PREFIX_AGENCY = (
    (re.compile(r"\b8955-\d{4,12}\b"), "calvet"),
    (re.compile(r"\b4440-\d{4,12}\b"), "dsh"),
    (re.compile(r"\b4500\d{4,12}\b"), "cchcs"),
)


def _agency_from_po_prefix(text: str) -> str:
    """Return the canonical agency if the text contains a Reytech-known
    PO prefix; '' otherwise. Used as a strong subject/body signal that
    bypasses domain-based resolution (forwarded RFQs and Reytech
    outbound replies still mention the original PO)."""
    if not text:
        return ""
    for pat, agency in _PO_PREFIX_AGENCY:
        if pat.search(text):
            return agency
    return ""


def _resolve_agency(headers: dict, body: str) -> str:
    """Best-effort agency_key derivation. Resolution order:
      1. Aggregator domain → 'aggregator' (newsletter, not real mail)
      2. Canonical PO prefix in subject → agency  (8955-, 4440-, 4500)
      3. From-address domain via institution_resolver
      4. To-address domain  (catches Reytech outbound replies where
         the buyer is the recipient, not sender)
      5. Subject as institution name
      6. First 500 chars of body
      7. PO prefix in body (last-ditch)
      → 'unknown' if all fail.
    """
    try:
        from src.core.institution_resolver import resolve as _resolve
    except Exception:
        return "unknown"

    from_addr = headers.get("from", "")
    to_addr = headers.get("to", "")
    subject = headers.get("subject", "")

    # Step 1: aggregator domain check (skip the rest, tag as such)
    if _is_aggregator(from_addr):
        return "aggregator"

    # Step 2: PO prefix in subject — strongest single signal
    ag = _agency_from_po_prefix(subject)
    if ag:
        return ag

    # Step 3: From-address domain
    out = _resolve("", email=from_addr)
    if out and out.get("agency"):
        return out["agency"]

    # Step 4: To-address domain (Reytech outbound replies have us in
    # From and the buyer in To — a 'Re: PO ...' from sales@reytechinc.com
    # to a CDCR buyer should resolve to cdcr).
    own = _OWN_ADDRESSES
    from_bare = _extract_address(from_addr)
    if from_bare in own and to_addr:
        out = _resolve("", email=to_addr)
        if out and out.get("agency"):
            return out["agency"]

    # Step 5: Subject as the institution name
    if subject:
        out = _resolve(subject, email=from_addr or to_addr)
        if out and out.get("agency"):
            return out["agency"]

    # Step 6: first 500 chars of body
    if body:
        out = _resolve(body[:500], email=from_addr)
        if out and out.get("agency"):
            return out["agency"]

    # Step 7: last-ditch — PO prefix in body
    ag = _agency_from_po_prefix(body[:2000] if body else "")
    if ag:
        return ag

    return "unknown"


def _classify_message(headers: dict, body: str,
                      attachments: list[tuple[str, bytes]]) -> str:
    """Coarse single-label classification — `app_internal` /
    `aggregator` / `rfq` / `award` / `amendment` / `quote_sent` /
    `other`. Trainer ignores `app_internal` and `aggregator`."""
    subj = (headers.get("subject", "") or "")
    from_addr = _extract_address(headers.get("from", ""))
    to_addr = _extract_address(headers.get("to", ""))
    subj_l = subj.lower()
    body_l = (body or "").lower()[:1000]
    text = subj_l + " " + body_l

    # Procurement-newsletter aggregators ('Calling all subcontractors',
    # 'Daily Buy Digest', etc.) hit the `bid` keyword but aren't real
    # buyer mail. Tag them so the trainer skips them entirely.
    if _is_aggregator(from_addr):
        return "aggregator"

    # App-internal notifications: the app sends these to its own
    # mailbox (CS Draft Ready, Order Digest, 500 Error, [URGENT] New
    # RFQ Arrived, [REMINDER] Drafts Waiting). Detect *before* keyword
    # matching so they don't false-positive as 'rfq' / 'award'.
    own = _OWN_ADDRESSES
    if from_addr in own and (to_addr in own or not to_addr):
        # Subject patterns that confirm app-internal even without
        # to-address parity (some notifications have empty To headers
        # in their MIME).
        internal_markers = (
            "[ACTION]", "[REMINDER]", "[URGENT]",
            "Reytech: ", "Reytech 5", "Order Digest",
            "Draft Ready", "Drafts Waiting", "Daily Digest",
        )
        if any(m.lower() in subj_l for m in internal_markers):
            return "app_internal"
        # If both from + to are us and subject doesn't look like a
        # quote-sent (no "quote" / "bid" / "RFQ" reply pattern), treat
        # as internal too. Real outbound buyer quotes are sent TO the
        # buyer, not back to ourselves.
        if "quote" not in subj_l and "rfq" not in subj_l:
            return "app_internal"

    if "amendment" in text:
        return "amendment"
    if "purchase order" in text or "award" in text or "po " in subj_l:
        return "award"
    if any(k in text for k in (
        "request for quote", "rfq", "solicitation", "bid",
    )):
        return "rfq"
    # Sent quotes from us — Reytech outbound has 'quote' in subject
    # plus we own the From: address (and recipient is NOT us).
    if "quote" in subj_l and from_addr in own and to_addr not in own:
        return "quote_sent"
    return "other"


# ─── Gmail fetching ─────────────────────────────────────────────────


def _fetch_full_message(service, msg_id: str) -> dict | None:
    """gmail.users.messages.get with format=raw for a stable parse path.
    Returns None on transient error so the loop can keep going."""
    try:
        result = service.users().messages().get(
            userId="me", id=msg_id, format="raw",
        ).execute()
        raw = result.get("raw", "")
        if not raw:
            return None
        # Gmail raw is URL-safe base64 of the RFC2822 message
        decoded = base64.urlsafe_b64decode(raw.encode("ascii"))
        return {
            "thread_id": result.get("threadId", ""),
            "label_ids": result.get("labelIds", []),
            "raw_bytes": decoded,
            "internal_date": result.get("internalDate", ""),
        }
    except Exception as e:
        sys.stderr.write(f"  fetch_full_message({msg_id}) failed: {e}\n")
        return None


def _build_query(args) -> str:
    """The default Gmail search. Mike said pull broadly — favor recall."""
    if args.query:
        return args.query

    since = (_utcnow() - timedelta(days=args.days)
             ).strftime("%Y/%m/%d")

    # Two clauses OR'd: (a) anything with a real attachment, or
    # (b) anything that looks RFQ-shaped by subject keyword. Mike's
    # case includes contract emails that may be all-text + Drive link,
    # so we don't strictly require has:attachment.
    kw = " OR ".join(f'"{k}"' for k in RFQ_KEYWORDS)
    q = (f"after:{since} ("
         f"(has:attachment filename:pdf) OR ({kw})"
         f")")
    return q


# ─── Main harvest loop ──────────────────────────────────────────────


def _save_message(out_dir: str, msg_id: str, parsed: dict,
                  full: dict, headers: dict, body: str,
                  attachments: list[tuple[str, bytes]]) -> str:
    msg_dir = os.path.join(out_dir, "messages", msg_id)
    _ensure_dir(msg_dir)
    _ensure_dir(os.path.join(msg_dir, "attachments"))

    # Save attachments first (so meta.json reflects what's actually
    # on disk if the operator interrupts mid-write)
    saved_attachments = []
    for fn, data in attachments:
        safe_name = _safe_filename(fn)
        path = os.path.join(msg_dir, "attachments", safe_name)
        try:
            with open(path, "wb") as f:
                f.write(data)
            saved_attachments.append({
                "filename": safe_name,
                "size_bytes": len(data),
                "ext": os.path.splitext(safe_name)[1].lower(),
            })
        except OSError as e:
            sys.stderr.write(f"  attachment save failed ({fn}): {e}\n")

    body_path = os.path.join(msg_dir, "body.txt")
    try:
        with open(body_path, "w", encoding="utf-8", errors="replace") as f:
            f.write(body)
    except OSError as e:
        sys.stderr.write(f"  body save failed: {e}\n")

    meta = {
        "msg_id": msg_id,
        "thread_id": full.get("thread_id", ""),
        "label_ids": full.get("label_ids", []),
        "internal_date_ms": full.get("internal_date", ""),
        "headers": {
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "cc": headers.get("cc", ""),
            "subject": headers.get("subject", ""),
            "date": headers.get("date", ""),
            "date_iso": headers.get("date_iso", ""),
            "message_id_header": headers.get("message_id_header", ""),
            "in_reply_to": headers.get("in_reply_to", ""),
            "references": headers.get("references", ""),
        },
        "agency_key": parsed["agency_key"],
        "classification": parsed["classification"],
        "attachments": saved_attachments,
        "body_chars": len(body),
        "harvested_at": _utcnow().isoformat(),
    }
    _write_json_atomic(os.path.join(msg_dir, "meta.json"), meta)
    return msg_dir


def _update_indexes(out_dir: str, msg_id: str, meta: dict) -> None:
    """Append msg_id to the master indexes. Cheap append since we
    only run this on newly-saved messages."""
    idx_path = os.path.join(out_dir, "index.json")
    by_agency_path = os.path.join(out_dir, "by_agency.json")
    by_thread_path = os.path.join(out_dir, "by_thread.json")

    idx = _load_json(idx_path, {})
    by_agency = _load_json(by_agency_path, {})
    by_thread = _load_json(by_thread_path, {})

    idx[msg_id] = {
        "date": meta["headers"].get("date_iso", ""),
        "from": meta["headers"].get("from", ""),
        "subject": meta["headers"].get("subject", ""),
        "agency_key": meta.get("agency_key", "unknown"),
        "classification": meta.get("classification", "other"),
        "attachment_count": len(meta.get("attachments", [])),
        "thread_id": meta.get("thread_id", ""),
    }

    ag = meta.get("agency_key") or "unknown"
    by_agency.setdefault(ag, [])
    if msg_id not in by_agency[ag]:
        by_agency[ag].append(msg_id)

    tid = meta.get("thread_id") or ""
    if tid:
        by_thread.setdefault(tid, [])
        if msg_id not in by_thread[tid]:
            by_thread[tid].append(msg_id)

    _write_json_atomic(idx_path, idx)
    _write_json_atomic(by_agency_path, by_agency)
    _write_json_atomic(by_thread_path, by_thread)


def _reclassify_in_place(out_dir: str) -> dict:
    """Walk every messages/<msg_id>/meta.json, re-decode the headers
    (RFC 2047 may be raw if harvested before the decoder fix), re-run
    the classifier and agency resolver, and rewrite meta.json + the
    indexes. No Gmail call — purely local correction.

    Used after a classifier upgrade to re-tag an existing corpus
    without paying the bandwidth cost of a full re-harvest.
    """
    msgs_dir = os.path.join(out_dir, "messages")
    if not os.path.isdir(msgs_dir):
        return {"reclassified": 0, "missing": 0,
                "class_changes": 0, "agency_changes": 0}
    out = {"reclassified": 0, "missing": 0,
           "class_changes": 0, "agency_changes": 0}
    for entry in sorted(os.listdir(msgs_dir)):
        meta_path = os.path.join(msgs_dir, entry, "meta.json")
        meta = _load_json(meta_path, None)
        if not meta:
            out["missing"] += 1
            continue
        h = meta.get("headers", {}) or {}
        # Re-decode subject + from + to (no-op if already decoded)
        new_h = dict(h)
        new_h["subject"] = _decode_header(h.get("subject", ""))
        new_h["from"] = _decode_header(h.get("from", ""))
        new_h["to"] = _decode_header(h.get("to", ""))
        # Re-classify with the NEW classifier on the decoded headers
        body_path = os.path.join(msgs_dir, entry, "body.txt")
        body = ""
        if os.path.exists(body_path):
            try:
                with open(body_path, "r", encoding="utf-8",
                          errors="replace") as f:
                    body = f.read()
            except OSError:
                pass
        new_class = _classify_message(new_h, body, [])
        new_agency = _resolve_agency(new_h, body)
        if new_class != meta.get("classification"):
            out["class_changes"] += 1
        if new_agency != meta.get("agency_key"):
            out["agency_changes"] += 1
        meta["headers"] = new_h
        meta["classification"] = new_class
        meta["agency_key"] = new_agency
        _write_json_atomic(meta_path, meta)
        out["reclassified"] += 1
    # Rebuild indexes off the corrected meta.json files
    _rebuild_indexes(out_dir)
    return out


def _rebuild_indexes(out_dir: str) -> dict:
    """Walk messages/<msg_id>/meta.json and regenerate the indexes
    from disk. Useful after manual edits or partial-write recovery."""
    msgs_dir = os.path.join(out_dir, "messages")
    if not os.path.isdir(msgs_dir):
        return {"rebuilt": 0, "missing": 0}
    idx, by_agency, by_thread = {}, {}, {}
    rebuilt, missing = 0, 0
    for entry in sorted(os.listdir(msgs_dir)):
        meta_path = os.path.join(msgs_dir, entry, "meta.json")
        meta = _load_json(meta_path, None)
        if not meta:
            missing += 1
            continue
        idx[entry] = {
            "date": meta["headers"].get("date_iso", ""),
            "from": meta["headers"].get("from", ""),
            "subject": meta["headers"].get("subject", ""),
            "agency_key": meta.get("agency_key", "unknown"),
            "classification": meta.get("classification", "other"),
            "attachment_count": len(meta.get("attachments", [])),
            "thread_id": meta.get("thread_id", ""),
        }
        ag = meta.get("agency_key") or "unknown"
        by_agency.setdefault(ag, []).append(entry)
        tid = meta.get("thread_id") or ""
        if tid:
            by_thread.setdefault(tid, []).append(entry)
        rebuilt += 1
    _write_json_atomic(os.path.join(out_dir, "index.json"), idx)
    _write_json_atomic(os.path.join(out_dir, "by_agency.json"), by_agency)
    _write_json_atomic(os.path.join(out_dir, "by_thread.json"), by_thread)
    return {"rebuilt": rebuilt, "missing": missing}


def harvest(args) -> int:
    out_dir = args.out_dir
    _ensure_dir(out_dir)
    _ensure_dir(os.path.join(out_dir, "messages"))

    if args.rebuild_indexes:
        result = _rebuild_indexes(out_dir)
        print(f"Rebuilt indexes: {result['rebuilt']} messages "
              f"({result['missing']} missing meta.json)")
        return 0

    if args.reclassify:
        result = _reclassify_in_place(out_dir)
        print(f"Reclassified {result['reclassified']} messages")
        print(f"  classification changes : {result['class_changes']}")
        print(f"  agency changes         : {result['agency_changes']}")
        print(f"  missing meta.json      : {result['missing']}")
        return 0

    try:
        from src.core.gmail_api import (
            is_configured, get_service, list_message_ids,
        )
    except Exception as e:
        sys.stderr.write(f"gmail_api import failed: {e}\n")
        return 2

    if not is_configured():
        sys.stderr.write(
            "Gmail API not configured. Set GMAIL_OAUTH_CLIENT_ID, "
            "GMAIL_OAUTH_CLIENT_SECRET, GMAIL_OAUTH_REFRESH_TOKEN.\n"
        )
        return 2

    inboxes = args.inboxes or DEFAULT_INBOXES
    query = _build_query(args)
    print(f"Harvesting Gmail → {out_dir}")
    print(f"  inboxes : {inboxes}")
    print(f"  query   : {query}")
    print(f"  max/box : {args.max_messages}")

    totals = {
        "examined": 0, "saved": 0, "skipped_existing": 0,
        "errors": 0, "attachments_saved": 0,
    }

    for inbox in inboxes:
        print(f"\n── inbox '{inbox}' ──")
        try:
            svc = get_service(inbox)
        except Exception as e:
            sys.stderr.write(f"  service('{inbox}') failed: {e}\n")
            totals["errors"] += 1
            continue
        try:
            ids = list_message_ids(
                svc, query=query, max_results=args.max_messages,
            )
        except Exception as e:
            sys.stderr.write(f"  list_message_ids failed: {e}\n")
            totals["errors"] += 1
            continue

        print(f"  matched {len(ids)} message ids")

        for i, mid in enumerate(ids):
            totals["examined"] += 1
            msg_dir = os.path.join(out_dir, "messages", mid)
            meta_path = os.path.join(msg_dir, "meta.json")
            if os.path.exists(meta_path) and not args.force:
                totals["skipped_existing"] += 1
                continue

            full = _fetch_full_message(svc, mid)
            if not full:
                totals["errors"] += 1
                continue

            try:
                m = email.message_from_bytes(full["raw_bytes"])
            except Exception as e:
                sys.stderr.write(f"  parse({mid}) failed: {e}\n")
                totals["errors"] += 1
                continue

            headers = {
                "from": _decode_header(m.get("From", "") or ""),
                "to": _decode_header(m.get("To", "") or ""),
                "cc": _decode_header(m.get("Cc", "") or ""),
                "subject": _decode_header(m.get("Subject", "") or ""),
                "date": m.get("Date", "") or "",
                "date_iso": _parse_date_iso(m.get("Date", "")),
                "message_id_header": m.get("Message-ID", "") or "",
                "in_reply_to": m.get("In-Reply-To", "") or "",
                "references": m.get("References", "") or "",
            }
            body = _decode_body(m)
            atts = _extract_attachments(m)
            # Filter to interesting extensions to avoid 5KB
            # text-signature attachments
            if not args.keep_all_attachments:
                atts = [(fn, data) for fn, data in atts
                        if os.path.splitext(_safe_filename(fn))[1].lower()
                        in _KEEP_ATTACHMENT_EXTS]

            agency = _resolve_agency(headers, body)
            classification = _classify_message(headers, body, atts)

            parsed = {
                "agency_key": agency,
                "classification": classification,
            }

            try:
                _save_message(out_dir, mid, parsed, full, headers,
                              body, atts)
            except Exception as e:
                sys.stderr.write(f"  save({mid}) failed: {e}\n")
                totals["errors"] += 1
                continue

            # Re-read the meta we just wrote so the index reflects on-
            # disk truth (including any failed attachment writes).
            meta = _load_json(
                os.path.join(out_dir, "messages", mid, "meta.json"),
                None,
            )
            if meta:
                _update_indexes(out_dir, mid, meta)
                totals["saved"] += 1
                totals["attachments_saved"] += len(meta.get("attachments", []))

            if args.progress and (i + 1) % args.progress == 0:
                print(f"  …{i+1}/{len(ids)} examined "
                      f"(saved={totals['saved']} skipped="
                      f"{totals['skipped_existing']} err="
                      f"{totals['errors']})")

    # Save watermark for next-run delta optimization (we don't strictly
    # use it yet — Gmail `after:` clause already handles that — but
    # the file is a useful breadcrumb).
    _write_json_atomic(
        os.path.join(out_dir, ".watermark"),
        {"last_run_iso": _utcnow().isoformat(),
         "totals": totals,
         "query": query,
         "inboxes": list(inboxes)},
    )

    print()
    print("─" * 60)
    print("Harvest complete:")
    for k, v in totals.items():
        print(f"  {k:20s} : {v}")
    return 0


def main():
    p = argparse.ArgumentParser(
        description="Pull all RFQ + sent + contract/amendment emails "
                    "from Gmail into a local corpus directory."
    )
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR,
                   help="Output directory (default: data/buyer_corpus/)")
    p.add_argument("--inbox", action="append", dest="inboxes",
                   default=None,
                   help="Inbox name from gmail_api.get_service "
                        "(repeat for multiple). Default: sales + mike.")
    p.add_argument("--days", type=int, default=DEFAULT_DAYS,
                   help="How far back to harvest (days). Default: 5y.")
    p.add_argument("--query", default="",
                   help="Custom Gmail query (overrides default broad pull)")
    p.add_argument("--max-messages", type=int, default=5000,
                   help="Per-inbox message cap (default: 5000)")
    p.add_argument("--force", action="store_true",
                   help="Re-fetch + overwrite already-saved msg_ids")
    p.add_argument("--keep-all-attachments", action="store_true",
                   help="Don't filter attachments by extension — keep "
                        "everything (signatures, calendar invites, etc.)")
    p.add_argument("--rebuild-indexes", action="store_true",
                   help="Rebuild index.json/by_agency.json/by_thread.json "
                        "from on-disk meta.json files (no Gmail call)")
    p.add_argument("--reclassify", action="store_true",
                   help="Re-decode headers + re-run classifier and "
                        "agency resolver on every saved message in place. "
                        "No Gmail call. Use after a classifier upgrade to "
                        "re-tag existing corpus.")
    p.add_argument("--progress", type=int, default=50,
                   help="Print a progress line every N messages (0=off)")
    args = p.parse_args()
    sys.exit(harvest(args))


if __name__ == "__main__":
    main()
