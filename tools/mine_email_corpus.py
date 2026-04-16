#!/usr/bin/env python3
"""Mine 2 years of agency emails from both Reytech inboxes.

Phase 0.5 empirical grounding — builds the dataset the rebuild's extractors,
profiles, and golden fixtures are calibrated against.

Usage:
    # Requires env vars: GMAIL_OAUTH_CLIENT_ID, GMAIL_OAUTH_CLIENT_SECRET,
    #                     GMAIL_OAUTH_REFRESH_TOKEN, GMAIL_OAUTH_REFRESH_TOKEN_2
    python tools/mine_email_corpus.py

    # Or with Railway env:
    railway run python tools/mine_email_corpus.py

Output:
    data/email_corpus/sales_corpus.jsonl      — one JSON per email from sales@
    data/email_corpus/mike_corpus.jsonl       — one JSON per email from mike@
    data/email_corpus/corpus_analysis.json    — frequency analysis + pattern report
    data/email_corpus/attachment_index.jsonl  — all attachments with SHA-256 + metadata

The raw corpus is gitignored (contains PII, buyer names, prices). The analysis
report and anonymized pattern summaries can ship to the repo.
"""
import email
import email.utils
import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("corpus_miner")

# ── Configuration ────────────────────────────────────────────────────────────

CORPUS_DIR = os.path.join("data", "email_corpus")
ATTACHMENT_DIR = os.path.join(CORPUS_DIR, "attachments")
START_DATE = "2024/04/15"   # 2 years back
END_DATE = "2026/04/16"     # Tomorrow (capture all of today)

# Agency domain patterns — broad filter for the "both passes" approach
AGENCY_DOMAINS = [
    "ca.gov", "cchcs.ca.gov", "cdcr.ca.gov", "calvet.ca.gov",
    "dgs.ca.gov", "fiscal.ca.gov", "caleprocure.ca.gov",
]

AGENCY_KEYWORDS = [
    "rfq", "rfp", "quote", "solicitation", "ams 704", "704",
    "price check", "bid", "procurement", "purchase order",
    "cchcs", "cdcr", "calvet", "california",
]

# Exclude patterns (spam, automated)
EXCLUDE_FROM = [
    "noreply", "no-reply", "mailer-daemon", "notifications@",
    "newsletter@", "marketing@", "support@google", "calendar-notification",
]

# Rate limiting
BATCH_SIZE = 50          # Messages per batch before brief pause
BATCH_PAUSE_SEC = 1.0    # Seconds between batches
ERROR_PAUSE_SEC = 5.0    # Seconds after an error before retry


# ── Gmail API helpers ────────────────────────────────────────────────────────

def get_gmail_service(inbox_name):
    """Get Gmail API service, handling import and auth errors gracefully."""
    try:
        from src.core.gmail_api import get_service, is_configured
        if not is_configured():
            log.error("Gmail API not configured. Set GMAIL_OAUTH_* env vars.")
            return None
        return get_service(inbox_name)
    except Exception as e:
        log.error("Failed to get Gmail service for %s: %s", inbox_name, e)
        return None


def list_all_message_ids(service, query, max_results=10000):
    """List all message IDs matching query, with pagination."""
    from src.core.gmail_api import list_message_ids
    try:
        return list_message_ids(service, query, max_results=max_results)
    except Exception as e:
        log.error("list_message_ids failed: %s", e)
        return []


def fetch_raw_message(service, msg_id, retries=3):
    """Fetch raw message with retry logic."""
    from src.core.gmail_api import get_raw_message
    for attempt in range(retries):
        try:
            return get_raw_message(service, msg_id)
        except Exception as e:
            if attempt < retries - 1:
                log.warning("Retry %d/%d for %s: %s", attempt + 1, retries, msg_id, e)
                time.sleep(ERROR_PAUSE_SEC * (attempt + 1))
            else:
                log.error("Failed after %d retries for %s: %s", retries, msg_id, e)
                return None


# ── Email parsing ────────────────────────────────────────────────────────────

def parse_email(raw_bytes, gmail_id):
    """Parse raw email bytes into a structured record."""
    msg = email.message_from_bytes(raw_bytes)

    # Headers
    subject = msg.get("Subject", "") or ""
    from_addr = msg.get("From", "") or ""
    to_addr = msg.get("To", "") or ""
    date_str = msg.get("Date", "") or ""
    message_id = msg.get("Message-ID", "") or ""
    in_reply_to = msg.get("In-Reply-To", "") or ""
    references = msg.get("References", "") or ""

    # Parse date
    date_tuple = email.utils.parsedate_tz(date_str)
    parsed_date = ""
    if date_tuple:
        try:
            parsed_date = datetime(*date_tuple[:6]).isoformat()
        except Exception:
            pass

    # Extract from email address
    from_email = ""
    match = re.search(r'[\w.-]+@[\w.-]+', from_addr)
    if match:
        from_email = match.group(0).lower()

    # Body text (prefer plain text, fallback to HTML stripped)
    body_text = ""
    body_html = ""
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain" and not body_text:
                try:
                    body_text = part.get_payload(decode=True).decode("utf-8", errors="replace")
                except Exception:
                    pass
            elif ct == "text/html" and not body_html:
                try:
                    body_html = part.get_payload(decode=True).decode("utf-8", errors="replace")
                except Exception:
                    pass
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                body_text = payload.decode("utf-8", errors="replace")
        except Exception:
            pass

    if not body_text and body_html:
        # Strip HTML tags for searchable text
        body_text = re.sub(r'<[^>]+>', ' ', body_html)
        body_text = re.sub(r'\s+', ' ', body_text).strip()

    # Attachments metadata
    attachments = []
    if msg.is_multipart():
        for part in msg.walk():
            filename = part.get_filename()
            if filename:
                content_type = part.get_content_type()
                payload = part.get_payload(decode=True)
                size = len(payload) if payload else 0
                sha256 = hashlib.sha256(payload).hexdigest() if payload else ""
                ext = os.path.splitext(filename)[1].lower()
                attachments.append({
                    "filename": filename,
                    "content_type": content_type,
                    "size": size,
                    "sha256": sha256,
                    "extension": ext,
                })

    # Thread detection
    is_reply = bool(in_reply_to or references)
    thread_depth = len(references.split()) if references else 0

    return {
        "gmail_id": gmail_id,
        "message_id": message_id,
        "subject": subject,
        "from_addr": from_addr,
        "from_email": from_email,
        "to_addr": to_addr,
        "date_str": date_str,
        "parsed_date": parsed_date,
        "body_preview": body_text[:2000] if body_text else "",
        "body_length": len(body_text),
        "is_reply": is_reply,
        "in_reply_to": in_reply_to,
        "thread_depth": thread_depth,
        "attachment_count": len(attachments),
        "attachments": attachments,
    }


def save_attachments(raw_bytes, gmail_id, corpus_name):
    """Save attachments to disk. Returns list of saved file paths."""
    msg = email.message_from_bytes(raw_bytes)
    saved = []
    if not msg.is_multipart():
        return saved

    att_dir = os.path.join(ATTACHMENT_DIR, corpus_name, gmail_id[:16])
    for part in msg.walk():
        filename = part.get_filename()
        if not filename:
            continue
        payload = part.get_payload(decode=True)
        if not payload:
            continue

        # Safe filename
        safe_name = re.sub(r'[^\w\s.\-()]', '_', filename)[:100]
        os.makedirs(att_dir, exist_ok=True)
        path = os.path.join(att_dir, safe_name)
        with open(path, "wb") as f:
            f.write(payload)
        saved.append({
            "path": path,
            "filename": filename,
            "size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        })

    return saved


# ── Classification (narrow + broad passes) ───────────────────────────────────

def classify_email(record):
    """Classify email as agency-related using both narrow and broad filters.

    Returns:
        dict with 'is_narrow' (matched existing PC/RFQ), 'is_broad' (keyword/domain match),
        'agency_domain', 'keywords_found'
    """
    from_email = record.get("from_email", "").lower()
    subject = record.get("subject", "").lower()
    body = record.get("body_preview", "").lower()
    text = subject + " " + body

    # Narrow: from a known agency domain
    agency_domain = ""
    for domain in AGENCY_DOMAINS:
        if domain in from_email:
            agency_domain = domain
            break

    # Broad: keyword match
    keywords_found = []
    for kw in AGENCY_KEYWORDS:
        if kw in text:
            keywords_found.append(kw)

    # Attachment-based (PDFs named 704, 703b, etc.)
    for att in record.get("attachments", []):
        fname = att.get("filename", "").lower()
        if any(x in fname for x in ["704", "703", "rfq", "solicitation", "bid"]):
            keywords_found.append(f"attachment:{fname[:30]}")

    is_narrow = bool(agency_domain)
    is_broad = bool(keywords_found) or is_narrow

    return {
        "is_narrow": is_narrow,
        "is_broad": is_broad,
        "agency_domain": agency_domain,
        "keywords_found": keywords_found[:10],
    }


# ── Analysis ─────────────────────────────────────────────────────────────────

def analyze_corpus(records):
    """Generate frequency analysis from mined corpus."""
    total = len(records)
    narrow = sum(1 for r in records if r.get("classification", {}).get("is_narrow"))
    broad = sum(1 for r in records if r.get("classification", {}).get("is_broad"))

    # Agency domain distribution
    domain_counts = {}
    for r in records:
        d = r.get("classification", {}).get("agency_domain", "")
        if d:
            domain_counts[d] = domain_counts.get(d, 0) + 1

    # Keyword frequency
    kw_counts = {}
    for r in records:
        for kw in r.get("classification", {}).get("keywords_found", []):
            kw_counts[kw] = kw_counts.get(kw, 0) + 1

    # Attachment type distribution
    ext_counts = {}
    total_attachments = 0
    for r in records:
        for att in r.get("attachments", []):
            ext = att.get("extension", "unknown")
            ext_counts[ext] = ext_counts.get(ext, 0) + 1
            total_attachments += 1

    # Reply thread distribution
    reply_count = sum(1 for r in records if r.get("is_reply"))
    thread_depths = [r.get("thread_depth", 0) for r in records if r.get("is_reply")]
    avg_thread_depth = sum(thread_depths) / len(thread_depths) if thread_depths else 0

    # Due date pattern detection
    due_date_patterns = []
    date_re = re.compile(r'due\s*(?:date|by|on|before)?\s*:?\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})', re.IGNORECASE)
    for r in records:
        text = r.get("subject", "") + " " + r.get("body_preview", "")
        matches = date_re.findall(text)
        if matches:
            due_date_patterns.extend(matches[:3])

    # Monthly volume
    monthly = {}
    for r in records:
        d = r.get("parsed_date", "")
        if d and len(d) >= 7:
            month = d[:7]
            monthly[month] = monthly.get(month, 0) + 1

    return {
        "total_emails": total,
        "narrow_match": narrow,
        "broad_match": broad,
        "delta_broad_minus_narrow": broad - narrow,
        "agency_domains": dict(sorted(domain_counts.items(), key=lambda x: -x[1])),
        "keyword_frequency": dict(sorted(kw_counts.items(), key=lambda x: -x[1])[:20]),
        "attachment_extensions": dict(sorted(ext_counts.items(), key=lambda x: -x[1])),
        "total_attachments": total_attachments,
        "reply_emails": reply_count,
        "reply_pct": round(reply_count / total * 100, 1) if total > 0 else 0,
        "avg_thread_depth": round(avg_thread_depth, 1),
        "due_date_samples": due_date_patterns[:20],
        "monthly_volume": dict(sorted(monthly.items())),
        "mined_at": datetime.now().isoformat(),
    }


# ── Main pipeline ────────────────────────────────────────────────────────────

def mine_inbox(inbox_name, output_file):
    """Mine all emails from one inbox."""
    log.info("=" * 60)
    log.info("Mining inbox: %s", inbox_name)
    log.info("=" * 60)

    service = get_gmail_service(inbox_name)
    if not service:
        return []

    # Build query — broad net, we'll classify in post-processing
    exclude = " ".join(f"-from:{e}" for e in EXCLUDE_FROM)
    query = f"in:inbox after:{START_DATE} before:{END_DATE} {exclude}"
    log.info("Query: %s", query)

    # Fetch all message IDs
    msg_ids = list_all_message_ids(service, query, max_results=10000)
    log.info("Found %d messages in %s", len(msg_ids), inbox_name)

    if not msg_ids:
        return []

    records = []
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        for i, msg_id in enumerate(msg_ids):
            # Rate limiting
            if i > 0 and i % BATCH_SIZE == 0:
                log.info("  Progress: %d/%d (%.0f%%)", i, len(msg_ids), i / len(msg_ids) * 100)
                time.sleep(BATCH_PAUSE_SEC)

            raw = fetch_raw_message(service, msg_id)
            if not raw:
                continue

            try:
                record = parse_email(raw, msg_id)
                record["inbox"] = inbox_name
                record["classification"] = classify_email(record)

                # Save attachments if agency-related
                if record["classification"]["is_broad"] and record["attachment_count"] > 0:
                    saved = save_attachments(raw, msg_id, inbox_name)
                    record["saved_attachments"] = len(saved)

                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                records.append(record)

            except Exception as e:
                log.warning("Parse error for %s: %s", msg_id, e)

    log.info("Mined %d emails from %s → %s", len(records), inbox_name, output_file)
    return records


def main():
    os.makedirs(CORPUS_DIR, exist_ok=True)
    os.makedirs(ATTACHMENT_DIR, exist_ok=True)

    all_records = []

    # Mine both inboxes
    sales_file = os.path.join(CORPUS_DIR, "sales_corpus.jsonl")
    mike_file = os.path.join(CORPUS_DIR, "mike_corpus.jsonl")

    sales_records = mine_inbox("sales", sales_file)
    all_records.extend(sales_records)

    mike_records = mine_inbox("mike", mike_file)
    all_records.extend(mike_records)

    if not all_records:
        log.warning("No emails mined. Check Gmail API configuration.")
        return 1

    # Run analysis
    log.info("Running corpus analysis on %d total emails...", len(all_records))
    analysis = analyze_corpus(all_records)
    analysis["sales_count"] = len(sales_records)
    analysis["mike_count"] = len(mike_records)

    analysis_file = os.path.join(CORPUS_DIR, "corpus_analysis.json")
    with open(analysis_file, "w", encoding="utf-8") as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    log.info("Analysis saved → %s", analysis_file)

    # Print summary
    print("\n" + "=" * 60)
    print("CORPUS MINING COMPLETE")
    print("=" * 60)
    print(f"  Total emails:      {analysis['total_emails']}")
    print(f"  Narrow match:      {analysis['narrow_match']} (from agency domains)")
    print(f"  Broad match:       {analysis['broad_match']} (keywords/attachments)")
    print(f"  Delta:             {analysis['delta_broad_minus_narrow']} (potential missed bids)")
    print(f"  Total attachments: {analysis['total_attachments']}")
    print(f"  Reply emails:      {analysis['reply_emails']} ({analysis['reply_pct']}%)")
    print(f"  Avg thread depth:  {analysis['avg_thread_depth']}")
    print(f"\n  Top agencies: {json.dumps(analysis['agency_domains'], indent=4)}")
    print(f"\n  Top keywords: {json.dumps(analysis['keyword_frequency'], indent=4)}")
    print(f"\n  Attachment types: {json.dumps(analysis['attachment_extensions'], indent=4)}")
    print(f"\n  Due date samples: {analysis['due_date_samples'][:5]}")
    print(f"\n  Monthly volume (last 6):")
    months = list(analysis["monthly_volume"].items())[-6:]
    for month, count in months:
        print(f"    {month}: {'#' * min(count, 50)} ({count})")
    print(f"\n  Output: {CORPUS_DIR}/")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
