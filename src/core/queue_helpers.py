"""Normalize PC and RFQ dicts into a common schema for the unified queue table."""
import re

_AUTO_NUMBER_RE = re.compile(r"^AUTO_[0-9a-f]+$", re.IGNORECASE)


def _resolve_display_number(number, raw):
    """If `number` is a placeholder AUTO_<hex> id, try to substitute the
    attachment-derived title so the queue shows something human-readable.

    Surface #17 follow-on (2026-05-05): PR #727 wired the cascade at ingest,
    but PCs/RFQs ingested before that fix still display AUTO_db670ad9 etc.
    on the queue. This is read-side only — the underlying record's
    pc_number / rfq_number is left intact (it's the canonical id; URL routing
    uses item_id which is independent).
    """
    if not number or not _AUTO_NUMBER_RE.match(str(number)):
        return number
    source_pdf = raw.get("source_pdf") or ""
    if not source_pdf:
        return number
    try:
        from src.core.ingest_pipeline import _attachment_filename_title
        title = _attachment_filename_title(source_pdf)
    except Exception:
        title = ""
    return title or number


STATUS_DISPLAY = {
    "new": "New", "parsed": "New", "parse_error": "New",
    "needs_review": "Needs Review",
    "draft": "Draft", "priced": "Draft", "ready": "Draft",
    "auto_drafted": "Draft", "quoted": "Draft", "generated": "Draft",
    "completed": "Draft", "converted": "Draft",
    "sent": "Sent", "pending_award": "Sent", "won": "Sent",
    "lost": "Not Responding", "expired": "Not Responding",
    "no_response": "Not Responding", "dismissed": "Not Responding",
    "archived": "Not Responding", "duplicate": "Not Responding",
}

STATUS_COLOR = {
    "New": "#4f8cff",
    "Needs Review": "#f0883e",
    "Draft": "#fbbf24",
    "Sent": "#3fb950",
    "Not Responding": "#f85149",
}


# Junk values from the legacy email-poller subject[:40] fallback. The
# new ingest path produces "AUTO_<short_id>" instead, but pre-PR-A
# rows still carry "GOOD" / "WORKSHEET" / "RFQ" etc. Mirror of
# src.api.dashboard._is_placeholder_number — kept inline so this
# module doesn't drag in the Flask import graph.
def _is_placeholder_number(value: str) -> bool:
    """True when the value looks like a buyer-content-derived placeholder."""
    if not value:
        return True
    s = str(value).strip()
    if not s or s == "(blank)":
        return True
    if s.startswith("AUTO_"):
        return True  # auto-generated, not a real number — show "Pending"
    if s.isupper() and s.isalpha() and 2 <= len(s) <= 20:
        return True
    if s.lower() in {"unknown", "rfq", "quote", "request", "worksheet", "good",
                     "bid", "vendor", "price", "check", "form"}:
        return True
    return False


def _derive_buyer(raw, queue_type):
    """Extract primary buyer name and optional sub-line from raw dict."""
    if queue_type == "pc":
        email = raw.get("requestor_email") or ""
        requestor = raw.get("requestor") or ""
        if email:
            primary = email.split("@")[0].replace(".", " ").title()
            sub = requestor if requestor and requestor != primary else ""
        else:
            primary = requestor or "\u2014"
            sub = ""
    else:
        email = raw.get("email_sender") or raw.get("requestor_email") or ""
        requestor = raw.get("requestor_name") or ""
        if email:
            primary = email.split("@")[0].replace(".", " ").title()
            sub = requestor if requestor and requestor != primary else ""
        else:
            primary = requestor or "\u2014"
            sub = ""
        # RFQ may have agency_name for generic_rfq form type
        agency_name = raw.get("agency_name") or ""
        if agency_name and raw.get("form_type") == "generic_rfq":
            sub = (sub + "\n" + agency_name).strip("\n") if sub else agency_name
    return primary, sub


def _item_count(raw, queue_type):
    """Return number of line items."""
    if queue_type == "pc":
        items = raw.get("items", [])
    else:
        items = raw.get("line_items", raw.get("items", []))
    if isinstance(items, str):
        try:
            import json
            items = json.loads(items)
        except Exception:
            items = []
    return len(items) if isinstance(items, list) else 0


def normalize_queue_item(raw, queue_type, item_id):
    """Return a dict with unified keys consumed by the _queue_table.html macro.

    Works for both active PCs and active RFQs.
    """
    status = raw.get("status", "new")
    display_status = STATUS_DISPLAY.get(status, "New")
    buyer, buyer_sub = _derive_buyer(raw, queue_type)

    if queue_type == "pc":
        number = raw.get("pc_number") or "(blank)"
        institution = raw.get("institution") or "\u2014"
        url = "/pricecheck/" + item_id
        quote_number = raw.get("reytech_quote_number") or ""
    else:
        number = raw.get("solicitation_number") or raw.get("rfq_number") or ""
        institution = raw.get("institution") or raw.get("agency_name") or "\u2014"
        url = "/rfq/" + item_id
        quote_number = raw.get("reytech_quote_number") or ""

    number = _resolve_display_number(number, raw)

    return {
        "id": item_id,
        "number": number,
        "institution": institution,
        "buyer": buyer,
        "buyer_sub": buyer_sub,
        "due_date": raw.get("due_date") or "",
        "due_time": raw.get("due_time") or "",
        "_urgency": raw.get("_urgency", "normal"),
        "_days_left": raw.get("_days_left"),
        # Hours remaining to the precise close time (time-aware). Falls back
        # to None when the deadline is date-only or unparseable; in that case
        # the UI flags the 2:00 PM default via "_time_explicit=False".
        "_hours_left": raw.get("_hours_left"),
        "_time_explicit": bool(raw.get("due_time")),
        # PR #429/#430/#432 + 2026-04-22 incident: a default-stamped deadline
        # is a guess, not a fact. UI must distinguish `default` from `email`
        # or `header` so operators don't treat 10+ "2d left" defaults as real.
        "due_date_source": (raw.get("due_date_source") or "").lower(),
        "item_count": _item_count(raw, queue_type),
        "_readiness": raw.get("_readiness", {}),
        "quote_number": quote_number,
        "status": status,
        "display_status": display_status,
        "status_color": STATUS_COLOR.get(display_status, "#8b90a0"),
        "url": url,
        "queue_type": queue_type,
        # Surface the parse-failure flag set upstream in routes_rfq.home()
        # so the queue row can render a badge. Set when a PC ingested from
        # email has zero items — the parser couldn't read the 704.
        "_parse_failed": bool(raw.get("_parse_failed")),
    }


def normalize_sent_item(raw, queue_type, item_id):
    """Return a normalized dict for sent/completed section rows."""
    base = normalize_queue_item(raw, queue_type, item_id)
    sent_date = raw.get("sent_at") or raw.get("updated_at") or ""
    search_parts = [
        (base["number"] or "").lower(),
        (base["institution"] or "").lower(),
        (base["buyer"] or "").lower(),
        base["status"].lower(),
    ]
    base["sent_date"] = sent_date
    base["search_text"] = " ".join(search_parts)
    return base
