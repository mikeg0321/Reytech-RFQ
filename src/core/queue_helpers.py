"""Normalize PC and RFQ dicts into a common schema for the unified queue table."""

STATUS_DISPLAY = {
    "new": "New", "parsed": "New", "parse_error": "New",
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
    "Draft": "#fbbf24",
    "Sent": "#3fb950",
    "Not Responding": "#f85149",
}


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

    return {
        "id": item_id,
        "number": number,
        "institution": institution,
        "buyer": buyer,
        "buyer_sub": buyer_sub,
        "due_date": raw.get("due_date") or "",
        "_urgency": raw.get("_urgency", "normal"),
        "_days_left": raw.get("_days_left"),
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
