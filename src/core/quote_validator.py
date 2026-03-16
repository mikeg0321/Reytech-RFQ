"""
Quote Validator — Hard gate before generate/send.
Blocks incomplete quotes. Returns specific missing items.
Status flow enforcement. Completion checklist.
"""
import logging
import json

log = logging.getLogger("reytech.validator")


# ── System 1: Pre-Send Validation Gate ────────────────────────────────────

def validate_ready_to_generate(rfq_data):
    """Check if RFQ is ready to generate a quote package.
    Returns {"ok": bool, "errors": [], "warnings": [], "score": int}
    """
    errors = []
    warnings = []
    items = rfq_data.get("line_items", rfq_data.get("items", []))

    if not items:
        errors.append("No line items — nothing to quote")
        return {"ok": False, "errors": errors, "warnings": warnings, "score": 0}

    zero_cost = []
    zero_price = []
    no_desc = []
    no_qty = []

    for i, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        idx = i + 1
        desc = item.get("description", item.get("desc", ""))

        if not desc or len(desc.strip()) < 3:
            no_desc.append(idx)

        qty = item.get("quantity", item.get("qty", 0))
        try:
            qty = float(str(qty).replace(",", ""))
        except (ValueError, TypeError):
            qty = 0
        if qty <= 0:
            no_qty.append(idx)

        cost = item.get("supplier_cost", item.get("cost", item.get("unit_cost", 0)))
        try:
            cost = float(str(cost or 0).replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            cost = 0
        if cost <= 0:
            zero_cost.append(idx)

        price = item.get("price_per_unit", item.get("bid_price", item.get("unit_price", 0)))
        try:
            price = float(str(price or 0).replace("$", "").replace(",", ""))
        except (ValueError, TypeError):
            price = 0
        if price <= 0:
            zero_price.append(idx)

    if zero_cost:
        errors.append(f"Items {zero_cost} have no cost — cannot calculate margin")
    if zero_price:
        errors.append(f"Items {zero_price} have $0 bid price — buyer gets free items")
    if no_desc:
        errors.append(f"Items {no_desc} have no description")
    if no_qty:
        warnings.append(f"Items {no_qty} have no quantity")

    if not rfq_data.get("requestor_email"):
        warnings.append("No buyer email — cannot send quote")
    if not rfq_data.get("requestor_name"):
        warnings.append("No buyer name")

    total_items = len(items)
    priced_items = total_items - len(zero_price)
    costed_items = total_items - len(zero_cost)
    score = int((priced_items + costed_items) / (total_items * 2) * 100) if total_items > 0 else 0

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "score": score,
        "summary": {
            "total_items": total_items,
            "priced": priced_items,
            "costed": costed_items,
            "missing_cost": zero_cost,
            "missing_price": zero_price,
        }
    }


def validate_ready_to_send(rfq_data):
    """Stricter check before sending — everything must be complete."""
    result = validate_ready_to_generate(rfq_data)

    if not rfq_data.get("requestor_email"):
        result["errors"].append("No buyer email address")
        result["ok"] = False

    has_files = (
        rfq_data.get("output_files") or
        rfq_data.get("generated_files") or
        rfq_data.get("reytech_quote_number") or
        rfq_data.get("status") in ("generated", "sent")
    )
    if not has_files:
        result["errors"].append("No generated quote PDF — generate first")
        result["ok"] = False

    if not rfq_data.get("reytech_quote_number"):
        result["warnings"].append("No Reytech quote number assigned")

    return result


# ── System 3: Status Flow Enforcement ─────────────────────────────────────

VALID_TRANSITIONS = {
    "new":       ["draft", "parsed", "dismissed"],
    "draft":     ["parsed", "priced", "dismissed"],
    "parsed":    ["draft", "priced", "auto_priced", "dismissed"],
    "auto_priced": ["priced", "ready", "dismissed"],
    "priced":    ["ready", "generated", "draft", "dismissed"],
    "ready":     ["generated", "priced", "draft", "dismissed"],
    "generated": ["sent", "ready", "priced", "draft"],
    "sent":      ["won", "lost", "expired", "generated"],
    "won":       ["sent"],
    "lost":      ["sent"],
    "expired":   ["draft", "sent"],
    "dismissed": ["draft", "new"],
}


def validate_transition(current_status, new_status):
    """Check if a status transition is valid.
    Returns {"ok": bool, "error": str}
    """
    current = (current_status or "new").lower()
    new = (new_status or "").lower()

    allowed = VALID_TRANSITIONS.get(current, [])
    if new in allowed:
        return {"ok": True}

    return {
        "ok": False,
        "error": f"Cannot go from '{current}' to '{new}'. "
                f"Allowed: {', '.join(allowed)}",
        "current": current,
        "requested": new,
        "allowed": allowed,
    }


# ── System 5: Quote Complete Checklist ────────────────────────────────────

def get_completion_checklist(rfq_data):
    """Complete checklist for quote readiness."""
    checks = []
    items = rfq_data.get("line_items", rfq_data.get("items", []))

    has_items = len(items) > 0
    checks.append({
        "label": "Line items loaded",
        "ok": has_items,
        "detail": f"{len(items)} items" if has_items else "No items",
    })

    described = sum(1 for i in items if (i.get("description", i.get("desc", "")) or "").strip())
    checks.append({
        "label": "All items have descriptions",
        "ok": described == len(items) and len(items) > 0,
        "detail": f"{described}/{len(items)}",
    })

    costed = 0
    for i in items:
        c = i.get("supplier_cost", i.get("cost", i.get("unit_cost", 0)))
        try:
            if float(str(c or 0).replace("$", "").replace(",", "")) > 0:
                costed += 1
        except (ValueError, TypeError):
            pass
    checks.append({
        "label": "All items have supplier cost",
        "ok": costed == len(items) and len(items) > 0,
        "detail": f"{costed}/{len(items)}",
        "severity": "error" if costed < len(items) else "ok",
    })

    priced = 0
    for i in items:
        p = i.get("price_per_unit", i.get("bid_price", i.get("unit_price", 0)))
        try:
            if float(str(p or 0).replace("$", "").replace(",", "")) > 0:
                priced += 1
        except (ValueError, TypeError):
            pass
    checks.append({
        "label": "All items have bid price",
        "ok": priced == len(items) and len(items) > 0,
        "detail": f"{priced}/{len(items)}",
        "severity": "error" if priced < len(items) else "ok",
    })

    has_email = bool(rfq_data.get("requestor_email"))
    checks.append({
        "label": "Buyer email address",
        "ok": has_email,
        "detail": rfq_data.get("requestor_email", "Missing"),
    })

    has_name = bool(rfq_data.get("requestor_name"))
    checks.append({
        "label": "Buyer name",
        "ok": has_name,
        "detail": rfq_data.get("requestor_name", "Missing"),
        "severity": "warning" if not has_name else "ok",
    })

    templates = rfq_data.get("templates", {})
    has_templates = bool(templates.get("703b") or templates.get("704b") or templates.get("bidpkg"))
    checks.append({
        "label": "Form templates uploaded",
        "ok": has_templates,
        "detail": f"{len([k for k, v in templates.items() if v])} templates" if has_templates else "None",
        "severity": "warning" if not has_templates else "ok",
    })

    has_quote = bool(rfq_data.get("output_files"))
    checks.append({
        "label": "Quote PDF generated",
        "ok": has_quote,
        "detail": f"{len(rfq_data.get('output_files', []))} files" if has_quote else "Not generated",
    })

    has_pc = bool(rfq_data.get("linked_pc_id") or rfq_data.get("source_pc"))
    checks.append({
        "label": "Linked to Price Check",
        "ok": has_pc,
        "detail": rfq_data.get("linked_pc_number", rfq_data.get("linked_pc_id", "Not linked")),
        "severity": "info" if not has_pc else "ok",
    })

    passed = sum(1 for c in checks if c["ok"])
    total = len(checks)
    score = int(passed / total * 100) if total > 0 else 0

    return {
        "checks": checks,
        "passed": passed,
        "total": total,
        "score": score,
        "ready_to_generate": all(
            c["ok"] for c in checks
            if c.get("severity") == "error" or c["label"] in ["Line items loaded", "All items have bid price"]
        ),
        "ready_to_send": score >= 80 and has_email and has_quote,
    }
