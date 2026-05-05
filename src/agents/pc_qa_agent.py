"""
PC Quality Assurance Agent — final document-readiness check before send.

By the time Save & Generate fires, pricing has already been validated through:
1. Auto-pricing pipeline (catalog, SCPRS, Amazon, Oracle, Grok)
2. Manual review/editing by the user
3. Markup tier selection
4. Pre-flight client-side validation

This QA agent does NOT re-audit pricing decisions. It checks:
- MATH: does the arithmetic add up?
- COMPLETENESS: all fields filled, all items accounted for?
- IDENTITY: MFG#s and item numbers correct format?
- SHIPPING/AGENCY: ship-to, delivery, agency info present?
- SPELLING: LLM check for typos/truncation in descriptions
- DUPLICATES: same description appearing twice?

Returns a structured report with issues ranked by severity.
"""

import logging
import os
import json
import re

log = logging.getLogger("pc_qa_agent")

# ─── Severity levels ───────────────────────────────────────────────────────
BLOCKER = "blocker"   # Cannot send — must fix
WARNING = "warning"   # Should review — likely wrong
INFO = "info"         # FYI — might be intentional

# ─── Categories ────────────────────────────────────────────────────────────
CAT_MATH = "math"
CAT_COMPLETE = "completeness"
CAT_IDENTITY = "identity"
CAT_SHIPPING = "shipping"
CAT_AGENCY = "agency"
CAT_SPELLING = "spelling"
CAT_DUPLICATE = "duplicate"
CAT_REQUIREMENTS = "requirements"  # Email-as-contract gaps


def _sell_price(item: dict, pricing: dict | None = None) -> float:
    """Resolve an item's sell price across PC and RFQ shapes.

    PC items store the operator's entered price in `unit_price`. RFQ items
    store it in `bid_price` (see routes_pricecheck_admin.py comment:
    "PC items use 'unit_price', RFQ uses 'final_price'/'bid_price'").
    Falling through to `pricing.recommended_price` catches auto-priced
    items whose operator value hasn't been persisted yet.

    Final fallback: derive from `cost × (1 + markup/100)` when both cost
    and markup are persisted but no price field is. Matches what the UI
    renders live in JS as Bid Price. Without this, a row that was catalog-
    priced (cost set) and operator-marked-up (markup set) but whose
    autosave debounce hadn't yet fired the price field shows a $344 bid
    in the DOM while QA reads `price=0` and fires the false-positive
    "Cost exists but no sell price set" blocker (incident 2026-05-04,
    Mike P0 mid-quote on PC AUTO_177b18e6).

    Until this helper was added, every QA price read in this module was
    PC-only — which silently reported 0 for every RFQ, disabling Generate
    Package on every priced RFQ.
    """
    p = pricing if pricing is not None else (item.get("pricing") or {})
    persisted = float(
        item.get("bid_price")
        or item.get("unit_price")
        or item.get("price_per_unit")
        or p.get("recommended_price")
        or 0
    )
    if persisted > 0:
        return persisted
    # Fallback: cost × markup, matching what the UI shows as Bid Price.
    try:
        cost = float(p.get("unit_cost") or item.get("vendor_cost") or 0)
        markup = float(p.get("markup_pct") or item.get("markup_pct") or 0)
    except (TypeError, ValueError):
        return 0.0
    if cost > 0 and markup > 0:
        return round(cost * (1 + markup / 100.0), 2)
    return 0.0


def run_qa(pc: dict, use_llm: bool = True) -> dict:
    """
    Run document-readiness QA on a Price Check. Returns structured report.
    """
    items = pc.get("items") or []
    issues = []
    tokens_used = 0

    if not items:
        return {"ok": True, "pass": False, "score": 0,
                "summary": "No items in PC",
                "issues": [{"severity": BLOCKER, "item_index": -1,
                            "field": "items", "category": CAT_COMPLETE,
                            "message": "PC has no line items"}],
                "item_scores": [], "totals_check": {}, "tokens_used": 0}

    # ═══ ITEM-LEVEL CHECKS ═══════════════════════════════════════════════

    for idx, item in enumerate(items):
        p = item.get("pricing") or {}
        issues.extend(_check_math(idx, item, p))
        issues.extend(_check_completeness(idx, item, p))
        issues.extend(_check_identity(idx, item, p))

    # ═══ PC-LEVEL CHECKS ═════════════════════════════════════════════════

    issues.extend(_check_shipping(pc))
    issues.extend(_check_agency(pc, items))
    issues.extend(_check_duplicates(items))
    issues.extend(_check_requirements(pc))
    totals = _verify_totals(pc, items)
    if not totals.get("correct"):
        issues.append({
            "severity": BLOCKER, "item_index": -1, "field": "totals",
            "category": CAT_MATH,
            "message": f"Total mismatch: calculated ${totals.get('calculated_total', 0):.2f}"
                       f" vs displayed ${totals.get('displayed_total', 0):.2f}",
            "value": totals.get("displayed_total"),
            "expected": totals.get("calculated_total"),
        })

    # ═══ LLM SPELLING CHECK ══════════════════════════════════════════════

    if use_llm:
        llm_issues, _tokens = _check_spelling_llm(pc, items)
        issues.extend(llm_issues)
        tokens_used = _tokens

    # ═══ COMPILE REPORT ══════════════════════════════════════════════════

    blockers = [i for i in issues if i["severity"] == BLOCKER]
    warnings = [i for i in issues if i["severity"] == WARNING]
    infos = [i for i in issues if i["severity"] == INFO]

    score = 100
    score -= len(blockers) * 15
    score -= len(warnings) * 5
    score -= len(infos) * 1
    score = max(0, min(100, score))

    item_scores = []
    for idx in range(len(items)):
        item_issues = [i for i in issues if i.get("item_index") == idx]
        item_blockers = sum(1 for i in item_issues if i["severity"] == BLOCKER)
        item_warnings = sum(1 for i in item_issues if i["severity"] == WARNING)
        status = "fail" if item_blockers else ("review" if item_warnings else "pass")
        item_score = 100 - item_blockers * 15 - item_warnings * 5
        item_scores.append({
            "idx": idx, "score": max(0, item_score),
            "issues_count": len(item_issues), "status": status,
        })

    passed = len(blockers) == 0
    summary = (f"{len(blockers)} blocker{'s' if len(blockers) != 1 else ''}, "
               f"{len(warnings)} warning{'s' if len(warnings) != 1 else ''}, "
               f"{len(infos)} info")

    log.info("QA report: %s — score %d/100 — %s", pc.get("pc_number", "?"), score, summary)

    return {
        "ok": True,
        "pass": passed,
        "score": score,
        "summary": summary,
        "issues": issues,
        "item_scores": item_scores,
        "totals_check": totals,
        "tokens_used": tokens_used,
    }


# ─── MATH: does the arithmetic add up? ─────────────────────────────────────

def _check_math(idx: int, item: dict, p: dict) -> list:
    issues = []
    if item.get("no_bid"):
        return issues

    qty = float(item.get("qty", 0) or 0)
    cost = float(p.get("unit_cost") or item.get("vendor_cost") or 0)
    price = _sell_price(item, p)

    # Negative margin — selling below cost
    if cost > 0 and price > 0 and price < cost:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "margin",
                        "category": CAT_MATH,
                        "message": f"Selling below cost: price ${price:.2f} < cost ${cost:.2f}",
                        "value": price, "expected": f"> ${cost:.2f}"})

    # Extension math: price * qty should be consistent
    if price > 0 and qty > 0:
        calc_ext = round(price * qty, 2)
        stored_ext = float(p.get("calculated_extension") or item.get("bid_extension") or 0)
        if stored_ext > 0 and abs(calc_ext - stored_ext) > 0.05:
            issues.append({"severity": BLOCKER, "item_index": idx, "field": "extension",
                            "category": CAT_MATH,
                            "message": f"Extension math: {qty:.0f} x ${price:.2f} = ${calc_ext:.2f}, "
                                       f"but stored as ${stored_ext:.2f}",
                            "value": stored_ext, "expected": calc_ext})

    return issues


# ─── COMPLETENESS: all fields filled, all items accounted for? ──────────────

def _check_completeness(idx: int, item: dict, p: dict) -> list:
    issues = []
    if item.get("no_bid"):
        return issues

    desc = (item.get("description") or "").strip()
    qty = float(item.get("qty", 0) or 0)
    uom = (item.get("uom") or "").strip()
    cost = float(p.get("unit_cost") or item.get("vendor_cost") or 0)
    price = _sell_price(item, p)

    if not desc or len(desc) < 3:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "description",
                        "category": CAT_COMPLETE,
                        "message": "Missing or empty description"})

    if qty <= 0:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "qty",
                        "category": CAT_COMPLETE,
                        "message": "Quantity is 0 or missing", "value": qty})

    if not uom:
        issues.append({"severity": WARNING, "item_index": idx, "field": "uom",
                        "category": CAT_COMPLETE,
                        "message": "UOM is empty — buyer may question the quote"})

    if cost <= 0:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "cost",
                        "category": CAT_COMPLETE,
                        "message": "No cost — item cannot be priced", "value": cost})

    if price <= 0 and cost > 0:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "price",
                        "category": CAT_COMPLETE,
                        "message": "Cost exists but no sell price set", "value": price})

    return issues


# ─── IDENTITY: MFG#s and item numbers correct? ─────────────────────────────

def _check_identity(idx: int, item: dict, p: dict) -> list:
    issues = []
    if item.get("no_bid"):
        return issues

    mfg = (item.get("mfg_number") or "").strip()

    # ASIN used as MFG# (B0 + 8 alphanumeric)
    if mfg and re.match(r'^B0[A-Z0-9]{8}$', mfg):
        issues.append({"severity": WARNING, "item_index": idx, "field": "mfg_number",
                        "category": CAT_IDENTITY,
                        "message": f"MFG# looks like an ASIN ({mfg}) — should be real part number",
                        "value": mfg})

    # No MFG# at all for a priced item
    price = _sell_price(item, p)
    if price > 0 and not mfg:
        issues.append({"severity": INFO, "item_index": idx, "field": "mfg_number",
                        "category": CAT_IDENTITY,
                        "message": "No MFG#/part number — 704 will have an empty field"})

    return issues


# ─── SHIPPING: ship-to, delivery, FOB ──────────────────────────────────────

def _check_shipping(pc: dict) -> list:
    issues = []

    ship_to = (pc.get("ship_to") or "").strip()
    if not ship_to:
        issues.append({"severity": BLOCKER, "item_index": -1, "field": "ship_to",
                        "category": CAT_SHIPPING,
                        "message": "Ship-to address is empty — required for 704 form"})

    delivery = (pc.get("delivery_option") or pc.get("delivery") or pc.get("delivery_time") or "").strip()
    if not delivery:
        issues.append({"severity": WARNING, "item_index": -1, "field": "delivery",
                        "category": CAT_SHIPPING,
                        "message": "No delivery timeframe selected"})

    return issues


# ─── AGENCY: solicitation, requestor, institution, dates ───────────────────

def _check_agency(pc: dict, items: list) -> list:
    issues = []

    # Solicitation / PC number
    sol = (pc.get("solicitation_number") or pc.get("pc_number") or "").strip()
    if not sol:
        issues.append({"severity": WARNING, "item_index": -1, "field": "pc_number",
                        "category": CAT_AGENCY,
                        "message": "No PC number or solicitation number"})

    # Requestor / buyer name
    requestor = (pc.get("requestor") or "").strip()
    if not requestor:
        issues.append({"severity": WARNING, "item_index": -1, "field": "requestor",
                        "category": CAT_AGENCY,
                        "message": "Requestor/buyer name is empty"})

    # Institution
    institution = (pc.get("institution") or "").strip()
    if not institution or institution.lower() in ("unknown", "default", ""):
        issues.append({"severity": WARNING, "item_index": -1, "field": "institution",
                        "category": CAT_AGENCY,
                        "message": "Institution not resolved — may affect form selection"})

    # Due date
    due = (pc.get("due_date") or "").strip()
    if not due:
        issues.append({"severity": INFO, "item_index": -1, "field": "due_date",
                        "category": CAT_AGENCY,
                        "message": "No due date set"})
    else:
        # Check if due date is in the past
        try:
            from datetime import datetime, timezone
            # Handle various date formats
            for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
                try:
                    due_dt = datetime.strptime(due, fmt)
                    if due_dt.date() < datetime.now().date():
                        issues.append({"severity": WARNING, "item_index": -1,
                                        "field": "due_date", "category": CAT_AGENCY,
                                        "message": f"Due date {due} is in the past"})
                    break
                except ValueError:
                    continue
        except Exception as _e:
            log.debug("suppressed: %s", _e)

    # Quote number — PCs are buyer documents, not Reytech quotes.
    # Quote numbers are assigned later when generating a formal response.
    # For RFQs, the solicitation number from the email contract is used.

    # All items accounted for — check bid count vs total
    active = [it for it in items if not it.get("no_bid")]
    unpriced = sum(1 for it in active if not _sell_price(it))
    if unpriced > 0:
        issues.append({"severity": BLOCKER, "item_index": -1, "field": "unpriced_items",
                        "category": CAT_COMPLETE,
                        "message": f"{unpriced} of {len(active)} active items have no price"})

    return issues


# ─── REQUIREMENTS: email-as-contract gap check ────────────────────────────

def _check_requirements(pc: dict) -> list:
    """Validate the generated package against email-extracted requirements.

    The buyer's email is a contract. If they said "food items require OBS-1600
    certification" and the package lacks OBS-1600, that's a gap we must surface
    before send. This delegates to form_qa.validate_against_requirements() so
    the RFQ and PC sides stay aligned.

    Severity:
      - WARNING for every gap (missing_form, due_date mismatch, etc.)
      - INFO when requirements extraction confidence is low (<0.5) — we
        don't trust the gaps enough to warn on them
      - Skipped silently when no requirements have been extracted
    """
    issues = []
    req_json = pc.get("requirements_json", "") or "{}"
    if not req_json or req_json == "{}":
        return issues

    try:
        from src.forms.form_qa import validate_against_requirements
    except Exception as e:
        log.debug("form_qa unavailable — skipping requirements check: %s", e)
        return issues

    # Generated files for this PC — may be empty if user hasn't generated yet.
    # We still run the check so the user sees the requirements up front.
    generated = pc.get("output_files") or []
    if not generated:
        _out = pc.get("output_pdf", "") or ""
        if _out:
            generated = [os.path.basename(_out)]

    try:
        result = validate_against_requirements(generated, req_json, pc)
    except Exception as e:
        log.debug("validate_against_requirements error: %s", e)
        return issues

    gaps = result.get("gaps") or []
    req_confidence = float(result.get("confidence") or 0)
    # Low-confidence extractions should advise, not warn — the extractor may
    # have hallucinated requirements from noisy email text.
    severity = INFO if req_confidence < 0.5 else WARNING

    for gap in gaps:
        issues.append({
            "severity": severity,
            "item_index": -1,
            "field": gap.get("form_id") or gap.get("type") or "requirement",
            "category": CAT_REQUIREMENTS,
            "message": gap.get("msg") or "Requirement gap detected",
            "gap_type": gap.get("type", ""),
            "extraction_confidence": req_confidence,
        })

    if gaps:
        log.info("QA requirements: %d gap(s) vs email contract (conf=%.2f)",
                 len(gaps), req_confidence)

    return issues


# ─── DUPLICATES: same description appearing twice ──────────────────────────

def _check_duplicates(items: list) -> list:
    issues = []
    active = [it for it in items if not it.get("no_bid")]
    descs = [(it.get("description") or "").strip().lower() for it in active]
    seen = {}
    for i, d in enumerate(descs):
        if d in seen and len(d) > 10:
            issues.append({"severity": WARNING, "item_index": i, "field": "description",
                            "category": CAT_DUPLICATE,
                            "message": f"Possible duplicate of item #{seen[d] + 1}"})
        seen[d] = i
    return issues


# ─── TOTALS: recalculate and compare ──────────────────────────────────────

def _verify_totals(pc: dict, items: list) -> dict:
    calc_subtotal = 0
    for it in items:
        if it.get("no_bid"):
            continue
        p = it.get("pricing") or {}
        price = _sell_price(it, p)
        qty = float(it.get("qty", 0) or 0)
        calc_subtotal += price * qty

    calc_subtotal = round(calc_subtotal, 2)
    stored = pc.get("profit_summary") or {}
    displayed_total = float(stored.get("total_revenue") or stored.get("total_bid") or 0)

    return {
        "calculated_total": calc_subtotal,
        "displayed_total": displayed_total,
        "correct": abs(calc_subtotal - displayed_total) < 1.0 if displayed_total > 0 else True,
    }


# ─── LLM SPELLING CHECK ──────────────────────────────────────────────────

def _check_spelling_llm(pc: dict, items: list) -> tuple:
    """Use Grok to check descriptions for typos, truncation, or garbled text.

    Returns (issues_list, tokens_used).
    """
    try:
        import requests
    except ImportError:
        return [], 0

    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        return [], 0

    # Build compact list of descriptions to check
    desc_lines = []
    for idx, it in enumerate(items):
        if it.get("no_bid"):
            continue
        desc = (it.get("description") or "").strip()
        if not desc:
            continue
        mfg = it.get("mfg_number", "")
        desc_lines.append(f"#{idx + 1}: {desc}" + (f" [MFG:{mfg}]" if mfg else ""))

    if not desc_lines:
        return [], 0

    prompt = f"""You are a document proofreader for government procurement quotes.
Check these item descriptions for:
1. Spelling errors or typos
2. Truncated/cut-off descriptions that look incomplete
3. Garbled text (encoding issues, random characters)
4. Obvious wrong product names or nonsensical descriptions

DO NOT check pricing, quantities, or business logic. Only text quality.

ITEMS:
{chr(10).join(desc_lines)}

Respond with a JSON array. Only include items with actual text problems:
[{{"item": 1, "issue": "brief description of text problem"}}]

If all descriptions look fine, respond: []"""

    try:
        resp = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}",
                     "Content-Type": "application/json"},
            json={
                "model": "grok-3-mini",
                "messages": [
                    {"role": "system",
                     "content": "You are a proofreader. Respond with JSON only. "
                                "Be strict about real typos but ignore product codes, "
                                "abbreviations (EA, PK, BX), and industry shorthand."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 500,
                "response_format": {"type": "json_object"},
            },
            timeout=20,
        )
        if resp.status_code != 200:
            log.warning("QA spelling Grok API %d: %s", resp.status_code, resp.text[:200])
            return [], 0

        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        tokens = data.get("usage", {}).get("total_tokens", 0)

        # Parse JSON response
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[-1]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()

        try:
            spell_issues = json.loads(content)
        except json.JSONDecodeError:
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if json_match:
                spell_issues = json.loads(json_match.group())
            else:
                return [], tokens

        if not isinstance(spell_issues, list):
            return [], tokens

        issues = []
        for si in spell_issues:
            item_num = si.get("item", 0)
            item_idx = item_num - 1 if item_num > 0 else -1
            issues.append({
                "severity": WARNING,
                "item_index": item_idx,
                "field": "description",
                "category": CAT_SPELLING,
                "message": f"Spelling: {si.get('issue', 'text issue')}",
            })

        log.info("QA spelling: %d issues found, %d tokens", len(issues), tokens)
        return issues, tokens

    except Exception as e:
        log.error("QA spelling Grok error: %s", e)
        return [], 0
