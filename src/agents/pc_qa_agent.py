"""
PC Quality Assurance Agent — validates every field, price, and match before send.

Runs two layers:
1. DETERMINISTIC: math checks, completeness, threshold violations (instant, free)
2. LLM (Grok): product match verification, price reasonableness, catch-all (costs tokens)

Returns a structured report with issues ranked by severity (blocker/warning/info).
Goal: human reviews ONLY flagged items instead of checking every line.
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


def run_qa(pc: dict, use_llm: bool = True) -> dict:
    """
    Run full QA/QC on a Price Check. Returns structured report.

    Args:
        pc: Full PC dict with items, pricing, metadata
        use_llm: Whether to call Grok for judgment-based checks

    Returns:
        {
            "ok": True,
            "pass": bool (True if 0 blockers),
            "score": int (0-100),
            "summary": "3 blockers, 2 warnings, 5 info",
            "issues": [{severity, item_index, field, message, value, expected}],
            "item_scores": [{idx, score, issues_count, status}],
            "totals_check": {subtotal, tax, total, correct: bool},
            "tokens_used": int,
        }
    """
    items = pc.get("items") or []
    issues = []
    tokens_used = 0

    if not items:
        return {"ok": True, "pass": False, "score": 0,
                "summary": "No items in PC", "issues": [{"severity": BLOCKER,
                "item_index": -1, "field": "items", "message": "PC has no line items"}],
                "item_scores": [], "totals_check": {}, "tokens_used": 0}

    # ═══ LAYER 1: DETERMINISTIC CHECKS (free, instant) ═══════════════════

    for idx, item in enumerate(items):
        p = item.get("pricing") or {}
        _issues = _check_item_deterministic(idx, item, p, pc)
        issues.extend(_issues)

    # ── PC-level checks ──
    issues.extend(_check_pc_level(pc, items))

    # ── Totals verification ──
    totals = _verify_totals(pc, items)
    if not totals.get("correct"):
        issues.append({
            "severity": WARNING, "item_index": -1, "field": "totals",
            "message": f"Total mismatch: calculated ${totals.get('calculated_total', 0):.2f} vs displayed ${totals.get('displayed_total', 0):.2f}",
            "value": totals.get("displayed_total"), "expected": totals.get("calculated_total"),
        })

    # ═══ LAYER 2: LLM JUDGMENT (Grok — costs tokens) ═════════════════════

    if use_llm:
        llm_issues, _tokens = _check_llm(pc, items)
        issues.extend(llm_issues)
        tokens_used = _tokens

    # ═══ COMPILE REPORT ═══════════════════════════════════════════════════

    blockers = [i for i in issues if i["severity"] == BLOCKER]
    warnings = [i for i in issues if i["severity"] == WARNING]
    infos = [i for i in issues if i["severity"] == INFO]

    # Score: start at 100, deduct per issue
    score = 100
    score -= len(blockers) * 15
    score -= len(warnings) * 5
    score -= len(infos) * 1
    score = max(0, min(100, score))

    # Per-item scores
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
    summary = f"{len(blockers)} blocker{'s' if len(blockers) != 1 else ''}, {len(warnings)} warning{'s' if len(warnings) != 1 else ''}, {len(infos)} info"

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


# ─── Deterministic item checks ─────────────────────────────────────────────

def _check_item_deterministic(idx: int, item: dict, p: dict, pc: dict) -> list:
    """Check a single item for completeness, math, and threshold violations."""
    issues = []
    no_bid = item.get("no_bid", False)
    if no_bid:
        return issues  # No-bid items skip all checks

    desc = (item.get("description") or "").strip()
    qty = item.get("qty", 0) or 0
    uom = (item.get("uom") or "").strip()
    cost = float(p.get("unit_cost") or item.get("vendor_cost") or 0)
    price = float(item.get("unit_price") or p.get("recommended_price") or 0)
    markup = float(p.get("markup_pct") or item.get("markup_pct") or 0)
    mfg = (item.get("mfg_number") or "").strip()
    link = (item.get("item_link") or "").strip()

    # ── Completeness ──
    if not desc or len(desc) < 3:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "description",
                        "message": "Missing or empty description"})

    if qty <= 0:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "qty",
                        "message": "Quantity is 0 or missing", "value": qty})

    if not uom:
        issues.append({"severity": WARNING, "item_index": idx, "field": "uom",
                        "message": "UOM is empty"})

    if cost <= 0:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "cost",
                        "message": "No cost — item cannot be priced", "value": cost})

    if price <= 0 and cost > 0:
        issues.append({"severity": BLOCKER, "item_index": idx, "field": "price",
                        "message": "Cost exists but no sell price set", "value": price})

    # ── Math checks ──
    if cost > 0 and price > 0:
        expected_price = round(cost * (1 + markup / 100), 2)
        if abs(expected_price - price) > 0.02:
            issues.append({"severity": INFO, "item_index": idx, "field": "price",
                            "message": f"Price ${price:.2f} doesn't match cost ${cost:.2f} x {markup}% markup (expected ${expected_price:.2f})",
                            "value": price, "expected": expected_price})

        # Extension check: only flag if Reytech has explicitly set a calculated extension
        # that doesn't match price * qty. The item["extension"] field is the BUYER's
        # original value from the parsed form — it's expected to differ from our pricing.
        _reytech_ext = float(p.get("calculated_extension") or 0)
        if _reytech_ext > 0:
            extension = round(price * qty, 2)
            if abs(extension - _reytech_ext) > 0.05:
                issues.append({"severity": WARNING, "item_index": idx, "field": "extension",
                                "message": f"Extension mismatch: {qty} x ${price:.2f} = ${extension:.2f}, shown ${_reytech_ext:.2f}",
                                "value": _reytech_ext, "expected": extension})

    # ── Margin checks ──
    if cost > 0 and price > 0:
        margin = (price - cost) / price * 100
        if margin < 0:
            issues.append({"severity": BLOCKER, "item_index": idx, "field": "margin",
                            "message": f"NEGATIVE margin: selling at ${price:.2f} below cost ${cost:.2f}",
                            "value": f"{margin:.1f}%"})
        elif margin < 5:
            issues.append({"severity": WARNING, "item_index": idx, "field": "margin",
                            "message": f"Very low margin: {margin:.1f}% (cost ${cost:.2f}, price ${price:.2f})",
                            "value": f"{margin:.1f}%"})
        elif markup > 200:
            issues.append({"severity": WARNING, "item_index": idx, "field": "markup",
                            "message": f"Extremely high markup: {markup:.0f}% — likely bad cost data",
                            "value": f"{markup:.0f}%"})

    # ── SCPRS ceiling check ──
    scprs = float(p.get("scprs_price") or 0)
    if scprs > 0 and price > scprs * 1.05:
        issues.append({"severity": WARNING, "item_index": idx, "field": "scprs_ceiling",
                        "message": f"Price ${price:.2f} exceeds SCPRS ceiling ${scprs:.2f} — buyer will reject",
                        "value": price, "expected": f"<= ${scprs:.2f}"})

    # ── Identity checks ──
    if mfg and re.match(r'^B0[A-Z0-9]{8}$', mfg):
        issues.append({"severity": WARNING, "item_index": idx, "field": "mfg_number",
                        "message": f"MFG# looks like an ASIN ({mfg}) — should be real part number",
                        "value": mfg})

    # ── Match confidence ──
    cat_conf = float(p.get("catalog_confidence") or 0)
    scprs_conf = float(p.get("scprs_confidence") or 0)
    best_conf = max(cat_conf, scprs_conf)
    if cost > 0 and best_conf > 0 and best_conf < 0.60:
        issues.append({"severity": WARNING, "item_index": idx, "field": "match_confidence",
                        "message": f"Low match confidence ({best_conf:.0%}) — may be wrong product",
                        "value": f"{best_conf:.0%}"})

    # ── Missing link for priced items ──
    if cost > 0 and price > 0 and not link:
        issues.append({"severity": INFO, "item_index": idx, "field": "item_link",
                        "message": "No supplier URL — ordering will require manual lookup"})

    return issues


# ─── PC-level checks ───────────────────────────────────────────────────────

def _check_pc_level(pc: dict, items: list) -> list:
    """Check PC-wide fields: solicitation#, dates, requestor."""
    issues = []

    if not pc.get("pc_number") and not pc.get("solicitation_number"):
        issues.append({"severity": WARNING, "item_index": -1, "field": "pc_number",
                        "message": "No PC number or solicitation number"})

    if not pc.get("due_date"):
        issues.append({"severity": INFO, "item_index": -1, "field": "due_date",
                        "message": "No due date set"})

    # Check for zero-priced items (excluding no-bid)
    active_items = [it for it in items if not it.get("no_bid")]
    unpriced = sum(1 for it in active_items
                   if not (it.get("unit_price") or (it.get("pricing") or {}).get("recommended_price")))
    if unpriced > 0:
        issues.append({"severity": BLOCKER, "item_index": -1, "field": "unpriced_items",
                        "message": f"{unpriced} of {len(active_items)} items have no price"})

    # Check for duplicate items (same description)
    descs = [it.get("description", "").strip().lower() for it in active_items]
    seen = {}
    for i, d in enumerate(descs):
        if d in seen and len(d) > 10:
            issues.append({"severity": WARNING, "item_index": i, "field": "duplicate",
                            "message": f"Possible duplicate of item {seen[d] + 1}"})
        seen[d] = i

    return issues


# ─── Totals verification ──────────────────────────────────────────────────

def _verify_totals(pc: dict, items: list) -> dict:
    """Recalculate totals from items and compare to stored values."""
    calc_subtotal = 0
    for it in items:
        if it.get("no_bid"):
            continue
        p = it.get("pricing") or {}
        price = float(it.get("unit_price") or p.get("recommended_price") or 0)
        qty = it.get("qty", 0) or 0
        calc_subtotal += price * qty

    calc_subtotal = round(calc_subtotal, 2)
    stored = pc.get("profit_summary") or {}
    displayed_total = float(stored.get("total_bid") or 0)

    return {
        "calculated_total": calc_subtotal,
        "displayed_total": displayed_total,
        "correct": abs(calc_subtotal - displayed_total) < 1.0 if displayed_total > 0 else True,
    }


# ─── LLM checks (Grok) ────────────────────────────────────────────────────

def _check_llm(pc: dict, items: list) -> tuple:
    """Use Grok to verify product matches and price reasonableness.

    Returns (issues_list, tokens_used).
    """
    try:
        import requests
    except ImportError:
        return [], 0

    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        return [], 0

    # Build compact item summary for Grok (minimize tokens)
    item_lines = []
    items_to_check = []
    for idx, it in enumerate(items):
        if it.get("no_bid"):
            continue
        p = it.get("pricing") or {}
        desc = it.get("description", "")[:80]
        cost = float(p.get("unit_cost") or it.get("vendor_cost") or 0)
        price = float(it.get("unit_price") or p.get("recommended_price") or 0)
        mfg = it.get("mfg_number", "")
        upc = it.get("upc", "")
        qty = it.get("qty", 0)
        uom = it.get("uom", "")
        cat_conf = float(p.get("catalog_confidence") or 0)
        matched_to = p.get("amazon_title") or p.get("catalog_match") or p.get("llm_product_name") or ""

        line = f"#{idx+1}: {desc}"
        if upc:
            line += f" [UPC:{upc}]"
        if mfg:
            line += f" [MFG:{mfg}]"
        line += f" | Qty:{qty} {uom} | Cost:${cost:.2f} | Price:${price:.2f}"
        if matched_to:
            line += f" | Matched:'{matched_to[:50]}' ({cat_conf:.0%})"
        item_lines.append(line)
        items_to_check.append(idx)

    if not item_lines:
        return [], 0

    prompt = f"""You are a procurement QA auditor for a California government reseller. Review this Price Check quote for errors.

ITEMS:
{chr(10).join(item_lines)}

For EACH item, check:
1. Does the matched product (if any) actually match the buyer's description?
2. Is the cost reasonable for this product? (too high = wrong product, too low = suspicious)
3. Is the sell price competitive but profitable?
4. Any red flags? (wrong UOM, impossible quantities, price for wrong pack size)

Respond in JSON array format. Only include items with issues:
[{{"item": 1, "severity": "warning", "issue": "brief description"}}]

If everything looks correct, respond: []"""

    try:
        resp = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "grok-3-mini",
                "messages": [
                    {"role": "system", "content": "You are a procurement quality auditor. Respond with JSON only."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 800,
            },
            timeout=25,
        )
        if resp.status_code != 200:
            log.warning("QA Grok API %d: %s", resp.status_code, resp.text[:200])
            return [], 0

        data = resp.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        tokens = data.get("usage", {}).get("total_tokens", 0)

        # Parse JSON
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[-1]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()

        try:
            llm_issues_raw = json.loads(content)
        except json.JSONDecodeError:
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if json_match:
                llm_issues_raw = json.loads(json_match.group())
            else:
                return [], tokens

        if not isinstance(llm_issues_raw, list):
            return [], tokens

        # Convert to standard format
        issues = []
        for li in llm_issues_raw:
            item_num = li.get("item", 0)
            # Convert 1-based to 0-based index
            item_idx = item_num - 1 if item_num > 0 else -1
            severity = li.get("severity", "warning").lower()
            if severity not in (BLOCKER, WARNING, INFO):
                severity = WARNING
            issues.append({
                "severity": severity,
                "item_index": item_idx,
                "field": "llm_review",
                "message": f"[AI] {li.get('issue', 'Review needed')}",
            })

        log.info("QA Grok: %d issues found, %d tokens", len(issues), tokens)
        return issues, tokens

    except Exception as e:
        log.error("QA Grok error: %s", e)
        return [], 0
