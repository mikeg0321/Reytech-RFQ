"""
auto_processor.py — Autonomous Bid Processing Engine for Reytech
Phase 7 | Version: 7.0

The brain that makes competitors look prehistoric.

This module orchestrates the full autonomous pipeline:
  Email → Detect type → Parse → Price research → Confidence score → 
  Generate response → Draft email → Queue for approval (or auto-send)

Supports: RFQs (704A/B + Bid Package) and Price Checks (AMS 704)

Key Features:
  - Auto-detection of document type (RFQ vs Price Check)
  - Parallel price research (SCPRS + Amazon)  
  - Confidence scoring per item and per quote
  - Response time tracking
  - Auto-email drafting with PDF attachments
  - Audit trail for every decision
"""

import json
import os
import re
import time
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger("autoprocessor")

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# ─── Import available modules ────────────────────────────────────────────────

try:
    from product_research import research_product, quick_lookup
    HAS_RESEARCH = True
except ImportError:
    HAS_RESEARCH = False

try:
    from pricing_oracle import recommend_price
    HAS_ORACLE = True
except ImportError:
    HAS_ORACLE = False

try:
    from won_quotes_db import find_similar_items, ingest_scprs_result
    HAS_WON_QUOTES = True
except ImportError:
    HAS_WON_QUOTES = False

try:
    from price_check import parse_ams704, fill_ams704, lookup_prices
    HAS_PRICE_CHECK = True
except ImportError:
    HAS_PRICE_CHECK = False


# ─── Processing Status ───────────────────────────────────────────────────────

PROCESSOR_STATUS = {
    "running": False,
    "current_job": None,
    "jobs_completed": 0,
    "jobs_failed": 0,
    "avg_response_time_sec": 0,
    "last_run": None,
}

# Audit log: persistent record of every auto-processing run
AUDIT_LOG_FILE = os.path.join(DATA_DIR, "auto_process_audit.json")


# ─── Document Type Detection ─────────────────────────────────────────────────

def detect_document_type(pdf_path: str) -> dict:
    """
    Detect whether a PDF is an RFQ, Price Check, or unknown.

    Returns:
        {
            "type": "price_check" | "rfq" | "unknown",
            "confidence": float (0-1),
            "signals": [str],  # what triggered detection
        }
    """
    signals = []
    scores = {"price_check": 0, "rfq": 0}

    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages[:2]:  # Check first 2 pages
            text += (page.extract_text() or "")
        text_lower = text.lower()

        # Price Check signals
        if "price check" in text_lower:
            scores["price_check"] += 3
            signals.append("text: 'price check' found")
        if "ams 704" in text_lower or "ams704" in text_lower:
            scores["price_check"] += 2
            signals.append("text: 'AMS 704' found")
        if "worksheet" in text_lower and "goods" in text_lower:
            scores["price_check"] += 2
            signals.append("text: 'worksheet' + 'goods' found")
        if "price per unit" in text_lower and "extension" in text_lower:
            scores["price_check"] += 1
            signals.append("text: price table headers found")

        # RFQ signals
        if "solicitation" in text_lower:
            scores["rfq"] += 2
            signals.append("text: 'solicitation' found")
        if "703b" in text_lower or "703-b" in text_lower:
            scores["rfq"] += 3
            signals.append("text: '703B' found")
        if "bid package" in text_lower:
            scores["rfq"] += 2
            signals.append("text: 'bid package' found")
        if "invitation for bid" in text_lower or "request for quote" in text_lower:
            scores["rfq"] += 2
            signals.append("text: RFQ/IFB language found")

        # Check form fields
        fields = reader.get_fields()
        if fields:
            field_names = set(fields.keys())
            pc_markers = {"PRICE PER UNITRow1", "EXTENSIONRow1", "COMPANY NAME", "Requestor"}
            rfq_markers = {"solicitation_number", "Solicitation", "award_method"}
            
            pc_hits = len(pc_markers & field_names)
            rfq_hits = len(rfq_markers & field_names)
            
            if pc_hits >= 3:
                scores["price_check"] += 3
                signals.append(f"fields: {pc_hits} AMS 704 fields found")
            if rfq_hits >= 2:
                scores["rfq"] += 3
                signals.append(f"fields: {rfq_hits} RFQ fields found")

        # Check filename
        fname = os.path.basename(pdf_path).lower()
        if "704" in fname and ("ams" in fname or "price" in fname or "pc" in fname or "den" in fname):
            scores["price_check"] += 1
            signals.append(f"filename: looks like Price Check")
        if "703" in fname or "bid" in fname or "rfq" in fname or "sol" in fname:
            scores["rfq"] += 1
            signals.append(f"filename: looks like RFQ")

    except Exception as e:
        signals.append(f"error: {e}")

    # Determine winner
    if scores["price_check"] > scores["rfq"] and scores["price_check"] >= 3:
        conf = min(1.0, scores["price_check"] / 8)
        return {"type": "price_check", "confidence": round(conf, 2), "signals": signals}
    elif scores["rfq"] > scores["price_check"] and scores["rfq"] >= 3:
        conf = min(1.0, scores["rfq"] / 8)
        return {"type": "rfq", "confidence": round(conf, 2), "signals": signals}
    else:
        return {"type": "unknown", "confidence": 0, "signals": signals}


# ─── Confidence Scoring ──────────────────────────────────────────────────────

def score_item_confidence(item: dict) -> dict:
    """
    Score confidence for a single line item's pricing.
    
    Returns:
        {
            "score": float (0-1),
            "grade": "A" | "B" | "C" | "F",
            "factors": {factor: score},
            "notes": [str],
        }
    """
    pricing = item.get("pricing", {})
    factors = {}
    notes = []

    # Factor 1: Do we have a supplier cost? (0 or 0.3)
    has_cost = bool(pricing.get("amazon_price") or pricing.get("unit_cost"))
    factors["has_cost"] = 0.3 if has_cost else 0
    if not has_cost:
        notes.append("No supplier cost found — manual entry needed")

    # Factor 2: Do we have SCPRS historical? (0 or 0.25)
    has_scprs = bool(pricing.get("scprs_price"))
    factors["has_scprs"] = 0.25 if has_scprs else 0
    if has_scprs:
        notes.append(f"SCPRS historical: ${pricing['scprs_price']:.2f}")

    # Factor 3: Is our price competitive vs SCPRS? (0-0.25)
    our_price = pricing.get("recommended_price", 0)
    scprs_price = pricing.get("scprs_price", 0)
    if our_price and scprs_price:
        ratio = our_price / scprs_price
        if ratio <= 0.98:  # Under SCPRS = very competitive
            factors["competitive"] = 0.25
            notes.append(f"Price is {(1-ratio)*100:.1f}% under SCPRS — strong position")
        elif ratio <= 1.05:  # Within 5% = acceptable
            factors["competitive"] = 0.15
            notes.append(f"Price is within 5% of SCPRS — competitive")
        elif ratio <= 1.15:  # Within 15% = risky
            factors["competitive"] = 0.05
            notes.append(f"Price is {(ratio-1)*100:.1f}% over SCPRS — consider lowering")
        else:
            factors["competitive"] = 0
            notes.append(f"⚠️ Price is {(ratio-1)*100:.1f}% over SCPRS — likely to lose")
    else:
        factors["competitive"] = 0.10  # Can't compare = moderate confidence

    # Factor 4: Amazon match quality (0-0.2)
    if pricing.get("amazon_title"):
        # Check if Amazon title matches description
        desc_words = set(item.get("description", "").lower().split())
        title_words = set(pricing["amazon_title"].lower().split())
        overlap = len(desc_words & title_words) / max(len(desc_words), 1)
        factors["match_quality"] = round(min(0.2, overlap * 0.3), 2)
        if overlap < 0.2:
            notes.append("Amazon match may not be exact — verify product")
    else:
        factors["match_quality"] = 0

    # Calculate total
    total = sum(factors.values())
    
    if total >= 0.8:
        grade = "A"
    elif total >= 0.6:
        grade = "B"  
    elif total >= 0.4:
        grade = "C"
    else:
        grade = "F"

    return {
        "score": round(total, 2),
        "grade": grade,
        "factors": factors,
        "notes": notes,
    }


def score_quote_confidence(items: list) -> dict:
    """
    Score overall confidence for a quote/price check.
    
    Returns:
        {
            "overall_score": float,
            "overall_grade": str,
            "items_scored": int,
            "grade_distribution": {"A": n, "B": n, "C": n, "F": n},
            "auto_send_eligible": bool,
            "recommendation": str,
        }
    """
    if not items:
        return {"overall_score": 0, "overall_grade": "F", "auto_send_eligible": False,
                "recommendation": "No items to score"}

    scores = []
    grades = {"A": 0, "B": 0, "C": 0, "F": 0}
    
    for item in items:
        item_score = score_item_confidence(item)
        item["confidence"] = item_score
        scores.append(item_score["score"])
        grades[item_score["grade"]] += 1

    overall = sum(scores) / len(scores) if scores else 0
    
    if overall >= 0.8:
        grade = "A"
    elif overall >= 0.6:
        grade = "B"
    elif overall >= 0.4:
        grade = "C"
    else:
        grade = "F"

    # Auto-send eligibility: all items must be B+ and overall A
    auto_eligible = grade == "A" and grades["C"] == 0 and grades["F"] == 0

    if auto_eligible:
        rec = "✅ High confidence — eligible for auto-send"
    elif grade in ("A", "B"):
        rec = "👍 Good confidence — review recommended before sending"
    elif grade == "C":
        rec = "⚠️ Moderate confidence — manual review required"
    else:
        rec = "❌ Low confidence — significant manual pricing needed"

    return {
        "overall_score": round(overall, 2),
        "overall_grade": grade,
        "items_scored": len(items),
        "grade_distribution": grades,
        "auto_send_eligible": auto_eligible,
        "recommendation": rec,
    }


# ─── Auto-Process Pipeline ──────────────────────────────────────────────────

def auto_process_price_check(pdf_path: str, pc_id: str = None) -> dict:
    """
    Full autonomous pipeline for a Price Check:
    Parse → SCPRS lookup → Amazon lookup → Score confidence → Generate PDF
    
    Returns dict with all results + timing.
    """
    start_time = time.time()
    result = {
        "type": "price_check",
        "pc_id": pc_id,
        "steps": [],
        "timing": {},
    }

    # Step 1: Parse
    t0 = time.time()
    if not HAS_PRICE_CHECK:
        result["error"] = "price_check module not available"
        return result
    
    parsed = parse_ams704(pdf_path)
    result["steps"].append({"step": "parse", "ok": not bool(parsed.get("error")),
                            "items": len(parsed.get("line_items", []))})
    result["timing"]["parse"] = round(time.time() - t0, 2)

    if parsed.get("error") or not parsed.get("line_items"):
        result["error"] = parsed.get("error", "No line items found")
        return result

    items = parsed["line_items"]

    # Step 2: SCPRS lookup
    t0 = time.time()
    scprs_found = 0
    if HAS_WON_QUOTES:
        for item in items:
            try:
                matches = find_similar_items(
                    item_number=item.get("item_number", ""),
                    description=item.get("description", ""),
                )
                if matches:
                    best = matches[0]
                    quote = best.get("quote", best)
                    if not item.get("pricing"):
                        item["pricing"] = {}
                    item["pricing"]["scprs_price"] = quote.get("unit_price")
                    item["pricing"]["scprs_confidence"] = best.get("match_confidence", 0)
                    scprs_found += 1
            except Exception as e:
                log.error(f"SCPRS error: {e}")
    result["steps"].append({"step": "scprs", "found": scprs_found, "total": len(items)})
    result["timing"]["scprs"] = round(time.time() - t0, 2)

    # Step 3: Amazon lookup
    t0 = time.time()
    amazon_found = 0
    if HAS_RESEARCH:
        for item in items:
            try:
                research = research_product(description=item.get("description", ""))
                if research.get("found"):
                    if not item.get("pricing"):
                        item["pricing"] = {}
                    item["pricing"]["amazon_price"] = research["price"]
                    item["pricing"]["amazon_title"] = research.get("title", "")
                    item["pricing"]["amazon_url"] = research.get("url", "")
                    item["pricing"]["price_source"] = "amazon"
                    amazon_found += 1
            except Exception as e:
                log.error(f"Amazon error: {e}")
            time.sleep(1.5)  # SerpApi rate limit
    result["steps"].append({"step": "amazon", "found": amazon_found, "total": len(items)})
    result["timing"]["amazon"] = round(time.time() - t0, 2)

    # Step 4: Calculate pricing (cost + 25% markup)
    t0 = time.time()
    priced = 0
    for item in items:
        p = item.get("pricing", {})
        cost = p.get("amazon_price") or p.get("scprs_price") or 0
        if cost > 0:
            markup = p.get("markup_pct", 25)
            p["unit_cost"] = cost
            p["markup_pct"] = markup
            p["recommended_price"] = round(cost * (1 + markup / 100), 2)
            item["pricing"] = p
            priced += 1
    result["steps"].append({"step": "pricing", "priced": priced, "total": len(items)})
    result["timing"]["pricing"] = round(time.time() - t0, 2)

    # Step 5: Confidence scoring
    t0 = time.time()
    confidence = score_quote_confidence(items)
    result["confidence"] = confidence
    result["steps"].append({"step": "confidence", "grade": confidence["overall_grade"],
                            "score": confidence["overall_score"]})
    result["timing"]["confidence"] = round(time.time() - t0, 2)

    # Step 6: Generate filled PDF
    t0 = time.time()
    pc_num = parsed.get("header", {}).get("price_check_number", "unknown")
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
    output_path = os.path.join(DATA_DIR, f"PC_{safe_name}_Reytech_.pdf")

    fill_result = fill_ams704(
        source_pdf=pdf_path,
        parsed_pc={"line_items": items, "header": parsed.get("header", {})},
        output_pdf=output_path,
        tax_rate=0.0,
    )
    result["steps"].append({"step": "generate_pdf", "ok": fill_result.get("ok", False)})
    result["timing"]["generate"] = round(time.time() - t0, 2)

    if fill_result.get("ok"):
        result["output_pdf"] = output_path

    # Total timing
    total_time = time.time() - start_time
    result["timing"]["total"] = round(total_time, 2)
    result["ok"] = fill_result.get("ok", False)
    result["summary"] = fill_result.get("summary", {})
    result["parsed"] = parsed

    # Draft email response
    result["draft_email"] = _draft_pc_response_email(parsed, items, confidence, output_path)

    # Audit log
    _log_audit(result)

    return result


def _draft_pc_response_email(parsed: dict, items: list, confidence: dict, pdf_path: str) -> dict:
    """Draft an email response for a completed Price Check."""
    header = parsed.get("header", {})
    requestor = header.get("requestor", "")
    pc_num = header.get("price_check_number", "")
    institution = header.get("institution", "")

    total_items = len(items)
    priced_items = sum(1 for i in items if i.get("pricing", {}).get("recommended_price"))
    
    # Calculate total
    total = sum(
        (i.get("pricing", {}).get("recommended_price", 0) or 0) * i.get("qty", 1)
        for i in items
    )

    subject = f"Price Check Response: {pc_num} — Reytech Inc."
    
    body = f"""Dear {requestor or 'Procurement Team'},

Please find attached our completed Price Check response for {pc_num}.

Summary:
- Items quoted: {priced_items}/{total_items}
- Total: ${total:,.2f}
- FOB Destination, Freight Prepaid
- Payment Terms: Net 45
- Delivery: 5-7 business days ARO

All pricing is valid for 30 days from the date of this response.

Please don't hesitate to contact us with any questions.

Best regards,
Rey
Reytech Inc.
"""

    return {
        "subject": subject,
        "body": body,
        "to": "",  # Will be filled from email sender
        "attachment": os.path.basename(pdf_path) if pdf_path else None,
        "confidence_grade": confidence.get("overall_grade", "F"),
    }


# ─── Response Time Tracking ─────────────────────────────────────────────────

def track_response_time(doc_type: str, received_at: str, responded_at: str = None) -> dict:
    """Track and analyze response times."""
    if not responded_at:
        responded_at = datetime.now(timezone.utc).isoformat()
    
    try:
        recv = datetime.fromisoformat(received_at)
        resp = datetime.fromisoformat(responded_at)
        delta = resp - recv
        minutes = delta.total_seconds() / 60
    except:
        minutes = 0

    # Load historical
    stats_file = os.path.join(DATA_DIR, "response_time_stats.json")
    stats = {"total": 0, "sum_minutes": 0, "fastest": 999999, "slowest": 0, "by_type": {}}
    if os.path.exists(stats_file):
        try:
            with open(stats_file) as f:
                stats = json.load(f)
        except:
            pass

    stats["total"] += 1
    stats["sum_minutes"] += minutes
    stats["fastest"] = min(stats["fastest"], minutes)
    stats["slowest"] = max(stats["slowest"], minutes)
    
    if doc_type not in stats["by_type"]:
        stats["by_type"][doc_type] = {"count": 0, "sum_minutes": 0}
    stats["by_type"][doc_type]["count"] += 1
    stats["by_type"][doc_type]["sum_minutes"] += minutes

    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)

    avg = stats["sum_minutes"] / stats["total"] if stats["total"] else 0
    return {
        "this_response_minutes": round(minutes, 1),
        "average_minutes": round(avg, 1),
        "fastest_minutes": round(stats["fastest"], 1),
        "total_responses": stats["total"],
    }


# ─── Audit Trail ─────────────────────────────────────────────────────────────

def _log_audit(result: dict):
    """Log every auto-processing run for compliance and debugging."""
    os.makedirs(DATA_DIR, exist_ok=True)
    
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "type": result.get("type"),
        "ok": result.get("ok", False),
        "timing": result.get("timing", {}),
        "confidence": result.get("confidence", {}).get("overall_grade"),
        "items": len(result.get("parsed", {}).get("line_items", [])),
        "steps": result.get("steps", []),
    }

    # Append to audit log
    audit = []
    if os.path.exists(AUDIT_LOG_FILE):
        try:
            with open(AUDIT_LOG_FILE) as f:
                audit = json.load(f)
        except:
            audit = []

    audit.append(entry)
    
    # Keep last 500 entries
    if len(audit) > 500:
        audit = audit[-500:]

    with open(AUDIT_LOG_FILE, "w") as f:
        json.dump(audit, f, indent=2, default=str)


def get_audit_stats() -> dict:
    """Get processing statistics from audit log."""
    if not os.path.exists(AUDIT_LOG_FILE):
        return {"total_runs": 0}
    
    try:
        with open(AUDIT_LOG_FILE) as f:
            audit = json.load(f)
    except:
        return {"total_runs": 0}

    if not audit:
        return {"total_runs": 0}

    ok = sum(1 for e in audit if e.get("ok"))
    grades = {}
    times = []
    for e in audit:
        g = e.get("confidence", "?")
        grades[g] = grades.get(g, 0) + 1
        t = e.get("timing", {}).get("total", 0)
        if t:
            times.append(t)

    return {
        "total_runs": len(audit),
        "successful": ok,
        "failed": len(audit) - ok,
        "success_rate": f"{ok/len(audit)*100:.0f}%" if audit else "0%",
        "avg_processing_seconds": round(sum(times)/len(times), 1) if times else 0,
        "confidence_grades": grades,
    }


# ─── Health Check ────────────────────────────────────────────────────────────

def system_health_check() -> dict:
    """Comprehensive health check of all system components."""
    health = {
        "status": "healthy",
        "components": {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Check modules
    health["components"]["product_research"] = {
        "available": HAS_RESEARCH,
        "status": "ok" if HAS_RESEARCH else "missing",
    }
    health["components"]["pricing_oracle"] = {
        "available": HAS_ORACLE,
        "status": "ok" if HAS_ORACLE else "missing",
    }
    health["components"]["won_quotes_db"] = {
        "available": HAS_WON_QUOTES,
        "status": "ok" if HAS_WON_QUOTES else "missing",
    }
    health["components"]["price_check"] = {
        "available": HAS_PRICE_CHECK,
        "status": "ok" if HAS_PRICE_CHECK else "missing",
    }

    # Check SerpApi key
    serpapi_key_file = os.path.join(DATA_DIR, ".serpapi_key")
    has_serpapi = os.path.exists(serpapi_key_file)
    health["components"]["serpapi"] = {
        "available": has_serpapi,
        "status": "ok" if has_serpapi else "no key configured",
    }

    # Check data directory
    health["components"]["data_volume"] = {
        "available": os.path.exists(DATA_DIR),
        "status": "ok" if os.path.exists(DATA_DIR) else "missing",
    }

    # Check Won Quotes KB
    kb_file = os.path.join(DATA_DIR, "won_quotes.json")
    if os.path.exists(kb_file):
        try:
            with open(kb_file) as f:
                kb = json.load(f)
            health["components"]["won_quotes_kb"] = {
                "records": len(kb.get("quotes", kb)) if isinstance(kb, dict) else len(kb),
                "status": "ok",
            }
        except:
            health["components"]["won_quotes_kb"] = {"status": "corrupt"}
    else:
        health["components"]["won_quotes_kb"] = {"status": "empty"}

    # Check research cache
    cache_file = os.path.join(DATA_DIR, "product_research_cache.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file) as f:
                cache = json.load(f)
            health["components"]["research_cache"] = {
                "entries": len(cache),
                "status": "ok",
            }
        except:
            health["components"]["research_cache"] = {"status": "corrupt"}

    # Audit stats
    health["processing_stats"] = get_audit_stats()

    # Overall status
    critical = ["product_research", "price_check"]
    for c in critical:
        if not health["components"].get(c, {}).get("available"):
            health["status"] = "degraded"
    
    if not has_serpapi:
        health["status"] = "degraded"

    return health
