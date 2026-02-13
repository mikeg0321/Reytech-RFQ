"""
won_quotes_db.py — Won Quotes Knowledge Base for Reytech RFQ Automation
Version: 6.0 | Module: Competitive Intelligence Layer

Transforms SCPRS from a "search on demand" tool into a persistent
competitive intelligence database. Every price lookup enriches the KB.
Over time, this becomes Reytech's unfair advantage.

Dependencies: None beyond stdlib (json, re, datetime, os)
Storage: data/won_quotes.json (JSON file, PostgreSQL migration planned v7.0)
"""

import json
import os
import re
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict

# ─── Configuration ───────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
WON_QUOTES_FILE = os.path.join(DATA_DIR, "won_quotes.json")
MAX_RECORDS = 10000  # LRU eviction cap
FRESHNESS_WEIGHTS = {
    180: 1.0,    # 0–6 months: full weight
    365: 0.8,    # 6–12 months
    730: 0.5,    # 12–24 months
    9999: 0.2,   # older than 24 months
}
TOKEN_STOP_WORDS = {
    "the", "a", "an", "and", "or", "for", "of", "to", "in", "on", "by",
    "with", "is", "at", "from", "as", "per", "ea", "each", "set", "pkg",
    "package", "box", "case", "unit", "lot", "item", "no", "number",
}
CATEGORY_KEYWORDS = {
    "medical_equipment": ["stryker", "medline", "medical", "surgical", "hospital",
                          "restraint", "catheter", "syringe", "bandage", "gauze",
                          "glove", "gown", "mask", "iv", "needle", "scalpel"],
    "office_supplies": ["paper", "pen", "pencil", "folder", "binder", "toner",
                        "ink", "cartridge", "staple", "envelope", "label",
                        "tape", "marker", "notepad", "clipboard"],
    "industrial": ["grainger", "uline", "tool", "drill", "wrench", "bolt",
                   "screw", "pipe", "valve", "motor", "pump", "filter",
                   "bearing", "cable", "wire", "hose"],
    "janitorial": ["cleaning", "bleach", "mop", "broom", "trash", "bag",
                   "soap", "sanitizer", "disinfectant", "wipe", "towel"],
    "general": [],  # fallback category
}


# ─── Storage Layer ───────────────────────────────────────────────────────────

def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def load_won_quotes() -> list:
    """Load the won quotes knowledge base from disk."""
    _ensure_data_dir()
    if not os.path.exists(WON_QUOTES_FILE):
        return []
    try:
        with open(WON_QUOTES_FILE, "r") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, IOError):
        return []


def save_won_quotes(quotes: list):
    """Save the won quotes KB to disk with LRU eviction."""
    _ensure_data_dir()
    # LRU eviction: keep most recently ingested if over cap
    if len(quotes) > MAX_RECORDS:
        quotes.sort(key=lambda q: q.get("ingested_at", ""), reverse=True)
        quotes = quotes[:MAX_RECORDS]
    with open(WON_QUOTES_FILE, "w") as f:
        json.dump(quotes, f, indent=2, default=str)


# ─── Text Processing ─────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    """Lowercase, strip special chars, collapse whitespace."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text: str) -> set:
    """Extract meaningful tokens from a description, filtering stop words."""
    normalized = normalize_text(text)
    tokens = set(normalized.split())
    tokens -= TOKEN_STOP_WORDS
    # Remove single-char tokens and pure numbers under 3 digits
    tokens = {t for t in tokens if len(t) > 1 and not (t.isdigit() and len(t) < 3)}
    return tokens


def classify_category(description: str) -> str:
    """Classify an item into a category based on keyword matching."""
    desc_lower = description.lower()
    best_category = "general"
    best_score = 0
    for category, keywords in CATEGORY_KEYWORDS.items():
        if category == "general":
            continue
        score = sum(1 for kw in keywords if kw in desc_lower)
        if score > best_score:
            best_score = score
            best_category = category
    return best_category


def generate_record_id(po_number: str, item_number: str, description: str) -> str:
    """Generate a deterministic ID for deduplication."""
    raw = f"{po_number}|{item_number}|{normalize_text(description)}"
    return f"wq_{hashlib.md5(raw.encode()).hexdigest()[:12]}"


# ─── Freshness Scoring ───────────────────────────────────────────────────────

def freshness_weight(award_date_str: str) -> float:
    """Calculate freshness weight based on how old the award is."""
    try:
        # Handle multiple date formats
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%dT%H:%M:%S"):
            try:
                award_date = datetime.strptime(award_date_str, fmt)
                break
            except ValueError:
                continue
        else:
            return 0.2  # Unknown date → minimum weight

        days_old = (datetime.now() - award_date).days
        for threshold, weight in sorted(FRESHNESS_WEIGHTS.items()):
            if days_old <= threshold:
                return weight
        return 0.2
    except Exception:
        return 0.2


# ─── Matching Engine ─────────────────────────────────────────────────────────

def token_overlap_score(tokens_a: set, tokens_b: set) -> float:
    """Calculate Jaccard-like overlap score between two token sets."""
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def find_similar_items(
    item_number: str,
    description: str,
    max_results: int = 10,
    min_confidence: float = 0.3,
    max_age_days: int = 730,
) -> list:
    """
    Find similar items in the Won Quotes KB using multi-layer matching.

    Returns list of dicts with 'quote' and 'match_confidence' keys,
    sorted by (confidence * freshness_weight) descending.
    """
    quotes = load_won_quotes()
    if not quotes:
        return []

    query_tokens = tokenize(description)
    normalized_item = normalize_text(item_number) if item_number else ""
    cutoff_date = datetime.now() - timedelta(days=max_age_days)
    results = []

    for quote in quotes:
        # Age filter
        try:
            award_str = quote.get("award_date", "")
            if award_str:
                for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
                    try:
                        award_dt = datetime.strptime(award_str, fmt)
                        if award_dt < cutoff_date:
                            break
                        break
                    except ValueError:
                        continue
        except Exception:
            pass

        confidence = 0.0
        match_reasons = []

        # Layer 1: Exact item number match
        quote_item = normalize_text(quote.get("item_number", ""))
        if normalized_item and quote_item and normalized_item == quote_item:
            confidence = 1.0
            match_reasons.append("exact_item_number")
        else:
            # Layer 2: Token overlap on description
            quote_tokens = set(quote.get("tokens", []))
            if not quote_tokens:
                quote_tokens = tokenize(quote.get("description", ""))

            overlap = token_overlap_score(query_tokens, quote_tokens)
            if overlap >= 0.7:
                confidence = 0.7 + (overlap - 0.7) * (0.95 - 0.7) / 0.3
                match_reasons.append(f"token_overlap_{overlap:.2f}")
            elif overlap >= 0.4:
                # Layer 3: Category + partial keyword match
                query_cat = classify_category(description)
                quote_cat = quote.get("category", classify_category(quote.get("description", "")))
                if query_cat == quote_cat and query_cat != "general":
                    confidence = 0.4 + overlap * 0.3
                    match_reasons.append(f"category_{query_cat}_overlap_{overlap:.2f}")

        if confidence < min_confidence:
            continue

        # Apply freshness weighting to sort score
        fw = freshness_weight(quote.get("award_date", ""))
        sort_score = confidence * fw

        results.append({
            "quote": quote,
            "match_confidence": round(confidence, 3),
            "freshness_weight": fw,
            "sort_score": round(sort_score, 3),
            "match_reasons": match_reasons,
        })

    # Sort by weighted score, then by confidence
    results.sort(key=lambda r: (r["sort_score"], r["match_confidence"]), reverse=True)
    return results[:max_results]


def get_price_history(
    item_number: str,
    description: str,
    months: int = 24,
) -> dict:
    """
    Get price history for an item class.

    Returns:
        {
            "matches": int,
            "min_price": float,
            "max_price": float,
            "median_price": float,
            "avg_price": float,
            "recent_avg": float (last 6 months),
            "trend": "rising" | "falling" | "stable" | "insufficient_data",
            "data_points": [...]
        }
    """
    similar = find_similar_items(
        item_number, description,
        max_results=50,
        min_confidence=0.5,
        max_age_days=months * 30,
    )

    if not similar:
        return {
            "matches": 0, "min_price": None, "max_price": None,
            "median_price": None, "avg_price": None, "recent_avg": None,
            "trend": "insufficient_data", "data_points": [],
        }

    prices = [s["quote"]["unit_price"] for s in similar if s["quote"].get("unit_price", 0) > 0]
    if not prices:
        return {
            "matches": len(similar), "min_price": None, "max_price": None,
            "median_price": None, "avg_price": None, "recent_avg": None,
            "trend": "insufficient_data", "data_points": similar,
        }

    prices_sorted = sorted(prices)
    n = len(prices_sorted)
    median = prices_sorted[n // 2] if n % 2 else (prices_sorted[n // 2 - 1] + prices_sorted[n // 2]) / 2

    # Recent prices (last 6 months)
    recent = [s["quote"]["unit_price"] for s in similar
              if s["freshness_weight"] >= 0.8 and s["quote"].get("unit_price", 0) > 0]
    recent_avg = sum(recent) / len(recent) if recent else None

    # Trend detection
    trend = "insufficient_data"
    if recent_avg is not None and len(prices) >= 3:
        older = [p for s, p in zip(similar, prices) if s["freshness_weight"] < 0.8]
        if older:
            older_avg = sum(older) / len(older)
            pct_change = (recent_avg - older_avg) / older_avg
            if pct_change > 0.05:
                trend = "rising"
            elif pct_change < -0.05:
                trend = "falling"
            else:
                trend = "stable"

    return {
        "matches": len(prices),
        "min_price": round(min(prices), 2),
        "max_price": round(max(prices), 2),
        "median_price": round(median, 2),
        "avg_price": round(sum(prices) / len(prices), 2),
        "recent_avg": round(recent_avg, 2) if recent_avg else None,
        "trend": trend,
        "data_points": similar[:10],  # Return top 10 for UI display
    }


# ─── Ingestion ───────────────────────────────────────────────────────────────

def ingest_scprs_result(
    po_number: str,
    item_number: str,
    description: str,
    unit_price: float,
    quantity: float = 1,
    supplier: str = "",
    department: str = "",
    award_date: str = "",
    source: str = "scprs_live",
) -> dict:
    """
    Ingest a single SCPRS result into the Won Quotes KB.

    Called automatically after every SCPRS lookup.
    Deduplicates by (po_number, item_number, normalized_description).
    """
    record_id = generate_record_id(po_number, item_number, description)
    tokens = list(tokenize(description))
    category = classify_category(description)
    now = datetime.now(timezone.utc).isoformat()

    record = {
        "id": record_id,
        "po_number": po_number,
        "item_number": item_number,
        "description": description,
        "normalized_description": normalize_text(description),
        "tokens": tokens,
        "category": category,
        "supplier": supplier,
        "department": department,
        "unit_price": float(unit_price) if unit_price else 0.0,
        "quantity": float(quantity) if quantity else 1,
        "total": round(float(unit_price or 0) * float(quantity or 1), 2),
        "award_date": award_date,
        "source": source,
        "confidence": 1.0 if source == "scprs_live" else 0.8,
        "ingested_at": now,
    }

    quotes = load_won_quotes()

    # Dedup check
    existing_idx = next(
        (i for i, q in enumerate(quotes) if q.get("id") == record_id),
        None
    )
    if existing_idx is not None:
        # Update existing record (price may have changed)
        quotes[existing_idx] = record
    else:
        quotes.append(record)

    save_won_quotes(quotes)
    return record


def ingest_scprs_bulk(results: list) -> dict:
    """
    Bulk ingest SCPRS results.

    Args:
        results: List of dicts with keys matching ingest_scprs_result params

    Returns:
        {"ingested": int, "updated": int, "skipped": int}
    """
    quotes = load_won_quotes()
    existing_ids = {q.get("id") for q in quotes}
    stats = {"ingested": 0, "updated": 0, "skipped": 0}

    for r in results:
        if not r.get("unit_price") or float(r["unit_price"]) <= 0:
            stats["skipped"] += 1
            continue

        record_id = generate_record_id(
            r.get("po_number", ""),
            r.get("item_number", ""),
            r.get("description", ""),
        )

        record = {
            "id": record_id,
            "po_number": r.get("po_number", ""),
            "item_number": r.get("item_number", ""),
            "description": r.get("description", ""),
            "normalized_description": normalize_text(r.get("description", "")),
            "tokens": list(tokenize(r.get("description", ""))),
            "category": classify_category(r.get("description", "")),
            "supplier": r.get("supplier", ""),
            "department": r.get("department", ""),
            "unit_price": float(r["unit_price"]),
            "quantity": float(r.get("quantity", 1)),
            "total": round(float(r["unit_price"]) * float(r.get("quantity", 1)), 2),
            "award_date": r.get("award_date", ""),
            "source": r.get("source", "scprs_bulk"),
            "confidence": 1.0,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        if record_id in existing_ids:
            idx = next(i for i, q in enumerate(quotes) if q.get("id") == record_id)
            quotes[idx] = record
            stats["updated"] += 1
        else:
            quotes.append(record)
            existing_ids.add(record_id)
            stats["ingested"] += 1

    save_won_quotes(quotes)
    return stats


# ─── Win Probability ─────────────────────────────────────────────────────────

def win_probability(
    proposed_price: float,
    item_number: str,
    description: str,
) -> dict:
    """
    Estimate win probability based on historical won prices.

    Returns:
        {
            "probability": float (0.0-1.0),
            "confidence_level": "high" | "medium" | "low" | "no_data",
            "vs_median": float (percentage above/below median),
            "vs_recent": float (percentage above/below recent avg),
            "data_points": int,
            "reasoning": str
        }
    """
    history = get_price_history(item_number, description, months=24)

    if history["matches"] == 0 or history["median_price"] is None:
        return {
            "probability": 0.5,  # No data = coin flip
            "confidence_level": "no_data",
            "vs_median": None,
            "vs_recent": None,
            "data_points": 0,
            "reasoning": "No historical pricing data available. Recommend manual review.",
        }

    median = history["median_price"]
    recent = history["recent_avg"] or median
    reference_price = recent if history["matches"] >= 3 else median

    # How does our price compare?
    pct_vs_reference = (proposed_price - reference_price) / reference_price

    # Probability model (simplified logistic)
    # At reference price: ~60% win rate
    # 5% below: ~80% win rate
    # 10% below: ~90% win rate
    # 5% above: ~40% win rate
    # 10% above: ~20% win rate
    import math
    # Logistic: P = 1 / (1 + e^(k * pct_above))
    k = 15  # steepness
    raw_prob = 1.0 / (1.0 + math.exp(k * pct_vs_reference))

    # Clamp to reasonable range
    probability = max(0.05, min(0.95, raw_prob))

    # Confidence based on data quality
    if history["matches"] >= 5 and any(s["freshness_weight"] >= 0.8 for s in history["data_points"]):
        confidence_level = "high"
    elif history["matches"] >= 2:
        confidence_level = "medium"
    else:
        confidence_level = "low"

    # Build reasoning
    direction = "below" if pct_vs_reference < 0 else "above"
    reasoning = (
        f"Proposed ${proposed_price:.2f} is {abs(pct_vs_reference)*100:.1f}% {direction} "
        f"the {'recent average' if recent != median else 'median'} of ${reference_price:.2f} "
        f"(based on {history['matches']} historical data points). "
    )
    if pct_vs_reference > 0.03:
        reasoning += "⚠️ Price is >3% above recent wins — consider adjusting down."
    elif pct_vs_reference < -0.10:
        reasoning += "Price is aggressive — high win probability but thin margins."
    else:
        reasoning += "Price is competitive and well-positioned."

    return {
        "probability": round(probability, 3),
        "confidence_level": confidence_level,
        "vs_median": round(pct_vs_reference * 100, 1) if median else None,
        "vs_recent": round(((proposed_price - recent) / recent) * 100, 1) if recent else None,
        "data_points": history["matches"],
        "reasoning": reasoning,
    }


# ─── Statistics ──────────────────────────────────────────────────────────────

def get_kb_stats() -> dict:
    """Return statistics about the Won Quotes Knowledge Base."""
    quotes = load_won_quotes()
    if not quotes:
        return {
            "total_records": 0,
            "categories": {},
            "departments": {},
            "suppliers": {},
            "date_range": None,
            "avg_unit_price": None,
        }

    categories = defaultdict(int)
    departments = defaultdict(int)
    suppliers = defaultdict(int)
    prices = []
    dates = []

    for q in quotes:
        categories[q.get("category", "unknown")] += 1
        if q.get("department"):
            departments[q["department"]] += 1
        if q.get("supplier"):
            suppliers[q["supplier"]] += 1
        if q.get("unit_price", 0) > 0:
            prices.append(q["unit_price"])
        if q.get("award_date"):
            dates.append(q["award_date"])

    return {
        "total_records": len(quotes),
        "categories": dict(categories),
        "departments": dict(sorted(departments.items(), key=lambda x: -x[1])[:10]),
        "suppliers": dict(sorted(suppliers.items(), key=lambda x: -x[1])[:10]),
        "date_range": {"earliest": min(dates), "latest": max(dates)} if dates else None,
        "avg_unit_price": round(sum(prices) / len(prices), 2) if prices else None,
        "total_value": round(sum(q.get("total", 0) for q in quotes), 2),
    }
