"""
item_identifier.py — Item Identification Agent for Reytech RFQ Automation
Phase 13 | Version: 1.0.0

The missing pipeline step between PARSE and PRICE.

Problem: PC line items come in as vague descriptions like "Engraved two line
name tag, black/white" — too vague for Amazon search, no SCPRS match.

Solution: This agent interprets item descriptions, generates optimized search
terms, detects product categories, and suggests sourcing strategies.

Modes:
  RULE-BASED (no API key): Cleans descriptions, strips noise words,
    detects categories from keyword patterns. Works offline.
  LLM-ENHANCED (with ANTHROPIC_API_KEY): Uses Claude Haiku to interpret
    ambiguous items, generate multiple search strategies, and match products
    to the right category. ~$0.01/item.

Pipeline position:
  Parse → IDENTIFY → SCPRS Lookup → Amazon Lookup → Price → Fill 704

Dependencies: requests (for LLM calls). anthropic SDK optional but preferred.
"""

import json
import os
import re
import time
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("item_id")

# ─── Configuration ───────────────────────────────────────────────────────────

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

CACHE_FILE = os.path.join(DATA_DIR, "item_id_cache.json")
CACHE_TTL_DAYS = 30  # Item IDs are stable — cache aggressively
MAX_CACHE_ENTRIES = 10000

# API config — use centralized secret registry
try:
    from src.core.secrets import get_agent_key
    ANTHROPIC_API_KEY = get_agent_key("item_identifier")
except ImportError:
    ANTHROPIC_API_KEY = os.environ.get("AGENT_ITEM_ID_KEY",
                       os.environ.get("ANTHROPIC_API_KEY", ""))
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"  # Cheapest, fastest — $0.25/$1.25 per M tokens

# Try anthropic SDK, fall back to requests
try:
    import anthropic
    HAS_SDK = True
except ImportError:
    HAS_SDK = False

try:
    import requests as _requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


# ─── Product Categories ─────────────────────────────────────────────────────
# Maps keywords to procurement categories. Used for:
# - Filtering search results
# - Routing to the right vendors
# - Pricing strategy (medical = higher margin, office = competitive)

CATEGORIES = {
    "medical": {
        "keywords": ["glove", "nitrile", "latex", "syringe", "bandage", "gauze",
                     "antiseptic", "sanitizer", "mask", "gown", "thermometer",
                     "stethoscope", "restraint", "catheter", "sharps", "biohazard",
                     "exam", "medical", "clinical", "patient", "wound", "suture"],
        "margin_hint": "15-30%",
        "notes": "Verify CDCR compliance. Check CCHCS contracts.",
    },
    "office": {
        "keywords": ["pen", "pencil", "paper", "folder", "binder", "stapler",
                     "tape", "envelope", "label", "marker", "highlighter",
                     "notebook", "clipboard", "desk", "chair", "file", "toner",
                     "ink", "cartridge", "name tag", "badge", "lanyard", "id card"],
        "margin_hint": "20-35%",
        "notes": "High competition on SCPRS. Undercut by 1-3%.",
    },
    "janitorial": {
        "keywords": ["mop", "broom", "bucket", "cleaner", "detergent", "soap",
                     "towel", "tissue", "trash", "bag", "liner", "disinfectant",
                     "bleach", "wipe", "spray", "floor", "polish", "vacuum"],
        "margin_hint": "20-30%",
        "notes": "Bulk pricing available. Check vendor contracts.",
    },
    "food_service": {
        "keywords": ["tray", "utensil", "fork", "spoon", "knife", "cup", "plate",
                     "napkin", "food", "container", "wrap", "aluminum", "foil",
                     "condiment", "seasoning", "cooking", "serving"],
        "margin_hint": "15-25%",
        "notes": "Institutional sizing. Verify CDCR kitchen compliance.",
    },
    "safety": {
        "keywords": ["helmet", "vest", "boot", "goggle", "earplug", "earmuff",
                     "harness", "cone", "sign", "extinguisher", "first aid",
                     "safety", "protective", "hi-vis", "reflective"],
        "margin_hint": "20-30%",
        "notes": "Cal/OSHA requirements may apply.",
    },
    "technology": {
        "keywords": ["cable", "adapter", "battery", "charger", "mouse", "keyboard",
                     "monitor", "printer", "usb", "hdmi", "ethernet", "phone",
                     "headset", "surge", "power strip", "ups", "toner"],
        "margin_hint": "15-25%",
        "notes": "Short shelf life on tech. Check warranty requirements.",
    },
}

# Noise words to strip from search queries
NOISE_WORDS = {
    "please", "provide", "supply", "furnish", "deliver", "per", "each",
    "unit", "item", "quantity", "qty", "as", "needed", "required",
    "approximately", "approx", "estimated", "est", "or", "equal",
    "equivalent", "similar", "comparable", "like", "such", "the",
    "a", "an", "and", "of", "for", "to", "in", "on", "with", "by",
    "state", "prison", "institution", "department", "facility",
}


# ─── Cache ───────────────────────────────────────────────────────────────────

def _load_cache() -> dict:
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_cache(cache: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    # Prune if too large
    if len(cache) > MAX_CACHE_ENTRIES:
        sorted_keys = sorted(cache, key=lambda k: cache[k].get("ts", 0))
        for k in sorted_keys[:len(cache) - MAX_CACHE_ENTRIES]:
            del cache[k]
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)


def _cache_key(description: str) -> str:
    return hashlib.md5(description.strip().lower().encode()).hexdigest()[:12]


def _cache_get(description: str) -> Optional[dict]:
    cache = _load_cache()
    key = _cache_key(description)
    entry = cache.get(key)
    if not entry:
        return None
    ts = entry.get("ts", 0)
    if time.time() - ts > CACHE_TTL_DAYS * 86400:
        return None
    return entry.get("result")


def _cache_set(description: str, result: dict):
    cache = _load_cache()
    cache[_cache_key(description)] = {
        "ts": time.time(),
        "desc": description[:100],
        "result": result,
    }
    _save_cache(cache)


# ─── Rule-Based Identification (no API key needed) ──────────────────────────

def _clean_description(desc: str) -> str:
    """Strip noise, normalize whitespace, remove quantities embedded in text."""
    text = desc.strip()
    # Remove embedded quantities like "22 EA" or "50 BX"
    text = re.sub(r'\b\d+\s*(EA|BX|CS|PK|DZ|CT|RL|PR|DZ|KT|ST|BT|GL|OZ|LB)\b',
                  '', text, flags=re.IGNORECASE)
    # Remove trailing sizes like ", Large" or "- Medium"
    # But keep them for search — they matter for product matching
    return re.sub(r'\s+', ' ', text).strip()


def _generate_search_terms(desc: str) -> list:
    """Generate 1-3 search term variants from a description."""
    clean = _clean_description(desc)
    words = clean.split()
    # Remove noise words
    meaningful = [w for w in words if w.lower() not in NOISE_WORDS and len(w) > 1]

    terms = []
    # Full meaningful description
    if meaningful:
        terms.append(" ".join(meaningful))

    # Without brand/model specifics (first 4-5 content words)
    if len(meaningful) > 5:
        terms.append(" ".join(meaningful[:5]))

    # Core noun phrase (last resort — just the nouns)
    nouns = [w for w in meaningful if not w.endswith((',', '/', '-'))]
    if len(nouns) >= 2 and " ".join(nouns) not in terms:
        terms.append(" ".join(nouns[:4]))

    return terms[:3] if terms else [clean[:60]]


def _detect_category(desc: str) -> dict:
    """Detect product category from description keywords."""
    desc_lower = desc.lower()
    scores = {}
    for cat, info in CATEGORIES.items():
        score = sum(1 for kw in info["keywords"] if kw in desc_lower)
        if score > 0:
            scores[cat] = score

    if not scores:
        return {"category": "general", "confidence": 0.3,
                "margin_hint": "20-25%", "notes": "Uncategorized item."}

    best = max(scores, key=scores.get)
    confidence = min(0.95, 0.4 + scores[best] * 0.15)
    info = CATEGORIES[best]
    return {
        "category": best,
        "confidence": round(confidence, 2),
        "margin_hint": info["margin_hint"],
        "notes": info["notes"],
        "all_matches": {k: v for k, v in scores.items()},
    }


def _identify_rule_based(description: str, qty: int = 0,
                         uom: str = "") -> dict:
    """Rule-based item identification. No API key needed."""
    search_terms = _generate_search_terms(description)
    category = _detect_category(description)

    return {
        "method": "rule_based",
        "original_description": description,
        "clean_description": _clean_description(description),
        "search_terms": search_terms,
        "primary_search": search_terms[0] if search_terms else description,
        "category": category,
        "qty": qty,
        "uom": uom,
        "suggestions": [],
        "llm_enhanced": False,
    }


# ─── LLM-Enhanced Identification ────────────────────────────────────────────

_LLM_SYSTEM = """You are a procurement product identification specialist for Reytech Inc.,
a California state government reseller. Given an item description from a state
procurement form (AMS 704), identify the product and provide:

1. What the item actually is (plain English)
2. 2-3 optimized Amazon search terms to find this product
3. Product category (medical, office, janitorial, food_service, safety, technology, general)
4. Common manufacturers/brands for this type of item
5. Any procurement notes (compliance, sizing, institutional requirements)

Respond in JSON only. No markdown, no explanation. Example:
{"product_name":"Magnetic Name Badge","search_terms":["engraved magnetic name badge","custom name tag magnetic backing","2-line engraved name badge"],"category":"office","brands":["Avery","Advantus","MakeBadge"],"notes":"Verify engraving spec with buyer. Standard 2-line format is Name + Title."}"""


def _call_llm(description: str, qty: int = 0, uom: str = "") -> Optional[dict]:
    """Call Claude Haiku for smart item identification."""
    if not ANTHROPIC_API_KEY:
        return None

    prompt = f"Item description from AMS 704 form: \"{description}\""
    if qty:
        prompt += f"\nQuantity: {qty} {uom}"

    try:
        if HAS_SDK:
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            response = client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=300,
                system=_LLM_SYSTEM,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text
        elif HAS_REQUESTS:
            resp = _requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 300,
                    "system": _LLM_SYSTEM,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            text = data["content"][0]["text"]
        else:
            log.warning("No HTTP client available for LLM call")
            return None

        # Parse JSON response
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r'^```\w*\n?', '', text)
            text = re.sub(r'\n?```$', '', text)
        return json.loads(text)

    except json.JSONDecodeError as e:
        log.warning("LLM returned non-JSON: %s", e)
        return None
    except Exception as e:
        log.warning("LLM call failed: %s", e)
        return None


def _identify_llm_enhanced(description: str, qty: int = 0,
                           uom: str = "") -> dict:
    """LLM-enhanced item identification. Requires ANTHROPIC_API_KEY."""
    # Get rule-based as baseline
    base = _identify_rule_based(description, qty, uom)

    llm_result = _call_llm(description, qty, uom)
    if not llm_result:
        log.info("LLM unavailable, using rule-based for: %s", description[:50])
        return base

    # Merge LLM results with rule-based
    base["method"] = "llm_enhanced"
    base["llm_enhanced"] = True
    base["product_name"] = llm_result.get("product_name", "")

    # LLM search terms take priority, but keep rule-based as fallback
    llm_terms = llm_result.get("search_terms", [])
    if llm_terms:
        base["search_terms"] = llm_terms[:3] + base["search_terms"][:1]
        base["primary_search"] = llm_terms[0]

    # LLM category overrides if confident
    llm_cat = llm_result.get("category", "")
    if llm_cat and llm_cat in CATEGORIES:
        base["category"]["category"] = llm_cat
        base["category"]["confidence"] = max(base["category"]["confidence"], 0.8)

    base["brands"] = llm_result.get("brands", [])
    base["procurement_notes"] = llm_result.get("notes", "")

    return base


# ─── Public API ──────────────────────────────────────────────────────────────

def identify_item(description: str, qty: int = 0, uom: str = "",
                  force_llm: bool = False) -> dict:
    """
    Identify a procurement item from its description.

    Returns:
        {
            "method": "rule_based" | "llm_enhanced",
            "original_description": str,
            "clean_description": str,
            "search_terms": [str, ...],       # Optimized for Amazon/Google
            "primary_search": str,             # Best single search term
            "category": {"category": str, "confidence": float, ...},
            "product_name": str,               # LLM only: plain English name
            "brands": [str, ...],              # LLM only: common brands
            "procurement_notes": str,           # LLM only: compliance notes
            "llm_enhanced": bool,
        }
    """
    if not description or not description.strip():
        return {"method": "none", "error": "Empty description",
                "search_terms": [], "category": {"category": "unknown"}}

    # Check cache
    cached = _cache_get(description)
    if cached and not force_llm:
        cached["from_cache"] = True
        return cached

    # Try LLM if key available
    if ANTHROPIC_API_KEY or force_llm:
        result = _identify_llm_enhanced(description, qty, uom)
    else:
        result = _identify_rule_based(description, qty, uom)

    result["identified_at"] = datetime.now().isoformat()

    # Cache the result
    _cache_set(description, result)

    return result


def identify_pc_items(items: list) -> list:
    """
    Identify all items in a Price Check.

    Args:
        items: List of PC line items with 'description', 'qty', 'uom' keys.

    Returns:
        Same list with 'identification' key added to each item.
    """
    results = []
    for item in items:
        desc = item.get("description", item.get("description_raw", ""))
        qty = item.get("qty", 0)
        uom = item.get("uom", "")

        ident = identify_item(desc, qty, uom)
        item["identification"] = ident

        # Upgrade search terms in the item for downstream agents
        if ident.get("primary_search"):
            item["_search_query"] = ident["primary_search"]
        if ident.get("category", {}).get("category"):
            item["_category"] = ident["category"]["category"]

        results.append(item)
        # Rate limit LLM calls
        if ident.get("llm_enhanced") and not ident.get("from_cache"):
            time.sleep(0.5)

    return results


def get_agent_status() -> dict:
    """Return agent health status for /api/health."""
    cache = _load_cache()
    return {
        "agent": "item_identifier",
        "version": "1.0.0",
        "mode": "llm_enhanced" if ANTHROPIC_API_KEY else "rule_based",
        "api_key_set": bool(ANTHROPIC_API_KEY),
        "model": ANTHROPIC_MODEL if ANTHROPIC_API_KEY else None,
        "cache_entries": len(cache),
        "cache_file": CACHE_FILE,
        "categories": list(CATEGORIES.keys()),
    }
