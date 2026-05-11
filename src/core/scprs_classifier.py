"""Shared classifier for scprs_po_lines `reytech_sells` / `opportunity_flag`.

Mike P0 2026-05-11 (cross-sell hunting arc): both `cchcs_intel_puller.py`
and `scprs_universal_pull.py` had their own HARDCODED keyword dicts
(`REYTECH_CATALOG` / `REYTECH_PRODUCTS`) of ~25 entries each. SCPRS has
~150-300K line items; the product_catalog table has the real catalog
(94 columns, fuzzy match with UPC / supplier SKU / mfg_number / token
overlap, gating at 0.65 confidence per `match_item()`). The cross-sell
intel surface (`opportunity_flag='WIN_BACK'` = "buyer bought this from
a competitor, Reytech sells it") was running on the 25-keyword stub
instead of the real catalog — accuracy was poor and most matches were
missed.

This module is the single source of truth for line classification.
Both pullers (cchcs_intel_puller, scprs_universal_pull) import and use
`classify_line()`. Going-forward ingest writes accurate flags; existing
rows are re-classified by `scripts/reclassify_scprs_lines_2026_05_11.py`.

Match priority (matches `product_catalog.match_item()`):
  1. UPC exact (0.99)
  2. Supplier SKU reverse-lookup (0.98)
  3. Exact part# on name/sku/mfg_number (0.98)
  4. Part# extracted from description (0.92)
  5. Token Jaccard similarity >= 0.65

A match at >= 0.65 confidence → `reytech_sells=1`, `opportunity_flag='WIN_BACK'`.
Below threshold → fall back to keyword classifier (legacy behavior;
prevents regressing categories the keyword dict happened to catch).
"""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


# Threshold for the real-catalog match. 0.65 matches `match_item()`'s
# internal Jaccard cutoff (raised from 0.50 → 0.65 after cross-category
# near-miss audits — see product_catalog.py:2567).
MATCH_CONFIDENCE_THRESHOLD = 0.65


# Keyword fallback — same shape as the original REYTECH_CATALOG /
# REYTECH_PRODUCTS dicts. Used only when catalog match is below
# threshold or unavailable. Keeps the GAP_ITEM flag working for items
# in Reytech's adjacent categories that aren't yet in the catalog.
_KEYWORD_FALLBACK: dict[str, dict[str, Any]] = {
    "nitrile gloves":        {"sku": "NITRILE-M", "category": "exam_gloves", "sells": True},
    "nitrile exam gloves":   {"sku": "NITRILE-M", "category": "exam_gloves", "sells": True},
    "nitrile":               {"sku": "NITRILE-M", "category": "exam_gloves", "sells": True},
    "latex gloves":          {"sku": None,        "category": "exam_gloves", "sells": False},
    "vinyl gloves":          {"sku": None,        "category": "exam_gloves", "sells": False},
    "exam gloves":           {"sku": "NITRILE-M", "category": "exam_gloves", "sells": True},
    "adult briefs":          {"sku": "BRIEFS-M",  "category": "incontinence", "sells": True},
    "incontinence briefs":   {"sku": "BRIEFS-M",  "category": "incontinence", "sells": True},
    "adult brief":           {"sku": "BRIEFS-M",  "category": "incontinence", "sells": True},
    "incontinence":          {"sku": "BRIEFS-M",  "category": "incontinence", "sells": True},
    "chux":                  {"sku": "CHUX-23",   "category": "incontinence", "sells": True},
    "underpads":             {"sku": "CHUX-23",   "category": "incontinence", "sells": True},
    "n95":                   {"sku": "N95-3M8210","category": "respiratory",  "sells": True},
    "respirator":            {"sku": "N95-3M8210","category": "respiratory",  "sells": True},
    "surgical mask":         {"sku": None,        "category": "respiratory",  "sells": False},
    "face mask":             {"sku": None,        "category": "respiratory",  "sells": False},
    "gauze":                 {"sku": None,        "category": "wound_care",   "sells": False},
    "wound dressing":        {"sku": None,        "category": "wound_care",   "sells": False},
    "abd pad":               {"sku": None,        "category": "wound_care",   "sells": False},
    "bandage":               {"sku": None,        "category": "wound_care",   "sells": False},
    "sharps container":      {"sku": None,        "category": "sharps",       "sells": False},
    "needle disposal":       {"sku": None,        "category": "sharps",       "sells": False},
    "hand sanitizer":        {"sku": None,        "category": "hand_hygiene", "sells": False},
    "restraint":             {"sku": None,        "category": "restraints",   "sells": False},
    "trash bag":             {"sku": None,        "category": "janitorial",   "sells": False},
    "paper towel":           {"sku": None,        "category": "janitorial",   "sells": False},
    "disinfectant":          {"sku": None,        "category": "janitorial",   "sells": False},
    "first aid kit":         {"sku": "FAK-ANSI-B","category": "first_aid",    "sells": True},
    "tourniquet":            {"sku": "CAT-GEN7",  "category": "trauma",       "sells": True},
    "hi-vis vest":           {"sku": "HIVIS-ANSI2","category": "safety",      "sells": True},
    "hi-vis":                {"sku": "HIVIS-ANSI2","category": "safety",      "sells": True},
    "hard hat":              {"sku": None,        "category": "safety",       "sells": True},
    "safety glasses":        {"sku": None,        "category": "safety",       "sells": True},
    "gown":                  {"sku": None,        "category": "clinical",     "sells": False},
}


def classify_line(description: str, item_id: str = "") -> dict:
    """Return classification dict for a SCPRS PO line.

    Returns:
        {
            "category": <category-string>,
            "reytech_sells": 0 | 1,
            "reytech_sku": <sku or None>,
            "opportunity_flag": "WIN_BACK" | "GAP_ITEM" | None,
            "match_confidence": float | None,
            "match_source": "catalog" | "keyword" | "other",
        }

    Resolution priority:
      1. product_catalog.match_item(description, item_id) >= 0.65 →
         reytech_sells=1, opportunity_flag="WIN_BACK", match_source="catalog"
      2. Keyword fallback (Reytech-adjacent categories) →
         flag set per sells T/F in dict, match_source="keyword"
      3. Nothing matches → category="other", reytech_sells=0, no flag
    """
    desc = (description or "").strip()
    pn = (item_id or "").strip()

    # ── Tier 1: real product_catalog match ────────────────────────────
    try:
        from src.agents.product_catalog import match_item
        matches = match_item(description=desc, part_number=pn, top_n=1)
        if matches:
            best = matches[0]
            confidence = float(best.get("match_confidence") or 0)
            if confidence >= MATCH_CONFIDENCE_THRESHOLD:
                return {
                    "category": (best.get("category") or "other").strip().lower() or "other",
                    "reytech_sells": 1,
                    "reytech_sku": (best.get("sku") or best.get("mfg_number") or None),
                    "opportunity_flag": "WIN_BACK",
                    "match_confidence": round(confidence, 3),
                    "match_source": "catalog",
                }
    except Exception as e:
        # Catalog unavailable (e.g., import error in a test context) — fall
        # through to keyword. Don't crash the SCPRS ingest if the catalog
        # module breaks; ingest is the higher-priority pipeline.
        log.debug("scprs_classifier: catalog match unavailable: %s", e)

    # ── Tier 2: keyword fallback ──────────────────────────────────────
    desc_lower = desc.lower()
    for keyword, data in _KEYWORD_FALLBACK.items():
        if keyword in desc_lower:
            sells = bool(data["sells"])
            return {
                "category": data["category"],
                "reytech_sells": 1 if sells else 0,
                "reytech_sku": data.get("sku"),
                "opportunity_flag": "WIN_BACK" if sells else "GAP_ITEM",
                "match_confidence": None,
                "match_source": "keyword",
            }

    # ── Tier 3: nothing — leave classification empty for re-try later ─
    return {
        "category": "other",
        "reytech_sells": 0,
        "reytech_sku": None,
        "opportunity_flag": None,
        "match_confidence": None,
        "match_source": "other",
    }
