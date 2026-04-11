"""
bid_decision_agent.py — Bid/No-Bid auto-scorer for incoming Price Checks.

Scores PCs on 4 dimensions to recommend bid/review/no-bid:
  1. Catalog coverage (30%) — do we carry these items?
  2. Win history (25%) — have we won for this buyer/agency?
  3. Margin potential (25%) — does SCPRS data suggest good margins?
  4. Complexity (20%) — item count, special requirements

V1: Rule-based scoring. V2: ML-trained on win/loss history.
"""
import json
import logging
import os

log = logging.getLogger("reytech.bid_decision")

# Weights for each dimension
W_CATALOG = 0.30
W_WIN_HISTORY = 0.25
W_MARGIN = 0.25
W_COMPLEXITY = 0.20

# Thresholds
THRESHOLD_BID = 70      # auto-proceed
THRESHOLD_REVIEW = 40   # manual check
# Below THRESHOLD_REVIEW = no-bid


def _score_catalog_coverage(items: list) -> float:
    """Score 0-100: what % of items match our product catalog?"""
    if not items:
        return 50  # neutral

    try:
        from src.core.catalog import search_catalog
        matched = 0
        for item in items:
            desc = item.get("description", "") or ""
            if not desc.strip():
                continue
            results = search_catalog(desc[:100], limit=1)
            if results:
                matched += 1
        total = max(len(items), 1)
        coverage = matched / total
        # Scale: 90%+ = 100, 50% = 50, 0% = 10
        return max(10, min(100, coverage * 110))
    except Exception as e:
        log.debug("Catalog coverage check error: %s", e)
        return 50  # neutral on error


def _score_win_history(agency: str, institution: str) -> float:
    """Score 0-100: have we won for this agency/institution?"""
    if not agency:
        return 30

    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Check won_quotes for this agency
            count = conn.execute(
                "SELECT COUNT(*) FROM won_quotes WHERE institution LIKE ? OR department LIKE ?",
                (f"%{agency}%", f"%{agency}%")
            ).fetchone()[0]

            if count >= 10:
                return 100
            elif count >= 5:
                return 85
            elif count >= 1:
                return 60
            else:
                return 20
    except Exception as e:
        log.debug("Win history check error: %s", e)
        return 30


def _score_margin_potential(items: list) -> float:
    """Score 0-100: do we have pricing data for these items?"""
    if not items:
        return 30

    try:
        from src.core.db import get_db
        has_pricing = 0
        with get_db() as conn:
            for item in items[:20]:  # cap at 20 lookups
                desc = item.get("description", "") or ""
                if not desc.strip():
                    continue
                # Check if we have any price history
                row = conn.execute(
                    "SELECT COUNT(*) FROM price_history WHERE description LIKE ? LIMIT 1",
                    (f"%{desc[:50]}%",)
                ).fetchone()
                if row and row[0] > 0:
                    has_pricing += 1

        total = max(len(items[:20]), 1)
        coverage = has_pricing / total
        # Good coverage = high margin confidence
        return max(20, min(100, coverage * 120))
    except Exception as e:
        log.debug("Margin potential check error: %s", e)
        return 30


def _score_complexity(items: list, has_food: bool = False) -> float:
    """Score 0-100: simpler PCs score higher (easier to win)."""
    count = len(items) if items else 0

    if count <= 5:
        base = 100
    elif count <= 10:
        base = 85
    elif count <= 20:
        base = 65
    elif count <= 40:
        base = 45
    else:
        base = 25

    # Deductions
    if has_food:
        base -= 15  # food items need OBS-1600, extra compliance

    return max(10, base)


def score_pc_items(items: list, agency: str = "", institution: str = "",
                   has_food: bool = False) -> dict:
    """Score a PC based on its items and metadata.

    Returns: {"total_score", "recommendation", "breakdown": {dimension: score}}
    """
    cat = _score_catalog_coverage(items)
    win = _score_win_history(agency, institution)
    margin = _score_margin_potential(items)
    comp = _score_complexity(items, has_food)

    total = (cat * W_CATALOG + win * W_WIN_HISTORY +
             margin * W_MARGIN + comp * W_COMPLEXITY)
    total = round(total, 1)

    if total >= THRESHOLD_BID:
        rec = "bid"
    elif total >= THRESHOLD_REVIEW:
        rec = "review"
    else:
        rec = "no-bid"

    return {
        "total_score": total,
        "recommendation": rec,
        "breakdown": {
            "catalog_coverage": round(cat, 1),
            "win_history": round(win, 1),
            "margin_potential": round(margin, 1),
            "complexity": round(comp, 1),
        },
    }


def score_pc(pc_id: str) -> dict:
    """Score a PC by ID. Loads from JSON, scores, saves to DB.

    Returns: same as score_pc_items() plus "pc_id" and "saved" flag.
    """
    try:
        from src.core.paths import DATA_DIR
        pc_path = os.path.join(DATA_DIR, f"{pc_id}.json")
        if not os.path.isfile(pc_path):
            return {"ok": False, "error": "PC not found"}

        with open(pc_path, "r") as f:
            pc = json.load(f)

        items = pc.get("items", []) or pc.get("parsed", {}).get("line_items", []) or []
        agency = pc.get("agency", "") or ""
        institution = pc.get("institution", "") or ""
        has_food = any("food" in (it.get("description", "") or "").lower() for it in items)

        result = score_pc_items(items, agency, institution, has_food)
        result["pc_id"] = pc_id
        result["ok"] = True

        # Save to DB
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO bid_scores
                       (pc_id, catalog_coverage, win_history_score, margin_potential,
                        complexity_score, total_score, recommendation)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (pc_id, result["breakdown"]["catalog_coverage"],
                     result["breakdown"]["win_history"],
                     result["breakdown"]["margin_potential"],
                     result["breakdown"]["complexity"],
                     result["total_score"], result["recommendation"])
                )
            result["saved"] = True
        except Exception as e:
            log.warning("Failed to save bid score for %s: %s", pc_id, e)
            result["saved"] = False

        log.info("Bid score for %s: %.1f (%s) — cat=%.0f win=%.0f margin=%.0f complex=%.0f",
                 pc_id, result["total_score"], result["recommendation"],
                 result["breakdown"]["catalog_coverage"],
                 result["breakdown"]["win_history"],
                 result["breakdown"]["margin_potential"],
                 result["breakdown"]["complexity"])

        return result

    except Exception as e:
        log.error("Bid scoring error for %s: %s", pc_id, e, exc_info=True)
        return {"ok": False, "error": str(e)}


def get_bid_score(pc_id: str) -> dict:
    """Load saved bid score from DB."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM bid_scores WHERE pc_id = ? ORDER BY scored_at DESC LIMIT 1",
                (pc_id,)
            ).fetchone()
            if not row:
                return {"ok": False, "error": "No score found"}
            return {"ok": True, **dict(row)}
    except Exception as e:
        return {"ok": False, "error": str(e)}
