"""CCHCS packet → PC item matcher.

Given a parsed CCHCS packet (from cchcs_packet_parser), find matching
items in existing Price Checks so we can auto-populate prices in the
filler (Phase 2). The matcher tries three strategies in order and
returns the first confident hit:

  1. Solicitation number exact match — if a PC exists with the same
     `pc_number` / `solicitation_number`, use its pricing directly
  2. MFG number exact match — walk every active PC's items, match on
     `mfg_number` / `part_number` / UPC
  3. Description token match — Jaccard similarity ≥ 0.60 against
     active PC item descriptions

Output shape is what the filler's `price_overrides` argument expects:

    {
        1: {"unit_cost": 295.00, "unit_price": 395.00,
            "source_pc_id": "pc_abc", "source_item_idx": 0,
            "match_strategy": "mfg_number", "confidence": 1.0},
        ...
    }

Every packet row that couldn't be matched is OMITTED from the return
dict (not included with price=0) — per Reytech standards, unpriced
rows should appear blank in the output PDF so the operator sees gaps
before sending.

Built 2026-04-13 overnight CCHCS automation session.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("reytech.cchcs_matcher")


# Tokenization helpers — matches the style used in product_catalog
_STOP_WORDS = {
    "the", "and", "for", "with", "pack", "of", "per", "ea", "each", "box",
    "pk", "set", "in", "by", "to", "is", "it", "at", "on", "or", "an", "as",
    "a", "new", "used", "refurb", "refurbished", "size", "color",
}


def _tokens(text: str) -> set:
    """Normalize a description to a set of meaningful tokens."""
    if not text:
        return set()
    # Lowercase, strip punctuation, split on whitespace
    cleaned = re.sub(r"[^\w\s]", " ", str(text).lower())
    raw_tokens = cleaned.split()
    return {t for t in raw_tokens if len(t) >= 2 and t not in _STOP_WORDS}


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _normalize_mfg(s: str) -> str:
    """Strip whitespace + uppercase for MFG# comparison."""
    if not s:
        return ""
    return re.sub(r"\s+", "", str(s)).upper()


def _normalize_sol(s: str) -> str:
    """Normalize solicitation/PC number: strip whitespace, remove
    leading '#', uppercase."""
    if not s:
        return ""
    s = str(s).strip().lstrip("#").upper()
    return s


def _pc_is_active(pc: Dict[str, Any]) -> bool:
    """A PC counts as a price source when it's not dismissed/dead."""
    status = (pc.get("status") or "").lower()
    return status not in (
        "dismissed", "deleted", "duplicate", "archived",
        "no_response", "not_responding", "expired", "reclassified",
    )


def _pc_item_price(item: Dict[str, Any]) -> Tuple[float, float]:
    """Return (unit_cost, unit_price) from a PC item, preferring the
    user-priced values over defaults.

    unit_cost = what Reytech pays
    unit_price = what Reytech bids (cost * markup)
    """
    pricing = item.get("pricing") or {}
    # Cost: prefer unit_cost, then catalog_cost, then vendor_cost
    unit_cost = (
        _safe_float(pricing.get("unit_cost"))
        or _safe_float(pricing.get("catalog_cost"))
        or _safe_float(pricing.get("web_cost"))
        or _safe_float(item.get("vendor_cost"))
        or 0.0
    )
    # Price: prefer unit_price (final quote), then recommended_price
    unit_price = (
        _safe_float(item.get("unit_price"))
        or _safe_float(pricing.get("recommended_price"))
        or _safe_float(pricing.get("final_price"))
        or 0.0
    )
    return unit_cost, unit_price


def _safe_float(v: Any) -> float:
    if v is None or v == "":
        return 0.0
    try:
        return float(str(v).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return 0.0


# ── Match strategies ──────────────────────────────────────────────────────

def _match_by_solicitation(
    packet: Dict[str, Any], pcs: Dict[str, Dict[str, Any]]
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Find an active PC whose pc_number / solicitation_number matches
    the packet's solicitation number. Returns (pc_id, pc) or None."""
    target = _normalize_sol(
        packet.get("header", {}).get("solicitation_number", "")
    )
    if not target:
        return None
    for pcid, pc in pcs.items():
        if not isinstance(pc, dict) or not _pc_is_active(pc):
            continue
        cand = _normalize_sol(
            pc.get("pc_number") or pc.get("solicitation_number") or ""
        )
        if cand and cand == target:
            return pcid, pc
    return None


def _match_by_mfg_number(
    packet_item: Dict[str, Any],
    pcs: Dict[str, Dict[str, Any]],
) -> Optional[Tuple[str, int, Dict[str, Any]]]:
    """Walk every active PC's items looking for an exact MFG#/part#
    match. Returns (pc_id, item_index, pc_item) for the first hit."""
    target = _normalize_mfg(
        packet_item.get("mfg_number") or packet_item.get("part_number") or ""
    )
    if not target or len(target) < 3:
        return None
    for pcid, pc in pcs.items():
        if not isinstance(pc, dict) or not _pc_is_active(pc):
            continue
        items = pc.get("items") or pc.get("parsed", {}).get("line_items") or []
        for i, pcit in enumerate(items):
            if not isinstance(pcit, dict):
                continue
            for field in ("mfg_number", "part_number", "upc"):
                cand = _normalize_mfg(pcit.get(field, ""))
                if cand and cand == target:
                    return pcid, i, pcit
            # Also check nested pricing.mfg_number
            pr = pcit.get("pricing") or {}
            cand = _normalize_mfg(pr.get("mfg_number") or pr.get("manufacturer_part") or "")
            if cand and cand == target:
                return pcid, i, pcit
    return None


def _match_by_description(
    packet_item: Dict[str, Any],
    pcs: Dict[str, Dict[str, Any]],
    min_confidence: float = 0.60,
) -> Optional[Tuple[str, int, Dict[str, Any], float]]:
    """Token-overlap fallback. Returns best hit above min_confidence
    or None."""
    target_desc = packet_item.get("description") or ""
    target_tokens = _tokens(target_desc)
    if len(target_tokens) < 2:
        return None
    best_pcid = None
    best_idx = -1
    best_item = None
    best_score = 0.0
    for pcid, pc in pcs.items():
        if not isinstance(pc, dict) or not _pc_is_active(pc):
            continue
        items = pc.get("items") or pc.get("parsed", {}).get("line_items") or []
        for i, pcit in enumerate(items):
            if not isinstance(pcit, dict):
                continue
            desc = pcit.get("description") or ""
            score = _jaccard(target_tokens, _tokens(desc))
            if score > best_score:
                best_score = score
                best_pcid = pcid
                best_idx = i
                best_item = pcit
    if best_score >= min_confidence and best_item is not None:
        return best_pcid, best_idx, best_item, best_score
    return None


# ── Public API ────────────────────────────────────────────────────────────

def match_packet_to_pcs(
    packet: Dict[str, Any],
    pcs: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Match every line item in the packet to a PC item.

    Returns:
        {
            "price_overrides": {row_idx: {"unit_cost": X, "unit_price": Y, ...}},
            "matched_count": int,
            "unmatched_count": int,
            "report": [
                {
                    "row_index": 1,
                    "packet_mfg": "DS8178",
                    "strategy": "mfg_number" | "solicitation" | "description" | "unmatched",
                    "confidence": 1.0,
                    "source_pc_id": "pc_abc",
                    "source_item_idx": 0,
                    "unit_cost": 295.00,
                    "unit_price": 395.00,
                    "reason": "matched on MFG#",
                },
                ...
            ],
        }
    """
    result: Dict[str, Any] = {
        "price_overrides": {},
        "matched_count": 0,
        "unmatched_count": 0,
        "report": [],
    }
    packet_items = packet.get("line_items") or []
    if not packet_items:
        return result

    # Strategy 0: try to find a PC matching the whole solicitation first.
    # If found, its items become the preferred search pool for MFG# + desc
    # matching (more precise than searching everything).
    sol_pc_hit = _match_by_solicitation(packet, pcs)
    sol_pool: Dict[str, Dict[str, Any]] = {}
    if sol_pc_hit:
        pcid, pc = sol_pc_hit
        sol_pool[pcid] = pc

    for packet_item in packet_items:
        row = int(packet_item.get("row_index", 0))
        report_entry = {
            "row_index": row,
            "packet_mfg": packet_item.get("mfg_number", ""),
            "packet_desc": (packet_item.get("description", "") or "")[:80],
        }

        # Try sol-scoped MFG# first
        hit = None
        if sol_pool:
            hit = _match_by_mfg_number(packet_item, sol_pool)
            if hit:
                report_entry["strategy"] = "solicitation+mfg_number"
                report_entry["confidence"] = 1.0

        # Fall through to full MFG# across all PCs
        if not hit:
            hit = _match_by_mfg_number(packet_item, pcs)
            if hit:
                report_entry["strategy"] = "mfg_number"
                report_entry["confidence"] = 1.0

        # Description fallback
        if not hit:
            desc_hit = _match_by_description(packet_item, pcs)
            if desc_hit:
                pcid, idx, pcit, score = desc_hit
                hit = (pcid, idx, pcit)
                report_entry["strategy"] = "description"
                report_entry["confidence"] = round(score, 2)

        if not hit:
            report_entry["strategy"] = "unmatched"
            report_entry["confidence"] = 0.0
            report_entry["reason"] = "no MFG# or description match found"
            result["unmatched_count"] += 1
            result["report"].append(report_entry)
            continue

        pcid, idx, pcit = hit
        unit_cost, unit_price = _pc_item_price(pcit)
        if unit_price <= 0 and unit_cost <= 0:
            # Match exists but has no pricing — still record so the
            # human can see it, but DON'T put it in price_overrides
            report_entry["reason"] = "matched but PC item has no cost/price"
            report_entry["source_pc_id"] = pcid
            report_entry["source_item_idx"] = idx
            report_entry["unit_cost"] = 0.0
            report_entry["unit_price"] = 0.0
            result["unmatched_count"] += 1
            result["report"].append(report_entry)
            continue

        # If we have a cost but no price yet, apply a default 25% markup
        # to compute a quoteable price. This is a last-resort fallback —
        # the PC flow should have user-confirmed prices, but a freshly
        # re-parsed PC might only have cost.
        if unit_price <= 0 and unit_cost > 0:
            unit_price = round(unit_cost * 1.25, 2)
            report_entry["reason"] = "applied default 25% markup (PC had cost only)"

        report_entry["source_pc_id"] = pcid
        report_entry["source_item_idx"] = idx
        report_entry["unit_cost"] = unit_cost
        report_entry["unit_price"] = unit_price
        result["report"].append(report_entry)
        result["price_overrides"][row] = {
            "unit_cost": unit_cost,
            "unit_price": unit_price,
            "source_pc_id": pcid,
            "source_item_idx": idx,
            "match_strategy": report_entry["strategy"],
            "confidence": report_entry["confidence"],
        }
        result["matched_count"] += 1

    return result


__all__ = [
    "match_packet_to_pcs",
    "_tokens",
    "_jaccard",
    "_normalize_mfg",
    "_normalize_sol",
]
