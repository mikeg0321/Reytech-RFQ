"""Quote ↔ Quote matcher — finds prior PC predecessors for a new RFQ.

The pure-function matcher lives in the Spine package (not spine_bridge)
because the scoring rules are part of the substrate's contract with the
operator: changing the threshold or weights changes what auto-links
happen. Substrate rules are versioned + tested here; spine_bridge only
does the I/O (read candidates, write the link).

Closes Mike's 5/17 directive ("PC goes out, RFQ comes in, should be
auto priced") at the matching layer. The auto-price substrate (task
#20, queued) reads the link to copy validated costs forward.

DESIGN
- Hard filter: same facility. Cross-facility links are almost always
  wrong by default (different ship-to → different ZIP → different tax
  → different costs).
- Three soft signals, combined into a single confidence in [0, 1]:
    1. same_solicitation_number — strong (buyer references prior bid).
    2. mfg_overlap_ratio — share of target.line_items whose MFG#
       appears in the candidate. Strong when present.
    3. desc_jaccard — token-set Jaccard over all line-item descriptions.
       Used as tiebreaker when MFG# data is sparse.
- AUTO_LINK_THRESHOLD = 0.50 — below this we don't auto-link; the
  operator can still manually create a link with confidence 1.0.
- Self-comparison filtered at this layer (find_pc_candidates skips
  the target quote_id even if it's in candidates).
"""
from __future__ import annotations

import re
from collections.abc import Iterable

from src.spine.model import Quote

# Below this combined confidence we do NOT auto-link. The operator can
# still create a link manually; this only governs the auto-linker.
AUTO_LINK_THRESHOLD = 0.50

# Signal weights — sum to 1.0. Adjusting these changes auto-link behavior
# substrate-wide, so changes must come with test updates.
_WEIGHT_SOLICITATION = 0.50
_WEIGHT_MFG = 0.35
_WEIGHT_DESC = 0.15

# Description token min length — single-letter tokens ("a", "to") are
# pure noise.
_TOKEN_MIN_LEN = 3
_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def _norm_mfg(s: str | None) -> str | None:
    """Normalize an MFG# for comparison. Strips whitespace, uppercases,
    drops trailing punctuation. Returns None when there's nothing
    meaningful left (line item had no MFG#)."""
    if s is None:
        return None
    out = s.strip().upper().rstrip(".,;:")
    return out or None


def _desc_tokens(s: str) -> set[str]:
    """Tokenize a description for Jaccard comparison. Lowercase
    alphanumeric runs of length ≥ _TOKEN_MIN_LEN."""
    return {
        t.lower()
        for t in _TOKEN_RE.findall(s or "")
        if len(t) >= _TOKEN_MIN_LEN
    }


def score_quote_pair(target: Quote, candidate: Quote) -> dict:
    """Compute match signals between target (newer) and candidate (prior).

    Returns dict:
        {
            "confidence": float in [0, 1],
            "evidence": {
                "same_facility": bool,
                "same_solicitation_number": bool,
                "mfg_overlap_ratio": float,
                "mfg_matches_count": int,
                "target_mfg_count": int,
                "desc_jaccard": float,
            },
        }

    Confidence is 0.0 when the same-facility filter fails OR when both
    soft signals (sol# / MFG#) are zero — that combination doesn't
    pass the "actually related" bar even if descriptions overlap.
    """
    same_facility = target.facility.strip().upper() == candidate.facility.strip().upper()
    same_sol = target.solicitation_number.strip() == candidate.solicitation_number.strip()

    target_mfgs = {m for m in (_norm_mfg(li.mfg_number) for li in target.line_items) if m}
    cand_mfgs = {m for m in (_norm_mfg(li.mfg_number) for li in candidate.line_items) if m}
    mfg_matches = target_mfgs & cand_mfgs
    mfg_overlap_ratio = (len(mfg_matches) / len(target_mfgs)) if target_mfgs else 0.0

    target_tokens: set[str] = set()
    for li in target.line_items:
        target_tokens |= _desc_tokens(li.description)
    cand_tokens: set[str] = set()
    for li in candidate.line_items:
        cand_tokens |= _desc_tokens(li.description)
    union = target_tokens | cand_tokens
    desc_jaccard = (len(target_tokens & cand_tokens) / len(union)) if union else 0.0

    evidence = {
        "same_facility": same_facility,
        "same_solicitation_number": same_sol,
        "mfg_overlap_ratio": round(mfg_overlap_ratio, 3),
        "mfg_matches_count": len(mfg_matches),
        "target_mfg_count": len(target_mfgs),
        "desc_jaccard": round(desc_jaccard, 3),
    }

    if not same_facility:
        return {"confidence": 0.0, "evidence": evidence}

    # If neither sol# nor MFG# signals fire, refuse to auto-link even
    # if descriptions overlap. Descriptions alone are too noisy
    # (commodity catalogs full of "bandage" / "gloves" / "syringe").
    if not same_sol and mfg_overlap_ratio == 0.0:
        return {"confidence": 0.0, "evidence": evidence}

    confidence = (
        _WEIGHT_SOLICITATION * (1.0 if same_sol else 0.0)
        + _WEIGHT_MFG * mfg_overlap_ratio
        + _WEIGHT_DESC * desc_jaccard
    )
    confidence = round(min(1.0, max(0.0, confidence)), 3)
    return {"confidence": confidence, "evidence": evidence}


def find_pc_candidates(
    target: Quote,
    candidates: Iterable[Quote],
    *,
    min_confidence: float = AUTO_LINK_THRESHOLD,
) -> list[dict]:
    """Score every candidate against `target` and return matches
    above `min_confidence`, sorted by confidence DESC.

    Args:
        target:         The newer Quote (typically an inbound RFQ).
        candidates:     Iterable of prior Quotes to score against.
                        `target` is filtered out if present.
        min_confidence: Lower bound; defaults to AUTO_LINK_THRESHOLD.

    Returns:
        List of dicts: {quote_id, confidence, evidence}, sorted by
        confidence DESC. Empty list when nothing meets the bar.
    """
    scored: list[dict] = []
    for c in candidates:
        if c.quote_id == target.quote_id:
            continue
        s = score_quote_pair(target, c)
        if s["confidence"] >= min_confidence:
            scored.append({
                "quote_id": c.quote_id,
                "confidence": s["confidence"],
                "evidence": s["evidence"],
            })
    scored.sort(key=lambda d: d["confidence"], reverse=True)
    return scored
