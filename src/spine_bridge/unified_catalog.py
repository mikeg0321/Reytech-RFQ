"""Unified catalog lookup — Spine-first, legacy-fallback.

Chrome MCP audit 2026-05-27 / G6 step 1 (Architect approval):
substrate-singleness gap closed at the READ layer. Spine catalog
(`spine_catalog` table, written by `spine.catalog.observe`) is the
canonical substrate per §0 LAW 1. Legacy `product_catalog`
(`src.agents.product_catalog.find_by_mfg_exact`) remains the
operator-asserted source for older operator-confirmed costs.

This module provides ONE canonical read primitive — new code (and
gradually-migrated old code) calls `lookup_by_mfg(mfg)`. It tries
Spine first; if Spine has nothing (substrate is still filling from
the URL→catalog write-through, PR #1140), falls back to legacy.

Why this is step 1 (not full collapse):
- Repointing every existing reader at once is high-risk substrate
  surgery. Each call site has subtly different consumer assumptions
  (cost_source field, freshness, identity-vs-fuzzy match).
- Building the canonical primitive AND making it AVAILABLE without
  forcing immediate adoption lets new code adopt it natively while
  old code migrates one site at a time in follow-up PRs.
- The shape of the returned dict is the canonical contract — once
  callers depend on it, the inner Spine-vs-legacy decision can be
  swapped without breaking them.

Subsequent steps (NOT in this PR):
- Repoint specific call sites (routes_pricecheck, routes_rfq) one
  by one with regression tests.
- Eventually delete legacy `find_by_mfg_exact` after the last reader
  is migrated.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger("reytech.spine_bridge.unified_catalog")


def _spine_db_path() -> str:
    """Same resolution as other spine_bridge modules. Env override
    first, then DATA_DIR/spine.db, last-ditch cwd fallback."""
    p = os.environ.get("SPINE_DB_PATH")
    if p:
        return p
    try:
        from src.core.paths import DATA_DIR
        return os.path.join(str(DATA_DIR), "spine.db")
    except Exception:
        return os.path.join(os.getcwd(), "data", "spine.db")


def _spine_entry_to_canonical(entry: dict) -> dict:
    """Convert spine_catalog row → unified result shape."""
    return {
        "source": "spine",
        "mfg_number": entry.get("mfg_number"),
        "cost_cents": entry.get("last_priced_cents"),
        "cost_dollars": (
            entry["last_priced_cents"] / 100.0
            if entry.get("last_priced_cents") else None
        ),
        "description": entry.get("canonical_description"),
        "uom": (entry.get("uoms_seen") or [None])[0] if isinstance(
            entry.get("uoms_seen"), list,
        ) else None,
        "last_priced_at": entry.get("last_priced_at"),
        "last_seen_at": entry.get("last_seen_at"),
        "last_priced_quote_id": entry.get("last_priced_quote_id"),
        "seen_count": entry.get("seen_count", 0),
        "is_stale": False,  # spine catalog tracks staleness via CATALOG_STALENESS_DAYS
    }


def _legacy_entry_to_canonical(entry: dict) -> dict:
    """Convert product_catalog row → unified result shape."""
    cost = entry.get("cost") or 0
    return {
        "source": "legacy",
        "mfg_number": entry.get("mfg_number"),
        "cost_cents": int(round(float(cost) * 100)) if cost else None,
        "cost_dollars": float(cost) if cost else None,
        "description": entry.get("name") or entry.get("description"),
        "uom": entry.get("uom"),
        "last_priced_at": (
            entry.get("cost_accepted_at") or entry.get("updated_at")
        ),
        "last_seen_at": entry.get("updated_at"),
        "last_priced_quote_id": None,
        "seen_count": entry.get("times_quoted", 0),
        "is_stale": False,
        "legacy_cost_source": entry.get("cost_source"),
        "legacy_product_id": entry.get("id"),
    }


def lookup_by_mfg(
    mfg_number: Optional[str],
    *,
    upc: Optional[str] = None,
    prefer: str = "spine",
) -> Optional[dict]:
    """Canonical catalog lookup by MFG#.

    Args:
        mfg_number: The MFG#/SKU to look up. Empty/None returns None.
        upc:        Optional UPC fallback (legacy only — Spine catalog
                    is keyed on normalized MFG# alone).
        prefer:     'spine' (default, canonical) — try Spine first,
                    fall back to legacy. 'legacy' — try legacy first,
                    fall back to Spine. 'spine_only' / 'legacy_only'
                    skip the fallback.

    Returns:
        Unified result dict with `source` field ('spine' or 'legacy')
        + cost_cents + description + last_priced_at + seen_count, or
        None when neither substrate has a match.

    Pure read — no side effects. Failures in one substrate fall
    through to the other; both failing returns None.
    """
    if not mfg_number or not mfg_number.strip():
        if not upc or not upc.strip():
            return None

    def _try_spine() -> Optional[dict]:
        try:
            from src.spine.catalog import get_entry
            db = _spine_db_path()
            if not Path(db).exists():
                return None
            entry = get_entry(db, mfg_number or "")
            if entry is None:
                return None
            # Only return if the entry has a usable cost — Spine rows
            # with no cost yet (URL→catalog write-through that hasn't
            # observed cost) are not useful for the cost-lookup
            # caller. Mirror the legacy gate (`cost > 0`).
            if not entry.get("last_priced_cents"):
                return None
            return _spine_entry_to_canonical(entry)
        except Exception as e:
            log.debug("spine catalog lookup failed: %s", e)
            return None

    def _try_legacy() -> Optional[dict]:
        try:
            from src.agents.product_catalog import find_by_mfg_exact
            entry = find_by_mfg_exact(mfg_number, upc=upc)
            if entry is None:
                return None
            return _legacy_entry_to_canonical(entry)
        except Exception as e:
            log.debug("legacy catalog lookup failed: %s", e)
            return None

    if prefer == "spine":
        return _try_spine() or _try_legacy()
    if prefer == "legacy":
        return _try_legacy() or _try_spine()
    if prefer == "spine_only":
        return _try_spine()
    if prefer == "legacy_only":
        return _try_legacy()

    # Unknown prefer value — fall through to default.
    return _try_spine() or _try_legacy()


__all__ = ["lookup_by_mfg"]
