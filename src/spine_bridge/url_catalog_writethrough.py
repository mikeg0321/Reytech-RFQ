"""URL lookup → Spine catalog write-through.

Pillar 3 / G9 (chrome MCP audit 2026-05-26): every successful URL
lookup (`item_link_lookup.lookup_from_url`) was a one-shot — the
operator-facing result was returned but the Spine catalog never
got richer. Over time the catalog's MFG#/description coverage
plateaus relative to what URL lookups have already proven exists.

This module is the substrate bridge: takes a `lookup_from_url`
result dict, validates the minimum fields needed for a catalog
observation, and writes one row to spine_catalog via the canonical
`observe()` writer.

Architectural rules:
  - Pure adapter — no business logic beyond data-quality gates.
  - Skips writes that would pollute the catalog (garbage title,
    missing MFG#, $0 price, login-required errors).
  - Failures are non-fatal — the URL lookup's own return value
    must still flow to the caller even if the write-through trips.
  - Single canonical writer preserved (spine.catalog.observe is the
    only function that writes spine_catalog; this is a caller).

Wiring point: called from `item_link_lookup.lookup_from_url` at the
success-path return. Other callers (admin URL-paste, scrape clients)
can call it directly if they have their own result dict.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger("reytech.spine_bridge.url_catalog")


# Titles the URL scrapers sometimes return when a bot stub or
# landing page substitutes for the real product page. Mirror of
# the heuristic in `item_link_lookup._is_garbage_title` (kept local
# so this module doesn't import the 2400-LOC lookup file just to
# check 6 strings).
_GARBAGE_TITLE_TOKENS = frozenset({
    "amazon.com",
    "404",
    "page not found",
    "captcha",
    "robot check",
    "access denied",
    "are you a robot",
    "sign in",
    "log in",
})


def _looks_like_garbage_title(title: str) -> bool:
    if not title:
        return True
    t = title.strip().lower()
    if len(t) < 3:
        return True
    return any(tok in t for tok in _GARBAGE_TITLE_TOKENS)


def _spine_db_path() -> str:
    """Resolve the Spine DB path. Mirrors the resolution used in
    `routes_spine.py` and the audit's PR-A fix.

    Env override first, then DATA_DIR/spine.db, last-ditch cwd
    fallback. Caller can pass an explicit override.
    """
    p = os.environ.get("SPINE_DB_PATH")
    if p:
        return p
    try:
        from src.core.paths import DATA_DIR
        return os.path.join(str(DATA_DIR), "spine.db")
    except Exception:
        return os.path.join(os.getcwd(), "data", "spine.db")


def observe_url_lookup(
    result: dict,
    *,
    actor: str = "url_lookup_writethrough",
    db_path: Optional[str] = None,
) -> Optional[dict]:
    """Write one observation to spine_catalog from a URL-lookup result.

    Returns the observation metadata dict on success, None when
    skipped. Never raises — write-through failures are logged but
    the caller's path stays intact.

    Args:
        result:  Dict returned by `item_link_lookup.lookup_from_url`.
                 Expected keys (any may be absent): mfg_number,
                 part_number, title, description, price, list_price,
                 sale_price, url, supplier, error, login_required.
        actor:   Audit field on spine_catalog rows. Defaults to
                 "url_lookup_writethrough" — distinguish from
                 operator-typed observations + ingest observations.
        db_path: Override the Spine DB path. Default uses env →
                 DATA_DIR → cwd fallback chain.

    Skip rules (return None without writing):
      - result has `error` set + `login_required` true → don't
        pollute catalog with auth-failure results.
      - mfg_number / part_number both empty → no identity, no
        usable observation.
      - price <= 0 across all price fields → no economic signal.
      - title is "garbage" per the heuristic → likely bot stub.
    """
    if not isinstance(result, dict):
        return None

    # Skip auth failures explicitly — these results carry no useful
    # catalog signal, only a "ask the operator to paste" sentinel.
    if result.get("login_required") and not result.get("price"):
        return None
    if result.get("error") and not (
        result.get("price") or result.get("list_price") or result.get("sale_price")
    ):
        return None

    mfg = (result.get("mfg_number") or result.get("part_number")
           or result.get("asin") or "").strip()
    if not mfg:
        log.debug("url_catalog_writethrough: skip — no mfg_number")
        return None

    description = (
        result.get("description") or result.get("title") or ""
    ).strip()
    title_for_garbage_check = result.get("title") or result.get("description") or ""
    if _looks_like_garbage_title(title_for_garbage_check):
        log.debug(
            "url_catalog_writethrough: skip — garbage title %r",
            title_for_garbage_check[:60],
        )
        return None
    if not description:
        return None

    # Pick the best price signal. Prefer MSRP/list (cost basis) over
    # sale (volatile). Convert dollars-to-cents.
    price_dollars = None
    for k in ("list_price", "price", "sale_price"):
        v = result.get(k)
        try:
            f = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            continue
        if f > 0:
            price_dollars = f
            break
    if price_dollars is None or price_dollars <= 0:
        log.debug("url_catalog_writethrough: skip — no positive price")
        return None
    cost_cents = int(round(price_dollars * 100))

    db = db_path or _spine_db_path()
    if not Path(db).exists():
        # Don't auto-create — the catalog substrate is initialized
        # elsewhere (routes_spine.py at boot). If it's not there,
        # this write isn't this module's responsibility to fix.
        log.debug("url_catalog_writethrough: skip — spine db not initialized")
        return None

    try:
        from src.spine.catalog import observe
        return observe(
            db,
            mfg_number=mfg,
            description=description[:500],
            uom=None,  # URL lookups rarely surface UOM cleanly
            unspsc=None,
            quote_id=None,  # No quote context on a raw URL lookup
            cost_cents=cost_cents,
            actor=actor,
        )
    except Exception as e:
        log.warning(
            "url_catalog_writethrough: observe failed for mfg=%s: %s",
            mfg, e,
        )
        return None


__all__ = ["observe_url_lookup"]
