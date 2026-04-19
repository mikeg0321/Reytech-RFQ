"""
Agency quote profile — static fallbacks for agencies without enough history.

Before this module, only agencies with >= _CAL_MIN_SAMPLES real won quotes in
institution_pricing_profile had a profile surfaced on the Oracle intelligence
panel. CalVet and every other non-CCHCS agency fell back to a blank panel,
even though agency_config.py had sensible defaults (markup %, payment terms,
shipping terms).

resolve_agency_profile(agency) returns a dict shaped like the V5
institution_profile block so callers can use it interchangeably:

    {
        "institution": "calvet",
        "name": "Cal Vet / DVA",
        "avg_winning_markup": 25.0,
        "price_sensitivity": "normal",
        "source": "agency_config_default",
        "payment_terms": "Net 30",
        "shipping_terms": "FOB Destination, Freight Prepaid",
        "delivery_days": "7-14 business days",
        "total_quotes": 0,   # 0 signals "synthetic"
        "win_rate": None,    # None signals "no history"
    }

Callers that render the panel should treat `source == "agency_config_default"`
as "using configured defaults, no real quote history yet" rather than hiding
the block. This is the gap the 2026-04-18 audit flagged: CalVet quote
intelligence was silently blank, operators had no signal at all.
"""
from __future__ import annotations

import logging
from typing import Optional

log = logging.getLogger("reytech.agency_quote_profile")


def _infer_sensitivity(markup_pct: float) -> str:
    """Cheap heuristic so non-CCHCS agencies land in a reasonable bucket.

    Low markup target (<=20%) = buyer is price-sensitive, compete harder.
    High markup target (>=30%) = buyer tolerates premium, less sensitive.
    Otherwise "normal".
    """
    if markup_pct <= 20:
        return "high"  # price-sensitive
    if markup_pct >= 30:
        return "low"   # premium-tolerant
    return "normal"


def resolve_agency_profile(agency: str) -> Optional[dict]:
    """Return a static agency_config-derived profile, or None if unknown.

    Input: agency string (any case, any alias the resolver knows).
    Output: dict with the same shape as the V5 institution_profile block,
    plus source='agency_config_default' and helpful context fields.
    """
    if not agency:
        return None

    try:
        from src.core.agency_config import DEFAULT_AGENCY_CONFIGS, match_agency
    except Exception as e:
        log.debug("agency_config import failed: %s", e)
        return None

    # Try direct key lookup first (cchcs, calvet, dsh, ...) then alias match.
    key = (agency or "").strip().lower()
    cfg = DEFAULT_AGENCY_CONFIGS.get(key)
    if not cfg:
        try:
            resolved_key, resolved_cfg = match_agency(agency)
            if resolved_cfg:
                key, cfg = resolved_key, resolved_cfg
        except Exception as e:
            log.debug("match_agency failed for %r: %s", agency, e)

    if not cfg:
        return None

    markup = float(cfg.get("default_markup_pct") or 25)
    return {
        "institution": key,
        "name": cfg.get("name") or agency,
        "avg_winning_markup": markup,
        "price_sensitivity": _infer_sensitivity(markup),
        "source": "agency_config_default",
        "payment_terms": cfg.get("payment_terms") or "Net 30",
        "shipping_terms": cfg.get("shipping_terms") or "FOB Destination",
        "delivery_days": cfg.get("delivery_days") or "7-14 business days",
        "total_quotes": 0,
        "win_rate": None,
    }
