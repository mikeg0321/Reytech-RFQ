"""Bundle-1 PR-1c: unified tax-rate resolution.

Source: audit item Y in the 2026-04-22 session audit. Two code paths
diverged on the same RFQ:
- `/api/rfq/<rid>/lookup-tax-rate` (endpoint) parsed the address
  with one regex, fell through to `parse_ship_to()`, and when that
  failed returned 7.25% CA base with no validation flag.
- `quote_generator.generate_quote_from_rfq()` called
  `_lookup_facility()` → `get_rate_for_facility()`, which used the
  facility's canonical zip and got 7.75% from CDTFA.

Same record, different rates. The UI showed 7.25% stale; the quote
PDF printed 7.75%. Operator can't trust the app.

### Contract
`resolve_tax(address)` is the **one** function both paths should call.
It tries canonical facility lookup FIRST (most trustworthy signal),
then falls back to regex parsing (PR #463 Audit X parser already
handles facility-led + comma-optional + zip-less inputs), then CDTFA.

Returns a dict with a normalized shape:
```
{
    "ok": bool,
    "rate": float | None,         # 0.0 <= rate <= 0.15
    "jurisdiction": str,          # "WASCO" / "CALIFORNIA (BASE)"
    "city": str,
    "county": str,
    "source": str,                # cdtfa_api | cache | fallback | default
    "facility_code": str,         # "WSP" | "" when not resolved via registry
    "resolve_reason": str,        # tax_resolver-level reason
                                  # (facility_registry | address_parse | default)
    "validated": bool,            # True only on cdtfa_api / cache source
}
```

Every caller — the endpoint and the quote generator — should read
the SAME dict shape, so "displayed == persisted == delivered" can
never drift again.

### What this module is NOT
Not a reimplementation of CDTFA logic. It wraps the existing
`src.core.tax_rates.lookup_tax_rate` + `src.agents.tax_agent.get_tax_rate`
APIs so all the caching, persistence, and upstream error handling
flows through. The value added here is a single call site + a
normalized response + facility-first priority.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional

log = logging.getLogger("reytech.tax_resolver")


def _empty_response(reason: str = "empty_input") -> Dict[str, Any]:
    return {
        "ok": False,
        "rate": None,
        "jurisdiction": "",
        "city": "",
        "county": "",
        "source": "",
        "facility_code": "",
        "resolve_reason": reason,
        "validated": False,
    }


def _normalize(
    payload: Dict[str, Any],
    facility_code: str = "",
    resolve_reason: str = "",
) -> Dict[str, Any]:
    """Shape the upstream tax_rates response into the unified dict.
    Validated = True only when the source is trustworthy
    (cdtfa_api / cache / persisted_cache); hardcoded fallbacks and
    the CA base default count as not-validated."""
    source = str(payload.get("source", ""))
    validated = source in ("cdtfa_api", "cache", "persisted_cache")
    return {
        "ok": payload.get("rate") is not None,
        "rate": payload.get("rate"),
        "jurisdiction": payload.get("jurisdiction", ""),
        "city": payload.get("city", ""),
        "county": payload.get("county", ""),
        "source": source,
        "facility_code": facility_code,
        "resolve_reason": resolve_reason,
        "validated": validated,
    }


def _parse_address(address: str) -> Dict[str, str]:
    """Extract street / city / zip from free-text address. Mirrors
    the parser shipped via PR #463 (Audit X) — facility-led and
    comma-optional state suffix both work."""
    if not address:
        return {"street": "", "city": "", "zip": ""}
    zip_matches = re.findall(r"\b(\d{5})\b", address)
    z = zip_matches[-1] if zip_matches else ""
    city_match = (
        re.search(r",\s*([A-Za-z\s]+),?\s*[A-Z][A-Za-z]\.?\s*\d{5}", address) or
        re.search(r",\s*([A-Za-z][A-Za-z\s]+?)\s*,\s*[A-Z]{2}", address)
    )
    city = city_match.group(1).strip() if city_match else ""
    street_match = re.search(r"(?:^|,\s*)(\d+\s+[^,\n]+?)(?=,|$)", address)
    street = street_match.group(1).strip() if street_match else ""
    return {"street": street, "city": city, "zip": z}


def resolve_tax(address: str, force_live: bool = False) -> Dict[str, Any]:
    """Single entry point for tax-rate resolution.

    Priority order:
      1. **facility_registry match** — if the address resolves to a
         canonical facility, use that facility's zip for CDTFA
         lookup. Most trustworthy because the zip is audited.
      2. **address parser** — zip extracted from the raw string.
      3. **CDTFA fallback / default** — handled upstream by
         `src.core.tax_rates.lookup_tax_rate`.

    `force_live=True` bypasses the local cache and forces a fresh
    CDTFA call (mirrors the existing `?force_live=1` knob on
    `/api/rfq/<rid>/lookup-tax-rate`).

    Never raises. Returns a normalized dict (see module docstring).
    Callers should trust `validated=True` as "this came from a real
    CDTFA hit"; `validated=False` means "we had to fall back."
    """
    if not address or not str(address).strip():
        return _empty_response("empty_input")

    addr = str(address).strip()

    # Both branches below call `tax_agent.get_tax_rate` — the SAME
    # underlying API the `/api/rfq/<rid>/lookup-tax-rate` endpoint
    # uses. That's the audit Y guarantee: as long as both callers
    # land on the same CDTFA + cache layer, the rate they get back
    # for a given input cannot diverge.

    # ── 1. Try canonical facility registry first ──
    facility_code = ""
    try:
        from src.core.facility_registry import resolve_with_reason
        record, reg_reason = resolve_with_reason(addr)
        if record:
            facility_code = record.code
            try:
                from src.agents.tax_agent import get_tax_rate
                # Strip the city out of the canonical "city, CA zip"
                # second address line so get_tax_rate gets a clean
                # city argument.
                _city = record.address_line2.split(",")[0].strip()
                payload = get_tax_rate(
                    street=record.address_line1,
                    city=_city,
                    zip_code=record.zip,
                    force_live=force_live,
                )
                return _normalize(
                    payload,
                    facility_code=facility_code,
                    resolve_reason=f"facility_registry:{reg_reason}",
                )
            except Exception as e:
                log.debug(
                    "get_tax_rate crashed on facility %s: %s",
                    record.code, e,
                )
        # `reg_reason` is one of: exact / substring_unique / zip_unique /
        # ambiguous_substring / ambiguous_zip / no_match / empty_input
        # When the registry returned None we fall through to address
        # parsing — don't log errors at WARNING level because most
        # free-text addresses are expected to miss the registry.
    except Exception as e:
        log.debug("facility_registry.resolve crashed: %s", e)

    # ── 2. Parse address + CDTFA ──
    parsed = _parse_address(addr)
    try:
        from src.agents.tax_agent import get_tax_rate
        if parsed["street"] and parsed["city"] and parsed["zip"]:
            payload = get_tax_rate(
                street=parsed["street"],
                city=parsed["city"],
                zip_code=parsed["zip"],
                force_live=force_live,
            )
        else:
            # Use tax_agent's own ship-to parser as the last-resort
            # parser — this is the same fallback `api_lookup_tax_rate`
            # uses, so behavior matches.
            from src.agents.tax_agent import parse_ship_to
            _parts = [p.strip() for p in addr.split(",")]
            tap = parse_ship_to("", _parts)
            payload = get_tax_rate(
                street=tap.get("street", ""),
                city=tap.get("city", ""),
                zip_code=tap.get("zip", ""),
                force_live=force_live,
            )
        return _normalize(
            payload,
            facility_code="",
            resolve_reason="address_parse",
        )
    except Exception as e:
        log.error("resolve_tax: get_tax_rate crashed: %s", e, exc_info=True)
        return _empty_response("lookup_crashed")
