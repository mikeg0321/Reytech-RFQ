"""Phase 1.1 fix — tax-lookup route falls back to buyer-side signals
when the primary delivery address is too short to resolve.

Live-drive context 2026-05-12: rfq_b57f85f7 had delivery_location="CA"
because Vision ingest missed the full address. The tax-lookup route
returned `ok:False, error:"No delivery address"` and the UI rendered
generic "❌ failed" — operator couldn't tell why it broke.

The fix walks the RFQ/PC record's other location signals (agency,
buyer_name, buyer_email, institution, ship_to_name) through the
facility_registry → CDTFA chain so a CalVet record with bad delivery
still picks up the correct Fresno facility rate via
buyer_email="@calvet.ca.gov" + buyer_name "Veterans Home of California
- Fresno".
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _read_py(path):
    with open(os.path.join(os.path.dirname(__file__), "..", path), encoding="utf-8") as f:
        return f.read()


# ── RFQ route fallback ───────────────────────────────────────────


def test_rfq_route_collects_fallback_hints_when_address_short():
    """The RFQ tax-lookup route must gather agency/buyer_name/buyer_email
    when the primary delivery address is < 5 chars, so the resolver can
    still match a canonical facility via those signals."""
    src = _read_py("src/api/modules/routes_rfq.py")
    start = src.index("def _api_lookup_tax_rate_locked(")
    body = src[start:start + 5000]
    # The hint-collection block must use the new fallback_hints list
    assert "fallback_hints = []" in body, (
        "Route must initialize fallback_hints list when address is short"
    )
    # Must look at all common buyer-side keys
    for key in ("agency", "buyer_name", "buyer_email",
                "institution", "ship_to_name"):
        assert f'"{key}"' in body, f"Route must include {key} in fallback hints"


def test_rfq_route_short_address_error_message_actionable():
    """When address is too short AND there are no buyer-side hints, the
    error must tell the operator what to do — not the old vague
    'No delivery address to look up'."""
    src = _read_py("src/api/modules/routes_rfq.py")
    start = src.index("def _api_lookup_tax_rate_locked(")
    body = src[start:start + 5000]
    assert "paste a full street + city + zip" in body, (
        "Short-address error must tell operator to paste a full address"
    )


def test_rfq_route_only_accepts_validated_primary_for_fallback_skip():
    """Even when the primary 'CA' resolves via the CA-base-rate default,
    the route must still try the buyer-side hints — default source must
    NOT count as 'primary_ok'."""
    src = _read_py("src/api/modules/routes_rfq.py")
    start = src.index("def _api_lookup_tax_rate_locked(")
    body = src[start:start + 6000]
    assert 'source") not in ("", "default")' in body, (
        "primary_ok must exclude empty + default source so fallback runs"
    )


# ── PC route fallback ────────────────────────────────────────────


def test_pc_route_collects_fallback_hints_when_address_short():
    """Symmetric to RFQ. PC records use requestor/agency/institution."""
    src = _read_py("src/api/modules/routes_pricecheck.py")
    start = src.index("def _api_pc_lookup_tax_rate_locked(")
    body = src[start:start + 5000]
    assert "fallback_hints = []" in body, (
        "PC route must initialize fallback_hints when ship_to is short"
    )
    for key in ("requestor", "agency", "institution"):
        assert f'"{key}"' in body, f"PC route must include {key} in fallback hints"


def test_pc_route_short_address_error_message_actionable():
    src = _read_py("src/api/modules/routes_pricecheck.py")
    start = src.index("def _api_pc_lookup_tax_rate_locked(")
    body = src[start:start + 5000]
    assert "paste a full street + city + zip" in body, (
        "Short ship-to error must tell operator to paste a full address"
    )


# ── JS error display ─────────────────────────────────────────────


def test_rfq_template_shows_actual_error_text():
    """The RFQ detail page must surface d.error from the route response,
    not a generic '❌ failed' that hides the cause."""
    src = _read_py("src/templates/rfq_detail.html")
    # Should no longer have the literal generic-failure innerHTML
    # The old line read:
    #   valEl.innerHTML='<span style="color:#f85149" title="..."> failed</span>'
    # New line includes "+ _short +" which is d.error sliced to 60 chars.
    assert "var _errText = d.error || 'failed';" in src, (
        "RFQ template must capture d.error for display"
    )
    # The visible text must include the truncated error, not just "failed"
    # (use the surrounding JS structure, not the unicode char, to keep the
    # assertion encoding-robust)
    assert "_short + '</span>'" in src or "+ _short + '</span>'" in src, (
        "RFQ template must render the (truncated) error text in the badge"
    )


def test_pc_template_shows_actual_error_text():
    src = _read_py("src/templates/pc_detail.html")
    assert "var _errText = d.error || 'Failed';" in src, (
        "PC template must capture d.error for display"
    )


# ── Auto-fire gating ─────────────────────────────────────────────


def test_rfq_auto_fire_skips_short_delivery():
    """The auto-fire on RFQ load must not run when delivery_location is
    < 5 chars — otherwise every page load with bad ingest data spams
    the UI with '❌ Delivery address too short' badges."""
    src = _read_py("src/templates/rfq_detail.html")
    # The new Jinja block uses _del|length >= 5 as a guard
    assert "_del|length >= 5" in src, (
        "RFQ auto-fire must gate on delivery_location length >= 5"
    )


# ── Resolver still works for valid CalVet inputs ─────────────────


def test_resolve_tax_via_fresno_hint_finds_calvet_facility():
    """Sanity check the fallback chain end-to-end: passing 'Fresno' or
    'Veterans Home of California - Fresno' through resolve_tax must
    return a non-default source via facility_registry."""
    from src.core.tax_resolver import resolve_tax
    for hint in ("Veterans Home of California - Fresno",
                 "Fresno",
                 "93706"):
        res = resolve_tax(hint)
        assert res.get("rate") is not None, f"{hint!r} should resolve"
        # Source must be from a canonical path, NOT the CA base default
        assert res.get("source") not in ("", "default"), (
            f"{hint!r} returned source={res.get('source')!r}; expected a non-default canonical source"
        )
        # Facility code should be CalVet Fresno
        assert res.get("facility_code") == "CALVETHOME-FR", (
            f"{hint!r} should resolve to CALVETHOME-FR, got {res.get('facility_code')!r}"
        )
