"""QuoteContract — the frozen single source of truth for any PDF
that gets rendered from a Price Check or RFQ.

## Why this exists

Product-engineer review 2026-04-24 of tonight's 12-PR session:

> The end-to-end path that turns a buyer email into a sendable
> Quote+Package isn't audited as a single contract — it's a chain
> of independent generators that each pull from slightly different
> fields and slightly different snapshots.
>
> `tax_resolver.resolve_tax()` reads `FacilityRecord` ✓
> `quote_generator.generate_quote_from_rfq()` reads its OWN
> `FACILITY_DB` (PR #501 collapsed it, but the shape stays).
> `package_generator`, `fill_703c`, `fill_704b`, `compliance_validator`
> all do their OWN facility/agency lookup with different priority
> orders.
>
> **How to apply:** Stop shipping per-symptom fixes. Treat "clean
> 1-item quote end-to-end" as a single contract. Every renderer
> reads from the SAME canonical snapshot.

This module IS that snapshot. The flow:

  1. At PC → RFQ convert time, call `assemble_from_rfq(rfq)` ONCE.
     Facility is resolved via `facility_registry.resolve()` (the
     canonical registry). Tax rate comes from the same facility
     record. Prices come from the operator's last saved unit_price.
  2. Every downstream renderer (quote PDF, package PDF, 703b/703c/704b
     fillers, compliance validator) receives the `QuoteContract` as
     a parameter and renders from its frozen fields. No renderer
     calls `facility_registry` / `institution_resolver` /
     `tax_resolver` on its own.
  3. An architecture test (`tests/test_architecture_contract.py`)
     fails CI if any module under `src/forms/` or
     `src/agents/packet*` imports the canonical resolvers directly.
     The allowlist shrinks over time as each renderer migrates.

## Contract is FROZEN

`@dataclass(frozen=True)` prevents any renderer from mutating fields
mid-render. If a renderer needs a transformed value (e.g., shipping
address split into two lines), it does that locally without
modifying the source contract.

## What this module is NOT

Not a cache. Not a DB table (yet). Not a Claude-composed artifact.
Just a deterministic projection of canonical registry state into the
shape every renderer needs. If the canonical registry changes, call
`assemble_from_rfq()` again to get a fresh contract.

## Next milestones after this PR

- Migrate `generate_quote_from_rfq` to receive a contract (THIS PR,
  partial — assembles the contract, uses `contract.facility` for
  the ship-to resolve; other call sites still use legacy lookup).
- Migrate 703b/703c/704b/package fillers to `fill_xxx(contract,
  output_path)` signatures. (Next PR.)
- Delete `quote_generator._lookup_facility_legacy` and the local
  `FACILITY_DB` / `_CITY_MAP` fallback. (Once no renderer needs it.)
- Migrate `institution_resolver._FACILITY_ADDRESSES` into
  `FacilityRecord.mailing_address` fields on the canonical registry.
  (Currently institution_resolver carries a parallel-universe dict.)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Tuple

log = logging.getLogger("reytech.quote_contract")


@dataclass(frozen=True)
class LineItem:
    """One line on the quote. All money in cents (integer) — no
    floating-point drift through the contract."""
    description: str
    quantity: int
    unit_price_cents: int
    mfg_number: str = ""
    uom: str = "EA"
    # Per-line tax override — None means "use the contract's tax rate".
    # Rare; most lines inherit contract tax.
    tax_rate_bps_override: Optional[int] = None

    @property
    def extended_cents(self) -> int:
        return self.quantity * self.unit_price_cents


@dataclass(frozen=True)
class QuoteContract:
    """Frozen snapshot of everything a renderer needs. Assembled ONCE
    per quote attempt; consumed by every PDF generator, email builder,
    and compliance validator downstream.

    ## Identity fields (from canonical facility_registry)

    `facility` — the matched `FacilityRecord` when the resolver had
    enough signal. None when the ship-to is unresolvable; renderers
    should fall back to `ship_to_raw` display and flag operator.

    `agency_code` / `agency_full` — mirrored from `facility.parent_*`
    when present, otherwise derived from `agency_hint`. Exposed at
    the top level so renderers don't have to reach through .facility.

    `ship_to_raw` — buyer's verbatim text, kept as audit trace. Never
    used for display — displays always go through `facility.address()`
    or, if facility is None, the render path's own last-resort.

    ## Commerce fields (from operator-set PC/RFQ values)

    `line_items` — tuple so renderers can iterate without accidental
    mutation. Integer cents so tax math is exact.

    `tax_rate_bps` — basis points (2500 = 0.25 = 25%) for integer-math
    consistency across renderers. Sourced from
    `tax_resolver.resolve_tax(facility)` at assembly time.

    `tax_jurisdiction` — human-readable ("BARSTOW", "CALIFORNIA (BASE)")
    for the Quote PDF footer.

    ## Provenance fields (audit trail)

    Used by future dashboards + support investigations — "which PR
    state generated this quote?" — without baking them into the PDF.
    """
    # Identity
    facility: Optional["FacilityRecord"]
    agency_code: str
    agency_full: str
    ship_to_raw: str
    # Slug from `facility_registry.resolve_with_reason()` describing
    # which path resolved this contract's facility:
    #   "agency_key"           — agency_key (e.g. calvet_barstow) won
    #   "exact"                — input matched a canonical alias verbatim
    #   "substring_unique"     — single substring match
    #   "zip_unique"           — single zip match
    #   "ambiguous_substring"  — multiple substring candidates → no match
    #   "ambiguous_zip"        — multiple zip candidates → no match
    #   "no_match"             — registry didn't find anything
    #   "empty_input"          — caller passed empty text
    # Renderers writing audit fields (`ship_to_resolve_reason` on the
    # quote dict) should read this from the contract instead of
    # importing `facility_registry` directly.
    ship_to_resolve_reason: str = ""

    # Commerce
    line_items: Tuple[LineItem, ...] = ()
    tax_rate_bps: int = 0
    # Decimal form of `tax_rate_bps` for renderers that want a 0.xxxx
    # value (e.g., quote PDF footer "Tax: 8.75%"). Same number, two
    # representations — bps stays canonical for integer tax math, decimal
    # is the convenience accessor. `0.0875 == 875 bps`.
    tax_rate: float = 0.0
    tax_jurisdiction: str = ""
    tax_source: str = ""  # "facility_registry" | "cdtfa_api" | "fallback"
    tax_validated: bool = False

    # Provenance
    source_pc_id: str = ""
    source_rfq_id: str = ""
    assembled_at: str = ""  # ISO datetime
    contract_version: int = 1

    # ── Derived convenience views ────────────────────────────────

    @property
    def subtotal_cents(self) -> int:
        return sum(li.extended_cents for li in self.line_items)

    @property
    def tax_cents(self) -> int:
        # Integer math: subtotal × bps ÷ 10000, rounded half-to-even.
        # Matches standard CA sales-tax rounding convention.
        return round(self.subtotal_cents * self.tax_rate_bps / 10000)

    @property
    def total_cents(self) -> int:
        return self.subtotal_cents + self.tax_cents

    @property
    def ship_to_address_lines(self) -> Tuple[str, ...]:
        """The 2-line ship-to block for PDF rendering. Pulls from the
        canonical facility when available; falls back to raw operator
        text otherwise. No ad-hoc parsing or city-fallback — if the
        registry didn't resolve, the raw stays raw."""
        if self.facility is not None:
            return (self.facility.address_line1, self.facility.address_line2)
        return (self.ship_to_raw,) if self.ship_to_raw else ()

    @property
    def ship_to_name(self) -> str:
        """The facility name for the ship-to header. Uses the canonical
        name when resolved; empty string when not (renderers should
        flag this to the operator, not silently print a guess)."""
        if self.facility is not None:
            return self.facility.canonical_name
        return ""


# ── Assembly ─────────────────────────────────────────────────────


def _empty_contract(ship_to_raw: str = "", source_pc_id: str = "",
                    source_rfq_id: str = "") -> QuoteContract:
    """Sentinel contract used when the caller doesn't supply enough
    to resolve anything. Renderers must treat this as "operator-review
    required" — don't ship a PDF from an empty contract."""
    return QuoteContract(
        facility=None,
        agency_code="",
        agency_full="",
        ship_to_raw=ship_to_raw,
        ship_to_resolve_reason="empty_input",
        line_items=(),
        tax_rate_bps=0,
        tax_rate=0.0,
        tax_jurisdiction="",
        tax_source="",
        tax_validated=False,
        source_pc_id=source_pc_id,
        source_rfq_id=source_rfq_id,
        assembled_at=datetime.now(timezone.utc).isoformat(),
    )


# ── Public resolver facades for renderer use ─────────────────────
# These wrap the canonical `facility_registry` / `tax_resolver` calls
# so every renderer can import ONE module (quote_contract) instead
# of reaching into 3+ resolver modules directly. The architecture
# test blocks renderers from the resolvers; these facades are the
# bridge. Renderers that need "the facility for this text" call
# `resolve_facility_for_text(text)`; renderers that need "the tax
# rate for this facility" call `tax_for_facility(facility)`. The
# canonical contract is still the preferred entry point — these are
# for edge-case fallbacks (email-body text that isn't in any RFQ
# field, for example).


def resolve_facility_for_text(text: str) -> Tuple[Optional["FacilityRecord"], str]:
    """Return `(FacilityRecord | None, reason_slug)` for free text.
    Thin facade over `facility_registry.resolve_with_reason` so
    renderers don't have to import the registry directly."""
    if not text or not str(text).strip():
        return None, "empty_input"
    try:
        from src.core.facility_registry import resolve_with_reason
    except Exception as e:
        log.debug("facility_registry import failed in facade: %s", e)
        return None, "no_match"
    try:
        rec, reason = resolve_with_reason(str(text).strip())
    except Exception as _e:
        return None, "no_match"
    return rec, reason or "no_match"


def resolve_facility_for_agency_key(agency_key: str) -> Optional["FacilityRecord"]:
    """Return the canonical `FacilityRecord` for a facility-specific
    agency_key (e.g., `calvet_barstow`). Returns None for generic parent
    keys (cdcr / calvet / cchcs) that don't pin a child facility.
    Thin facade over `facility_registry.resolve_by_agency_key`."""
    if not agency_key:
        return None
    try:
        from src.core.facility_registry import resolve_by_agency_key
    except Exception as e:
        log.debug("facility_registry.resolve_by_agency_key unavailable: %s", e)
        return None
    try:
        return resolve_by_agency_key(agency_key)
    except Exception:
        return None


def all_canonical_facilities() -> list:
    """Enumerate every canonical `FacilityRecord`. Used by zip-lookup
    paths that scan all facilities for a zip match. Thin facade over
    `facility_registry.all_facilities`."""
    try:
        from src.core.facility_registry import all_facilities
    except Exception as e:
        log.debug("facility_registry.all_facilities unavailable: %s", e)
        return []
    try:
        return list(all_facilities())
    except Exception:
        return []


def ship_to_for_text(text: str) -> str:
    """Return a comma-joined ship-to address for free-text input, or "" on
    no match. Resolves through canonical `facility_registry.resolve()` then
    returns `"line1, line2"` from the FacilityRecord — never the raw
    operator text. The output format mirrors what
    `institution_resolver.get_ship_to_address` historically returned, so
    callers migrating off institution_resolver get a drop-in replacement
    without changing string assumptions downstream.

    Used by `ship_to_resolver.lookup_buyer_ship_to` as the canonical
    fallback after PO history + CRM lookup miss. Going through this facade
    instead of importing `institution_resolver.get_ship_to_address`
    directly keeps ship_to_resolver out of the architectural-ratchet
    allowlist and lets a future PR delete `_FACILITY_ADDRESSES` from
    institution_resolver without touching renderer code.
    """
    rec, _reason = resolve_facility_for_text(text)
    if rec is None:
        return ""
    return f"{rec.address_line1}, {rec.address_line2}"


def canonical_name(text: str) -> str:
    """Return the canonical institution/agency name for free text. On miss,
    returns the input unchanged (mirrors `institution_resolver.normalize`
    semantics — this is a label normalizer, not an identity check, so
    losing un-canonicalizable strings would silently corrupt downstream
    aggregates).

    Thin facade over `institution_resolver.normalize`. Callers should use
    this in preference to importing institution_resolver directly so the
    eventual fold of institution_resolver into facility_registry doesn't
    have to touch every caller.
    """
    if text is None:
        return ""
    s = str(text)
    if not s.strip():
        return s
    try:
        from src.core.institution_resolver import normalize
    except Exception as e:
        log.debug("institution_resolver.normalize unavailable: %s", e)
        return s
    try:
        return normalize(s)
    except Exception:
        return s


def same_institution(name_a: str, name_b: str) -> bool:
    """Return True if two free-text institution names refer to the same
    canonical entity. Used by `pc_rfq_linker` and dashboard match logic.

    Thin facade over `institution_resolver.same_institution`. Preserves
    the existing matcher's recall guarantees (same agency without
    facility code = match, substring match >= 5 chars, etc.) — folding
    that logic into `facility_registry.resolve()` would break callers
    that rely on the lossy "same agency" branch.
    """
    if not name_a or not name_b:
        return False
    try:
        from src.core.institution_resolver import same_institution as _impl
    except Exception as e:
        log.debug("institution_resolver.same_institution unavailable: %s", e)
        return False
    try:
        return bool(_impl(name_a, name_b))
    except Exception:
        return False


def classify_agency(name: str = "", email: str = "",
                    ship_to: str = "") -> dict:
    """Resolve a (name, email, ship_to) tuple to its canonical agency
    classification. Returns the same shape that
    `institution_resolver.resolve` returns — `{canonical, agency,
    facility_code, original, source}` — so callers swap one import line.

    The 3-input fallback (raw name → ship_to → email domain → garbage
    label filter) is classification logic, NOT facility identity. It
    intentionally lives separate from `facility_registry.resolve()`
    which refuses to silently guess on ambiguous facility text. Email-
    domain matching belongs HERE, not in facility_registry.

    Thin facade over `institution_resolver.resolve`. Callers like
    `routes_pricecheck` (SCPRS staleness check) need just the agency
    key — read `result["agency"]` for that.
    """
    empty = {"canonical": "", "agency": "", "facility_code": "",
             "original": "", "source": ""}
    if not name and not email and not ship_to:
        return empty
    try:
        from src.core.institution_resolver import resolve as _resolve
    except Exception as e:
        log.debug("institution_resolver.resolve unavailable: %s", e)
        return empty
    try:
        out = _resolve(name or "", email=email or "", ship_to=ship_to or "")
        if not isinstance(out, dict):
            return empty
        # Normalize missing keys for callers that index without .get
        for k in ("canonical", "agency", "facility_code", "original", "source"):
            out.setdefault(k, "")
        return out
    except Exception:
        return empty


def tax_for_facility(facility) -> dict:
    """Return tax info for a resolved facility as a dict:
      {"rate": 0.0875, "rate_bps": 875, "jurisdiction": "BARSTOW",
       "source": "facility_registry", "validated": True}
    Thin facade over `tax_resolver.resolve_tax(address)`."""
    if facility is None:
        return {"rate": 0.0, "rate_bps": 0, "jurisdiction": "",
                "source": "", "validated": False}
    bps, jur, src, validated = _resolve_tax_for_facility(facility)
    return {
        "rate": round(bps / 10000.0, 6),
        "rate_bps": bps,
        "jurisdiction": jur,
        "source": src,
        "validated": validated,
    }


def tax_for_address(address_text: str) -> dict:
    """Return tax info for a free-text address (zip-only, full street,
    or any string `tax_resolver.resolve_tax` accepts). Same dict shape
    as `tax_for_facility`, plus `facility_code` when the resolver was
    able to anchor on a canonical facility. Empty / error inputs
    return a zeroed dict — callers check `validated` before using.

    Used by renderer paths that only have an address string (e.g., a
    zip code from a PDF field, or a delivery_location from an RFQ
    row) without a pre-resolved `FacilityRecord`. The facade routes
    through the canonical tax_resolver, so this function and
    `tax_for_facility(facility)` share the same tax-source code path.
    """
    out = {"rate": 0.0, "rate_bps": 0, "jurisdiction": "",
           "source": "", "validated": False, "facility_code": ""}
    if not address_text or not str(address_text).strip():
        return out
    try:
        from src.core.tax_resolver import resolve_tax
    except Exception as e:
        log.debug("tax_resolver import failed in tax_for_address: %s", e)
        return out
    try:
        info = resolve_tax(str(address_text).strip())
    except Exception as e:
        log.debug("tax_resolver.resolve_tax failed: %s", e)
        return out
    if not info:
        return out
    rate = float(info.get("rate") or 0.0)
    bps = int(round(rate * 10000))
    out.update({
        "rate": round(bps / 10000.0, 6),
        "rate_bps": bps,
        "jurisdiction": str(info.get("jurisdiction") or ""),
        "source": str(info.get("source") or ""),
        "validated": bool(info.get("validated", False)),
        "facility_code": str(info.get("facility_code") or ""),
    })
    return out


def _resolve_facility_from_rfq(rfq: dict) -> Tuple[Optional["FacilityRecord"], str, str]:
    """Apply canonical `facility_registry` resolution to the RFQ's
    ship-to fields in priority order.

    Returns `(record, canonical_ship_to_raw, reason)` where:
      - `record` is the matched `FacilityRecord` or None
      - `canonical_ship_to_raw` is the first non-empty input field we tried
      - `reason` is the resolver-provided slug ("agency_key", "exact",
        "substring_unique", "zip_unique", "ambiguous_substring",
        "ambiguous_zip", "no_match", "empty_input")

    Priority order (matches PR #504's agency-key-first contract):
      1. `agency_key` → `resolve_by_agency_key()` (e.g. calvet_barstow
         → CALVETHOME-BF). Wins over text because the converter has
         already done the agency-resolution work; trusting text
         caused incident f81c4e9b.
      2. Text fields in legacy order: delivery_location → ship_to_name
         → ship_to → institution_name → agency_name → department →
         requestor_name. Each goes through `resolve_with_reason()`
         so the contract carries the audit slug.
    """
    try:
        from src.core.facility_registry import (
            resolve_with_reason, resolve_by_agency_key,
        )
    except Exception as e:
        log.debug("facility_registry import failed in assembly: %s", e)
        return None, "", "no_match"

    # Pass 1 — agency_key first (PR #504's invariant).
    agency_key = (rfq.get("agency_key") or "").strip()
    if agency_key:
        try:
            rec = resolve_by_agency_key(agency_key)
        except Exception as _e:
            rec = None
        if rec is not None:
            return rec, "", "agency_key"

    # Pass 2 — text fallback in legacy priority.
    candidates = [
        ("delivery_location", rfq.get("delivery_location") or ""),
        ("ship_to_name",      rfq.get("ship_to_name") or ""),
        ("ship_to",           rfq.get("ship_to") or ""),
        ("institution_name",  rfq.get("institution_name") or ""),
        ("agency_name",       rfq.get("agency_name") or ""),
        ("department",        rfq.get("department") or ""),
        ("requestor_name",    rfq.get("requestor_name") or ""),
    ]
    first_raw = ""
    last_reason = "empty_input"
    for field_name, value in candidates:
        value = (value or "").strip()
        if not value:
            continue
        if not first_raw:
            first_raw = value
        try:
            rec, reason = resolve_with_reason(value)
        except Exception as _e:
            rec, reason = None, "no_match"
        last_reason = reason
        if rec is not None:
            return rec, value, reason
    return None, first_raw, last_reason


def _resolve_tax_for_facility(facility) -> Tuple[int, str, str, bool]:
    """Ask `tax_resolver` for the rate corresponding to the resolved
    facility. Returns (bps, jurisdiction, source, validated). Falls
    back to (0, "", "fallback", False) if resolver fails — renderers
    see the not-validated flag and can refuse to emit a PDF.
    """
    if facility is None:
        return 0, "", "", False
    try:
        from src.core.tax_resolver import resolve_tax
        # Address string in the shape tax_resolver expects.
        addr = f"{facility.address_line1}, {facility.address_line2}"
        out = resolve_tax(addr)
        rate = out.get("rate") or 0.0
        bps = int(round(rate * 10000))
        return (
            bps,
            str(out.get("jurisdiction") or ""),
            str(out.get("source") or ""),
            bool(out.get("validated", False)),
        )
    except Exception as e:
        log.debug("tax_resolver failed in assembly: %s", e)
        return 0, "", "fallback", False


def _line_items_from_rfq(rfq: dict) -> Tuple[LineItem, ...]:
    """Project RFQ items into immutable LineItems. Money goes to cents
    at assembly time so no renderer ever sees floats — tax math stays
    exact across all PDFs."""
    items = rfq.get("items") or rfq.get("line_items") or []
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            qty = int(float(it.get("quantity") or it.get("qty") or 1))
        except (TypeError, ValueError):
            qty = 1
        try:
            raw_price = (it.get("unit_price")
                         or it.get("final_price")
                         or it.get("bid_price")
                         or 0)
            unit_cents = int(round(float(raw_price) * 100))
        except (TypeError, ValueError):
            unit_cents = 0
        if unit_cents <= 0:
            # Skip lines with no priced unit — the contract represents
            # what will be SHIPPED, not what's still being worked.
            continue
        out.append(LineItem(
            description=str(it.get("description") or "")[:500],
            quantity=qty,
            unit_price_cents=unit_cents,
            mfg_number=str(it.get("mfg_number") or it.get("part_number") or ""),
            uom=str(it.get("uom") or "EA"),
        ))
    return tuple(out)


def assemble_from_rfq(rfq: dict) -> QuoteContract:
    """Primary entry point — build a frozen `QuoteContract` from an
    RFQ dict using ONLY canonical resolvers (facility_registry +
    tax_resolver). No ad-hoc facility dicts, no city fallbacks, no
    per-renderer tax tables.

    Never raises. Returns `_empty_contract(...)` when assembly can't
    derive anything useful — caller is expected to surface that to
    the operator rather than ship a PDF from empty state.
    """
    if not isinstance(rfq, dict):
        return _empty_contract()

    facility, ship_to_raw, reason = _resolve_facility_from_rfq(rfq)
    tax_bps, tax_jur, tax_src, tax_validated = _resolve_tax_for_facility(facility)

    if facility is not None:
        agency_code = facility.parent_agency or ""
        agency_full = facility.parent_agency_full or ""
    else:
        # No facility → leave agency fields empty. Renderers that
        # need an agency block should flag operator-review required.
        agency_code = ""
        agency_full = ""

    return QuoteContract(
        facility=facility,
        agency_code=agency_code,
        agency_full=agency_full,
        ship_to_raw=ship_to_raw,
        ship_to_resolve_reason=reason,
        line_items=_line_items_from_rfq(rfq),
        tax_rate_bps=tax_bps,
        tax_rate=round(tax_bps / 10000.0, 6),
        tax_jurisdiction=tax_jur,
        tax_source=tax_src,
        tax_validated=tax_validated,
        source_pc_id=str(rfq.get("source_pc_id") or rfq.get("pc_id") or ""),
        source_rfq_id=str(rfq.get("id") or rfq.get("rfq_id") or ""),
        assembled_at=datetime.now(timezone.utc).isoformat(),
    )


def assemble_from_pc(pc: dict) -> QuoteContract:
    """Build a contract directly from a PC (before convert to RFQ).
    Useful for preview renderers that want to see the contract the
    downstream quote would use, without first having to convert. The
    assembly logic is identical to `assemble_from_rfq` because PC and
    RFQ carry the same ship-to / item shape.
    """
    return assemble_from_rfq(pc)
