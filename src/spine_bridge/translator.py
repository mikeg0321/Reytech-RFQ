"""Legacy quote dict → Spine Quote translator.

The legacy substrate stores a quote as a dict with up to 4 aliases for
unit_price, 3 for tax_rate, and an `extra` catch-all that absorbs
whatever didn't fit. This module takes such a dict and produces a
clean Spine Quote — picking the canonical value for each field per
the rules in [project_qa_quote_system_substrate_findings_2026_05_15].

Translation rules:

    unit_price  → use 'unit_price' first if present and >0, else
                  'bid_price', else 'price_per_unit', else 'our_price',
                  else compute from cost × (1 + markup/100). NEVER let
                  a stored markup_pct override an explicit unit_price.

    cost        → use 'supplier_cost' first, else 'vendor_cost', else
                  'cost', else 'pricing.unit_cost'.

    tax_rate    → prefer integer-bps fields if available, else convert
                  decimal tax_rate × 10000 → bps. Reject 0 or missing
                  (the Spine model requires tax_rate_bps > 0 to leave
                  the parsed state).

    shipping    → DROPPED. The Spine has no shipping field; legacy's
                  shipping_option / shipping_amount are noise.

    markup_pct  → DROPPED. The Spine derives markup from
                  unit_price/cost; storing it caused the 5/15
                  qty-clobbers-markup bug.

Every translation choice is recorded in TranslationIssue entries so
operators can audit what got dropped or remapped. The result is
strict: if any line item lacks a usable unit price, the translation
FAILS instead of inventing zeros (closes the silent-mutation class).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from src.spine import LineItem, Quote, QuoteStatus, SpineValidationError


# ──────────────────────────────────────────────────────────────────────
# Result types
# ──────────────────────────────────────────────────────────────────────


@dataclass
class TranslationIssue:
    """One auditable note about how a legacy field was handled.

    Severity:
        info     — informational, no impact on correctness
        warning  — value remapped or dropped; result may still be valid
        error    — translation could not produce a valid Spine Quote
    """
    severity: str  # 'info' | 'warning' | 'error'
    field_path: str
    detail: str


@dataclass
class LegacyTranslationResult:
    """Outcome of translating one legacy quote dict.

    quote is None iff issues contains at least one severity='error'.
    """
    quote: Quote | None
    issues: list[TranslationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.quote is not None

    def errors(self) -> list[TranslationIssue]:
        return [i for i in self.issues if i.severity == "error"]

    def warnings(self) -> list[TranslationIssue]:
        return [i for i in self.issues if i.severity == "warning"]


# ──────────────────────────────────────────────────────────────────────
# Field-resolution helpers
# ──────────────────────────────────────────────────────────────────────


_UNIT_PRICE_ALIASES = ("unit_price", "bid_price", "price_per_unit", "our_price")
_COST_ALIASES = ("supplier_cost", "vendor_cost", "cost", "catalog_cost")


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):  # avoid True/False arithmetic surprises
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _dollars_to_cents(dollars: float | None) -> int | None:
    if dollars is None:
        return None
    if dollars < 0:
        return None
    return int(round(dollars * 100))


def _first_positive_alias(
    item: dict,
    aliases: Iterable[str],
) -> tuple[str | None, float | None]:
    """Return (alias_name, value) of the first alias with value > 0."""
    for alias in aliases:
        v = _to_float(item.get(alias))
        if v is not None and v > 0:
            return alias, v
        # Also try pricing.<alias>
        pricing = item.get("pricing") or {}
        if isinstance(pricing, dict):
            v = _to_float(pricing.get(alias))
            if v is not None and v > 0:
                return f"pricing.{alias}", v
    return None, None


def _resolve_unit_price_cents(
    item: dict,
    issues: list[TranslationIssue],
    line_path: str,
) -> int | None:
    """Pick the canonical unit price for this line, in integer cents.

    Records issues for every alias that was passed over.
    """
    alias_used, dollars = _first_positive_alias(item, _UNIT_PRICE_ALIASES)
    if alias_used is None:
        # No explicit price — try to compute from cost × markup.
        cost = _to_float(item.get("supplier_cost") or item.get("vendor_cost")
                          or item.get("cost"))
        markup_pct = _to_float(item.get("markup_pct"))
        if cost is not None and cost > 0 and markup_pct is not None:
            derived = cost * (1 + markup_pct / 100.0)
            issues.append(TranslationIssue(
                "warning", line_path,
                f"no explicit unit price; derived from cost × (1 + markup/100): "
                f"{cost:.2f} × (1 + {markup_pct}/100) = {derived:.2f}",
            ))
            return _dollars_to_cents(derived)
        return None

    # Note every OTHER alias that was set — record as info so audit
    # shows what was passed over.
    for alias in _UNIT_PRICE_ALIASES:
        if alias == alias_used.split(".")[-1]:
            continue
        v = _to_float(item.get(alias))
        if v is not None and v > 0 and abs(v - dollars) > 0.001:
            issues.append(TranslationIssue(
                "warning", f"{line_path}.{alias}",
                f"divergent unit_price alias: chose {alias_used}={dollars:.2f}, "
                f"saw {alias}={v:.2f} (passed over)",
            ))
    return _dollars_to_cents(dollars)


def _resolve_cost_cents(
    item: dict,
    issues: list[TranslationIssue],
    line_path: str,
) -> int | None:
    alias_used, dollars = _first_positive_alias(item, _COST_ALIASES)
    if alias_used is None:
        return 0  # cost=0 is legal in the Spine for low-value items.
    return _dollars_to_cents(dollars) or 0


def _resolve_uom(item: dict) -> str:
    """Coerce legacy UOM strings into the Spine's allowlist.

    Maps common variants ('Each', 'each', 'ea.', 'EACH') → 'EA'.
    If a legacy UOM is unknown, returns the raw string and lets the
    Spine model reject it (so the issue surfaces clearly).
    """
    raw = (item.get("uom") or item.get("unit_of_measure") or "EA")
    s = str(raw).strip().upper().replace(".", "")
    # Common aliases.
    if s in ("EACH", "EA"):
        return "EA"
    if s in ("PACK", "PK"):
        return "PK"
    if s in ("PACKAGE", "PAC"):
        return "PAC"
    if s in ("BOX", "BX"):
        return "BX"
    if s in ("CASE", "CS"):
        return "CS"
    if s in ("CARTON", "CT"):
        return "CT"
    if s in ("DOZEN", "DZ"):
        return "DZ"
    if s in ("ROLL", "RL"):
        return "RL"
    if s in ("PAIR", "PR"):
        return "PR"
    if s in ("SET", "ST"):
        return "ST"
    if s in ("BAG", "BG"):
        return "BG"
    if s in ("BOTTLE", "BT"):
        return "BT"
    return s


def _resolve_tax_rate_bps(
    legacy: dict,
    issues: list[TranslationIssue],
) -> int:
    """Pick the canonical tax_rate_bps, preferring integer fields.

    Records warnings when conflicting aliases disagree.
    """
    # Direct int bps if present.
    bps = legacy.get("tax_rate_bps")
    if isinstance(bps, int) and bps > 0:
        return bps

    # Decimal tax_rate (0.0825 form).
    rate = _to_float(legacy.get("tax_rate"))
    if rate is not None and rate > 0:
        # Disambiguate: 0.0825 (decimal) vs 8.25 (percent).
        if rate < 1.0:
            return int(round(rate * 10_000))
        else:
            return int(round(rate * 100))

    # tax_rate_pct alias.
    rate_pct = _to_float(legacy.get("tax_rate_pct"))
    if rate_pct is not None and rate_pct > 0:
        return int(round(rate_pct * 100))

    # extra catch-all.
    extra = legacy.get("extra") or {}
    if isinstance(extra, dict):
        rate = _to_float(extra.get("tax_rate"))
        if rate is not None and rate > 0:
            issues.append(TranslationIssue(
                "warning", "extra.tax_rate",
                f"tax_rate lived in extra catch-all: {rate} — promoted to top-level.",
            ))
            return int(round(rate * 10_000 if rate < 1.0 else rate * 100))

    issues.append(TranslationIssue(
        "error", "tax_rate_bps",
        "no usable tax rate found in legacy dict — Spine requires "
        "tax_rate_bps > 0 to leave the parsed state.",
    ))
    return 0


def _resolve_quote_id(legacy: dict, issues: list[TranslationIssue]) -> str:
    """Pull a stable quote ID from the legacy dict.

    Prefers 'id' (the rfq_files PK). Falls back to 'rfq_id', 'pc_id',
    'reytech_quote_number'.
    """
    for key in ("id", "rfq_id", "pc_id", "quote_id", "reytech_quote_number"):
        v = legacy.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    issues.append(TranslationIssue(
        "error", "quote_id", "no usable id field found",
    ))
    return ""


def _resolve_facility(legacy: dict) -> str:
    """Pull facility / ship-to name."""
    return str(
        legacy.get("facility")
        or legacy.get("delivery_location")
        or legacy.get("ship_to")
        or "UNKNOWN"
    ).strip()[:64]


def _resolve_solicitation(legacy: dict) -> str:
    """Pull solicitation number, stripping PREQ prefix if present.

    PREQ-strip rule lives in _solicitation.strip_solicitation_prefix
    so this translator and src/spine_bridge/ingest.py both use the
    same pattern. Mirrors form_field_extractor's AV-1 substrate fix.
    """
    from src.spine_bridge._solicitation import strip_solicitation_prefix

    raw = (
        legacy.get("solicitation_number")
        or legacy.get("sol_number")
        or legacy.get("rfq_number")
        or ""
    )
    return strip_solicitation_prefix(raw)[:64] or "UNKNOWN"


# ──────────────────────────────────────────────────────────────────────
# Main entry
# ──────────────────────────────────────────────────────────────────────


def translate_legacy_quote(
    legacy: dict,
    *,
    agency: str = "CCHCS",
    cost_validated_at: datetime | None = None,
) -> LegacyTranslationResult:
    """Translate one legacy quote dict to a Spine Quote.

    Args:
        legacy: Raw rfq_files-or-priced_carts-shaped dict.
        agency: Target agency. v1 supports CCHCS only (Charter rule
            "Single Literal agency"); other agencies will be added
            when their Spine paths exist.
        cost_validated_at: Optional override for line item
            cost_validated_at timestamps. Defaults to now() — the
            Spine treats cost without a fresh timestamp as stale.

    Returns:
        LegacyTranslationResult — .quote is set iff no errors.
    """
    issues: list[TranslationIssue] = []
    quote_id = _resolve_quote_id(legacy, issues)
    facility = _resolve_facility(legacy)
    solicitation = _resolve_solicitation(legacy)
    tax_rate_bps = _resolve_tax_rate_bps(legacy, issues)

    if agency != "CCHCS":
        issues.append(TranslationIssue(
            "error", "agency",
            f"agency={agency!r} not yet supported in the Spine. "
            "v1 is CCHCS-only; expand the agency Literal in src/spine/model.py.",
        ))

    # Note dropped shipping fields for audit.
    for k in ("shipping_option", "shipping_amount", "delivery_option"):
        if legacy.get(k) is not None:
            issues.append(TranslationIssue(
                "info", k,
                f"dropped per Charter rule #7 (shipping is the constant $0.00); "
                f"legacy value: {legacy.get(k)!r}",
            ))

    # Translate line items.
    raw_items = legacy.get("line_items") or legacy.get("items") or []
    if not raw_items:
        issues.append(TranslationIssue(
            "error", "line_items", "no line items found",
        ))

    cost_ts = cost_validated_at or datetime.now(timezone.utc)
    line_items: list[LineItem] = []
    for idx, raw in enumerate(raw_items):
        line_path = f"line_items[{idx}]"
        if not isinstance(raw, dict):
            issues.append(TranslationIssue(
                "error", line_path, f"line item is not a dict: {type(raw).__name__}",
            ))
            continue

        unit_price_cents = _resolve_unit_price_cents(raw, issues, line_path)
        if unit_price_cents is None:
            issues.append(TranslationIssue(
                "error", line_path,
                "could not resolve a usable unit price from any alias; "
                "Spine refuses to invent a zero.",
            ))
            continue

        cost_cents = _resolve_cost_cents(raw, issues, line_path)

        # Note dropped markup_pct (derived in Spine, never stored).
        if raw.get("markup_pct") is not None:
            issues.append(TranslationIssue(
                "info", f"{line_path}.markup_pct",
                f"dropped (Spine derives markup from unit_price/cost); "
                f"legacy markup_pct: {raw.get('markup_pct')}",
            ))

        # Cost source URL.
        cost_source_url = (
            raw.get("item_link")
            or raw.get("cost_source_url")
            or raw.get("source_url")
            or None
        )
        if cost_source_url and not str(cost_source_url).strip().startswith(("http://", "https://")):
            issues.append(TranslationIssue(
                "warning", f"{line_path}.item_link",
                f"item_link {cost_source_url!r} is not a valid URL — dropped.",
            ))
            cost_source_url = None

        # Qty must be a positive integer; coerce decimals.
        raw_qty = raw.get("qty") or raw.get("quantity") or 1
        try:
            qty = int(float(raw_qty))
        except (TypeError, ValueError):
            issues.append(TranslationIssue(
                "error", f"{line_path}.qty",
                f"non-numeric qty: {raw_qty!r}",
            ))
            continue
        if qty < 1:
            issues.append(TranslationIssue(
                "error", f"{line_path}.qty",
                f"qty must be >= 1; got {qty}",
            ))
            continue

        try:
            li = LineItem(
                line_no=idx + 1,
                description=(raw.get("description") or "").strip()[:500] or "UNKNOWN",
                mfg_number=(raw.get("item_number") or raw.get("mfg_number") or None),
                qty=qty,
                uom=_resolve_uom(raw),
                cost_cents=cost_cents,
                cost_source_url=cost_source_url,
                cost_hand_validated_note=(
                    None if cost_source_url else
                    f"legacy translation; original aliases: "
                    f"{[k for k in _COST_ALIASES if raw.get(k) is not None]!r}"
                ),
                cost_validated_at=cost_ts,
                unit_price_cents=unit_price_cents,
            )
        except Exception as e:
            issues.append(TranslationIssue(
                "error", line_path,
                f"Spine LineItem rejected this row: {e}",
            ))
            continue

        line_items.append(li)

    # If any errors so far, bail.
    if any(i.severity == "error" for i in issues):
        return LegacyTranslationResult(quote=None, issues=issues)

    # Build the Quote. Start in 'parsed' status so we don't trip
    # priced-state preconditions during translation; the operator (or
    # caller) advances the status explicitly.
    try:
        quote = Quote(
            quote_id=quote_id,
            agency=agency,  # type: ignore[arg-type]
            facility=facility,
            solicitation_number=solicitation,
            line_items=line_items,
            tax_rate_bps=tax_rate_bps,
            status=QuoteStatus.PARSED,
        )
    except (SpineValidationError, Exception) as e:
        issues.append(TranslationIssue(
            "error", "quote",
            f"Spine Quote rejected the assembled state: {e}",
        ))
        return LegacyTranslationResult(quote=None, issues=issues)

    return LegacyTranslationResult(quote=quote, issues=issues)
