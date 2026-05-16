"""Email contract → Spine Quote ingest.

The Vision-primary parser (legacy: src/agents/...) produces an
"email contract" dict for every inbound RFQ — see PR #914 (5/11) for
the canonical schema. This module turns that contract into a Spine
Quote with tax_rate_bps already resolved.

Charter rule #6: tax_rate_bps is MANDATORY at ingest. If CDTFA lookup
fails, ingest FAILS. The Spine deliberately does not allow a quote to
exist in any state without its tax rate resolved — the latency between
ingest and pricing was the gap the 5/15 tax-zero bug hid in.

The CDTFA tax resolver is injected. In prod this is the existing
tax_resolver.resolve_tax (from src.core); in tests we pass a stub
that returns deterministic bps. This keeps the Spine package
Flask-free AND CDTFA-free.

Email contract schema (subset we require):

    {
      "rfq_id": "abc123",                 # required
      "agency": "CCHCS",                  # required (v1 CCHCS only)
      "facility": "SATF Corcoran 93212",  # required for tax lookup
      "ship_to": "...",                   # required for tax lookup
      "solicitation_number": "PREQ 10847262",
      "line_items": [
        { "description": "...", "qty": 10, "uom": "EA",
          "item_number": "MFG-X" },
        ...
      ],
      "buyer": { "name": "...", "email": "..." },  # optional
      "due_date": "2026-05-13",                    # optional
    }

Note: line items have NO cost or unit_price at ingest. The operator
adds those during pricing. The Spine accepts unit_price_cents=0 in
'parsed' status; the priced→finalized transitions will demand real
prices later.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from src.spine import LineItem, Quote, QuoteStatus, SpineValidationError
from src.spine_bridge.translator import (
    TranslationIssue,
    _resolve_uom,  # private but project-internal; keeps UOM logic single-sourced
)


# Tax resolver callable: ship-to address (or full contract) → bps, or None.
# Production wires this to tax_resolver.resolve_tax which calls CDTFA.
TaxResolver = Callable[[str], int | None]


@dataclass
class IngestResult:
    """Outcome of ingesting one email contract.

    quote is None iff issues contains at least one severity='error'.
    """
    quote: Quote | None
    issues: list[TranslationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.quote is not None

    def errors(self) -> list[TranslationIssue]:
        return [i for i in self.issues if i.severity == "error"]


# ──────────────────────────────────────────────────────────────────────
# Ingest
# ──────────────────────────────────────────────────────────────────────


def ingest_email_contract(
    contract: dict,
    *,
    tax_resolver: TaxResolver,
    ingest_ts: datetime | None = None,
) -> IngestResult:
    """Turn an email contract into a Spine Quote in 'parsed' status.

    Args:
        contract: Vision-parsed email contract dict (see module docstring).
        tax_resolver: Function (ship_to_str) → bps-or-None. Required.
        ingest_ts: Optional timestamp override (for test determinism).
            Defaults to now(). Used as cost_validated_at on every line
            item — at ingest, cost is 0 with no source, so the
            timestamp is just provenance for "when this quote was
            created in the Spine".

    Returns:
        IngestResult with .quote set iff:
        - the agency is supported,
        - tax_resolver returned a positive bps,
        - every line item parsed (description + qty + uom),
        - no contract-level fields conflict with Spine invariants.
    """
    issues: list[TranslationIssue] = []
    ingest_ts = ingest_ts or datetime.now(timezone.utc)

    # ── Required header fields ───────────────────────────────────────

    rfq_id = (contract.get("rfq_id") or contract.get("id") or "").strip()
    if not rfq_id:
        issues.append(TranslationIssue(
            "error", "rfq_id", "email contract has no rfq_id / id field",
        ))

    agency = contract.get("agency") or "CCHCS"
    if agency != "CCHCS":
        issues.append(TranslationIssue(
            "error", "agency",
            f"agency={agency!r} not yet supported in the Spine. v1 is CCHCS-only.",
        ))

    facility = (contract.get("facility") or "").strip()[:64]
    ship_to = str(contract.get("ship_to") or "").strip()
    if not facility and not ship_to:
        issues.append(TranslationIssue(
            "error", "facility",
            "no facility OR ship_to in contract — tax lookup impossible",
        ))
    if not facility:
        # Derive a short facility label from ship_to first line.
        facility = ship_to.split("\n")[0][:64] or "UNKNOWN"

    # Strip PREQ-style prefix via the shared helper so this and the
    # translator can't drift (one of the substrate-meltdown classes).
    from src.spine_bridge._solicitation import strip_solicitation_prefix
    sol_raw = strip_solicitation_prefix(contract.get("solicitation_number"))
    if not sol_raw:
        issues.append(TranslationIssue(
            "error", "solicitation_number",
            "missing solicitation_number — CCHCS requires it for routing",
        ))
    solicitation = sol_raw[:64]

    # ── MANDATORY tax-at-ingest ──────────────────────────────────────

    tax_lookup_input = ship_to or facility
    tax_bps: int | None = None
    try:
        tax_bps = tax_resolver(tax_lookup_input)
    except Exception as e:
        issues.append(TranslationIssue(
            "error", "tax_rate_bps",
            f"tax_resolver raised an exception for {tax_lookup_input!r}: {e}",
        ))

    if tax_bps is None or tax_bps <= 0:
        issues.append(TranslationIssue(
            "error", "tax_rate_bps",
            f"CDTFA tax resolver returned no usable rate for "
            f"{tax_lookup_input!r}. Charter rule #6: tax is mandatory at "
            "ingest — refusing to create a quote without it.",
        ))

    # Record contract-side fields we deliberately don't store on the Spine.
    for k in ("shipping_option", "shipping_amount", "delivery_option"):
        if contract.get(k) is not None:
            issues.append(TranslationIssue(
                "info", k,
                f"dropped per Charter rule #7; contract value: {contract.get(k)!r}",
            ))

    # ── Line items ───────────────────────────────────────────────────

    raw_items = contract.get("line_items") or contract.get("items") or []
    if not raw_items:
        issues.append(TranslationIssue(
            "error", "line_items",
            "contract has no line_items — refuse to ingest empty quote",
        ))

    line_items: list[LineItem] = []
    for idx, raw in enumerate(raw_items):
        line_path = f"line_items[{idx}]"
        if not isinstance(raw, dict):
            issues.append(TranslationIssue(
                "error", line_path,
                f"line item is not a dict: {type(raw).__name__}",
            ))
            continue

        desc = (raw.get("description") or "").strip()[:500]
        if not desc:
            issues.append(TranslationIssue(
                "error", f"{line_path}.description",
                "line item has no description",
            ))
            continue

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

        # At ingest, we don't yet know the unit price OR cost. The
        # Spine model accepts (cost_cents=0, unit_price_cents=0) in
        # 'parsed' status. The operator advances to 'priced' after
        # adding prices.
        try:
            li = LineItem(
                line_no=idx + 1,
                description=desc,
                mfg_number=raw.get("item_number") or raw.get("mfg_number") or None,
                qty=qty,
                uom=_resolve_uom(raw),
                cost_cents=0,
                cost_source_url=None,
                cost_hand_validated_note=None,
                cost_validated_at=ingest_ts,
                unit_price_cents=0,
            )
        except Exception as e:
            issues.append(TranslationIssue(
                "error", line_path,
                f"Spine LineItem rejected this row: {e}",
            ))
            continue

        line_items.append(li)

    # ── Bail on any error ────────────────────────────────────────────

    if any(i.severity == "error" for i in issues):
        return IngestResult(quote=None, issues=issues)

    # ── Build the Quote ──────────────────────────────────────────────

    try:
        quote = Quote(
            quote_id=rfq_id,
            agency=agency,  # type: ignore[arg-type]
            facility=facility,
            solicitation_number=solicitation,
            line_items=line_items,
            tax_rate_bps=tax_bps,  # type: ignore[arg-type]  # validated above
            status=QuoteStatus.PARSED,
            created_at=ingest_ts,
            updated_at=ingest_ts,
        )
    except (SpineValidationError, Exception) as e:
        issues.append(TranslationIssue(
            "error", "quote",
            f"Spine Quote rejected the assembled state: {e}",
        ))
        return IngestResult(quote=None, issues=issues)

    return IngestResult(quote=quote, issues=issues)
