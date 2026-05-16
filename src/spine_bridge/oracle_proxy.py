"""Oracle proxy — the Spine's read-only window into the Pricing Oracle.

Oracle suggests. Operator decides. Substrate stores only operator-typed
values. This module is the bridge: it returns suggestions per line
item, never writes to spine_quotes.

v1 (this file) returns FIXTURE data — the substrate-side surface is
built first so the editor UI can wire in. PR-O4 will replace the
fixture with a real call to the parent Reytech-RFQ pricing-oracle
module. The fixture shape is the contract; the real bridge must
honor it.

See `project_spine_oracle_wiring_plan_2026_05_15.md` (memory) for the
full architectural rule: oracle is a SUGGESTER, never a writer; no
auto-apply on ingest; no `oracle_suggested_*` fields on Quote/LineItem.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.spine.model import Quote


@dataclass(frozen=True)
class OracleSource:
    """A single evidence point behind a suggestion.

    `kind` is one of:
      - "vendor_catalog"  — known supplier catalog hit
      - "web_scrape"      — fresh URL scrape (vendor website)
      - "scprs_award"     — prior California state award lookup
      - "competitor_intel"— recent winning-bid intel
      - "operator_history"— prior Reytech quote on the same SKU
    """
    kind: str
    label: str
    url: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class OracleLineSuggestion:
    line_no: int
    # Suggested cost basis (per-unit cents). None = oracle has no
    # confidence in any source; operator must hand-validate.
    suggested_cost_cents: int | None
    cost_sources: tuple[OracleSource, ...]
    # Suggested unit price (per-unit cents). Derived from cost +
    # markup_ladder OR anchored to a competitor signal.
    suggested_unit_price_cents: int | None
    price_basis: str  # human-readable explanation: "scprs award ladder", etc.
    # Competitor signal: most recent winning unit_price for the same
    # SKU/agency, if known. delta_pct is signed: negative = we'd
    # undercut, positive = we'd be above.
    competitor_unit_price_cents: int | None
    competitor_vendor: str | None
    competitor_delta_pct: float | None
    # Confidence: "high" | "medium" | "low". UI uses this to flag
    # low-confidence suggestions and force the operator to verify
    # sources before clicking accept.
    confidence: str
    # Days since the underlying data was refreshed. > 30 → stale
    # warning. > 90 → suggestion hidden / operator hand-validates.
    freshness_days: int


def suggestions_for_quote(quote: "Quote") -> list[OracleLineSuggestion]:
    """Return a per-line list of oracle suggestions.

    v1: returns deterministic fixture data so the editor UI can wire
    in and walk through the click-to-accept flow. The shape is the
    contract — PR-O4 (real bridge) will replace this function body
    with a call to the parent repo's pricing-oracle module while
    preserving the return type.

    Deterministic generation rules (so fixture data is interesting
    enough to drive the UI):

    - First line: high-confidence suggestion with a competitor signal
      saying we'd undercut by 1.7% — the "beat the winner" badge.
    - Second line: medium-confidence; competitor data is stale.
    - Third line onward: cycles confidence levels and signals so the
      editor exercises every visual variant.
    """
    out: list[OracleLineSuggestion] = []
    for idx, li in enumerate(quote.line_items):
        cycle = idx % 3
        # Fixture: suggest cost very close to operator's typed value
        # (so the UI can show "your typed: $X, suggested: $X") and
        # unit price ~5% under operator's typed value.
        op_cost = li.cost_cents
        op_price = li.unit_price_cents
        suggested_cost = max(1, int(op_cost * 0.96)) if op_cost else None
        suggested_price = max(1, int(op_price * 0.95)) if op_price else None
        competitor_price = (
            int(op_price * 0.985) if op_price and cycle != 2 else None
        )
        if competitor_price is not None and op_price:
            delta_pct = round(
                (competitor_price - op_price) * 100.0 / op_price, 1
            )
        else:
            delta_pct = None
        out.append(OracleLineSuggestion(
            line_no=li.line_no,
            suggested_cost_cents=suggested_cost,
            cost_sources=(
                OracleSource(
                    kind="vendor_catalog",
                    label="Uline catalog",
                    url=f"https://www.uline.com/Product/Detail/{li.mfg_number or 'EXAMPLE'}",
                    detail="Wholesale 2024 catalog page",
                ),
                OracleSource(
                    kind="web_scrape",
                    label="Vendor site",
                    url=li.cost_source_url
                         or "https://supplier.example.com/sku/lookup",
                    detail="Last scrape: 12 days ago",
                ),
            ) if suggested_cost else (),
            suggested_unit_price_cents=suggested_price,
            price_basis={
                0: "scprs award ladder + 5.5% markup",
                1: "competitor anchor − 1.5%",
                2: "operator history + freshness premium",
            }[cycle],
            competitor_unit_price_cents=competitor_price,
            competitor_vendor=(
                "ACME Medical" if cycle == 0
                else "ULINE" if cycle == 1
                else None
            ),
            competitor_delta_pct=delta_pct,
            confidence=("high", "medium", "low")[cycle],
            freshness_days={0: 12, 1: 28, 2: 6}[cycle],
        ))
    return out


def suggestion_to_dict(s: OracleLineSuggestion) -> dict:
    """Serialize a suggestion for JSON transport."""
    return {
        "line_no": s.line_no,
        "suggested_cost_cents": s.suggested_cost_cents,
        "suggested_unit_price_cents": s.suggested_unit_price_cents,
        "price_basis": s.price_basis,
        "competitor_unit_price_cents": s.competitor_unit_price_cents,
        "competitor_vendor": s.competitor_vendor,
        "competitor_delta_pct": s.competitor_delta_pct,
        "confidence": s.confidence,
        "freshness_days": s.freshness_days,
        "cost_sources": [
            {
                "kind": src.kind,
                "label": src.label,
                "url": src.url,
                "detail": src.detail,
            }
            for src in s.cost_sources
        ],
    }
