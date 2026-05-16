"""Diff between EmailContract (ground truth) and Quote (operator state).

Every operator override is traceable to:
  - which field on which line was changed
  - what the contract said the buyer wanted
  - what the operator typed instead

Per Mike 2026-05-16: the contract IS the master; everything else is
deltas. This module is the pure function that computes those deltas.

The diff is asymmetric: the contract is authoritative for buyer-
visible fields (qty, description, uom, facility, solicitation #,
ship-to). The Quote is authoritative for vendor-side fields (cost,
unit_price, source URL) — those have no contract counterpart to
diff against.

Diff entries have severity:
  - "info"      : the operator legitimately filled in a contract gap
                  (e.g., contract had no MFG #, operator typed one).
  - "override"  : the operator's value differs from the contract's
                  buyer-stated value. Worth surfacing in audit.
  - "warning"   : the operator's value diverges by a wide margin,
                  e.g., qty 30 → qty 300 (likely typo).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


@dataclass(frozen=True)
class ContractDelta:
    """One field-level divergence between contract and quote."""
    severity: str   # "info" | "override" | "warning"
    field_path: str
    contract_value: object
    quote_value: object
    detail: str


def contract_vs_quote(contract: "EmailContract", quote: "Quote") -> list[ContractDelta]:
    """Compute per-field deltas. Pure function.

    Order matters for operator review — surface the most operationally
    significant divergences first:
      1. Top-level mismatches (sol#, facility, tax rate, agency)
      2. Line count mismatch (operator added/removed lines)
      3. Per-line buyer-visible field overrides (qty, description, uom)
      4. Per-line buyer-suggested MFG# vs operator MFG#

    Returns an empty list when the operator's quote is byte-faithful
    to the contract (rare in practice — operators always fill in
    vendor-side cost/price, but those aren't diffed here because the
    contract has no opinion on them).
    """
    deltas: list[ContractDelta] = []

    # ─── Top-level — buyer-stated procurement metadata ─────────────
    if contract.agency != quote.agency:
        deltas.append(ContractDelta(
            severity="override",
            field_path="agency",
            contract_value=contract.agency,
            quote_value=quote.agency,
            detail="operator changed agency — verify ship-to + tax rate",
        ))

    if contract.solicitation_number != quote.solicitation_number:
        deltas.append(ContractDelta(
            severity="override",
            field_path="solicitation_number",
            contract_value=contract.solicitation_number,
            quote_value=quote.solicitation_number,
            detail="operator changed solicitation number — could mismatch buyer thread",
        ))

    if contract.facility != quote.facility:
        deltas.append(ContractDelta(
            severity="override",
            field_path="facility",
            contract_value=contract.facility,
            quote_value=quote.facility,
            detail="operator changed facility — affects ship-to + tax",
        ))

    if contract.tax_rate_bps is not None and contract.tax_rate_bps != quote.tax_rate_bps:
        deltas.append(ContractDelta(
            severity="override",
            field_path="tax_rate_bps",
            contract_value=contract.tax_rate_bps,
            quote_value=quote.tax_rate_bps,
            detail=(
                f"operator overrode tax rate "
                f"({contract.tax_rate_bps/100:.2f}% → {quote.tax_rate_bps/100:.2f}%)"
            ),
        ))

    # ─── Line item count ──────────────────────────────────────────
    n_contract = len(contract.line_items)
    n_quote = len(quote.line_items)
    if n_contract != n_quote:
        deltas.append(ContractDelta(
            severity="override",
            field_path="line_items.length",
            contract_value=n_contract,
            quote_value=n_quote,
            detail=(
                f"operator changed line count "
                f"({n_contract} → {n_quote})"
            ),
        ))

    # ─── Per-line — match by line_no, skip missing on either side ──
    quote_by_line = {li.line_no: li for li in quote.line_items}
    for c_li in contract.line_items:
        q_li = quote_by_line.get(c_li.line_no)
        if q_li is None:
            deltas.append(ContractDelta(
                severity="override",
                field_path=f"line_items[{c_li.line_no}]",
                contract_value=c_li.description,
                quote_value=None,
                detail="operator removed this line",
            ))
            continue

        path = f"line_items[{c_li.line_no}]"

        if c_li.description != q_li.description:
            deltas.append(ContractDelta(
                severity="override",
                field_path=f"{path}.description",
                contract_value=c_li.description,
                quote_value=q_li.description,
                detail="operator changed buyer-stated description",
            ))

        if c_li.qty != q_li.qty:
            sev = "warning" if abs(q_li.qty - c_li.qty) > c_li.qty * 5 else "override"
            deltas.append(ContractDelta(
                severity=sev,
                field_path=f"{path}.qty",
                contract_value=c_li.qty,
                quote_value=q_li.qty,
                detail=(
                    f"operator changed qty ({c_li.qty} → {q_li.qty})"
                    + (" — wide divergence, likely typo" if sev == "warning" else "")
                ),
            ))

        if c_li.uom.upper() != q_li.uom.upper():
            deltas.append(ContractDelta(
                severity="override",
                field_path=f"{path}.uom",
                contract_value=c_li.uom,
                quote_value=q_li.uom,
                detail="operator changed UOM",
            ))

        # MFG # — buyer's suggestion vs operator's typed.
        c_mfg = (c_li.mfg_number_suggested or "").strip()
        q_mfg = (q_li.mfg_number or "").strip()
        if c_mfg and q_mfg and c_mfg.upper() != q_mfg.upper():
            deltas.append(ContractDelta(
                severity="override",
                field_path=f"{path}.mfg_number",
                contract_value=c_mfg,
                quote_value=q_mfg,
                detail="operator chose a different MFG # than buyer suggested",
            ))
        elif c_mfg and not q_mfg:
            deltas.append(ContractDelta(
                severity="info",
                field_path=f"{path}.mfg_number",
                contract_value=c_mfg,
                quote_value=None,
                detail="operator left MFG # blank despite buyer suggestion",
            ))
        elif q_mfg and not c_mfg:
            deltas.append(ContractDelta(
                severity="info",
                field_path=f"{path}.mfg_number",
                contract_value=None,
                quote_value=q_mfg,
                detail="operator filled in MFG # the buyer did not specify",
            ))

    # Quote-only lines (added by operator).
    contract_line_nums = {li.line_no for li in contract.line_items}
    for q_li in quote.line_items:
        if q_li.line_no in contract_line_nums:
            continue
        deltas.append(ContractDelta(
            severity="override",
            field_path=f"line_items[{q_li.line_no}]",
            contract_value=None,
            quote_value=q_li.description,
            detail="operator added a line the buyer did not request",
        ))

    return deltas


def delta_to_dict(d: ContractDelta) -> dict:
    return {
        "severity": d.severity,
        "field_path": d.field_path,
        "contract_value": d.contract_value,
        "quote_value": d.quote_value,
        "detail": d.detail,
    }
