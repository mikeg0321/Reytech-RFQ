"""Auto-price carry-forward — copies validated costs from a linked PC
into a new RFQ on exact-MFG# matches.

Closes Mike's 5/17 directive end-to-end: "PC goes out, RFQ comes in,
should be auto priced". The auto-link substrate (PR #1041/#1042) put
the (RFQ, PC) pair in evidence; this layer carries the actual numbers.

DESIGN
- Pure function. Takes two Quotes, returns (new target Quote, summary
  dict). No DB writes here — the caller (ingest hookup, queued
  PR #1044) decides whether to persist.
- Match by NORMALIZED MFG# only. Description-based matching is too
  noisy for an auto action that touches money. The PC's validated cost
  carries forward; the new RFQ's other line items are untouched.
- Carry rule: target's `cost_cents` MUST be 0 (operator hasn't priced
  it yet). If the operator already typed a cost, we don't overwrite —
  but we DO record any divergence in the `deltas` summary so they can
  see what the prior PC charged.
- Carried fields: cost_cents, cost_source_url, cost_validated_at.
  unit_price_cents is NEVER carried (operator chooses markup per quote).
  cost_hand_validated_note gets a stamped CARRIED prefix so the
  provenance is operator-readable and machine-parseable.
- Staleness handling: we copy cost_validated_at verbatim from the PC.
  If the PC validated 60 days ago, the LineItem's freshness check in
  the FINALIZED transition will refuse — forcing the operator to
  re-verify. That's correct: we shouldn't ship on stale costs even
  if a prior bid used them.
"""
from __future__ import annotations

import re

from src.spine.model import LineItem, Quote


# Note prefix that marks a carried cost. Operator-readable; machine-
# parseable via parse_carry_note().
COST_CARRY_PREFIX = "CARRIED:"

_CARRY_NOTE_RE = re.compile(
    r"^CARRIED:(?P<from_id>[A-Za-z0-9_\-]+):(?P<from_line>\d+)"
    r"(?:\s+\((?P<meta>[^)]*)\))?"
    r"(?:\s*\|\s*(?P<existing>.*))?$"
)


def _norm_mfg(s: str | None) -> str | None:
    """Mirror of quote_matcher._norm_mfg — keep them in sync.

    Could deduplicate by importing, but the matcher and the pricer
    might evolve their normalization independently. Keep both
    fully-spelled for now; add a shared util when the rules diverge.
    """
    if s is None:
        return None
    out = s.strip().upper().rstrip(".,;:")
    return out or None


def _make_carry_note(
    source_quote_id: str,
    source_line_no: int,
    source_validated_at,
    existing_note: str | None,
) -> str:
    """Build the stamped note string. Preserves any prior operator
    note after the pipe."""
    meta = (
        f"validated_at={source_validated_at.isoformat()}"
        if source_validated_at is not None
        else "validated_at=unknown"
    )
    stamp = f"{COST_CARRY_PREFIX}{source_quote_id}:{source_line_no} ({meta})"
    existing = (existing_note or "").strip()
    if existing:
        return f"{stamp} | {existing}"
    return stamp


def parse_carry_note(note: str | None) -> dict | None:
    """Extract provenance from a carry-stamped note.

    Returns dict with from_quote_id, from_line_no, meta, existing_note
    on a match. Returns None when the note isn't carry-stamped — the
    caller distinguishes "no provenance" from "operator-written note".
    """
    if not note:
        return None
    m = _CARRY_NOTE_RE.match(note.strip())
    if not m:
        return None
    return {
        "from_quote_id": m.group("from_id"),
        "from_line_no": int(m.group("from_line")),
        "meta": m.group("meta"),
        "existing_note": (m.group("existing") or "").strip() or None,
    }


def carry_forward_costs(
    target: Quote,
    source: Quote,
) -> tuple[Quote, dict]:
    """Carry validated costs from `source` (prior PC) into `target`
    (new RFQ) for line items matched by normalized MFG#.

    Returns:
        (new_target_quote, summary_dict)

        summary_dict:
            {
                "carried": [
                    {"target_line_no": int, "source_line_no": int,
                     "source_quote_id": str, "cost_cents": int}
                ],
                "skipped_no_match": [int, ...],   # target line_nos
                "skipped_already_priced": [int, ...],
                "skipped_no_mfg": [int, ...],
                "deltas": [
                    {"target_line_no": int, "source_line_no": int,
                     "target_cost_cents": int, "source_cost_cents": int}
                ],
                "source_quote_id": str,
            }

    Idempotent: running carry_forward_costs twice with the same inputs
    produces the same result. The second run sees the carried lines as
    already_priced (because cost_cents > 0 now) — they're already
    carried, no further action needed.
    """
    # Build source MFG# index: norm_mfg → list of (line_no, line_item)
    source_index: dict[str, list[tuple[int, LineItem]]] = {}
    for li in source.line_items:
        norm = _norm_mfg(li.mfg_number)
        if norm is None:
            continue
        source_index.setdefault(norm, []).append((li.line_no, li))

    carried: list[dict] = []
    skipped_no_match: list[int] = []
    skipped_already_priced: list[int] = []
    skipped_no_mfg: list[int] = []
    deltas: list[dict] = []

    new_lines: list[LineItem] = []
    for tli in target.line_items:
        norm = _norm_mfg(tli.mfg_number)
        if norm is None:
            new_lines.append(tli)
            skipped_no_mfg.append(tli.line_no)
            continue

        matches = source_index.get(norm)
        if not matches:
            new_lines.append(tli)
            skipped_no_match.append(tli.line_no)
            continue

        # If the PC has the same MFG# on multiple lines (rare but
        # possible with different qty/uom), prefer the one with the
        # highest validated cost — most authoritative.
        source_line_no, src = max(matches, key=lambda pair: pair[1].cost_cents)

        if tli.cost_cents > 0:
            # Operator already priced. Record a delta if the prior PC
            # charged something different so the editor can show it.
            if src.cost_cents > 0 and src.cost_cents != tli.cost_cents:
                deltas.append({
                    "target_line_no": tli.line_no,
                    "source_line_no": source_line_no,
                    "target_cost_cents": tli.cost_cents,
                    "source_cost_cents": src.cost_cents,
                })
            new_lines.append(tli)
            skipped_already_priced.append(tli.line_no)
            continue

        if src.cost_cents == 0:
            # PC line has no cost either — nothing to carry.
            new_lines.append(tli)
            skipped_no_match.append(tli.line_no)
            continue

        # Carry forward.
        new_note = _make_carry_note(
            source.quote_id,
            source_line_no,
            src.cost_validated_at,
            tli.cost_hand_validated_note,
        )
        new_line = tli.model_copy(update={
            "cost_cents": src.cost_cents,
            "cost_source_url": src.cost_source_url,
            "cost_validated_at": src.cost_validated_at,
            "cost_hand_validated_note": new_note,
        })
        new_lines.append(new_line)
        carried.append({
            "target_line_no": tli.line_no,
            "source_line_no": source_line_no,
            "source_quote_id": source.quote_id,
            "cost_cents": src.cost_cents,
        })

    new_target = target.model_copy(update={"line_items": new_lines})
    summary = {
        "source_quote_id": source.quote_id,
        "carried": carried,
        "skipped_no_match": skipped_no_match,
        "skipped_already_priced": skipped_already_priced,
        "skipped_no_mfg": skipped_no_mfg,
        "deltas": deltas,
    }
    return new_target, summary
