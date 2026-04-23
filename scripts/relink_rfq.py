"""One-shot: re-run the triangulated linker on a specific RFQ that missed
its PC link under the old 2-anchor rule.

Usage (after PR #433 deploys to prod):
    railway ssh "python scripts/relink_rfq.py 9ad8a0ac"

Dry-run (no save, just show what the linker would return):
    railway ssh "python scripts/relink_rfq.py 9ad8a0ac --dry"

Relies on the updated _run_triangulated_linker from PR #433 (strong
verbatim-item override + institution→agency fallback).
"""
from __future__ import annotations

import json
import sys


def _main(rfq_id: str, dry_run: bool = False) -> int:
    from src.api.data_layer import load_rfqs, _save_single_rfq
    from src.core.ingest_pipeline import _run_triangulated_linker
    from src.core.request_classifier import RequestClassification

    rfqs = load_rfqs()
    # Prefix-match because Mike often pastes the 8-char id, full id starts with it.
    match = None
    for rid, rfq in rfqs.items():
        if rid == rfq_id or rid.startswith(rfq_id):
            match = (rid, rfq)
            break
    if not match:
        print(f"RFQ {rfq_id!r} not found", file=sys.stderr)
        return 2

    rid, rfq = match
    print(f"Found RFQ {rid}")
    print(f"  rfq_number:          {rfq.get('rfq_number')}")
    print(f"  solicitation_number: {rfq.get('solicitation_number')!r}")
    print(f"  agency:              {rfq.get('agency')!r}")
    print(f"  institution:         {rfq.get('institution')!r}")
    print(f"  current linked_pc_id: {rfq.get('linked_pc_id')!r}")
    print(f"  current link_reason:  {rfq.get('link_reason')!r}")

    items = rfq.get("line_items") or rfq.get("items") or []
    if isinstance(items, str):
        try:
            items = json.loads(items)
        except Exception:
            items = []
    print(f"  items: {len(items)}")
    for i, it in enumerate(items[:10]):
        print(f"    {i+1}. {(it.get('description') or it.get('desc') or '')[:90]}")

    classification = RequestClassification(
        shape=rfq.get("form_type") or "",
        agency=(rfq.get("agency") or "").lower() or "other",
        institution=(rfq.get("institution") or "").lower(),
        solicitation_number=(rfq.get("solicitation_number") or "").strip(),
    )
    print()
    print("Running linker...")
    linked_pc_id, reason, confidence = _run_triangulated_linker(rid, classification, items)
    print(f"  linked_pc_id: {linked_pc_id!r}")
    print(f"  reason:       {reason!r}")
    print(f"  confidence:   {confidence}")

    if not linked_pc_id:
        print("No link found — leaving RFQ untouched.")
        return 0

    if dry_run:
        print("[--dry] Not saving. Re-run without --dry to persist.")
        return 0

    rfq["linked_pc_id"] = linked_pc_id
    rfq["link_reason"] = reason
    rfq["link_confidence"] = confidence
    _save_single_rfq(rid, rfq)
    print(f"Saved: RFQ {rid} now linked to PC {linked_pc_id}")
    return 0


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--dry"]
    dry = "--dry" in sys.argv[1:]
    if not args:
        print("Usage: python scripts/relink_rfq.py <rfq_id_or_prefix> [--dry]", file=sys.stderr)
        raise SystemExit(1)
    raise SystemExit(_main(args[0], dry_run=dry))
