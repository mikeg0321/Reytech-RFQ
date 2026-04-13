"""Detect CCHCS Non-IT RFQ Packets and tag PCs accordingly.

This is the Phase 5 detection layer — a tiny helper that the email
ingestion pipeline calls when creating a PC from an attachment. It
checks the source filename + email subject against the canonical
packet patterns and adds `packet_type=cchcs_non_it` to the PC dict
so downstream code (the Phase 4 generate route, the home queue, the
filler) can route accordingly.

Built 2026-04-13 overnight. Phase 5 of 5. See
_overnight_review/MORNING_REVIEW.md.

Why a separate module: keeping detection out of the email_poller and
out of process_rfq_email means we touch huge brittle modules with
the smallest possible surface — a one-liner hook into the existing
PC creation path. Detection logic lives here where it can be unit
tested in isolation and improved without re-running the full poller
suite.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, Optional

log = logging.getLogger("reytech.cchcs_detector")

# Tag value for the packet_type field — referenced by the route
# (routes_cchcs_packet.py) and the matcher when scoping searches.
PACKET_TYPE_CCHCS = "cchcs_non_it"


def tag_pc_if_packet(pc: Dict[str, Any]) -> bool:
    """Inspect a PC dict and tag it as a CCHCS packet if applicable.

    Mutates `pc` in place. Returns True if the PC was tagged (or was
    already tagged), False otherwise. Idempotent and safe to call
    multiple times.

    Detection inputs (in order of confidence):
      1. source_pdf basename
      2. email_subject
      3. email_attachment filenames if present in pc.get("attachments")

    The CCHCS packet pattern is owned by cchcs_packet_parser.
    looks_like_cchcs_packet — this helper is the routing wrapper that
    decides whether to apply it and where the result is stored.
    """
    if not isinstance(pc, dict):
        return False

    # Already tagged? Idempotent return
    if pc.get("packet_type") == PACKET_TYPE_CCHCS:
        return True

    try:
        from src.forms.cchcs_packet_parser import looks_like_cchcs_packet
    except Exception as e:
        log.debug("cchcs_detector: parser unavailable: %s", e)
        return False

    candidates: Iterable[str] = _collect_filenames(pc)
    subject = pc.get("email_subject", "") or ""

    for filename in candidates:
        if filename and looks_like_cchcs_packet(filename=filename, subject=subject):
            pc["packet_type"] = PACKET_TYPE_CCHCS
            log.info(
                "cchcs_detector: tagged PC %s as %s (filename=%r)",
                pc.get("id", "?"), PACKET_TYPE_CCHCS, filename[:60],
            )
            return True

    # Subject-only match (no filename hit but subject screams PREQ/Non-Cloud)
    if subject and looks_like_cchcs_packet(filename="", subject=subject):
        pc["packet_type"] = PACKET_TYPE_CCHCS
        log.info(
            "cchcs_detector: tagged PC %s as %s (subject=%r)",
            pc.get("id", "?"), PACKET_TYPE_CCHCS, subject[:60],
        )
        return True

    return False


def _collect_filenames(pc: Dict[str, Any]) -> list:
    """Pull every filename-ish string off a PC dict for detection."""
    out = []
    src = pc.get("source_pdf") or pc.get("source_file") or ""
    if src:
        out.append(os.path.basename(src))
    for att in pc.get("attachments", []) or []:
        if isinstance(att, dict):
            for k in ("filename", "name", "path"):
                v = att.get(k)
                if v:
                    out.append(os.path.basename(str(v)))
                    break
        elif isinstance(att, str):
            out.append(os.path.basename(att))
    return out


def backfill_existing_pcs(pcs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Walk every PC in `pcs` and tag any that are CCHCS packets but
    haven't been flagged yet. Returns a summary dict.

    Used by the one-shot backfill script and by an admin endpoint
    that operators can call after deploying Phase 5 to retroactively
    tag prod PCs that were ingested before this code existed.
    """
    summary = {"total": 0, "tagged_now": 0, "already_tagged": 0,
               "not_packet": 0, "tagged_ids": []}
    for pc_id, pc in pcs.items():
        if not isinstance(pc, dict):
            continue
        summary["total"] += 1
        if pc.get("packet_type") == PACKET_TYPE_CCHCS:
            summary["already_tagged"] += 1
            continue
        if tag_pc_if_packet(pc):
            summary["tagged_now"] += 1
            summary["tagged_ids"].append(pc_id)
        else:
            summary["not_packet"] += 1
    return summary


__all__ = [
    "tag_pc_if_packet",
    "backfill_existing_pcs",
    "PACKET_TYPE_CCHCS",
]
