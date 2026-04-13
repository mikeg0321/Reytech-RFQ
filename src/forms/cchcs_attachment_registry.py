"""Registry mapping CCHCS Non-IT RFQ packet placeholder pages to the
real attachment templates that should be filled and spliced into each
position.

The packet is an 18-page PDF where pages 6-14 are 1-page placeholder
sheets describing which attachment the supplier must include ("Attachment
1 — Bidder Declaration, see form GSPD-05-105", etc.). Those placeholder
pages get REPLACED at their original index with the filled real form.

This module is intentionally data-driven: adding support for a new CA
packet format (CalVet, CDCR bid package, etc.) means adding one entry
per attachment here. The splice helper in `cchcs_packet_filler.py`
iterates this list and swaps each placeholder with the filled template.

Attachments already handled inline (their form fields live on the
placeholder page itself) are NOT in this list:
    - Att 6  CUF Certification (page 11) — form fields inline
    - Att 9  GSPD-ITGP Non-Cloud (page 14) — reference doc, no fill
    - Att 11 AMS 708 GenAI (pages 15-18) — form fields inline

Built 2026-04-13 as part of the CCHCS packet automation PR.
"""
from __future__ import annotations

from typing import Callable, Dict, List, Optional, TypedDict


class AttachmentSpec(TypedDict):
    num: int                  # attachment number as labeled in packet
    placeholder_page: int     # 1-indexed page in the source packet
    template: str             # filename in data/templates/
    filler: str               # name of fill_fn in cchcs_attachment_fillers
    description: str


# Order matters: splicer walks in placeholder_page order and substitutes.
CCHCS_ATTACHMENTS: List[AttachmentSpec] = [
    {
        "num": 1,
        "placeholder_page": 6,
        "template": "bidder_declaration_blank.pdf",
        "filler": "fill_bidder_declaration",
        "description": "Bidder Declaration GSPD-05-105",
    },
    {
        "num": 2,
        "placeholder_page": 7,
        "template": "dvbe_843_blank.pdf",
        "filler": "fill_dvbe_843",
        "description": "DVBE Program Declarations STD 843",
    },
    {
        "num": 3,
        "placeholder_page": 8,
        "template": "calrecycle_74_blank.pdf",
        "filler": "fill_calrecycle_74",
        "description": "Postconsumer Recycled-Content Certification (CalRecycle 74)",
    },
    {
        "num": 4,
        "placeholder_page": 9,
        "template": "std204_blank.pdf",
        "filler": "fill_std204",
        "description": "Payee Data Record STD 204",
    },
    {
        "num": 5,
        "placeholder_page": 10,
        "template": "sellers_permit_reytech.pdf",
        "filler": "splice_static",
        "description": "California Retailer Seller's Permit (pre-filled static)",
    },
    {
        "num": 7,
        "placeholder_page": 12,
        "template": "ca_civil_rights_attachment_blank.pdf",
        "filler": "fill_ca_civil_rights",
        "description": "California Civil Rights Laws Attachment",
    },
    {
        "num": 8,
        "placeholder_page": 13,
        "template": "darfur_act_blank.pdf",
        "filler": "fill_darfur_act",
        "description": "DARFUR Contracting Act Certification",
    },
]


def placeholder_page_set() -> set:
    """Return a set of 1-indexed page numbers that the splicer should
    replace. Any packet page NOT in this set is copied as-is."""
    return {a["placeholder_page"] for a in CCHCS_ATTACHMENTS}


def spec_for_page(page_num: int) -> Optional[AttachmentSpec]:
    for a in CCHCS_ATTACHMENTS:
        if a["placeholder_page"] == page_num:
            return a
    return None


__all__ = [
    "CCHCS_ATTACHMENTS",
    "AttachmentSpec",
    "placeholder_page_set",
    "spec_for_page",
]
