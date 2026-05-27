"""Neutral home for cross-renderer types previously in cchcs_703b.py.

`ReytechIdentity` (vendor-side identity values: business name, FEIN,
seller's permit, address, contact info, payment terms) and
`SpineFormFillError` (raised when a filled-form matching gate disagrees
with its source model) are shared across every agency_forms renderer:
the CCHCS-specific fillers (703b/703c/704b/704c/bidpkg), the universal
state forms (std_204, std_1000, dvbe_843, darfur, calrecycle_74, cuf),
and the template resolver. They lived in `cchcs_703b.py` for historical
reasons — that module was the first to need them and ~10 others grew to
import them transitively.

This was moved here as PR-D-prep so the CCHCS-specific renderers can be
deleted in PR-Job1-D without breaking every other agency_forms importer.
The module is intentionally minimal — pure type definitions, no I/O, no
PDF logic, and zero legacy imports (verified by
`test_no_legacy_imports`).

Architect-approved 2026-05-27 per CLAUDE.md §0 LAW 4 (creation of a new
src/spine/ module).
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from src.spine.model import SpineValidationError


# ──────────────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────────────


class SpineFormFillError(SpineValidationError):
    """Raised when the matching gate finds the filled form bytes
    disagree with the source Quote + identity."""


# ──────────────────────────────────────────────────────────────────────
# Reytech identity — config-driven, not substrate-driven.
# ──────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ReytechIdentity:
    """Vendor-side identity values that every CCHCS form requires.

    Defaults are Reytech's REAL documented compliance record (per the
    Mike-shipped R26Q39 / R25Q161 buyer-facing quotes). Production
    overrides via REYTECH_* env vars only when something legitimately
    differs (e.g., FEIN change). Local dev "just works" because the
    defaults ARE the prod values — closes the 5/18 ship-blocking class
    where 703B/704B/bidpkg shipped with placeholder identity (caught
    line-by-line on R26Q40 vision-walk: Greenwald/President/Irvine,
    FEIN 00-0000000, seller's permit 000-000000 — every one of those
    would have failed CCHCS responsiveness review).

    The Spine substrate has NO vendor_* fields by Charter rule — this
    dataclass is the boundary between "operator config" and the
    Quote model.
    """
    business_name: str = "Reytech Inc."
    address: str = "30 Carnoustie Way, Trabuco Canyon, CA 92679"
    contact_person: str = "Michael Guadan"
    title: str = "Owner"
    phone: str = "949-229-1575"
    fax: str = ""
    email: str = "sales@reytechinc.com"
    fein: str = "00-0000000"                       # placeholder; set REYTECH_FEIN
    sellers_permit: str = "245652416-00001"        # CA Sellers Permit per R26Q39 letterhead
    cert_number: str = ""
    cert_expiration: str = ""
    payment_terms_days: str = "30"
    payment_discount_pct: str = "0"
    # Bid-response delivery commitment in calendar days from PO receipt.
    # 30 was the original placeholder; per Mike's R26Q39 buyer reference
    # ("Bidder offers and agrees if this response is accepted within 45
    # calendar days...") the bid-validity window is 45. Delivery_days
    # is read into the 703B "Deliveries must be completed within" field
    # so keeping 30 here is conservative — operator overrides at need.
    delivery_days: str = "30"

    @classmethod
    def from_env(cls) -> "ReytechIdentity":
        """Construct from REYTECH_* environment variables. Any unset
        var falls back to the dataclass default — local dev "just
        works" but production must explicitly populate FEIN +
        seller's permit at least."""
        return cls(
            business_name=os.environ.get("REYTECH_BUSINESS_NAME", cls.business_name),
            address=os.environ.get("REYTECH_ADDRESS", cls.address),
            contact_person=os.environ.get("REYTECH_CONTACT_PERSON", cls.contact_person),
            title=os.environ.get("REYTECH_CONTACT_TITLE", cls.title),
            phone=os.environ.get("REYTECH_PHONE", cls.phone),
            fax=os.environ.get("REYTECH_FAX", cls.fax),
            email=os.environ.get("REYTECH_EMAIL", cls.email),
            fein=os.environ.get("REYTECH_FEIN", cls.fein),
            sellers_permit=os.environ.get("REYTECH_SELLERS_PERMIT", cls.sellers_permit),
            cert_number=os.environ.get("REYTECH_CERT_NUMBER", cls.cert_number),
            cert_expiration=os.environ.get("REYTECH_CERT_EXPIRATION", cls.cert_expiration),
            payment_terms_days=os.environ.get(
                "REYTECH_PAYMENT_TERMS_DAYS", cls.payment_terms_days,
            ),
            payment_discount_pct=os.environ.get(
                "REYTECH_PAYMENT_DISCOUNT_PCT", cls.payment_discount_pct,
            ),
            delivery_days=os.environ.get("REYTECH_DELIVERY_DAYS", cls.delivery_days),
        )


__all__ = [
    "ReytechIdentity",
    "SpineFormFillError",
]
