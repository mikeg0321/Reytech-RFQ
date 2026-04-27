"""Unified ship-to resolution for PC, RFQ, and the CRM buyer-lookup API.

Priority chain (first hit wins):
  1. SCPRS PO history — most-common ship-to for this buyer's email/name.
     This is the strongest signal: the buyer's own past orders tell us
     exactly where they want things delivered.
  2. CRM customer records — matched by name or email against the local
     customers list; returns the stored mailing address.
  3. Canonical facility registry via `quote_contract.ship_to_for_text` —
     CA facility address from `core/facility_registry.FacilityRecord`
     (CDCR / CCHCS / CalVet / DSH, sourced from the agencies' public
     facility lists on CA.gov).

Prior to 2026-04-14, the PC and RFQ detail-load auto-fill only hit
step 3, losing the CRM contact + PO history signal that the
`/api/crm/buyer-lookup` API already exposed. Centralizing the logic
here means all three code paths agree on "what's the ship-to for this
buyer".
"""
import logging
from typing import Callable, Optional

log = logging.getLogger("reytech")


def lookup_buyer_ship_to(
    name: str = "",
    email: str = "",
    institution: str = "",
    _load_customers: Optional[Callable] = None,
) -> dict:
    """Resolve a ship-to address for a buyer via the full priority chain.

    Returns ``{"ship_to": str, "institution": str, "agency": str,
    "source": str}``. ``source`` is ``"po_history"``, ``"crm_contact"``,
    ``"institution_resolver"``, or ``""`` when nothing matched.

    ``_load_customers`` is injected by callers that want step 2 to run
    against the local CRM customers list. Callers that don't supply it
    (e.g. PC/RFQ detail load — no customers cache handy) get steps 1
    and 3 only.
    """
    name = (name or "").strip()
    email = (email or "").strip()
    institution = (institution or "").strip()
    ship_to = ""
    inst = ""
    agency = ""
    source = ""

    # 1. SCPRS PO history
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = None
            if email:
                row = conn.execute("""
                    SELECT ship_to_address, dept_name, COUNT(*) as cnt
                    FROM scprs_po_master WHERE buyer_email = ? AND ship_to_address != ''
                    GROUP BY ship_to_address ORDER BY cnt DESC LIMIT 1
                """, (email,)).fetchone()
            if not row and name:
                row = conn.execute("""
                    SELECT ship_to_address, dept_name, COUNT(*) as cnt
                    FROM scprs_po_master WHERE buyer_name LIKE ? AND ship_to_address != ''
                    GROUP BY ship_to_address ORDER BY cnt DESC LIMIT 1
                """, (f"%{name}%",)).fetchone()
            if row and row[0]:
                ship_to = row[0]
                inst = row[1] or ""
                source = "po_history"
    except Exception as e:
        log.debug("lookup_buyer_ship_to PO search: %s", e)

    # 2. CRM customer records (only if caller injected a loader)
    if not ship_to and _load_customers and (name or email):
        try:
            customers = _load_customers() or []
            q = (email or name).lower()
            for c in customers:
                searchable = " ".join([
                    c.get("display_name") or "",
                    c.get("company") or "",
                    c.get("qb_name") or "",
                    c.get("email") or "",
                ]).lower()
                if q and q in searchable:
                    addr_parts = [c.get("display_name") or c.get("company") or ""]
                    if c.get("address"):
                        addr_parts.append(c["address"])
                    if c.get("city"):
                        addr_parts.append(c["city"])
                    if c.get("state"):
                        addr_parts.append(c["state"])
                    if c.get("zip"):
                        addr_parts.append(c["zip"])
                    ship_to = ", ".join(p for p in addr_parts if p)
                    inst = c.get("display_name") or c.get("company") or ""
                    agency = c.get("agency") or ""
                    source = "crm_contact"
                    break
        except Exception as e:
            log.debug("lookup_buyer_ship_to CRM search: %s", e)

    # 3. Canonical facility_registry via the QuoteContract facade.
    # Was: `institution_resolver.get_ship_to_address`. Migrated 2026-04-25;
    # the legacy helper + `_FACILITY_ADDRESSES` parallel dict were deleted
    # 2026-04-27 (S2 follow-up). This facade is the only ship-to path now.
    if not ship_to:
        try:
            from src.core.quote_contract import ship_to_for_text
            for q in (institution, name, inst):
                if not q:
                    continue
                auto = ship_to_for_text(q)
                if auto:
                    ship_to = auto
                    source = "canonical_facility_registry"
                    if not inst:
                        inst = q
                    break
        except Exception as e:
            log.debug("lookup_buyer_ship_to canonical: %s", e)

    return {
        "ship_to": ship_to,
        "institution": inst,
        "agency": agency,
        "source": source,
    }
