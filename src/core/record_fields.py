"""Canonical read accessors for PC and RFQ records.

Why this exists: PC → RFQ conversion is deepcopy (per CLAUDE.md — no field
remapping on write). That means a converted RFQ carries whatever field names
the PC used, and an originally-ingested RFQ carries whatever the ingest layer
produced. Downstream consumers (QA, 704 fill, package builder, UI) must
tolerate both shapes WITHOUT mutating the stored record.

This module is the single place alias lists live. Every reader should use
these accessors instead of reinventing `.get("foo") or .get("bar") or ...`
chains scattered across the codebase.
"""
from __future__ import annotations


def _first_non_empty(d: dict, keys):
    for k in keys:
        v = d.get(k)
        if v not in (None, "", 0, 0.0):
            return v
    return None


def _first_numeric(d: dict, keys) -> float:
    for k in keys:
        v = d.get(k)
        if v in (None, ""):
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f:
            return f
    return 0.0


# ── Item-level readers ────────────────────────────────────────────────────

def item_unit_price(item: dict) -> float:
    """Per-unit sell price. Canonical = unit_price; tolerates bid/our/pricing variants."""
    if not isinstance(item, dict):
        return 0.0
    direct = _first_numeric(item, ("unit_price", "bid_price", "our_price", "our_quote"))
    if direct:
        return direct
    pricing = item.get("pricing") or {}
    if isinstance(pricing, dict):
        return _first_numeric(pricing, ("recommended_price", "bid_price", "unit_price"))
    return 0.0


def item_unit_cost(item: dict) -> float:
    """Per-unit cost basis. Canonical = vendor_cost (item) or pricing.unit_cost."""
    if not isinstance(item, dict):
        return 0.0
    pricing = item.get("pricing") or {}
    if isinstance(pricing, dict):
        c = _first_numeric(pricing, ("unit_cost", "vendor_cost", "catalog_cost", "web_cost"))
        if c:
            return c
    return _first_numeric(item, ("vendor_cost", "unit_cost", "supplier_cost"))


def item_qty(item: dict) -> float:
    if not isinstance(item, dict):
        return 0.0
    return _first_numeric(item, ("qty", "quantity"))


def item_mfg(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    return str(_first_non_empty(item, ("mfg_number", "mfg", "part_number", "mfr_part_number")) or "")


def item_description(item: dict) -> str:
    if not isinstance(item, dict):
        return ""
    return str(_first_non_empty(item, ("description", "desc", "name")) or "")


def item_link(item: dict) -> str:
    """Supplier/product URL for this item."""
    if not isinstance(item, dict):
        return ""
    direct = _first_non_empty(item, ("item_link", "supplier_url", "product_url", "url"))
    if direct:
        return str(direct)
    pricing = item.get("pricing") or {}
    if isinstance(pricing, dict):
        return str(_first_non_empty(pricing, ("supplier_url", "amazon_url", "product_url")) or "")
    return ""


# ── Record-level readers (PC and RFQ share these aliases) ────────────────

def record_ship_to(rec: dict) -> str:
    if not isinstance(rec, dict):
        return ""
    return str(_first_non_empty(rec, ("ship_to", "ship_to_name", "delivery_location", "deliver_to")) or "")


def record_requestor(rec: dict) -> str:
    if not isinstance(rec, dict):
        return ""
    return str(_first_non_empty(rec, ("requestor", "requestor_name", "buyer", "contact_name")) or "")


def record_requestor_email(rec: dict) -> str:
    if not isinstance(rec, dict):
        return ""
    return str(_first_non_empty(rec, ("requestor_email", "email", "buyer_email", "contact_email")) or "")


def record_pc_number(rec: dict) -> str:
    if not isinstance(rec, dict):
        return ""
    return str(_first_non_empty(rec, ("pc_number", "solicitation_number", "rfq_number")) or "")


def record_agency(rec: dict) -> str:
    if not isinstance(rec, dict):
        return ""
    return str(_first_non_empty(rec, ("agency", "agency_key", "institution")) or "")


def record_items(rec: dict) -> list:
    """Return the items list regardless of which key holds it. Does NOT copy."""
    if not isinstance(rec, dict):
        return []
    items = rec.get("items")
    if items:
        return items
    items = rec.get("line_items")
    return items if items else []


def record_total(rec: dict) -> float:
    if not isinstance(rec, dict):
        return 0.0
    return _first_numeric(rec, ("total", "total_price", "grand_total"))


def record_subtotal(rec: dict) -> float:
    if not isinstance(rec, dict):
        return 0.0
    return _first_numeric(rec, ("subtotal", "sub_total", "items_total"))


# ── Catalog hydration: catalog-is-bible read-side backfill ───────────────

def hydrate_item_from_catalog(item: dict) -> None:
    """Backfill item_link / photo_url / mfg_number / upc / manufacturer
    from the product catalog. Mutates `item` in place — only fills fields
    that are empty, never overwrites operator-entered values.

    Architecture: autosave writes everything the operator sources
    (supplier URL, photo, UPC) to the product_catalog + product_suppliers
    tables. This is the matching READ half — on every RFQ/PC detail render,
    empty display fields are re-populated from the catalog. Reloads never
    lose what was already saved.
    """
    import logging as _logging
    _log = _logging.getLogger("record_fields.hydrate")

    if not isinstance(item, dict):
        return

    needs = any(not item.get(f) for f in ("item_link", "photo_url", "mfg_number", "upc", "manufacturer"))
    if not needs:
        return

    desc = item.get("description", "") or ""
    pn = item.get("mfg_number", "") or item.get("item_number", "") or ""
    upc = item.get("upc", "") or ""
    if not desc and not pn and not upc:
        return

    try:
        from src.agents.product_catalog import match_item, get_product_suppliers
    except Exception as _e:
        _log.debug("hydrate import: %s", _e)
        return

    try:
        matches = match_item(desc, part_number=pn, top_n=1, upc=upc)
    except Exception as _e:
        _log.debug("hydrate match: %s", _e)
        return
    if not matches:
        return

    m = matches[0]
    # Confidence threshold is flag-tunable so prod can nudge it up/down via
    # /api/admin/flags without a deploy. Default preserves prior 0.75 gate.
    try:
        from src.core.flags import get_flag
        _threshold = float(get_flag("pipeline.confidence_threshold", 0.75))
    except Exception:
        _threshold = 0.75
    if (m.get("match_confidence") or 0) < _threshold:
        return

    if not item.get("photo_url") and m.get("photo_url"):
        item["photo_url"] = m["photo_url"]
    if not item.get("mfg_number") and m.get("mfg_number"):
        item["mfg_number"] = m["mfg_number"]
    if not item.get("upc") and m.get("upc"):
        item["upc"] = m["upc"]
    if not item.get("manufacturer") and m.get("manufacturer"):
        item["manufacturer"] = m["manufacturer"]

    if not item.get("item_link"):
        pid = m.get("id")
        if pid:
            try:
                suppliers = get_product_suppliers(pid)
            except Exception as _e:
                _log.debug("hydrate suppliers: %s", _e)
                suppliers = []
            for s in suppliers:
                url = (s.get("supplier_url") or "").strip()
                if url:
                    item["item_link"] = url
                    if not item.get("supplier_name"):
                        item["supplier_name"] = s.get("supplier_name", "")
                    break


# ── QA adapter: normalized view without mutating source ──────────────────

def build_qa_view(rec: dict) -> dict:
    """Build a canonical view suitable for pc_qa_agent.run_qa().

    Does NOT mutate `rec`. Returns a shallow-copy dict with:
      - `items` populated from items OR line_items
      - every item carries canonical `unit_price`, `vendor_cost`, `qty`
        (added as additional keys; originals preserved)
      - top-level `ship_to`, `requestor`, `agency`, `institution`,
        `pc_number`, `solicitation_number`, `total`, `subtotal`
        resolved through alias lists

    CLAUDE.md rule preserved: source record is NOT modified. Only the
    returned dict has canonicalized fields — it is a VIEW, not a rewrite.
    """
    if not isinstance(rec, dict):
        return {"items": []}

    view = dict(rec)
    items_src = record_items(rec)

    normalized_items = []
    for it in items_src:
        if not isinstance(it, dict):
            normalized_items.append(it)
            continue
        canon = dict(it)
        up = item_unit_price(it)
        uc = item_unit_cost(it)
        q = item_qty(it)
        if up and not canon.get("unit_price"):
            canon["unit_price"] = up
        if uc and not canon.get("vendor_cost"):
            canon["vendor_cost"] = uc
        if q and not canon.get("qty"):
            canon["qty"] = q
        normalized_items.append(canon)

    view["items"] = normalized_items

    ship = record_ship_to(rec)
    if ship:
        view["ship_to"] = ship
    requestor = record_requestor(rec)
    if requestor:
        view["requestor"] = requestor
    agency = record_agency(rec)
    if agency:
        view["agency"] = agency
        view.setdefault("institution", agency)
    pc_num = record_pc_number(rec)
    if pc_num:
        view["pc_number"] = pc_num
        view.setdefault("solicitation_number", pc_num)
    total = record_total(rec)
    if total:
        view["total"] = total
    subtotal = record_subtotal(rec)
    if subtotal:
        view["subtotal"] = subtotal

    return view
