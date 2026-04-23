"""One-shot: repair pc_5063d1cd so the detail page renders the $179.09
pricing. The item was seeded with flat top-level unit_price/ext_price
but the template reads item['pricing'] (nested). Hydrate with the full
expected shape.
"""
from __future__ import annotations


def _main() -> int:
    from src.api.data_layer import _load_price_checks, _save_single_pc

    pcs = _load_price_checks()
    pc = pcs.get("pc_5063d1cd")
    if not pc:
        print("pc_5063d1cd not found")
        return 2

    item = dict(pc["items"][0]) if pc.get("items") else {}
    item.update({
        "description": item.get("description", "BLS Provider Course Videos: USB"),
        "qty": 2,
        "quantity": 2,
        "uom": "EACH",
        "unit_of_issue": "EACH",
        "unit_cost": 179.09,
        "supplier_cost": 179.09,
        "unit_price": 179.09,
        "price_per_unit": 179.09,
        "extension": 358.18,
        "line_total": 358.18,
        "pricing": {
            "unit_cost": 179.09,
            "markup_pct": 0.0,
            "unit_price": 179.09,
            "extension": 358.18,
            "recommended_price": 179.09,
            "price_source": "manual:recovered-from-desktop-pdf",
            "confidence": 1.0,
        },
    })
    pc["items"] = [item]
    pc["line_items"] = [item]
    _save_single_pc("pc_5063d1cd", pc)
    print("patched pc_5063d1cd")
    print("keys on pricing:", list(item["pricing"].keys()))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
