"""Bundle-6 PR-6a — linker→pricing-copy post-link hook.

Closes audit item "linker→pricing copy" from project_2026_04_22_session_audit.
Root pain: the triangulated linker would bind an RFQ to its prior PC, but
the operator still had to navigate to the PC to read the price — or, worse,
land on the PC detail page and accidentally re-price there instead of the
RFQ (actual incident: Mike on /pricecheck/pc_5063d1cd when he should have
been on /rfq/9ad8a0ac).

This file locks the hook's contract:
  * Runs only when the linker returns a non-empty pc_id.
  * Copies pricing only for RFQ items with no existing pricing.
  * Idempotent: items already stamped with pricing_copied_from_pc are
    skipped so re-running doesn't double-write.
  * Description similarity threshold >= 0.75 (same as linker anchor 4).
  * Stamps the RFQ record with pricing_copied_from_pc so the detail page
    can render the "Pricing copied from PC #X" banner.
"""
from __future__ import annotations

import pytest


def _call(rfq_id, pc_id, rfq_items):
    from src.core.ingest_pipeline import _copy_pc_pricing_to_rfq
    return _copy_pc_pricing_to_rfq(rfq_id, pc_id, rfq_items)


def _seed_pc(pid, items):
    from src.api.data_layer import _save_single_pc
    _save_single_pc(pid, {
        "id": pid, "status": "priced", "pc_number": "PC-" + pid,
        "institution": "CCHCS", "items": items,
    })


def _seed_rfq(rid, items):
    from src.api.data_layer import _save_single_rfq
    _save_single_rfq(rid, {
        "id": rid, "status": "new", "rfq_number": "RFQ-" + rid,
        "solicitation_number": "RFQ-" + rid,
        "line_items": items, "items": items,
    })


def test_copy_happy_path_matching_descriptions(temp_data_dir):
    """PC has priced items with matching descriptions → RFQ gets pricing."""
    pc_items = [
        {"description": "Engraved name tag, black/white", "qty": 10,
         "supplier_cost": 12.0, "price_per_unit": 15.0, "markup_pct": 25,
         "pricing": {"recommended_price": 15.0, "price_source": "amazon"}},
    ]
    rfq_items = [
        {"description": "Engraved name tag, black/white", "qty": 10, "uom": "EA"},
    ]
    _seed_pc("pc_happy", pc_items)
    _seed_rfq("rfq_happy", rfq_items)

    report = _call("rfq_happy", "pc_happy", rfq_items)

    assert report["copied"] == 1
    assert report["skipped"] == 0
    # Flat fields copied:
    assert rfq_items[0]["price_per_unit"] == 15.0
    assert rfq_items[0]["supplier_cost"] == 12.0
    assert rfq_items[0]["markup_pct"] == 25
    # Nested pricing subdict merged:
    assert rfq_items[0]["pricing"]["recommended_price"] == 15.0
    assert rfq_items[0]["pricing"]["price_source"] == "amazon"
    # Audit stamps:
    assert rfq_items[0]["pricing_copied_from_pc"] == "pc_happy"
    assert rfq_items[0]["pricing_copied_at"]


def test_idempotent_skips_already_copied(temp_data_dir):
    """An RFQ item already carrying pricing_copied_from_pc is NOT re-touched."""
    pc_items = [{"description": "Widget A", "qty": 1,
                 "supplier_cost": 20.0, "price_per_unit": 30.0,
                 "pricing": {"recommended_price": 30.0}}]
    rfq_items = [{"description": "Widget A", "qty": 1,
                  "pricing_copied_from_pc": "some_other_pc",
                  "price_per_unit": 99.99}]  # user's manual override
    _seed_pc("pc_idem", pc_items)
    _seed_rfq("rfq_idem", rfq_items)

    report = _call("rfq_idem", "pc_idem", rfq_items)

    assert report["copied"] == 0
    assert report["skipped"] == 1
    assert rfq_items[0]["price_per_unit"] == 99.99
    assert rfq_items[0]["pricing_copied_from_pc"] == "some_other_pc"


def test_skips_items_with_existing_price(temp_data_dir):
    """RFQ items already priced (by a human) must not be overwritten."""
    pc_items = [{"description": "Thing B", "qty": 1,
                 "supplier_cost": 5.0, "price_per_unit": 7.0,
                 "pricing": {"recommended_price": 7.0}}]
    rfq_items = [{"description": "Thing B", "qty": 1, "price_per_unit": 99.0}]
    _seed_pc("pc_existing", pc_items)
    _seed_rfq("rfq_existing", rfq_items)

    report = _call("rfq_existing", "pc_existing", rfq_items)
    assert report["copied"] == 0
    assert rfq_items[0]["price_per_unit"] == 99.0
    assert "pricing_copied_from_pc" not in rfq_items[0]


def test_skips_when_no_desc_match(temp_data_dir):
    """Low desc similarity (<0.75) → no copy."""
    pc_items = [{"description": "Something completely unrelated",
                 "supplier_cost": 5, "price_per_unit": 10,
                 "pricing": {"recommended_price": 10}}]
    rfq_items = [{"description": "Office chair ergonomic", "qty": 1}]
    _seed_pc("pc_nomatch", pc_items)
    _seed_rfq("rfq_nomatch", rfq_items)

    report = _call("rfq_nomatch", "pc_nomatch", rfq_items)
    assert report["copied"] == 0
    assert report["skipped"] == 1


def test_missing_pc_returns_zero(temp_data_dir):
    """Linker returned a pc_id that no longer exists — hook no-ops, returns
    zero copies, never raises."""
    rfq_items = [{"description": "x", "qty": 1}]
    report = _call("rfq_missing_pc", "pc_does_not_exist", rfq_items)
    assert report["copied"] == 0
    assert "not found" in report["reason"]


def test_rfq_record_stamped_with_pc_id_for_banner(temp_data_dir):
    """After a copy, the RFQ record carries pricing_copied_from_pc so the
    detail template can render the 'Pricing copied from PC #X' banner."""
    pc_items = [{"description": "Widget Q", "qty": 1,
                 "supplier_cost": 5.0, "price_per_unit": 8.0,
                 "pricing": {"recommended_price": 8.0}}]
    rfq_items = [{"description": "Widget Q", "qty": 1}]
    _seed_pc("pc_banner", pc_items)
    _seed_rfq("rfq_banner", rfq_items)

    report = _call("rfq_banner", "pc_banner", rfq_items)
    assert report["copied"] == 1

    from src.api.data_layer import load_rfqs
    rfq = load_rfqs()["rfq_banner"]
    assert rfq["pricing_copied_from_pc"] == "pc_banner"
    assert rfq["pricing_copied_at"]


def test_pc_with_no_pricing_is_skipped(temp_data_dir):
    """If the PC item has a matching description but no pricing at all,
    there's nothing useful to copy — skip."""
    pc_items = [{"description": "Widget R", "qty": 1}]  # no prices
    rfq_items = [{"description": "Widget R", "qty": 1}]
    _seed_pc("pc_empty", pc_items)
    _seed_rfq("rfq_empty", rfq_items)

    report = _call("rfq_empty", "pc_empty", rfq_items)
    assert report["copied"] == 0


def test_rfq_detail_renders_pricing_copied_banner(auth_client, temp_data_dir):
    """Template-side: when rfq.pricing_copied_from_pc is set, the banner
    renders with the correct data-testid + PC link."""
    from src.api.data_layer import _save_single_rfq
    _save_single_rfq("rfq_bn_render", {
        "id": "rfq_bn_render", "status": "new",
        "rfq_number": "RFQ-BN", "solicitation_number": "RFQ-BN",
        "linked_pc_id": "pc_src_12345678",
        "linked_pc_number": "PC-SRC",
        "pricing_copied_from_pc": "pc_src_12345678",
        "line_items": [{"description": "X", "qty": 1, "uom": "EA"}],
    })
    resp = auth_client.get("/rfq/rfq_bn_render")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    assert 'data-testid="rfq-pricing-copied-banner"' in html
    assert "/pricecheck/pc_src_12345678" in html


def test_rfq_detail_hides_banner_when_no_copy(auth_client, temp_data_dir):
    """Absent pricing_copied_from_pc → no banner rendered."""
    from src.api.data_layer import _save_single_rfq
    _save_single_rfq("rfq_bn_off", {
        "id": "rfq_bn_off", "status": "new",
        "rfq_number": "RFQ-OFF", "solicitation_number": "RFQ-OFF",
        "line_items": [{"description": "X", "qty": 1, "uom": "EA"}],
    })
    resp = auth_client.get("/rfq/rfq_bn_off")
    assert resp.status_code == 200
    assert b'rfq-pricing-copied-banner' not in resp.data
