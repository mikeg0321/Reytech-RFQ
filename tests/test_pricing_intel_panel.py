"""Tier 1a / PR-D1 — inline pricing intel panel (audit 2026-05-07).

Tests the helper module (`src.core.pricing_intel_panel`) + the new
`/api/rfq/<rid>/pricing-intel` endpoint + the rfq_detail.html chip
slots that the JS populates.

The audit's framing: pricing decisions happen on rfq_detail.html, but
the data that should drive them (last-won-by-buyer, SCPRS ceiling)
lives 2 clicks deep on /growth-intel/buyer. Closing that gap is the
single highest win-rate lift in the audit.
"""
from __future__ import annotations

import json
import os
import sqlite3

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Helper: last_won_for_buyer
# ─────────────────────────────────────────────────────────────────────────────


def _seed_won_quote(temp_data_dir, *, qn, contact_email, items, sent_at):
    """Insert a `won` row in the test `quotes` table with the given line items."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT OR REPLACE INTO quotes
               (quote_number, status, contact_email, line_items, sent_at,
                created_at, is_test)
               VALUES (?, 'won', ?, ?, ?, datetime('now'), 0)""",
            (qn, contact_email, json.dumps(items), sent_at)
        )
        conn.commit()
    finally:
        conn.close()


def test_last_won_for_buyer_matches_by_part_number(app, temp_data_dir):
    from src.core.pricing_intel_panel import last_won_for_buyer
    from src.core.db import get_db

    _seed_won_quote(temp_data_dir,
                    qn="R26Q500",
                    contact_email="buyer@example.test",
                    items=[{
                        "description": "Widget Type A",
                        "part_number": "W-12345",
                        "pricing": {"unit_price": 19.99},
                    }],
                    sent_at="2026-04-15T12:00:00Z")

    with get_db() as conn:
        out = last_won_for_buyer(
            conn, "buyer@example.test",
            description="totally different description",
            part_number="W-12345",
        )
    assert out["price"] == 19.99
    assert out["quote_number"] == "R26Q500"
    assert out["won_at"] == "2026-04-15"


def test_last_won_for_buyer_matches_by_fuzzy_description(app, temp_data_dir):
    from src.core.pricing_intel_panel import last_won_for_buyer
    from src.core.db import get_db

    _seed_won_quote(temp_data_dir,
                    qn="R26Q501",
                    contact_email="buyer2@example.test",
                    items=[{
                        "description": "Stainless steel mop bucket 32oz",
                        "part_number": "",
                        "pricing": {"unit_price": 47.50},
                    }],
                    sent_at="2026-03-10T12:00:00Z")

    with get_db() as conn:
        out = last_won_for_buyer(
            conn, "buyer2@example.test",
            description="stainless steel mop with attached scrubber",
            part_number="",
        )
    # Fuzzy: first 3 ≥3-char words: "Stainless", "steel", "mop" — all 3 must hit
    assert out["price"] == 47.50
    assert out["quote_number"] == "R26Q501"


def test_last_won_for_buyer_no_match_returns_empty(app, temp_data_dir):
    from src.core.pricing_intel_panel import last_won_for_buyer
    from src.core.db import get_db

    with get_db() as conn:
        out = last_won_for_buyer(
            conn, "nobody@example.test",
            description="anything", part_number="QQ-9999",
        )
    assert out == {}


def test_last_won_for_buyer_empty_email_returns_empty(app, temp_data_dir):
    from src.core.pricing_intel_panel import last_won_for_buyer
    from src.core.db import get_db

    with get_db() as conn:
        assert last_won_for_buyer(conn, "", "x", "y") == {}
        assert last_won_for_buyer(conn, None, "x", "y") == {}


def test_last_won_excludes_self_quote_number(app, temp_data_dir):
    """Calling from within an RFQ that has its own quote_number must not
    self-match (would falsely "anchor" the operator to their own bid)."""
    from src.core.pricing_intel_panel import last_won_for_buyer
    from src.core.db import get_db

    _seed_won_quote(temp_data_dir,
                    qn="R26Q502",
                    contact_email="buyer3@example.test",
                    items=[{"description": "Self-match item",
                            "part_number": "PN-SELF",
                            "pricing": {"unit_price": 100.00}}],
                    sent_at="2026-02-01T00:00:00Z")

    with get_db() as conn:
        # Without exclusion: matches.
        assert last_won_for_buyer(conn, "buyer3@example.test",
                                   "x", "PN-SELF")["price"] == 100.00
        # With exclusion: empty.
        out = last_won_for_buyer(conn, "buyer3@example.test",
                                  "x", "PN-SELF",
                                  exclude_quote_number="R26Q502")
        assert out == {}


# ─────────────────────────────────────────────────────────────────────────────
# Helper: scprs_ceiling_for_item
# ─────────────────────────────────────────────────────────────────────────────


def _seed_scprs_po_master(temp_data_dir, po_id, po_number):
    """scprs_po_lines has a FK to scprs_po_master; seed a parent row."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    try:
        # Permissive insert — column set varies across migrations; only put
        # the truly required columns.
        conn.execute(
            "INSERT OR REPLACE INTO scprs_po_master (id, po_number, is_test) "
            "VALUES (?, ?, 0)",
            (po_id, po_number)
        )
        conn.commit()
    finally:
        conn.close()


def _seed_scprs_po_line(temp_data_dir, *, po_id, line_num, description,
                        unit_price, quantity=1, item_id=""):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT OR REPLACE INTO scprs_po_lines
               (po_id, line_num, description, unit_price, quantity, item_id, is_test)
               VALUES (?, ?, ?, ?, ?, ?, 0)""",
            (po_id, line_num, description, unit_price, quantity, item_id)
        )
        conn.commit()
    finally:
        conn.close()


def test_scprs_ceiling_finds_match_by_description(app, temp_data_dir):
    from src.core.pricing_intel_panel import scprs_ceiling_for_item
    from src.core.db import get_db

    _seed_scprs_po_master(temp_data_dir, po_id=1001, po_number="PO-1001")
    # Three lines, prices 10, 12, 14 — median = 12
    for i, p in enumerate([10.0, 12.0, 14.0], start=1):
        _seed_scprs_po_line(temp_data_dir, po_id=1001, line_num=i,
                            description="Stainless steel mop bucket",
                            unit_price=p)

    with get_db() as conn:
        out = scprs_ceiling_for_item(conn,
                                      "stainless steel mop with handle",
                                      part_number="")
    assert out["sample_count"] == 3
    assert out["ceiling"] == 12.0
    assert out["low"] == 10.0
    assert out["high"] == 14.0


def test_scprs_ceiling_part_match_then_description_fallback(
        app, temp_data_dir):
    """When a part_number is provided but matches nothing in SCPRS
    (item_id sparse on legacy POs), fall through to description match
    instead of silently returning empty."""
    from src.core.pricing_intel_panel import scprs_ceiling_for_item
    from src.core.db import get_db

    _seed_scprs_po_master(temp_data_dir, po_id=1099, po_number="PO-1099")
    # SCPRS rows have NO item_id but matching description
    for i, p in enumerate([15.0, 17.0, 19.0], start=1):
        _seed_scprs_po_line(temp_data_dir, po_id=1099, line_num=i,
                            description="Disposable nitrile glove case",
                            unit_price=p, item_id="")

    with get_db() as conn:
        out = scprs_ceiling_for_item(
            conn,
            description="Disposable nitrile glove medium 100ct",
            part_number="GLV-NIT-M",   # non-empty — but item_id = "" in seed
        )
    # Part-number match would return zero rows; description fallback hits.
    assert out["sample_count"] == 3
    assert out["ceiling"] == 17.0


def test_scprs_ceiling_no_match_returns_empty(app, temp_data_dir):
    from src.core.pricing_intel_panel import scprs_ceiling_for_item
    from src.core.db import get_db

    with get_db() as conn:
        assert scprs_ceiling_for_item(conn, "nothing matches this", "") == {}


def test_scprs_ceiling_empty_inputs_return_empty(app, temp_data_dir):
    from src.core.pricing_intel_panel import scprs_ceiling_for_item
    from src.core.db import get_db

    with get_db() as conn:
        assert scprs_ceiling_for_item(conn, "", "") == {}


def test_scprs_per_unit_normalizes_line_total(app, temp_data_dir):
    """Verify the per-unit guard works on the helper."""
    from src.core.pricing_intel_panel import _scprs_per_unit
    # qty=5, unit_price=$100 (line total) → $20/each
    assert _scprs_per_unit(100.0, 5) == 20.0
    # qty=1: pass through
    assert _scprs_per_unit(50.0, 1) == 50.0
    # qty=3, p=$5 (genuinely per-unit): pass through (p < q*2)
    assert _scprs_per_unit(5.0, 3) == 5.0


# ─────────────────────────────────────────────────────────────────────────────
# compute_panel
# ─────────────────────────────────────────────────────────────────────────────


def test_compute_panel_returns_by_line_keyed_dict(app, temp_data_dir):
    from src.core.pricing_intel_panel import compute_panel

    _seed_won_quote(temp_data_dir, qn="R26Q510",
                    contact_email="cp@example.test",
                    items=[{"description": "Compose panel item",
                            "part_number": "CP-001",
                            "pricing": {"unit_price": 75.0}}],
                    sent_at="2026-01-01T00:00:00Z")

    items = [
        {"description": "Compose panel item", "part_number": "CP-001"},
        {"description": "No history item", "part_number": ""},
        {"description": "", "part_number": ""},  # empty → empty result
    ]
    out = compute_panel(items, contact_email="cp@example.test")

    assert "by_line" in out
    assert 1 in out["by_line"]
    assert out["by_line"][1]["last_won"]["price"] == 75.0
    # Item 2: no historical match
    assert out["by_line"][2] == {"last_won": {}, "scprs_ceiling": {}}
    # Item 3: empty desc + empty part — empty result, not an exception
    assert out["by_line"][3] == {}


def test_compute_panel_empty_items_returns_empty(app, temp_data_dir):
    from src.core.pricing_intel_panel import compute_panel
    assert compute_panel([]) == {"by_line": {}}
    assert compute_panel(None) == {"by_line": {}}


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint: /api/rfq/<rid>/pricing-intel
# ─────────────────────────────────────────────────────────────────────────────


def _seed_rfq_via_save(rfq_id, **fields):
    """Persist an RFQ via the canonical writer so all column hooks fire."""
    from src.api.data_layer import _save_single_rfq
    rfq = {
        "id": rfq_id,
        "received_at": "2026-05-07T10:00:00Z",
        "rfq_number": fields.pop("rfq_number", "R26Q900"),
        "status": "new",
        "items": [],
    }
    rfq.update(fields)
    _save_single_rfq(rfq_id, rfq, raise_on_error=True)


def test_pricing_intel_endpoint_requires_auth(anon_client):
    resp = anon_client.get("/api/rfq/abc/pricing-intel")
    # Either 401 or 302 to login is acceptable — anything but 200
    assert resp.status_code != 200


def test_pricing_intel_endpoint_404_when_rfq_missing(client):
    resp = client.get("/api/rfq/nonexistent_rid_zzz/pricing-intel")
    assert resp.status_code == 404
    body = resp.get_json()
    assert body["ok"] is False


def test_pricing_intel_endpoint_returns_intel_for_seeded_rfq(
        client, temp_data_dir):
    """End-to-end: seeded won quote + RFQ with matching item →
    endpoint returns the buyer's last-won price keyed by line number."""
    # First, seed the won quote.
    _seed_won_quote(temp_data_dir, qn="R26Q600",
                    contact_email="endto@example.test",
                    items=[{"description": "Endpoint test widget",
                            "part_number": "ET-1",
                            "pricing": {"unit_price": 33.33}}],
                    sent_at="2026-04-01T10:00:00Z")
    # Then seed the RFQ that should resolve.
    _seed_rfq_via_save(
        "rfq_pricing_intel_e2e",
        rfq_number="R26Q700",
        requestor_email="endto@example.test",
        items=[
            {"description": "Endpoint test widget",
             "part_number": "ET-1",
             "qty": 5, "uom": "EA"},
        ],
    )
    # Hit endpoint.
    resp = client.get("/api/rfq/rfq_pricing_intel_e2e/pricing-intel")
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json()
    assert body["ok"] is True
    assert "1" in body["by_line"]
    last_won = body["by_line"]["1"]["last_won"]
    assert last_won["price"] == 33.33
    assert last_won["quote_number"] == "R26Q600"


def test_pricing_intel_endpoint_empty_for_rfq_with_no_items(
        client, temp_data_dir):
    _seed_rfq_via_save("rfq_pricing_intel_empty",
                       rfq_number="R26Q701",
                       items=[])
    resp = client.get("/api/rfq/rfq_pricing_intel_empty/pricing-intel")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["by_line"] == {}


# ─────────────────────────────────────────────────────────────────────────────
# Template render-safety
# ─────────────────────────────────────────────────────────────────────────────


def test_rfq_detail_template_includes_pricing_intel_chip_slots(
        client, temp_data_dir):
    """Render rfq_detail.html and confirm the chip slots + JS are present."""
    _seed_rfq_via_save(
        "rfq_render_chip_test",
        rfq_number="R26Q800",
        items=[{"description": "Render test", "part_number": "RT-1",
                "qty": 1, "uom": "EA", "supplier_cost": 10.0,
                "markup_pct": 30, "price_per_unit": 13.0}],
    )
    resp = client.get("/rfq/rfq_render_chip_test")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    # Chip slot present
    assert 'class="pricing-intel-chip"' in html
    assert 'data-testid="pricing-intel-line-1"' in html
    # JS handler present
    assert "rfqPricingIntel" in html
    assert "/api/rfq/" in html
    # The route under /api/rfq/<RID>/pricing-intel — RID is JSON-injected
    assert "/pricing-intel" in html
