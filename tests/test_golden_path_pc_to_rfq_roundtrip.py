"""Golden Path — PC → RFQ deepcopy round-trip (Batch G1).

Locks down the contract that the user-facing
``POST /api/pc/<pcid>/convert-to-rfq`` endpoint preserves a Price
Check's pricing payload verbatim into the new RFQ.

The 2026-03-31 incident root cause was field-by-field remapping that
silently zeroed bid prices, blanked MFG numbers, and broke the
PC↔RFQ link. This file pins the deepcopy contract end-to-end (real
HTTP request, real JSON store, real load_rfqs) so that any future
regression — whether someone swaps the `_copy.deepcopy(pc)` line
for a manual dict comprehension, or someone adds a side-effect that
mutates the source PC after conversion — fails this suite.
"""
import copy
import json
import os
from datetime import datetime

import pytest


def _golden_pc():
    """A realistic PC payload exercising every field the operator
    actually depends on after conversion. Values chosen so that any
    field-zeroing or wrong-key bug shows up with a clear assertion."""
    return {
        "id": "pc_golden_001",
        "pc_number": "PC-GOLD-001",
        "status": "priced",
        "agency": "CCHCS",
        "institution": "CCHCS",
        "requestor": "Jane Buyer",
        "requestor_email": "jane@cdcr.ca.gov",
        "ship_to": "1600 9th St, Sacramento CA 95814",
        "reytech_quote_number": "Q-2026-GOLD-001",
        "due_date": "2026-04-30",
        "email_uid": "msg_golden_001",
        "items": [
            {
                "description": "Stryker bed mattress, hospital grade — 80x36",
                "part_number": "6500-001-430",
                "mfg_number": "6500-001-430",
                "qty": 2,
                "uom": "EA",
                "unit_price": 454.40,
                "supplier_cost": 320.00,
                "pricing": {
                    "amazon_asin": "B07GXXXXX1",
                    "recommended_price": 454.40,
                    "supplier": "Stryker via DGS",
                    "source": "scprs_won_quote",
                    "scprs_unit_price": 480.00,
                    "discount_pct": 5.3,
                },
            },
            {
                "description": "Elastic bandage, 4-inch x 5-yard, latex-free",
                "part_number": "EB-4-5",
                "mfg_number": "EB-4-5",
                "qty": 24,
                "uom": "BX",
                "unit_price": 9.85,
                "supplier_cost": 6.20,
                "pricing": {
                    "amazon_asin": "B08YYYYYY2",
                    "recommended_price": 9.85,
                    "supplier": "S&S Worldwide",
                    "source": "catalog",
                },
            },
        ],
    }


@pytest.fixture
def seeded_pc(temp_data_dir):
    """Write the golden PC into the on-disk JSON store the route reads from."""
    pc = _golden_pc()
    pc_path = os.path.join(temp_data_dir, "price_checks.json")
    with open(pc_path, "w") as f:
        json.dump({pc["id"]: pc}, f)
    return pc


def _load_rfq_from_store(temp_data_dir, rfq_id):
    """Read the RFQ from the same store the route writes to (SQLite)."""
    from src.api.data_layer import load_rfqs
    return load_rfqs().get(rfq_id)


def _load_pc_from_store(temp_data_dir, pc_id):
    """Read the PC from the same store the route writes to (SQLite)."""
    from src.api.data_layer import _load_price_checks
    return _load_price_checks().get(pc_id)


class TestGoldenPathRoundTrip:

    def test_status_priced_is_carried_through(self, auth_client, temp_data_dir, seeded_pc):
        """A priced PC should land in the RFQ store with status=priced."""
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        assert resp.status_code == 200, resp.get_data(as_text=True)
        body = resp.get_json()
        assert body["ok"] is True
        rfq = _load_rfq_from_store(temp_data_dir, body["rfq_id"])
        assert rfq is not None, "RFQ was not persisted to rfqs.json"
        assert rfq["status"] == "priced"

    def test_item_count_and_descriptions_verbatim(
        self, auth_client, temp_data_dir, seeded_pc
    ):
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        rfq = _load_rfq_from_store(temp_data_dir, resp.get_json()["rfq_id"])

        # The deepcopy preserves the PC's `items` array unchanged, AND the
        # convert helper sets `line_items` from `items` for the RFQ schema.
        items = rfq.get("items") or rfq.get("line_items") or []
        assert len(items) == 2, f"expected 2 items, got {len(items)}"
        descs = [it["description"] for it in items]
        assert descs == [it["description"] for it in seeded_pc["items"]], (
            "descriptions must round-trip byte-for-byte (no rewriting allowed)"
        )

    def test_mfg_numbers_preserved(self, auth_client, temp_data_dir, seeded_pc):
        """The 2026-03-31 incident zeroed MFG#. Pin them down here."""
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        rfq = _load_rfq_from_store(temp_data_dir, resp.get_json()["rfq_id"])
        items = rfq.get("items") or rfq.get("line_items") or []
        for original, converted in zip(seeded_pc["items"], items):
            assert converted.get("part_number") == original["part_number"]
            assert converted.get("mfg_number") == original["mfg_number"]

    def test_unit_prices_and_supplier_costs_preserved(
        self, auth_client, temp_data_dir, seeded_pc
    ):
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        rfq = _load_rfq_from_store(temp_data_dir, resp.get_json()["rfq_id"])
        items = rfq.get("items") or rfq.get("line_items") or []
        for original, converted in zip(seeded_pc["items"], items):
            assert converted["unit_price"] == original["unit_price"], (
                f"unit_price drifted for {original['description']!r}: "
                f"{converted['unit_price']} != {original['unit_price']}"
            )
            assert converted["supplier_cost"] == original["supplier_cost"]
            assert converted["qty"] == original["qty"]

    def test_pricing_metadata_preserved(self, auth_client, temp_data_dir, seeded_pc):
        """Amazon ASIN, recommended_price, supplier, source must survive."""
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        rfq = _load_rfq_from_store(temp_data_dir, resp.get_json()["rfq_id"])
        items = rfq.get("items") or rfq.get("line_items") or []
        for original, converted in zip(seeded_pc["items"], items):
            op = original["pricing"]
            cp = converted.get("pricing") or {}
            assert cp.get("amazon_asin") == op["amazon_asin"]
            assert cp.get("recommended_price") == op["recommended_price"]
            assert cp.get("supplier") == op["supplier"]
            assert cp.get("source") == op["source"]

    def test_buyer_and_quote_identity_round_trip(
        self, auth_client, temp_data_dir, seeded_pc
    ):
        """The RFQ must keep the operator's quote number + buyer identity."""
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        rfq = _load_rfq_from_store(temp_data_dir, resp.get_json()["rfq_id"])
        # Both the verbatim PC fields AND the RFQ-shaped aliases should be present
        assert rfq.get("reytech_quote_number") == "Q-2026-GOLD-001"
        assert rfq.get("requestor") == "Jane Buyer" or rfq.get("requestor_name") == "Jane Buyer"
        assert (rfq.get("requestor_email") or rfq.get("email")) == "jane@cdcr.ca.gov"
        assert rfq.get("delivery_location") == seeded_pc["ship_to"] or rfq.get("ship_to") == seeded_pc["ship_to"]

    def test_bidirectional_linking(self, auth_client, temp_data_dir, seeded_pc):
        """RFQ must point back to PC, and PC must point forward to RFQ."""
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        rfq_id = resp.get_json()["rfq_id"]
        rfq = _load_rfq_from_store(temp_data_dir, rfq_id)

        # RFQ → PC backlinks
        assert rfq.get("linked_pc_id") == seeded_pc["id"]
        assert rfq.get("source_pc") == seeded_pc["id"]
        assert rfq.get("source") == "pc_conversion"

        # PC → RFQ forward link
        pc = _load_pc_from_store(temp_data_dir, seeded_pc["id"])
        assert pc.get("linked_rfq_id") == rfq_id
        assert pc.get("converted_to_rfq") is True

    def test_rfq_is_independent_of_pc_after_conversion(
        self, auth_client, temp_data_dir, seeded_pc
    ):
        """A true deepcopy means: editing the RFQ on disk later doesn't
        retroactively mutate the source PC. This is the bug class that
        the original incident was — shared references between the two
        records caused PC pricing to silently change when the RFQ was
        edited."""
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        rfq_id = resp.get_json()["rfq_id"]

        # Re-save the RFQ with a mutated unit_price via the same DAL the route uses
        from src.api.data_layer import _save_single_rfq
        rfq = _load_rfq_from_store(temp_data_dir, rfq_id)
        items = rfq.get("items") or rfq.get("line_items") or []
        items[0]["unit_price"] = 999.99
        rfq["items"] = items
        _save_single_rfq(rfq_id, rfq)

        pc = _load_pc_from_store(temp_data_dir, seeded_pc["id"])
        assert pc["items"][0]["unit_price"] == 454.40, (
            "PC pricing must not mutate when RFQ is edited — independence broken"
        )

    def test_response_summary_matches_persisted_state(
        self, auth_client, temp_data_dir, seeded_pc
    ):
        """The endpoint's JSON response should agree with what landed on disk."""
        resp = auth_client.post(f"/api/pc/{seeded_pc['id']}/convert-to-rfq")
        body = resp.get_json()
        rfq = _load_rfq_from_store(temp_data_dir, body["rfq_id"])
        items = rfq.get("items") or rfq.get("line_items") or []
        assert body["items"] == len(items)
        assert "agency_key" in body
        assert "agency_name" in body

    def test_missing_pc_returns_404(self, auth_client, temp_data_dir):
        """Sanity: converting a non-existent PC must 404, not 500."""
        resp = auth_client.post("/api/pc/pc_does_not_exist/convert-to-rfq")
        assert resp.status_code == 404
        assert resp.get_json()["ok"] is False
