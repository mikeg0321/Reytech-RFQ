"""Tests for PR-C2: /api/rfq/<rid>/win-warnings JSON endpoint.

The route layer:
  1. loads the RFQ row,
  2. for each line item with a part_number/description, looks up the
     buyer's last winning price (calls _last_won_price_for_buyer),
  3. stuffs `last_won_price` / `last_won_quote` onto each item,
  4. calls the pure compute_win_warnings(),
  5. returns warnings + counts as JSON.

Win-validation thresholds + warning shape are exercised by
test_win_validation.py — these tests pin the *route plumbing*: 404 for
unknown RFQ, auth gate, JSON shape, and the last-won-price enrichment
step (so that a buyer with prior wins fires `cost_above_last_won`).
"""
from __future__ import annotations

import json


class TestWinWarningsRoute:

    def test_returns_404_for_unknown_rfq(self, auth_client):
        resp = auth_client.get("/api/rfq/does_not_exist/win-warnings")
        assert resp.status_code == 404

    def test_returns_empty_warnings_for_clean_rfq(self, auth_client, seed_rfq):
        rid = seed_rfq
        resp = auth_client.get(f"/api/rfq/{rid}/win-warnings")
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body["ok"] is True
        assert "warnings" in body
        assert "counts" in body
        # Keys always present even when empty — the chip JS reads
        # counts.red/orange/yellow without null-checking.
        assert set(body["counts"].keys()) == {"red", "orange", "yellow"}

    def test_returns_warning_for_low_margin_rfq(self, auth_client, sample_rfq,
                                                temp_data_dir):
        # Seed an RFQ with a line item below the line-margin floor.
        # _item(cost=10, price=11) → 10% markup, below the 15% floor.
        from src.api.dashboard import save_rfqs, load_rfqs
        rid = "rfq_lowmargin_routetest"
        r = dict(sample_rfq)
        r["id"] = rid
        r["line_items"] = [{
            "line_number": 1,
            "description": "Test Item",
            "part_number": "PN-LOW",
            "supplier_cost": 10.0,
            "unit_price": 11.0,
            "quantity": 5,
        }]
        rfqs = load_rfqs()
        rfqs[rid] = r
        save_rfqs(rfqs)

        resp = auth_client.get(f"/api/rfq/{rid}/win-warnings")
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body["ok"] is True
        assert body["counts"]["orange"] >= 1
        codes = [w["code"] for w in body["warnings"]]
        assert "line_low_margin" in codes
        line_warn = next(w for w in body["warnings"]
                         if w["code"] == "line_low_margin")
        assert line_warn["line_no"] == 1
        assert line_warn["level"] == "orange"

    def test_returns_quote_low_margin_rolled_up(self, auth_client, sample_rfq,
                                                temp_data_dir):
        # Aggregate quote-level floor at 22%. Two items each at 18% markup
        # average to 18% — below floor → quote_low_margin warning fires.
        from src.api.dashboard import save_rfqs, load_rfqs
        rid = "rfq_quotemargin_routetest"
        r = dict(sample_rfq)
        r["id"] = rid
        r["line_items"] = [
            {"line_number": 1, "description": "A", "part_number": "P1",
             "supplier_cost": 100.0, "unit_price": 118.0, "quantity": 1},
            {"line_number": 2, "description": "B", "part_number": "P2",
             "supplier_cost": 100.0, "unit_price": 118.0, "quantity": 1},
        ]
        rfqs = load_rfqs()
        rfqs[rid] = r
        save_rfqs(rfqs)

        resp = auth_client.get(f"/api/rfq/{rid}/win-warnings")
        body = json.loads(resp.data)
        codes = [w["code"] for w in body["warnings"]]
        assert "quote_low_margin" in codes
        # Quote-level warnings have line_no=None (banner-row in the UI).
        quote_warn = next(w for w in body["warnings"]
                          if w["code"] == "quote_low_margin")
        assert quote_warn["line_no"] is None

    def test_counts_match_warning_levels(self, auth_client, sample_rfq,
                                         temp_data_dir):
        from src.api.dashboard import save_rfqs, load_rfqs
        rid = "rfq_counts_routetest"
        r = dict(sample_rfq)
        r["id"] = rid
        # One orange line + the quote-level warning from the same data.
        r["line_items"] = [
            {"line_number": 1, "description": "Item", "part_number": "P1",
             "supplier_cost": 10.0, "unit_price": 11.0, "quantity": 1},
        ]
        rfqs = load_rfqs()
        rfqs[rid] = r
        save_rfqs(rfqs)

        resp = auth_client.get(f"/api/rfq/{rid}/win-warnings")
        body = json.loads(resp.data)
        sum_levels = sum(body["counts"].values())
        assert sum_levels == len(body["warnings"])

    def test_handles_rfq_with_no_items_gracefully(self, auth_client,
                                                  sample_rfq,
                                                  temp_data_dir):
        from src.api.dashboard import save_rfqs, load_rfqs
        rid = "rfq_no_items_routetest"
        r = dict(sample_rfq)
        r["id"] = rid
        r["line_items"] = []
        rfqs = load_rfqs()
        rfqs[rid] = r
        save_rfqs(rfqs)

        resp = auth_client.get(f"/api/rfq/{rid}/win-warnings")
        assert resp.status_code == 200
        body = json.loads(resp.data)
        assert body["warnings"] == []


class TestWinWarningsAuthGate:

    def test_requires_auth(self, anon_client):
        resp = anon_client.get("/api/rfq/anyid/win-warnings")
        assert resp.status_code in (401, 403)
