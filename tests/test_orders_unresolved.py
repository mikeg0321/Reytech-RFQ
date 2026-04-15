"""Tests for the /orders/unresolved queue and POST retry-match endpoint.

Covers the audit complaint that 4 unresolved $0.00 award POs were silently
sitting in the system with no UI surface to retry them.

Linkage model: order_dal persists `quote_number`, not `rfq_id` — so an
order is "unresolved" iff its quote_number is missing or doesn't appear
in any known RFQ record from rfqs.json.
"""

import json
import os


def _seed_orders(orders):
    from src.core.order_dal import save_order
    for oid, o in orders.items():
        save_order(oid, o, actor="test")


def _write_rfqs(temp_data_dir, rfqs):
    with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
        json.dump(rfqs, f)


class TestUnresolvedQueuePage:
    def test_loads_when_empty(self, client, temp_data_dir):
        _write_rfqs(temp_data_dir, {})
        r = client.get("/orders/unresolved")
        assert r.status_code == 200
        assert b"All POs matched" in r.data or b"Unresolved POs" in r.data

    def test_lists_unmatched_order(self, client, temp_data_dir):
        _seed_orders({
            "ORD-X1": {
                "order_id": "ORD-X1",
                "po_number": "PO-12345",
                "total": 0.0,
                "agency": "CDCR",
                # no quote_number → unresolved
            }
        })
        _write_rfqs(temp_data_dir, {})
        r = client.get("/orders/unresolved")
        assert r.status_code == 200
        html = r.data.decode()
        assert "ORD-X1" in html
        assert "PO-12345" in html
        assert "Retry match" in html

    def test_hides_matched_order(self, client, temp_data_dir):
        _seed_orders({
            "ORD-OK": {
                "order_id": "ORD-OK",
                "po_number": "PO-555",
                "quote_number": "R26Q500",
                "total": 1234.0,
            }
        })
        _write_rfqs(temp_data_dir, {
            "rfq_abc": {"solicitation_number": "SOL-1", "quote_number": "R26Q500"}
        })
        r = client.get("/orders/unresolved")
        assert r.status_code == 200
        assert b"ORD-OK" not in r.data


class TestRetryMatchAPI:
    def test_match_by_po_to_solicitation(self, client, temp_data_dir):
        _seed_orders({
            "ORD-Y1": {
                "order_id": "ORD-Y1",
                "po_number": "SOL-9999",
                "total": 500.0,
            }
        })
        _write_rfqs(temp_data_dir, {
            "rfq_zzz": {"solicitation_number": "SOL-9999",
                        "quote_number": "R26Q777", "total_price": 500.0}
        })
        r = client.post("/api/orders/ORD-Y1/retry-match")
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["matched"] is True
        assert body["rfq_id"] == "rfq_zzz"
        assert body["quote_number"] == "R26Q777"

    def test_no_match_returns_ok_false_match(self, client, temp_data_dir):
        _seed_orders({
            "ORD-Y2": {"order_id": "ORD-Y2", "po_number": "PO-NOMATCH", "total": 0.0}
        })
        _write_rfqs(temp_data_dir, {})
        r = client.post("/api/orders/ORD-Y2/retry-match")
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["matched"] is False

    def test_unknown_order_returns_404(self, client, temp_data_dir):
        _write_rfqs(temp_data_dir, {})
        r = client.post("/api/orders/ORD-DOES-NOT-EXIST/retry-match")
        assert r.status_code == 404

    def test_already_matched_is_idempotent(self, client, temp_data_dir):
        _seed_orders({
            "ORD-Y3": {"order_id": "ORD-Y3",
                       "quote_number": "R26Q888", "total": 100.0}
        })
        _write_rfqs(temp_data_dir, {
            "rfq_existing": {"quote_number": "R26Q888"}
        })
        r = client.post("/api/orders/ORD-Y3/retry-match")
        assert r.status_code == 200
        body = r.get_json()
        assert body["ok"] is True
        assert body["matched"] is True
        assert body.get("already") is True
