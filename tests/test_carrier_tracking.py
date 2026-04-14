"""Carrier tracking framework — Orders V2 phase 5.

Covers detect_carrier(), tracking_url(), carrier_and_url(),
check_tracking_status(), and the /api/order/<oid>/line/<lid>/tracking-status
+ /api/order/<oid>/tracking-candidates route endpoints.
"""
import sqlite3
import pytest


class TestDetectCarrier:
    @pytest.mark.parametrize("tn,expected", [
        ("1Z999AA10123456784", "UPS"),
        ("1ZE2V2030123456789", "UPS"),
        ("1z999aa10123456784", "UPS"),   # lowercase normalizes
        ("TBA000000000001", "Amazon"),
        ("TBA12345678901234567890", "Amazon"),
        ("9400111699000000000000", "USPS"),
        ("9300120111111111111111", "USPS"),
        ("LN123456789US", "USPS"),      # 13-char international
        ("123456789012", "FedEx"),       # 12-digit
        ("123456789012345", "FedEx"),   # 15-digit
        ("1234567890123456789012", "FedEx"),  # 22-digit ground
        ("C12345678901234", "OnTrac"),
        ("not-a-valid-number", "Unknown"),
        ("", "Unknown"),
    ])
    def test_carrier_detection(self, tn, expected):
        from src.core.carrier_tracking import detect_carrier
        assert detect_carrier(tn) == expected

    def test_none_input_is_safe(self):
        from src.core.carrier_tracking import detect_carrier
        assert detect_carrier(None) == "Unknown"

    def test_whitespace_and_dashes_stripped(self):
        from src.core.carrier_tracking import detect_carrier
        # UPS with spaces + dashes
        assert detect_carrier("1Z 999 AA1 0123 456 784") == "UPS"
        assert detect_carrier("1Z-999-AA10123456784") == "UPS"


class TestTrackingUrl:
    def test_ups_url(self):
        from src.core.carrier_tracking import tracking_url
        url = tracking_url("UPS", "1Z999AA10123456784")
        assert "ups.com" in url
        assert "1Z999AA10123456784" in url

    def test_fedex_url(self):
        from src.core.carrier_tracking import tracking_url
        url = tracking_url("FedEx", "123456789012")
        assert "fedex.com" in url
        assert "123456789012" in url

    def test_unknown_carrier_returns_empty(self):
        from src.core.carrier_tracking import tracking_url
        assert tracking_url("Mystery", "abc123") == ""

    def test_unknown_carrier_auto_detects(self):
        """Passing 'Unknown' (or empty) falls through to detect_carrier."""
        from src.core.carrier_tracking import tracking_url
        url = tracking_url("Unknown", "1Z999AA10123456784")
        assert "ups.com" in url

    def test_empty_tracking_returns_empty(self):
        from src.core.carrier_tracking import tracking_url
        assert tracking_url("UPS", "") == ""


class TestCarrierAndUrl:
    def test_returns_both(self):
        from src.core.carrier_tracking import carrier_and_url
        carrier, url = carrier_and_url("TBA123456789012345")
        assert carrier == "Amazon"
        assert "amazon.com" in url

    def test_known_carrier_takes_precedence(self):
        """When the caller passes a known carrier (e.g., from a
        vendor email header), don't auto-detect — use what they
        told us."""
        from src.core.carrier_tracking import carrier_and_url
        # This TN shape matches USPS (94... 22 digits), but if the
        # vendor email said "FedEx" we trust them.
        carrier, url = carrier_and_url("9400111699000000000000",
                                         known_carrier="FedEx")
        assert carrier == "FedEx"
        assert "fedex.com" in url


def _seed_order_line(order_id: str, line_number: int = 1,
                     tracking: str = "1Z999AA10123456784",
                     carrier: str = "",
                     status: str = "shipped",
                     delivery_date: str = ""):
    from src.core.db import DB_PATH
    conn = sqlite3.connect(DB_PATH, timeout=10)
    # Seed the order row first so FKs don't complain
    conn.execute(
        """INSERT OR IGNORE INTO orders
           (id, quote_number, agency, po_number, total, status, created_at)
           VALUES (?, 'R26Q1', 'cchcs', 'PO1', 100, 'new', datetime('now'))""",
        (order_id,),
    )
    cur = conn.execute(
        """INSERT INTO order_line_items
           (order_id, line_number, description, qty_ordered, tracking_number,
            carrier, sourcing_status, delivery_date, created_at)
           VALUES (?, ?, 'Test Item', 1, ?, ?, ?, ?, datetime('now'))""",
        (order_id, line_number, tracking, carrier, status, delivery_date),
    )
    lid = cur.lastrowid
    conn.commit()
    conn.close()
    return lid


class TestCheckTrackingStatus:
    def test_returns_manual_status_with_auto_carrier(self, temp_data_dir):
        from src.core.carrier_tracking import check_tracking_status
        lid = _seed_order_line("ord_ct1", tracking="1Z999AA10123456784")
        result = check_tracking_status("ord_ct1", lid)
        assert result["ok"] is True
        assert result["carrier"] == "UPS"
        assert "ups.com" in result["carrier_url"]
        assert result["tracking_number"] == "1Z999AA10123456784"
        assert result["status"] == "shipped"
        assert result["source"] == "manual"

    def test_needs_api_check_flag(self, temp_data_dir):
        """needs_api_check should be True for shipped items without
        a delivery_date, False for delivered items."""
        from src.core.carrier_tracking import check_tracking_status

        lid1 = _seed_order_line("ord_ct2", line_number=1,
                                 tracking="1Z999AA10123456784",
                                 status="shipped")
        lid2 = _seed_order_line("ord_ct2", line_number=2,
                                 tracking="1Z999BB10123456784",
                                 status="delivered",
                                 delivery_date="2026-04-10")
        r1 = check_tracking_status("ord_ct2", lid1)
        r2 = check_tracking_status("ord_ct2", lid2)
        assert r1["needs_api_check"] is True
        assert r2["needs_api_check"] is False

    def test_missing_line_returns_not_found(self, temp_data_dir):
        from src.core.carrier_tracking import check_tracking_status
        result = check_tracking_status("ord_nope", 999)
        assert result["ok"] is False

    def test_no_tracking_number_no_url(self, temp_data_dir):
        from src.core.carrier_tracking import check_tracking_status
        lid = _seed_order_line("ord_ct3", tracking="")
        result = check_tracking_status("ord_ct3", lid)
        assert result["ok"] is True
        assert result["carrier_url"] == ""
        assert result["needs_api_check"] is False


class TestTrackingStatusRoute:
    def test_endpoint_returns_status(self, auth_client, temp_data_dir):
        lid = _seed_order_line("ord_rt1", tracking="1Z999AA10123456784")
        r = auth_client.get(f"/api/order/ord_rt1/line/{lid}/tracking-status")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["carrier"] == "UPS"

    def test_endpoint_404_on_missing_line(self, auth_client, temp_data_dir):
        r = auth_client.get("/api/order/ord_missing/line/999/tracking-status")
        assert r.status_code == 404
        d = r.get_json()
        assert d["ok"] is False


class TestTrackingCandidates:
    def test_returns_only_undelivered_with_tracking(self, auth_client, temp_data_dir):
        # One candidate (shipped, no delivery_date)
        _seed_order_line("ord_cand", line_number=1,
                         tracking="1Z999AA10123456784", status="shipped")
        # Not a candidate (delivered)
        _seed_order_line("ord_cand", line_number=2,
                         tracking="1Z999BB10123456784", status="delivered",
                         delivery_date="2026-04-10")
        # Not a candidate (no tracking)
        _seed_order_line("ord_cand", line_number=3,
                         tracking="", status="pending")

        r = auth_client.get("/api/order/ord_cand/tracking-candidates")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert d["count"] == 1
        assert d["candidates"][0]["carrier"] == "UPS"
        assert d["candidates"][0]["line_number"] == 1

    def test_empty_when_nothing_to_track(self, auth_client, temp_data_dir):
        r = auth_client.get("/api/order/nothing/tracking-candidates")
        d = r.get_json()
        assert d["ok"] is True
        assert d["count"] == 0
