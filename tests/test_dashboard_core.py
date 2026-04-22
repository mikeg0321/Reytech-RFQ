"""
tests/test_dashboard_core.py — Core infrastructure tests for dashboard.py

Covers: route module loading, data layer & caching, notification system,
date/path utilities, and key API endpoints.
"""

import json
import os
import pytest


# ═══════════════════════════════════════════════════════════════════════════════
# A. Route Module Loading (5 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestRouteModuleLoading:

    def test_all_route_modules_load(self, app):
        """App fixture loaded without error and has routes registered."""
        assert app is not None
        rules = list(app.url_map.iter_rules())
        assert len(rules) > 0, "No routes registered on the app"

    def test_route_count_sanity(self, app):
        """Verify a significant number of routes are registered (> 100)."""
        rules = list(app.url_map.iter_rules())
        assert len(rules) > 100, (
            f"Expected > 100 routes, got {len(rules)}. "
            "Route modules may not be loading correctly."
        )

    def test_no_duplicate_endpoints(self, app):
        """No two routes should share the same endpoint name."""
        endpoints = [rule.endpoint for rule in app.url_map.iter_rules()]
        seen = {}
        duplicates = []
        for ep in endpoints:
            if ep in seen:
                duplicates.append(ep)
            seen[ep] = True
        assert len(duplicates) == 0, (
            f"Duplicate endpoint names found: {duplicates}"
        )

    def test_health_endpoint_works(self, client):
        """GET /ping returns 200 with 'pong' response."""
        resp = client.get("/ping")
        assert resp.status_code == 200

    def test_version_endpoint_returns_commit(self, anon_client, monkeypatch):
        """GET /version returns the Railway commit SHA as JSON, no auth required."""
        monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "abc123def4567890")
        resp = anon_client.get("/version")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["commit"] == "abc123def4567890"
        assert body["short"] == "abc123d"

    def test_version_endpoint_fallback_when_env_missing(self, anon_client, monkeypatch):
        """GET /version reports 'unknown' when neither env var is set — caller's poll
        script will then time out cleanly instead of matching a falsy value."""
        monkeypatch.delenv("RAILWAY_GIT_COMMIT_SHA", raising=False)
        monkeypatch.delenv("GIT_COMMIT_SHA", raising=False)
        resp = anon_client.get("/version")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["commit"] == "unknown"

    def test_auth_required_on_protected(self, anon_client):
        """GET /analytics without auth returns 401."""
        resp = anon_client.get("/analytics")
        assert resp.status_code == 401


# ═══════════════════════════════════════════════════════════════════════════════
# B. Data Layer & Caching (8 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestDataLayerCaching:

    def test_load_price_checks_empty(self, app):
        """Loading PCs from an empty test DB returns empty dict."""
        with app.app_context():
            from src.api.data_layer import _load_price_checks
            result = _load_price_checks()
            assert isinstance(result, dict)
            assert len(result) == 0

    def test_save_and_load_pc(self, app):
        """Save a PC via _save_single_pc(), then load it back."""
        with app.app_context():
            from src.api.data_layer import _save_single_pc, _load_price_checks
            pc_data = {
                "id": "test-pc-save-001",
                "pc_number": "OS - Test - Save",
                "institution": "CSP-Sacramento",
                "status": "parsed",
                "items": [
                    {"item_number": "1", "qty": 10, "uom": "EA",
                     "description": "Test widget"},
                ],
                "created_at": "2026-01-01T00:00:00",
            }
            _save_single_pc("test-pc-save-001", pc_data)
            loaded = _load_price_checks()
            assert "test-pc-save-001" in loaded
            pc = loaded["test-pc-save-001"]
            assert pc.get("pc_number") == "OS - Test - Save"

    def test_load_rfqs_empty(self, app):
        """Loading RFQs from an empty test DB returns empty dict."""
        with app.app_context():
            from src.api.data_layer import load_rfqs
            result = load_rfqs()
            assert isinstance(result, dict)
            assert len(result) == 0

    def test_cached_json_load_returns_data(self, app, temp_data_dir):
        """_cached_json_load reads a JSON file correctly."""
        with app.app_context():
            from src.api.data_layer import _cached_json_load
            test_file = os.path.join(temp_data_dir, "test_cache.json")
            test_data = {"key": "value", "count": 42}
            with open(test_file, "w") as f:
                json.dump(test_data, f)
            result = _cached_json_load(test_file)
            assert result == test_data

    def test_cached_json_load_missing_file(self, app, temp_data_dir):
        """_cached_json_load returns fallback on missing file."""
        with app.app_context():
            from src.api.data_layer import _cached_json_load
            missing = os.path.join(temp_data_dir, "nonexistent.json")
            result = _cached_json_load(missing, fallback={"default": True})
            assert result == {"default": True}

    def test_safe_path_blocks_traversal(self, app, temp_data_dir):
        """_safe_path neutralizes path traversal by extracting basename only."""
        with app.app_context():
            from src.api.data_layer import _safe_path
            # _safe_path uses os.path.basename, so ../../etc/passwd becomes just "passwd"
            # which resolves safely inside the base dir (no traversal)
            result = _safe_path("../../etc/passwd", temp_data_dir)
            base = os.path.realpath(temp_data_dir)
            assert os.path.realpath(result).startswith(base)
            # The filename should be just "passwd", not a path to /etc/passwd
            assert os.path.basename(result) == "passwd"

    def test_safe_path_allows_valid(self, app, temp_data_dir):
        """_safe_path allows valid filenames within the base directory."""
        with app.app_context():
            from src.api.data_layer import _safe_path
            result = _safe_path("somefile.json", temp_data_dir)
            assert result is not None
            assert temp_data_dir.replace("\\", "/") in result.replace("\\", "/") or \
                   os.path.realpath(temp_data_dir) in os.path.realpath(result)

    def test_sanitize_input_strips_html(self, app):
        """_sanitize_input strips HTML tags from input."""
        with app.app_context():
            from src.api.data_layer import _sanitize_input
            result = _sanitize_input("<script>alert(1)</script>")
            assert "<script>" not in result
            assert "alert(1)" in result


# ═══════════════════════════════════════════════════════════════════════════════
# C. Notification System (3 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestNotificationSystem:

    def test_push_notification_adds(self, app):
        """Calling _push_notification adds to the notifications deque."""
        with app.app_context():
            from src.api.data_layer import _push_notification, _notifications
            _notifications.clear()
            _push_notification({"type": "test", "title": "Test notification"})
            assert len(_notifications) == 1
            assert _notifications[0]["title"] == "Test notification"
            assert "ts" in _notifications[0]
            assert _notifications[0]["read"] is False

    def test_push_notification_max_20(self, app):
        """Pushing 25 notifications only keeps 20 (deque maxlen)."""
        with app.app_context():
            from src.api.data_layer import _push_notification, _notifications
            _notifications.clear()
            for i in range(25):
                _push_notification({"type": "test", "title": f"Notif {i}"})
            assert len(_notifications) == 20
            # Most recent should be first (appendleft)
            assert _notifications[0]["title"] == "Notif 24"

    def test_notification_list_api(self, client):
        """GET /api/notifications returns JSON with notifications array."""
        resp = client.get("/api/notifications")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data is not None
        assert "notifications" in data
        assert isinstance(data["notifications"], list)


# ═══════════════════════════════════════════════════════════════════════════════
# D. Date/Path Utilities (4 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestDatePathUtilities:

    def test_pst_now_iso_format(self, app):
        """_pst_now_iso() returns an ISO 8601 string with T separator."""
        with app.app_context():
            from src.api.data_layer import _pst_now_iso
            result = _pst_now_iso()
            assert isinstance(result, str)
            assert "T" in result
            # Should contain timezone offset or timezone name
            assert "+" in result or "-" in result.split("T")[1]

    def test_safe_filename_sanitizes(self, app):
        """_safe_filename strips dangerous characters from filenames."""
        with app.app_context():
            from src.api.data_layer import _safe_filename
            result = _safe_filename("../../../etc/passwd")
            assert ".." not in result
            assert "/" not in result
            assert "\\" not in result

    def test_safe_filename_preserves_extension(self, app):
        """_safe_filename keeps the .pdf extension."""
        with app.app_context():
            from src.api.data_layer import _safe_filename
            result = _safe_filename("my document.pdf")
            assert result.endswith(".pdf")

    def test_validate_pdf_path_blocks_outside(self, app, temp_data_dir):
        """_validate_pdf_path rejects empty paths and resolves safely."""
        with app.app_context():
            from src.api.data_layer import _validate_pdf_path
            # Empty path should raise ValueError
            with pytest.raises(ValueError):
                _validate_pdf_path("")
            # Non-empty path gets resolved via _safe_path (basename extraction)
            result = _validate_pdf_path("test.pdf")
            assert result.endswith("test.pdf")


# ═══════════════════════════════════════════════════════════════════════════════
# E. API Endpoints (5 tests)
# ═══════════════════════════════════════════════════════════════════════════════

class TestAPIEndpoints:

    def test_api_health(self, client):
        """GET /api/health returns JSON with status ok."""
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data is not None
        assert data.get("status") == "ok"

    def test_api_status(self, client):
        """GET /api/status returns system info JSON."""
        resp = client.get("/api/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data is not None
        # Should contain poll status and rfq count
        assert "poll" in data or "rfqs" in data

    def test_search_page_loads(self, client):
        """GET /search?q=test returns 200."""
        resp = client.get("/search?q=test")
        assert resp.status_code == 200

    def test_quotes_page_loads(self, client):
        """GET /quotes returns 200."""
        resp = client.get("/quotes")
        assert resp.status_code == 200

    def test_orders_page_loads(self, client):
        """GET /orders returns 200."""
        resp = client.get("/orders")
        assert resp.status_code == 200
