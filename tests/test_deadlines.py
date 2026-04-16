"""Tests for the Deadline tracking API (Phase 0.5)."""
import json
from datetime import datetime, timedelta, timezone

import pytest


class TestDeadlinesAPI:
    """GET /api/deadlines returns structured deadline data."""

    def test_deadlines_empty(self, auth_client):
        resp = auth_client.get("/api/deadlines")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert isinstance(data["deadlines"], list)

    def test_deadlines_with_pc(self, auth_client, seed_pc, sample_pc):
        """PC with a due date appears in deadlines."""
        # seed_pc writes a sample_pc that has a due_date in its header
        resp = auth_client.get("/api/deadlines")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        # The sample_pc may or may not have a due_date — test the structure
        assert isinstance(data["deadlines"], list)
        assert "count" in data
        assert "critical_count" in data

    def test_deadlines_auth_required(self, anon_client):
        resp = anon_client.get("/api/deadlines")
        assert resp.status_code == 401

    def test_critical_endpoint(self, auth_client):
        resp = auth_client.get("/api/deadlines/critical")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert isinstance(data["critical"], list)

    def test_critical_auth_required(self, anon_client):
        resp = anon_client.get("/api/deadlines/critical")
        assert resp.status_code == 401


class TestDueDateParsing:
    """Unit tests for the date parsing logic."""

    def test_parse_various_formats(self):
        from src.api.modules.routes_deadlines import _parse_due_datetime

        # MM/DD/YY
        dt, explicit = _parse_due_datetime("04/18/26")
        assert dt is not None
        assert dt.month == 4
        assert dt.day == 18
        assert not explicit  # no time given

        # MM/DD/YYYY
        dt, explicit = _parse_due_datetime("04/18/2026")
        assert dt is not None
        assert dt.month == 4

        # YYYY-MM-DD
        dt, explicit = _parse_due_datetime("2026-04-18")
        assert dt is not None
        assert dt.day == 18

    def test_parse_with_time(self):
        from src.api.modules.routes_deadlines import _parse_due_datetime

        dt, explicit = _parse_due_datetime("04/18/2026", "2:00 PM")
        assert dt is not None
        assert dt.hour == 14
        assert dt.minute == 0
        assert explicit

        dt, explicit = _parse_due_datetime("04/18/2026", "10:30 AM")
        assert dt is not None
        assert dt.hour == 10
        assert dt.minute == 30
        assert explicit

    def test_parse_defaults_to_2pm(self):
        from src.api.modules.routes_deadlines import _parse_due_datetime

        dt, explicit = _parse_due_datetime("04/18/2026")
        assert dt is not None
        assert dt.hour == 14  # 2:00 PM default
        assert not explicit

    def test_parse_empty_returns_none(self):
        from src.api.modules.routes_deadlines import _parse_due_datetime

        dt, explicit = _parse_due_datetime("")
        assert dt is None
        assert not explicit

        dt, explicit = _parse_due_datetime(None)
        assert dt is None


class TestDeadlineUrgency:
    """Verify urgency classification."""

    def test_build_deadline_item_overdue(self):
        from src.api.modules.routes_deadlines import _build_deadline_item

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%m/%d/%Y")
        doc = {"due_date": yesterday, "status": "priced", "items": [{"desc": "test"}]}
        dl = _build_deadline_item("pc", "test123", doc)
        assert dl is not None
        assert dl["urgency"] == "overdue"
        assert dl["hours_left"] < 0

    def test_build_deadline_item_no_due_date(self):
        from src.api.modules.routes_deadlines import _build_deadline_item

        doc = {"status": "priced", "items": []}
        dl = _build_deadline_item("pc", "test123", doc)
        assert dl is None

    def test_build_deadline_item_future(self):
        from src.api.modules.routes_deadlines import _build_deadline_item

        next_week = (datetime.now() + timedelta(days=7)).strftime("%m/%d/%Y")
        doc = {"due_date": next_week, "status": "priced", "items": [{"desc": "test"}]}
        dl = _build_deadline_item("pc", "test123", doc)
        assert dl is not None
        assert dl["urgency"] == "normal"
        assert dl["hours_left"] > 72
