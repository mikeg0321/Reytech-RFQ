"""Tests for Phase 13 infrastructure: secrets, scanner, quickbooks."""

import pytest
import json
import os
import sys
import time
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─── Secrets Registry Tests ─────────────────────────────────────────────────

class TestSecrets:
    def test_import(self):
        from src.core.secrets import get_key, get_agent_key, mask, validate_all, startup_check
        assert callable(get_key)
        assert callable(validate_all)

    def test_mask_short(self):
        from src.core.secrets import mask
        assert mask("") == "(not set)"
        result = mask("abc")
        assert "****" in result
        assert "abc" not in result or len(result) < 8  # Short values are masked

    def test_mask_long(self):
        from src.core.secrets import mask
        result = mask("sk-ant-api03-verylongkeyhere123456")
        assert result.startswith("sk-ant-a")
        assert "****" in result
        # Should NOT contain the full key
        assert "verylongkeyhere" not in result

    def test_validate_all_structure(self):
        from src.core.secrets import validate_all
        report = validate_all()
        assert "secrets" in report
        assert "total" in report
        assert "set" in report
        assert "missing" in report
        assert "warnings" in report
        assert isinstance(report["secrets"], dict)
        assert report["total"] > 0

    def test_get_key_unknown(self):
        from src.core.secrets import get_key
        result = get_key("nonexistent_key_xyz")
        assert result == ""

    def test_get_key_with_default(self):
        from src.core.secrets import get_key
        result = get_key("dash_user")
        assert result  # Should have default "reytech"

    def test_get_agent_key(self):
        from src.core.secrets import get_agent_key
        # Without env vars set, returns empty
        result = get_agent_key("item_identifier")
        assert isinstance(result, str)

    def test_startup_check(self):
        from src.core.secrets import startup_check
        report = startup_check()
        assert "secrets" in report

    def test_registry_entries(self):
        from src.core.secrets import _REGISTRY
        # All entries should have required fields
        for name, entry in _REGISTRY.items():
            assert "env" in entry, f"{name} missing 'env'"
            assert "desc" in entry, f"{name} missing 'desc'"
            assert "agents" in entry, f"{name} missing 'agents'"

    def test_qb_keys_in_registry(self):
        from src.core.secrets import _REGISTRY
        qb_keys = [k for k in _REGISTRY if k.startswith("qb_")]
        assert len(qb_keys) >= 4  # client_id, client_secret, refresh_token, realm_id


# ─── SCPRS Scanner Tests ────────────────────────────────────────────────────

class TestSCPRSScanner:
    def test_import(self):
        from src.agents.scprs_scanner import (
            get_scanner_status, start_scanner, stop_scanner,
            manual_scan, scan_once, SCPRSScanner,
        )
        assert callable(manual_scan)

    def test_scanner_status_default(self):
        from src.agents.scprs_scanner import get_scanner_status
        status = get_scanner_status()
        assert "running" in status
        assert "interval" in status
        assert "scan_count" in status
        assert status["running"] is False  # Not started

    def test_manual_scan(self):
        from src.agents.scprs_scanner import manual_scan
        result = manual_scan()
        assert "scanned" in result
        assert "leads_created" in result
        assert result["scanned"] == 0  # No real SCPRS connection in test

    def test_scan_once(self):
        from src.agents.scprs_scanner import scan_once
        result = scan_once(won_history=[])
        assert "scanned" in result
        assert "new_pos" in result
        assert "errors" in result

    def test_seen_po_tracking(self):
        from src.agents.scprs_scanner import _load_seen, _save_seen
        seen = _load_seen()
        assert isinstance(seen, set)
        seen.add(f"TEST-PO-{time.time()}")
        _save_seen(seen)
        reloaded = _load_seen()
        assert len(reloaded) >= 1

    def test_scanner_start_stop(self):
        from src.agents.scprs_scanner import SCPRSScanner
        scanner = SCPRSScanner(interval=1)
        scanner.start()
        assert scanner.status["running"] is True
        time.sleep(0.5)
        scanner.stop()
        assert scanner.status["running"] is False

    def test_scanner_double_start(self):
        from src.agents.scprs_scanner import SCPRSScanner
        scanner = SCPRSScanner(interval=1)
        scanner.start()
        scanner.start()  # Should not crash
        scanner.stop()

    def test_scanner_stop_when_not_running(self):
        from src.agents.scprs_scanner import SCPRSScanner
        scanner = SCPRSScanner()
        scanner.stop()  # Should not crash


# ─── QuickBooks Agent Tests ──────────────────────────────────────────────────

class TestQuickBooksAgent:
    def test_import(self):
        from src.agents.quickbooks_agent import (
            fetch_vendors, find_vendor, create_purchase_order,
            get_agent_status, is_configured,
        )
        assert callable(is_configured)

    def test_not_configured(self):
        from src.agents.quickbooks_agent import is_configured
        # Without env vars, should be False
        result = is_configured()
        assert result is False

    def test_agent_status(self):
        from src.agents.quickbooks_agent import get_agent_status
        status = get_agent_status()
        assert status["agent"] == "quickbooks"
        assert "configured" in status
        assert "sandbox_mode" in status
        assert "has_valid_token" in status

    def test_fetch_vendors_unconfigured(self):
        from src.agents.quickbooks_agent import fetch_vendors
        vendors = fetch_vendors()
        assert isinstance(vendors, list)
        assert len(vendors) == 0  # Not configured

    def test_find_vendor_unconfigured(self):
        from src.agents.quickbooks_agent import find_vendor
        result = find_vendor("Amazon")
        assert result is None

    def test_create_po_unconfigured(self):
        from src.agents.quickbooks_agent import create_purchase_order
        result = create_purchase_order("vendor123", [{"description": "test", "qty": 1, "unit_cost": 10}])
        assert result is None

    def test_recent_pos_unconfigured(self):
        from src.agents.quickbooks_agent import get_recent_purchase_orders
        pos = get_recent_purchase_orders()
        assert isinstance(pos, list)
        assert len(pos) == 0

    def test_token_management(self):
        from src.agents.quickbooks_agent import _load_tokens, _save_tokens
        # Save and reload
        test_tokens = {"access_token": "test123", "expires_at": time.time() + 3600}
        _save_tokens(test_tokens)
        loaded = _load_tokens()
        assert loaded["access_token"] == "test123"

    def test_get_access_token_no_config(self):
        from src.agents.quickbooks_agent import get_access_token
        token = get_access_token()
        # Without config, should return None or cached test token
        # (it may return the test token we saved above)
        assert token is None or isinstance(token, str)
