"""Infrastructure tests — alerting, webhooks, backup verification, deploy config.

Tests that the operational infrastructure is correctly wired:
- Error alerting routes to Slack
- Webhook event types include operational events
- Backup verification catches real issues
- Deploy config is valid
- Railway config matches expectations
"""
import json
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


class TestWebhookEventTypes:
    """Operational events must be registered in the webhook system."""

    def test_server_error_registered(self):
        from src.core.webhooks import EVENT_TYPES
        assert "server_error" in EVENT_TYPES

    def test_db_lock_timeout_registered(self):
        from src.core.webhooks import EVENT_TYPES
        assert "db_lock_timeout" in EVENT_TYPES

    def test_enrichment_failed_registered(self):
        from src.core.webhooks import EVENT_TYPES
        assert "enrichment_failed" in EVENT_TYPES

    def test_deploy_complete_registered(self):
        from src.core.webhooks import EVENT_TYPES
        assert "deploy_complete" in EVENT_TYPES

    def test_business_events_still_present(self):
        """Ensure we didn't break existing business events."""
        from src.core.webhooks import EVENT_TYPES
        for event in ["new_rfq", "quote_sent", "quote_won", "order_created"]:
            assert event in EVENT_TYPES, f"Missing business event: {event}"


class TestWebhookRoutingForErrors:
    """Server errors must be routed to the webhook system."""

    def test_error_events_in_webhook_filter(self):
        """The webhook event filter must include operational error events."""
        webhook_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "core", "webhooks.py")
        with open(webhook_path, "r") as f:
            source = f.read()
        for event in ["server_error", "db_lock_timeout", "enrichment_failed"]:
            assert event in source, f"'{event}' not found in webhooks.py event filter"


class TestErrorHandlerWiring:
    """app.py 500 handler must call both send_alert AND fire_event."""

    def test_500_handler_fires_webhook(self):
        app_path = os.path.join(os.path.dirname(__file__), "..", "app.py")
        with open(app_path, "r") as f:
            source = f.read()
        assert "fire_event" in source, "500 handler should call fire_event for Slack"
        assert "server_error" in source, "500 handler should fire server_error event"


class TestBackupVerification:
    """Backup verify script correctly validates and rejects databases."""

    def test_valid_db_passes(self, temp_data_dir):
        """A properly initialized test DB should pass verification."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
        from verify_backup import verify_database

        db_path = os.path.join(temp_data_dir, "reytech.db")
        result = verify_database(db_path)

        assert result["ok"] is True, (
            f"Valid DB failed verification:\n"
            + "\n".join(result["errors"])
        )

    def test_empty_file_fails(self, tmp_path):
        """An empty file should fail verification."""
        from scripts.verify_backup import verify_database

        db_path = str(tmp_path / "empty.db")
        with open(db_path, "w") as f:
            f.write("")

        result = verify_database(db_path)
        assert result["ok"] is False

    def test_missing_file_fails(self, tmp_path):
        """A nonexistent file should fail verification."""
        from scripts.verify_backup import verify_database

        result = verify_database(str(tmp_path / "nonexistent.db"))
        assert result["ok"] is False

    def test_corrupt_file_fails(self, tmp_path):
        """A file with wrong header should fail verification."""
        from scripts.verify_backup import verify_database

        db_path = str(tmp_path / "corrupt.db")
        with open(db_path, "wb") as f:
            f.write(b"this is not a sqlite database")

        result = verify_database(db_path)
        assert result["ok"] is False


class TestRailwayConfig:
    """Railway deployment config must match expectations."""

    def test_railway_toml_exists(self):
        toml_path = os.path.join(os.path.dirname(__file__), "..", "railway.toml")
        assert os.path.exists(toml_path), "railway.toml missing"

    def test_single_worker(self):
        """Must use 1 worker (SQLite can't handle multi-process writes)."""
        toml_path = os.path.join(os.path.dirname(__file__), "..", "railway.toml")
        with open(toml_path) as f:
            content = f.read()
        assert "--workers 1" in content, (
            "Railway must use --workers 1 for SQLite compatibility"
        )

    def test_wal_timeout(self):
        """Gunicorn timeout must be >= 120s for long PDF generation."""
        toml_path = os.path.join(os.path.dirname(__file__), "..", "railway.toml")
        with open(toml_path) as f:
            content = f.read()
        assert "--timeout 120" in content

    def test_volume_mounted(self):
        """Data volume must be mounted at /data."""
        toml_path = os.path.join(os.path.dirname(__file__), "..", "railway.toml")
        with open(toml_path) as f:
            content = f.read()
        assert 'mountPath = "/data"' in content


class TestDeployScripts:
    """Deploy and staging scripts must exist and be valid."""

    def test_deploy_script_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "scripts", "deploy.sh")
        assert os.path.exists(path)

    def test_staging_script_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "scripts", "staging.sh")
        assert os.path.exists(path)

    def test_verify_backup_script_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "scripts", "verify_backup.py")
        assert os.path.exists(path)

    def test_verify_backup_compiles(self):
        import py_compile
        path = os.path.join(os.path.dirname(__file__), "..", "scripts", "verify_backup.py")
        py_compile.compile(path, doraise=True)
