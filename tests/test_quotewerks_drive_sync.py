"""Tests for QuoteWerks → Google Drive sync.

Covers sync_quotewerks_from_drive() and /api/catalog/sync-quotewerks-drive.
Uses monkeypatch to stub gdrive.download_file and the importer so tests
never touch real Drive or real catalog DB state.
"""
import os
import pytest


class TestSyncQuotewerksFromDrive:
    """The function must fail clearly when misconfigured and route through
    the existing importer when downloads succeed. Drive is the canonical
    source per user rule: quotewerks export lives in Drive, app pulls it."""

    def test_no_file_id_and_no_env_returns_error(self, monkeypatch):
        monkeypatch.delenv("QUOTEWERKS_DRIVE_FILE_ID", raising=False)
        from src.agents.product_catalog import sync_quotewerks_from_drive
        result = sync_quotewerks_from_drive(file_id=None)
        assert result["ok"] is False
        assert "QUOTEWERKS_DRIVE_FILE_ID" in result["error"]

    def test_drive_not_configured_returns_error(self, monkeypatch):
        import src.core.gdrive as _gd
        monkeypatch.setattr(_gd, "is_configured", lambda: False)
        from src.agents.product_catalog import sync_quotewerks_from_drive
        result = sync_quotewerks_from_drive(file_id="fake-drive-id")
        assert result["ok"] is False
        assert "not configured" in result["error"].lower()

    def test_download_failure_returns_error(self, monkeypatch):
        import src.core.gdrive as _gd
        monkeypatch.setattr(_gd, "is_configured", lambda: True)
        monkeypatch.setattr(_gd, "download_file", lambda fid, path: False)
        from src.agents.product_catalog import sync_quotewerks_from_drive
        result = sync_quotewerks_from_drive(file_id="fake-drive-id")
        assert result["ok"] is False
        assert "Download" in result["error"] or "download" in result["error"]
        assert result["drive_file_id"] == "fake-drive-id"

    def test_empty_download_returns_error(self, monkeypatch, tmp_path):
        import src.core.gdrive as _gd
        import src.core.paths as _paths
        monkeypatch.setattr(_paths, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(_gd, "is_configured", lambda: True)

        def _fake_download(fid, path):
            with open(path, "wb") as f:
                f.write(b"")
            return True

        monkeypatch.setattr(_gd, "download_file", _fake_download)
        from src.agents.product_catalog import sync_quotewerks_from_drive
        result = sync_quotewerks_from_drive(file_id="fake-drive-id")
        assert result["ok"] is False
        assert "empty" in result["error"].lower()

    def test_happy_path_data_manager_format(self, monkeypatch, tmp_path):
        import src.core.gdrive as _gd
        import src.core.paths as _paths
        import src.agents.product_catalog as _pc
        monkeypatch.setattr(_paths, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(_gd, "is_configured", lambda: True)

        # Write a minimal Data Manager-style CSV (no DocumentItems_ prefix)
        def _fake_download(fid, path):
            with open(path, "w", encoding="utf-8") as f:
                f.write("Description,ManufacturerPartNumber,ItemURL,UnitList\n")
                f.write("Test Widget,WID-001,https://example.com/widget,19.99\n")
            return True

        monkeypatch.setattr(_gd, "download_file", _fake_download)

        # Stub the importer — we're testing the bridge, not the importer
        monkeypatch.setattr(_pc, "init_catalog_db", lambda: None)
        monkeypatch.setattr(_pc, "import_quotewerks_csv",
                            lambda path, replace=False: {
                                "imported": 1, "updated": 0, "skipped": 0,
                                "total_rows": 1, "errors": []
                            })

        result = _pc.sync_quotewerks_from_drive(file_id="fake-drive-id")
        assert result["ok"] is True
        assert result["format"] == "data_manager"
        assert result["imported"] == 1
        assert result["drive_file_id"] == "fake-drive-id"
        assert result["downloaded_bytes"] > 0

    def test_happy_path_documents_report_format(self, monkeypatch, tmp_path):
        import src.core.gdrive as _gd
        import src.core.paths as _paths
        import src.agents.product_catalog as _pc
        monkeypatch.setattr(_paths, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(_gd, "is_configured", lambda: True)

        # Documents Report format: columns prefixed with DocumentItems_ or DocumentHeaders_
        def _fake_download(fid, path):
            with open(path, "w", encoding="utf-8") as f:
                f.write("DocumentItems_Description,DocumentItems_ManufacturerPartNumber,"
                        "DocumentItems_ItemURL,DocumentItems_UnitList\n")
                f.write("Test Widget,WID-001,https://example.com/w,19.99\n")
            return True

        monkeypatch.setattr(_gd, "download_file", _fake_download)
        monkeypatch.setattr(_pc, "init_catalog_db", lambda: None)
        monkeypatch.setattr(_pc, "import_qw_documents_report",
                            lambda path, replace=False: {
                                "imported": 1, "updated": 0, "skipped": 0,
                                "urls_stored": 1, "total_rows": 1,
                                "errors": [], "qa_flags": []
                            })

        result = _pc.sync_quotewerks_from_drive(file_id="fake-drive-id")
        assert result["ok"] is True
        assert result["format"] == "documents_report"
        assert result["urls_stored"] == 1

    def test_env_var_fallback(self, monkeypatch, tmp_path):
        # When no file_id arg, use QUOTEWERKS_DRIVE_FILE_ID env var
        import src.core.gdrive as _gd
        import src.core.paths as _paths
        import src.agents.product_catalog as _pc
        monkeypatch.setenv("QUOTEWERKS_DRIVE_FILE_ID", "env-drive-id")
        monkeypatch.setattr(_paths, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(_gd, "is_configured", lambda: True)

        captured_fid = []

        def _fake_download(fid, path):
            captured_fid.append(fid)
            with open(path, "w") as f:
                f.write("Description,UnitList\nTest,9.99\n")
            return True

        monkeypatch.setattr(_gd, "download_file", _fake_download)
        monkeypatch.setattr(_pc, "init_catalog_db", lambda: None)
        monkeypatch.setattr(_pc, "import_quotewerks_csv",
                            lambda path, replace=False: {"imported": 1, "total_rows": 1})

        result = _pc.sync_quotewerks_from_drive()
        assert result["ok"] is True
        assert captured_fid == ["env-drive-id"]


class TestSyncQuotewerksDriveRoute:
    """/api/catalog/sync-quotewerks-drive should surface errors faithfully."""

    def test_route_returns_500_on_missing_config(self, client, monkeypatch):
        monkeypatch.delenv("QUOTEWERKS_DRIVE_FILE_ID", raising=False)
        r = client.post("/api/catalog/sync-quotewerks-drive",
                        json={}, content_type="application/json")
        assert r.status_code == 500
        body = r.get_json()
        assert body["ok"] is False
        assert "QUOTEWERKS_DRIVE_FILE_ID" in body["error"]

    def test_route_happy_path(self, client, monkeypatch, tmp_path):
        import src.core.gdrive as _gd
        import src.core.paths as _paths
        import src.agents.product_catalog as _pc
        monkeypatch.setattr(_paths, "DATA_DIR", str(tmp_path))
        monkeypatch.setattr(_gd, "is_configured", lambda: True)

        def _fake_download(fid, path):
            with open(path, "w") as f:
                f.write("Description,UnitList\nFoo,1.23\n")
            return True

        monkeypatch.setattr(_gd, "download_file", _fake_download)
        monkeypatch.setattr(_pc, "init_catalog_db", lambda: None)
        monkeypatch.setattr(_pc, "import_quotewerks_csv",
                            lambda path, replace=False: {"imported": 1, "total_rows": 1})

        r = client.post("/api/catalog/sync-quotewerks-drive",
                        json={"file_id": "route-test-id"},
                        content_type="application/json")
        assert r.status_code == 200, r.data
        body = r.get_json()
        assert body["ok"] is True
        assert body["drive_file_id"] == "route-test-id"
        assert body["imported"] == 1
