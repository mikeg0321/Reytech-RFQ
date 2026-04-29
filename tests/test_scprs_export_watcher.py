"""Tests for the SCPRS export watcher (auto-ingest from Drive
`Backups/SCPRS Exports/`).

Verifies the file-selection + dedup-by-imported_at logic without
hitting Drive. Drive helpers are monkeypatched on the `gdrive`
module the watcher imports from."""
from __future__ import annotations

import io
import sys
from unittest.mock import patch

import pytest


def _patch_drive(monkeypatch, find_folder=None, list_files=None,
                 download_bytes=None, configured=True, root_id="ROOT"):
    """Stub out the gdrive helpers the watcher walks through."""
    from src.core import gdrive as _g
    monkeypatch.setattr(_g, "is_configured", lambda: configured)
    monkeypatch.setattr(_g, "GOOGLE_DRIVE_ROOT_FOLDER_ID", root_id, raising=False)
    if find_folder is not None:
        monkeypatch.setattr(_g, "find_folder", find_folder)
    if list_files is not None:
        monkeypatch.setattr(_g, "list_files", list_files)
    if download_bytes is not None:
        # Monkeypatch the watcher's helper directly (it uses MediaIoBase
        # internally so easier to stub at the boundary).
        from src.agents import scprs_export_watcher as _w
        monkeypatch.setattr(_w, "_download_file_bytes", download_bytes)


def test_watcher_returns_zero_when_drive_not_configured(monkeypatch):
    from src.agents import scprs_export_watcher as w
    _patch_drive(monkeypatch, configured=False)
    summary = w.scan_once()
    assert summary["scanned"] == 0
    assert summary["ingested"] == 0


def test_watcher_skips_when_watch_path_missing(monkeypatch):
    """If `Backups/SCPRS Exports/` folder doesn't exist in Drive,
    return clean zero — operator hasn't created the folder yet."""
    from src.agents import scprs_export_watcher as w
    _patch_drive(
        monkeypatch,
        find_folder=lambda name, parent: None,
    )
    summary = w.scan_once()
    assert summary["scanned"] == 0
    assert summary["ingested"] == 0


def test_watcher_only_picks_xls_files(monkeypatch):
    """Folder might have .xlsx, .pdf, README.txt etc. — only .xls files
    (the SCPRS Detail Information format) get ingested."""
    from src.agents import scprs_export_watcher as w
    files_seen = []

    def _list(parent_id):
        return [
            {"id": "f1", "name": "Detail_Information_2026-04-28.xls",
             "mimeType": "application/vnd.ms-excel",
             "modifiedTime": "2026-04-28T16:44:50.000Z"},
            {"id": "f2", "name": "notes.pdf",
             "mimeType": "application/pdf",
             "modifiedTime": "2026-04-28T17:00:00.000Z"},
            {"id": "f3", "name": "old_export.xlsx",
             "mimeType": "application/vnd.ms-excel.openxmlformats",
             "modifiedTime": "2026-04-25T12:00:00.000Z"},
        ]

    def _download(file_id):
        files_seen.append(file_id)
        # Return a tiny HTML table that import_html can handle.
        return b"<table><tr><th>Business Unit</th><th>Department Name</th><th>Purchase Document #</th><th>Associated PO #</th><th>Start Date</th><th>End Date</th><th>Grand Total</th></tr></table>"

    _patch_drive(
        monkeypatch,
        find_folder=lambda name, parent: f"id_{name}",
        list_files=_list,
        download_bytes=_download,
    )
    summary = w.scan_once()
    assert summary["scanned"] == 1   # only the .xls counted
    # The xls was downloaded and attempted to ingest (parse may yield 0
    # POs since the body is just a header — that's fine, no error).
    assert "f1" in files_seen
    assert "f2" not in files_seen
    assert "f3" not in files_seen


def test_watcher_skips_files_older_than_last_imported_at(
        monkeypatch, auth_client):
    """If the DB's MAX(imported_at) is later than a file's modifiedTime,
    don't re-ingest — we already processed it. This is the dedup
    that prevents the watcher from importing the same export every
    5 minutes."""
    from src.core.db import get_db
    from src.agents import scprs_export_watcher as w

    # Seed a "we already imported a newer file" timestamp.
    with get_db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS scprs_reytech_wins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number TEXT NOT NULL,
                business_unit TEXT,
                dept_name TEXT,
                associated_po TEXT,
                start_date TEXT,
                end_date TEXT,
                grand_total REAL,
                items_json TEXT,
                imported_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        c.execute("DELETE FROM scprs_reytech_wins")
        c.execute("""
            INSERT INTO scprs_reytech_wins
            (po_number, business_unit, dept_name, grand_total, items_json,
             imported_at)
            VALUES ('TEST', '8955', 'Test', 0, '[]', '2026-04-28 17:00:00')
        """)
        c.commit()

    download_calls = []

    def _list(parent_id):
        return [
            # Modified BEFORE the import timestamp → must skip
            {"id": "old_file", "name": "Detail_Information_old.xls",
             "mimeType": "application/vnd.ms-excel",
             "modifiedTime": "2026-04-28T16:00:00.000Z"},
        ]

    def _download(file_id):
        download_calls.append(file_id)
        return b"<table></table>"

    _patch_drive(
        monkeypatch,
        find_folder=lambda name, parent: f"id_{name}",
        list_files=_list,
        download_bytes=_download,
    )
    summary = w.scan_once()
    assert summary["skipped_older"] == 1
    assert summary["ingested"] == 0
    assert download_calls == []   # never downloaded


def test_watcher_ingests_newer_file_and_logs_status(
        monkeypatch, auth_client):
    """A file newer than the last_imported_at watermark is fetched,
    decoded, and passed to import_html; the watcher's status reflects
    the most recent file name."""
    from src.core.db import get_db
    from src.agents import scprs_export_watcher as w

    # No previous import — watermark is empty.
    with get_db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS scprs_reytech_wins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number TEXT NOT NULL,
                business_unit TEXT,
                dept_name TEXT,
                associated_po TEXT,
                start_date TEXT,
                end_date TEXT,
                grand_total REAL,
                items_json TEXT,
                imported_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        c.execute("DELETE FROM scprs_reytech_wins")
        c.commit()

    minimal_html = (
        "<table>"
        "<tr><th>Business Unit</th><th>Department Name</th>"
        "<th>Purchase Document #</th><th>Associated PO #</th>"
        "<th>Start Date</th><th>End Date</th><th>Grand Total</th></tr>"
        "<tr><td>'8955</td><td>Dept of Veterans Affairs</td>"
        "<td>'0000076737</td><td></td><td>03/30/2026</td>"
        "<td></td><td>$87609.27</td></tr>"
        "</table>"
    )

    def _list(parent_id):
        return [
            {"id": "new_file", "name": "Detail_Information_2026-04-28.xls",
             "mimeType": "application/vnd.ms-excel",
             "modifiedTime": "2026-04-28T20:00:00.000Z"},
        ]

    def _download(file_id):
        return minimal_html.encode("utf-8")

    _patch_drive(
        monkeypatch,
        find_folder=lambda name, parent: f"id_{name}",
        list_files=_list,
        download_bytes=_download,
    )
    summary = w.scan_once()
    assert summary["ingested"] == 1
    assert summary["skipped_older"] == 0
    assert len(summary["files"]) == 1
    assert summary["files"][0]["name"] == "Detail_Information_2026-04-28.xls"
    # Status reflects the latest file
    status = w.get_status()
    assert status["last_file_seen"] == "Detail_Information_2026-04-28.xls"
    # Verify the row landed in the DB
    with get_db() as c:
        row = c.execute(
            "SELECT po_number, business_unit FROM scprs_reytech_wins"
        ).fetchone()
    assert row is not None
    assert row["po_number"] == "0000076737"
    assert row["business_unit"] == "8955"


def test_watcher_idempotent_re_ingest_same_file(
        monkeypatch, auth_client):
    """Re-running with the same file (same modifiedTime) after one
    successful ingest must skip on the second pass — that's how we
    prevent the 5-min loop from re-processing the same export forever.
    """
    from src.core.db import get_db
    from src.agents import scprs_export_watcher as w

    with get_db() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS scprs_reytech_wins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number TEXT NOT NULL,
                business_unit TEXT,
                dept_name TEXT,
                associated_po TEXT,
                start_date TEXT,
                end_date TEXT,
                grand_total REAL,
                items_json TEXT,
                imported_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        c.execute("DELETE FROM scprs_reytech_wins")
        c.commit()

    minimal_html = (
        "<table>"
        "<tr><th>Business Unit</th><th>Department Name</th>"
        "<th>Purchase Document #</th><th>Associated PO #</th>"
        "<th>Start Date</th><th>End Date</th><th>Grand Total</th></tr>"
        "<tr><td>'8955</td><td>Dept of Veterans Affairs</td>"
        "<td>'0000076737</td><td></td><td>03/30/2026</td>"
        "<td></td><td>$87609.27</td></tr>"
        "</table>"
    )

    def _list(parent_id):
        return [
            {"id": "f1", "name": "x.xls",
             "mimeType": "application/vnd.ms-excel",
             "modifiedTime": "2026-04-28T01:00:00.000Z"},
        ]

    def _download(file_id):
        return minimal_html.encode("utf-8")

    _patch_drive(
        monkeypatch,
        find_folder=lambda name, parent: f"id_{name}",
        list_files=_list,
        download_bytes=_download,
    )
    s1 = w.scan_once()
    s2 = w.scan_once()
    assert s1["ingested"] == 1
    # Second pass: imported_at watermark now > file's modifiedTime → skip.
    assert s2["ingested"] == 0
    assert s2["skipped_older"] == 1


def test_watcher_status_keys_present():
    """Status dict must always have the keys the /health/quoting card
    reads — so the card can't crash with a KeyError mid-render."""
    from src.agents.scprs_export_watcher import get_status
    s = get_status()
    for key in ("running", "last_run_at", "last_file_seen",
                "files_ingested", "scans_completed", "watch_path"):
        assert key in s, f"missing key: {key}"


def test_watcher_admin_endpoint_runs_scan(
        monkeypatch, auth_client):
    """POST /api/admin/scprs-watcher-scan triggers scan_once and
    returns the summary."""
    from src.agents import scprs_export_watcher as w
    _patch_drive(
        monkeypatch,
        find_folder=lambda name, parent: None,  # path missing → 0 files
    )
    resp = auth_client.post("/api/admin/scprs-watcher-scan")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "scanned" in data
    assert data["scanned"] == 0


# ── Folder bootstrap on watcher boot ──────────────────────────────────


def _reset_status():
    """Reset the module-level status dict so each bootstrap test starts
    with a clean slate (the dict persists across tests in the same
    interpreter)."""
    from src.agents import scprs_export_watcher as w
    w._status["bootstrap_status"] = None
    w._status["watch_folder_id"] = None
    w._status["bootstrap_at"] = None


def test_ensure_watch_folder_skipped_when_drive_unconfigured(monkeypatch):
    """No Drive creds → bootstrap returns None and records the
    'skipped_no_drive_config' status; the watcher still functions, it
    just never finds files."""
    from src.agents import scprs_export_watcher as w
    _reset_status()
    _patch_drive(monkeypatch, configured=False)

    fid = w._ensure_watch_folder_id()
    assert fid is None
    assert w._status["bootstrap_status"] == "skipped_no_drive_config"


def test_ensure_watch_folder_creates_chain_when_missing(monkeypatch):
    """First boot in a fresh tenant: Backups/SCPRS Exports/ doesn't
    exist yet. Bootstrap walks the chain via _get_or_create_folder
    and ends up with a folder ID + ok status."""
    from src.agents import scprs_export_watcher as w
    from src.core import gdrive as _g
    _reset_status()

    created = []

    def _goc(name, parent):
        created.append((name, parent))
        # Pretend each segment is created with a deterministic ID
        return f"id_{name.replace(' ', '_')}"

    monkeypatch.setattr(_g, "is_configured", lambda: True)
    monkeypatch.setattr(_g, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT",
                        raising=False)
    monkeypatch.setattr(_g, "_get_or_create_folder", _goc)

    fid = w._ensure_watch_folder_id()
    assert fid == "id_SCPRS_Exports"
    assert created == [("Backups", "ROOT"),
                       ("SCPRS Exports", "id_Backups")]
    assert w._status["bootstrap_status"] == "ok"
    assert w._status["watch_folder_id"] == "id_SCPRS_Exports"
    assert w._status["bootstrap_at"] is not None


def test_ensure_watch_folder_idempotent_on_repeat(monkeypatch):
    """Calling bootstrap twice with the same folders already present
    is a no-op (the underlying _get_or_create_folder finds the
    existing folder by name)."""
    from src.agents import scprs_export_watcher as w
    from src.core import gdrive as _g
    _reset_status()

    monkeypatch.setattr(_g, "is_configured", lambda: True)
    monkeypatch.setattr(_g, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT",
                        raising=False)
    # The "existing folder" case — _get_or_create_folder just returns
    # the cached ID without creating.
    monkeypatch.setattr(
        _g, "_get_or_create_folder",
        lambda name, parent: f"id_{name.replace(' ', '_')}",
    )

    a = w._ensure_watch_folder_id()
    b = w._ensure_watch_folder_id()
    assert a == b == "id_SCPRS_Exports"


def test_ensure_watch_folder_records_error_on_drive_failure(monkeypatch):
    """If Drive returns a 5xx mid-walk, bootstrap records the error
    string and returns None — the watcher's main loop still starts
    and re-resolves the folder on each tick."""
    from src.agents import scprs_export_watcher as w
    from src.core import gdrive as _g
    _reset_status()

    def _boom(name, parent):
        if name == "SCPRS Exports":
            raise RuntimeError("Drive 503")
        return f"id_{name}"

    monkeypatch.setattr(_g, "is_configured", lambda: True)
    monkeypatch.setattr(_g, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT",
                        raising=False)
    monkeypatch.setattr(_g, "_get_or_create_folder", _boom)

    fid = w._ensure_watch_folder_id()
    assert fid is None
    assert w._status["bootstrap_status"].startswith("error:")
    assert "Drive 503" in w._status["bootstrap_status"]


def test_get_status_includes_bootstrap_fields(monkeypatch):
    """The /health/quoting watcher card surfaces bootstrap state via
    get_status(); make sure the new fields are there."""
    from src.agents import scprs_export_watcher as w
    _reset_status()
    s = w.get_status()
    assert "bootstrap_status" in s
    assert "watch_folder_id" in s
    assert "bootstrap_at" in s
