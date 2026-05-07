"""Render-side tests for the /admin/observed-sends page (PR-G3).

Pins:
  * The page route is auth-required.
  * Empty + filled states render without UndefinedError.
  * Status-filter query string narrows the rows.
  * Drive-backup notes marker is parsed into a folder URL the
    template can surface.
  * Confirmed observation with a successful drive_backup marker
    renders the "📁 Drive" link AND the "✓ archived" badge instead
    of the "Backup to Drive" button.
"""
from __future__ import annotations

import importlib
import json
import sys

import pytest


@pytest.fixture
def routes_mod():
    """Just hand back the already-loaded module — the dashboard's
    exec()-based loader already registered the routes with `bp`, so
    re-importing would trigger duplicate-endpoint errors."""
    return importlib.import_module("src.api.modules.routes_observed_sends")


# ─── _drive_backup_url_from_notes helper ─────────────────────────────


def test_drive_backup_url_extracts_last_successful(routes_mod):
    notes = (
        '{"kind":"drive_backup","at":"2026-05-07T01:00:00",'
        '"folder_id":"old","folder_url":"https://drive.google.com/drive/folders/old",'
        '"uploaded":2,"error":""}\n'
        '{"kind":"drive_backup","at":"2026-05-07T02:00:00",'
        '"folder_id":"new","folder_url":"https://drive.google.com/drive/folders/new",'
        '"uploaded":3,"error":""}'
    )
    assert routes_mod._drive_backup_url_from_notes(notes) \
        == "https://drive.google.com/drive/folders/new"


def test_drive_backup_url_skips_failed_attempts(routes_mod):
    notes = (
        '{"kind":"drive_backup","at":"2026-05-07T01:00:00",'
        '"folder_id":"","folder_url":"","uploaded":0,'
        '"error":"Drive folder chain create failed"}'
    )
    assert routes_mod._drive_backup_url_from_notes(notes) == ""


def test_drive_backup_url_returns_empty_when_no_marker(routes_mod):
    assert routes_mod._drive_backup_url_from_notes("operator note") == ""
    assert routes_mod._drive_backup_url_from_notes("") == ""
    assert routes_mod._drive_backup_url_from_notes(None) == ""


def test_drive_backup_url_picks_successful_when_mixed(routes_mod):
    notes = (
        '{"kind":"drive_backup","at":"2026-05-07T01:00:00",'
        '"folder_id":"","folder_url":"","uploaded":0,"error":"flake"}\n'
        '{"kind":"drive_backup","at":"2026-05-07T02:00:00",'
        '"folder_id":"good","folder_url":"https://drive.google.com/drive/folders/good",'
        '"uploaded":2,"error":""}'
    )
    assert routes_mod._drive_backup_url_from_notes(notes) \
        == "https://drive.google.com/drive/folders/good"


# ─── /admin/observed-sends page route ────────────────────────────────


def test_page_requires_auth(anon_client):
    r = anon_client.get("/admin/observed-sends")
    assert r.status_code in (401, 302, 403)


def test_page_renders_empty_state(client, monkeypatch):
    """Empty list → empty-state copy, no UndefinedError."""
    monkeypatch.setattr(
        "src.agents.observed_send_store.list_observed_sends",
        lambda **kw: [],
    )
    r = client.get("/admin/observed-sends?status=auto_attached")
    assert r.status_code == 200
    assert b"No observations" in r.data
    assert b"data-testid=\"empty-state\"" in r.data


def test_page_renders_pending_row_with_actions(client, monkeypatch):
    rows = [{
        "id": 1, "gmail_message_id": "msg_001",
        "thread_id": "thread_aaa",
        "subject": "Reytech Quote R26Q40",
        "to_email": "valentina@cchcs.ca.gov",
        "sent_at": "Tue, 06 May 2026 15:30:00 -0700",
        "matched_record_id": "rfq_a5b09b56", "matched_record_kind": "rfq",
        "match_signal": "quote_number", "match_value": "R26Q40",
        "confidence": 0.95, "status": "pending",
        "decided_by": "", "decided_at": "", "notes": "",
    }]
    monkeypatch.setattr(
        "src.agents.observed_send_store.list_observed_sends",
        lambda **kw: rows,
    )
    r = client.get("/admin/observed-sends?status=pending")
    assert r.status_code == 200
    assert b"Reytech Quote R26Q40" in r.data
    assert b"data-testid=\"confirm-1\"" in r.data
    assert b"data-testid=\"reject-1\"" in r.data
    # Pending rows must not show backup button or drive link
    assert b"data-testid=\"backup-1\"" not in r.data
    assert b"data-testid=\"drive-link-1\"" not in r.data


def test_page_renders_confirmed_with_backup_link(client, monkeypatch):
    """Confirmed rows that already have a drive_backup marker in notes
    show the Drive link AND the '✓ archived' badge — NOT the
    Backup button."""
    backup_marker = json.dumps({
        "kind": "drive_backup",
        "at": "2026-05-07T01:30:00",
        "folder_id": "folder_xyz",
        "folder_url": "https://drive.google.com/drive/folders/folder_xyz",
        "uploaded": 3, "error": "",
    })
    rows = [{
        "id": 4, "gmail_message_id": "msg_004", "thread_id": "thread_ddd",
        "subject": "Reytech Quote R26Q38 — archived",
        "to_email": "cchcs@example.com",
        "sent_at": "Fri, 02 May 2026 11:00:00 -0700",
        "matched_record_id": "pc_beta", "matched_record_kind": "pc",
        "match_signal": "quote_number", "match_value": "R26Q38",
        "confidence": 0.95, "status": "confirmed",
        "decided_by": "mike", "decided_at": "2026-05-06T20:00:00Z",
        "notes": backup_marker,
    }]
    monkeypatch.setattr(
        "src.agents.observed_send_store.list_observed_sends",
        lambda **kw: rows,
    )
    r = client.get("/admin/observed-sends?status=confirmed")
    assert r.status_code == 200
    assert b"data-testid=\"drive-link-4\"" in r.data
    assert b"folder_xyz" in r.data
    assert b"\xe2\x9c\x93 archived" in r.data  # '✓ archived'
    # Already backed up → no Backup button
    assert b"data-testid=\"backup-4\"" not in r.data


def test_page_renders_confirmed_without_backup_shows_button(
        client, monkeypatch):
    rows = [{
        "id": 3, "gmail_message_id": "msg_003", "thread_id": "thread_ccc",
        "subject": "Reytech Quote R26Q39",
        "to_email": "someone@example.com",
        "sent_at": "Sun, 04 May 2026 14:00:00 -0700",
        "matched_record_id": "pc_alpha", "matched_record_kind": "pc",
        "match_signal": "quote_number", "match_value": "R26Q39",
        "confidence": 0.95, "status": "confirmed",
        "decided_by": "mike", "decided_at": "2026-05-07T05:00:00Z",
        "notes": "looks right",
    }]
    monkeypatch.setattr(
        "src.agents.observed_send_store.list_observed_sends",
        lambda **kw: rows,
    )
    r = client.get("/admin/observed-sends?status=confirmed")
    assert r.status_code == 200
    assert b"data-testid=\"backup-3\"" in r.data
    # No drive link yet
    assert b"data-testid=\"drive-link-3\"" not in r.data
