"""Contract tests for src/agents/observed_send_backup.py.

Pins the Drive-backup behavior of confirmed observed-sends (PR-H of
post-quote queue item 23, 2026-05-07).

Critical contracts:
  * Refuses to back up rows that aren't `confirmed`.
  * Refuses when row has no gmail_message_id.
  * Pulls attachments from the actual Sent Gmail message (server
    truth — what the buyer received, not what we generated).
  * Folder placement is `Backups/Sent Quote Packages/{year}/Q{q}/{record_label}/`,
    using the SENT date (parsed from message header), not now().
  * Folder segments are sanitized — no slashes, control chars, or
    reserved Windows chars sneak through.
  * Idempotent — re-running on a row reuses the folder; gdrive
    `upload_bytes` updates same-name files in place.
  * Drive flake leaves the obs row's `status` unchanged (Reytech Law
    22 — never destroy operator decisions on backup failure).
  * Marker stamped on observation `notes` field on success and
    failure so the UI surface can show "Backed up" vs "Backup error".
"""
from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mod():
    if "src.agents.observed_send_backup" in sys.modules:
        del sys.modules["src.agents.observed_send_backup"]
    return importlib.import_module("src.agents.observed_send_backup")


@pytest.fixture
def conn(tmp_path):
    """Fresh in-memory DB seeded with migration 41 schema."""
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    from src.core.migrations import MIGRATIONS
    for num, _name, sql in MIGRATIONS:
        if num == 41:
            db.executescript(sql)
    yield db
    db.close()


def _seed_obs(db, *, status="confirmed", gmail_id="msg_001",
              sent_at="Tue, 06 May 2026 15:30:00 -0700",
              record_id="rfq_alpha", match_value="R26Q40"):
    db.execute("""
        INSERT INTO observed_sends (
            gmail_message_id, thread_id, subject, to_email, sent_at,
            matched_record_id, matched_record_kind, match_signal,
            match_value, confidence, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (gmail_id, "thread_x", "Reytech Quote R26Q40",
          "buyer@example.com", sent_at, record_id, "rfq",
          "quote_number", match_value, 0.95, status))
    db.commit()
    return db.execute(
        "SELECT id FROM observed_sends WHERE gmail_message_id=?",
        (gmail_id,)).fetchone()["id"]


# ─── helpers ──────────────────────────────────────────────────────────


def test_quarter_for_calendar_quarters(mod):
    from datetime import datetime
    assert mod._quarter_for(datetime(2026, 1, 15)) == "Q1"
    assert mod._quarter_for(datetime(2026, 3, 31)) == "Q1"
    assert mod._quarter_for(datetime(2026, 4, 1)) == "Q2"
    assert mod._quarter_for(datetime(2026, 7, 1)) == "Q3"
    assert mod._quarter_for(datetime(2026, 12, 31)) == "Q4"


def test_safe_folder_segment_strips_path_chars(mod):
    assert mod._safe_folder_segment("normal name") == "normal name"
    assert mod._safe_folder_segment("foo/bar") == "foo_bar"
    assert mod._safe_folder_segment("foo\\bar") == "foo_bar"
    assert mod._safe_folder_segment('test"quote*') == "test_quote_"
    assert mod._safe_folder_segment("") == ""
    # Control chars
    assert "\x00" not in mod._safe_folder_segment("foo\x00bar")


def test_parse_sent_at_handles_rfc2822(mod):
    dt = mod._parse_sent_at("Tue, 06 May 2026 15:30:00 -0700")
    assert dt.year == 2026 and dt.month == 5 and dt.day == 6


def test_parse_sent_at_falls_back_to_now_on_garbage(mod):
    dt = mod._parse_sent_at("not a date")
    # Just confirm we got some recent datetime — exact value depends on now()
    assert dt.year >= 2025


def test_extract_attachments_walks_multipart(mod):
    """Build a real multipart/mixed message with one attachment and
    one inline body part — extractor returns just the attachment."""
    raw = (
        b"From: me@example.com\r\n"
        b"To: buyer@example.com\r\n"
        b"Subject: test\r\n"
        b"Content-Type: multipart/mixed; boundary=BOUNDARY\r\n"
        b"\r\n"
        b"--BOUNDARY\r\n"
        b"Content-Type: text/plain\r\n"
        b"\r\n"
        b"hello body\r\n"
        b"--BOUNDARY\r\n"
        b'Content-Type: application/pdf; name="quote.pdf"\r\n'
        b'Content-Disposition: attachment; filename="quote.pdf"\r\n'
        b"Content-Transfer-Encoding: base64\r\n"
        b"\r\n"
        b"JVBERi0xLjQK\r\n"  # base64 for "%PDF-1.4\n"
        b"--BOUNDARY--\r\n"
    )
    out = mod._extract_attachments(raw)
    assert len(out) == 1
    fname, mime, data = out[0]
    assert fname == "quote.pdf"
    assert mime == "application/pdf"
    assert data.startswith(b"%PDF-1.4")


# ─── backup_observation refusal cases ────────────────────────────────


def test_backup_refuses_unknown_observation(mod, conn):
    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        result = mod.backup_observation(99999)
    assert not result["ok"]
    assert "not found" in result["error"]


def test_backup_refuses_pending_observation(mod, conn):
    obs_id = _seed_obs(conn, status="pending")
    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        result = mod.backup_observation(obs_id)
    assert not result["ok"]
    assert "confirmed" in result["error"]


def test_backup_refuses_when_no_gmail_message_id(mod, conn):
    obs_id = _seed_obs(conn, gmail_id="")
    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        result = mod.backup_observation(obs_id)
    assert not result["ok"]
    assert "gmail_message_id" in result["error"]


# ─── backup_observation happy path ───────────────────────────────────


def test_backup_uploads_each_attachment_to_drive(mod, conn):
    obs_id = _seed_obs(conn)
    raw = _build_raw(["quote.pdf", "704B.pdf"])

    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service",
                   return_value=MagicMock()), \
             patch("src.core.gmail_api.get_raw_message",
                   return_value=raw), \
             patch("src.core.gdrive.is_configured", return_value=True), \
             patch("src.core.gdrive.GOOGLE_DRIVE_ROOT_FOLDER_ID",
                   "root_drive_folder"), \
             patch("src.core.gdrive._get_or_create_folder",
                   side_effect=lambda name, parent:
                   f"folder_{name.replace(' ', '_')}_{parent[-4:]}"), \
             patch("src.core.gdrive.upload_bytes",
                   side_effect=lambda data, folder, name, mime_type=None:
                   f"file_{name}"):
            result = mod.backup_observation(obs_id)

    assert result["ok"]
    assert len(result["uploaded"]) == 2
    names = {u["filename"] for u in result["uploaded"]}
    assert names == {"quote.pdf", "704B.pdf"}
    assert "drive.google.com/drive/folders/" in result["folder_url"]


def test_backup_uses_sent_at_year_for_folder_placement(mod, conn):
    """A 2026-Q2 send must go to Backups/Sent Quote Packages/2026/Q2/...,
    not under whatever quarter today happens to be in."""
    obs_id = _seed_obs(conn,
                       sent_at="Mon, 05 May 2025 09:00:00 -0700")  # 2025 Q2
    raw = _build_raw(["q.pdf"])

    seen_segments = []

    def _track_create(name, parent):
        seen_segments.append(name)
        return f"folder_{len(seen_segments)}"

    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=MagicMock()), \
             patch("src.core.gmail_api.get_raw_message", return_value=raw), \
             patch("src.core.gdrive.is_configured", return_value=True), \
             patch("src.core.gdrive.GOOGLE_DRIVE_ROOT_FOLDER_ID", "root"), \
             patch("src.core.gdrive._get_or_create_folder", side_effect=_track_create), \
             patch("src.core.gdrive.upload_bytes", return_value="file_x"):
            mod.backup_observation(obs_id)

    assert "Backups" in seen_segments
    assert "Sent Quote Packages" in seen_segments
    assert "2025" in seen_segments
    assert "Q2" in seen_segments


def test_backup_marks_observation_notes_with_drive_url(mod, conn):
    obs_id = _seed_obs(conn)
    raw = _build_raw(["q.pdf"])

    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=MagicMock()), \
             patch("src.core.gmail_api.get_raw_message", return_value=raw), \
             patch("src.core.gdrive.is_configured", return_value=True), \
             patch("src.core.gdrive.GOOGLE_DRIVE_ROOT_FOLDER_ID", "root"), \
             patch("src.core.gdrive._get_or_create_folder",
                   return_value="leaf_folder_id"), \
             patch("src.core.gdrive.upload_bytes",
                   return_value="file_id"):
            mod.backup_observation(obs_id)

    notes = conn.execute(
        "SELECT notes FROM observed_sends WHERE id=?",
        (obs_id,)).fetchone()["notes"]
    marker = json.loads(notes.split("\n")[-1])
    assert marker["kind"] == "drive_backup"
    assert marker["folder_id"] == "leaf_folder_id"
    assert marker["uploaded"] == 1


# ─── failure modes ───────────────────────────────────────────────────


def test_backup_returns_error_when_drive_not_configured(mod, conn):
    obs_id = _seed_obs(conn)

    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=MagicMock()), \
             patch("src.core.gmail_api.get_raw_message",
                   return_value=_build_raw(["q.pdf"])), \
             patch("src.core.gdrive.is_configured", return_value=False), \
             patch("src.core.gdrive.GOOGLE_DRIVE_ROOT_FOLDER_ID", ""):
            result = mod.backup_observation(obs_id)

    assert not result["ok"]
    assert "Drive folder chain create failed" in result["error"]


def test_backup_does_not_change_observation_status_on_failure(mod, conn):
    """Drive flake should not flip status away from `confirmed`."""
    obs_id = _seed_obs(conn)

    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=MagicMock()), \
             patch("src.core.gmail_api.get_raw_message",
                   side_effect=RuntimeError("Gmail flake")):
            mod.backup_observation(obs_id)

    status_after = conn.execute(
        "SELECT status FROM observed_sends WHERE id=?",
        (obs_id,)).fetchone()["status"]
    assert status_after == "confirmed"


def test_backup_returns_error_when_no_attachments(mod, conn):
    obs_id = _seed_obs(conn)
    # Single text/plain part — no attachment
    raw = (
        b"From: me\r\nTo: you\r\nSubject: hi\r\n"
        b"Content-Type: text/plain\r\n\r\nbody")

    with patch("src.core.db.get_db") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = conn
        mock_get_db.return_value.__exit__.return_value = False
        with patch("src.core.gmail_api.is_configured", return_value=True), \
             patch("src.core.gmail_api.get_service", return_value=MagicMock()), \
             patch("src.core.gmail_api.get_raw_message", return_value=raw):
            result = mod.backup_observation(obs_id)

    assert not result["ok"]
    assert "no attachments" in result["error"]


# ─── helpers for tests ───────────────────────────────────────────────


def _build_raw(filenames):
    """Build a multipart/mixed message with one attachment per name."""
    boundary = "BOUND"
    parts = [
        f"From: me@example.com\r\n"
        f"To: buyer@example.com\r\n"
        f"Subject: test\r\n"
        f"Date: Tue, 06 May 2026 15:30:00 -0700\r\n"
        f"Content-Type: multipart/mixed; boundary={boundary}\r\n"
        f"\r\n"
    ]
    for name in filenames:
        parts.append(f"--{boundary}\r\n")
        parts.append(
            f'Content-Type: application/pdf; name="{name}"\r\n'
            f'Content-Disposition: attachment; filename="{name}"\r\n'
            f"Content-Transfer-Encoding: base64\r\n"
            f"\r\n"
            f"JVBERi0xLjQK\r\n"  # %PDF-1.4
        )
    parts.append(f"--{boundary}--\r\n")
    return "".join(parts).encode()
