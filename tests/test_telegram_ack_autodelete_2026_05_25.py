"""Telegram ack + 24h auto-delete substrate.

Mike's 2026-05-25 directive: "The messages should go through UI
enhancements and should be auto deleted 24hrs after 'read'. If I want
to save any, I can 'unread'."

Pins the substrate behaviors:
  1. Every bot-sent Telegram message includes an inline [✓ Got it] button.
  2. _send_telegram records a row in telegram_messages with sent_at.
  3. Webhook handler `tg_ack`: sets acked_at + expires_at (trimmed),
     flips keyboard to [↩️ Keep it], answers callback with a toast.
  4. Webhook handler `tg_unack`: clears acked_at + expires_at, flips
     keyboard back to [✓ Got it].
  5. expires_at trim: min(acked_at + 24h, sent_at + 47h). The 1h
     buffer below Telegram's 48h hard wall absorbs cron jitter.
  6. Cleanup sweep deletes messages whose expires_at is past + marks
     deleted_at. Telegram-rejected deletes still mark deleted_at so we
     stop retrying past the 48h window.
  7. Webhook auth: missing/wrong X-Telegram-Bot-Api-Secret-Token → 401.
  8. Webhook auth: callback from non-allowed chat → no DB mutation.
"""
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from unittest.mock import patch

import pytest


# ── Keyboard shape ────────────────────────────────────────────────────────


def test_ack_keyboard_unread_has_got_it_button():
    from src.agents.notify_agent import _ack_keyboard
    kb = _ack_keyboard("unread")
    assert kb["inline_keyboard"][0][0]["text"] == "✓ Got it"
    assert kb["inline_keyboard"][0][0]["callback_data"] == "tg_ack"


def test_ack_keyboard_acked_has_keep_it_button():
    from src.agents.notify_agent import _ack_keyboard
    kb = _ack_keyboard("acked")
    assert kb["inline_keyboard"][0][0]["text"] == "↩️ Keep it"
    assert kb["inline_keyboard"][0][0]["callback_data"] == "tg_unack"


# ── _send_telegram attaches keyboard + writes row ─────────────────────────


def test_send_telegram_attaches_inline_keyboard(monkeypatch, tmp_path):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")

    import importlib
    import src.agents.notify_agent as na
    importlib.reload(na)

    captured = {}

    class _Resp:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *a): return None
        def read(self):
            return b'{"ok":true,"result":{"message_id":42}}'

    def _fake_urlopen(req, timeout=None):
        captured["data"] = req.data.decode("utf-8")
        return _Resp()

    with patch("urllib.request.urlopen", side_effect=_fake_urlopen), \
         patch("src.agents.notify_agent._record_telegram_send",
               return_value=None):
        result = na._send_telegram(
            event_type="oracle_weekly",
            title="t", body="b", urgency="info", context={},
        )

    assert result["ok"] is True
    assert result["message_id"] == 42
    # The payload must include reply_markup with the [✓ Got it] button
    assert "reply_markup" in captured["data"]
    # urlencoded JSON of the keyboard — check the button text encoded.
    # "Got+it" or "Got%20it" depending on quoting; "Got" alone is enough.
    assert "Got" in captured["data"]


# ── expires_at trim ───────────────────────────────────────────────────────


def test_expires_at_normal_case_is_ack_plus_24h():
    """A message acked shortly after send → expires_at = acked + 24h
    (the natural 24h window applies, not the hard cap)."""
    from src.api.modules.routes_telegram import _compute_expires_at
    sent = datetime(2026, 5, 25, 12, 0, tzinfo=timezone.utc)
    acked = sent + timedelta(minutes=10)
    expires_iso = _compute_expires_at(sent.isoformat(), acked)
    expires = datetime.fromisoformat(expires_iso)
    delta = (expires - acked).total_seconds() / 3600
    assert 23.9 < delta < 24.1, (
        f"normal-case expires_at should be acked+24h; got Δ={delta}h"
    )


def test_expires_at_trims_to_47h_after_send_when_acked_late():
    """A message acked 30h after send → natural would be ack+24h=54h
    after send, BUT Telegram's deleteMessage hard wall is 48h. We trim
    to 47h after send to leave a 1h cron-jitter buffer."""
    from src.api.modules.routes_telegram import _compute_expires_at
    sent = datetime(2026, 5, 25, 0, 0, tzinfo=timezone.utc)
    acked = sent + timedelta(hours=30)
    expires_iso = _compute_expires_at(sent.isoformat(), acked)
    expires = datetime.fromisoformat(expires_iso)
    after_send_h = (expires - sent).total_seconds() / 3600
    assert 46.9 < after_send_h < 47.1, (
        f"late-ack expires_at should trim to sent+47h; got {after_send_h}h"
    )


def test_expires_at_past_for_very_late_ack():
    """A message acked >47h after send → expires_at is in the past.
    Cleanup sweep tries once, Telegram rejects (past 48h window),
    delete_error logged, deleted_at set — stop retrying."""
    from src.api.modules.routes_telegram import _compute_expires_at
    sent = datetime(2026, 5, 25, 0, 0, tzinfo=timezone.utc)
    acked = sent + timedelta(hours=50)  # already past 47h cap
    expires_iso = _compute_expires_at(sent.isoformat(), acked)
    expires = datetime.fromisoformat(expires_iso)
    assert expires < acked, (
        f"50h-late ack should produce expires_at < acked_at "
        f"(immediate cleanup attempt); got expires={expires}, acked={acked}"
    )


# ── Webhook callback handler ──────────────────────────────────────────────


@pytest.fixture
def tg_db(monkeypatch):
    """Real sqlite3 connection backing the telegram_messages writes."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE telegram_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER NOT NULL,
            chat_id TEXT NOT NULL,
            event_type TEXT,
            title TEXT,
            sent_at TEXT NOT NULL,
            acked_at TEXT,
            expires_at TEXT,
            deleted_at TEXT,
            delete_error TEXT,
            UNIQUE(message_id, chat_id)
        );
    """)
    conn.commit()
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")

    class _Ctx:
        def __enter__(_self): return conn
        def __exit__(_self, *a): return None

    with patch("src.core.db.get_db", return_value=_Ctx()):
        yield conn
    conn.close()


def test_callback_ack_sets_acked_at_and_expires_at(tg_db):
    """tg_ack callback → DB row's acked_at + expires_at populated."""
    sent_at = datetime.now(timezone.utc).isoformat()
    tg_db.execute(
        "INSERT INTO telegram_messages (message_id, chat_id, sent_at) "
        "VALUES (?, ?, ?)",
        (100, "999", sent_at),
    )
    tg_db.commit()

    from src.api.modules.routes_telegram import _handle_callback

    api_calls = []
    with patch("src.api.modules.routes_telegram._telegram_api",
               side_effect=lambda m, p: api_calls.append((m, p)) or {"ok": True}):
        result = _handle_callback({
            "id": "cb_id_1",
            "data": "tg_ack",
            "from": {"id": 999},
            "message": {
                "message_id": 100,
                "chat": {"id": 999},
            },
        })

    assert result["ok"] is True
    row = tg_db.execute(
        "SELECT acked_at, expires_at FROM telegram_messages WHERE message_id=100"
    ).fetchone()
    assert row["acked_at"] is not None
    assert row["expires_at"] is not None

    # Keyboard was edited to "Keep it"
    edits = [c for c in api_calls if c[0] == "editMessageReplyMarkup"]
    assert len(edits) == 1, f"expected 1 keyboard edit, got: {api_calls}"
    keep_button_text = edits[0][1]["reply_markup"]["inline_keyboard"][0][0]["text"]
    assert keep_button_text == "↩️ Keep it"

    # User got a toast
    answers = [c for c in api_calls if c[0] == "answerCallbackQuery"]
    assert len(answers) == 1
    assert "auto-delete" in answers[0][1]["text"].lower()


def test_callback_unack_clears_state(tg_db):
    """tg_unack → DB row's acked_at + expires_at cleared, keyboard
    reverts to [✓ Got it]."""
    sent_at = datetime.now(timezone.utc).isoformat()
    acked_at = datetime.now(timezone.utc).isoformat()
    tg_db.execute(
        "INSERT INTO telegram_messages (message_id, chat_id, sent_at, "
        "acked_at, expires_at) VALUES (?, ?, ?, ?, ?)",
        (101, "999", sent_at, acked_at, acked_at),
    )
    tg_db.commit()

    from src.api.modules.routes_telegram import _handle_callback

    api_calls = []
    with patch("src.api.modules.routes_telegram._telegram_api",
               side_effect=lambda m, p: api_calls.append((m, p)) or {"ok": True}):
        result = _handle_callback({
            "id": "cb_id_2",
            "data": "tg_unack",
            "from": {"id": 999},
            "message": {"message_id": 101, "chat": {"id": 999}},
        })

    assert result["ok"] is True
    row = tg_db.execute(
        "SELECT acked_at, expires_at FROM telegram_messages WHERE message_id=101"
    ).fetchone()
    assert row["acked_at"] is None
    assert row["expires_at"] is None


def test_callback_rejects_foreign_caller(tg_db, monkeypatch):
    """A callback from a chat_id that isn't TELEGRAM_CHAT_ID must not
    mutate the DB."""
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "999")
    sent_at = datetime.now(timezone.utc).isoformat()
    tg_db.execute(
        "INSERT INTO telegram_messages (message_id, chat_id, sent_at) "
        "VALUES (?, ?, ?)",
        (102, "999", sent_at),
    )
    tg_db.commit()

    from src.api.modules.routes_telegram import _handle_callback

    api_calls = []
    with patch("src.api.modules.routes_telegram._telegram_api",
               side_effect=lambda m, p: api_calls.append((m, p)) or {"ok": True}):
        result = _handle_callback({
            "id": "cb_id_3",
            "data": "tg_ack",
            "from": {"id": 666},  # not allowed
            "message": {"message_id": 102, "chat": {"id": 666}},
        })

    assert result["ok"] is False
    row = tg_db.execute(
        "SELECT acked_at FROM telegram_messages WHERE message_id=102"
    ).fetchone()
    assert row["acked_at"] is None, "foreign caller mutated DB"


def test_unknown_callback_data_ignored(tg_db):
    """callback_data not matching tg_ack/tg_unack → no DB mutation,
    answerCallbackQuery still fires so Telegram doesn't show a
    permanent loading spinner."""
    from src.api.modules.routes_telegram import _handle_callback

    api_calls = []
    with patch("src.api.modules.routes_telegram._telegram_api",
               side_effect=lambda m, p: api_calls.append((m, p)) or {"ok": True}):
        result = _handle_callback({
            "id": "cb_id_4",
            "data": "evil:payload",
            "from": {"id": 999},
            "message": {"message_id": 103, "chat": {"id": 999}},
        })
    assert result["ok"] is False
    assert any(c[0] == "answerCallbackQuery" for c in api_calls)


# ── Cleanup sweep ─────────────────────────────────────────────────────────


def test_cleanup_sweep_deletes_expired_messages(tg_db):
    """A row with expires_at in the past → deleteMessage POSTed,
    deleted_at populated. Idempotent — re-running skips."""
    now = datetime.now(timezone.utc)
    past = (now - timedelta(minutes=5)).isoformat()
    future = (now + timedelta(hours=23)).isoformat()
    sent_at = (now - timedelta(hours=2)).isoformat()
    acked_at = (now - timedelta(hours=1)).isoformat()
    # Expired row → should be deleted
    tg_db.execute(
        "INSERT INTO telegram_messages (message_id, chat_id, sent_at, "
        "acked_at, expires_at) VALUES (?, ?, ?, ?, ?)",
        (200, "999", sent_at, acked_at, past),
    )
    # Future row → should be skipped
    tg_db.execute(
        "INSERT INTO telegram_messages (message_id, chat_id, sent_at, "
        "acked_at, expires_at) VALUES (?, ?, ?, ?, ?)",
        (201, "999", sent_at, acked_at, future),
    )
    # Already-deleted row → should be skipped
    tg_db.execute(
        "INSERT INTO telegram_messages (message_id, chat_id, sent_at, "
        "acked_at, expires_at, deleted_at) VALUES (?, ?, ?, ?, ?, ?)",
        (202, "999", sent_at, acked_at, past, now.isoformat()),
    )
    tg_db.commit()

    from src.api.modules.routes_telegram import run_telegram_cleanup_sweep

    deletes = []
    def _fake_api(method, payload):
        deletes.append((method, payload))
        return {"ok": True}

    with patch("src.api.modules.routes_telegram._telegram_api",
               side_effect=_fake_api):
        summary = run_telegram_cleanup_sweep()

    # Only the expired+undeleted row gets a deleteMessage call
    assert summary["checked"] == 1
    assert summary["deleted"] == 1
    assert summary["failures"] == 0
    assert any(d[0] == "deleteMessage" and d[1]["message_id"] == 200
               for d in deletes)
    # And the DB row is now marked deleted
    row = tg_db.execute(
        "SELECT deleted_at FROM telegram_messages WHERE message_id=200"
    ).fetchone()
    assert row["deleted_at"] is not None


def test_cleanup_sweep_marks_telegram_rejection_as_final(tg_db):
    """If Telegram rejects deleteMessage (e.g., past 48h window), we
    still mark deleted_at + log delete_error so the row never retries.
    Stop the bleeding."""
    now = datetime.now(timezone.utc)
    past = (now - timedelta(minutes=5)).isoformat()
    sent_at = (now - timedelta(hours=50)).isoformat()  # very old
    tg_db.execute(
        "INSERT INTO telegram_messages (message_id, chat_id, sent_at, "
        "acked_at, expires_at) VALUES (?, ?, ?, ?, ?)",
        (300, "999", sent_at, past, past),
    )
    tg_db.commit()

    from src.api.modules.routes_telegram import run_telegram_cleanup_sweep

    def _fake_api(method, payload):
        return {"ok": False, "description": "Bad Request: message can't be deleted"}

    with patch("src.api.modules.routes_telegram._telegram_api",
               side_effect=_fake_api):
        summary = run_telegram_cleanup_sweep()

    assert summary["failures"] == 1
    row = tg_db.execute(
        "SELECT deleted_at, delete_error FROM telegram_messages WHERE message_id=300"
    ).fetchone()
    assert row["deleted_at"] is not None, (
        "Telegram-rejected row must still be marked deleted_at so the "
        "cleanup cron stops retrying"
    )
    assert "can't be deleted" in (row["delete_error"] or "")


# ── setWebhook registration ───────────────────────────────────────────────


def test_ensure_webhook_skips_when_creds_missing(monkeypatch):
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    from src.api.modules.routes_telegram import ensure_telegram_webhook_registered
    result = ensure_telegram_webhook_registered()
    assert result["ok"] is False
    assert "TELEGRAM_BOT_TOKEN" in result["skipped"]


def test_ensure_webhook_registers_with_secret_and_callback_filter(monkeypatch):
    """setWebhook payload must include secret_token + restrict
    allowed_updates to callback_query (no inbound message flood)."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "T")
    monkeypatch.setenv("TELEGRAM_WEBHOOK_SECRET", "S")
    monkeypatch.setenv("PUBLIC_URL", "https://example.com")

    from src.api.modules import routes_telegram

    captured = {}
    def _fake_api(method, payload):
        captured["method"] = method
        captured["payload"] = payload
        return {"ok": True}

    with patch.object(routes_telegram, "_telegram_api",
                      side_effect=_fake_api):
        result = routes_telegram.ensure_telegram_webhook_registered()

    assert result["ok"] is True
    assert captured["method"] == "setWebhook"
    assert captured["payload"]["secret_token"] == "S"
    assert captured["payload"]["url"] == "https://example.com/telegram/webhook"
    assert captured["payload"]["allowed_updates"] == ["callback_query"]
