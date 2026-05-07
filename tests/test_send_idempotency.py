"""Tier 2c — send-idempotency on api_resend_package (audit 2026-05-07).

The audit named `/api/rfq/<rid>/resend-package` as the surface where
double-clicks could send the same PDF twice. This test file pins:

  1. `recently_delivered()` helper in `src/core/dal.py` — exact-match
     dedup on (manifest_id, recipient_email, package_hash) within an
     N-second window. Fail-open on bad inputs / DB error.

  2. `api_resend_package` route — second call within 60s skips the
     Gmail API entirely and returns `{"ok": True, "deduped": True}`.
     Distinct recipients / regenerated packages still go through.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import pytest


# ── recently_delivered() helper ────────────────────────────────────

def _seed_delivery(manifest_id, rfq_id, recipient_email, package_hash,
                   delivered_at=None):
    """Insert a row into package_delivery directly."""
    from src.core.db import get_db
    delivered_at = delivered_at or datetime.now().isoformat()
    with get_db() as conn:
        conn.execute("""
            INSERT INTO package_delivery
            (manifest_id, rfq_id, delivered_at, recipient_email,
             recipient_name, email_subject, email_log_id, package_hash)
            VALUES (?,?,?,?,?,?,?,?)
        """, (manifest_id, rfq_id, delivered_at, recipient_email,
              "", "", None, package_hash))


def test_recently_delivered_returns_false_when_no_match():
    from src.core.dal import recently_delivered
    assert recently_delivered(99, "buyer@example.com", "abc123") is False


def test_recently_delivered_returns_true_for_recent_exact_match():
    from src.core.dal import recently_delivered
    _seed_delivery(101, "rfq_a", "buyer@example.com", "hash_abc")
    assert recently_delivered(101, "buyer@example.com", "hash_abc") is True


def test_recently_delivered_returns_false_outside_window():
    from src.core.dal import recently_delivered
    old = (datetime.now() - timedelta(seconds=120)).isoformat()
    _seed_delivery(102, "rfq_b", "buyer@example.com", "hash_old",
                   delivered_at=old)
    # Default window 60s → 120s old should NOT match
    assert recently_delivered(102, "buyer@example.com", "hash_old") is False


def test_recently_delivered_respects_custom_window():
    from src.core.dal import recently_delivered
    old = (datetime.now() - timedelta(seconds=120)).isoformat()
    _seed_delivery(103, "rfq_c", "buyer@example.com", "hash_x",
                   delivered_at=old)
    # Widen window to 200s → should match
    assert recently_delivered(103, "buyer@example.com", "hash_x",
                              window_seconds=200) is True


def test_recently_delivered_distinguishes_recipient():
    from src.core.dal import recently_delivered
    _seed_delivery(104, "rfq_d", "first@example.com", "hash_q")
    assert recently_delivered(104, "first@example.com", "hash_q") is True
    assert recently_delivered(104, "second@example.com", "hash_q") is False


def test_recently_delivered_distinguishes_hash():
    """A regenerated package (different bytes) is allowed through."""
    from src.core.dal import recently_delivered
    _seed_delivery(105, "rfq_e", "buyer@example.com", "hash_v1")
    assert recently_delivered(105, "buyer@example.com", "hash_v1") is True
    assert recently_delivered(105, "buyer@example.com", "hash_v2") is False


def test_recently_delivered_distinguishes_manifest():
    """A different RFQ's package is independent of the dedup window."""
    from src.core.dal import recently_delivered
    _seed_delivery(106, "rfq_f", "buyer@example.com", "hash_y")
    assert recently_delivered(106, "buyer@example.com", "hash_y") is True
    assert recently_delivered(107, "buyer@example.com", "hash_y") is False


def test_recently_delivered_fails_open_on_empty_args():
    """Empty manifest/email/hash → False, never True."""
    from src.core.dal import recently_delivered
    assert recently_delivered(None, "x", "y") is False
    assert recently_delivered(0, "x", "y") is False
    assert recently_delivered(1, "", "y") is False
    assert recently_delivered(1, "x", "") is False


def test_recently_delivered_fails_open_on_db_error():
    """A DB exception must NOT block sends — fail open with False."""
    from src.core import dal
    with patch("src.core.dal.get_db", side_effect=RuntimeError("db down")):
        assert dal.recently_delivered(1, "x@y.com", "abc") is False


# ── api_resend_package route — full path with mocked Gmail ─────────

def _seed_rfq_and_manifest(seed_db_quote):
    """Helper: create an RFQ + a package_manifest row + write the package
    PDF bytes to OUTPUT_DIR so the route's disk-first branch finds them
    (avoids the lazily-created `rfq_files` table dependency)."""
    import os
    from src.core.db import get_db
    from src.core.paths import OUTPUT_DIR
    from src.api.data_layer import _save_single_rfq

    rid = "rfq_idemp_test_001"
    sol = "SOL-IDEMP-001"
    pkg_filename = f"RFQ_Package_{sol}_ReytechInc.pdf"
    rfq = {
        "id": rid,
        "solicitation_number": sol,
        "requestor_name": "Test Buyer",
        "agency": "cdcr",
        "status": "generated",
    }
    _save_single_rfq(rid, rfq)

    with get_db() as conn:
        cur = conn.execute("""
            INSERT INTO package_manifest
            (rfq_id, created_at, package_filename, agency_key,
             total_forms, total_pages, overall_status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (rid, datetime.now().isoformat(),
              pkg_filename, "cdcr",
              4, 10, "ready"))
        manifest_id = cur.lastrowid

    # Write the package PDF bytes to disk so the route's disk branch
    # (`os.path.exists(pkg_path)`) picks them up before falling through
    # to the rfq_files table lookup.
    pkg_dir = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(pkg_dir, exist_ok=True)
    pkg_path = os.path.join(pkg_dir, pkg_filename)
    with open(pkg_path, "wb") as _f:
        _f.write(b"%PDF-1.4 fake package content for test " * 4)

    return rid, manifest_id


@pytest.fixture
def fake_gmail_send():
    """Patch every gmail_api hook the route uses — no real send."""
    sent_calls = []

    def _capture_send(*args, **kwargs):
        sent_calls.append(kwargs)
        return {"id": "fake_msg_id"}

    with patch("src.core.gmail_api.is_configured", return_value=True):
        with patch("src.core.gmail_api.get_send_service",
                   return_value=MagicMock()):
            with patch("src.core.gmail_api.send_message",
                       side_effect=_capture_send) as mock_send:
                yield mock_send, sent_calls


def test_api_resend_package_first_call_sends(seed_db_quote, fake_gmail_send,
                                              auth_client):
    """First resend → Gmail send fires, response has no `deduped` flag."""
    mock_send, _calls = fake_gmail_send
    rid, _ = _seed_rfq_and_manifest(seed_db_quote)

    resp = auth_client.post(
        f"/api/rfq/{rid}/resend-package",
        json={"to": "buyer@cdcr.gov", "subject": "Test", "body": "..."},
    )
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["ok"] is True
    assert body.get("deduped") is not True
    assert mock_send.call_count == 1


def test_api_resend_package_second_call_within_window_dedupes(
        seed_db_quote, fake_gmail_send, auth_client):
    """Second resend within 60s → no Gmail call, deduped=True in response."""
    mock_send, _calls = fake_gmail_send
    rid, _ = _seed_rfq_and_manifest(seed_db_quote)

    payload = {"to": "buyer@cdcr.gov", "subject": "Test", "body": "..."}
    r1 = auth_client.post(f"/api/rfq/{rid}/resend-package", json=payload)
    r2 = auth_client.post(f"/api/rfq/{rid}/resend-package", json=payload)
    assert r1.status_code == 200
    assert r2.status_code == 200
    body2 = r2.get_json()
    assert body2["ok"] is True
    assert body2["deduped"] is True
    assert "Already sent" in body2["message"]
    # Gmail send was called exactly ONCE despite two route calls
    assert mock_send.call_count == 1


def test_api_resend_package_different_recipient_not_deduped(
        seed_db_quote, fake_gmail_send, auth_client):
    """Same package to a different recipient → both sends go through."""
    mock_send, _calls = fake_gmail_send
    rid, _ = _seed_rfq_and_manifest(seed_db_quote)

    auth_client.post(f"/api/rfq/{rid}/resend-package",
                     json={"to": "first@example.com",
                           "subject": "T", "body": "."})
    r2 = auth_client.post(f"/api/rfq/{rid}/resend-package",
                          json={"to": "second@example.com",
                                "subject": "T", "body": "."})
    body2 = r2.get_json()
    assert body2["ok"] is True
    assert body2.get("deduped") is not True
    assert mock_send.call_count == 2


def test_api_resend_package_invalid_email_still_rejected(
        seed_db_quote, fake_gmail_send, auth_client):
    """Idempotency gate doesn't bypass the existing email validator."""
    mock_send, _calls = fake_gmail_send
    rid, _ = _seed_rfq_and_manifest(seed_db_quote)

    resp = auth_client.post(
        f"/api/rfq/{rid}/resend-package",
        json={"to": "not-an-email", "subject": "T", "body": "."},
    )
    assert resp.status_code == 400
    assert mock_send.call_count == 0
