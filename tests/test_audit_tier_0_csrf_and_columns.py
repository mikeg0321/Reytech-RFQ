"""Audit Tier-0 fixes (2026-05-07).

Two unrelated P0s shipped together because each is small:

* **Tier 0a** — QuickBooks OAuth2 callback was CSRF-able. `state=reytech` was
  hardcoded; nothing validated the value on callback. Anyone with a valid
  Intuit auth code could redirect Mike's browser to our callback and rebind
  the realm to their books. Fix: per-session random state + validation.

* **Tier 0b** — `_save_single_rfq` / `save_rfqs` / `_save_single_pc` /
  `_save_price_checks` were missing 7-8 columns from their INSERT lists
  (`email_thread_id`, `email_message_id`, `original_sender`,
  `gmail_draft_id`, `gmail_message_ids`, `gmail_thread_duplicate_of`,
  `requirements_json`, plus PC-only `bundle_id`). The columns existed in
  schema (added by `_migrate_columns`), and the data was being written
  to the `data_json` blob, but every SQL-side query (`WHERE
  email_thread_id=?`, JOIN on thread, observed-send link by
  message-id) silently saw empty strings on freshly-saved records.
  The thread-aware-ingest substrate (PRs #808-#821) was relying on
  backfill scripts to populate these. Fix: write them at save time.

These tests pin both fixes so a future refactor cannot silently undo them.
"""
from __future__ import annotations

import json
import sqlite3

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Tier 0a — QuickBooks OAuth state CSRF
# ─────────────────────────────────────────────────────────────────────────────


def _qb_state_in_session(client_obj):
    """Helper: pull `qb_oauth_state` out of the test client's session."""
    with client_obj._client.session_transaction() as sess:
        return sess.get("qb_oauth_state")


def _force_qb_available(monkeypatch):
    """Monkeypatch every namespace that holds the QB_AVAILABLE flag.

    Route modules are exec'd into `dashboard.py` globals (see CLAUDE.md
    "Module loading"), so the QB_AVAILABLE the route handler reads at
    runtime can come from dashboard's namespace, the module's own
    namespace, or `src.api.config`. We patch all three to be safe, plus
    stub `quickbooks_agent.QB_CLIENT_ID` so the second early-return
    doesn't fire either.
    """
    targets = [
        "src.api.modules.routes_intel_ops",
        "src.api.dashboard",
        "src.api.config",
    ]
    for tgt in targets:
        try:
            mod = __import__(tgt, fromlist=["QB_AVAILABLE"])
            monkeypatch.setattr(mod, "QB_AVAILABLE", True, raising=False)
        except ImportError:
            pass
    # The connect handler does `from src.agents.quickbooks_agent import
    # QB_CLIENT_ID, QB_SANDBOX` and bails if QB_CLIENT_ID is empty.
    try:
        from src.agents import quickbooks_agent
        monkeypatch.setattr(quickbooks_agent, "QB_CLIENT_ID",
                            "test-client-id", raising=False)
        monkeypatch.setattr(quickbooks_agent, "QB_SANDBOX", False,
                            raising=False)
    except ImportError:
        pass


def test_qb_connect_sets_random_state_in_session(client, monkeypatch):
    """`api_qb_connect` must put a fresh random state in session, not 'reytech'."""
    _force_qb_available(monkeypatch)

    resp = client.get("/api/qb/connect", follow_redirects=False)

    # Should redirect to Intuit
    assert resp.status_code in (301, 302, 303, 307, 308), resp.status_code
    assert "appcenter.intuit.com" in resp.headers.get("Location", "")
    # State must NOT be the old hardcoded value
    assert "state=reytech&" not in resp.headers["Location"]
    assert "state=reytech" not in resp.headers["Location"].split("&")[-1]
    # Session should now hold a token of meaningful length
    state = _qb_state_in_session(client)
    assert state is not None, "qb_oauth_state not stored in session"
    assert len(state) >= 24, f"state token too short ({len(state)}) — should be >=24 chars"
    assert state != "reytech"
    # Redirect URL should contain the same token
    assert f"state={state}" in resp.headers["Location"]


def test_qb_callback_rejects_missing_state(client, monkeypatch):
    """A callback with no `state` param must reject (no token swap attempted)."""
    _force_qb_available(monkeypatch)

    # No prior connect call → no session state. Hit callback directly.
    resp = client.get("/api/qb/callback?code=abc&realmId=999",
                      follow_redirects=False)

    # Must redirect (not 200 success)
    assert resp.status_code in (301, 302, 303, 307, 308)
    # Must redirect to /agents (the audit page), not /settings or success
    assert "/agents" in resp.headers.get("Location", "")


def test_qb_callback_rejects_mismatched_state(client, monkeypatch):
    """State mismatch → reject; do NOT exchange code for tokens."""
    _force_qb_available(monkeypatch)

    # Pre-seed session with a known state via the connect endpoint
    client.get("/api/qb/connect", follow_redirects=False)
    legitimate_state = _qb_state_in_session(client)
    assert legitimate_state is not None

    # Sentinel — if _save_tokens fires, we want to know.
    save_calls = []

    def _spy(*args, **kwargs):
        save_calls.append((args, kwargs))

    try:
        from src.agents import quickbooks_agent
        monkeypatch.setattr(quickbooks_agent, "_save_tokens", _spy)
    except Exception:
        pass

    # Hit callback with a DIFFERENT state value
    resp = client.get(
        "/api/qb/callback?code=evil-attacker-code&realmId=666"
        "&state=attacker-supplied-value",
        follow_redirects=False,
    )

    assert resp.status_code in (301, 302, 303, 307, 308)
    assert "/agents" in resp.headers.get("Location", "")
    # Critical: tokens must NOT have been saved.
    assert save_calls == [], (
        f"Mismatched-state callback fired _save_tokens! Calls: {save_calls}"
    )


def test_qb_callback_state_is_one_shot(client, monkeypatch):
    """A callback that consumed the state should clear it; a second mismatch fails."""
    _force_qb_available(monkeypatch)
    # Establish session state
    client.get("/api/qb/connect", follow_redirects=False)
    state = _qb_state_in_session(client)
    assert state is not None

    # First (correct) callback consumes it. Code is bogus, so it'll error
    # on the token swap — but we only care that state was popped.
    client.get(
        f"/api/qb/callback?code=bogus&realmId=1&state={state}",
        follow_redirects=False,
    )
    # Session should now have no qb_oauth_state
    assert _qb_state_in_session(client) is None, (
        "state should have been popped from session after callback"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Tier 0b — 7/8-column INSERT round-trip
# ─────────────────────────────────────────────────────────────────────────────


_RFQ_THREAD_COLS = [
    "email_thread_id",
    "email_message_id",
    "original_sender",
    "gmail_draft_id",
    "gmail_thread_duplicate_of",
]
_PC_THREAD_COLS = _RFQ_THREAD_COLS + ["bundle_id"]


def _direct_select(temp_data_dir, table, rid, cols):
    """Open the test SQLite directly and read named columns for the row."""
    import os
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    try:
        col_list = ", ".join(cols)
        row = conn.execute(
            f"SELECT {col_list} FROM {table} WHERE id = ?", (rid,)
        ).fetchone()
        return dict(zip(cols, row)) if row else None
    finally:
        conn.close()


def test_save_single_rfq_writes_thread_columns(app, temp_data_dir):
    """`_save_single_rfq` populates the 7 audit columns, not just the blob."""
    from src.api.data_layer import _save_single_rfq

    rid = "rfq_audit_tier0b_001"
    rfq = {
        "id": rid,
        "received_at": "2026-05-07T10:00:00Z",
        "agency": "cchcs",
        "institution": "ciw_rhu",
        "rfq_number": "R26Q900",
        "items": [],
        "status": "new",
        "email_uid": "uid-12345",
        "email_subject": "Test thread-aware ingest",
        "body_text": "Body",
        "due_date": "",
        "email_thread_id": "thread-abc-123",
        "email_message_id": "<msg.abc@example.test>",
        "original_sender": "buyer@example.test",
        "gmail_draft_id": "draft-xyz-789",
        "gmail_message_ids": ["<msg.abc@example.test>", "<msg.def@example.test>"],
        "gmail_thread_duplicate_of": "",
        "requirements_json": {"due_by": "2026-05-09", "notes": "rush"},
    }

    _save_single_rfq(rid, rfq, raise_on_error=True)

    cols = _RFQ_THREAD_COLS + ["gmail_message_ids", "requirements_json"]
    got = _direct_select(temp_data_dir, "rfqs", rid, cols)
    assert got is not None, f"row {rid} not in rfqs table"

    # Scalar columns: written verbatim
    assert got["email_thread_id"] == "thread-abc-123"
    assert got["email_message_id"] == "<msg.abc@example.test>"
    assert got["original_sender"] == "buyer@example.test"
    assert got["gmail_draft_id"] == "draft-xyz-789"
    assert got["gmail_thread_duplicate_of"] == ""

    # JSON-bag columns: lists/dicts get serialised
    assert isinstance(got["gmail_message_ids"], str)
    parsed_ids = json.loads(got["gmail_message_ids"])
    assert parsed_ids == ["<msg.abc@example.test>", "<msg.def@example.test>"]

    assert isinstance(got["requirements_json"], str)
    parsed_req = json.loads(got["requirements_json"])
    assert parsed_req == {"due_by": "2026-05-09", "notes": "rush"}


def test_save_rfqs_bulk_writes_thread_columns(app, temp_data_dir):
    """The bulk `save_rfqs` writer must mirror `_save_single_rfq` behaviour."""
    from src.api.data_layer import save_rfqs

    rid = "rfq_audit_tier0b_bulk_002"
    rfqs = {
        rid: {
            "id": rid,
            "received_at": "2026-05-07T11:00:00Z",
            "agency": "cdcr",
            "institution": "csp_sac",
            "rfq_number": "R26Q901",
            "items": [],
            "status": "new",
            "email_uid": "uid-bulk",
            "due_date": "",
            "email_thread_id": "thread-bulk-456",
            "email_message_id": "<bulk.abc@example.test>",
            "original_sender": "bulk@example.test",
            "gmail_message_ids": ["<bulk.abc@example.test>"],
            "gmail_thread_duplicate_of": "thread-original-xyz",
            "requirements_json": "{\"already_string\": true}",
        }
    }

    save_rfqs(rfqs, raise_on_error=True)

    got = _direct_select(
        temp_data_dir, "rfqs", rid,
        _RFQ_THREAD_COLS + ["gmail_message_ids", "requirements_json"],
    )
    assert got is not None
    assert got["email_thread_id"] == "thread-bulk-456"
    assert got["gmail_thread_duplicate_of"] == "thread-original-xyz"
    # Already-string requirements_json passes through unchanged
    assert got["requirements_json"] == "{\"already_string\": true}"


def test_save_single_pc_writes_thread_and_bundle_columns(app, temp_data_dir):
    """`_save_single_pc` populates the 8 audit columns (7 + bundle_id)."""
    from src.api.data_layer import _save_single_pc

    pc_id = "pc_audit_tier0b_003"
    pc = {
        "id": pc_id,
        "created_at": "2026-05-07T12:00:00Z",
        "requestor": "buyer@example.test",
        "agency": "cchcs",
        "institution": "ciw_rhu",
        "items": [{"description": "Widget", "qty": 1}],
        "pc_number": "PC-test-001",
        "status": "parsed",
        "email_uid": "uid-pc",
        "email_subject": "PC test",
        "due_date": "",
        "email_thread_id": "thread-pc-789",
        "email_message_id": "<pc.abc@example.test>",
        "original_sender": "buyer@example.test",
        "gmail_draft_id": "draft-pc-456",
        "gmail_message_ids": ["<pc.abc@example.test>"],
        "gmail_thread_duplicate_of": "",
        "requirements_json": {"deadline": "ASAP"},
        "bundle_id": "bundle-grouping-001",
    }

    _save_single_pc(pc_id, pc, raise_on_error=True)

    cols = _PC_THREAD_COLS + ["gmail_message_ids", "requirements_json"]
    got = _direct_select(temp_data_dir, "price_checks", pc_id, cols)
    assert got is not None, f"row {pc_id} not in price_checks table"

    assert got["email_thread_id"] == "thread-pc-789"
    assert got["email_message_id"] == "<pc.abc@example.test>"
    assert got["original_sender"] == "buyer@example.test"
    assert got["gmail_draft_id"] == "draft-pc-456"
    assert got["gmail_thread_duplicate_of"] == ""
    assert got["bundle_id"] == "bundle-grouping-001"

    parsed_ids = json.loads(got["gmail_message_ids"])
    assert parsed_ids == ["<pc.abc@example.test>"]

    parsed_req = json.loads(got["requirements_json"])
    assert parsed_req == {"deadline": "ASAP"}


def test_save_price_checks_bulk_writes_thread_and_bundle_columns(app, temp_data_dir):
    """The bulk `_save_price_checks` writer mirrors the single-PC writer."""
    from src.api.data_layer import _save_price_checks

    pc_id = "pc_audit_tier0b_bulk_004"
    pcs = {
        pc_id: {
            "id": pc_id,
            "created_at": "2026-05-07T13:00:00Z",
            "agency": "calvet",
            "institution": "calvet_yvc",
            "items": [],
            "pc_number": "PC-bulk-002",
            "status": "parsed",
            "email_uid": "uid-pc-bulk",
            "due_date": "",
            "email_thread_id": "thread-pc-bulk",
            "email_message_id": "<pc.bulk@example.test>",
            "original_sender": "bulk@example.test",
            "gmail_message_ids": ["<pc.bulk@example.test>"],
            "bundle_id": "bundle-bulk-002",
            "requirements_json": {"x": 1},
        }
    }

    _save_price_checks(pcs, raise_on_error=True)

    got = _direct_select(
        temp_data_dir, "price_checks", pc_id,
        _PC_THREAD_COLS + ["requirements_json"],
    )
    assert got is not None
    assert got["email_thread_id"] == "thread-pc-bulk"
    assert got["bundle_id"] == "bundle-bulk-002"
    assert json.loads(got["requirements_json"]) == {"x": 1}


def test_thread_columns_default_to_empty_when_record_lacks_them(app, temp_data_dir):
    """A record without thread fields must save without error and default to ''."""
    from src.api.data_layer import _save_single_rfq, _save_single_pc

    rid = "rfq_audit_tier0b_default_005"
    _save_single_rfq(rid, {
        "id": rid, "received_at": "", "agency": "", "institution": "",
        "rfq_number": "R26Q902", "items": [], "status": "new",
    }, raise_on_error=True)

    got = _direct_select(temp_data_dir, "rfqs", rid,
                         _RFQ_THREAD_COLS + ["gmail_message_ids", "requirements_json"])
    assert got is not None
    for col in _RFQ_THREAD_COLS:
        assert got[col] == "", f"{col} should default empty, got {got[col]!r}"
    # JSON bags default to "[]" and "{}" respectively (post-serialisation)
    assert got["gmail_message_ids"] in ("[]", "")
    assert got["requirements_json"] in ("{}", "")

    pc_id = "pc_audit_tier0b_default_006"
    _save_single_pc(pc_id, {
        "id": pc_id, "created_at": "", "items": [],
        "pc_number": "PC-defaults", "status": "parsed",
    }, raise_on_error=True)

    got = _direct_select(temp_data_dir, "price_checks", pc_id, _PC_THREAD_COLS)
    assert got is not None
    for col in _PC_THREAD_COLS:
        assert got[col] == "", f"{col} should default empty, got {got[col]!r}"
