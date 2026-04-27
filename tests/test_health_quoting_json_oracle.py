"""Locks the `oracle_calibration` field shape on the JSON variant of
/health/quoting (the endpoint downstream monitors scrape).

The page route has unit coverage in test_oracle_health_card.py. This
file covers the HTTP contract: if someone renames a field, drops a key,
or breaks auth, external dashboards silently break — tests here catch
that before merge.

Scope is deliberately narrow: one happy-path shape assertion + one
populated-data shape assertion. Full status-logic coverage already
lives in the unit tests.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta


REQUIRED_KEYS = {
    "status", "rows", "wins", "losses_price", "losses_other",
    "losses_total", "win_rate_pct", "agencies", "is_stale",
    "days_since_update", "last_updated",
}


def test_api_health_quoting_exposes_oracle_calibration_shape(auth_client):
    """Default empty DB: endpoint must return ok=True and include the
    full oracle_calibration block with status='no_data'. External
    monitors key off the `status` and `rows` fields."""
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True

    cal = data.get("oracle_calibration")
    assert cal is not None, (
        "oracle_calibration missing from JSON payload — downstream "
        "monitors will break. Check routes_health.quoting_health_json()."
    )
    missing = REQUIRED_KEYS - set(cal.keys())
    assert not missing, f"oracle_calibration missing keys: {missing}"

    # Empty DB → no_data status, None win_rate (not 0.0 — the distinction
    # lets monitors show '--' vs '0%').
    assert cal["status"] == "no_data"
    assert cal["rows"] == 0
    assert cal["win_rate_pct"] is None


def test_api_health_quoting_reports_populated_oracle_stats(auth_client, temp_data_dir):
    """With rows in oracle_calibration, the JSON payload reflects real
    aggregates. Guards against silent regressions where _build_oracle_
    calibration_card() stops aggregating (e.g., SQL error swallowed)."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    fresh = (datetime.now() - timedelta(hours=6)).isoformat()
    conn.execute("""
        INSERT INTO oracle_calibration
            (category, agency, sample_size, win_count,
             loss_on_price, loss_on_other, last_updated)
        VALUES (?,?,?,?,?,?,?)
    """, ("medical", "CDCR", 18, 5, 10, 3, fresh))
    conn.commit()
    conn.close()

    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    cal = resp.get_json()["oracle_calibration"]

    assert cal["status"] == "healthy"
    assert cal["rows"] == 1
    assert cal["wins"] == 5
    assert cal["losses_price"] == 10
    assert cal["losses_other"] == 3
    assert cal["losses_total"] == 13
    # 5 / (5 + 13) = 27.78%
    assert cal["win_rate_pct"] == 27.8
    assert cal["is_stale"] is False


# ── email_poll card on the same JSON payload (Plan §4.3) ───────────────


EMAIL_POLL_KEYS = {
    "status", "running", "paused", "last_check_at",
    "lag_seconds", "lag_human", "error", "emails_found_lifetime",
}


def test_api_health_quoting_exposes_email_poll_shape(auth_client):
    """Plan §4.3: the email_poll card must be present on the JSON payload
    so external monitors can alert on poller lag without scraping HTML."""
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True

    ep = data.get("email_poll")
    assert ep is not None, (
        "email_poll missing from JSON payload — downstream monitors "
        "will break. Check routes_health.quoting_health_json()."
    )
    missing = EMAIL_POLL_KEYS - set(ep.keys())
    assert not missing, f"email_poll missing keys: {missing}"

    assert ep["status"] in (
        "healthy", "warn", "stale", "paused", "error", "unknown"
    )


def test_health_quoting_html_renders_email_poll_card(auth_client):
    """Plan §4.3: the HTML page renders the new card without UndefinedError
    AND the user-facing 'Email poller' string appears. This is the closest
    we get to Chrome verify without Chrome MCP — it proves the Jinja
    template binding doesn't crash on the new fields and the title is
    visible. Chrome MCP follow-up captures the visual render post-deploy."""
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    # Card title must be present so the operator sees it in the page
    assert "Email poller" in body, (
        "/health/quoting HTML missing the 'Email poller' card — "
        "template render swallowed an error or the card block "
        "didn't merge. Inspect quoting_health.html around the "
        "{% set _ep = email_poll %} block."
    )
    # The lag stat must be visible (defaulted to '—' when no data)
    assert "Last successful poll" in body


# ── gmail_send card on the same JSON payload (Plan §4.3 sub-2) ──────────


GMAIL_SEND_KEYS = {
    "status", "last_send_at", "lag_seconds", "lag_human",
    "sent_24h", "sent_7d", "failed_24h", "pending_drafts", "last_error",
}


def test_api_health_quoting_exposes_gmail_send_shape(auth_client):
    """Plan §4.3 sub-2: the gmail_send card must be present on the JSON
    payload so the same downstream monitors that already alert on poller
    lag can alert on send-side outages too."""
    resp = auth_client.get("/api/health/quoting?days=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True

    gs = data.get("gmail_send")
    assert gs is not None, (
        "gmail_send missing from JSON payload — downstream monitors "
        "will break. Check routes_health.quoting_health_json()."
    )
    missing = GMAIL_SEND_KEYS - set(gs.keys())
    assert not missing, f"gmail_send missing keys: {missing}"

    assert gs["status"] in (
        "healthy", "warn", "stale", "error", "unknown"
    )


def test_health_quoting_html_renders_gmail_send_card(auth_client):
    """Plan §4.3 sub-2: the HTML page renders the gmail_send card without
    UndefinedError AND the 'Gmail send' string appears so the operator
    sees the card title. Chrome MCP follow-up captures visual render
    post-deploy."""
    resp = auth_client.get("/health/quoting")
    assert resp.status_code == 200, resp.data[:500]
    body = resp.data.decode("utf-8", errors="replace")
    assert "Gmail send" in body, (
        "/health/quoting HTML missing the 'Gmail send' card — "
        "template render swallowed an error or the card block "
        "didn't merge. Inspect quoting_health.html around the "
        "{% set _gs = gmail_send %} block."
    )
    assert "Last successful send" in body
