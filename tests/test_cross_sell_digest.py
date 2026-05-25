"""Cross-sell weekly digest — build + send.

Mike P0 2026-05-11 needle-mover #2 Phase 2b. PR #901 shipped the intel
module + API; this companion module builds an HTML + plain digest and
sends via notify_agent. Scheduler fires Mondays 8am PT (matches
oracle_weekly_report pattern).
"""
from __future__ import annotations

import sqlite3
from unittest.mock import patch, MagicMock

import pytest

from src.agents.cross_sell_digest import (
    build_digest_body,
    send_weekly_digest,
    _fmt_money,
    _fmt_buyer_label,
)


# ─── Formatters ──────────────────────────────────────────────────────────


def test_fmt_money_basic():
    assert _fmt_money(1234567) == "$1,234,567"
    assert _fmt_money(0) == "$0"
    assert _fmt_money(None) == "$0"
    assert _fmt_money("garbage") == "$0"


def test_fmt_buyer_label_prefers_name_with_email():
    out = _fmt_buyer_label({"buyer_name": "Susan Brown", "buyer_email": "susan@cdcr.ca.gov"})
    assert "Susan Brown" in out
    assert "susan@cdcr.ca.gov" in out


def test_fmt_buyer_label_dedups_when_name_equals_email():
    """When name == email (which happens when the SCPRS importer fell
    back to the email as the name), don't print it twice."""
    out = _fmt_buyer_label({
        "buyer_name": "susan@cdcr.ca.gov", "buyer_email": "susan@cdcr.ca.gov",
    })
    # Should NOT contain the email twice with the bracket form
    assert "&lt;" not in out


def test_fmt_buyer_label_empty():
    assert _fmt_buyer_label({}) == "(unknown buyer)"


# ─── build_digest_body — structure ───────────────────────────────────────


def _ensure_is_test_columns(conn):
    for table in ("scprs_po_master", "scprs_po_lines"):
        try:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN is_test INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass


@pytest.fixture
def seeded_db(temp_data_dir):
    """Seed a minimal cross-sell dataset — 3 buyers with WIN_BACK rows
    on a mix of categories so the digest has real content."""
    from src.core.db import get_db
    from datetime import datetime, timedelta
    with get_db() as conn:
        _ensure_is_test_columns(conn)
        conn.executescript(
            "DELETE FROM scprs_po_lines; DELETE FROM scprs_po_master;"
        )
        recent = (datetime.now() - timedelta(days=10)).strftime("%m/%d/%Y")
        seeds = [
            # (master, lines)
            (
                (1, "PO-A", "DEPT1", "CDCR", "alice@cdcr.ca.gov", "Alice", "Echelon"),
                [("Nitrile gloves M", "NITRILE-M", "exam_gloves", 50, 100, 5000)],
            ),
            (
                (2, "PO-B", "DEPT1", "CDCR", "bob@cdcr.ca.gov", "Bob", "McKesson"),
                [("N95 respirator", "N95-3M8210", "respiratory", 100, 10, 1000)],
            ),
            (
                (3, "PO-C", "DEPT2", "CalVet", "carol@calvet.ca.gov", "Carol", "Cardinal"),
                [("Nitrile gloves L", "NITRILE-M", "exam_gloves", 100, 50, 5000)],
            ),
        ]
        for m, lines in seeds:
            pid, po_num, dc, dn, em, nm, sup = m
            conn.execute(
                "INSERT INTO scprs_po_master "
                "(id, po_number, dept_code, dept_name, buyer_email, buyer_name, "
                " supplier, start_date, is_test) "
                "VALUES (?,?,?,?,?,?,?,?,0)",
                (pid, po_num, dc, dn, em, nm, sup, recent),
            )
            for desc, sku, cat, qty, unit_p, total in lines:
                conn.execute(
                    "INSERT INTO scprs_po_lines "
                    "(po_id, po_number, line_num, description, reytech_sku, "
                    " category, quantity, unit_price, line_total, reytech_sells, "
                    " opportunity_flag, is_test) "
                    "VALUES (?,?,1,?,?,?,?,?,?,1,'WIN_BACK',0)",
                    (pid, po_num, desc, sku, cat, qty, unit_p, total),
                )
        conn.commit()


def test_digest_body_returns_plain_and_html(seeded_db):
    out = build_digest_body(window_days=365, top_n=10)
    assert out["ok"] is True
    assert "plain" in out and "html" in out
    assert out["prospect_count"] == 3
    # Phase 2c-1 reframe: digest header now reads "Distribution-list
    # candidates" instead of "Cross-sell weekly digest" — the verb is
    # registration, not cold outreach.
    assert "Distribution-list candidates" in out["plain"]
    assert "<table" in out["html"]
    assert "<h2" in out["html"]


def test_digest_html_renames_table_header_to_distro_list(seeded_db):
    """The buyer table header in the HTML must reflect the new framing."""
    out = build_digest_body(window_days=365, top_n=10)
    assert "Distribution-list candidates</h3>" in out["html"]
    # The old "Top prospects" framing should be gone from the table heading.
    assert "Top prospects</h3>" not in out["html"]


def test_digest_html_includes_freshness_column(seeded_db):
    """Each candidate row should carry a freshness badge (FRESH/warm/etc.)."""
    out = build_digest_body(window_days=365, top_n=10)
    # The 3 seeded buyers all have recent=10d-ago POs → FRESH tier.
    assert "FRESH" in out["html"]
    # Header column should be present.
    assert "Freshness</th>" in out["html"]


def test_digest_html_includes_agency_column(seeded_db):
    """Mike's pivot needs the agency on each row so he can pick which
    procurement portal to register on."""
    out = build_digest_body(window_days=365, top_n=10)
    assert "Agency</th>" in out["html"]
    assert "CDCR" in out["html"]
    assert "CalVet" in out["html"]


def test_digest_intro_states_registration_goal(seeded_db):
    """The intro paragraph must frame the goal as 'get on the distribution
    list', not 'cold outreach'."""
    out = build_digest_body(window_days=365, top_n=10)
    html_lc = out["html"].lower()
    assert "distribution list" in html_lc
    # Cold-outreach framing should NOT lead the digest.
    assert "send outreach this week" not in html_lc


def test_digest_body_includes_top_prospects(seeded_db):
    out = build_digest_body(window_days=365, top_n=10)
    # All 3 seeded buyers should appear in the body
    for buyer in ("alice@cdcr.ca.gov", "bob@cdcr.ca.gov", "carol@calvet.ca.gov"):
        # Email or name should be in either plain or html
        assert (buyer in out["plain"] or buyer in out["html"] or
                buyer.split("@")[0] in out["html"]), (
            f"buyer {buyer} missing from digest body"
        )


def test_digest_body_includes_recommendations_section(seeded_db):
    out = build_digest_body(window_days=365)
    assert "Recommendations" in out["html"]


def test_digest_body_includes_categories_section(seeded_db):
    out = build_digest_body(window_days=365)
    assert "Top categories" in out["html"]
    assert "NITRILE-M" in out["html"]


def test_digest_body_empty_db_returns_ok_with_zero_count(temp_data_dir):
    from src.core.db import get_db
    with get_db() as conn:
        _ensure_is_test_columns(conn)
        conn.executescript(
            "DELETE FROM scprs_po_lines; DELETE FROM scprs_po_master;"
        )
        conn.commit()
    out = build_digest_body(window_days=90)
    assert out["ok"] is True
    assert out["prospect_count"] == 0


# ─── send_weekly_digest ──────────────────────────────────────────────────


def test_send_skips_when_no_prospects(temp_data_dir):
    """Don't email Mike an empty digest — nothing to act on."""
    from src.core.db import get_db
    with get_db() as conn:
        _ensure_is_test_columns(conn)
        conn.executescript(
            "DELETE FROM scprs_po_lines; DELETE FROM scprs_po_master;"
        )
        conn.commit()
    with patch("src.agents.notify_agent.send_alert") as mock_send:
        out = send_weekly_digest()
    assert out["ok"] is True
    assert out.get("skipped") == "no_prospects"
    mock_send.assert_not_called()


def test_send_calls_notify_agent_with_cross_sell_event(seeded_db):
    """Real prospects → send_alert with event_type='cross_sell_weekly'.

    2026-05-25: The explicit channels=["email"] kwarg was removed so
    routing follows CHANNEL_MAP["cross_sell_weekly"] = ["telegram",
    "bell"]. The contract this test pins is the EVENT TYPE + cooldown
    key (which together drive the routing), not a hardcoded channel
    list — that's now notify_agent's responsibility.
    """
    mock_result = {"ok": True, "results": {"telegram": {"ok": True}}}
    with patch("src.agents.notify_agent.send_alert",
               return_value=mock_result) as mock_send:
        out = send_weekly_digest(window_days=365)
    assert out["ok"] is True
    mock_send.assert_called_once()
    kwargs = mock_send.call_args.kwargs
    # The caller MUST NOT pin a channel list — routing is centralized
    # in notify_agent.CHANNEL_MAP so a single edit there moves every
    # report-tier digest at once.
    assert "channels" not in kwargs or kwargs["channels"] is None, (
        "regression: cross_sell_digest re-added channels=[...] kwarg, "
        "bypassing CHANNEL_MAP — the 2026-05-25 fix relied on this "
        "fallback to route reports to Telegram"
    )
    assert kwargs.get("event_type") == "cross_sell_weekly"
    assert kwargs.get("cooldown_key") == "cross_sell_weekly"
    assert "html_body" in kwargs.get("context", {})


def test_send_marks_failure_when_notify_returns_not_ok(seeded_db):
    """If notify_agent reports ok=False, send_weekly_digest propagates."""
    mock_result = {"ok": False, "error": "SMTP refused"}
    with patch("src.agents.notify_agent.send_alert", return_value=mock_result):
        out = send_weekly_digest(window_days=365)
    assert out["ok"] is False


def test_send_handles_notify_exception(seeded_db):
    """If notify_agent throws, return ok=False rather than crashing."""
    with patch("src.agents.notify_agent.send_alert",
               side_effect=RuntimeError("simulated")):
        out = send_weekly_digest(window_days=365)
    assert out["ok"] is False
    assert "send failed" in out["error"]
