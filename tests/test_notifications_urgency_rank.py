"""Pin: grouped /notifications view's urgency sort knows all 5 tiers
declared in notify_agent (urgent | warning | deal | draft | info).

Chrome MCP audit 2026-05-26 anomaly #6: `po_received` events showed up
in prod with `urgency="deal"` — a valid value per the notify_agent
docstring + send_alert signature. But the grouped view's
`_URGENCY_RANK` only knew {urgent, warning, info}, so deal + draft
events sorted to position 3 (the "unknown" default), BELOW info.
Substrate-singleness: the schema lives in notify_agent, the sort had
its own subset. This test pins the full enum so a future short-list
regression is caught.
"""
from __future__ import annotations


def test_urgency_rank_knows_all_five_canonical_tiers():
    """Mirror of notify_agent.py's urgency contract."""
    from src.api.modules.routes_notifications import _query_grouped  # noqa
    # Pull the rank table — it lives inside _query_grouped's _key
    # closure but is module-level enough to inspect by string check
    # in source (mirrors the same pattern as the cooldown emit-site
    # test in test_scprs_undercut_governance.py).
    from pathlib import Path
    src = Path(__file__).parent.parent / "src" / "api" / "modules" / "routes_notifications.py"
    content = src.read_text(encoding="utf-8")
    # The 5-tier enum must be present verbatim.
    for tier in ("urgent", "warning", "deal", "draft", "info"):
        assert f'"{tier}":' in content, (
            f"urgency tier {tier!r} missing from _URGENCY_RANK in "
            f"routes_notifications.py — the grouped view's sort will "
            f"bucket events tagged {tier!r} into 'unknown' rank and "
            f"sort them below info events. Mirror notify_agent.py."
        )


def test_deal_events_sort_above_info_in_grouped_view(auth_client):
    """End-to-end: seed deal + info events, fetch the grouped API,
    confirm deal sorts above info."""
    from datetime import datetime
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute(
            "DELETE FROM notifications "
            "WHERE event_type IN ('po_received','order_digest')"
        )
        now = datetime.utcnow().isoformat()
        conn.execute(
            "INSERT INTO notifications "
            "(created_at, event_type, urgency, title, body) "
            "VALUES (?, ?, ?, ?, ?)",
            (now, "po_received", "deal", "PO received", "x"),
        )
        conn.execute(
            "INSERT INTO notifications "
            "(created_at, event_type, urgency, title, body) "
            "VALUES (?, ?, ?, ?, ?)",
            (now, "order_digest", "info", "Daily digest", "y"),
        )

    r = auth_client.get("/api/notifications/grouped?days=1")
    assert r.status_code == 200
    groups = r.get_json()["groups"]
    types_in_order = [g["event_type"] for g in groups
                      if g["event_type"] in ("po_received", "order_digest")]
    # po_received (deal) should appear BEFORE order_digest (info)
    assert types_in_order == ["po_received", "order_digest"], (
        f"deal sorted below info — _URGENCY_RANK missing 'deal' or "
        f"with wrong relative rank. Got order: {types_in_order}"
    )
