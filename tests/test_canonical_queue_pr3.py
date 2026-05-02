"""PR-3 regression tests: home queue + stale-quotes via canonical.

Two surfaces migrated to the canonical-state truth layer:

  1. `routes_rfq.home()` queue split — replaced ad-hoc allow-list
     (`_pc_actionable` / `_actionable_rfq`) with canonical
     `is_active_queue` deny-list. Symptom this fixes: Mike's
     2026-05-02 screenshot showed "Queue (5)" while 68 stale rows
     existed — their statuses (e.g. empty, "in_progress") weren't
     in the allow-list, so the table silently dropped them.

  2. `routes_analytics.stale_quotes` ("📬 Awaiting Response"
     widget) — replaced inline `r.status != "sent"` with canonical
     `is_awaiting_buyer`. The integrity guard (sent_at != created_at)
     was missing entirely, so writers stamping created_at into
     sent_at produced rows that displayed as "stale 0 days".

These tests lock both contracts so a future tweak doesn't drift back.
"""
from __future__ import annotations

from datetime import datetime, timedelta


# ─── Home queue: status-set → predicate migration ────────────────────────


def test_home_queue_includes_unknown_status_under_canonical():
    """Pre-PR-3, an RFQ with status='in_progress' would silently
    drop from the queue because that string wasn't in
    `_actionable_rfq`. Canonical deny-list keeps it visible."""
    from src.core.canonical_state import is_active_queue
    rec = {"status": "in_progress", "is_test": 0}
    assert is_active_queue(rec) is True


def test_home_queue_includes_empty_status_under_canonical():
    """A partial-write RFQ with status='' would drop from the old
    allow-list. Canonical surfaces it for operator review (empty
    status is a data quality bug they need to see)."""
    from src.core.canonical_state import is_active_queue
    rec = {"status": "", "is_test": 0}
    assert is_active_queue(rec) is True


def test_home_queue_excludes_pending_award_under_canonical():
    """Conversely, pending_award (PC post-sent) was in the old
    sent-set but PR-3 also added it to ACTIVE_QUEUE_EXCLUDED_STATUSES
    canonically, so a PC sitting in pending_award shouldn't appear
    in the active queue. It's the buyer's turn now."""
    from src.core.canonical_state import is_active_queue
    rec = {"status": "pending_award", "is_test": 0}
    assert is_active_queue(rec) is False


# ─── Home queue: actual route returns 200 ────────────────────────────────


def test_home_route_renders_after_canonical_migration(auth_client):
    """Smoke check: the home() route still 200s after swapping the
    allow-list out for is_active_queue. Catches import errors,
    template signature changes, or accidental name re-use."""
    resp = auth_client.get("/")
    assert resp.status_code == 200, resp.data[:200]


def test_home_template_uses_queue_label_not_rfq_queue(auth_client):
    """PR-3 renamed 'RFQ QUEUE (704A/704B...)' → 'Queue (704A/704B...)'.
    Verify the new label rendered in the actual response body."""
    resp = auth_client.get("/")
    body = resp.data.decode("utf-8", errors="replace")
    # The PC card header is unchanged ('Price Checks'), the RFQ
    # card header drops 'RFQ' from the title literal.
    # We don't assert the absence of "RFQ" — it appears throughout
    # the page legitimately. Just assert the new title literal is
    # present in the queue_table macro arg position.
    assert "'Queue', '(704A/704B" in body or '"Queue", "(704A/704B' in body or 'Queue</span>' in body or '>Queue<' in body or '>Queue ' in body, (
        "PR-3: queue_table('rfq', ...) should render header 'Queue'"
    )


# ─── stale-quotes: predicate migration ───────────────────────────────────


def test_stale_quotes_route_uses_canonical_predicate():
    """Lock the migration: the stale_quotes route source must
    import is_awaiting_buyer (the canonical predicate) and not
    fall back to inline `status != 'sent'` checks."""
    from src.api.modules.routes_analytics import stale_quotes
    import inspect
    src = inspect.getsource(stale_quotes)
    assert "is_awaiting_buyer" in src, (
        "PR-3: stale_quotes must use canonical is_awaiting_buyer"
    )
    # The deleted inline checks should not have crept back.
    assert 'status") != "sent"' not in src, (
        "PR-3 regressed: inline status check is back; "
        "use canonical is_awaiting_buyer"
    )


def test_stale_quotes_filters_writer_stamped_sent_at_rows(auth_client):
    """End-to-end: seed an RFQ where sent_at == created_at (the
    bug). Hit /api/stale-quotes and assert the row is NOT in the
    response. Pre-PR-3 the inline check would have included it
    as 'stale 0 days'."""
    from src.api.data_layer import _save_single_rfq

    # Bug-shape row: same string in both fields, status='sent',
    # well in the past so the days threshold doesn't filter it.
    bad_ts = (datetime.now() - timedelta(days=10)).isoformat()
    _save_single_rfq("pr3-bad-1", {
        "id": "pr3-bad-1",
        "status": "sent",
        "rfq_number": "PR3-BAD-1",
        "solicitation_number": "PR3-BAD-1",
        "created_at": bad_ts,
        "sent_at": bad_ts,  # writer stamped created_at into sent_at
        "line_items": [],
    })

    # Healthy row: real sent_at after a creation timestamp 10 days ago.
    good_created = (datetime.now() - timedelta(days=10)).isoformat()
    good_sent = (datetime.now() - timedelta(days=8)).isoformat()
    _save_single_rfq("pr3-good-1", {
        "id": "pr3-good-1",
        "status": "sent",
        "rfq_number": "PR3-GOOD-1",
        "solicitation_number": "PR3-GOOD-1",
        "created_at": good_created,
        "sent_at": good_sent,
        "line_items": [],
    })

    # Bust the 120s in-process cache before hitting the route.
    # routes_analytics.py is loaded via exec() into the dashboard
    # namespace per CLAUDE.md (module loading section), so we reach
    # `_stale_cache` via dashboard, not via direct import.
    from src.api import dashboard
    dashboard._stale_cache = {"data": None, "ts": 0}

    resp = auth_client.get("/api/stale-quotes?days=3")
    assert resp.status_code == 200
    payload = resp.get_json()
    ids = {row["id"] for row in payload.get("stale", [])}

    assert "pr3-good-1" in ids, (
        "Healthy sent row dropped — predicate may be too strict"
    )
    assert "pr3-bad-1" not in ids, (
        "PR-3 regressed: writer-stamped-created_at row appeared "
        "in stale-quotes; canonical integrity guard should drop it"
    )
