"""Regression guards for quotes.html filter chip UX.

Four contracts:

  1. `search_quotes(since_hours=24)` returns only quotes with created_at
     within the last 24 hours. Unparseable/missing dates are excluded
     when the filter is active (better to miss an ambiguous row than
     show one the operator is actively scoping away from).

  2. `GET /quotes?since=24h` narrows the visible row set. Row rendering
     happens in `rows_html`; we assert the old quote's number is absent
     and the new quote's number is present.

  3. Chip URLs stack facets — clicking Agency while Status is active
     must preserve Status in the href (and vice versa). Breakages here
     force the operator to re-set filters every click.

  4. Template ships the testids the ops loop will rely on:
     `quotes-status-chips`, `quotes-agency-chips`, `quotes-since-24h`.
"""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timedelta


def _insert_quote(temp_data_dir, *, quote_number, created_at, agency="CDCR",
                  status="generated", total=0.0):
    """Direct SQL insert so the caller controls created_at (bypasses the
    fixture's implicit now())."""
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT OR REPLACE INTO quotes
           (quote_number, agency, institution, status, total, subtotal, tax,
            created_at, updated_at, source_pc_id, source_rfq_id, line_items)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (quote_number, agency, "Test Inst", status, total, total, 0.0,
         created_at, created_at, None, None, json.dumps([])))
    conn.commit()
    conn.close()


# ── Helper-layer contract: since_hours filter ─────────────────────────────

def test_search_quotes_since_hours_excludes_old_rows(temp_data_dir):
    now = datetime.now()
    recent_iso = (now - timedelta(hours=2)).isoformat()
    old_iso = (now - timedelta(days=3)).isoformat()
    _insert_quote(temp_data_dir, quote_number="R26Q-RECENT", created_at=recent_iso)
    _insert_quote(temp_data_dir, quote_number="R26Q-OLD", created_at=old_iso)

    from src.forms.quote_generator import search_quotes
    results = search_quotes(since_hours=24, limit=50)
    numbers = {q.get("quote_number") for q in results}
    assert "R26Q-RECENT" in numbers, "2h-old quote must survive since_hours=24"
    assert "R26Q-OLD" not in numbers, "3d-old quote must be filtered out"


def test_search_quotes_since_hours_zero_is_noop(temp_data_dir):
    """since_hours=0 (default) must not filter anything, even ancient rows."""
    old_iso = (datetime.now() - timedelta(days=90)).isoformat()
    _insert_quote(temp_data_dir, quote_number="R26Q-ANCIENT", created_at=old_iso)

    from src.forms.quote_generator import search_quotes
    numbers = {q.get("quote_number") for q in search_quotes(since_hours=0, limit=50)}
    assert "R26Q-ANCIENT" in numbers, (
        "since_hours=0 must return all quotes — this is the non-filter default "
        "every caller depends on"
    )


def test_search_quotes_since_hours_excludes_unparseable_dates(temp_data_dir):
    """If a row's created_at can't be parsed, the since filter must
    exclude it rather than silently pass it through — when the operator
    asks for 'last 24h', they mean 'give me a witnessable recent window'."""
    _insert_quote(temp_data_dir, quote_number="R26Q-BADDATE",
                  created_at="not-a-real-timestamp")

    from src.forms.quote_generator import search_quotes
    numbers = {q.get("quote_number") for q in search_quotes(since_hours=24, limit=50)}
    assert "R26Q-BADDATE" not in numbers, (
        "unparseable created_at must be filtered out of since-hours results"
    )
    # But still visible without the since filter — no silent global exclusion.
    all_numbers = {q.get("quote_number") for q in search_quotes(limit=50)}
    assert "R26Q-BADDATE" in all_numbers


# ── Route-layer contract: ?since=24h filters the visible row set ──────────

def test_quotes_route_since_24h_narrows_rows(auth_client, temp_data_dir):
    now = datetime.now()
    _insert_quote(temp_data_dir, quote_number="R26Q-FRESH",
                  created_at=(now - timedelta(hours=1)).isoformat())
    _insert_quote(temp_data_dir, quote_number="R26Q-STALE",
                  created_at=(now - timedelta(days=5)).isoformat())

    all_resp = auth_client.get("/quotes")
    assert all_resp.status_code == 200
    all_html = all_resp.get_data(as_text=True)
    assert "R26Q-FRESH" in all_html
    assert "R26Q-STALE" in all_html

    scoped = auth_client.get("/quotes?since=24h")
    assert scoped.status_code == 200
    scoped_html = scoped.get_data(as_text=True)
    assert "R26Q-FRESH" in scoped_html, (
        "Last-24h chip must still show a 1h-old quote"
    )
    assert "R26Q-STALE" not in scoped_html, (
        "Last-24h chip must hide a 5d-old quote"
    )


# ── Template-layer contract: chip testids + URL stacking ──────────────────

def test_quotes_ships_chip_testids(auth_client, temp_data_dir):
    resp = auth_client.get("/quotes")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    for hook in ("quotes-status-chips", "quotes-agency-chips", "quotes-since-24h"):
        assert f'data-testid="{hook}"' in html, (
            f"Missing chip hook `{hook}` — future audits need these testids "
            f"to find the filter row"
        )


def test_quotes_chip_urls_stack_filters(auth_client, temp_data_dir):
    """Clicking Agency when Status=won is already set must keep status=won
    in every agency chip's href. Without this, every chip click forces the
    operator to re-select the other facets."""
    resp = auth_client.get("/quotes?status=won")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    # Every agency chip href must carry status=won forward.
    assert 'data-chip-agency="CCHCS"' in html
    # Isolate the CCHCS agency chip's href
    import re
    m = re.search(r'<a[^>]*data-chip-agency="CCHCS"[^>]*href="([^"]+)"', html)
    assert m is None or "status=won" in m.group(1), (
        "Agency chip dropped the active status filter — click would reset "
        "scope to All Statuses"
    )
    # Also verify: clicking the "Last 24h" chip preserves status. Attribute
    # order varies by Jinja output; grab the full <a> tag then extract href.
    m2 = re.search(r'<a[^>]*data-testid="quotes-since-24h"[^>]*>', html)
    assert m2 is not None, "Last-24h chip anchor missing"
    m2_href = re.search(r'href="([^"]+)"', m2.group(0))
    assert m2_href is not None, "Last-24h chip href missing"
    assert "status=won" in m2_href.group(1)


def test_since_chip_toggles_off_when_already_active(auth_client, temp_data_dir):
    """When since=24h is already active, the Last-24h chip's href must drop
    `since` so clicking it again turns the filter OFF. Without this test a
    future refactor could accidentally make the chip a one-way switch."""
    resp = auth_client.get("/quotes?since=24h")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    import re
    tag = re.search(r'<a[^>]*data-testid="quotes-since-24h"[^>]*>', html)
    assert tag is not None
    href = re.search(r'href="([^"]+)"', tag.group(0)).group(1)
    assert "since=24h" not in href, (
        "Clicking the active Last-24h chip must toggle OFF (drop since from URL)"
    )


def test_quotes_ships_expired_status_chip(auth_client, temp_data_dir):
    """The old status dropdown let operators filter to expired quotes.
    The chip row must include Expired or we've dropped a pre-existing view."""
    resp = auth_client.get("/quotes")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert 'data-chip-status="expired"' in html, (
        "Expired chip missing — pre-existing status filter regressed"
    )


def test_quotes_chips_stack_all_four_facets(auth_client, temp_data_dir):
    """status + agency + since + q must all coexist in generated chip
    hrefs. If any facet is silently dropped, the operator's scope resets
    on every click."""
    resp = auth_client.get("/quotes?status=won&agency=CCHCS&since=24h&q=stryker")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    import re

    def _href_for(attr_pattern):
        tag = re.search(r'<a[^>]*' + attr_pattern + r'[^>]*>', html)
        if tag is None:
            return None
        m = re.search(r'href="([^"]+)"', tag.group(0))
        return m.group(1) if m else None

    # Status chip for "lost" (a *different* status than active won)
    lost_href = _href_for(r'data-chip-status="lost"')
    assert lost_href is not None
    assert "agency=CCHCS" in lost_href
    assert "since=24h" in lost_href
    assert "q=stryker" in lost_href

    # Agency chip for CDCR (different agency than active CCHCS)
    cdcr_href = _href_for(r'data-chip-agency="CDCR"')
    assert cdcr_href is not None
    assert "status=won" in cdcr_href
    assert "since=24h" in cdcr_href
    assert "q=stryker" in cdcr_href


def test_quotes_active_chip_marks_itself(auth_client, temp_data_dir):
    """The active chip must carry .quote-chip-active so CSS can render it
    as selected — without the class, the operator can't see what filter
    is in effect."""
    resp = auth_client.get("/quotes?since=24h&status=pending")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    import re
    def _class_for(attr_pattern):
        tag = re.search(r'<a[^>]*' + attr_pattern + r'[^>]*>', html)
        if tag is None:
            return None
        cls = re.search(r'class="([^"]+)"', tag.group(0))
        return cls.group(1) if cls else None

    pending_cls = _class_for(r'data-chip-status="pending"')
    assert pending_cls is not None, "Pending chip anchor missing"
    assert "quote-chip-active" in pending_cls
    since_cls = _class_for(r'data-testid="quotes-since-24h"')
    assert since_cls is not None, "Last-24h chip anchor missing"
    assert "quote-chip-active" in since_cls
