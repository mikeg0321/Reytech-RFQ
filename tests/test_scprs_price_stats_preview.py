"""Phase 1.5-A2 — boot wire-up + preview admin endpoint.

Companion to test_scprs_price_stats.py. Pins:
  1. `rebuild_scprs_price_stats` is invoked from `app.py::_deferred_init`
     (env-gated by `SCPRS_ROLLUP_ON_BOOT`).
  2. `/oracle/price-stats/preview` returns HTML, requires auth, doesn't
     mutate state, supports filter query params, renders an empty-state
     banner when the rollup table isn't populated yet.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


# ── Boot wire-up ─────────────────────────────────────────────────


def test_app_boot_calls_rebuild_scprs_price_stats():
    """`app.py::_deferred_init` must invoke the rollup builder alongside
    the placeholder-ASIN backfill so the table actually gets populated
    on every Railway deploy."""
    src = _read("app.py")
    idx = src.index("def _deferred_init")
    body = src[idx:idx + 12000]
    assert "rebuild_scprs_price_stats" in body, (
        "Boot must invoke SCPRS rollup builder; otherwise the table "
        "stays empty in prod and Phase 1.5-B has no data to read"
    )


def test_boot_rollup_is_env_gated():
    """SCPRS_ROLLUP_ON_BOOT=0 must disable the boot-time rebuild
    without code change — so we can kill it from Railway env if a
    bug surfaces without redeploying."""
    src = _read("app.py")
    idx = src.index("def _deferred_init")
    body = src[idx:idx + 12000]
    assert "SCPRS_ROLLUP_ON_BOOT" in body, (
        "Boot wire-up must check SCPRS_ROLLUP_ON_BOOT env var so prod "
        "can disable without redeploy"
    )


# ── Preview endpoint ─────────────────────────────────────────────


def test_price_stats_preview_returns_200_and_html(auth_client):
    r = auth_client.get("/oracle/price-stats/preview")
    assert r.status_code == 200
    assert "text/html" in r.headers.get("Content-Type", "")
    body = r.get_data(as_text=True)
    assert "SCPRS price-stats preview" in body
    assert "total buckets" in body
    assert "<table>" in body


def test_price_stats_preview_requires_auth(anon_client):
    r = anon_client.get("/oracle/price-stats/preview")
    assert r.status_code in (401, 302, 403), (
        f"preview endpoint must require auth, got {r.status_code}"
    )


def test_price_stats_preview_does_not_mutate(auth_client):
    """Pure read-only. Hitting the endpoint must NOT call the rollup
    builder — that would silently swap the table during a casual page
    load. Static-analysis check on the route source since mock-patching
    is awkward across the dynamic Flask blueprint loader."""
    src = _read(os.path.join("src", "api", "modules", "routes_pricecheck_admin.py"))
    # Locate the preview handler body
    start = src.index("def oracle_price_stats_preview")
    end = src.index("@bp.route", start)
    body = src[start:end]
    # Grep for an actual CALL — the docstring mentions the function
    # name in the empty-state hint, so a bare-name check would be a
    # false positive. A call requires `rebuild_scprs_price_stats(`.
    assert "rebuild_scprs_price_stats(" not in body, (
        "preview handler must NOT call the rollup builder — that would "
        "silently swap the table on every page load"
    )
    # Also pin: no import of the builder function. If a future edit
    # adds the import, even unused, it's a smell worth flagging.
    assert "from src.agents.scprs_price_stats import rebuild" not in body, (
        "preview handler must NOT import the rollup builder"
    )
    # And confirm the endpoint actually loads
    r = auth_client.get("/oracle/price-stats/preview")
    assert r.status_code == 200


def test_price_stats_preview_filter_query_params(auth_client):
    """Query params must filter the rendered rows. Even with an empty
    rollup the endpoint returns 200 (empty state)."""
    r = auth_client.get("/oracle/price-stats/preview?key_type=mfg&agency=cchcs&limit=5")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    # Filters present in the rendered UI
    assert "mfg only" in body  # filter link
    assert "Match Key" in body  # column header


def test_price_stats_preview_empty_state_banner(auth_client):
    """When the rollup is empty (boot backfill hasn't run yet), the
    preview must show a helpful empty-state row instead of an
    invisible blank table."""
    r = auth_client.get("/oracle/price-stats/preview")
    body = r.get_data(as_text=True)
    # Either we get real rows (rollup populated) or the empty-state
    # message. Both keep the operator un-confused.
    has_empty_state = "No rollup rows match the filter" in body
    has_real_rows = "<code>mfg</code>" in body or "<code>unspsc</code>" in body
    assert has_empty_state or has_real_rows, (
        "preview must surface either real rows or the empty-state hint"
    )


def test_price_stats_preview_limit_caps_at_500(auth_client):
    """Defensive: an operator passing ?limit=99999 must not be able to
    pull every row in the table (could be hundreds of thousands of
    buckets on prod)."""
    r = auth_client.get("/oracle/price-stats/preview?limit=99999")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    # The "Showing top N" banner caps at 500
    import re
    m = re.search(r'Showing top <strong>(\d+)</strong>', body)
    if m:
        assert int(m.group(1)) <= 500, (
            f"limit must be capped at 500, got {m.group(1)}"
        )
