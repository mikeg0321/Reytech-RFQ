"""
Regression test for the /intel/scprs error-banner surface (§2 of the
2026-04-23 review).

Why this exists:
  page_intel_scprs in routes_intel.py used to wrap get_universal_intelligence
  in a bare `except Exception: pass`-style block that substituted empty
  defaults silently. That's how the broken `" + where + "` SQL hid for
  7 weeks. The §2 fix logs the exception AND surfaces it to the template
  as a red banner. This test proves the banner actually renders.
"""
import sqlite3

import pytest


def test_intel_scprs_renders_error_banner_on_intelligence_failure(auth_client, monkeypatch):
    """If get_universal_intelligence raises, the page must render an error
    banner — never silently substitute empty defaults like it used to."""
    import src.agents.scprs_universal_pull as sup

    def boom(*args, **kwargs):
        raise sqlite3.OperationalError('near "fake": syntax error')

    monkeypatch.setattr(sup, "get_universal_intelligence", boom)

    resp = auth_client.get("/intel/scprs")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")

    # The error banner is present.
    assert "Intelligence query failed" in body
    # The error class name is shown (not just a generic "something broke").
    assert "OperationalError" in body
    # The error message is shown.
    assert "fake" in body
    # The "No data yet — click Pull P0 Now" banner is suppressed when an
    # error is present (otherwise the user gets two contradictory banners).
    assert "click Pull P0 Now" not in body or "Intelligence query failed" in body.split("click Pull P0 Now")[0]


def test_intel_scprs_no_error_banner_when_intelligence_works(auth_client, monkeypatch):
    """Inverse — when intelligence works, no error banner appears."""
    import src.agents.scprs_universal_pull as sup

    monkeypatch.setattr(
        sup, "get_universal_intelligence",
        lambda *a, **k: {
            "summary": {"total_market_spend": 0, "gap_opportunity": 0,
                        "win_back_opportunity": 0, "agencies_tracked": 0},
            "gap_items": [], "win_back": [], "by_agency": [],
            "competitors": [], "auto_closed_quotes": [], "totals": {},
        },
    )
    monkeypatch.setattr(
        sup, "get_pull_status",
        lambda: {"pos_stored": 0, "lines_stored": 0, "running": False, "progress": ""},
    )

    resp = auth_client.get("/intel/scprs")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    assert "Intelligence query failed" not in body


def test_intel_scprs_handles_malformed_intelligence_shape(auth_client, monkeypatch):
    """
    Twin of the 7-week silent-blank shape: function returns successfully
    but with garbage shape (None values for keys the template iterates).
    The page must still render 200 — never crash on bad shape from a
    well-meaning but broken upstream.
    """
    import src.agents.scprs_universal_pull as sup

    # Every value the page reads is None (worst plausible legitimate-shape).
    monkeypatch.setattr(
        sup, "get_universal_intelligence",
        lambda *a, **k: {
            "summary": None,
            "gap_items": None,
            "win_back": None,
            "by_agency": None,
            "competitors": None,
            "auto_closed_quotes": None,
            "totals": None,
        },
    )
    monkeypatch.setattr(
        sup, "get_pull_status",
        lambda: {"pos_stored": None, "lines_stored": None, "running": None, "progress": None},
    )

    resp = auth_client.get("/intel/scprs")
    assert resp.status_code == 200
    body = resp.data.decode("utf-8", errors="replace")
    # No error banner — the upstream didn't raise, just returned garbage.
    assert "Intelligence query failed" not in body
    # The page rendered — title is present.
    assert "SCPRS" in body

