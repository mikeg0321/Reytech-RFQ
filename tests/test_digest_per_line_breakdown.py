"""PR-D — loss-digest per-line breakdown rendering.

PR-C started persisting line-level deltas into `competitor_intel_lines`.
This PR makes that data visible:
  1. `generate_weekly_report()` queries the child rows for each loss
     and attaches `lines: [...]` to the loss dict.
  2. `format_report_email()` renders an inset table under each loss
     with per-line columns (SCPRS desc, their $, our $, Δ%, match-by).
     Sorted by |delta| desc so the worst offenders surface first.
     Color-coded:
       red  — competitor cheaper (we lost on price for that line)
       green — we were cheaper (we lost the bundle despite winning)
       amber — essentially tied (within ±2%)
       grey — no per-item match (matched_by="none")

Back-compat: losses without per-line data render as before (single row).
"""
from __future__ import annotations

import os
import sqlite3
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest


# ── generate_weekly_report attaches lines to each loss ───────────


def _setup_schema(conn):
    """Schema minimal enough to exercise the digest queries."""
    conn.executescript("""
        CREATE TABLE quotes (
            quote_number TEXT, institution TEXT, total REAL,
            po_number TEXT, created_at TEXT, status TEXT, is_test INTEGER
        );
        CREATE TABLE competitor_intel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            found_at TEXT, pc_id TEXT, quote_number TEXT,
            our_price REAL, competitor_name TEXT, competitor_price REAL,
            price_delta REAL, price_delta_pct REAL, po_number TEXT,
            agency TEXT, institution TEXT, item_summary TEXT,
            items_detail TEXT, solicitation TEXT, outcome TEXT, notes TEXT,
            loss_reason_class TEXT
        );
        CREATE TABLE competitor_intel_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            competitor_intel_id INTEGER NOT NULL,
            line_num INTEGER, scprs_description TEXT,
            scprs_unit_price REAL, scprs_quantity REAL,
            scprs_mfg TEXT, scprs_unspsc TEXT,
            our_item_idx INTEGER, our_unit_price REAL, our_mfg TEXT,
            price_delta_pct REAL, matched_by TEXT DEFAULT 'none',
            created_at TEXT
        );
        CREATE TABLE oracle_calibration (
            category TEXT, agency TEXT, sample_size INTEGER,
            win_count INTEGER, loss_on_price INTEGER, loss_on_other INTEGER,
            avg_winning_margin REAL, recommended_max_markup REAL,
            last_updated TEXT
        );
        CREATE TABLE winning_prices (
            id INTEGER PRIMARY KEY, fingerprint TEXT,
            margin_pct REAL, recorded_at TEXT
        );
        CREATE TABLE action_items (
            id INTEGER PRIMARY KEY, action_type TEXT, description TEXT,
            priority TEXT, source_quote TEXT, status TEXT, created_at TEXT
        );
    """)


def test_generate_weekly_report_attaches_per_line_lines(monkeypatch):
    """A loss with two child rows in competitor_intel_lines should
    surface them sorted by |delta| desc on the loss dict."""
    from src.agents import oracle_weekly_report as owr
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _setup_schema(conn)

    # Seed one loss + two child lines
    conn.execute(
        "INSERT INTO competitor_intel "
        "(id, found_at, quote_number, competitor_name, competitor_price, "
        " our_price, price_delta_pct, agency, loss_reason_class, outcome) "
        "VALUES (1, ?, 'R26Q42', 'Acme', 540, 600, -10, 'cchcs', 'price', 'lost')",
        (owr.datetime.now().isoformat(),),
    )
    conn.executemany(
        "INSERT INTO competitor_intel_lines "
        "(competitor_intel_id, line_num, scprs_description, scprs_unit_price, "
        " scprs_quantity, scprs_mfg, our_item_idx, our_unit_price, "
        " price_delta_pct, matched_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 1, "Bandage Mfg # X-1", 8.00, 100, "X-1", 0, 10.00, -20.0,
             "mfg_exact", "now"),
            (1, 2, "Gloves Mfg # G-1",  5.50, 100, "G-1", 1,  5.00,  10.0,
             "mfg_exact", "now"),
        ],
    )
    conn.commit()

    class _Ctx:
        def __init__(self, c):
            self._c = c
        def __enter__(self):
            return self._c
        def __exit__(self, *a):
            return False
    monkeypatch.setattr("src.core.db.get_db", lambda: _Ctx(conn))

    report = owr.generate_weekly_report()
    assert report["loss_count"] == 1
    loss = report["losses"][0]
    assert "lines" in loss
    assert len(loss["lines"]) == 2
    # Sort order: |delta| desc → -20 first, +10 second
    assert loss["lines"][0]["delta_pct"] == -20.0
    assert loss["lines"][1]["delta_pct"] == 10.0
    # Per-line metadata preserved
    assert loss["lines"][0]["matched_by"] == "mfg_exact"
    assert loss["lines"][0]["their_price"] == 8.00
    assert loss["lines"][0]["our_price"] == 10.00


def test_generate_weekly_report_loss_without_child_rows_gets_empty_lines(monkeypatch):
    """Losses logged before PR-C (no child rows in
    competitor_intel_lines) must still appear in the report with
    `lines: []` so the renderer's loop is safe."""
    from src.agents import oracle_weekly_report as owr
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _setup_schema(conn)
    conn.execute(
        "INSERT INTO competitor_intel "
        "(id, found_at, quote_number, competitor_name, competitor_price, "
        " our_price, price_delta_pct, agency, loss_reason_class, outcome) "
        "VALUES (99, ?, 'R25Q1', 'OldVendor', 100, 120, -16.7, 'cchcs', '', 'lost')",
        (owr.datetime.now().isoformat(),),
    )
    conn.commit()

    class _Ctx:
        def __init__(self, c):
            self._c = c
        def __enter__(self):
            return self._c
        def __exit__(self, *a):
            return False
    monkeypatch.setattr("src.core.db.get_db", lambda: _Ctx(conn))

    report = owr.generate_weekly_report()
    assert report["loss_count"] == 1
    assert report["losses"][0]["lines"] == []


# ── format_report_email renders the breakdown ────────────────────


def test_format_report_email_renders_per_line_breakdown():
    """A loss with `lines` should produce a nested table with the
    per-line columns + color-coded delta cells."""
    from src.agents.oracle_weekly_report import format_report_email
    report = {
        "period_start": "2026-05-06",
        "period_end": "2026-05-13",
        "generated_at": "2026-05-13T08:00:00",
        "wins": [], "win_count": 0, "win_revenue": 0,
        "losses": [{
            "id": 1, "quote": "R26Q42", "competitor": "Acme",
            "their_price": 540, "our_price": 600, "delta_pct": -10,
            "agency": "cchcs", "reason": "price",
            "date": "2026-05-12",
            "lines": [
                {"line_num": 1, "desc": "Bandage Sterile 4x4",
                 "their_price": 8.0, "qty": 100, "their_mfg": "X-1",
                 "our_idx": 0, "our_price": 10.0, "delta_pct": -20.0,
                 "matched_by": "mfg_exact"},
                {"line_num": 2, "desc": "Gloves Nitrile L",
                 "their_price": 5.5, "qty": 100, "their_mfg": "G-1",
                 "our_idx": 1, "our_price": 5.0, "delta_pct": 10.0,
                 "matched_by": "mfg_exact"},
            ],
        }],
        "calibrations": [], "winning_prices_total": 0,
        "winning_prices_unique": 0, "avg_margin_all_time": 0,
        "supplier_leads": [], "pending_actions": [],
    }
    html = format_report_email(report)
    # Per-line breakdown columns present
    assert "Their $" in html
    assert "Our $" in html
    assert "Match" in html
    # Per-line data surfaces
    assert "Bandage Sterile 4x4" in html
    assert "Gloves Nitrile L" in html
    # Both delta percentages render (with sign)
    assert "-20.0%" in html
    assert "+10.0%" in html
    # Match-quality cell
    assert "mfg_exact" in html


def test_format_report_email_back_compat_loss_without_lines():
    """A loss without `lines` (legacy data) must still render as a
    plain row — no error, no broken HTML."""
    from src.agents.oracle_weekly_report import format_report_email
    report = {
        "period_start": "2026-05-06",
        "period_end": "2026-05-13",
        "generated_at": "2026-05-13T08:00:00",
        "wins": [], "win_count": 0, "win_revenue": 0,
        "losses": [{
            "quote": "R25Q1", "competitor": "OldVendor",
            "their_price": 100, "our_price": 120, "delta_pct": -16.7,
            "agency": "cchcs", "reason": "", "date": "2026-05-10",
            # no "lines" key
        }],
        "calibrations": [], "winning_prices_total": 0,
        "winning_prices_unique": 0, "avg_margin_all_time": 0,
        "supplier_leads": [], "pending_actions": [],
    }
    html = format_report_email(report)
    # The loss appears, but no per-line table
    assert "R25Q1" in html
    assert "OldVendor" in html
    # Nested breakdown table NOT present (no per-line headers)
    assert "Their $" not in html
    assert "matched_by" not in html


def test_format_report_email_color_codes_per_line_deltas():
    """Per-line delta colors: red when competitor cheaper, green when
    we were cheaper, amber when tied (|Δ| < 2%)."""
    from src.agents.oracle_weekly_report import format_report_email
    report = {
        "period_start": "2026-05-06", "period_end": "2026-05-13",
        "generated_at": "2026-05-13T08:00:00",
        "wins": [], "win_count": 0, "win_revenue": 0,
        "losses": [{
            "id": 1, "quote": "R26Q42", "competitor": "Acme",
            "their_price": 100, "our_price": 110, "delta_pct": -9,
            "agency": "cchcs", "reason": "price", "date": "2026-05-12",
            "lines": [
                # Competitor cheaper → red
                {"line_num": 1, "desc": "Item A", "their_price": 8, "qty": 1,
                 "our_price": 10, "delta_pct": -20.0, "matched_by": "mfg_exact"},
                # We were cheaper → green
                {"line_num": 2, "desc": "Item B", "their_price": 12, "qty": 1,
                 "our_price": 10, "delta_pct": 20.0, "matched_by": "mfg_exact"},
                # Tied → amber
                {"line_num": 3, "desc": "Item C", "their_price": 10, "qty": 1,
                 "our_price": 10, "delta_pct": 0.5, "matched_by": "mfg_exact"},
            ],
        }],
        "calibrations": [], "winning_prices_total": 0,
        "winning_prices_unique": 0, "avg_margin_all_time": 0,
        "supplier_leads": [], "pending_actions": [],
    }
    html = format_report_email(report)
    # Red color for competitor-cheaper line
    assert "#f85149" in html  # red
    # Green for we-were-cheaper line
    assert "#3fb950" in html  # green
    # Amber for tied line
    assert "#d29922" in html  # amber


def test_format_report_email_truncates_long_line_lists():
    """When a loss has > 15 lines, render only the top 15 (by |delta|)
    and a footer indicating how many more were elided."""
    from src.agents.oracle_weekly_report import format_report_email
    lines = [
        {"line_num": i, "desc": f"Item {i}", "their_price": 10, "qty": 1,
         "our_price": 11, "delta_pct": -float(i),
         "matched_by": "mfg_exact"}
        for i in range(1, 21)  # 20 lines
    ]
    report = {
        "period_start": "2026-05-06", "period_end": "2026-05-13",
        "generated_at": "2026-05-13T08:00:00",
        "wins": [], "win_count": 0, "win_revenue": 0,
        "losses": [{
            "id": 1, "quote": "R26Q42", "competitor": "Acme",
            "their_price": 100, "our_price": 110, "delta_pct": -9,
            "agency": "cchcs", "reason": "price", "date": "2026-05-12",
            "lines": lines,
        }],
        "calibrations": [], "winning_prices_total": 0,
        "winning_prices_unique": 0, "avg_margin_all_time": 0,
        "supplier_leads": [], "pending_actions": [],
    }
    html = format_report_email(report)
    assert "5 more lines" in html, html[-500:]
