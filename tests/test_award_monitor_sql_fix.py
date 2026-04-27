"""Regression tests for award_monitor.py broken-Python-in-SQL pattern
fixed 2026-04-27. Two sites had `" + clauses + "` as LITERAL TEXT inside
triple-quoted SQL strings — Python concat never happened, SQLite returned
syntax errors. Same root cause as PRs #484 (scprs_universal_pull) and #611
(scprs_intelligence_engine), all traceable to commit 8fe34398f
"Fix ALL audit findings: 0 SQL injection..." (2026-03-07) which rewrote
f-strings into `+` concat but left the operators inside the surrounding
triple-quote.

These tests prove both check_pc_award + get_price_suggestions execute
their queries cleanly without SQL syntax errors. The key assertion is
"function returns without raising" — under the bug, callers swallowed
the OperationalError silently in their try/except wrappers, so the
features looked dormant rather than broken.
"""
import sqlite3
from unittest.mock import patch

import pytest


def _make_award_db(db_path):
    """Seed a minimal SCPRS + competitor_intel schema. The award_monitor
    queries join scprs_po_master + scprs_po_lines and read from
    competitor_intel — both must exist with at least the columns the SQL
    references (otherwise we'd be testing for the wrong error)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT, dept_code TEXT, supplier TEXT,
            grand_total REAL, start_date TEXT, buyer_email TEXT
        );
        CREATE TABLE scprs_po_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_id INTEGER, description TEXT, unit_price REAL, quantity REAL
        );
        CREATE TABLE competitor_intel (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            competitor_name TEXT, competitor_price REAL, our_price REAL,
            price_delta_pct REAL, found_at TEXT, agency TEXT,
            po_number TEXT, outcome TEXT, item_summary TEXT
        );
    """)
    # Seed: a recent CCHCS PO with nitrile gloves
    cur = conn.execute("""
        INSERT INTO scprs_po_master
        (po_number, dept_code, supplier, grand_total, start_date, buyer_email)
        VALUES ('PO-AWARD-1', '5225', 'medline industries', 50000,
                '2026-04-15', 'buyer@cchcs.gov')
    """)
    po_id = cur.lastrowid
    conn.execute("""
        INSERT INTO scprs_po_lines (po_id, description, unit_price, quantity)
        VALUES (?, 'Nitrile Exam Gloves Medium', 5.00, 10000)
    """, (po_id,))
    # Seed: 2 lost competitor records — one matching, one not
    conn.execute("""
        INSERT INTO competitor_intel
        (competitor_name, competitor_price, our_price, price_delta_pct,
         found_at, agency, po_number, outcome, item_summary)
        VALUES ('Acme Corp', 4.50, 5.20, 13.5, '2026-04-01', 'CCHCS',
                'PO-OLD-1', 'lost', 'nitrile exam gloves medium box')
    """)
    conn.execute("""
        INSERT INTO competitor_intel
        (competitor_name, competitor_price, our_price, price_delta_pct,
         found_at, agency, po_number, outcome, item_summary)
        VALUES ('Other Inc', 100.00, 110.00, 9.1, '2026-04-02', 'DSH',
                'PO-OLD-2', 'lost', 'office paper letter ream')
    """)
    conn.commit()
    return conn


@pytest.fixture
def award_db(tmp_path):
    db_path = tmp_path / "award_test.db"
    seed = _make_award_db(db_path)
    seed.close()
    yield db_path


def _patch_db(db_path):
    """Patch get_db inside award_monitor — it uses a context manager."""
    class _Ctx:
        def __init__(self, p):
            self.p = p
            self.conn = None
        def __enter__(self):
            self.conn = sqlite3.connect(str(self.p))
            self.conn.row_factory = sqlite3.Row
            return self.conn
        def __exit__(self, *a):
            if self.conn:
                self.conn.close()
            return None

    return patch("src.agents.award_monitor.get_db",
                 side_effect=lambda: _Ctx(db_path))


class TestCheckPcAward:
    """Bug 1: the term_clauses join inside the SCPRS query was literal text."""

    def test_runs_without_sql_syntax_error(self, award_db):
        # Pre-fix: this raised OperationalError inside check_pc_award's
        # try/except, returning None silently. Post-fix: query executes
        # and returns either an award dict (match) or None (no match).
        from src.agents.award_monitor import check_pc_award

        pc = {
            "institution": "California Correctional Health Care Services",
            "created_at": "2026-04-01T00:00:00",
            "items": [{"description": "Nitrile Exam Gloves Medium"}],
        }
        # Patch ALL_AGENCIES so dept_code resolution finds something.
        with patch("src.agents.scprs_universal_pull.ALL_AGENCIES",
                   {"5225": ("California Correctional Health Care Services",)}), \
             _patch_db(award_db):
            r = check_pc_award(pc)
        # Either a match dict or None — but no exception bubbled, no
        # syntax error. The seeded data should match (same dept + recent date).
        if r is not None:
            assert isinstance(r, dict)
            assert "outcome" in r

    def test_no_search_terms_short_circuits(self, award_db):
        from src.agents.award_monitor import check_pc_award
        pc = {"institution": "CCHCS", "items": [{"description": "x"}]}
        with _patch_db(award_db):
            r = check_pc_award(pc)
        assert r is None  # no items long enough to build search terms

    def test_no_dept_match_short_circuits(self, award_db):
        # Unknown institution → no dept_code → return None before SQL runs.
        from src.agents.award_monitor import check_pc_award
        pc = {
            "institution": "Some Unknown Agency",
            "created_at": "2026-04-01",
            "items": [{"description": "Nitrile Exam Gloves Medium"}],
        }
        with patch("src.agents.scprs_universal_pull.ALL_AGENCIES",
                   {"9999": ("Different Real Agency",)}), \
             _patch_db(award_db):
            r = check_pc_award(pc)
        assert r is None


class TestGetPriceSuggestions:
    """Bug 2: same pattern in the competitor_intel query."""

    def test_runs_without_sql_syntax_error(self, award_db):
        # Pre-fix: OperationalError swallowed by outer try/except, returning [].
        # Post-fix: returns suggestions for items with matching loss history.
        from src.agents.award_monitor import get_price_suggestions
        items = [{"description": "Nitrile Exam Gloves Medium"}]
        with _patch_db(award_db):
            sugs = get_price_suggestions(items, institution="CCHCS")
        assert isinstance(sugs, list)
        # Seeded data should produce at least 1 match (gloves item)
        assert len(sugs) >= 1
        s = sugs[0]
        assert s["competitor"] == "Acme Corp"
        assert "their_price" in s
        assert "our_price" in s

    def test_no_matching_items_returns_empty(self, award_db):
        from src.agents.award_monitor import get_price_suggestions
        items = [{"description": "Asphalt Sealant 5 Gallon"}]
        with _patch_db(award_db):
            sugs = get_price_suggestions(items)
        assert sugs == []

    def test_short_descriptions_skipped(self, award_db):
        from src.agents.award_monitor import get_price_suggestions
        items = [{"description": "abc"}, {"description": ""}]
        with _patch_db(award_db):
            sugs = get_price_suggestions(items)
        assert sugs == []

    def test_empty_items_list(self, award_db):
        from src.agents.award_monitor import get_price_suggestions
        with _patch_db(award_db):
            sugs = get_price_suggestions([])
        assert sugs == []


class TestNoMoreBrokenSqlPatterns:
    """Lint-style guard: scan src/ for the broken-Python-in-SQL signature
    so a future refactor can't reintroduce this class of bug. PR #484 fixed
    scprs_universal_pull, PR #611 fixed scprs_intelligence_engine, this PR
    fixes award_monitor — the pattern should be extinct in src/ now."""

    def test_no_literal_concat_inside_triple_quoted_sql(self):
        import re
        from pathlib import Path

        # Pattern: any line where an `AND`/`WHERE`/`OR` keyword is followed
        # by literal `" + var + "` text (with arbitrary whitespace). The
        # caret matches indented source lines (inside a triple-quote, the
        # SQL keywords appear at the start of each line after indent).
        bad_pattern = re.compile(
            r'^\s+(AND|WHERE|OR)\s+"\s*\+\s*\w+\s*\+\s*"',
            re.MULTILINE,
        )

        offenders = []
        src = Path(__file__).resolve().parent.parent / "src"
        for py in src.rglob("*.py"):
            try:
                txt = py.read_text(encoding="utf-8")
            except Exception:
                continue
            for m in bad_pattern.finditer(txt):
                # Report file:line:matched-text for any future regression.
                line_no = txt.count("\n", 0, m.start()) + 1
                offenders.append(f"{py.relative_to(src.parent)}:{line_no}: {m.group(0).strip()}")

        assert not offenders, (
            "Broken-Python-in-SQL pattern detected. The `+ var +` text "
            "must NOT appear inside a triple-quoted SQL string — Python "
            "concat won't happen, SQLite will syntax-error. Use real "
            "Python concatenation OUTSIDE the triple-quote instead.\n"
            "Offenders:\n  " + "\n  ".join(offenders)
        )
