"""Pin S-13 — won_quotes schema must match between db.py and won_quotes_db.py.

Audit 2026-05-07 v2 §S-13: two CREATE TABLE statements with conflicting
nullability. Whichever ran first won; subsequent INSERT into the wrong
shape could throw at runtime.

These tests pin the canonical schema (db.py is the source of truth) and
that won_quotes_db.py's _ensure_won_quotes_table() doesn't regress
toward NOT NULL constraints.
"""
from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_won_quotes_db_create_does_not_use_not_null():
    """The won_quotes_db.py _ensure_ helper must NOT introduce
    NOT NULL constraints that diverge from db.py's nullable schema."""
    import pathlib
    src = pathlib.Path(
        "src/knowledge/won_quotes_db.py"
    ).read_text(encoding="utf-8")

    # Extract the won_quotes CREATE TABLE block in won_quotes_db.py
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS won_quotes\s*\((.*?)\)\s*\"\"\"",
        src, re.DOTALL,
    )
    assert m, "won_quotes_db.py is missing the CREATE TABLE statement"
    create_block = m.group(1)

    assert "NOT NULL" not in create_block, (
        "S-13 regression: won_quotes_db.py reintroduced NOT NULL on a "
        "won_quotes column. Canonical schema (src/core/db.py:977) is "
        "fully nullable. NOT NULL here causes runtime divergence."
    )


def test_won_quotes_db_create_does_not_use_default_clauses_that_diverge():
    """db.py defines the schema with no DEFAULTs on quantity/source.
    won_quotes_db pre-fix had DEFAULT 1 on quantity and DEFAULT
    'scprs_live' on source — meaning a fresh DB inserted via won_quotes_db
    would write different values than the same INSERT against db.py's
    table. confidence DEFAULT 1.0 is in BOTH so it's fine."""
    import pathlib
    src = pathlib.Path(
        "src/knowledge/won_quotes_db.py"
    ).read_text(encoding="utf-8")
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS won_quotes\s*\((.*?)\)\s*\"\"\"",
        src, re.DOTALL,
    )
    create_block = m.group(1)

    # confidence DEFAULT 1.0 IS canonical (matches db.py). Only fail on
    # the divergent ones.
    assert "DEFAULT 1\n" not in create_block + "\n", \
        "S-13 regression: quantity DEFAULT 1 diverges from db.py canonical"
    assert "DEFAULT 'scprs_live'" not in create_block, \
        "S-13 regression: source DEFAULT 'scprs_live' diverges from db.py"


def test_won_quotes_db_create_aligns_with_db_py():
    """Belt-and-suspenders: run both CREATE TABLE statements in sequence
    against an in-memory SQLite. They must produce IDENTICAL pragma
    table_info output (column count + types + nullability)."""
    import sqlite3
    import pathlib

    db_src = pathlib.Path("src/core/db.py").read_text(encoding="utf-8")
    wq_src = pathlib.Path(
        "src/knowledge/won_quotes_db.py"
    ).read_text(encoding="utf-8")

    db_create = re.search(
        r"(CREATE TABLE IF NOT EXISTS won_quotes\s*\(.*?\));",
        db_src, re.DOTALL,
    ).group(1)
    wq_create = re.search(
        r"(CREATE TABLE IF NOT EXISTS won_quotes\s*\(.*?\))\s*\"\"\"",
        wq_src, re.DOTALL,
    ).group(1)

    def schema_signature(sql):
        c = sqlite3.connect(":memory:")
        c.execute(sql)
        rows = c.execute("PRAGMA table_info(won_quotes)").fetchall()
        # (cid, name, type, notnull, dflt_value, pk) — drop cid for stability.
        return [(r[1], r[2], r[3], r[5]) for r in rows]

    sig_db = schema_signature(db_create)
    sig_wq = schema_signature(wq_create)
    assert sig_db == sig_wq, (
        f"S-13: won_quotes schemas diverge.\n"
        f"  db.py:           {sig_db}\n"
        f"  won_quotes_db.py:{sig_wq}"
    )
