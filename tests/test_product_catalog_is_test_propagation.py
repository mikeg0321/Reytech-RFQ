"""Regression: PC→catalog writes propagate is_test so test data is tagged.

RE-AUDIT-4 (P1 reduced). The `product_catalog` table has an `is_test` column
(schema line 86 of product_catalog.py; migration 0 adds it on existing DBs)
and the smart_search/_smart_rank/match_item read paths all filter
`is_test=0`. But the three PC→catalog write funnels
(`save_pc_items_to_catalog`, `record_outcome_to_catalog`, and the shared
`add_to_catalog` INSERT) never populated the column. That meant every row
defaulted `is_test=0`, so test PCs silently seeded rows that ranked in live
quoting results.

Fix: add `is_test: int = 0` kwarg to `add_to_catalog`, include it in the
INSERT statement, and have both PC wrappers pull `pc.get("is_test")` once
per call and pass through.

This test is a source-level guard — it asserts the signature, the INSERT,
and the kwarg plumbing are all present. That avoids having to spin up a
real SQLite fixture for a multi-file propagation change.
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_CATALOG = _REPO / "src" / "agents" / "product_catalog.py"


def _read() -> str:
    return _CATALOG.read_text(encoding="utf-8")


def _slice_fn(body: str, def_prefix: str) -> str:
    start = body.find(def_prefix)
    assert start >= 0, f"{def_prefix!r} not found in product_catalog.py"
    nxt = body.find("\ndef ", start + 1)
    return body[start:nxt] if nxt > 0 else body[start:]


def test_add_to_catalog_accepts_is_test_kwarg():
    body = _read()
    sig_start = body.find("def add_to_catalog(")
    assert sig_start >= 0
    sig_end = body.find("):", sig_start)
    sig = body[sig_start:sig_end]
    assert "is_test" in sig, (
        "RE-AUDIT-4 regression: add_to_catalog signature dropped `is_test`. "
        "PC wrappers rely on it to mark test rows."
    )
    assert "is_test: int = 0" in sig or "is_test:int=0" in sig or "is_test=0" in sig, (
        "RE-AUDIT-4 regression: add_to_catalog `is_test` default changed "
        "away from 0. Default MUST be 0 so callers that don't know about "
        "the flag (CSV imports, award_tracker) don't accidentally tag "
        "production rows as test."
    )


def test_insert_into_product_catalog_populates_is_test_column():
    """The INSERT statement must mention is_test in the column list AND
    bind it in the value tuple. Anything less and we ship rows that look
    like prod even when the caller passed is_test=1."""
    body = _read()
    fn = _slice_fn(body, "def add_to_catalog(")
    # Column list must include is_test
    insert_start = fn.find("INSERT INTO product_catalog (")
    assert insert_start >= 0, "add_to_catalog no longer INSERTs into product_catalog"
    insert_end = fn.find(")", insert_start)
    columns = fn[insert_start:insert_end]
    assert "is_test" in columns, (
        "RE-AUDIT-4 regression: add_to_catalog INSERT no longer mentions "
        "is_test in the column list. Rows will silently default to 0 "
        "whatever the caller passes."
    )
    # Value bind must reference the local is_test parameter
    assert "bool(is_test)" in fn or "int(is_test)" in fn, (
        "RE-AUDIT-4 regression: add_to_catalog INSERT no longer binds the "
        "`is_test` kwarg. The column might be listed but the value row won't "
        "include it — SQLite will raise `values do not match columns`."
    )


def test_save_pc_items_to_catalog_propagates_is_test_from_pc():
    fn = _slice_fn(_read(), "def save_pc_items_to_catalog(")
    assert "pc.get(\"is_test\")" in fn, (
        "RE-AUDIT-4 regression: save_pc_items_to_catalog no longer reads "
        "is_test from the PC dict. Test PCs will seed prod catalog rows."
    )
    assert "is_test=pc_is_test" in fn, (
        "RE-AUDIT-4 regression: save_pc_items_to_catalog no longer passes "
        "is_test to add_to_catalog."
    )


def test_record_outcome_to_catalog_propagates_is_test_from_pc():
    fn = _slice_fn(_read(), "def record_outcome_to_catalog(")
    assert "pc.get(\"is_test\")" in fn, (
        "RE-AUDIT-4 regression: record_outcome_to_catalog no longer reads "
        "is_test from the PC. A test mark-won/lost will create real "
        "catalog rows through the feedback-loop path."
    )
    assert "is_test=pc_is_test" in fn, (
        "RE-AUDIT-4 regression: record_outcome_to_catalog no longer passes "
        "is_test to add_to_catalog."
    )


def test_smart_search_still_filters_is_test():
    """If the write path gets plumbed but the read path stops filtering,
    the whole defense collapses. Guard that smart_search still excludes
    is_test=1 rows."""
    body = _read()
    fn = _slice_fn(body, "def smart_search(")
    assert "is_test" in fn, (
        "RE-AUDIT-4 regression: smart_search no longer filters is_test. "
        "Without this the propagation work is moot — test rows will rank "
        "in live quoting."
    )


def test_is_test_default_is_safe_for_csv_importers():
    """CSV importers (QB / QuoteWerks) don't know about is_test.
    add_to_catalog's default MUST be 0 so they keep producing prod rows."""
    body = _read()
    sig_start = body.find("def add_to_catalog(")
    sig_end = body.find("):", sig_start)
    sig = body[sig_start:sig_end]
    # Normalize whitespace for regex match
    norm = "".join(sig.split())
    assert "is_test:int=0" in norm or "is_test=0" in norm, (
        "RE-AUDIT-4 regression: add_to_catalog `is_test` default MUST remain 0 — "
        "QB/QuoteWerks CSV importers call this with no is_test kwarg and "
        "must keep producing prod rows."
    )
