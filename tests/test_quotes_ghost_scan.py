"""Tests for `src.core.quotes_ghost_scan` — read-only ghost-marker walk
over the quotes table.

Background — PR #675 + PR #699 stopped *new* ghost-bound quote
allocations. This module audits the existing rows that were burned
before the gates landed (per session memo
`project_session_2026_05_01_ghost_quote_arc.md` punch-list item #4).

The scan is read-only by contract — these tests pin that:
  - placeholder source RFQ → quote bucketed as `placeholder_source`
  - placeholder source PC  → quote bucketed as `placeholder_source`
  - orphaned source pointer → bucketed as `orphaned_source`
  - own-data ghost markers (Reytech buyer / empty draft) → `own_markers`
  - no source link + sparse data → `no_source`
  - clean quote → `clean` (not flagged)
  - test rows excluded by default
  - the scanner never writes (DB unchanged after run)
"""
from __future__ import annotations

import json
import os
from datetime import datetime

import pytest


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("quotes", "rfqs", "price_checks"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()
    # `_load_price_checks` has a 30s in-process cache; bust it so tests
    # that seed a PC after a sibling test ran see fresh data.
    try:
        from src.api import data_layer as _dl
        _dl._pc_cache = None
        _dl._pc_cache_time = 0
    except Exception:
        pass


def _seed_quote(
    conn, *, quote_number: str, agency: str = "CDCR",
    total: float = 100.0, status: str = "sent",
    contact_email: str = "buyer@calvet.ca.gov",
    institution: str = "VHC-Yountville",
    items_count: int = 1,
    items_detail: str = '[{"qty":1,"description":"x"}]',
    rfq_number: str = "",
    source_rfq_id: str = "",
    source_pc_id: str = "",
    is_test: int = 0,
):
    when = datetime.now().isoformat()
    conn.execute(
        """INSERT INTO quotes (quote_number, created_at, agency, institution,
                               contact_email, rfq_number,
                               source_rfq_id, source_pc_id,
                               total, items_count, items_detail,
                               status, is_test)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (quote_number, when, agency, institution, contact_email, rfq_number,
         source_rfq_id, source_pc_id, total, items_count, items_detail,
         status, is_test),
    )
    conn.commit()


def _seed_rfq_db(conn, *, rid: str, sol: str = "8955-00001234",
                 buyer: str = "buyer@calvet.ca.gov",
                 line_items=None) -> None:
    """Seed an RFQ into SQLite — that's where `load_rfqs()` reads from.

    Mirrors `_save_single_rfq`: full record stored in `data_json` blob
    plus key columns shadowed for indexability. The scanner round-trips
    via `data_json`, so the gate sees the original `solicitation_number`.
    """
    if line_items is None:
        line_items = [{"qty": 1, "description": "Real product",
                       "price_per_unit": 100.0}]
    rfq = {
        "id": rid,
        "solicitation_number": sol,
        "rfq_number": sol,
        "requestor_email": buyer,
        "line_items": line_items,
    }
    when = datetime.now().isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO rfqs
           (id, received_at, agency, institution, requestor_name, requestor_email,
            rfq_number, items, status, source, email_uid, notes, updated_at, data_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (rid, when, "CalVet", "VHC", "", buyer,
         sol, json.dumps(line_items), "new", "test", "", "",
         when, json.dumps(rfq)),
    )
    conn.commit()


def _seed_pc_db(conn, *, pc_id: str, pc_number: str = "PC-2026-0001",
                buyer: str = "buyer@calvet.ca.gov", items=None) -> None:
    """Seed a PC into SQLite — that's where `_load_price_checks()` reads from."""
    if items is None:
        items = [{"qty": 1, "description": "Real product", "unit_price": 100.0}]
    pc = {
        "id": pc_id,
        "pc_number": pc_number,
        "requestor_email": buyer,
        "items": items,
    }
    when = datetime.now().isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO price_checks
           (id, created_at, requestor, agency, institution, items,
            source_file, quote_number, pc_number, total_items, status,
            email_uid, email_subject, due_date, pc_data, ship_to, data_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (pc_id, when, buyer, "CalVet", "VHC", json.dumps(items),
         "", "", pc_number, len(items), "parsed",
         "", "", "", "{}", "", json.dumps(pc)),
    )
    conn.commit()


# ── Placeholder source RFQ ──────────────────────────────────────────────


def test_quote_with_placeholder_source_rfq_is_ghost(temp_data_dir):
    """The R26Q45 incident shape: quote allocated against an RFQ with
    sol=WORKSHEET. The scan must flag this quote as placeholder_source."""
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_rfq_db(c, rid="rfq_ghost_1", sol="WORKSHEET")
        _seed_quote(c, quote_number="R26Q-G1",
                   source_rfq_id="rfq_ghost_1",
                   contact_email="buyer@calvet.ca.gov")

    report = scan_quotes()
    assert report["ghost_count"] == 1
    placeholder = report["by_bucket"]["placeholder_source"]
    assert len(placeholder) == 1
    cls = placeholder[0]
    assert cls["quote_number"] == "R26Q-G1"
    assert cls["source_kind"] == "rfq"
    assert any("WORKSHEET" in r for r in cls["reasons"]), (
        f"reasons should name the offending sol verbatim; got {cls['reasons']!r}"
    )


def test_quote_with_placeholder_source_pc_is_ghost(temp_data_dir):
    """Same shape on the PC side — quote allocated against a PC whose
    pc_number was a placeholder."""
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_pc_db(c, pc_id="pc_ghost_1", pc_number="WORKSHEET")
        _seed_quote(c, quote_number="R26Q-G2",
                   source_pc_id="pc_ghost_1",
                   contact_email="buyer@calvet.ca.gov")

    report = scan_quotes()
    assert report["ghost_count"] == 1
    placeholder = report["by_bucket"]["placeholder_source"]
    assert len(placeholder) == 1
    assert placeholder[0]["source_kind"] == "pc"


# ── Orphaned source pointer ─────────────────────────────────────────────


def test_quote_with_orphaned_rfq_pointer_is_ghost(temp_data_dir):
    """Quote references source_rfq_id but the RFQ is gone — bucket as
    orphaned_source."""
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q-O1",
                   source_rfq_id="rfq_was_deleted",
                   contact_email="buyer@calvet.ca.gov")

    report = scan_quotes()
    assert report["ghost_count"] == 1
    orphaned = report["by_bucket"]["orphaned_source"]
    assert len(orphaned) == 1
    assert orphaned[0]["quote_number"] == "R26Q-O1"
    assert orphaned[0]["source_kind"] == "orphaned"


# ── Own-data ghost markers ──────────────────────────────────────────────


def test_quote_with_reytech_buyer_is_ghost(temp_data_dir):
    """Reytech buyer email on a quote we sent — parser misclassified."""
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_rfq_db(c, rid="rfq_clean_1")
        _seed_quote(c, quote_number="R26Q-RB",
                   source_rfq_id="rfq_clean_1",
                   contact_email="sales@reytechinc.com")

    report = scan_quotes()
    assert report["ghost_count"] == 1
    own = report["by_bucket"]["own_markers"]
    assert len(own) == 1
    assert own[0]["quote_number"] == "R26Q-RB"
    assert any("reytech" in r.lower() for r in own[0]["reasons"])


def test_quote_with_empty_draft_pattern_is_ghost(temp_data_dir):
    """Empty draft: no institution, zero items, zero total — almost
    always abandoned in-flight."""
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q-ED",
                   contact_email="buyer@calvet.ca.gov",
                   institution="", total=0.0,
                   items_count=0, items_detail="[]",
                   source_rfq_id="", source_pc_id="", rfq_number="")

    report = scan_quotes()
    assert report["ghost_count"] == 1
    # No source link + own markers → bucketed as no_source (more
    # specific than own_markers, since the lack-of-source IS the
    # primary signal).
    bucket = (
        report["by_bucket"]["no_source"]
        + report["by_bucket"]["own_markers"]
    )
    assert len(bucket) == 1
    assert bucket[0]["quote_number"] == "R26Q-ED"


# ── Clean quote not flagged ─────────────────────────────────────────────


def test_clean_quote_not_flagged(temp_data_dir):
    """A quote built off a real RFQ with real buyer, real items,
    real institution must NOT appear in any ghost bucket."""
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_rfq_db(c, rid="rfq_clean_2", sol="8955-00001234")
        _seed_quote(c, quote_number="R26Q-OK",
                   source_rfq_id="rfq_clean_2",
                   contact_email="keith.alsing@calvet.ca.gov",
                   institution="VHC-Yountville",
                   total=2500.0, items_count=3,
                   items_detail='[{"qty":1},{"qty":2},{"qty":3}]')

    report = scan_quotes()
    assert report["ghost_count"] == 0
    assert report["clean_count"] == 1


# ── Test rows excluded by default ───────────────────────────────────────


def test_test_quotes_excluded_by_default(temp_data_dir):
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        # A test quote that WOULD be flagged if it were real.
        _seed_quote(c, quote_number="R26Q-TEST",
                   source_rfq_id="rfq_was_deleted",
                   is_test=1)

    report = scan_quotes()
    assert report["total_quotes"] == 0
    assert report["ghost_count"] == 0


def test_test_quotes_included_when_flag_set(temp_data_dir):
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q-TEST2",
                   source_rfq_id="rfq_was_deleted",
                   is_test=1)

    report = scan_quotes(include_test=True)
    assert report["total_quotes"] == 1
    assert report["ghost_count"] == 1


# ── No-write contract ───────────────────────────────────────────────────


def test_scanner_never_mutates_quotes_table(temp_data_dir):
    """The scan is read-only by contract. After a full run, every
    column on every quote row must be byte-identical."""
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_rfq_db(c, rid="rfq_mut_check", sol="WORKSHEET")
        _seed_quote(c, quote_number="R26Q-MUT",
                   source_rfq_id="rfq_mut_check",
                   contact_email="x@calvet.ca.gov",
                   total=1234.56, items_count=2)
        before = c.execute(
            "SELECT * FROM quotes WHERE quote_number=?", ("R26Q-MUT",)
        ).fetchone()
        before_dict = dict(before)

    scan_quotes()

    with _conn() as c:
        after = c.execute(
            "SELECT * FROM quotes WHERE quote_number=?", ("R26Q-MUT",)
        ).fetchone()
        after_dict = dict(after)

    assert before_dict == after_dict, (
        "scan_quotes mutated the quote row — it must be read-only"
    )


# ── Limit + bucketing sanity ────────────────────────────────────────────


def test_limit_truncates_walk(temp_data_dir):
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        for i in range(5):
            _seed_quote(c, quote_number=f"R26Q-L{i}",
                       source_rfq_id="rfq_was_deleted")

    report = scan_quotes(limit=2)
    assert report["total_quotes"] == 2


def test_multiple_buckets_in_one_scan(temp_data_dir):
    """End-to-end: a mix of ghost shapes + a clean row classifies
    correctly without cross-contamination."""
    from src.core.quotes_ghost_scan import scan_quotes
    with _conn() as c:
        _wipe(c)
        _seed_rfq_db(c, rid="rfq_clean", sol="8955-00001234")
        _seed_rfq_db(c, rid="rfq_ghost", sol="WORKSHEET")
        _seed_quote(c, quote_number="R26Q-CLEAN",
                   source_rfq_id="rfq_clean",
                   contact_email="buyer@calvet.ca.gov",
                   institution="VHC", total=500, items_count=1)
        _seed_quote(c, quote_number="R26Q-PLACE",
                   source_rfq_id="rfq_ghost",
                   contact_email="buyer@calvet.ca.gov")
        _seed_quote(c, quote_number="R26Q-ORPH",
                   source_rfq_id="missing_rfq",
                   contact_email="buyer@calvet.ca.gov")
        _seed_quote(c, quote_number="R26Q-RB2",
                   source_rfq_id="rfq_clean",
                   contact_email="sales@reytechinc.com")

    report = scan_quotes()
    assert report["total_quotes"] == 4
    assert report["clean_count"] == 1
    assert report["ghost_count"] == 3
    assert len(report["by_bucket"]["placeholder_source"]) == 1
    assert len(report["by_bucket"]["orphaned_source"]) == 1
    assert len(report["by_bucket"]["own_markers"]) == 1
