"""PR-N — dedup-at-ingest by pc_number.

Mike: "I had to mark a lot duplicate, because they kept showing in the
queue, even after sent." Re-polled emails with the same buyer pc_number
were creating fresh PC rows because dedup-by-email_uid only fires when
the UID matches.

Pinned guarantees:
  1. Two PCs with the same pc_number + same agency → second auto-marked
     duplicate at ingest, with closed_reason pointing at the first.
  2. Real-life prod case: pc_number=10844466 with different agencies
     (CCHCS vs CDCR) does NOT dedup — different state agencies number
     their quote requests independently.
  3. Placeholder pc_numbers (AUTO_xxx, RT-..., WORKSHEET, etc.) NEVER
     trigger dedup — every synthesized record stays distinct.
  4. The first PC isn't disturbed; only the new ingest row gets the
     duplicate stamp + closed_at + dedup_of pointer.
  5. status_history append matches the shape PR-M writes from
     status-change routes so /admin/funnel can read the reason.
  6. Already-duplicate / already-deleted existing rows are skipped —
     we never chain a dup-of-a-dup.
  7. Flag `ingest.dedup_by_pc_number_enabled=False` disables the check.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _init(tmp_path, monkeypatch):
    tmp_db = tmp_path / "dedup_test.db"
    monkeypatch.setenv("REYTECH_DB_PATH", str(tmp_db))
    import importlib
    from src.core import db as _db_mod
    importlib.reload(_db_mod)
    _db_mod.init_db()
    from src.core.migrations import run_migrations
    run_migrations()
    return _db_mod


def _seed_pc(db, pc_id, pc_number, agency, status="sent", created_at=None):
    """Insert a baseline PC row directly via SQL (bypasses _save_single_pc
    so the test isn't coupled to the save layer's auto-tagging side-effects)."""
    created_at = created_at or datetime.now().isoformat()
    with db.get_db() as conn:
        conn.execute(
            "INSERT INTO price_checks "
            "(id, pc_number, agency, institution, status, created_at, total_items, items) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (pc_id, pc_number, agency, agency, status, created_at, 0, "[]"),
        )


# ── Find helper ──────────────────────────────────────────────────────


def test_find_active_pc_by_number_returns_match(tmp_path, monkeypatch):
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_existing01", "10844466", "cchcs", status="sent")
    from src.core.ingest_pipeline import _find_active_pc_by_number
    hit = _find_active_pc_by_number("10844466", agency="cchcs")
    assert hit is not None
    assert hit["id"] == "pc_existing01"
    assert hit["status"] == "sent"


def test_find_skips_different_agency(tmp_path, monkeypatch):
    """CCHCS 10844466 + CDCR 10844466 are independent — confirmed in prod."""
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_cchcs", "10844466", "cchcs", status="sent")
    from src.core.ingest_pipeline import _find_active_pc_by_number
    hit = _find_active_pc_by_number("10844466", agency="cdcr")
    assert hit is None


def test_find_skips_placeholder_pc_numbers(tmp_path, monkeypatch):
    """Synthesized placeholders never collide.

    AUTO_xxx and short-all-caps sentinels (WORKSHEET / RFQ / GOOD) are
    rejected up front by `_looks_like_sol_placeholder`. RT-* numbers
    are NOT placeholders (they're our synthesized internal sol#s), but
    in practice each carries a unique uuid suffix so cross-record
    collision can't happen via the normal ingest path."""
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_a", "AUTO_abc123", "cchcs", status="parsed")
    _seed_pc(db, "pc_c", "WORKSHEET", "cchcs", status="parsed")
    from src.core.ingest_pipeline import _find_active_pc_by_number
    assert _find_active_pc_by_number("AUTO_abc123", agency="cchcs") is None
    assert _find_active_pc_by_number("WORKSHEET", agency="cchcs") is None
    assert _find_active_pc_by_number("", agency="cchcs") is None
    assert _find_active_pc_by_number(None, agency="cchcs") is None


def test_find_skips_already_duplicate_or_deleted(tmp_path, monkeypatch):
    """Don't chain a dup-of-a-dup."""
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_dup", "10838974", "cchcs", status="duplicate")
    _seed_pc(db, "pc_del", "10838974", "cchcs", status="deleted")
    _seed_pc(db, "pc_arc", "10838974", "cchcs", status="archived")
    from src.core.ingest_pipeline import _find_active_pc_by_number
    assert _find_active_pc_by_number("10838974", agency="cchcs") is None


def test_find_picks_active_over_terminal(tmp_path, monkeypatch):
    """When some rows are dup/deleted and one is sent, we still match the sent."""
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_dup", "10838974", "cchcs", status="duplicate")
    _seed_pc(db, "pc_sent", "10838974", "cchcs", status="sent")
    from src.core.ingest_pipeline import _find_active_pc_by_number
    hit = _find_active_pc_by_number("10838974", agency="cchcs")
    assert hit is not None
    assert hit["id"] == "pc_sent"


# ── Full ingest path ─────────────────────────────────────────────────


def _stub_classifier(monkeypatch, *, pc_number, agency, items_count=1):
    """Patch process_buyer_request's classify_request + parser so we don't
    need real fixture PDFs. We're testing the dedup branch only."""
    from src.core.request_classifier import RequestClassification

    fake_cls = RequestClassification(
        shape="pc_704_docx",
        agency=agency,
        institution=agency,
        solicitation_number=pc_number,
        primary_file="fake.pdf",
        is_quote_only=True,
        confidence=0.95,
        reasons=["test-stub"],
    )

    def _fake_classify(**kw):
        return fake_cls

    def _fake_dispatch(path, classification):
        items = [{"description": f"Item {i+1}", "qty": 1}
                 for i in range(items_count)]
        header = {"pc_number": pc_number}
        return items, header, None

    # process_buyer_request runs `from src.core.request_classifier import
    # classify_request` inside the function on each call — patch the
    # source module so the import resolves to our stub.
    import src.core.request_classifier as rc
    monkeypatch.setattr(rc, "classify_request", _fake_classify)
    import src.core.ingest_pipeline as ip
    monkeypatch.setattr(ip, "_dispatch_parser", _fake_dispatch)
    # Skip the multi-attach + body-extract paths (they re-classify).
    monkeypatch.setattr(
        ip, "_multi_attachment_vision_union",
        lambda **kw: [],
    )
    # Skip ghost detection so we don't get noise.
    import src.core.ghost_detection as gd
    monkeypatch.setattr(gd, "detect_ghost_pattern",
                        lambda *a, **kw: "")


def test_ingest_dedups_second_email_for_same_pc_number(tmp_path, monkeypatch):
    """End-to-end: seed a sent PC, ingest another email with same pc_number,
    new row gets status='duplicate' + closed_reason + dedup_of."""
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_first", "10838974", "cchcs", status="sent")
    _stub_classifier(monkeypatch, pc_number="10838974", agency="cchcs")

    # Need a fake file path that exists (the dispatcher won't actually
    # read it because we stubbed `_dispatch_parser`).
    f = tmp_path / "fake.pdf"
    f.write_bytes(b"%PDF-1.4\n")

    from src.core.ingest_pipeline import process_buyer_request
    result = process_buyer_request(
        files=[str(f)],
        email_subject="Re: Price Check 10838974",
        email_sender="buyer@cchcs.ca.gov",
        email_uid="uid-different-from-first",
    )
    assert result.ok is True
    assert result.record_type == "pc"
    assert result.record_id and result.record_id != "pc_first"

    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status, pc_data FROM price_checks WHERE id=?",
            (result.record_id,),
        ).fetchone()
    assert row["status"] == "duplicate"
    import json
    pc_blob = json.loads(row["pc_data"])
    assert pc_blob["closed_reason"].startswith("auto-dedup: pc_number=10838974")
    assert "pc_first" in pc_blob["closed_reason"]
    assert pc_blob["dedup_of"] == "pc_first"
    assert pc_blob.get("closed_at")
    # status_history shape matches PR-M's contract
    hist = pc_blob.get("status_history") or []
    assert len(hist) == 1
    assert hist[0]["to"] == "duplicate"
    assert hist[0]["actor"] == "ingest_pipeline"
    assert hist[0]["reason"].startswith("auto-dedup")


def test_ingest_skips_dedup_for_different_agency(tmp_path, monkeypatch):
    """Same pc_number 10844466 but new ingest is CDCR — new PC stays active."""
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_cchcs_first", "10844466", "cchcs", status="sent")
    _stub_classifier(monkeypatch, pc_number="10844466", agency="cdcr")

    f = tmp_path / "fake.pdf"
    f.write_bytes(b"%PDF-1.4\n")

    from src.core.ingest_pipeline import process_buyer_request
    result = process_buyer_request(
        files=[str(f)],
        email_subject="Price Check 10844466",
        email_sender="buyer@cdcr.ca.gov",
        email_uid="uid-cdcr",
    )
    assert result.ok is True
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status FROM price_checks WHERE id=?",
            (result.record_id,),
        ).fetchone()
    # New record stays in parsed/needs_review — NOT duplicate
    assert row["status"] != "duplicate"


def test_ingest_first_pc_untouched_by_dedup(tmp_path, monkeypatch):
    """Dedup only stamps the new row; the original sent PC keeps its status."""
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_first", "10838974", "cchcs", status="sent")
    _stub_classifier(monkeypatch, pc_number="10838974", agency="cchcs")

    f = tmp_path / "fake.pdf"
    f.write_bytes(b"%PDF-1.4\n")

    from src.core.ingest_pipeline import process_buyer_request
    process_buyer_request(
        files=[str(f)],
        email_subject="Re: 10838974",
        email_sender="buyer@cchcs.ca.gov",
        email_uid="uid-second",
    )
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status FROM price_checks WHERE id=?",
            ("pc_first",),
        ).fetchone()
    assert row["status"] == "sent"  # untouched


def test_flag_disables_dedup(tmp_path, monkeypatch):
    """ingest.dedup_by_pc_number_enabled=False short-circuits the check."""
    db = _init(tmp_path, monkeypatch)
    _seed_pc(db, "pc_first", "10838974", "cchcs", status="sent")
    _stub_classifier(monkeypatch, pc_number="10838974", agency="cchcs")

    # Patch the flag lookup the helper uses.
    import src.core.flags as _flags

    def _flag(key, default=False):
        if key == "ingest.dedup_by_pc_number_enabled":
            return False
        return default

    monkeypatch.setattr(_flags, "get_flag", _flag)

    f = tmp_path / "fake.pdf"
    f.write_bytes(b"%PDF-1.4\n")

    from src.core.ingest_pipeline import process_buyer_request
    result = process_buyer_request(
        files=[str(f)],
        email_subject="Re: 10838974",
        email_sender="buyer@cchcs.ca.gov",
        email_uid="uid-flag-off",
    )
    with db.get_db() as conn:
        row = conn.execute(
            "SELECT status FROM price_checks WHERE id=?",
            (result.record_id,),
        ).fetchone()
    assert row["status"] != "duplicate"
