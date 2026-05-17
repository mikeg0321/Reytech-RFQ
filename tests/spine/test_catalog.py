"""Spine catalog substrate tests.

Covers:
- observe() upsert behavior on first + repeat observations
- description / uom / unspsc union over time
- last_priced_at update only when cost_cents > 0
- get_entry + iter_entries read paths
- find_stale_priced_entries (task #22 signal)
- record_enrichment (task #23 substrate)
- MFG# normalization (mirrors quote_matcher / auto_pricer)
- Input validation
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.spine.catalog import (
    CATALOG_STALENESS_DAYS,
    ENRICHMENT_FAILED,
    ENRICHMENT_FETCHED,
    ENRICHMENT_PENDING,
    find_stale_priced_entries,
    get_entry,
    iter_entries,
    observe,
    record_enrichment,
)
from src.spine.db import init_db
from src.spine.model import SpineValidationError


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_catalog.db"
    init_db(str(p))
    return str(p)


# ──────────────────────────────────────────────────────────────────────
# observe — first call creates row
# ──────────────────────────────────────────────────────────────────────


def test_observe_creates_row_on_first_call(db_path):
    out = observe(
        db_path,
        mfg_number="MFG-A",
        description="Bandage, sterile, 4x4",
        uom="BX",
        quote_id="rfq_001",
        actor="ingest",
    )
    assert out["created"] is True
    assert out["seen_count"] == 1
    assert out["mfg_number"] == "MFG-A"
    assert out["catalog_id"].startswith("cat_")


def test_observe_persists_full_row(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            uom="BX", quote_id="rfq_001", actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry is not None
    assert entry["mfg_number"] == "MFG-A"
    assert entry["canonical_description"] == "Bandage"
    assert entry["descriptions"] == ["Bandage"]
    assert entry["uoms_seen"] == ["BX"]
    assert entry["unspsc_codes"] == []
    assert entry["seen_count"] == 1
    assert entry["last_seen_quote_id"] == "rfq_001"
    assert entry["last_priced_at"] is None
    assert entry["last_priced_cents"] is None
    assert entry["enrichment_status"] == ENRICHMENT_PENDING


# ──────────────────────────────────────────────────────────────────────
# observe — repeat updates seen_count + unions
# ──────────────────────────────────────────────────────────────────────


def test_observe_repeat_increments_seen_count(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_001", actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_002", actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_003", actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["seen_count"] == 3
    assert entry["last_seen_quote_id"] == "rfq_003"


def test_observe_unions_new_descriptions(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage, 4x4",
            quote_id="rfq_001", actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage 4-inch sterile",
            quote_id="rfq_002", actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["descriptions"] == ["Bandage, 4x4", "Bandage 4-inch sterile"]


def test_observe_does_not_duplicate_same_description(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage 4x4",
            quote_id="rfq_001", actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage 4x4",
            quote_id="rfq_002", actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["descriptions"] == ["Bandage 4x4"]


def test_observe_unions_uoms(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            uom="BX", quote_id="rfq_001", actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            uom="CS", quote_id="rfq_002", actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["uoms_seen"] == ["BX", "CS"]


def test_observe_unions_unspsc(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            unspsc="42312001", quote_id="rfq_001", actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            unspsc="42312002", quote_id="rfq_002", actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["unspsc_codes"] == ["42312001", "42312002"]


def test_observe_updates_canonical_description_to_latest(db_path):
    observe(db_path, mfg_number="MFG-A", description="OLD desc",
            quote_id="rfq_001", actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="NEW desc",
            quote_id="rfq_002", actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["canonical_description"] == "NEW desc"


# ──────────────────────────────────────────────────────────────────────
# observe — last_priced_at updates only when cost > 0
# ──────────────────────────────────────────────────────────────────────


def test_observe_records_last_priced_when_cost_positive(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_priced", cost_cents=4200, actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["last_priced_cents"] == 4200
    assert entry["last_priced_quote_id"] == "rfq_priced"
    assert entry["last_priced_at"] is not None


def test_observe_does_not_record_price_when_zero(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_unpriced", cost_cents=0, actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["last_priced_at"] is None
    assert entry["last_priced_cents"] is None


def test_observe_preserves_prior_price_on_unpriced_repeat(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_p1", cost_cents=4200, actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_unpriced", cost_cents=None, actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["last_priced_cents"] == 4200
    assert entry["last_priced_quote_id"] == "rfq_p1"


def test_observe_updates_price_on_repriced(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_p1", cost_cents=4200, actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_p2", cost_cents=4500, actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry["last_priced_cents"] == 4500
    assert entry["last_priced_quote_id"] == "rfq_p2"


# ──────────────────────────────────────────────────────────────────────
# MFG# normalization
# ──────────────────────────────────────────────────────────────────────


def test_observe_normalizes_mfg_case_whitespace(db_path):
    observe(db_path, mfg_number="  mfg-a ", description="Bandage",
            quote_id="rfq_001", actor="ingest")
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_002", actor="ingest")
    entry = get_entry(db_path, "mfg-a")
    assert entry is not None
    assert entry["seen_count"] == 2
    # Both lookups hit the same row.
    assert get_entry(db_path, "MFG-A")["catalog_id"] == entry["catalog_id"]


def test_observe_strips_trailing_punctuation(db_path):
    observe(db_path, mfg_number="MFG-A.", description="Bandage",
            quote_id="rfq_001", actor="ingest")
    entry = get_entry(db_path, "MFG-A")
    assert entry is not None


# ──────────────────────────────────────────────────────────────────────
# iter_entries / read path
# ──────────────────────────────────────────────────────────────────────


def test_iter_entries_returns_all_sorted_by_last_seen_desc(db_path):
    observe(db_path, mfg_number="MFG-A", description="A",
            quote_id="rfq_a", actor="ingest")
    observe(db_path, mfg_number="MFG-B", description="B",
            quote_id="rfq_b", actor="ingest")
    observe(db_path, mfg_number="MFG-C", description="C",
            quote_id="rfq_c", actor="ingest")
    entries = iter_entries(db_path)
    assert len(entries) == 3
    # last-seen most-recent (MFG-C) first.
    assert entries[0]["mfg_number"] == "MFG-C"
    assert entries[-1]["mfg_number"] == "MFG-A"


def test_iter_entries_filters_by_enrichment_status(db_path):
    observe(db_path, mfg_number="MFG-A", description="A",
            quote_id="rfq_a", actor="ingest")
    pending = iter_entries(db_path, enrichment_status=ENRICHMENT_PENDING)
    assert len(pending) == 1
    fetched = iter_entries(db_path, enrichment_status=ENRICHMENT_FETCHED)
    assert fetched == []


def test_iter_entries_respects_limit(db_path):
    for i in range(5):
        observe(db_path, mfg_number=f"MFG-{i}", description=f"D{i}",
                quote_id=f"rfq_{i}", actor="ingest")
    assert len(iter_entries(db_path, limit=3)) == 3


def test_get_entry_returns_none_when_absent(db_path):
    assert get_entry(db_path, "MFG-ZZZZ") is None


def test_get_entry_returns_none_when_mfg_empty(db_path):
    assert get_entry(db_path, "") is None
    assert get_entry(db_path, "   ") is None


# ──────────────────────────────────────────────────────────────────────
# find_stale_priced_entries (task #22)
# ──────────────────────────────────────────────────────────────────────


def test_find_stale_priced_returns_only_old_priced_entries(db_path):
    """Fresh observation isn't stale; ancient priced row is."""
    observe(db_path, mfg_number="MFG-FRESH", description="fresh",
            quote_id="rfq_f", cost_cents=4200, actor="ingest")

    # Forcibly stamp an ancient last_priced_at on a second entry by
    # poking sqlite directly (the substrate's only writer rule applies
    # to spine_quotes; this test reaches into spine_catalog for setup).
    import sqlite3
    observe(db_path, mfg_number="MFG-STALE", description="stale",
            quote_id="rfq_s", cost_cents=4200, actor="ingest")
    long_ago = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE spine_catalog SET last_priced_at = ? WHERE mfg_number = ?",
        (long_ago, "MFG-STALE"),
    )
    conn.commit()
    conn.close()

    stale = find_stale_priced_entries(db_path)
    mfgs = [e["mfg_number"] for e in stale]
    assert "MFG-STALE" in mfgs
    assert "MFG-FRESH" not in mfgs


def test_find_stale_priced_skips_unpriced(db_path):
    """Unpriced entries don't appear regardless of age."""
    observe(db_path, mfg_number="MFG-UNPRICED", description="x",
            quote_id="rfq_u", actor="ingest")
    assert find_stale_priced_entries(db_path) == []


def test_find_stale_priced_custom_days(db_path):
    """Caller can lower threshold to flag more aggressive staleness."""
    observe(db_path, mfg_number="MFG-RECENT", description="x",
            quote_id="rfq_r", cost_cents=4200, actor="ingest")

    import sqlite3
    days_2_ago = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE spine_catalog SET last_priced_at = ? WHERE mfg_number = ?",
        (days_2_ago, "MFG-RECENT"),
    )
    conn.commit()
    conn.close()

    # Default 30-day window: not stale.
    assert find_stale_priced_entries(db_path) == []
    # 1-day window: stale.
    stale = find_stale_priced_entries(db_path, days=1)
    assert len(stale) == 1


def test_find_stale_priced_sorted_oldest_first(db_path):
    """Worst staleness surfaces at the top of the list."""
    import sqlite3
    observe(db_path, mfg_number="MFG-MED", description="x",
            quote_id="rfq_m", cost_cents=1000, actor="ingest")
    observe(db_path, mfg_number="MFG-OLDEST", description="x",
            quote_id="rfq_o", cost_cents=1000, actor="ingest")
    observe(db_path, mfg_number="MFG-RECENT-STALE", description="x",
            quote_id="rfq_r", cost_cents=1000, actor="ingest")

    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE spine_catalog SET last_priced_at = ? WHERE mfg_number = ?",
                 ((datetime.now(timezone.utc) - timedelta(days=200)).isoformat(),
                  "MFG-OLDEST"))
    conn.execute("UPDATE spine_catalog SET last_priced_at = ? WHERE mfg_number = ?",
                 ((datetime.now(timezone.utc) - timedelta(days=90)).isoformat(),
                  "MFG-MED"))
    conn.execute("UPDATE spine_catalog SET last_priced_at = ? WHERE mfg_number = ?",
                 ((datetime.now(timezone.utc) - timedelta(days=45)).isoformat(),
                  "MFG-RECENT-STALE"))
    conn.commit()
    conn.close()

    stale = find_stale_priced_entries(db_path)
    mfgs = [e["mfg_number"] for e in stale]
    assert mfgs == ["MFG-OLDEST", "MFG-MED", "MFG-RECENT-STALE"]


def test_constant_matches_line_freshness_window():
    """The catalog's staleness window mirrors the per-LineItem cost
    freshness gate so an entry that's stale here is also stale at
    the finalize transition."""
    from src.spine.model import COST_VALIDATION_FRESHNESS_DAYS
    assert CATALOG_STALENESS_DAYS == COST_VALIDATION_FRESHNESS_DAYS


# ──────────────────────────────────────────────────────────────────────
# record_enrichment (task #23 substrate)
# ──────────────────────────────────────────────────────────────────────


def test_record_enrichment_updates_urls_and_status(db_path):
    observe(db_path, mfg_number="MFG-A", description="Bandage",
            quote_id="rfq_001", actor="ingest")
    record_enrichment(
        db_path,
        mfg_number="MFG-A",
        source_url="https://supplier.example.com/sku/MFG-A",
        photo_url="https://supplier.example.com/img/MFG-A.jpg",
        photo_path="/data/catalog/MFG-A.jpg",
        status=ENRICHMENT_FETCHED,
        actor="enrichment_fetcher",
    )
    entry = get_entry(db_path, "MFG-A")
    assert entry["source_url"] == "https://supplier.example.com/sku/MFG-A"
    assert entry["photo_url"] == "https://supplier.example.com/img/MFG-A.jpg"
    assert entry["photo_path"] == "/data/catalog/MFG-A.jpg"
    assert entry["enrichment_status"] == ENRICHMENT_FETCHED
    assert entry["source_url_checked_at"] is not None


def test_record_enrichment_failure_stamps_status(db_path):
    observe(db_path, mfg_number="MFG-A", description="x",
            quote_id="rfq_001", actor="ingest")
    record_enrichment(db_path, mfg_number="MFG-A",
                      status=ENRICHMENT_FAILED, actor="fetcher")
    entry = get_entry(db_path, "MFG-A")
    assert entry["enrichment_status"] == ENRICHMENT_FAILED
    # URLs stay None — fetcher didn't find anything.
    assert entry["source_url"] is None


def test_record_enrichment_preserves_existing_fields(db_path):
    observe(db_path, mfg_number="MFG-A", description="x",
            uom="BX", quote_id="rfq_001", cost_cents=4200, actor="ingest")
    record_enrichment(db_path, mfg_number="MFG-A",
                      source_url="https://x.com/a",
                      status=ENRICHMENT_FETCHED, actor="fetcher")
    entry = get_entry(db_path, "MFG-A")
    # Catalog data is untouched.
    assert entry["canonical_description"] == "x"
    assert entry["last_priced_cents"] == 4200
    assert entry["uoms_seen"] == ["BX"]
    # Enrichment data is set.
    assert entry["source_url"] == "https://x.com/a"


def test_record_enrichment_rejects_unknown_mfg(db_path):
    with pytest.raises(SpineValidationError, match="no catalog entry"):
        record_enrichment(db_path, mfg_number="MFG-ABSENT",
                          status=ENRICHMENT_FETCHED, actor="fetcher")


def test_record_enrichment_rejects_invalid_status(db_path):
    observe(db_path, mfg_number="MFG-A", description="x",
            quote_id="rfq_001", actor="ingest")
    with pytest.raises(SpineValidationError, match="status must"):
        record_enrichment(db_path, mfg_number="MFG-A",
                          status="wat", actor="fetcher")


# ──────────────────────────────────────────────────────────────────────
# Input validation
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("bad_mfg", ["", "   ", None])
def test_observe_rejects_empty_mfg(db_path, bad_mfg):
    with pytest.raises(SpineValidationError):
        observe(db_path, mfg_number=bad_mfg, description="x",  # type: ignore[arg-type]
                quote_id="rfq", actor="ingest")


@pytest.mark.parametrize("bad_desc", ["", "   "])
def test_observe_rejects_empty_description(db_path, bad_desc):
    with pytest.raises(SpineValidationError):
        observe(db_path, mfg_number="MFG-A", description=bad_desc,
                quote_id="rfq", actor="ingest")


def test_observe_rejects_negative_cost(db_path):
    with pytest.raises(SpineValidationError, match="cost_cents"):
        observe(db_path, mfg_number="MFG-A", description="x",
                quote_id="rfq", cost_cents=-1, actor="ingest")


def test_observe_rejects_empty_actor(db_path):
    with pytest.raises(SpineValidationError):
        observe(db_path, mfg_number="MFG-A", description="x",
                quote_id="rfq", actor="")
