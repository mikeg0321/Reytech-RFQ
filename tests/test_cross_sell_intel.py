"""Cross-sell intel — aggregation, ranking, recommendations.

Mike P0 2026-05-11 needle-mover #2. The cross_sell_intel module
aggregates scprs_po_lines.reytech_sells=1 rows into a ranked prospect
list + per-category rollup + actionable recommendations.

These tests pin:
  * Noise filters (Services bucket, empty buyer_email, line_total bounds)
  * Recency-weighted ranking (Mike's "no" to pure-$ ranking)
  * Recommendation generation structure
"""
from __future__ import annotations

import sqlite3

import pytest

from src.agents.cross_sell_intel import (
    _days_since,
    _freshness_tier,
    _recency_decay,
    get_prospects,
    get_top_items_by_spend,
    get_general_recommendations,
)


# ─── _days_since helper ──────────────────────────────────────────────────


def test_days_since_handles_multiple_formats():
    """Accepts mm/dd/yyyy + yyyy-mm-dd + mm-dd-yyyy."""
    # 30 days ago in different formats should all parse
    from datetime import datetime, timedelta, timezone
    d = datetime.now(timezone.utc) - timedelta(days=30)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        s = d.strftime(fmt)
        days = _days_since(s)
        assert days is not None
        assert 29 <= days <= 31, f"expected ~30 days for {fmt} {s}, got {days}"


def test_days_since_none_on_unparseable():
    assert _days_since(None) is None
    assert _days_since("") is None
    assert _days_since("garbage") is None


# ─── _recency_decay ──────────────────────────────────────────────────────


def test_recency_decay_today_is_one():
    """Bought today → full weight."""
    assert _recency_decay(0) == pytest.approx(1.0, abs=0.001)


def test_recency_decay_half_life():
    """Default half-life is 90 days — 90-day-old buy gets 0.5 weight."""
    assert _recency_decay(90) == pytest.approx(0.5, abs=0.001)


def test_recency_decay_old_buy_dampened():
    """A 365-day-old buy gets <0.1 weight — outranked by recent activity."""
    assert _recency_decay(365) < 0.1


def test_recency_decay_none_returns_zero():
    """Records with no date — treat as max-decay (don't surface)."""
    assert _recency_decay(None) == 0.0


# ─── DB fixture helpers ──────────────────────────────────────────────────


def _ensure_is_test_columns(conn):
    """Migrations 22-23 add is_test to scprs tables; they don't run in
    tests, so add the columns inline. Idempotent."""
    for table in ("scprs_po_master", "scprs_po_lines"):
        try:
            conn.execute(
                f"ALTER TABLE {table} ADD COLUMN is_test INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass  # column already exists


@pytest.fixture
def seeded_db(temp_data_dir):
    """Seed scprs_po_master + scprs_po_lines with a mix of signal + noise.

    The dataset shape:
      - buyer A: $5000 nitrile from Echelon, 10 days ago      (signal)
      - buyer B: $20000 nitrile from Echelon, 200 days ago    (older signal)
      - buyer C: $1000 services bucket (NOISE — Services SKU)
      - buyer D: $50 N95 (signal)
      - no-buyer-email row: $99999 nitrile (NOISE — empty buyer)
      - $200000 line (NOISE — exceeds 100k cap)
    """
    from src.core.db import get_db
    with get_db() as conn:
        _ensure_is_test_columns(conn)
        conn.executescript("""
            DELETE FROM scprs_po_lines;
            DELETE FROM scprs_po_master;
        """)
        # Master rows
        rows = [
            (1, "PO-A", "DEPT1", "CDCR", "alice@cdcr.ca.gov", "Alice", "Echelon", 0),
            (2, "PO-B", "DEPT1", "CDCR", "bob@cdcr.ca.gov", "Bob", "McKesson", 0),
            (3, "PO-C", "DEPT1", "CDCR", "carol@cdcr.ca.gov", "Carol", "Cardinal", 0),
            (4, "PO-D", "DEPT1", "CDCR", "dave@cdcr.ca.gov", "Dave", "Echelon", 0),
            (5, "PO-NOBUYER", "DEPT1", "CDCR", None, None, "Echelon", 0),
            (6, "PO-HUGE", "DEPT1", "CDCR", "huge@cdcr.ca.gov", "Huge", "Echelon", 0),
            (7, "PO-REYTECH", "DEPT1", "CDCR", "rey@cdcr.ca.gov", "Rey", "Reytech Inc.", 0),
        ]
        from datetime import datetime, timedelta
        recent = (datetime.now() - timedelta(days=10)).strftime("%m/%d/%Y")
        old = (datetime.now() - timedelta(days=200)).strftime("%m/%d/%Y")
        for pid, po_num, dept_code, dept_name, email, name, supplier, is_test in rows:
            conn.execute(
                "INSERT INTO scprs_po_master "
                "(id, po_number, dept_code, dept_name, buyer_email, buyer_name, "
                " supplier, start_date, is_test) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (pid, po_num, dept_code, dept_name, email, name, supplier,
                 recent if pid != 2 else old, is_test)
            )

        # Line rows
        lines = [
            # (po_id, desc, sku, category, qty, unit_price, line_total, sells, opp_flag)
            (1, "Nitrile gloves M", "NITRILE-M", "exam_gloves", 100, 50, 5000, 1, "WIN_BACK"),
            (2, "Nitrile gloves L", "NITRILE-M", "exam_gloves", 400, 50, 20000, 1, "WIN_BACK"),
            (3, "Service contract", "Services", "general", 1, 1000, 1000, 1, "WIN_BACK"),  # NOISE
            (4, "N95 respirator", "N95-3M8210", "respiratory", 5, 10, 50, 1, "WIN_BACK"),
            (5, "Nitrile gloves", "NITRILE-M", "exam_gloves", 1, 99999, 99999, 1, "WIN_BACK"),  # NOISE
            (6, "Huge line", "NITRILE-M", "exam_gloves", 1, 200000, 200000, 1, "WIN_BACK"),  # NOISE
            (7, "Nitrile from Reytech", "NITRILE-M", "exam_gloves", 1, 100, 100, 1, "WIN_BACK"),  # noise (Reytech itself)
        ]
        for po_id, desc, sku, cat, qty, unit_p, line_total, sells, flag in lines:
            conn.execute(
                "INSERT INTO scprs_po_lines "
                "(po_id, po_number, line_num, description, reytech_sku, category, "
                " quantity, unit_price, line_total, reytech_sells, opportunity_flag, is_test) "
                "VALUES (?,?,1,?,?,?,?,?,?,?,?,0)",
                (po_id, f"PO-{po_id}", desc, sku, cat, qty, unit_p,
                 line_total, sells, flag)
            )
        conn.commit()
    yield


# ─── get_prospects filters ───────────────────────────────────────────────


def test_get_prospects_filters_services_sku(seeded_db):
    """Carol's 'Services' SKU row must NOT appear in prospects (noise)."""
    out = get_prospects(top_n=10)
    emails = [p["buyer_email"] for p in out]
    assert "carol@cdcr.ca.gov" not in emails


def test_get_prospects_filters_empty_buyer_email(seeded_db):
    """The PO-NOBUYER row had no buyer_email — must NOT contribute."""
    out = get_prospects(top_n=10)
    # None buyer should be absent
    for p in out:
        assert p["buyer_email"] not in (None, "")


def test_get_prospects_filters_huge_line_totals(seeded_db):
    """Line totals >= 100k are noise (service contracts) — excluded."""
    out = get_prospects(top_n=10)
    emails = [p["buyer_email"] for p in out]
    assert "huge@cdcr.ca.gov" not in emails


def test_get_prospects_filters_reytech_as_supplier(seeded_db):
    """When the 'competitor' supplier IS Reytech, exclude — that's our own sale."""
    out = get_prospects(top_n=10)
    emails = [p["buyer_email"] for p in out]
    assert "rey@cdcr.ca.gov" not in emails


def test_get_prospects_keeps_signal_rows(seeded_db):
    """Alice ($5K recent), Bob ($20K old), Dave ($50 recent) all surface."""
    out = get_prospects(top_n=10)
    emails = [p["buyer_email"] for p in out]
    assert "alice@cdcr.ca.gov" in emails
    assert "bob@cdcr.ca.gov" in emails
    assert "dave@cdcr.ca.gov" in emails


def test_get_prospects_ranks_recent_above_older_with_more_spend(seeded_db):
    """Bob has $20K but it's 200 days old (decay ~0.21). Alice has $5K
    10 days old (decay ~0.93). Alice's score (5000 * 0.93 = 4650) beats
    Bob's score (20000 * 0.21 = 4200) — recency wins despite less $.

    This is the explicit Mike-2026-05-11 requirement: don't rank
    purely by $."""
    out = get_prospects(top_n=10)
    emails_in_order = [p["buyer_email"] for p in out]
    alice_idx = emails_in_order.index("alice@cdcr.ca.gov")
    bob_idx = emails_in_order.index("bob@cdcr.ca.gov")
    assert alice_idx < bob_idx, (
        "Alice ($5K 10d ago) should rank above Bob ($20K 200d ago) "
        "after recency decay."
    )


# ─── get_top_items_by_spend ──────────────────────────────────────────────


def test_top_items_groups_by_sku(seeded_db):
    """NITRILE-M aggregates across multiple buyers; N95-3M8210 is separate."""
    items = get_top_items_by_spend(top_n=10)
    skus = {it["reytech_sku"] for it in items}
    assert "NITRILE-M" in skus
    assert "N95-3M8210" in skus
    # Services bucket excluded
    assert "Services" not in skus


def test_top_items_sums_across_buyers(seeded_db):
    """NITRILE-M total: Alice $5K + Bob $20K = $25K (other rows filtered)."""
    items = get_top_items_by_spend(top_n=10)
    nitrile = [it for it in items if it["reytech_sku"] == "NITRILE-M"]
    assert len(nitrile) == 1
    # 5000 (Alice) + 20000 (Bob) = 25000 — others filtered out
    assert nitrile[0]["competitor_spend"] == 25000
    assert nitrile[0]["distinct_buyers"] == 2


# ─── get_general_recommendations ─────────────────────────────────────────


def test_recommendations_returns_structured_bullets(seeded_db):
    """The recommendations endpoint returns a dict with bullets list +
    metadata. Each bullet has kind, headline, action."""
    out = get_general_recommendations(days_back=365)
    assert out["ok"] is True
    assert "bullets" in out
    assert isinstance(out["bullets"], list)
    assert "generated_at" in out
    assert "prospect_count" in out
    for b in out["bullets"]:
        assert "kind" in b
        assert "headline" in b
        assert "action" in b


def test_recommendations_includes_top_prospect_bullet(seeded_db):
    """At least one bullet must be of kind=top_prospect with a buyer_email."""
    out = get_general_recommendations(days_back=365)
    top_prospect = [b for b in out["bullets"] if b["kind"] == "top_prospect"]
    assert len(top_prospect) == 1
    assert "buyer_email" in top_prospect[0]


def test_recommendations_includes_category_bullet(seeded_db):
    """Top-category bullet identifies highest-spend Reytech-sellable category."""
    out = get_general_recommendations(days_back=365)
    cats = [b for b in out["bullets"] if b["kind"] == "top_category"]
    assert len(cats) == 1
    assert cats[0].get("sku") == "NITRILE-M"


def test_recommendations_caps_at_5_bullets(seeded_db):
    """Never more than 5 bullets — keeps the digest scannable."""
    out = get_general_recommendations(days_back=365)
    assert len(out["bullets"]) <= 5


def test_recommendations_with_empty_db(temp_data_dir):
    """Empty SCPRS data → no bullets, no crash."""
    from src.core.db import get_db
    with get_db() as conn:
        _ensure_is_test_columns(conn)
        conn.executescript(
            "DELETE FROM scprs_po_lines; DELETE FROM scprs_po_master;"
        )
        conn.commit()
    out = get_general_recommendations()
    assert out["ok"] is True
    assert out["bullets"] == []
    assert out["prospect_count"] == 0


# ─── Phase 2c-1: known-customer filter ───────────────────────────────────
#
# Mike's feedback on the first digest 2026-05-11: "has some buyers we
# already work with and is like 4 months old, I think i need more
# actionable, 'you need to be on this buyers distribution list'". The
# fix excludes any buyer who shows up in scprs_buyers.buys_from_reytech,
# the quotes table, or contacts.is_reytech_customer.


def test_freshness_tier_buckets():
    """Days-since-buy → operator-meaningful labels."""
    assert _freshness_tier(0) == "fresh"
    assert _freshness_tier(15) == "fresh"
    assert _freshness_tier(30) == "fresh"
    assert _freshness_tier(31) == "warm"
    assert _freshness_tier(90) == "warm"
    assert _freshness_tier(91) == "stale"
    assert _freshness_tier(180) == "stale"
    assert _freshness_tier(181) == "old"
    assert _freshness_tier(None) == "unknown"


def test_prospect_dict_carries_freshness_tag(seeded_db):
    """Every prospect row should include `freshness` for the digest column."""
    out = get_prospects(top_n=10)
    assert len(out) > 0
    for p in out:
        assert "freshness" in p
        assert p["freshness"] in ("fresh", "warm", "stale", "old", "unknown")


def test_get_prospects_excludes_known_reytech_customer_from_scprs_buyers(seeded_db):
    """Buyers flagged buys_from_reytech=1 must NOT appear as prospects."""
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO scprs_buyers "
            "(buyer_email, buys_from_reytech, reytech_spend) "
            "VALUES (?, 1, 5000.0)",
            ("alice@cdcr.ca.gov",),
        )
        conn.commit()
    out = get_prospects(top_n=10)
    emails = [p["buyer_email"] for p in out]
    assert "alice@cdcr.ca.gov" not in emails
    # Bob + Dave (also seeded) should still appear — they're not known customers.
    assert "bob@cdcr.ca.gov" in emails
    assert "dave@cdcr.ca.gov" in emails


def test_get_prospects_excludes_known_reytech_customer_from_quotes(seeded_db):
    """Buyers we've quoted before must NOT appear as prospects — they're
    not distribution-list CANDIDATES, they're existing relationships."""
    from src.core.db import get_db
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO quotes (quote_number, created_at, contact_email) "
            "VALUES (?, ?, ?)",
            ("Q-EXISTING-001", now, "bob@cdcr.ca.gov"),
        )
        conn.commit()
    out = get_prospects(top_n=10)
    emails = [p["buyer_email"] for p in out]
    assert "bob@cdcr.ca.gov" not in emails


def test_get_prospects_excludes_known_reytech_customer_from_contacts(seeded_db):
    """contacts.is_reytech_customer=1 must filter that email from prospects."""
    from src.core.db import get_db
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO contacts "
            "(id, created_at, buyer_email, is_reytech_customer) "
            "VALUES (?, ?, ?, 1)",
            ("c-dave", now, "dave@cdcr.ca.gov"),
        )
        conn.commit()
    out = get_prospects(top_n=10)
    emails = [p["buyer_email"] for p in out]
    assert "dave@cdcr.ca.gov" not in emails


def test_get_prospects_known_customer_filter_case_insensitive(seeded_db):
    """Email casing differences must NOT defeat the filter."""
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO scprs_buyers "
            "(buyer_email, buys_from_reytech) VALUES (?, 1)",
            ("ALICE@CDCR.CA.GOV",),  # uppercased in customer list
        )
        conn.commit()
    out = get_prospects(top_n=10)
    # Seeded data has alice@cdcr.ca.gov (lowercase); filter must match.
    assert "alice@cdcr.ca.gov" not in [p["buyer_email"] for p in out]


# ─── Phase 2c-1: reframed action verbs ───────────────────────────────────


def test_top_prospect_action_uses_distribution_list_verb(seeded_db):
    """The #1 prospect bullet must talk about registration / distro list,
    not 'send outreach this week'."""
    out = get_general_recommendations(days_back=365)
    top = [b for b in out["bullets"] if b["kind"] == "top_prospect"]
    assert len(top) == 1
    action = top[0]["action"].lower()
    assert "distribution list" in action
    assert "send outreach this week" not in action


def test_top_agency_action_uses_registration_verb(seeded_db):
    """The #1 agency bullet must frame the action as vendor registration."""
    out = get_general_recommendations(days_back=365)
    agencies = [b for b in out["bullets"] if b["kind"] == "top_agency"]
    assert len(agencies) == 1
    action = agencies[0]["action"].lower()
    assert ("vendor distribution list" in action
            or "register" in action), action


def test_fresh_signal_bullet_when_recent_prospect_exists(seeded_db):
    """A buyer with a ≤30d-old PO should produce a 'fresh_signal' bullet."""
    out = get_general_recommendations(days_back=365)
    fresh = [b for b in out["bullets"] if b["kind"] == "fresh_signal"]
    # Alice + Dave both ≤30d in the seed → at least one fresh bullet.
    assert len(fresh) == 1
    action = fresh[0]["action"].lower()
    assert "registered" in action or "register" in action


def test_no_fresh_signal_bullet_when_all_prospects_old(temp_data_dir):
    """When the only signal is 200d+ old, the fresh_signal bullet must
    NOT fire (don't fabricate freshness)."""
    from src.core.db import get_db
    from datetime import datetime, timedelta
    with get_db() as conn:
        _ensure_is_test_columns(conn)
        conn.executescript(
            "DELETE FROM scprs_po_lines; DELETE FROM scprs_po_master;"
        )
        old = (datetime.now() - timedelta(days=200)).strftime("%m/%d/%Y")
        conn.execute(
            "INSERT INTO scprs_po_master "
            "(id, po_number, dept_code, dept_name, buyer_email, buyer_name, "
            " supplier, start_date, is_test) "
            "VALUES (1, 'PO-OLD', 'DEPT1', 'CDCR', 'old@cdcr.ca.gov', "
            " 'OldGuy', 'Echelon', ?, 0)",
            (old,),
        )
        conn.execute(
            "INSERT INTO scprs_po_lines "
            "(po_id, po_number, line_num, description, reytech_sku, category, "
            " quantity, unit_price, line_total, reytech_sells, opportunity_flag, "
            " is_test) "
            "VALUES (1, 'PO-OLD', 1, 'Nitrile gloves', 'NITRILE-M', 'exam_gloves', "
            " 100, 50, 5000, 1, 'WIN_BACK', 0)"
        )
        conn.commit()
    out = get_general_recommendations(days_back=365)
    fresh = [b for b in out["bullets"] if b["kind"] == "fresh_signal"]
    assert fresh == []
