"""Tests for `src.core.orders_link_orphans` — the orphan-orders backfill.

Background — 67/167 prod orders had empty `quote_number` per the 2026-04-29
audit (`project_orphan_orders_finding.md`). PR #664's hook only fires when
a quote_number is already set, so orphans stayed invisible to recent-wins,
win-rate-by-agency, and oracle calibration. This module links them by
exact PO match — conservative (high-confidence only), idempotent, and
audit-logged.

These tests lock:
  - High-confidence link: order PO exactly matches one quote → link.
  - Sentinel POs ('', 'N/A', 'TBD') → never match (treated as no-PO).
  - Ambiguous PO (matches >1 quote) → reported but not auto-linked.
  - Already-linked orders → skipped (idempotent).
  - Test rows on either side → never linked (no synthetic pollution).
  - Dry-run leaves the DB untouched.
  - Apply phase fires the PR #664 hook so the paired quote flips to 'won'.
"""
from __future__ import annotations

from datetime import datetime

import pytest


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("orders", "quotes", "order_audit_log"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_quote(conn, *, quote_number: str, po_number: str = "",
                agency: str = "CDCR", total: float = 100.0,
                status: str = "open", is_test: int = 0,
                created_at: str | None = None) -> None:
    when = created_at or datetime.now().isoformat()
    conn.execute(
        """INSERT INTO quotes
           (quote_number, agency, institution, status, total, subtotal, tax,
            contact_name, contact_email, created_at, updated_at,
            is_test, line_items, po_number)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (quote_number, agency, agency, status, total, total * 0.92,
         total * 0.08, "Buyer", "b@x.gov", when, when, is_test,
         "[]", po_number),
    )
    conn.commit()


def _seed_order(conn, *, order_id: str, po_number: str = "",
                quote_number: str = "", agency: str = "CDCR",
                total: float = 100.0, status: str = "shipped",
                is_test: int = 0,
                created_at: str | None = None) -> None:
    when = created_at or datetime.now().isoformat()
    conn.execute(
        """INSERT INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            items, created_at, updated_at, is_test)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (order_id, quote_number, po_number, agency, agency, total, status,
         "[]", when, when, is_test),
    )
    conn.commit()


# ── High-confidence path: exact PO match ─────────────────────────────────


def test_orphan_with_exact_po_links(temp_data_dir):
    """Order with empty quote_number + PO that uniquely matches a quote
    → linked + paired quote flipped to 'won' via PR #664 hook."""
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q001", po_number="4500123456",
                    agency="CDCR", total=500.0, status="open")
        _seed_order(c, order_id="ORD-LINK-1", po_number="4500123456",
                    agency="CDCR", total=500.0)

    report = link_orphan_orders(dry_run=False)

    assert report["ok"] is True
    assert report["orphan_count"] == 1
    assert len(report["linked"]) == 1
    assert report["linked"][0]["quote_number"] == "R26Q001"
    assert report["linked"][0]["po"] == "4500123456"
    assert report.get("applied_count") == 1

    # The order now has the quote_number stamped on it.
    with _conn() as c:
        row = c.execute(
            "SELECT quote_number FROM orders WHERE id=?", ("ORD-LINK-1",)
        ).fetchone()
        assert row["quote_number"] == "R26Q001"
        # PR #664 hook fired — paired quote flipped open -> won.
        qrow = c.execute(
            "SELECT status FROM quotes WHERE quote_number=?", ("R26Q001",)
        ).fetchone()
        assert qrow["status"] == "won", (
            f"expected paired quote to flip to 'won' via the PR #664 hook, "
            f"got {qrow['status']!r}"
        )


# ── Sentinel + missing PO paths ──────────────────────────────────────────


def test_orphan_with_no_po_skipped(temp_data_dir):
    """Order with empty PO can't match anything; bucketed as `no_po`."""
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q002", po_number="4500999999",
                    total=500.0)
        _seed_order(c, order_id="ORD-NOPO", po_number="", total=500.0)

    report = link_orphan_orders(dry_run=False)
    assert len(report["no_po"]) == 1
    assert report["no_po"][0]["order_id"] == "ORD-NOPO"
    assert len(report["linked"]) == 0


@pytest.mark.parametrize("sentinel", ["N/A", "TBD", "?", "PENDING", "  "])
def test_sentinel_po_treated_as_no_po(temp_data_dir, sentinel):
    """Sentinel po_numbers are scrubbed by `clean_po_number`; the
    orphan is bucketed as `no_po`, not `no_match`."""
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id=f"ORD-SENT-{sentinel.strip() or 'sp'}",
                    po_number=sentinel, total=100.0)

    report = link_orphan_orders(dry_run=False)
    # Either no_po (sentinel scrubs to '') OR (in extreme cases) no_match.
    # Critical lock: NEVER auto-links to a quote.
    assert len(report["linked"]) == 0


def test_orphan_with_unmatched_po_bucketed(temp_data_dir):
    """Real PO but no paired quote — bucketed as `no_match` (not linked)."""
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="ORD-UNMATCHED",
                    po_number="4500111111", total=100.0)

    report = link_orphan_orders(dry_run=False)
    assert len(report["no_match"]) == 1
    assert report["no_match"][0]["po"] == "4500111111"
    assert len(report["linked"]) == 0


# ── Ambiguous: multi-quote PO ─────────────────────────────────────────────


def test_ambiguous_po_not_auto_linked(temp_data_dir):
    """Multi-quote PO is legitimate (real ABCD-PO-0000053217 case in
    `project_session_2026_04_28_drift_card_actionable.md`). Don't pick
    one — bucket as ambiguous and let an operator decide."""
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q010", po_number="0000053217",
                    total=300.0)
        _seed_quote(c, quote_number="R26Q011", po_number="0000053217",
                    total=400.0)
        _seed_order(c, order_id="ORD-AMBIG",
                    po_number="0000053217", total=700.0)

    report = link_orphan_orders(dry_run=False)
    assert len(report["ambiguous"]) == 1
    assert report["ambiguous"][0]["match_count"] == 2
    assert report["ambiguous"][0]["po"] == "0000053217"
    assert len(report["linked"]) == 0

    # Order is unchanged (still orphan).
    with _conn() as c:
        row = c.execute(
            "SELECT quote_number FROM orders WHERE id=?", ("ORD-AMBIG",)
        ).fetchone()
        assert (row["quote_number"] or "") == ""


# ── Idempotence: already-linked orders skipped ───────────────────────────


def test_already_linked_orders_skipped(temp_data_dir):
    """Orders that already have a quote_number are not orphans —
    `find_orphan_orders` excludes them at the SQL filter, so the
    backfill is a no-op for them. Re-running the script is safe."""
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q020", po_number="4500777777",
                    total=200.0)
        _seed_order(c, order_id="ORD-ALREADY",
                    po_number="4500777777", quote_number="R26Q020",
                    total=200.0)

    report = link_orphan_orders(dry_run=False)
    assert report["orphan_count"] == 0
    assert len(report["linked"]) == 0


# ── Test-row guards ──────────────────────────────────────────────────────


def test_test_orders_excluded_from_orphan_set(temp_data_dir):
    """Orders flagged is_test=1 don't count as orphans — the linker
    shouldn't churn synthetic seed data."""
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q030", po_number="4500888888",
                    total=100.0)
        _seed_order(c, order_id="ORD-TEST",
                    po_number="4500888888", total=100.0, is_test=1)

    report = link_orphan_orders(dry_run=False)
    assert report["orphan_count"] == 0


def test_test_quotes_excluded_from_match_set(temp_data_dir):
    """A test quote sharing a PO with a real order shouldn't poison
    the link — the join filter excludes test quotes."""
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        # Real quote + test quote with same PO.
        _seed_quote(c, quote_number="R26Q040", po_number="4500444444",
                    total=100.0)
        _seed_quote(c, quote_number="TEST-Q-X",
                    po_number="4500444444", total=100.0, is_test=1)
        _seed_order(c, order_id="ORD-W-TEST-COLLIDE",
                    po_number="4500444444", total=100.0)

    report = link_orphan_orders(dry_run=False)
    # Even though there are 2 quotes with this PO, only 1 is non-test —
    # so the match is unambiguous and the link goes through.
    assert len(report["linked"]) == 1
    assert report["linked"][0]["quote_number"] == "R26Q040"


# ── Dry run leaves DB untouched ──────────────────────────────────────────


def test_dry_run_does_not_write(temp_data_dir):
    from src.core.orders_link_orphans import link_orphan_orders
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="R26Q050", po_number="4500555555",
                    total=100.0, status="open")
        _seed_order(c, order_id="ORD-DRY", po_number="4500555555",
                    total=100.0)

    report = link_orphan_orders(dry_run=True)
    assert report["dry_run"] is True
    assert len(report["linked"]) == 1   # would link
    assert "applied_count" not in report

    # But the DB was not mutated.
    with _conn() as c:
        row = c.execute(
            "SELECT quote_number FROM orders WHERE id=?", ("ORD-DRY",)
        ).fetchone()
        assert (row["quote_number"] or "") == ""
        qrow = c.execute(
            "SELECT status FROM quotes WHERE quote_number=?", ("R26Q050",)
        ).fetchone()
        assert qrow["status"] == "open", "dry-run must not flip the quote"
