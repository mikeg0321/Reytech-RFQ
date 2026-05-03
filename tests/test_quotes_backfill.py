"""Tests for src/core/quotes_backfill.ensure_quote_won_for_order — the
inverse-direction architecture-layer fix.

PR #630 wired forward direction (quote marked won -> orders row).
This file locks the inverse: when an order arrives via a non-Mark-Won
path (PO email, Drive watcher, manual SCPRS reconcile), the paired
quote auto-flips to status='won'.

Without this hook, the recent-wins KPI under-reports because the
2026-04 SCPRS reconcile pulled real POs into orders but their paired
quotes stayed 'open' / 'pending' / 'priced'. PR #660 patched recent-
wins to drive off orders as a workaround. This is the underlying fix.
"""
from __future__ import annotations

import json
from datetime import datetime

import pytest


def _ensure():
    from src.core.quotes_backfill import ensure_quote_won_for_order
    return ensure_quote_won_for_order


def _backfill():
    from src.core.quotes_backfill import backfill_orders_quotes_drift
    return backfill_orders_quotes_drift


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("orders", "quotes", "quote_audit_log", "order_audit_log"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    conn.commit()


def _seed_quote(conn, *, quote_number, status="open", total=500.0,
                agency="CDCR", is_test=0):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO quotes
          (quote_number, agency, institution, status, total, subtotal,
           tax, contact_name, contact_email, created_at, updated_at,
           is_test, line_items)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (quote_number, agency, "Test Inst", status, total,
          total * 0.92, total * 0.08, "Buyer", "b@x.gov",
          when, when, is_test, json.dumps([])))


def _seed_order(conn, *, order_id, quote_number, po_number="PO-X", status="new"):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution, total, status,
           items, created_at, updated_at, buyer_name, buyer_email, ship_to,
           ship_to_address, total_cost, margin_pct, po_pdf_path,
           fulfillment_type, notes, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, "CDCR", "Test Inst", 500.0,
          status, "[]", when, when, "Buyer", "b@x.gov", "Ship To",
          "[]", 0, 0, "", "dropship", "", 0))


def _quote_status(conn, quote_number):
    r = conn.execute(
        "SELECT status FROM quotes WHERE quote_number=?",
        (quote_number,)
    ).fetchone()
    return r["status"] if r else None


# ── ensure_quote_won_for_order: direct-call contract ──────────────────


def test_flips_open_quote_to_won():
    fn = _ensure()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="QW-1", status="open")
        c.commit()

    out = fn("QW-1", order_id="ORD-1", po_number="PO-X")
    assert out["ok"] is True
    assert out["flipped"] is True
    assert out["prev_status"] == "open"

    with _conn() as c:
        assert _quote_status(c, "QW-1") == "won"


def test_idempotent_when_already_won():
    fn = _ensure()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="QW-2", status="won")
        c.commit()

    out = fn("QW-2", order_id="ORD-2")
    assert out["ok"] is True
    assert out["flipped"] is False
    assert out["prev_status"] == "won"


def test_does_not_override_lost_status():
    """Operator decision — lost is final, even if an order somehow
    landed for it (e.g., bookkeeping shuffle). Don't second-guess."""
    fn = _ensure()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="QW-3", status="lost")
        c.commit()

    out = fn("QW-3", order_id="ORD-3")
    assert out["ok"] is True
    assert out["flipped"] is False
    assert out["prev_status"] == "lost"

    with _conn() as c:
        assert _quote_status(c, "QW-3") == "lost"


def test_does_not_override_voided_or_cancelled():
    fn = _ensure()
    for final_status in ("voided", "cancelled", "deleted"):
        with _conn() as c:
            _wipe(c)
            _seed_quote(c, quote_number="QW-X", status=final_status)
            c.commit()
        out = fn("QW-X", order_id="ORD-X")
        assert out["ok"] is True
        assert out["flipped"] is False, (
            f"status {final_status!r} must not be flipped to won")


def test_flips_priced_sent_pending_statuses():
    """All in-flight quote statuses should flip when an order lands.
    Distinguishes from final statuses (won/lost/voided/cancelled)."""
    fn = _ensure()
    for in_flight in ("open", "pending", "priced", "sent", "draft", "new", ""):
        qn = f"QW-IF-{in_flight or 'empty'}"
        with _conn() as c:
            _wipe(c)
            _seed_quote(c, quote_number=qn, status=in_flight)
            c.commit()
        out = fn(qn, order_id="ORD-IF")
        assert out["ok"] is True
        assert out["flipped"] is True, (
            f"in-flight status {in_flight!r} should flip to won")


def test_returns_error_when_quote_missing():
    fn = _ensure()
    with _conn() as c:
        _wipe(c)
        c.commit()
    out = fn("DOES-NOT-EXIST", order_id="ORD-Z")
    assert out["ok"] is False
    assert "not found" in out["error"].lower()


def test_empty_quote_number_rejected():
    fn = _ensure()
    out = fn("", order_id="ORD-EMPTY")
    assert out["ok"] is False
    assert "required" in out["error"].lower()


# ── save_order hot-path integration ──────────────────────────────────


def test_save_order_flips_paired_quote_to_won():
    """The architecture-layer fix: any path that writes an order with a
    quote_number must end with the quote at status='won'. Locks the
    inverse-direction wiring."""
    from src.core.order_dal import save_order

    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="HOOK-1", status="open")
        c.commit()

    save_order("ORD-HOOK-1", {
        "order_id": "ORD-HOOK-1",
        "quote_number": "HOOK-1",
        "po_number": "PO-HOOK-A",
        "agency": "CDCR",
        "total": 500.0,
        "status": "new",
    }, actor="test")

    with _conn() as c:
        assert _quote_status(c, "HOOK-1") == "won"


def test_save_order_without_quote_number_skips_quote_flip():
    """Order with no quote_number (e.g., direct PO entry without a
    paired quote) should not crash and should not write to quotes."""
    from src.core.order_dal import save_order

    with _conn() as c:
        _wipe(c)
        c.commit()

    ok = save_order("ORD-ORPHAN", {
        "order_id": "ORD-ORPHAN",
        "quote_number": "",
        "po_number": "PO-ORPHAN",
        "agency": "CDCR",
        "total": 100.0,
        "status": "new",
    }, actor="test")
    assert ok is True


def test_save_order_lost_quote_stays_lost():
    """save_order's hook must respect the lost-status guard — operator
    decision on the quote outranks the order's existence."""
    from src.core.order_dal import save_order

    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="HOOK-LOST", status="lost")
        c.commit()

    save_order("ORD-LOST", {
        "order_id": "ORD-LOST",
        "quote_number": "HOOK-LOST",
        "po_number": "PO-L",
        "agency": "CDCR",
        "total": 200.0,
        "status": "new",
    }, actor="test")

    with _conn() as c:
        assert _quote_status(c, "HOOK-LOST") == "lost"


# ── backfill_orders_quotes_drift ─────────────────────────────────────


def test_backfill_flips_drifted_quotes():
    """One-time scan: find every order with a quote_number, flip the
    paired quote to won unless it's already in a final state."""
    fn = _backfill()
    with _conn() as c:
        _wipe(c)
        # Three drifted quotes (open, priced, sent) + their orders
        _seed_quote(c, quote_number="DRIFT-A", status="open")
        _seed_quote(c, quote_number="DRIFT-B", status="priced")
        _seed_quote(c, quote_number="DRIFT-C", status="sent")
        # One already-won quote + order — should not be flipped
        _seed_quote(c, quote_number="ALREADY-WON", status="won")
        # One lost quote + order — should be skipped (final)
        _seed_quote(c, quote_number="LOST-Q", status="lost")

        _seed_order(c, order_id="O-A", quote_number="DRIFT-A")
        _seed_order(c, order_id="O-B", quote_number="DRIFT-B")
        _seed_order(c, order_id="O-C", quote_number="DRIFT-C")
        _seed_order(c, order_id="O-W", quote_number="ALREADY-WON")
        _seed_order(c, order_id="O-L", quote_number="LOST-Q")
        c.commit()

    result = fn()
    assert result["ok"] is True
    assert sorted(result["flipped"]) == ["DRIFT-A", "DRIFT-B", "DRIFT-C"]
    assert result["skipped_already_won"] == 1
    assert any(qn == "LOST-Q" for qn, _ in result["skipped_final"])

    with _conn() as c:
        assert _quote_status(c, "DRIFT-A") == "won"
        assert _quote_status(c, "DRIFT-B") == "won"
        assert _quote_status(c, "DRIFT-C") == "won"
        assert _quote_status(c, "ALREADY-WON") == "won"
        assert _quote_status(c, "LOST-Q") == "lost"


def test_backfill_dry_run_classifies_without_writing():
    """dry_run=True must classify identically but never write. The
    report shows the would-be flips; the quotes table stays untouched.
    Pins the safe path for running on prod before applying."""
    fn = _backfill()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="DRY-A", status="open")
        _seed_quote(c, quote_number="DRY-B", status="sent")
        _seed_quote(c, quote_number="DRY-LOST", status="lost")
        _seed_order(c, order_id="OD-A", quote_number="DRY-A")
        _seed_order(c, order_id="OD-B", quote_number="DRY-B")
        _seed_order(c, order_id="OD-L", quote_number="DRY-LOST")
        c.commit()

    result = fn(dry_run=True)
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert sorted(result["flipped"]) == ["DRY-A", "DRY-B"]
    assert any(qn == "DRY-LOST" for qn, _ in result["skipped_final"])

    # Critical: nothing actually moved.
    with _conn() as c:
        assert _quote_status(c, "DRY-A") == "open"
        assert _quote_status(c, "DRY-B") == "sent"
        assert _quote_status(c, "DRY-LOST") == "lost"


def test_backfill_dry_run_then_apply_match():
    """The dry-run flip list must be a subset of (or equal to) what an
    apply run actually flips. Anything else means the script lies on
    prod about what it'll do."""
    fn = _backfill()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="EQ-1", status="open")
        _seed_quote(c, quote_number="EQ-2", status="priced")
        _seed_order(c, order_id="OE-1", quote_number="EQ-1")
        _seed_order(c, order_id="OE-2", quote_number="EQ-2")
        c.commit()

    dry = fn(dry_run=True)
    applied = fn(dry_run=False)

    assert sorted(dry["flipped"]) == sorted(applied["flipped"])
    # After apply, a re-run must be a no-op (idempotent).
    rerun = fn(dry_run=False)
    assert rerun["flipped"] == []
    assert rerun["skipped_already_won"] == 2


def test_backfill_actor_string_lands_in_audit_log():
    """The actor= argument must propagate into the per-flip audit row
    so prod backfill writes are distinguishable from in-flow hooks."""
    fn = _backfill()
    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="ACTOR-1", status="open")
        _seed_order(c, order_id="OA-1", quote_number="ACTOR-1")
        c.commit()

    fn(dry_run=False, actor="custom_backfill_actor_2026")

    with _conn() as c:
        try:
            row = c.execute(
                "SELECT actor FROM quote_audit_log WHERE quote_number=? "
                "AND action='mark_won_from_order' ORDER BY created_at DESC LIMIT 1",
                ("ACTOR-1",)
            ).fetchone()
        except Exception:
            row = None
    if row is not None:
        assert row["actor"] == "custom_backfill_actor_2026"


# ── CLI script integration ──────────────────────────────────────────


def test_cli_dry_run_emits_summary(capsys):
    """The `python scripts/backfill_orders_quotes_drift.py` invocation
    (no --apply) must print a usable summary and exit 0 without
    mutating any quote rows."""
    import importlib.util
    import os
    import sys

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script_path = os.path.join(repo_root, "scripts",
                               "backfill_orders_quotes_drift.py")
    spec = importlib.util.spec_from_file_location(
        "backfill_cli", script_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="CLI-1", status="open")
        _seed_order(c, order_id="OCLI-1", quote_number="CLI-1")
        c.commit()

    old_argv = sys.argv
    sys.argv = ["backfill_orders_quotes_drift.py"]
    try:
        rc = mod.main()
    finally:
        sys.argv = old_argv

    captured = capsys.readouterr()
    assert rc == 0
    assert "would_flip=1" in captured.out
    assert "dry-run" in captured.out

    with _conn() as c:
        # No writes on dry-run.
        assert _quote_status(c, "CLI-1") == "open"


def test_cli_apply_actually_flips(capsys):
    import importlib.util
    import os
    import sys

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script_path = os.path.join(repo_root, "scripts",
                               "backfill_orders_quotes_drift.py")
    spec = importlib.util.spec_from_file_location(
        "backfill_cli_apply", script_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    with _conn() as c:
        _wipe(c)
        _seed_quote(c, quote_number="CLI-2", status="open")
        _seed_order(c, order_id="OCLI-2", quote_number="CLI-2")
        c.commit()

    old_argv = sys.argv
    sys.argv = ["backfill_orders_quotes_drift.py", "--apply"]
    try:
        rc = mod.main()
    finally:
        sys.argv = old_argv

    captured = capsys.readouterr()
    assert rc == 0
    assert "flipped_count=1" in captured.out

    with _conn() as c:
        assert _quote_status(c, "CLI-2") == "won"
