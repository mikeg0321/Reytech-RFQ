"""The Spine — Snapshot + render-matching gate tests.

These tests prove the substrate invariants that close the 2026-05-15
failure class:

  Quote model state == PDF render bytes == snapshot bytes == bytes
  delivered to the agency.

The render-matching gate (quote_pdf.py) makes hop 2 structural.
The snapshot table (db.py) makes hop 3 immutable.
The finalized→sent precondition (routes_spine.py) couples them.

If any of these tests fail, the substrate is unsafe to ship.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.spine import (
    LineItem,
    Quote,
    QuoteStatus,
    SpineRenderMismatchError,
    SpineValidationError,
    init_db,
    iter_snapshots,
    latest_snapshot,
    read_snapshot,
    render_quote_pdf,
    write_quote,
    write_snapshot,
)


# ──────────────────────────────────────────────────────────────────────
# Fixtures — minimal valid finalized Quote ready to snapshot.
# ──────────────────────────────────────────────────────────────────────


def _fresh_ts() -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=1)


def _finalized_quote(quote_id: str = "Q-snap-001", **overrides) -> Quote:
    li = LineItem(
        line_no=1,
        description="Resvent CPAP — 30 units",
        mfg_number="20A",
        qty=30,
        uom="EA",
        cost_cents=25_000,
        cost_source_url="https://shop.resvent.com/products/ibreeze",
        cost_validated_at=_fresh_ts(),
        unit_price_cents=33_750,
    )
    base = dict(
        quote_id=quote_id,
        agency="CCHCS",
        facility="Test - CCWF Chowchilla",
        solicitation_number="10846581",
        line_items=[li],
        tax_rate_bps=775,
        status=QuoteStatus.FINALIZED,
    )
    base.update(overrides)
    return Quote(**base)


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_snap.db"
    init_db(str(p))
    return str(p)


# ──────────────────────────────────────────────────────────────────────
# W-Q-013 — Render-matching gate
# ──────────────────────────────────────────────────────────────────────


def test_render_matching_gate_passes_on_correct_render():
    """Happy path: renderer + gate agree, bytes returned cleanly."""
    q = _finalized_quote()
    pdf_bytes = render_quote_pdf(q)
    assert pdf_bytes.startswith(b"%PDF-")
    assert len(pdf_bytes) > 500


def test_render_matching_gate_catches_zero_tax_injection(monkeypatch):
    """5/15 substrate failure class: tax line emitted as $0.00 on a
    non-zero subtotal. The gate must refuse to return the bytes.

    Sabotage technique: replace _totals_block with one that hard-codes
    the tax cell to "$0.00" regardless of model state. This is the
    exact shape of the bug class that shipped both 5/15 quotes.
    """
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.platypus import Table, TableStyle

    import src.spine.quote_pdf as qpdf

    def liar_totals(quote):
        rows = [
            ["SUBTOTAL", qpdf.format_dollars(quote.subtotal_cents)],
            [f"TAX ({qpdf.format_tax_rate(quote.tax_rate_bps)})", "$0.00"],
            ["SHIPPING", qpdf.format_dollars(0)],
            ["TOTAL", qpdf.format_dollars(quote.total_cents)],
        ]
        tbl = Table(rows, colWidths=[1.6 * inch, 1.4 * inch], hAlign="RIGHT")
        tbl.setStyle(TableStyle([
            ("FONT", (0, 0), (-1, -2), "Helvetica", 10),
            ("FONT", (0, -1), (-1, -1), "Helvetica-Bold", 11),
        ]))
        return tbl

    monkeypatch.setattr(qpdf, "_totals_block", liar_totals)

    q = _finalized_quote()
    with pytest.raises(SpineRenderMismatchError) as excinfo:
        render_quote_pdf(q)
    msg = str(excinfo.value)
    assert "TAX" in msg
    assert "$0.00" in msg  # displayed (wrong) value
    assert "$784.69" in msg  # expected value


def test_render_matching_gate_catches_wrong_extension(monkeypatch):
    """If a per-line extension is mis-rendered (e.g., qty × unit_price
    table cell shows the wrong product), the gate fires. This closes
    the case where the totals look right but a line item is wrong.
    """
    import src.spine.quote_pdf as qpdf
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.platypus import Paragraph, Table, TableStyle

    def liar_li(quote):
        s = qpdf._styles()
        header = ["#", "MFG #", "DESCRIPTION", "QTY", "UOM", "UNIT PRICE", "EXTENSION"]
        rows = [header]
        for li in quote.line_items:
            rows.append([
                str(li.line_no), li.mfg_number or "",
                Paragraph(qpdf._escape_pdf_text(li.description), s["li_desc"]),
                f"{li.qty:,}", li.uom,
                qpdf.format_dollars(li.unit_price_cents),
                "$0.01",  # THE LIE — every extension claims to be one cent
            ])
        tbl = Table(rows, colWidths=[0.4*inch, 0.95*inch, 2.85*inch, 0.55*inch,
                                      0.45*inch, 0.9*inch, 0.9*inch],
                    repeatRows=1)
        return tbl

    monkeypatch.setattr(qpdf, "_line_item_table", liar_li)

    q = _finalized_quote()
    with pytest.raises(SpineRenderMismatchError) as excinfo:
        render_quote_pdf(q)
    assert "extension" in str(excinfo.value).lower()
    assert "$10,125.00" in str(excinfo.value)


# ──────────────────────────────────────────────────────────────────────
# W-Q-014 — Snapshot immutability
# ──────────────────────────────────────────────────────────────────────


def test_snapshot_writes_one_row(db_path):
    q = _finalized_quote()
    write_quote(db_path, q, actor="seed")
    res = write_snapshot(db_path, q, actor="operator")
    snaps = iter_snapshots(db_path, q.quote_id)
    assert len(snaps) == 1
    assert snaps[0]["snapshot_id"] == res["snapshot_id"]
    assert snaps[0]["sha256"] == res["sha256"]
    assert snaps[0]["actor"] == "operator"


def test_snapshot_is_idempotent_on_unchanged_state(db_path):
    """Clicking Snapshot twice on an unchanged Quote MUST NOT
    duplicate the audit row. Identity is on state, not PDF bytes
    (which vary because ReportLab embeds creation timestamps).
    """
    q = _finalized_quote()
    write_quote(db_path, q, actor="seed")
    r1 = write_snapshot(db_path, q, actor="operator", note="first click")
    r2 = write_snapshot(db_path, q, actor="operator", note="second click")
    assert r1["snapshot_id"] == r2["snapshot_id"]
    assert len(iter_snapshots(db_path, q.quote_id)) == 1


def test_snapshot_bytes_are_byte_identical_on_read(db_path):
    """The pdf_bytes column ships back byte-for-byte. sha256 verifies."""
    q = _finalized_quote()
    write_quote(db_path, q, actor="seed")
    res = write_snapshot(db_path, q, actor="operator")
    loaded = read_snapshot(db_path, res["snapshot_id"])
    assert loaded is not None
    assert hashlib.sha256(bytes(loaded["pdf_bytes"])).hexdigest() == res["sha256"]


def test_snapshot_rejects_sabotaged_render(db_path, monkeypatch):
    """The gate fires BEFORE the snapshot row is written. No partial
    state — sabotage attempts leave the snapshots table empty.
    """
    import src.spine.quote_pdf as qpdf
    from reportlab.lib.units import inch
    from reportlab.platypus import Table, TableStyle

    def liar_totals(quote):
        rows = [
            ["SUBTOTAL", qpdf.format_dollars(quote.subtotal_cents)],
            [f"TAX ({qpdf.format_tax_rate(quote.tax_rate_bps)})", "$0.00"],
            ["SHIPPING", qpdf.format_dollars(0)],
            ["TOTAL", qpdf.format_dollars(quote.total_cents)],
        ]
        tbl = Table(rows, colWidths=[1.6 * inch, 1.4 * inch], hAlign="RIGHT")
        return tbl

    monkeypatch.setattr(qpdf, "_totals_block", liar_totals)

    q = _finalized_quote()
    write_quote(db_path, q, actor="seed")
    with pytest.raises(SpineRenderMismatchError):
        write_snapshot(db_path, q, actor="attacker")
    assert len(iter_snapshots(db_path, q.quote_id)) == 0


def test_snapshot_changes_when_state_changes(db_path):
    """Edit the quote, snapshot again — get a new snapshot_id (state
    diverged), original snapshot row preserved (immutable).
    """
    q1 = _finalized_quote()
    write_quote(db_path, q1, actor="seed")
    r1 = write_snapshot(db_path, q1, actor="op", note="approved at 35%")

    # Edit unit price; re-snapshot.
    new_li = q1.line_items[0].model_copy(update={"unit_price_cents": 40_000})
    q2 = q1.model_copy(update={"line_items": [new_li]})
    write_quote(db_path, q2, actor="op", note="price up")
    r2 = write_snapshot(db_path, q2, actor="op", note="approved at higher")

    assert r1["snapshot_id"] != r2["snapshot_id"]
    snaps = iter_snapshots(db_path, q1.quote_id)
    assert len(snaps) == 2
    # Both rows present, newest-first.
    assert snaps[0]["snapshot_id"] == r2["snapshot_id"]
    assert snaps[1]["snapshot_id"] == r1["snapshot_id"]


def test_snapshot_requires_non_empty_actor(db_path):
    q = _finalized_quote()
    write_quote(db_path, q, actor="seed")
    with pytest.raises(SpineValidationError):
        write_snapshot(db_path, q, actor="")
    with pytest.raises(SpineValidationError):
        write_snapshot(db_path, q, actor="   ")


def test_latest_snapshot_returns_most_recent(db_path):
    q1 = _finalized_quote()
    write_quote(db_path, q1, actor="seed")
    r1 = write_snapshot(db_path, q1, actor="op")
    new_li = q1.line_items[0].model_copy(update={"unit_price_cents": 40_000})
    q2 = q1.model_copy(update={"line_items": [new_li]})
    write_quote(db_path, q2, actor="op")
    r2 = write_snapshot(db_path, q2, actor="op")
    assert latest_snapshot(db_path, q1.quote_id)["snapshot_id"] == r2["snapshot_id"]


def test_latest_snapshot_returns_none_when_absent(db_path):
    q = _finalized_quote()
    write_quote(db_path, q, actor="seed")
    assert latest_snapshot(db_path, q.quote_id) is None
