"""Regression test for Plan §6.2 sub-1 follow-up: buyer email must
propagate from a PC/RFQ source through generate_quote → _log_quote
→ upsert_quote → quotes.contact_email column.

Originating finding: PR #620 (buyer pricing memory rollup panel) shipped
with empty data on prod despite Win/Loss showing 102W/379L all-time.
Root cause: `generate_quote_from_pc` builds a `data` dict with no email
fields, so the `result` dict that flows into `_log_quote` lacks anything
to populate `quotes.contact_email`. Without `contact_email`, every
buyer-keyed view stays empty regardless of how many quotes ship.

These tests lock the chain so a future refactor can't silently drop the
email field again — they verify the END state (the row in `quotes`)
rather than any intermediate dict, which is what the buyer pricing
memory panel actually reads.
"""
from __future__ import annotations

import os

import pytest


@pytest.fixture
def sample_pc_with_email(sample_pc):
    """Augment sample_pc with the buyer email fields the parser writes."""
    pc = dict(sample_pc)
    pc["parsed"] = dict(pc.get("parsed", {}))
    pc["parsed"]["header"] = dict(pc["parsed"].get("header", {}))
    pc["parsed"]["header"]["requestor_email"] = "buyer@cdcr.ca.gov"
    pc["parsed"]["header"]["requestor_name"] = "Test Buyer"
    pc["original_sender"] = "buyer@cdcr.ca.gov"
    return pc


def _read_quote_row(quote_number: str) -> dict | None:
    """Read the persisted row directly. We don't trust intermediate
    dicts — the buyer pricing memory panel queries `quotes` directly,
    so that's the source of truth this test verifies."""
    from src.core.db import get_db
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM quotes WHERE quote_number = ?",
            (quote_number,),
        ).fetchone()
    return dict(row) if row else None


def test_pc_quote_writes_contact_email_to_quotes_row(
    tmp_path, sample_pc_with_email
):
    """The headline assertion: after generate_quote_from_pc, the
    `quotes` row for the new quote_number has `contact_email` populated
    from the PC's parsed.header.requestor_email."""
    from src.forms.quote_generator import generate_quote_from_pc

    out = str(tmp_path / "pc_email.pdf")
    qn = "R26Q888"
    r = generate_quote_from_pc(sample_pc_with_email, out, quote_number=qn)
    assert r.get("ok"), f"generate_quote_from_pc failed: {r}"

    row = _read_quote_row(qn)
    assert row is not None, f"Quote row {qn} not persisted"
    assert row["contact_email"] == "buyer@cdcr.ca.gov", (
        f"contact_email expected 'buyer@cdcr.ca.gov', got "
        f"{row['contact_email']!r}. The PC→quote chain dropped the "
        f"buyer email — buyer pricing memory panel will stay empty."
    )


def test_pc_quote_falls_back_to_original_sender_when_header_blank(
    tmp_path, sample_pc
):
    """If the parser didn't extract requestor_email but the Gmail thread
    sender column is populated (older ingest path), the quote row should
    still get a populated contact_email."""
    from src.forms.quote_generator import generate_quote_from_pc

    pc = dict(sample_pc)
    # No parsed.header.requestor_email; only original_sender available.
    pc["original_sender"] = "thread-sender@calvet.ca.gov"

    out = str(tmp_path / "pc_fallback.pdf")
    qn = "R26Q889"
    r = generate_quote_from_pc(pc, out, quote_number=qn)
    assert r.get("ok"), f"generate_quote_from_pc failed: {r}"

    row = _read_quote_row(qn)
    assert row is not None
    assert row["contact_email"] == "thread-sender@calvet.ca.gov"


def test_pc_quote_leaves_contact_email_blank_when_no_source(tmp_path, sample_pc):
    """A PC with neither header nor original_sender should not crash —
    contact_email stays empty (which the buyer pricing memory panel
    correctly excludes from the rollup, per its WHERE clause)."""
    from src.forms.quote_generator import generate_quote_from_pc

    pc = dict(sample_pc)
    pc.pop("original_sender", None)
    pc["parsed"] = dict(pc.get("parsed", {}))
    pc["parsed"]["header"] = {"institution": pc.get("institution", "")}

    out = str(tmp_path / "pc_blank.pdf")
    qn = "R26Q890"
    r = generate_quote_from_pc(pc, out, quote_number=qn)
    assert r.get("ok"), f"generate_quote_from_pc failed: {r}"

    row = _read_quote_row(qn)
    assert row is not None
    assert (row["contact_email"] or "") == ""


def test_rfq_quote_writes_contact_email_to_quotes_row(tmp_path, sample_rfq):
    """The RFQ path already had `requestor_email` in its data dict, but
    that didn't survive into the result dict either. Lock the same
    contract for RFQ-sourced quotes."""
    from src.forms.quote_generator import generate_quote_from_rfq

    out = str(tmp_path / "rfq_email.pdf")
    qn = "R26Q891"
    r = generate_quote_from_rfq(sample_rfq, out, quote_number=qn)
    assert r.get("ok"), f"generate_quote_from_rfq failed: {r}"

    row = _read_quote_row(qn)
    assert row is not None
    # sample_rfq has requestor_email = "jane@state.ca.gov"
    assert row["contact_email"] == "jane@state.ca.gov"


def test_buyer_pricing_memory_panel_picks_up_quote_after_send(
    tmp_path, sample_pc_with_email
):
    """End-to-end with the actual /growth-intel panel: after a PC quote
    is generated AND its status is moved to 'sent', the buyer rollup
    panel should return a row for that buyer."""
    from src.forms.quote_generator import generate_quote_from_pc, update_quote_status
    from src.api.modules.routes_growth_intel import _build_buyer_pricing_memory

    out = str(tmp_path / "e2e.pdf")
    qn = "R26Q892"
    r = generate_quote_from_pc(sample_pc_with_email, out, quote_number=qn)
    assert r.get("ok")
    # Move to 'sent' so the test mirrors the operator workflow that
    # populates a "sent" row the panel can find.
    update_quote_status(qn, "sent", actor="test")

    panel = _build_buyer_pricing_memory(window_days=90, limit=20)
    assert panel["ok"]
    emails = [r["contact_email"] for r in panel["rows"]]
    assert "buyer@cdcr.ca.gov" in emails, (
        f"Buyer pricing memory panel doesn't see the new quote's buyer. "
        f"Rows: {panel['rows']}"
    )
