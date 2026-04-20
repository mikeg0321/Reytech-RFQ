"""Regression guards for the RFQ detail action-bar consolidation.

Mark Sent was promoted from the low lifecycle row (~line 985) into the
primary action bar (~line 556) so a user finishing pricing sees the
next lifecycle step next to Generate/Send Quote rather than 400+ lines
down the page. These tests lock that placement in so a future refactor
that moves the button back down (or duplicates it) fails here.
"""
from __future__ import annotations

import json
import os
import pytest


# ── helpers ───────────────────────────────────────────────────────────

def _seed_with_status(temp_data_dir, sample_rfq: dict, status: str) -> str:
    """Write the sample_rfq fixture with the given status; return its id."""
    rfq = dict(sample_rfq)
    rfq["status"] = status
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({rfq["id"]: rfq}, f)
    return rfq["id"]


def _fetch_detail(client, rid: str) -> str:
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200, (
        f"/rfq/{rid} returned {resp.status_code}, expected 200"
    )
    return resp.get_data(as_text=True)


# ── placement guards ──────────────────────────────────────────────────

class TestMarkSentPlacement:
    @pytest.mark.parametrize("status", ["generated", "ready", "ready_to_send"])
    def test_mark_sent_is_in_primary_action_bar(
        self, client, temp_data_dir, sample_rfq, status
    ):
        """The primary Mark Sent button carries data-testid
        `rfq-mark-sent-primary`. Visible only when status is
        generated / ready / ready_to_send."""
        rid = _seed_with_status(temp_data_dir, sample_rfq, status)
        html = _fetch_detail(client, rid)
        assert 'data-testid="rfq-mark-sent-primary"' in html, (
            f"Mark Sent primary button missing from detail page for status={status}"
        )

    @pytest.mark.parametrize("status", ["new", "won", "lost", "no_response"])
    def test_mark_sent_hidden_when_status_disallows(
        self, client, temp_data_dir, sample_rfq, status
    ):
        rid = _seed_with_status(temp_data_dir, sample_rfq, status)
        html = _fetch_detail(client, rid)
        assert 'data-testid="rfq-mark-sent-primary"' not in html, (
            f"Mark Sent must be hidden for status={status}"
        )

    def test_mark_sent_appears_exactly_once(
        self, client, temp_data_dir, sample_rfq
    ):
        """Regression guard: after the consolidation the button must not
        live in both the primary bar AND the bottom lifecycle row. One
        source of truth per action."""
        rid = _seed_with_status(temp_data_dir, sample_rfq, "generated")
        html = _fetch_detail(client, rid)
        # Each rendered button contains the onclick handler. Count those.
        count = html.count("updateRfqStatus('sent')")
        assert count == 1, (
            f"expected 1 Mark Sent button, found {count} — duplicate in lifecycle row?"
        )


class TestPrimaryActionBarShape:
    """The primary action bar still contains Finalize / Generate / Send
    Quote — Mark Sent is an addition, not a replacement."""

    def test_core_buttons_still_present(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_with_status(temp_data_dir, sample_rfq, "generated")
        html = _fetch_detail(client, rid)
        assert 'data-testid="rfq-save-pricing"' in html
        assert 'data-testid="rfq-preview-quote"' in html
        assert 'data-testid="rfq-generate-package"' in html
        assert 'sendQuoteEmail()' in html

    def test_lifecycle_row_retains_other_transitions(
        self, client, temp_data_dir, sample_rfq
    ):
        """Won / Lost / No Response still live in the lifecycle row —
        only Mark Sent moved up."""
        rid = _seed_with_status(temp_data_dir, sample_rfq, "generated")
        html = _fetch_detail(client, rid)
        assert "recordRfqOutcome('won')" in html
        assert "recordRfqOutcome('lost')" in html
        assert "recordRfqOutcome('no_response')" in html
