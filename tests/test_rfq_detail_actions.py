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


class TestNotesToBuyerHidden:
    """Notes to Buyer is a 704-fill concern — hiding on the RFQ detail page
    per user direction 2026-04-20. Existing stored `quote_notes` values are
    still rendered on the generated quote PDF; only the input is gone."""

    def test_textarea_removed_from_detail_page(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_with_status(temp_data_dir, sample_rfq, "new")
        html = _fetch_detail(client, rid)
        assert 'name="quote_notes"' not in html, (
            "Notes to Buyer textarea must not render on RFQ detail"
        )
        assert "📝 Notes to Buyer" not in html, (
            "Notes to Buyer label must not render on RFQ detail"
        )

    def test_textarea_hidden_across_statuses(
        self, client, temp_data_dir, sample_rfq
    ):
        for status in ("new", "generated", "sent", "won", "lost"):
            rid = _seed_with_status(temp_data_dir, sample_rfq, status)
            html = _fetch_detail(client, rid)
            assert 'name="quote_notes"' not in html, (
                f"Notes to Buyer textarea leaked at status={status}"
            )

    def test_stored_quote_notes_do_not_leak_to_detail_page(
        self, client, temp_data_dir, sample_rfq
    ):
        """Even if an RFQ already has quote_notes set (from earlier 704
        fill), the value must not render on the detail page via some
        other echo path."""
        rfq = dict(sample_rfq)
        rfq["status"] = "new"
        rfq["quote_notes"] = "LEAKY_NOTE_MARKER_ZX9"
        import json as _json
        import os as _os
        path = _os.path.join(temp_data_dir, "rfqs.json")
        with open(path, "w", encoding="utf-8") as f:
            _json.dump({rfq["id"]: rfq}, f)
        html = _fetch_detail(client, rfq["id"])
        assert "LEAKY_NOTE_MARKER_ZX9" not in html, (
            "Stored quote_notes must not be displayed on RFQ detail"
        )


class TestPackageSettingsRelocated:
    """The per-RFQ `Package Settings` card was renamed `Templates & Files`
    and the per-RFQ form-include checkboxes were removed — the canonical
    place to configure which forms an agency requires is
    /settings/packages (app-level, agency-wide).

    Incident to prevent: a user looks at the RFQ detail page, unchecks
    `704B` to "not include it for this one", generates a package, and
    then an RFQ for the same agency the next day silently picks up the
    wrong form set. Config belongs at agency level, not per-RFQ.
    """

    def test_card_title_is_templates_and_files(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_with_status(temp_data_dir, sample_rfq, "new")
        html = _fetch_detail(client, rid)
        assert "Templates &amp; Files" in html or "Templates & Files" in html, (
            "Templates & Files card title missing on RFQ detail"
        )
        # The old "Package Settings" label must not render in the card
        # summary (still allowed to appear in hidden/data attrs).
        assert "⚙ Package Settings" not in html, (
            "Old 'Package Settings' card title must not remain on RFQ detail"
        )

    def test_per_rfq_form_checkboxes_removed(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_with_status(temp_data_dir, sample_rfq, "new")
        html = _fetch_detail(client, rid)
        assert 'class="pkg-form"' not in html, (
            "Per-RFQ pkg-form checkboxes must not render on RFQ detail — "
            "agency-wide config lives at /settings/packages"
        )
        assert "Include in package:" not in html, (
            "'Include in package:' header belongs to the removed checkbox block"
        )

    def test_points_to_app_level_settings(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_with_status(temp_data_dir, sample_rfq, "new")
        html = _fetch_detail(client, rid)
        assert 'href="/settings/packages"' in html, (
            "RFQ detail must link to app-level Settings → Packages"
        )

    def test_autosave_js_does_not_collect_package_forms(
        self, client, temp_data_dir, sample_rfq
    ):
        """Belt-and-suspenders: with the checkboxes gone, the autosave
        JS that read `.pkg-form` must also be gone so the payload never
        tries to overwrite stored agency defaults with an empty set."""
        rid = _seed_with_status(temp_data_dir, sample_rfq, "new")
        html = _fetch_detail(client, rid)
        assert "document.querySelectorAll('.pkg-form')" not in html, (
            ".pkg-form autosave collector must be removed"
        )
        assert "payload.package_forms=" not in html, (
            "Autosave payload must no longer send package_forms from detail page"
        )


class TestIntelColumnHeaderLabeled:
    """The item-table intel column used to be a bare 📊 header — the
    label text is invisible without hover, so users could not tell what
    the column meant or what the 📊N / 📦 / — glyphs indicated.

    Locking in a visible "📊 History" label plus an explanatory title
    attribute so a future "tighten the columns" pass doesn't silently
    revert to the icon-only header.
    """

    def test_header_carries_visible_label(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_with_status(temp_data_dir, sample_rfq, "new")
        html = _fetch_detail(client, rid)
        assert ">📊 History<" in html, (
            "Intel column header must show a visible 'History' label"
        )

    def test_header_title_explains_glyphs(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_with_status(temp_data_dir, sample_rfq, "new")
        html = _fetch_detail(client, rid)
        # The title attribute should explain each glyph (📊N / 📦 / —)
        assert "📊N" in html, "header tooltip must document the 📊N glyph"
        assert "📦" in html, "header tooltip must document the 📦 catalog-match glyph"
