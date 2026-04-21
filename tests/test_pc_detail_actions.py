"""Regression guards for the PC detail action-bar dedup.

On `status=='sent'` the page used to render Mark Won / Mark Lost TWICE:
once in the green "Sent — Awaiting award decision" banner at the top and
once in the stage-aware action bar a few rows below. The action-bar pair
was removed (banner is the canonical placement — it sits right next to
the award-decision message, so the decision-time buttons belong there).
These tests lock that in so a future refactor doesn't re-introduce the
duplicate.
"""
from __future__ import annotations

import json
import os


def _seed_with_status(temp_data_dir, sample_pc: dict, status: str) -> str:
    pc = dict(sample_pc)
    pc["status"] = status
    path = os.path.join(temp_data_dir, "price_checks.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({pc["id"]: pc}, f)
    return pc["id"]


def _fetch(client, pcid: str) -> str:
    resp = client.get(f"/pricecheck/{pcid}")
    assert resp.status_code == 200, (
        f"/pricecheck/{pcid} returned {resp.status_code}"
    )
    return resp.get_data(as_text=True)


class TestMarkWonLostDedup:
    def test_sent_status_renders_mark_won_exactly_once(
        self, client, temp_data_dir, sample_pc
    ):
        pcid = _seed_with_status(temp_data_dir, sample_pc, "sent")
        html = _fetch(client, pcid)
        won_count = html.count("adminAction('won')")
        assert won_count == 1, (
            f"expected 1 Mark Won button for sent PC, found {won_count} "
            f"— duplicate in stage action bar?"
        )

    def test_sent_status_renders_mark_lost_exactly_once(
        self, client, temp_data_dir, sample_pc
    ):
        pcid = _seed_with_status(temp_data_dir, sample_pc, "sent")
        html = _fetch(client, pcid)
        lost_count = html.count("adminAction('lost')")
        assert lost_count == 1, (
            f"expected 1 Mark Lost button for sent PC, found {lost_count} "
            f"— duplicate in stage action bar?"
        )

    def test_sent_status_keeps_view_sent_button(
        self, client, temp_data_dir, sample_pc
    ):
        """View Sent is unique to the sent-stage action bar — it must not
        be removed alongside the Mark Won/Lost dedup."""
        pcid = _seed_with_status(temp_data_dir, sample_pc, "sent")
        html = _fetch(client, pcid)
        assert "viewOriginalPc()" in html
        assert ">View Sent<" in html

    def test_sent_banner_still_carries_decision_buttons(
        self, client, temp_data_dir, sample_pc
    ):
        """Belt-and-suspenders: the remaining pair lives in the banner
        (next to the 'Awaiting award decision' message). Verify they're
        present — if someone removed the banner buttons too, the page
        would have zero decision-time affordances."""
        pcid = _seed_with_status(temp_data_dir, sample_pc, "sent")
        html = _fetch(client, pcid)
        assert "Mark Won" in html
        assert "Mark Lost" in html


class TestStatusBannerCopyMatchesButtons:
    """The colored status banner at the top tells the user what to do next
    ("then Save & Generate", "Regenerate if prices changed"). Those phrases
    must match the actual button labels in the stage action bar below —
    otherwise the user hunts for a button that doesn't exist with that name.
    """

    def test_priced_banner_names_the_real_button(
        self, client, temp_data_dir, sample_pc
    ):
        """priced-status action bar button is 'Save & Generate' — the
        banner hint must say the same, not the old 'Save & Fill 704'."""
        pcid = _seed_with_status(temp_data_dir, sample_pc, "priced")
        html = _fetch(client, pcid)
        assert "Save &amp; Generate" in html or "Save & Generate" in html, (
            "Priced banner must point to the actual 'Save & Generate' button"
        )
        # Old copy must not leak back in
        assert "Save &amp; Fill 704" not in html, (
            "Priced banner still references the old 'Save & Fill 704' label"
        )

    def test_completed_banner_names_the_real_button(
        self, client, temp_data_dir, sample_pc
    ):
        """completed-status action bar button is 'Regenerate' — the
        banner hint must say the same, not the old 'Re-fill'."""
        pcid = _seed_with_status(temp_data_dir, sample_pc, "completed")
        html = _fetch(client, pcid)
        assert "Regenerate if prices changed" in html, (
            "Completed banner must use 'Regenerate' to match the button label"
        )
        assert "Re-fill if prices changed" not in html, (
            "Completed banner still references the old 'Re-fill' label"
        )


class TestNoDeadSaveAndGenerateCluster:
    """saveAndGenerate() / _doGenerate704() / _showPipelineError() were a
    43-line dead cluster: no button had onclick="saveAndGenerate(...)",
    and the three functions were reachable only from each other. They
    kept setting button text back to old labels like "Save & Fill 704"
    and "Re-fill 704" — labels that don't match the real buttons anymore.
    Deleted. savePrices() is the single live save-and-generate entry.
    """

    def test_dead_saveAndGenerate_function_is_gone(
        self, client, temp_data_dir, sample_pc
    ):
        pcid = _seed_with_status(temp_data_dir, sample_pc, "priced")
        html = _fetch(client, pcid)
        assert "function saveAndGenerate" not in html, (
            "Dead saveAndGenerate() function is back — removed 2026-04-20 "
            "because no button calls it"
        )
        assert "function _doGenerate704" not in html, (
            "Dead _doGenerate704() is back"
        )
        assert "function _showPipelineError" not in html, (
            "Dead _showPipelineError() is back"
        )

    def test_no_button_onclick_points_to_saveAndGenerate(
        self, client, temp_data_dir, sample_pc
    ):
        pcid = _seed_with_status(temp_data_dir, sample_pc, "priced")
        html = _fetch(client, pcid)
        assert 'onclick="saveAndGenerate' not in html, (
            "Save path must go through savePrices() — not the deleted "
            "saveAndGenerate()"
        )

    def test_save_and_fill_label_is_gone_from_toast(
        self, client, temp_data_dir, sample_pc
    ):
        """The savePrices() fallback toast used to say "PDF will generate
        on next Save & Fill" — but the button is labeled "Save &
        Generate". Aligned."""
        pcid = _seed_with_status(temp_data_dir, sample_pc, "priced")
        html = _fetch(client, pcid)
        assert "next Save & Fill" not in html, (
            "Stale 'Save & Fill' label still referenced in savePrices toast"
        )
        assert "next Save & Generate" in html, (
            "savePrices fallback toast must name the real button"
        )


class TestEarlyStageBannerCopy:
    """The 'new' and 'parsed' status banners tell the user what button
    to click next. They used to reference tool names from the old
    7-button pricing UI ('SCPRS/Amazon/Catalog') which no longer exist
    as separate top-level buttons. Each banner embeds its own real
    button — point the copy at that button's label so the hint is
    actionable, not archaeology.
    """

    def test_new_banner_points_to_auto_price_now_button(
        self, client, temp_data_dir, sample_pc
    ):
        pcid = _seed_with_status(temp_data_dir, sample_pc, "new")
        html = _fetch(client, pcid)
        assert "click Auto-Price Now" in html, (
            "New banner must point to the 'Auto-Price Now' button that "
            "lives in the banner itself"
        )
        assert "run SCPRS/Amazon/Catalog" not in html, (
            "New banner still lists removed individual-source tools"
        )

    def test_parsed_banner_points_to_auto_enrich_button(
        self, client, temp_data_dir, sample_pc
    ):
        pc = dict(sample_pc)
        pc["status"] = "parsed"
        pc.pop("enrichment_status", None)
        path = os.path.join(temp_data_dir, "price_checks.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({pc["id"]: pc}, f)
        html = _fetch(client, pc["id"])
        assert "Click Auto-Enrich" in html, (
            "Parsed banner must point to the 'Auto-Enrich' button"
        )
        assert "Click to search Catalog + SCPRS" not in html, (
            "Parsed banner still uses the old vague 'search Catalog + "
            "SCPRS' copy"
        )
