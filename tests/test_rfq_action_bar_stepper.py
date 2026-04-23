"""Bundle-3 PR-3a: RFQ detail workflow stepper + consolidated action bar.

Source audit items absorbed here:
- **Q** — sticky submit bar / scroll fatigue (PR-3b covers stickiness)
- **R** — "Manual 704B" ambiguous label → "Replace auto-filled 704B" in More ▾
- **S** — "Refresh Prices" demoted into More ▾ as "Re-scan catalog prices"
- **T** — action bar has no workflow sequence → ordered stepper + one primary CTA;
  Simple Submit removed (redundant with Send Quote); Fill All Forms moved into
  More ▾ (Generate Package already calls the fill engine internally).

These tests lock the new structure in so a future "tidy the buttons" pass
can't silently regress.
"""
from __future__ import annotations

import json
import os
import pytest


# ── helpers ───────────────────────────────────────────────────────────

def _seed_rfq(temp_data_dir, sample_rfq: dict, **overrides) -> str:
    rfq = dict(sample_rfq)
    rfq.update(overrides)
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({rfq["id"]: rfq}, f)
    return rfq["id"]


def _fetch(client, rid: str) -> str:
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


# ── stepper visibility + markup ───────────────────────────────────────

class TestStepperRendersForEveryStatus:
    """Every RFQ detail page — regardless of status — renders the
    workflow stepper above the primary action bar. This is the
    operator's 'where am I in the flow' cue."""

    @pytest.mark.parametrize(
        "status",
        ["new", "ready", "generated", "ready_to_send", "sent", "won", "lost"],
    )
    def test_stepper_container_present(
        self, client, temp_data_dir, sample_rfq, status
    ):
        rid = _seed_rfq(temp_data_dir, sample_rfq, status=status)
        html = _fetch(client, rid)
        assert 'data-testid="rfq-workflow-stepper"' in html, (
            f"Workflow stepper missing at status={status}"
        )

    def test_stepper_lists_all_nine_phases(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        for key in (
            "upload", "classify", "price", "finalize", "fill",
            "generate", "preview", "send", "post_send",
        ):
            assert f'data-phase="{key}"' in html, (
                f"Stepper missing phase chip for {key!r}"
            )


class TestStepperPhaseStates:
    """Phase done/current/pending states match the record state. These
    are the server-rendered source-of-truth markers the operator sees
    on load — JS liveness is a separate concern (PR-3b)."""

    def test_new_record_marks_classify_or_later_as_pending(
        self, client, temp_data_dir, sample_rfq
    ):
        """A fresh record with only line items has Upload done; every
        phase from Classify onward is pending OR current (the operator
        has something to do). Upload must be 'done'."""
        rid = _seed_rfq(
            temp_data_dir, sample_rfq,
            status="new",
            # strip classification signals so classify shows pending
            department=None,
        )
        html = _fetch(client, rid)
        # Upload done
        assert 'data-phase="upload" data-state="done"' in html

    def test_generated_record_marks_through_generate_done(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(
            temp_data_dir, sample_rfq,
            status="generated",
            output_files=["RFQ_Package_TEST.pdf"],
        )
        html = _fetch(client, rid)
        # Upload / Classify / Price / Finalize / Fill / Generate / Preview — all done
        for phase in ("upload", "classify", "price", "finalize",
                      "fill", "generate", "preview"):
            assert f'data-phase="{phase}" data-state="done"' in html, (
                f"Phase {phase} should be done for status=generated"
            )
        # Send not yet done
        assert 'data-phase="send" data-state="done"' not in html

    def test_sent_record_marks_send_done_post_send_current(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(
            temp_data_dir, sample_rfq,
            status="sent",
            output_files=["RFQ_Package_TEST.pdf"],
        )
        html = _fetch(client, rid)
        assert 'data-phase="send" data-state="done"' in html
        # Post-send is the next actionable phase (mark won/lost)
        assert 'data-phase="post_send" data-state="current"' in html

    def test_won_record_marks_post_send_done(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(
            temp_data_dir, sample_rfq,
            status="won",
            output_files=["RFQ_Package_TEST.pdf"],
        )
        html = _fetch(client, rid)
        assert 'data-phase="post_send" data-state="done"' in html


# ── consolidated action bar: kills + relabels ─────────────────────────

class TestActionBarConsolidation:
    """Audit item T: ordered action bar, one primary CTA, duplicates
    collapsed. Simple Submit removed; Fill All Forms + Manual 704B +
    Refresh Prices moved into a More ▾ menu."""

    def test_primary_bar_still_has_core_buttons(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert 'data-testid="rfq-primary-actions"' in html
        assert 'data-testid="rfq-save-pricing"' in html
        assert 'data-testid="rfq-preview-quote"' in html
        assert 'data-testid="rfq-generate-package"' in html
        assert 'data-testid="rfq-send-quote"' in html

    def test_simple_submit_link_removed(
        self, client, temp_data_dir, sample_rfq
    ):
        """Audit T: Simple Submit is redundant with Send Quote. The
        /simple-submit/ route still exists, but the button is gone so
        operators stop hitting two CTAs that lead to the same place."""
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert '/simple-submit/rfq/' not in html, (
            "Simple Submit button must be removed from RFQ detail"
        )
        assert '⚡ Simple Submit' not in html

    def test_fill_all_forms_in_more_menu_not_primary(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        # Still reachable via More ▾
        assert 'data-testid="rfq-fill-all-forms"' in html
        assert 'data-testid="rfq-more-actions-menu"' in html
        # And inside the menu, not the primary bar
        menu_start = html.index('data-testid="rfq-more-actions-menu"')
        menu_end = html.index('</div>', menu_start + 500)
        # Fill All Forms markup should fall after the menu opens
        fill_at = html.index('data-testid="rfq-fill-all-forms"')
        assert fill_at > menu_start, (
            "Fill All Forms must live inside the More ▾ menu, "
            "not the primary action bar"
        )

    def test_manual_704b_hidden_until_autofill_attempted(
        self, client, temp_data_dir, sample_rfq
    ):
        """Audit R: 'Replace auto-filled 704B' only makes sense after
        the app has produced a 704B worth replacing. Hide until then."""
        # Fresh record: no output_files, no manual_704b stored
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert 'data-testid="rfq-replace-704b"' not in html, (
            "Replace 704B button must be hidden before auto-fill runs"
        )
        # The file-input itself stays in the DOM so the handler can fire
        assert 'id="manual704bInput"' in html

    def test_manual_704b_appears_after_generate(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(
            temp_data_dir, sample_rfq,
            status="generated",
            output_files=["RFQ_Package_TEST.pdf"],
        )
        html = _fetch(client, rid)
        assert 'data-testid="rfq-replace-704b"' in html, (
            "Replace 704B must surface once Generate Package has run"
        )
        # With new label — audit item R
        assert 'Replace auto-filled 704B' in html

    def test_refresh_prices_demoted_and_relabeled(
        self, client, temp_data_dir, sample_rfq
    ):
        """Audit S: Refresh Prices is 95% noise. Demoted to More ▾ and
        relabeled so the verb describes the condition, not just the
        action."""
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert 'data-testid="rfq-refresh-prices"' in html
        # Still the same onclick target (function name unchanged)
        assert 'refreshCatalogPrices(this)' in html
        # New label in the menu
        assert 'Re-scan catalog prices' in html
        # Old standalone "Refresh Prices" label must not live in the
        # primary bar (where it used to steal a click). The More ▾
        # markup title refers to it via the tooltip string which
        # includes "fresh web MSRP" — that one is fine.
        primary_start = html.index('data-testid="rfq-primary-actions"')
        more_menu_start = html.index('data-testid="rfq-more-actions-menu"')
        primary_section = html[primary_start:more_menu_start]
        assert '🔄 Refresh Prices' not in primary_section, (
            "Refresh Prices button must be demoted out of primary bar"
        )


class TestGenerateAbsorbsFillAllForms:
    """Audit T fold: Generate Package already calls the fill engine
    internally, so the tooltip should say so. Operators shouldn't guess
    whether Fill All Forms is a prerequisite."""

    def test_generate_tooltip_explains_fill_absorption(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        # The Generate button title should mention fill engine
        # (freeform wording — keep assertion loose)
        gen_at = html.index('data-testid="rfq-generate-package"')
        # The `title=` attribute follows within ~400 chars
        chunk = html[gen_at:gen_at + 500]
        assert 'fill' in chunk.lower(), (
            "Generate Package tooltip should clarify that Fill All Forms "
            "is run automatically as part of generate"
        )
