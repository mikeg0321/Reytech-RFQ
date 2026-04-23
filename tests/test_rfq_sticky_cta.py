"""Bundle-3 PR-3b: sticky CTA bar + Cmd/Ctrl+Enter keyboard commit.

Source audit item Q (2026-04-22): scroll fatigue on a 1-2 item RFQ —
operator must scroll up to verify totals, down to hit Send. Sticky CTA
mirrors Finalize / Generate / Send + live Total/Profit/Items in a 1-line
bar pinned to viewport bottom. Plus Cmd/Ctrl+Enter from any price cell
fires Generate Package so a "type-then-ship" muscle-memory flow exists.

These tests pin the new structure so a future "tidy the inline JS" pass
can't quietly delete the sticky bar, the spacer, or the keyboard hook.
"""
from __future__ import annotations

import json
import os
import pytest


def _seed(temp_data_dir, sample_rfq, **overrides):
    rfq = dict(sample_rfq)
    rfq.update(overrides)
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({rfq["id"]: rfq}, f)
    return rfq["id"]


def _fetch(client, rid):
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


class TestStickyCtaRenders:
    """Sticky bar must render on every RFQ detail page (not just one
    status), since the scroll-fatigue problem applies regardless of
    where in the lifecycle the operator is."""

    @pytest.mark.parametrize(
        "status",
        ["new", "ready", "generated", "sent", "won", "lost"],
    )
    def test_sticky_container_present(
        self, client, temp_data_dir, sample_rfq, status
    ):
        rid = _seed(temp_data_dir, sample_rfq, status=status)
        html = _fetch(client, rid)
        assert 'data-testid="rfq-sticky-cta"' in html, (
            f"sticky CTA missing at status={status}"
        )

    def test_sticky_buttons_have_testids(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        for tid in ("sticky-finalize", "sticky-generate", "sticky-send",
                    "sticky-total", "sticky-profit", "sticky-priced"):
            assert f'data-testid="{tid}"' in html, (
                f"sticky element {tid!r} missing"
            )

    def test_sticky_buttons_are_qa_gated(
        self, client, temp_data_dir, sample_rfq
    ):
        """Sticky buttons must carry the same data-qa-gated="1" attribute
        the inline buttons use, so the QA-block JS that disables actions
        until validators pass disables sticky and inline together."""
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        # All three sticky CTAs gated
        for tid in ("sticky-finalize", "sticky-generate", "sticky-send"):
            anchor = html.index(f'data-testid="{tid}"')
            chunk = html[anchor:anchor + 400]
            assert 'data-qa-gated="1"' in chunk, (
                f"{tid} must carry data-qa-gated=\"1\""
            )


class TestStickySpacer:
    """A `position:fixed` bar overlaps the bottom of the page. Must
    reserve viewport space below the main content so the operator can
    scroll the last few rows above the bar."""

    def test_spacer_present(self, client, temp_data_dir, sample_rfq):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert 'data-testid="rfq-sticky-spacer"' in html, (
            "spacer reserving viewport space below main content is missing"
        )


class TestStickyClickDelegation:
    """Sticky buttons must NOT duplicate the inline handlers — they
    must delegate into the inline buttons via testid lookup. Single
    source of truth = no risk of sticky firing a handler that diverges
    from inline (e.g. inline runs busy-state toggle, sticky doesn't)."""

    def _button_chunk(self, html: str, testid: str) -> str:
        """Return the full <button> tag containing the given testid —
        inclusive of attributes (like `onclick`) declared BEFORE the
        testid attribute. The button opens at the nearest preceding
        `<button` and closes at the nearest following `</button>`."""
        anchor = html.index(f'data-testid="{testid}"')
        start = html.rfind("<button", 0, anchor)
        end = html.index("</button>", anchor)
        assert start != -1, f"no <button> opening before {testid}"
        return html[start:end + len("</button>")]

    def test_sticky_finalize_delegates_to_inline(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        chunk = self._button_chunk(html, "sticky-finalize")
        assert "_stickyClick('rfq-save-pricing')" in chunk

    def test_sticky_generate_delegates_to_inline(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        chunk = self._button_chunk(html, "sticky-generate")
        assert "_stickyClick('rfq-generate-package')" in chunk

    def test_sticky_send_delegates_to_inline(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        chunk = self._button_chunk(html, "sticky-send")
        assert "_stickyClick('rfq-send-quote')" in chunk

    def test_sticky_click_helper_defined(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        # Helper must be exposed on window so onclick can reach it
        assert "window._stickyClick = _stickyClick" in html


class TestKeyboardCommit:
    """Cmd/Ctrl+Enter from a price/cost cell must fire Generate
    Package. The handler is delegated at document level so it survives
    the recalcFromBid()-driven DOM swaps."""

    def test_keydown_handler_present(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        # Document-level keydown listener
        assert "document.addEventListener('keydown'" in html, (
            "keydown handler for Cmd/Ctrl+Enter missing"
        )
        # And it actually targets the price/cost inputs
        assert 'input[name^="price_"], input[name^="cost_"]' in html, (
            "keydown handler must filter to price/cost cells only"
        )
        # And it fires the generate handler
        assert "_stickyClick('rfq-generate-package')" in html


class TestRecalcSync:
    """The sticky Total/Profit/Items chips must stay in sync with the
    canonical recalc() outputs (#tot / #pft / #rfq-items-priced-kpi).
    The sync wraps recalc() once after DOMContentLoaded — the inline
    recalc() lives in a script block lower on the page."""

    def test_recalc_hook_present(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert "_hookRecalc" in html, (
            "recalc-wrapping helper missing — sticky cells will not sync"
        )
        assert "_syncStickyKpis" in html
        # Reads from the canonical KPI cells, doesn't recompute math
        for cell in ("'tot'", "'pft'", "'rfq-items-priced-kpi'"):
            assert cell in html, (
                f"sync helper must read source-of-truth cell {cell}"
            )


class TestInlineActionsStillRender:
    """Belt-and-suspenders: the sticky bar is additive. The inline
    primary action bar from PR-3a must STILL render so the operator
    has BOTH a mid-page CTA cluster and a viewport-pinned one."""

    def test_inline_primary_actions_intact(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert 'data-testid="rfq-primary-actions"' in html
        assert 'data-testid="rfq-save-pricing"' in html
        assert 'data-testid="rfq-generate-package"' in html
        assert 'data-testid="rfq-send-quote"' in html
        assert 'data-testid="rfq-workflow-stepper"' in html
