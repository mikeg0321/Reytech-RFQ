"""Regression: QA gate banner sits ABOVE the action bar, not below it.

Incident 2026-05-01 (Mike's screenshot): the inline `#rfq-qa-gate-banner`
rendered AFTER `[data-testid="rfq-primary-actions"]` in the DOM. With
3 bullet points the banner pushed past the spacer and visually crowded
the lower-right area where the Save / Finalize buttons live.

Incident 2026-05-04 (Mike's second screenshot): the sticky `#sticky-
qa-gate-chip` lived INSIDE `#rfq-sticky-cta`, so when blockers fired
the bar grew taller and the action buttons row crowded / wrapped on
narrow viewports — partially hiding the Finalize / Generate / Send
buttons. The chip now lives in its own fixed bar
`#rfq-sticky-qa-bar` pinned just above the CTA, so the action
buttons keep a stable height regardless of blocker state.

Three structural fixes are locked here:
  1. The banner now precedes the primary action bar in DOM order so its
     expansion never crowds the buttons.
  2. The blocker chip rides along in a sticky strip
     (`#rfq-sticky-qa-bar`) so when the operator scrolls past the
     inline buttons, the blocker count + most-urgent message stays
     on screen instead of disappearing.
  3. That sticky strip is a separate fixed element from
     `#rfq-sticky-cta`, so growing/shrinking the chip never moves the
     action buttons.
"""
from __future__ import annotations

import json
import os


def _seed(temp_data_dir, sample_rfq):
    rfq = dict(sample_rfq)
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({rfq["id"]: rfq}, f)
    return rfq["id"]


def _fetch(client, rid):
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    return resp.get_data(as_text=True)


def test_qa_banner_renders_before_primary_action_bar(client, temp_data_dir, sample_rfq):
    rid = _seed(temp_data_dir, sample_rfq)
    html = _fetch(client, rid)
    banner_pos = html.find('id="rfq-qa-gate-banner"')
    actions_pos = html.find('data-testid="rfq-primary-actions"')
    assert banner_pos > 0, "rfq-qa-gate-banner missing from page"
    assert actions_pos > 0, "rfq-primary-actions missing from page"
    assert banner_pos < actions_pos, (
        "QA banner must render BEFORE the primary action bar so the "
        "banner's bullets never push the Save/Finalize buttons. "
        f"banner_pos={banner_pos} actions_pos={actions_pos}"
    )


def test_sticky_qa_bar_renders_above_cta(client, temp_data_dir, sample_rfq):
    """The QA blocker chip rides in its OWN fixed bar
    (`#rfq-sticky-qa-bar`) pinned above `#rfq-sticky-cta`, so the chip
    appearing/disappearing never changes the CTA's height. The QA bar
    must precede the CTA in DOM order (so visual stack: QA bar above
    CTA) and must contain the chip."""
    rid = _seed(temp_data_dir, sample_rfq)
    html = _fetch(client, rid)
    qa_bar_pos = html.find('id="rfq-sticky-qa-bar"')
    sticky_pos = html.find('id="rfq-sticky-cta"')
    chip_pos = html.find('id="sticky-qa-gate-chip"')
    assert qa_bar_pos > 0, "rfq-sticky-qa-bar missing"
    assert sticky_pos > 0, "sticky CTA missing"
    assert chip_pos > 0, "sticky-qa-gate-chip missing"
    assert qa_bar_pos < sticky_pos, (
        "rfq-sticky-qa-bar must render BEFORE rfq-sticky-cta in DOM "
        "order so the QA bar visually sits above the CTA"
    )
    # Chip lives inside the QA bar (between qa_bar opening tag and the
    # CTA opening tag), not inside the CTA.
    assert qa_bar_pos < chip_pos < sticky_pos, (
        "sticky-qa-gate-chip must live INSIDE rfq-sticky-qa-bar (between "
        f"qa_bar_pos={qa_bar_pos} and sticky_pos={sticky_pos}), but "
        f"chip_pos={chip_pos}"
    )


def test_only_one_qa_gate_banner_id(client, temp_data_dir, sample_rfq):
    """During the move I deleted the old banner div. Make sure there's
    exactly one element with id `rfq-qa-gate-banner` (duplicate IDs would
    silently break the JS that reads `getElementById('rfq-qa-gate-banner')`)."""
    rid = _seed(temp_data_dir, sample_rfq)
    html = _fetch(client, rid)
    # Leading space avoids matching the substring inside `data-testid="..."`.
    count = html.count(' id="rfq-qa-gate-banner"')
    assert count == 1, (
        f"expected exactly one #rfq-qa-gate-banner, found {count} — old "
        "post-actions div was probably not removed during the rearrange"
    )


def test_only_one_sticky_qa_chip_id(client, temp_data_dir, sample_rfq):
    """The split moved the chip out of #rfq-sticky-cta into
    #rfq-sticky-qa-bar. Make sure the move didn't leave a stale copy
    behind (duplicate IDs would let the bar grow on its own without the
    JS toggle reaching the right element)."""
    rid = _seed(temp_data_dir, sample_rfq)
    html = _fetch(client, rid)
    count = html.count(' id="sticky-qa-gate-chip"')
    assert count == 1, (
        f"expected exactly one #sticky-qa-gate-chip, found {count}"
    )


def test_qa_bar_default_hidden(client, temp_data_dir, sample_rfq):
    """The QA bar defaults to display:none — it's only revealed by JS
    when both (a) blockers exist and (b) the CTA is visible. A fresh
    page load must not flash the bar."""
    rid = _seed(temp_data_dir, sample_rfq)
    html = _fetch(client, rid)
    anchor = html.index('id="rfq-sticky-qa-bar"')
    chunk = html[anchor:anchor + 600]
    assert "display:none" in chunk, (
        "rfq-sticky-qa-bar must render with inline display:none so it "
        "doesn't flash before the JS gate runs"
    )


def test_qa_bar_position_above_cta(client, temp_data_dir, sample_rfq):
    """The QA bar is `position:fixed` and starts at `bottom:0`; the JS
    `_syncSpacerHeight` overrides bottom to the CTA's offsetHeight at
    runtime so the bar floats exactly on top of the CTA. Lock the
    base style so a future tidy-pass doesn't accidentally make it
    `position:absolute` (which would scroll with the page)."""
    rid = _seed(temp_data_dir, sample_rfq)
    html = _fetch(client, rid)
    anchor = html.index('id="rfq-sticky-qa-bar"')
    chunk = html[anchor:anchor + 600]
    assert "position:fixed" in chunk
    # Z-index above the CTA so a future glow/shadow doesn't put the
    # bar behind the CTA's box-shadow.
    assert "z-index:41" in chunk


def test_qa_bar_sync_wiring_present(client, temp_data_dir, sample_rfq):
    """The runtime contract: _syncSpacerHeight reads BOTH bars'
    heights and sets the QA bar's `bottom` to the CTA's height. Lock
    the sentinels so the wiring can't be quietly removed."""
    rid = _seed(temp_data_dir, sample_rfq)
    html = _fetch(client, rid)
    # Both bars are looked up at JS init.
    assert "getElementById('rfq-sticky-qa-bar')" in html
    # _syncSpacerHeight anchors the QA bar to the CTA's top.
    assert "qaBar.style.bottom" in html
    # _renderQaBar gates visibility on (CTA visible) AND (blockers).
    assert "_renderQaBar" in html
    assert "window._renderRfqStickyQaBar" in html
    # applyGate stamps the bar's blocker state instead of toggling chip
    # display directly — that's the whole point of the split.
    assert "dataset.hasBlockers" in html
