"""Regression: QA gate banner sits ABOVE the action bar, not below it.

Incident 2026-05-01 (Mike's screenshot): the inline `#rfq-qa-gate-banner`
rendered AFTER `[data-testid="rfq-primary-actions"]` in the DOM. With
3 bullet points the banner pushed past the spacer and visually crowded
the lower-right area where the Save / Finalize buttons live.

Two structural fixes are locked here:
  1. The banner now precedes the primary action bar in DOM order so its
     expansion never crowds the buttons.
  2. The sticky CTA bar carries a `#sticky-qa-gate-chip` so when the
     operator scrolls past the inline buttons, the blocker count + most
     urgent message travels with them in the bottom-pinned strip
     instead of disappearing off-screen.
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


def test_sticky_cta_carries_qa_gate_chip(client, temp_data_dir, sample_rfq):
    rid = _seed(temp_data_dir, sample_rfq)
    html = _fetch(client, rid)
    sticky_pos = html.find('id="rfq-sticky-cta"')
    chip_pos = html.find('id="sticky-qa-gate-chip"')
    assert sticky_pos > 0, "sticky CTA missing"
    assert chip_pos > sticky_pos, (
        "sticky-qa-gate-chip must live INSIDE the sticky CTA bar so the "
        "QA blocker message follows the buttons when scrolled past inline"
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
