"""Bundle-2 PR-2d: visual ingest confirmation (audit item P).

Mike's exact grievance from the 2026-04-22 session audit:
    "no visual confirmation received or validated data."

He had just uploaded an email screenshot to RFQ #9ad8a0ac. Server-
side, the tax rate went from 7.25% → 7.75% via CDTFA, the body_text
populated, the requirements extractor could have run. UI showed
**nothing**. App-as-source-of-truth collapses when the app stays
silent about being one.

These tests pin the three visible-side fixes:
  1. `rfqPulseField()` helper + CSS keyframe exist so any ingest-
     trigger can flash the updated cell.
  2. `lookupTaxRate()` calls the pulse helper + `showMsg()` when
     the returned rate differs from what was already on screen.
  3. Requirements extractor auto-fires on page load when body has
     content but `requirements_json` hasn't been populated yet.
"""
from __future__ import annotations

import json
import os

import pytest


def _seed_rfq(temp_data_dir, sample_rfq, **overrides):
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


class TestPulseHelper:
    """A CSS keyframe + `window.rfqPulseField` helper must be
    available on every RFQ detail page so future ingest triggers
    can flash an updated cell without re-implementing the animation."""

    def test_keyframe_defined(self, client, temp_data_dir, sample_rfq):
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert "@keyframes rfq-field-pulse" in html
        # Tunable color = green; changing the shade later is fine,
        # but the animation MUST exist.
        assert "rgba(63,185,80" in html

    def test_pulse_helper_exposed_on_window(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        # Helper is assigned to window.* so any later inline script
        # can call it (e.g., PR-2e's slot-dismiss could pulse the
        # slot strip when it clears a slot).
        assert "window.rfqPulseField = function" in html

    def test_pulse_helper_sets_updated_from_tooltip(
        self, client, temp_data_dir, sample_rfq
    ):
        """Operator hovering a just-pulsed field should see 'Updated
        from <source>' — makes silent updates observable without
        inspecting network tab."""
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        assert "Updated from " in html


class TestTaxRateChangePulse:
    """`lookupTaxRate()` must pulse + toast when the returned rate
    differs from what was already on screen. No-op on same-value
    re-confirms (that'd be noise)."""

    def test_lookup_captures_prior_rate_and_computes_diff(
        self, client, temp_data_dir, sample_rfq
    ):
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        # Find the lookupTaxRate function body
        start = html.index("function lookupTaxRate(")
        chunk = html[start:start + 3000]
        # Prior-rate capture — regression guard against removing
        # the "compare before flashing" optimization
        assert "priorRate" in chunk
        # Diff-based branch on newRate
        assert "newRate - priorRate" in chunk or "priorRate - newRate" in chunk
        # Pulse helper invoked
        assert "rfqPulseField" in chunk
        # Toast shown with both old and new rates in the message
        assert "Tax rate updated" in chunk

    def test_same_value_rejection_threshold_is_loose(
        self, client, temp_data_dir, sample_rfq
    ):
        """The diff guard uses a 0.001 absolute threshold so
        floating-point round-trips (e.g. 7.75 → 7.75 via CDTFA
        cache) don't fire a spurious pulse."""
        rid = _seed_rfq(temp_data_dir, sample_rfq, status="new")
        html = _fetch(client, rid)
        start = html.index("function lookupTaxRate(")
        chunk = html[start:start + 3000]
        assert "0.001" in chunk


class TestRequirementsAutoFire:
    """Auto-fire the requirements extractor on page load when body
    has content but requirements haven't been parsed. Uses a
    setTimeout so it doesn't race the tax-lookup autofire."""

    def test_autofire_present_when_body_populated(
        self, client, temp_data_dir, sample_rfq
    ):
        """Body has content, no requirements — auto-fire should
        render."""
        rid = _seed_rfq(
            temp_data_dir, sample_rfq,
            status="new",
            body_text="Please quote the following items. Due 2026-05-01.",
        )
        html = _fetch(client, rid)
        # Guard markers — the auto-fire block gates on the button's
        # current text to avoid re-triggering after a "No
        # requirements detected" result.
        assert "Extract Now" in html  # the button label we gate on
        assert "reExtractRequirements()" in html
        # The gated block carries this specific guard comment.
        assert "Only fire if it's still the first-time" in html

    def test_no_autofire_when_body_empty(
        self, client, temp_data_dir, sample_rfq
    ):
        """No body text means no extractor signal. The setTimeout
        block must not render — otherwise the extractor fires with
        nothing to extract and logs a wasteful 'no match' reason."""
        rid = _seed_rfq(
            temp_data_dir, sample_rfq,
            status="new",
            body_text="",
            body="",
            body_preview="",
        )
        html = _fetch(client, rid)
        # The Jinja guard is `_body_has_content` — if it's False, the
        # setTimeout block never renders, so the label substring
        # inside that block won't appear in the HTML.
        # Only the gated block contains this guard comment — the
        # outer helper JS block has its own generic top-comment but
        # never this specific first-time-label check.
        assert "Only fire if it's still the first-time" not in html

    def test_no_autofire_when_requirements_already_parsed(
        self, client, temp_data_dir, sample_rfq
    ):
        """If requirements_json is populated, extractor already ran
        — no need to re-fire on every page load."""
        rid = _seed_rfq(
            temp_data_dir, sample_rfq,
            status="new",
            body_text="Please quote these.",
            requirements_json={"forms_required": ["703b"], "extraction_method": "regex"},
        )
        html = _fetch(client, rid)
        # Only the gated block contains this guard comment — the
        # outer helper JS block has its own generic top-comment but
        # never this specific first-time-label check.
        assert "Only fire if it's still the first-time" not in html
