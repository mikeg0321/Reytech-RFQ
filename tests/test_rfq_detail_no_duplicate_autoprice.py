"""RFQ-2 regression: duplicate Auto-Price buttons removed from RFQ header.

Prior to 2026-04-22 the header had two buttons 1 row apart:
  - "Auto-Price All" → applyRecommendations() (legacy)
  - "Auto-Price"     → runAutoPrice() (oracle path)

Operators couldn't tell which to click. Audit RFQ-2 said drop the older one
(applyRecommendations). This test locks in that deletion so it doesn't drift
back in via a merge.

Also RFQ-3: the 704B worksheet HTML had a hardcoded
'CALIFORNIA CORRECTIONAL HEALTH CARE SERVICES' fallback that leaked
cchcs-specific text into non-CCHCS quotes. Replaced with the r.agency_name
rendered through the existing agency_display normalization upstream.
"""
from __future__ import annotations

from pathlib import Path

RFQ_DETAIL = (
    Path(__file__).resolve().parents[1]
    / "src" / "templates" / "rfq_detail.html"
)


def _html() -> str:
    return RFQ_DETAIL.read_text(encoding="utf-8")


def test_auto_price_all_button_removed():
    html = _html()
    assert "Auto-Price All" not in html, (
        "RFQ-2 regressed: 'Auto-Price All' button came back. "
        "Keep only the 'Auto-Price' runAutoPrice() button."
    )


def test_apply_recommendations_handler_removed():
    html = _html()
    # The handler was the legacy path. Deleting the button without deleting the
    # handler leaves dead JS that still calls POST /apply-recommendations.
    assert "applyRecommendations(" not in html, (
        "applyRecommendations() handler should be deleted along with the button."
    )


def test_run_auto_price_button_still_present():
    # Sanity: the surviving button must still be there.
    html = _html()
    assert "runAutoPrice()" in html
    assert 'aria-label="Auto-Price"' in html


def test_no_hardcoded_cchcs_fallback_in_704b_worksheet():
    html = _html()
    # The old fallback string leaked CCHCS department text into non-CCHCS quotes.
    assert "CALIFORNIA CORRECTIONAL HEALTH CARE SERVICES" not in html, (
        "RFQ-3 regressed: hardcoded CCHCS fallback is back in the 704B worksheet."
    )
