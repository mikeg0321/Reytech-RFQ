"""PC-15 regression: email composer To: field must not fall back to the display name.

Prior to 2026-04-22 the PC detail template populated the composer's
To: input with:
    pc.original_sender or pc.requestor_email or pc.requestor or ''

`requestor` is a DISPLAY NAME (e.g., "Mohammad Chechi"), not an email address.
When that branch fires the composer shows "To: Mohammad Chechi" → browser +
server-side email validator mark the address invalid → the Send button sits
disabled with no obvious reason. Operators thought the feature was broken.

Fix: drop the `pc.requestor` fallback. When there's no real address on file,
leave the field blank so the empty-buyer-email banner surfaces.
"""
from __future__ import annotations

from pathlib import Path

PC_DETAIL = (
    Path(__file__).resolve().parents[1]
    / "src" / "templates" / "pc_detail.html"
)


def test_buyer_email_does_not_fall_back_to_requestor_name():
    html = PC_DETAIL.read_text(encoding="utf-8")
    assert "pc.get('requestor', '')" not in html, (
        "PC-15 regressed: composer To: field fell back to pc.requestor, "
        "which is a display name — stop populating an invalid address."
    )


def test_buyer_email_still_reads_valid_address_sources():
    # Sanity: the two real address sources must still be wired.
    html = PC_DETAIL.read_text(encoding="utf-8")
    assert "pc.get('original_sender'" in html
    assert "pc.get('requestor_email'" in html
