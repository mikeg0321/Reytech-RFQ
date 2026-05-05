"""Mike P0 2026-05-06 (Audit P0 #4): /Sig field detection guard must
match by /FT == /Sig regardless of field name.

Background: `_703b_overlay_signature` is the legacy positional-stamp path.
It runs only when the template has NO /Sig field — the assumption being
that "no /Sig" means "old form with a printed signature line, draw at the
fixed position."

Pre-fix the guard at ~line 729 of reytech_filler_v4.py required BOTH:
  1. /FT == /Sig
  2. /T (field name) ∈ SIGN_FIELDS

That's wrong. SIGN_FIELDS is the list of fields we *fill with the signature
image*, not the list of fields that count as "the form has a /Sig field".
A 703B variant with a /Sig field named, e.g., "DigitalSignature" would:
  - Not be filled (correct — not in whitelist)
  - But still count as "no /Sig field present" (wrong — there's a /Sig
    field, just one we don't recognize)
  - → positional overlay runs → signature drawn at fixed position over
    a blank /Sig field → looks broken to the buyer

Post-fix: detect by /FT == /Sig only.

These are source-level guards — they read the function body and assert
the right pattern is in place.
"""
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


def test_sig_field_guard_uses_ft_only_not_field_name_whitelist():
    """The /Sig field detection in fill_703b_with_sig must NOT use the
    SIGN_FIELDS whitelist. It must only check /FT == /Sig."""
    body = (REPO / "src/forms/reytech_filler_v4.py").read_text(encoding="utf-8")
    # Find the relevant block — search for the comment "Check if template has"
    # Use a stable marker
    marker = "Check if template has any /Sig field"
    block_start = body.find(marker)
    # Fallback to old marker (pre-fix) so the test message is informative
    if block_start < 0:
        block_start = body.find("Check if template has /Sig fields")
    assert block_start > 0, "could not locate the /Sig detection block"
    # Read ~25 lines after the marker
    block = body[block_start:block_start + 1200]
    # The fix: detection condition does NOT couple /FT == /Sig with SIGN_FIELDS
    bad_pattern = '== "/Sig" and str(_obj.get("/T", "")) in SIGN_FIELDS'
    assert bad_pattern not in block, (
        "Pre-fix guard pattern is back: "
        '`/FT == "/Sig" and /T in SIGN_FIELDS`. '
        "This couples 'is a sig field' with 'we recognize the name'. "
        "Variant /Sig names (DigitalSignature, AuthorizedSignature_Alt, etc.) "
        "would bypass the guard → _703b_overlay_signature double-stamps over "
        "a blank /Sig field. Detection must use /FT only."
    )
    # The right pattern: just /FT == /Sig, sets _has_sig_field = True
    assert '== "/Sig":' in block or "== '/Sig':" in block, (
        "Post-fix detection should be `if str(_obj.get(\"/FT\", \"\")) == \"/Sig\":` "
        "(no SIGN_FIELDS check)."
    )


def test_sig_guard_break_propagates_outer_loop():
    """Make sure the inner-loop break doesn't leave us scanning every page
    after a /Sig field is already found — the previous code only broke out
    of the inner loop and kept scanning subsequent pages, which was wasteful
    but harmless. Post-fix should break both loops."""
    body = (REPO / "src/forms/reytech_filler_v4.py").read_text(encoding="utf-8")
    marker = "Check if template has any /Sig field"
    block_start = body.find(marker)
    if block_start < 0:
        return  # Skip — pre-fix layout, the cleanup test is just a nice-to-have
    block = body[block_start:block_start + 1200]
    # After setting _has_sig_field = True, the outer loop should break too.
    # We accept either an explicit `if _has_sig_field: break` after the inner
    # for-loop, or any structural cue that propagates.
    assert "if _has_sig_field:" in block and block.count("break") >= 2, (
        "Post-fix should propagate the break to the outer page loop, "
        "e.g. `if _has_sig_field: break` after the inner for-loop. "
        "Otherwise we keep walking pages after we've already answered the "
        "question."
    )
