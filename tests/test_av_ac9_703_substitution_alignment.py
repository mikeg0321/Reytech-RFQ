"""PR-AV-AC9 — 703C satisfies a 703B requirement in the alignment
rollup (and vice versa).

CONTEXT

PR-AV3 (substrate-pivot, see [[project_mr_wolf_handoff_2026_05_13_substrate_pivot]])
added 703B↔703C substitution at form-generation time. When the
agency_config requires `703b` and the buyer sends `703c`, the
generator runs `fill_703c` and the manifest carries a 703C review
row — not a 703B row.

The alignment rollup at this layer (review_alignment.py) iterated
the agency's `required_forms` literally: for each required `form_id`
it looked up `review_by_id[form_id]` and reported "missing" when
the row was absent. So a 703C-only manifest under an agency that
requires 703B falsely reported "Missing required forms: AMS 703B"
even though the substitute IS present in the package.

On rfq_9e63456e (5/15 PREQ 10847262 audit), this was the 3rd of 3
critical "blocking send" issues:

  ⚠️ 3 issue(s) blocking send
   - Missing required forms: AMS 703B  ← FALSE POSITIVE
   - Form QA failed — 4 field missing
   - 1 item(s) without a unit price

The buyer attached `AMS 703C - RFQ - F_R - 03-25.pdf`; AV3
correctly filled 703C; but the alignment banner kept flagging the
missing 703B slot.

THE FIX

A small substitution map in `_build_forms_checklist`:

  _FORM_SUBSTITUTES = {"703b": "703c", "703c": "703b"}

When iterating required forms, if the required form is absent from
the manifest but its substitute IS present, render the substitute
row in the required slot (keeps agency ordering intact). The
existing `missing_required` rollup at L343 then sees a present
row and the banner doesn't fire.

WHAT THIS TEST PINS
===================

  - 703B required, 703C in manifest → checklist row present,
    NO "Missing required forms" issue
  - 703C required, 703B in manifest → same behavior, mirrored
  - 703B required, both 703B and 703C in manifest → 703B wins
    (preferred — agency literally said 703B)
  - 703B required, neither in manifest → still reports missing
  - Substrate guard: PR-AV-AC9 marker present
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _build(required, reviewed):
    """Run compute_review_alignment with a minimal but realistic
    inputs set. `required` is the agency's required_forms list;
    `reviewed` is the manifest.reviews list (each item gets a
    form_id + form_filename + verdict).
    """
    from src.api.review_alignment import compute_review_alignment
    manifest = {
        "reviews": [
            {"form_id": fid, "form_filename": fn,
             "verdict": "pending", "notes": ""}
            for fid, fn in reviewed
        ],
        "required_forms": required,
        "field_audit": {},
        "source_validation": {"errors": [], "warnings": []},
    }
    rfq = {
        "requestor_name": "Test", "requestor_email": "t@x.com",
        "agency": "cchcs", "due_date": "2026-05-30",
        "line_items": [
            {"description": "X", "qty": 1, "price_per_unit": 1.0,
             "unit_price": 1.0},
        ],
    }
    agency_cfg = {"name": "CCHCS / CDCR", "required_forms": list(required)}
    return compute_review_alignment(rfq=rfq, manifest=manifest,
                                    agency_cfg=agency_cfg)


def _missing_required_msgs(result):
    return [
        i for i in result["rollup"]["issues"]
        if "Missing required forms" in i
    ]


def test_703b_required_but_703c_present_no_missing():
    """The headline scenario from rfq_9e63456e. Agency wants 703B,
    buyer sent 703C → AV3 filled 703C → manifest has 703C only →
    alignment rollup must NOT report 703B as missing.
    """
    result = _build(
        required=["703b", "704b", "quote", "bidpkg"],
        reviewed=[
            ("703c", "10847262_703C_Reytech.pdf"),
            ("704b", "10847262_704B_Reytech.pdf"),
            ("quote", "10847262_Quote_Reytech.pdf"),
            ("bidpkg", "10847262_BidPackage_Reytech.pdf"),
        ],
    )
    assert not _missing_required_msgs(result), (
        f"703B+703C substitution must suppress the missing-form banner "
        f"when only 703C is in the manifest; got "
        f"issues={result['rollup']['issues']}"
    )
    # The checklist row should still mark the 703-slot as required
    # — operator should see it filled, not absent.
    forms = result["forms_checklist"]
    seven_oh_three = [r for r in forms if r["form_id"] in ("703b", "703c")]
    assert len(seven_oh_three) == 1, (
        f"exactly one 703-slot row expected; got {seven_oh_three}"
    )
    assert seven_oh_three[0]["required"] is True
    assert seven_oh_three[0]["missing"] is False
    assert seven_oh_three[0]["form_id"] == "703c"


def test_703c_required_but_703b_present_no_missing():
    """Mirror: some agencies (CalVet) require 703C; if buyer sent
    703B and AV3 filled 703B, the alignment must not falsely flag.
    """
    result = _build(
        required=["703c", "704b", "quote", "bidpkg"],
        reviewed=[
            ("703b", "10847262_703B_Reytech.pdf"),
            ("704b", "10847262_704B_Reytech.pdf"),
            ("quote", "10847262_Quote_Reytech.pdf"),
            ("bidpkg", "10847262_BidPackage_Reytech.pdf"),
        ],
    )
    assert not _missing_required_msgs(result)


def test_703b_preferred_when_both_present():
    """Determinism: if BOTH 703B and 703C are in the manifest and
    agency required 703B, render 703B (the literally-named form).
    """
    result = _build(
        required=["703b", "704b", "quote", "bidpkg"],
        reviewed=[
            ("703b", "10847262_703B_Reytech.pdf"),
            ("703c", "10847262_703C_Reytech.pdf"),
            ("704b", "10847262_704B_Reytech.pdf"),
            ("quote", "10847262_Quote_Reytech.pdf"),
            ("bidpkg", "10847262_BidPackage_Reytech.pdf"),
        ],
    )
    forms = result["forms_checklist"]
    # First 703-row should be the required form_id literally
    seven_oh_three = [r for r in forms if r["form_id"] in ("703b", "703c")]
    required_row = next(r for r in seven_oh_three if r["required"])
    assert required_row["form_id"] == "703b", (
        "when agency asks for 703B and both forms exist, render the "
        "703B row (not 703C) in the required slot"
    )


def test_neither_703_present_still_reports_missing():
    """Defense: AC9 must not hide a genuinely-missing 703 form. If
    neither 703B nor 703C is in the manifest under an agency that
    required 703B, the banner still fires.
    """
    result = _build(
        required=["703b", "704b", "quote", "bidpkg"],
        reviewed=[
            ("704b", "10847262_704B_Reytech.pdf"),
            ("quote", "10847262_Quote_Reytech.pdf"),
            ("bidpkg", "10847262_BidPackage_Reytech.pdf"),
        ],
    )
    missing = _missing_required_msgs(result)
    assert missing, (
        "genuinely-missing 703B (no 703C present) must still flag "
        "the rollup banner"
    )
    # The literal "AMS 703B" should appear in the message
    assert any("AMS 703B" in m for m in missing)


def test_source_grep_ac9_marker_present():
    src = (REPO_ROOT / "src" / "api" / "review_alignment.py").read_text(encoding="utf-8")
    assert "PR-AV-AC9" in src, "PR-AV-AC9 marker must remain in review_alignment.py"
    assert '"703b": "703c"' in src and '"703c": "703b"' in src, (
        "the bidirectional substitution map must be in review_alignment.py"
    )
