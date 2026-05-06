"""Cross-queue dedup keyed on solicitation_hint — pinned source-text contract.

2026-05-06 Mike P0: solicitation 10838974 was created as both pc_5d8c296f
and rfq a5b09b56 from the same source 704B file. Two pre-existing dedup
checks failed silently because:

  1. PC creation only blocked when pc_number itself matched an existing
     RFQ's solicitation_number. PCs with AUTO_xxx placeholders couldn't
     bridge to the RFQ side.
  2. RFQ creation matched email_uid OR pc_number but not the PC's
     stored solicitation hint.

The fix stores `solicitation_number` on every PC at creation and adds
matching checks on both sides. These tests pin the source text so the
contract can't silently regress without a test failure.
"""

import os


def _read_dashboard():
    p = os.path.join(os.path.dirname(__file__), "..", "src/api/dashboard.py")
    with open(p, encoding="utf-8") as f:
        return f.read()


def test_pc_creation_stores_solicitation_number():
    """PC dict must carry solicitation_number from rfq_email.solicitation_hint."""
    src = _read_dashboard()
    # The new PC dict construction must include the solicitation_number key
    # and source it from _pc_sol_hint (which itself comes from solicitation_hint).
    assert "_pc_sol_hint = rfq_email.get(\"solicitation_hint\", \"\")" in src
    assert "\"solicitation_number\": _pc_sol_hint or \"\"" in src


def test_pc_creation_also_blocks_by_sol_hint():
    """PC creation must skip when sol_hint already exists in the RFQ queue,
    not just when pc_number matches. AUTO_xxx PCs cannot bridge via pc_number."""
    src = _read_dashboard()
    assert "if _pc_sol_hint and _pc_sol_hint not in (\"\", \"unknown\", \"RFQ\") and _pc_sol_hint in _rfq_sols" in src
    assert "SKIP PC: sol_hint" in src


def test_rfq_creation_matches_pc_solicitation_number_field():
    """RFQ side must check the PC's explicit solicitation_number field
    (in addition to pc_number)."""
    src = _read_dashboard()
    assert "_xq_pcsol = (_xq_pc.get(\"solicitation_number\") or \"\").strip()" in src
    assert "_xq_pcsol == _sol_hint_xq" in src
    assert "matches PC" in src and "solicitation_number" in src


def test_rfq_creation_still_matches_email_uid():
    """Don't regress the original email_uid check — keep it alongside."""
    src = _read_dashboard()
    assert "_xq_pc.get(\"email_uid\") == _email_uid_xq" in src
    assert "already exists as PC" in src


def test_rfq_creation_still_matches_pc_number():
    """Don't regress the original pc_number check either."""
    src = _read_dashboard()
    assert "_xq_pcnum = (_xq_pc.get(\"pc_number\") or \"\").strip()" in src
    assert "_xq_pcnum == _sol_hint_xq" in src
