"""PC status must flip to 'completed' on successful generate (Surfaces #11+#13).

Mike's screenshot 2026-05-04 (PC pc_177b18e6): Save & Generate succeeded
twice (Railway log: `POST /pricecheck/.../generate → 200 (1634ms)`,
`Filled AMS 704 saved`, `1/1 items priced, subtotal=$160.00`) but the
workflow guide stayed pinned at "Parsed" and the action bar kept showing
**Save & Generate** instead of **Send Quote**.

Root cause: routes_pricecheck._generate_pc_pdf line 3340 flipped status
to "draft" — which pc_detail.html line 565 maps to the "Priced" workflow
step, NOT "Generated". And the action bar at line 593 only renders
**Send Quote** when `st in ('completed','converted')`. So "draft" =
permanent stuck on Save & Generate.

Fix: post-generate status is "completed" (PC PDF) or "converted" (after
PC→RFQ promotion). Both map to the "Generated" workflow step AND unlock
**Send Quote** in the action bar.

Surface #13 (no persistent download UI) auto-resolves: action bar in the
'completed' state shows **Preview 704** (line 595) and **Regenerate**
(line 596), both persistent — no more reliance on the 20-second toast.
"""
from __future__ import annotations

import re
from pathlib import Path


def test_generate_pc_pdf_flips_status_to_completed_not_draft():
    """Source-level guard: the post-generate transition must be "completed",
    NOT "draft". A future "let me put it back to draft so it shows in the
    drafts list" PR would silently revert Mike's quote-flow."""
    src = Path("src/api/modules/routes_pricecheck.py").read_text(encoding="utf-8")

    # Find the post-pipeline-success block and assert "completed" appears
    # in the same vicinity as "704 PDF filled" (the system note).
    pattern = (
        r'_transition_status\(pc,\s*"completed",\s*actor="system",\s*'
        r'notes="704 PDF filled[^"]*"'
    )
    matches = re.findall(pattern, src)
    assert matches, (
        "_generate_pc_pdf must transition status to 'completed' (not 'draft') "
        "after pipeline success. 'draft' maps to the 'Priced' workflow step "
        "(pc_detail.html:565), which leaves the action bar stuck on "
        "Save & Generate. Surface #11+#13 from the 2026-05-04 P0 chain."
    )


def test_pc_to_rfq_conversion_flips_status_to_converted_not_draft():
    """When a PC is promoted to an RFQ, status should be 'converted' (it has
    a converted_rfq_id, after all) — not 'draft'. Both 'completed' and
    'converted' map to the 'Generated' workflow step AND unlock Send Quote."""
    src = Path("src/api/modules/routes_pricecheck.py").read_text(encoding="utf-8")

    pattern = (
        r'_transition_status\(pc,\s*"converted",\s*actor="system",\s*'
        r'notes="Reytech quote generated"'
    )
    matches = re.findall(pattern, src)
    assert matches, (
        "PC→RFQ conversion path must transition PC status to 'converted', "
        "not 'draft'. The PC carries a converted_rfq_id at this point, so "
        "'draft' is wrong AND the workflow guide gets stuck on 'Priced'."
    )


def test_no_post_generate_draft_transitions_remain():
    """Catch-all: there must be no `_transition_status(pc, "draft", ...,
    notes="...generated...")` patterns left in routes_pricecheck.py.
    Reasoning: any `notes` containing 'generated' or 'filled' implies the
    quote artifact exists — and per pc_detail.html status mapping, 'draft'
    leaves Mike stuck on the priced/save-generate stage forever."""
    src = Path("src/api/modules/routes_pricecheck.py").read_text(encoding="utf-8")

    bad_patterns = [
        r'_transition_status\(pc,\s*"draft"[^)]*notes="[^"]*generated',
        r'_transition_status\(pc,\s*"draft"[^)]*notes="[^"]*filled',
        r'_transition_status\(pc,\s*"draft"[^)]*notes="[^"]*Generated',
    ]
    for pat in bad_patterns:
        hits = re.findall(pat, src)
        assert not hits, (
            f"Found post-generate 'draft' transition: {hits[0]}. "
            f"Should be 'completed' (or 'converted' for PC→RFQ). "
            f"See test_pc_status_flip_on_generate.py for context."
        )


def test_completed_status_in_valid_set():
    """`completed` must remain in the valid PC statuses set so the manual
    `POST /api/pricecheck/<id>/status` route accepts it. If a future
    cleanup PR shrinks the valid set, this test fails fast."""
    from src.api.modules import routes_pricecheck  # noqa: F401

    src = Path("src/api/modules/routes_pricecheck.py").read_text(encoding="utf-8")
    # The valid set is declared inline in api_pc_change_status.
    assert '"completed"' in src, (
        "'completed' status removed from valid set. "
        "_generate_pc_pdf relies on transitioning to it."
    )
    assert '"converted"' in src, (
        "'converted' status removed from valid set. "
        "PC→RFQ promotion relies on transitioning to it."
    )
