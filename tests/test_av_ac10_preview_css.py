"""PR-AV-AC10 — CSS fixes for the review-package preview pane.

Two UX bugs surfaced during the 5/15 DevTools-MCP walk of rfq_9e63456e:

1. Clicking the "⤢ Max" button (intended to hide the form list and
   expand the preview) instead COLLAPSED the preview to a 2px-wide
   strip. Root cause: `toggleView('collapse-left')` adds the
   `collapse-left` class which sets grid-template-columns to
   `0 1fr 0` AND hides `.rv-checklist` via `display:none`. CSS Grid
   removes display:none items from auto-placement entirely — so
   `.rv-preview` (which had no explicit grid-column) auto-snapped
   to column 1 (the 0px slot). Same root cause would bite anytime
   sibling visibility toggled.

   Fix: pin each `.rv-3col` child to its semantic column via
   explicit `grid-column` so sibling visibility can't reflow the
   layout. Plus `min-width:0` on all grid children so the 1fr
   column can shrink past content intrinsic width.

2. Deliverable card filenames wrapped mid-word
   ("10847262_BidPackage_Reyt|ech.pdf") because `.rv-fname` had
   `word-break:break-all`. In a narrow 180px card this produced
   ugly breaks that hid which file the row referred to.

   Fix: single-line + ellipsis + existing `title` attr for the
   hover tooltip. Per Mike's rule
   ([[feedback_text_width_overflow_check]]):
   "make sure text is appropriate, some text was wide before".

WHAT THIS TEST PINS
===================

Static template/CSS source checks — the live DOM verification was
done via DevTools-MCP on prod 5/15 (Max mode rendered the preview
at 1329px, filenames showed clean ellipsis):

  - `.rv-3col > .rv-preview { grid-column: 2/3 }` literal present
  - `.rv-3col > .rv-checklist { grid-column: 1/2 }` literal present
  - `.rv-3col > .rv-audit { grid-column: 3/4 }` literal present
  - `.rv-3col > * { min-width: 0; min-height: 0; }` literal present
  - `.rv-fname` rule contains `text-overflow:ellipsis` AND
    `white-space:nowrap` (the ellipsis approach)
  - `.rv-fname` rule does NOT contain `word-break:break-all`
    (the prior mid-word-break rule)
  - PR-AV-AC10 marker present in template
"""
from __future__ import annotations

from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = REPO_ROOT / "src" / "templates" / "rfq_review.html"


def test_grid_columns_pinned_explicitly():
    """Each .rv-3col child must have an explicit grid-column so
    sibling display:none can't reflow.
    """
    src = TEMPLATE.read_text(encoding="utf-8")
    assert ".rv-3col > .rv-preview{grid-column:2/3}" in src, (
        "preview pane must be pinned to grid-column 2/3"
    )
    assert ".rv-3col > .rv-checklist{grid-column:1/2}" in src, (
        "form-list pane must be pinned to grid-column 1/2"
    )
    assert ".rv-3col > .rv-audit{grid-column:3/4}" in src, (
        "audit pane must be pinned to grid-column 3/4"
    )


def test_grid_children_have_min_width_zero():
    """Grid children need min-width:0 to shrink past content
    intrinsic width — otherwise the 1fr column inherits content's
    minimum-content width and can collapse around an empty iframe.
    """
    src = TEMPLATE.read_text(encoding="utf-8")
    assert ".rv-3col > *{min-width:0;min-height:0}" in src


def test_filename_uses_ellipsis_not_word_break():
    """`.rv-checklist .rv-fname` must use ellipsis truncation,
    not word-break:break-all (which produced mid-word breaks like
    'Reyt|ech.pdf' in the narrow 180px card).
    """
    src = TEMPLATE.read_text(encoding="utf-8")
    # Locate the rv-fname rule block (heuristic: the one in
    # .rv-checklist that we updated)
    fname_idx = src.find(".rv-checklist .rv-fname{")
    assert fname_idx > 0, "no .rv-checklist .rv-fname rule found"
    block = src[fname_idx:fname_idx + 400]
    assert "text-overflow:ellipsis" in block, (
        "AC10 filename rule must use text-overflow:ellipsis"
    )
    assert "white-space:nowrap" in block, (
        "AC10 filename rule must use white-space:nowrap"
    )
    assert "word-break:break-all" not in block, (
        "AC10 filename rule must NOT use word-break:break-all "
        "(that produced 'Reyt|ech.pdf' mid-word breaks)"
    )


def test_ac10_marker_present():
    src = TEMPLATE.read_text(encoding="utf-8")
    assert "PR-AV-AC10" in src, "PR-AV-AC10 marker must remain in rfq_review.html"


def test_title_attr_preserved_for_filename_tooltip():
    """When the filename is truncated by ellipsis, the operator
    needs a way to see the full string. The template already had
    `title="{{ f.filename }}"` on the .rv-fname span — confirm it's
    still there so the hover tooltip works.
    """
    src = TEMPLATE.read_text(encoding="utf-8")
    assert 'class="rv-fname" title="{{ f.filename }}"' in src, (
        "rv-fname span must retain `title` attribute for hover "
        "tooltip — otherwise ellipsis truncation hides info"
    )
