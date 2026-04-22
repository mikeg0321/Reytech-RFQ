"""CC-4 regression guard: _apply_checkbox_updates used to clobber the
parent /V for every matching widget, so radio-group siblings with a
shared /Parent (CUF "Check Box21.0..3", AMS 708 Yes/No) had the last
iterated widget win. Usually that was an OFF sibling, which wiped the
ON choice. Acrobat reads group state from parent /V, so the visible
group appeared unchecked.

Fix shape: collect on/off picks per parent into a dict during widget
iteration, then apply parent /V once after the loop — preferring the
ON pick over OFF siblings.
"""
from __future__ import annotations

import re
from pathlib import Path


FILLER = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "forms"
    / "cchcs_packet_filler.py"
)


def _strip_comment_lines(src: str) -> str:
    kept = []
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def _apply_checkbox_body() -> str:
    src = FILLER.read_text(encoding="utf-8")
    m = re.search(
        r"def _apply_checkbox_updates\([\s\S]*?\n(?=\ndef [a-zA-Z_])",
        src,
    )
    assert m, "_apply_checkbox_updates body not located"
    return m.group(0)


def test_parent_v_is_deferred_per_parent():
    """The fix collects per-parent picks first, then writes /V in a
    second pass. Guard that both the collection map and the second pass
    exist."""
    body = _strip_comment_lines(_apply_checkbox_body())
    assert "parent_on_pick" in body, (
        "CC-4 regression: parent_on_pick map is gone — the widget loop "
        "must collect per-parent picks instead of writing parent /V inline."
    )
    assert "parent_objs" in body, (
        "CC-4 regression: parent_objs map is gone — without it we cannot "
        "reapply /V in the second pass without reopening widgets."
    )
    assert re.search(
        r"for\s+pkey\s*,\s*pobj\s+in\s+parent_objs\.items\(\)\s*:",
        body,
    ), (
        "CC-4 regression: the second-pass `for pkey, pobj in "
        "parent_objs.items():` loop is missing. Parent /V will never be "
        "written."
    )


def test_parent_v_is_not_clobbered_per_widget():
    """The old inline clobber must be gone — parent /V must NOT be set
    inside the per-widget branch."""
    body = _apply_checkbox_body()
    # The banned shape was:
    #     pobj[NameObject("/V")] = export
    # *inside* the widget loop, right after `pobj = parent.get_object()`.
    # Guard that this specific inline assignment no longer sits under the
    # `if parent is not None:` branch adjacent to the per-widget work.
    inline_ban = re.search(
        r"pobj\s*=\s*parent\.get_object\(\)\s*\n\s*pobj\[NameObject\(\"/V\"\)\]\s*=\s*export\b",
        body,
    )
    assert not inline_ban, (
        "CC-4 regression: parent /V is being clobbered inline per widget "
        "again. Last-write-wins will blow away ON picks when OFF siblings "
        "iterate later."
    )


def test_off_does_not_override_on_for_same_parent():
    """The collection step must guard against OFF writes clobbering an
    existing ON pick. An `if export_name != "/Off"` branch (or equivalent)
    should direct ON writes to overwrite, and OFF writes to `setdefault`.
    """
    body = _strip_comment_lines(_apply_checkbox_body())
    # The ON branch writes directly; the OFF branch uses setdefault. If
    # both paths used plain assignment, OFF would still win on a later
    # sibling iteration.
    assert 'parent_on_pick.setdefault' in body, (
        "CC-4 regression: OFF branch should use `parent_on_pick.setdefault"
        "(pkey, export)` so it only records /Off when no ON sibling has "
        "claimed the parent."
    )
    assert re.search(
        r"if\s+export_name\s*!=\s*[\"']/Off[\"']\s*:",
        body,
    ), (
        "CC-4 regression: the ON/OFF branch guarding parent_on_pick is "
        "missing. OFF siblings will clobber ON picks again."
    )


def test_module_still_compiles():
    import py_compile
    py_compile.compile(str(FILLER), doraise=True)
