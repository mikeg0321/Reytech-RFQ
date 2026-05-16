"""Helpers for auditing the Spine template's JavaScript.

Reviewer 2026-05-15: the inline regex in
test_no_autosave_hooks_in_spine_template was powerful but brittle.
Extracted here so it has one home, one test, and any future check
(e.g., "no fetch() inside form submit handlers") can extend it
without growing the test file.

The substrate invariant under test:

  The Spine UI MUST NEVER fire a per-keystroke network call. Client-
  side recompute (markup ↔ price two-way binding, KPI live updates)
  is fine. The first thing that touches the network on each
  operator action must be the Save button.
"""
from __future__ import annotations

import re

# Banned literal patterns. These all imply per-keystroke autosave in
# the legacy Reytech template idiom and must never appear in Spine.
BANNED_LITERALS: tuple[str, ...] = (
    'oninput="',
    'onchange="recalc',
    'onchange="trigger',
    'triggerPcAutosave',
    'doPcAutosave',
    'sendBeacon',
)

# Listener events that, if their callback body issues a network call,
# would amount to per-keystroke autosave.
_KEYSTROKE_LISTENER_RE = re.compile(
    r"addEventListener\(['\"](input|keyup|keydown|keypress)['\"][^)]*\)",
    re.IGNORECASE,
)

# Network primitives that, if found inside a keystroke listener body,
# violate the invariant.
NETWORK_NEEDLES: tuple[str, ...] = ("fetch(", "XMLHttpRequest", "sendBeacon")


def find_banned_literals(template_text: str) -> list[str]:
    """Return any banned literal substring found in template_text."""
    return [p for p in BANNED_LITERALS if p in template_text]


def find_keystroke_network_calls(template_text: str) -> list[str]:
    """Return a list of offending listener-snippet strings.

    Walks every addEventListener('input'|'keyup'|'keydown'|'keypress')
    match, isolates the callback body by tracking matching braces,
    and reports the match snippet if its body contains a network
    call.
    """
    offenses: list[str] = []
    for m in _KEYSTROKE_LISTENER_RE.finditer(template_text):
        body = _extract_callback_body(template_text, m.end())
        if body is None:
            continue
        if any(needle in body for needle in NETWORK_NEEDLES):
            snippet = template_text[m.start(): m.end()]
            offenses.append(snippet + " … body: " + body[:200])
    return offenses


def _extract_callback_body(text: str, start_idx: int) -> str | None:
    """Walk forward from start_idx, find the first '{' (the callback
    body opener), then scan to its matching '}' tracking nesting.
    Returns the body text or None if no body found within a short
    look-ahead window."""
    open_brace = text.find("{", start_idx)
    if open_brace == -1 or open_brace - start_idx > 80:
        return None
    depth = 1
    i = open_brace + 1
    while i < len(text) and depth > 0:
        ch = text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
        i += 1
    return text[open_brace + 1: i - 1]
