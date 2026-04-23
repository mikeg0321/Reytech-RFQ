"""PC autosave race-condition guardrails (2026-04-23).

Root finding: pc_f7ba7a6b shipped $558.48 in the email body despite the UI
showing 22% markup (= $567.79). The PC's disk state was markup=20%, so
the UI's 22% had never actually persisted. Root cause is the autosave
debounce + unload race: user types markup, tabs away within the debounce
window (2.5s), the XHR never fires, the server keeps the old value.

Fix: shorten the debounce to 1.2s AND fire a `navigator.sendBeacon` on
`beforeunload` so the final snapshot survives tab teardown. These tests
lock both behaviors in the template source — a regression that removes
either one trips the suite.
"""
from __future__ import annotations

import re
from pathlib import Path

TEMPLATE = Path(__file__).resolve().parents[1] / "src" / "templates" / "pc_detail.html"


def _src() -> str:
    return TEMPLATE.read_text(encoding="utf-8")


def test_trigger_pc_autosave_debounce_is_short():
    """triggerPcAutosave debounce must be ≤ 1500ms to bound the window in
    which a typed-then-navigated edit can be lost before the beforeunload
    sendBeacon catches it."""
    src = _src()
    m = re.search(
        r"function triggerPcAutosave\(\)\s*\{[^}]*?setTimeout\("
        r"\s*doPcAutosave\s*,\s*(\d+)\s*\)",
        src,
        flags=re.DOTALL,
    )
    assert m, "triggerPcAutosave setTimeout not found — refactor likely broke the autosave entry point"
    delay = int(m.group(1))
    assert delay <= 1500, (
        f"triggerPcAutosave debounce is {delay}ms, too long. "
        "A user who types a markup change and tabs away within the "
        "debounce window loses their edit. Keep it ≤ 1500ms and rely "
        "on the beforeunload sendBeacon as the belt-and-suspenders catch."
    )


def _extract_beforeunload_handler(src: str) -> str:
    """Return the full beforeunload handler body.

    Nested braces defeat greedy regex, so we slice from the
    `addEventListener('beforeunload'` site to the matching `});` that
    closes the addEventListener call.
    """
    start = src.find("addEventListener('beforeunload'")
    assert start != -1, "beforeunload handler not found"
    # Walk forward tracking brace depth; start after the opening `function(e) {`
    body_start = src.find("function(e)", start)
    brace = src.find("{", body_start)
    depth = 1
    i = brace + 1
    while i < len(src) and depth > 0:
        c = src[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
        i += 1
    return src[start:i + 2]  # include the trailing `);`


def test_beforeunload_uses_sendbeacon():
    """The beforeunload handler must call navigator.sendBeacon so the
    final unsaved snapshot reaches the server during tab teardown.

    XHR calls fired from beforeunload are aborted by the browser; only
    sendBeacon survives. Without this the 'typed-but-didn't-wait' case
    silently loses data.
    """
    handler = _extract_beforeunload_handler(_src())
    assert "sendBeacon" in handler, (
        "beforeunload must call navigator.sendBeacon to persist the final "
        "snapshot. Removing this reintroduces the 2026-04-23 markup-drop bug."
    )
    assert "/save-prices" in handler, (
        "sendBeacon target must be the save-prices endpoint."
    )


def test_sendbeacon_sends_json_blob():
    """sendBeacon must transmit a Blob with application/json type so the
    Flask save endpoint (which uses request.get_json) parses the body
    instead of treating it as form-urlencoded."""
    handler = _extract_beforeunload_handler(_src())
    assert "new Blob" in handler, (
        "sendBeacon payload must be wrapped in a Blob — a raw string "
        "defaults to Content-Type: text/plain which Flask's get_json "
        "handles but is fragile. Be explicit: application/json."
    )
    assert "application/json" in handler, (
        "Blob type must be application/json for the save-prices endpoint."
    )


def test_doPcAutosave_still_guards_against_concurrent_inflight():
    """Regression lock: the in-flight guard prevents overlapping saves
    that would race to clobber each other. Trivial but easy to
    accidentally remove during future refactors."""
    src = _src()
    m = re.search(
        r"function doPcAutosave\(\)\s*\{[^}]*?if\s*\(\s*_pcSaveInFlight\s*\)\s*return",
        src,
        flags=re.DOTALL,
    )
    assert m, "doPcAutosave in-flight guard missing — concurrent saves can race"


def test_pcLastSaved_only_updated_on_server_ok():
    """The client must not mark a snapshot as 'saved' unless the server
    confirmed d.ok === true. A regression that updates _pcLastSaved
    optimistically would silently drop failed saves (user thinks it
    saved, disk still has old value).
    """
    src = _src()
    # Find the .then block that processes the server response
    m = re.search(
        r"\.then\(function\s*\(d\)\s*\{[^}]*?if\s*\(\s*d\.ok\s*\)\s*\{[^}]*?_pcLastSaved\s*=",
        src,
        flags=re.DOTALL,
    )
    assert m, (
        "_pcLastSaved assignment must be gated on d.ok. Without the guard, "
        "a failed save silently marks itself complete and the user never "
        "knows their edit was lost."
    )
