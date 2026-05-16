"""Tests for the template-audit helper.

The autosave check used to be a brittle inline regex in
test_edit_ui.py. Reviewer 2026-05-15 asked for it to live in a
helper. These tests prove the helper distinguishes the things it
must distinguish: client-side recompute (allowed) vs per-keystroke
network calls (banned).
"""
from __future__ import annotations

from tests.spine._template_audit import (
    BANNED_LITERALS,
    NETWORK_NEEDLES,
    find_banned_literals,
    find_keystroke_network_calls,
)


# ──────────────────────────────────────────────────────────────────────
# find_banned_literals
# ──────────────────────────────────────────────────────────────────────


def test_find_banned_literals_returns_empty_when_clean():
    assert find_banned_literals("nothing autosave-y here") == []


def test_find_banned_literals_flags_oninput_attribute():
    html = '<input oninput="doSave()">'
    assert "oninput=\"" in find_banned_literals(html)


def test_find_banned_literals_flags_legacy_autosave_function_names():
    js = "function whatever(){ triggerPcAutosave(); }"
    assert "triggerPcAutosave" in find_banned_literals(js)


def test_banned_list_is_non_empty_documentation():
    assert len(BANNED_LITERALS) >= 3
    assert "sendBeacon" in BANNED_LITERALS


# ──────────────────────────────────────────────────────────────────────
# find_keystroke_network_calls
# ──────────────────────────────────────────────────────────────────────


def test_keystroke_recompute_with_no_network_passes():
    """The Spine UI's actual pattern: input listeners that recompute
    markup/price client-side, no network calls."""
    js = """
    document.querySelectorAll('input').forEach(function(el){
      el.addEventListener('input', function(){
        const cost = parseFloat(other.value);
        someDiv.textContent = (cost * 1.35).toFixed(2);
      });
    });
    """
    assert find_keystroke_network_calls(js) == []


def test_keystroke_with_fetch_is_caught():
    js = """
    input.addEventListener('input', function(){
      fetch('/api/autosave', {method: 'POST'});
    });
    """
    offenses = find_keystroke_network_calls(js)
    assert len(offenses) == 1
    assert "fetch(" in offenses[0]


def test_keystroke_with_xhr_is_caught():
    js = """
    input.addEventListener('keyup', function(){
      const x = new XMLHttpRequest();
      x.open('POST', '/x');
      x.send();
    });
    """
    offenses = find_keystroke_network_calls(js)
    assert len(offenses) == 1
    assert "XMLHttpRequest" in offenses[0]


def test_keystroke_with_sendbeacon_is_caught():
    js = """
    el.addEventListener('keypress', function(e){
      navigator.sendBeacon('/track', e.key);
    });
    """
    offenses = find_keystroke_network_calls(js)
    assert len(offenses) == 1


def test_keystroke_followed_by_unrelated_fetch_passes():
    """The brittle inline regex used to false-positive here because
    the look-ahead window crossed listener boundaries. The brace-
    tracking helper handles it correctly: only the listener's actual
    callback body is inspected, not arbitrary subsequent code.
    """
    js = """
    input.addEventListener('input', function(){
      doRecomputeOnly();
    });
    // Later, the Save button — distinct handler, fetch is fine here.
    saveBtn.addEventListener('click', function(){
      fetch('/state', {method: 'POST'});
    });
    """
    offenses = find_keystroke_network_calls(js)
    assert offenses == []


def test_keystroke_with_nested_braces_handled():
    """Nested braces inside the callback (object literals, blocks)
    must not confuse the brace tracker."""
    js = """
    input.addEventListener('input', function(){
      if (something) {
        const x = {a: 1, b: 2};
        fetch('/danger');
      }
    });
    """
    offenses = find_keystroke_network_calls(js)
    assert len(offenses) == 1
    assert "fetch(" in offenses[0]


def test_keystroke_listener_with_no_body_is_skipped():
    """If we don't see a '{' within 80 chars of the listener, it's
    probably a callback reference (input.addEventListener('input',
    handler)). The helper skips those — handler bodies are audited
    separately at their definition site."""
    js = "input.addEventListener('input', recomputePriceFromMarkup);"
    offenses = find_keystroke_network_calls(js)
    assert offenses == []


def test_network_needles_documented():
    assert "fetch(" in NETWORK_NEEDLES
    assert "XMLHttpRequest" in NETWORK_NEEDLES
    assert "sendBeacon" in NETWORK_NEEDLES
