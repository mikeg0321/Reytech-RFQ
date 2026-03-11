"""
test_namespace_injection.py
===========================
Tests that catch the class of bugs where route modules loaded via
dashboard's exec_module injection fail when called with a stripped
or incomplete globals namespace.

The specific bugs this catches:
  - NameError: name 'DATA_DIR' is not defined  (routes_pricecheck)
  - NameError: name 'do_poll_check' is not defined (same)
  - NameError: name 'UPLOAD_DIR' is not defined
  - NameError: name 'CONFIG' is not defined

Run with:
    pytest tests/test_namespace_injection.py -v
"""
import importlib.util
import sys
import os
import types
import pytest

# ── Helpers ──────────────────────────────────────────────────────────────────

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def load_module_with_empty_namespace(module_path: str) -> types.ModuleType:
    """
    Load a route module with ONLY its own explicit imports — no injected globals.
    This simulates the worst-case: injection failed or globals weren't propagated.
    Any NameError during load = bug.
    """
    spec = importlib.util.spec_from_file_location("_test_module", module_path)
    mod = importlib.util.module_from_spec(spec)
    # Deliberately do NOT inject dashboard globals
    spec.loader.exec_module(mod)
    return mod


def load_module_simulating_injection(module_path: str, extra_globals: dict = None) -> types.ModuleType:
    """
    Load a route module the same way dashboard._load_route_module does it —
    inject a shared namespace, then exec. Verifies the injection path works.
    """
    spec = importlib.util.spec_from_file_location("_test_module", module_path)
    mod = importlib.util.module_from_spec(spec)

    # Minimal shared namespace (what dashboard provides)
    from src.core.paths import DATA_DIR, UPLOAD_DIR, OUTPUT_DIR
    import logging, json, os as _os
    shared = {
        "DATA_DIR": DATA_DIR,
        "UPLOAD_DIR": UPLOAD_DIR,
        "OUTPUT_DIR": OUTPUT_DIR,
        "os": _os,
        "json": json,
        "log": logging.getLogger("test"),
        "CONFIG": {"email": {}},
        "POLL_STATUS": {"running": False, "last_check": None, "error": None, "paused": False},
    }
    if extra_globals:
        shared.update(extra_globals)
    mod.__dict__.update(shared)
    spec.loader.exec_module(mod)
    return mod


# ── Tests: Explicit imports (module must NOT rely on injection for basics) ───

ROUTE_MODULES = [
    "src/api/modules/routes_pricecheck.py",
    "src/api/modules/routes_rfq.py",
    "src/api/modules/routes_crm.py",
]


@pytest.mark.parametrize("rel_path", ROUTE_MODULES)
def test_route_module_has_explicit_data_dir_import(rel_path):
    """Every route module must import DATA_DIR explicitly, not rely on injection."""
    full_path = os.path.join(ROOT, rel_path)
    if not os.path.exists(full_path):
        pytest.skip(f"Module not found: {rel_path}")

    with open(full_path) as f:
        source = f.read()

    # Must import DATA_DIR from src.core.paths (not rely on injected global)
    assert "from src.core.paths import" in source and "DATA_DIR" in source, (
        f"{rel_path} does not explicitly import DATA_DIR from src.core.paths. "
        "This causes NameError when called outside the injection namespace."
    )


@pytest.mark.parametrize("rel_path", ROUTE_MODULES)
def test_route_module_compiles(rel_path):
    """Every route module must compile without syntax errors."""
    import py_compile
    full_path = os.path.join(ROOT, rel_path)
    if not os.path.exists(full_path):
        pytest.skip(f"Module not found: {rel_path}")
    py_compile.compile(full_path, doraise=True)


# ── Tests: do_poll_check is callable from routes_pricecheck ──────────────────

def test_safe_poll_check_wrapper_exists():
    """routes_pricecheck must define _safe_do_poll_check to handle namespace gaps."""
    path = os.path.join(ROOT, "src/api/modules/routes_pricecheck.py")
    with open(path) as f:
        source = f.read()
    assert "_safe_do_poll_check" in source, (
        "_safe_do_poll_check wrapper missing from routes_pricecheck. "
        "Without it, do_poll_check() fails with NameError when called from the "
        "injected module context where dashboard globals aren't guaranteed."
    )


def test_safe_poll_check_falls_back_to_dashboard():
    """_safe_do_poll_check must attempt sys.modules lookup as fallback."""
    path = os.path.join(ROOT, "src/api/modules/routes_pricecheck.py")
    with open(path) as f:
        source = f.read()
    assert "sys.modules" in source or "src.api.dashboard" in source, (
        "_safe_do_poll_check does not have a sys.modules fallback. "
        "It will fail silently when globals are missing."
    )


def test_nuke_and_poll_uses_safe_wrapper():
    """api_nuke_and_poll must use _safe_do_poll_check, not bare do_poll_check."""
    path = os.path.join(ROOT, "src/api/modules/routes_pricecheck.py")
    with open(path) as f:
        lines = f.readlines()

    in_nuke = False
    for line in lines:
        if "def api_nuke_and_poll" in line:
            in_nuke = True
        if in_nuke and "do_poll_check()" in line and "_safe_" not in line:
            pytest.fail(
                "api_nuke_and_poll calls bare do_poll_check() which fails with "
                "NameError when DATA_DIR is not in the injected namespace. "
                "Use _safe_do_poll_check() instead."
            )
        if in_nuke and line.startswith("def ") and "api_nuke_and_poll" not in line:
            break  # past the function


def test_poll_now_uses_safe_wrapper():
    """api_poll_now must use _safe_do_poll_check."""
    path = os.path.join(ROOT, "src/api/modules/routes_pricecheck.py")
    with open(path) as f:
        lines = f.readlines()

    in_fn = False
    for line in lines:
        if "def api_poll_now" in line:
            in_fn = True
        if in_fn and "do_poll_check()" in line and "_safe_" not in line:
            pytest.fail(
                "api_poll_now calls bare do_poll_check() — use _safe_do_poll_check()."
            )
        if in_fn and line.startswith("def ") and "api_poll_now" not in line:
            break


# ── Tests: do_poll_check itself handles missing globals ──────────────────────

def test_do_poll_check_has_data_dir_guard():
    """do_poll_check must have a try/except NameError guard for DATA_DIR/UPLOAD_DIR."""
    path = os.path.join(ROOT, "src/api/dashboard.py")
    with open(path) as f:
        source = f.read()

    # Find the function body
    start = source.index("def do_poll_check():")
    end = source.index("\ndef ", start + 1)
    fn_body = source[start:end]

    assert "NameError" in fn_body, (
        "do_poll_check() has no NameError guard for DATA_DIR/UPLOAD_DIR. "
        "Add: try: _DATA_DIR = DATA_DIR / except NameError: from src.core.paths import ..."
    )
    assert "_DATA_DIR" in fn_body or "_UPLOAD_DIR" in fn_body, (
        "do_poll_check() must use local _DATA_DIR/_UPLOAD_DIR variables "
        "fetched via the NameError guard, not bare DATA_DIR."
    )


def test_do_poll_check_sets_last_check_on_connect_attempt():
    """last_check must be set even when IMAP connect fails, so null = truly never ran."""
    path = os.path.join(ROOT, "src/api/dashboard.py")
    with open(path) as f:
        source = f.read()

    start = source.index("def do_poll_check():")
    end = source.index("\ndef ", start + 1)
    fn_body = source[start:end]

    # last_check should be set BEFORE the `if connected:` branch
    last_check_pos = fn_body.find('POLL_STATUS["last_check"]')
    if_connected_pos = fn_body.find("if connected:")
    assert last_check_pos != -1, "POLL_STATUS['last_check'] never set in do_poll_check"
    assert last_check_pos < if_connected_pos, (
        "POLL_STATUS['last_check'] is only set inside 'if connected:' block. "
        "It should be set unconditionally after the connect() call so we can "
        "distinguish 'never ran' (null) from 'ran but IMAP failed'."
    )


# ── Tests: Email poller self-forward detection ────────────────────────────────

def test_self_forward_bypasses_reply_filter():
    """Confirmed self-forwards must set _is_self_forward=True to skip is_reply_followup."""
    path = os.path.join(ROOT, "src/agents/email_poller.py")
    with open(path) as f:
        source = f.read()

    assert "_is_self_forward = True" in source, (
        "Self-forward detection does not set _is_self_forward=True. "
        "Without this, Fwd: emails from mike@ get misrouted as follow-ups "
        "by is_reply_followup() and never reach is_rfq_email()."
    )
    assert "_is_self_forward = False" in source, (
        "_is_self_forward flag never initialised to False before the self-email block. "
        "This can cause NameError or use stale value from a previous iteration."
    )
    assert "None if _is_self_forward else is_reply_followup" in source, (
        "is_reply_followup() is not skipped for self-forwards. "
        "Fwd: emails from mike@ will be misrouted as follow-ups."
    )


def test_reply_followup_not_called_on_known_rfq_subjects():
    """is_reply_followup should return None for strong RFQ keywords regardless of Re:/Fwd: prefix."""
    sys.path.insert(0, ROOT)
    from src.agents.email_poller import is_reply_followup
    import email as email_mod

    # Simulate a forwarded CALVET RFQ — no active sender match, should return None
    raw_msg = email_mod.message_from_string(
        "From: mike@reytechinc.com\nSubject: Fwd: CALVET RFQ DUE 03/13/26\n\n"
        "---------- Forwarded message ---------\nFrom: buyer@calvet.ca.gov\n\n"
        "Please provide a quote for the following items..."
    )
    result = is_reply_followup(
        raw_msg,
        "Fwd: CALVET RFQ DUE 03/13/26",
        "---------- Forwarded message ---------\nFrom: buyer@calvet.ca.gov\n\nPlease provide a quote",
        "mike@reytechinc.com",
        []
    )
    # Should be None — unknown sender, no active items
    assert result is None, (
        f"is_reply_followup returned {result} for a forwarded CALVET RFQ with no active sender. "
        "Unknown senders should pass through to is_rfq_email()."
    )


# ── Tests: Poll loop crash visibility ────────────────────────────────────────

def test_poll_loop_logs_crashes():
    """email_poll_loop must catch and log exceptions so crashes aren't silent."""
    path = os.path.join(ROOT, "src/api/dashboard.py")
    with open(path) as f:
        source = f.read()

    start = source.index("def email_poll_loop():")
    end = source.index("\ndef ", start + 1)
    fn_body = source[start:end]

    assert "POLL_STATUS[\"error\"]" in fn_body or "POLL_STATUS['error']" in fn_body, (
        "email_poll_loop does not write to POLL_STATUS['error'] on exception. "
        "Crashes are invisible — last_check stays null with no explanation."
    )
    assert "log.error" in fn_body or "log.warning" in fn_body, (
        "email_poll_loop has no log.error/log.warning call. "
        "Exceptions are swallowed silently."
    )
