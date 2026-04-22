"""RE-AUDIT-6 (P0): PC quote send hardening.

Two fixes in one commit:

1. **Duplicate-send guard** — `api_pc_send_quote` previously:
   - Had no in-flight lock, so a double-click (or two browser tabs)
     fired two concurrent smtplib sends. The buyer saw two identical
     emails, both marked the PC `sent` twice, and the activity log
     appended twice.
   - Had no status guard, so a user who refreshed the page after
     sending and clicked "Send" again silently re-sent.

   Fixed with: module-level `_pc_send_inflight` set guarded by
   `_pc_send_inflight_lock`, plus an `already_sent` 409 check
   (bypass via `force=true`).

2. **Raw SMTP → Gmail API** — the send path used
   `smtplib.SMTP_SSL("smtp.gmail.com", 465)` with
   `GMAIL_ADDRESS` / `GMAIL_PASSWORD` (app-password auth). Google
   is deprecating app passwords in stages. Migrated to the same
   pattern bundle-send and send_quote_email use:
   `gmail_api.get_send_service()` + `gmail_api.send_message()`.

Prior migration references: routes_pricecheck_gen.py (bundle-send),
routes_analytics.py (IN-5 send_quote_email).
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_PC_ADMIN = _REPO / "src" / "api" / "modules" / "routes_pricecheck_admin.py"


def _read(rel: str) -> str:
    return (_REPO / rel).read_text(encoding="utf-8")


def _strip_comments_and_docstrings(src: str) -> str:
    """Drop comments + triple-quoted blocks so guards match code, not prose."""
    import re
    src = re.sub(r'"""[\s\S]*?"""', "", src)
    src = re.sub(r"'''[\s\S]*?'''", "", src)
    out = []
    for line in src.splitlines():
        s = line.lstrip()
        if s.startswith("#"):
            continue
        if " # " in line:
            line = line.split(" # ", 1)[0]
        out.append(line)
    return "\n".join(out)


# ── Migration guards: off smtplib, onto gmail_api ───────────────────────────

def test_pc_send_quote_no_smtplib_smtp_ssl():
    body = _strip_comments_and_docstrings(_read("src/api/modules/routes_pricecheck_admin.py"))
    assert "smtplib.SMTP_SSL(" not in body, (
        "OB-20 regression: smtplib.SMTP_SSL( is back in routes_pricecheck_admin.py. "
        "PC send-quote must go through src.core.gmail_api like the bundle-send "
        "and send_quote_email migrations."
    )


def test_pc_send_quote_no_smtplib_import():
    body = _strip_comments_and_docstrings(_read("src/api/modules/routes_pricecheck_admin.py"))
    assert "import smtplib" not in body, (
        "OB-20 regression: raw `import smtplib` is back — migration should "
        "have removed both the import and the send call."
    )


def test_pc_send_quote_no_gmail_password_dependency():
    body = _strip_comments_and_docstrings(_read("src/api/modules/routes_pricecheck_admin.py"))
    # There's a legitimate use of GMAIL_ADDRESS elsewhere (email_log sender).
    # GMAIL_PASSWORD is strictly the deprecated app-password — it should be
    # fully gone from the send path.
    # Find the api_pc_send_quote function only (slice between def and next def).
    start = body.find("def api_pc_send_quote(")
    assert start >= 0, "api_pc_send_quote function not found"
    # Next top-level def after it
    next_def = body.find("\ndef ", start + 1)
    send_fn = body[start:next_def] if next_def > 0 else body[start:]
    assert 'os.environ.get("GMAIL_PASSWORD"' not in send_fn, (
        "OB-20 regression: api_pc_send_quote reads GMAIL_PASSWORD — the "
        "OAuth migration must not fall back to app-password auth."
    )


def test_pc_send_quote_uses_gmail_api_send_message():
    body = _read("src/api/modules/routes_pricecheck_admin.py")
    assert "gmail_api.send_message(" in body, (
        "OB-20 regression: gmail_api.send_message call missing from the "
        "PC send-quote path."
    )


def test_pc_send_quote_checks_gmail_api_is_configured():
    body = _read("src/api/modules/routes_pricecheck_admin.py")
    assert "gmail_api.is_configured()" in body, (
        "OB-20 regression: missing is_configured() guard. Unconfigured "
        "prod will 500 from inside googleapiclient instead of returning "
        "a clean 400."
    )


def test_pc_send_quote_preserves_reytech_attachment_name():
    """gmail_api.send_message derives the attachment filename from
    os.path.basename(path). If we don't copy to a named temp file, the
    buyer receives an internal filename (whatever the on-disk PDF is
    named) instead of Quote_<pc_num>_Reytech.pdf."""
    body = _read("src/api/modules/routes_pricecheck_admin.py")
    assert "attach_name" in body and "shutil.copy" in body, (
        "OB-20 regression: PDF is no longer copied to a temp file with "
        "the Reytech-branded filename. Buyer will see an internal name."
    )


def test_pc_send_quote_cleans_up_temp_dir():
    body = _read("src/api/modules/routes_pricecheck_admin.py")
    assert "shutil.rmtree(tmp_dir" in body, (
        "OB-20 regression: temp dir cleanup is gone. Each failed send "
        "leaks a dir under /tmp until the worker restarts."
    )


# ── Single-flight guard source-level checks ────────────────────────────────

def test_pc_send_inflight_helpers_defined():
    body = _read("src/api/modules/routes_pricecheck_admin.py")
    for needed in (
        "_pc_send_inflight_lock",
        "_pc_send_inflight",
        "_claim_pc_send_slot",
        "_release_pc_send_slot",
    ):
        assert needed in body, (
            f"PC-12 regression: {needed} missing — the single-flight guard "
            "against double-click duplicate sends is not defined."
        )


def test_pc_send_quote_calls_claim_and_release():
    body = _read("src/api/modules/routes_pricecheck_admin.py")
    start = body.find("def api_pc_send_quote(")
    next_def = body.find("\ndef ", start + 1)
    fn = body[start:next_def] if next_def > 0 else body[start:]
    assert "_claim_pc_send_slot(" in fn, (
        "PC-12 regression: api_pc_send_quote no longer claims a send slot. "
        "A double-click will fire two concurrent sends to the buyer."
    )
    assert "_release_pc_send_slot(" in fn and "finally:" in fn, (
        "PC-12 regression: _release_pc_send_slot must be in a finally: "
        "block so a crashed send cannot permanently lock the PC."
    )


def test_pc_send_quote_already_sent_guard_present():
    body = _read("src/api/modules/routes_pricecheck_admin.py")
    start = body.find("def api_pc_send_quote(")
    next_def = body.find("\ndef ", start + 1)
    fn = body[start:next_def] if next_def > 0 else body[start:]
    assert 'pc.get("status") == "sent"' in fn, (
        "PC-12 regression: already-sent guard missing — refresh + click "
        "will silently re-send to the buyer."
    )
    assert 'force' in fn, (
        "PC-12: already-sent guard must be bypassable via `force=true` "
        "for intentional re-sends."
    )
    assert " 409" in fn, (
        "PC-12: already-sent / in-flight paths must return status 409."
    )


# ── Functional: single-flight helpers behave correctly ─────────────────────

def test_claim_and_release_slot_semantics():
    """Second claim returns False until release — basic lock correctness."""
    from src.api.modules.routes_pricecheck_admin import (
        _claim_pc_send_slot,
        _release_pc_send_slot,
    )
    pcid = "pc_test_slot_semantics"
    # Ensure clean slate (in case a prior test crashed)
    _release_pc_send_slot(pcid)

    assert _claim_pc_send_slot(pcid) is True, "first claim should succeed"
    assert _claim_pc_send_slot(pcid) is False, (
        "second concurrent claim MUST fail — otherwise double-click "
        "duplicate-send returns."
    )
    _release_pc_send_slot(pcid)
    assert _claim_pc_send_slot(pcid) is True, (
        "after release, the slot must be reclaimable — otherwise a crashed "
        "send permanently locks the PC."
    )
    _release_pc_send_slot(pcid)


def test_release_slot_idempotent():
    """Double release must not raise — important because `finally:`
    runs after every path including the early-return 'no pdf' path."""
    from src.api.modules.routes_pricecheck_admin import _release_pc_send_slot
    _release_pc_send_slot("pc_never_claimed_" + "abc")
    _release_pc_send_slot("pc_never_claimed_" + "abc")  # no exception


# ── Sanity: gmail_api contract still intact ────────────────────────────────

def test_gmail_api_send_message_signature_stable():
    body = _read("src/core/gmail_api.py")
    for needed in (
        "def send_message(",
        "def is_configured()",
        "def get_send_service(",
    ):
        assert needed in body, f"gmail_api contract changed: {needed!r} missing"
