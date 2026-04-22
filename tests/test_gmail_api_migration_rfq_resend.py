"""Regression: RFQ resend-package site migrated off smtplib.SMTP_SSL to gmail_api.

Second of the 5 remaining outbound smtplib sites (after PR #365 bundle-send,
PR #411 PC send-quote). `api_resend_package` in routes_rfq.py was the support
re-delivery path — "support view" resend of the latest approved package to a
buyer. Same app-password problem as the other sites: GMAIL_ADDRESS +
GMAIL_PASSWORD via smtplib.SMTP_SSL to smtp.gmail.com:465, silently breaks
when Google deprecates app passwords.

Fix: gmail_api.is_configured() gate → get_send_service() → send_message()
with the pkg_data written to a named temp file under pkg_filename so the
buyer receives `RFQ_Package_<sol>_ReytechInc.pdf` and not an internal temp
name. Cleanup in finally: block.

Prior migration references:
  - routes_pricecheck_gen.py (PR #365 bundle-send)
  - routes_pricecheck_admin.py (PR #411 PC send-quote)
  - routes_analytics.py (IN-5 send_quote_email)
"""
from __future__ import annotations

from pathlib import Path


_REPO = Path(__file__).resolve().parent.parent
_ROUTES_RFQ = _REPO / "src" / "api" / "modules" / "routes_rfq.py"


def _read(rel_or_path) -> str:
    if isinstance(rel_or_path, Path):
        return rel_or_path.read_text(encoding="utf-8")
    return (_REPO / rel_or_path).read_text(encoding="utf-8")


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


def _resend_fn_body() -> str:
    """Slice api_resend_package between its def and the next top-level def."""
    body = _read(_ROUTES_RFQ)
    start = body.find("def api_resend_package(")
    assert start >= 0, "api_resend_package not found in routes_rfq.py"
    next_def = body.find("\ndef ", start + 1)
    return body[start:next_def] if next_def > 0 else body[start:]


# ── Migration guards: off smtplib, onto gmail_api ───────────────────────────

def test_rfq_resend_no_smtplib_smtp_ssl():
    body = _strip_comments_and_docstrings(_read(_ROUTES_RFQ))
    assert "smtplib.SMTP_SSL(" not in body, (
        "Gmail API regression: smtplib.SMTP_SSL( is back in routes_rfq.py. "
        "api_resend_package must route through src.core.gmail_api like the "
        "bundle-send and PC send-quote migrations."
    )


def test_rfq_resend_no_smtplib_import():
    fn = _strip_comments_and_docstrings(_resend_fn_body())
    assert "import smtplib" not in fn, (
        "Gmail API regression: `import smtplib` is back inside "
        "api_resend_package. Migration removed both the import and send."
    )


def test_rfq_resend_no_gmail_password_dependency():
    """GMAIL_PASSWORD is the deprecated app-password env var. OAuth
    migration must not fall back to app-password auth."""
    fn = _strip_comments_and_docstrings(_resend_fn_body())
    assert 'os.environ.get("GMAIL_PASSWORD"' not in fn, (
        "Gmail API regression: api_resend_package reads GMAIL_PASSWORD — "
        "OAuth migration must not fall back to app-password auth."
    )


def test_rfq_resend_uses_gmail_api_send_message():
    fn = _resend_fn_body()
    assert "gmail_api.send_message(" in fn, (
        "Gmail API regression: gmail_api.send_message call missing from "
        "api_resend_package."
    )


def test_rfq_resend_checks_gmail_api_is_configured():
    fn = _resend_fn_body()
    assert "gmail_api.is_configured()" in fn, (
        "Gmail API regression: missing is_configured() guard. Unconfigured "
        "prod will 500 from inside googleapiclient instead of returning a "
        "clean 400."
    )


def test_rfq_resend_writes_pkg_to_named_tempfile():
    """gmail_api.send_message derives the attachment filename from
    os.path.basename(path). pkg_data is in memory — without writing to
    a file named pkg_filename, the buyer receives a temp-internal name
    instead of RFQ_Package_<sol>_ReytechInc.pdf."""
    fn = _resend_fn_body()
    assert "named_pdf" in fn and "pkg_filename" in fn, (
        "Gmail API regression: pkg_data is no longer written to a temp "
        "file named pkg_filename. Buyer will see the internal temp name."
    )
    # The write must go to the named path — guard against "write to any file
    # but attach the wrong one".
    assert 'open(named_pdf, "wb"' in fn or "open(named_pdf,'wb'" in fn, (
        "Gmail API regression: pkg_data is no longer written to the "
        "named_pdf path before being attached."
    )


def test_rfq_resend_cleans_up_temp_dir():
    fn = _resend_fn_body()
    assert "shutil.rmtree(tmp_dir" in fn, (
        "Gmail API regression: temp dir cleanup for RFQ resend is gone. "
        "Each send leaks a dir under /tmp until the worker restarts."
    )
    assert "finally:" in fn, (
        "Gmail API regression: cleanup is not in a finally: block. A "
        "send exception will leave tmp_dir on disk."
    )


def test_rfq_resend_preserves_auth_required():
    """Sanity: the migration must not have silently dropped @auth_required."""
    body = _read(_ROUTES_RFQ)
    start = body.find('@bp.route("/api/rfq/<rid>/resend-package"')
    assert start >= 0, "resend-package route decorator missing"
    # Look forward up to the def line — @auth_required must appear first.
    window = body[start:body.find("def api_resend_package(", start)]
    assert "@auth_required" in window, (
        "Gmail API regression: @auth_required decorator dropped during "
        "migration. The support-view resend endpoint must not be public."
    )


# ── Sanity: gmail_api contract stable

def test_gmail_api_send_message_signature_stable():
    body = _read("src/core/gmail_api.py")
    for needed in (
        "def send_message(",
        "def is_configured()",
        "def get_send_service(",
    ):
        assert needed in body, f"gmail_api contract changed: {needed!r} missing"
