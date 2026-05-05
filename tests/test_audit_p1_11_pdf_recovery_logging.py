"""Audit P1 #11 (2026-05-06): PDF recovery from DB must log
schema-drift errors at WARNING level.

Background: `routes_pricecheck.py` has THREE call sites that fall back
from the `rfq_files` table to `email_attachments` when recovering a PC's
source PDF. Each site catches the inner exception. Two of three were
upgraded from `log.debug("Suppressed: ...")` to `log.warning("PC %s:
email_attachments lookup failed (likely schema drift) — %s")` in the
prior P1 audit pass; the third site (line ~2620) was missed because it
used a different exception variable name (`_e` vs `_ea_e`) so the
multi-occurrence edit didn't catch it.

The actual user-facing failure mode this fixes: when the
`email_attachments` table is missing or has schema drift, the operator
sees a generic "Source PDF not found" error and spends 20+ min
re-uploading the original. With WARNING-level logging, the schema
problem is visible in the operator's log tail / the error card on
/health.

These tests pin all three sites at the AST/source level so a future
edit can't silently regress one back to debug-level.
"""
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PRICECHECK = REPO / "src/api/modules/routes_pricecheck.py"


def test_all_three_email_attachments_lookups_warn_on_failure():
    """Three sites in routes_pricecheck.py do
    `SELECT data, filename FROM email_attachments WHERE pc_id=?`. All
    three must log at WARNING (not DEBUG) when the SELECT fails — the
    exception almost always means schema drift, not a transient error."""
    body = PRICECHECK.read_text(encoding="utf-8")
    select_sites = body.count(
        "SELECT data, filename FROM email_attachments WHERE pc_id=?"
    )
    assert select_sites == 3, (
        f"Expected 3 email_attachments recovery sites; found "
        f"{select_sites}. If a site was added or removed, update this "
        f"test."
    )
    # Each site's surrounding 12-line window must contain the
    # log.warning hint. Cheaper than parsing the AST.
    schema_drift_warnings = body.count("schema drift")
    assert schema_drift_warnings >= 3, (
        f"Expected at least 3 'schema drift' log.warning hints "
        f"(one per recovery site); found {schema_drift_warnings}. "
        f"A site was likely missed in the audit pass."
    )


def test_each_recovery_site_pairs_select_with_warning_within_window():
    """Locate every email_attachments SELECT and verify the next ~15
    lines contain a log.warning with the schema-drift hint. Catches the
    case where one site is fixed but a future copy-paste site forgets."""
    body = PRICECHECK.read_text(encoding="utf-8")
    needle = "SELECT data, filename FROM email_attachments WHERE pc_id=?"
    cursor = 0
    sites_checked = 0
    while True:
        idx = body.find(needle, cursor)
        if idx == -1:
            break
        sites_checked += 1
        # Peek ahead ~800 chars (the except-block + multi-line log.warning
        # comment + call together fit under that). Must see "schema drift"
        # inside.
        window = body[idx:idx + 800]
        assert "schema drift" in window, (
            f"PDF-recovery site #{sites_checked} (offset {idx}) does "
            f"not log 'schema drift' within 800 chars of the SELECT. "
            f"Audit P1 #11 fix was missed for this site."
        )
        cursor = idx + len(needle)
    assert sites_checked == 3, (
        f"Walked {sites_checked} sites; expected 3."
    )


def test_no_email_attachments_recovery_uses_debug_log():
    """Specifically the email_attachments PDF-recovery branch must NOT
    fall back to a `log.debug(` call. Other log.debug calls in the
    file are unrelated (intentionally quiet); just ban the combination
    `email_attachments` SELECT + actual `log.debug(` invocation within
    the same window. The substring `log.debug` may legitimately appear
    in comments explaining the audit history — match the call site
    only by including the open paren."""
    body = PRICECHECK.read_text(encoding="utf-8")
    needle = "SELECT data, filename FROM email_attachments WHERE pc_id=?"
    cursor = 0
    site = 0
    while True:
        idx = body.find(needle, cursor)
        if idx == -1:
            break
        site += 1
        window = body[idx:idx + 800]
        # Allow log.debug elsewhere in the file; only ban an actual
        # log.debug(...) CALL inside the except-block under the SELECT.
        end_marker = window.find("if row and row[")
        block = window[:end_marker] if end_marker > 0 else window
        assert "log.debug(" not in block, (
            f"PDF-recovery site #{site} (offset {idx}) caught the "
            f"email_attachments lookup with a log.debug() call. Audit "
            f"P1 #11 requires log.warning here so schema drift is "
            f"visible."
        )
        cursor = idx + len(needle)
    assert site == 3
