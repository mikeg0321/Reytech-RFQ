"""Feature tests for deadline defaults + daily digest.

Guards:
  1. add_business_days skips weekends correctly.
  2. compute_default_deadline returns parseable mm/dd/YYYY + 2:00 PM.
  3. resolve_or_default priority: header > email > default.
  4. apply_default_if_missing is a no-op when due_date already set.
  5. Centralized save hook: _save_single_pc + _save_single_rfq call
     apply_default_if_missing before writing to DB.
  6. Daily digest body formats OVERDUE / DUE TODAY / DUE TOMORROW sections.
  7. send_daily_digest uses gmail_api (not smtplib).
  8. Digest + backfill are registered at startup in routes_intel_ops.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.core.deadline_defaults import (
    add_business_days,
    apply_default_if_missing,
    compute_default_deadline,
    resolve_or_default,
)
from src.agents.notify_agent import _build_digest_body, _format_digest_line


REPO = Path(__file__).resolve().parent.parent


def _strip_comments(src: str) -> str:
    src = re.sub(r'""".*?"""', "", src, flags=re.DOTALL)
    src = re.sub(r"'''.*?'''", "", src, flags=re.DOTALL)
    return "\n".join(
        line for line in src.splitlines()
        if not line.lstrip().startswith("#")
    )


# ── 1. add_business_days ────────────────────────────────────────────────────

def test_add_business_days_from_monday():
    mon = datetime(2026, 4, 20)  # Monday
    assert add_business_days(mon, 2).date() == datetime(2026, 4, 22).date()  # Wed


def test_add_business_days_skips_weekend():
    fri = datetime(2026, 4, 24)  # Friday
    # Fri + 2 business days = Tuesday (skip Sat+Sun)
    assert add_business_days(fri, 2).date() == datetime(2026, 4, 28).date()


def test_add_business_days_from_saturday():
    sat = datetime(2026, 4, 25)  # Saturday
    # Sat + 1 biz day = Monday
    assert add_business_days(sat, 1).date() == datetime(2026, 4, 27).date()


def test_add_business_days_zero_is_noop():
    d = datetime(2026, 4, 20)
    assert add_business_days(d, 0) == d


# ── 2. compute_default_deadline ─────────────────────────────────────────────

def test_default_deadline_format_parseable():
    date_str, time_str = compute_default_deadline()
    # Must be parseable by datetime.strptime in the same formats
    # _parse_due_datetime supports.
    datetime.strptime(date_str, "%m/%d/%Y")
    assert time_str == "02:00 PM"


def test_default_deadline_is_two_biz_days_out():
    # Pick a known Monday so the result is deterministic
    mon = datetime(2026, 4, 20, 10, 0, tzinfo=timezone(timedelta(hours=-8)))
    date_str, _ = compute_default_deadline(now=mon)
    assert date_str == "04/22/2026"  # Wednesday


# ── 3. resolve_or_default priority ──────────────────────────────────────────

def test_resolve_prefers_header():
    d, t, src = resolve_or_default("04/25/2026", "3:00 PM", email_body="by 5/1/2026")
    assert src == "header"
    assert d == "04/25/2026"
    assert t == "3:00 PM"


def test_resolve_falls_through_to_email():
    d, t, src = resolve_or_default("", "", email_body="Please respond by 05/01/2026")
    assert src == "email"
    assert d == "2026-05-01"  # _extract_due_date returns ISO


def test_resolve_falls_through_to_default():
    d, t, src = resolve_or_default("", "", email_body="no date mentioned here")
    assert src == "default"
    datetime.strptime(d, "%m/%d/%Y")
    assert t == "02:00 PM"


# ── 4. apply_default_if_missing idempotence ────────────────────────────────

def test_apply_default_when_missing():
    doc = {"id": "pc_x", "status": "new", "due_date": ""}
    src = apply_default_if_missing(doc)
    assert src == "default"
    assert doc["due_date"]
    assert doc["due_date_source"] == "default"


def test_apply_default_is_noop_when_set():
    doc = {"id": "pc_x", "due_date": "04/25/2026", "due_date_source": "header"}
    src = apply_default_if_missing(doc)
    assert src is None
    assert doc["due_date"] == "04/25/2026"


# ── 4b. body-key fallback — regression for the 2026-04-22 incident ─────────
# Before the fix, apply_default_if_missing only looked at explicit email_body
# arg or doc["email_body"]. Ingest writes the buyer text under body_text /
# body / body_preview, so callers that pass only the doc silently lost email-
# extracted due dates and fell through to the 2-biz-day default.

def test_apply_default_reads_body_text_from_doc():
    """body_text is the canonical ingest key — must be picked up automatically."""
    doc = {
        "id": "rfq_x", "status": "new", "due_date": "",
        "body_text": "Please respond no later than 04/23/2026.",
    }
    src = apply_default_if_missing(doc)
    assert src == "email", "body_text should have fed the email-extract path"
    assert doc["due_date"] == "2026-04-23"


def test_apply_default_reads_body_from_doc():
    """Secondary ingest key — same precedence logic."""
    doc = {
        "id": "pc_x", "status": "new", "due_date": "",
        "body": "Quotes due by 05/01/2026 at 2:00 PM",
    }
    src = apply_default_if_missing(doc)
    assert src == "email"
    assert doc["due_date"] == "2026-05-01"


def test_apply_default_reads_body_preview_from_doc():
    """Tertiary ingest key — covers the narrow Gmail-preview-only case."""
    doc = {
        "id": "pc_x", "status": "new", "due_date": "",
        "body_preview": "... no later than 06/15/2026 ...",
    }
    src = apply_default_if_missing(doc)
    assert src == "email"
    assert doc["due_date"] == "2026-06-15"


def test_apply_default_prefers_email_body_when_multiple_keys_present():
    """Precedence: email_body > body_text > body > body_preview. Admin edits
    sometimes re-save with email_body; that value wins when both are present."""
    doc = {
        "id": "rfq_x", "status": "new", "due_date": "",
        "email_body": "... due 04/23/2026 ...",
        "body_text": "... due 05/01/2026 ...",
    }
    src = apply_default_if_missing(doc)
    assert src == "email"
    assert doc["due_date"] == "2026-04-23"


def test_apply_default_falls_to_default_when_no_body_keys():
    """Clean doc with no body text anywhere — must still stamp the default."""
    doc = {"id": "pc_x", "status": "new", "due_date": ""}
    src = apply_default_if_missing(doc)
    assert src == "default"
    assert doc["due_date_source"] == "default"


def test_apply_default_explicit_arg_overrides_doc_body():
    """Caller-provided email_body wins over any doc-level body keys."""
    doc = {
        "id": "rfq_x", "status": "new", "due_date": "",
        "body_text": "... due 05/01/2026 ...",
    }
    src = apply_default_if_missing(doc, email_body="... due 04/23/2026 ...")
    assert src == "email"
    assert doc["due_date"] == "2026-04-23"


# ── 5. Save hooks centralize the default ────────────────────────────────────

def test_save_single_pc_invokes_default_helper():
    src = (REPO / "src/api/data_layer.py").read_text(encoding="utf-8")
    code = _strip_comments(src)
    m = re.search(r"def _save_single_pc\([^)]*\).*?def \w", code, re.DOTALL)
    assert m, "_save_single_pc body not found"
    body = m.group(0)
    assert "apply_default_if_missing" in body, \
        "_save_single_pc must call apply_default_if_missing (centralizes the default)"


def test_save_single_rfq_invokes_default_helper():
    src = (REPO / "src/api/data_layer.py").read_text(encoding="utf-8")
    code = _strip_comments(src)
    m = re.search(r"def _save_single_rfq\([^)]*\).*?def \w", code, re.DOTALL)
    assert m, "_save_single_rfq body not found"
    body = m.group(0)
    assert "apply_default_if_missing" in body, \
        "_save_single_rfq must call apply_default_if_missing (centralizes the default)"


# ── 6. Digest body formatting ───────────────────────────────────────────────

def test_format_digest_line_today():
    it = {
        "doc_type": "rfq",
        "pc_number": "10843276",
        "institution": "CIW",
        "hours_left": 6.5,
        "urgency": "urgent",
        "due_time": "2:00 PM",
        "due_date": "04/22/2026",
        "countdown_text": "6.5h remaining",
    }
    line = _format_digest_line(it)
    assert "RFQ 10843276" in line
    assert "CIW" in line
    assert "today" in line
    assert "2:00 PM" in line


def test_format_digest_line_overdue():
    it = {"doc_type": "pc", "pc_number": "PC-123", "institution": "CDCR",
          "hours_left": -5, "urgency": "overdue", "countdown_text": "5.0h overdue",
          "due_time": "", "due_date": "04/21/2026"}
    line = _format_digest_line(it)
    assert "OVERDUE" in line


def test_build_digest_body_sections():
    items = [
        {"doc_type": "rfq", "pc_number": "A", "institution": "CIW",
         "hours_left": -2, "urgency": "overdue", "countdown_text": "2h overdue",
         "due_time": "", "due_date": "04/21/2026"},
        {"doc_type": "pc", "pc_number": "B", "institution": "CDCR",
         "hours_left": 3, "urgency": "critical", "countdown_text": "3h remaining",
         "due_time": "2:00 PM", "due_date": "04/22/2026"},
        {"doc_type": "rfq", "pc_number": "C", "institution": "CalVet",
         "hours_left": 30, "urgency": "urgent", "countdown_text": "30h",
         "due_time": "5:00 PM", "due_date": "04/23/2026"},
    ]
    body = _build_digest_body(items)
    assert "OVERDUE" in body
    assert "DUE TODAY" in body
    assert "DUE TOMORROW" in body
    assert "A" in body and "B" in body and "C" in body


def test_build_digest_body_empty():
    body = _build_digest_body([])
    assert "clear" in body.lower() or "no pcs" in body.lower()


# ── 7. Digest uses gmail_api, not smtplib ──────────────────────────────────

def test_send_daily_digest_uses_gmail_api_only():
    src = (REPO / "src/agents/notify_agent.py").read_text(encoding="utf-8")
    code = _strip_comments(src)
    m = re.search(r"def send_daily_digest\(\).*?(?=\ndef |\n# ═)", code, re.DOTALL)
    assert m, "send_daily_digest body not found"
    body = m.group(0)
    assert "gmail_api.send_message" in body, \
        "digest must send via gmail_api.send_message (Gmail OAuth, not app password)"
    assert "smtplib.SMTP(" not in body, \
        "digest must not use smtplib — Gmail API path only"


# ── 8. Startup registration ────────────────────────────────────────────────

def test_daily_digest_registered_at_startup():
    src = (REPO / "src/api/modules/routes_intel_ops.py").read_text(encoding="utf-8")
    code = _strip_comments(src)
    assert "start_daily_digest" in code, \
        "start_daily_digest must be registered at the same startup site as start_stale_watcher"


def test_backfill_registered_at_startup():
    src = (REPO / "src/api/modules/routes_intel_ops.py").read_text(encoding="utf-8")
    code = _strip_comments(src)
    assert "backfill_missing_deadlines" in code, \
        "one-time backfill must be registered at startup so existing blank due_dates get defaults"
