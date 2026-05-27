"""Pins the substrate fix shipped 2026-05-27 after deploy-log audit:
form-field drift was logged as WARNING and forgotten.

Prod log 2026-05-27 04:22:48 (one RFQ package gen):
  [wrn] fill_and_sign_pdf PRE-FILL: 14/22 field names not in template
        AMS 704B - PR 10847395 - Attachment 2.pdf: [...]
  [wrn] fill_and_sign_pdf: 14/22 intended fields not found in output: [...]
  [wrn] fill_and_sign_pdf PRE-FILL: 93/174 field names not in template
        BID PACKAGE _ FORMS (Under 100k) - Attachment 3.pdf: [...]
  [wrn] fill_and_sign_pdf: 93/174 intended fields not found in output: [...]

Operator never saw these — they're in deploy logs only. The monthly
`forms_drift_monitor` scheduler caught drift once a month. By then 30
days of packages have shipped with empty fields.

Fix: `_log_form_drift_audit(template, unmatched, total, phase)` writes
one audit_trail row per drift event during the live package-gen run,
so the audit-trail dashboard surfaces drift immediately.
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

from src.forms.reytech_filler_v4 import _log_form_drift_audit


REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET = REPO_ROOT / "src" / "forms" / "reytech_filler_v4.py"


def test_helper_writes_audit_row_on_drift(monkeypatch):
    """When called with non-empty unmatched_fields, _log_form_drift_audit
    must invoke dal._audit with a `form_drift` entity_type."""
    calls = []

    def fake_audit(entity_type, entity_id, action, actor=None,
                   old_value=None, new_value=None):
        calls.append({
            "entity_type": entity_type, "entity_id": entity_id,
            "action": action, "actor": actor,
            "old_value": old_value, "new_value": new_value,
        })

    monkeypatch.setattr("src.core.dal._audit", fake_audit)

    _log_form_drift_audit(
        "/data/uploads/abc/AMS 704B - Test.pdf",
        ["COMPANY NAME_2", "Signature Date", "Vendor Name"],
        22,
        "pre_fill",
    )
    assert len(calls) == 1
    c = calls[0]
    assert c["entity_type"] == "form_drift"
    assert c["entity_id"] == "AMS 704B - Test.pdf"
    assert "pre_fill: 3/22" in c["action"]
    assert c["actor"] == "reytech_filler_v4"
    # new_value carries the sorted, capped field list
    assert "COMPANY NAME_2" in c["new_value"]


def test_helper_skips_when_no_drift(monkeypatch):
    """Empty `unmatched_fields` must NOT write an audit row — drift-free
    fills shouldn't pollute the audit table."""
    calls = []
    monkeypatch.setattr(
        "src.core.dal._audit",
        lambda *a, **kw: calls.append((a, kw)),
    )

    _log_form_drift_audit(
        "/data/uploads/abc/test.pdf", [], 10, "pre_fill"
    )
    assert calls == []


def test_helper_never_raises_on_audit_failure(monkeypatch):
    """An audit-write failure must NOT break PDF generation."""
    def fake_audit(*a, **kw):
        raise RuntimeError("audit DB locked")

    monkeypatch.setattr("src.core.dal._audit", fake_audit)

    # Must not raise
    _log_form_drift_audit(
        "/data/uploads/abc/test.pdf", ["FIELD_X"], 10, "post_fill"
    )


def test_helper_called_from_both_drift_sites():
    """Source-grep: both PRE-FILL and post-fill drift detection sites
    must call _log_form_drift_audit. Pin against regression that adds
    a third drift site without the audit hook."""
    src = TARGET.read_text(encoding="utf-8")

    # PRE-FILL site — adjacent to the existing warning log
    prefill_idx = src.find("fill_and_sign_pdf PRE-FILL")
    assert prefill_idx > 0
    prefill_block = src[prefill_idx:prefill_idx + 800]
    assert "_log_form_drift_audit" in prefill_block, (
        "PRE-FILL drift site must call _log_form_drift_audit "
        "alongside the WARNING log — otherwise the audit row is missing."
    )
    assert '"pre_fill"' in prefill_block

    # Post-fill verification site
    postfill_idx = src.find("intended fields not found in output")
    assert postfill_idx > 0
    postfill_block = src[postfill_idx:postfill_idx + 800]
    assert "_log_form_drift_audit" in postfill_block, (
        "Post-fill drift site must call _log_form_drift_audit alongside "
        "the WARNING log."
    )
    assert '"post_fill"' in postfill_block


def test_helper_truncates_long_field_lists():
    """The new_value column has a 2000-char cap (per dal._audit's
    truncation at `(new_value or '')[:2000]`). For a Bid Package with
    174 fields, the joined list could exceed that. We sort+take first
    50 to stay well under — pin the truncation behavior."""
    calls = []

    def fake_audit(entity_type, entity_id, action, actor=None,
                   old_value=None, new_value=None):
        calls.append(new_value)

    import sys
    sys.modules.pop("src.core.dal", None)  # ensure fresh import
    import src.core.dal as _dal_mod
    orig = _dal_mod._audit
    _dal_mod._audit = fake_audit
    try:
        # 174 fields à 30 chars each = 5220 chars unbounded
        huge_list = [f"FIELD_NAME_{i:03d}_LONG_TEXT" for i in range(174)]
        _log_form_drift_audit(
            "/data/uploads/abc/bigform.pdf", huge_list, 174, "pre_fill"
        )
    finally:
        _dal_mod._audit = orig

    assert len(calls) == 1
    # new_value must be <= 2000 chars (the dal cap is at 2000)
    assert len(calls[0]) <= 2000
    # And it must include the first sorted field name
    assert calls[0].startswith("FIELD_NAME_000_LONG_TEXT")
