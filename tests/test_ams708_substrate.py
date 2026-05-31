"""PR-4 / Phase 2 substrate — AMS 708 form registration + flag swap.

2026-05-12: rfq_0ebe242f (CCWF Ashley) email contract said explicitly
that CCHCS/CDCR is dropping STD 1000 in favor of AMS 708 for the
GenAI Use Disclosure. This PR registers the new form, wires email-
detection patterns, adds it to CCHCS optional_forms, and ships a
flag-gated swap helper (`AMS708_REPLACES_STD1000=1`) that flips any
agency form list with `std1000` to use `ams708` instead.

The standalone filler `fill_ams708_standalone` is a typed no-op until
the blank PDF template (`data/templates/ams_708_blank.pdf`) lands in
the repo. The substrate plumbing is independent of the filler, so
this can ship today and the filler can fill in tomorrow.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── Forms catalog + agency config ─────────────────────────────────


def test_ams708_in_forms_catalog():
    from src.core.agency_config import AVAILABLE_FORMS
    ids = [f["id"] for f in AVAILABLE_FORMS]
    assert "ams708" in ids, f"ams708 missing from AVAILABLE_FORMS: {ids!r}"


def test_ams708_in_cchcs_optional_forms():
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    assert "ams708" in cchcs["optional_forms"]


def test_std1000_still_in_cchcs_optional_forms_until_swap_enabled():
    """The swap is GATED. Until the env var flips, std1000 stays in
    the optional list so today's behavior is preserved."""
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    cchcs = DEFAULT_AGENCY_CONFIGS["cchcs"]
    assert "std1000" in cchcs["optional_forms"]


# ── Email pattern detection ───────────────────────────────────────


def test_ams708_detected_in_email_body():
    """Buyer email saying 'AMS 708 required' must surface in
    extract_required_forms_from_text as ams708."""
    from src.core.agency_config import extract_required_forms_from_text
    text = "Please include the AMS 708 GenAI use disclosure form."
    result = extract_required_forms_from_text(text)
    assert "ams708" in result["forms"]


def test_genai_keywords_also_route_to_ams708():
    """rfq_0ebe242f said 'New GENAI form'. That keyword must route
    to ams708 (not std1000) under the new mapping."""
    from src.core.agency_config import extract_required_forms_from_text
    result = extract_required_forms_from_text(
        "New GENAI form required in RFQ packages moving forward"
    )
    # Either pattern set can match GENAI but ams708 should be present.
    assert "ams708" in result["forms"], result


def test_std1000_legacy_string_still_matches():
    """An old buyer email that literally says 'STD 1000' must still
    detect the std1000 form — we haven't removed the form, just added
    the new one."""
    from src.core.agency_config import extract_required_forms_from_text
    result = extract_required_forms_from_text("Submit STD 1000 with your bid.")
    assert "std1000" in result["forms"]


# ── Swap helper ──────────────────────────────────────────────────


def test_swap_no_op_when_flag_off(monkeypatch):
    from src.core.agency_config import swap_std1000_for_ams708
    monkeypatch.delenv("AMS708_REPLACES_STD1000", raising=False)
    forms = ["quote", "std1000", "dvbe843"]
    result = swap_std1000_for_ams708(forms)
    assert result == ["quote", "std1000", "dvbe843"]


def test_swap_replaces_std1000_when_flag_on(monkeypatch):
    from src.core.agency_config import swap_std1000_for_ams708
    monkeypatch.setenv("AMS708_REPLACES_STD1000", "1")
    forms = ["quote", "std1000", "dvbe843"]
    result = swap_std1000_for_ams708(forms)
    assert result == ["quote", "ams708", "dvbe843"]


def test_swap_preserves_order(monkeypatch):
    from src.core.agency_config import swap_std1000_for_ams708
    monkeypatch.setenv("AMS708_REPLACES_STD1000", "1")
    forms = ["703b", "704b", "bidpkg", "quote", "std1000", "calrecycle74"]
    result = swap_std1000_for_ams708(forms)
    assert result == ["703b", "704b", "bidpkg", "quote", "ams708", "calrecycle74"]


def test_swap_is_idempotent(monkeypatch):
    """If a form list already has ams708, running the swap doesn't
    add a duplicate (and doesn't preserve a stray std1000)."""
    from src.core.agency_config import swap_std1000_for_ams708
    monkeypatch.setenv("AMS708_REPLACES_STD1000", "1")
    forms = ["quote", "ams708", "std1000", "dvbe843"]
    result = swap_std1000_for_ams708(forms)
    assert result == ["quote", "ams708", "dvbe843"]
    # Run again — must be identical
    assert swap_std1000_for_ams708(result) == result


def test_swap_no_std1000_in_list_returns_unchanged(monkeypatch):
    from src.core.agency_config import swap_std1000_for_ams708
    monkeypatch.setenv("AMS708_REPLACES_STD1000", "1")
    forms = ["quote", "dvbe843"]
    assert swap_std1000_for_ams708(forms) == ["quote", "dvbe843"]


def test_swap_accepts_truthy_env_aliases(monkeypatch):
    from src.core.agency_config import _ams708_swap_enabled
    for val in ("1", "true", "yes", "on", "TRUE", "Yes"):
        monkeypatch.setenv("AMS708_REPLACES_STD1000", val)
        assert _ams708_swap_enabled(), f"{val!r} should enable the swap"


def test_swap_rejects_falsey_env(monkeypatch):
    from src.core.agency_config import _ams708_swap_enabled
    for val in ("0", "false", "no", "off", "", "  ", "maybe"):
        monkeypatch.setenv("AMS708_REPLACES_STD1000", val)
        assert not _ams708_swap_enabled(), f"{val!r} must NOT enable the swap"


def test_swap_handles_non_list_input(monkeypatch):
    """Defensive: a None or string passed in should return unchanged."""
    from src.core.agency_config import swap_std1000_for_ams708
    monkeypatch.setenv("AMS708_REPLACES_STD1000", "1")
    assert swap_std1000_for_ams708(None) is None
    assert swap_std1000_for_ams708("std1000") == "std1000"


# ── Standalone filler stub ────────────────────────────────────────


def test_fill_ams708_standalone_skips_when_template_missing(tmp_path, caplog, monkeypatch):
    """The standalone 708 is derived from the CDCR bid-package template. When
    that source is absent, the filler must skip gracefully (return False, log
    WARN) rather than raise — so the generator can surface the gap."""
    import logging
    import src.forms.fill_ams708 as m
    monkeypatch.setattr(m, "_bidpkg_template_path",
                        lambda: str(tmp_path / "no_such_template.pdf"))
    out = tmp_path / "ams708_test.pdf"
    rfq = {"solicitation_number": "R26Q41", "sign_date": "2026-05-12"}
    config = {
        "company": {
            "name": "Reytech Inc.", "phone": "949-229-1575",
            "address": "30 Carnoustie Way", "city": "Trabuco Canyon",
            "state": "CA", "zip": "92679", "owner": "Mike Garrison",
            "title": "Owner", "fein": "47-4588061",
        }
    }
    with caplog.at_level(logging.WARNING, logger="reytech.fill_ams708"):
        ok = m.fill_ams708_standalone(rfq, config, str(out))
    assert ok is False
    assert not out.exists(), "filler must NOT write a file when template missing"
    msgs = [r.message for r in caplog.records]
    assert any("source template not present" in m for m in msgs), (
        f"expected template-missing warning, got: {msgs!r}"
    )


def test_ams708_template_available_now_that_source_lands():
    """The 708 is derived from the bid-package template, which IS in the repo.
    (Superseded the old `_returns_false_today` pin once the derive-from-bidpkg
    filler shipped — the standalone blank approach was abandoned.)"""
    from src.forms.fill_ams708 import ams708_template_available
    assert ams708_template_available() is True
