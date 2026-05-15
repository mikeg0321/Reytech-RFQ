"""PR-AV-AC3 — normalize PREQ prefix when comparing email vs RFQ sol#.

Third companion to PR-AV-AC1 + PR-AV-AC2. Closes the last false-positive
class from the 5/15 PREQ 10847262 (rfq_9e63456e) audit.

CONTEXT

AV-1 (PR #1005-era, see [[project_email_contract_substrate_pr_av_2026_05_14]])
intentionally strips known agency prefixes from the solicitation number
during ingest:

    "PREQ 10847262" → "10847262"

The intent: every downstream consumer (file naming, search, output dir,
form fills) sees the bare opaque sol#, not the PREQ-prefixed email
shorthand. CCHCS / DSH / CalVet all carry a sol# of just digits in the
record. This is correct and load-bearing.

THE BUG

`validate_against_requirements` (Phase 2 email-as-contract) reads the
buyer email's solicitation_number verbatim. The extractor sees the
raw `PREQ 10847262` from the email body and writes that to the
requirements_json blob. The check at line ~793:

    req_sol = (reqs.get("solicitation_number") or "").strip()
    rfq_sol = (rfq_data.get("solicitation_number") or "").strip()
    if req_sol and rfq_sol and req_sol != rfq_sol:
        _add_gap("solicitation_mismatch", ..., "critical")

Compared `"PREQ 10847262"` against `"10847262"` and flagged a critical
mismatch. Mike sees: "Email says solicitation PREQ 10847262 but RFQ
is set to 10847262 — verify before sending".

That's a false positive: AV-1 did exactly what it was supposed to do;
the audit is reading two different stages of the same value.

THE FIX

New `_normalize_solicitation()` helper applies the same prefix-strip
to both sides before comparing. Identical input now resolves to
identical bare-sol# strings; the mismatch banner only fires on a
genuine numeric mismatch.

NORMALIZATION RULES (deliberately narrow)

  - "PREQ " or "PREQ-" prefix (case-insensitive) → strip
  - Leading/trailing whitespace → strip
  - Everything else preserved verbatim (sol#s are opaque identifiers,
    do NOT lowercase or otherwise mutate)

We do NOT add the broader "STD-", "RFQ-", or similar prefixes
speculatively — only the patterns observed in production. The
single-source-of-truth is `form_field_extractor`'s sol# strip: if
a new prefix is added there, mirror it here.

WHAT THIS TEST PINS
  - `PREQ 10847262` (email) vs `10847262` (RFQ) → no mismatch
  - `PREQ-10847262` (hyphen variant) → no mismatch
  - `preq 10847262` (case-insensitive) → no mismatch
  - `10847262` vs `10847262` → no mismatch (no-op normalization)
  - `10847262` vs `10847263` → real mismatch still detected
  - Empty inputs → no spurious mismatch
  - Source-grep: PR-AV-AC3 marker + _normalize_solicitation present
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run(req_sol, rfq_sol):
    """Convenience: build the requirements + rfq dicts and call
    validate_against_requirements with no other context."""
    from src.forms.form_qa import validate_against_requirements
    reqs = {"solicitation_number": req_sol, "confidence": 0.9}
    rfq = {"solicitation_number": rfq_sol}
    return validate_against_requirements(
        generated_files=[], requirements_json=json.dumps(reqs),
        rfq_data=rfq, config={},
    )


def _has_sol_mismatch(result):
    return any(g.get("type") == "solicitation_mismatch" for g in result["gaps"])


# ── Tests ───────────────────────────────────────────────────────────


def test_preq_prefix_normalized_no_mismatch():
    """The flagged 5/15 case: email says `PREQ 10847262`, RFQ has
    `10847262`. After PR-AV-AC3 these compare equal — no mismatch
    gap reported.
    """
    result = _run("PREQ 10847262", "10847262")
    assert not _has_sol_mismatch(result), (
        f"PREQ-prefixed email should equal bare sol# after normalize; "
        f"gaps={result['gaps']}"
    )


def test_preq_hyphen_variant_normalized():
    """Some buyers write `PREQ-10847262` instead of `PREQ 10847262`."""
    result = _run("PREQ-10847262", "10847262")
    assert not _has_sol_mismatch(result), (
        f"PREQ-hyphen variant should also normalize; gaps={result['gaps']}"
    )


def test_preq_case_insensitive():
    """Don't surprise the operator when the buyer writes lowercase."""
    result = _run("preq 10847262", "10847262")
    assert not _has_sol_mismatch(result), (
        f"lowercase preq should normalize; gaps={result['gaps']}"
    )


def test_both_bare_no_op_normalization():
    """No-op: bare sol# on both sides still matches."""
    result = _run("10847262", "10847262")
    assert not _has_sol_mismatch(result)


def test_real_mismatch_still_detected():
    """Defensive: AC3 doesn't hide an actual numeric mismatch."""
    result = _run("PREQ 10847262", "10847263")
    assert _has_sol_mismatch(result), (
        f"genuine sol# mismatch must still fire; gaps={result['gaps']}"
    )


def test_bare_vs_bare_real_mismatch_detected():
    """Defensive: no-prefix case still fires when the numbers differ."""
    result = _run("10847262", "99999999")
    assert _has_sol_mismatch(result)


def test_empty_sides_no_spurious_mismatch():
    """Empty inputs on either side must not trip the mismatch banner."""
    assert not _has_sol_mismatch(_run("", "10847262"))
    assert not _has_sol_mismatch(_run("10847262", ""))
    assert not _has_sol_mismatch(_run("", ""))


def test_helper_directly_strips_preq():
    """Pin the helper contract independently of the call site."""
    from src.forms.form_qa import _normalize_solicitation
    assert _normalize_solicitation("PREQ 10847262") == "10847262"
    assert _normalize_solicitation("PREQ-10847262") == "10847262"
    assert _normalize_solicitation("preq 10847262") == "10847262"
    assert _normalize_solicitation("  PREQ 10847262  ") == "10847262"
    # Don't over-strip
    assert _normalize_solicitation("10847262") == "10847262"
    assert _normalize_solicitation("STD-123-456") == "STD-123-456"
    assert _normalize_solicitation("") == ""
    assert _normalize_solicitation(None) == ""


def test_source_grep_ac3_marker_present():
    """Lock the normalize call into validate_against_requirements so
    future refactors don't silently drop the prefix-strip.
    """
    target = REPO_ROOT / "src" / "forms" / "form_qa.py"
    src = target.read_text(encoding="utf-8")
    assert "PR-AV-AC3" in src, "PR-AV-AC3 marker must remain in form_qa.py"
    assert "def _normalize_solicitation" in src, (
        "_normalize_solicitation helper must be defined in form_qa.py"
    )
    assert "_normalize_solicitation((reqs.get" in src, (
        "validate_against_requirements must apply _normalize_solicitation "
        "to the requirements-side sol#"
    )
    assert "_normalize_solicitation((rfq_data.get" in src, (
        "validate_against_requirements must apply _normalize_solicitation "
        "to the rfq-side sol#"
    )
