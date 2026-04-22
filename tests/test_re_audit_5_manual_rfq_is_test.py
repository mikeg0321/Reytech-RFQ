"""RE-AUDIT-5 regression guard.

`api_rfq_create_manual` in routes_rfq.py built the saved RFQ dict with no
`is_test` key at all. Every downstream filter
(`not q.get("is_test")`, quoting-health funnel, oracle calibration,
manager_agent, qa_agent) treated these rows as live — which is correct for
real RFQs but gave QA/dev no way to seed an opt-out test RFQ through this
endpoint. PC convert-to-rfq sets is_test explicitly; this path must too.

Fix: derive `is_test` from the caller payload (wins) or a `TEST-` prefix on
the solicitation number, and persist it on the saved rfq dict.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_RFQ = (
    Path(__file__).resolve().parents[1]
    / "src" / "api" / "modules" / "routes_rfq.py"
)


def _body() -> str:
    src = ROUTES_RFQ.read_text(encoding="utf-8")
    m = re.search(
        r"def api_rfq_create_manual\(\)[\s\S]*?(?=\n@bp\.route|\ndef [a-zA-Z_]|\Z)",
        src,
    )
    assert m, "api_rfq_create_manual body not located"
    return m.group(0)


def test_manual_create_persists_is_test_key():
    """RFQ dict built by api_rfq_create_manual must include an is_test key."""
    body = _body()
    assert re.search(r'["\']is_test["\']\s*:', body), (
        "RE-AUDIT-5 regression: api_rfq_create_manual is building an RFQ "
        "dict without an `is_test` field. Downstream filters treat missing "
        "is_test as live — test-seeded rows cannot opt out."
    )


def test_manual_create_honors_caller_is_test_flag():
    """is_test must be derivable from the JSON body (caller wins)."""
    body = _body()
    assert re.search(r'data\.get\(\s*["\']is_test["\']', body), (
        "RE-AUDIT-5 regression: api_rfq_create_manual must read `is_test` "
        "from the request body so QA/dev can seed test RFQs."
    )


def test_manual_create_flags_test_solicitation_prefix():
    """A solicitation number starting with `TEST` should force is_test=True."""
    body = _body()
    # Accept upper() or lower() comparison against a TEST prefix.
    assert re.search(r'sol\.(?:upper|lower)\(\)\.startswith\(\s*["\']TEST', body, re.IGNORECASE) \
        or re.search(r'startswith\(\s*["\']TEST', body, re.IGNORECASE), (
        "RE-AUDIT-5 regression: api_rfq_create_manual should auto-flag "
        "is_test when the solicitation number carries a TEST- prefix."
    )
