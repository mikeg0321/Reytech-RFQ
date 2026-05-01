"""Regression: 703B fill must not run for agencies that don't require it,
even when a stale `tmpl["703b"]` carries over from a prior CCHCS workflow.

Incident 2026-05-01 (rfq_7813c4e1, agency=Cal Vet / DVA):
  Generated package contained `WORKSHEET_703B_Reytech.pdf` — an empty
  3-page PDF with zero AcroForm fields. CalVet's `required_forms` does
  NOT include 703B, but the dispatcher condition was

      if _include("703b") or _include("703c") or "703c" in tmpl or "703b" in tmpl:

  so a stray buyer-template upload (or an attachment leftover from a
  prior CCHCS draft) triggered an attempted fill against a non-703B
  template, producing the empty PDF. Form QA then flunked 14 fields on
  the empty 703B (Business Name, Address, FEIN, Solicitation Number,
  Due Date, BidExpirationDate, ...) and the package was marked
  INCOMPLETE.

The fix: drop the `or "703b" in tmpl` / `or "703c" in tmpl` arms.
Buyer-uploaded templates are not authorization to write a form the
agency doesn't ship — that authorization comes from agency_config.

These tests grep the route file directly because the dispatcher lives
inside a 1000-line route handler that requires the full Flask app
fixture to exercise end-to-end. The condition is small enough that a
text-level guard is cheaper than reproducing the harness, and it
locks the regression at the exact site that caused the 2026-05-01
incident.
"""
from __future__ import annotations

import os
import re

ROUTE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src", "api", "modules", "routes_rfq_gen.py",
)


def _read_route() -> str:
    with open(ROUTE_FILE, "r", encoding="utf-8") as fh:
        return fh.read()


def test_703_dispatch_does_not_fire_on_stale_template_alone():
    """The dispatcher condition must include only the agency-required
    arms (`_include("703b")` / `_include("703c")`) and must NOT fall
    back to the buyer-template presence (`"703b" in tmpl`)."""
    src = _read_route()
    # Find the dispatcher line. Pattern: `if _include("703b") or ...`
    m = re.search(
        r'if _include\("703b"\) or _include\("703c"\)([^\n:]*):',
        src,
    )
    assert m, (
        "Could not locate the 703B dispatcher condition in "
        "routes_rfq_gen.py — search pattern out of date or the line was "
        "refactored away."
    )
    tail = m.group(1)
    assert '"703b" in tmpl' not in tail, (
        f'703B dispatcher must not fire on stale `tmpl["703b"]` alone. '
        f"Current tail of the condition: {tail!r}. The 2026-05-01 "
        "incident produced an empty 3-page 703B PDF on a CalVet RFQ "
        "via exactly this fallthrough."
    )
    assert '"703c" in tmpl' not in tail, (
        f'703C dispatcher must not fire on stale `tmpl["703c"]` alone. '
        f"Tail: {tail!r}."
    )


def test_703_dispatcher_carries_a_why_comment():
    """Inline guidance keeps the next operator from re-adding the
    fallthrough by 'fixing' the missing-template warning. The comment
    must reference the agency-gate intent so a grep for `agency` finds
    the rationale."""
    src = _read_route()
    # Locate the comment block above the condition and check it names
    # the agency-gate concern.
    idx = src.find('if _include("703b") or _include("703c"):')
    assert idx > 0, "703B dispatcher condition not found"
    preamble = src[max(0, idx - 800):idx]
    assert "agency" in preamble.lower(), (
        "Expected a 'why' comment naming the agency-gate intent above the "
        "703B dispatcher. Without it, a future operator who sees a CalVet "
        "RFQ skip 703B fill will likely re-add the buyer-template arm."
    )
