"""Shared helper: normalize legacy solicitation numbers.

Two callers (`translator.py` and `ingest.py`) used to duplicate the
PREQ-strip loop; that duplication was the kind of substrate drift the
project was built to prevent — fix one place, miss the other, ship
inconsistent solicitation IDs to the agency. This module is the
single source of truth.

The strip pattern was widened 2026-05-15 (per reviewer feedback) from
the original three literal prefixes ("PREQ ", "PREQ-", "PREQ") to a
case-insensitive regex covering every variant seen in real RFQ
traffic so far:

  - "PREQ 10847262"       → "10847262"
  - "PREQ-10847262"       → "10847262"
  - "PREQ10847262"        → "10847262"      (compact form)
  - "PREQ:10847262"       → "10847262"
  - "preq  10847262"      → "10847262"      (lowercase + double space)
  - "PRE-Q 10847262"      → "10847262"      (dash in middle, seen once)

Anything not matching the pattern passes through unchanged. The
function never raises — empty / None input returns the empty string.
"""
from __future__ import annotations

import re

# Matches `PREQ` or `PRE-Q` (any case) followed by optional separator
# (space, hyphen, colon, period) and the digits. Anchored to start
# so we don't accidentally strip a `PREQ` that appears mid-string.
_PREQ_PREFIX_RE = re.compile(
    r"""^
    p\s*r\s*e\s*-?\s*q     # "PREQ" with optional whitespace + optional hyphen
    [\s\-:.]*              # separator: space/hyphen/colon/period (any count)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def strip_solicitation_prefix(raw: str | None) -> str:
    """Return solicitation number with PREQ-style prefix stripped.

    Pure function. Trust-boundary friendly: handles None, whitespace,
    case, separator variants. Never raises.

    >>> strip_solicitation_prefix("PREQ 10847262")
    '10847262'
    >>> strip_solicitation_prefix("PREQ-10847262")
    '10847262'
    >>> strip_solicitation_prefix("PREQ10847262")
    '10847262'
    >>> strip_solicitation_prefix("preq:10847262")
    '10847262'
    >>> strip_solicitation_prefix("10846581")
    '10846581'
    >>> strip_solicitation_prefix(None)
    ''
    >>> strip_solicitation_prefix("  PREQ  10847262  ")
    '10847262'
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    return _PREQ_PREFIX_RE.sub("", s).strip()
