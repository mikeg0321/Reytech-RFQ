"""Tests for the shared PREQ-prefix strip helper.

This helper is the single source of truth used by both
src/spine_bridge/translator.py and src/spine_bridge/ingest.py.
If the two callers ever drift on this rule, agency-bound
solicitation IDs diverge silently — exactly the substrate failure
class the project was built to prevent.

Reviewer's 2026-05-15 feedback: widen the pattern from three
literal prefixes to a case + separator + spacing-tolerant regex.
"""
from __future__ import annotations

import pytest

from src.spine_bridge._solicitation import strip_solicitation_prefix


@pytest.mark.parametrize("raw,expected", [
    # Documented happy path
    ("PREQ 10847262", "10847262"),
    ("PREQ-10847262", "10847262"),
    ("PREQ10847262", "10847262"),
    # Case variants
    ("preq 10847262", "10847262"),
    ("Preq 10847262", "10847262"),
    # Other separators
    ("PREQ:10847262", "10847262"),
    ("PREQ.10847262", "10847262"),
    # Extra whitespace
    ("PREQ  10847262", "10847262"),
    ("  PREQ 10847262  ", "10847262"),
    # PRE-Q variant (seen once in the wild)
    ("PRE-Q 10847262", "10847262"),
    ("PRE-Q-10847262", "10847262"),
    # No prefix → passthrough
    ("10846581", "10846581"),
    ("PREQ", ""),                     # bare prefix → empty
    # Edge cases
    ("", ""),
    (None, ""),
    ("   ", ""),
])
def test_strip_solicitation_prefix(raw, expected):
    assert strip_solicitation_prefix(raw) == expected


def test_strip_does_not_remove_preq_mid_string():
    """Only strip when PREQ is at the START of the string. Anywhere
    else, it's part of the legitimate ID."""
    assert strip_solicitation_prefix("X-PREQ-12345") == "X-PREQ-12345"
    assert strip_solicitation_prefix("FOO PREQ 999") == "FOO PREQ 999"


def test_strip_handles_numeric_input():
    """If the caller passes a bare int (which legacy sometimes does),
    coerce to string first and pass through unchanged."""
    assert strip_solicitation_prefix(10846581) == "10846581"
