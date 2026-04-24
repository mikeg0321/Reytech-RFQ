"""Regression: `_extract_due_date` + `_extract_due_time` cover the
labeled-field patterns common in government RFQ emails.

Mike's 2026-04-23 home queue showed 30+ PCs all stamped with
`due_date_source = "default"` because their email bodies used label
formats ("Due Date: 4/24/2026", "Bid Open Date: …", "Closing 4/24")
that the original verb-prefixed-only regexes missed. PR #492 was a
band-aid (real deadlines sort above defaults); this is the upstream
fix that makes more deadlines BE real.

Coverage:
- Labeled field patterns (Due Date, Date Due, Quote Due, Bid Open
  Date, Closing Date, Submission Deadline, Reply By, Submit By, etc.)
- Numeric (4/24/2026) and long (April 24, 2026) date formats
- Both / and - date separators
- Time extraction (by 2:00 PM, at noon, standalone "5pm PST")
- Negatives — must NOT extract from prose without deadline cues

The original verb-prefixed patterns are also pinned here so adding
labeled patterns can't silently regress the verb path.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.agents.requirement_extractor import (
    _extract_due_date,
    _extract_due_time,
)


# ── Labeled-field patterns (NEW — closes the 16.8h spam loop) ────────


@pytest.mark.parametrize("body,expected", [
    ("Please review the attached RFQ. Due Date: 4/24/2026", "2026-04-24"),
    ("Date Due: 04-24-2026", "2026-04-24"),
    ("Quote Due: April 24, 2026 at 2:00 PM PST", "2026-04-24"),
    ("Bid Open Date: 4/24/26", "2026-04-24"),
    ("Bid Opening Date: 04/24/2026", "2026-04-24"),
    ("Closing Date: 04/24/2026", "2026-04-24"),
    ("Closing 4/24/2026 — please confirm", "2026-04-24"),
    ("Closes 04/24/2026 EOB", "2026-04-24"),
    ("Submission Deadline: April 24, 2026", "2026-04-24"),
    ("Reply By: 4/24/2026", "2026-04-24"),
    ("Submit By: 4/24/2026 by 5:00 PM", "2026-04-24"),
    ("Quotes Due: 4/24/2026", "2026-04-24"),
    ("Bid Due: April 24, 2026", "2026-04-24"),
    ("Response Required: 04/24/2026", "2026-04-24"),
])
def test_labeled_patterns_extract_correctly(body, expected):
    assert _extract_due_date(body) == expected


def test_labeled_pattern_with_weekday_prefix():
    """'Due Date: Thursday April 24, 2026' — weekday before month."""
    assert _extract_due_date("Due Date: Thursday April 24, 2026") == "2026-04-24"


def test_labeled_pattern_with_dash_separator():
    assert _extract_due_date("Quote Due: 4-24-2026") == "2026-04-24"


# ── Verb-prefixed patterns (REGRESSION — original behavior) ──────────


@pytest.mark.parametrize("body,expected", [
    ("Please respond by 4/24/2026", "2026-04-24"),
    ("Quotes due 4/24/2026", "2026-04-24"),
    ("Required no later than April 24, 2026", "2026-04-24"),
    ("Please respond by Thursday April 24, 2026", "2026-04-24"),
    ("Deadline 04/24/26", "2026-04-24"),
    ("Need this before 4/24/2026", "2026-04-24"),
])
def test_verb_prefixed_patterns_still_work(body, expected):
    assert _extract_due_date(body) == expected


# ── Time extraction ──────────────────────────────────────────────────


@pytest.mark.parametrize("body,expected", [
    ("Submit by 2:00 PM PST", "2:00 PM PST"),
    ("Quote due at 5:00 PM", "5:00 PM"),
    ("Before 14:00", "14:00"),
    ("Submit by noon Thursday", "12:00 PM"),
    ("Reply by midnight", "11:59 PM"),
    ("Quote Due: 4/24/2026 by 5pm PST", "5pm PST"),
    ("Standalone 5:00 PM PST appears here", "5:00 PM PST"),
])
def test_time_patterns(body, expected):
    assert _extract_due_time(body) == expected


def test_eob_cob_eod_returns_sentinel():
    for body in ("Submit by end of business",
                 "Reply by close of business Thursday",
                 "Need by EOB",
                 "Quote due COB Friday",
                 "Need this by EOD"):
        assert _extract_due_time(body) == "COB", body


# ── Negatives — must NOT extract ─────────────────────────────────────


@pytest.mark.parametrize("body", [
    "No deadline mentioned in this body",
    "",
    "Item description: Stanley Wrist Strap part 4/24",  # 4/24 is a part frag
    "Reference document dated 04/24/2026",  # "dated" is not a deadline cue
    "Order placed 4/24/2026 for review",  # "placed" not a deadline
])
def test_no_extraction_from_non_deadline_prose(body):
    assert _extract_due_date(body) == ""


# ── Edge cases ───────────────────────────────────────────────────────


def test_empty_input():
    assert _extract_due_date("") == ""
    assert _extract_due_date(None) == ""
    assert _extract_due_time("") == ""
    assert _extract_due_time(None) == ""


def test_first_match_wins():
    """Two deadline-shaped strings — labeled wins over verb if both exist."""
    body = ("Original RFQ shipped by 3/15/2026. "
            "Quote Due: 4/24/2026 — please respond.")
    # Labeled "Quote Due" is most-specific, fires before "by"-prefixed.
    assert _extract_due_date(body) == "2026-04-24"


def test_label_with_optional_colon_and_dash():
    for sep in (":", "-", ""):
        body = f"Due Date{sep} 4/24/2026"
        assert _extract_due_date(body) == "2026-04-24", sep


def test_two_digit_year_normalizes():
    """4/24/26 should parse as 2026, not 1926."""
    assert _extract_due_date("Due Date: 4/24/26") == "2026-04-24"


def test_abbreviated_month_name():
    """`Apr 24, 2026` should parse same as `April 24, 2026`."""
    assert _extract_due_date("Due Date: Apr 24, 2026") == "2026-04-24"
