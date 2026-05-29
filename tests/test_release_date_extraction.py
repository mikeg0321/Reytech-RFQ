"""Release/Issue Date extraction — incident sol 10847187 (2026-05-29).

The 703B "Release Date" shipped blank because no extractor captured it; the
buyer's header carried it ("Release Date: 5/27/26") but neither Vision nor the
requirement extractor read it. `_extract_release_date` closes the capture gap;
it must NEVER steal the due date.
"""
from __future__ import annotations

from src.agents.requirement_extractor import (
    _extract_release_date,
    _extract_due_date,
    _extract_with_regex,
)

_HEADER = "Solicitation Number: 10847187   Release Date: 5/27/26   Due Date: 5/29/26"


def test_numeric_release_date():
    assert _extract_release_date(_HEADER) == "2026-05-27"


def test_does_not_steal_due_date():
    # The bug-adjacent risk: release-date capture must not grab the due date.
    assert _extract_due_date(_HEADER) == "2026-05-29"
    assert _extract_release_date(_HEADER) != _extract_due_date(_HEADER)


def test_long_format():
    assert _extract_release_date("Release Date: May 27, 2026") == "2026-05-27"


def test_issue_date_alias():
    assert _extract_release_date("Issue Date: 02/09/2026") == "2026-02-09"


def test_absent_release_date_returns_empty():
    assert _extract_release_date("Due Date: 5/29/26") == ""


def test_regex_extractor_populates_release_date():
    reqs = _extract_with_regex(_HEADER, [])
    assert reqs.release_date == "2026-05-27"
    assert reqs.due_date == "2026-05-29"
