"""Plan §4.4 — Historical replay (resolution slice).

Complements the pricing-oracle replay in `test_historical_replay.py`
by replaying the **agency-resolution** layer of the pipeline against
real RFQ message metadata harvested from the buyer corpus on
2026-04-29.

### Why a separate file

`test_historical_replay.py` already pins the pricing path with 30+
realistic items. It doesn't touch resolution. This file pins
resolution. Two thin files beat one fat file when the failure modes
are independent — a pricing-oracle drift and a resolver pattern drift
should fail cleanly to their own owners.

### What this CATCHES

If anyone:
- Removes / changes a pattern in `agency_config.match_patterns`
  (e.g. drops "CDCR.CA.GOV" thinking it's redundant with "CDCR")
- Reorders match precedence (Barstow before generic CalVet, or vice
  versa)
- Breaks the search-text concatenation in `match_agency()`

…CI fails on the affected sample with a message pointing at the
specific historical RFQ that broke and the pattern it used to match.

### How the fixture was built

`scripts/harvest_buyer_corpus.py` walked Gmail history 2024-04 →
2026-04 and tagged every message with an `agency_key`. The fixture
`tests/fixtures/historical_replay_samples.json` is a small slice of
those tags — awarded-thread inbound RFQs with preserved `from` +
`subject` + the resolver tag the corpus assigned at harvest time.
The 3.4 GB corpus itself is NOT in the repo; the fixture is the only
runtime dependency.

To extend coverage, append to the fixture JSON. Each entry becomes a
parameterized CI assertion automatically.
"""
from __future__ import annotations

import json
import os

import pytest

_FIX_PATH = os.path.join(
    os.path.dirname(__file__),
    "fixtures",
    "historical_replay_samples.json",
)


def _load_samples():
    if not os.path.exists(_FIX_PATH):
        return []
    with open(_FIX_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


_SAMPLES = _load_samples()


@pytest.fixture(scope="module")
def samples():
    if not _SAMPLES:
        pytest.skip(f"replay fixture missing: {_FIX_PATH}")
    return _SAMPLES


class TestHistoricalResolutionReplay:
    """For real awarded-thread RFQs from the corpus, production
    `match_agency()` must continue to resolve to the same `agency_key`
    the corpus tagged at harvest time."""

    @pytest.mark.parametrize(
        "sample",
        _SAMPLES,
        ids=lambda s: f"{s['expected_agency_key']}-{s['msg_id'][:10]}",
    )
    def test_replay_matches_corpus_tag(self, sample):
        from src.core.agency_config import match_agency

        rfq_data = {
            "email_sender": sample["from"],
            "email_subject": sample["subject"],
            # Other resolver-input fields blank — keeps the test scoped
            # to the from/subject signal that's universally available
            # in the corpus index. Adding institution / ship_to / etc.
            # would broaden what the resolver "sees", which would hide
            # regressions in the from/subject path we want pinned.
            "agency_name": "",
            "agency": "",
            "institution": "",
            "delivery_location": "",
            "ship_to": "",
            "solicitation_number": "",
            "requestor_email": "",
        }
        key, cfg = match_agency(rfq_data)
        assert key == sample["expected_agency_key"], (
            f"Resolver regression on real-world historical RFQ "
            f"(msg {sample['msg_id']}, {sample.get('date', '?')}): "
            f"expected '{sample['expected_agency_key']}', got '{key}'. "
            f"From: {sample['from']!r}; Subject: {sample['subject']!r}. "
            f"Matched-by: {cfg.get('matched_by', '<unset>')}. "
            f"If the resolver was intentionally narrowed, restore the "
            f"pattern OR remove this sample from the fixture — but "
            f"understand which historical RFQs you're choosing to "
            f"no-longer-resolve."
        )


class TestReplayFixtureShape:
    """Sanity-check the fixture itself so test failures are about the
    code under test, not malformed test data."""

    def test_fixture_has_at_least_one_sample_per_major_agency(
        self, samples,
    ):
        keys = {s["expected_agency_key"] for s in samples}
        assert "cchcs" in keys, (
            "fixture must include ≥1 CCHCS sample — CCHCS is the largest-"
            "volume buyer; not pinning it leaves a major regression hole."
        )
        assert "calvet" in keys, (
            "fixture must include ≥1 CalVet sample — CalVet uses a "
            "different pattern set than CCHCS; resolver bugs that affect "
            "one rarely affect the other."
        )

    def test_fixture_has_required_fields(self, samples):
        required = ("msg_id", "expected_agency_key", "from", "subject")
        for s in samples:
            missing = [k for k in required if k not in s]
            assert not missing, (
                f"sample {s.get('msg_id', '?')} missing required keys: "
                f"{missing}"
            )
            assert s["from"], (
                f"sample {s['msg_id']}: empty 'from' would make the test "
                f"trivially pass on any resolver"
            )

    def test_fallback_sample_proves_fallback_path(self, samples):
        """At least one sample should be a non-CA-buyer (e.g. BidNet
        aggregator, generic noreply) to verify the resolver falls back
        to 'other' on legitimately unrecognized senders. Without this,
        a resolver bug that always-matched-first-config would still
        pass the per-agency tests."""
        fallbacks = [
            s for s in samples if s["expected_agency_key"] == "other"
        ]
        assert fallbacks, (
            "fixture should include ≥1 expected-'other' sample to verify "
            "fallback behavior on non-CA-buyer mail (aggregators, vendor "
            "outreach, etc.). Without this, a 'always match first config' "
            "resolver bug would pass the per-agency parametrize cases."
        )
