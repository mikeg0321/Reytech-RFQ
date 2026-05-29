"""Release/Issue Date extraction — incident sol 10847187 (2026-05-29).

The 703B "Release Date" shipped blank because no extractor captured it; the
buyer's header carried it ("Release Date: 5/27/26") but neither Vision nor the
requirement extractor read it. `_extract_release_date` closes the capture gap;
it must NEVER steal the due date.
"""
from __future__ import annotations

import json
import os

from src.agents.requirement_extractor import (
    RFQRequirements,
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


# ── Threading: a captured release_date must LAND on the RFQ record ──────────
# The other half of the gap. #1207 captured release_date at ingest but the
# consumer sites threaded only due_date, so rfq_data["release_date"] stayed
# unset and the 703B "Release Date" field rendered blank. The 703B filler
# reading rfq_data["release_date"] is already covered by
# test_703b_prefix_detect.py::test_fill_703b_reads_top_buyer_fields_*; these
# prove the value actually reaches the persisted record.

def _seed_rfq_json(temp_data_dir, rec):
    with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as fh:
        json.dump({rec["id"]: rec}, fh)


def test_release_date_threads_onto_rfq_record(auth_client, temp_data_dir, monkeypatch):
    rid = "rfq-reldate-thread"
    _seed_rfq_json(temp_data_dir, {
        "id": rid, "status": "new", "source": "email",
        "email_subject": "RFQ 10847187 IV pole mount",
        "body_text": "Solicitation 10847187. Release Date: 5/27/26. Due Date: 5/29/26.",
        "due_date": "TBD",  # no release_date key
    })

    def _fake_extract(body, subject, attachments):
        return RFQRequirements(forms_required=["AMS 703B"], due_date="2026-05-29",
                               release_date="2026-05-27", extraction_method="regex",
                               confidence=0.6)
    monkeypatch.setattr(
        "src.agents.requirement_extractor.extract_requirements", _fake_extract)

    resp = auth_client.post(f"/api/rfq/{rid}/re-extract-requirements")
    assert resp.status_code == 200, resp.data
    assert resp.get_json().get("ok") is True

    from src.api.dashboard import load_rfqs
    saved = load_rfqs().get(rid, {})
    assert saved.get("release_date") == "2026-05-27", (
        f"release_date was not threaded onto the record: {saved.get('release_date')!r}")


def test_release_date_thread_does_not_clobber_existing(auth_client, temp_data_dir, monkeypatch):
    rid = "rfq-reldate-noclobber"
    _seed_rfq_json(temp_data_dir, {
        "id": rid, "status": "new", "source": "email",
        "email_subject": "RFQ x", "body_text": "Release Date: 5/27/26.",
        "due_date": "TBD", "release_date": "2026-01-01",  # operator-set / earlier
    })

    def _fake_extract(body, subject, attachments):
        return RFQRequirements(forms_required=["AMS 703B"], release_date="2026-05-27",
                               extraction_method="regex", confidence=0.6)
    monkeypatch.setattr(
        "src.agents.requirement_extractor.extract_requirements", _fake_extract)

    resp = auth_client.post(f"/api/rfq/{rid}/re-extract-requirements")
    assert resp.status_code == 200, resp.data

    from src.api.dashboard import load_rfqs
    assert load_rfqs().get(rid, {}).get("release_date") == "2026-01-01", (
        "existing release_date must not be clobbered by re-extraction")
