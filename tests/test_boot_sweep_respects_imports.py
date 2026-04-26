"""Regression test for the is_test boot-sweep exemption (PR #570).

The boot fix at db.py:_check_and_repair_schema marks any quote whose
quote_number doesn't match ^R26Q\\d+$ as is_test=1. Without the
exemption added in PR #570, this re-flagged 464 of 503 prod rows on
every deploy because QuoteWerks DocNos like '23-0003' fail the regex.

Pin the exemption rule so a future refactor can't silently drop it.
"""

import re


_PATCH_FILE = "src/core/db.py"


def _read_patch():
    with open(_PATCH_FILE, "r", encoding="utf-8") as f:
        return f.read()


class TestBootSweepExemption:
    def test_quotewerks_marker_in_exemption_check(self):
        body = _read_patch()
        # Both stamps must be checked
        assert "'QuoteWerks:'" in body or '"QuoteWerks:"' in body, (
            "Boot sweep must exempt QuoteWerks-imported rows by "
            "checking for 'QuoteWerks:' in status_notes"
        )
        assert "'SCPRS-verify'" in body or '"SCPRS-verify"' in body, (
            "Boot sweep must exempt SCPRS-verified rows by checking "
            "for 'SCPRS-verify' in status_notes"
        )

    def test_real_quote_pattern_preserved(self):
        body = _read_patch()
        assert "r'^R26Q\\d+$'" in body, (
            "The R26Q### pattern that distinguishes Reytech-app quotes "
            "from test fixtures must still be present"
        )

    def test_status_notes_pulled_from_query(self):
        body = _read_patch()
        # The exemption check needs status_notes from the query, so the
        # SELECT must include it
        assert "status_notes" in body, (
            "Boot sweep must SELECT status_notes so the exemption can "
            "check for importer markers"
        )
