"""§0 LAW 6 forcing function — "READ THE WHOLE CONTRACT".

The supplemental AMS 701B "Purchase Order Distribution List" must NOT be parsed
as line items. Coleman 10842771: its 21 per-facility rows (LINE/QTY/UOM/
DESCRIPTION columns) were Vision-parsed as 21 phantom line items by the multi-
attachment union, and the Facility/Address/Zip columns were discarded — yielding
a single-facility ship-to and single-jurisdiction tax on a 21-jurisdiction
order.

These pins guard:
  1. The distribution-list parser (detect + structure, incl. facility/address/zip).
  2. The union SKIPS a distribution-list sibling — it never reaches Vision, so it
     never mints phantom line items. (The core regression guard.)
  3. A primary form's cross-reference to a distribution list is detectable
     ("see attached distribution list").

The on-record disposition manifest + parsed distribution_list are integration-
covered by the Coleman canary (test_coleman_10842771_canary.py).
"""
import json
import os

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "coleman_10842771")
P701B = os.path.join(FIX, "ams701b_distribution_list.pdf")
P704B = os.path.join(FIX, "704b_golden_pre_pr1170.pdf")

from src.forms.distribution_list_parser import (  # noqa: E402
    is_distribution_list,
    parse_distribution_list,
    text_references_distribution,
)


class TestDistributionListParser:
    def test_detects_701b(self):
        assert is_distribution_list(P701B) is True

    def test_704b_not_misdetected(self):
        # The real line-item quote form must NOT be treated as a distribution
        # list (false-positive guard — else we'd drop the actual items).
        assert is_distribution_list(P704B) is False

    def test_parse_yields_21_facility_rows_with_addresses(self):
        d = parse_distribution_list(P701B)
        assert d["row_count"] == 21
        assert d["sku_totals"] == {"8700-0893-01": 19, "LF03699": 2}
        assert len(d["distinct_facilities"]) == 21
        for r in d["rows"]:
            assert r["facility_code"], "every row must carry a facility code"
            assert r["facility_address"], "every row must carry an address"
            assert r["zip"], "every row must carry a zip (tax jurisdiction)"

    def test_matches_committed_ground_truth(self):
        gt = json.load(open(
            os.path.join(FIX, "ams701b_distribution_list.parsed.json"),
            encoding="utf-8",
        ))
        d = parse_distribution_list(P701B)
        assert d["row_count"] == gt["row_count"]
        assert d["distinct_facilities"] == gt["distinct_facilities"]
        assert d["sku_totals"] == gt["sku_totals"]

    def test_cross_reference_cue_detected(self):
        assert text_references_distribution(
            "***PLEASE SEE ATTACHED DISTRIBUTION LIST"
        ) is True
        assert text_references_distribution("just normal line items") is False


class TestUnionSkipsDistributionList:
    def test_distribution_list_sibling_never_reaches_vision(self, monkeypatch):
        """The regression guard: a distribution-list sibling must be excluded
        from the item union BEFORE any Vision call — so it can never mint
        phantom line items."""
        from src.core import ingest_pipeline as ip
        from src.core.request_classifier import SHAPE_GENERIC_RFQ_PDF

        # Vision "available" so we pass the early-return gate and reach the
        # candidate filter where the skip lives.
        import src.forms.vision_parser as vp
        monkeypatch.setattr(vp, "is_available", lambda: True)

        # If the 701B ever reaches Vision, record it — that's the bug.
        vision_calls = []

        def _spy(path):
            vision_calls.append(os.path.basename(path))
            return []

        monkeypatch.setattr(ip, "_vision_primary_extract", _spy)

        # Force the 701B's classified shape to GENERIC_RFQ_PDF so it passes
        # the shape gate — the distribution-list skip (which runs AFTER the
        # gate) is the behavior under test. Without the skip, the 701B would
        # be a candidate and get Vision-parsed.
        class _Classification:
            _per_file_info = {
                os.path.basename(P701B): {
                    "shape": SHAPE_GENERIC_RFQ_PDF,
                    "info": {"pricing_page_score": 1},
                },
            }

        extra = ip._multi_attachment_vision_union(
            primary_path=P704B,
            all_files=[P704B, P701B],
            classification=_Classification(),
            primary_items=[],
        )

        assert extra == [], "a distribution-list sibling contributed line items"
        assert os.path.basename(P701B) not in vision_calls, (
            "the AMS 701B distribution list was Vision-parsed as items — the "
            "LAW 6 union skip regressed (Coleman 10842771 phantom-items bug)"
        )
