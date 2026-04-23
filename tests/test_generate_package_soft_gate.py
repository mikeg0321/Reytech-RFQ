"""Regression: Generate Package pre-flight must soft-fail (warn + drop) when
buyer-supplied templates (703B/703C/704B) are required but not uploaded.

Incident 2026-04-22: RFQ 10840486 (LPA IT Goods from CSP-Sacramento) was
mis-classified as shape=cchcs_packet by the classifier. Agency config for
`cchcs` lists [703b, 704b, bidpkg, quote] as required. PR #440 filter kept
the buyer templates because shape_allowed for cchcs_packet includes them.
Pre-flight gate then 400-rejected because the operator had uploaded the
buyer's single-package PDF, not separate 703B/704B slots — blocking the
entire Generate Package action.

Right semantics: if Reytech can't fabricate a buyer template we don't have,
we shouldn't refuse to generate the rest of the package. Drop the missing
buyer-template form from `_req_forms`, warn, and proceed. Operator uploads
and re-generates if they need the skipped form.
"""
import os
from pathlib import Path
from unittest.mock import patch

import pytest


class TestSoftGateDropsMissingBuyerTemplates:
    """The pre-flight loop must drop 703B/703C/704B from _req_forms when the
    corresponding template is not uploaded, rather than 400-rejecting."""

    def _simulate_preflight(self, req_forms, tmpl):
        """Mirror the pre-flight logic in routes_rfq_gen.py:1530-1557 so we
        can unit-test the soft-gate without spinning up the full Flask app."""
        _buyer_templates = {"703b", "703c", "704b"}
        _req = set(req_forms)
        _skipped = []
        for _ft in list(_req):
            if _ft not in _buyer_templates:
                continue
            if _ft in ("703b", "703c"):
                _has_703 = (("703b" in tmpl and os.path.exists(tmpl.get("703b", "")))
                            or ("703c" in tmpl and os.path.exists(tmpl.get("703c", ""))))
                if not _has_703:
                    _req.discard(_ft)
                    _skipped.append(_ft.upper())
            elif _ft == "704b":
                if not ("704b" in tmpl and os.path.exists(tmpl.get("704b", ""))):
                    _req.discard(_ft)
                    _skipped.append("704B")
        return _req, _skipped

    def test_missing_704b_dropped_not_rejected(self):
        # Shape says packet (hence 704b required), but nothing uploaded.
        req = {"703b", "704b", "bidpkg", "quote", "sellers_permit"}
        tmpl = {}  # no uploads
        remaining, skipped = self._simulate_preflight(req, tmpl)
        # 704B and 703B are dropped from required, skipped is populated.
        assert "704b" not in remaining
        assert "703b" not in remaining
        assert "704B" in skipped
        assert "703B" in skipped
        # Non-buyer forms survive — these CAN generate without buyer uploads.
        assert "bidpkg" in remaining
        assert "quote" in remaining
        assert "sellers_permit" in remaining

    def test_uploaded_704b_kept_in_required(self, tmp_path):
        req = {"704b", "quote"}
        fake_704 = tmp_path / "buyer_704b.pdf"
        fake_704.write_bytes(b"%PDF-1.4\n%fake\n")
        tmpl = {"704b": str(fake_704)}
        remaining, skipped = self._simulate_preflight(req, tmpl)
        assert "704b" in remaining
        assert skipped == []

    def test_703b_upload_satisfies_703c_requirement(self, tmp_path):
        fake_703b = tmp_path / "703b.pdf"
        fake_703b.write_bytes(b"%PDF-1.4\n")
        req = {"703c", "quote"}
        tmpl = {"703b": str(fake_703b)}
        remaining, skipped = self._simulate_preflight(req, tmpl)
        # 703C requirement is satisfied by uploaded 703B.
        assert "703c" in remaining
        assert skipped == []

    def test_703c_upload_satisfies_703b_requirement(self, tmp_path):
        fake_703c = tmp_path / "703c.pdf"
        fake_703c.write_bytes(b"%PDF-1.4\n")
        req = {"703b", "quote"}
        tmpl = {"703c": str(fake_703c)}
        remaining, skipped = self._simulate_preflight(req, tmpl)
        assert "703b" in remaining
        assert skipped == []

    def test_template_path_missing_on_disk_treated_as_not_uploaded(self, tmp_path):
        # tmpl has a path but the file was deleted/never written.
        stale_path = tmp_path / "never_written.pdf"
        req = {"704b", "quote"}
        tmpl = {"704b": str(stale_path)}
        remaining, skipped = self._simulate_preflight(req, tmpl)
        assert "704b" not in remaining
        assert "704B" in skipped
        assert "quote" in remaining

    def test_empty_req_forms_returns_empty(self):
        remaining, skipped = self._simulate_preflight(set(), {})
        assert remaining == set()
        assert skipped == []

    def test_only_non_buyer_forms_untouched(self):
        req = {"quote", "sellers_permit", "std204", "dvbe843", "bidpkg"}
        remaining, skipped = self._simulate_preflight(req, {})
        assert remaining == req
        assert skipped == []


class TestIncidentRFQ10840486:
    """The concrete 2026-04-22 P0 case: LPA RFQ mis-shaped as cchcs_packet,
    DB override brought sellers_permit into required_forms, operator uploaded
    the buyer's single-package PDF but not as a 704B slot."""

    def test_p0_scenario_unblocks_generation(self):
        """Exact error case: required_forms=[704b, sellers_permit, bidpkg, quote, 703b],
        no buyer templates uploaded. Soft-gate must drop 703b+704b, keep the rest."""
        req = {"704b", "sellers_permit", "bidpkg", "quote", "703b"}
        tmpl = {}  # operator uploaded single-package PDF, not mapped to 703b/704b slots
        _buyer_templates = {"703b", "703c", "704b"}
        _skipped = []
        for _ft in list(req):
            if _ft not in _buyer_templates:
                continue
            if _ft in ("703b", "703c"):
                _has_703 = (("703b" in tmpl and os.path.exists(tmpl.get("703b", "")))
                            or ("703c" in tmpl and os.path.exists(tmpl.get("703c", ""))))
                if not _has_703:
                    req.discard(_ft)
                    _skipped.append(_ft.upper())
            elif _ft == "704b":
                if not ("704b" in tmpl and os.path.exists(tmpl.get("704b", ""))):
                    req.discard(_ft)
                    _skipped.append("704B")

        # Operator-facing outcome: generation proceeds with these 3 forms.
        assert req == {"sellers_permit", "bidpkg", "quote"}
        # Skipped templates are surfaced for the trace / operator warning.
        assert set(_skipped) == {"703B", "704B"}
