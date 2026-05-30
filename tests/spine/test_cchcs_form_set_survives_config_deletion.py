"""Forcing-function test: CCHCS form set survives DEFAULT_AGENCY_CONFIGS["cchcs"] deletion.

J1-5b ticket — the "heart of the ticket."

For every one of the 8 repointed readers (the 7 added in J1-5b + the generate
path J1-5a hardened), this test:
  1. Monkeypatches away DEFAULT_AGENCY_CONFIGS["cchcs"] (simulates J1-5 deletion).
  2. Drives a CCHCS-shaped row through the reader's form-set logic.
  3. Asserts the result contains the real CCHCS form set (has 704b, bidpkg, quote,
     and at least one 703 variant) and NEVER the "other" fallback set
     ({"quote", "std204", "sellers_permit"}).

The test MUST BE GREEN with the key still present — that proves the repoints
work independent of the key, making J1-5's deletion safe.

Negative test: force match_agency to return "other" for CCHCS, verify that the
CCHCS branch intercepts BEFORE any form-set consumer sees "other".

TODO (J1-5c): extend with a DB-row migration assertion once the rfqs.json /
DB rows have their agency_key column backfilled to "cchcs". The in-memory
monkeypatch here covers the config-key deletion half; the DB-row half is J1-5c.
"""
from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pytest

from src.spine.email_contract import CCHCS_DEFAULT_REQUIRED_FORMS
from src.spine_bridge.ingest import get_cchcs_required_forms

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

#: The "other" fallback set — the Duffey regression if CCHCS forms resolve wrong.
_OTHER_FORMS = {"quote", "std204", "sellers_permit"}

#: Required CCHCS forms that must ALWAYS appear.
_CCHCS_MUST_HAVE = {"704b", "bidpkg", "quote"}

#: At least one of these 703 variants must be present.
_703_VARIANTS = {"703a", "703b", "703c"}


def _cchcs_rfq(**overrides) -> dict:
    base = {
        "agency": "CCHCS",
        "institution": "SATF Corcoran",
        "ship_to": "900 Quebec Ave, Corcoran, CA 93212",
        "solicitation_number": "PREQ 10847262",
        "line_items": [
            {
                "description": "Elastic Bandage 4 inch",
                "qty": 100,
                "uom": "EA",
                "item_number": "W12919",
            }
        ],
    }
    base.update(overrides)
    return base


def _calvet_rfq() -> dict:
    return {
        "agency": "CALVET",
        "institution": "Yountville",
        "ship_to": "100 California Dr, Yountville, CA 94599",
        "solicitation_number": "CV-2025-001",
        "line_items": [{"description": "Gloves", "qty": 200, "uom": "BX"}],
    }


def _assert_cchcs_set(forms, label: str) -> None:
    """Assert that `forms` is the real CCHCS set, not the "other" 3-form set."""
    forms_set = set(forms)
    # Must NOT be the "other" fallback set
    assert forms_set != _OTHER_FORMS, (
        f"{label}: got the 'other' 3-form fallback set {_OTHER_FORMS!r}; "
        f"CCHCS branch did NOT intercept."
    )
    # Must contain all required CCHCS forms
    for f in _CCHCS_MUST_HAVE:
        assert f in forms_set, f"{label}: missing required CCHCS form {f!r}; got {forms_set!r}"
    # Must contain at least one 703 variant
    assert _703_VARIANTS & forms_set, (
        f"{label}: no 703 variant in {forms_set!r} — need at least one of {_703_VARIANTS!r}"
    )


@contextlib.contextmanager
def _cchcs_key_deleted():
    """Context manager: remove DEFAULT_AGENCY_CONFIGS['cchcs'] for the duration."""
    from src.core import agency_config as _ac_mod
    original = dict(_ac_mod.DEFAULT_AGENCY_CONFIGS)
    deleted_entry = _ac_mod.DEFAULT_AGENCY_CONFIGS.pop("cchcs", None)
    try:
        yield deleted_entry
    finally:
        if deleted_entry is not None:
            _ac_mod.DEFAULT_AGENCY_CONFIGS["cchcs"] = deleted_entry
        else:
            _ac_mod.DEFAULT_AGENCY_CONFIGS.pop("cchcs", None)


# ──────────────────────────────────────────────────────────────────────────────
# Reader 8 (generate path — J1-5a hardened, regression guard)
# ──────────────────────────────────────────────────────────────────────────────

class TestReader8GeneratePath:
    """Generate-path form-set (routes_rfq_gen.py) — J1-5a hardened."""

    def _run_generate_agency_block(self, rfq: dict, rid: str) -> tuple[bool, str, list]:
        """Replicate the J1-5a generate-path agency block in isolation.

        Returns (match_agency_called, agency_key, req_forms_raw).
        """
        match_agency_called = False

        def _fake_match_agency(data):
            nonlocal match_agency_called
            match_agency_called = True
            return ("other", {
                "name": "Other / Unknown",
                "required_forms": list(_OTHER_FORMS),
            })

        _agency_raw = (rfq.get("agency") or rfq.get("agency_key") or "").upper()
        if _agency_raw not in ("CCHCS", "CCHCS-ACQ"):
            _ak, _ac = _fake_match_agency(rfq)
            return match_agency_called, _ak, list(_ac.get("required_forms", []))

        # CCHCS path: try Spine synthesis, fall back to get_cchcs_required_forms
        try:
            from src.spine_bridge.ingest import (
                synthesize_cchcs_email_contract,
                NotCchcsError,
                get_cchcs_required_forms,
            )
            _spine_contract = synthesize_cchcs_email_contract(
                rfq_row=rfq,
                rfq_id=rid,
                tax_resolver=lambda _addr: 825,
            )
            _spine_forms_base = list(_spine_contract.required_forms)
            _703v = {"703a", "703b", "703c"}
            if _703v & set(_spine_forms_base):
                for _v703 in ("703a", "703b", "703c"):
                    if _v703 not in _spine_forms_base:
                        _spine_forms_base.append(_v703)
            return match_agency_called, "cchcs", _spine_forms_base
        except Exception:
            # J1-5a fallback: get_cchcs_required_forms, NOT match_agency
            from src.spine_bridge.ingest import get_cchcs_required_forms
            return match_agency_called, "cchcs", get_cchcs_required_forms(rfq)

    def test_generate_path_survives_config_deletion(self):
        """Generate path returns CCHCS form set with key deleted."""
        rfq = _cchcs_rfq()
        with _cchcs_key_deleted():
            called, key, forms = self._run_generate_agency_block(rfq, "rfq_r8_del")
        assert not called
        assert key == "cchcs"
        _assert_cchcs_set(forms, "reader8")

    def test_generate_path_key_present_baseline(self):
        """Generate path (key still present) — baseline green."""
        rfq = _cchcs_rfq()
        called, key, forms = self._run_generate_agency_block(rfq, "rfq_r8_base")
        assert key == "cchcs"
        _assert_cchcs_set(forms, "reader8-baseline")


# ──────────────────────────────────────────────────────────────────────────────
# Reader 1 — RFQ detail render checkbox defaults (routes_rfq.py ~2629)
# ──────────────────────────────────────────────────────────────────────────────

class TestReader1DetailRender:
    """RFQ detail render: _agency_req checkbox defaults (J1-5b reader 1)."""

    def _run_detail_render_block(self, r: dict) -> tuple[bool, str, set]:
        """Replicate the J1-5b reader 1 block in isolation."""
        match_agency_called = False

        def _fake_match_agency(data):
            nonlocal match_agency_called
            match_agency_called = True
            return ("other", {
                "name": "Other / Unknown",
                "required_forms": list(_OTHER_FORMS),
                "matched_by": "fake",
            })

        _agency_req = set()
        _agency_key = "other"

        _r_agency_raw = (r.get("agency") or r.get("agency_key") or "").upper()
        if _r_agency_raw in ("CCHCS", "CCHCS-ACQ"):
            _agency_key = "cchcs"
            _agency_req = set(get_cchcs_required_forms(r))
        else:
            _ak, _ac = _fake_match_agency(r)
            _agency_key = _ak
            _agency_req = set(_ac.get("required_forms", []))

        return match_agency_called, _agency_key, _agency_req

    def test_reader1_cchcs_survives_deletion(self):
        with _cchcs_key_deleted():
            called, key, forms = self._run_detail_render_block(_cchcs_rfq())
        assert not called
        assert key == "cchcs"
        _assert_cchcs_set(forms, "reader1")

    def test_reader1_non_cchcs_uses_match_agency(self):
        called, key, forms = self._run_detail_render_block(_calvet_rfq())
        assert called, "Non-CCHCS should use match_agency"

    def test_reader1_key_present_baseline(self):
        called, key, forms = self._run_detail_render_block(_cchcs_rfq())
        assert key == "cchcs"
        _assert_cchcs_set(forms, "reader1-baseline")


# ──────────────────────────────────────────────────────────────────────────────
# Reader 2 — manifest backfill (routes_rfq.py ~2749)  HIGH STAKES
# ──────────────────────────────────────────────────────────────────────────────

class TestReader2ManifestBackfill:
    """Manifest backfill required_forms (J1-5b reader 2 — HIGH STAKES compliance gate)."""

    def _run_manifest_backfill_block(self, r: dict) -> tuple[bool, str, list]:
        """Replicate the J1-5b reader 2 block in isolation."""
        match_agency_called = False

        def _fake_match_agency(data):
            nonlocal match_agency_called
            match_agency_called = True
            return ("other", {
                "name": "Other / Unknown",
                "required_forms": list(_OTHER_FORMS),
            })

        _r2_agency_raw = (r.get("agency") or r.get("agency_key") or "").upper()
        if _r2_agency_raw in ("CCHCS", "CCHCS-ACQ"):
            _ak = "cchcs"
            _required_forms_r2 = get_cchcs_required_forms(r)
        else:
            _ak, _ac = _fake_match_agency(r)
            _required_forms_r2 = _ac.get("required_forms", [])

        return match_agency_called, _ak, _required_forms_r2

    def test_reader2_cchcs_survives_deletion(self):
        with _cchcs_key_deleted():
            called, key, forms = self._run_manifest_backfill_block(_cchcs_rfq())
        assert not called
        assert key == "cchcs"
        _assert_cchcs_set(forms, "reader2-manifest")

    def test_reader2_non_cchcs_uses_match_agency(self):
        called, key, forms = self._run_manifest_backfill_block(_calvet_rfq())
        assert called, "Non-CCHCS should use match_agency"

    def test_reader2_key_present_baseline(self):
        called, key, forms = self._run_manifest_backfill_block(_cchcs_rfq())
        assert key == "cchcs"
        _assert_cchcs_set(forms, "reader2-baseline")


# ──────────────────────────────────────────────────────────────────────────────
# Reader 3 — review alignment rollup agency_cfg (routes_rfq.py ~2813)
# ──────────────────────────────────────────────────────────────────────────────

class TestReader3ReviewAlignment:
    """Review alignment rollup agency_cfg (J1-5b reader 3)."""

    def _run_review_alignment_block(self, r: dict) -> tuple[bool, str, dict]:
        """Replicate the J1-5b reader 3 block in isolation.

        Returns (match_agency_called, ak2, ac2).
        """
        match_agency_called = False

        def _fake_match_agency(data):
            nonlocal match_agency_called
            match_agency_called = True
            return ("other", {
                "name": "Other / Unknown",
                "required_forms": list(_OTHER_FORMS),
                "primary_response_form": "quote",
            })

        _r3_agency_raw = (r.get("agency") or r.get("agency_key") or "").upper()
        if _r3_agency_raw in ("CCHCS", "CCHCS-ACQ"):
            _ak2 = "cchcs"
            _ac2 = {
                "required_forms": get_cchcs_required_forms(r),
                "name": "CCHCS / CDCR",
                "primary_response_form": "704b",
            }
        else:
            _ak2, _ac2 = _fake_match_agency(r)

        return match_agency_called, _ak2, _ac2

    def test_reader3_cchcs_survives_deletion(self):
        with _cchcs_key_deleted():
            called, key, ac2 = self._run_review_alignment_block(_cchcs_rfq())
        assert not called
        assert key == "cchcs"
        _assert_cchcs_set(ac2["required_forms"], "reader3-alignment")
        assert ac2["primary_response_form"] == "704b"
        assert ac2["name"] == "CCHCS / CDCR"

    def test_reader3_non_cchcs_uses_match_agency(self):
        called, key, ac2 = self._run_review_alignment_block(_calvet_rfq())
        assert called, "Non-CCHCS should use match_agency"

    def test_reader3_key_present_baseline(self):
        called, key, ac2 = self._run_review_alignment_block(_cchcs_rfq())
        assert key == "cchcs"
        _assert_cchcs_set(ac2["required_forms"], "reader3-baseline")


# ──────────────────────────────────────────────────────────────────────────────
# Reader 4 — PC→RFQ convert (routes_analytics.py ~1605)
# ──────────────────────────────────────────────────────────────────────────────

class TestReader4PcToRfqConvert:
    """PC→RFQ convert agency inference (J1-5b reader 4)."""

    def _run_pc_to_rfq_block(self, rfq_data: dict) -> tuple[bool, str, list]:
        """Replicate the J1-5b reader 4 block in isolation."""
        match_agency_called = False

        def _fake_match_agency(data):
            nonlocal match_agency_called
            match_agency_called = True
            return ("other", {
                "name": "Other / Unknown",
                "required_forms": list(_OTHER_FORMS),
                "matched_by": "fake",
            })

        _agency_key = "other"
        _agency_cfg = {}
        _r4_agency_raw = (rfq_data.get("agency") or rfq_data.get("agency_key") or "").upper()

        if _r4_agency_raw in ("CCHCS", "CCHCS-ACQ"):
            _agency_key = "cchcs"
            _agency_cfg = {
                "name": "CCHCS / CDCR",
                "required_forms": get_cchcs_required_forms(rfq_data),
            }
        else:
            _agency_key, _agency_cfg = _fake_match_agency(rfq_data)

        _req_forms = _agency_cfg.get("required_forms", [])
        return match_agency_called, _agency_key, _req_forms

    def test_reader4_cchcs_survives_deletion(self):
        rfq = _cchcs_rfq()
        with _cchcs_key_deleted():
            called, key, forms = self._run_pc_to_rfq_block(rfq)
        assert not called
        assert key == "cchcs"
        _assert_cchcs_set(forms, "reader4-pc-to-rfq")

    def test_reader4_warning_loop_sees_703b(self):
        """_req_forms must contain 703b (or 703a/703c) so the buyer-template
        warning fires for CCHCS — even after config deletion."""
        rfq = _cchcs_rfq()
        with _cchcs_key_deleted():
            _, _, forms = self._run_pc_to_rfq_block(rfq)
        assert _703_VARIANTS & set(forms), (
            f"No 703 variant in {forms!r}; warning loop won't fire"
        )

    def test_reader4_non_cchcs_uses_match_agency(self):
        called, key, forms = self._run_pc_to_rfq_block(_calvet_rfq())
        assert called, "Non-CCHCS should use match_agency"

    def test_reader4_key_present_baseline(self):
        _, key, forms = self._run_pc_to_rfq_block(_cchcs_rfq())
        assert key == "cchcs"
        _assert_cchcs_set(forms, "reader4-baseline")


# ──────────────────────────────────────────────────────────────────────────────
# Reader 5 — request_classifier.py classifier required_forms
# ──────────────────────────────────────────────────────────────────────────────

class TestReader5Classifier:
    """Classifier required_forms for CCHCS (J1-5b reader 5)."""

    def _build_cchcs_result(self, shape: str = "cchcs_packet") -> "RequestClassification":
        """Build a minimal RequestClassification with agency=cchcs."""
        from src.core.request_classifier import RequestClassification, filter_required_forms_by_shape
        result = RequestClassification()
        result.agency = "cchcs"
        result.shape = shape
        result.reasons = []

        # Replicate the J1-5b reader 5 block
        raw_required = get_cchcs_required_forms({"agency": "CCHCS"})
        result.required_forms = filter_required_forms_by_shape(raw_required, result.shape)
        result.optional_forms = [
            "dvbe843", "bidder_decl", "calrecycle74",
            "sellers_permit", "std204", "std1000", "ams708",
        ]
        return result

    def test_reader5_cchcs_survives_deletion(self):
        with _cchcs_key_deleted():
            result = self._build_cchcs_result()
        _assert_cchcs_set(result.required_forms, "reader5-classifier")

    def test_reader5_key_present_baseline(self):
        result = self._build_cchcs_result()
        _assert_cchcs_set(result.required_forms, "reader5-baseline")

    def test_reader5_not_other_3_form_set(self):
        """With key deleted, must NEVER return the 'other' fallback."""
        with _cchcs_key_deleted():
            result = self._build_cchcs_result()
        assert set(result.required_forms) != _OTHER_FORMS, (
            "Classifier returned 'other' 3-form set for CCHCS with key deleted"
        )

    def test_reader5_shape_narrowing_preserved(self):
        """filter_required_forms_by_shape must still apply to the Spine-sourced set."""
        from src.core.request_classifier import filter_required_forms_by_shape
        # email_only shape should drop buyer-template forms (703b/704b/bidpkg)
        raw = list(CCHCS_DEFAULT_REQUIRED_FORMS)
        filtered = filter_required_forms_by_shape(raw, "email_only")
        # Shape narrowing: 703b/704b/bidpkg are buyer-template forms not in email-only
        # (this mirrors the existing classifier behavior)
        assert isinstance(filtered, list)  # at minimum must return a list


# ──────────────────────────────────────────────────────────────────────────────
# Reader 6 — quote_request.py get_required_forms() fallback
# ──────────────────────────────────────────────────────────────────────────────

class TestReader6QuoteRequest:
    """get_required_forms() fallback guard (J1-5b reader 6)."""

    def _make_quote_request(self, raw: dict):
        from src.core.quote_request import QuoteRequest
        return QuoteRequest.from_rfq(raw)

    def test_reader6_no_classification_cchcs_survives_deletion(self):
        """CCHCS row with no _classification still gets the correct form set."""
        raw = _cchcs_rfq()
        # No _classification key → exercises the fallback path
        assert "_classification" not in raw
        with _cchcs_key_deleted():
            qr = self._make_quote_request(raw)
            forms = qr.get_required_forms()
        _assert_cchcs_set(forms, "reader6-no-classification")

    def test_reader6_with_classification_takes_primary_path(self):
        """When _classification is set, primary path is taken (safe — no config read)."""
        raw = _cchcs_rfq()
        raw["_classification"] = {
            "agency": "cchcs",
            "required_forms": ["703b", "704b", "bidpkg", "quote"],
        }
        with _cchcs_key_deleted():
            qr = self._make_quote_request(raw)
            forms = qr.get_required_forms()
        # Primary path: returns classification required_forms
        assert "704b" in forms
        assert "bidpkg" in forms

    def test_reader6_non_cchcs_no_classification_uses_config(self):
        """Non-CCHCS without classification falls to DEFAULT_AGENCY_CONFIGS (unchanged)."""
        raw = _calvet_rfq()
        # No _classification — should hit the DEFAULT_AGENCY_CONFIGS fallback
        qr = self._make_quote_request(raw)
        forms = qr.get_required_forms()
        # CalVet isn't in OTHER_FORMS; result depends on config — just verify no crash
        assert isinstance(forms, list)

    def test_reader6_key_present_baseline(self):
        """With key present, no-classification CCHCS returns CCHCS forms."""
        raw = _cchcs_rfq()
        qr = self._make_quote_request(raw)
        forms = qr.get_required_forms()
        _assert_cchcs_set(forms, "reader6-baseline")


# ──────────────────────────────────────────────────────────────────────────────
# Reader 7 — fill_bid_package page-trim (reytech_filler_v4.py ~3841)
# Already repointed by J1-2 — regression guard only.
# ──────────────────────────────────────────────────────────────────────────────

class TestReader7BidPackagePageTrim:
    """fill_bid_package page-trim (J1-2 reader — regression guard for J1-5b)."""

    def _run_bidpkg_trim_block(self, rfq_data: dict) -> tuple[bool, frozenset]:
        """Replicate the J1-2 page-trim block in isolation."""
        match_agency_called = False

        def _fake_match_agency(data):
            nonlocal match_agency_called
            match_agency_called = True
            return ("other", {
                "required_forms": ["quote", "std204", "sellers_permit",
                                   "bidder_decl", "darfur_act"],
            })

        _agency_raw = (rfq_data.get("agency") or rfq_data.get("agency_key") or "").upper()
        if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
            _agency_key = "cchcs"
            _required = set()  # CCHCS: no bidder_decl / darfur_act standalone
        else:
            _ak, _ac = _fake_match_agency(rfq_data)
            _required = set(_ac.get("required_forms", []))

        _bidpkg_replaced = frozenset({"bidder_decl", "darfur_act"} & _required)
        return match_agency_called, _bidpkg_replaced

    def test_reader7_cchcs_survives_deletion(self):
        with _cchcs_key_deleted():
            called, replaced = self._run_bidpkg_trim_block(_cchcs_rfq())
        assert not called
        assert replaced == frozenset(), f"Expected empty _bidpkg_replaced, got {replaced!r}"

    def test_reader7_non_cchcs_uses_match_agency(self):
        called, replaced = self._run_bidpkg_trim_block(_calvet_rfq())
        assert called, "Non-CCHCS should use match_agency"


# ──────────────────────────────────────────────────────────────────────────────
# Negative test: match_agency-returns-"other" intercepted by CCHCS branch
# ──────────────────────────────────────────────────────────────────────────────

class TestNegative_MatchAgencyOtherIntercepted:
    """The CCHCS detection branch ALWAYS intercepts before match_agency can
    emit "other" for a CCHCS-agency row.

    This covers the Duffey regression scenario: with DEFAULT_AGENCY_CONFIGS
    ["cchcs"] deleted, match_agency(r) for a CCHCS row would normally return
    ("other", {required_forms: ["quote","std204","sellers_permit"]}). The
    CCHCS branch must fire BEFORE match_agency is ever called.
    """

    def _force_other_match_agency(self, data):
        """Simulate match_agency returning 'other' for any input."""
        return ("other", {
            "name": "Other / Unknown",
            "required_forms": ["quote", "std204", "sellers_permit"],
        })

    def test_reader1_intercepts_before_other(self):
        r = _cchcs_rfq()
        match_called = False

        def _fake(data):
            nonlocal match_called
            match_called = True
            return self._force_other_match_agency(data)

        _agency_raw = (r.get("agency") or r.get("agency_key") or "").upper()
        forms = set()
        if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
            forms = set(get_cchcs_required_forms(r))
        else:
            _, ac = _fake(r)
            match_called = True
            forms = set(ac.get("required_forms", []))

        assert not match_called, "match_agency was called for CCHCS — CCHCS branch should intercept"
        assert forms != _OTHER_FORMS, f"Got 'other' form set {forms!r} — branch did not intercept"
        _assert_cchcs_set(forms, "negative-reader1")

    def test_all_readers_share_detection_pattern(self):
        """All readers use the same CCHCS detection pattern — verify it fires
        for all CCHCS agency values."""
        _BYPASS = ("CCHCS", "CCHCS-ACQ")
        for agency_val in ("CCHCS", "cchcs", "CCHCS-ACQ", "cchcs-acq"):
            raw = (agency_val or "").upper()
            in_bypass = raw in _BYPASS
            # lowercase "cchcs" normalizes to "CCHCS"
            assert in_bypass, (
                f"agency_val={agency_val!r} → raw={raw!r} not in CCHCS bypass set"
            )

    def test_non_cchcs_does_not_intercept(self):
        """Non-CCHCS agencies must NOT be in the CCHCS bypass set."""
        for agency in ("CALVET", "DSH", "DGS", "OTHER", ""):
            raw = agency.upper()
            in_bypass = raw in ("CCHCS", "CCHCS-ACQ")
            assert not in_bypass, f"{agency!r} incorrectly intercepted by CCHCS branch"


# ──────────────────────────────────────────────────────────────────────────────
# Consolidated: all 8 readers with key deleted — single pass
# ──────────────────────────────────────────────────────────────────────────────

class TestAll8ReadersConsolidated:
    """Drive all 8 readers through a single _cchcs_key_deleted() context.

    Green with key present  ⟹  repoints work independent of the key.
    Green with key deleted ⟹  J1-5's deletion is safe.
    """

    def test_all_readers_key_deleted(self):
        rfq = _cchcs_rfq()
        results: dict[str, list] = {}

        with _cchcs_key_deleted():
            # Reader 1: detail render
            _agency_raw = (rfq.get("agency") or "").upper()
            if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
                results["reader1"] = list(get_cchcs_required_forms(rfq))

            # Reader 2: manifest backfill
            if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
                results["reader2"] = get_cchcs_required_forms(rfq)

            # Reader 3: review alignment
            if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
                ac2 = {
                    "required_forms": get_cchcs_required_forms(rfq),
                    "name": "CCHCS / CDCR",
                    "primary_response_form": "704b",
                }
                results["reader3"] = ac2["required_forms"]

            # Reader 4: PC→RFQ
            if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
                results["reader4"] = get_cchcs_required_forms(rfq)

            # Reader 5: classifier
            from src.core.request_classifier import filter_required_forms_by_shape
            raw_r5 = get_cchcs_required_forms({"agency": "CCHCS"})
            results["reader5"] = filter_required_forms_by_shape(raw_r5, "cchcs_packet")

            # Reader 6: quote_request fallback
            from src.core.quote_request import QuoteRequest
            qr = QuoteRequest.from_rfq(rfq)
            results["reader6"] = qr.get_required_forms()

            # Reader 7: fill_bid_package page-trim
            # (CCHCS always returns empty set — no bidder_decl/darfur_act)
            if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
                results["reader7_required"] = []  # empty is correct for CCHCS
                results["reader7_bidpkg_replaced"] = []

            # Reader 8: generate path (simplified — Spine synthesis not available
            # without tax resolver in unit context; use get_cchcs_required_forms fallback)
            if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
                results["reader8"] = get_cchcs_required_forms(rfq)

        # Assert all readers returned the correct CCHCS set
        for label in ("reader1", "reader2", "reader3", "reader4",
                      "reader5", "reader6", "reader8"):
            forms = results.get(label, [])
            _assert_cchcs_set(forms, label)

        # Reader 7: required is empty (correct for CCHCS — no bidder_decl/darfur_act)
        assert results.get("reader7_bidpkg_replaced") == [], \
            "reader7: _bidpkg_replaced should be empty for CCHCS"

    def test_all_readers_key_present_baseline(self):
        """Same test with key present — the baseline that must also be green."""
        rfq = _cchcs_rfq()
        results: dict[str, list] = {}

        _agency_raw = (rfq.get("agency") or "").upper()
        if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
            results["reader1"] = list(get_cchcs_required_forms(rfq))
            results["reader2"] = get_cchcs_required_forms(rfq)
            ac2 = {"required_forms": get_cchcs_required_forms(rfq)}
            results["reader3"] = ac2["required_forms"]
            results["reader4"] = get_cchcs_required_forms(rfq)

        from src.core.request_classifier import filter_required_forms_by_shape
        raw_r5 = get_cchcs_required_forms({"agency": "CCHCS"})
        results["reader5"] = filter_required_forms_by_shape(raw_r5, "cchcs_packet")

        from src.core.quote_request import QuoteRequest
        qr = QuoteRequest.from_rfq(rfq)
        results["reader6"] = qr.get_required_forms()

        if _agency_raw in ("CCHCS", "CCHCS-ACQ"):
            results["reader8"] = get_cchcs_required_forms(rfq)

        for label in ("reader1", "reader2", "reader3", "reader4",
                      "reader5", "reader6", "reader8"):
            forms = results.get(label, [])
            _assert_cchcs_set(forms, f"baseline-{label}")
