"""Regression tests for the QA-gate false positives that hard-blocked
finalize on the Coleman CCHCS bid (sol 10847187, manifest 144) on
2026-05-29. Four independent false-positives + the "Review failed:
unknown" observability hole. See Mr. Wolf report 2026-05-29.
"""
from datetime import datetime, timedelta

from src.forms.form_qa import (
    _normalize_solicitation,
    verify_package_completeness,
    validate_against_requirements,
    BID_PACKAGE_INTERNAL_FORMS,
)


# ── Fix 4: solicitation-prefix normalization (close the class) ──────────
class TestSolicitationNormalization:
    def test_strips_pr_prefix(self):
        # The exact Coleman false positive: "PR 10847187" == "10847187"
        assert _normalize_solicitation("PR 10847187") == "10847187"

    def test_strips_preq_prefix_still(self):
        # The original PR-AV-AC3 case must keep working.
        assert _normalize_solicitation("PREQ-10847262") == "10847262"
        assert _normalize_solicitation("PREQ 10847262") == "10847262"

    def test_strips_other_agency_labels(self):
        for raw, want in [
            ("RFQ 12345", "12345"),
            ("RFP-9988", "9988"),
            ("SOLICITATION: 55512", "55512"),
            ("BID #4321", "4321"),
            ("PO 7777", "7777"),
        ]:
            assert _normalize_solicitation(raw) == want, raw

    def test_bare_number_unchanged(self):
        assert _normalize_solicitation("10847187") == "10847187"

    def test_does_not_eat_embedded_letters(self):
        # No leading label → leave alphanumeric core intact.
        assert _normalize_solicitation("AB12345") == "AB12345"

    def test_matched_pair_compares_equal(self):
        assert (_normalize_solicitation("PR 10847187")
                == _normalize_solicitation("10847187"))


# ── Fix 1: bid-package-internal forms are satisfied by the bid package ──
class TestCompletenessHonorsBidpkg:
    def test_sellers_permit_not_missing_when_bidpkg_present(self):
        # Coleman: contract extraction unioned sellers_permit into required.
        res = verify_package_completeness(
            agency_key="cchcs",
            required_forms={"704b", "bidpkg", "quote", "sellers_permit"},
            generated_files=[
                "10847187_704b_Reytech.pdf",
                "10847187_BidPackage_Reytech.pdf",
                "10847187_Quote_Reytech.pdf",
            ],
            has_bid_package=True,
        )
        assert res["passed"] is True
        assert "sellers_permit" not in res["missing"]
        assert not any("sellers_permit" in i for i in res["issues"])

    def test_all_internal_forms_covered_by_bidpkg(self):
        res = verify_package_completeness(
            agency_key="cchcs",
            required_forms=set(BID_PACKAGE_INTERNAL_FORMS) | {"bidpkg"},
            generated_files=["10847187_BidPackage_Reytech.pdf"],
            has_bid_package=True,
        )
        assert res["passed"] is True
        assert res["missing"] == []

    def test_genuinely_missing_owned_form_still_blocks(self):
        # A non-internal required form with no file MUST still hard-fail.
        res = verify_package_completeness(
            agency_key="cchcs",
            required_forms={"704b", "bidpkg", "quote"},
            generated_files=["10847187_BidPackage_Reytech.pdf"],
            has_bid_package=True,
        )
        assert res["passed"] is False
        assert "704b" in res["missing"]

    def test_internal_form_still_missing_when_no_bidpkg(self):
        # Without a bid package, an internal form genuinely isn't covered.
        res = verify_package_completeness(
            agency_key="dgs",
            required_forms={"quote", "sellers_permit"},
            generated_files=["x_Quote_Reytech.pdf"],
            has_bid_package=False,
        )
        assert res["passed"] is False
        assert "sellers_permit" in res["missing"]


# ── Fix 3: buyer-mentioned-but-unrendered form → warning, not critical ──
class TestRequirementsValidatorAdvisory:
    def test_std817_boilerplate_does_not_block(self):
        out = validate_against_requirements(
            generated_files=["10847187_704b_Reytech.pdf",
                             "10847187_BidPackage_Reytech.pdf"],
            requirements_json={"forms_required": ["std817"]},
            rfq_data={"solicitation_number": "10847187"},
            config={},
            strict=True,
        )
        # Surfaced (LAW 6) but advisory — never a hard block.
        assert out["passed"] is True
        gaps = [g for g in out["gaps"] if g["type"] == "missing_form"]
        assert gaps and all(g["severity"] == "warning" for g in gaps)

    # ── Fix 4 end-to-end: prefixed sol# no longer a critical mismatch ──
    def test_pr_prefixed_sol_no_mismatch_gap(self):
        out = validate_against_requirements(
            generated_files=["10847187_BidPackage_Reytech.pdf"],
            requirements_json={"solicitation_number": "PR 10847187"},
            rfq_data={"solicitation_number": "10847187"},
            config={},
            strict=True,
        )
        assert not any(g["type"] == "solicitation_mismatch" for g in out["gaps"])

    def test_real_sol_mismatch_still_critical(self):
        out = validate_against_requirements(
            generated_files=["x.pdf"],
            requirements_json={"solicitation_number": "PR 99999999"},
            rfq_data={"solicitation_number": "10847187"},
            config={},
            strict=True,
        )
        mm = [g for g in out["gaps"] if g["type"] == "solicitation_mismatch"]
        assert mm and mm[0]["severity"] == "critical"
        assert out["passed"] is False


# ── Fix 2: a deadline is a warning, never a finalize hard-block ─────────
class TestDeadlineNotCritical:
    def test_due_today_not_passed_and_not_critical(self):
        today = datetime.now().date().isoformat()
        out = validate_against_requirements(
            generated_files=["x.pdf"],
            requirements_json={"due_date": today},
            rfq_data={"due_date": today},
            config={},
            strict=True,
        )
        assert out["passed"] is True
        assert not any(g["type"] == "deadline_passed" for g in out["gaps"])
        today_gaps = [g for g in out["gaps"] if g["type"] == "deadline_today"]
        assert all(g["severity"] == "warning" for g in today_gaps)

    def test_genuinely_past_deadline_stays_critical(self):
        # The midnight bug only mis-fired for TODAY. A deadline that has
        # really passed (yesterday or earlier) MUST still hard-block — you
        # don't submit a closed solicitation without buyer confirmation.
        yesterday = (datetime.now().date() - timedelta(days=1)).isoformat()
        out = validate_against_requirements(
            generated_files=["x.pdf"],
            requirements_json={"due_date": yesterday},
            rfq_data={"due_date": yesterday},
            config={},
            strict=True,
        )
        passed_gaps = [g for g in out["gaps"] if g["type"] == "deadline_passed"]
        assert passed_gaps and all(g["severity"] == "critical" for g in passed_gaps)
        assert out["passed"] is False


# ── Problem 2: review_form returns (ok, error), never a silent "unknown" ─
class TestReviewFormObservability:
    def test_no_matching_row_returns_reason(self, monkeypatch):
        import src.core.dal as dal

        class _FakeCur:
            rowcount = 0

        class _FakeConn:
            def execute(self, *a, **k):
                # COUNT(*) path returns a fetchone()-able; UPDATE returns cur.
                class _R:
                    def fetchone(self_inner):
                        return [0]
                return _FakeCur() if a and a[0].strip().upper().startswith("UPDATE") else _R()

        from contextlib import contextmanager

        @contextmanager
        def _fake_get_db():
            yield _FakeConn()

        monkeypatch.setattr(dal, "get_db", _fake_get_db)
        ok, err = dal.review_form(999999, "ghost_form", "approved")
        assert ok is False
        assert err and "ghost_form" in err
