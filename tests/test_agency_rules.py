"""Tests for agency_rules read/write API and Form QA integration.

Claude extraction path is mocked — we validate the idempotent upsert,
read filters, Form QA surfacing, and rule-confidence averaging.
"""
import pytest


@pytest.fixture
def clean_rules():
    from src.core.db import get_db
    with get_db() as conn:
        conn.execute("DELETE FROM agency_rules")
        conn.commit()
    yield
    with get_db() as conn:
        conn.execute("DELETE FROM agency_rules")
        conn.commit()


class TestUpsertAndRead:
    def test_upsert_creates_rule(self, clean_rules):
        from src.core.agency_rules import upsert_rule, get_rules_for_agency
        rid = upsert_rule("CDCR", "signature", "Sign in blue ink only",
                          source_email_id="gmail-1", confidence=0.8)
        assert rid > 0
        rules = get_rules_for_agency("CDCR")
        assert len(rules) == 1
        assert rules[0]["rule_text"] == "Sign in blue ink only"
        assert rules[0]["sample_count"] == 1

    def test_upsert_idempotent_bumps_sample(self, clean_rules):
        from src.core.agency_rules import upsert_rule, get_rules_for_agency
        rid1 = upsert_rule("CDCR", "signature", "Sign in blue ink only",
                           source_email_id="gmail-1", confidence=0.8)
        rid2 = upsert_rule("CDCR", "signature", "Sign in blue ink only",
                           source_email_id="gmail-2", confidence=0.6)
        assert rid1 == rid2  # same rule, same id
        rules = get_rules_for_agency("CDCR")
        assert len(rules) == 1
        assert rules[0]["sample_count"] == 2
        # Confidence averaged: (0.8 + 0.6) / 2 = 0.7
        assert abs(rules[0]["confidence"] - 0.7) < 0.01
        # Both source emails stored
        assert "gmail-1" in rules[0]["source_email_ids"]
        assert "gmail-2" in rules[0]["source_email_ids"]

    def test_rule_type_filter(self, clean_rules):
        from src.core.agency_rules import upsert_rule, get_rules_for_agency
        upsert_rule("CDCR", "signature", "Sign blue ink", confidence=0.9)
        upsert_rule("CDCR", "delivery", "3 business days max", confidence=0.9)
        sigs = get_rules_for_agency("CDCR", rule_type="signature")
        deliv = get_rules_for_agency("CDCR", rule_type="delivery")
        assert len(sigs) == 1 and sigs[0]["rule_type"] == "signature"
        assert len(deliv) == 1 and deliv[0]["rule_type"] == "delivery"

    def test_confidence_threshold(self, clean_rules):
        from src.core.agency_rules import upsert_rule, get_rules_for_agency
        upsert_rule("CDCR", "misc", "High confidence rule", confidence=0.9)
        upsert_rule("CDCR", "misc", "Low confidence rule", confidence=0.3)
        high_only = get_rules_for_agency("CDCR", min_confidence=0.7)
        assert len(high_only) == 1
        assert high_only[0]["rule_text"] == "High confidence rule"

    def test_agency_case_insensitive(self, clean_rules):
        from src.core.agency_rules import upsert_rule, get_rules_for_agency
        upsert_rule("CDCR", "misc", "test", confidence=0.8)
        assert len(get_rules_for_agency("cdcr")) == 1
        assert len(get_rules_for_agency("CDCR")) == 1
        assert len(get_rules_for_agency("CalVet")) == 0

    def test_invalid_rule_type_demoted_to_misc(self, clean_rules):
        from src.core.agency_rules import upsert_rule, get_rules_for_agency
        upsert_rule("CDCR", "not_a_real_type", "some rule", confidence=0.8)
        rules = get_rules_for_agency("CDCR")
        assert rules[0]["rule_type"] == "misc"

    def test_deactivate(self, clean_rules):
        from src.core.agency_rules import upsert_rule, get_rules_for_agency, deactivate_rule
        rid = upsert_rule("CDCR", "misc", "rule to deactivate", confidence=0.8)
        assert len(get_rules_for_agency("CDCR")) == 1
        deactivate_rule(rid)
        assert len(get_rules_for_agency("CDCR", active_only=True)) == 0
        assert len(get_rules_for_agency("CDCR", active_only=False)) == 1


class TestSummaryForQA:
    def test_summary_bucketed_by_type(self, clean_rules):
        from src.core.agency_rules import upsert_rule, summarize_for_qa
        upsert_rule("CDCR", "signature", "Sign blue", confidence=0.9)
        upsert_rule("CDCR", "signature", "Sign both pages", confidence=0.8)
        upsert_rule("CDCR", "delivery", "3 day max", confidence=0.9)
        s = summarize_for_qa("CDCR", min_confidence=0.7)
        assert s["agency"] == "CDCR"
        assert s["rule_count"] == 3
        assert set(s["types"]) == {"signature", "delivery"}
        assert len(s["by_type"]["signature"]) == 2


class TestAgencyClassifier:
    def test_keywords_match_email_bodies(self):
        from src.agents.agency_rules_extractor import _classify_agency
        assert _classify_agency("buyer@cdcr.ca.gov", "RFQ 2025-03", "") == "cdcr"
        assert _classify_agency("", "Quote for CCHCS Stockton", "") == "cchcs"
        assert _classify_agency("", "", "Please send to CH Yountville") == "calvet"
        assert _classify_agency("foo@example.com", "hi", "unrelated body") == ""


class TestExtractionReturnShape:
    """run_extraction should return a complete report shape even when
    Gmail is not configured (returns early with empty summary)."""

    def test_no_gmail_returns_empty_report(self, monkeypatch, clean_rules):
        import src.core.gmail_api as _ga
        monkeypatch.setattr(_ga, "is_configured", lambda: False)
        from src.agents.agency_rules_extractor import run_extraction
        r = run_extraction(days=30, dry_run=True)
        assert r["emails_fetched"] == 0
        assert r["rules_upserted"] == 0
        assert "agencies" in r

    def test_classification_buckets_correctly(self, monkeypatch, clean_rules):
        import src.agents.agency_rules_extractor as _ex
        mock_emails = [
            {"gmail_id": "g1", "from": "b@cdcr.ca.gov", "subject": "RFQ",
             "body": "please quote", "date": "2026-01-01"},
            {"gmail_id": "g2", "from": "b@cchcs.ca.gov", "subject": "need",
             "body": "...", "date": "2026-01-02"},
            {"gmail_id": "g3", "from": "spam@random.com", "subject": "hi",
             "body": "unrelated", "date": "2026-01-03"},
        ]
        monkeypatch.setattr(_ex, "fetch_buyer_emails", lambda **kw: mock_emails)
        # Stub Claude so no real calls
        monkeypatch.setattr(_ex, "_extract_rules_batch",
                            lambda agency, batch: [
                                {"rule_type": "misc", "rule_text": f"test for {agency}",
                                 "confidence": 0.8, "source_email_id": "g1"}
                            ])
        from src.agents.agency_rules_extractor import run_extraction
        r = run_extraction(days=30, dry_run=False)
        assert r["emails_fetched"] == 3
        # g3 has no agency match, should not appear
        assert "cdcr" in r["agencies"]
        assert "cchcs" in r["agencies"]
        assert "spam" not in r["agencies"]
        # Stubbed extractor produces 1 rule each
        from src.core.agency_rules import get_rules_for_agency
        assert len(get_rules_for_agency("cdcr")) == 1
        assert len(get_rules_for_agency("cchcs")) == 1


class TestFormQAHook:
    """Form QA should surface agency rules as warnings without blocking."""

    def test_rejection_rules_added_to_warnings(self, clean_rules, tmp_path):
        from src.core.agency_rules import upsert_rule
        upsert_rule("CDCR", "rejection_reason",
                    "Previous quote rejected for missing signed W-9",
                    confidence=0.6)
        upsert_rule("CDCR", "signature", "Always sign in blue ink",
                    confidence=0.85)

        from src.forms.form_qa import run_form_qa
        report = run_form_qa(
            out_dir=str(tmp_path),
            output_files=[],
            form_id_map=[],
            rfq_data={},
            config={},
            agency_key="CDCR",
            required_forms=set(),
        )
        assert "agency_rules" in report
        assert report["agency_rules"]["count"] >= 2
        # rejection_reason surfaces as a warning
        joined = "\n".join(report["warnings"])
        assert "signed W-9" in joined

    def test_no_rules_no_failure(self, clean_rules, tmp_path):
        """When no rules exist, Form QA proceeds normally."""
        from src.forms.form_qa import run_form_qa
        report = run_form_qa(
            out_dir=str(tmp_path),
            output_files=[],
            form_id_map=[],
            rfq_data={},
            config={},
            agency_key="CDCR",
            required_forms=set(),
        )
        # Hook is non-blocking, report should still be shaped correctly
        assert "warnings" in report
        assert "critical_issues" in report
