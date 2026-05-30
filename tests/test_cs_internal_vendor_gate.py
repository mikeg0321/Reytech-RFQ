"""ISSUE-13 (2026-05-29 sweep): the OUTBOUND draft gates must skip Reytech's
own internal vendors / back-office (e.g. the bookkeeper streamlineoc.com).

The inbound ingest gate (email_poller.BLOCKED_SENDERS) already blocked the
bookkeeper's domain (PR #1221), but the CS-draft + po_confirm paths used a
SEPARATE denylist that didn't know she was internal — so reply drafts kept
getting queued to her. These tests lock the convergence: one
`is_internal_vendor` source of truth, consulted by the CS-draft skip gate.
"""

from src.agents import cs_agent


class TestIsInternalVendor:
    def test_bookkeeper_domain_is_internal(self):
        assert cs_agent.is_internal_vendor("shaina@streamlineoc.com") is True
        # case / whitespace insensitive
        assert cs_agent.is_internal_vendor("  Shaina@StreamlineOC.com ") is True

    def test_customer_domain_is_not_internal(self):
        assert cs_agent.is_internal_vendor("buyer@cdcr.ca.gov") is False
        assert cs_agent.is_internal_vendor("") is False
        assert cs_agent.is_internal_vendor("not-an-email") is False


class TestCsDraftSkipsInternalVendor:
    def test_skips_bookkeeper(self):
        skip, reason = cs_agent._should_skip_cs_draft("shaina@streamlineoc.com")
        assert skip is True
        assert "internal vendor" in reason.lower()

    def test_does_not_skip_real_customer(self):
        skip, _reason = cs_agent._should_skip_cs_draft("buyer@cdcr.ca.gov")
        assert skip is False

    def test_existing_govspend_reject_still_works(self):
        # regression: the new branch must not break the pre-existing list
        skip, reason = cs_agent._should_skip_cs_draft("admin@govspendemail.com")
        assert skip is True
