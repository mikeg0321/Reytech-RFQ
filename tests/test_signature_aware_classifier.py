"""PR-AA — signature-aware classifier: veto RFQ when email is from a
non-procurement team.

The 2026-05-13 forensic on rfq_b57f85f7 found a CalVet accounting-team
email got classified as an RFQ because Tier 0 of `is_rfq_email()` fires
on any `*.ca.gov` procurement domain without reading the body or
signature block. The accountant's signature ("Sarah Smith / Accounts
Payable / CalVet") + body ("requesting updated W-9 for vendor file")
were strong CS-team signals that the RFQ pipeline ignored.

Result: misleading RFQ row in the queue + operator wasted cycles +
no signal reaches the CS classifier where it belongs.

Fix: new helper `is_non_rfq_team_email(subject, body, sender)` runs as
a NEGATIVE gate at the top of `is_rfq_email`, after the existing
recall/PC/newsletter gates but BEFORE Tier 0 procurement-domain win.
If signature OR body OR subject contains strong non-procurement-team
signals AND no RFQ-strong keyword, veto the classification.

Pinned guarantees:
  1. `extract_signature_block` returns the last ~5 lines of the body.
  2. `is_non_rfq_team_email` returns True for accounting/AP/billing/
     credentialing/HR/IT-helpdesk signatures.
  3. `is_non_rfq_team_email` returns False when the email also has an
     RFQ-strong keyword (real RFQs occasionally CC accounting).
  4. `is_rfq_email` vetoes a procurement-domain email when the
     signature is non-RFQ team.
  5. A real RFQ from procurement still classifies True even if it
     mentions "invoice" or "payment" in passing.
  6. Empty / missing body / signature is a no-op (don't veto on
     thin signal).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# ── extract_signature_block ────────────────────────────────────────


def test_extract_signature_block_picks_last_lines():
    from src.agents.email_poller import extract_signature_block
    body = """Hi Reytech,

Could you please send an updated W-9 for our vendor file?
Our records have you listed but the form is from 2022.

Thanks,
Sarah Smith
Accounts Payable Specialist
California Department of Veterans Affairs
sarah.smith@calvet.ca.gov
(916) 555-1234"""
    sig = extract_signature_block(body)
    assert "Accounts Payable" in sig
    assert "Sarah Smith" in sig


def test_extract_signature_block_handles_missing():
    from src.agents.email_poller import extract_signature_block
    assert extract_signature_block("") == ""
    assert extract_signature_block(None) == ""
    assert extract_signature_block("just one line") == "just one line"


def test_extract_signature_block_skips_reply_quote():
    """Should not pull text from `> quoted reply` lines as signature."""
    from src.agents.email_poller import extract_signature_block
    body = """Yes, please send it over.

Thanks,
Anna Wilson
Senior Buyer
> On Mon, you wrote:
> Original message here"""
    sig = extract_signature_block(body)
    assert "Senior Buyer" in sig
    assert "Original message" not in sig


# ── is_non_rfq_team_email ──────────────────────────────────────────


def test_non_rfq_team_fires_on_accounts_payable():
    from src.agents.email_poller import is_non_rfq_team_email
    subject = "W-9 request for vendor file"
    body = """Hi,

We need an updated W-9 for our records.

Sarah Smith
Accounts Payable Specialist
CalVet"""
    assert is_non_rfq_team_email(subject, body, "sarah@calvet.ca.gov") is True


def test_non_rfq_team_fires_on_credentialing():
    """Credentialing departments at CCHCS/CDCR send vendor-compliance
    requests, not RFQs. Must be vetoed."""
    from src.agents.email_poller import is_non_rfq_team_email
    body = """Please complete the attached vendor agreement.

Maria Lopez
Credentialing Coordinator
CCHCS"""
    assert is_non_rfq_team_email("Vendor onboarding", body, "m@cchcs.ca.gov") is True


def test_non_rfq_team_fires_on_billing_office():
    from src.agents.email_poller import is_non_rfq_team_email
    body = """Following up on invoice INV-12345.

Joe Hughes
Billing Office
Dept of Corrections"""
    assert is_non_rfq_team_email("Re: invoice", body, "j@cdcr.ca.gov") is True


def test_non_rfq_team_returns_false_when_no_team_signal():
    from src.agents.email_poller import is_non_rfq_team_email
    body = """Please provide quote for the attached items.

Anna Wilson
Senior Buyer
Procurement Department"""
    # "Senior Buyer" + "Procurement" are RFQ-positive signatures
    assert is_non_rfq_team_email("RFQ 12345", body, "a@calvet.ca.gov") is False


def test_non_rfq_team_returns_false_when_rfq_strong_keyword_present():
    """A real RFQ might CC the AP team. Don't veto if there's a strong
    RFQ keyword AND the AP signal might be incidental."""
    from src.agents.email_poller import is_non_rfq_team_email
    body = """Please quote the following items per attached 704.
Reply to procurement@calvet — AP will process payment after award.

Anna Wilson
Senior Buyer"""
    assert is_non_rfq_team_email("RFQ 12345 — 704 attached", body,
                                  "a@calvet.ca.gov") is False


def test_non_rfq_team_handles_empty_body():
    from src.agents.email_poller import is_non_rfq_team_email
    assert is_non_rfq_team_email("test", "", "x@calvet.ca.gov") is False


def test_non_rfq_team_w9_subject_alone():
    """Subject 'W-9 request' is enough even without signature signal."""
    from src.agents.email_poller import is_non_rfq_team_email
    assert is_non_rfq_team_email("Updated W-9 needed", "",
                                  "x@calvet.ca.gov") is True


# ── is_rfq_email integration ────────────────────────────────────────


def test_is_rfq_email_vetoes_accounting_at_procurement_domain():
    """The headline test: rfq_b57f85f7-shaped email — accounting team
    at calvet.ca.gov asking about a W-9 — must NOT classify as RFQ."""
    from src.agents.email_poller import is_rfq_email
    subject = "Updated W-9 for vendor file"
    body = """Hi Reytech,

We're updating our vendor file and need a current W-9 from you.

Thanks,
Sarah Smith
Accounts Payable Specialist
California Department of Veterans Affairs
sarah.smith@calvet.ca.gov"""
    # No PDFs, no RFQ keywords — just a procurement-domain sender
    result = is_rfq_email(subject, body, attachments=[],
                          sender_email="sarah.smith@calvet.ca.gov")
    assert result is False, \
        "Accounting-team email at procurement domain must be vetoed from RFQ pipeline"


def test_is_rfq_email_still_passes_real_rfq_from_procurement():
    """Real RFQ from a procurement-domain sender still classifies True."""
    from src.agents.email_poller import is_rfq_email
    subject = "RFQ 12345 — Medical supplies needed"
    body = """Please provide quote per attached 704.

Anna Wilson
Senior Buyer
CalVet Procurement"""
    result = is_rfq_email(subject, body, attachments=[],
                          sender_email="a@calvet.ca.gov")
    assert result is True


def test_is_rfq_email_vetoes_credentialing_email():
    from src.agents.email_poller import is_rfq_email
    subject = "Vendor agreement attached"
    body = """Please review and sign.

Maria Lopez
Credentialing Coordinator"""
    result = is_rfq_email(subject, body, attachments=["vendor_agreement.pdf"],
                          sender_email="m@cchcs.ca.gov")
    assert result is False


def test_is_rfq_email_body_wins_over_704_pdf_attachment():
    """PR-AA strengthening 2026-05-13 — Mike: "always read body even
    past Tier 2." An AP signature + non-RFQ subject MUST veto even
    when a coincidentally 704-shaped filename is attached. The
    opposite case (a real RFQ with AP cosignatory) flows through the
    RFQ-strong-keyword override inside `is_non_rfq_team_email`."""
    from src.agents.email_poller import is_rfq_email
    subject = "Updated W-9 for vendor file"
    body = """Hi Reytech,

We're updating our vendor file and need a current W-9 from you.

Thanks,
Sarah Smith
Accounts Payable Specialist
California Department of Veterans Affairs"""
    result = is_rfq_email(subject, body,
                          attachments=["AMS_704B_25CB021.pdf"],
                          sender_email="sarah.smith@calvet.ca.gov")
    assert result is False, \
        "Body signature must veto even when an RFQ-shaped PDF filename is attached"


def test_is_rfq_email_real_rfq_with_ap_cosignatory_still_passes():
    """Real RFQ with AP cosignatory: RFQ-strong-keyword in body
    overrides the AP signature inside is_non_rfq_team_email itself."""
    from src.agents.email_poller import is_rfq_email
    subject = "RFQ 25CB021 — Medical supplies needed"
    body = """Please provide quote per attached 704.
Reply to procurement@calvet — AP will process payment after award.

Anna Wilson
Senior Buyer
CalVet Procurement
cc: Sarah Smith, Accounts Payable"""
    result = is_rfq_email(subject, body,
                          attachments=["AMS_704B_25CB021.pdf"],
                          sender_email="a@calvet.ca.gov")
    assert result is True
