"""AMS 703A bidder-information fill — Coleman 10842771 (2026-05-29).

The 703A shipped with a BLANK BIDDER INFORMATION block (empty Business Name /
Address / FEIN / Seller's Permit) because fill_703a relied SOLELY on mirror-fill
from a prior 703B submission and, when none existed, copied the buyer's blank input
unchanged. These tests pin that the bidder block now fills directly from config
whether or not a prior 703B exists.
"""
from __future__ import annotations

import os
import pytest

pytest.importorskip("pypdf")
from pypdf import PdfReader

FIXTURE = os.path.join("data", "templates", "703a_blank.pdf")

_CONFIG = {"company": {
    "name": "Reytech Inc.",
    "address": "30 Carnoustie Way Trabuco Canyon CA 92679",
    "owner": "Michael Guadan", "title": "Owner",
    "phone": "949-229-1575", "email": "sales@reytechinc.com",
    "fein": "47-4588061", "sellers_permit": "245652416 - 00001",
    "cert_number": "2002605", "cert_expiration": "5/31/2027",
}}
_RFQ = {"solicitation_number": "10842771", "due_date": "05/29/2026",
        "release_date": "05/27/2026", "sign_date": "05/29/2026"}

_EXPECTED = {
    "703A_Business Name": "Reytech Inc.",
    "703A_Contact Person": "Michael Guadan",
    "703A_Title": "Owner",
    "703A_Phone": "949-229-1575",
    "703A_Email": "sales@reytechinc.com",
    "703A_Federal Employer Identification Number FEIN": "47-4588061",
    "703A_Retailers CA Sellers Permit Number": "245652416 - 00001",
}


def _vals(pdf_path):
    f = PdfReader(pdf_path).get_fields() or {}
    return {k: str((f.get(k) or {}).get("/V", "")).strip() for k in f}


@pytest.mark.skipif(not os.path.exists(FIXTURE), reason="703a_blank.pdf fixture missing")
def test_703a_bidder_block_fills_without_prior(tmp_path, monkeypatch):
    """No prior 703B → fill_703a must fill the bidder block DIRECTLY from config
    (the exact Coleman 10842771 condition), not ship blank."""
    from src.forms import reytech_filler_v4 as r4
    # Force the no-prior branch.
    monkeypatch.setattr("src.forms.prior_submissions.latest_for", lambda *a, **k: None)
    out = str(tmp_path / "703a_noprior.pdf")
    r4.fill_703a(FIXTURE, dict(_RFQ), _CONFIG, out)
    v = _vals(out)
    blanks = [k for k, exp in _EXPECTED.items() if v.get(k, "") == ""]
    assert not blanks, f"703A bidder fields shipped BLANK with no prior: {blanks}"
    for k, exp in _EXPECTED.items():
        assert v[k] == exp, f"{k}: expected {exp!r}, got {v[k]!r}"
    assert "Carnoustie" in v.get("703A_Address", ""), "703A_Address not filled"


@pytest.mark.skipif(not os.path.exists(FIXTURE), reason="703a_blank.pdf fixture missing")
def test_703a_solicitation_and_dates_fill(tmp_path, monkeypatch):
    from src.forms import reytech_filler_v4 as r4
    monkeypatch.setattr("src.forms.prior_submissions.latest_for", lambda *a, **k: None)
    out = str(tmp_path / "703a_dates.pdf")
    r4.fill_703a(FIXTURE, dict(_RFQ), _CONFIG, out)
    v = _vals(out)
    assert "10842771" in v.get("703A_Solicitation Number", "")
    assert v.get("703A_BidExpirationDate", "") != ""  # 45-day expiry computed
