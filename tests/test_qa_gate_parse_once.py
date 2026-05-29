"""Parse-once pins for the two post-merge QA gates.

Both gates re-parsed large PDFs redundantly (the dominant cost of package
generation after the bid-package fill, per the 2026-05-29 trace tr_b8466bfd):

  * package_integrity.check_package called _extract_pages TWICE (once in
    detect_duplicate_forms, once in find_blank_bidder_info) on the same
    ~9 MB merged package.
  * pricing_alignment.check_alignment called extract_pdf_totals once PER
    form-id, and the route lists the 15-page CCHCS bid package under up to
    4 form-ids (bidpkg + calrecycle74/sellers_permit/dvbe843 aliases, all
    the same file) — so it was parsed up to 4×.

These tests pin: the dedup is parse-once AND result-identical to the prior
self-extraction. Pure performance — no behavior change.
"""
from __future__ import annotations

import src.forms.package_integrity as pi
import src.forms.pricing_alignment as pa


# ── package_integrity ────────────────────────────────────────────────


def test_check_package_extracts_pages_once(monkeypatch):
    """check_package must parse the package exactly ONCE, then feed both
    detectors from that single extraction."""
    calls = []

    def fake_extract(path):
        calls.append(path)
        return ["Reytech Inc — bidder block filled", "distinct page two content here"]

    monkeypatch.setattr(pi, "_extract_pages", fake_extract)
    res = pi.check_package("/fake/merged.pdf", company_name="Reytech Inc")
    assert calls == ["/fake/merged.pdf"], f"expected 1 extraction, got {len(calls)}"
    assert res["ok"] is True
    assert res["bidder_info_present"] is True


def test_detect_duplicate_forms_param_matches_self_extract(monkeypatch):
    """Passing pages= yields the SAME result as letting the function extract."""
    canned = [
        "DUPLICATED FORM body long enough to exceed the blank threshold xxxxx",
        "DUPLICATED FORM body long enough to exceed the blank threshold xxxxx",
        "A DIFFERENT FORM with its own distinct content yyyyy zzzzz wwwww",
    ]
    monkeypatch.setattr(pi, "_extract_pages", lambda p: list(canned))
    self_extracted = pi.detect_duplicate_forms("x.pdf")
    via_param = pi.detect_duplicate_forms("x.pdf", pages=list(canned))
    assert self_extracted == via_param
    assert len(self_extracted) == 1  # the duplicated form is detected


def test_find_blank_bidder_info_param_matches_self_extract(monkeypatch):
    canned = ["nothing relevant on page one", "R E Y T E C H I N C spaced out"]
    monkeypatch.setattr(pi, "_extract_pages", lambda p: list(canned))
    self_extracted = pi.find_blank_bidder_info("x.pdf", "Reytech Inc")
    via_param = pi.find_blank_bidder_info("x.pdf", "Reytech Inc", pages=list(canned))
    assert self_extracted == via_param
    assert self_extracted["present"] is True  # space-tolerant match


# ── pricing_alignment ────────────────────────────────────────────────


def test_check_alignment_parses_each_unique_file_once(monkeypatch):
    """The same physical file listed under multiple form-ids must be parsed
    ONCE (the CCHCS bid-package-alias case)."""
    calls = []

    def fake_totals(path, form_id=""):
        calls.append(path)
        return None  # header-only/no-totals form — still counts the parse

    monkeypatch.setattr(pa, "extract_pdf_totals", fake_totals)
    rfq = {"line_items": [{"qty": 1, "unit_price": 10.0, "description": "x"}],
           "tax_enabled": False}
    bidpkg = "/out/10842771_BidPackage_Reytech.pdf"
    quote = "/out/10842771_Quote_Reytech.pdf"
    files = [
        ("bidpkg", bidpkg, "Bid Package"),
        ("calrecycle74", bidpkg, "CalRecycle (via bidpkg)"),
        ("sellers_permit", bidpkg, "Seller's Permit (via bidpkg)"),
        ("dvbe843", bidpkg, "DVBE 843 (via bidpkg)"),
        ("quote", quote, "Reytech Quote"),
    ]
    result = pa.check_alignment(rfq, files)
    assert calls.count(bidpkg) == 1, f"bid package parsed {calls.count(bidpkg)}× (want 1)"
    assert calls.count(quote) == 1
    assert len(calls) == 2  # two UNIQUE files, not five entries
    # by_form still records every form-id (behavior preserved)
    assert set(result["by_form"]) == {"bidpkg", "calrecycle74", "sellers_permit",
                                      "dvbe843", "quote"}


def test_check_alignment_dedup_preserves_blockers(monkeypatch):
    """Caching by path does not change blocker/warning output: a divergent file
    repeated under N form-ids still produces a blocker per form-id, exactly as
    before (the loop still visits each form-id; only the parse is reused)."""
    def fake_totals(path, form_id=""):
        return {"subtotal": 999.0, "tax": None, "total": None, "line_count": 1}

    monkeypatch.setattr(pa, "extract_pdf_totals", fake_totals)
    rfq = {"line_items": [{"qty": 1, "unit_price": 10.0, "description": "x"}],
           "tax_enabled": False}  # canonical subtotal = 10.00, PDF says 999 → blocker
    f = "/out/dup.pdf"
    result = pa.check_alignment(rfq, [("a", f, "A"), ("b", f, "B")])
    # one subtotal blocker per form-id (a, b) — same as pre-dedup
    subtotal_blockers = [b for b in result["blockers"] if b.get("field") == "subtotal"]
    assert len(subtotal_blockers) == 2
