"""
Tests for quote_generator.py: agency detection, numbering, PDF output, ASIN flow.

Verified 2026-02-14:
    generate_quote(data, path, agency=, quote_number=, tax_rate=, include_tax=,
                   shipping=, terms=, expiry_days=) -> {ok, path, quote_number, ...}
    _detect_agency(data) -> str (CDCR, CCHCS, CalVet, DGS, DEFAULT)
"""
import os
import json
import pytest

from quote_generator import (
    generate_quote, generate_quote_from_pc, generate_quote_from_rfq,
    _detect_agency, _next_quote_number, peek_next_quote_number,
    get_all_quotes, search_quotes, AGENCY_CONFIGS,
    update_quote_status, get_quote_stats, set_quote_counter,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Agency Detection
# ═══════════════════════════════════════════════════════════════════════════════

class TestAgencyDetection:

    def test_cdcr_from_corrections(self):
        assert _detect_agency({"institution": "Dept of Corrections"}) == "CDCR"

    def test_cchcs_from_name(self):
        assert _detect_agency({"institution": "CCHCS"}) == "CCHCS"

    def test_cchcs_from_department(self):
        assert _detect_agency({"department": "CCHCS Healthcare"}) == "CCHCS"

    def test_calvet(self):
        assert _detect_agency({"institution": "Veterans Affairs Home"}) == "CalVet"

    def test_dgs(self):
        assert _detect_agency({"institution": "Department of General Services"}) == "DGS"

    def test_default_for_unknown(self):
        assert _detect_agency({"institution": "Random Org"}) == "DEFAULT"

    def test_case_insensitive(self):
        assert _detect_agency({"institution": "cdcr facility"}) == "CDCR"

    # CDCR prison abbreviation detection (bug fix 2026-02-14)
    def test_csp_sacramento(self):
        assert _detect_agency({"institution": "CSP-Sacramento"}) == "CDCR"

    def test_csp_corcoran(self):
        assert _detect_agency({"institution": "CSP-Corcoran"}) == "CDCR"

    def test_scc_conservation(self):
        assert _detect_agency({"institution": "SCC - Sierra Conservation Center"}) == "CDCR"

    def test_cim_standalone(self):
        assert _detect_agency({"institution": "CIM"}) == "CDCR"

    def test_folsom_state_prison(self):
        assert _detect_agency({"institution": "Folsom State Prison"}) == "CDCR"

    def test_san_quentin(self):
        assert _detect_agency({"institution": "San Quentin State Prison"}) == "CDCR"

    def test_satf(self):
        assert _detect_agency({"institution": "SATF Corcoran"}) == "CDCR"

    def test_chcf_is_cchcs(self):
        """CHCF (California Health Care Facility) is healthcare, not prison."""
        assert _detect_agency({"institution": "CHCF - California Health Care Facility"}) == "CCHCS"


# ═══════════════════════════════════════════════════════════════════════════════
# Agency Configs
# ═══════════════════════════════════════════════════════════════════════════════

class TestAgencyConfigs:

    def test_cchcs_no_bill_to_no_permit(self):
        cfg = AGENCY_CONFIGS["CCHCS"]
        assert cfg["show_bill_to"] is False
        assert cfg["show_permit"] is False

    def test_cdcr_has_bill_to_and_permit(self):
        cfg = AGENCY_CONFIGS["CDCR"]
        assert cfg["show_bill_to"] is True
        assert cfg["show_permit"] is True
        assert any("P.O. Box" in ln for ln in cfg.get("bill_to_lines", []))

    def test_calvet_has_bill_to(self):
        cfg = AGENCY_CONFIGS["CalVet"]
        assert cfg["show_bill_to"] is True

    def test_all_configs_have_required_keys(self):
        for name, cfg in AGENCY_CONFIGS.items():
            for key in ("show_bill_to", "show_permit"):
                assert key in cfg, f"{name} missing {key}"


# ═══════════════════════════════════════════════════════════════════════════════
# Quote Numbering
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteNumbering:

    def test_format_r_yy_q_n(self):
        num = _next_quote_number()
        assert num.startswith("R")
        assert "Q" in num
        year_part = num[1:3]
        assert year_part.isdigit()
        seq_part = num.split("Q")[1]
        assert seq_part.isdigit()

    def test_sequential(self):
        n1 = _next_quote_number()
        n2 = _next_quote_number()
        seq1 = int(n1.split("Q")[1])
        seq2 = int(n2.split("Q")[1])
        assert seq2 == seq1 + 1

    def test_peek_is_idempotent(self):
        p1 = peek_next_quote_number()
        p2 = peek_next_quote_number()
        assert p1 == p2

    def test_peek_matches_next(self):
        peeked = peek_next_quote_number()
        actual = _next_quote_number()
        assert peeked == actual


# ═══════════════════════════════════════════════════════════════════════════════
# PDF Generation
# ═══════════════════════════════════════════════════════════════════════════════

class TestGenerateQuote:

    def test_basic(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "test.pdf")
        r = generate_quote(sample_stryker_quote, out,
                           agency="CDCR", quote_number="R26Q99")
        assert r["ok"] is True
        assert os.path.exists(out)
        assert os.path.getsize(out) > 1000
        assert r["quote_number"] == "R26Q99"

    def test_totals_math(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "t.pdf")
        r = generate_quote(sample_stryker_quote, out,
                           quote_number="T1", include_tax=False)
        # 454.40*2 + 69.12*2 + 69.12*2 = 908.80 + 138.24 + 138.24 = 1185.28
        assert r["subtotal"] == pytest.approx(1185.28, abs=0.01)
        assert r["tax"] == 0.0
        assert r["total"] == r["subtotal"]

    def test_with_tax(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "tax.pdf")
        r = generate_quote(sample_stryker_quote, out,
                           quote_number="TAX1", include_tax=True, tax_rate=0.0725)
        assert r["tax"] > 0
        assert r["total"] == pytest.approx(r["subtotal"] + r["tax"], abs=0.02)

    def test_shipping(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "ship.pdf")
        r = generate_quote(sample_stryker_quote, out,
                           quote_number="SH1", include_tax=False, shipping=25.00)
        assert r["shipping"] == 25.00
        assert r["total"] == r["subtotal"] + 25.00

    def test_empty_items(self, tmp_path):
        data = {"institution": "Test", "line_items": []}
        out = str(tmp_path / "empty.pdf")
        r = generate_quote(data, out, quote_number="E1")
        assert r["ok"] is True
        assert r["total"] == 0.0
        assert r["items_count"] == 0

    def test_items_count(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "cnt.pdf")
        r = generate_quote(sample_stryker_quote, out, quote_number="C1")
        assert r["items_count"] == 3


# ═══════════════════════════════════════════════════════════════════════════════
# generate_quote_from_pc — ASIN + address matching
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteFromPC:

    def test_asin_in_output_pdf(self, tmp_path, sample_pc):
        out = str(tmp_path / "pc.pdf")
        r = generate_quote_from_pc(sample_pc, out, quote_number="PC1")
        assert r["ok"]
        import pdfplumber
        with pdfplumber.open(out) as pdf:
            text = pdf.pages[0].extract_text()
        assert "B07TEST123" in text       # ASIN in MFG PART # column
        assert "Ref ASIN:" in text         # ASIN appended to description

    def test_addresses_in_pdf(self, tmp_path, sample_pc):
        """Both To: and Ship To: should contain the ship address."""
        out = str(tmp_path / "addr.pdf")
        r = generate_quote_from_pc(sample_pc, out, quote_number="AD1")
        import pdfplumber
        with pdfplumber.open(out) as pdf:
            text = pdf.pages[0].extract_text()
        # "100 Prison Road" should appear in both To and Ship To sections
        assert text.count("100 Prison Road") >= 2

    def test_no_bid_excluded(self, tmp_path, sample_pc):
        sample_pc["items"][1]["no_bid"] = True
        out = str(tmp_path / "nb.pdf")
        r = generate_quote_from_pc(sample_pc, out, quote_number="NB1")
        assert r["items_count"] == 1

    def test_uses_recommended_price(self, tmp_path, sample_pc):
        out = str(tmp_path / "rp.pdf")
        r = generate_quote_from_pc(sample_pc, out, quote_number="RP1",
                                   include_tax=False)
        # item 1: 22 * 15.72 = 345.84, item 2: 5 * 53.74 = 268.70
        assert r["subtotal"] == pytest.approx(345.84 + 268.70, abs=0.02)


# ═══════════════════════════════════════════════════════════════════════════════
# generate_quote_from_rfq
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteFromRFQ:

    def test_basic(self, tmp_path, sample_rfq):
        out = str(tmp_path / "rfq.pdf")
        r = generate_quote_from_rfq(sample_rfq, out, quote_number="RFQ1")
        assert r["ok"]
        assert r["items_count"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# Quotes Log (searchable database)
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuotesLog:

    def test_logged_after_generation(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "log.pdf")
        generate_quote(sample_stryker_quote, out, quote_number="LOG1")
        quotes = get_all_quotes()
        assert any(q["quote_number"] == "LOG1" for q in quotes)

    def test_search_by_number(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "s.pdf")
        generate_quote(sample_stryker_quote, out,
                       quote_number="SRCH1", agency="CDCR")
        results = search_quotes(query="SRCH1")
        assert len(results) >= 1

    def test_search_by_agency(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "a.pdf")
        generate_quote(sample_stryker_quote, out,
                       quote_number="AG1", agency="CalVet")
        results = search_quotes(agency="CalVet")
        assert all(r["agency"] == "CalVet" for r in results)

    def test_logged_with_pending_status(self, tmp_path, sample_stryker_quote):
        out = str(tmp_path / "st.pdf")
        generate_quote(sample_stryker_quote, out, quote_number="ST1")
        quotes = get_all_quotes()
        q = next(q for q in quotes if q["quote_number"] == "ST1")
        assert q["status"] == "pending"


# ═══════════════════════════════════════════════════════════════════════════════
# Win/Loss Tracking
# ═══════════════════════════════════════════════════════════════════════════════

class TestWinLossTracking:

    def _gen(self, tmp_path, sample_stryker_quote, qn):
        out = str(tmp_path / f"{qn}.pdf")
        generate_quote(sample_stryker_quote, out, quote_number=qn)

    def test_mark_won(self, tmp_path, sample_stryker_quote):
        self._gen(tmp_path, sample_stryker_quote, "WON1")
        ok = update_quote_status("WON1", "won", po_number="PO-12345")
        assert ok is True
        q = next(q for q in get_all_quotes() if q["quote_number"] == "WON1")
        assert q["status"] == "won"
        assert q["po_number"] == "PO-12345"

    def test_mark_lost(self, tmp_path, sample_stryker_quote):
        self._gen(tmp_path, sample_stryker_quote, "LOST1")
        ok = update_quote_status("LOST1", "lost", notes="Price too high")
        assert ok is True
        q = next(q for q in get_all_quotes() if q["quote_number"] == "LOST1")
        assert q["status"] == "lost"
        assert q["status_notes"] == "Price too high"

    def test_mark_back_to_pending(self, tmp_path, sample_stryker_quote):
        self._gen(tmp_path, sample_stryker_quote, "PEND1")
        update_quote_status("PEND1", "won")
        update_quote_status("PEND1", "pending")
        q = next(q for q in get_all_quotes() if q["quote_number"] == "PEND1")
        assert q["status"] == "pending"

    def test_invalid_status(self, tmp_path, sample_stryker_quote):
        self._gen(tmp_path, sample_stryker_quote, "INV1")
        ok = update_quote_status("INV1", "invalid_status")
        assert ok is False

    def test_nonexistent_quote(self):
        ok = update_quote_status("FAKE999", "won")
        assert ok is False

    def test_filter_by_status(self, tmp_path, sample_stryker_quote):
        self._gen(tmp_path, sample_stryker_quote, "FW1")
        self._gen(tmp_path, sample_stryker_quote, "FL1")
        update_quote_status("FW1", "won")
        update_quote_status("FL1", "lost")
        won = search_quotes(status="won")
        assert any(q["quote_number"] == "FW1" for q in won)
        assert not any(q["quote_number"] == "FL1" for q in won)

    def test_stats(self, tmp_path, sample_stryker_quote):
        self._gen(tmp_path, sample_stryker_quote, "S1")
        self._gen(tmp_path, sample_stryker_quote, "S2")
        self._gen(tmp_path, sample_stryker_quote, "S3")
        update_quote_status("S1", "won")
        update_quote_status("S2", "lost")
        stats = get_quote_stats()
        assert stats["won"] >= 1
        assert stats["lost"] >= 1
        assert stats["pending"] >= 1
        assert stats["win_rate"] > 0


# ═══════════════════════════════════════════════════════════════════════════════
# Quote Number Format — R{YY}Q{seq}
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuoteNumberFormat:

    def test_has_q_separator(self):
        num = _next_quote_number()
        assert "Q" in num  # R{YY}Q{seq} format

    def test_starts_with_r_and_year(self):
        num = _next_quote_number()
        assert num.startswith("R")
        assert "Q" in num

    def test_set_counter(self):
        set_quote_counter(99)
        num = _next_quote_number()
        assert num.endswith("Q100")

    def test_lock_in_reuses_number(self, tmp_path, sample_stryker_quote):
        """Passing an existing quote_number should reuse it, not consume a new one."""
        peek_before = peek_next_quote_number()
        out = str(tmp_path / "lock.pdf")
        r = generate_quote(sample_stryker_quote, out, quote_number="R26Q99")
        peek_after = peek_next_quote_number()
        assert r["quote_number"] == "R26Q99"
        assert peek_before == peek_after  # counter unchanged
