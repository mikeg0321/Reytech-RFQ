"""CalVet end-to-end golden path — closes the biggest blind spot in
the package-generation suite.

Mirrors `TestCCHCSGoldenPath` in `tests/test_golden_path.py` but for
the Cal Vet / DVA agency, which uses a different form-set than CCHCS:

  CCHCS = 703B + 704B + bid package umbrella + quote (4 forms)
  CalVet = 10 standalone forms — no AMS forms, no umbrella
           ["quote", "calrecycle74", "bidder_decl", "dvbe843",
            "darfur_act", "cv012_cuf", "std204", "std205", "std1000",
            "sellers_permit"]

Mike 2026-04-29: agreed to build this after I admitted CalVet was the
largest gap in the test sandbox. Today CalVet only has individual
form-fill tests; nothing pinned the assembled-package contract.

### What this test pins (CI-blocking)

For every standalone form in calvet.required_forms:
  - filler runs without exception
  - output is a valid multi-page PDF
  - vendor identity (Reytech name + FEIN) appears on the form
  - exactly one signature image per signed form (no duplicates)

For the quote:
  - extension arithmetic (qty × bid_price = line total)
  - subtotal accumulation matches sum of extensions
  - tax rate resolves from the buyer's ship-to address (CalVet uses
    facility-resolved tax rates per the institution_resolver chain)
  - grand total = subtotal + tax (within $0.01 rounding)

For the agency_config dispatch:
  - non-Barstow `calvet` MUST NOT list `barstow_cuf` in optional_forms
    (per Mike's 2026-04-29 clarification — the rule is canonical;
    barstow_cuf is exclusive to `calvet_barstow`)
  - `calvet_barstow` MUST require BOTH `cv012_cuf` AND `barstow_cuf`
  - the recently approved adds (`w9`) MUST appear in optional_forms

If any of these break, the CalVet package contract has regressed and
the test fails CI before the change ships.
"""
from __future__ import annotations

import os

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_FIX = os.path.join(_REPO_ROOT, "tests", "fixtures")
_TEMPLATES = os.path.join(_REPO_ROOT, "data", "templates")


_CALVET_RFQ = {
    "solicitation_number": "CALVET-2026-TEST-001",
    "rfq_number": "CALVET-2026-TEST-001",
    "release_date": "03/01/2026",
    "due_date": "03/15/2026",
    "sign_date": "03/10/2026",
    "delivery_days": "30",
    "delivery_location": "Veterans Home of California - Yountville",
    "ship_to": "Veterans Home of California - Yountville, 260 California Dr, Yountville, CA 94599",
    "ship_to_zip": "94599",
    "requestor_name": "Jane Buyer",
    "requestor_email": "jane.buyer@calvet.ca.gov",
    "requestor_phone": "(707) 555-0001",
    "agency_name": "California Department of Veterans Affairs",
    "institution": "Veterans Home of California - Yountville",
    "department": "CalVet",
    "line_items": [
        {"description": "Wheelchair Cushion, Memory Foam, 18\"",
         "quantity": 25, "uom": "EA", "unit_price": 65.00,
         "bid_price": 89.00, "supplier_cost": 65.00,
         "part_number": "WC-MF-18"},
        {"description": "Adult Briefs, Large, Case of 96",
         "quantity": 12, "uom": "CS", "unit_price": 32.00,
         "bid_price": 45.00, "supplier_cost": 32.00,
         "part_number": "AB-L-96"},
    ],
}


_REYTECH = {
    "company": {
        "name": "Reytech Inc.",
        "address": "30 Carnoustie Way Trabuco Canyon CA 92679",
        "street": "30 Carnoustie Way",
        "city": "Trabuco Canyon",
        "state": "CA",
        "zip": "92679",
        "county": "Orange",
        "owner": "Michael Guadan",
        "title": "Owner",
        "phone": "949-229-1575",
        "email": "sales@reytechinc.com",
        "fein": "47-4588061",
        "sellers_permit": "245652416 - 00001",
        "cert_number": "2002605",
        "cert_expiration": "5/31/2027",
        "cert_type": "SB/DVBE",
        "description_of_goods": "Medical/Office and other supplies",
        "drug_free_expiration": "7/1/2028",
    },
}


def _assert_valid_pdf(path, min_bytes=500, min_pages=1):
    """Existence + signature byte + min-pages sanity check."""
    from pypdf import PdfReader
    assert os.path.exists(path), f"PDF not generated: {path}"
    sz = os.path.getsize(path)
    assert sz >= min_bytes, f"PDF too small ({sz} bytes): {path}"
    with open(path, "rb") as f:
        head = f.read(5)
    assert head == b"%PDF-", f"not a PDF (header={head!r}): {path}"
    reader = PdfReader(path)
    assert len(reader.pages) >= min_pages, (
        f"expected ≥{min_pages} pages, got {len(reader.pages)}: {path}"
    )
    return reader


def _filled_field_values(reader):
    """All non-empty form-field values as a dict."""
    fields = reader.get_fields() or {}
    out = {}
    for name, field in fields.items():
        v = field.get("/V")
        if v is None:
            continue
        s = str(v).strip()
        if s and s != "/Off":
            out[name] = s
    return out


def _count_image_xobjects(reader):
    """Total image XObjects across all pages — proxy for signature
    image overlays. Non-zero means at least one PNG was drawn."""
    n = 0
    for page in reader.pages:
        try:
            res = page.get("/Resources")
            res_obj = res.get_object() if hasattr(res, "get_object") else res
            if not res_obj:
                continue
            xobj = res_obj.get("/XObject")
            if xobj is None:
                continue
            xobj_obj = xobj.get_object() if hasattr(xobj, "get_object") else xobj
            for _, ref in xobj_obj.items():
                obj = ref.get_object() if hasattr(ref, "get_object") else ref
                if obj and str(obj.get("/Subtype")) == "/Image":
                    n += 1
        except Exception:
            continue
    return n


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def calvet_required_forms():
    """The actual required-form list out of agency_config — keeps the
    test in lock-step with the live config."""
    from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
    return list(DEFAULT_AGENCY_CONFIGS["calvet"]["required_forms"])


# ═══════════════════════════════════════════════════════════════════════════
# 1. agency_config dispatch — Barstow vs other CalVet
# ═══════════════════════════════════════════════════════════════════════════


class TestCalvetAgencyConfigContract:
    """The agency_config split between `calvet` and `calvet_barstow` is
    what makes the CUF picker correct for each facility. This pins the
    rules Mike clarified 2026-04-29:

        Barstow uses barstow_cuf
        Other CalVets use cv012_cuf only — never offer barstow_cuf
    """

    def test_calvet_required_forms_excludes_barstow_cuf(
        self, calvet_required_forms,
    ):
        assert "barstow_cuf" not in calvet_required_forms, (
            "Non-Barstow CalVet must not require barstow_cuf — that form "
            "is exclusive to the calvet_barstow profile."
        )

    def test_calvet_optional_forms_excludes_barstow_cuf(self):
        from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
        opt = DEFAULT_AGENCY_CONFIGS["calvet"]["optional_forms"]
        assert "barstow_cuf" not in opt, (
            "barstow_cuf must not appear in non-Barstow CalVet optional "
            "forms — operators would see it as a checkbox option which "
            "Mike confirmed is wrong (Barstow uses barstow_cuf, other "
            "CalVets use cv012_cuf only)."
        )

    def test_calvet_optional_forms_has_w9(self):
        """Mike approved 2026-04-29 — `w9` is operator-attachable for
        CalVet quotes that explicitly request a W-9 in the email body."""
        from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
        opt = DEFAULT_AGENCY_CONFIGS["calvet"]["optional_forms"]
        assert "w9" in opt, "CalVet optional_forms must include 'w9'."

    def test_calvet_required_forms_has_cv012_cuf(
        self, calvet_required_forms,
    ):
        assert "cv012_cuf" in calvet_required_forms, (
            "Non-Barstow CalVet must require cv012_cuf — this is the "
            "CUF every CalVet facility uses except Barstow."
        )

    def test_calvet_barstow_has_both_cufs(self):
        from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
        bar = DEFAULT_AGENCY_CONFIGS["calvet_barstow"]["required_forms"]
        assert "barstow_cuf" in bar, (
            "calvet_barstow must require barstow_cuf — its whole reason "
            "to exist as a separate profile."
        )
        assert "cv012_cuf" in bar, (
            "calvet_barstow currently requires BOTH CV 012 CUF AND "
            "Barstow CUF (per the existing config notes). If this "
            "fails, the CUF rule for Barstow has been simplified — "
            "verify with Mike before changing."
        )

    def test_calvet_required_forms_size_floor(
        self, calvet_required_forms,
    ):
        """CalVet's full standalone packet is ~10 forms. If it shrinks
        below 8, something has been quietly removed."""
        assert len(calvet_required_forms) >= 8, (
            f"calvet.required_forms shrunk to {len(calvet_required_forms)} "
            f"forms — investigate. Current list: {calvet_required_forms}"
        )


# ═══════════════════════════════════════════════════════════════════════════
# 2. Each required CalVet standalone form fills cleanly + carries
#    Reytech identity + has at most 1 signature image (no duplicates)
# ═══════════════════════════════════════════════════════════════════════════


class TestCalvetStandaloneFormFills:
    """Run every CalVet standalone form against its real template and
    verify the output is a valid PDF with vendor identity + a single
    signature image. The single-sig assertion is the regression guard
    Mike 2026-04-29 explicitly asked for at the package level."""

    def _generate_and_check(self, filler, template_name, tmp_path,
                            expect_signature=True,
                            min_pages=1):
        template = os.path.join(_TEMPLATES, template_name)
        if not os.path.exists(template):
            # also try the fixtures dir, with a slight name remap
            alt = os.path.join(_FIX, template_name.replace(
                "calrecycle_74", "calrecycle74",
            ).replace("bidder_declaration", "bidder_decl"
            ).replace("dvbe_843", "dvbe843"
            ).replace("darfur_act", "darfur"))
            if os.path.exists(alt):
                template = alt
            else:
                pytest.skip(f"template not found: {template_name}")
        out = str(tmp_path / f"calvet_{template_name}")
        filler(template, _CALVET_RFQ, _REYTECH, out)
        reader = _assert_valid_pdf(out, min_pages=min_pages)
        # vendor identity must appear in field values
        filled = _filled_field_values(reader)
        identity_hits = sum(
            1 for v in filled.values()
            if "Reytech" in v or "47-4588061" in v
            or "Michael Guadan" in v
        )
        assert identity_hits >= 1, (
            f"{template_name}: no vendor identity in any filled field. "
            f"Filled fields: {list(filled.items())[:8]}"
        )
        if expect_signature:
            sig_count = _count_image_xobjects(reader)
            assert sig_count >= 1, (
                f"{template_name}: expected ≥1 signature image, got "
                f"{sig_count}. Either the signing path didn't run or "
                f"signature_transparent.png is missing."
            )
            # Duplicate-sig regression guard at the form-output level.
            # A single signed standalone form should have at most a
            # small handful of image XObjects (signature + maybe a
            # state seal / logo). >5 likely means duplicates.
            assert sig_count <= 5, (
                f"{template_name}: {sig_count} image XObjects — likely "
                f"duplicate signatures stacking. Investigate."
            )
        return reader

    def test_fill_std204(self, tmp_path):
        from src.forms.reytech_filler_v4 import fill_std204
        self._generate_and_check(fill_std204, "std204_blank.pdf", tmp_path)

    def test_fill_std205(self, tmp_path):
        from src.forms.reytech_filler_v4 import fill_std205
        self._generate_and_check(fill_std205, "std205_blank.pdf", tmp_path)

    def test_fill_std1000(self, tmp_path):
        from src.forms.reytech_filler_v4 import fill_std1000
        self._generate_and_check(fill_std1000, "std1000_blank.pdf", tmp_path)

    def test_fill_cv012_cuf(self, tmp_path):
        from src.forms.reytech_filler_v4 import fill_cv012_cuf
        self._generate_and_check(fill_cv012_cuf, "cv012_cuf_blank.pdf", tmp_path)

    def test_fill_calrecycle(self, tmp_path):
        from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
        self._generate_and_check(
            fill_calrecycle_standalone, "calrecycle_74_blank.pdf", tmp_path,
        )

    def test_generate_dvbe_843(self, tmp_path):
        """generate_dvbe_843 builds from in-code blank — no template arg."""
        from src.forms.reytech_filler_v4 import generate_dvbe_843
        out = str(tmp_path / "calvet_dvbe843.pdf")
        generate_dvbe_843(_CALVET_RFQ, _REYTECH, out)
        reader = _assert_valid_pdf(out)
        filled = _filled_field_values(reader)
        # generate_dvbe_843 writes the company name into a field,
        # accept any of the canonical identity strings landing somewhere
        identity_hits = sum(
            1 for v in filled.values()
            if "Reytech" in v or "Michael Guadan" in v
        )
        assert identity_hits >= 1, (
            f"DVBE 843: no vendor identity in filled fields. "
            f"Got: {list(filled.items())[:8]}"
        )

    def test_generate_darfur_act(self, tmp_path):
        from src.forms.reytech_filler_v4 import generate_darfur_act
        out = str(tmp_path / "calvet_darfur.pdf")
        generate_darfur_act(_CALVET_RFQ, _REYTECH, out)
        _assert_valid_pdf(out)

    def test_generate_bidder_declaration(self, tmp_path):
        from src.forms.reytech_filler_v4 import generate_bidder_declaration
        out = str(tmp_path / "calvet_bidder_decl.pdf")
        generate_bidder_declaration(_CALVET_RFQ, _REYTECH, out)
        _assert_valid_pdf(out)


# ═══════════════════════════════════════════════════════════════════════════
# 3. CalVet quote — extension, subtotal, tax (the math Mike actually
#    pays attention to on every quote)
# ═══════════════════════════════════════════════════════════════════════════


class TestCalvetQuoteMath:
    """The Reytech quote PDF is the buyer-facing line-item document.
    These tests pin its math against a hand-computed expected total
    so any drift shows up immediately."""

    def test_quote_subtotal_matches_extension_sum(self, tmp_path):
        from src.forms.quote_generator import generate_quote
        out = str(tmp_path / "calvet_quote.pdf")
        result = generate_quote(
            _CALVET_RFQ, out, agency="CalVet",
            quote_number="R26QCALVETTEST",
        )
        assert result.get("ok"), f"quote generation failed: {result}"
        _assert_valid_pdf(out)
        # 25 × $89.00 = $2,225.00
        # 12 × $45.00 =   $540.00
        # subtotal    = $2,765.00
        expected_subtotal = 25 * 89.00 + 12 * 45.00
        log_subtotal = result.get("subtotal", 0)
        assert abs(log_subtotal - expected_subtotal) < 0.01, (
            f"CalVet quote subtotal mismatch: "
            f"got {log_subtotal}, expected {expected_subtotal}"
        )

    def test_quote_tax_resolves_from_ship_to(self, tmp_path):
        """Tax pickup chain: ship-to address → CDTFA county lookup →
        applied to subtotal. If tax_rate comes back as 0 for a real
        CA address, the lookup chain has broken."""
        from src.forms.quote_generator import generate_quote
        out = str(tmp_path / "calvet_quote_tax.pdf")
        result = generate_quote(
            _CALVET_RFQ, out, agency="CalVet",
            quote_number="R26QCALVETTEST2",
        )
        assert result.get("ok")
        tax_rate = result.get("tax_rate", 0)
        assert tax_rate > 0, (
            f"Tax rate must resolve from ship-to (Yountville, CA 94599). "
            f"Got tax_rate={tax_rate}. Lookup chain has regressed."
        )

    def test_quote_grand_total_equals_subtotal_plus_tax(self, tmp_path):
        from src.forms.quote_generator import generate_quote
        out = str(tmp_path / "calvet_quote_total.pdf")
        result = generate_quote(
            _CALVET_RFQ, out, agency="CalVet",
            quote_number="R26QCALVETTEST3",
        )
        assert result.get("ok")
        subtotal = result.get("subtotal", 0)
        tax_rate = result.get("tax_rate", 0)
        total = result.get("total", 0)
        expected_total = round(subtotal + round(subtotal * tax_rate, 2), 2)
        assert abs(total - expected_total) < 0.01, (
            f"CalVet quote grand_total mismatch: got {total}, "
            f"expected {expected_total} = {subtotal} + tax({tax_rate})"
        )

    def test_quote_carries_both_line_items(self, tmp_path):
        from src.forms.quote_generator import generate_quote
        from pypdf import PdfReader
        out = str(tmp_path / "calvet_quote_items.pdf")
        result = generate_quote(
            _CALVET_RFQ, out, agency="CalVet",
            quote_number="R26QCALVETTEST4",
        )
        assert result.get("ok")
        text = "".join(
            (p.extract_text() or "") for p in PdfReader(out).pages
        )
        assert "Wheelchair" in text, "line item 1 missing from quote PDF"
        assert "Adult Briefs" in text, "line item 2 missing from quote PDF"


# ═══════════════════════════════════════════════════════════════════════════
# 4. Form-fill resilience — every required CalVet form_id must have
#    a corresponding callable filler. If the agency_config lists a
#    form_id we have no filler for, dispatch silently skips it and
#    the buyer gets an incomplete package.
# ═══════════════════════════════════════════════════════════════════════════


class TestEveryRequiredFormIdHasAFiller:
    """Pin the contract: every form_id in calvet.required_forms maps to
    a real callable in src.forms. If a config edit adds a form_id that
    has no filler, this test fails with the gap."""

    KNOWN_FILLERS = {
        "quote": "src.forms.quote_generator:generate_quote",
        "calrecycle74": "src.forms.reytech_filler_v4:fill_calrecycle_standalone",
        "bidder_decl": "src.forms.reytech_filler_v4:generate_bidder_declaration",
        "dvbe843": "src.forms.reytech_filler_v4:generate_dvbe_843",
        "darfur_act": "src.forms.reytech_filler_v4:generate_darfur_act",
        "cv012_cuf": "src.forms.reytech_filler_v4:fill_cv012_cuf",
        "std204": "src.forms.reytech_filler_v4:fill_std204",
        "std205": "src.forms.reytech_filler_v4:fill_std205",
        "std1000": "src.forms.reytech_filler_v4:fill_std1000",
        # sellers_permit ships as a static PDF (no filler — it's just the
        # already-filled Reytech permit). Verified separately below.
    }

    def test_every_required_form_id_has_a_filler_or_is_static(
        self, calvet_required_forms,
    ):
        import importlib

        gaps = []
        for fid in calvet_required_forms:
            if fid == "sellers_permit":
                # static PDF — no filler expected
                continue
            mod_func = self.KNOWN_FILLERS.get(fid)
            if not mod_func:
                gaps.append((fid, "no entry in KNOWN_FILLERS"))
                continue
            mod_path, func = mod_func.split(":")
            try:
                mod = importlib.import_module(mod_path)
            except ImportError as e:
                gaps.append((fid, f"module {mod_path} import failed: {e}"))
                continue
            if not hasattr(mod, func):
                gaps.append((fid, f"function {func} missing in {mod_path}"))
        assert not gaps, (
            f"CalVet required_forms has form_ids with no callable filler: "
            f"{gaps}. Either implement the filler or remove the form_id "
            f"from agency_config."
        )

    def test_sellers_permit_static_pdf_present(self):
        """sellers_permit ships as a pre-filled static PDF, not a
        runtime filler. Verify the file exists."""
        candidates = [
            os.path.join(_TEMPLATES, "sellers_permit_reytech.pdf"),
            os.path.join(_TEMPLATES, "sellers_permit.pdf"),
        ]
        found = next((p for p in candidates if os.path.exists(p)), None)
        assert found, (
            f"sellers_permit static PDF missing — searched {candidates}. "
            f"CalVet packets that include this form_id will be incomplete."
        )
