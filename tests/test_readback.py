"""Read-back verifier tests — catch fills that silently dropped data.

The verifier reuses fill_engine's own field map builders as the source of
truth for "what the filler intended to write" and then diffs each expected
write against the output PDF. These tests cover the happy path and the
fail modes that matter:

  * All fields read back non-empty → passed=True
  * Static field blank in output but expected → passed=False + miss
  * Row field blank in output but expected → passed=False + miss
  * Quote value empty → field skipped (not counted, not a miss)
  * Corrupt PDF bytes → passed=False with readable error
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from src.core.quote_model import Quote, DocType, LineItem
from src.forms.profile_registry import load_profiles
from src.forms.readback import verify_readback, ReadbackReport


def _priced_quote() -> Quote:
    q = Quote(
        doc_type=DocType.PC,
        line_items=[
            LineItem(
                line_no=1, description="Gauze rolls",
                qty=10, unit_cost=Decimal("2.50"),
                unit_price=Decimal("2.50"), extension=Decimal("25.00"),
            ),
        ],
    )
    q.header.solicitation_number = "R26Q0042"
    q.header.institution_key = "cchcs"
    q.vendor.name = "Reytech Inc."
    q.vendor.representative = "Michael Guadan"
    q.vendor.email = "sales@reytechinc.com"
    q.vendor.phone = "916-555-0100"
    q.vendor.sb_cert = "SB-12345"
    q.buyer.requestor_name = "Jane Buyer"
    q.buyer.requestor_phone = "916-555-0200"
    return q


def _fill_and_read(quote: Quote, profile_id: str = "704a_reytech_standard"):
    from src.forms.fill_engine import fill
    profiles = load_profiles()
    profile = profiles[profile_id]
    pdf_bytes = fill(quote, profile)
    return pdf_bytes, profile


class TestReadbackHappyPath:
    def test_all_fields_readback_ok(self):
        """A normally-filled PDF should pass readback verification."""
        quote = _priced_quote()
        pdf_bytes, profile = _fill_and_read(quote)

        report = verify_readback(pdf_bytes, quote, profile)

        assert isinstance(report, ReadbackReport)
        assert report.profile_id == "704a_reytech_standard"
        assert report.fields_expected > 0, "expected at least some static writes"
        # A healthy fill should read back at least the vendor name.
        assert report.fields_readback_ok > 0
        assert report.passed, f"misses: {[m.pdf_field for m in report.misses]}"

    def test_summary_format(self):
        quote = _priced_quote()
        pdf_bytes, profile = _fill_and_read(quote)
        report = verify_readback(pdf_bytes, quote, profile)
        s = report.summary
        assert profile.id in s
        assert "read back" in s or "no expected" in s


class TestReadbackDetectsMissing:
    def test_corrupt_pdf_is_reported_not_raised(self):
        """Unreadable PDF bytes should produce a failing report, not an exception."""
        quote = _priced_quote()
        profiles = load_profiles()
        profile = profiles["704a_reytech_standard"]

        report = verify_readback(b"not a pdf", quote, profile)

        assert report.passed is False
        assert any(m.pdf_field == "<pdf>" for m in report.misses)

    def test_empty_quote_fields_are_not_flagged(self):
        """Fields with empty Quote values should be skipped entirely — neither
        counted as expected nor as missed. Otherwise every sparse quote
        would flood the report with false positives."""
        quote = _priced_quote()
        # Deliberately wipe an optional field the filler usually writes
        quote.vendor.sb_cert = ""
        pdf_bytes, profile = _fill_and_read(quote)

        report = verify_readback(pdf_bytes, quote, profile)
        assert report.passed, f"misses: {[m.pdf_field for m in report.misses]}"
        # Nothing in the misses list should reference the sb_cert pdf field
        sb_field = profile.get_field("vendor.sb_cert")
        if sb_field:
            missed_names = {m.pdf_field for m in report.misses}
            assert sb_field.pdf_field not in missed_names


class TestReadbackDetectsBlankFillOutput:
    """Simulate a filler that silently drops a required value.

    We stub _build_static_field_map to assert it expected a value, while
    the actual PDF contains no such field — proving the verifier flags
    the miss even when the filler *thinks* it wrote something.
    """

    def test_expected_write_missing_in_pdf_is_flagged(self, monkeypatch):
        quote = _priced_quote()
        pdf_bytes, profile = _fill_and_read(quote)

        # Pretend the filler claimed it wrote a field that doesn't exist
        # in the PDF. The readback should surface it as a miss.
        def _fake_static_map(_q, _p):
            return {"GHOST_FIELD_THAT_DOES_NOT_EXIST": "should be readable"}

        def _fake_row_map(_q, _p):
            return {}

        monkeypatch.setattr(
            "src.forms.fill_engine._build_static_field_map", _fake_static_map,
        )
        monkeypatch.setattr(
            "src.forms.fill_engine._build_row_field_map", _fake_row_map,
        )

        report = verify_readback(pdf_bytes, quote, profile)

        assert report.passed is False
        assert report.fields_expected == 1
        assert report.fields_readback_ok == 0
        assert len(report.misses) == 1
        assert report.misses[0].pdf_field == "GHOST_FIELD_THAT_DOES_NOT_EXIST"
        assert report.misses[0].kind == "static"

    def test_row_field_miss_has_kind_row(self, monkeypatch):
        quote = _priced_quote()
        pdf_bytes, profile = _fill_and_read(quote)

        def _fake_static_map(_q, _p):
            return {}

        def _fake_row_map(_q, _p):
            return {"GHOST_ROW_FIELD": "99.99"}

        monkeypatch.setattr(
            "src.forms.fill_engine._build_static_field_map", _fake_static_map,
        )
        monkeypatch.setattr(
            "src.forms.fill_engine._build_row_field_map", _fake_row_map,
        )

        report = verify_readback(pdf_bytes, quote, profile)

        assert report.passed is False
        assert any(m.kind == "row" for m in report.misses)
