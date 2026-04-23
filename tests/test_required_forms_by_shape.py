"""Regression: buyer-template forms (703B/704B/bidpkg) must not be required
when the classifier shape indicates the buyer did not send them.

Incident 2026-04-22: LPA IT Goods RFQ from CCHCS was classified as a non-packet
shape but generate-package still gate-rejected on missing 704B because
`required_forms` came from `agency_config[cchcs]` verbatim, regardless of shape.
"""
import pytest

from src.core.request_classifier import (
    BUYER_TEMPLATE_FORMS,
    SHAPE_CCHCS_PACKET,
    SHAPE_EMAIL_ONLY,
    SHAPE_GENERIC_RFQ_PDF,
    SHAPE_GENERIC_RFQ_DOCX,
    SHAPE_GENERIC_RFQ_XLSX,
    SHAPE_PC_704_DOCX,
    SHAPE_PC_704_PDF_DOCUSIGN,
    SHAPE_PC_704_PDF_FILLABLE,
    SHAPE_UNKNOWN,
    filter_required_forms_by_shape,
)


# Canonical CCHCS required_forms from agency_config — the case that produced
# the 2026-04-22 P0.
CCHCS_REQUIRED = ["703b", "704b", "bidpkg", "quote"]
CCHCS_DB_OVERRIDE = ["sellers_permit", "quote", "704b", "bidpkg", "703b"]


class TestPacketShapeKeepsBuyerTemplates:
    def test_cchcs_packet_keeps_all_packet_forms(self):
        out = filter_required_forms_by_shape(CCHCS_REQUIRED, SHAPE_CCHCS_PACKET)
        assert set(out) == set(CCHCS_REQUIRED)

    def test_cchcs_packet_with_db_override_keeps_all(self):
        out = filter_required_forms_by_shape(CCHCS_DB_OVERRIDE, SHAPE_CCHCS_PACKET)
        assert set(out) == set(CCHCS_DB_OVERRIDE)


class TestNonPacketShapeDropsBuyerTemplates:
    @pytest.mark.parametrize("shape", [
        SHAPE_EMAIL_ONLY,
        SHAPE_GENERIC_RFQ_PDF,
        SHAPE_GENERIC_RFQ_DOCX,
        SHAPE_GENERIC_RFQ_XLSX,
    ])
    def test_non_packet_shapes_drop_703_704_bidpkg(self, shape):
        out = filter_required_forms_by_shape(CCHCS_REQUIRED, shape)
        # Quote survives (not a buyer template), others dropped.
        assert set(out) == {"quote"}

    def test_non_packet_with_db_override_drops_buyer_templates(self):
        # The actual error case: RFQ 9ad8a0ac had this exact required_forms
        # list from the DB override. All buyer templates must be dropped.
        out = filter_required_forms_by_shape(
            CCHCS_DB_OVERRIDE, SHAPE_GENERIC_RFQ_PDF
        )
        assert set(out) == {"sellers_permit", "quote"}

    def test_pc_shapes_keep_only_704b(self):
        for shape in (SHAPE_PC_704_DOCX, SHAPE_PC_704_PDF_DOCUSIGN, SHAPE_PC_704_PDF_FILLABLE):
            out = filter_required_forms_by_shape(CCHCS_REQUIRED, shape)
            assert set(out) == {"704b", "quote", "bidpkg"} - {"bidpkg"} | {"704b", "quote"}
            # Collapse: 704b and quote stay, 703b + bidpkg go
            assert "703b" not in out and "bidpkg" not in out
            assert "704b" in out and "quote" in out


class TestUnknownShapeUsesUploadedTemplates:
    def test_unknown_shape_no_uploads_drops_buyer_templates(self):
        out = filter_required_forms_by_shape(CCHCS_REQUIRED, "")
        assert set(out) == {"quote"}

    def test_unknown_shape_drops_when_no_uploads(self):
        out = filter_required_forms_by_shape(CCHCS_REQUIRED, SHAPE_UNKNOWN)
        assert set(out) == {"quote"}

    def test_unknown_shape_with_704b_upload_keeps_704b(self):
        out = filter_required_forms_by_shape(
            CCHCS_REQUIRED, SHAPE_UNKNOWN, uploaded_templates=["704b"]
        )
        assert "704b" in out
        assert "703b" not in out
        assert "bidpkg" not in out
        assert "quote" in out

    def test_703b_upload_satisfies_703c_requirement(self):
        out = filter_required_forms_by_shape(
            ["703c", "quote"], SHAPE_UNKNOWN, uploaded_templates=["703b"]
        )
        assert "703c" in out
        assert "quote" in out

    def test_703c_upload_satisfies_703b_requirement(self):
        out = filter_required_forms_by_shape(
            ["703b", "quote"], SHAPE_UNKNOWN, uploaded_templates=["703c"]
        )
        assert "703b" in out


class TestNonBuyerTemplateFormsAlwaysSurvive:
    @pytest.mark.parametrize("shape", [
        SHAPE_EMAIL_ONLY,
        SHAPE_CCHCS_PACKET,
        SHAPE_GENERIC_RFQ_PDF,
        SHAPE_UNKNOWN,
        "",
    ])
    def test_quote_and_permits_always_pass(self, shape):
        forms = ["quote", "sellers_permit", "std204", "dvbe843",
                 "bidder_decl", "darfur_act", "calrecycle74"]
        out = filter_required_forms_by_shape(forms, shape)
        assert set(out) == set(forms)


class TestOrderPreservation:
    def test_output_preserves_input_order(self):
        forms = ["quote", "703b", "sellers_permit", "704b"]
        out = filter_required_forms_by_shape(forms, SHAPE_GENERIC_RFQ_PDF)
        # Buyer templates stripped, order of survivors preserved.
        assert out == ["quote", "sellers_permit"]


class TestEmptyAndNoneInputs:
    def test_empty_required_forms(self):
        assert filter_required_forms_by_shape([], SHAPE_CCHCS_PACKET) == []

    def test_none_shape_still_strips_buyer_templates(self):
        out = filter_required_forms_by_shape(CCHCS_REQUIRED, None)
        assert set(out) == {"quote"}

    def test_none_uploaded_templates(self):
        out = filter_required_forms_by_shape(
            CCHCS_REQUIRED, SHAPE_UNKNOWN, uploaded_templates=None
        )
        assert set(out) == {"quote"}


def test_buyer_template_forms_constant_covers_all_buyer_templates():
    """If a new buyer-template form (e.g., 705) is ever added, the constant
    must be updated too. This test fails loudly if BUYER_TEMPLATE_FORMS
    drifts from the four known buyer-supplied templates."""
    assert BUYER_TEMPLATE_FORMS == frozenset({"703b", "703c", "704b", "bidpkg"})
