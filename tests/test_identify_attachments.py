"""Regression guards for src.forms.rfq_parser.identify_attachments.

Incident 2026-04-15: CCHCS RFQ 10837703 (RFQ 20260413_215152_19d88d)
hit "Missing required templates: 704B" at package generation time
because the 704B Acquisition Quote Worksheet filename contained "RFQ"
("AMS_704B_-_CCHCS_Acquisition_Quote_Worksheet_-_RFQ_10837703.pdf")
and the elif chain matched the generic "RFQ" branch (703B) before the
specific "704B" branch ever ran. The 704B file was silently registered
as 703B, the real 704B slot stayed empty, and the pre-flight template
check failed.

These tests pin the correct ordering so the specific-before-generic
rule can't be re-reversed by a future refactor.
"""
from src.forms.rfq_parser import identify_attachments


class TestIdentifyAttachmentsOrdering:

    def test_704b_worksheet_with_rfq_in_name_classifies_as_704b(self):
        """The CCHCS 704B filename contains "RFQ 10837703" — the
        specific 704B marker must win over the generic RFQ fallback."""
        path = "AMS 704B -CCHCS Acquisition Quote Worksheet - RFQ 10837703.pdf"
        result = identify_attachments([path])
        assert result == {"704b": path}, (
            f"704B worksheet with RFQ in name mis-classified: {result}. "
            f"Specific 704B marker must be checked before generic RFQ."
        )

    def test_703b_informal_competitive_still_classifies_as_703b(self):
        path = "AMS 703B - RFQ - Informal Competitive - RFQ 10837703.pdf"
        result = identify_attachments([path])
        assert result == {"703b": path}

    def test_sanitized_upload_filename_704b(self):
        """Filenames from /api/rfq/<rid>/upload-parse-doc are prefixed
        with doc_<rid>_ after sanitization — make sure the prefix
        doesn't throw off detection."""
        path = "doc_20260413_215152_19d88d_AMS_704B_-CCHCS_Acquisition_Quote_Worksheet_-_RFQ_10837703.pdf"
        result = identify_attachments([path])
        assert result == {"704b": path}

    def test_sanitized_upload_filename_703b(self):
        path = "doc_20260413_215152_19d88d_AMS_703B_-_RFQ_-_Informal_Competitive_-_RFQ_10837703.pdf"
        result = identify_attachments([path])
        assert result == {"703b": path}

    def test_703c_fair_and_reasonable_still_wins_over_rfq_fallback(self):
        path = "Fair_and_Reasonable_703C_RFQ_12345.pdf"
        result = identify_attachments([path])
        assert result == {"703c": path}

    def test_bid_package_still_classified(self):
        path = "Bid_Package_Forms_Under_100k.pdf"
        result = identify_attachments([path])
        assert result == {"bidpkg": path}

    def test_multiple_files_each_classified_separately(self):
        files = [
            "AMS 704B Worksheet RFQ 111.pdf",
            "AMS 703B RFQ 111.pdf",
            "Bid_Package.pdf",
        ]
        result = identify_attachments(files)
        assert result["704b"] == files[0]
        assert result["703b"] == files[1]
        assert result["bidpkg"] == files[2]


class TestIdentifyDshAttachments:
    """DSH packets ship 3 flat per-solicitation PDFs (Attachment A/B/C).
    These must be detected BEFORE the generic 'FORMS' bidpkg fallback —
    AttC's Required Forms checklist would otherwise be mis-classified
    as a bid package and the dsh_attC slot would stay empty.
    """

    def test_dsh_attA_bidder(self):
        path = "dsh_25CB020_attachA_bidder.pdf"
        result = identify_attachments([path])
        assert result == {"dsh_attA": path}

    def test_dsh_attB_pricing(self):
        path = "dsh_25CB020_attachB_pricing.pdf"
        result = identify_attachments([path])
        assert result == {"dsh_attB": path}

    def test_dsh_attC_forms_not_misclassified_as_bidpkg(self):
        """AttC contains 'forms' in the filename — the DSH-specific
        dsh_attC branch must beat the generic 'FORMS' → bidpkg branch."""
        path = "dsh_25CB020_attachC_forms.pdf"
        result = identify_attachments([path])
        assert result == {"dsh_attC": path}

    def test_attachment_word_variants(self):
        for fn in (
            "Attachment_A_Bidders_Information.pdf",
            "ATTACHMENT A — BIDDER.pdf",
            "attach_a.pdf",
        ):
            result = identify_attachments([fn])
            assert "dsh_attA" in result, f"Failed to classify {fn!r} as dsh_attA: {result}"

    def test_full_dsh_packet_classifies_all_three(self):
        files = [
            "dsh_25CB020_attachA_bidder.pdf",
            "dsh_25CB020_attachB_pricing.pdf",
            "dsh_25CB020_attachC_forms.pdf",
        ]
        result = identify_attachments(files)
        assert result["dsh_attA"] == files[0]
        assert result["dsh_attB"] == files[1]
        assert result["dsh_attC"] == files[2]
        assert "bidpkg" not in result
