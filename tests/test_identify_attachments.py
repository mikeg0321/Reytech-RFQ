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
