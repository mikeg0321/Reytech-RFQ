"""PR-A Step 6 (2026-05-11) — Proofpoint SecureMessage shape detection.

Mike P000 DSH use-case: secure@dsh.ca.gov sends "Secure Message from
DSH" with a Proofpoint portal link. The actual RFQ PDF is behind the
encryption portal, not in the email. Pre-fix, the classifier saw the
wrapper email as `SHAPE_EMAIL_ONLY` (or worse — misclassified a
SecureMessage.html wrapper as the primary file). The operator had to
manually identify the email, log into Proofpoint, download the real
PDF, and re-upload.

This step adds `SHAPE_PROOFPOINT_SECUREMESSAGE` detection so the
ingest pipeline can:
  - Recognize the wrapper email immediately (no false-positive
    "empty RFQ" record)
  - Dispatch to the auto-login + download handler (PR-A Steps 7-8)
  - Fall back to a `needs_manual_pull` status when creds aren't set

The detector uses a 2-of-4 signal threshold across sender pattern,
subject pattern, body pattern, and wrapper-attachment filename. The
sender pattern is the strongest signal; a single corroborating hit
elsewhere clears the threshold.
"""
from __future__ import annotations

import pytest

from src.core.request_classifier import (
    SHAPE_PROOFPOINT_SECUREMESSAGE,
    ALL_SHAPES,
    _SHAPE_BUYER_TEMPLATE_REQUIREMENTS,
    BUYER_TEMPLATE_FORMS,
    _detect_proofpoint_securemessage,
    classify_request,
    filter_required_forms_by_shape,
)


# ─── Shape registry sanity ───────────────────────────────────────────────


class TestShapeRegistry:

    def test_proofpoint_shape_in_all_shapes(self):
        assert SHAPE_PROOFPOINT_SECUREMESSAGE in ALL_SHAPES

    def test_proofpoint_shape_has_no_buyer_template_requirements(self):
        """Wrapper email never carries buyer templates — they're behind
        the portal. Explicit empty set so the filter doesn't fall to
        the 'unknown shape' permissive bucket."""
        assert SHAPE_PROOFPOINT_SECUREMESSAGE in _SHAPE_BUYER_TEMPLATE_REQUIREMENTS
        assert _SHAPE_BUYER_TEMPLATE_REQUIREMENTS[SHAPE_PROOFPOINT_SECUREMESSAGE] == frozenset()

    def test_filter_required_forms_drops_buyer_templates_for_proofpoint(self):
        """Even when agency_config lists 703B/704B as required, the
        wrapper-shape filter must strip them — Reytech has nothing to
        fill until the portal is opened."""
        required = ["703b", "704b", "bidpkg", "quote", "sellers_permit"]
        filtered = filter_required_forms_by_shape(
            required, SHAPE_PROOFPOINT_SECUREMESSAGE,
        )
        # Buyer templates dropped...
        for f in ["703b", "704b", "bidpkg"]:
            assert f not in [x.lower() for x in filtered]
        # ...Reytech-supplied forms survive.
        for f in ["quote", "sellers_permit"]:
            assert f in [x.lower() for x in filtered]


# ─── _detect_proofpoint_securemessage signal logic ──────────────────────


class TestDetectorSignals:

    def test_sender_alone_does_not_fire(self):
        """Sender pattern is strong but single-signal-fires are noisy.
        Threshold is 2-of-4 across sender/subject/body/attachment."""
        hit, _ = _detect_proofpoint_securemessage(
            email_sender="securemail@dsh.ca.gov",
        )
        assert hit is False

    def test_subject_alone_does_not_fire(self):
        hit, _ = _detect_proofpoint_securemessage(
            email_subject="Secure Message from buyer",
        )
        assert hit is False

    def test_body_alone_does_not_fire(self):
        hit, _ = _detect_proofpoint_securemessage(
            email_body="You have received a secure message via Proofpoint Encryption.",
        )
        # Body has 2 separate matches but only counts once per class.
        assert hit is False

    def test_sender_plus_subject_fires(self):
        hit, reasons = _detect_proofpoint_securemessage(
            email_sender="securemail@dsh.ca.gov",
            email_subject="*** Secure Mail *** Quote Request",
        )
        assert hit is True
        # Both signals reported.
        assert any("sender" in r for r in reasons)
        assert any("subject" in r for r in reasons)

    def test_sender_plus_body_fires(self):
        hit, reasons = _detect_proofpoint_securemessage(
            email_sender="notification@proofpoint.com",
            email_body=(
                "You have received a secure message. Click here to read the "
                "encrypted message in the Proofpoint Encryption portal."
            ),
        )
        assert hit is True

    def test_subject_plus_attachment_fires(self):
        hit, _ = _detect_proofpoint_securemessage(
            email_subject="Secure Message from DSH Atascadero",
            attachments=["/tmp/SecureMessage.html"],
        )
        assert hit is True

    def test_full_dsh_wrapper_email_fires(self):
        """A realistic DSH wrapper email: secure sender, secure subject,
        portal-link body, SecureMessage.html attachment. All 4 signals
        fire; threshold is comfortably cleared."""
        hit, reasons = _detect_proofpoint_securemessage(
            email_sender="securemail@dsh.ca.gov",
            email_subject="*** Secure Mail *** Quote Request - Atascadero State Hospital",
            email_body=(
                "<html><body>"
                "<p>You have received a secure message from the Department of "
                "State Hospitals.</p>"
                "<p><a href='https://securereader.proofpoint.com/abc123'>"
                "Read the Message</a></p>"
                "</body></html>"
            ),
            attachments=["/tmp/SecureMessage.html"],
        )
        assert hit is True
        assert len(reasons) >= 4

    def test_real_dsh_wrapper_email_25cb021_calibration(self):
        """REAL-WORLD calibration sample (DSH 25CB021, May 2026):
          - Sender is the buyer's normal Outlook address, NOT a Proofpoint
            gateway (\"Corneliu.Butuza@dsh.ca.gov\")
          - Subject is \"FW: Please find attached quote request 25CB021\" —
            no Proofpoint markers
          - Body is short: \"This is a secure message. Click here
            https://securemail.dsh.ca.gov/formpostdir/securereader?...\"
          - Wrapper attachment: SecureMessageAtt.html

        Pre-fix this scored 1-of-4 (only the wrapper-attachment name
        matched) → False positive class: real DSH emails ingested as
        SHAPE_EMAIL_ONLY. This test pins the post-fix detector against
        the actual on-the-wire DSH wrapper.
        """
        hit, reasons = _detect_proofpoint_securemessage(
            email_subject="FW: Please find attached quote request 25CB021",
            email_sender='"Butuza, Corneliu@DSH-A" <Corneliu.Butuza@dsh.ca.gov>',
            email_body=(
                "This is a secure message. Click here "
                "https://securemail.dsh.ca.gov/formpostdir/securereader?"
                "id=IEAcvLIKBaWCqFS2XQkZRtOcA5sGXlar&brand=5039a2d3  by "
                "2026-06-07 17:33 PDT to read your message.   After that, "
                "open the attachment."
            ),
            attachments=[
                "/tmp/SecureMessageAtt.html",
                "/tmp/logo.png",
                "/tmp/lock.gif",
            ],
        )
        assert hit is True, (
            f"real DSH wrapper must detect; reasons={reasons}"
        )
        # Both signals corroborate (body phrase + wrapper attachment).
        kinds = " ".join(reasons)
        assert "body pattern" in kinds
        assert "wrapper attachment" in kinds

    def test_securemail_multi_segment_subdomain_body_pattern(self):
        """The body URL pattern must match multi-segment domains like
        `securemail.dsh.ca.gov` (not just single-segment `securemail.foo.gov`).
        Regression for the \\w+ → [\\w.-]+ fix."""
        # Body alone (single signal) doesn't clear threshold but we
        # verify the regex matches by inspecting the reasons list.
        hit, reasons = _detect_proofpoint_securemessage(
            email_body="click https://securemail.dsh.ca.gov/x to read",
        )
        body_hit = any(
            "securemail" in r and "body pattern" in r for r in reasons
        )
        assert body_hit, (
            f"multi-segment domain securemail.dsh.ca.gov must match; reasons={reasons}"
        )

    def test_normal_dsh_email_does_not_fire(self):
        """Regular DSH email with a real RFQ PDF attached must NOT be
        flagged as a Proofpoint wrapper just because the agency
        sometimes uses Proofpoint. False positive here would route to
        the wrong handler."""
        hit, _ = _detect_proofpoint_securemessage(
            email_sender="bob.smith@dsh.ca.gov",
            email_subject="RFQ - Medical Supplies",
            email_body=(
                "Hi Mike, please find attached the price-check worksheet for "
                "the medical supply RFQ. Quotes due by Friday. - Bob"
            ),
            attachments=["/tmp/AMS_704_DSH.pdf"],
        )
        assert hit is False

    def test_empty_inputs_do_not_fire(self):
        hit, reasons = _detect_proofpoint_securemessage()
        assert hit is False
        assert reasons == []

    def test_attachment_naming_variants(self):
        """Proofpoint sometimes attaches the wrapper as
        SecureMessageATT00001.html or securemail.html or similar."""
        for fname in (
            "SecureMessage.html",
            "SecureMessageATT00001.html",
            "secureemail.html",
            "securemail_blob.html",
        ):
            hit, _ = _detect_proofpoint_securemessage(
                email_sender="securemail@dsh.ca.gov",
                attachments=[f"/tmp/{fname}"],
            )
            assert hit is True, f"failed to detect {fname}"


# ─── End-to-end classify_request override ────────────────────────────────


class TestClassifyRequestProofpoint:

    def test_classify_overrides_email_only_to_proofpoint(self):
        """No attachments, but Proofpoint signals fire — shape goes to
        SHAPE_PROOFPOINT_SECUREMESSAGE (not SHAPE_EMAIL_ONLY)."""
        result = classify_request(
            attachments=[],
            email_subject="*** Secure Mail *** Quote Request",
            email_body=(
                "You have received a secure message. Please click here to "
                "read the encrypted message via Proofpoint Encryption."
            ),
            email_sender="securemail@dsh.ca.gov",
        )
        assert result.shape == SHAPE_PROOFPOINT_SECUREMESSAGE

    def test_classify_overrides_wrapper_html_to_proofpoint(self, tmp_path):
        """When a SecureMessage.html attachment is present, the override
        clears `primary_file` so downstream parsers don't waste a
        Vision call on the wrapper."""
        f = tmp_path / "SecureMessage.html"
        f.write_text(
            "<html><body>"
            "<a href='https://securereader.proofpoint.com/x'>Read the Message</a>"
            "</body></html>"
        )
        result = classify_request(
            attachments=[str(f)],
            email_subject="Secure Message from DSH",
            email_body="Click here to read your secure message.",
            email_sender="securemail@dsh.ca.gov",
        )
        assert result.shape == SHAPE_PROOFPOINT_SECUREMESSAGE
        # Primary file cleared so ingest_pipeline doesn't try to parse
        # the wrapper HTML as the RFQ.
        assert result.primary_file == ""
        assert result.primary_file_type == "proofpoint_wrapper"

    def test_classify_picks_up_agency_from_sender(self):
        """secure@dsh.ca.gov → agency=dsh from existing AGENCY_KEYWORDS,
        even though the wrapper body has no agency text."""
        result = classify_request(
            attachments=[],
            email_subject="*** Secure Mail ***",
            email_body="Click here to read your encrypted message via Proofpoint Encryption.",
            email_sender="securemail@dsh.ca.gov",
        )
        assert result.shape == SHAPE_PROOFPOINT_SECUREMESSAGE
        assert result.agency == "dsh"

    def test_normal_rfq_email_classifies_normally(self, tmp_path):
        """Regression: a regular RFQ email from a DSH buyer (not a
        Proofpoint wrapper) must NOT be overridden to the wrapper
        shape."""
        # Make a tiny DOCX-ish file so the classifier picks SOME shape.
        # We don't need it to parse — just exist.
        f = tmp_path / "AMS_704_DSH.docx"
        f.write_bytes(b"PK\x03\x04")
        result = classify_request(
            attachments=[str(f)],
            email_subject="RFQ - Medical Supplies",
            email_body="Please find attached the price check worksheet.",
            email_sender="bob.smith@dsh.ca.gov",
        )
        assert result.shape != SHAPE_PROOFPOINT_SECUREMESSAGE

    def test_is_quote_only_false_for_proofpoint(self):
        """The wrapper shape is NOT a quote-only PC shape — it routes
        to RFQ-class handling until the portal yields the real PDF."""
        result = classify_request(
            attachments=[],
            email_subject="*** Secure Mail *** Quote Request",
            email_body="You have received a secure message via Proofpoint Encryption.",
            email_sender="securemail@dsh.ca.gov",
        )
        assert result.shape == SHAPE_PROOFPOINT_SECUREMESSAGE
        assert result.is_quote_only is False
