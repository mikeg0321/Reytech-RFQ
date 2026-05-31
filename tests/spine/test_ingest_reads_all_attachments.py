"""LAW 6 "Teeth" forcing function.

Tests that FAIL THE BUILD when:
  (a) An EmailContract has attachment_refs with no matching
      AttachmentDisposition (unaccounted attachment).
  (b) A parsed AttachmentDisposition carries cross_references but
      cross_refs_resolved is False (cross-reference target never parsed).

These tests are the forcing function referenced in CLAUDE.md §0 LAW 6
and the AttachmentDisposition docstring. A docs-only restatement of the
rule does NOT satisfy it — this test must fail for contracts that violate
either condition.

Additionally, tests cover the send-prep gate (HTTP 409) that enforces
the same two conditions at send time.

Architect authorization: O4 ticket, recorded in PR. EmailContract field
addition authorized under LAW 4 as "field on existing model, not a new
substrate."

Incident motivating this test: 2026-05-28 Coleman 10842771 — the 704B
row 4 referenced an AMS 701B distribution list that ingest had parsed but
mis-modeled, discarding facility/address/zip columns. The forcing
function here would have blocked send-prep until ingest explicitly
accounted for the cross-reference target as fully parsed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from flask import Flask
from flask.testing import FlaskClient

from src.api.modules.routes_spine import make_spine_blueprint
from src.spine import (
    AttachmentDisposition,
    ContractLineItem,
    EmailContract,
    LineItem,
    Quote,
    QuoteStatus,
    init_db,
    write_email_contract,
    write_quote,
)


# ── helpers ────────────────────────────────────────────────────────────


def _make_line_item(line_no: int = 1) -> ContractLineItem:
    return ContractLineItem(line_no=line_no, description="Test item", qty=1, uom="EA")


def _make_contract(
    *,
    contract_id: str = "contract_test_001",
    rfq_id: str = "Q-att-001",
    attachment_refs: list[str] | None = None,
    attachment_dispositions: list[AttachmentDisposition] | None = None,
) -> EmailContract:
    """Build a minimal EmailContract with the given attachment fields."""
    return EmailContract(
        contract_id=contract_id,
        rfq_id=rfq_id,
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10848901",
        line_items=[_make_line_item()],
        attachment_refs=attachment_refs or [],
        attachment_dispositions=attachment_dispositions or [],
    )


def _make_disp(
    ref: str,
    *,
    status: str = "parsed",
    reason: str | None = None,
    cross_references: list[str] | None = None,
    cross_refs_resolved: bool = True,
) -> AttachmentDisposition:
    return AttachmentDisposition(
        ref=ref,
        status=status,  # type: ignore[arg-type]
        reason=reason,
        cross_references=cross_references or [],
        cross_refs_resolved=cross_refs_resolved,
    )


# ── Part A: model-level contract construction tests ───────────────────


class TestAttachmentDispositionModel:
    """AttachmentDisposition model construction and validation."""

    def test_disposition_parsed_no_cross_refs(self):
        d = _make_disp("forms/704b.pdf")
        assert d.ref == "forms/704b.pdf"
        assert d.status == "parsed"
        assert d.cross_references == []
        assert d.cross_refs_resolved is True

    def test_disposition_classified_non_rfq_requires_reason(self):
        """classified_non_rfq WITHOUT a reason is now REJECTED (LAW 6).

        LAW 6 requires every non-RFQ classification to carry "a recorded
        reason." The model_validator on AttachmentDisposition enforces this
        at construction time — a reasonless classified_non_rfq disposition
        must raise ValidationError so the gap can never silently pass the
        send-gate or appear in a persisted contract.

        This test was previously named
        ``test_disposition_classified_non_rfq_requires_no_reason_field``
        and asserted the opposite (that reason=None was accepted). It was
        flipped when the model_validator was added.
        """
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="classified_non_rfq"):
            AttachmentDisposition(
                ref="logo.png",
                status="classified_non_rfq",
                # reason intentionally omitted -- must be rejected
            )

    def test_disposition_classified_non_rfq_empty_reason_rejected(self):
        """Whitespace-only reason is also rejected (LAW 6)."""
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="classified_non_rfq"):
            AttachmentDisposition(
                ref="logo.png",
                status="classified_non_rfq",
                reason="   ",  # whitespace-only -- treated as empty
            )

    def test_disposition_classified_non_rfq_with_reason(self):
        """classified_non_rfq WITH a real reason constructs successfully."""
        d = _make_disp(
            "cover_page.pdf",
            status="classified_non_rfq",
            reason="Blank cover page -- no bid-requirement content.",
        )
        assert d.status == "classified_non_rfq"
        assert d.reason == "Blank cover page -- no bid-requirement content."

    def test_disposition_parsed_no_reason_still_allowed(self):
        """parsed disposition does NOT require a reason (LAW 6 only
        mandates a reason for classified_non_rfq). This test guards
        against regressions that would break the parsed path."""
        d = AttachmentDisposition(
            ref="forms/704b.pdf",
            status="parsed",
            # reason intentionally omitted -- must still succeed
        )
        assert d.status == "parsed"
        assert d.reason is None

    def test_disposition_parsed_with_cross_references(self):
        d = _make_disp(
            "forms/704b.pdf",
            cross_references=["see attached distribution list"],
            cross_refs_resolved=False,
        )
        assert d.cross_references == ["see attached distribution list"]
        assert d.cross_refs_resolved is False

    def test_disposition_rejects_extra_fields(self):
        with pytest.raises(Exception):
            AttachmentDisposition(
                ref="forms/704b.pdf",
                status="parsed",
                phantom_field="not allowed",  # extra="forbid"
            )

    def test_disposition_invalid_status_rejected(self):
        with pytest.raises(Exception):
            AttachmentDisposition(
                ref="forms/704b.pdf",
                status="unknown_status",  # type: ignore[arg-type]
            )

    def test_disposition_empty_ref_rejected(self):
        with pytest.raises(Exception):
            AttachmentDisposition(ref="", status="parsed")


class TestEmailContractAttachmentDispositions:
    """EmailContract attachment_dispositions field integration."""

    def test_contract_no_attachments_empty_dispositions_ok(self):
        """Empty refs + empty dispositions is the valid base case."""
        c = _make_contract()
        assert c.attachment_refs == []
        assert c.attachment_dispositions == []

    def test_contract_with_refs_and_matching_dispositions(self):
        c = _make_contract(
            attachment_refs=["forms/703b.pdf", "forms/704b.pdf"],
            attachment_dispositions=[
                _make_disp("forms/703b.pdf"),
                _make_disp("forms/704b.pdf"),
            ],
        )
        assert len(c.attachment_dispositions) == 2

    def test_contract_accepts_partial_dispositions(self):
        """The model itself does NOT validate completeness — that is the
        send-gate's job (gate ENFORCES; model allows partial state so
        ingest can build the contract incrementally). This test documents
        the intentional split of responsibility."""
        # A contract with 2 refs but only 1 disposition is valid at
        # model level — the send-gate will block it.
        c = _make_contract(
            attachment_refs=["forms/703b.pdf", "forms/704b.pdf"],
            attachment_dispositions=[
                _make_disp("forms/703b.pdf"),
                # 704b missing — gate will catch this
            ],
        )
        assert len(c.attachment_dispositions) == 1

    def test_contract_rejects_extra_fields(self):
        with pytest.raises(Exception):
            EmailContract(
                contract_id="c001",
                rfq_id="Q-001",
                agency="CCHCS",
                facility="SAC",
                solicitation_number="10848901",
                line_items=[_make_line_item()],
                attachment_refs=[],
                attachment_dispositions=[],
                phantom_field="not allowed",  # extra="forbid"
            )


# ── Part B: send-gate enforcement tests ───────────────────────────────


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    p = tmp_path / "spine_att_gate.db"
    init_db(str(p))
    return str(p)


@pytest.fixture
def client(db_path: str) -> FlaskClient:
    app = Flask(__name__)
    app.testing = True
    app.register_blueprint(make_spine_blueprint(db_path, auth_decorator=None))
    return app.test_client()


def _build_and_finalize_quote(
    client: FlaskClient,
    db_path: str,
    *,
    quote_id: str = "Q-att-gate",
    contract: EmailContract,
) -> None:
    """Seed the DB with a finalized quote + the given contract."""
    write_email_contract(db_path, contract)
    q_parsed = Quote(
        quote_id=quote_id,
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10848901",
        line_items=[
            LineItem(
                line_no=1, description="Test Item", mfg_number="X-1",
                qty=1, uom="EA",
                cost_cents=8000,
                cost_source_url="https://example.com/x",
                cost_validated_at=datetime.now(timezone.utc),
                unit_price_cents=12500,
            )
        ],
        tax_rate_bps=775,
        status=QuoteStatus.PARSED,
    )
    client.post(f"/spine/quotes/{quote_id}/state",
                json=q_parsed.to_persisted_dict())
    q_priced = q_parsed.model_copy(update={"status": QuoteStatus.PRICED})
    client.post(f"/spine/quotes/{quote_id}/state",
                json=q_priced.to_persisted_dict())
    q_final = q_priced.model_copy(update={"status": QuoteStatus.FINALIZED})
    client.post(f"/spine/quotes/{quote_id}/state",
                json=q_final.to_persisted_dict())
    snap_r = client.post(f"/spine/quotes/{quote_id}/snapshot")
    assert snap_r.status_code == 200, snap_r.get_data(as_text=True)


def _send_prep(client: FlaskClient, quote_id: str = "Q-att-gate"):
    return client.post(
        f"/spine/quotes/{quote_id}/send-prep",
        json={"to": "buyer@cdcr.ca.gov"},
    )


# ── (a) Unaccounted attachment → gate blocks ────────────────────────


def test_gate_blocks_when_attachment_ref_has_no_disposition(client, db_path):
    """FORCING FUNCTION (a): attachment_refs present but attachment_dispositions
    empty → send-prep returns 409 disposition_missing.

    This test MUST FAIL THE BUILD for any EmailContract that has refs
    without dispositions. It directly enforces the LAW 6 "Teeth" clause.
    """
    contract = _make_contract(
        rfq_id="Q-att-gate",
        attachment_refs=["forms/704b_buyer.pdf"],
        attachment_dispositions=[],  # no disposition recorded — BUG
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    assert r.status_code == 409, (
        "FORCING FUNCTION VIOLATED: send-prep should block (409) when "
        "an attachment has no recorded disposition. Got: "
        + r.get_data(as_text=True)
    )
    payload = r.get_json()
    assert payload["error"] == "disposition_missing"
    assert "unaccounted_refs" in payload
    assert "forms/704b_buyer.pdf" in payload["unaccounted_refs"]


def test_gate_blocks_when_multiple_refs_missing_dispositions(client, db_path):
    """All missing refs are reported together in the 409 payload."""
    contract = _make_contract(
        contract_id="contract_multi_001",
        rfq_id="Q-att-gate",
        attachment_refs=[
            "forms/703b_buyer.pdf",
            "forms/704b_buyer.pdf",
            "forms/dist_list.pdf",
        ],
        attachment_dispositions=[
            # Only one disposition recorded — two are missing
            _make_disp("forms/703b_buyer.pdf"),
        ],
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    assert r.status_code == 409
    payload = r.get_json()
    assert payload["error"] == "disposition_missing"
    missing = set(payload["unaccounted_refs"])
    assert "forms/704b_buyer.pdf" in missing
    assert "forms/dist_list.pdf" in missing
    assert "forms/703b_buyer.pdf" not in missing  # this one has a disposition


# ── (b) Unresolved cross-reference → gate blocks ────────────────────


def test_gate_blocks_when_parsed_form_has_unresolved_cross_reference(
    client, db_path
):
    """FORCING FUNCTION (b): a parsed disposition has cross_references
    pointing to a target that was never parsed → 409 cross_reference_unresolved.

    This directly models the 2026-05-28 Coleman 10842771 incident: the
    704B pointed to an AMS 701B distribution list via 'PLEASE SEE ATTACHED
    DISTRIBUTION LIST'. Had this gate existed, it would have blocked send
    until ingest fully parsed the distribution list rows.
    """
    contract = _make_contract(
        rfq_id="Q-att-gate",
        attachment_refs=["forms/704b.pdf", "forms/ams701b_dist_list.pdf"],
        attachment_dispositions=[
            AttachmentDisposition(
                ref="forms/704b.pdf",
                status="parsed",
                reason="Line items extracted.",
                cross_references=["PLEASE SEE ATTACHED DISTRIBUTION LIST"],
                cross_refs_resolved=False,  # target never parsed — BUG
            ),
            _make_disp("forms/ams701b_dist_list.pdf"),  # parsed but not linked
        ],
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    assert r.status_code == 409, (
        "FORCING FUNCTION VIOLATED: send-prep should block (409) when "
        "a parsed form carries an unresolved cross-reference. Got: "
        + r.get_data(as_text=True)
    )
    payload = r.get_json()
    assert payload["error"] == "cross_reference_unresolved"
    assert "unresolved_refs" in payload
    assert "forms/704b.pdf" in payload["unresolved_refs"]


def test_gate_blocks_cross_ref_unresolved_even_when_target_disposition_exists(
    client, db_path
):
    """cross_refs_resolved=False triggers the gate even if the target
    attachment_ref IS in the dispositions list. The distinction is that
    'cross_refs_resolved' tracks whether ingest CONFIRMED the cross-reference
    match — having both dispositions present doesn't prove ingest actually
    read them in the right order and linked them.

    This tests that the gate key is cross_refs_resolved, not a ref-count heuristic.
    """
    contract = _make_contract(
        rfq_id="Q-att-gate",
        attachment_refs=["forms/704b.pdf", "forms/dist_list.pdf"],
        attachment_dispositions=[
            AttachmentDisposition(
                ref="forms/704b.pdf",
                status="parsed",
                cross_references=["see attached distribution list"],
                cross_refs_resolved=False,  # ingest didn't confirm resolution
            ),
            _make_disp("forms/dist_list.pdf"),
        ],
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    assert r.status_code == 409
    assert r.get_json()["error"] == "cross_reference_unresolved"


# ── (c) Happy paths — gate passes ────────────────────────────────────


def test_gate_passes_when_no_attachments(client, db_path):
    """No attachments → no dispositions required → gate passes."""
    contract = _make_contract(
        rfq_id="Q-att-gate",
        attachment_refs=[],
        attachment_dispositions=[],
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    # Gate passes (200 or some other non-409 for a different reason, but
    # not the attachment-disposition gate). The Inspector gate may block
    # on missing form templates if fixtures are absent — that's OK, we
    # only assert it's NOT the disposition gate.
    assert r.status_code != 409 or r.get_json().get("error") not in (
        "disposition_missing", "cross_reference_unresolved"
    ), (
        "Disposition gate should NOT fire when there are no attachments. "
        "Got 409 with: " + r.get_data(as_text=True)
    )


def test_gate_passes_when_all_refs_have_dispositions_no_cross_refs(
    client, db_path
):
    """All refs covered by dispositions, no cross-references → gate passes."""
    contract = _make_contract(
        rfq_id="Q-att-gate",
        attachment_refs=["forms/703b.pdf", "forms/704b.pdf"],
        attachment_dispositions=[
            _make_disp("forms/703b.pdf"),
            _make_disp("forms/704b.pdf"),
        ],
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    assert r.status_code != 409 or r.get_json().get("error") not in (
        "disposition_missing", "cross_reference_unresolved"
    )


def test_gate_passes_when_cross_refs_resolved(client, db_path):
    """Parsed form with cross-references AND cross_refs_resolved=True passes."""
    contract = _make_contract(
        rfq_id="Q-att-gate",
        attachment_refs=["forms/704b.pdf", "forms/dist_list.pdf"],
        attachment_dispositions=[
            AttachmentDisposition(
                ref="forms/704b.pdf",
                status="parsed",
                reason="Line items + ship-to extracted.",
                cross_references=["see attached distribution list"],
                cross_refs_resolved=True,  # ingest confirmed the target was parsed
            ),
            _make_disp(
                "forms/dist_list.pdf",
                reason="21-facility distribution list fully parsed.",
            ),
        ],
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    assert r.status_code != 409 or r.get_json().get("error") not in (
        "disposition_missing", "cross_reference_unresolved"
    )


def test_gate_passes_for_classified_non_rfq_attachment(client, db_path):
    """classified_non_rfq disposition satisfies the ref-coverage check."""
    contract = _make_contract(
        rfq_id="Q-att-gate",
        attachment_refs=["forms/704b.pdf", "company_logo.png"],
        attachment_dispositions=[
            _make_disp("forms/704b.pdf"),
            _make_disp(
                "company_logo.png",
                status="classified_non_rfq",
                reason="Reytech logo image — no bid-requirement content.",
            ),
        ],
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    assert r.status_code != 409 or r.get_json().get("error") not in (
        "disposition_missing", "cross_reference_unresolved"
    )


def test_gate_does_not_fire_for_classified_non_rfq_cross_references(
    client, db_path
):
    """classified_non_rfq dispositions with cross_references are ignored by the
    gate — only 'parsed' dispositions carry cross_refs_resolved checks.
    A non-RFQ file (e.g., a cover page) might reference other pages but
    the gate only cares about parsed forms."""
    contract = _make_contract(
        rfq_id="Q-att-gate",
        attachment_refs=["forms/704b.pdf", "cover_page.pdf"],
        attachment_dispositions=[
            _make_disp("forms/704b.pdf"),
            AttachmentDisposition(
                ref="cover_page.pdf",
                status="classified_non_rfq",
                reason="Cover page — no bid content.",
                cross_references=["see page 2 for details"],
                cross_refs_resolved=False,  # irrelevant for non-rfq
            ),
        ],
    )
    _build_and_finalize_quote(client, db_path, contract=contract)
    r = _send_prep(client)
    assert r.status_code != 409 or r.get_json().get("error") not in (
        "disposition_missing", "cross_reference_unresolved"
    )


# ── (d) Contract with no dispositions at all (legacy) ────────────────


def test_gate_skipped_when_no_contract_bound(client, db_path):
    """When no EmailContract is bound to the quote, the attachment-
    disposition gate is skipped entirely (same condition as the Inspector
    gate). Legacy quotes that predate the contract substrate are not
    blocked by this gate.

    This test documents the intentional behavior — the gate is
    contract-scoped, not quote-scoped.
    """
    # Seed a finalized quote with NO contract
    q = Quote(
        quote_id="Q-no-contract",
        agency="CCHCS",
        facility="SAC",
        solicitation_number="10848901",
        line_items=[
            LineItem(
                line_no=1, description="Test Item", mfg_number="X-1",
                qty=1, uom="EA",
                cost_cents=8000,
                cost_source_url="https://example.com/x",
                cost_validated_at=datetime.now(timezone.utc),
                unit_price_cents=12500,
            )
        ],
        tax_rate_bps=775,
        status=QuoteStatus.FINALIZED,
    )
    write_quote(db_path, q, actor="test")
    snap_r = client.post("/spine/quotes/Q-no-contract/snapshot")
    assert snap_r.status_code == 200, snap_r.get_data(as_text=True)

    r = client.post(
        "/spine/quotes/Q-no-contract/send-prep",
        json={"to": "buyer@cdcr.ca.gov"},
    )
    # Should not be blocked by the attachment-disposition gate
    assert r.status_code != 409 or r.get_json().get("error") not in (
        "disposition_missing", "cross_reference_unresolved"
    )
