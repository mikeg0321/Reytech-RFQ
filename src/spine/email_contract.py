"""EmailContract — the master substrate primitive.

Per Mike 2026-05-16: the email contract IS the ground truth that
every downstream object (Spine Quote, rendered PDFs, snapshots,
send envelopes) is compared against. Operator edits become DELTAS
from the contract, not state-from-scratch. The audit chain reads:

    inbound email
       │
       ▼
    EmailContract (immutable; what the buyer asked for)
       │ ────── compared via contract_vs_quote() ──────► deltas
       ▼
    Quote (mutable; what the operator built)
       │
       ▼
    Snapshot (immutable; what the operator approved)
       │
       ▼
    Send envelope (immutable; what shipped)

Every operator override is traceable to: which contract field, what
the contract said, what the operator typed. Closes the substrate
class "we shipped something different than the buyer requested,
and nobody could tell after the fact."

Architectural rules:
  - `extra="forbid"` on the model — no alias creep.
  - One-writer-per-table for `spine_email_contracts`.
  - Append-only (the contract is the BUYER'S statement; later
    corrections create a NEW contract row, never modify the prior).
  - Quote.contract_id references the contract that drove ingest.
  - Diff function is pure: contract + quote → list of deltas.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, get_args

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ──────────────────────────────────────────────────────────────────────
# FormCode — the canonical literal for every form Reytech can render.
# ──────────────────────────────────────────────────────────────────────
# This is the single source of truth for what the email-contract's
# `required_forms` can declare AND what the agency_forms FORM_REGISTRY
# (built in Phase 2) must satisfy. A code added here without a registered
# renderer fails the build-time architecture-contract test
# `test_every_form_code_has_renderer`.
#
# Naming convention: lowercase + underscores, agency-prefix-free for
# generic forms. "cchcs_*" prefix only for the few forms whose shape
# is agency-specific by design (703B/704B/bidpkg are CCHCS-named in the
# filename but conceptually generic enough that future agencies may
# share them — left bare for that reason).
FormCode = Literal[
    "703b",          # CCHCS 703B cover sheet + certifications
    "703c",          # 703B alternate (CCHCS sometimes ships this)
    "704b",          # CCHCS 704B line-item form
    "704c",          # 704B alternate
    "bidpkg",        # CCHCS bid package (14-page bundle)
    "quote",         # Reytech Quote PDF
    "calrecycle_74", # CalRecycle 74 EPP form
    "std_204",       # CA STD 204 Payee Data Record
    "std_1000",      # CA STD 1000 GenAI disclosure
    "dvbe_843",      # CA DVBE 843 declaration
    "darfur",        # Darfur Act certification
    "cuf",           # CV 012 Commercially Useful Function
]

ALL_FORM_CODES: tuple[str, ...] = get_args(FormCode)

# CCHCS standard response set. Every CCHCS bid Mike has shipped to date
# has consisted of exactly these four files. The default here is the
# empirical truth — the Phase-2 ingest classifier will derive this from
# attachment shapes per email; until then, this default keeps existing
# fixtures passing and codifies what the buyer has been asking for.
CCHCS_DEFAULT_REQUIRED_FORMS: list[str] = ["703b", "704b", "bidpkg", "quote"]


# ──────────────────────────────────────────────────────────────────────
# ContractLineItem — what the buyer described per line
# ──────────────────────────────────────────────────────────────────────


class ContractLineItem(BaseModel):
    """One line as the buyer described it in the inbound RFQ.

    Distinct from src/spine/model.py::LineItem (operator's edited
    state). Contract line items have NO cost / unit_price / markup —
    those are vendor-side fields the operator fills in. The contract
    captures what the buyer ASKED FOR: a product description, a
    quantity, a UOM. Anything else the buyer happened to write
    (estimated unit price, prior award price, MFG # suggestion) is
    captured as buyer-supplied hints in `buyer_hints` — operator may
    choose to honor them or override.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    line_no: int = Field(ge=1)
    description: str = Field(min_length=1, max_length=500)
    qty: int = Field(gt=0)
    uom: str = Field(min_length=1, max_length=8)
    # Buyer-supplied MFG # / SKU if present in the RFQ. May be blank
    # — many buyers leave it for the vendor to fill in.
    mfg_number_suggested: str | None = Field(default=None, max_length=64)
    # Free-form buyer notes/hints about this line (e.g., "preferred:
    # Resvent CPAP"). Carries forward to the operator's editor.
    buyer_hints: str | None = Field(default=None, max_length=1000)


# ──────────────────────────────────────────────────────────────────────
# EmailContract — the master record of what the buyer asked for
# ──────────────────────────────────────────────────────────────────────


class EmailContract(BaseModel):
    """Frozen ingestion record. The buyer's statement, captured once.

    Created at ingest time from the inbound RFQ email + attachments.
    Never modified after first write. If the buyer sends a revised
    RFQ (rebid), that creates a NEW EmailContract row pointing to
    the same RFQ thread, NOT a modification of the original.

    Every Spine Quote links to one EmailContract via `contract_id`.
    The contract is the ground truth that diffs are computed against.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    contract_id: str = Field(
        min_length=4, max_length=80,
        pattern=r"^[A-Za-z0-9_\-]+$",
        description=(
            "Stable identifier for this ingestion event. Convention: "
            "'contract_<rfq_or_pc_id>_<ingest_timestamp_secs>'."
        ),
    )

    # Linkage to upstream artifacts.
    rfq_id: str | None = Field(default=None, max_length=64,
        description="Spine quote_id this contract drove ingest for.")
    pc_id: str | None = Field(default=None, max_length=64,
        description="Legacy price-check ID if ingested via PC flow.")
    source_email_id: str | None = Field(default=None, max_length=128,
        description="Inbound email message-id (Gmail/Proofpoint).")
    source_thread_id: str | None = Field(default=None, max_length=128,
        description="Email thread ID for rebid grouping.")

    # Buyer identity — who's asking.
    buyer_name: str | None = Field(default=None, max_length=128)
    buyer_email: str | None = Field(default=None, max_length=128)
    buyer_phone: str | None = Field(default=None, max_length=32)
    buyer_title: str | None = Field(default=None, max_length=128)
    # Chrome MCP audit 2026-05-27 / G12: CalVet admitted as a Spine-
    # renderable agency. Forward declaration for the next-after-Job-#1
    # migration. Mirror of the same literal widening on Quote.agency
    # in model.py — keeping both in sync so EmailContract and Quote
    # agency literals can never drift.
    agency: Literal["CCHCS", "CalVet"]
    facility: str = Field(min_length=1, max_length=128)
    institution_code: str | None = Field(default=None, max_length=32,
        description="Legacy institution code if known (e.g., 'CCWF').")

    # Procurement-level metadata — what the buyer's request is about.
    solicitation_number: str = Field(min_length=1, max_length=64)
    rfq_title: str | None = Field(default=None, max_length=200,
        description="Subject line or buyer-supplied title.")
    release_date: datetime | None = Field(default=None,
        description="When the buyer released the RFQ.")
    due_date: datetime | None = Field(default=None,
        description="Bid-response deadline.")

    # Shipping / tax context — drives every downstream PDF.
    ship_to_address: str | None = Field(default=None, max_length=500,
        description="Full ship-to address as the buyer specified.")
    ship_to_facility: str | None = Field(default=None, max_length=128)
    tax_rate_bps: int | None = Field(default=None, ge=0, le=2000,
        description=(
            "Tax rate in basis points as the buyer or CDTFA stated. "
            "When the buyer didn't specify, this is the CDTFA lookup "
            "at ingest time."
        ),
    )

    # Bill-to (invoicing) block. STRUCTURALLY separate from the
    # buyer-side procurement officer (buyer_*) — the requesting
    # officer is NOT who pays the invoice. CalVet bills go to
    # APinvoices@calvet.ca.gov / 1227 "O" Street Sacramento; CDCR
    # bills go to the agency AP inbox, not the unit's procurement
    # analyst. The Quote PDF's "Bill to:" block reads these fields;
    # pre-5/18 it incorrectly used buyer_email for Bill-to → quotes
    # got addressed to the procurement officer (who can't pay them).
    # Operator can fill via /contract-override when ingest misses.
    bill_to_name: str | None = Field(default=None, max_length=200,
        description=(
            "Legal entity name on the invoice (e.g., 'California "
            "Department of Veterans Affairs', 'California Correctional "
            "Health Care Services')."
        ),
    )
    bill_to_address: str | None = Field(default=None, max_length=500,
        description=(
            "Full Bill-to address as printed on the invoice. Multi-line "
            "newline-separated."
        ),
    )
    bill_to_email: str | None = Field(default=None, max_length=128,
        description=(
            "AP invoicing email at the agency (e.g., "
            "'APinvoices@calvet.ca.gov'). NOT the procurement officer."
        ),
    )

    # The buyer's line items — descriptions, quantities, UOMs.
    line_items: list[ContractLineItem] = Field(min_length=1)

    # Attachments referenced (PDF, image, DOC paths/URLs).
    attachment_refs: list[str] = Field(
        default_factory=list,
        description=(
            "Storage paths or message-attachment refs for the "
            "originals. Lets future audits re-render the parser's "
            "view of what the buyer sent."
        ),
    )

    # ── Required output specification ─────────────────────────────────
    # Closes the "we shipped something the buyer didn't ask for, or
    # didn't ship something they did" class (5/15 finding #7). Every
    # downstream renderer iterates this list; the /package endpoint's
    # output-vs-contract gate refuses 409 on any divergence between
    # rendered_set and this declared set.
    required_forms: list[FormCode] = Field(
        default_factory=lambda: list(CCHCS_DEFAULT_REQUIRED_FORMS),
        description=(
            "Forms the buyer's email + attachments require in the "
            "response packet. Phase-1 default is the CCHCS empirical "
            "standard; Phase-2 derives this per email from attachment "
            "shape classification."
        ),
    )
    response_due: datetime | None = Field(
        default=None,
        description=(
            "Bid-response deadline expressed by the buyer's email "
            "(may differ from `due_date` if the buyer specified a "
            "separate response cutoff). Falls back to `due_date` "
            "downstream when unset."
        ),
    )
    response_packaging: Literal["single_pdf", "separate_pdfs", "either"] = Field(
        default="separate_pdfs",
        description=(
            "How the buyer wants the response delivered. CCHCS empirical "
            "default is `separate_pdfs` (four attachments). `single_pdf` "
            "means merge to one file; `either` accepts both."
        ),
    )
    parse_confidence: Literal["high", "medium", "low"] = Field(
        default="high",
        description=(
            "Parser's confidence that this contract faithfully reflects "
            "the buyer's email. `low` is the trigger for the "
            "`/queue/rejected` triage surface — operator must hand-"
            "validate before any quote ships."
        ),
    )

    # Provenance.
    ingest_parser_version: str = Field(default="unknown", max_length=32,
        description="Version tag of the parser that produced this "
                    "contract — for replay / regression debugging.")
    ingested_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )

    @field_validator("line_items")
    @classmethod
    def _consecutive_lines(cls, items: list[ContractLineItem]) -> list[ContractLineItem]:
        nums = [li.line_no for li in items]
        if len(set(nums)) != len(nums):
            raise ValueError(f"duplicate line_no in contract: {nums!r}")
        if nums != sorted(nums):
            raise ValueError(f"line_items must be sorted by line_no: {nums!r}")
        return items
