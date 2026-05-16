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
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    agency: Literal["CCHCS"]
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
