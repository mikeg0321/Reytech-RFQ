"""The Spine — canonical quote data model.

Every architectural invariant from src/spine/SPINE_CHARTER.md is
encoded here as a Pydantic constraint. Drift between the charter and
this model is the bug class the whole project was built to prevent.

Hard rules (enforced by Pydantic, not by review-time discipline):
- extra='forbid' on every model: unknown fields RAISE at construction.
- Integer cents only: no floats, no rounding-error class.
- Single field per logical value: NO bid_price / price_per_unit /
  our_price / shipping_amount / shipping_option / stored markup_pct.
- markup_pct_display, extension_cents, subtotal_cents, tax_cents,
  total_cents are @computed_field — derived on every read, never
  persisted, structurally incapable of drift.
- Status transitions enforce preconditions; status changes do NOT
  recompute line item values.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)


# ──────────────────────────────────────────────────────────────────────
# Time helpers — encoded once, here, so the substrate has a single PST
# clock instead of every renderer recomputing one.
# ──────────────────────────────────────────────────────────────────────

_PST_ZONE = ZoneInfo("America/Los_Angeles")


def _pst_today() -> date:
    """Today's date in America/Los_Angeles (PST/PDT-aware).

    The Spine substrate's single source of operator-local "today."
    Encoded in model.py so the freeze-at-finalize stamp and any future
    PST-anchored field share one clock — eliminating the cross-midnight
    drift class where a date-derived field re-evaluates per render.
    """
    return datetime.now(_PST_ZONE).date()


# ──────────────────────────────────────────────────────────────────────
# Errors
# ──────────────────────────────────────────────────────────────────────


class SpineValidationError(ValueError):
    """Raised when a Spine invariant is violated.

    Distinct from pydantic.ValidationError so callers can distinguish
    'this state is malformed' (pydantic) from 'this state transition is
    illegal' (Spine business rule).
    """


# ──────────────────────────────────────────────────────────────────────
# Constants — operator-visible config thresholds. Encoded once, here.
# ──────────────────────────────────────────────────────────────────────

COST_VALIDATION_REQUIRED_ABOVE_CENTS = 10_000   # $100.00
COST_VALIDATION_FRESHNESS_DAYS = 30
SUPPORTED_AGENCIES = ("CCHCS",)                  # v1: one agency only.

# UOM allowlist — derived from Mike's actual procurement traffic, not
# from a generic ISO list. New UOMs must be added explicitly + tested.
SUPPORTED_UOM = (
    "EA",   # each
    "PK",   # pack
    "PAC",  # pack (alt spelling used by some CCHCS PDFs)
    "BX",   # box
    "CS",   # case
    "CT",   # carton
    "DZ",   # dozen
    "RL",   # roll
    "PR",   # pair
    "ST",   # set
    "BG",   # bag
    "BT",   # bottle
    "KIT",  # kit — assembled bundle sold as a single procurement unit
            # (e.g. CHCF wheel/tire/handrim kit for INVACARE 9000XT).
)


# ──────────────────────────────────────────────────────────────────────
# QuoteStatus state machine
# ──────────────────────────────────────────────────────────────────────


class QuoteStatus(str, Enum):
    """Linear state machine. No backward transitions in v1.

    parsed     — Vision/operator created the quote; line items present
                 but pricing may be incomplete. No tax requirement yet.
    priced     — All line items have cost + unit_price + cost source.
                 tax_rate_bps required.
    finalized  — Operator confirmed the priced state for ship. Cost
                 freshness gate fires here. No recompute on transition.
    sent       — Quote PDF has been delivered to the buyer. Terminal.
    """

    PARSED = "parsed"
    PRICED = "priced"
    FINALIZED = "finalized"
    SENT = "sent"


_ALLOWED_TRANSITIONS: dict[QuoteStatus, set[QuoteStatus]] = {
    QuoteStatus.PARSED: {QuoteStatus.PRICED},
    QuoteStatus.PRICED: {QuoteStatus.FINALIZED, QuoteStatus.PARSED},
    QuoteStatus.FINALIZED: {QuoteStatus.SENT, QuoteStatus.PRICED},
    QuoteStatus.SENT: set(),  # terminal — no further transitions in v1.
}


# Computed fields that must be excluded from persisted dicts /
# state-transition copies. Listed by exact field name so updates to
# the model schema fail loudly via the test suite, not silently here.
_COMPUTED_FIELD_NAMES_QUOTE = {"subtotal_cents", "tax_cents", "total_cents", "display_number"}
_COMPUTED_FIELD_NAMES_LINE = {"extension_cents", "markup_pct_display"}
_COMPUTED_FIELD_NAMES: dict = {
    **{name: True for name in _COMPUTED_FIELD_NAMES_QUOTE},
    "line_items": {"__all__": _COMPUTED_FIELD_NAMES_LINE},
}


# ──────────────────────────────────────────────────────────────────────
# LineItem
# ──────────────────────────────────────────────────────────────────────


class LineItem(BaseModel):
    """One row on a quote.

    Stored fields are the minimal set that fully describes the row.
    Anything derivable (extension, markup display) is a @computed_field.
    """

    model_config = ConfigDict(
        extra="forbid",          # unknown fields RAISE — no alias creep.
        str_strip_whitespace=True,
        frozen=False,            # mutable for in-place edits before
                                 # write; the DB write is the atomic
                                 # boundary, not the model.
    )

    line_no: int = Field(ge=1, description="1-based position on the quote.")
    description: str = Field(min_length=1, max_length=500)
    mfg_number: str | None = Field(default=None, max_length=64)
    qty: int = Field(gt=0, description="Whole-unit quantity.")
    uom: str = Field(min_length=1, max_length=8)

    cost_cents: int = Field(
        ge=0,
        description="Operator-verified per-unit cost in integer cents.",
    )
    # Cost source: at least one of url/note required when cost > threshold.
    # Validated by Quote.priced→finalized transition, not at LineItem
    # level — a 'parsed' quote may have cost without source yet.
    cost_source_url: str | None = Field(default=None, max_length=2000)
    cost_hand_validated_note: str | None = Field(default=None, max_length=500)
    cost_validated_at: datetime | None = Field(default=None)

    unit_price_cents: int = Field(
        ge=0,
        description=(
            "Operator-typed sell price per unit, integer cents. "
            "ONLY price field. No bid_price / price_per_unit / our_price."
        ),
    )

    @field_validator("uom")
    @classmethod
    def _uom_in_allowlist(cls, v: str) -> str:
        v_up = v.upper().strip()
        if v_up not in SUPPORTED_UOM:
            raise ValueError(
                f"uom={v!r} not in SUPPORTED_UOM={SUPPORTED_UOM!r}. "
                "Add it explicitly to spine/model.py if this is real."
            )
        return v_up

    @field_validator("cost_source_url")
    @classmethod
    def _url_shape(cls, v: str | None) -> str | None:
        if v is None or v == "":
            return None
        if not re.match(r"^https?://", v, re.IGNORECASE):
            raise ValueError(
                f"cost_source_url={v!r} must start with http:// or https://"
            )
        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def extension_cents(self) -> int:
        """qty × unit_price — derived on every read, never persisted."""
        return self.qty * self.unit_price_cents

    @computed_field  # type: ignore[prop-decorator]
    @property
    def markup_pct_display(self) -> float | None:
        """Display-only markup percentage. NOT stored, NOT writable.

        Returns None if cost is unknown or zero — there is no "default
        markup" fallback. The whole class of bugs caused by
        `markup = item.get('markup_pct') or default_markup or 25` cannot
        exist because markup is not a stored field.
        """
        if not self.cost_cents:
            return None
        delta = self.unit_price_cents - self.cost_cents
        return round(delta * 100.0 / self.cost_cents, 1)

    def cost_source_present(self) -> bool:
        """True iff at least one of url or hand-validated note is set."""
        if self.cost_source_url and self.cost_source_url.strip():
            return True
        if self.cost_hand_validated_note and self.cost_hand_validated_note.strip():
            return True
        return False

    def cost_validation_fresh(self, *, now: datetime | None = None) -> bool:
        """True iff cost_validated_at is within COST_VALIDATION_FRESHNESS_DAYS."""
        if self.cost_validated_at is None:
            return False
        ref = now if now is not None else datetime.now(timezone.utc)
        # Normalize to UTC-aware for safe subtraction.
        ts = self.cost_validated_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)
        return (ref - ts) <= timedelta(days=COST_VALIDATION_FRESHNESS_DAYS)


# ──────────────────────────────────────────────────────────────────────
# Quote
# ──────────────────────────────────────────────────────────────────────


class Quote(BaseModel):
    """One Spine quote. Single source of truth for the CCHCS flow.

    NO shipping field — Mike's universal procurement rule is encoded by
    the field's absence. Quote PDFs render the literal $0.00 in the
    renderer, not from the model.

    NO `extra` catch-all — the persistence P0 class
    (project_persistence_p0_class_2026_05_12) cannot recur here because
    unknown fields raise at construction.
    """

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )

    quote_id: str = Field(min_length=4, max_length=64, pattern=r"^[A-Za-z0-9_\-]+$")
    # Chrome MCP audit 2026-05-27 / G12 (operator-architect approval):
    # CalVet admitted as a Spine-renderable agency. This is a forward
    # declaration only — Job #1 (CCHCS legacy deletion) is still the
    # active migration; CalVet is the next-after-Job-#1 agency in the
    # queue. Legacy CalVet path remains until its own deletion gate
    # closes. Per §0 LAW 1: each new agency literal is a forward
    # commitment, NOT a coexistence guarantee.
    agency: Literal["CCHCS", "CalVet"]
    facility: str = Field(min_length=1, max_length=64)
    solicitation_number: str = Field(min_length=1, max_length=64)
    line_items: list[LineItem] = Field(min_length=1)
    tax_rate_bps: int = Field(
        ge=0,
        le=2000,  # 20%; sanity ceiling, CA max ~10.75%.
        description=(
            "Tax rate in basis points (1% = 100 bps). "
            "MUST be > 0 to reach status=priced. Validated at ingest."
        ),
    )
    status: QuoteStatus = QuoteStatus.PARSED

    # Buyer-facing sequential identity. Assigned once at first write by
    # db.write_quote pulling from spine_counters (PR #1039). Stored as
    # (year, seq) integer pair; the display string `R26Q####` is
    # computed at the surface so a year-rollover edit never desyncs
    # the rendered form from the underlying integer. Both nullable to
    # support (a) replay of pre-#1040 rows that never had a seq
    # assigned and (b) test fixtures that construct Quotes directly.
    quote_seq: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Sequential integer assigned from spine_counters at first "
            "persist. Substrate-owned: callers do NOT set this; "
            "db.write_quote stamps it."
        ),
    )
    quote_year: int | None = Field(
        default=None,
        ge=2024,
        le=2099,
        description=(
            "Year the quote_seq was assigned in. Pairs with quote_seq "
            "to render the buyer-facing R{yy}Q#### display."
        ),
    )

    # Frozen PST sign date — stamped exactly once, when the Quote first
    # transitions into FINALIZED, and preserved across every subsequent
    # render. Closes the cross-midnight drift class identified at
    # forms_render.py: a get_pst_date() injection re-derives today's date
    # on every adapter call, so a Quote finalized at 23:55 and rendered
    # again at 00:05 would silently change the bytes the buyer receives.
    # Pydantic validates the type; the freeze rule is enforced by
    # with_status (stamp on the first PARSED/PRICED → FINALIZED hop;
    # subsequent transitions must NOT re-stamp). Operators can also
    # construct a Quote with an explicit sign_date_pst (replay / fixture
    # path) — the explicit value wins and never gets clobbered.
    sign_date_pst: date | None = Field(
        default=None,
        description=(
            "Frozen Pacific-time (America/Los_Angeles) sign date. None "
            "until the first transition into status=finalized, at which "
            "point with_status stamps PST today. Renderers MUST use this "
            "in preference to today's date so the rendered bytes are "
            "deterministic from approved Spine state — independent of "
            "when the render happens to be executed."
        ),
    )

    # Provenance (immutable after first write).
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("line_items")
    @classmethod
    def _line_numbers_unique_and_consecutive(cls, items: list[LineItem]) -> list[LineItem]:
        nums = [li.line_no for li in items]
        if len(set(nums)) != len(nums):
            raise ValueError(f"duplicate line_no in line_items: {nums!r}")
        # Don't enforce strict 1..N consecutive — operators may delete
        # rows. Just sorted-ascending.
        if nums != sorted(nums):
            raise ValueError(f"line_items must be sorted by line_no ascending: {nums!r}")
        return items

    @model_validator(mode="after")
    def _check_status_preconditions(self) -> "Quote":
        """Validate the current status against its preconditions.

        This runs on every Quote construction — so a Quote loaded from
        the DB with an inconsistent state will raise immediately,
        rather than letting the inconsistency propagate to a renderer.
        """
        if self.status in (QuoteStatus.PRICED, QuoteStatus.FINALIZED, QuoteStatus.SENT):
            if self.tax_rate_bps <= 0:
                raise SpineValidationError(
                    f"status={self.status.value} requires tax_rate_bps > 0; "
                    f"got tax_rate_bps={self.tax_rate_bps}. "
                    "Tax must be looked up + validated at ingest."
                )
            for li in self.line_items:
                if li.unit_price_cents <= 0:
                    raise SpineValidationError(
                        f"status={self.status.value} requires every line item to have "
                        f"unit_price_cents > 0; line_no={li.line_no} has {li.unit_price_cents}."
                    )

        if self.status in (QuoteStatus.FINALIZED, QuoteStatus.SENT):
            for li in self.line_items:
                if li.cost_cents >= COST_VALIDATION_REQUIRED_ABOVE_CENTS:
                    if not li.cost_source_present():
                        raise SpineValidationError(
                            f"status={self.status.value} requires cost source on "
                            f"line_no={li.line_no} (cost_cents={li.cost_cents} >= "
                            f"{COST_VALIDATION_REQUIRED_ABOVE_CENTS}). "
                            "Set cost_source_url OR cost_hand_validated_note."
                        )
                    if not li.cost_validation_fresh():
                        raise SpineValidationError(
                            f"status={self.status.value} requires fresh "
                            f"cost_validated_at on line_no={li.line_no} "
                            f"(within {COST_VALIDATION_FRESHNESS_DAYS} days). "
                            f"Got cost_validated_at={li.cost_validated_at!r}."
                        )
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def subtotal_cents(self) -> int:
        return sum(li.extension_cents for li in self.line_items)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def tax_cents(self) -> int:
        """Integer-cents tax with banker's rounding.

        Closes finding #18 (shipping_option=included → tax_cents=0)
        structurally — there is no shipping_option field, and no
        conditional that can zero this on a non-zero subtotal.

        Uses integer-only banker's (round-half-to-even) on the bps
        math: subtotal_cents × tax_rate_bps / 10_000. This matches the
        rounding used on the 5/15 manifests (e.g., 9e63456e's tax
        $3,863.99 on subtotal $46,836.20 at 8.25%). Pure floor
        division would have produced $3,863.98 — a 1¢ deviation that
        compounds into operator-facing math errors.
        """
        total = self.subtotal_cents * self.tax_rate_bps
        quotient, remainder = divmod(total, 10_000)
        double_rem = remainder * 2
        if double_rem > 10_000:
            return quotient + 1
        if double_rem < 10_000:
            return quotient
        # Exact half — round to even.
        return quotient + 1 if quotient % 2 else quotient

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total_cents(self) -> int:
        """Subtotal + Tax. Shipping is the implicit constant $0.00."""
        return self.subtotal_cents + self.tax_cents

    @computed_field  # type: ignore[prop-decorator]
    @property
    def display_number(self) -> str | None:
        """Buyer-facing identifier — `R26Q40`, `R25Q161`, etc.

        Computed from quote_seq + quote_year so the rendered string is
        always consistent with the stored integers. Returns None when
        either component is absent — legacy rows from before the
        sequential-numbering substrate landed (PR #1040) won't have
        an assignment; renderers fall back to quote_id in that case.

        Format: `R{yy}Q{seq}` — NO zero-padding, mirrors Mike's prior
        buyer-facing convention (R25Q161, R26Q39). The substrate
        stores quote_seq as an integer so the rendered width grows
        naturally from `R26Q1` through `R26Q9999`+ without truncating.
        """
        if self.quote_seq is None or self.quote_year is None:
            return None
        return f"R{self.quote_year % 100:02d}Q{self.quote_seq}"

    # ──────────────────────────────────────────────────────────────
    # State machine
    # ──────────────────────────────────────────────────────────────

    def to_persisted_dict(self) -> dict:
        """Serialize for persistence / transport.

        Returns model_dump(mode='json') with every @computed_field
        excluded — subtotal_cents / tax_cents / total_cents /
        extension_cents / markup_pct_display are derived on every read
        and must never appear in stored or transported state.

        Routes use this for both GET responses (so the client never
        receives stale computed fields it might try to echo back) and
        as the inverse of model_validate (so a POST round-trip writes
        and reads the same JSON shape).
        """
        return self.model_dump(mode="json", exclude=_COMPUTED_FIELD_NAMES)

    def with_status(self, new_status: QuoteStatus) -> "Quote":
        """Return a new Quote with the requested status transition.

        Validates against _ALLOWED_TRANSITIONS, then re-validates the
        full Quote (which fires the model validator and enforces all
        per-status preconditions). Does NOT mutate line item values —
        finding #9 (Finalize button reverts operator edits) closed by
        construction.

        Raises SpineValidationError on illegal transitions OR on
        precondition violations during the transition. Construction
        of a Quote directly with bad preconditions raises Pydantic's
        ValidationError (a separate type contract).
        """
        if new_status not in _ALLOWED_TRANSITIONS[self.status]:
            raise SpineValidationError(
                f"illegal transition: {self.status.value} → {new_status.value}. "
                f"Allowed from {self.status.value}: "
                f"{sorted(s.value for s in _ALLOWED_TRANSITIONS[self.status])!r}"
            )
        new_state = self.model_dump(mode="python", exclude=_COMPUTED_FIELD_NAMES)
        new_state["status"] = new_status.value
        new_state["updated_at"] = datetime.now(timezone.utc)
        # Freeze the PST sign date the FIRST time the Quote enters
        # FINALIZED. Stamping here (not on every transition) means
        # FINALIZED → SENT and PRICED ↔ FINALIZED rebid arcs preserve
        # the original frozen date — the bytes the buyer eventually
        # receives match the date displayed at first finalize.
        if (
            new_status == QuoteStatus.FINALIZED
            and new_state.get("sign_date_pst") is None
        ):
            new_state["sign_date_pst"] = _pst_today()
        try:
            return Quote.model_validate(new_state)
        except Exception as e:
            # Preserve the state-transition error type contract.
            raise SpineValidationError(str(e)) from e
