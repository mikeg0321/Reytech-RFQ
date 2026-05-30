"""Email contract → Spine Quote ingest.

The Vision-primary parser (legacy: src/agents/...) produces an
"email contract" dict for every inbound RFQ — see PR #914 (5/11) for
the canonical schema. This module turns that contract into a Spine
Quote with tax_rate_bps already resolved.

Charter rule #6: tax_rate_bps is MANDATORY at ingest. If CDTFA lookup
fails, ingest FAILS. The Spine deliberately does not allow a quote to
exist in any state without its tax rate resolved — the latency between
ingest and pricing was the gap the 5/15 tax-zero bug hid in.

The CDTFA tax resolver is injected. In prod this is the existing
tax_resolver.resolve_tax (from src.core); in tests we pass a stub
that returns deterministic bps. This keeps the Spine package
Flask-free AND CDTFA-free.

Email contract schema (subset we require):

    {
      "rfq_id": "abc123",                 # required
      "agency": "CCHCS",                  # required (v1 CCHCS only)
      "facility": "SATF Corcoran 93212",  # required for tax lookup
      "ship_to": "...",                   # required for tax lookup
      "solicitation_number": "PREQ 10847262",
      "line_items": [
        { "description": "...", "qty": 10, "uom": "EA",
          "item_number": "MFG-X" },
        ...
      ],
      "buyer": { "name": "...", "email": "..." },  # optional
      "due_date": "2026-05-13",                    # optional
    }

Note: line items have NO cost or unit_price at ingest. The operator
adds those during pricing. The Spine accepts unit_price_cents=0 in
'parsed' status; the priced→finalized transitions will demand real
prices later.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from src.spine import (
    ContractLineItem,
    EmailContract,
    LineItem,
    Quote,
    QuoteStatus,
    SpineValidationError,
)
from src.spine_bridge.translator import (
    TranslationIssue,
    _resolve_uom,  # private but project-internal; keeps UOM logic single-sourced
)


# Tax resolver callable: ship-to address (or full contract) → bps, or None.
# Production wires this to tax_resolver.resolve_tax which calls CDTFA.
TaxResolver = Callable[[str], int | None]


@dataclass
class IngestResult:
    """Outcome of ingesting one email contract.

    On success (ok==True), `quote` is the canonical Spine Quote and
    `email_contract` is the immutable buyer-statement that drove
    ingest. Callers should write both to the DB: write_email_contract
    FIRST (it's the master substrate), then write_quote. If either
    field is None, the ingest failed — see issues for details.
    """
    quote: Quote | None
    email_contract: EmailContract | None = None
    issues: list[TranslationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.quote is not None and self.email_contract is not None

    def errors(self) -> list[TranslationIssue]:
        return [i for i in self.issues if i.severity == "error"]


# ──────────────────────────────────────────────────────────────────────
# Ingest
# ──────────────────────────────────────────────────────────────────────


def ingest_email_contract(
    contract: dict,
    *,
    tax_resolver: TaxResolver,
    ingest_ts: datetime | None = None,
) -> IngestResult:
    """Turn an email contract into a Spine Quote in 'parsed' status.

    Args:
        contract: Vision-parsed email contract dict (see module docstring).
        tax_resolver: Function (ship_to_str) → bps-or-None. Required.
        ingest_ts: Optional timestamp override (for test determinism).
            Defaults to now(). Used as cost_validated_at on every line
            item — at ingest, cost is 0 with no source, so the
            timestamp is just provenance for "when this quote was
            created in the Spine".

    Returns:
        IngestResult with .quote set iff:
        - the agency is supported,
        - tax_resolver returned a positive bps,
        - every line item parsed (description + qty + uom),
        - no contract-level fields conflict with Spine invariants.
    """
    issues: list[TranslationIssue] = []
    ingest_ts = ingest_ts or datetime.now(timezone.utc)

    # ── Required header fields ───────────────────────────────────────

    rfq_id = (contract.get("rfq_id") or contract.get("id") or "").strip()
    if not rfq_id:
        issues.append(TranslationIssue(
            "error", "rfq_id", "email contract has no rfq_id / id field",
        ))

    agency = contract.get("agency") or "CCHCS"
    if agency != "CCHCS":
        issues.append(TranslationIssue(
            "error", "agency",
            f"agency={agency!r} not yet supported in the Spine. v1 is CCHCS-only.",
        ))

    facility = (contract.get("facility") or "").strip()[:64]
    ship_to = str(contract.get("ship_to") or "").strip()
    if not facility and not ship_to:
        issues.append(TranslationIssue(
            "error", "facility",
            "no facility OR ship_to in contract — tax lookup impossible",
        ))
    if not facility:
        # Derive a short facility label from ship_to first line.
        facility = ship_to.split("\n")[0][:64] or "UNKNOWN"

    # Strip PREQ-style prefix via the shared helper so this and the
    # translator can't drift (one of the substrate-meltdown classes).
    from src.spine_bridge._solicitation import strip_solicitation_prefix
    sol_raw = strip_solicitation_prefix(contract.get("solicitation_number"))
    if not sol_raw:
        issues.append(TranslationIssue(
            "error", "solicitation_number",
            "missing solicitation_number — CCHCS requires it for routing",
        ))
    solicitation = sol_raw[:64]

    # ── MANDATORY tax-at-ingest ──────────────────────────────────────

    tax_lookup_input = ship_to or facility
    tax_bps: int | None = None
    try:
        tax_bps = tax_resolver(tax_lookup_input)
    except Exception as e:
        issues.append(TranslationIssue(
            "error", "tax_rate_bps",
            f"tax_resolver raised an exception for {tax_lookup_input!r}: {e}",
        ))

    if tax_bps is None or tax_bps <= 0:
        issues.append(TranslationIssue(
            "error", "tax_rate_bps",
            f"CDTFA tax resolver returned no usable rate for "
            f"{tax_lookup_input!r}. Charter rule #6: tax is mandatory at "
            "ingest — refusing to create a quote without it.",
        ))

    # Record contract-side fields we deliberately don't store on the Spine.
    for k in ("shipping_option", "shipping_amount", "delivery_option"):
        if contract.get(k) is not None:
            issues.append(TranslationIssue(
                "info", k,
                f"dropped per Charter rule #7; contract value: {contract.get(k)!r}",
            ))

    # ── Line items ───────────────────────────────────────────────────

    raw_items = contract.get("line_items") or contract.get("items") or []
    if not raw_items:
        issues.append(TranslationIssue(
            "error", "line_items",
            "contract has no line_items — refuse to ingest empty quote",
        ))

    line_items: list[LineItem] = []
    for idx, raw in enumerate(raw_items):
        line_path = f"line_items[{idx}]"
        if not isinstance(raw, dict):
            issues.append(TranslationIssue(
                "error", line_path,
                f"line item is not a dict: {type(raw).__name__}",
            ))
            continue

        desc = (raw.get("description") or "").strip()[:500]
        if not desc:
            issues.append(TranslationIssue(
                "error", f"{line_path}.description",
                "line item has no description",
            ))
            continue

        raw_qty = raw.get("qty") or raw.get("quantity") or 1
        try:
            qty = int(float(raw_qty))
        except (TypeError, ValueError):
            issues.append(TranslationIssue(
                "error", f"{line_path}.qty",
                f"non-numeric qty: {raw_qty!r}",
            ))
            continue
        if qty < 1:
            issues.append(TranslationIssue(
                "error", f"{line_path}.qty",
                f"qty must be >= 1; got {qty}",
            ))
            continue

        # At ingest, we don't yet know the unit price OR cost. The
        # Spine model accepts (cost_cents=0, unit_price_cents=0) in
        # 'parsed' status. The operator advances to 'priced' after
        # adding prices.
        try:
            li = LineItem(
                line_no=idx + 1,
                description=desc,
                mfg_number=raw.get("item_number") or raw.get("mfg_number") or None,
                qty=qty,
                uom=_resolve_uom(raw),
                cost_cents=0,
                cost_source_url=None,
                cost_hand_validated_note=None,
                cost_validated_at=ingest_ts,
                unit_price_cents=0,
            )
        except Exception as e:
            issues.append(TranslationIssue(
                "error", line_path,
                f"Spine LineItem rejected this row: {e}",
            ))
            continue

        line_items.append(li)

    # ── Bail on any error ────────────────────────────────────────────

    if any(i.severity == "error" for i in issues):
        return IngestResult(quote=None, issues=issues)

    # ── Build the Quote ──────────────────────────────────────────────

    try:
        quote = Quote(
            quote_id=rfq_id,
            agency=agency,  # type: ignore[arg-type]
            facility=facility,
            solicitation_number=solicitation,
            line_items=line_items,
            tax_rate_bps=tax_bps,  # type: ignore[arg-type]  # validated above
            status=QuoteStatus.PARSED,
            created_at=ingest_ts,
            updated_at=ingest_ts,
        )
    except (SpineValidationError, Exception) as e:
        issues.append(TranslationIssue(
            "error", "quote",
            f"Spine Quote rejected the assembled state: {e}",
        ))
        return IngestResult(quote=None, issues=issues)

    # ── Build the immutable EmailContract (master substrate) ─────────
    try:
        email_contract = _build_email_contract(
            contract=contract,
            rfq_id=rfq_id,
            agency=agency,
            facility=facility,
            solicitation=solicitation,
            tax_bps=tax_bps,
            raw_items=raw_items,
            ingest_ts=ingest_ts,
        )
    except (SpineValidationError, Exception) as e:
        issues.append(TranslationIssue(
            "error", "email_contract",
            f"EmailContract rejected the buyer-side projection: {e}",
        ))
        return IngestResult(quote=None, issues=issues)

    return IngestResult(
        quote=quote,
        email_contract=email_contract,
        issues=issues,
    )


# ──────────────────────────────────────────────────────────────────────
# EmailContract construction — pure projection from the contract dict
# ──────────────────────────────────────────────────────────────────────


_CID_SAFE = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"


def _sanitize_for_cid(s: str) -> str:
    return "".join(c if c in _CID_SAFE else "-" for c in s)[:60]


def _resolve_canonical_bill_to(agency: str | None) -> tuple[str | None, str | None, str | None]:
    """Return (bill_to_name, bill_to_email, bill_to_address) for a
    canonical agency code.

    CCHCS reads from the Spine-native `src.spine.agency_constants`
    module (Architect-approved per §0 LAW 4, ticket PR-Job1-A0, 2026-05-27).
    This is the Job #1 prerequisite: Job #1's deletion of
    `AGENCY_CONFIGS["CCHCS"]` would silently strip bill_to_* from every
    new CCHCS EmailContract if the Spine still depended on the legacy
    entry. The Spine now owns the CCHCS bill-to answer.

    Other agencies (CDCR, CalVet, DSH, DGS) continue to read from the
    legacy AGENCY_CONFIGS table — they are OUT OF SCOPE for Job #1 and
    will migrate to Spine-native constants under their own tickets.

    Legacy AGENCY_CONFIGS stores bill_to_name + bill_to_lines (4-5
    strings: street(s), city/state/zip, sometimes an email as the last
    line). We pop the last line if it's email-shaped, then "\\n"-join
    the rest as the single bill_to_address string the Spine model holds.

    §0 LAW 6: this MUST run AT INGEST so the EmailContract carries the
    answer the renderer needs — no incremental render-time lookup. The
    PDF address blocks in src/spine/quote_pdf.py:528-591 read the
    contract; if it's null, they fall back to bare quote.agency /
    quote.facility, which is the bug this resolver closes (caught by
    Mike on Duffey rfq_89bb9a3e PDF on 2026-05-26).

    Returns (None, None, None) when the agency is unknown so the caller
    can fall back to raw-dict values without breaking ingest for
    non-migrated agencies.
    """
    if not agency:
        return None, None, None
    if agency == "CCHCS":
        from src.spine.agency_constants import cchcs_bill_to_tuple
        name, email, address_lines = cchcs_bill_to_tuple()
        address = "\n".join(address_lines) or None
        return name, email, address
    try:
        from src.forms.quote_generator import AGENCY_CONFIGS
    except Exception:
        return None, None, None
    cfg = AGENCY_CONFIGS.get(agency)
    if not cfg:
        return None, None, None
    name = (cfg.get("bill_to_name") or "").strip() or None
    lines = [str(s).strip() for s in (cfg.get("bill_to_lines") or []) if s and str(s).strip()]
    email: str | None = None
    if lines and "@" in lines[-1]:
        email = lines.pop()
    address = "\n".join(lines) or None
    return name, email, address


def _resolve_canonical_ship_to(facility: str | None) -> tuple[str | None, str | None]:
    """Return (ship_to_facility, ship_to_address) by resolving free-text
    facility through the canonical facility_registry. ship_to_address
    joins address_line1 + address_line2 with "\\n" so the renderer
    (_address_to_html in quote_pdf.py) splits them onto separate lines.

    Returns (None, None) when the registry can't unambiguously resolve
    (per facility_registry.resolve contract — never silently guesses).
    The caller's fallback uses raw-dict values, preserving today's
    behavior for un-resolvable facility strings.
    """
    if not facility:
        return None, None
    try:
        from src.core.facility_registry import resolve as _resolve_facility
    except Exception:
        return None, None
    rec = _resolve_facility(facility)
    if not rec:
        return None, None
    addr_lines = [s for s in (rec.address_line1, rec.address_line2) if s and str(s).strip()]
    address = "\n".join(addr_lines) or None
    return rec.canonical_name or None, address


def _build_email_contract(
    *,
    contract: dict,
    rfq_id: str,
    agency: str,
    facility: str,
    solicitation: str,
    tax_bps: int | None,
    raw_items: list,
    ingest_ts: datetime,
) -> EmailContract:
    """Build an EmailContract from the parsed contract dict.

    Called only after Quote-construction succeeds, so we can trust the
    header/line invariants. Captures buyer-side fields that Quote
    intentionally drops (buyer name/email, due_date, attachment refs,
    parser version) so the audit chain stays complete.

    Bill-to + ship-to addresses are resolved canonically here (LAW 6)
    from AGENCY_CONFIGS + facility_registry when the raw dict doesn't
    supply them. The Vision/Adobe parser doesn't extract bill-to
    (it's an agency constant, not in the solicitation PDF) and rarely
    extracts ship-to addresses cleanly. Raw-dict values still win when
    present so an operator override or a richer parser doesn't get
    stomped — fallback-only, never override.
    """
    contract_id = f"contract_{_sanitize_for_cid(rfq_id)}_{int(ingest_ts.timestamp())}"

    buyer = contract.get("buyer") or {}
    if not isinstance(buyer, dict):
        buyer = {}

    def _opt_str(*keys: str, src: dict | None = None) -> str | None:
        src = src or contract
        for k in keys:
            v = src.get(k)
            if v is not None and str(v).strip():
                return str(v).strip()
        return None

    def _opt_dt(*keys: str) -> datetime | None:
        for k in keys:
            v = contract.get(k)
            if not v:
                continue
            if isinstance(v, datetime):
                return v
            try:
                return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
            except Exception:
                continue
        return None

    contract_line_items: list[ContractLineItem] = []
    for idx, raw in enumerate(raw_items):
        if not isinstance(raw, dict):
            continue
        desc = (raw.get("description") or "").strip()[:500]
        if not desc:
            continue
        try:
            qty = int(float(raw.get("qty") or raw.get("quantity") or 1))
        except (TypeError, ValueError):
            continue
        if qty < 1:
            continue
        contract_line_items.append(ContractLineItem(
            line_no=idx + 1,
            description=desc,
            qty=qty,
            uom=_resolve_uom(raw),
            mfg_number_suggested=(
                _opt_str("item_number", "mfg_number", "mfg_number_suggested", src=raw)
            ),
            buyer_hints=_opt_str("buyer_hints", "hints", "notes", src=raw),
        ))

    # required_forms + response_packaging — PR-2 (Job #1). The shadow-
    # ingest classifier emits these; validate FormCodes here and fall
    # back to the CCHCS default on anything unrecognized.
    from src.spine.email_contract import ALL_FORM_CODES, CCHCS_DEFAULT_REQUIRED_FORMS
    _raw_forms = contract.get("required_forms")
    if (
        isinstance(_raw_forms, list)
        and _raw_forms
        and all(f in ALL_FORM_CODES for f in _raw_forms)
    ):
        _required_forms = list(_raw_forms)
    else:
        _required_forms = list(CCHCS_DEFAULT_REQUIRED_FORMS)
    _raw_packaging = contract.get("response_packaging")
    _response_packaging = (
        _raw_packaging
        if _raw_packaging in ("single_pdf", "separate_pdfs", "either")
        else "separate_pdfs"
    )

    # Canonical address resolution — LAW 6 (every answer at ingest).
    # Raw-dict values win when present (operator override / richer
    # parser); canonical lookup fills the gap that Vision/Adobe leaves.
    _cn_bill_name, _cn_bill_email, _cn_bill_addr = _resolve_canonical_bill_to(agency)
    _cn_ship_facility, _cn_ship_addr = _resolve_canonical_ship_to(facility)

    return EmailContract(
        contract_id=contract_id,
        rfq_id=rfq_id,
        pc_id=_opt_str("pc_id"),
        source_email_id=_opt_str("source_email_id", "message_id", "email_id"),
        source_thread_id=_opt_str("source_thread_id", "thread_id"),
        buyer_name=_opt_str("name", src=buyer),
        buyer_email=_opt_str("email", src=buyer),
        buyer_phone=_opt_str("phone", src=buyer),
        buyer_title=_opt_str("title", src=buyer),
        agency=agency,  # type: ignore[arg-type]
        facility=facility,
        institution_code=_opt_str("institution_code", "institution"),
        solicitation_number=solicitation,
        rfq_title=_opt_str("rfq_title", "subject", "title"),
        release_date=_opt_dt("release_date", "released_at"),
        due_date=_opt_dt("due_date", "due_at"),
        bill_to_name=_opt_str("bill_to_name") or _cn_bill_name,
        bill_to_email=_opt_str("bill_to_email") or _cn_bill_email,
        bill_to_address=_opt_str("bill_to_address") or _cn_bill_addr,
        ship_to_address=_opt_str("ship_to", "ship_to_address") or _cn_ship_addr,
        ship_to_facility=_opt_str("ship_to_facility") or _cn_ship_facility,
        tax_rate_bps=tax_bps,
        line_items=contract_line_items,
        attachment_refs=[
            str(a) for a in (contract.get("attachment_refs") or [])
            if str(a).strip()
        ],
        required_forms=_required_forms,  # type: ignore[arg-type]
        response_packaging=_response_packaging,  # type: ignore[arg-type]
        ingest_parser_version=_opt_str("parser_version", "ingest_parser_version") or "unknown",
        ingested_at=ingest_ts,
    )


# ──────────────────────────────────────────────────────────────────────
# Generate-time on-ramp (J1-1)
# ──────────────────────────────────────────────────────────────────────


class NotCchcsError(ValueError):
    """Raised by synthesize_cchcs_email_contract when the RFQ row is not
    a CCHCS quote.

    J1-1: the on-ramp is CCHCS-only. The caller (generate-time bridge in
    routes_rfq_gen.py, J1-2) must gate on this before calling so it does
    not silently produce a CCHCS contract for a non-CCHCS RFQ.
    """


def synthesize_cchcs_email_contract(
    rfq_row: dict,
    rfq_id: str,
    *,
    tax_resolver: TaxResolver,
    synthesis_ts: datetime | None = None,
) -> EmailContract:
    """Synthesize an EmailContract for a CCHCS RFQ row at generate time.

    This is the generate-time on-ramp for Job #1 (J1-1). It takes the
    legacy RFQ dict as loaded by ``load_rfqs()`` (the ``r`` dict) and
    returns a transient Spine ``EmailContract`` built on-demand by
    reusing ``_build_email_contract``.

    **No persistence.** This function does NOT write to spine.db, does
    NOT create a Quote object, and does NOT touch any legacy DB table.
    It is a pure synthesizer — call it once per generate request,
    discard after use. The J1-2 ticket wires the returned contract into
    the form-set selection path; J1-4/J1-5 delete the legacy config
    entries after that wiring proves correct.

    **Tax resolver is injected** (same pattern as
    ``ingest_email_contract``) so the caller can pass the prod CDTFA
    resolver or a deterministic stub in tests. Use
    ``shadow_ingest._make_tax_resolver()`` for the prod binding.

    Args:
        rfq_row: The legacy RFQ dict from ``load_rfqs()[rfq_id]``.
            Must have ``agency`` (or ``agency_key``) matching CCHCS.
            Required keys for a non-empty contract:
            ``solicitation_number``, ``line_items``.
            Optional but carried: ``ship_to``, ``facility``/
            ``institution``, ``due_date``, ``requestor_email``.
        rfq_id: The string RFQ identifier (e.g. ``"rfq_abc123"``).
            Used as ``rfq_id`` on the synthesized contract.
        tax_resolver: Callable ``(address: str) -> int | None`` that
            returns basis-points or None. Same contract as
            ``ingest_email_contract``'s resolver.
        synthesis_ts: Timestamp override for test determinism.
            Defaults to ``datetime.now(timezone.utc)``.

    Returns:
        An ``EmailContract`` with all CCHCS canonical bill-to fields
        populated from ``src.spine.agency_constants.cchcs_bill_to_tuple()``.

    Raises:
        NotCchcsError: if ``rfq_row`` is not a CCHCS quote (agency field
            does not resolve to "CCHCS"). Caller must check before calling.
        ValueError: if tax_resolver returns no usable rate or raises.
        SpineValidationError: if the synthesized contract fails Pydantic
            validation (e.g. empty line_items or missing solicitation).

    Example (J1-2 caller pattern)::

        from src.spine_bridge.ingest import (
            synthesize_cchcs_email_contract, NotCchcsError,
        )
        from src.spine_bridge.shadow_ingest import _make_tax_resolver

        try:
            contract = synthesize_cchcs_email_contract(
                rfq_row=r, rfq_id=rid, tax_resolver=_make_tax_resolver(),
            )
        except NotCchcsError:
            pass  # not CCHCS — continue legacy path
        # use contract.bill_to_name, contract.required_forms, etc.
    """
    synthesis_ts = synthesis_ts or datetime.now(timezone.utc)

    # ── Agency gate ──────────────────────────────────────────────────
    # Normalize the legacy agency field the same way shadow_ingest does.
    _agency_raw = (
        rfq_row.get("agency")
        or rfq_row.get("agency_key")
        or ""
    )
    _agency_upper = str(_agency_raw).strip().upper()
    if _agency_upper not in ("CCHCS", "CCHCS-ACQ"):
        raise NotCchcsError(
            f"synthesize_cchcs_email_contract: rfq_id={rfq_id!r} has "
            f"agency={_agency_raw!r} — expected CCHCS or CCHCS-ACQ. "
            "Call only for CCHCS RFQs."
        )

    # ── Project legacy RFQ dict → ingest contract shape ──────────────
    # Mirrors _build_contract_dict in shadow_ingest.py, but reads
    # directly from the legacy RFQ dict (already loaded; no
    # classification object or email metadata needed at generate time).
    facility = (
        (rfq_row.get("institution") or "").strip()
        or (rfq_row.get("facility") or "").strip()
        or "UNKNOWN"
    )
    ship_to = (
        (rfq_row.get("ship_to") or "").strip()
        or (rfq_row.get("delivery_address") or "").strip()
        or (rfq_row.get("delivery_location") or "").strip()
    )

    sol_raw = (rfq_row.get("solicitation_number") or "").strip()

    raw_items = rfq_row.get("line_items") or rfq_row.get("items") or []
    line_items_for_contract: list[dict] = []
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        desc = (it.get("description") or "").strip()
        if not desc:
            continue
        line_items_for_contract.append({
            "description": desc,
            "qty": it.get("qty") or it.get("quantity") or 1,
            "uom": (it.get("uom") or it.get("unit") or "EA"),
            "item_number": (
                it.get("item_number")
                or it.get("mfg_number")
                or it.get("mfg")
                or ""
            ),
        })

    # ── Tax resolution ────────────────────────────────────────────────
    tax_lookup_input = ship_to or facility
    try:
        tax_bps: int | None = tax_resolver(tax_lookup_input)
    except Exception as exc:
        raise ValueError(
            f"synthesize_cchcs_email_contract: tax_resolver raised for "
            f"{tax_lookup_input!r}: {exc}"
        ) from exc
    if tax_bps is None or tax_bps <= 0:
        raise ValueError(
            f"synthesize_cchcs_email_contract: tax_resolver returned no "
            f"usable rate for {tax_lookup_input!r}. "
            "Charter rule #6: tax is mandatory."
        )

    # ── Solicitation strip (reuse shared helper) ──────────────────────
    from src.spine_bridge._solicitation import strip_solicitation_prefix
    solicitation = strip_solicitation_prefix(sol_raw) or sol_raw or "UNKNOWN"

    # ── Assemble the ingest-contract dict ─────────────────────────────
    # This is the same dict shape _build_email_contract() reads.
    contract_dict: dict = {
        "rfq_id": rfq_id,
        "agency": "CCHCS",
        "facility": facility,
        "ship_to": ship_to,
        "solicitation_number": solicitation,
        "line_items": line_items_for_contract,
        "buyer": {
            "email": (rfq_row.get("requestor_email") or "").strip(),
        },
        "due_date": rfq_row.get("due_date") or "",
        "rfq_title": rfq_row.get("rfq_title") or rfq_row.get("subject") or "",
        "parser_version": "generate_time_bridge_v1",
        # attachment_refs, source_email_id, source_thread_id are not
        # available at generate time — leave absent (optional in schema).
    }

    # ── Build the contract via the canonical builder ──────────────────
    # _build_email_contract is the single home for EmailContract
    # construction; we reuse it so bill_to resolution and required_forms
    # defaulting stay in one place. Any fix there applies here too.
    return _build_email_contract(
        contract=contract_dict,
        rfq_id=rfq_id,
        agency="CCHCS",
        facility=facility,
        solicitation=solicitation,
        tax_bps=tax_bps,
        raw_items=line_items_for_contract,
        ingest_ts=synthesis_ts,
    )
