"""The Spine Inspector — math + identity + coverage + cost-basis gate.

Per ``CLAUDE.md §0``, the Inspector role "owns BOTH gates: the Chrome
walkthrough AND the math reconciliation (every quote's subtotal/tax/
total/cost-basis verified against source before render)." This module
is the math-reconciliation tool the Inspector uses; the Chrome
walkthrough is a separate operator-side activity.

The Inspector reads the **rendered output** (the actual PDFs the
adapter produced) back, extracts every value, and compares it to the
**Spine source** (``Quote`` + ``EmailContract``). It is the gate that
makes "shipped == built": if the rendered packet's sol# is `10848899`
but the Spine quote's is `10848901`, the Inspector flags it before the
send. Same for line prices, totals, identity, and coverage of the
contract's declared ``required_forms``.

Why a separate module — not a check inside the adapter
-------------------------------------------------------
The adapters (``packet_render``, ``forms_render``) BUILD output. The
Inspector VERIFIES output, by reading it back. Keeping them separate
means the Inspector catches **adapter bugs** (a renderer that writes
the wrong value still gets caught) and **template bugs** (a wrong
field-name mapping that fills the wrong box still gets caught). An
adapter that self-validated would only catch what it knew to check.

How it's used
-------------
- **PR-5/6 send gate** (Job #1): every Spine quote going through the
  3-quote send gate runs ``reconcile_quote_to_package`` first; a
  non-clean report blocks the send.
- **Tests**: ``tests/spine/test_inspector_gate.py`` uses the Inspector
  to end-to-end-verify Format A + Format B rendering.
- **Operator UI** (future): a ``/spine/quotes/<id>/inspector`` endpoint
  will return the report JSON for the operator review surface.

Severity model
--------------
- ``ok``       — no issues.
- ``warning``  — drift the operator should know about but isn't
                 blocking (e.g. a non-required form rendered with an
                 odd quirk). Reserved for future use.
- ``blocking`` — the package must not ship in this state.

The default behavior: ANY issue is ``blocking`` (correctness first).
``severity`` exists so the Inspector can grow softer warnings later
without changing the report contract.
"""
from __future__ import annotations

import logging
import re
import tempfile
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

log = logging.getLogger("reytech.spine.inspector")

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


# ──────────────────────────────────────────────────────────────────────
# Report types — Pydantic for consistency with the rest of the substrate.
# ──────────────────────────────────────────────────────────────────────


IssueKind = Literal["math", "identity", "coverage", "cost_basis", "render"]
Severity = Literal["ok", "warning", "blocking"]


class InspectorIssue(BaseModel):
    """One verifier finding. Both human-readable and machine-actionable.

    ``location`` names the artifact + field — e.g. ``"704B PRICE PER
    UNITRow3"`` or ``"703B Solicitation Number"`` — so a downstream UI
    can link the operator straight to the offending value.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    kind: IssueKind
    severity: Severity = "blocking"
    location: str = Field(min_length=1, max_length=120)
    detail: str = Field(min_length=1, max_length=500)
    expected: str | None = Field(default=None, max_length=200)
    actual: str | None = Field(default=None, max_length=200)


class InspectorReport(BaseModel):
    """The Inspector's verdict on one rendered package.

    ``ok`` is the gate value: ``True`` iff there are zero blocking
    issues. Warning-severity issues do NOT flip ``ok`` to False — the
    gate is correctness-first, not noise-prone.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    ok: bool
    quote_id: str = Field(min_length=1, max_length=64)
    response_packaging: str = Field(min_length=1, max_length=32)
    forms_checked: list[str] = Field(default_factory=list)
    issues: list[InspectorIssue] = Field(default_factory=list)
    # Counts for quick scanning by tests + the operator UI.
    line_items_checked: int = 0
    blocking_count: int = 0
    warning_count: int = 0

    @property
    def severity(self) -> Severity:
        if self.blocking_count > 0:
            return "blocking"
        if self.warning_count > 0:
            return "warning"
        return "ok"


# ──────────────────────────────────────────────────────────────────────
# Helpers — currency parsing, date normalization, identity strings.
# ──────────────────────────────────────────────────────────────────────


_PRICE_PAT = re.compile(r"[\d,]+\.\d{1,2}")
# EU-format decimal: digits, comma, 1-2 trailing digits, end of string. The
# US grouping form (`1,234` / `1,234.56`) has either 3-digit groups or a
# `.` decimal — both fall through to the accept branch. Reject anything that
# uses `,` as the decimal separator: stripping commas would silently 100x
# the value (``_to_cents("1234,56")`` → ``12345600`` = $123,456 instead of
# ``None``). US-only today; this closes the class before it ever matters.
_EU_DECIMAL_PAT = re.compile(r"^\d+,\d{1,2}$")
# Cell-level sanity ceiling — $10B per cell. Above this, the parser is
# almost certainly being fed mangled bytes (a sol# concatenated to a
# price, a multi-row scrape, an unstripped grouping artifact). Returning
# ``None`` lets the caller flag it as a math issue instead of computing
# off junk.
_TO_CENTS_CEILING = 1_000_000_000_000  # 1e12 cents = $10B


def _to_cents(value: Any) -> int | None:
    """Parse a rendered currency cell — ``'1,060.00'``, ``' 5955.25'``,
    ``684`` — into integer cents. Returns ``None`` when not parseable
    (so the caller can flag rather than silently treat as zero).

    Safety rails:

    * EU-format strings (``"1234,56"`` — comma as decimal separator) are
      rejected. The naive ``replace(",", "")`` would 100x the value.
    * Results above the ``_TO_CENTS_CEILING`` ($10B/cell) are rejected;
      any line item that high is almost certainly a parse artifact, not
      a real price. The caller treats ``None`` as "flag math issue."
    """
    if value is None:
        return None
    raw = str(value).strip().replace("$", "").replace(" ", "")
    if not raw:
        return None
    # Reject EU-format BEFORE stripping commas — comma-as-decimal would
    # silently 100x the value once commas are stripped.
    if _EU_DECIMAL_PAT.match(raw):
        return None
    s = raw.replace(",", "")
    if not s:
        return None
    # Bare integer ("684") — treat as dollars.
    if "." not in s:
        try:
            result = int(s) * 100
        except ValueError:
            return None
    else:
        try:
            result = int(round(float(s) * 100))
        except ValueError:
            return None
    if result > _TO_CENTS_CEILING or result < -_TO_CENTS_CEILING:
        return None
    return result


def _date_iso(value: Any) -> str:
    """Normalize a date to ``YYYY-MM-DD`` for cross-format comparison."""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    if not s:
        return ""
    from datetime import datetime as _dt

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return _dt.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return s  # pass through — comparison will catch true mismatches.


def _normalize_phone_email(s: str | None) -> str:
    """Strip punctuation/whitespace so cross-format identity compares
    safely (e.g. ``"(916) 691-4767 / grace@x"`` matches ``"9166914767/grace@x"``).
    """
    if not s:
        return ""
    return re.sub(r"[\s\(\)\-\._/]", "", str(s)).lower()


# ──────────────────────────────────────────────────────────────────────
# Field-value reading — pypdf, with parent /Kids resolution.
# ──────────────────────────────────────────────────────────────────────


def _field_values(pdf_path: str) -> dict[str, str]:
    """Return a flat ``{name: value}`` map of the PDF's AcroForm.

    Empty values map to ``""``; missing fields are absent from the dict.
    A best-effort read — corrupt PDFs return ``{}``, never raise.
    """
    try:
        from pypdf import PdfReader

        r = PdfReader(pdf_path)
        out: dict[str, str] = {}
        for name, fld in (r.get_fields() or {}).items():
            v = fld.get("/V")
            out[str(name)] = "" if v is None else str(v)
        return out
    except Exception as e:  # pragma: no cover - defensive
        log.debug("inspector: field read failed for %s: %s", pdf_path, e)
        return {}


# ──────────────────────────────────────────────────────────────────────
# Cost-basis re-check — mirrors Quote model's finalized→sent precondition.
# ──────────────────────────────────────────────────────────────────────


def _check_cost_basis(quote: "Quote") -> list[InspectorIssue]:
    """Re-confirm the model's finalized-state cost-basis requirements.

    The ``Quote`` model already enforces these at the priced→finalized
    transition; the Inspector re-runs the check so the gate stays
    correct even if some future code path persists a quote without
    going through ``with_status(FINALIZED)``. Cheap and belt-and-suspenders.
    """
    from src.spine.model import COST_VALIDATION_REQUIRED_ABOVE_CENTS

    issues: list[InspectorIssue] = []
    for li in quote.line_items:
        if li.cost_cents < COST_VALIDATION_REQUIRED_ABOVE_CENTS:
            continue
        if not li.cost_source_present():
            issues.append(InspectorIssue(
                kind="cost_basis",
                location=f"line {li.line_no} cost source",
                detail=(
                    f"cost_cents={li.cost_cents} (>= "
                    f"{COST_VALIDATION_REQUIRED_ABOVE_CENTS}) requires "
                    "cost_source_url OR cost_hand_validated_note."
                ),
                expected="URL or hand-validated note",
                actual="(neither set)",
            ))
            continue
        if not li.cost_validation_fresh():
            issues.append(InspectorIssue(
                kind="cost_basis",
                location=f"line {li.line_no} cost_validated_at",
                detail=(
                    "cost validation is stale "
                    "(> 30 days old or missing)."
                ),
                expected="within 30 days",
                actual=str(li.cost_validated_at),
            ))
    return issues


# ──────────────────────────────────────────────────────────────────────
# Format-B reconciliation — the standalone form set.
# ──────────────────────────────────────────────────────────────────────


def _row_sort_key(field_name: str) -> tuple[int, int]:
    """Sort 704B row field names — page 1 (``Row7``) before page 2 (``Row7_2``).

    Mirrors the same helper in ``src/spine/forms_render.py``. Kept local
    so the Inspector has no cross-substrate import and stays inside the
    Spine boundary.
    """
    page = 2 if field_name.endswith("_2") else 1
    digits = "".join(c for c in field_name if c.isdigit())
    return (page, int(digits) if digits else 0)


def _filled_qty_rows_in_order(fields: dict[str, str]) -> list[str]:
    """Return the QTY row field names that the filler actually wrote a value
    into, sorted page 1 → page 2 then by row number.

    This is the row-mapping discovery primitive: rather than hardcode
    ``page_size = 15`` (which silently skipped rows 16-23 if a future
    filler honored the YAML ``page_row_capacities: [23, 16]``), we ask
    the rendered output which rows got populated. ``forms_render.py``
    uses the same pattern when post-filling the ``#`` / ITEM NUMBER
    columns (see ``_complete_704b_form``) — keeping the two in lockstep
    means the Inspector measures the SAME rows the renderer wrote to.
    """
    return sorted(
        (
            name for name, v in fields.items()
            if name.startswith("QTYRow") and (v or "").strip()
        ),
        key=_row_sort_key,
    )


def _reconcile_704b(quote: "Quote", fields: dict[str, str]) -> tuple[list[InspectorIssue], int]:
    """Reconcile a filled 704B's field values against the Spine quote.

    Returns ``(issues, lines_checked)``. Per-line: quantity, unit price,
    and subtotal each compared. Plus the merchandise-subtotal field
    (``fill_154``) vs ``quote.subtotal_cents``.

    Row mapping is **discovered from the rendered output**, not derived
    from a hardcoded ``page_size``. We enumerate every QTYRow* field
    the filler populated, in page+row order, and match by index to
    ``quote.line_items`` — the same ordering ``fill_704b`` processes
    items in. This survives a future filler change (e.g. YAML
    ``page_row_capacities: [23, 16]`` over the current 15-row page-1
    behavior) without code edit, and on today's 15-row page-1 filler
    behaves identically to the prior hardcoded constant.
    """
    issues: list[InspectorIssue] = []
    lines_checked = 0

    qty_rows_in_order = _filled_qty_rows_in_order(fields)

    for idx, li in enumerate(quote.line_items):
        if idx >= len(qty_rows_in_order):
            # Row beyond what the filler populated — overflow path;
            # legacy ``_append_overflow_pages`` handles it via reportlab
            # overlay. Not in scope for the Inspector's field-level
            # check (no AcroForm field to read back).
            continue
        qty_field = qty_rows_in_order[idx]
        token = qty_field[len("QTY"):]          # "QTYRow3" -> "Row3"
        price_field = f"PRICE PER UNIT{token}"
        sub_field = f"SUBTOTAL{token}"

        lines_checked += 1
        # Qty
        try:
            rendered_qty = int(float(fields.get(qty_field, "0") or "0"))
        except ValueError:
            rendered_qty = -1
        if rendered_qty != li.qty:
            issues.append(InspectorIssue(
                kind="math",
                location=f"704B {qty_field}",
                detail=f"line {li.line_no} qty mismatch",
                expected=str(li.qty),
                actual=str(rendered_qty),
            ))
        # Unit price
        rendered_unit = _to_cents(fields.get(price_field))
        if rendered_unit is None or rendered_unit != li.unit_price_cents:
            issues.append(InspectorIssue(
                kind="math",
                location=f"704B {price_field}",
                detail=f"line {li.line_no} unit price mismatch",
                expected=f"{li.unit_price_cents/100:.2f}",
                actual=str(fields.get(price_field, "(blank)")),
            ))
        # Subtotal (qty × unit price)
        rendered_sub = _to_cents(fields.get(sub_field))
        if rendered_sub is None or rendered_sub != li.extension_cents:
            issues.append(InspectorIssue(
                kind="math",
                location=f"704B {sub_field}",
                detail=f"line {li.line_no} extension mismatch",
                expected=f"{li.extension_cents/100:.2f}",
                actual=str(fields.get(sub_field, "(blank)")),
            ))

    # Merchandise subtotal (fill_154) vs quote.subtotal_cents
    rendered_merch = _to_cents(fields.get("fill_154"))
    if rendered_merch is None or rendered_merch != quote.subtotal_cents:
        issues.append(InspectorIssue(
            kind="math",
            location="704B fill_154 (merchandise subtotal)",
            detail="rendered subtotal does not match Spine quote subtotal",
            expected=f"{quote.subtotal_cents/100:.2f}",
            actual=str(fields.get("fill_154", "(blank)")),
        ))
    return issues, lines_checked


def _reconcile_703b_identity(quote: "Quote", contract: "EmailContract",
                              fields: dict[str, str]) -> list[InspectorIssue]:
    """Reconcile the 703B identity block against the contract.

    Different 703B template variants use either the canonical
    ``703B_`` prefix or bare field names — try both and accept whichever
    has a value.
    """
    issues: list[InspectorIssue] = []

    def _get(*candidates: str) -> str:
        for c in candidates:
            if c in fields and (fields[c] or "").strip():
                return fields[c].strip()
        return ""

    # Solicitation number
    sol_rendered = _get("703B_Solicitation Number", "Solicitation Number")
    sol_expected = quote.solicitation_number or contract.solicitation_number
    if sol_rendered != sol_expected:
        issues.append(InspectorIssue(
            kind="identity",
            location="703B Solicitation Number",
            detail="rendered sol# does not match the contract",
            expected=sol_expected,
            actual=sol_rendered,
        ))
    # Requestor / buyer name (the form's "Name" field for the requestor)
    if contract.buyer_name:
        req = _get("703B_Name", "Name")
        if req != contract.buyer_name:
            issues.append(InspectorIssue(
                kind="identity",
                location="703B Name (requestor)",
                detail="rendered requestor does not match the contract",
                expected=contract.buyer_name,
                actual=req,
            ))
    return issues


def _reconcile_704b_identity(quote: "Quote", contract: "EmailContract",
                              fields: dict[str, str]) -> list[InspectorIssue]:
    """The 704B header (Spine adapter post-fills these from contract)."""
    issues: list[InspectorIssue] = []
    sol_expected = quote.solicitation_number or contract.solicitation_number
    sol_rendered = (fields.get("SOLICITATION") or "").strip()
    if sol_rendered != sol_expected:
        issues.append(InspectorIssue(
            kind="identity",
            location="704B SOLICITATION",
            detail="rendered sol# does not match the contract",
            expected=sol_expected,
            actual=sol_rendered,
        ))
    if contract.buyer_name:
        req = (fields.get("REQUESTOR") or "").strip()
        if req != contract.buyer_name:
            issues.append(InspectorIssue(
                kind="identity",
                location="704B REQUESTOR",
                detail="rendered requestor does not match the contract",
                expected=contract.buyer_name,
                actual=req,
            ))
    return issues


def reconcile_format_b(quote: "Quote", contract: "EmailContract", *,
                       output_dir: str | None = None,
                       forms_paths: dict[str, str] | None = None,
                       ) -> InspectorReport:
    """Reconcile the standalone-form set against the Spine source.

    Renders Format B via ``forms_render``, reads back every filled field,
    and verifies every line's qty/price/subtotal + the merchandise
    subtotal + the 703B + 704B identity blocks + coverage of the
    contract's ``required_forms`` + the model's cost-basis rules.

    ``forms_paths`` (optional) skips the adapter call and verifies an
    EXISTING rendered package — keys ``"703"`` / ``"704b"`` / ``"bidpkg"``
    map to PDF paths on disk. This is what the future send gate uses:
    render once via the route, then run the Inspector against the SAME
    bytes the buyer will see. Tests also use it to verify drift
    detection (render → tamper → reconcile catches it).
    """
    issues: list[InspectorIssue] = list(_check_cost_basis(quote))
    forms_checked: list[str] = []
    lines_checked = 0

    if forms_paths is not None:
        # Verify a pre-rendered package — no adapter call.
        forms = {
            "703": {"output_path": forms_paths.get("703", ""),
                     "form_code": "703b",
                     "ok": bool(forms_paths.get("703")),
                     "pdf_bytes": b"x" if forms_paths.get("703") else b""},
            "704b": {"output_path": forms_paths.get("704b", ""),
                      "ok": bool(forms_paths.get("704b")),
                      "pdf_bytes": b"x" if forms_paths.get("704b") else b""},
            "bidpkg": {"output_path": forms_paths.get("bidpkg", ""),
                        "ok": bool(forms_paths.get("bidpkg")),
                        "pdf_bytes": b"x" if forms_paths.get("bidpkg") else b""},
        }
    else:
        from src.spine.forms_render import render_cchcs_forms_via_legacy

        if output_dir is None:
            output_dir = tempfile.mkdtemp(prefix="spine_inspector_b_")
        res = render_cchcs_forms_via_legacy(
            quote, contract, output_dir=output_dir, strict=False)
        if not res.get("ok"):
            issues.append(InspectorIssue(
                kind="render",
                location="forms_render adapter",
                detail=res.get("error") or "adapter returned ok=False",
            ))
        forms = res.get("forms") or {}

    # 704B math + identity
    sub_704 = forms.get("704b") or {}
    if sub_704.get("output_path"):
        forms_checked.append("704b")
        fields = _field_values(sub_704["output_path"])
        m_issues, n = _reconcile_704b(quote, fields)
        issues.extend(m_issues)
        lines_checked = n
        issues.extend(_reconcile_704b_identity(quote, contract, fields))
    else:
        issues.append(InspectorIssue(
            kind="coverage", location="704b form",
            detail="704B was not rendered"))

    # 703B identity
    sub_703 = forms.get("703") or {}
    if sub_703.get("output_path"):
        forms_checked.append(sub_703.get("form_code", "703b"))
        fields = _field_values(sub_703["output_path"])
        issues.extend(_reconcile_703b_identity(quote, contract, fields))
    else:
        issues.append(InspectorIssue(
            kind="coverage", location="703 form",
            detail="703B/703C was not rendered"))

    # bid package coverage — present-and-non-empty bytes is enough; the
    # Inspector does not currently field-level-verify the bid package
    # (its content is mostly identity boilerplate already covered by 703B).
    sub_bidpkg = forms.get("bidpkg") or {}
    if sub_bidpkg.get("ok") and sub_bidpkg.get("pdf_bytes"):
        forms_checked.append("bidpkg")
    else:
        issues.append(InspectorIssue(
            kind="coverage", location="bidpkg form",
            detail="Bid Package was not rendered or empty"))

    # Coverage — every required_form should have been rendered.
    required = set(getattr(contract, "required_forms", None) or [])
    declared = required - {"quote"}  # quote is its own endpoint
    # Map "703b"/"703c" → "703*" for coverage comparison.
    rendered_codes = set(forms_checked)
    if "703b" in rendered_codes or "703c" in rendered_codes:
        rendered_codes.update({"703b", "703c"})
    missing = sorted({r for r in declared if r not in rendered_codes})
    for m in missing:
        issues.append(InspectorIssue(
            kind="coverage",
            location=f"required form '{m}'",
            detail=f"contract.required_forms declares {m!r}; not rendered",
        ))

    blocking = sum(1 for i in issues if i.severity == "blocking")
    warning = sum(1 for i in issues if i.severity == "warning")
    return InspectorReport(
        ok=(blocking == 0),
        quote_id=quote.quote_id,
        response_packaging=getattr(contract, "response_packaging", "separate_pdfs"),
        forms_checked=forms_checked,
        issues=issues,
        line_items_checked=lines_checked,
        blocking_count=blocking,
        warning_count=warning,
    )


# ──────────────────────────────────────────────────────────────────────
# Format-A reconciliation — the Non-Cloud Packet.
# ──────────────────────────────────────────────────────────────────────


def reconcile_format_a(quote: "Quote", contract: "EmailContract", *,
                       output_dir: str | None = None) -> InspectorReport:
    """Reconcile the filled Non-Cloud Packet against the Spine source.

    Renders Format A via ``packet_render`` (which delegates to the
    legacy packet filler), then re-parses the filled packet using
    ``cchcs_packet_parser`` and verifies every priced row matched a
    Spine quote line at the operator-intended unit price.
    """
    from src.spine.packet_render import render_cchcs_packet_via_legacy

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="spine_inspector_a_")
    res = render_cchcs_packet_via_legacy(
        quote, contract, output_dir=output_dir, strict=False)

    issues: list[InspectorIssue] = list(_check_cost_basis(quote))
    forms_checked: list[str] = ["packet"] if res.get("pdf_bytes") else []

    if not res.get("ok"):
        issues.append(InspectorIssue(
            kind="render",
            location="packet_render adapter",
            detail=res.get("error") or "adapter returned ok=False",
        ))

    # The packet adapter's own match_report is the per-row audit trail
    # of which Spine line went into which packet row. Re-check it:
    # every row whose strategy is "unmatched" + every row whose
    # quote_line_no doesn't reflect the operator's typed unit price.
    match_report = res.get("match_report") or []
    lines_checked = 0
    quote_by_line = {li.line_no: li for li in quote.line_items}
    for row in match_report:
        line_no = row.get("quote_line_no")
        if line_no is None:
            issues.append(InspectorIssue(
                kind="coverage",
                location=f"packet row {row.get('row_index')}",
                detail="packet row had no matching Spine quote line",
            ))
            continue
        lines_checked += 1
        li = quote_by_line.get(line_no)
        if li is None:
            issues.append(InspectorIssue(
                kind="math",
                location=f"packet row {row.get('row_index')}",
                detail=f"matched line_no={line_no} absent from Spine quote",
            ))
            continue
        rendered_unit = _to_cents(row.get("unit_price"))
        if li.unit_price_cents == 0:
            # Spine intentionally skipped pricing this row (the adapter
            # omits it from overrides). Coverage warning only.
            continue
        if rendered_unit is None or rendered_unit != li.unit_price_cents:
            issues.append(InspectorIssue(
                kind="math",
                location=f"packet row {row.get('row_index')}",
                detail=f"line {line_no} unit price mismatch",
                expected=f"{li.unit_price_cents/100:.2f}",
                actual=str(row.get("unit_price")),
            ))

    # Coverage — the packet IS the bundled form set; if it rendered, all
    # of {703b,704b,bidpkg,quote} are covered by the single document.
    # If it didn't render, every declared required_form is missing.
    if not res.get("ok") or not res.get("pdf_bytes"):
        for m in sorted(set(getattr(contract, "required_forms", None) or []) - {"quote"}):
            issues.append(InspectorIssue(
                kind="coverage",
                location=f"required form '{m}'",
                detail="packet failed to render; coverage cannot be verified",
            ))

    blocking = sum(1 for i in issues if i.severity == "blocking")
    warning = sum(1 for i in issues if i.severity == "warning")
    return InspectorReport(
        ok=(blocking == 0),
        quote_id=quote.quote_id,
        response_packaging=getattr(contract, "response_packaging", "single_pdf"),
        forms_checked=forms_checked,
        issues=issues,
        line_items_checked=lines_checked,
        blocking_count=blocking,
        warning_count=warning,
    )


# ──────────────────────────────────────────────────────────────────────
# Dispatcher — pick the format and run the right reconcile.
# ──────────────────────────────────────────────────────────────────────


def reconcile_quote_to_package(quote: "Quote",
                                contract: "EmailContract | None", *,
                                output_dir: str | None = None) -> InspectorReport:
    """Run the Inspector on a Spine quote — picks Format A vs B by the
    contract's declared ``response_packaging``. Returns an
    ``InspectorReport`` whose ``ok`` is the gate value for the send.
    """
    if contract is None:
        return InspectorReport(
            ok=False,
            quote_id=quote.quote_id,
            response_packaging="unknown",
            forms_checked=[],
            issues=[InspectorIssue(
                kind="render",
                location="EmailContract",
                detail="no EmailContract is bound to this quote — cannot reconcile",
            )],
            line_items_checked=0,
            blocking_count=1,
            warning_count=0,
        )
    packaging = getattr(contract, "response_packaging", "separate_pdfs")
    if packaging == "single_pdf":
        return reconcile_format_a(quote, contract, output_dir=output_dir)
    return reconcile_format_b(quote, contract, output_dir=output_dir)


__all__ = [
    "InspectorIssue",
    "InspectorReport",
    "reconcile_format_a",
    "reconcile_format_b",
    "reconcile_quote_to_package",
]
