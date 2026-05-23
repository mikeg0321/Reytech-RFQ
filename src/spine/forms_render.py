"""Spine → legacy CCHCS standalone-form-set adapter.

The COMMON CCHCS response format is NOT the bundled Non-Cloud Packet
(that minority format is handled by ``src/spine/packet_render.py``). It
is a set of separate buyer forms:

    AMS 703B  *or*  AMS 703C   (the bid-response cover sheet)
    AMS 704B                   (the line-item quote worksheet)
    CDCR Bid Package           (the 14-page certifications bundle)

plus the Reytech Quote PDF (handled elsewhere). This module renders that
standalone set.

Why an adapter and not a Spine renderer
---------------------------------------
The Spine grew its OWN from-scratch agency-form renderers
(``src/spine/agency_forms/cchcs_{703b,704b,bidpkg}.py``). They filled
*blank templates* and produced output that failed CCHCS responsiveness
review (the 2026-05-18 "trash" + a 21-minute operator hand-finish). They
are RETIRED.

The fix — ratified in ``src/spine/SPINE_CHARTER.md`` → "Second adapter —
``forms_render.py``" — is to delegate to the verified legacy fillers in
``src/forms/reytech_filler_v4.py`` (``fill_703b`` / ``fill_703c`` /
``fill_704b`` / ``fill_bid_package``): a 4-month, 120-commit, audited
code path Reytech ships real bids on. The fillers fill the BUYER'S OWN
template PDFs — exactly what the proven legacy route
``routes_rfq_gen.py::generate`` does (lines ~3549-3564).

This module is the exact sibling of ``packet_render.py``: it maps a
Spine ``Quote`` + ``EmailContract`` onto the legacy fillers' call shape
(``(input_path, rfq_data, config, output_path)``) and back to bytes.

Dependency boundary
-------------------
The adapter is a sanctioned boundary-crosser. It imports exactly:

  - ``src.forms.reytech_filler_v4`` — the verified fillers + ``load_config``.
  - ``src.core.paths``             — ``DATA_DIR`` / ``OUTPUT_DIR`` constants.

That import set is the adapter's whole purpose and is enforced
file-scoped in ``test_no_legacy_imports`` (``_FILE_SCOPED_LEGACY_IMPORTS``);
every other ``src/spine/`` file still gets zero legacy imports. The
attachment classifier is inlined here (mirroring ``rfq_parser.
identify_attachments``) so the adapter stays self-contained.

Hard requirement
----------------
The adapter fills the buyer's actual template PDFs. It REQUIRES
``contract.attachment_refs`` to resolve the buyer's 703B/703C, 704B and
Bid Package templates on disk. If no contract is bound, or a required
template cannot be located, the adapter fails loudly with ``ok=False``
and an actionable error — it never silently produces a blank document.
"""
from __future__ import annotations

import contextlib
import io
import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any

log = logging.getLogger("reytech.spine.forms_render")

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


# ──────────────────────────────────────────────────────────────────────
# Template resolution — locate the buyer's form PDFs on disk.
# Mirrors src/spine/packet_render.py's resolver, extended to classify a
# resolved PDF into a CCHCS form slot (703b / 703c / 704b / bidpkg).
# ──────────────────────────────────────────────────────────────────────


def _search_roots() -> list[str]:
    """Directories an ``attachment_refs`` entry may be relative to.

    A ref is stored by Spine ingest as a path or a bare filename; it may
    be relative to the repo root, the persistent ``DATA_DIR`` volume, the
    generated-output dir, or the legacy uploads dir.
    """
    roots: list[str] = []
    try:
        repo_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        roots.append(repo_root)
    except Exception as e:  # pragma: no cover - defensive
        log.debug("forms_render: repo-root resolution failed: %s", e)
    try:
        from src.core.paths import DATA_DIR, OUTPUT_DIR

        roots.append(str(DATA_DIR))
        roots.append(str(OUTPUT_DIR))
        roots.append(os.path.join(str(DATA_DIR), "uploads"))
    except Exception as e:  # pragma: no cover - paths optional in tests
        log.debug("forms_render: src.core.paths unavailable: %s", e)
    seen: set[str] = set()
    uniq: list[str] = []
    for r in roots:
        if r and r not in seen:
            seen.add(r)
            uniq.append(r)
    return uniq


def _candidate_paths(ref: str) -> list[str]:
    """Every on-disk path an ``attachment_refs`` entry might denote."""
    if not ref:
        return []
    out = [ref]
    base = os.path.basename(ref)
    for root in _search_roots():
        out.append(os.path.join(root, ref))
        if base and base != ref:
            out.append(os.path.join(root, base))
    seen: set[str] = set()
    uniq: list[str] = []
    for p in out:
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def _resolve_existing_pdfs(contract: "EmailContract") -> list[str]:
    """Resolve every ``attachment_refs`` entry to an existing PDF on disk."""
    refs = list(getattr(contract, "attachment_refs", None) or [])
    existing: list[str] = []
    for ref in refs:
        for cand in _candidate_paths(ref):
            if (
                cand.lower().endswith(".pdf")
                and os.path.isfile(cand)
                and cand not in existing
            ):
                existing.append(cand)
    return existing


def _classify_form_templates(pdf_paths: list[str]) -> dict[str, str]:
    """Map resolved PDF paths onto CCHCS form slots by filename.

    Mirrors ``src/forms/rfq_parser.py::identify_attachments`` — the
    classifier the proven legacy upload path uses. Order matters: the
    most specific markers (703C, 704B) are checked before the generic
    ``703B``/``RFQ`` markers, because a 704B "Acquisition Quote
    Worksheet" filename commonly also contains "RFQ".

    Two passes: pass 1 matches explicit form codes; pass 2 falls back to
    the bare ``RFQ`` marker for the 703B slot only when it is still
    empty (the 703B cover sheet is frequently named ``..._703B_-_RFQ_-_
    Informal_Competitive_...``). ``setdefault`` keeps the first hit per
    slot so a stray RFQ-named PDF cannot displace a real classified one.
    """
    slots: dict[str, str] = {}
    # Pass 1 — explicit form-code markers.
    for path in pdf_paths:
        name = os.path.basename(path).upper()
        if "703C" in name or "FAIR_AND_REASONABLE" in name or "FAIR AND REASONABLE" in name:
            slots.setdefault("703c", path)
        elif "704B" in name or "704 B" in name or "QUOTE_WORKSHEET" in name or "WORKSHEET" in name:
            slots.setdefault("704b", path)
        elif "703B" in name:
            slots.setdefault("703b", path)
        elif (
            "BID_PACKAGE" in name or "BID PACKAGE" in name or "BIDPACKAGE" in name
            or "BIDPKG" in name or "PACKAGE" in name or "FORMS" in name
        ):
            slots.setdefault("bidpkg", path)
    # Pass 2 — bare "RFQ" fallback for the 703B cover sheet only.
    if "703b" not in slots and "703c" not in slots:
        for path in pdf_paths:
            name = os.path.basename(path).upper()
            if "RFQ" in name and "704" not in name:
                slots.setdefault("703b", path)
                break
    return slots


def _pick_703_code(contract: "EmailContract") -> str:
    """Decide which 703 variant the buyer's email contract declares.

    LAW 6 — the contract is the ground truth. ``required_forms`` carries
    the declared form set. 703B is the common form; 703C ("Fair and
    Reasonable / Exempt") is the alternate CCHCS sometimes ships. CLAUDE.md
    forbids ever including both. The legacy ``fill_703b`` self-corrects
    (redirects to ``fill_703c``) if handed a 703C template, so a wrong
    guess here is recoverable — but the contract decides first.
    """
    rf = set(getattr(contract, "required_forms", None) or [])
    if "703c" in rf and "703b" not in rf:
        return "703c"
    return "703b"


# ──────────────────────────────────────────────────────────────────────
# Spine model → legacy ``r`` dict.
# This is the heart of the adapter. The legacy fillers consume the whole
# ``rfq_files`` dict (``rfq_data``); this builds an equivalent ``r`` from
# the Spine Quote + EmailContract. A PARTIAL ``r`` makes the fillers emit
# blank/wrong fields — the 2026-05-18 "trash" failure mode — so every
# field the three fillers read is mapped here. Field contract enumerated
# from fill_703b / fill_703c / fill_704b / fill_bid_package +
# ams704_helpers.build_704_item_fields (2026-05-22).
# ──────────────────────────────────────────────────────────────────────


def _fmt_date(value: Any) -> str:
    """Render a contract date as US ``m/d/Y`` — the form-field convention.

    The legacy fillers parse ``release_date`` / ``due_date`` with that
    format. ``value`` may be a ``datetime``, an ISO string, or ``None``.
    """
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%m/%d/%Y")
    s = str(value).strip()
    if not s:
        return ""
    from datetime import datetime as _dt

    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z",
                "%m/%d/%Y", "%m/%d/%y"):
        try:
            return _dt.strptime(s, fmt).strftime("%m/%d/%Y")
        except ValueError:
            continue
    return s  # unrecognized — pass through rather than blank it.


def _build_legacy_rfq_dict(quote: "Quote", contract: "EmailContract") -> dict:
    """Build the legacy ``r`` (``rfq_data``) dict the fillers consume.

    The Spine ``Quote`` is the source of truth for line items, prices and
    the solicitation number. The ``EmailContract`` carries the buyer-side
    procurement metadata (requestor, dates, ship-to) — LAW 6: it is the
    complete spec resolved at ingest.
    """
    from src.forms.reytech_filler_v4 import get_pst_date

    line_items: list[dict] = []
    for li in quote.line_items:
        unit_price = li.unit_price_cents / 100.0
        mfg = (li.mfg_number or "").strip()
        # NO markup_pct / markup key — pricing_math.canonical_unit_price
        # would forward-compute cost×markup and override the operator's
        # typed unit_price. unit_price is the only price the legacy
        # fillers must see. unit_price + price_per_unit are both set
        # because ams704_helpers / pricing_math read different aliases.
        line_items.append({
            "line_number": li.line_no,
            "description": li.description,
            "qty": li.qty,
            "uom": li.uom,
            "unit_price": unit_price,
            "price_per_unit": unit_price,
            "supplier_cost": li.cost_cents / 100.0,
            "mfg_number": mfg,
            "part_number": mfg,
        })

    sol = quote.solicitation_number or contract.solicitation_number

    # Sign date — the frozen PST date stamped into the Quote at first
    # FINALIZED (model.py with_status). Falls back to today's PST date
    # only when the Quote has not yet been finalized (parsed/priced
    # render paths), so the adapter behavior is unchanged for the
    # pre-finalize editor preview; once finalized, every subsequent
    # render reads the same frozen value and the bytes the buyer
    # receives are deterministic even when the render crosses midnight.
    if quote.sign_date_pst is not None:
        sign_date_str = quote.sign_date_pst.strftime("%m/%d/%Y")
    else:
        sign_date_str = get_pst_date()

    r: dict[str, Any] = {
        "solicitation_number": sol,
        "sign_date": sign_date_str,
        "release_date": _fmt_date(getattr(contract, "release_date", None)),
        "due_date": _fmt_date(
            getattr(contract, "due_date", None)
            or getattr(contract, "response_due", None)
        ),
        # Requestor block (703B/703C "Name / Email / Phone"). The buyer-
        # side procurement officer — distinct from the AP bill-to block.
        "requestor_name": contract.buyer_name or "",
        "requestor_email": contract.buyer_email or "",
        "requestor_phone": contract.buyer_phone or "",
        # Agency / facility — read by fill_bid_package's match_agency()
        # for inline-page trimming; CCHCS resolves to an empty
        # required-standalones set either way (keeps the inline pages).
        "agency": quote.agency,
        "facility": quote.facility or contract.facility,
        "institution": quote.facility or contract.facility,
        "line_items": line_items,
    }

    # delivery_location is optional — only set it when the buyer gave a
    # ship-to. fill_703b/703c write Dropdown2 only when it is truthy.
    ship_to = (
        getattr(contract, "ship_to_address", None)
        or getattr(contract, "ship_to_facility", None)
    )
    if ship_to:
        r["delivery_location"] = ship_to

    return r


# ──────────────────────────────────────────────────────────────────────
# PDF helpers.
# ──────────────────────────────────────────────────────────────────────


def _read_bytes(path: str) -> bytes:
    """Read a file's bytes, or ``b""`` if it cannot be read."""
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("forms_render: could not read %s: %s", path, e)
        return b""


def _is_real_pdf(data: bytes) -> bool:
    """A non-trivial PDF — header present and more than a stub."""
    return bool(data) and data[:5] == b"%PDF-" and len(data) > 1024


def _call_filler_quietly(filler, template: str, r: dict, config: dict,
                         out_path: str) -> None:
    """Invoke a legacy filler with its stdout captured.

    The ``reytech_filler_v4`` fillers narrate progress with ``print()``,
    including Unicode glyphs (✓ ⚠ ℹ). On a non-UTF-8 stdout (a Windows
    cp1252 console) an unguarded ``print`` of those glyphs raises
    ``UnicodeEncodeError`` and aborts the fill. Production runs on Linux
    UTF-8 so this never bites there — but an adapter should insulate the
    Spine from the legacy callee's I/O side effects regardless. Capturing
    stdout makes the delegated fill stream-encoding-independent and keeps
    the filler's chatter out of the Spine's logs; it is surfaced at debug.
    """
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        filler(template, r, config, out_path)
    chatter = buf.getvalue().strip()
    if chatter:
        log.debug("forms_render: filler output for %s:\n%s", out_path, chatter)


def _row_sort_key(field_name: str) -> tuple[int, int]:
    """Sort 704B row field names — page 1 (``Row7``) before page 2 (``Row7_2``)."""
    page = 2 if field_name.endswith("_2") else 1
    digits = "".join(c for c in field_name if c.isdigit())
    return (page, int(digits) if digits else 0)


def _complete_704b_form(output_path: str, quote: "Quote", contract: "EmailContract") -> None:
    """Fill the 704B fields the legacy ``fill_704b`` deliberately leaves blank.

    ``fill_704b`` never writes the SOLICITATION#/REQUESTOR/DEPARTMENT/
    PHONE-EMAIL/DATE header or the ``#`` line-number column — a documented
    "buyer fills, never overwrite" rule (sourced from CCHCS buyer feedback).
    That is correct for the LEGACY flow, where the buyer's uploaded 704B
    template arrives with that header pre-filled.

    The Spine flow is different: LAW 6 — the ``EmailContract`` is the
    complete spec, and the Spine holds the solicitation number, the
    requestor, and the dates authoritatively. A Spine-rendered 704B with a
    blank solicitation# is the Spine failing to use its own ground truth.

    So the adapter completes those fields from the contract — **fill-if-
    empty only**: any field the buyer already populated is read and left
    untouched, so this never overwrites a buyer value, it only fills a gap.
    The header fields ``SOLICITATION``/``REQUESTOR``/``Date1_af_date`` are
    shared across both 704B pages — updating per page regenerates the
    appearance on each so page 2's header is no longer blank.

    Best-effort: a failure here logs and leaves ``fill_704b``'s output
    intact rather than aborting the render.
    """
    try:
        from pypdf import PdfReader, PdfWriter

        reader = PdfReader(output_path)
        fields = reader.get_fields() or {}

        def _is_empty(name: str) -> bool:
            """True iff the field exists and currently has no value."""
            f = fields.get(name)
            if f is None:
                return False  # field absent on this template — skip it
            v = f.get("/V")
            return not (v and str(v).strip())

        values: dict[str, str] = {}

        # ── Header — buyer's procurement block ────────────────────────
        sol = quote.solicitation_number or contract.solicitation_number
        if sol and _is_empty("SOLICITATION"):
            values["SOLICITATION"] = sol
        if contract.buyer_name and _is_empty("REQUESTOR"):
            values["REQUESTOR"] = contract.buyer_name
        facility = contract.facility or quote.facility
        if facility and _is_empty("DEPARTMENT"):
            values["DEPARTMENT"] = f"CCHCS - {facility}"
        phone_email = " / ".join(
            x for x in (contract.buyer_phone, contract.buyer_email) if x
        )
        if phone_email and _is_empty("PHONEEMAIL"):
            values["PHONEEMAIL"] = phone_email
        worksheet_date = _fmt_date(
            getattr(contract, "release_date", None)
        ) or _fmt_date(getattr(contract, "due_date", None))
        if worksheet_date and _is_empty("Date1_af_date"):
            values["Date1_af_date"] = worksheet_date

        # ── '#' line-number column + ITEM NUMBER (MFG#) — fill-if-empty.
        # fill_704b writes the per-row QTY field; every filled QTYRow{tok}
        # marks a populated row. Match those rows, in order, to the Spine
        # quote's line items: the '#' field is `{tok}` (e.g. `Row3`), the
        # MFG# field is `ITEM NUMBER{tok}`.
        qty_rows = sorted(
            (
                name for name, f in fields.items()
                if name.startswith("QTYRow")
                and f.get("/V") and str(f.get("/V")).strip()
            ),
            key=_row_sort_key,
        )
        for idx, qty_field in enumerate(qty_rows):
            if idx >= len(quote.line_items):
                break
            token = qty_field[len("QTY"):]          # "QTYRow3" -> "Row3"
            li = quote.line_items[idx]
            if _is_empty(token):
                values[token] = str(li.line_no)
            inum_field = f"ITEM NUMBER{token}"
            if li.mfg_number and _is_empty(inum_field):
                values[inum_field] = li.mfg_number

        if not values:
            return

        writer = PdfWriter()
        writer.append(reader)
        # Update per page — shared header fields (both pages) get their
        # appearance regenerated on each widget this way.
        for page in writer.pages:
            try:
                writer.update_page_form_field_values(
                    page, values, auto_regenerate=False
                )
            except Exception as e:  # pragma: no cover - defensive
                log.debug("forms_render: 704B page fill skipped: %s", e)
        with open(output_path, "wb") as fh:
            writer.write(fh)
        log.info(
            "forms_render: 704B completion filled %d field(s): %s",
            len(values), sorted(values),
        )
    except Exception as e:
        log.warning("forms_render: 704B completion skipped (%s)", e)


def _merge_pdfs(paths: list[str]) -> bytes:
    """Concatenate the given PDFs into one. Returns ``b""`` on failure."""
    real = [p for p in paths if p and os.path.isfile(p)]
    if not real:
        return b""
    try:
        from pypdf import PdfWriter

        writer = PdfWriter()
        for p in real:
            writer.append(p)
        buf = io.BytesIO()
        writer.write(buf)
        return buf.getvalue()
    except Exception as e:  # pragma: no cover - defensive
        log.warning("forms_render: PDF merge failed: %s", e)
        return b""


# ──────────────────────────────────────────────────────────────────────
# Public API — the adapter.
# ──────────────────────────────────────────────────────────────────────

# The standalone form set this adapter renders, in submission order.
# 703 (cover) → 704B (quote worksheet) → bid package.
_FORM_ORDER = ("703", "704b", "bidpkg")


def render_cchcs_forms_via_legacy(
    quote: "Quote",
    contract: "EmailContract | None",
    *,
    output_dir: str | None = None,
    strict: bool = True,
) -> dict[str, Any]:
    """Render the CCHCS standalone form set for a Spine quote.

    Delegates to the verified legacy fillers in
    ``src/forms/reytech_filler_v4.py`` after locating the buyer's template
    PDFs and mapping the Spine quote onto the fillers' call shape.

    Args:
        quote:      Validated Spine ``Quote`` — source of truth for line
                    items, prices and the solicitation number.
        contract:   The ``EmailContract`` that drove ingest. REQUIRED —
                    its ``attachment_refs`` locate the buyer's template
                    PDFs; ``required_forms`` declares the 703 variant.
        output_dir: Where the legacy fillers write the filled PDFs.
                    ``None`` → a private temp dir (bytes are returned in
                    ``pdf_bytes`` / per-form ``pdf_bytes`` regardless).
        strict:     Accepted for adapter-signature parity with
                    ``packet_render.render_cchcs_packet_via_legacy``. The
                    legacy ``reytech_filler_v4`` fillers self-verify — they
                    either write a valid PDF or raise — so there is no
                    separate strict gate to forward; ``ok`` always
                    reflects "every form in the set rendered".

    Returns:
        ``{
            "ok": bool,            # every required form rendered ok
            "error": str,          # actionable detail when not ok
            "forms": {             # per-form sub-results, keyed 703/704b/bidpkg
                "703":    {"ok", "form_code", "template", "output_path",
                           "pdf_bytes", "error"},
                "704b":   {...},
                "bidpkg": {...},
            },
            "output_path": str,    # merged 3-form PDF on disk
            "pdf_bytes": bytes,    # merged 3-form PDF bytes
            "match_report": {...}, # format, 703 variant, templates resolved
        }``
    """
    result: dict[str, Any] = {
        "ok": False,
        "error": "",
        "forms": {},
        "output_path": "",
        "pdf_bytes": b"",
        "match_report": {},
    }

    if contract is None:
        result["error"] = (
            "no EmailContract is bound to this quote. The CCHCS form-set "
            "adapter fills the BUYER'S template PDFs and needs "
            "contract.attachment_refs to locate the 703B/703C, 704B and "
            "Bid Package documents. Ingest the quote via the Spine "
            "email-contract path, or POST /contract-override with the "
            "attachments, before requesting the forms."
        )
        return result

    # ── Resolve + classify the buyer's template PDFs ──────────────────
    existing = _resolve_existing_pdfs(contract)
    if not existing:
        result["error"] = (
            "could not locate any of the buyer's form template PDFs. "
            f"contract.attachment_refs={list(contract.attachment_refs)!r} "
            "resolved to no PDF on disk. The 703B/703C, 704B and Bid "
            "Package PDFs the buyer emailed must be stored and referenced "
            "in attachment_refs — the fillers fill those exact documents."
        )
        return result

    slots = _classify_form_templates(existing)
    code703 = _pick_703_code(contract)
    # Resolve the 703 template: prefer the slot matching the declared
    # variant; accept the other if only it was found (the filler
    # self-corrects on the template's actual field prefix).
    tmpl_703 = slots.get(code703) or slots.get("703b") or slots.get("703c")
    tmpl_704b = slots.get("704b")
    tmpl_bidpkg = slots.get("bidpkg")

    # required_forms gates which forms render; default to the full
    # standalone set when the contract did not narrow it.
    required = set(getattr(contract, "required_forms", None) or [])
    want_703 = (not required) or bool(required & {"703b", "703c"})
    want_704b = (not required) or ("704b" in required)
    want_bidpkg = (not required) or ("bidpkg" in required)

    missing: list[str] = []
    if want_703 and not tmpl_703:
        missing.append("703B/703C cover sheet")
    if want_704b and not tmpl_704b:
        missing.append("704B quote worksheet")
    if want_bidpkg and not tmpl_bidpkg:
        missing.append("CDCR Bid Package")
    if missing:
        result["error"] = (
            "could not locate template PDF(s) for: "
            + ", ".join(missing)
            + ". Resolved attachments: "
            + repr([os.path.basename(p) for p in existing])
            + ". Classified slots: "
            + repr({k: os.path.basename(v) for k, v in slots.items()})
            + ". Every required form's buyer template must be present in "
            "attachment_refs."
        )
        return result

    # ── Build the legacy rfq dict + load CONFIG ───────────────────────
    try:
        from src.forms.reytech_filler_v4 import (
            fill_703b,
            fill_703c,
            fill_704b,
            fill_bid_package,
            load_config,
        )
    except Exception as e:  # pragma: no cover - import guard
        result["error"] = f"legacy filler import failed: {e}"
        return result

    try:
        config = load_config()
    except Exception as e:
        result["error"] = f"could not load reytech_config.json: {e}"
        return result

    r = _build_legacy_rfq_dict(quote, contract)
    sol = r["solicitation_number"]

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="spine_forms_")
    os.makedirs(output_dir, exist_ok=True)

    # ── Render each form via its verified legacy filler ───────────────
    # (filler, template, output filename, requested?) — 703 dispatches
    # by the declared variant; the filler itself redirects 703b<->703c
    # if the template's field prefix disagrees.
    fill_703 = fill_703c if code703 == "703c" else fill_703b
    plan: list[tuple[str, str, Any, str | None, bool]] = [
        ("703", code703, fill_703, tmpl_703, want_703),
        ("704b", "704b", fill_704b, tmpl_704b, want_704b),
        ("bidpkg", "bidpkg", fill_bid_package, tmpl_bidpkg, want_bidpkg),
    ]

    forms: dict[str, dict[str, Any]] = {}
    rendered_paths: list[str] = []
    out_label = {"703": code703.upper(), "704b": "704B", "bidpkg": "BidPackage"}

    for key, form_code, filler, template, wanted in plan:
        if not wanted:
            continue
        sub: dict[str, Any] = {
            "ok": False,
            "form_code": form_code,
            "template": template or "",
            "output_path": "",
            "pdf_bytes": b"",
            "error": "",
        }
        out_path = os.path.join(output_dir, f"{sol}_{out_label[key]}_Reytech.pdf")
        try:
            _call_filler_quietly(filler, template, r, config, out_path)
        except Exception as e:
            log.exception(
                "forms_render: %s filler crashed for quote=%s", form_code, quote.quote_id
            )
            sub["error"] = f"{form_code} filler crashed: {e}"
            forms[key] = sub
            continue

        if not os.path.isfile(out_path):
            sub["error"] = (
                f"{form_code} filler returned without writing {out_path}"
            )
            forms[key] = sub
            continue

        # LAW 6 — complete the 704B header / # column from the contract.
        # fill_704b leaves those "buyer fields" blank; the Spine holds the
        # authoritative values and fills them (fill-if-empty).
        if key == "704b":
            _complete_704b_form(out_path, quote, contract)

        data = _read_bytes(out_path)
        sub["output_path"] = out_path
        sub["pdf_bytes"] = data
        if _is_real_pdf(data):
            sub["ok"] = True
            rendered_paths.append(out_path)
        else:
            sub["error"] = (
                f"{form_code} output is not a usable PDF "
                f"({len(data)} bytes) — filler produced no real document"
            )
        forms[key] = sub

    result["forms"] = forms

    # ── Merge + verdict ───────────────────────────────────────────────
    if rendered_paths:
        merged = _merge_pdfs(rendered_paths)
        if merged:
            merged_path = os.path.join(output_dir, f"{sol}_CCHCS_Forms_Reytech.pdf")
            try:
                with open(merged_path, "wb") as fh:
                    fh.write(merged)
                result["output_path"] = merged_path
            except Exception as e:  # pragma: no cover - defensive
                log.warning("forms_render: could not write merged PDF: %s", e)
            result["pdf_bytes"] = merged

    failed = [k for k, v in forms.items() if not v["ok"]]
    result["ok"] = bool(forms) and not failed
    if failed:
        details = "; ".join(
            f"{k}: {forms[k]['error'] or 'render failed'}" for k in failed
        )
        result["error"] = f"form render failed — {details}"

    result["match_report"] = {
        "format": getattr(contract, "response_packaging", "separate_pdfs"),
        "required_forms": sorted(required) if required else [],
        "form_703_variant": code703,
        "forms_rendered": [k for k, v in forms.items() if v["ok"]],
        "templates_resolved": {
            k: os.path.basename(v) for k, v in slots.items()
        },
        "line_items": len(quote.line_items),
    }

    log.info(
        "forms_render: quote=%s sol=%s ok=%s 703=%s rendered=%s failed=%s",
        quote.quote_id, sol, result["ok"], code703,
        result["match_report"]["forms_rendered"], failed,
    )
    return result


__all__ = ["render_cchcs_forms_via_legacy"]
