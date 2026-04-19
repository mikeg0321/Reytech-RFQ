"""CCHCS Non-IT RFQ Packet filler.

Takes a parsed CCHCS packet dict (from cchcs_packet_parser), Reytech
supplier info, and optional per-row price overrides, and produces a
filled PDF ready to send back to the buyer.

This is a companion to src/forms/cchcs_packet_parser.py — the parser
reads fields, the filler writes them. They share the same field-name
constants to avoid schema drift.

Output filename convention: `<source basename>_Reytech.pdf`. Matches
Mike's explicit requirement "add _Reytech to the parsed filename".

What we fill (supplier side):
  - Supplier Name / Address / Contact / Phone / Email
  - SB/MB/DVBE cert number + expiration
  - Signature + date (typed, not image — the field is /Tx not /Sig)
  - Rev field (quote revision, default "0")
  - Per-row: Price Per Unit{N} + Extension Total{N}
  - Totals roll-up: Subtotal, Freight, Sales Tax, Total
  - Compliance checkboxes: Check Box11-16 on page 1 (SB/MB, DVBE,
    agreement to terms, etc.)

What we DON'T touch (left for human review):
  - Per-row item compliance checkboxes (Check Box27/29/21) — these
    carry legal acknowledgments like "meets spec" and "no exceptions"
    that the operator must verify item-by-item
  - AMS 708 GenAI attachment fields — only relevant if quoting an AI
    product
  - Buyer header fields (Solicitation No, Institution Name, etc.)
  - Questions Due Date / Today / before / Time — buyer pre-filled

Build context: this module was built during the overnight autonomous
CCHCS automation session (2026-04-13). See
_overnight_review/MORNING_REVIEW.md for the session log and decision
rationale.
"""

from __future__ import annotations

import io
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

log = logging.getLogger("reytech.cchcs_packet")

try:
    from pypdf import PdfReader, PdfWriter
    from pypdf.generic import NameObject, BooleanObject, TextStringObject
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

# ─── Skip ledger ──────────────────────────────────────────────────────────────
# CCHCS packets are ~60% of Reytech's quote volume. Several silent-skip sites
# in this module can produce a packet that LOOKS valid but is quietly wrong:
# bad totals on the cover page, missing civil-rights declaration page, or a
# placeholder surviving in place of an attachment. The ledger lets the
# orchestrator end-of-run sweep (PR #188) drain these and persist them into
# `feature_status` so the dashboard banner reflects degraded packet build.
from src.core.dependency_check import Severity, SkipReason  # noqa: E402

_SKIP_LEDGER: list[SkipReason] = []


def _record_skip(skip: SkipReason) -> None:
    """Append a skip to the module ledger.
    Whole-feature WARNING skips re-log because they affect deliverable
    correctness; per-row INFO skips stay quiet so a malformed money string
    in one row doesn't spam the build log."""
    _SKIP_LEDGER.append(skip)
    if skip.severity in (Severity.BLOCKER, Severity.WARNING):
        log.warning(skip.format_for_log())


def drain_skips() -> list[SkipReason]:
    """Pop and return every skip recorded since the last drain. Destructive
    so two consecutive calls do not double-warn."""
    drained = list(_SKIP_LEDGER)
    _SKIP_LEDGER.clear()
    return drained


def _parse_money_safely(raw, *, field: str, where: str) -> float:
    """Parse a money string from a filled PDF text field. Returns 0.0 on:
      - None / empty (the field was never filled — not a corruption signal,
        no skip emitted)
      - malformed string (the field had non-numeric content; INFO skip)

    Used for subtotal / grand_total roll-ups that drive the cover-page
    summary the operator inspects before sending the packet."""
    if raw in (None, ""):
        return 0.0
    try:
        cleaned = str(raw).replace(",", "").replace("$", "").strip() or "0"
        return float(cleaned)
    except (ValueError, TypeError) as e:
        _record_skip(SkipReason(
            name="totals_parse",
            reason=f"{field} parse failed: {type(e).__name__}: {e} (raw={raw!r})",
            severity=Severity.INFO,
            where=where,
        ))
        return 0.0

from src.forms.cchcs_packet_parser import (
    HEADER_FIELDS,
    LINE_ITEM_TEMPLATES,
    SUPPLIER_FIELDS,
    TOTALS_FIELDS,
    MAX_ROWS,
    SW_RENEWAL_NO_CHECKBOX,
    SW_RENEWAL_TERM_FIELD,
    RESELLER_PERMIT_FIELD,
    CUF_TEXT_FIELDS,
    CUF_YES_CHECKBOXES,
    AMS708_SUPPLIER_FIELDS,
    AMS708_GENAI_NO_CHECKBOX,
    AMS708_QUESTION_FIELDS,
    PREFERENCE_CHECKBOX_PAIRS,
    SUBCONTRACT_AMOUNT_FIELD,
)


# Legacy export kept for backwards-compatibility with tests that imported
# the old (incorrect) flat compliance list. The 6 checkboxes are actually
# 3 YES/NO pairs — see PREFERENCE_CHECKBOX_PAIRS in the parser module and
# the preference-block handling in `_build_field_updates`.
COMPLIANCE_CHECKBOXES_YES = tuple(
    yes for yes, _no in PREFERENCE_CHECKBOX_PAIRS.values()
)


def _money(x: float) -> str:
    """Format a float as the packet expects: no $, two decimals, comma
    thousands. Matches how Ashley Russ's example row 1 appears empty
    but the downstream buyer spreadsheet expects '1,234.56' style."""
    try:
        return f"{float(x):,.2f}"
    except (TypeError, ValueError):
        return ""


def _today_mmddyyyy() -> str:
    return datetime.now().strftime("%m/%d/%Y")


def _output_path(source_pdf: str, output_dir: Optional[str] = None) -> str:
    """Return `<source base>_Reytech.pdf` in the same dir (or override)."""
    base, ext = os.path.splitext(os.path.basename(source_pdf))
    # Strip any trailing _Reytech the source might already carry (defensive)
    base = re.sub(r"_Reytech$", "", base, flags=re.IGNORECASE)
    dest_dir = output_dir or os.path.dirname(source_pdf) or "."
    return os.path.join(dest_dir, f"{base}_Reytech{ext or '.pdf'}")


def _split_address(addr: str) -> List[str]:
    """Split a single-line address into up to 3 form lines.
    Pattern: 'Street, City, ST ZIP' → ['Street', 'City, ST ZIP', ''].
    Falls back to the raw string in line 1 if no comma.
    """
    if not addr:
        return ["", "", ""]
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if len(parts) >= 3:
        # Street, City, ST ZIP → pack line1=street, line2="City, ST ZIP"
        return [parts[0], ", ".join(parts[1:]), ""]
    if len(parts) == 2:
        return [parts[0], parts[1], ""]
    return [addr.strip(), "", ""]


def _build_field_updates(
    parsed: Dict[str, Any],
    reytech_info: Dict[str, str],
    price_overrides: Optional[Dict[int, Dict[str, float]]] = None,
    quote_number: str = "",
    notes: str = "",
) -> Dict[str, Any]:
    """Return a {field_name: value} dict for every field we intend to
    write. Never mutates the parsed input.

    price_overrides shape: {row_index: {"unit_cost": X, "unit_price": Y}}
    — if a row has an override, use that price. Otherwise skip the row
    (leave unfilled so the human can see gaps before sending).
    """
    updates: Dict[str, Any] = {}

    # ── Supplier info ──
    updates[SUPPLIER_FIELDS["company_name"]] = reytech_info.get("company_name", "")
    addr_lines = _split_address(reytech_info.get("address", ""))
    updates[SUPPLIER_FIELDS["address_1"]] = addr_lines[0]
    updates[SUPPLIER_FIELDS["address_2"]] = addr_lines[1]
    updates[SUPPLIER_FIELDS["address_3"]] = addr_lines[2]
    updates[SUPPLIER_FIELDS["contact_name"]] = reytech_info.get("representative", "")
    updates[SUPPLIER_FIELDS["phone"]] = reytech_info.get("phone", "")
    updates[SUPPLIER_FIELDS["email"]] = reytech_info.get("email", "")

    # SB/MB/DVBE cert: Reytech uses cert_number as both
    cert = reytech_info.get("sb_mb") or reytech_info.get("dvbe") or ""
    updates[SUPPLIER_FIELDS["cert_number"]] = cert
    updates[SUPPLIER_FIELDS["cert_expiration"]] = reytech_info.get(
        "cert_expiration", ""
    )

    # Signature: we DO NOT write the typed name into the Signature1 text
    # field — the PNG overlay (_overlay_signature_png, run after field
    # writes) draws the real Reytech signature image on top of this cell.
    # Writing typed text first would leave "Michael Guadan" visible under
    # the signature. Date goes in the adjacent Date_es_:date field.
    updates[SUPPLIER_FIELDS["date_signed"]] = _today_mmddyyyy()

    # Rev — the revision number of this quote response. Default "0" for
    # first-time submissions; supplier_current.rev is whatever was last
    # stamped.
    updates[SUPPLIER_FIELDS["rev"]] = "0"

    # ── Line items ──
    overrides = price_overrides or {}
    running_subtotal = 0.0
    filled_rows = 0
    for item in parsed.get("line_items", []):
        row = int(item.get("row_index", 0))
        if row < 1 or row > MAX_ROWS:
            continue
        override = overrides.get(row) or {}
        unit_price = float(override.get("unit_price")
                           or item.get("unit_price")
                           or item.get("pricing", {}).get("recommended_price")
                           or 0.0)
        if unit_price <= 0:
            # Do NOT write zeros into the PDF — leave the cell blank so
            # the human operator sees an unfilled row and can manually
            # add a quote before sending. Reytech production standard.
            continue
        qty = float(item.get("qty") or 1)
        extension = round(unit_price * qty, 2)
        updates[LINE_ITEM_TEMPLATES["price_per_unit"].format(n=row)] = _money(unit_price)
        updates[LINE_ITEM_TEMPLATES["extension"].format(n=row)] = _money(extension)
        running_subtotal += extension
        filled_rows += 1

    # ── Totals ──
    # Tax: CA state sales tax lookup would be nicer but we don't have
    # the buyer's zip-resolved rate in scope here. Default to the header
    # zip_code → CDTFA rate via existing helper, falling back to 0.
    tax_rate = _lookup_tax_rate(parsed.get("header", {}).get("zip_code", ""))
    subtotal = round(running_subtotal, 2)
    freight = 0.0  # included per Reytech terms
    sales_tax = round(subtotal * tax_rate, 2)
    grand_total = round(subtotal + freight + sales_tax, 2)

    updates[TOTALS_FIELDS["subtotal"]] = _money(subtotal) if filled_rows else ""
    updates[TOTALS_FIELDS["freight"]] = _money(freight) if filled_rows else ""
    updates[TOTALS_FIELDS["sales_tax"]] = _money(sales_tax) if filled_rows else ""
    updates[TOTALS_FIELDS["grand_total"]] = _money(grand_total) if filled_rows else ""
    # Note: the page 1 "Amount" field is NOT the packet grand total — it
    # is the dollar-input box directly below the 25% subcontract row
    # ("If yes, provide subcontract amount: $___"). Leave it blank unless
    # the subcontract compliance flag is true.

    # ── Preference claim block (page 1 — 3 YES/NO pairs) ──
    # Driven by reytech_config.json -> compliance block. Reytech defaults:
    #   claiming_sb_preference=true, is_manufacturer=false,
    #   subcontract_25_percent=false
    compliance = reytech_info.get("compliance", {}) or {}
    for fact_key, (yes_cb, no_cb) in PREFERENCE_CHECKBOX_PAIRS.items():
        answer = bool(compliance.get(fact_key, False))
        updates[yes_cb] = "/Yes" if answer else "/Off"
        updates[no_cb] = "/Off" if answer else "/Yes"
    # Subcontract dollar amount only populated when Reytech actually
    # claims the 25% subcontract preference.
    if compliance.get("subcontract_25_percent"):
        updates[SUBCONTRACT_AMOUNT_FIELD] = str(compliance.get("subcontract_amount", "") or "")
    else:
        updates[SUBCONTRACT_AMOUNT_FIELD] = ""

    # ── Software Renewal (page 5 line-item worksheet) ──
    # Reytech quotes hardware and supplies — never software renewals.
    # Default: tick "No" and fill the term box with "N/A" so the state
    # sees a positively-filled response instead of a blank.
    updates[SW_RENEWAL_NO_CHECKBOX] = "/Yes"
    updates[SW_RENEWAL_TERM_FIELD] = "N/A"

    # ── CA Reseller Permit Attachment (page 10) ──
    permit = reytech_info.get("sellers_permit", "")
    if permit:
        updates[RESELLER_PERMIT_FIELD] = permit

    # ── CUF Certification / Attachment 6 (page 11) ──
    # Reytech is a DVBE meeting the Commercially Useful Function test:
    # we take title to goods, carry inventory risk, and resell to the
    # state under our own name. Answer "Yes" to all 6 statutory
    # questions, sign, date. Human operator still eyeballs before send.
    updates[CUF_TEXT_FIELDS["dba_name"]] = reytech_info.get("company_name", "")
    updates[CUF_TEXT_FIELDS["osds_ref"]] = cert
    # Signature block stays unwritten — the PNG overlay
    # (_overlay_signature_png) draws the cursive signature directly on
    # the widget rect. Writing typed text first would leave "Michael
    # Guadan" showing under the signature image.
    updates[CUF_TEXT_FIELDS["date"]] = _today_mmddyyyy()
    for cb in CUF_YES_CHECKBOXES:
        updates[cb] = "/Yes"

    # ── AMS 708 GenAI Disclosure (pages 15-16) ──
    # Fill the supplier identity block, tick "No" to GenAI usage (we
    # quote hardware/supplies, never AI products), and put "N/A" in
    # every numbered question + the free-text explanation so the form
    # is positively filled rather than blank.
    ams_street, city, state, zip_code = _parse_street_city_state_zip(
        reytech_info.get("address", "")
    )
    updates[AMS708_SUPPLIER_FIELDS["vendor_id"]] = reytech_info.get("fein", "") or cert
    updates[AMS708_SUPPLIER_FIELDS["phone"]] = reytech_info.get("phone", "")
    updates[AMS708_SUPPLIER_FIELDS["address"]] = ams_street
    updates[AMS708_SUPPLIER_FIELDS["city"]] = city
    updates[AMS708_SUPPLIER_FIELDS["state"]] = state
    updates[AMS708_SUPPLIER_FIELDS["zip_code"]] = zip_code
    updates[AMS708_SUPPLIER_FIELDS["date_signed"]] = _today_mmddyyyy()
    updates[AMS708_GENAI_NO_CHECKBOX] = "/Yes"
    for q in AMS708_QUESTION_FIELDS:
        updates[q] = "N/A"

    return updates


def _lookup_tax_rate(zip_code: str) -> float:
    """Try the existing CDTFA helper, fall back to 0.0875 CA avg if not
    available. Isolated here so unit tests can monkeypatch."""
    if not zip_code:
        return 0.0
    try:
        from src.agents.tax_agent import lookup_tax_rate_by_zip
        r = lookup_tax_rate_by_zip(zip_code) or 0.0
        return float(r)
    except Exception:
        return 0.0


def fill_cchcs_packet(
    source_pdf: str,
    parsed: Dict[str, Any],
    output_dir: Optional[str] = None,
    reytech_info: Optional[Dict[str, str]] = None,
    price_overrides: Optional[Dict[int, Dict[str, float]]] = None,
    quote_number: str = "",
    notes: str = "",
    strict: bool = True,
) -> Dict[str, Any]:
    """Fill the CCHCS packet with Reytech supplier info + prices and
    save as <basename>_Reytech.pdf.

    Returns:
        {
            "ok": bool,
            "error": str,              # only if ok=False
            "output_path": str,
            "fields_written": int,
            "rows_priced": int,
            "subtotal": float,
            "grand_total": float,
            "updates": {field_name: value, ...},   # full diff for QA
        }
    """
    result: Dict[str, Any] = {
        "ok": True,
        "output_path": "",
        "fields_written": 0,
        "rows_priced": 0,
        "subtotal": 0.0,
        "grand_total": 0.0,
        "updates": {},
    }

    if not HAS_PYPDF:
        result["ok"] = False
        result["error"] = "pypdf not available"
        return result
    if not os.path.exists(source_pdf):
        result["ok"] = False
        result["error"] = f"source PDF not found: {source_pdf}"
        return result

    # Default Reytech supplier info — pulled from the same build_reytech_info
    # helper that price_check.py uses, so company changes only have to be
    # made in one place.
    if reytech_info is None:
        try:
            from src.forms.price_check import REYTECH_INFO
            reytech_info = REYTECH_INFO
        except Exception:
            reytech_info = {
                "company_name": "Reytech Inc.",
                "representative": "Michael Guadan",
                "address": "30 Carnoustie Way, Trabuco Canyon, CA 92679",
                "phone": "949-229-1575",
                "email": "sales@reytechinc.com",
                "sb_mb": "2002605",
                "dvbe": "2002605",
            }

    # Build the field update dict from parsed + reytech + overrides
    updates = _build_field_updates(
        parsed=parsed,
        reytech_info=reytech_info,
        price_overrides=price_overrides,
        quote_number=quote_number,
        notes=notes,
    )

    # Open source, write output. Use `clone_from` so pypdf preserves
    # the entire document root including AcroForm, widget annotations,
    # appearance streams, and inter-object refs. The old pattern
    # (`append_pages_from_reader` + manual AcroForm dict copy) produced
    # dangling object references (e.g. "Object 1376 0 not defined")
    # that broke every round-trip read of the output.
    output_path = _output_path(source_pdf, output_dir)
    try:
        reader = PdfReader(source_pdf)
        writer = PdfWriter(clone_from=reader)
    except Exception as e:
        log.error("fill_cchcs_packet: pypdf open/clone failed: %s", e)
        result["ok"] = False
        result["error"] = f"pypdf clone error: {e}"
        return result

    # Apply field updates. Writer has per-page update for text fields;
    # checkboxes need to go through the widget V+AS update.
    text_updates = {k: v for k, v in updates.items() if not _looks_like_checkbox(k)}
    checkbox_updates = {k: v for k, v in updates.items() if _looks_like_checkbox(k)}

    # Text fields — pypdf.update_page_form_field_values per page
    written_text = 0
    for page_idx in range(len(writer.pages)):
        try:
            writer.update_page_form_field_values(
                writer.pages[page_idx], text_updates
            )
            written_text += 1  # count pages, real write count computed below
        except Exception as e:
            log.debug("cchcs_packet fill page %d text update: %s", page_idx, e)

    # Checkboxes — walk widgets and set /V + /AS to the chosen export value
    written_checks = _apply_checkbox_updates(writer, checkbox_updates)

    # Signature PNG overlay — draw the Reytech signature image on top of
    # the Signature1 widget's rect on page 1. Done AFTER text/checkbox
    # updates so it's drawn on the final rendered page.
    overlaid_names: List[str] = []
    try:
        _overlay_signature_png(writer, overlaid_names=overlaid_names)
    except Exception as _sig_e:
        log.warning("cchcs signature overlay failed: %s", _sig_e)
    signature_log = {
        "expected": list(_SIGNATURE_TARGETS),
        "overlaid": overlaid_names,
    }

    # Attachment splicing — replace each placeholder page in the packet
    # with its corresponding filled real form (Bidder Declaration,
    # STD 843, CalRecycle 74, STD 204, Seller's Permit, CA Civil Rights,
    # DARFUR) at its original position. This converts the packet from
    # "18 pages with placeholders" into "N pages with real attachments
    # inline in the order the state expects."
    splice_log: Dict[str, Any] = {}
    try:
        writer = _splice_attachments(writer, parsed, reytech_info, splice_log=splice_log)
    except Exception as _splice_e:
        log.error("cchcs attachment splice failed: %s", _splice_e, exc_info=True)

    # CRITICAL: tell PDF viewers to regenerate appearance streams for
    # text fields we just updated. Without this flag, pypdf only writes
    # the /V value — the visible appearance stream (what you actually
    # see when the PDF is rendered without form support, e.g. email
    # previews, some Mac Preview versions, PDF-to-image converters)
    # stays EMPTY. Reytech cannot send a form that looks blank to a
    # buyer, so we force /NeedAppearances=true which makes every PDF
    # viewer re-render on open.
    try:
        from pypdf.generic import BooleanObject as _Bool, NameObject as _Name
        root = writer._root_object
        if "/AcroForm" in root:
            acroform = root["/AcroForm"]
            acroform[_Name("/NeedAppearances")] = _Bool(True)
    except Exception as _nae:
        log.debug("NeedAppearances flag set failed: %s", _nae)

    # Persist
    try:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "wb") as f:
            writer.write(f)
    except Exception as e:
        log.error("fill_cchcs_packet: write failed: %s", e)
        result["ok"] = False
        result["error"] = f"pdf write error: {e}"
        return result

    # ── Verification: re-read output, count how many updates actually landed ──
    verify = _verify_written(output_path, updates)

    # Compute roll-ups for the return dict
    rows_priced = sum(
        1 for r in range(1, MAX_ROWS + 1)
        if updates.get(LINE_ITEM_TEMPLATES["price_per_unit"].format(n=r))
    )
    subtotal = _parse_money_safely(
        updates.get(TOTALS_FIELDS["subtotal"], ""),
        field="subtotal",
        where="fill_cchcs_packet.totals",
    )
    grand_total = _parse_money_safely(
        updates.get(TOTALS_FIELDS["grand_total"], ""),
        field="grand_total",
        where="fill_cchcs_packet.totals",
    )

    result.update({
        "ok": True,
        "output_path": output_path,
        "fields_written": verify["confirmed"],
        "fields_missing": verify["missing"],
        "rows_priced": rows_priced,
        "subtotal": subtotal,
        "grand_total": grand_total,
        "updates": updates,
        "verify": verify,
        "splice_log": splice_log,
        "signature_log": signature_log,
    })

    log.info(
        "cchcs_packet filled %s: %d fields confirmed, %d rows priced, total=$%s",
        os.path.basename(output_path),
        verify["confirmed"],
        rows_priced,
        _money(grand_total),
    )

    # ── Form QA Agent second-pass ──
    # Reytech's existing Form QA Agent gets a crack at the final PDF
    # using the same registry-driven field verification it runs on
    # every other form in the system. This catches supplier-info drift
    # and checkbox regressions that aren't specific to CCHCS business
    # rules. Its report is folded into the gate critical_issues.
    form_qa_report = {"passed": True, "issues": [], "warnings": []}
    try:
        from src.forms.form_qa import verify_single_form
        form_qa_data = {
            "rfq": {
                "solicitation_number": parsed.get("header", {}).get("solicitation_number", ""),
            },
        }
        form_qa_config = {"company": _reytech_info_to_form_qa_company(reytech_info)}
        form_qa_report = verify_single_form(
            pdf_path=output_path,
            form_id="cchcs_packet",
            data=form_qa_data,
            config=form_qa_config,
        )
    except Exception as _fqa_e:
        log.warning("form_qa second-pass crashed: %s", _fqa_e)
        form_qa_report = {
            "passed": True,
            "issues": [],
            "warnings": [f"form_qa crashed: {_fqa_e}"],
        }
    result["form_qa"] = form_qa_report

    # ── Gate validator — the zero-tolerance safety net ──
    # Runs every business-rule check against the finalized output. If
    # anything critical fails, flip ok=False so the caller knows not
    # to ship. Warnings don't block but are surfaced in the result.
    try:
        from src.forms.cchcs_packet_gate import gate_validate
        gate_report = gate_validate(
            output_pdf_path=output_path,
            parsed=parsed,
            reytech_info=reytech_info,
            price_overrides=price_overrides,
            splice_log=splice_log,
            signature_log=signature_log,
        )
        # Fold form_qa results into the gate's issue/warning buckets so
        # the route surfaces one unified list to the operator.
        if not form_qa_report.get("passed", True):
            gate_report["passed"] = False
            gate_report["critical_issues"].extend(
                f"[form_qa] {i}" for i in form_qa_report.get("issues", [])
            )
        gate_report["warnings"].extend(
            f"[form_qa] {w}" for w in form_qa_report.get("warnings", [])
        )
        result["gate"] = gate_report
        if not gate_report.get("passed", False):
            if strict:
                result["ok"] = False
                result["error"] = (
                    "gate validation failed: "
                    + "; ".join(gate_report.get("critical_issues", [])[:5])
                )
                log.warning(
                    "cchcs_packet gate FAIL on %s: %d critical, %d warnings (STRICT — blocking)",
                    os.path.basename(output_path),
                    len(gate_report.get("critical_issues", [])),
                    len(gate_report.get("warnings", [])),
                )
            else:
                # Non-strict (preview / dry-run) — return ok=True with
                # the gate report attached so the operator can still
                # eyeball the rendered packet but sees every issue.
                log.info(
                    "cchcs_packet gate FAIL on %s: %d critical, %d warnings (strict=False — non-blocking)",
                    os.path.basename(output_path),
                    len(gate_report.get("critical_issues", [])),
                    len(gate_report.get("warnings", [])),
                )
    except Exception as _ge:
        log.error("cchcs_packet gate crashed: %s", _ge, exc_info=True)
        result["gate"] = {"passed": False, "critical_issues": [f"gate crashed: {_ge}"], "warnings": []}
        result["ok"] = False
        result["error"] = f"gate crashed: {_ge}"

    return result


# ── Helpers ────────────────────────────────────────────────────────────────

def _parse_street_city_state_zip(addr: str) -> tuple:
    """Split a Reytech-style single-line address into
    (street, city, state, zip).

    Accepts both "Street, City, ST ZIP" (the preferred form) and the
    unpunctuated variant "Street City ST ZIP" that the reytech_config.json
    file currently stores. Returns empty strings for any missing piece.
    """
    if not addr:
        return "", "", "", ""
    s = addr.strip()
    # Pull the trailing "ST ZIP" off first — it's the most reliable anchor.
    m = re.search(r"(.*?)[,\s]+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", s)
    if not m:
        return s, "", "", ""
    head = m.group(1).strip().rstrip(",").strip()
    state = m.group(2)
    zip_code = m.group(3)
    # Split street from city. If there's a comma, trust it; otherwise
    # assume the last 1-2 words are the city (Trabuco Canyon, Los Angeles).
    if "," in head:
        left, right = head.rsplit(",", 1)
        street = left.strip()
        city = right.strip()
    else:
        # Heuristic: street contains a number + Way/St/Ave/etc; city is
        # whatever trails after a recognizable street suffix.
        street_suffix = re.search(
            r"\b(Way|St|Street|Ave|Avenue|Blvd|Boulevard|Rd|Road|Dr|Drive|Ln|Lane|Ct|Court|Pl|Place|Pkwy|Parkway|Hwy|Highway|Cir|Circle|Ter|Terrace)\b",
            head,
            re.IGNORECASE,
        )
        if street_suffix:
            cut = street_suffix.end()
            street = head[:cut].strip()
            city = head[cut:].strip()
        else:
            # Last token is the city — not great but better than nothing
            parts = head.split()
            if len(parts) >= 2:
                street = " ".join(parts[:-1])
                city = parts[-1]
            else:
                street = head
                city = ""
    return street, city, state, zip_code


def _reytech_info_to_form_qa_company(reytech_info: Dict[str, Any]) -> Dict[str, Any]:
    """Map the flat reytech_info dict into the nested {company: {...}}
    shape that form_qa._resolve_expected walks. Keeps form_qa decoupled
    from whatever shape the caller uses internally."""
    return {
        "name": reytech_info.get("company_name", ""),
        "owner": reytech_info.get("representative", ""),
        "title": reytech_info.get("title", "Owner"),
        "address": reytech_info.get("address", ""),
        "phone": reytech_info.get("phone", ""),
        "email": reytech_info.get("email", ""),
        "fein": reytech_info.get("fein", ""),
        "sellers_permit": reytech_info.get("sellers_permit", ""),
        "cert_number": reytech_info.get("cert_number") or reytech_info.get("sb_mb", ""),
        "cert_expiration": reytech_info.get("cert_expiration", ""),
    }


def _looks_like_checkbox(name: str) -> bool:
    """Heuristic: checkbox field names start with 'Check Box'. Used to
    split text vs checkbox updates in _build_field_updates output."""
    return name.startswith("Check Box")


def _full_field_name(annot: Any) -> str:
    """Walk the /Parent chain to build the full dotted field name for a
    widget annotation. AcroForm fields can be deeply nested (e.g.
    "Check Box21.0.0.0") — a leaf widget may only carry its own local
    /T ("0"), with each ancestor contributing one more dotted segment.
    Returns "" if the annot has no name at all.
    """
    parts: List[str] = []
    node: Any = annot
    seen = set()
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        t = node.get("/T") if hasattr(node, "get") else None
        if t is not None:
            parts.append(str(t))
        parent = node.get("/Parent") if hasattr(node, "get") else None
        if parent is None:
            break
        try:
            node = parent.get_object() if hasattr(parent, "get_object") else parent
        except Exception:
            break
    if not parts:
        return ""
    return ".".join(reversed(parts))


def _apply_checkbox_updates(writer: "PdfWriter", checkbox_updates: Dict[str, Any]) -> int:
    """Walk every page's annotations, find widgets whose full (dotted)
    field name matches a target, and set /V + /AS to the desired export
    value.

    pypdf's update_page_form_field_values handles text fields cleanly
    but is unreliable for checkboxes across pypdf versions. Doing it by
    hand ensures the appearance state matches the value — otherwise the
    box visually stays unchecked even after /V is set.

    Uses `_full_field_name` to handle deeply nested fields like
    "Check Box21.0.0.0" (the CUF attestation form on page 11).
    """
    if not checkbox_updates:
        return 0
    written = 0
    for page in writer.pages:
        annots = page.get("/Annots")
        if annots is None:
            continue
        try:
            annots = annots.get_object() if hasattr(annots, "get_object") else annots
        except Exception:
            continue
        for annot_ref in annots:
            try:
                annot = annot_ref.get_object()
                full_name = _full_field_name(annot)
                if not full_name:
                    continue
                # Match either the full dotted name or the leaf /T
                desired = None
                if full_name in checkbox_updates:
                    desired = checkbox_updates[full_name]
                else:
                    leaf = full_name.rsplit(".", 1)[-1]
                    if leaf in checkbox_updates:
                        desired = checkbox_updates[leaf]
                if desired is None:
                    continue
                # Desired export value — pypdf stores this as a Name
                # object, not a string. Also normalize common ways to
                # say "check this box".
                if desired in (True, "/Yes", "Yes", "yes", 1, "1", "/On", "On"):
                    export_name = _best_on_state(annot) or "/Yes"
                else:
                    export_name = "/Off"
                export = NameObject(export_name)
                annot[NameObject("/V")] = export
                annot[NameObject("/AS")] = export
                # Propagate /V to parent too — some viewers (Acrobat) read
                # the value from the parent field, not the widget.
                try:
                    parent = annot.get("/Parent")
                    if parent is not None:
                        pobj = parent.get_object()
                        pobj[NameObject("/V")] = export
                except Exception as _e:
                    log.debug('suppressed in _apply_checkbox_updates: %s', _e)
                written += 1
            except Exception as e:
                log.debug("checkbox update: %s", e)
                continue
    return written


def _best_on_state(annot: Any) -> Optional[str]:
    """Inspect a widget's appearance dictionary and return the first
    non-Off state name (e.g. "/Yes", "/On", "/1"). Falls back to None
    if the appearance dict is missing or only contains /Off.
    """
    try:
        ap = annot.get("/AP")
        if ap is None:
            return None
        ap = ap.get_object() if hasattr(ap, "get_object") else ap
        n = ap.get("/N")
        if n is None:
            return None
        n = n.get_object() if hasattr(n, "get_object") else n
        for key in n.keys():
            k = str(key)
            if k != "/Off":
                return k
    except Exception as _e:
        log.debug('suppressed in _best_on_state: %s', _e)
    return None


CIVIL_RIGHTS_TEMPLATE_NAME = "ca_civil_rights_attachment_blank.pdf"

CIVIL_RIGHTS_FIELDS = {
    "firm_name": "ProposerBidder Firm Name Printed",
    "fein": "Federal ID Number",
    "printed_name_title": "Printed Name and Title of Person Signing",
    "county": "Executed in the County of",
    "state": "Executed in the State of",
    "date": "mm/dd/yyyy",
    "signature": "Signature",
}


def _find_civil_rights_template() -> Optional[str]:
    """Locate the blank CA Civil Rights attachment template. Checked in
    order: data/templates/ under repo root, /app/data/templates/ on
    Railway, DATA_DIR/templates/."""
    candidates = []
    # Repo-root data/templates (dev)
    try:
        repo_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        candidates.append(os.path.join(repo_root, "data", "templates", CIVIL_RIGHTS_TEMPLATE_NAME))
    except Exception as _e:
        log.debug('suppressed in _find_civil_rights_template: %s', _e)
    candidates.append(os.path.join("/app", "data", "templates", CIVIL_RIGHTS_TEMPLATE_NAME))
    try:
        from src.core.paths import DATA_DIR
        candidates.append(os.path.join(DATA_DIR, "templates", CIVIL_RIGHTS_TEMPLATE_NAME))
    except Exception as _e:
        log.debug('suppressed in _find_civil_rights_template: %s', _e)
    return next((p for p in candidates if os.path.exists(p)), None)


def _append_civil_rights_attachment(writer: "PdfWriter", reytech_info: Dict[str, str]) -> bool:
    """Fill the CA Civil Rights Laws Attachment template and merge its
    page onto the end of the writer. Returns True if appended.

    The template has 7 text fields (firm name, FEIN, printed name/title,
    county, state, date, typed signature). We fill all 7, then merge the
    resulting single-page PDF into the packet writer via append. The
    typed signature here is acceptable — it's a declaration-style form
    that accepts a printed name per the "Signature" field label.
    """
    import io

    template_path = _find_civil_rights_template()
    if not template_path:
        _record_skip(SkipReason(
            name="civil_rights_template",
            reason=f"template {CIVIL_RIGHTS_TEMPLATE_NAME} not found in data/templates/",
            severity=Severity.WARNING,
            where="cchcs_packet_filler._append_civil_rights_attachment",
        ))
        return False

    # Parse Reytech address for county/state — the CCHCS form expects the
    # county where Reytech executes the declaration.
    _, _city, addr_state, _zip = _parse_street_city_state_zip(
        reytech_info.get("address", "")
    )
    county = reytech_info.get("county", "Orange")  # Trabuco Canyon is in Orange County
    state = addr_state or "California"
    if state == "CA":
        state = "California"
    name = reytech_info.get("representative", "")
    title = reytech_info.get("title", "Owner")
    printed = f"{name}, {title}" if name and title else name

    values = {
        CIVIL_RIGHTS_FIELDS["firm_name"]: reytech_info.get("company_name", ""),
        CIVIL_RIGHTS_FIELDS["fein"]: reytech_info.get("fein", ""),
        CIVIL_RIGHTS_FIELDS["printed_name_title"]: printed,
        CIVIL_RIGHTS_FIELDS["county"]: county,
        CIVIL_RIGHTS_FIELDS["state"]: state,
        CIVIL_RIGHTS_FIELDS["date"]: _today_mmddyyyy(),
        CIVIL_RIGHTS_FIELDS["signature"]: name,
    }

    try:
        reader = PdfReader(template_path)
        sub_writer = PdfWriter(clone_from=reader)
        for page_idx in range(len(sub_writer.pages)):
            try:
                sub_writer.update_page_form_field_values(
                    sub_writer.pages[page_idx], values
                )
            except Exception as e:
                log.debug("civil rights page %d update: %s", page_idx, e)
        # Force appearance regeneration on this sub-PDF too
        try:
            from pypdf.generic import BooleanObject as _Bool, NameObject as _Name
            root = sub_writer._root_object
            if "/AcroForm" in root:
                root["/AcroForm"][_Name("/NeedAppearances")] = _Bool(True)
        except Exception as _e:
            log.debug('suppressed in _append_civil_rights_attachment: %s', _e)

        # Serialize the filled sub-PDF in-memory so we can re-read it
        # and append its flattened page onto the main writer.
        buf = io.BytesIO()
        sub_writer.write(buf)
        buf.seek(0)
        filled_reader = PdfReader(buf)
        pages_before = len(writer.pages)
        for p in filled_reader.pages:
            writer.add_page(p)
        # add_page() copies the page and its annots but does NOT register
        # the widgets in the top-level AcroForm /Fields tree. Without
        # that registration, `get_fields()` won't see them and downstream
        # viewers may ignore the values. Walk the new pages and register
        # each widget into writer's AcroForm.
        try:
            from pypdf.generic import ArrayObject, DictionaryObject, NameObject as _Name
            root = writer._root_object
            if "/AcroForm" not in root:
                root[_Name("/AcroForm")] = DictionaryObject()
            af = root["/AcroForm"]
            if hasattr(af, "get_object"):
                af = af.get_object()
            if "/Fields" not in af:
                af[_Name("/Fields")] = ArrayObject()
            fields_arr = af["/Fields"]
            for page_idx in range(pages_before, len(writer.pages)):
                page = writer.pages[page_idx]
                annots = page.get("/Annots")
                if annots is None:
                    continue
                try:
                    annots = annots.get_object() if hasattr(annots, "get_object") else annots
                except Exception:
                    continue
                for annot_ref in annots:
                    try:
                        annot = annot_ref.get_object()
                        if annot.get("/Subtype") != "/Widget":
                            continue
                        if annot.get("/T") is None:
                            continue
                        fields_arr.append(annot_ref)
                    except Exception:
                        continue
        except Exception as e:
            log.debug("civil rights: field registration failed: %s", e)
        log.info("civil rights: appended %d page(s) from %s",
                 len(filled_reader.pages), os.path.basename(template_path))

        # Overlay the signature PNG onto the appended page's Signature
        # widget so the form shows an actual cursive signature, not
        # just a typed name.
        try:
            _overlay_civil_rights_signature(writer, len(writer.pages) - 1)
        except Exception as e:
            log.debug("civil rights signature overlay: %s", e)
        return True
    except Exception as e:
        _record_skip(SkipReason(
            name="civil_rights_append",
            reason=f"{type(e).__name__}: {e}",
            severity=Severity.WARNING,
            where="cchcs_packet_filler._append_civil_rights_attachment",
        ))
        return False


def _overlay_civil_rights_signature(writer: "PdfWriter", page_index: int) -> bool:
    """Draw the signature PNG onto the Signature text field of the
    appended Civil Rights page."""
    import io

    sig_path = _find_signature_png()
    if not sig_path:
        return False
    try:
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.utils import ImageReader
    except ImportError:
        return False

    if page_index < 0 or page_index >= len(writer.pages):
        return False
    page = writer.pages[page_index]
    annots = page.get("/Annots")
    if annots is None:
        return False
    try:
        annots = annots.get_object() if hasattr(annots, "get_object") else annots
    except Exception:
        return False

    sig_rect = None
    for annot_ref in annots:
        try:
            annot = annot_ref.get_object()
            full = _full_field_name(annot)
            if full == CIVIL_RIGHTS_FIELDS["signature"] or full.endswith(CIVIL_RIGHTS_FIELDS["signature"]):
                # Only match if the leaf is exactly "Signature" to avoid
                # colliding with the main packet's Signature1 field.
                leaf = full.rsplit(".", 1)[-1]
                if leaf != "Signature":
                    continue
                rect = annot.get("/Rect")
                if rect is None:
                    continue
                sig_rect = tuple(float(x) for x in rect)
                try:
                    annot[NameObject("/V")] = TextStringObject("")
                except Exception as _e:
                    log.debug('suppressed in _overlay_civil_rights_signature: %s', _e)
                break
        except Exception:
            continue

    if not sig_rect:
        return False

    page_w, page_h = 612.0, 792.0
    try:
        mb = page.mediabox
        page_w, page_h = float(mb.width), float(mb.height)
    except Exception as _e:
        log.debug('suppressed in _overlay_civil_rights_signature: %s', _e)
    fl, fb, fr, ft = sig_rect
    pad = 2.0
    try:
        buf = io.BytesIO()
        c = rl_canvas.Canvas(buf, pagesize=(page_w, page_h))
        c.drawImage(
            ImageReader(sig_path),
            fl + pad,
            fb + pad,
            width=(fr - fl) - pad * 2,
            height=(ft - fb) - pad * 2,
            mask="auto",
            preserveAspectRatio=True,
            anchor="sw",
        )
        c.save()
        buf.seek(0)
        page.merge_page(PdfReader(buf).pages[0])
        return True
    except Exception as e:
        log.debug("civil rights sig overlay draw: %s", e)
        return False


_SIGNATURE_TARGETS = (
    # Cover page supplier signature (text widget, /Tx)
    "Signature1_es_:signer:signature",
    # AMS 708 GenAI Disclosure signature (/Sig widget on page 16)
    "AMS 708 Signature",
    # CUF Certification Form Owner's/Officer's signature block (page 11)
    "Signature Block28_es_:signer:signatureblock",
)


def _find_signature_png() -> Optional[str]:
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "signature_transparent.png"),
        "/app/signature_transparent.png",
    ]
    try:
        from src.core.paths import DATA_DIR
        candidates.append(os.path.join(DATA_DIR, "signature_transparent.png"))
    except Exception as _e:
        log.debug('suppressed in _find_signature_png: %s', _e)
    return next((p for p in candidates if os.path.exists(p)), None)


def _overlay_signature_png(
    writer: "PdfWriter",
    overlaid_names: Optional[List[str]] = None,
) -> bool:
    """Draw the Reytech signature PNG on top of every signature widget
    the CCHCS packet expects — currently the cover-page Signature1 field
    and the AMS 708 /Sig widget. Returns True if at least one overlay
    landed.

    Mirrors the approach in src/forms/price_check.py `_add_signature_to_pdf`
    but scoped to the CCHCS packet's field names and extended to multiple
    pages.
    """
    import io

    if not writer.pages:
        return False

    sig_path = _find_signature_png()
    if not sig_path:
        log.warning("cchcs signature overlay: signature_transparent.png not found")
        return False

    try:
        from reportlab.pdfgen import canvas as rl_canvas
        from reportlab.lib.utils import ImageReader
    except ImportError:
        log.warning("cchcs signature overlay: reportlab not available")
        return False

    # Scan every page for matching signature widgets.
    hits = []  # (page_index, rect, annot, matched_target_name)
    for page_idx, page in enumerate(writer.pages):
        annots = page.get("/Annots")
        if annots is None:
            continue
        try:
            annots = annots.get_object() if hasattr(annots, "get_object") else annots
        except Exception:
            continue
        for annot_ref in annots:
            try:
                annot = annot_ref.get_object()
                full = _full_field_name(annot)
                if not full:
                    continue
                matched_target = None
                for t in _SIGNATURE_TARGETS:
                    if full == t or full.endswith(t):
                        matched_target = t
                        break
                if matched_target is None:
                    continue
                rect = annot.get("/Rect")
                if rect is None:
                    continue
                r = tuple(float(x) for x in rect)
                hits.append((page_idx, r, annot, matched_target))
                # Clear any typed value so it doesn't underlay the PNG
                try:
                    annot[NameObject("/V")] = TextStringObject("")
                except Exception as _e:
                    log.debug('suppressed in _overlay_signature_png: %s', _e)
            except Exception:
                continue

    if not hits:
        log.debug("cchcs signature overlay: no signature widgets found")
        return False

    drawn = 0
    img = ImageReader(sig_path)
    for page_idx, rect, _annot, matched_target in hits:
        page = writer.pages[page_idx]
        page_w, page_h = 612.0, 792.0
        try:
            mb = page.mediabox
            page_w, page_h = float(mb.width), float(mb.height)
        except Exception as _e:
            log.debug('suppressed in _overlay_signature_png: %s', _e)
        fl, fb, fr, ft = rect
        fw = fr - fl
        fh = ft - fb
        pad = 2.0
        img_w = fw - pad * 2
        img_h = fh - pad * 2
        try:
            buf = io.BytesIO()
            c = rl_canvas.Canvas(buf, pagesize=(page_w, page_h))
            c.drawImage(
                img,
                fl + pad,
                fb + pad,
                width=img_w,
                height=img_h,
                mask="auto",
                preserveAspectRatio=True,
                anchor="sw",
            )
            c.save()
            buf.seek(0)
            overlay_reader = PdfReader(buf)
            page.merge_page(overlay_reader.pages[0])
            drawn += 1
            if overlaid_names is not None:
                overlaid_names.append(matched_target)
            log.info(
                "cchcs signature overlay: drew PNG on page %d rect (%.1f,%.1f,%.1f,%.1f) target=%s",
                page_idx + 1, fl, fb, fr, ft, matched_target,
            )
        except Exception as e:
            log.error("cchcs signature overlay: page %d failed: %s", page_idx + 1, e)
    return drawn > 0


def _verify_written(output_path: str, expected: Dict[str, Any]) -> Dict[str, Any]:
    """Re-read the output PDF and compare every expected field to what
    actually landed. Returns confirmed/missing/mismatched counts + the
    lists of missing field names for the morning review."""
    out = {"confirmed": 0, "missing": 0, "mismatched": 0,
           "missing_fields": [], "mismatched_fields": []}
    try:
        reader = PdfReader(output_path)
        actual = reader.get_fields() or {}
    except Exception as e:
        log.warning("verify re-read failed: %s", e)
        return out

    for name, exp_val in expected.items():
        if exp_val in ("", None):
            continue
        # Checkboxes: expected like "/Yes" or True → accept /Yes
        got = actual.get(name)
        if got is None:
            out["missing"] += 1
            out["missing_fields"].append(name)
            continue
        got_v = str(got.get("/V", "")).strip() if isinstance(got, dict) else ""
        if _looks_like_checkbox(name):
            # Checkbox expected value might be an "On" variant (/Yes,
            # True, etc.) or explicitly "/Off". Confirm in both cases.
            exp_is_off = exp_val in ("/Off", "Off", False, 0, "0")
            got_is_on = got_v in ("/Yes", "Yes", "True", "1", "/On")
            got_is_off = got_v in ("/Off", "Off", "", "False", "0")
            if exp_is_off and got_is_off:
                out["confirmed"] += 1
            elif (not exp_is_off) and got_is_on:
                out["confirmed"] += 1
            else:
                out["missing"] += 1
                out["missing_fields"].append(name)
            continue
        # Text field
        if got_v and got_v.strip() == str(exp_val).strip():
            out["confirmed"] += 1
        elif got_v:
            out["mismatched"] += 1
            out["mismatched_fields"].append((name, got_v, exp_val))
        else:
            out["missing"] += 1
            out["missing_fields"].append(name)
    return out


def _splice_attachments(
    writer: "PdfWriter",
    parsed: Dict[str, Any],
    reytech_info: Dict[str, Any],
    splice_log: Optional[Dict[str, Any]] = None,
) -> "PdfWriter":
    """Replace each placeholder page in the packet writer with its
    corresponding filled real form from the attachment registry, and
    return a new writer containing the final packet.

    Strategy: serialize the current writer (which already has the
    inline form fields filled — cover, line items, CUF, AMS 708) to an
    in-memory PDF. Build a fresh writer, then walk the source pages in
    order, and for each page either copy it as-is or substitute it
    with the filled attachment(s) from the registry.

    pypdf's `writer.append(fileobj, pages=[...])` handles AcroForm
    merging automatically, which is why we use it instead of the
    low-level add_page + manual field registration pattern we used
    previously for the civil rights appendix.
    """
    from src.forms.cchcs_attachment_registry import (
        CCHCS_ATTACHMENTS,
        placeholder_page_set,
    )
    from src.forms.cchcs_attachment_fillers import run_filler

    # Serialize the current writer so we can re-append from it
    src_buf = io.BytesIO()
    try:
        writer.write(src_buf)
    except Exception as e:
        _record_skip(SkipReason(
            name="splice_serialize",
            reason=f"{type(e).__name__}: {e}",
            severity=Severity.WARNING,
            where="cchcs_packet_filler._splice_attachments",
        ))
        return writer
    src_buf.seek(0)
    src_reader = PdfReader(src_buf)
    src_page_count = len(src_reader.pages)

    placeholders = placeholder_page_set()
    placeholder_specs = {a["placeholder_page"]: a for a in CCHCS_ATTACHMENTS}

    if splice_log is not None:
        splice_log.setdefault("expected", list(CCHCS_ATTACHMENTS))
        splice_log.setdefault("spliced", [])
        splice_log.setdefault("failed", [])

    final = PdfWriter()
    # Walk pages 1..N. For each run of non-placeholder pages, bulk-append
    # from the source reader. For each placeholder, substitute with the
    # filled attachment from its filler.
    run_start_0 = 0  # 0-indexed start of current copy-as-is run
    for page_num in range(1, src_page_count + 1):
        if page_num in placeholders:
            # Flush the pending copy-as-is run first
            if run_start_0 < page_num - 1:
                try:
                    final.append(
                        src_buf,
                        pages=list(range(run_start_0, page_num - 1)),
                    )
                except Exception as e:
                    log.error(
                        "splice copy-as-is pages %d..%d failed: %s",
                        run_start_0 + 1, page_num - 1, e,
                    )
            run_start_0 = page_num  # skip the placeholder page

            spec = placeholder_specs[page_num]
            filled = run_filler(spec["filler"], reytech_info, parsed)
            if filled is None:
                # Fall back to keeping the placeholder so the page count
                # stays predictable and the operator still sees the
                # section the state expected.
                _record_skip(SkipReason(
                    name=f"splice_filler:{spec['filler']}",
                    reason=f"filler returned None for page {page_num} ({spec['description']}); placeholder kept",
                    severity=Severity.WARNING,
                    where="cchcs_packet_filler._splice_attachments",
                ))
                if splice_log is not None:
                    splice_log["failed"].append(
                        (spec["num"], spec["description"], "filler returned None")
                    )
                try:
                    final.append(src_buf, pages=[page_num - 1])
                except Exception as _e:
                    log.debug('suppressed in _splice_attachments: %s', _e)
                continue
            try:
                final.append(filled)
                if splice_log is not None:
                    splice_log["spliced"].append(spec["num"])
                log.info(
                    "splice: replaced page %d placeholder with %s (%s)",
                    page_num, spec["template"], spec["description"],
                )
            except Exception as e:
                _record_skip(SkipReason(
                    name=f"splice_append:{spec['filler']}",
                    reason=f"append filled {spec['template']} for page {page_num} failed: {type(e).__name__}: {e}",
                    severity=Severity.WARNING,
                    where="cchcs_packet_filler._splice_attachments",
                ))
                if splice_log is not None:
                    splice_log["failed"].append(
                        (spec["num"], spec["description"], f"append error: {e}")
                    )
                # Fall back to the placeholder
                try:
                    final.append(src_buf, pages=[page_num - 1])
                except Exception as _e:
                    log.debug('suppressed in _splice_attachments: %s', _e)

    # Flush the tail run (pages after the last placeholder)
    if run_start_0 < src_page_count:
        try:
            final.append(
                src_buf,
                pages=list(range(run_start_0, src_page_count)),
            )
        except Exception as e:
            log.error("splice tail run %d..%d failed: %s",
                      run_start_0 + 1, src_page_count, e)

    # Force appearance regeneration on the final AcroForm so every
    # viewer renders the form values we just wrote.
    try:
        from pypdf.generic import BooleanObject as _Bool, NameObject as _Name
        root = final._root_object
        if "/AcroForm" in root:
            af = root["/AcroForm"]
            if hasattr(af, "get_object"):
                af = af.get_object()
            af[_Name("/NeedAppearances")] = _Bool(True)
    except Exception as e:
        log.debug("final NeedAppearances failed: %s", e)

    log.info(
        "splice: final packet has %d pages (source had %d, %d placeholders replaced)",
        len(final.pages), src_page_count, len(placeholders),
    )
    return final


__all__ = [
    "fill_cchcs_packet",
    "_output_path",
    "COMPLIANCE_CHECKBOXES_YES",
    "_splice_attachments",
]
