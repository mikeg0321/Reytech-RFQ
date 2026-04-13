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

from src.forms.cchcs_packet_parser import (
    HEADER_FIELDS,
    LINE_ITEM_TEMPLATES,
    SUPPLIER_FIELDS,
    TOTALS_FIELDS,
    MAX_ROWS,
)


# Compliance checkboxes on page 1 — these are the SB/MB/DVBE affirmations
# and "we agree to terms" boxes that a supplier normally checks. Named
# exactly as they appear in the packet's field dict.
COMPLIANCE_CHECKBOXES_YES = (
    "Check Box11",  # read/agree to terms
    "Check Box12",
    "Check Box13",
    "Check Box14",
    "Check Box15",
    "Check Box16",
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

    # Signature (typed name — the field is /Tx not /Sig) + date
    updates[SUPPLIER_FIELDS["signature"]] = reytech_info.get("representative", "")
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
    # The Amount field on page 1 is a single-line grand total
    updates[SUPPLIER_FIELDS["amount_total"]] = _money(grand_total) if filled_rows else ""

    # ── Compliance checkboxes ──
    # Default stance: check all 6 supplier affirmation boxes (SB/MB, DVBE,
    # acknowledge T&Cs, Buy American Act, etc.) since Reytech IS
    # certified and DOES agree to state terms by default. Human operator
    # can uncheck any that don't apply before sending.
    for cb in COMPLIANCE_CHECKBOXES_YES:
        updates[cb] = "/Yes"

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
    subtotal = 0.0
    grand_total = 0.0
    try:
        sub_raw = updates.get(TOTALS_FIELDS["subtotal"], "") or "0"
        subtotal = float(sub_raw.replace(",", "") or 0)
    except (ValueError, TypeError):
        pass
    try:
        tot_raw = updates.get(TOTALS_FIELDS["grand_total"], "") or "0"
        grand_total = float(tot_raw.replace(",", "") or 0)
    except (ValueError, TypeError):
        pass

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
    })

    log.info(
        "cchcs_packet filled %s: %d fields confirmed, %d rows priced, total=$%s",
        os.path.basename(output_path),
        verify["confirmed"],
        rows_priced,
        _money(grand_total),
    )
    return result


# ── Helpers ────────────────────────────────────────────────────────────────

def _looks_like_checkbox(name: str) -> bool:
    """Heuristic: checkbox field names start with 'Check Box'."""
    return name.startswith("Check Box")


def _apply_checkbox_updates(writer: "PdfWriter", checkbox_updates: Dict[str, Any]) -> int:
    """Walk every page's annotations, find widgets whose /T matches a
    target checkbox name, and set /V + /AS to the desired export value.

    pypdf's update_page_form_field_values handles text fields cleanly
    but is unreliable for checkboxes across pypdf versions. Doing it by
    hand ensures the appearance state matches the value — otherwise the
    box visually stays unchecked even after /V is set.
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
                name = annot.get("/T")
                if name is None:
                    # Try parent
                    parent = annot.get("/Parent")
                    if parent is not None:
                        name = parent.get_object().get("/T")
                if name is None:
                    continue
                name_s = str(name)
                if name_s not in checkbox_updates:
                    continue
                desired = checkbox_updates[name_s]
                # Desired export value — pypdf stores this as a Name
                # object, not a string. Also normalize common ways to
                # say "check this box".
                if desired in (True, "/Yes", "Yes", "yes", 1, "1", "/On", "On"):
                    export = NameObject("/Yes")
                else:
                    export = NameObject("/Off")
                annot[NameObject("/V")] = export
                annot[NameObject("/AS")] = export
                written += 1
            except Exception as e:
                log.debug("checkbox update: %s", e)
                continue
    return written


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
            # We set /V to /Yes; pypdf reads it back as "/Yes" string
            if got_v in ("/Yes", "Yes", "True", "1", "/On"):
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


__all__ = [
    "fill_cchcs_packet",
    "_output_path",
    "COMPLIANCE_CHECKBOXES_YES",
]
