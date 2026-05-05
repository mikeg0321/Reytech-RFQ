"""Alignment-confirmation rollup for /rfq/<id>/review-package.

Built 2026-05-01 (PR-A of the global send-flow fix). Replaces three scattered
banners (source-validation / missing-forms / form-QA) with a single rollup that
answers Mike's question: "is this package aligned with what the buyer asked
for, and have I returned every required form?"

Five checks roll up into one verdict:
  1. forms_on_disk      — every required form has an on-disk file with size > 0
  2. qa_passed          — manifest.field_audit._qa_passed is true
  3. source_valid       — manifest.source_validation has no errors
  4. buyer_present      — RFQ has buyer name + email + agency + due-date
  5. items_priced       — every line item has unit_price > 0

The page also shows two side tables driven by this module:
  - items_alignment     — buyer-asked items vs your 704B (per Mike's "1e all"
                          requirement). Source = parsed PDF + email body
                          (Q2 "both"). When neither produced items (manual-
                          entry case like flushable wipes), banner says so.
  - forms_checklist     — required form name | on-disk filename | KB | QA
                          verdict. Promoted from the old form_id-only list
                          per Mike's "ESPECIALLY package to make sure i
                          returned all forms."

Pure logic — no DB, no filesystem, no Flask. Caller passes in everything as
plain dicts. This makes the unit tests trivial and avoids circular imports
into dashboard.py.
"""
from __future__ import annotations

import os
from typing import Optional


# ── Public types (plain dicts; no dataclasses to keep this lib-free) ────────

def compute_review_alignment(rfq, manifest, agency_cfg, output_dir=None,
                             bidpkg_internal=None, source_items=None):
    """Return alignment dict for the review-package template.

    Parameters
    ----------
    rfq : dict
        The RFQ record from rfqs.json. Reads requestor_name, requestor_email,
        original_sender, agency, due_date, line_items, items, pc_id, source_pdf
        items, email_body items.
    manifest : dict
        package_manifest row with reviews, required_forms, missing_forms,
        generated_forms, field_audit, source_validation, quote_total,
        item_count, agency_name. Must already be loaded.
    agency_cfg : dict
        Agency config (from match_agency()). Reads name + required_forms.
    output_dir : str, optional
        Absolute path to the output dir for this RFQ (where the on-disk PDFs
        live). Used for size lookups. If None, sizes are reported as None.
    bidpkg_internal : set[str], optional
        Form IDs that live inside the bid package and aren't standalone
        deliverables. Defaults to the canonical CCHCS set.
    source_items : list[dict], optional
        Items extracted from the buyer's source RFQ (parsed PDF + email body
        merged). Caller is responsible for the merge logic — this module
        just diffs against the RFQ's own line_items. When None or empty, the
        items_alignment.has_source flag is False and the UI shows the manual-
        entry banner.

    Returns
    -------
    dict with keys:
        rollup           — {aligned: bool, issues: [str], checks: {name: bool}}
        items_alignment  — {has_source: bool, rows: [...], pc_total_delta}
        forms_checklist  — [{form_id, display_name, required, generated,
                             filename, size_kb, qa_verdict, missing}]
    """
    # Distinguish "no filter wanted" (caller passed empty set) from "use
    # defaults" (caller passed None). The truthy `or` here was clobbering
    # an explicit empty set into the default 7-form filter, hiding 5 of 9
    # CalVet deliverables (P0 incident 2026-05-04, RFQ 7d3c0fee Auralis):
    # routes_rfq.py builds _bidpkg_internal=set() when there's no bidpkg
    # in the manifest, then passes `_bidpkg_internal or None` — `set() or
    # None` evaluates to None, and we'd re-default. The agency-required
    # standalone forms then got filtered as if they were inside a bid
    # package that doesn't exist.
    if bidpkg_internal is None:
        bidpkg_internal = {
            "dvbe843", "sellers_permit", "calrecycle74", "darfur_act",
            "bidder_decl", "std21", "genai_708"
        }

    forms = _build_forms_checklist(
        manifest=manifest, agency_cfg=agency_cfg,
        output_dir=output_dir, bidpkg_internal=bidpkg_internal,
    )
    items = _build_items_alignment(
        rfq=rfq, manifest=manifest, source_items=source_items,
    )
    rollup = _build_rollup(
        rfq=rfq, manifest=manifest, forms=forms, items=items,
    )

    # Agency-aware label: which buyer form carries the line-item response?
    # CalVet → CV012 CUF, CCHCS → 704B, DSH → AttB, etc. Falls back to
    # "Quote" so the items table never says a misleading form name.
    primary_form_id = (agency_cfg or {}).get("primary_response_form") or "quote"
    primary_form_label = FORM_DISPLAY_NAMES.get(primary_form_id,
                                                 primary_form_id.replace("_", " ").title())

    return {
        "rollup": rollup,
        "items_alignment": items,
        "forms_checklist": forms,
        "primary_form_id": primary_form_id,
        "primary_form_label": primary_form_label,
    }


# ── Forms checklist ─────────────────────────────────────────────────────────

# Display names — keep in sync with the JS map in rfq_review.html (line 371).
FORM_DISPLAY_NAMES = {
    "quote": "Reytech Quote",
    "703b": "AMS 703B",
    "703c": "AMS 703C",
    "704b": "AMS 704B",
    "704": "AMS 704",
    "bidpkg": "Bid Package",
    "bidder_decl": "Bidder Declaration",
    "calrecycle74": "CalRecycle 74",
    "darfur_act": "Darfur Act",
    "dvbe843": "DVBE PD 843",
    "cv012_cuf": "CV 012 CUF",
    "barstow_cuf": "Barstow CUF",
    "std204": "STD 204",
    "std205": "STD 205",
    "std1000": "STD 1000",
    "sellers_permit": "Seller's Permit",
    "obs1600": "OBS 1600 (Food Cert)",
    "drug_free_workplace": "Drug-Free Workplace",
    "genai_708": "AMS 708 (GenAI)",
    "std21": "STD 21",
}


def _build_forms_checklist(manifest, agency_cfg, output_dir, bidpkg_internal):
    """One row per required form + any extra generated forms.

    Excludes bid-package internal forms from the standalone list — those are
    already inside the merged bid package PDF and shouldn't show as "missing"
    when they're not standalone files.
    """
    required = list((agency_cfg or {}).get("required_forms") or
                    manifest.get("required_forms") or [])
    reviews = manifest.get("reviews") or []
    field_audit = manifest.get("field_audit") or {}
    if not isinstance(field_audit, dict):
        field_audit = {}

    # Map form_id -> review row for fast lookup
    review_by_id = {}
    for rv in reviews:
        fid = rv.get("form_id") or ""
        if fid:
            review_by_id.setdefault(fid, rv)

    rows = []
    seen = set()

    # Order: required forms first (in agency order), then anything generated
    # but not required (rare — operator added a one-off).
    for form_id in required:
        if form_id in bidpkg_internal:
            continue  # internal — not a standalone deliverable
        rows.append(_forms_row(form_id, review_by_id, field_audit,
                               output_dir, required=True))
        seen.add(form_id)

    for rv in reviews:
        fid = rv.get("form_id") or ""
        if not fid or fid in seen or fid in bidpkg_internal:
            continue
        rows.append(_forms_row(fid, review_by_id, field_audit,
                               output_dir, required=False))
        seen.add(fid)

    return rows


def _forms_row(form_id, review_by_id, field_audit, output_dir, required):
    review = review_by_id.get(form_id) or {}
    filename = review.get("form_filename") or ""
    size_kb = None
    missing = not filename
    if filename and output_dir:
        path = os.path.join(output_dir, filename)
        try:
            if os.path.exists(path):
                size_bytes = os.path.getsize(path)
                size_kb = max(1, round(size_bytes / 1024))
                if size_bytes == 0:
                    missing = True  # zero-byte = silently failed generate
            else:
                missing = True  # filename in manifest but not on disk
        except OSError:
            size_kb = None  # filesystem error — don't crash, report None

    fa = field_audit.get(form_id) if isinstance(field_audit, dict) else None
    qa_verdict = "unknown"
    if isinstance(fa, dict):
        if fa.get("errors"):
            qa_verdict = "fail"
        elif fa.get("warnings"):
            qa_verdict = "warn"
        elif fa.get("checks"):
            qa_verdict = "pass"

    return {
        "form_id": form_id,
        "display_name": FORM_DISPLAY_NAMES.get(form_id,
                                                form_id.replace("_", " ").title()),
        "required": required,
        "filename": filename,
        "size_kb": size_kb,
        "verdict": review.get("verdict") or "pending",
        "qa_verdict": qa_verdict,
        "missing": missing,
        "notes": review.get("notes") or "",
    }


# ── Items alignment ─────────────────────────────────────────────────────────

def _build_items_alignment(rfq, manifest, source_items):
    """Compare buyer-asked items against the 704B (RFQ line items).

    `source_items` is the merged list of items from the parsed source PDF
    AND the email body (Mike's Q2 "both"). When that's empty (manual-entry
    case), has_source=False and the template shows a "no source to compare
    against" banner — rows still render so Mike can see his own items.
    """
    our_items = list(rfq.get("line_items") or rfq.get("items") or [])

    has_source = bool(source_items)
    src = list(source_items or [])

    # Best-effort match by line index, then by description tokens.
    # This is the alignment hint, not a hard validator — Mike eyeballs it.
    rows = []
    for idx, our in enumerate(our_items):
        their = src[idx] if idx < len(src) else None
        rows.append({
            "line": idx + 1,
            "our_desc": (our.get("description") or "").strip(),
            "our_qty": _to_int(our.get("qty") or our.get("quantity") or 1),
            "our_mfg": (our.get("part_number") or our.get("mfg_number") or "").strip(),
            "our_unit": _to_float(our.get("price_per_unit") or
                                  our.get("unit_price") or 0),
            "their_desc": (their or {}).get("description", "").strip() if their else "",
            "their_qty": _to_int((their or {}).get("qty") or
                                  (their or {}).get("quantity") or 0) if their else 0,
            "their_mfg": ((their or {}).get("part_number") or
                          (their or {}).get("mfg_number") or "").strip() if their else "",
            "match": _classify_match(our, their),
        })

    return {
        "has_source": has_source,
        "rows": rows,
        "buyer_extra_count": max(0, len(src) - len(our_items)),
    }


def _classify_match(our, their):
    """Return 'matched' / 'qty_differs' / 'desc_differs' / 'no_source' / 'unmatched'."""
    if not their:
        return "no_source"
    o_qty = _to_int(our.get("qty") or our.get("quantity") or 0)
    t_qty = _to_int(their.get("qty") or their.get("quantity") or 0)
    o_desc = (our.get("description") or "").strip().lower()
    t_desc = (their.get("description") or "").strip().lower()
    if o_qty != t_qty and t_qty > 0:
        return "qty_differs"
    if o_desc and t_desc and not _desc_overlap(o_desc, t_desc):
        return "desc_differs"
    return "matched"


def _desc_overlap(a, b, min_token_overlap=0.4):
    """Cheap token-overlap check — same threshold philosophy as catalog match."""
    at = set(_tokenize(a))
    bt = set(_tokenize(b))
    if not at or not bt:
        return False
    overlap = len(at & bt) / max(1, min(len(at), len(bt)))
    return overlap >= min_token_overlap


def _tokenize(s):
    import re
    return [t for t in re.findall(r"[a-z0-9]+", s.lower()) if len(t) >= 3]


# ── Rollup (the 5-check verdict) ────────────────────────────────────────────

def _build_rollup(rfq, manifest, forms, items):
    """Roll up to a single verdict: aligned (green) or N issues (red).

    issues is the operator-facing list — each entry is one short line that
    tells Mike exactly what to fix. We do not surface internal field names.
    """
    issues = []
    checks = {}

    # 1. Forms on disk — every required form has a non-zero file.
    missing_required = [f for f in forms if f["required"] and f["missing"]]
    forms_ok = not missing_required
    checks["forms_on_disk"] = forms_ok
    if not forms_ok:
        names = ", ".join(f["display_name"] for f in missing_required[:5])
        more = "" if len(missing_required) <= 5 else f" (+{len(missing_required)-5} more)"
        issues.append(f"Missing required forms: {names}{more}")

    # 2. QA passed
    fa = manifest.get("field_audit") or {}
    qa_passed = bool(isinstance(fa, dict) and fa.get("_qa_passed"))
    qa_run = isinstance(fa, dict) and "_qa_passed" in fa
    checks["qa_passed"] = qa_passed if qa_run else None
    if qa_run and not qa_passed:
        crit = (fa.get("_qa_summary") or {}).get("critical_issues") or []
        issues.append(f"Form QA failed — {len(crit)} critical issue(s)")

    # 3. Source validation
    sv = manifest.get("source_validation") or {}
    source_errors = list((sv or {}).get("errors") or [])
    source_ok = not source_errors
    checks["source_valid"] = source_ok
    if source_errors:
        issues.append(f"Source validation: {source_errors[0]}"
                      + (f" (+{len(source_errors)-1} more)" if len(source_errors) > 1 else ""))

    # 4. Buyer + agency + due-date present
    buyer_name = (rfq.get("requestor_name") or "").strip()
    buyer_email = (rfq.get("requestor_email") or rfq.get("original_sender") or "").strip()
    agency = (manifest.get("agency_name") or rfq.get("agency") or "").strip()
    due_date = (rfq.get("due_date") or "").strip()
    buyer_ok = bool(buyer_name and buyer_email and agency)
    checks["buyer_present"] = buyer_ok
    if not buyer_ok:
        miss = []
        if not buyer_name: miss.append("buyer name")
        if not buyer_email: miss.append("buyer email")
        if not agency: miss.append("agency")
        issues.append("Buyer info incomplete: " + ", ".join(miss))
    if buyer_ok and not due_date:
        # Due date is a softer warning — flag as a check fail but the message
        # is gentler since some RFQs legitimately don't have a posted due.
        checks["due_date_present"] = False
        issues.append("No due-date on RFQ — verify before sending")
    else:
        checks["due_date_present"] = bool(due_date)

    # 5. All items priced
    our_items = items["rows"]
    unpriced = [r for r in our_items if r["our_unit"] <= 0]
    items_priced = bool(our_items) and not unpriced
    checks["items_priced"] = items_priced
    if not our_items:
        issues.append("No line items on RFQ")
    elif unpriced:
        issues.append(f"{len(unpriced)} item(s) without a unit price")

    aligned = (forms_ok and (qa_passed or not qa_run) and source_ok
               and buyer_ok and items_priced)

    return {
        "aligned": aligned,
        "issues": issues,
        "checks": checks,
        "summary": {
            "forms_required": sum(1 for f in forms if f["required"]),
            "forms_present": sum(1 for f in forms if f["required"] and not f["missing"]),
            "qa_run": qa_run,
            "items_total": len(our_items),
            "items_priced": len(our_items) - len(unpriced) if our_items else 0,
        },
    }


# ── Tiny coercion helpers ───────────────────────────────────────────────────

def _to_int(v):
    try: return int(float(v or 0))
    except (TypeError, ValueError): return 0

def _to_float(v):
    try: return float(v or 0)
    except (TypeError, ValueError): return 0.0
