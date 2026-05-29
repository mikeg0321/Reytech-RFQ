"""Cross-document pricing alignment gate (Mike P0 2026-05-07
post-quote queue Q7 — "pricing has to be 100% truth").

The 2026-05-06 RFQ a5b09b56 incident exposed that every customer-
facing document in the package — Quote PDF, 704B, CalRecycle 74,
even the email body — computes its own totals from a separate code
path, with no enforcement that those totals agree. Mike's logs from
that session showed:

  ✓ 704B filled + signed — $514.72
  ...
  Quote R26Q40 generated: $492.58 total

…inside the SAME `/rfq/<rid>/generate-package` call. A 4.4% mismatch
on a buyer-facing artifact. Operator never saw it; it shipped silently.

This module is the substrate fix:

  1. **`compute_canonical_totals(rfq)`** is the single source of truth
     for per-row extensions, subtotal, tax, shipping, and total. Every
     downstream renderer must read these numbers, not recompute them.

  2. **`extract_pdf_totals(pdf_path, expected_form_id)`** parses the
     generated PDF and returns the totals it actually wrote. Different
     forms have different layouts; the helper knows the field-name /
     text-extraction patterns for each.

  3. **`check_alignment(rfq, generated_files)`** runs canonical vs.
     extracted for every required form, returns a list of blockers
     when any document's totals deviate by > $0.01 from canonical.

  4. **Per-row invariant**: every row must satisfy
     `qty × unit_price = extension` (within rounding). Catches
     row-level extension miscalculations that aggregate-only checks
     would miss.

The package generator at `routes_rfq_gen.py:generate_rfq_package`
calls `check_alignment(...)` post-fill and merges the blockers into
the existing completeness gate (mirrors the form_capacity registry
pattern from PR #801).

## Doctrine

Every code path that writes a price/total/extension to a buyer-facing
document MUST source from `compute_canonical_totals(rfq)`. No
private recomputation. Future PRs will lint for this — any
`sum(it.qty * it.price ...)` outside this module is a violation
once a third instance surfaces.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


# Tolerance for $-comparisons. Floating-point + per-row rounding can
# accumulate 1-2 cents across 40 items; > $0.01 difference is a real
# divergence the operator should see.
_DOLLAR_TOLERANCE = 0.01


def _safe_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    try:
        s = str(v).replace("$", "").replace(",", "").strip()
        if not s:
            return 0.0
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def compute_canonical_totals(rfq: Dict) -> Dict:
    """The single source of truth for an RFQ's price totals.

    Returns:
      {
        "line_extensions": [{"line_no", "qty", "unit_price",
                             "extension", "description"}, ...],
        "subtotal": float,        # sum of extensions, pre-tax/shipping
        "tax_rate": float,        # 0.0875 = 8.75%
        "tax": float,             # subtotal × tax_rate, rounded to cents
        "shipping": float,        # rfq.shipping_cost or 0
        "total": float,           # subtotal + tax + shipping
        "items_priced": int,      # count of rows where unit_price > 0
        "items_total": int,       # total row count
      }

    Rounding rule (matches quote_generator.py): per-row extension is
    `round(qty * unit_price, 2)`. Subtotal sums those rounded
    extensions. Tax is `round(subtotal * tax_rate, 2)`. This guarantees
    `subtotal == sum(line_extensions)` exactly (no off-by-one cents
    from un-rounded multiplication).
    """
    items = rfq.get("line_items") or rfq.get("items") or []
    line_extensions = []
    subtotal = 0.0
    items_priced = 0

    for idx, it in enumerate(items):
        if not isinstance(it, dict):
            continue
        qty = _safe_float(it.get("qty") or it.get("quantity") or 0)
        unit_price = _safe_float(
            it.get("price_per_unit")
            or it.get("unit_price")
            or it.get("bid_price")
            or 0
        )
        extension = round(qty * unit_price, 2)
        line_no = it.get("line_number") or (idx + 1)
        line_extensions.append({
            "line_no": line_no,
            "qty": qty,
            "unit_price": unit_price,
            "extension": extension,
            "description": (it.get("description") or "")[:120],
        })
        subtotal += extension
        if unit_price > 0:
            items_priced += 1

    subtotal = round(subtotal, 2)

    # Tax rate sources, in priority order. Per-RFQ override beats
    # institution lookup beats default. RFQ records carry tax_rate as
    # a fraction (0.0875), some legacy paths carry it as a percent
    # (8.75) — normalize.
    tax_rate_raw = (
        rfq.get("tax_rate")
        if rfq.get("tax_rate") is not None
        else rfq.get("sales_tax_rate", 0.0875)
    )
    try:
        tax_rate = float(tax_rate_raw)
    except (ValueError, TypeError):
        tax_rate = 0.0875
    if tax_rate > 1:
        tax_rate = tax_rate / 100.0

    tax_enabled = bool(rfq.get("tax_enabled", True))
    tax = round(subtotal * tax_rate, 2) if tax_enabled else 0.0

    shipping = _safe_float(rfq.get("shipping_cost") or rfq.get("shipping") or 0)

    total = round(subtotal + tax + shipping, 2)

    return {
        "line_extensions": line_extensions,
        "subtotal": subtotal,
        "tax_rate": tax_rate,
        "tax": tax,
        "shipping": shipping,
        "total": total,
        "items_priced": items_priced,
        "items_total": len(line_extensions),
    }


def extract_pdf_totals(pdf_path: str, form_id: str = "") -> Optional[Dict]:
    """Parse a generated PDF and return the totals it ACTUALLY wrote.

    Returns None when the PDF can't be read or has no extractable
    totals (e.g. a 703B that doesn't carry items). Returns:

      {
        "subtotal": float | None,
        "tax": float | None,
        "shipping": float | None,
        "total": float | None,
        "line_count": int,         # number of item rows detected
      }

    Extraction strategy: pdfplumber text + regex over the labeled
    rows ("SUBTOTAL", "TAX", "TOTAL"). Fragile in principle, but
    every form Reytech ships has a stable label layout. When a new
    form lands, add its label patterns here.
    """
    try:
        import pdfplumber
    except ImportError:
        log.debug("pdfplumber not available — skipping PDF totals extraction")
        return None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = "\n".join(
                (page.extract_text() or "") for page in pdf.pages
            )
    except Exception as e:
        log.debug("extract_pdf_totals: failed to read %s: %s", pdf_path, e)
        return None

    if not text.strip():
        return None

    def _find_money(label_re: str) -> Optional[float]:
        r"""Find a $ amount on the same line as `label_re`.

        🚨 2026-05-12 Mike P0 rfq_8efe9fae: the regex previously made
        `$` optional (`\$?`). For a quote PDF rendering `TAX (8.35%)
        $724.12`, the non-greedy `.{0,40}?` captured the FIRST number
        after `TAX` — which is `8.35` (the rate inside parens), NOT
        the real tax amount `$724.12`. Result: the pricing-alignment
        QA banner falsely reported `quote TAX $8.35 ≠ canonical
        $724.12` and blocked send — even though the PDF was correct.

        Require `$` so percent-in-parens labels don't leak.
        """
        m = re.search(
            label_re + r".{0,80}?\$\s*([\d,]+\.\d{2})",
            text,
            re.IGNORECASE,
        )
        if m:
            return _safe_float(m.group(1))
        return None

    subtotal = _find_money(r"\bSUBTOTAL\b")
    tax = _find_money(r"\bTAX\b")
    shipping = _find_money(r"\bSHIPPING\b")
    total = _find_money(r"\bTOTAL\b(?!\s*PRICE)")
    if total is None:
        # Quote PDF uses "TOTAL" near the bottom, but the table
        # header may also have "TOTAL PRICE" which we exclude
        # above. Some forms label it just "GRAND TOTAL".
        total = _find_money(r"\bGRAND\s+TOTAL\b")

    if subtotal is None and tax is None and total is None:
        # No price labels found — likely a header-only form
        # (703B's Bidder Information section). Not a divergence;
        # just skip alignment for this form.
        return None

    return {
        "subtotal": subtotal,
        "tax": tax,
        "shipping": shipping,
        "total": total,
        "line_count": len(re.findall(r"^\s*\d+\s+\w", text, re.MULTILINE)),
    }


def check_alignment(rfq: Dict, generated_files: List[Tuple[str, str, str]]) -> Dict:
    """Walk every generated document and assert its totals match the
    canonical RFQ totals.

    `generated_files` is a list of `(form_id, file_path, label)` tuples
    — e.g. `[("quote", "/data/output/123/Quote.pdf", "Reytech Quote"),
             ("704b",  "/data/output/123/704B.pdf",  "AMS 704B"),
             ...]`

    Returns:
      {
        "ok": bool,
        "canonical": <compute_canonical_totals output>,
        "blockers": [
          {
            "form_id", "file_path", "label",
            "field": "subtotal" | "tax" | "total" | "row_invariant",
            "canonical_value": float,
            "pdf_value": float,
            "diff": float,
            "message": "Quote PDF SUBTOTAL $452.95 ≠ canonical $452.93 (+$0.02)",
          },
          ...
        ],
        "warnings": [...],   # near-miss ($0.01 ≤ diff ≤ $0.03)
        "by_form": {form_id: {extracted, diffs}},
      }
    """
    canonical = compute_canonical_totals(rfq)
    blockers: List[Dict] = []
    warnings: List[Dict] = []
    by_form: Dict[str, Dict] = {}

    # Per-row invariant — independent of any PDF.
    for ext in canonical["line_extensions"]:
        recomputed = round(ext["qty"] * ext["unit_price"], 2)
        if abs(recomputed - ext["extension"]) > _DOLLAR_TOLERANCE:
            blockers.append({
                "form_id": "canonical",
                "file_path": "",
                "label": "Canonical row math",
                "field": "row_invariant",
                "canonical_value": recomputed,
                "pdf_value": ext["extension"],
                "diff": ext["extension"] - recomputed,
                "line_no": ext["line_no"],
                "message": (
                    f"Row {ext['line_no']}: qty {ext['qty']} × unit "
                    f"${ext['unit_price']:.2f} should equal "
                    f"${recomputed:.2f}, but canonical computed "
                    f"${ext['extension']:.2f}. Per-row math integrity broken."
                ),
            })

    # Parse each unique file ONCE. The route lists the same physical file
    # under multiple form-ids (the CCHCS bid package is registered as `bidpkg`
    # AND as its bidpkg-covered aliases calrecycle74 / sellers_permit / dvbe843
    # — all pointing at <sol>_BidPackage_Reytech.pdf), so without this the
    # 15-page bid package was pdfplumber-parsed up to 4×. extract_pdf_totals
    # ignores form_id, so caching by file_path is result-identical.
    _totals_by_path: Dict[str, Optional[Dict]] = {}
    for form_id, file_path, label in (generated_files or []):
        if file_path in _totals_by_path:
            extracted = _totals_by_path[file_path]
        else:
            extracted = extract_pdf_totals(file_path, form_id=form_id)
            _totals_by_path[file_path] = extracted
        if extracted is None:
            by_form[form_id] = {"extracted": None, "skipped": True}
            continue
        diffs = []
        for field in ("subtotal", "tax", "total"):
            cv = canonical.get(field)
            pv = extracted.get(field)
            if cv is None or pv is None:
                continue
            d = pv - cv
            if abs(d) > _DOLLAR_TOLERANCE:
                row = {
                    "form_id": form_id,
                    "file_path": file_path,
                    "label": label,
                    "field": field,
                    "canonical_value": cv,
                    "pdf_value": pv,
                    "diff": d,
                    "message": (
                        f"{label} ({form_id}) {field.upper()} "
                        f"${pv:.2f} ≠ canonical ${cv:.2f} "
                        f"({'+' if d > 0 else ''}${d:.2f})"
                    ),
                }
                diffs.append(row)
                if abs(d) > 0.03:
                    blockers.append(row)
                else:
                    warnings.append(row)
        by_form[form_id] = {
            "extracted": extracted,
            "diffs": diffs,
            "skipped": False,
        }

    return {
        "ok": not blockers,
        "canonical": canonical,
        "blockers": blockers,
        "warnings": warnings,
        "by_form": by_form,
    }
