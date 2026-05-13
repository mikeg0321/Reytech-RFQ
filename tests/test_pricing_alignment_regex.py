"""Pin `extract_pdf_totals` regex against the false-positive shape that
blocked Mike P0 2026-05-12 rfq_8efe9fae.

Quote PDF renders `TAX (8.35%)  $724.12`. The old regex made `$`
optional and non-greedy-captured the FIRST `\d+\.\d{2}` after `TAX` —
which is `8.35` (the rate inside parens). The pricing-alignment QA
banner then reported `quote TAX $8.35 ≠ canonical $724.12` and
blocked the package from sending, even though the PDF was correct.

These pins catch the regression class — any future change that
re-loosens the `$` requirement or shortens the lookahead window
must continue to read the actual dollar amount, never the percent.
"""
from __future__ import annotations

import re
import pytest


# Re-derive the regex used by `pricing_alignment._find_money` to keep
# this test independent of internal-function exposure. If the
# production regex changes, update this string AND ensure all
# assertions below still pass.
_MONEY_TAIL = r".{0,80}?\$\s*([\d,]+\.\d{2})"


def _find(label_re: str, text: str):
    m = re.search(label_re + _MONEY_TAIL, text, re.IGNORECASE)
    return m.group(1) if m else None


# ── Mike P0 shape ─────────────────────────────────────────────────


def test_tax_rate_in_parens_does_not_leak_into_amount():
    """The exact shape from rfq_8efe9fae quote PDF."""
    text = "SUBTOTAL    $8,672.11\nTAX (8.35%)  $724.12\nTOTAL    $9,396.23\n"
    assert _find(r"\bTAX\b", text) == "724.12"
    assert _find(r"\bSUBTOTAL\b", text) == "8,672.11"
    assert _find(r"\bTOTAL\b(?!\s*PRICE)", text) == "9,396.23"


def test_tax_no_parens_works():
    """When tax has no rate annotation, capture still works."""
    text = "SUBTOTAL  $100.00\nTAX  $8.25\nTOTAL  $108.25\n"
    assert _find(r"\bTAX\b", text) == "8.25"


def test_tax_with_high_rate_in_parens():
    """A 10.25% rate (Los Angeles County, e.g.) — two-digit percent."""
    text = "SUBTOTAL  $1,000.00\nTAX (10.25%)  $102.50\nTOTAL  $1,102.50\n"
    assert _find(r"\bTAX\b", text) == "102.50"


def test_tax_label_with_three_decimal_rate():
    """Some agencies render rate with 3-decimal precision (8.350%)."""
    text = "SUBTOTAL  $8,672.11\nTAX (8.350%)  $724.12\nTOTAL  $9,396.23\n"
    assert _find(r"\bTAX\b", text) == "724.12"


def test_quote_with_total_price_header_not_confused_with_total_row():
    """The line-item table header contains 'TOTAL PRICE' which the
    `(?!\\s*PRICE)` negative lookahead excludes."""
    text = (
        "ITEM  DESCRIPTION  QTY  UNIT PRICE  TOTAL PRICE\n"
        "1  Widget  3  $10.00  $30.00\n"
        "SUBTOTAL  $30.00\n"
        "TAX (8.00%)  $2.40\n"
        "TOTAL  $32.40\n"
    )
    # TOTAL should hit the totals-row $32.40, not get tangled with the
    # table header.
    assert _find(r"\bTOTAL\b(?!\s*PRICE)", text) == "32.40"


def test_zero_tax_with_rate_zero_in_parens():
    """A tax-exempt quote renders TAX (0.00%) $0.00."""
    text = "SUBTOTAL  $100.00\nTAX (0.00%)  $0.00\nTOTAL  $100.00\n"
    assert _find(r"\bTAX\b", text) == "0.00"


# ── Direct call to production function (integration pin) ──────────


def test_extract_pdf_totals_uses_fixed_regex_via_module():
    """`extract_pdf_totals` lives in `pricing_alignment`. Confirm the
    in-module regex stays in lockstep with the test above by reading
    the source. If a future refactor splits the regex into a constant
    or a helper, update this test."""
    from pathlib import Path
    src_path = Path(__file__).parent.parent / "src" / "forms" / "pricing_alignment.py"
    src = src_path.read_text(encoding="utf-8")
    # The `\$?` (optional $) variant must NOT come back.
    assert r"\$?\s*([\d,]+\.\d{2})" not in src, (
        "extract_pdf_totals regex regressed to optional-$ form. This "
        "lets percent-in-parens labels leak into the captured amount. "
        "Mike P0 2026-05-12 rfq_8efe9fae was this exact bug."
    )
    # The fixed `\$` (required) variant MUST be present.
    assert r"\$\s*([\d,]+\.\d{2})" in src, (
        "extract_pdf_totals regex doesn't have the required-$ tail. "
        "Without it, money extraction is fragile against label-embedded "
        "numbers."
    )
