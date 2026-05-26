"""Regression: legacy RFQ detail tax label renders as percent, not fraction.

Bug shape (Handoff A 2026-05-25): a legacy RFQ whose stored `tax_rate`
field had been written as a FRACTION (0.0775) instead of the legacy-
canonical PERCENT form (7.75) rendered as "Tax (0.077%)" on
`/rfq/<id>` because `rfq_detail.html:768` formatted the value with
`'%.3f'|format(r.tax_rate|float)` — no multiply-by-100, no
normalization.

The legacy convention for `tax_rate` storage is PERCENT (see
`rfq_detail.html:2747` JS comment + the input bounds `min=0 max=15
step=0.25`). Some Spine-side write paths contaminated the field with
the fraction form. The fix is a defensive normalize in the template:
treat <=1 as fraction (multiply by 100), treat >1 as percent (use
as-is). Same heuristic `src/forms/pricing_alignment.py:144` uses.

This test pins both storage conventions against the canonical display
of "7.75%" so a regression to either side fails the build.
"""
from __future__ import annotations

import json
import os
import re


def _seed(temp_data_dir, sample_rfq, tax_rate):
    rfq = dict(sample_rfq)
    rfq["tax_rate"] = tax_rate
    path = os.path.join(temp_data_dir, "rfqs.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({rfq["id"]: rfq}, f)
    return rfq["id"]


def _tax_label(html: str) -> str | None:
    """Return the percent string inside the right-rail Tax label.

    The relevant template fragment is the right-column Tax KPI:
      <div ...>Tax <span ...>(<percent>%)</span></div><div id="taxt">
    Anchor on the `id="taxt"` sibling to disambiguate from any other
    'Tax' string elsewhere on the page.
    """
    m = re.search(r'>Tax\s*<span[^>]*>\(([^)]+)%\)</span>[^<]*</div>\s*<div id="taxt"', html)
    return m.group(1) if m else None


def test_tax_label_renders_percent_when_stored_as_fraction(
    client, temp_data_dir, sample_rfq
):
    """tax_rate=0.0775 (the bug case — fraction contamination) must
    still render as "7.75" in the label. Before the fix this rendered
    as "0.077", invisible to the operator who expected percent."""
    rid = _seed(temp_data_dir, sample_rfq, 0.0775)
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    label = _tax_label(resp.get_data(as_text=True))
    assert label == "7.75", (
        f"Tax label should render '7.75' for stored fraction 0.0775, "
        f"got {label!r}. Regression to the 'Tax (0.077%)' bug — "
        f"the template must normalize <=1 values as fractions."
    )


def test_tax_label_renders_percent_when_stored_as_percent(
    client, temp_data_dir, sample_rfq
):
    """tax_rate=7.75 (legacy-canonical — percent) must render as
    "7.75" in the label. Boundary case ensures the fix didn't break
    the existing happy path."""
    rid = _seed(temp_data_dir, sample_rfq, 7.75)
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    label = _tax_label(resp.get_data(as_text=True))
    assert label == "7.75", (
        f"Tax label should render '7.75' for stored percent 7.75, "
        f"got {label!r}. Fix must NOT double-multiply already-percent values."
    )


def test_tax_label_omitted_when_tax_rate_zero(
    client, temp_data_dir, sample_rfq
):
    """tax_rate=0 falsy → the parenthesized percent span is omitted
    by the outer `{% if r.get('tax_rate') %}` guard. Just the bare
    "Tax" label shows, no parenthesized value."""
    rid = _seed(temp_data_dir, sample_rfq, 0)
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    label = _tax_label(resp.get_data(as_text=True))
    assert label is None, (
        f"Tax label parenthesized percent should be OMITTED when "
        f"tax_rate is 0, got {label!r}"
    )


def test_tax_label_handles_decimal_fraction_8_375_percent(
    client, temp_data_dir, sample_rfq
):
    """An 8.375% CA tax rate stored as fraction (0.08375) must render
    as "8.38" — two decimal places, multiplied by 100. Covers the
    common case of facilities with district add-ons."""
    rid = _seed(temp_data_dir, sample_rfq, 0.08375)
    resp = client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    label = _tax_label(resp.get_data(as_text=True))
    assert label == "8.38", (
        f"Expected '8.38' for fraction 0.08375 (rounded to 2dp), "
        f"got {label!r}"
    )
