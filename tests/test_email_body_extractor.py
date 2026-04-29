"""Tests for src/forms/email_body_extractor — PR-B body-text extractor.

Per project_email_body_rfq_parser_gap.md, this is the production-quality
extractor that runs when a buyer pastes the RFQ into the email body
without a parseable attachment. Locks the kill criterion: if these
tests start regressing, the extractor was producing garbage that the
operator was overriding — flip the flag off rather than silent-corrupt.

Coverage:
  - HTML stripping + signature stripping
  - 5 pattern stages (tabular_full, bullet, please_quote, inline_qty_x_desc,
    tabular_simple)
  - Negative cases: signature blocks, headers, footer disclaimers don't
    leak into items
  - End-to-end: realistic Keith Alsing-style email body produces parsed items
"""
from __future__ import annotations

import pytest


def _ex():
    from src.forms.email_body_extractor import extract_items
    return extract_items


def _pp():
    from src.forms.email_body_extractor import preprocess
    return preprocess


# ── Preprocessing ───────────────────────────────────────────────────


def test_strip_html_removes_tags():
    from src.forms.email_body_extractor import strip_html
    out = strip_html("<p>Hello <b>world</b></p>")
    assert "<" not in out
    assert "Hello" in out and "world" in out


def test_strip_html_drops_style_and_script():
    from src.forms.email_body_extractor import strip_html
    out = strip_html("<style>body{color:red}</style>Hello<script>alert(1)</script>World")
    assert "color:red" not in out
    assert "alert" not in out
    assert "Hello" in out and "World" in out


def test_strip_html_decodes_entities():
    from src.forms.email_body_extractor import strip_html
    out = strip_html("Tom&#39;s &amp; Jerry&nbsp;widgets")
    assert "Tom's" in out
    assert "&" in out and "amp" not in out


def test_strip_signature_cuts_at_dash_dash():
    from src.forms.email_body_extractor import strip_signature
    body = "Need 5 widgets.\n--\nKeith Alsing\nCalVet\nphone: 555-1234"
    out = strip_signature(body)
    assert "Keith" not in out
    assert "Need 5 widgets" in out


def test_strip_signature_cuts_at_sent_from_iphone():
    from src.forms.email_body_extractor import strip_signature
    body = "Please quote 3 widgets\n\nSent from my iPhone"
    out = strip_signature(body)
    assert "iPhone" not in out
    assert "Please quote" in out


def test_strip_signature_cuts_at_confidentiality_notice():
    from src.forms.email_body_extractor import strip_signature
    body = "5 x widgets\n\nCONFIDENTIALITY NOTICE: this email may contain..."
    out = strip_signature(body)
    assert "CONFIDENTIALITY" not in out


# ── Pattern stages ──────────────────────────────────────────────────


def test_tabular_full_extracts_calvet_style():
    """LINE_NO QTY UOM PART# DESC — strongest signal."""
    body = """
LINE NO.  QTY/UNIT  U OF M  PART #    DESCRIPTION
1         20        CS      MCK-123   BANDAGE ELASTIC 6 INCH
2         50        EA      MCK-456   GAUZE PAD 4X4 STERILE
"""
    items = _ex()(body)
    assert len(items) == 2
    assert items[0]["qty"] == 20
    assert items[0]["uom"] == "CS"
    assert items[0]["mfg_number"] == "MCK-123"
    assert "BANDAGE" in items[0]["description"].upper()
    assert all(i["source"] == "email_body_regex" for i in items)
    assert all(i["needs_review"] is True for i in items)


def test_bullet_dash_list():
    body = """
Hi Reytech, please price the following:

- 5 widgets type A
- 12 gizmos heavy duty
- 100 fasteners stainless steel

Thanks
"""
    items = _ex()(body)
    assert len(items) == 3
    qtys = sorted(i["qty"] for i in items)
    assert qtys == [5, 12, 100]


def test_please_quote_pattern():
    body = """
Please quote 3 of widget assembly XL
also need pricing on 25 fasteners m6
"""
    items = _ex()(body)
    assert len(items) >= 1
    assert any(i["qty"] == 3 for i in items)


def test_inline_qty_x_desc():
    body = "We need 5 x heavy duty mounting brackets for our project."
    items = _ex()(body)
    assert len(items) >= 1
    assert items[0]["qty"] == 5


def test_tabular_simple_no_uom():
    body = """
1   20   ELASTIC BANDAGE 6 INCH
2   50   GAUZE PAD STERILE
"""
    items = _ex()(body)
    assert len(items) == 2
    assert items[0]["qty"] == 20
    assert items[0]["uom"] == "EA"
    assert items[1]["qty"] == 50


# ── Stage precedence ────────────────────────────────────────────────


def test_tabular_full_wins_over_inline():
    """When both patterns could match, tabular_full takes precedence so
    we don't double-count one item."""
    body = """
1  10  EA  PN-A  WIDGET A
"""
    items = _ex()(body)
    assert len(items) == 1
    assert items[0]["mfg_number"] == "PN-A"
    assert items[0]["qty"] == 10


# ── Negative cases (filter out junk) ────────────────────────────────


def test_short_body_returns_empty():
    assert _ex()("") == []
    assert _ex()("hi") == []


def test_signature_block_does_not_create_items():
    """The phone number 'Phone: 555-123-4567' must not look like
    'qty=555 description=123-4567'."""
    body = """
Hi Reytech,

Just touching base.

Keith Alsing
Veterans Home
Phone: 555-123-4567
Email: keith@calvet.ca.gov
"""
    items = _ex()(body)
    # Should find no real items in this signature-heavy email
    assert items == []


def test_header_row_does_not_create_item():
    body = """
LINE NO.  QTY  DESCRIPTION
"""
    items = _ex()(body)
    assert items == []


def test_html_stripped_before_extraction():
    body = """
<html><body>
<p>Please quote:</p>
<ul>
<li>5 widgets</li>
<li>10 gizmos</li>
</ul>
</body></html>
"""
    items = _ex()(body)
    assert len(items) == 2
    qtys = sorted(i["qty"] for i in items)
    assert qtys == [5, 10]


def test_negative_token_filter_skips_disclaimer_lines():
    """Lines containing 'page 1 of 5' should not match tabular patterns."""
    body = """
Page 1 of 5
LINE NO QTY UOM PART # DESCRIPTION
1  10  EA  PN-A  WIDGET A
"""
    items = _ex()(body)
    assert len(items) == 1
    assert items[0]["qty"] == 10
    assert "Page" not in items[0]["description"]


def test_signature_strip_prevents_phone_false_positive():
    """End-to-end: signature with 'Phone: 555-1234' must not produce items."""
    body = """
Need pricing on 5 widgets.

--
Keith Alsing
Phone: 555-1234
Fax: 555-5678
"""
    items = _ex()(body)
    # Only the legit "5 widgets" should land
    assert len(items) == 1
    assert items[0]["qty"] == 5
    assert "widget" in items[0]["description"].lower()


# ── Item shape contract ─────────────────────────────────────────────


def test_extracted_items_have_required_fields():
    body = "Please quote 3 widgets"
    items = _ex()(body)
    assert items, "expected at least 1 item"
    item = items[0]
    for k in ("item_number", "qty", "uom", "mfg_number", "description",
              "row_index", "pricing", "source", "needs_review"):
        assert k in item, f"missing required field {k!r}"
    assert item["source"] == "email_body_regex"
    assert item["needs_review"] is True


def test_qty_validated_in_range():
    """qty=0 or qty>99999 should be rejected (sanity)."""
    body = """
1  0    EA  PN-Z  ZERO QTY
2  100000  EA  PN-Y  TOO MUCH
3  5    EA  PN-A  REAL ITEM
"""
    items = _ex()(body)
    qtys = [i["qty"] for i in items]
    assert 0 not in qtys
    assert 100000 not in qtys
    assert 5 in qtys


# ── Realistic Keith Alsing-style fixture ────────────────────────────


def test_keith_alsing_style_body_produces_items():
    """Synthetic but representative of the body-only RFQ that surfaced
    in 2026-04-29's pc_a391db8f / rfq_7813c4e1 incident. Fixture is
    hand-crafted because we don't have the real prod email body in repo."""
    body = """
Hello Reytech,

I need a quick price check on the following items for our facility:

LINE NO  QTY  U OF M  PART #         DESCRIPTION
1        24   CS      MCK-1001682    INCONTINENCE BRIEF LARGE
2        12   CS      MCK-1001683    INCONTINENCE BRIEF MEDIUM
3        50   EA      W12919         GAUZE PAD 4X4 STERILE
4        10   BX      FN4368         ALCOHOL PREP PADS

Please respond by EOD Friday. Delivery to Veterans Home of California, Yountville.

Thanks,
Keith Alsing
CalVet Yountville
keith.alsing@calvet.ca.gov
Phone: 707-555-0100

CONFIDENTIALITY NOTICE: This email is intended only for the named recipient.
"""
    items = _ex()(body)
    assert len(items) == 4, f"expected 4, got {len(items)}: {[i['description'] for i in items]}"
    qtys = [i["qty"] for i in items]
    assert qtys == [24, 12, 50, 10]
    descs = [i["description"].upper() for i in items]
    assert any("INCONTINENCE" in d for d in descs)
    assert any("GAUZE" in d for d in descs)
    assert any("ALCOHOL" in d for d in descs)
    # Signature block must NOT have leaked
    assert all("707" not in i["mfg_number"] for i in items)
    assert all("CONFIDENTIALITY" not in i["description"].upper() for i in items)


def test_bullet_style_prose_request():
    """Buyer who didn't use a table — common informal RFQ shape."""
    body = """
Hi Reytech,

For our upcoming order, please quote:

* 100 disposable gloves nitrile
* 50 sanitizer bottles 8oz
* 25 mask boxes surgical

Need this by next Thursday. Thanks!
"""
    items = _ex()(body)
    assert len(items) == 3
    qtys = sorted(i["qty"] for i in items)
    assert qtys == [25, 50, 100]
