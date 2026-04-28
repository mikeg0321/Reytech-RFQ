"""Tests for the canonical-prefix-aware PO extraction (PR #636).

Closes the bug PR #635's po_prefix card surfaced: 57.3% of prod
POs were "unidentified" because every parse regex used `(\\d{7,13})`
(digits only). For CalVet's `8955-NNNNNNNN` and DSH's
`4440-NNNNNNN`, the regex split at the dash and either picked the
short prefix or the long tail — losing the canonical PO format
permanently.

These tests:
  1. Lock the shared `extract_canonical_po(text)` helper so a
     future "simplify" of the regex can't silently regress.
  2. Lock that all three agency formats survive every extraction
     site:
       - po_email_v2.extract_po_numbers (V2 inbound matching)
       - email_poller._parse_po_pdf via po_email_v2 helper (PDF text)
       - email_poller.is_purchase_order_email (subject/body)
  3. Lock the boot-time CalVet backfill: bare-numeric po_numbers
     on Veterans Home rows get the `8955-` prefix prepended.
"""
from __future__ import annotations

from datetime import datetime

import pytest


# ── Shared canonical extractor ──────────────────────────────────────────


@pytest.mark.parametrize("text,expected", [
    # Canonical CalVet — full prefix preserved
    ("PO# 8955-0000044935",          "8955-0000044935"),
    ("Purchase Order: 8955-0000067018", "8955-0000067018"),
    # CalVet PDF header noise — date + zeros + PO. Must pick the PO.
    ("00015 02/19/2026 00000000 8955-0000044935", "8955-0000044935"),
    # CCHCS — pure digits, prefix matches 4500
    ("PO 4500750017",                 "4500750017"),
    ("4500123456",                    "4500123456"),
    # DSH — dashed prefix
    ("4440-1234567",                  "4440-1234567"),
    # No PO at all
    ("",                              ""),
    ("nothing here",                  ""),
    # Multiple canonical POs — longest wins
    ("contact 4500750017 or 8955-0000044935", "8955-0000044935"),
    # Generic fallback — no canonical match, longest digit run wins
    ("Order# 0000044935",             "0000044935"),
])
def test_extract_canonical_po(text, expected):
    from src.core.order_dal import extract_canonical_po
    assert extract_canonical_po(text) == expected


def test_extract_canonical_po_prefers_canonical_over_date():
    """A PDF header line `00015 02/19/2026 00000000 8955-0000044935`
    used to lose the PO — the regex matched all 4 numeric runs and
    the caller picked the LAST one (the bare tail). Now the
    canonical-prefixed token wins regardless of position."""
    from src.core.order_dal import extract_canonical_po
    # PO appears FIRST in this text, but date and zeros come after.
    text1 = "8955-0000044935 02/19/2026 00000000"
    # PO appears LAST.
    text2 = "02/19/2026 00000000 8955-0000044935"
    # Both should return the canonical PO, not a date or zeros.
    assert extract_canonical_po(text1) == "8955-0000044935"
    assert extract_canonical_po(text2) == "8955-0000044935"


def test_extract_canonical_po_skips_short_runs():
    """Avoid matching a date `2026` or a line number `15` as a PO."""
    from src.core.order_dal import extract_canonical_po
    assert extract_canonical_po("Page 15 of 20, 2026 forms") == ""


# ── po_email_v2.extract_po_numbers (V2 inbound matching) ───────────────


def test_v2_extract_po_numbers_captures_dashed_prefix():
    """Pre-PR #636 the V2 patterns used `(\\d{4,12})` — digits only.
    For input `PO# 8955-0000044935` the match would split into
    `8955` + `0000044935` and neither would match an existing
    order with the canonical full PO."""
    from src.core.po_email_v2 import extract_po_numbers
    pos = extract_po_numbers("PO# 8955-0000044935")
    assert "8955-0000044935" in pos


def test_v2_extract_po_numbers_handles_all_three_agencies():
    from src.core.po_email_v2 import extract_po_numbers
    text = (
        "From: cdcr-buyer@ca.gov\n"
        "PO 4500750017 from CCHCS\n"
        "Purchase Order: 8955-0000044935 from CalVet\n"
        "P.O. 4440-1234567 from DSH\n"
    )
    pos = extract_po_numbers(text)
    assert "4500750017" in pos
    assert "8955-0000044935" in pos
    assert "4440-1234567" in pos


# ── _parse_po_pdf (via canonical extractor) ─────────────────────────────


def test_parse_po_pdf_canonical_picks_calvet_prefix():
    """STD-65 PDF text has 'PURCHASE ORDER NUMBER' header followed
    by a line containing date + zeros + the actual PO. The bug
    was picking the LAST numeric token (the bare tail). Now the
    canonical 8955- match wins."""
    from src.core.order_dal import extract_canonical_po
    pdf_header_line = "00015 02/19/2026 00000000 8955-0000044935"
    assert extract_canonical_po(pdf_header_line) == "8955-0000044935"


def test_parse_po_pdf_canonical_handles_cchcs():
    from src.core.order_dal import extract_canonical_po
    pdf_header_line = "00015 02/19/2026 00000000 4500750017"
    assert extract_canonical_po(pdf_header_line) == "4500750017"


def test_parse_po_pdf_canonical_handles_dsh():
    from src.core.order_dal import extract_canonical_po
    pdf_header_line = "00015 02/19/2026 00000000 4440-1234567"
    assert extract_canonical_po(pdf_header_line) == "4440-1234567"


# ── is_purchase_order_email subject/body extraction ─────────────────────


def test_po_email_subject_captures_calvet_prefix():
    """Pattern `po\\s*distribution\\s*:?\\s*(\\d+)` used to capture
    digits only, splitting CalVet's dashed prefix at the wrong
    place. Updated pattern preserves it."""
    from src.agents.email_poller import is_purchase_order_email
    detect = is_purchase_order_email(
        subject="PO Distribution: 8955-0000044935, 12345678, BARSTOW, REYTECH",
        body="Award notification — see attached",
        sender="<buyer@calvet.ca.gov>",
        pdf_names=[],
    )
    assert detect is not None
    assert detect.get("po_number") == "8955-0000044935"


def test_po_email_subject_captures_dsh_prefix():
    from src.agents.email_poller import is_purchase_order_email
    detect = is_purchase_order_email(
        subject="Purchase Order 4440-1234567 attached",
        body="Award PO see attached document",
        sender="<buyer@dsh.ca.gov>",
        pdf_names=[],
    )
    assert detect is not None
    assert detect.get("po_number") == "4440-1234567"


def test_po_email_subject_still_captures_cchcs():
    """Regression check: don't break CCHCS while fixing CalVet/DSH."""
    from src.agents.email_poller import is_purchase_order_email
    detect = is_purchase_order_email(
        subject="PO 4500750017 from CCHCS",
        body="Purchase order award notification",
        sender="<buyer@cdcr.ca.gov>",
        pdf_names=[],
    )
    assert detect is not None
    assert detect.get("po_number") == "4500750017"


# ── Boot-time CalVet backfill ───────────────────────────────────────────


def _conn():
    from src.core.db import get_db
    return get_db()


def _wipe(conn):
    for tbl in ("orders", "quotes"):
        try:
            conn.execute(f"DELETE FROM {tbl}")
        except Exception:
            pass
    try:
        conn.execute("DROP INDEX IF EXISTS idx_orders_po_quote")
    except Exception:
        pass
    conn.commit()


def _seed_order(conn, *, order_id, po_number="", quote_number="",
                institution="", agency="", is_test=0):
    when = datetime.now().isoformat()
    conn.execute("""
        INSERT INTO orders
          (id, quote_number, po_number, agency, institution,
           total, status, items, created_at, updated_at, is_test)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (order_id, quote_number, po_number, agency, institution,
          100.0, "open", "[]", when, when, is_test))


def test_backfill_prepends_8955_to_veterans_home_bare_numeric():
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        # 3 CalVet rows with bare numeric POs — the bug
        _seed_order(c, order_id="cv1", po_number="0000067018",
                    quote_number="Q1",
                    institution="Veterans Home of California - Barstow")
        _seed_order(c, order_id="cv2", po_number="0000053217",
                    quote_number="Q2",
                    institution="Veterans Home of California - Yountville")
        _seed_order(c, order_id="cv3", po_number="0000051992",
                    quote_number="Q3",
                    institution="Veterans Home of California - Fresno")
        # 1 already-correct CalVet row — must NOT double-prefix
        _seed_order(c, order_id="cv_ok", po_number="8955-0000044935",
                    quote_number="Q4",
                    institution="Veterans Home of California - Chula Vista")
        # 1 CCHCS row with the 4500 prefix — must NOT touch
        _seed_order(c, order_id="cc1", po_number="4500750017",
                    quote_number="Q5",
                    institution="Mule Creek State Prison")
        # 1 DSH row with the 4440- prefix — must NOT touch
        _seed_order(c, order_id="dsh1", po_number="4440-1234567",
                    quote_number="Q6",
                    institution="DSH Atascadero")
        c.commit()

    _migrate_columns()

    with _conn() as c:
        rows = c.execute(
            "SELECT id, po_number FROM orders ORDER BY id"
        ).fetchall()
    by_id = {r["id"]: r["po_number"] for r in rows}

    # Backfilled
    assert by_id["cv1"] == "8955-0000067018"
    assert by_id["cv2"] == "8955-0000053217"
    assert by_id["cv3"] == "8955-0000051992"
    # Untouched
    assert by_id["cv_ok"] == "8955-0000044935"
    assert by_id["cc1"] == "4500750017"
    assert by_id["dsh1"] == "4440-1234567"


def test_backfill_is_idempotent():
    """The backfill runs every boot. Re-running on already-prefixed
    rows must NOT match (they already start with 8955-)."""
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="cv1", po_number="0000067018",
                    quote_number="Q1",
                    institution="Veterans Home of California - Barstow")
        c.commit()
    _migrate_columns()
    _migrate_columns()
    _migrate_columns()
    with _conn() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id = 'cv1'"
        ).fetchone()
    # Single prefix — not '8955-8955-…'
    assert row["po_number"] == "8955-0000067018"


def test_backfill_skips_non_calvet_facilities():
    """Even with a bare numeric po_number, a non-Veterans-Home
    institution must not get the CalVet prefix prepended. The
    institution check is the safety guard."""
    from src.core.db import _migrate_columns
    with _conn() as c:
        _wipe(c)
        _seed_order(c, order_id="cdcr1", po_number="0000067018",
                    quote_number="QC",
                    institution="CDCR Headquarters")
        c.commit()
    _migrate_columns()
    with _conn() as c:
        row = c.execute(
            "SELECT po_number FROM orders WHERE id = 'cdcr1'"
        ).fetchone()
    # Untouched — institution isn't Veterans Home
    assert row["po_number"] == "0000067018"


# ── End-to-end: po_prefix card flips after backfill ────────────────────


def test_po_prefix_card_calvet_count_increases_after_backfill():
    """Lock the cause-and-effect: po_prefix card was the diagnostic;
    this backfill is the cure. CalVet bucket should grow by N
    after backfill, and unidentified should shrink by N."""
    from src.core.db import _migrate_columns
    from src.api.modules.routes_health import _build_po_prefix_card

    with _conn() as c:
        _wipe(c)
        for i in range(5):
            _seed_order(c, order_id=f"cv{i}",
                        po_number=f"00000{i:05d}",
                        quote_number=f"Q{i}",
                        institution="Veterans Home of California - Barstow")
        c.commit()

    pre = _build_po_prefix_card()
    assert pre["unidentified"] == 5
    assert pre["by_prefix"]["CalVet"] == 0

    _migrate_columns()

    post = _build_po_prefix_card()
    assert post["unidentified"] == 0
    assert post["by_prefix"]["CalVet"] == 5
