"""Source-level guard for the PC-vs-RFQ filename heuristic.

2026-05-05 P0 incident (Sommony Pech / CCHCS, solicitation 10838974):
an attachment named "RFQ - Informal Competitive - Attachment 1-_
10838974.pdf" was auto-classified as a PC by `dashboard.py:_is_pc_filename`
because that function falls back to a PDF-form-field heuristic that
matches `{Requestor, EXTENSIONRow1, ...}` — field names many CCHCS-
flavored RFQs share with AMS 704 templates. Once the file landed on the
"PC candidate" list, the email-poller path in dashboard.py created a PC
directly, bypassing `src/core/request_classifier.classify_request`
entirely.

The fix adds an early hard-reject for filenames that START with
RFQ / Solicitation / Informal Competitive (word-anchored) before the
field-set heuristic gets a chance.

`_is_pc_filename` is defined as a nested closure inside the email-poller
flow in dashboard.py — not import-friendly. So this test file does two
things:

1. **Source-level guard** — parse dashboard.py and assert the rejection
   regex + intent comment are still present. A future "tighten the
   regex" PR that drops the rejection fails fast.

2. **Behavioral test** — re-compile the same regex inline and verify it
   correctly classifies the exact filename from Mike's incident plus
   the standard PC and RFQ shapes.
"""
from __future__ import annotations

import re
from pathlib import Path

DASHBOARD_PATH = Path("src/api/dashboard.py")


def test_pc_filename_dashboard_rejects_rfq_prefix():
    """The rejection block must be in dashboard.py, ahead of the
    AMS/704-positive checks. A future refactor that moves it after the
    positive checks (or removes it) would re-open the bug."""
    src = DASHBOARD_PATH.read_text(encoding="utf-8")
    # The intent comment is the durable anchor — pin it so the why-context
    # survives future cleanups.
    assert "10838974" in src, (
        "incident reference (CCHCS sol 10838974) must remain in the "
        "_is_pc_filename block so future readers know why the rfq-prefix "
        "rejection exists. Removing it loses the institutional memory."
    )
    # The rejection regex itself.
    assert re.search(
        r"r[\"']\^\(rfq\|solicitation\|informal\\s\+competitive\)\(\?\!\[a-z\]\)",
        src,
    ), (
        "_is_pc_filename must hard-reject filenames starting with rfq / "
        "solicitation / informal competitive before the field-name "
        "heuristic. See 2026-05-05 P0 incident in the comment."
    )


def test_pc_filename_rejection_regex_behavior():
    """Re-compile the same regex and verify per-filename behavior.

    Pinning here means a future PR that adjusts the regex (e.g. to allow
    a new RFQ marker variant) MUST update this test, which forces the
    author to think about the AMS 704 carve-out."""
    rfq_reject_re = re.compile(r"^(rfq|solicitation|informal\s+competitive)(?![a-z])")
    ams_prefix = ("ams 704", "ams_704", "ams704")

    def filename_should_route_pc(filename: str) -> bool:
        """Mirror of the early-rejection block in dashboard.py:_is_pc_filename.
        Returns False when the early-reject fires; True when it allows
        the filename to fall through to the rest of the heuristic."""
        bn = filename.lower()
        if not bn.startswith(ams_prefix):
            if rfq_reject_re.match(bn):
                return False  # explicit RFQ rejection
        return True  # falls through (may still be rejected by other checks)

    # Mike's exact incident filename — must be rejected.
    assert not filename_should_route_pc(
        "RFQ - Informal Competitive - Attachment 1-_ 10838974.pdf"
    ), "Mike's exact incident — rfq prefix must reject"

    # Variants that should also reject.
    for f in [
        "RFQ_10838974.pdf",
        "rfq-quote-2026.pdf",
        "Solicitation 12345 CCHCS.pdf",
        "SOLICITATION-2026-001.pdf",
        "Informal Competitive Quote.pdf",
        "informal competitive bid.pdf",
    ]:
        assert not filename_should_route_pc(f), f"should reject: {f!r}"

    # Standard AMS 704 PC names — must NOT be rejected.
    for f in [
        "AMS 704 Price Check Worksheet- RT Supplies 4.3.26.pdf",
        "AMS 704 - Heel Donut - 04.29.26.pdf",
        "ams_704_price_check.pdf",
        "ams704_quote_worksheet.pdf",
    ]:
        assert filename_should_route_pc(f), f"should NOT reject: {f!r}"

    # AMS 704 prefix wins even when "rfq" appears mid-filename. This is
    # the carve-out the regex's `not bn.startswith` check protects.
    assert filename_should_route_pc(
        "AMS 704 - RFQ Response Worksheet.pdf"
    ), "AMS 704 prefix must override mid-filename 'rfq' substring"

    # Edge case: "rfq" embedded mid-name without AMS 704 prefix should
    # NOT be rejected — could be operator naming convention. The regex
    # is anchored to `^` so this passes the early reject and falls
    # through to other checks.
    assert filename_should_route_pc(
        "Quote with rfq in the middle.pdf"
    ), "non-prefix 'rfq' substring should not trigger early reject"


def test_pc_filename_block_survives_above_field_heuristic():
    """Pin ordering: the rfq-prefix reject must run BEFORE the existing
    `704b/703b/bid package/...` reject AND BEFORE the field-name
    heuristic at the bottom. If a future PR re-orders, this test
    catches it."""
    src = DASHBOARD_PATH.read_text(encoding="utf-8")
    rfq_reject_idx = src.find('r"^(rfq|solicitation|informal\\s+competitive)(?![a-z])')
    if rfq_reject_idx < 0:
        rfq_reject_idx = src.find("r'^(rfq|solicitation|informal\\s+competitive)(?![a-z])")
    field_heuristic_idx = src.find('{"COMPANY NAME", "Requestor", "PRICE PER UNITRow1", "EXTENSIONRow1"}')
    assert rfq_reject_idx > 0, "rfq-reject regex not found"
    assert field_heuristic_idx > 0, "field-heuristic set literal not found"
    assert rfq_reject_idx < field_heuristic_idx, (
        "rfq-prefix rejection must run BEFORE the field-set heuristic — "
        "otherwise the heuristic still false-positives on CCHCS-flavored "
        "RFQs whose form fields overlap with AMS 704 templates."
    )
