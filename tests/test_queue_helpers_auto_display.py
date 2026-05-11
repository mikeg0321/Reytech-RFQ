"""AUTO_<hex> queue display fallback to attachment filename.

Surface #17 follow-on (2026-05-05). PR #727 wired the cascade at *ingest*
(`_attachment_filename_title` is now step 4 in the PC/RFQ name resolver).
But records ingested before that fix still hold `pc_number = "AUTO_db670ad9"`
on disk. Fixing them at write-time means a backfill; we get most of the
operator value with a read-side override on the queue list.

`normalize_queue_item` is the chokepoint that all queue rows pass through —
adding the override there means PC and RFQ queues both light up at once.

Guard rails this test pins:
1. AUTO_* with `source_pdf` → filename title replaces the hex
2. AUTO_* with NO source_pdf → AUTO_* stays (don't display "(blank)")
3. AUTO_* with hash-shaped filename → AUTO_* stays (no regression to worse)
4. Real pc_number / rfq_number → unchanged
5. Same behavior for PC and RFQ branches (no per-queue-type drift)
6. URL routing is independent of the display override (uses item_id)
"""
from __future__ import annotations

from src.core.queue_helpers import normalize_queue_item


# ─── PC branch ───────────────────────────────────────────────────────────

def test_pc_auto_number_resolves_to_attachment_title():
    raw = {
        "pc_number": "AUTO_db670ad9",
        "source_pdf": "/data/attachments/AMS 704 - Heel Donut - 04.29.26.pdf",
        "status": "new",
    }
    out = normalize_queue_item(raw, "pc", "pc_db670ad9")
    assert out["number"] == "Heel Donut - 04.29.26", (
        f"AUTO_* PC should display attachment title, got {out['number']!r}"
    )


def test_pc_auto_number_no_source_pdf_keeps_auto_id():
    raw = {"pc_number": "AUTO_43afa525", "status": "new"}
    out = normalize_queue_item(raw, "pc", "pc_43afa525")
    assert out["number"] == "AUTO_43afa525", (
        "Without source_pdf we have nothing to substitute — keep AUTO_* "
        "rather than showing '(blank)' or stripping the row."
    )


def test_pc_auto_number_hash_shaped_filename_keeps_auto_id():
    """`_attachment_filename_title` rejects pure-hex titles (would be worse
    than AUTO_*). Confirm the resolver respects the empty-string signal."""
    raw = {
        "pc_number": "AUTO_db670ad9",
        "source_pdf": "/tmp/abc123def456.pdf",
        "status": "new",
    }
    out = normalize_queue_item(raw, "pc", "pc_db670ad9")
    assert out["number"] == "AUTO_db670ad9"


def test_pc_real_number_unchanged():
    raw = {
        "pc_number": "PC-2026-0042",
        "source_pdf": "/data/attachments/Heel Donut.pdf",
        "status": "new",
    }
    out = normalize_queue_item(raw, "pc", "pc_177b18e6")
    assert out["number"] == "PC-2026-0042", (
        "Override must NOT mutate non-AUTO_* numbers — that would silently "
        "rename real PCs in the queue."
    )


def test_pc_url_independent_of_display_override():
    raw = {
        "pc_number": "AUTO_db670ad9",
        "source_pdf": "/data/attachments/AMS 704 - Heel Donut - 04.29.26.pdf",
        "status": "new",
    }
    out = normalize_queue_item(raw, "pc", "pc_db670ad9")
    assert out["url"] == "/pricecheck/pc_db670ad9", (
        "URL must use item_id, not the display-overridden number — "
        "otherwise clicking 'Heel Donut - 04.29.26' would 404."
    )


# ─── RFQ branch ──────────────────────────────────────────────────────────

def test_rfq_auto_number_resolves_to_attachment_title():
    raw = {
        "solicitation_number": "AUTO_a1b2c3d4",
        "source_pdf": "/data/attachments/RFQ - Office Chairs.pdf",
        "status": "new",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_a1b2c3d4")
    assert out["number"] == "Office Chairs", (
        f"AUTO_* RFQ should display attachment title, got {out['number']!r}"
    )


def test_rfq_auto_rfq_number_field_resolves():
    """RFQ falls back to `rfq_number` when `solicitation_number` is absent;
    confirm the AUTO_* override sees that path too."""
    raw = {
        "rfq_number": "AUTO_99887766",
        "source_pdf": "/data/attachments/Quote - Sterile Gauze.pdf",
        "status": "new",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_99887766")
    assert out["number"] == "Sterile Gauze"


def test_rfq_real_number_unchanged():
    raw = {
        "solicitation_number": "R26Q39",
        "source_pdf": "/data/attachments/whatever.pdf",
        "status": "sent",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_xyz")
    assert out["number"] == "R26Q39"


# ─── Source guard — pin the regex shape ──────────────────────────────────

def test_resolve_display_number_pattern_matches_existing_prod_records():
    """The two AUTO_* PCs Mike has on prod (pc_db670ad9, pc_43afa525) have
    pc_number values matching `AUTO_<8-hex>`. Pin the pattern so a future
    'tighten the regex' edit doesn't accidentally exclude prod records."""
    from src.core.queue_helpers import _AUTO_NUMBER_RE
    assert _AUTO_NUMBER_RE.match("AUTO_db670ad9")
    assert _AUTO_NUMBER_RE.match("AUTO_43afa525")
    # Reject non-hex / non-AUTO inputs so we don't override real numbers
    assert not _AUTO_NUMBER_RE.match("PC-2026-0042")
    assert not _AUTO_NUMBER_RE.match("R26Q39")
    assert not _AUTO_NUMBER_RE.match("AUTO_")
    assert not _AUTO_NUMBER_RE.match("AUTODRAFT")
    assert not _AUTO_NUMBER_RE.match("auto_db670ad9_extra")


# ─── 2026-05-11 Mike P0 — placeholder + email_subject substitution ───────
#
# Two RFQs from Keith Alsing on prod:
#   rfq_7813c4e1: solicitation_number="RFQ Flushable Wipes 4.28", sent
#   rfq_8efe9fae: solicitation_number="WORKSHEET", email_subject=
#                 "Medical Supplies RDQ Due Date 5/11/26", parsed (active)
# Both rendered as "WORKSHEET / Keith Alsing" in the active queue
# (the sent one in Sent/Completed, the new one above). Mike thought they
# were duplicates because the visual labels were identical. The new one
# is a real, distinct RFQ with different items.


def test_rfq_worksheet_placeholder_substitutes_email_subject():
    """When solicitation_number/rfq_number is "WORKSHEET" but email_subject
    is populated, surface the subject as the row label so identical
    placeholders from the same buyer don't look indistinguishable."""
    raw = {
        "solicitation_number": "WORKSHEET",
        "rfq_number": "WORKSHEET",
        "email_subject": "Medical Supplies RDQ Due Date 5/11/26",
        "requestor_name": "Keith Alsing",
        "status": "parsed",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_8efe9fae")
    assert out["number"] == "Medical Supplies RDQ Due Date 5/11/26"


def test_rfq_good_placeholder_substitutes_email_subject():
    """Same fallback for the "GOOD" placeholder (another legacy
    parser-derived garbage value)."""
    raw = {
        "solicitation_number": "GOOD",
        "email_subject": "RFQ - Diabetic Test Strips",
        "status": "new",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_test")
    assert out["number"] == "RFQ - Diabetic Test Strips"


def test_pc_worksheet_falls_back_to_pdf_when_no_subject():
    """When the row has no email_subject but has source_pdf, use the PDF
    filename title (the original AUTO_ cascade still works for
    WORKSHEET/GOOD/etc)."""
    raw = {
        "pc_number": "WORKSHEET",
        "source_pdf": "/data/attachments/AMS 704 - Penlight Order.pdf",
        "status": "new",
    }
    out = normalize_queue_item(raw, "pc", "pc_w1")
    assert out["number"] == "Penlight Order"


def test_placeholder_long_subject_truncates_to_60ch():
    """Some subjects are wildly long (full forwarded email chain); keep
    the queue cell readable at ~50ch by capping at 60ch."""
    long_subject = "RFQ for the procurement of medical supplies including " * 5
    raw = {
        "rfq_number": "WORKSHEET",
        "email_subject": long_subject,
        "status": "new",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_long")
    assert len(out["number"]) == 60
    assert out["number"] == long_subject[:60]


def test_real_number_never_overridden_by_subject():
    """Substitution must NEVER fire for real numbers — a real R26Q42
    RFQ with an email_subject must still display R26Q42, not the
    subject line."""
    raw = {
        "solicitation_number": "R26Q42",
        "email_subject": "RFQ - Diabetic Test Strips",
        "status": "sent",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_real")
    assert out["number"] == "R26Q42"


def test_unknown_placeholder_substitutes_email_subject():
    """The "unknown" placeholder (from email-poller fallback path) gets
    the same treatment — operator-readable label instead of generic word."""
    raw = {
        "rfq_number": "unknown",
        "email_subject": "Emergency PPE Quote Needed",
        "status": "parsed",
    }
    out = normalize_queue_item(raw, "rfq", "rfq_u1")
    assert out["number"] == "Emergency PPE Quote Needed"
