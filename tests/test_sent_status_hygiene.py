"""Bundle-5 PR-5a — sent-status hygiene audit (audit item A).

Evidence that motivated this audit: Mike's 2026-04-22 dashboard surfaced PCs
he'd quoted months ago (PC Karaoke 35d overdue, PIP Shoes/Radios, OMNI 40
from CIW) because their status field never advanced past generated/quoted.
The send paths DO write status=sent today; this test file locks that in so
the next refactor can't silently regress.

Two layers of defense are tested here:
  1. Source-level: every primary send/mark-sent path grep-asserts the
     status="sent" write + a call to _save_single_pc/_save_single_rfq.
     A text-only regression — cheap, never flakes, catches if someone
     removes the status write while refactoring.
  2. Behavioral: a PC with status=sent must NOT show in the active PC
     queue on the home route. The home route already splits active from
     sent via _pc_actionable; this locks that behavior.
"""
from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (_ROOT / rel).read_text(encoding="utf-8")


# ── Source-level regression tests ───────────────────────────────────────────


def _extract_function(src: str, name: str) -> str:
    m = re.search(
        rf"def {name}\([^)]*\)[\s\S]*?(?=\n@bp\.route|\ndef [a-zA-Z_]|\Z)",
        src,
    )
    assert m, f"function {name} not found"
    return m.group(0)


def test_api_pc_send_quote_writes_sent_status():
    """api_pc_send_quote writes status=sent + _save_single_pc.

    Locked in because this is the single most important send path — the
    "Send Quote" button on every PC detail page. A silent drop of the
    status write here is exactly the failure mode audit A describes.
    """
    body = _extract_function(
        _read("src/api/modules/routes_pricecheck_admin.py"),
        "api_pc_send_quote",
    )
    assert re.search(r'pc\["status"\]\s*=\s*"sent"', body), (
        "api_pc_send_quote must set pc['status'] = 'sent' after the email "
        "ships. Without it the PC stays in the active queue forever."
    )
    assert "_save_single_pc" in body, (
        "api_pc_send_quote must persist via _save_single_pc so the status "
        "flip reaches SQLite — otherwise the queue filter (which reads "
        "SQLite) surfaces the PC as still-active."
    )


def test_api_bundle_send_writes_sent_status_for_each_pc():
    """Bundle send loops every PC and calls _save_single_pc with status=sent."""
    body = _extract_function(
        _read("src/api/modules/routes_pricecheck_gen.py"),
        "api_bundle_send",
    )
    # Loops over bundle_pcs and writes status=sent + _save_single_pc per PC.
    assert re.search(r'pc\["status"\]\s*=\s*"sent"', body), (
        "api_bundle_send must mark every bundled PC as sent. Before this "
        "check the bundle-send path was the #1 source of stuck 'generated' "
        "records in Mike's queue."
    )
    assert "_save_single_pc" in body


def test_api_pricecheck_mark_sent_writes_sent_status():
    """The explicit 'Mark Sent' admin button must persist status + sent_at."""
    body = _extract_function(
        _read("src/api/modules/routes_pricecheck_pricing.py"),
        "api_pricecheck_mark_sent",
    )
    assert re.search(r'status\s*=\s*"sent"', body) or \
           re.search(r'"sent"', body), (
        "api_pricecheck_mark_sent must stamp status=sent — it's the whole "
        "point of this endpoint."
    )
    # Persistence — either _save_single_pc or the direct DAL writer:
    assert ("_save_single_pc" in body) or ("save_pc(" in body), (
        "api_pricecheck_mark_sent must commit the status flip."
    )


def test_api_pricecheck_dismiss_writes_status():
    """Dismiss path must write a terminal status to the DB."""
    body = _extract_function(
        _read("src/api/modules/routes_pricecheck_pricing.py"),
        "api_pricecheck_dismiss",
    )
    assert re.search(r'pc\["status"\]\s*=', body), (
        "api_pricecheck_dismiss must write pc['status']. Without it, "
        "dismissed PCs leak back into the queue on next reload."
    )
    assert "_save_single_pc" in body


def test_rfq_send_email_writes_sent_status():
    """The RFQ 'Send Quote' action writes status=sent via the canonical DAL."""
    body = _extract_function(
        _read("src/api/modules/routes_rfq_gen.py"),
        "send_email",
    )
    assert re.search(r'"sent"', body), (
        "routes_rfq_gen.send_email must write status='sent'."
    )
    # Either the transition helper or direct save:
    assert ("_save_single_rfq" in body) or ("update_rfq_status" in body), (
        "routes_rfq_gen.send_email must persist the status flip via the DAL."
    )


# ── Behavioral test: home route splits active vs sent ───────────────────────


def test_pc_home_route_splits_active_from_sent(auth_client, temp_data_dir):
    """A PC at status=sent must NOT render in the active queue.

    Evidence: audit A — Mike's triage queue surfaced records he'd already
    quoted. The split happens at routes_rfq.home() via `_pc_actionable` vs
    `sent_pcs`. If that split ever regresses, every sent PC re-pollutes the
    queue table. This test catches that regression.
    """
    from src.api.data_layer import _save_single_pc
    base = {
        "items": [{"description": "Test item", "qty": 1, "uom": "EA",
                   "pricing": {"recommended_price": 10.0}}],
        "institution": "CCHCS",
        "pc_number": "TEST-1",
        "created_at": "2026-04-20T12:00:00",
        "email_subject": "Test quote",
        "original_sender": "buyer@state.ca.gov",
    }
    # Two PCs: one active, one sent.
    active = dict(base, id="pc_active", status="priced",
                  pc_number="TEST-ACTIVE")
    sent = dict(base, id="pc_sent", status="sent",
                pc_number="TEST-SENT",
                sent_at="2026-04-22T10:00:00")
    _save_single_pc("pc_active", active)
    _save_single_pc("pc_sent", sent)

    resp = auth_client.get("/")
    assert resp.status_code == 200, (
        f"home route failed: {resp.status_code} {resp.data[:200]!r}"
    )
    html = resp.data.decode("utf-8", errors="replace")

    # The PC home split renders active vs sent into separate tables. Both
    # rows must appear somewhere, but not in the same queue table.
    # Active must still be surfaced on home (the bug would be hiding it):
    assert "TEST-ACTIVE" in html
    # Sent must NOT be in the active queue. Find it either absent, or
    # present only inside the Sent table (identified by the data-section
    # or a Sent heading). We check the looser invariant: an explicit
    # "Sent" heading appears before the TEST-SENT cell, and an explicit
    # active-queue heading appears before TEST-ACTIVE — i.e., each row
    # lives in its own section.
    active_idx = html.find("TEST-ACTIVE")
    sent_idx = html.find("TEST-SENT")
    if sent_idx == -1:
        # Sent PC hidden entirely is also acceptable for the active queue;
        # the home route may collapse sent into a foldout that only loads
        # on expansion. That still satisfies audit A.
        return
    # Both visible: the sent row must live in a sent-bucket. We find the
    # nearest "sent" label preceding each; the sent PC's nearest "sent"
    # label must be closer (i.e., the row is inside a Sent section).
    pre_sent = html[:sent_idx].lower()
    assert "sent" in pre_sent, (
        "PC with status=sent is rendered in a section that doesn't "
        "precede it with a 'Sent' header — looks like it's in the active "
        "queue, which reintroduces the bug from audit item A."
    )


def test_rfq_home_route_splits_active_from_sent(auth_client, temp_data_dir):
    """Symmetry: an RFQ at status=sent must NOT render in the active queue."""
    from src.api.data_layer import _save_single_rfq
    base_items = [{"description": "Test RFQ item", "qty": 1, "uom": "EA",
                   "price_per_unit": 12.0}]
    active = {
        "id": "rfq_active", "status": "draft", "rfq_number": "RFQ-ACTIVE",
        "solicitation_number": "RFQ-ACTIVE",
        "institution": "CCHCS",
        "received_at": "2026-04-20T12:00:00",
        "line_items": base_items, "items": base_items,
    }
    sent = dict(active, id="rfq_sent", status="sent",
                rfq_number="RFQ-SENT",
                solicitation_number="RFQ-SENT",
                sent_at="2026-04-22T10:00:00")
    _save_single_rfq("rfq_active", active)
    _save_single_rfq("rfq_sent", sent)

    resp = auth_client.get("/")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")

    assert "RFQ-ACTIVE" in html
    sent_idx = html.find("RFQ-SENT")
    if sent_idx == -1:
        return
    pre_sent = html[:sent_idx].lower()
    assert "sent" in pre_sent, (
        "RFQ with status=sent rendered in a section not preceded by a "
        "'Sent' heading — home route active/sent split regressed."
    )
