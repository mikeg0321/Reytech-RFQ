"""O-3 / O-4 regression: app-level email signatures MUST NOT appear in
outbound email bodies. Gmail appends the authoritative signature; ours
creates a double-sign (O-3) or uses the wrong identity (O-4:
"Mike Gonzales / mike@reytechinc.com").

Implements grep-invariant defense #5 from feedback_production_ready_definition.md:
  - non-canonical identity:  "mike@reytechinc.com", "Gonzales"
  - app-level signature:     "30 Carnoustie", "SB/DVBE Cert"

Any future re-introduction of those strings to the response bodies from
`/api/order/<oid>/reply-all` or `/api/order/<oid>/delivery-update` fails
the pre-push test sandbox.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime

FORBIDDEN_PATTERNS = [
    "30 Carnoustie",         # street address — app-level sig marker
    "SB/DVBE Cert",          # cert footer — app-level sig marker
    "mike@reytechinc.com",   # non-canonical email (canonical is sales@)
    "Gonzales",              # non-canonical last name (canonical is Guadan)
    "949-229-1575",          # phone, present in both sig blocks
    "(949) 229-1575",
]


def _seed_order(temp_data_dir, oid="ORD-PO-SIG-TEST"):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            buyer_name, buyer_email, created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, "Q-SIG-1", "PO-SIG-1", "cchcs", "CCHCS", 100.0, "new",
         "Test Buyer", "buyer@cchcs.ca.gov", now, now, ""),
    )
    conn.execute(
        """INSERT INTO order_line_items
           (order_id, line_number, description, qty_ordered, unit_price,
            unit_cost, extended_price, extended_cost, sourcing_status,
            delivery_date, tracking_number, carrier,
            created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, 1, "Widget", 1, 100.0, 40.0, 100.0, 40.0, "delivered",
         "2026-04-15", "1Z999", "UPS", now, now),
    )
    conn.commit()
    conn.close()
    return oid


def _assert_no_signature(body: str, source: str):
    for needle in FORBIDDEN_PATTERNS:
        assert needle not in body, (
            f"O-3/O-4: {source} leaked forbidden signature token {needle!r}. "
            f"Gmail appends the signature — do not hardcode it in route handlers."
        )


# ── O-3: /api/order/<oid>/reply-all ────────────────────────────────────

def test_reply_all_body_has_no_app_signature(auth_client, temp_data_dir):
    oid = _seed_order(temp_data_dir, oid="ORD-PO-REPLYALL-SIG")

    # This route is GET today (O-14); the response surface is the outbox
    # record saved to the DB. Get the outbox body via its own endpoint if
    # available, else via the redirect response body (which embeds a preview).
    resp = auth_client.get(f"/api/order/{oid}/reply-all",
                           follow_redirects=False)
    # Route redirects to /orders after saving outbox. Check the outbox row
    # directly from the DB — source of truth.
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT body_plain, body_html FROM email_outbox "
            "WHERE order_id=? ORDER BY created_at DESC LIMIT 1",
            (oid,)
        ).fetchone()
    except sqlite3.OperationalError:
        # If email_outbox table doesn't exist in test DB, the route may
        # return the body inline. Fall back to response text.
        row = None
    conn.close()

    if row is not None:
        _assert_no_signature(row["body_plain"] or "", "reply-all body_plain")
        _assert_no_signature(row["body_html"] or "", "reply-all body_html")
    else:
        text = resp.get_data(as_text=True) or ""
        _assert_no_signature(text, "reply-all response")


# ── O-4: /api/order/<oid>/delivery-update ──────────────────────────────

def test_delivery_update_body_has_no_wrong_identity(auth_client, temp_data_dir):
    oid = _seed_order(temp_data_dir, oid="ORD-PO-DELIVERY-SIG")

    resp = auth_client.post(
        f"/api/order/{oid}/delivery-update",
        json={"items": [{"line_id": "L001"}], "note": "Test note"},
    )
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json() or {}

    # draft_url is a Gmail compose URL with body= query param; if the
    # body contains a signature, it's leaking into the draft.
    draft_url = body.get("draft_url", "") or ""
    _assert_no_signature(draft_url, "delivery-update draft_url")


# ── Invariant: the route source itself must not re-introduce the strings

def test_route_source_does_not_re_hardcode_signature():
    """Grep-invariant: reply-all / delivery-update route handlers must not
    contain signature/identity strings. Catches re-introduction in PRs
    without needing to set up a full test flow."""
    route_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "api", "modules", "routes_orders_full.py",
    )
    with open(route_file, encoding="utf-8") as f:
        src = f.read()

    # Extract just the reply_all + delivery_update function bodies by
    # locating function def lines and taking everything up to the next
    # `@bp.route(` or `def api_`.
    for fn_name in ("api_order_reply_all", "api_order_delivery_update"):
        start = src.find(f"def {fn_name}(")
        assert start >= 0, f"Could not locate {fn_name} in source"
        # Find end: next function def on the module
        rest = src[start:]
        # Terminate at next top-level `@bp.route(` after ~5 lines (skip decorators of THIS fn)
        end_rel = rest.find("\n@bp.route(", 10)
        fn_body = rest[:end_rel] if end_rel > 0 else rest

        for needle in FORBIDDEN_PATTERNS:
            assert needle not in fn_body, (
                f"O-3/O-4: forbidden signature string {needle!r} re-appeared "
                f"in {fn_name}. Remove — Gmail attaches the signature."
            )
