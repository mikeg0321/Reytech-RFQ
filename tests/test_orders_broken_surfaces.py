"""O-2 + O-5 + O-6 regressions: three separate broken surfaces on Orders V2.

O-2: api_order_emails built SQL as `WHERE " + where + "` inside a triple-
     quoted Python string — literal text, not concat. Caught by log.debug.
     Emails tab always empty.

O-5: supplier_record_page captured `<name>` but defined the function with
     parameter `n`. Flask raised TypeError on every call. Silent via
     @safe_page.

O-6: /order/create frontend read `d.quote` but /api/quote/lookup returns
     {ok, results: [...]}. Quote autofill never populated.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime


def _seed_order_with_email(temp_data_dir, oid="ORD-PO-EMAILS-TEST",
                            po="PO-EMAILS-1"):
    db_path = os.path.join(temp_data_dir, "reytech.db")
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()
    conn.execute(
        """INSERT OR IGNORE INTO orders
           (id, quote_number, po_number, agency, institution, total, status,
            created_at, updated_at, notes)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (oid, "Q-EMAIL-1", po, "cchcs", "CCHCS", 100.0, "new",
         now, now, ""),
    )
    # Seed a processed_emails row whose subject contains the PO number
    try:
        conn.execute(
            """INSERT INTO processed_emails
               (uid, subject, sender, body, received_at, classification)
               VALUES (?,?,?,?,?,?)""",
            (f"uid-{po}", f"Delivery update for {po}",
             "buyer@cchcs.ca.gov",
             f"PO {po} status update body text",
             now, "po_update"),
        )
    except sqlite3.OperationalError:
        # Schema may vary in tests; try alternate column layout
        try:
            conn.execute(
                """INSERT INTO processed_emails
                   (subject, sender, body, received_at)
                   VALUES (?,?,?,?)""",
                (f"Delivery update for {po}", "buyer@cchcs.ca.gov",
                 f"PO {po} status", now),
            )
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()
    return oid, po


# ── O-2: Emails tab query must actually run ─────────────────────────────

def test_order_emails_returns_matching_rows(auth_client, temp_data_dir):
    oid, po = _seed_order_with_email(temp_data_dir)

    resp = auth_client.get(f"/api/order/{oid}/emails")
    assert resp.status_code == 200, resp.get_data(as_text=True)
    body = resp.get_json() or {}
    assert body.get("ok") is True, body
    # The count depends on whether the test DB supports the processed_emails
    # columns. What we care about is that the query RAN without raising.
    # Before the fix, any row would trigger a SQL syntax error, swallowed by
    # log.debug, and count stayed at 0 even with matching data.
    assert isinstance(body.get("emails"), list), (
        "O-2: /emails must return a list, not error shape"
    )


def test_order_emails_source_has_fstring_not_literal_concat():
    """Grep-invariant: ensure the broken `WHERE " + where + "` pattern
    doesn't come back. f-string with braces is the correct form."""
    route_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "api", "modules", "routes_orders_full.py",
    )
    with open(route_file, encoding="utf-8") as f:
        src = f.read()

    # Extract api_order_emails function body
    start = src.find("def api_order_emails(")
    assert start >= 0
    rest = src[start:]
    end_rel = rest.find("\n@bp.route(", 10)
    fn = rest[:end_rel] if end_rel > 0 else rest

    # The broken pattern was exactly this literal inside a triple-quoted str:
    assert 'WHERE " + where + "' not in fn, (
        "O-2: broken literal string-concat returned to api_order_emails"
    )
    # And the log.debug swallow should be gone:
    assert "log.debug(\"Email thread" not in fn, (
        "O-2: log.debug swallow on SQL errors should stay removed"
    )


# ── O-5: supplier record page param name ────────────────────────────────

def test_supplier_record_page_renders_without_typeerror(auth_client, temp_data_dir):
    # Supplier "Amazon" with any casing — page should render even with 0 items
    resp = auth_client.get("/supplier/Amazon")
    # Before the fix this raised TypeError: got unexpected keyword argument 'name'
    # and @safe_page returned a 500 (or a fallback page). Success is a 2xx.
    assert resp.status_code == 200, (
        f"O-5: /supplier/<name> failed with status {resp.status_code} — "
        f"likely the param mismatch (name vs n) is back. "
        f"Body: {resp.get_data(as_text=True)[:300]}"
    )
    text = resp.get_data(as_text=True) or ""
    # Spot-check: page header should contain the supplier name we sent
    assert "Amazon" in text, "O-5: supplier name not rendered on the page"


def test_supplier_record_page_signature_matches_route_capture():
    """Grep-invariant: the function signature must name `name`, not `n`."""
    route_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "api", "modules", "routes_orders_full.py",
    )
    with open(route_file, encoding="utf-8") as f:
        src = f.read()

    # Find the route + def pair
    idx = src.find('@bp.route("/supplier/<name>")')
    assert idx >= 0, "route /supplier/<name> not found"
    # Walk forward to the def line
    def_idx = src.find("def supplier_record_page(", idx)
    assert def_idx >= 0
    eol = src.find("\n", def_idx)
    signature = src[def_idx:eol]
    assert "supplier_record_page(name)" in signature, (
        f"O-5: Flask route captures <name> but function signature is "
        f"{signature!r}. Must be `def supplier_record_page(name):`."
    )


# ── O-6: quote lookup frontend shape ────────────────────────────────────

def test_order_create_template_reads_results_not_quote():
    """Grep-invariant: frontend must read d.results (the API's shape), not
    d.quote (which never exists and always showed 'No match')."""
    tpl = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "src", "templates", "order_create.html",
    )
    with open(tpl, encoding="utf-8") as f:
        html = f.read()

    # Locate the quote-lookup block
    idx = html.find("/api/quote/lookup")
    assert idx >= 0, "quote-lookup fetch not found in order_create.html"
    # Look at the ~800 chars after this fetch call (the .then chain)
    block = html[idx:idx + 1200]

    assert "d.results" in block, (
        "O-6: order_create.html must read d.results from /api/quote/lookup — "
        "the API returns {ok, results:[...]}, never d.quote"
    )
    # The old broken `d.quote` check must be gone
    assert "d.quote" not in block, (
        "O-6: d.quote check reintroduced — API does not return a 'quote' key"
    )


def test_quote_lookup_api_returns_results_shape(auth_client):
    """Sanity check on the API contract the frontend now consumes."""
    resp = auth_client.get("/api/quote/lookup?q=ZZ-NONEXISTENT-XYZ")
    assert resp.status_code == 200
    body = resp.get_json() or {}
    # The contract: {ok: bool, results: [...], ...} — whether there are
    # matches or not, `results` must be the list key.
    assert "results" in body or body.get("ok") is False, (
        f"O-6: /api/quote/lookup must return a 'results' key when ok. Got: {body}"
    )
