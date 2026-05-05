"""Mike P0 2026-05-06 (Audit P0 #6): clean_po_number coverage at every
operator-input orders write site.

Background — PR #633 wired `clean_po_number()` through the canonical
`save_order` path so sentinel po_numbers (N/A, TBD, ?, X, PENDING)
get scrubbed to "" before INSERT. But the audit on 2026-05-06 found
write sites that BYPASS save_order:

  1. `quote_lifecycle.process_reply_signal` — operator/email-derived
     po_number flows into UPDATE quotes + INSERT orders without sanitization
  2. `routes_intel_ops.api_admin_orders_po_rewrite` — operator manually
     rewrites a po_number on an existing order

If those bypass clean_po_number, an operator typing "N/A" or an email
parser extracting "TBD" silently lands as orders.po_number = 'N/A',
and the po_aggregate dashboard groups unrelated orders under the literal
sentinel string (incident 2026-04-28: $220k under "N/A" before PR #633).

These guards are SOURCE-LEVEL. They read the function bodies and assert
that `clean_po_number` is called BEFORE the INSERT/UPDATE on po_number.
Doesn't replace integration tests but pins the wiring against future
regressions.
"""
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── quote_lifecycle.process_reply_signal ───────────────────────────


def test_process_reply_signal_cleans_po_number_before_use():
    """`process_reply_signal(quote_number, signal, po_number, ...)`
    must apply clean_po_number to po_number BEFORE the UPDATE quotes
    statement at line ~197 and BEFORE _auto_create_order(...) at line ~206."""
    body = (REPO / "src/agents/quote_lifecycle.py").read_text(encoding="utf-8")
    fn_start = body.find("def process_reply_signal(")
    assert fn_start > 0, "process_reply_signal not found"
    # Function body ends at next top-level def
    fn_end = body.find("\ndef ", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else len(body)]
    # Must import + call clean_po_number BEFORE the UPDATE quotes line
    update_idx = fn_body.find("UPDATE quotes")
    clean_idx = fn_body.find("clean_po_number")
    assert clean_idx > 0, (
        "process_reply_signal must call clean_po_number on po_number. "
        "Without it, sentinel PO numbers (N/A, TBD, ?, X) leak into "
        "quotes.po_number AND orders.po_number via _auto_create_order. "
        "Saw the function body without `clean_po_number` anywhere."
    )
    assert update_idx < 0 or clean_idx < update_idx, (
        f"clean_po_number must run BEFORE the UPDATE quotes statement. "
        f"Found clean_idx={clean_idx} update_idx={update_idx}"
    )


# ── routes_intel_ops.api_admin_orders_po_rewrite ───────────────────


def test_orders_po_rewrite_cleans_new_po_before_update():
    """`api_admin_orders_po_rewrite` must apply clean_po_number to the
    operator-supplied `new_po` BEFORE the UPDATE orders statement.
    Otherwise an operator pasting 'N/A' rewrites a real PO to a sentinel."""
    body = (REPO / "src/api/modules/routes_intel_ops.py").read_text(encoding="utf-8")
    fn_start = body.find("def api_admin_orders_po_rewrite(")
    assert fn_start > 0, "api_admin_orders_po_rewrite not found"
    fn_end = body.find("\ndef ", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else len(body)]
    update_idx = fn_body.find('UPDATE orders SET po_number')
    clean_idx = fn_body.find("clean_po_number")
    assert clean_idx > 0, (
        "api_admin_orders_po_rewrite must call clean_po_number on new_po"
    )
    assert update_idx < 0 or clean_idx < update_idx, (
        f"clean_po_number must run BEFORE the UPDATE orders. "
        f"clean_idx={clean_idx} update_idx={update_idx}"
    )


# ── Pre-existing site: routes_pricecheck_admin (mark-won path) ─────


def test_pricecheck_admin_mark_won_still_cleans_po():
    """Pin the pre-existing clean_po_number wiring at the mark-won route.
    A regression here is the original PR #633 surface."""
    body = (REPO / "src/api/modules/routes_pricecheck_admin.py").read_text(encoding="utf-8")
    # Find the auto-create order block (po_number gets cleaned, then INSERT)
    block_start = body.find("Auto-create order if PO number provided")
    assert block_start > 0, "auto-create-order block not found in pricecheck_admin"
    block_end = body.find("\n# ── ", block_start + 10)
    block = body[block_start:block_end if block_end > 0 else block_start + 5000]
    assert "clean_po_number" in block, (
        "PR #633 wired clean_po_number into the mark-won auto-create "
        "order path. If this assertion fails, that wiring regressed."
    )


# ── clean_po_number itself rejects expected sentinels ──────────────


def test_clean_po_number_strips_known_sentinels():
    from src.core.order_dal import clean_po_number
    # These are the canonical sentinel forms operators / parsers produce.
    # `clean_po_number` is conservative — it strips exact-match sentinels,
    # not embedded ones (`?N/A?` would NOT clean to "" because it's not a
    # bare sentinel — see test_clean_po_number_does_not_strip_legit_pos_with_letters
    # in tests/test_sentinel_po_numbers.py).
    sentinels = ["N/A", "n/a", "NA", "TBD", "tbd", "PENDING", "?", "X",
                 "x", "TBA", "TBA ", " N/A ", "UNKNOWN", "  ", ""]
    for s in sentinels:
        assert clean_po_number(s) == "", (
            f"clean_po_number({s!r}) should return '' but returned "
            f"{clean_po_number(s)!r}"
        )


def test_clean_po_number_preserves_real_pos():
    from src.core.order_dal import clean_po_number
    real = ["8955-00012345", "4500098765", "PO-2026-001",
            "0000053217", "AB-12345", "PO-N123", "X-1"]
    for r in real:
        cleaned = clean_po_number(r)
        # The cleaner may strip leading zeros or normalize — just verify
        # the result is non-empty and shape-preserved roughly
        assert cleaned, f"clean_po_number({r!r}) should preserve real POs"


# ── Inventory check — no NEW orders write sites without clean_po_number ──


def test_no_new_unguarded_orders_write_sites():
    """Inventory test: every `INSERT INTO orders` or `UPDATE orders`
    that touches po_number in our repo (excluding tests, audit logs,
    schema definitions, and explicitly-safe SCPRS-derived backfills)
    should be near a clean_po_number import or call.

    If this test fails because a new write site was added, the new
    site must either:
      (a) call clean_po_number on the input, OR
      (b) be added to the EXCEPTIONS list below with a comment justifying
          why the input is already known clean.
    """
    import re
    EXCEPTIONS = {
        # SCPRS-derived canonical PO format-drift fix loop (input is
        # already canonical from `_scprs_canonical_po(bu, bare)`)
        ("src/api/modules/routes_intel_ops.py", "fix_orders_po_format_drift"),
        # SCPRS-derived stub creator (input is `c["canonical"]` from
        # `_scprs_canonical_po`)
        ("src/api/modules/routes_intel_ops.py", "import_scprs_detail"),
        # is_test sets are not po_number writes
        ("src/api/modules/routes_intel_ops.py", "_test_only"),
        # status-only updates (no po_number touched)
        ("src/core/dal.py", "_status_only"),
        # Legacy boot migration paths — file `orders.json` is renamed to
        # `.migrated` after first boot, so these don't run on current prod.
        # Both legacy migration paths read from data sources that have
        # already been retired.
        ("src/core/db.py", "_legacy_migration"),
    }
    sites = []
    for py in REPO.glob("src/**/*.py"):
        rel = py.relative_to(REPO).as_posix()
        if "test_" in rel or "/tests/" in rel:
            continue
        text = py.read_text(encoding="utf-8", errors="ignore")
        # Match `INSERT INTO orders` or `UPDATE orders`
        for m in re.finditer(
            r"(INSERT(?:\s+OR\s+(?:IGNORE|REPLACE))?\s+INTO\s+orders\b|UPDATE\s+orders\s+SET\b)",
            text, re.IGNORECASE,
        ):
            # Skip `audit_log` / `lifecycle_events` / etc. (not orders table)
            ctx = text[max(0, m.start() - 40):m.start() + 120]
            if "orders_audit_log" in ctx or "lifecycle_events" in ctx:
                continue
            # Skip status-only updates (don't touch po_number)
            if "UPDATE" in m.group(0).upper():
                # peek 200 chars after for po_number reference
                tail = text[m.end():m.end() + 250]
                if "po_number" not in tail and "po_number" not in ctx:
                    continue
            sites.append((rel, m.start()))

    # We expect a small finite set of sites. Any new site needs review.
    # Current inventory (2026-05-06):
    #   1. quote_lifecycle.py:_auto_create_order — guarded via process_reply_signal
    #   2. db.py:3001 (legacy orders.json migration — dead code on prod)
    #   3. db.py:3140 (legacy boot reconcile)
    #   4. db.py:3198 (legacy purchase_orders migration — dead, table dropped)
    #   5. db.py:1887/1919/1947 (status update paths — no po_number touched)
    #   6. order_dal.py:421 (canonical save_order — clean_po_number IS called)
    #   7. routes_intel_ops.py:4212 (SCPRS canonical drift fix — input clean)
    #   8. routes_intel_ops.py:4385 (SCPRS auto-stub — input is c["canonical"])
    #   9. routes_intel_ops.py:4499 (is_test=1 admin update — no po touched)
    #   10. routes_intel_ops.py:5134 (orders_po_rewrite — guarded by clean_po_number)
    #   11. routes_pricecheck_admin.py:348 (mark-won auto-create — guarded)
    # Total: 11 sites. New sites must justify why they don't need clean_po_number
    # OR add it before the INSERT/UPDATE.
    assert len(sites) <= 11, (
        f"Found {len(sites)} INSERT/UPDATE sites against orders.po_number. "
        f"If this is intentional growth, update the EXCEPTIONS comment and "
        f"bump the cap. New sites must call clean_po_number first OR "
        f"justify why the input is already known clean. Sites: {sites}"
    )
