"""PR-4 regression tests: POs to source via canonical predicate.

Background — Mike's 2026-05-02 screenshot showed "99 POs? doesnt
even make sense". Tracing back: `/api/funnel/stats` set
`orders_active = sum(1 for o in orders.values() if o.get("status")
not in ("closed",))` — every non-closed order in the DB counted,
regardless of whether it was already invoiced, paid, cancelled-but-
not-closed, or already linked to a quote. Production had ~58
distinct POs but ~99 ghost / partially-mutated rows that hadn't
been GC'd.

PR-4 routes every "active orders" / "POs to source" counter through
`get_active_orders()`, which applies the canonical `is_sourceable_po`
predicate.

PR-4 originally landed with a Scientist-style dual-emit (canonical +
legacy + diff log). PR-6 (#696) deleted the legacy half once the
canonical numbers stabilized — the assertions below reflect the
post-cleanup shape.

Locks:
  - canonical sourceable count excludes invoiced/paid/closed/cancelled
  - already-quoted orders are excluded (sourcing handled by us)
  - sentinel po_numbers (N/A, TBD, ?) are excluded
  - the 3 home-page surfaces all read the same number
"""
from __future__ import annotations

import pytest


# ─── canonical exclusion semantics ───────────────────────────────────────


class TestCanonicalSourceableSemantics:
    """Property tests against `get_active_orders().total` — the
    headline number on /api/funnel/stats, /api/manager/metrics, and
    /api/dashboard/init funnel.orders. All three read this through
    the same helper now."""

    def test_excludes_invoiced(self):
        from src.core import metrics
        from src.core.order_dal import save_order
        save_order("ord-pr4-1", {"status": "new", "total": 100,
                                  "po_number": "0000088001"}, actor="t")
        save_order("ord-pr4-2", {"status": "invoiced", "total": 200,
                                  "po_number": "0000088002"}, actor="t")
        save_order("ord-pr4-3", {"status": "paid", "total": 300,
                                  "po_number": "0000088003"}, actor="t")
        out = metrics.get_active_orders()
        # Only the 'new' row is sourceable.
        assert out["total"] == 1
        # PR-6 (#696): the dual-emit `total_legacy` field was removed
        # once canonical numbers settled.
        assert "total_legacy" not in out

    def test_excludes_already_quoted(self):
        """Order with quote_number populated = already sourced
        (a quote is in flight). Don't double-count the operator's
        attention."""
        from src.core import metrics
        from src.core.order_dal import save_order
        save_order("ord-pr4-4", {"status": "new", "total": 100,
                                  "po_number": "0000088004",
                                  "quote_number": "R26Q001"},
                   actor="t")
        save_order("ord-pr4-5", {"status": "new", "total": 200,
                                  "po_number": "0000088005"},
                   actor="t")
        out = metrics.get_active_orders()
        assert out["total"] == 1, (
            "already-quoted orders shouldn't count as sourceable"
        )

    def test_excludes_sentinel_po_numbers(self):
        """N/A, TBD, ? po_numbers are placeholder strings, not real
        POs. Pre-PR-4 they were counted; post-PR-4 they're filtered."""
        from src.core import metrics
        from src.core.order_dal import save_order
        # NOTE: most sentinels get scrubbed to '' by clean_po_number
        # at the writer side. We rely on canonical predicate as
        # belt-and-suspenders for any sentinel that escaped (via
        # legacy data, manual seed, etc.). Empty string IS a sentinel
        # too — so we have to seed a real PO and a sentinel-PO
        # variant separately to exercise both branches.
        save_order("ord-pr4-6", {"status": "new", "total": 100,
                                  "po_number": "0000088006"},
                   actor="t")
        # `clean_po_number` will scrub "TBD" to ''; that empty string
        # is itself a sentinel so canonical drops it.
        save_order("ord-pr4-7", {"status": "new", "total": 999,
                                  "po_number": "TBD"},
                   actor="t")
        out = metrics.get_active_orders()
        assert out["total"] == 1, (
            "sentinel po_numbers (N/A/TBD/?) shouldn't count"
        )

    def test_invoiced_counted_separately(self, caplog):
        """Invoiced rows aren't sourceable — they show up under
        `closed` / `invoiced_value` so the orders dashboard can still
        render its 'completed' badge alongside the active backlog."""
        from src.core import metrics
        from src.core.order_dal import save_order
        save_order("ord-pr4-8", {"status": "invoiced", "total": 500,
                                  "po_number": "0000088008"},
                   actor="t")
        save_order("ord-pr4-9", {"status": "new", "total": 100,
                                  "po_number": "0000088009"},
                   actor="t")
        out = metrics.get_active_orders()
        assert out["total"] == 1, "only the 'new' row is sourceable"
        assert out["closed"] >= 1, (
            "invoiced row should be counted under `closed` so the "
            "orders dashboard can render the completed badge"
        )


# ─── Migration coverage: route surfaces use the helper ───────────────────


class TestRouteSurfacesUseCanonical:
    """Lock the migration: each home-page surface that previously
    inlined its own filter now goes through `get_active_orders` (or
    its result), so they can't drift."""

    def test_funnel_stats_uses_get_active_orders(self):
        from src.api.modules.routes_intel_ops import api_funnel_stats
        import inspect
        src = inspect.getsource(api_funnel_stats)
        assert "get_active_orders" in src, (
            "PR-4: api_funnel_stats must call get_active_orders, "
            "not inline `status not in ('closed',)`"
        )

    def test_manager_metrics_uses_get_active_orders(self):
        from src.api.modules.routes_intel_ops import api_manager_metrics
        import inspect
        src = inspect.getsource(api_manager_metrics)
        assert "get_active_orders" in src, (
            "PR-4: api_manager_metrics must call get_active_orders, "
            "not inline status IN ('active','processing','shipped')"
        )

    def test_legacy_inline_filter_not_reintroduced(self):
        """Belt-and-suspenders: the broad `status not in ('closed',)`
        filter was the bug. Lint-style guard that it doesn't creep
        back as the primary computation. The fallback inside an
        `except` is fine — it kicks in only if the canonical helper
        is unimportable."""
        from src.api.modules.routes_intel_ops import api_funnel_stats
        import inspect
        src = inspect.getsource(api_funnel_stats)
        # The orders section (after `# ── Orders ──` marker) must
        # call get_active_orders before any inline filter.
        marker = "# ── Orders ──"
        assert marker in src, "orders section marker missing in api_funnel_stats"
        orders_block = src.split(marker, 1)[1]
        # Reach to the next major section comment (or end of fn).
        for next_marker in ("# ── Won", "# ── Leads", "# Win rate"):
            if next_marker in orders_block:
                orders_block = orders_block.split(next_marker, 1)[0]
                break
        assert "get_active_orders" in orders_block, (
            "PR-4 regressed: orders block of api_funnel_stats no "
            "longer calls get_active_orders"
        )
        # And the inline filter, if present, must only appear inside
        # an except (the safety fallback). Quick check: it's
        # reachable only after `except`.
        before_except = orders_block.split("except", 1)[0]
        assert "status\") not in (\"closed\"" not in before_except, (
            "PR-4 regressed: inline `status not in ('closed',)` "
            "filter is back on the happy path of api_funnel_stats"
        )


# ─── End-to-end: api_funnel_stats returns canonical orders_active ────────


def test_funnel_stats_endpoint_returns_canonical_count(auth_client):
    """Seed a mix of sourceable + non-sourceable orders, hit
    /api/funnel/stats, assert orders_active matches canonical
    (NOT legacy)."""
    from src.core.order_dal import save_order
    save_order("ord-fs-1", {"status": "new", "total": 100,
                             "po_number": "0000077001"}, actor="t")
    save_order("ord-fs-2", {"status": "invoiced", "total": 200,
                             "po_number": "0000077002"}, actor="t")
    save_order("ord-fs-3", {"status": "paid", "total": 300,
                             "po_number": "0000077003"}, actor="t")
    save_order("ord-fs-4", {"status": "new", "total": 400,
                             "po_number": "0000077004",
                             "quote_number": "R26Q077"}, actor="t")

    resp = auth_client.get("/api/funnel/stats")
    assert resp.status_code == 200
    payload = resp.get_json()
    # Sourceable: only ord-fs-1 (new, real PO, no quote_number).
    # Pre-PR-4: 3 ('new' + 'invoiced' + 'paid' all status != 'closed').
    # Post-PR-4: 1.
    assert payload.get("orders_active") == 1, (
        f"PR-4 regressed: expected 1 sourceable PO, got "
        f"{payload.get('orders_active')}"
    )
