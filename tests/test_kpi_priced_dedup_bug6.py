"""Bug 6 — drop the duplicate "Items Priced" cell from the KPI strip.

Mike's bug report (2026-05-02 image #8): the priced count rendered in
both the KPI strip (`Items Priced` 6th cell) AND the margin-summary-bar
(`Priced` cell) AND the line-items section header. The KPI-strip
instance is the duplicative one — same number, three places, screen
real-estate burn.

This file locks:
  1. The 6-col `repeat(6, ...)` grid is gone — the strip is now 5 cols.
  2. The `rfq-items-priced-kpi` cell ID is no longer rendered.
  3. The "Items Priced" label is gone from the strip (avoid label
     leakage from a different surface re-introducing the cell).
  4. The line-items header still shows "X/Y priced" — canonical surface.
  5. The margin-summary-bar still has its `Priced` cell (separate
     contextual surface; not what Mike flagged).
"""
from __future__ import annotations


def _seed(client, rid="rfq_kpi_dedup"):
    from src.api.data_layer import _save_single_rfq
    _save_single_rfq(rid, {
        "id": rid, "status": "generated",
        "rfq_number": "KPI-DEDUP",
        "solicitation_number": "KPI-DEDUP",
        "institution": "CCHCS",
        "line_items": [
            {"description": "A", "qty": 1, "price_per_unit": 50.0},
            {"description": "B", "qty": 1, "price_per_unit": 25.0},
        ],
    })
    return rid


def test_kpi_strip_is_5_cols_not_6(auth_client, temp_data_dir):
    rid = _seed(auth_client)
    resp = auth_client.get(f"/rfq/{rid}")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")
    # New 5-col grid — the KPI strip itself.
    assert "grid-template-columns:repeat(5, minmax(0,1fr))" in html
    # Old 6-col definition for the same strip must be gone.
    assert "data-testid=\"rfq-summary-stack\"" in html
    # Sanity: the rfq-summary-stack opening tag now uses repeat(5,...).
    stack_open = html.split('data-testid="rfq-summary-stack"', 1)[1].split(">", 1)[0]
    assert "repeat(5, minmax(0,1fr))" in stack_open
    assert "repeat(6, minmax(0,1fr))" not in stack_open


def test_kpi_strip_no_items_priced_cell(auth_client, temp_data_dir):
    rid = _seed(auth_client)
    resp = auth_client.get(f"/rfq/{rid}")
    html = resp.data.decode("utf-8", errors="replace")
    # The cell ID is gone from rendered HTML.
    assert 'id="rfq-items-priced-kpi"' not in html
    # The cell label is gone too.
    assert ">Items Priced<" not in html


def test_line_items_header_still_shows_priced_count(auth_client, temp_data_dir):
    """Canonical priced-count surface — line-items section header. Must
    NOT regress with the KPI-strip cleanup."""
    rid = _seed(auth_client)
    resp = auth_client.get(f"/rfq/{rid}")
    html = resp.data.decode("utf-8", errors="replace")
    # The header renders "<priced>/<total> priced" — exact format from
    # the Jinja loop. Use the priced=2 case (both items have price).
    assert "2/2 priced" in html


def test_margin_summary_bar_still_has_priced_cell(auth_client, temp_data_dir):
    """The margin-summary-bar Priced cell is a different contextual
    surface (cost/margin context). Mike flagged the KPI-strip duplicate,
    not this one — guard against accidental over-removal."""
    rid = _seed(auth_client)
    resp = auth_client.get(f"/rfq/{rid}")
    html = resp.data.decode("utf-8", errors="replace")
    assert 'id="ms-items-priced"' in html
