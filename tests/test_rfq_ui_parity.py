"""Bundle-6 PR-6b — RFQ detail UI parity with PC.

Closes RFQ-UI-1 (MFG# column left of Description) and RFQ-UI-2 (Summary
stacks Subtotal + Tax + Total + Profit) from the 2026-04-22 session audit.
Those audits came from Mike comparing the PC detail page side-by-side with
the RFQ detail page: PC renders MFG# before Description and shows the full
Subtotal/Tax/Profit breakdown; RFQ rendered Part# after Description and
only showed Revenue + Profit + Items Priced (no tax view, no subtotal).

The fixes are surface-level (column reorder, KPI cells) but matter for
operator speed — Mike reads line items left-to-right and expects the part
number to anchor each row. The summary stack also lets him sanity-check
the tax rate before clicking Generate Package without scrolling down.

RFQ-UI-3 (unified line-item macro) is explicitly out of scope for this PR
— it's a larger refactor that would touch both templates and requires
careful regression-proofing against several column-count-specific flows.
"""
from __future__ import annotations


def _seed_rfq(rid, **overrides):
    from src.api.data_layer import _save_single_rfq
    r = {
        "id": rid, "status": "new",
        "rfq_number": rid, "solicitation_number": rid,
        "line_items": [{
            "description": "Engraved name tag, black/white",
            "item_number": "WDG-100", "qty": 10, "uom": "EA",
            "supplier_cost": 12.0, "price_per_unit": 15.0,
        }],
        "items": [],
    }
    r.update(overrides)
    r["items"] = r["line_items"]
    _save_single_rfq(rid, r)


# ── RFQ-UI-1: MFG# left of Description ─────────────────────────────────────


def test_rfq_items_table_part_column_is_left_of_description(
        auth_client, temp_data_dir):
    """The items table header must render Part # BEFORE Description.

    We lock this by searching the <thead> and asserting the first
    occurrence of 'Part #' precedes the first occurrence of 'Description'.
    A regression (swap back) trips this immediately.
    """
    _seed_rfq("rfq_col_order")
    resp = auth_client.get("/rfq/rfq_col_order")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")

    # Narrow to the items <thead> block — the template includes the word
    # "Description" in other panels (AI add row, screenshot helpers).
    thead_start = html.find('class="it"')
    assert thead_start != -1
    thead_end = html.find("</thead>", thead_start)
    thead = html[thead_start:thead_end]

    part_idx = thead.find("Part #")
    desc_idx = thead.find("Description")
    assert part_idx != -1, "Part # header missing from items table"
    assert desc_idx != -1, "Description header missing from items table"
    assert part_idx < desc_idx, (
        "RFQ-UI-1 regression: Part # column must render LEFT of Description. "
        "This is how Mike reads rows (part-number anchors each line) and "
        "matches PC detail column order."
    )


def test_rfq_items_row_part_cell_precedes_description_cell(
        auth_client, temp_data_dir):
    """The <td> order in each row must match the <th> order."""
    _seed_rfq("rfq_row_order")
    resp = auth_client.get("/rfq/rfq_row_order")
    html = resp.data.decode("utf-8", errors="replace")

    tbody_start = html.find('class="it"')
    # Use name="part_0" and name="desc_0" positions as a proxy — those are
    # the first row's input names.
    part0 = html.find('name="part_0"', tbody_start)
    desc0 = html.find('name="desc_0"', tbody_start)
    assert part0 != -1 and desc0 != -1
    assert part0 < desc0, (
        "Row <td> cells out of sync with <thead>. Part input must appear "
        "before Description textarea."
    )


# ── RFQ-UI-2: Summary stacks Subtotal + Tax + Total + Profit ────────────────


def test_rfq_summary_has_subtotal_tax_total_profit(auth_client, temp_data_dir):
    """The KPI strip exposes every piece of the math by distinct cell."""
    _seed_rfq("rfq_sum", tax_rate=7.75, tax_enabled=True)
    resp = auth_client.get("/rfq/rfq_sum")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8", errors="replace")

    assert 'data-testid="rfq-summary-stack"' in html
    # Each KPI cell has a stable id the recalc() JS writes to:
    assert 'id="subt"' in html
    assert 'id="taxt"' in html
    assert 'id="tot"' in html
    assert 'id="pft"' in html
    # Items Priced + Status stay on the strip (regression guard: don't
    # lose existing info while adding new info).
    assert 'id="rfq-items-priced-kpi"' in html
    assert 'id="rfq-status-kpi"' in html


def test_rfq_summary_shows_tax_rate_in_label_when_set(
        auth_client, temp_data_dir):
    """When the RFQ has a tax rate, the 'Tax' cell header displays it."""
    _seed_rfq("rfq_taxlbl", tax_rate=7.75, tax_enabled=True)
    resp = auth_client.get("/rfq/rfq_taxlbl")
    html = resp.data.decode("utf-8", errors="replace")
    # Accept either the full float formatted or simple presence of the rate.
    # The template renders "%.3f|format" — look for '7.750' near the Tax label.
    stack_start = html.find('data-testid="rfq-summary-stack"')
    assert stack_start != -1
    # Stack is short — find Tax header region and the rate formatted there:
    stack_region = html[stack_start:stack_start + 5000]
    assert "Tax" in stack_region
    assert "7.750" in stack_region


def test_rfq_summary_hides_tax_rate_when_unset(auth_client, temp_data_dir):
    """No tax_rate → label says 'Tax' without a percent suffix."""
    _seed_rfq("rfq_notax")
    resp = auth_client.get("/rfq/rfq_notax")
    html = resp.data.decode("utf-8", errors="replace")
    stack_start = html.find('data-testid="rfq-summary-stack"')
    region = html[stack_start:stack_start + 5000]
    # The label literal "Tax" is present, but no "Tax (X.XXX%)" text.
    # Detect by looking for the open paren that only appears when tax_rate truthy.
    assert 'id="taxt"' in region
    # A tax label with parens only appears when tax_rate is truthy — check
    # the prefix '>Tax ' (with trailing space before '(X')
    assert ">Tax (" not in region


def test_rfq_summary_recalc_writes_to_subtotal_and_tax_cells(
        auth_client, temp_data_dir):
    """The inline recalc() script must include subt/taxt writes.

    We don't execute the JS in tests; instead we lock that the writer
    calls exist in the template source so a refactor can't silently
    remove them (and leave the cells permanently stuck at $0.00).
    """
    _seed_rfq("rfq_recalc_assert")
    resp = auth_client.get("/rfq/rfq_recalc_assert")
    html = resp.data.decode("utf-8", errors="replace")
    # Writer lines we depend on — look for explicit assignments to the
    # new cells inside the recalc path.
    assert "subEl.textContent=" in html or "subEl.textContent =" in html
    assert "taxEl.textContent=" in html or "taxEl.textContent =" in html
    assert "rfqTaxRate" in html
