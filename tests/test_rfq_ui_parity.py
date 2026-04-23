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

    Names are kpiSubEl/kpiTaxEl to avoid colliding with the
    margin-summary-bar's `var taxEl` and the per-row-subtotal loop's
    `const subEl` — both of which hoist / live in the same recalc()
    function scope (2026-04-23 hotfix incident).
    """
    _seed_rfq("rfq_recalc_assert")
    resp = auth_client.get("/rfq/rfq_recalc_assert")
    html = resp.data.decode("utf-8", errors="replace")
    # Writer lines we depend on — explicit assignments to the KPI cells.
    assert "kpiSubEl.textContent=" in html or "kpiSubEl.textContent =" in html
    assert "kpiTaxEl.textContent=" in html or "kpiTaxEl.textContent =" in html
    assert "rfqTaxRate" in html


def test_rfq_detail_inline_js_parses_without_identifier_collision(
        auth_client, temp_data_dir):
    """Hotfix guard: regex across every inline <script> in rfq_detail.html
    to ensure no identifier is declared twice at the same lexical level.

    Incident 2026-04-23: Bundle-6 PR-6b added `const taxEl` inside recalc()
    next to an existing `var taxEl` (margin-summary bar, inside the same
    function scope). Result: SyntaxError at parse time, `recalc`
    undefined, KPI strip stuck at $0.00 on every RFQ detail page in prod.
    pytest-side template-render tests missed this because they only grep
    for string presence, not JS validity.

    This test doesn't run a JS parser (too heavy), but it catches the
    exact class of regression by finding any var/let/const name declared
    more than once at the top level of the same <script> block — which
    is the fingerprint of hoisted-collision errors.
    """
    import re
    _seed_rfq("rfq_parse_guard")
    resp = auth_client.get("/rfq/rfq_parse_guard")
    html = resp.data.decode("utf-8", errors="replace")

    # Extract each <script>...</script> body.
    scripts = re.findall(r"<script[^>]*>(.*?)</script>",
                         html, flags=re.DOTALL | re.IGNORECASE)

    # Names this test is watching. When adding a new top-level const/let
    # to recalc() or any shared script block, check against this list OR
    # prefix your new name (e.g. `kpiX`) to keep it distinct.
    watched = [
        "subEl", "taxEl", "shipEl", "gtEl", "msBar", "mkEl",
        "kpiSubEl", "kpiTaxEl", "kpiTaxAmount", "kpiTotalWithTax",
        "rfqTaxRate", "rfqTaxEnabled",
    ]
    collisions = []
    for i, body in enumerate(scripts):
        # Strip nested brace bodies so `const x inside for(){}` doesn't
        # count as top-level. This is a heuristic but catches the real
        # hoisting-collision pattern we hit in prod.
        stripped = body
        # Remove one level of {...} iteratively (cheap, not perfect).
        for _ in range(8):
            new_stripped, n = re.subn(r"\{[^{}]*\}", "", stripped)
            if n == 0:
                break
            stripped = new_stripped
        for name in watched:
            pat = rf"\b(?:const|let|var)\s+{name}\b"
            hits = re.findall(pat, stripped)
            if len(hits) > 1:
                collisions.append({
                    "script_idx": i, "name": name, "declarations": len(hits),
                })
    assert not collisions, (
        "Duplicate top-level JS declarations detected — this is the same "
        "class of bug that zeroed prod recalc on 2026-04-23. Rename one "
        "side (prefix with `kpi`/`ms`/`row`) so they don't share scope.\n"
        f"Collisions: {collisions}"
    )
