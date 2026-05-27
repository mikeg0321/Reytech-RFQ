"""Currency input formatting in the RFQ row.

Closes the bug class behind the 2026-05-26 operator screenshot where
`YOUR COST` and `BID PRICE` rendered `8.0` / `9.6` / `13.5` instead of
`8.00` / `9.60` / `13.50`. Root cause was two-seam: server-side raw float
value + `type="number"` which strips trailing zeros on browser parse.

Substrate fix:
  - rfq_detail.html: cost/price inputs now type=text inputmode=decimal,
    value formatted as %.2f, wired to sanitizePrice/fmtCurrency,
    width relaxed to min-width + field-sizing:content.
  - shared_item_utils.js: sanitizePrice/fmtCurrency overloaded so that
    when called with an HTMLInputElement they mutate el.value in place
    (previously they were silent no-ops at every (this)-style call site).
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


# ──────────────────────────────────────────────────────────────────────
# RFQ detail HTML render
# ──────────────────────────────────────────────────────────────────────

class TestRfqDetailCurrencyRender:

    def test_cost_value_zero_padded_two_decimals(self, client, sample_rfq, temp_data_dir):
        """supplier_cost=350.00 must render as value="350.00", not "350.0"."""
        import json, os
        with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
            json.dump({sample_rfq["id"]: sample_rfq}, f)
        resp = client.get(f"/rfq/{sample_rfq['id']}")
        assert resp.status_code == 200
        html = resp.get_data(as_text=True)
        assert 'name="cost_0"' in html, "cost input must render"
        # Find the cost input and check value attribute carries trailing zeros
        cost_token = 'name="cost_0"'
        idx = html.find(cost_token)
        snippet = html[idx : idx + 600]
        assert 'value="350.00"' in snippet, (
            f"supplier_cost=350.00 must render value=\"350.00\" "
            f"(trailing zero preserved); snippet: {snippet[:300]}"
        )

    def test_price_fractional_renders_two_decimals(self, client, sample_rfq, temp_data_dir):
        """price_per_unit=454.40 must render as "454.40", not "454.4"."""
        import json, os
        with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
            json.dump({sample_rfq["id"]: sample_rfq}, f)
        resp = client.get(f"/rfq/{sample_rfq['id']}")
        html = resp.get_data(as_text=True)
        idx = html.find('name="price_0"')
        assert idx >= 0
        snippet = html[idx : idx + 600]
        assert 'value="454.40"' in snippet, (
            f"price_per_unit=454.40 must render value=\"454.40\"; snippet: {snippet[:300]}"
        )

    def test_one_decimal_price_padded(self, client, sample_rfq, temp_data_dir):
        """price_per_unit=9.6 (the operator screenshot symptom) must render as "9.60"."""
        import json, os
        sample_rfq["line_items"][0]["supplier_cost"] = 8.0
        sample_rfq["line_items"][0]["price_per_unit"] = 9.6
        with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
            json.dump({sample_rfq["id"]: sample_rfq}, f)
        resp = client.get(f"/rfq/{sample_rfq['id']}")
        html = resp.get_data(as_text=True)
        cost_snip = html[html.find('name="cost_0"') : html.find('name="cost_0"') + 600]
        price_snip = html[html.find('name="price_0"') : html.find('name="price_0"') + 600]
        assert 'value="8.00"' in cost_snip, f"got: {cost_snip[:300]}"
        assert 'value="9.60"' in price_snip, f"got: {price_snip[:300]}"

    def test_zero_or_none_renders_empty(self, client, sample_rfq, temp_data_dir):
        """Missing/zero cost+price must render as empty (placeholder shows through)."""
        import json, os
        sample_rfq["line_items"][0]["supplier_cost"] = 0
        sample_rfq["line_items"][0]["price_per_unit"] = None
        with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
            json.dump({sample_rfq["id"]: sample_rfq}, f)
        resp = client.get(f"/rfq/{sample_rfq['id']}")
        html = resp.get_data(as_text=True)
        cost_snip = html[html.find('name="cost_0"') : html.find('name="cost_0"') + 600]
        price_snip = html[html.find('name="price_0"') : html.find('name="price_0"') + 600]
        # placeholder is "0.00"; value should be empty string
        assert 'value=""' in cost_snip, f"got: {cost_snip[:300]}"
        assert 'value=""' in price_snip, f"got: {price_snip[:300]}"

    def test_inputs_use_text_inputmode_decimal(self, client, sample_rfq, temp_data_dir):
        """Inputs must be type=text inputmode=decimal so trailing zeros stick.

        `type="number"` would let the browser strip "9.60" back to "9.6" on
        every render — that was the original bug. This pins the migration.
        """
        import json, os
        with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
            json.dump({sample_rfq["id"]: sample_rfq}, f)
        resp = client.get(f"/rfq/{sample_rfq['id']}")
        html = resp.get_data(as_text=True)
        for name in ("cost_0", "price_0"):
            # type= comes before name= in the rendered tag; widen the window.
            anchor = html.find(f'name="{name}"')
            tag_start = html.rfind("<input", 0, anchor)
            tag_end = html.find(">", anchor)
            assert tag_start >= 0 and tag_end > tag_start, f"could not isolate <input> for {name}"
            tag = html[tag_start : tag_end + 1]
            assert 'type="text"' in tag, f"{name} must be type=text; got: {tag}"
            assert 'inputmode="decimal"' in tag, f"{name} needs inputmode=decimal; got: {tag}"
            assert 'type="number"' not in tag, (
                f"{name} must NOT be type=number — that strips trailing zeros; got: {tag}"
            )

    def test_inputs_auto_grow_with_field_sizing(self, client, sample_rfq, temp_data_dir):
        """Inputs must use field-sizing:content so long prices don't clip."""
        import json, os
        with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
            json.dump({sample_rfq["id"]: sample_rfq}, f)
        resp = client.get(f"/rfq/{sample_rfq['id']}")
        html = resp.get_data(as_text=True)
        for name in ("cost_0", "price_0"):
            snip = html[html.find(f'name="{name}"') : html.find(f'name="{name}"') + 600]
            assert "field-sizing:content" in snip, (
                f"{name} must declare field-sizing:content for auto-grow; got: {snip[:300]}"
            )
            assert "min-width:110px" in snip, (
                f"{name} must keep min-width:110px floor; got: {snip[:300]}"
            )

    def test_inputs_wire_format_helpers(self, client, sample_rfq, temp_data_dir):
        """Inputs must call sanitizePrice on input and fmtCurrency on blur.

        These are the element-aware helpers in shared_item_utils.js that
        keep typed text within currency shape and reformat on blur.
        """
        import json, os
        with open(os.path.join(temp_data_dir, "rfqs.json"), "w") as f:
            json.dump({sample_rfq["id"]: sample_rfq}, f)
        resp = client.get(f"/rfq/{sample_rfq['id']}")
        html = resp.get_data(as_text=True)
        for name in ("cost_0", "price_0"):
            snip = html[html.find(f'name="{name}"') : html.find(f'name="{name}"') + 600]
            assert "sanitizePrice(this)" in snip, f"{name} missing oninput sanitizer"
            assert "fmtCurrency(this)" in snip, f"{name} missing onblur formatter"


# ──────────────────────────────────────────────────────────────────────
# shared_item_utils.js element-aware helper overload
# ──────────────────────────────────────────────────────────────────────

class TestSharedItemUtilsElementOverload:
    """Pin the helper overload so a future revert breaks loudly.

    sanitizePrice/fmtCurrency were value-in/value-out, but every call site
    in pc_detail.html + routes_pricecheck.py + the new rfq_detail.html
    passes `this` (an HTMLInputElement). Pre-fix the return value was
    discarded — silent no-op. Post-fix the helper detects the element
    and mutates el.value in place.
    """

    SHARED_JS = REPO_ROOT / "src" / "static" / "shared_item_utils.js"

    def test_helper_file_exists(self):
        assert self.SHARED_JS.exists(), f"missing {self.SHARED_JS}"

    def test_sanitizePrice_detects_input_element(self):
        src = self.SHARED_JS.read_text(encoding="utf-8")
        # Must check tagName + 'value' to detect an HTMLInputElement.
        # We don't pin exact prose, just the substantive pieces.
        sp_idx = src.find("function sanitizePrice")
        assert sp_idx >= 0
        sp_body = src[sp_idx : sp_idx + 1500]
        assert "tagName" in sp_body, (
            "sanitizePrice must detect HTMLInputElement via tagName — "
            "otherwise oninput=\"sanitizePrice(this)\" is a silent no-op"
        )
        assert "v.value = cleaned" in sp_body, (
            "sanitizePrice(el) must mutate el.value in place"
        )

    def test_fmtCurrency_writes_two_decimals_to_input(self):
        src = self.SHARED_JS.read_text(encoding="utf-8")
        fc_idx = src.find("function fmtCurrency")
        assert fc_idx >= 0
        fc_body = src[fc_idx : fc_idx + 1500]
        assert "tagName" in fc_body, (
            "fmtCurrency must detect HTMLInputElement — that's the whole point"
        )
        assert ".toFixed(2)" in fc_body, (
            "fmtCurrency(el) must write 2-decimal padded value"
        )
        assert "v.value = nEl.toFixed(2)" in fc_body, (
            "fmtCurrency(el) must assign to el.value (not discard the return)"
        )

    def test_value_in_signature_preserved(self):
        """Value-in/value-out callers (templates that pass a number for display)
        must still work — only the element-call path is new."""
        src = self.SHARED_JS.read_text(encoding="utf-8")
        fc_body = src[src.find("function fmtCurrency") : src.find("function fmtCurrency") + 1500]
        # When called with a number, must still return "$x.xx" or em-dash
        assert "toLocaleString" in fc_body
        assert "'$'" in fc_body or '"$"' in fc_body


# ──────────────────────────────────────────────────────────────────────
# Edit-doc dialog (routes_pricecheck_pricing.py ed_price input)
# ──────────────────────────────────────────────────────────────────────

class TestEditDocPriceInput:
    """Same bug class lives in the edit-doc dialog `ed_price_{i}` input.
    Server formats value as :.2f but the input was type=number which
    strips the trailing zero on render. Pin the type swap."""

    ROUTE_FILE = REPO_ROOT / "src" / "api" / "modules" / "routes_pricecheck_pricing.py"

    def test_ed_price_input_uses_text_inputmode_decimal(self):
        src = self.ROUTE_FILE.read_text(encoding="utf-8")
        # find the ed_price_ input line
        idx = src.find('name="ed_price_{i}"')
        assert idx >= 0, "ed_price_{i} input not found"
        line_start = src.rfind("\n", 0, idx) + 1
        line_end = src.find("\n", idx)
        line = src[line_start:line_end]
        assert 'type="text"' in line, f"ed_price must be type=text; got: {line}"
        assert 'inputmode="decimal"' in line, f"ed_price needs inputmode=decimal; got: {line}"
        assert 'type="number"' not in line, "ed_price must NOT be type=number"

    def test_ed_price_input_wires_formatter(self):
        src = self.ROUTE_FILE.read_text(encoding="utf-8")
        idx = src.find('name="ed_price_{i}"')
        line_start = src.rfind("\n", 0, idx) + 1
        line_end = src.find("\n", idx)
        line = src[line_start:line_end]
        assert "sanitizePrice(this)" in line, "ed_price missing oninput sanitizer"
        assert "fmtCurrency(this)" in line, "ed_price missing onblur formatter"
