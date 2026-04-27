"""Phase 4.7.2: lock in the swap-link JS in pc_detail.html.

When the oracle-auto-price endpoint returns
recommendation.category_intel.suggested_alternative, the price cell
must render a "↓ swap to $X.XX" link under the price input that
sets the value on click.

Pure string-presence guards (we can't run JS from pytest). The
visual verification is the operator's job in Chrome.
"""

from pathlib import Path

PC_DETAIL = (Path(__file__).resolve().parent.parent / "src"
             / "templates" / "pc_detail.html")
ROUTES = (Path(__file__).resolve().parent.parent / "src" / "api"
          / "modules" / "routes_pricecheck_admin.py")


class TestBackendSurfacesCategoryIntel:
    def test_oracle_auto_price_includes_category_intel_field(self):
        content = ROUTES.read_text(encoding="utf-8")
        # The item_recs.append() block must include category_intel
        assert '"category_intel": cat_intel,' in content

    def test_pulls_from_recommendation(self):
        content = ROUTES.read_text(encoding="utf-8")
        assert 'cat_intel = rec.get("category_intel")' in content


class TestSwapLinkRenders:
    def setup_method(self):
        self.content = PC_DETAIL.read_text(encoding="utf-8")

    def test_reads_category_intel_from_item(self):
        assert "var ci = it.category_intel || null;" in self.content

    def test_extracts_suggested_alternative_price(self):
        assert "ci.suggested_alternative.quote_price" in self.content

    def test_skips_if_alt_equals_engine(self):
        # Don't show swap link when alt == engine (no actionable diff)
        assert "Math.abs(altPrice - it.recommended_price) > 0.01" in self.content

    def test_swap_link_class_present(self):
        # CSS class so the test below can lock the listener wiring
        assert "rt-swap-link" in self.content
        assert "rt-cat-swap" in self.content

    def test_click_handler_swaps_input(self):
        # Click must actually update the price input value
        assert "priceEl.value = altPrice.toFixed(2);" in self.content

    def test_recalcs_markup_after_swap(self):
        # And update the markup field if cost is known
        assert "((altPrice - actualCost) / actualCost * 100)" in self.content

    def test_clears_prior_swap_link_on_repeat(self):
        # A re-run of oracleAutoPrice() shouldn't accumulate links
        assert ".rt-cat-swap" in self.content
        assert "if (prior) prior.remove();" in self.content

    def test_logs_acceptance_to_telemetry_endpoint(self):
        # Phase 4.7.3 wiring — clicking the swap link must POST to
        # /api/oracle/intel-acceptance so we capture the decision
        assert "/api/oracle/intel-acceptance" in self.content
        # Fire-and-forget — must wrap in try/catch so failure can't
        # block the UI swap
        assert "accepted: true" in self.content
        assert ".catch(function(_){})" in self.content
