"""Tests for the SVG progress ring + utility macros (Batch H).

The macro lives in src/templates/_components.html. It's pure Jinja and is
imported into home.html (and any future page that wants a circular indicator
or a tw-card stat). These tests:

  - Render the macro through Jinja directly so we can assert SVG output
    even when Flask isn't available.
  - Exercise edge cases (0%, 100%, >100%, custom size/colour) that have
    bitten hand-rolled SVG attempts before — overflow arc rendering,
    aria-valuenow rounding, dashoffset stays inside the dasharray.
  - Render home.html through the actual Flask client to catch import
    errors or missing-variable crashes.
"""
import os

import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape


_TEMPLATES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src", "templates",
)


@pytest.fixture(scope="module")
def env():
    return Environment(
        loader=FileSystemLoader(_TEMPLATES),
        autoescape=select_autoescape(["html"]),
    )


@pytest.fixture(scope="module")
def ring(env):
    """Bind the progress_ring macro for direct calls."""
    template = env.from_string(
        "{% import '_components.html' as ui %}"
        "{{ ui.progress_ring(value=value, max=max, size=size,"
        " color=color, label=label, sublabel=sublabel, id=id) }}"
    )

    def _render(value=0, max=100, size=56, color="var(--ac)",
                label="", sublabel="", id=""):
        return template.render(
            value=value, max=max, size=size, color=color,
            label=label, sublabel=sublabel, id=id,
        )
    return _render


class TestProgressRingMacro:

    def test_renders_two_circles(self, ring):
        out = ring(value=42, max=100)
        # Two circles: track + fill
        assert out.count("<circle") == 2
        assert "ring-fill" in out

    def test_default_label_shows_percent(self, ring):
        out = ring(value=42, max=100)
        assert "42%" in out

    def test_custom_label_overrides_percent(self, ring):
        out = ring(value=42, max=100, label="Pricing")
        assert "Pricing" in out
        # When a custom label is set, the auto "%" string should not appear
        # in the centre block (it may still appear elsewhere — assert only
        # that the label text itself made it into the markup).
        assert "<div class=\"ring-label\">Pricing</div>" in out

    def test_zero_offset_at_full(self, ring):
        out = ring(value=100, max=100)
        # At 100% the dashoffset should be ~0 (arc fully drawn).
        # Pull the offset attribute out and check it's near zero.
        import re
        m = re.search(r'stroke-dashoffset="([0-9.]+)"', out)
        assert m, "stroke-dashoffset attribute missing"
        assert float(m.group(1)) < 0.5

    def test_full_offset_at_zero(self, ring):
        out = ring(value=0, max=100)
        import re
        m_arr = re.search(r'stroke-dasharray="([0-9.]+)"', out)
        m_off = re.search(r'stroke-dashoffset="([0-9.]+)"', out)
        assert m_arr and m_off
        # Empty ring — offset should equal the dasharray (no fill drawn)
        assert abs(float(m_off.group(1)) - float(m_arr.group(1))) < 0.5

    def test_overflow_clamps_to_110_percent(self, ring):
        """If the caller passes value=150% we don't want a negative
        dashoffset producing a wraparound arc — clamp to 110%."""
        out = ring(value=150, max=100)
        import re
        m_off = re.search(r'stroke-dashoffset="(-?[0-9.]+)"', out)
        assert m_off
        # 110% clamp ⇒ offset = circ * (1 - 1.10) = -0.10 * circ. For the
        # default size (56), circ ≈ 158, so the clamped offset is about -16.
        # Real wraparound (no clamp) would be ≈ -circ. Assert the magnitude
        # is small relative to the dasharray.
        m_arr = re.search(r'stroke-dasharray="([0-9.]+)"', out)
        circ = float(m_arr.group(1))
        offset = float(m_off.group(1))
        assert offset < 0
        assert abs(offset) < 0.25 * circ, (
            f"clamp failed: offset {offset} is more than 25% of circ {circ}"
        )

    def test_negative_value_clamped_to_zero(self, ring):
        """Defensive: a negative computed value (data bug) shouldn't
        produce a positive offset > dasharray."""
        out = ring(value=-25, max=100)
        import re
        m_arr = re.search(r'stroke-dasharray="([0-9.]+)"', out)
        m_off = re.search(r'stroke-dashoffset="([0-9.]+)"', out)
        assert m_arr and m_off
        assert float(m_off.group(1)) <= float(m_arr.group(1)) + 0.1

    def test_zero_max_does_not_divide_by_zero(self, ring):
        """A row with max=0 is malformed but the macro should still
        render — the 'no data' state shouldn't 500."""
        out = ring(value=0, max=0)
        assert "<svg" in out  # didn't crash

    def test_size_scales_stroke(self, ring):
        small = ring(size=24)
        big = ring(size=120)
        # Bigger ring ⇒ thicker stroke. Pull the first stroke-width.
        import re
        s_small = int(re.search(r'stroke-width="(\d+)"', small).group(1))
        s_big = int(re.search(r'stroke-width="(\d+)"', big).group(1))
        assert s_big > s_small

    def test_aria_attributes_present(self, ring):
        out = ring(value=33, max=100, label="Goal")
        assert 'role="progressbar"' in out
        assert 'aria-valuenow="33"' in out
        assert 'aria-valuemin="0"' in out
        assert 'aria-valuemax="100"' in out
        assert 'aria-label="Goal"' in out

    def test_id_attribute_attached(self, ring):
        out = ring(id="goal-ring")
        assert 'id="goal-ring"' in out


class TestStatCardMacro:

    def test_renders_label_and_value(self, env):
        tpl = env.from_string(
            "{% import '_components.html' as ui %}"
            "{{ ui.stat_card('Revenue', '$12.3K', sub='this month') }}"
        )
        out = tpl.render()
        assert "Revenue" in out
        assert "$12.3K" in out
        assert "this month" in out
        assert "tw-card" in out


class TestHomeRenderWithRing:
    """Render the actual home.html through Flask to catch import bugs
    or missing-fixture crashes the macro might introduce."""

    def test_home_renders_without_errors(self, auth_client):
        resp = auth_client.get("/")
        assert resp.status_code == 200
        body = resp.get_data(as_text=True)
        # The new ring should be present in the markup
        assert 'id="goal-ring"' in body
        assert "ring-fill" in body
