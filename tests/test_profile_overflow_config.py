"""Profile `overflow:` block — plumbing + silent-drop guard.

Problem this addresses: until now the fill engine iterated `page_row_capacities`
and silently dropped any items past the last row slot. For cchcs_it_rfq (10-row
cap) that means an 11-item RFQ shipped with item 11 invisible — catastrophic
bid error with no log, no error, no sign.

Fix in this PR: parse the YAML `overflow:` block into `FormProfile.overflow`,
add `effective_page_capacities()` that extends the capacity list for
`duplicate_page` mode, and add an up-front guard in `_fill_acroform` that
refuses to fill when items exceed capacity without a declared overflow mode,
or when a declared mode isn't yet implemented.

Scope boundary: this PR does NOT implement the actual page duplication —
`_fill_acroform` raises `NotImplementedError` when `overflow.mode ==
"duplicate_page"` and items exceed capacity. PR #317 will implement the
cloning (non-trivial: the cchcs_it_rfq blank has hierarchical identity
fields and the row page is actually page 5, not page 1 as the YAML
currently declares — both need fixture work).
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from src.core.quote_model import Quote, LineItem, QuoteHeader, DocType
from src.forms.fill_engine import _fill_acroform
from src.forms.profile_registry import FormProfile, FieldMapping, load_profile, load_profiles


class TestLoadOverflow:
    def test_overflow_extracted_from_yaml(self, tmp_path):
        yml = tmp_path / "x.yaml"
        yml.write_text(
            "id: t\n"
            "form_type: t\n"
            "blank_pdf: n.pdf\n"
            "fill_mode: acroform\n"
            "page_row_capacities: [10]\n"
            "overflow:\n"
            "  mode: duplicate_page\n"
            "  source_page: 1\n"
            "  row_field_suffix_pattern: '_{page}'\n"
            "fields:\n"
            "  vendor.name: {pdf_field: 'S'}\n",
            encoding="utf-8",
        )
        p = load_profile(str(yml))
        assert p.overflow == {
            "mode": "duplicate_page",
            "source_page": 1,
            "row_field_suffix_pattern": "_{page}",
        }

    def test_missing_overflow_is_empty_dict(self, tmp_path):
        yml = tmp_path / "x.yaml"
        yml.write_text(
            "id: t\n"
            "form_type: t\n"
            "blank_pdf: n.pdf\n"
            "fill_mode: acroform\n"
            "fields:\n"
            "  vendor.name: {pdf_field: 'S'}\n",
            encoding="utf-8",
        )
        p = load_profile(str(yml))
        assert p.overflow == {}

    def test_real_cchcs_it_rfq_profile_has_duplicate_page_overflow(self):
        """Sanity: the production cchcs_it_rfq YAML declares duplicate_page."""
        profiles = load_profiles()
        p = profiles.get("cchcs_it_rfq_reytech_standard")
        if p is None:
            pytest.skip("cchcs_it_rfq profile not in registry")
        assert p.overflow.get("mode") == "duplicate_page"
        assert p.page_row_capacities == [10]


class TestEffectivePageCapacities:
    def _profile(self, caps, overflow=None):
        return FormProfile(
            id="t", form_type="t", blank_pdf="", fill_mode="acroform",
            page_row_capacities=caps, fields=[],
            overflow=overflow or {},
        )

    def test_base_returned_when_items_fit(self):
        p = self._profile([10])
        assert p.effective_page_capacities(5) == [10]
        assert p.effective_page_capacities(10) == [10]

    def test_extends_for_duplicate_page(self):
        p = self._profile(
            [10],
            overflow={"mode": "duplicate_page", "source_page": 1, "row_field_suffix_pattern": "_{page}"},
        )
        assert p.effective_page_capacities(15) == [10, 10]
        assert p.effective_page_capacities(25) == [10, 10, 10]
        assert p.effective_page_capacities(30) == [10, 10, 10]

    def test_no_extension_without_overflow_mode(self):
        """Without an overflow declaration, caller sees the shortfall and
        must decide — don't silently pretend the form fits."""
        p = self._profile([10])  # no overflow
        assert p.effective_page_capacities(25) == [10]

    def test_no_extension_for_unknown_mode(self):
        p = self._profile([10], overflow={"mode": "some_future_thing"})
        assert p.effective_page_capacities(25) == [10]

    def test_source_page_outside_capacity_list_falls_back_to_last_nonzero(self):
        """`source_page` in YAML refers to an absolute 1-indexed PDF page.
        For profiles with one logical row-page (page_row_capacities=[10])
        but row widgets on PDF page 5 (source_page=5), the helper must still
        extend — falling back to the last non-zero base capacity. This is
        the real cchcs_it_rfq shape."""
        p = self._profile([10], overflow={"mode": "duplicate_page", "source_page": 5})
        assert p.effective_page_capacities(25) == [10, 10, 10]
        assert p.effective_page_capacities(15) == [10, 10]

    def test_zero_source_capacity_returns_base(self):
        p = self._profile([0, 10], overflow={"mode": "duplicate_page", "source_page": 1})
        assert p.effective_page_capacities(25) == [0, 10]

    def test_empty_capacities_returns_empty(self):
        p = self._profile([])
        assert p.effective_page_capacities(5) == []


class TestFillAcroformOverflowGuard:
    """The guard runs before any PDF I/O, so we can use a profile with a
    non-existent blank_pdf — the guard fires first."""

    def _profile(self, *, capacity: int, overflow=None):
        return FormProfile(
            id="t", form_type="t",
            # Nonexistent path is fine: the guard runs before fill I/O
            blank_pdf="does-not-matter-for-guard",
            fill_mode="acroform",
            page_row_capacities=[capacity],
            fields=[
                FieldMapping(semantic="items[n].description", pdf_field="Item{n}"),
            ],
            overflow=overflow or {},
        )

    def _quote(self, n_items: int) -> Quote:
        items = [
            LineItem(line_no=i + 1, description=f"Item {i + 1}", qty=1, unit_cost=Decimal("1.00"))
            for i in range(n_items)
        ]
        return Quote(doc_type=DocType.PC, header=QuoteHeader(), line_items=items)

    def test_no_overflow_declared_raises_runtime_error(self):
        """11 items, 10-row cap, no overflow block → hard error so the caller
        can't ship a quote silently missing item 11."""
        p = self._profile(capacity=10)
        q = self._quote(11)
        with pytest.raises(RuntimeError, match="exceed row-field capacity 10"):
            _fill_acroform(q, p)

    def test_duplicate_page_declared_does_not_trip_guard(self):
        """PR #317 implements duplicate_page. The guard must pass it through
        to the actual fill path; we prove this by asserting the failure comes
        from downstream PDF I/O (missing blank), not from the guard or a
        capability-gap NotImplementedError."""
        p = self._profile(
            capacity=10,
            overflow={"mode": "duplicate_page", "source_page": 1, "row_field_suffix_pattern": "_{page}"},
        )
        q = self._quote(15)
        with pytest.raises(Exception) as exc_info:
            _fill_acroform(q, p)
        assert not isinstance(exc_info.value, NotImplementedError), (
            "duplicate_page is implemented — guard should not raise NotImplementedError"
        )
        msg = str(exc_info.value)
        assert "exceed row-field capacity" not in msg
        assert "does-not-matter-for-guard" in msg or "No such file" in msg

    def test_unknown_overflow_mode_raises_runtime_error(self):
        p = self._profile(capacity=10, overflow={"mode": "some_future_thing"})
        q = self._quote(15)
        with pytest.raises(RuntimeError, match="not supported"):
            _fill_acroform(q, p)

    def test_no_bid_items_dont_count_toward_capacity(self):
        """Items with no_bid=True are skipped by the fill path, so they
        shouldn't push total over capacity. 11 items with one marked no_bid
        = 10 active. The guard must NOT fire — we prove that by asserting
        the failure comes from downstream PDF I/O (missing blank), not the
        guard's own 'exceed row-field capacity' message."""
        p = self._profile(capacity=10)
        q = self._quote(11)
        q.line_items[5].no_bid = True
        with pytest.raises(Exception) as exc_info:
            _fill_acroform(q, p)
        # Guard message would contain "exceed row-field capacity";
        # I/O failure message mentions the missing file.
        msg = str(exc_info.value)
        assert "exceed row-field capacity" not in msg, (
            f"guard fired on 10 active items (capacity 10): {msg}"
        )
        assert "does-not-matter-for-guard" in msg or "No such file" in msg

    def test_items_within_capacity_pass_guard(self):
        """Exactly at capacity should not trip the guard."""
        p = self._profile(capacity=10)
        q = self._quote(10)
        with pytest.raises(Exception) as exc_info:
            _fill_acroform(q, p)
        msg = str(exc_info.value)
        assert "exceed row-field capacity" not in msg, (
            f"guard fired at exactly capacity: {msg}"
        )
        assert "does-not-matter-for-guard" in msg or "No such file" in msg
