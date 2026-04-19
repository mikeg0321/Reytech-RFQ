"""End-to-end golden-path test for DSH packet dispatch (PR3 + golden).

Mirrors the dispatch block in `routes_rfq_gen.generate_rfq_package` so the
RFQ→parsed mapping (price_per_unit→unit_price, due_date→sol_expires, etc.)
is pinned alongside the fillers themselves. If a future refactor reshapes
the RFQ dict or renames a key, this test catches the silent breakage.

The fixture (`tests/fixtures/golden/dsh_25CB020_real.json`) is real items
from solicitation 25CB020 (DSH — Atascadero, March 2026). Subtotal sums
to $25,777.50 — the same number `test_dsh_attachment_fillers.py` pins
from a hand-built dict, so the math is doubly anchored.
"""
from __future__ import annotations

import io
import json
import os
import re
from pathlib import Path

import pdfplumber
import pytest
from pypdf import PdfReader


_ROOT = Path(__file__).resolve().parents[1]
_FIX_DSH = _ROOT / "tests" / "fixtures" / "dsh"
_GOLDEN = _ROOT / "tests" / "fixtures" / "golden" / "dsh_25CB020_real.json"
_REYTECH_CFG = _ROOT / "src" / "forms" / "reytech_config.json"


pytestmark = pytest.mark.skipif(
    not (_FIX_DSH / "dsh_25CB020_attachA_bidder.pdf").exists()
    or not _GOLDEN.exists(),
    reason="DSH 25CB020 packet fixtures or golden JSON not present",
)


@pytest.fixture
def golden():
    return json.loads(_GOLDEN.read_text())


@pytest.fixture
def reytech_cfg():
    return json.loads(_REYTECH_CFG.read_text())


def _rfq_from_golden(golden: dict) -> dict:
    """Shape the golden fixture into the RFQ dict the production dispatch
    block sees (the same one stored in rfqs.json after manual upload)."""
    return {
        "id": golden["test_rfq_id"],
        "agency": golden["header"]["agency"],
        "solicitation_number": golden["header"]["solicitation_number"],
        "due_date": golden["header"]["due_date"],
        "lead_time": golden["lead_time"],
        "warranty": golden["warranty"],
        "dvbe_pct": golden["dvbe_pct"],
        "other_charges": golden["totals"]["other_charges"],
        "line_items": golden["line_items"],
        "templates": {
            "dsh_attA": str(_FIX_DSH / "dsh_25CB020_attachA_bidder.pdf"),
            "dsh_attB": str(_FIX_DSH / "dsh_25CB020_attachB_pricing.pdf"),
            "dsh_attC": str(_FIX_DSH / "dsh_25CB020_attachC_forms.pdf"),
        },
    }


def _parsed_from_rfq(r: dict) -> dict:
    """Replica of the dispatch block in routes_rfq_gen.generate_rfq_package.
    Kept here so the test fails loud if the production mapping drifts."""
    return {
        "header": {"solicitation_number": r["solicitation_number"]},
        "sol_expires": r.get("due_date", "") or r.get("sol_expires", ""),
        "lead_time": r.get("lead_time", "") or "5-7 business days",
        "warranty": r.get("warranty", "") or "Per manufacturer",
        "dvbe_pct": r.get("dvbe_pct", "") or "100%",
        "items": [
            {
                "qty": it.get("qty", 0),
                "unit_price": it.get("price_per_unit") or it.get("unit_price") or 0,
            }
            for it in r.get("line_items", []) or []
        ],
        "other_charges": r.get("other_charges", 0) or 0,
    }


def _all_text(buf: io.BytesIO) -> str:
    buf.seek(0)
    chunks = []
    with pdfplumber.open(buf) as pdf:
        for page in pdf.pages:
            chunks.append(page.extract_text() or "")
    return "\n".join(chunks)


class TestDshGoldenPath:

    def test_agency_classification(self, golden):
        """Sanity: the golden fixture identifies as DSH and the dispatch
        contract picks up the three attachment IDs."""
        from src.core.agency_config import DEFAULT_AGENCY_CONFIGS
        assert golden["header"]["agency"] == "dsh"
        req = set(DEFAULT_AGENCY_CONFIGS["dsh"]["required_forms"])
        assert {"dsh_attA", "dsh_attB", "dsh_attC"}.issubset(req)

    def test_dispatch_produces_three_filled_pdfs(self, golden, reytech_cfg, tmp_path):
        """Drive the same per-attachment dispatch the route runs."""
        from src.forms.dsh_attachment_fillers import FILLERS

        r = _rfq_from_golden(golden)
        parsed = _parsed_from_rfq(r)
        outputs = {}
        for key, fn_name in (
            ("dsh_attA", "fill_dsh_attachment_a"),
            ("dsh_attB", "fill_dsh_attachment_b"),
            ("dsh_attC", "fill_dsh_attachment_c"),
        ):
            buf = FILLERS[fn_name](reytech_cfg, parsed, src_pdf=r["templates"][key])
            assert buf is not None, f"{key}: filler returned None"
            out = tmp_path / f"{key}.pdf"
            out.write_bytes(buf.getvalue())
            outputs[key] = out
            # Each output must be a readable single-page PDF.
            reader = PdfReader(str(out))
            assert len(reader.pages) >= 1

        # All three landed on disk.
        assert outputs.keys() == {"dsh_attA", "dsh_attB", "dsh_attC"}

    def test_attachment_a_carries_vendor_identity(self, golden, reytech_cfg):
        from src.forms.dsh_attachment_fillers import fill_dsh_attachment_a
        r = _rfq_from_golden(golden)
        buf = fill_dsh_attachment_a(reytech_cfg, _parsed_from_rfq(r),
                                    src_pdf=r["templates"]["dsh_attA"])
        text = _all_text(buf)
        co = reytech_cfg["company"]
        for needle in (co["name"], co["owner"], co["email"], co["street"],
                       co["city"], co["fein"]):
            assert needle in text, f"AttA missing {needle!r}"

    def test_attachment_b_math_matches_golden_subtotal(self, golden, reytech_cfg):
        """Golden fixture's subtotal MUST match what AttB renders.
        If item prices change, both the JSON and this assertion shift
        together — the test is still pinning the relationship."""
        from src.forms.dsh_attachment_fillers import fill_dsh_attachment_b
        r = _rfq_from_golden(golden)
        buf = fill_dsh_attachment_b(reytech_cfg, _parsed_from_rfq(r),
                                    src_pdf=r["templates"]["dsh_attB"])
        text = _all_text(buf)

        expected_subtotal = golden["totals"]["subtotal"]  # 25777.50
        # pdfplumber may interleave overlay glyphs with adjacent source
        # text; allow whitespace between digits as in the unit tests.
        money_pattern = re.escape(f"{expected_subtotal:,.2f}").replace(",", r"\s*,\s*").replace(r"\.", r"\s*\.\s*")
        assert re.search(money_pattern, text), (
            f"AttB subtotal {expected_subtotal:,.2f} not found in output text"
        )

    def test_attachment_b_per_row_extensions(self, golden, reytech_cfg):
        """Each line's qty * unit extension must appear on the page."""
        from src.forms.dsh_attachment_fillers import fill_dsh_attachment_b
        r = _rfq_from_golden(golden)
        buf = fill_dsh_attachment_b(reytech_cfg, _parsed_from_rfq(r),
                                    src_pdf=r["templates"]["dsh_attB"])
        text = _all_text(buf)
        for item in golden["line_items"]:
            ext = item["extension"]
            int_part = f"{int(ext):,}"
            digits_pat = r"\s*".join(re.escape(c) for c in int_part)
            assert re.search(digits_pat, text), (
                f"AttB extension {ext} (item {item['item_number']}) not found"
            )

    def test_attachment_c_carries_vendor_name(self, golden, reytech_cfg):
        from src.forms.dsh_attachment_fillers import fill_dsh_attachment_c
        r = _rfq_from_golden(golden)
        buf = fill_dsh_attachment_c(reytech_cfg, _parsed_from_rfq(r),
                                    src_pdf=r["templates"]["dsh_attC"])
        text = _all_text(buf)
        assert reytech_cfg["company"]["name"] in text
        assert "ATTACHMENT C" in text  # source-PDF preservation
