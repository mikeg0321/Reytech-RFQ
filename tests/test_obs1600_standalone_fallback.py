"""
Precondition for PR-8 (replace contaminated legacy bidpkg fallback).

The genuine 16-page CCHCS bid-package blank (`tests/fixtures/cchcs_bidpkg_blank.pdf`)
does NOT contain an OBS 1600 page (the contaminated 14-page file did, at pages 3-4).

When the swap lands, `api_generate_obs1600` will no longer find OBS 1600 form
fields inside the bidpkg and will fall through to `_generate_standalone_obs1600`
(the reportlab standalone path). That fallback has been effectively dead code in
prod because the contaminated 14-page file always satisfied the in-bidpkg branch.

This test proves the standalone fallback produces a valid OBS 1600 PDF BEFORE
the swap is made. If this test fails, do NOT proceed with the swap — fix the
fallback first.

Architect-authorized per ticket D (PR-8), with Closer's precondition that the
standalone path must be confirmed working pre-swap.
"""

from __future__ import annotations

import os
import tempfile

import pypdf
import pytest


def _minimal_food_items():
    """Minimal but realistic food-line set covering CA-grown Yes/No/N/A."""
    return [
        {
            "line_number": 1,
            "description": "Cherry Tomato Grape 1lb Clamshell, USA Grown",
            "code": "53101602",
            "ca_grown": "Yes",
            "pct": "100%",
        },
        {
            "line_number": 2,
            "description": "Iceberg Lettuce, Whole Head, Field-Packed",
            "code": "50281800",
            "ca_grown": "Yes",
            "pct": "100%",
        },
        {
            "line_number": 3,
            "description": "Russet Potatoes, #2 Size, 50lb Sack",
            "code": "50221103",
            "ca_grown": "No",
            "pct": "N/A",
        },
    ]


def _minimal_config():
    """Mirrors the shape `_generate_standalone_obs1600` reads from `load_config()`."""
    return {
        "company": {
            "name": "Reytech Inc.",
            "owner": "Michael Guadan",
            "title": "Owner",
        }
    }


def test_standalone_obs1600_produces_valid_pdf(tmp_path):
    """The reportlab fallback must produce a parseable, non-trivial PDF."""
    from src.api.modules.routes_rfq_admin import _generate_standalone_obs1600

    food_items = _minimal_food_items()
    config = _minimal_config()
    rfq_data = {
        "solicitation_number": "TEST-OBS1600-FALLBACK",
        "sign_date": "05/22/2026",
        "line_items": [],
    }
    output_path = str(tmp_path / "obs1600_standalone.pdf")

    _generate_standalone_obs1600(food_items, config, rfq_data, output_path)

    assert os.path.exists(output_path), "standalone fallback did not write a file"

    size = os.path.getsize(output_path)
    assert size > 1500, f"standalone OBS 1600 suspiciously small ({size} bytes)"

    with open(output_path, "rb") as f:
        header = f.read(5)
    assert header == b"%PDF-", f"output is not a PDF (header={header!r})"

    # pypdf must be able to open it (not a corrupted write).
    reader = pypdf.PdfReader(output_path)
    assert len(reader.pages) >= 1, "OBS 1600 standalone produced 0 pages"


def test_standalone_obs1600_contains_key_markers(tmp_path):
    """The rendered PDF must contain the form identifier, vendor name, and sol#."""
    import pdfplumber

    from src.api.modules.routes_rfq_admin import _generate_standalone_obs1600

    food_items = _minimal_food_items()
    config = _minimal_config()
    sol = "TEST-MARKERS-12345"
    rfq_data = {
        "solicitation_number": sol,
        "sign_date": "05/22/2026",
        "line_items": [],
    }
    output_path = str(tmp_path / "obs1600_markers.pdf")

    _generate_standalone_obs1600(food_items, config, rfq_data, output_path)

    with pdfplumber.open(output_path) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    assert "OBS 1600" in text, "form identifier 'OBS 1600' missing from output"
    assert "Reytech Inc." in text, "vendor name missing from output"
    assert sol in text, f"solicitation # {sol!r} missing from output"
    assert "California-Grown" in text, "form title missing from output"


def test_standalone_obs1600_renders_food_rows(tmp_path):
    """Each food item's line_number and description must appear in the table."""
    import pdfplumber

    from src.api.modules.routes_rfq_admin import _generate_standalone_obs1600

    food_items = _minimal_food_items()
    config = _minimal_config()
    rfq_data = {
        "solicitation_number": "TEST-ROWS",
        "sign_date": "05/22/2026",
        "line_items": [],
    }
    output_path = str(tmp_path / "obs1600_rows.pdf")

    _generate_standalone_obs1600(food_items, config, rfq_data, output_path)

    with pdfplumber.open(output_path) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    for item in food_items:
        # description is truncated to 55 chars in the renderer
        desc_fragment = item["description"][:30]
        assert desc_fragment in text, f"food row missing: {desc_fragment!r}"
        assert str(item["code"]) in text, f"code missing: {item['code']!r}"


def test_standalone_obs1600_empty_food_list_still_renders(tmp_path):
    """Zero food items is an edge case — the function must not crash and must
    still produce a valid PDF (empty table, signature block intact)."""
    from src.api.modules.routes_rfq_admin import _generate_standalone_obs1600

    config = _minimal_config()
    rfq_data = {
        "solicitation_number": "TEST-EMPTY",
        "sign_date": "05/22/2026",
        "line_items": [],
    }
    output_path = str(tmp_path / "obs1600_empty.pdf")

    _generate_standalone_obs1600([], config, rfq_data, output_path)

    assert os.path.exists(output_path)
    reader = pypdf.PdfReader(output_path)
    assert len(reader.pages) >= 1
