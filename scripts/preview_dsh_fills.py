"""Generate sample filled DSH AttA/B/C PDFs for visual verification."""
from __future__ import annotations

import json
import os
from pathlib import Path

from src.forms.dsh_attachment_fillers import (
    fill_dsh_attachment_a,
    fill_dsh_attachment_b,
    fill_dsh_attachment_c,
)

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "artifacts" / "dsh_fills"
OUT.mkdir(parents=True, exist_ok=True)

REYTECH = json.loads((ROOT / "src" / "forms" / "reytech_config.json").read_text())

PARSED = {
    "header": {"solicitation_number": "25CB020"},
    "sol_expires": "03/30/2026",
    "lead_time": "5-7 business days",
    "warranty": "Per manufacturer (1-year limited)",
    "dvbe_pct": "100%",
    "items": [
        {"qty": 50,  "unit_price": 25.50},
        {"qty": 300, "unit_price": 24.10},
        {"qty": 300, "unit_price": 24.10},
        {"qty": 300, "unit_price": 24.10},
        {"qty": 50,  "unit_price": 18.75},
        {"qty": 50,  "unit_price": 18.75},
        {"qty": 50,  "unit_price": 18.75},
    ],
}

FIX = ROOT / "tests" / "fixtures" / "dsh"

for name, src, fn in (
    ("AttA", FIX / "dsh_25CB020_attachA_bidder.pdf",  fill_dsh_attachment_a),
    ("AttB", FIX / "dsh_25CB020_attachB_pricing.pdf", fill_dsh_attachment_b),
    ("AttC", FIX / "dsh_25CB020_attachC_forms.pdf",   fill_dsh_attachment_c),
):
    buf = fn(REYTECH, PARSED, src_pdf=str(src))
    out = OUT / f"{name}_filled.pdf"
    out.write_bytes(buf.getvalue())
    print(f"wrote {out} ({len(buf.getvalue())} bytes)")
