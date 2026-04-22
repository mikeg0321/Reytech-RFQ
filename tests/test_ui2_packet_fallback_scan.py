"""UI-2 P0 regression guards — DATA_DIR fallback scan excludes packets.

After UI-1 (PR #387) added `_existing_packet_url` from the dedicated
`cchcs_packet_output_pdf` slot, the DATA_DIR fallback scan at
routes_pricecheck.py:~1305 was still free to pick up packet PDFs by
filename. CCHCS packet outputs are named `<source>_Reytech.pdf` (see
cchcs_packet_filler._output_path), which makes the scan's
`if "Reytech" in _f` branch assign the packet path to
`_existing_quote_url` — mislabeling it as "Reytech Quote" in the
compose panel.

Fix: skip any file whose basename matches the already-resolved packet
basename in the DATA_DIR loop.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PC = ROOT / "src" / "api" / "modules" / "routes_pricecheck.py"


def _fallback_scan_body() -> str:
    """Extract the DATA_DIR fallback loop body."""
    src = ROUTES_PC.read_text(encoding="utf-8")
    m = re.search(
        r"# Fallback: scan DATA_DIR(.*?)(?:# SCPRS data staleness|# ── Bundle)",
        src, re.DOTALL,
    )
    assert m, "UI-2: could not locate the DATA_DIR fallback scan block"
    return m.group(1)


def test_packet_basename_is_computed_before_scan():
    """The packet basename must be captured before the scan loop runs,
    so every branch inside the loop can check against it."""
    src = ROUTES_PC.read_text(encoding="utf-8")
    assert "_packet_basename" in src, (
        "UI-2: _packet_basename must be computed from _pk so the scan "
        "can skip the packet file"
    )
    # Must appear BEFORE the DATA_DIR listdir loop
    idx_basename = src.find("_packet_basename = os.path.basename")
    idx_listdir = src.find("for _f in os.listdir(DATA_DIR):")
    assert idx_basename > 0 and idx_listdir > idx_basename, (
        "UI-2: _packet_basename must be computed BEFORE the scan loop"
    )


def test_scan_skips_packet_file():
    body = _fallback_scan_body()
    assert "_packet_basename and _f == _packet_basename" in body, (
        "UI-2: the DATA_DIR scan must skip files whose basename matches "
        "the resolved packet basename, or packets get mislabeled as "
        "'Reytech Quote' via the 'Reytech' substring branch"
    )


def test_scan_skip_happens_before_reytech_branch():
    """The packet-skip continue must execute before the 'Reytech in _f'
    branch, otherwise the packet still falls into the quote slot."""
    body = _fallback_scan_body()
    idx_skip = body.find("_f == _packet_basename")
    idx_reytech = body.find('"Reytech" in _f')
    assert idx_skip > 0, "UI-2: packet-skip guard missing"
    assert idx_reytech > 0, "UI-2: reytech-branch expected in scan"
    assert idx_skip < idx_reytech, (
        "UI-2: the packet-skip continue must run BEFORE the "
        "'Reytech in _f' branch so a packet doesn't get classified "
        "as a Reytech quote before the skip fires"
    )


def test_skip_is_noop_when_no_packet():
    """When _pk is empty, _packet_basename is '' and the guard `if
    _packet_basename and _f == _packet_basename` must short-circuit
    to false. Guard against accidentally changing the guard to
    `if _f == _packet_basename` (would skip every empty-basename case
    on some platforms) or dropping the truthiness check."""
    body = _fallback_scan_body()
    assert "if _packet_basename and _f == _packet_basename" in body, (
        "UI-2: the guard must be `if _packet_basename and _f == _packet_basename` "
        "so the skip is a no-op when no packet is present"
    )
