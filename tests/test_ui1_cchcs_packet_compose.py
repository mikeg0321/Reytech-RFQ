"""UI-1 P0 regression guards — CCHCS packet shows up as its own compose
attachment.

After CC-2 split the packet output into its own slot
(`cchcs_packet_output_pdf`), the compose panel at pc_detail.html only
read `pc.output_pdf` and mislabeled whatever it found there as "AMS 704".
If both a packet and a 704 existed, the packet was invisible to the send
flow; if only the packet existed, it was attached under the wrong label.

UI-1 fixes:
  1. routes_pricecheck.py resolves `_existing_packet_url` from the
     dedicated slot and skips the packet path when probing for a 704.
  2. pc_detail.html renders a separate "CCHCS Packet" attachment row.
  3. _refreshComposeAttachments accepts a third arg (newPacketUrl).
  4. generateCchcsPacket calls _refreshComposeAttachments on success so
     the packet shows up without a page reload.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_PC = ROOT / "src" / "api" / "modules" / "routes_pricecheck.py"
TEMPLATE = ROOT / "src" / "templates" / "pc_detail.html"


def test_routes_pricecheck_resolves_existing_packet_url():
    src = ROUTES_PC.read_text(encoding="utf-8")
    assert "_existing_packet_url" in src, (
        "UI-1: routes_pricecheck must resolve _existing_packet_url for "
        "the compose panel"
    )
    assert 'pc.get("cchcs_packet_output_pdf")' in src, (
        "UI-1: must read the dedicated cchcs_packet_output_pdf slot"
    )


def test_routes_pricecheck_skips_packet_when_probing_704():
    """If CC-2's setdefault landed the packet path into pc.output_pdf,
    the 704 probe must not pick it up — otherwise the packet gets
    double-surfaced under the wrong label."""
    src = ROUTES_PC.read_text(encoding="utf-8")
    # Find the 704 probe loop body
    m = re.search(
        r'for _op_key in \("output_pdf", "original_pdf"\):(.*?)break',
        src, re.DOTALL,
    )
    assert m, "UI-1: could not locate 704 probe loop"
    body = m.group(1)
    assert "_op != _pk" in body, (
        "UI-1: 704 probe must skip entries that match the packet path "
        "(_op != _pk) so the packet isn't mislabeled as AMS 704"
    )


def test_routes_pricecheck_passes_packet_url_to_template():
    src = ROUTES_PC.read_text(encoding="utf-8")
    assert "existing_packet_url=_existing_packet_url" in src, (
        "UI-1: existing_packet_url must be passed to the template"
    )


def test_template_reads_packet_slot():
    src = TEMPLATE.read_text(encoding="utf-8")
    assert "cchcs_packet_output_pdf" in src, (
        "UI-1: compose panel must read the dedicated packet slot"
    )
    assert "packetPdf" in src, (
        "UI-1: template must expose a packetPdf JS var"
    )
    assert "existing_packet_url" in src, (
        "UI-1: template must accept the existing_packet_url Jinja var"
    )


def test_template_renders_cchcs_packet_attachment_row():
    src = TEMPLATE.read_text(encoding="utf-8")
    assert "'CCHCS Packet'" in src, (
        "UI-1: compose panel must render a 'CCHCS Packet' attachment "
        "label (distinct from 'AMS 704')"
    )


def test_template_skips_704_row_when_output_pdf_equals_packet():
    """Guard against the CC-2 setdefault double-surfacing the packet."""
    src = TEMPLATE.read_text(encoding="utf-8")
    assert "outputPdf !== packetPdf" in src, (
        "UI-1: 704 attachment row must be suppressed when "
        "output_pdf is actually the packet path"
    )


def test_refresh_hook_accepts_packet_url_arg():
    src = TEMPLATE.read_text(encoding="utf-8")
    assert "function(newQuoteUrl, new704Url, newPacketUrl)" in src, (
        "UI-1: _refreshComposeAttachments must accept a third arg "
        "(newPacketUrl) so the packet generator can refresh attachments"
    )


def test_generate_cchcs_packet_refreshes_attachments():
    """The generator must push the fresh packet URL into the compose
    panel so the operator doesn't have to reload the page."""
    src = TEMPLATE.read_text(encoding="utf-8")
    # Narrow to the generator function body
    m = re.search(
        r"function generateCchcsPacket\(btn\) \{(.*?)\n    \}",
        src, re.DOTALL,
    )
    assert m, "UI-1: could not locate generateCchcsPacket body"
    body = m.group(1)
    assert "_refreshComposeAttachments" in body, (
        "UI-1: generateCchcsPacket must call _refreshComposeAttachments "
        "on success so the packet surfaces in the compose panel"
    )
    assert "d.download_url" in body, (
        "UI-1: the packet download_url from the response must be used"
    )
