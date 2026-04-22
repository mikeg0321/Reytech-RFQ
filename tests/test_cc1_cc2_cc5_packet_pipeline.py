"""CC-1 + CC-2 + CC-5 regression guards for api_cchcs_packet_generate.

CC-1: filled packets must write to OUTPUT_DIR (persistent), not the
source directory (uploads/). Before the fix output_dir was
os.path.dirname(source_pdf), so outputs landed next to the raw upload.

CC-2: packet output must live in its own PC field
(cchcs_packet_output_pdf) — the generic `output_pdf` is used by the
704 generator and can't be a shared slot without the two clobbering
each other.

CC-5: the generated PDF must be registered in rfq_files via
save_rfq_file, otherwise the /Files tab never sees it.
"""
import re
from pathlib import Path

ROUTES = Path(__file__).resolve().parents[1] / "src" / "api" / "modules" / "routes_cchcs_packet.py"


def _generate_body() -> str:
    src = ROUTES.read_text(encoding="utf-8")
    start = src.find("def api_cchcs_packet_generate(")
    assert start >= 0, "api_cchcs_packet_generate not found"
    rest = src[start:]
    next_def = re.search(r"\n(?:def |@bp\.route)", rest[len("def api_cchcs_packet_generate("):])
    end = len(rest) if not next_def else len("def api_cchcs_packet_generate(") + next_def.start()
    return rest[:end]


def test_module_compiles():
    import py_compile
    py_compile.compile(str(ROUTES), doraise=True)


def test_cc1_output_dir_is_persistent():
    body = _generate_body()
    # Must assign OUTPUT_DIR (the canonical persistent output dir)
    assert "output_dir = OUTPUT_DIR" in body, \
        "CC-1: output_dir must be OUTPUT_DIR (not os.path.dirname(source_pdf))"
    # Must not fall back to writing next to the source
    assert "output_dir = os.path.dirname(source_pdf)" not in body, \
        "CC-1: must not write outputs into the source/upload directory"


def test_cc1_output_dir_import():
    src = ROUTES.read_text(encoding="utf-8")
    assert re.search(r"from src\.core\.paths import[^\n]*\bOUTPUT_DIR\b", src), \
        "CC-1: must import OUTPUT_DIR from src.core.paths"


def test_cc2_dedicated_packet_slot():
    body = _generate_body()
    assert 'pc["cchcs_packet_output_pdf"] = output_path' in body, \
        "CC-2: packet output must land in pc['cchcs_packet_output_pdf']"


def test_cc2_does_not_clobber_generic_output_pdf():
    body = _generate_body()
    # Old pattern: pc["output_pdf"] = output_path  (unconditional clobber)
    # New pattern: pc.setdefault("output_pdf", output_path)
    bad = re.search(r'pc\["output_pdf"\]\s*=\s*output_path', body)
    assert not bad, "CC-2: must not unconditionally overwrite pc['output_pdf']"
    assert 'pc.setdefault("output_pdf", output_path)' in body, \
        "CC-2: must setdefault to avoid clobbering an existing 704 output"


def test_cc5_registers_in_rfq_files():
    body = _generate_body()
    assert "save_rfq_file(" in body, \
        "CC-5: must call save_rfq_file to register the packet PDF"
    # Must pass a category that identifies packet outputs
    assert 'category="cchcs_packet"' in body, \
        "CC-5: save_rfq_file must tag the row with category='cchcs_packet'"
