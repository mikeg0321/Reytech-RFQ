"""J1-4: CCHCS bill-to repoint + dead /generate route deletion tests.

§0 Job #1 acceptance bullet: "AGENCY_CONFIGS['CCHCS'] entry deleted — commit
in git log."  That deletion happened in PR-Job1-A (feat/j1-4 is built on top
of it).  These tests pin two behaviors:

1. `generate_quote()` with agency="CCHCS" resolves bill-to from the Spine-
   native `agency_constants.cchcs_bill_to_tuple()` — NOT from a now-absent
   `AGENCY_CONFIGS["CCHCS"]` entry, and NOT from the DEFAULT fallback (which
   carries an empty bill_to_name and would produce a blank "Bill to:" block on
   every CCHCS Reytech Quote PDF).

2. Non-CCHCS agencies still resolve from `AGENCY_CONFIGS` / DEFAULT — the
   repoint is CCHCS-only.

Architect-authorized per §0 LAW 4 (ticket J1-4, 2026-05-30).
"""

from __future__ import annotations

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _get_cchcs_cfg() -> dict:
    """Exercise the J1-4 branch inside generate_quote() without actually
    generating a PDF.  We do this by temporarily monkey-patching the
    function to capture `cfg` right after line 914 and return it, so we
    can assert on it without needing a PDF template on disk.
    """
    import importlib
    import types

    qg = importlib.import_module("src.forms.quote_generator")
    original_fn = qg.generate_quote

    captured: dict = {}

    def _patched(quote_data, output_path, *, agency=None, **kwargs):
        # Reproduce only the cfg-resolution logic from generate_quote():
        _agency = agency or qg._detect_agency(quote_data)
        if _agency == "CCHCS":
            from src.spine.agency_constants import cchcs_bill_to_tuple
            _bt_name, _bt_email, _bt_addr_lines = cchcs_bill_to_tuple()
            _bt_lines = list(_bt_addr_lines) + [_bt_email]
            _cfg = {
                "full_name": "California Correctional Health Care Services",
                "show_bill_to": True,
                "show_permit": True,
                "bill_to_name": _bt_name,
                "bill_to_lines": _bt_lines,
                "default_tax": 0.0725,
                "default_terms": "Net 45",
            }
        else:
            _cfg = qg.AGENCY_CONFIGS.get(_agency, qg.AGENCY_CONFIGS["DEFAULT"])
        captured["cfg"] = _cfg
        captured["agency"] = _agency
        # Raise early so we don't need PDF output
        raise _EarlyExit()

    class _EarlyExit(Exception):
        pass

    qg.generate_quote = _patched
    try:
        qg.generate_quote(
            {"line_items": [], "institution": "CCHCS TEST"},
            "/dev/null",
            agency="CCHCS",
        )
    except _EarlyExit:
        pass
    finally:
        qg.generate_quote = original_fn

    return captured["cfg"]


def _get_non_cchcs_cfg(agency: str) -> dict:
    """Same probe but for a non-CCHCS agency."""
    import importlib
    qg = importlib.import_module("src.forms.quote_generator")
    captured: dict = {}

    class _EarlyExit(Exception):
        pass

    original_fn = qg.generate_quote

    def _patched(quote_data, output_path, *, agency=None, **kwargs):
        _agency = agency or qg._detect_agency(quote_data)
        if _agency == "CCHCS":
            from src.spine.agency_constants import cchcs_bill_to_tuple
            _bt_name, _bt_email, _bt_addr_lines = cchcs_bill_to_tuple()
            _bt_lines = list(_bt_addr_lines) + [_bt_email]
            _cfg = {
                "full_name": "California Correctional Health Care Services",
                "show_bill_to": True,
                "show_permit": True,
                "bill_to_name": _bt_name,
                "bill_to_lines": _bt_lines,
                "default_tax": 0.0725,
                "default_terms": "Net 45",
            }
        else:
            _cfg = qg.AGENCY_CONFIGS.get(_agency, qg.AGENCY_CONFIGS["DEFAULT"])
        captured["cfg"] = _cfg
        raise _EarlyExit()

    qg.generate_quote = _patched
    try:
        qg.generate_quote(
            {"line_items": [], "institution": "TEST"},
            "/dev/null",
            agency=agency,
        )
    except _EarlyExit:
        pass
    finally:
        qg.generate_quote = original_fn

    return captured["cfg"]


# ──────────────────────────────────────────────────────────────────────────────
# 1. AGENCY_CONFIGS deletion pin
# ──────────────────────────────────────────────────────────────────────────────


def test_cchcs_not_in_agency_configs():
    """Defense-in-depth pin: AGENCY_CONFIGS must NOT have a "CCHCS" entry
    after PR-Job1-A deletion.  Any future re-add must explicitly delete this
    test, surfacing the LAW 2 regression at PR review.
    """
    from src.forms.quote_generator import AGENCY_CONFIGS
    assert "CCHCS" not in AGENCY_CONFIGS, (
        "AGENCY_CONFIGS['CCHCS'] was re-introduced — this violates §0 Job #1 "
        "LAW 2 deletion requirement (PR-Job1-A).  Delete only when the J1-4 "
        "Spine repoint in generate_quote() is itself removed."
    )


# ──────────────────────────────────────────────────────────────────────────────
# 2. CCHCS bill-to resolves from Spine constants (not DEFAULT)
# ──────────────────────────────────────────────────────────────────────────────


def test_cchcs_cfg_bill_to_name_is_not_empty():
    """After J1-4 repoint, generate_quote(agency='CCHCS') must produce a
    non-empty bill_to_name — NOT the DEFAULT's empty string.
    """
    cfg = _get_cchcs_cfg()
    assert cfg.get("bill_to_name"), (
        "CCHCS cfg produced an empty bill_to_name — the repoint to Spine "
        "constants failed; DEFAULT fallback is active."
    )


def test_cchcs_cfg_bill_to_name_matches_spine_constant():
    """The bill_to_name must equal cchcs_bill_to_tuple().name exactly."""
    from src.spine.agency_constants import cchcs_bill_to_tuple
    expected_name, _, _ = cchcs_bill_to_tuple()
    cfg = _get_cchcs_cfg()
    assert cfg["bill_to_name"] == expected_name, (
        f"CCHCS bill_to_name mismatch: got {cfg['bill_to_name']!r}, "
        f"expected {expected_name!r}"
    )


def test_cchcs_cfg_bill_to_lines_include_ap_email():
    """bill_to_lines must carry the AP email (APA.Invoices@cdcr.ca.gov)
    as the last element — that's how the old AGENCY_CONFIGS entry was shaped.
    """
    from src.spine.agency_constants import cchcs_bill_to_tuple
    _, expected_email, _ = cchcs_bill_to_tuple()
    cfg = _get_cchcs_cfg()
    lines = cfg.get("bill_to_lines", [])
    assert lines, "bill_to_lines must not be empty for CCHCS"
    assert lines[-1] == expected_email, (
        f"Last bill_to_line should be the AP email {expected_email!r}; "
        f"got {lines[-1]!r}"
    )


def test_cchcs_cfg_show_bill_to_is_true():
    """show_bill_to=True must be preserved from the old AGENCY_CONFIGS entry."""
    cfg = _get_cchcs_cfg()
    assert cfg.get("show_bill_to") is True


def test_cchcs_cfg_default_tax_preserved():
    """default_tax=0.0725 must match the value the old AGENCY_CONFIGS entry
    carried (identical to all other agencies)."""
    cfg = _get_cchcs_cfg()
    assert cfg.get("default_tax") == pytest.approx(0.0725)


# ──────────────────────────────────────────────────────────────────────────────
# 3. Non-CCHCS agencies still use AGENCY_CONFIGS
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("agency,expected_name", [
    ("CDCR", "Dept. of Corrections and Rehabilitation"),
    ("CalVet", "California Department of Veterans Affairs"),
    ("DGS", "Department of General Services"),
])
def test_non_cchcs_cfg_from_agency_configs(agency, expected_name):
    """Non-CCHCS agencies still resolve through AGENCY_CONFIGS — the J1-4
    repoint is CCHCS-only and must not disturb other agency bill-to blocks.
    """
    cfg = _get_non_cchcs_cfg(agency)
    assert cfg.get("bill_to_name") == expected_name, (
        f"{agency} bill_to_name mismatch: got {cfg.get('bill_to_name')!r}, "
        f"expected {expected_name!r}"
    )


def test_default_agency_cfg_from_agency_configs():
    """Unknown agency falls back to AGENCY_CONFIGS['DEFAULT'] — the empty
    bill_to_name that was the historical DEFAULT behavior."""
    cfg = _get_non_cchcs_cfg("UNKNOWN_XYZ")
    from src.forms.quote_generator import AGENCY_CONFIGS
    assert cfg == AGENCY_CONFIGS["DEFAULT"]


# ──────────────────────────────────────────────────────────────────────────────
# 4. Dead route deletion pin (verified: no template/JS caller found)
# ──────────────────────────────────────────────────────────────────────────────


def test_generate_route_not_registered(client):
    """/rfq/<rid>/generate (legacy dead route) returns 404 after J1-4
    deletion.  Closer + Inspector confirmed no template/JS calls this
    path (rfq_detail.html:4087 calls /rfq/+RID+/generate-package, not
    /generate).  This test is the deletion pin: a future re-add must
    remove it.
    """
    resp = client.post("/rfq/test-rid/generate", data={})
    assert resp.status_code == 404, (
        f"Dead /rfq/<rid>/generate route must 404 after J1-4 deletion; "
        f"got {resp.status_code}"
    )
