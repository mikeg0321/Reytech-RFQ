"""mirror-fall-back-on-crash — PR mr-wolf #4c.

When a code-fill function (fill_703c, future fill_X variants) crashes
during package generation, the dispatcher should recover by mirror-
filling from a prior submission of the registered `mirror_fallback`
form. The bid ships; the underlying code-fill bug surfaces in logs
for post-bid investigation.

Tests pin the recovery semantics + the no-recovery degradation
paths (no fallback registered, no prior available, prefix-mapping
missing, etc.) so a future change to `_attempt_mirror_fallback`
can't silently regress the resilience.
"""
from __future__ import annotations

import io
import sqlite3
from pathlib import Path

import pytest

from src.api.modules.routes_rfq_gen import _attempt_mirror_fallback


# ── Hermetic DB fixture (re-use prior_submissions test setup) ───────


def _build_temp_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "fallback_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE prior_submissions (
            id                  TEXT PRIMARY KEY,
            form_id             TEXT NOT NULL,
            agency_key          TEXT DEFAULT '',
            pdf_data            BLOB NOT NULL,
            filename            TEXT NOT NULL,
            source_rfq_id       TEXT DEFAULT '',
            source_quote_number TEXT DEFAULT '',
            captured_at         TEXT NOT NULL,
            blessed             INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    db_path = _build_temp_db(tmp_path)

    class _Conn:
        def __init__(self, path):
            self._raw = sqlite3.connect(str(path))
            self._raw.row_factory = sqlite3.Row

        def __enter__(self):
            return self._raw

        def __exit__(self, *a):
            self._raw.commit()
            self._raw.close()

        def execute(self, *args, **kwargs):
            return self._raw.execute(*args, **kwargs)

    def _fake_get_db():
        return _Conn(db_path)

    import src.core.db as _db
    monkeypatch.setattr(_db, "get_db", _fake_get_db, raising=False)
    return db_path


# ── Fixture: a synthesized 703B prior + a 703C target PDF ───────────


def _build_acroform_pdf(fields: dict[str, str]) -> bytes:
    from pypdf import PdfWriter
    from pypdf.generic import (
        ArrayObject, DictionaryObject, NameObject, NumberObject,
        TextStringObject, FloatObject,
    )

    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    annots = []
    for i, (name, value) in enumerate(fields.items()):
        widget = DictionaryObject({
            NameObject("/Type"):    NameObject("/Annot"),
            NameObject("/Subtype"): NameObject("/Widget"),
            NameObject("/FT"):      NameObject("/Tx"),
            NameObject("/T"):       TextStringObject(name),
            NameObject("/V"):       TextStringObject(value),
            NameObject("/Ff"):      NumberObject(0),
            NameObject("/Rect"):    ArrayObject([
                FloatObject(50 + (i % 4) * 130),
                FloatObject(700 - (i // 4) * 30),
                FloatObject(170 + (i % 4) * 130),
                FloatObject(720 - (i // 4) * 30),
            ]),
            NameObject("/P"):       page.indirect_reference,
        })
        annots.append(writer._add_object(widget))
    page[NameObject("/Annots")] = ArrayObject(annots)
    acro = DictionaryObject({
        NameObject("/Fields"): ArrayObject(annots),
        NameObject("/NeedAppearances"): NumberObject(1),
    })
    writer._root_object[NameObject("/AcroForm")] = writer._add_object(acro)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def _field_values(pdf_bytes: bytes) -> dict[str, str]:
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(pdf_bytes))
    out = {}
    for name, fld in (reader.get_fields() or {}).items():
        v = fld.get("/V")
        out[name] = str(v) if v is not None else ""
    return out


@pytest.fixture
def primed_priors(isolated_db):
    """Pre-populate prior_submissions with a 703B carrying Reytech's
    static fields, keyed for CCHCS agency. Returns the prior bytes."""
    from src.forms.prior_submissions import capture
    prior_bytes = _build_acroform_pdf({
        "703B_Business Name":  "Reytech Inc.",
        "703B_FEIN":           "82-XXXXXXX",
        "703B_Contact Person": "Michael Guadan",
        "703B_Address":        "30 Carnoustie Way",
    })
    capture("703b", prior_bytes, agency_key="cchcs",
            filename="prior_703B_cchcs.pdf")
    return prior_bytes


@pytest.fixture
def target_703c_pdf(tmp_path) -> str:
    """A 703C template the operator is trying to fill — same suffix
    space as 703B but with 703C_ prefix."""
    target_bytes = _build_acroform_pdf({
        "703C_Business Name":  "",
        "703C_FEIN":           "",
        "703C_Contact Person": "",
        "703C_Address":        "",
    })
    path = tmp_path / "703c_target.pdf"
    path.write_bytes(target_bytes)
    return str(path)


# ── Recovery succeeds when a prior + fallback exist ────────────────


def test_703c_codefill_crash_recovers_via_703b_mirror(isolated_db, primed_priors,
                                                       target_703c_pdf, tmp_path):
    """The canonical recovery: fill_703c crashes (synthetic exception),
    `_attempt_mirror_fallback("703c", ...)` finds a prior 703B in the
    DB and mirror-fills with 703B_ → 703C_ translation."""
    output_path = str(tmp_path / "703c_filled.pdf")
    rfq_data = {"agency": "cchcs", "solicitation_number": "10846357"}

    recovered = _attempt_mirror_fallback(
        "703c", target_703c_pdf, rfq_data, output_path,
        Exception("simulated fill_703c crash"),
    )
    assert recovered is True
    # Verify the output carries Reytech's static fields under 703C_ keys.
    output = Path(output_path).read_bytes()
    vals = _field_values(output)
    assert vals["703C_Business Name"] == "Reytech Inc."
    assert vals["703C_FEIN"] == "82-XXXXXXX"
    assert vals["703C_Contact Person"] == "Michael Guadan"


def test_recovery_fails_when_no_prior_exists(isolated_db, target_703c_pdf, tmp_path):
    """No prior 703B in DB → no recovery (caller re-raises). The
    bid will fail to ship, surfaces in errors[]."""
    output_path = str(tmp_path / "703c_filled.pdf")
    recovered = _attempt_mirror_fallback(
        "703c", target_703c_pdf, {"agency": "cchcs"}, output_path,
        Exception("simulated crash"),
    )
    assert recovered is False
    assert not Path(output_path).exists()


def test_recovery_fails_when_form_has_no_mirror_fallback(isolated_db,
                                                          target_703c_pdf,
                                                          tmp_path):
    """703B itself has no `mirror_fallback` registered (it IS the
    canonical fill). A crash inside `fill_703b` cannot recover via
    this path — must be fixed in the code-fill function."""
    output_path = str(tmp_path / "703b_filled.pdf")
    recovered = _attempt_mirror_fallback(
        "703b", target_703c_pdf, {"agency": "cchcs"}, output_path,
        Exception("simulated crash"),
    )
    assert recovered is False


def test_recovery_fails_when_form_id_is_unknown(isolated_db, target_703c_pdf,
                                                 tmp_path):
    """An unregistered form_id (typo, future form not yet added to
    the registry) must not crash _attempt_mirror_fallback — returns
    False so caller re-raises the original code-fill exception."""
    output_path = str(tmp_path / "weird.pdf")
    recovered = _attempt_mirror_fallback(
        "totally_made_up_form", target_703c_pdf,
        {"agency": "cchcs"}, output_path,
        Exception("simulated crash"),
    )
    assert recovered is False


def test_recovery_prefers_same_agency_prior(isolated_db, target_703c_pdf, tmp_path):
    """When priors exist for multiple agencies, the same-agency one
    wins (CCHCS 703C bid recovers from a CCHCS 703B prior, not a
    CalVet 703B prior with different Reytech metadata)."""
    from src.forms.prior_submissions import capture
    cchcs_prior = _build_acroform_pdf({"703B_Business Name": "CCHCS-Prior"})
    calvet_prior = _build_acroform_pdf({"703B_Business Name": "CalVet-Prior"})
    capture("703b", cchcs_prior, agency_key="cchcs",
            filename="cchcs_prior.pdf")
    capture("703b", calvet_prior, agency_key="calvet",
            filename="calvet_prior.pdf")

    output_path = str(tmp_path / "703c_cchcs.pdf")
    recovered = _attempt_mirror_fallback(
        "703c", target_703c_pdf, {"agency": "cchcs"}, output_path,
        Exception("simulated crash"),
    )
    assert recovered is True
    vals = _field_values(Path(output_path).read_bytes())
    assert vals["703C_Business Name"] == "CCHCS-Prior"


def test_recovery_does_not_raise_on_internal_errors(isolated_db, tmp_path):
    """Internal helper errors (bad input PDF, write failure) degrade
    to False return — never raises. The dispatcher's outer
    try/except can then surface the ORIGINAL code-fill exception
    instead of getting confused by a recovery-stage exception."""
    bogus_input = str(tmp_path / "does_not_exist.pdf")
    output_path = str(tmp_path / "out.pdf")
    recovered = _attempt_mirror_fallback(
        "703c", bogus_input, {"agency": "cchcs"}, output_path,
        Exception("simulated crash"),
    )
    assert recovered is False
