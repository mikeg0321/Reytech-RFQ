"""prior_submissions — pin the capture/lookup contract.

PR mr-wolf #4b. Operational completion of PR #4. Tests:
  - schema init creates the table
  - `capture` inserts a row + returns a non-empty id
  - `latest_for` returns the most-recent capture by `captured_at`
  - agency_key narrows the lookup
  - blessed=1 wins over blessed=0 at the same captured_at
  - empty / missing inputs degrade gracefully (no raises, return None / "")
  - filesystem fallback covers operators who placed PDFs pre-PR-#4b
  - `_form_id_from_filename` maps generator filenames to canonical form_ids
  - `capture_from_rfq_generated_files` sweeps rfq_files into priors

Hermetic — uses a temp sqlite file + monkeypatched `get_db`. No real
DB boot.
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest


# ── Hermetic DB fixture ─────────────────────────────────────────────


def _build_temp_db(tmp_path: Path) -> Path:
    """Build a temp sqlite with the schemas prior_submissions needs to
    capture against (the table itself + rfq_files for the sweep helper)."""
    db_path = tmp_path / "prior_submissions_test.db"
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
    conn.execute("""
        CREATE TABLE rfq_files (
            id          TEXT PRIMARY KEY,
            rfq_id      TEXT NOT NULL,
            filename    TEXT NOT NULL,
            file_type   TEXT NOT NULL,
            category    TEXT DEFAULT 'template',
            data        BLOB,
            created_at  TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    """Point `src.core.db.get_db` at a hermetic sqlite file."""
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


# ── capture / latest_for ────────────────────────────────────────────


def test_capture_inserts_row_and_returns_id(isolated_db):
    from src.forms.prior_submissions import capture, count_for
    rid = capture("703b", b"%PDF-1.4...", agency_key="cchcs",
                  source_rfq_id="rfq_canonical",
                  source_quote_number="R26Q42",
                  filename="10846357_703B_Reytech.pdf")
    assert rid.startswith("ps_")
    assert count_for("703b") == 1
    assert count_for("703b", agency_key="cchcs") == 1
    assert count_for("703b", agency_key="calvet") == 0


def test_latest_for_returns_most_recent_capture(isolated_db):
    from src.forms.prior_submissions import capture, latest_for
    capture("703b", b"OLD", agency_key="cchcs", filename="old.pdf")
    time.sleep(0.01)  # ensure distinct captured_at
    capture("703b", b"NEWER", agency_key="cchcs", filename="newer.pdf")
    assert latest_for("703b", agency_key="cchcs") == b"NEWER"


def test_latest_for_falls_back_to_any_agency_when_specific_misses(isolated_db):
    from src.forms.prior_submissions import capture, latest_for
    capture("703b", b"GLOBAL", agency_key="", filename="g.pdf")
    # CCHCS-specific prior doesn't exist → falls through to any-agency.
    assert latest_for("703b", agency_key="cchcs") == b"GLOBAL"


def test_blessed_wins_over_recent_at_same_agency(isolated_db):
    """Operator-blessed priors should outrank time-based newer ones."""
    from src.forms.prior_submissions import capture, latest_for
    capture("703b", b"BLESSED_OLD", agency_key="cchcs",
            filename="b.pdf", blessed=True)
    time.sleep(0.01)
    capture("703b", b"FRESH_UNBLESSED", agency_key="cchcs",
            filename="f.pdf", blessed=False)
    assert latest_for("703b", agency_key="cchcs") == b"BLESSED_OLD"


def test_latest_for_returns_none_when_no_prior_exists(isolated_db):
    from src.forms.prior_submissions import latest_for
    assert latest_for("703b") is None
    assert latest_for("703b", agency_key="cchcs") is None


def test_capture_empty_inputs_degrade_gracefully(isolated_db):
    from src.forms.prior_submissions import capture
    assert capture("", b"data") == ""
    assert capture("703b", b"") == ""
    assert capture("703b", None) == ""  # type: ignore[arg-type]


def test_capture_accepts_file_path(isolated_db, tmp_path):
    from src.forms.prior_submissions import capture, latest_for
    pdf_path = tmp_path / "fake.pdf"
    pdf_path.write_bytes(b"%PDF FROM FILE")
    rid = capture("703b", str(pdf_path), agency_key="cchcs",
                  filename="fake.pdf")
    assert rid.startswith("ps_")
    assert latest_for("703b", agency_key="cchcs") == b"%PDF FROM FILE"


# ── _form_id_from_filename — generator-pattern inference ────────────


def test_form_id_from_filename_maps_canonical_tokens():
    from src.forms.prior_submissions import _form_id_from_filename
    from src.forms.form_registry import all_form_ids
    known = {f.lower(): f for f in all_form_ids()}
    assert _form_id_from_filename("10846357_703B_Reytech.pdf", known) == "703b"
    assert _form_id_from_filename("10846357_703A_Reytech.pdf", known) == "703a"
    assert _form_id_from_filename("10846357_703C_Reytech.pdf", known) == "703c"
    assert _form_id_from_filename("10846357_704B_Reytech.pdf", known) == "704b"


def test_form_id_from_filename_handles_compound_tokens():
    from src.forms.prior_submissions import _form_id_from_filename
    from src.forms.form_registry import all_form_ids
    known = {f.lower(): f for f in all_form_ids()}
    # `BidPackage` and `bid_package` aliases → `bidpkg`
    assert _form_id_from_filename("10846357_BidPackage_Reytech.pdf", known) == "bidpkg"
    # GenAI 708 alias → ams708
    assert _form_id_from_filename("10846357_GenAI_708_Reytech.pdf", known) == "ams708"


def test_form_id_from_filename_returns_empty_on_unknown():
    from src.forms.prior_submissions import _form_id_from_filename
    from src.forms.form_registry import all_form_ids
    known = {f.lower(): f for f in all_form_ids()}
    assert _form_id_from_filename("10846357_FooBar_Reytech.pdf", known) == ""
    assert _form_id_from_filename("randomname.pdf", known) == ""
    assert _form_id_from_filename("", known) == ""


# ── capture_from_rfq_generated_files — Mark Sent sweep ──────────────


def _seed_rfq_file(db_path: Path, *, rid, filename, data, category="generated"):
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO rfq_files (id, rfq_id, filename, file_type, category, data, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
        (f"rf_{filename}", rid, filename, "generated_package", category, data),
    )
    conn.commit()
    conn.close()


def test_capture_from_rfq_files_sweeps_known_forms(isolated_db):
    """The canonical sweep — every generated form attached to an RFQ
    gets a prior_submissions row keyed by inferred form_id."""
    from src.forms.prior_submissions import capture_from_rfq_generated_files, count_for
    rid = "rfq_test123"
    _seed_rfq_file(isolated_db, rid=rid, filename="10846357_703B_Reytech.pdf", data=b"703B-bytes")
    _seed_rfq_file(isolated_db, rid=rid, filename="10846357_704B_Reytech.pdf", data=b"704B-bytes")
    _seed_rfq_file(isolated_db, rid=rid, filename="10846357_BidPackage_Reytech.pdf", data=b"BIDPKG-bytes")

    captured = capture_from_rfq_generated_files(rid, agency_key="cchcs",
                                                 source_quote_number="R26Q42")
    assert captured == 3
    assert count_for("703b", agency_key="cchcs") == 1
    assert count_for("704b", agency_key="cchcs") == 1
    assert count_for("bidpkg", agency_key="cchcs") == 1


def test_capture_from_rfq_files_skips_unknown_filenames(isolated_db):
    """Filenames that don't match a known form_id token must NOT
    spawn priors — we don't want operator-uploaded random PDFs
    polluting the prior pool."""
    from src.forms.prior_submissions import capture_from_rfq_generated_files, count_for
    rid = "rfq_test456"
    _seed_rfq_file(isolated_db, rid=rid, filename="some_random_attachment.pdf",
                   data=b"random")
    _seed_rfq_file(isolated_db, rid=rid, filename="10846357_703B_Reytech.pdf",
                   data=b"703B")
    captured = capture_from_rfq_generated_files(rid, agency_key="cchcs")
    assert captured == 1
    assert count_for("703b", agency_key="cchcs") == 1


def test_capture_from_rfq_files_ignores_non_generated_category(isolated_db):
    """Only `category='generated'` rows get captured — buyer-uploaded
    templates (`category='template'`) are NOT priors."""
    from src.forms.prior_submissions import capture_from_rfq_generated_files, count_for
    rid = "rfq_test789"
    _seed_rfq_file(isolated_db, rid=rid, filename="10846357_703B_Reytech.pdf",
                   data=b"buyer-upload", category="template")
    captured = capture_from_rfq_generated_files(rid, agency_key="cchcs")
    assert captured == 0
    assert count_for("703b") == 0


def test_capture_from_rfq_files_handles_missing_rfq(isolated_db):
    """No matching rfq_id → zero captures, no raise."""
    from src.forms.prior_submissions import capture_from_rfq_generated_files
    assert capture_from_rfq_generated_files("rfq_does_not_exist") == 0
    assert capture_from_rfq_generated_files("") == 0
