"""Mike P0 2026-05-06 (PR #727 follow-up): backfill AUTO_<hex> records.

PR #727 added `_attachment_filename_title` as the 4th cascade step in
`_create_record` so new ingests with a blank PRICE CHECK / Solicitation
field fall through to the email-attachment filename instead of straight
to `AUTO_<short_id>`. Pre-#727 records still display `#AUTO_db670ad9`
in queue UIs even though their `source_pdf` carries a perfectly
readable title like `AMS 704 - Heel Donut - 04.29.26.pdf`.

`rename_auto_hex_records()` walks every PC + RFQ. For each one whose
pc_number/rfq_number matches `^AUTO_[0-9a-f]{8}$`, re-runs
`_attachment_filename_title()` against `source_pdf` and rewrites the
field if a usable title comes back. Idempotent. Logged.
"""
import json
from pathlib import Path
import pytest


@pytest.fixture
def tmp_pc_rfq(tmp_path, monkeypatch):
    """Wire dashboard's load/save helpers to a temp data dir."""
    pcs_path = tmp_path / "price_checks.json"
    rfqs_path = tmp_path / "rfqs.json"
    pcs_path.write_text("{}")
    rfqs_path.write_text("{}")
    # Stub the dashboard helpers so the backfill reads/writes our tmp files
    state = {"pcs": {}, "rfqs": {}}

    def _load_pcs():
        return state["pcs"]

    def _save_pc(pc_id, pc):
        state["pcs"][pc_id] = pc

    def _load_rfqs():
        return state["rfqs"]

    def _save_rfq(rfq_id, rfq):
        state["rfqs"][rfq_id] = rfq

    import sys, types
    fake_dashboard = types.ModuleType("src.api.dashboard")
    fake_dashboard._load_price_checks = _load_pcs
    fake_dashboard._save_single_pc = _save_pc
    fake_dashboard.load_rfqs = _load_rfqs
    fake_dashboard._save_single_rfq = _save_rfq
    monkeypatch.setitem(sys.modules, "src.api.dashboard", fake_dashboard)
    return state


# ── Renames pre-#727 AUTO_<hex> when source_pdf yields good title ──


def test_renames_pc_with_good_attachment_title(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["pcs"]["pc-1"] = {
        "pc_number": "AUTO_db670ad9",
        "source_pdf": "/data/inbox/AMS 704 - Heel Donut - 04.29.26.pdf",
    }
    result = rename_auto_hex_records()
    assert result["pcs_renamed"] == 1
    assert tmp_pc_rfq["pcs"]["pc-1"]["pc_number"] == "Heel Donut - 04.29.26"


def test_renames_rfq_with_good_attachment_title(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["rfqs"]["rfq-1"] = {
        "rfq_number": "AUTO_abc12345",
        "source_pdf": "/data/inbox/RFQ - Adjustable Bed Rail.pdf",
    }
    result = rename_auto_hex_records()
    assert result["rfqs_renamed"] == 1
    assert tmp_pc_rfq["rfqs"]["rfq-1"]["rfq_number"] == "Adjustable Bed Rail"


# ── Records with already-good names are NOT touched ────────────────


def test_does_not_rename_records_with_real_pc_number(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["pcs"]["pc-1"] = {
        "pc_number": "PC-2026-1234",
        "source_pdf": "/data/inbox/AMS 704 - Heel Donut - 04.29.26.pdf",
    }
    result = rename_auto_hex_records()
    assert result["pcs_renamed"] == 0
    assert tmp_pc_rfq["pcs"]["pc-1"]["pc_number"] == "PC-2026-1234"


def test_does_not_rename_solicitation_numbers(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["rfqs"]["rfq-1"] = {
        "rfq_number": "R26Q38",
        "source_pdf": "/data/inbox/RFQ - Foo.pdf",
    }
    result = rename_auto_hex_records()
    assert result["rfqs_renamed"] == 0
    assert tmp_pc_rfq["rfqs"]["rfq-1"]["rfq_number"] == "R26Q38"


# ── AUTO_<hex> with no usable filename → keep AUTO_<hex>, count skip ─


def test_keeps_auto_hex_when_no_source_pdf(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["pcs"]["pc-1"] = {
        "pc_number": "AUTO_11111111",
        "source_pdf": "",
    }
    result = rename_auto_hex_records()
    assert result["pcs_renamed"] == 0
    assert result["auto_hex_skipped"] == 1
    assert tmp_pc_rfq["pcs"]["pc-1"]["pc_number"] == "AUTO_11111111"


def test_keeps_auto_hex_when_filename_is_pure_hex(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["pcs"]["pc-1"] = {
        "pc_number": "AUTO_22222222",
        "source_pdf": "/data/inbox/abc123def456.pdf",  # pure hex
    }
    result = rename_auto_hex_records()
    assert result["pcs_renamed"] == 0
    assert result["auto_hex_skipped"] == 1
    assert tmp_pc_rfq["pcs"]["pc-1"]["pc_number"] == "AUTO_22222222"


# ── Idempotence ────────────────────────────────────────────────────


def test_re_running_finds_nothing(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["pcs"]["pc-1"] = {
        "pc_number": "AUTO_db670ad9",
        "source_pdf": "/data/inbox/AMS 704 - Heel Donut - 04.29.26.pdf",
    }
    first = rename_auto_hex_records()
    assert first["pcs_renamed"] == 1
    second = rename_auto_hex_records()
    assert second["pcs_renamed"] == 0
    assert tmp_pc_rfq["pcs"]["pc-1"]["pc_number"] == "Heel Donut - 04.29.26"


# ── Mixed populations get exactly the AUTO_<hex> ones renamed ──────


def test_mixed_population_renames_only_auto_hex(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["pcs"] = {
        "pc-1": {"pc_number": "AUTO_db670ad9",
                 "source_pdf": "/inbox/AMS 704 - Coloring Posters.pdf"},
        "pc-2": {"pc_number": "PC-2026-100",
                 "source_pdf": "/inbox/AMS 704 - Other.pdf"},
        "pc-3": {"pc_number": "AUTO_11111111", "source_pdf": ""},
        "pc-4": {"pc_number": "AUTO_aabbccdd",
                 "source_pdf": "/inbox/Quote - Engraved Nameplate.pdf"},
    }
    tmp_pc_rfq["rfqs"] = {
        "rfq-1": {"rfq_number": "R26Q01",
                  "source_pdf": "/inbox/RFQ - Existing.pdf"},
        "rfq-2": {"rfq_number": "AUTO_99999999",
                  "source_pdf": "/inbox/RFQ - Wheelchair Cushion.pdf"},
    }
    result = rename_auto_hex_records()
    assert result["pcs_renamed"] == 2  # pc-1 and pc-4
    assert result["rfqs_renamed"] == 1  # rfq-2 only
    assert result["auto_hex_skipped"] == 1  # pc-3 (no source_pdf)
    assert tmp_pc_rfq["pcs"]["pc-1"]["pc_number"] == "Coloring Posters"
    assert tmp_pc_rfq["pcs"]["pc-2"]["pc_number"] == "PC-2026-100"  # untouched
    assert tmp_pc_rfq["pcs"]["pc-3"]["pc_number"] == "AUTO_11111111"  # skipped
    assert tmp_pc_rfq["pcs"]["pc-4"]["pc_number"] == "Engraved Nameplate"
    assert tmp_pc_rfq["rfqs"]["rfq-1"]["rfq_number"] == "R26Q01"  # untouched
    assert tmp_pc_rfq["rfqs"]["rfq-2"]["rfq_number"] == "Wheelchair Cushion"


# ── dry_run flag previews without writing ──────────────────────────


def test_dry_run_does_not_write(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    tmp_pc_rfq["pcs"]["pc-1"] = {
        "pc_number": "AUTO_db670ad9",
        "source_pdf": "/inbox/AMS 704 - Heel Donut - 04.29.26.pdf",
    }
    result = rename_auto_hex_records(dry_run=True)
    assert result["pcs_renamed"] == 1
    # But the original record is unchanged in dry-run
    assert tmp_pc_rfq["pcs"]["pc-1"]["pc_number"] == "AUTO_db670ad9"


# ── Scan covers both PCs and RFQs without crash on empty inputs ────


def test_handles_empty_state(tmp_pc_rfq):
    from src.core.ingest_pipeline import rename_auto_hex_records
    result = rename_auto_hex_records()
    assert result == {"pcs_renamed": 0, "rfqs_renamed": 0,
                       "scanned": 0, "auto_hex_skipped": 0}


# ── Boot wiring ────────────────────────────────────────────────────


def test_backfill_is_wired_into_deferred_init():
    body = (Path(__file__).resolve().parent.parent / "app.py").read_text(encoding="utf-8")
    assert "from src.core.ingest_pipeline import rename_auto_hex_records" in body
    assert "rename_auto_hex_records()" in body
