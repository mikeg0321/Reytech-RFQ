"""Phase 1.6 PR3g: training_corpus tests."""

import json
import os
from unittest.mock import patch

import pytest


def _seed_pc(conn, pcid="PC-FP-1", agency="CDCR Folsom",
             quote_number="2026Q-001", source_file=""):
    # Discover schema dynamically — test DB may not have all prod columns
    cols = {r[1] for r in conn.execute("PRAGMA table_info(price_checks)")}
    from datetime import datetime
    now = datetime.utcnow().isoformat()
    fields = ["id", "agency", "institution", "requirements_json", "status",
              "created_at"]
    values = [pcid, agency, agency,
              json.dumps({"forms_required": ["703b", "dvbe843"],
                          "due_date": "2026-05-15"}),
              "sent", now]
    for col, val in (("quote_number", quote_number),
                     ("reytech_quote_number", quote_number),
                     ("source_file", source_file)):
        if col in cols:
            fields.append(col); values.append(val)
    placeholders = ",".join("?" * len(fields))
    conn.execute(
        f"INSERT INTO price_checks ({', '.join(fields)}) VALUES ({placeholders})",
        values,
    )


def _seed_order(conn, quote_number="2026Q-001",
                po_number="PO-001", po_date="2026-04-15",
                po_pdf_path=""):
    from datetime import datetime
    cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)")}
    fields = ["quote_number", "po_number", "po_date",
              "agency", "institution", "status", "created_at"]
    values = [quote_number, po_number, po_date,
              "CDCR Folsom", "CDCR Folsom", "delivered",
              datetime.utcnow().isoformat()]
    if "po_pdf_path" in cols:
        fields.append("po_pdf_path"); values.append(po_pdf_path)
    placeholders = ",".join("?" * len(fields))
    conn.execute(
        f"INSERT INTO orders ({', '.join(fields)}) VALUES ({placeholders})",
        values,
    )


class TestBuildTrainingPair:
    def test_returns_skipped_when_quote_missing(self, app):
        from src.agents.training_corpus import build_training_pair
        r = build_training_pair("ghost-id", "pc")
        assert r["status"] == "skipped_no_data"

    def test_creates_manifest_with_no_artifacts_skips(self, app):
        from src.agents.training_corpus import build_training_pair
        from src.core.db import get_db
        with get_db() as conn:
            _seed_pc(conn)
            conn.commit()

        # No source_file, no order with po_pdf_path → no artifacts
        r = build_training_pair("PC-FP-1", "pc")
        assert r["status"] == "skipped_no_artifacts"

    def test_creates_manifest_with_pc_source_file(self, app, tmp_path):
        from src.agents.training_corpus import build_training_pair
        from src.core.db import get_db

        src = tmp_path / "buyer.pdf"
        src.write_bytes(b"%PDF-1.4\n")
        out_dir = tmp_path / "outputs"
        out_dir.mkdir()
        po_pdf = out_dir / "shipped.pdf"
        po_pdf.write_bytes(b"%PDF-1.4\nshipped\n")

        with get_db() as conn:
            _seed_pc(conn, source_file=str(src))
            _seed_order(conn, po_pdf_path=str(po_pdf))
            conn.commit()

        # Patch DATA_DIR to keep training_pairs in tmp
        with patch("src.agents.training_corpus._data_dir",
                   return_value=str(tmp_path)):
            r = build_training_pair("PC-FP-1", "pc")
        assert r["status"] == "created", f"got {r}"
        assert r["incoming_count"] == 1
        assert r["outgoing_count"] == 1

        # Manifest contents
        with open(r["manifest_path"]) as f:
            m = json.load(f)
        assert m["agency"] == "CDCR Folsom"
        assert m["po_number"] == "PO-001"
        assert m["won"] is True
        assert m["contract"]["forms_required"] == ["703b", "dvbe843"]
        assert m["contract"]["due_date"] == "2026-05-15"
        assert len(m["incoming_blanks"]) == 1
        assert len(m["outgoing_fills"]) == 1

    def test_idempotent_skip_when_manifest_exists(self, app, tmp_path):
        from src.agents.training_corpus import build_training_pair
        from src.core.db import get_db

        src = tmp_path / "buyer.pdf"
        src.write_bytes(b"%PDF-1.4\n")
        with get_db() as conn:
            _seed_pc(conn, source_file=str(src))
            conn.commit()

        with patch("src.agents.training_corpus._data_dir",
                   return_value=str(tmp_path)):
            r1 = build_training_pair("PC-FP-1", "pc")
            r2 = build_training_pair("PC-FP-1", "pc")
        assert r1["status"] == "created"
        assert r2["status"] == "skipped_exists"

    def test_force_rewrites(self, app, tmp_path):
        from src.agents.training_corpus import build_training_pair
        from src.core.db import get_db

        src = tmp_path / "buyer.pdf"
        src.write_bytes(b"%PDF-1.4\n")
        with get_db() as conn:
            _seed_pc(conn, source_file=str(src))
            conn.commit()

        with patch("src.agents.training_corpus._data_dir",
                   return_value=str(tmp_path)):
            build_training_pair("PC-FP-1", "pc")
            r = build_training_pair("PC-FP-1", "pc", force=True)
        assert r["status"] == "created"


class TestBootstrapFromOrders:
    def test_empty_orders_returns_zero(self, app, tmp_path):
        from src.agents.training_corpus import bootstrap_from_orders
        with patch("src.agents.training_corpus._data_dir",
                   return_value=str(tmp_path)):
            s = bootstrap_from_orders(days=365)
        assert s["scanned"] >= 0
        assert s["created"] == 0

    def test_bootstrap_processes_orders(self, app, tmp_path):
        from src.agents.training_corpus import bootstrap_from_orders
        from src.core.db import get_db

        src = tmp_path / "buyer.pdf"
        src.write_bytes(b"%PDF-1.4\n")
        out = tmp_path / "ship.pdf"
        out.write_bytes(b"%PDF-1.4\n")

        with get_db() as conn:
            _seed_pc(conn, pcid="PC-A", quote_number="QA",
                     source_file=str(src))
            _seed_order(conn, quote_number="QA", po_pdf_path=str(out))
            conn.commit()

        with patch("src.agents.training_corpus._data_dir",
                   return_value=str(tmp_path)):
            s = bootstrap_from_orders(days=365)
        assert s["scanned"] >= 1
        assert s["created"] >= 1
        assert "CDCR Folsom" in s["by_agency"]


class TestCoverageReport:
    def test_empty_when_no_corpus(self, app, tmp_path):
        from src.agents.training_corpus import coverage_report
        with patch("src.agents.training_corpus._data_dir",
                   return_value=str(tmp_path)):
            r = coverage_report()
        assert r["total_pairs"] == 0
        assert r["by_agency"] == {}


class TestEndpoints:
    def test_coverage_endpoint(self, client):
        r = client.get("/api/training-corpus/coverage")
        assert r.status_code == 200
        d = r.get_json()
        assert d["ok"] is True
        assert "total_pairs" in d
        assert "by_agency" in d

    def test_build_one_unknown_quote(self, client):
        r = client.post("/api/training-corpus/build/pc/no_such")
        assert r.status_code == 200
        d = r.get_json()
        assert d.get("status") == "skipped_no_data"

    def test_build_invalid_type(self, client):
        r = client.post("/api/training-corpus/build/bogus/x")
        assert r.status_code == 400
