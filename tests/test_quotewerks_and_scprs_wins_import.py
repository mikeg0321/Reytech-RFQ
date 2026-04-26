"""Phase 0.7d: QuoteWerks + SCPRS-wins import + verify outcome.

Tests three pieces:
  1. scripts/import_quotewerks_export.py — CSV → quotes table
  2. scripts/import_scprs_reytech_wins.py — HTML → scprs_reytech_wins
  3. core/oracle_backfill.verify_quotewerks_outcomes — joins them
     and flips quote status to won/lost per Mike's rule.
"""

import json
import os
import tempfile

import pytest

from src.core.db import get_db


# ── QuoteWerks importer ──────────────────────────────────────────────


_QW_HEADER = (
    "DocumentHeaders_DocNo,DocumentHeaders_DocType,DocumentHeaders_DocStatus,"
    "DocumentHeaders_DocDate,DocumentHeaders_GrandTotal,"
    "DocumentHeaders_SoldToCompany,DocumentHeaders_ShipToCompany,"
    "DocumentHeaders_SoldToPONumber,DocumentHeaders_ConvertedRef,"
    "DocumentItems_Description,DocumentItems_ManufacturerPartNumber,"
    "DocumentItems_VendorPartNumber,DocumentItems_QtyTotal,"
    "DocumentItems_UnitPrice,DocumentItems_UnitCost,"
    "DocumentItems_UnitOfMeasure,DocumentItems_Manufacturer\n"
)


def _qw_row(doc_no, doc_type="QUOTE", doc_status="Open",
            doc_date="3/15/2025", total=100.0, sold_to="CDCR",
            ship_to="CDCR", po_number="", converted_ref="",
            description="Test Item", mfg_part="MFG-1",
            vendor_part="", qty=1, unit_price=10.0, unit_cost=7.0,
            uom="EA", manufacturer=""):
    return (
        f'"{doc_no}","{doc_type}","{doc_status}","{doc_date}","{total}",'
        f'"{sold_to}","{ship_to}","{po_number}","{converted_ref}",'
        f'"{description}","{mfg_part}","{vendor_part}","{qty}",'
        f'"{unit_price}","{unit_cost}","{uom}","{manufacturer}"\n'
    )


def _write_qw_csv(rows):
    fh = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                      encoding="utf-8", newline="")
    fh.write(_QW_HEADER)
    for r in rows:
        fh.write(r)
    fh.close()
    return fh.name


class TestQuoteWerksImport:
    def test_dry_run(self):
        from scripts.import_quotewerks_export import import_csv
        path = _write_qw_csv([
            _qw_row("Q-T1", description="Glove M", mfg_part="GL-M"),
            _qw_row("Q-T1", description="Glove L", mfg_part="GL-L"),
            _qw_row("Q-T2", description="Bandage", doc_status="Closed"),
        ])
        try:
            r = import_csv(path, dry_run=True)
            assert r["quotes_read"] == 2
            assert r["items_total"] == 3
            assert r["by_status"].get("won") == 1   # closed
            assert r["by_status"].get("sent") == 1  # open
        finally:
            os.unlink(path)

    def test_inserts_real_rows(self):
        from scripts.import_quotewerks_export import import_csv
        path = _write_qw_csv([
            _qw_row("Q-T-INS", description="Real Item", po_number="PO-X"),
        ])
        try:
            r = import_csv(path)
            assert r["quotes_inserted"] == 1
            with get_db() as conn:
                row = conn.execute(
                    "SELECT * FROM quotes WHERE quote_number=?",
                    ("Q-T-INS",)
                ).fetchone()
            assert row is not None
            assert row["status"] == "sent"
            items = json.loads(row["line_items"])
            assert items[0]["description"] == "Real Item"
        finally:
            os.unlink(path)

    def test_idempotent_reimport(self):
        from scripts.import_quotewerks_export import import_csv
        path = _write_qw_csv([
            _qw_row("Q-T-IDEM", description="Idem Item"),
        ])
        try:
            r1 = import_csv(path)
            r2 = import_csv(path)
            assert r1["quotes_inserted"] == 1
            assert r2["quotes_inserted"] == 0
            assert r2["quotes_updated"] == 1
        finally:
            os.unlink(path)


# ── SCPRS Reytech-wins importer ──────────────────────────────────────


def _wins_html(pos):
    """Build a SCPRS-style HTML table from PO dicts.

    Each PO dict: {po, dept, date, total, items: [{line, desc}]}
    """
    parts = ["<table>",
             "<tr><th>Business Unit</th><th>Department Name</th>"
             "<th>Purchase Document #</th><th>Associated PO #</th>"
             "<th>Start Date</th><th>End Date</th><th>Grand Total</th>"
             "<th>PO Total</th><th>Line #</th><th>Item ID</th>"
             "<th>Item Description</th><th>UNSPSC</th></tr>"]
    for p in pos:
        parts.append(
            f"<tr><td>'8955</td><td>{p['dept']}</td>"
            f"<td>'{p['po']}</td><td></td><td>{p['date']}</td><td></td>"
            f"<td>${p['total']}</td><td></td><td></td><td></td>"
            f"<td></td><td></td></tr>"
        )
        for it in p.get("items", []):
            parts.append(
                f"<tr><td>'8955</td><td>{p['dept']}</td>"
                f"<td>'{p['po']}</td><td></td><td></td><td></td>"
                f"<td></td><td></td><td>{it['line']}</td>"
                f"<td></td><td>{it['desc']}</td><td>42311500</td></tr>"
            )
    parts.append("</table>")
    return "".join(parts)


class TestScprsWinsImport:
    def test_parse_and_insert(self):
        from scripts.import_scprs_reytech_wins import import_html
        html = _wins_html([
            {"po": "PO-WIN-1", "dept": "Dept of Veterans Affairs",
             "date": "03/15/2025", "total": 5000,
             "items": [
                 {"line": "1", "desc": "Glove Medium Powder Free"},
                 {"line": "2", "desc": "Bandage 4x4 Sterile"},
             ]},
            {"po": "PO-WIN-2", "dept": "Dept of Corrections",
             "date": "06/01/2025", "total": 1200,
             "items": [{"line": "1", "desc": "N95 Respirator"}]},
        ])
        r = import_html(html)
        assert r["pos_parsed"] == 2
        assert r["pos_inserted"] == 2
        assert r["items_total"] == 3
        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM scprs_reytech_wins WHERE po_number=?",
                ("PO-WIN-1",)
            ).fetchone()
        assert row is not None
        assert row["dept_name"] == "Dept of Veterans Affairs"
        assert row["grand_total"] == 5000
        items = json.loads(row["items_json"])
        assert len(items) == 2

    def test_idempotent_reimport_overwrites(self):
        from scripts.import_scprs_reytech_wins import import_html
        html_v1 = _wins_html([
            {"po": "PO-IDEM", "dept": "X", "date": "01/01/2025",
             "total": 100, "items": [{"line": "1", "desc": "OldDesc"}]},
        ])
        html_v2 = _wins_html([
            {"po": "PO-IDEM", "dept": "X", "date": "01/01/2025",
             "total": 200, "items": [{"line": "1", "desc": "NewDesc"}]},
        ])
        import_html(html_v1)
        r2 = import_html(html_v2)
        assert r2["pos_inserted"] == 0
        assert r2["pos_updated"] == 1
        with get_db() as conn:
            row = conn.execute(
                "SELECT grand_total, items_json FROM scprs_reytech_wins "
                "WHERE po_number=?", ("PO-IDEM",)
            ).fetchone()
        assert row["grand_total"] == 200
        assert "NewDesc" in row["items_json"]


# ── Verify QuoteWerks outcomes ───────────────────────────────────────


class TestVerifyQuotewerksOutcomes:
    def _seed_sent_quote(self, qnum, agency, items, created_at="2025-03-10"):
        with get_db() as conn:
            conn.execute("""
                INSERT INTO quotes (quote_number, status, agency, institution,
                                    line_items, total, created_at, is_test)
                VALUES (?, 'sent', ?, ?, ?, 0, ?, 0)
            """, (qnum, agency, agency, json.dumps(items), created_at))
            conn.commit()

    def _seed_win(self, po, dept, items, date="2025-03-15"):
        from scripts.import_scprs_reytech_wins import _ensure_table
        _ensure_table()
        with get_db() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO scprs_reytech_wins
                (po_number, dept_name, start_date, items_json)
                VALUES (?, ?, ?, ?)
            """, (po, dept, date, json.dumps(items)))
            conn.commit()

    def test_match_marks_won(self):
        from src.core.oracle_backfill import verify_quotewerks_outcomes
        self._seed_sent_quote(
            "QW-V-1", "CDCR / Corrections",
            [{"description": "Glove Medium Powder Free", "unit_price": 11.95}],
        )
        self._seed_win("PO-V-1", "Dept of Corrections",
                       [{"description": "Glove Medium Powder-Free Box"}],
                       date="2025-03-15")

        r = verify_quotewerks_outcomes()
        assert r["source"] == "scprs_reytech_wins"
        assert r["marked_won"] >= 1
        with get_db() as conn:
            row = conn.execute(
                "SELECT status, po_number FROM quotes WHERE quote_number=?",
                ("QW-V-1",)
            ).fetchone()
        assert row["status"] == "won"
        assert row["po_number"] == "PO-V-1"

    def test_no_match_marks_lost(self):
        from src.core.oracle_backfill import verify_quotewerks_outcomes
        self._seed_sent_quote(
            "QW-V-2", "CalVet",
            [{"description": "Wheelchair Aluminum", "unit_price": 200.0}],
        )
        self._seed_win("PO-V-OTHER", "Other Dept",
                       [{"description": "Different Item"}],
                       date="2025-03-15")
        r = verify_quotewerks_outcomes()
        assert r["marked_lost"] >= 1
        with get_db() as conn:
            row = conn.execute(
                "SELECT status FROM quotes WHERE quote_number=?",
                ("QW-V-2",)
            ).fetchone()
        assert row["status"] == "lost"

    def test_dry_run_doesnt_write(self):
        from src.core.oracle_backfill import verify_quotewerks_outcomes
        self._seed_sent_quote(
            "QW-V-DRY", "CDCR",
            [{"description": "Test Dry Item", "unit_price": 5.0}],
        )
        self._seed_win("PO-V-DRY", "CDCR",
                       [{"description": "Test Dry Item"}])
        r = verify_quotewerks_outcomes(dry_run=True)
        with get_db() as conn:
            row = conn.execute(
                "SELECT status FROM quotes WHERE quote_number=?",
                ("QW-V-DRY",)
            ).fetchone()
        assert row["status"] == "sent"
