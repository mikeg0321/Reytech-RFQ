"""Throwaway: dump /outreach/next with V2-PR-8 template UI for Chrome MCP."""
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta


def test_dump_v2_template_states(auth_client, monkeypatch, tmp_path):
    data_dir = tmp_path / "v2_tpl_dump"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    db = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE quotes (id INTEGER PRIMARY KEY, quote_number TEXT, "
        "agency TEXT, status TEXT, source TEXT, created_at TEXT, "
        "is_test INTEGER DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE email_outbox (id TEXT PRIMARY KEY, to_address TEXT, "
        "status TEXT, subject TEXT, sent_at TEXT, created_at TEXT)"
    )
    from src.core.migrations import MIGRATIONS, _run_migration_28
    for v in (24, 25, 26, 27):
        conn.executescript(next(m for m in MIGRATIONS if m[0] == v)[2])
    _run_migration_28(conn)

    today = date.today()

    def seed_p(po, dept, dname, supplier, total, desc, cat, qty, lt, sells,
                opp, bn, be):
        cur = conn.execute(
            "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
            "agency_code, supplier, grand_total, start_date, buyer_name, "
            "buyer_email, end_date) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (po, dept, dname, dept, supplier, total, today.isoformat(),
             bn, be,
             (today + timedelta(days=45)).isoformat() if "MED" in supplier else ""),
        )
        conn.execute(
            "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
            "description, category, quantity, unit_price, line_total, "
            "reytech_sells, opportunity_flag) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (cur.lastrowid, po, 0, desc, cat, qty, lt/qty, lt, sells, opp),
        )

    # CCHCS — registered + has rebid window → rebid_memo
    seed_p("PO-CCHCS-MED", "4700", "CCHCS / Correctional Health",
           "Medline Industries", 12000.0, "nitrile gloves M",
           "exam_gloves", 500, 4000.0, 1, "WIN_BACK", "Jane",
           "jane@cchcs.gov")
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source, "
        "procurement_officer_email, procurement_officer_name) "
        "VALUES ('4700', 'registered', 'operator', 'officer@cchcs.ca.gov', "
        "'CCHCS Procurement Officer')"
    )

    # CDCR — not_registered → rfq_list_inclusion
    seed_p("PO-CDCR-1", "5225", "CDCR / Corrections", "Cardinal", 9000.0,
           "disinfectant wipes", "exam_gloves", 400, 4000.0, 0,
           "GAP_ITEM", "Bob", "bob@cdcr.gov")
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source) "
        "VALUES ('5225', 'not_registered', 'operator')"
    )

    # DSH — registered + no rebid → capability_refresher
    seed_p("PO-DSH-1", "4440", "DSH / State Hospitals", "Henry Schein",
           5500.0, "absorbent underpads", "incontinence", 200, 5500.0, 1,
           "WIN_BACK", "Carol", "carol@dsh.gov")
    conn.execute(
        "INSERT INTO agency_vendor_registry (dept_code, status, source, "
        "procurement_officer_email) "
        "VALUES ('4440', 'registered', 'operator', 'po@dsh.ca.gov')"
    )

    # Reytech cert (so {sb_cert_clause} fills in)
    conn.execute(
        "INSERT INTO reytech_certifications (cert_type, cert_number, "
        "expires_at, is_active) VALUES ('SB', 'SB-12345', ?, 1)",
        ((today + timedelta(days=365)).isoformat(),),
    )

    # Reytech capability credit (CCHCS exam_gloves win)
    cur = conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "supplier, grand_total, start_date) VALUES (?,?,?,?,?,?)",
        ("R26Q0321", "4700", "CCHCS", "Reytech Inc.", 3840.0,
         (today - timedelta(days=60)).isoformat()),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
        "description, category, quantity, unit_price, line_total) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (cur.lastrowid, "R26Q0321", 0, "nitrile exam gloves medium",
         "exam_gloves", 500, 7.68, 3840.0),
    )

    conn.commit()
    conn.close()

    @contextmanager
    def _seeded():
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        try:
            yield c
            c.commit()
        finally:
            c.close()
    monkeypatch.setattr("src.core.db.get_db", _seeded)
    import src.agents.prospect_scorer as scorer
    monkeypatch.setattr(scorer, "_get_db", _seeded)

    resp = auth_client.get("/outreach/next")
    assert resp.status_code == 200
    out = os.path.abspath("docs/screenshots/outreach_v2_templates.html")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "wb") as f:
        f.write(resp.data)
    print(f"\nWROTE: {out}\n")
