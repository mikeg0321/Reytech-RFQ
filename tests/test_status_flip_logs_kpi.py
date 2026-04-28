"""Regression test for the KPI gap PR #621 surfaced.

Originating finding: PR #619 (last-5 quotes cost_source chips) and PR
#620 (buyer pricing memory) both render off `operator_quote_sent`. On
prod, the table was empty even though Win/Loss showed 102 won + 379
lost — meaning hundreds of historical quotes flipped to status='sent'
without writing a KPI row.

Trace: two RFQ-side send paths flip the quote status to 'sent' via
`update_quote_status(qn, 'sent')` but skip `log_quote_sent()`:
  - routes_rfq_admin.py:833 (Email Sent flow)
  - routes_rfq_gen.py:2968  (RFQ generated bid response email)

Two paths that DO log:
  - routes_pricecheck_admin.py:5615 (PC send-quote)
  - routes_rfq_admin.py:324 (RFQ admin Mark Sent endpoint)

Fix: centralize the KPI log inside `update_quote_status` so any path —
existing or future — gets telemetry on the transition into 'sent'.
Idempotency: only fire when OLD status != 'sent' so re-saves don't
double-count. Test quotes are excluded so smoke fixtures don't
pollute the KPI table.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

import pytest


def _ensure_kpi_table(conn):
    """The conftest's init_db() runs SCHEMA but not migrations. The
    operator_quote_sent table lives in migration #34. Same workaround
    PR #619's tests use."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS operator_quote_sent (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            quote_id TEXT NOT NULL,
            quote_type TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            started_at TEXT,
            time_to_send_seconds INTEGER,
            item_count INTEGER DEFAULT 0,
            agency_key TEXT,
            quote_total REAL DEFAULT 0
        )
    """)


def _seed_quote(conn, *, quote_number, status="pending", agency="CDCR",
                total=100.0, source_rfq_id=None, source_pc_id=None,
                items_count=2, is_test=0):
    """Seed a quote with line_items matching items_count. The KPI write
    inside update_quote_status sources item_count from len(line_items)
    because get_all_quotes' SELECT doesn't include the items_count
    column — the in-memory shape and the DB shape diverge slightly
    here. Seeding real line items keeps the test honest about what
    update_quote_status actually sees."""
    when = datetime.now().isoformat()
    line_items = [{"description": f"item {i}", "qty": 1,
                   "unit_price": 10.0} for i in range(items_count)]
    conn.execute("""
        INSERT INTO quotes
          (quote_number, agency, status, total, created_at, updated_at,
           contact_email, items_count, items_detail, line_items,
           source_rfq_id, source_pc_id, is_test, status_history)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (quote_number, agency, status, total, when, when,
          "buyer@example.gov", items_count,
          json.dumps(line_items), json.dumps(line_items),
          source_rfq_id, source_pc_id, is_test, "[]"))


def _kpi_count(conn, quote_id: str) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM operator_quote_sent WHERE quote_id = ?",
        (quote_id,),
    ).fetchone()[0]


def _wipe(conn):
    _ensure_kpi_table(conn)
    conn.execute("DELETE FROM quotes")
    conn.execute("DELETE FROM operator_quote_sent")
    conn.commit()


def test_status_flip_to_sent_writes_kpi_row():
    """Headline: a fresh pending → sent transition writes one row to
    operator_quote_sent. This is what the §4.1 KPI surface, PR #619 chips
    card, and PR #620 buyer panel all read."""
    from src.core.db import get_db
    from src.forms.quote_generator import update_quote_status

    qn = "R26K001"
    with get_db() as c:
        _wipe(c)
        _seed_quote(c, quote_number=qn, status="pending",
                    items_count=3, source_rfq_id="rfq-test-1")
        c.commit()

    assert update_quote_status(qn, "sent", actor="user") is True

    with get_db() as c:
        assert _kpi_count(c, qn) == 1
        row = c.execute(
            "SELECT quote_type, item_count, agency_key, quote_total "
            "FROM operator_quote_sent WHERE quote_id = ?", (qn,),
        ).fetchone()
    # source_rfq_id present → quote_type rfq
    assert row["quote_type"] == "rfq"
    assert row["item_count"] == 3
    assert row["agency_key"] == "CDCR"
    assert abs(row["quote_total"] - 100.0) < 0.01


def test_kpi_idempotent_on_resave_of_sent():
    """If the same quote has its status set to 'sent' twice (operator
    double-click, automation retry, etc.), only ONE KPI row should be
    written — the transition is what we measure, not the persistence."""
    from src.core.db import get_db
    from src.forms.quote_generator import update_quote_status

    qn = "R26K002"
    with get_db() as c:
        _wipe(c)
        _seed_quote(c, quote_number=qn, status="pending")
        c.commit()

    update_quote_status(qn, "sent", actor="user")
    update_quote_status(qn, "sent", actor="user")  # idempotent re-save

    with get_db() as c:
        assert _kpi_count(c, qn) == 1


def test_kpi_not_written_for_test_quotes():
    """is_test=1 quotes (smoke fixtures, regression seeds) must not
    write KPI rows — they'd skew the operator's <90s percentile."""
    from src.core.db import get_db
    from src.forms.quote_generator import update_quote_status

    qn = "TEST-KPI-1"
    with get_db() as c:
        _wipe(c)
        _seed_quote(c, quote_number=qn, status="pending", is_test=1)
        c.commit()

    update_quote_status(qn, "sent", actor="user")

    with get_db() as c:
        assert _kpi_count(c, qn) == 0


def test_kpi_quote_type_pc_when_source_pc_id():
    """Source-PC quotes get quote_type='pc'; source-RFQ get 'rfq'.
    The chips card uses this to load the right detail row."""
    from src.core.db import get_db
    from src.forms.quote_generator import update_quote_status

    qn = "R26K003"
    with get_db() as c:
        _wipe(c)
        _seed_quote(c, quote_number=qn, status="pending",
                    source_pc_id="pc-test-1")  # no source_rfq_id
        c.commit()

    update_quote_status(qn, "sent", actor="user")

    with get_db() as c:
        row = c.execute("SELECT quote_type FROM operator_quote_sent "
                        "WHERE quote_id = ?", (qn,)).fetchone()
    assert row["quote_type"] == "pc"


def test_status_flip_to_other_states_does_not_write_kpi():
    """KPI fires on 'sent' specifically. Marks-as-won, lost, cancelled
    must not — they're outcomes downstream of send, not sends
    themselves."""
    from src.core.db import get_db
    from src.forms.quote_generator import update_quote_status

    qn = "R26K004"
    with get_db() as c:
        _wipe(c)
        _seed_quote(c, quote_number=qn, status="sent")  # already sent
        c.commit()

    update_quote_status(qn, "won", po_number="PO-12345", actor="user")
    update_quote_status(qn, "lost", actor="user")  # operator changed mind
    update_quote_status(qn, "cancelled", actor="user")

    with get_db() as c:
        assert _kpi_count(c, qn) == 0
