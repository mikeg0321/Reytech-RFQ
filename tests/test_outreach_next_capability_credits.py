"""
Tests for V2-PR-3 capability-credit assembler on /outreach/next.

Thesis: in CA public-sector procurement, "Reytech delivered X to CDCR
on PO Y" is 10× more credible to a procurement officer than any
capability statement. This module tests:

  1. Migration 25 creates outreach_credit_shown feedback table
  2. _capability_credits_by_dept — sourced from scprs_po_lines JOIN
     scprs_po_master (NOT won_quotes_kb — that table lacks category,
     dept_code, is_test, and its winning_price is polluted with
     line-total-as-unit-price per feedback_scprs_prices)
  3. Per-unit price normalization (line_total / quantity)
  4. Preference order: same_dept_and_category > category_only >
     same_dept_only
  5. is_test=0 filter on both tables
  6. 24-month age window
  7. Reytech canonical-supplier pattern match
  8. Feature flag FEATURE_CAPABILITY_CREDITS rollback
  9. Feedback write-path: _log_credits_shown records one row per
     (prospect_dept, credit_po) render
 10. E2E: credit block renders on card with history, suppressed
     entirely on card without history
"""
import os
import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta

import pytest


def _load_rmod():
    import sys, importlib.util, types
    key = "routes_outreach_next_test"
    if key in sys.modules:
        return sys.modules[key]
    path = os.path.abspath("src/api/modules/routes_outreach_next.py")
    spec = importlib.util.spec_from_file_location(key, path)
    mod = importlib.util.module_from_spec(spec)

    class _Bp:
        def route(self, *a, **k):
            return lambda f: f
    shared_stub = types.ModuleType("src.api.shared")
    shared_stub.bp = _Bp()
    shared_stub.auth_required = lambda f: f
    sys.modules.setdefault("src.api.shared", shared_stub)
    render_stub = types.ModuleType("src.api.render")
    render_stub.render_page = lambda *a, **k: ""
    sys.modules.setdefault("src.api.render", render_stub)
    try:
        spec.loader.exec_module(mod)
    except Exception:
        from importlib import import_module
        mod = import_module("src.api.modules.routes_outreach_next")
    sys.modules[key] = mod
    return mod


# ── Migration 25 ─────────────────────────────────────────────────────────────

def test_migration_25_creates_outreach_credit_shown(tmp_path):
    db_path = str(tmp_path / "mig25.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    mig = next(m for m in MIGRATIONS if m[0] == 25)
    conn.executescript(mig[2])
    cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(outreach_credit_shown)").fetchall()}
    for required in {"prospect_dept_code", "credit_po_number",
                     "credit_dept_code", "credit_category", "match_type",
                     "shown_at", "is_test"}:
        assert required in cols, f"missing: {required}"
    # Idempotent.
    conn.executescript(mig[2])
    conn.close()


# ── SCPRS test fixture with Reytech wins ─────────────────────────────────────

@pytest.fixture
def scprs_with_reytech_wins(tmp_path):
    """Seed scprs_po_master + scprs_po_lines with:
      - 2 Reytech wins at CCHCS in exam_gloves (category match + dept match)
      - 1 Reytech win at CDCR in exam_gloves (category only — diff dept)
      - 1 Reytech win at CCHCS in sharps (same dept only — diff category)
      - 1 Medline contract at CCHCS (NOT Reytech — must not match)
      - 1 stale Reytech win (3 years ago — must be filtered by age)
      - 1 is_test=1 Reytech win (must be filtered)
      - 1 Reytech win where line_total=0 (must be filtered — not shippable)
    """
    db_path = str(tmp_path / "credits.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_number TEXT UNIQUE, dept_code TEXT, dept_name TEXT,
            supplier TEXT, grand_total REAL, start_date TEXT,
            is_test INTEGER DEFAULT 0
        );
        CREATE TABLE scprs_po_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_id INTEGER, po_number TEXT, line_num INTEGER,
            description TEXT, category TEXT,
            quantity REAL, unit_price REAL, line_total REAL,
            opportunity_flag TEXT, is_test INTEGER DEFAULT 0
        );
    """)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 25)[2])

    today = date.today()
    recent = (today - timedelta(days=90)).isoformat()
    older = (today - timedelta(days=400)).isoformat()
    stale = (today - timedelta(days=3 * 365)).isoformat()

    def seed(po, dept, dname, supplier, start, desc, cat, qty, line_total,
             is_test=0):
        cur = conn.execute(
            "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
            "supplier, grand_total, start_date, is_test) "
            "VALUES (?,?,?,?,?,?,?)",
            (po, dept, dname, supplier, line_total, start, is_test),
        )
        conn.execute(
            "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
            "description, category, quantity, unit_price, line_total, "
            "is_test) VALUES (?,?,?,?,?,?,?,?,?)",
            (cur.lastrowid, po, 0, desc, cat, qty, line_total / qty if qty else 0,
             line_total, is_test),
        )

    # CCHCS + exam_gloves + Reytech (most recent, highest credit)
    seed("R26Q0321", "4700", "CCHCS", "Reytech Inc.", recent,
         "nitrile exam gloves medium", "exam_gloves", 500, 3840.0)
    # CCHCS + exam_gloves + Reytech (older but within 24mo)
    seed("R25Q8100", "4700", "CCHCS", "REYTECH INCORPORATED", older,
         "nitrile exam gloves large", "exam_gloves", 400, 3200.0)
    # CDCR + exam_gloves + Reytech (category match, different dept)
    seed("R26Q0500", "5225", "CDCR", "Rey-Tech", recent,
         "nitrile exam gloves M", "exam_gloves", 600, 4800.0)
    # CCHCS + sharps + Reytech (same dept, different category)
    seed("R26Q0200", "4700", "CCHCS", "Reytech Inc.", recent,
         "sharps container 1 gal", "sharps", 100, 2500.0)
    # CCHCS + exam_gloves + MEDLINE (NOT Reytech — must not match)
    seed("PO-MED-1", "4700", "CCHCS", "Medline Industries", recent,
         "gloves nitrile S", "exam_gloves", 300, 2400.0)
    # STALE Reytech win (3 years ago)
    seed("R23Q0099", "4700", "CCHCS", "Reytech Inc.", stale,
         "ancient gloves order", "exam_gloves", 100, 800.0)
    # is_test=1 Reytech win (must be filtered)
    seed("R26Q-TEST", "4700", "CCHCS", "Reytech Inc.", recent,
         "synthetic test gloves", "exam_gloves", 100, 9999.0, is_test=1)
    # Reytech win with line_total=0 (un-shippable — must be filtered)
    seed("R26Q-ZERO", "4700", "CCHCS", "Reytech Inc.", recent,
         "zero total gloves", "exam_gloves", 10, 0.0)

    conn.commit()
    return conn


# ── _capability_credits_by_dept ──────────────────────────────────────────────

def test_capability_credits_prefers_same_dept_and_category(scprs_with_reytech_wins):
    """With prospect dept=CCHCS (4700) and cats=[exam_gloves], only
    same-dept+same-category candidates exist at CCHCS: R26Q0321 (90d),
    R25Q8100 (400d). Both should appear, newest first."""
    mod = _load_rmod()
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"4700": ["exam_gloves"]},  # single category so we can assert order
        limit_per=2,
    )
    credits = result["4700"]
    assert len(credits) == 2
    # Both in the best bucket.
    assert all(c["match_type"] == "same_dept_and_category" for c in credits)
    # Most recent first (ORDER BY start_date DESC within bucket).
    assert credits[0]["po_number"] == "R26Q0321"
    assert credits[1]["po_number"] == "R25Q8100"


def test_capability_credits_mixed_categories_picks_by_date_within_best_bucket(
    scprs_with_reytech_wins
):
    """When prospect wants BOTH exam_gloves and sharps, multiple
    same_dept_and_category credits are available; date ordering decides
    within the bucket. Documents the actual behavior."""
    mod = _load_rmod()
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"4700": ["exam_gloves", "sharps"]},
        limit_per=2,
    )
    credits = result["4700"]
    assert len(credits) == 2
    # All qualify as same_dept_and_category; date DESC picks R26Q0321
    # (90d) and R26Q0200 (90d), both newer than R25Q8100 (400d).
    assert all(c["match_type"] == "same_dept_and_category" for c in credits)
    top_pos = {c["po_number"] for c in credits}
    assert top_pos == {"R26Q0321", "R26Q0200"}


def test_capability_credits_falls_back_to_category_only(scprs_with_reytech_wins):
    """When the prospect agency has no same-dept wins, fall back to
    category-only matches from other agencies."""
    mod = _load_rmod()
    # Use a dept_code that has no Reytech history.
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"7800": ["exam_gloves"]},  # CalVet — no Reytech rows seeded
        limit_per=3,
    )
    credits = result["7800"]
    # Every credit is category_only (different dept, matching category).
    assert len(credits) >= 1
    assert all(c["match_type"] == "category_only" for c in credits)


def test_capability_credits_per_unit_normalization(scprs_with_reytech_wins):
    """Per-unit price MUST come from line_total/quantity, NEVER from
    unit_price column (which in SCPRS is often a line total masquerading
    as unit price — feedback_scprs_prices)."""
    mod = _load_rmod()
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"4700": ["exam_gloves"]},
        limit_per=1,
    )
    credit = result["4700"][0]
    # R26Q0321 = 500qty × $3,840 line_total → $7.68/unit.
    assert credit["per_unit_price"] == 7.68
    assert credit["quantity"] == 500
    assert credit["line_total"] == 3840.0


def test_capability_credits_filters_is_test(scprs_with_reytech_wins):
    mod = _load_rmod()
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"4700": ["exam_gloves"]},
        limit_per=10,
    )
    po_numbers = {c["po_number"] for c in result["4700"]}
    assert "R26Q-TEST" not in po_numbers


def test_capability_credits_filters_zero_line_total(scprs_with_reytech_wins):
    """Row with line_total=0 can't produce a shippable per-unit figure
    — must be excluded."""
    mod = _load_rmod()
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"4700": ["exam_gloves"]},
        limit_per=10,
    )
    po_numbers = {c["po_number"] for c in result["4700"]}
    assert "R26Q-ZERO" not in po_numbers


def test_capability_credits_filters_old_wins_outside_24mo(scprs_with_reytech_wins):
    mod = _load_rmod()
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"4700": ["exam_gloves"]},
        limit_per=10,
        age_months=24,
    )
    po_numbers = {c["po_number"] for c in result["4700"]}
    assert "R23Q0099" not in po_numbers


def test_capability_credits_excludes_non_reytech_suppliers(scprs_with_reytech_wins):
    mod = _load_rmod()
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"4700": ["exam_gloves"]},
        limit_per=10,
    )
    po_numbers = {c["po_number"] for c in result["4700"]}
    assert "PO-MED-1" not in po_numbers  # Medline, not Reytech


def test_capability_credits_matches_reytech_variants(scprs_with_reytech_wins):
    """Canonical supplier patterns: Reytech Inc., REYTECH INCORPORATED,
    Rey-Tech all match."""
    mod = _load_rmod()
    result = mod._capability_credits_by_dept(
        scprs_with_reytech_wins,
        {"4700": ["exam_gloves"], "5225": ["exam_gloves"]},
        limit_per=10,
    )
    all_pos = {c["po_number"] for dc in result for c in result[dc]}
    # Rey-Tech variant (CDCR) matches.
    assert "R26Q0500" in all_pos
    # REYTECH INCORPORATED variant (CCHCS) matches.
    assert "R25Q8100" in all_pos


def test_capability_credits_empty_input(scprs_with_reytech_wins):
    mod = _load_rmod()
    assert mod._capability_credits_by_dept(
        scprs_with_reytech_wins, {}, limit_per=2) == {}


def test_capability_credits_schema_tolerant_without_tables(tmp_path):
    mod = _load_rmod()
    conn = sqlite3.connect(str(tmp_path / "empty.db"))
    conn.row_factory = sqlite3.Row
    assert mod._capability_credits_by_dept(
        conn, {"4700": ["exam_gloves"]}) == {}


# ── _log_credits_shown feedback write-path ────────────────────────────────────

def test_log_credits_shown_writes_one_row_per_credit(tmp_path):
    mod = _load_rmod()
    db_path = str(tmp_path / "log.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 25)[2])
    credits = [
        {"po_number": "R26Q0321", "credit_dept_code": "4700",
         "category": "exam_gloves", "match_type": "same_dept_and_category"},
        {"po_number": "R26Q0500", "credit_dept_code": "5225",
         "category": "exam_gloves", "match_type": "category_only"},
    ]
    mod._log_credits_shown(conn, "4700", credits)
    conn.commit()
    rows = conn.execute(
        "SELECT prospect_dept_code, credit_po_number, match_type "
        "FROM outreach_credit_shown ORDER BY id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0][1] == "R26Q0321"
    assert rows[1][1] == "R26Q0500"


def test_log_credits_shown_no_op_on_empty(tmp_path):
    mod = _load_rmod()
    db_path = str(tmp_path / "log_empty.db")
    conn = sqlite3.connect(db_path)
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 25)[2])
    mod._log_credits_shown(conn, "4700", [])  # must not raise
    count = conn.execute(
        "SELECT COUNT(*) FROM outreach_credit_shown").fetchone()[0]
    assert count == 0


def test_log_credits_shown_suppresses_table_missing(tmp_path):
    mod = _load_rmod()
    conn = sqlite3.connect(str(tmp_path / "no_table.db"))
    # No table exists — helper must not crash the page.
    mod._log_credits_shown(conn, "4700", [{"po_number": "X",
        "credit_dept_code": "4700", "category": "c", "match_type": "x"}])


# ── Feature flag ──────────────────────────────────────────────────────────────

def test_feature_flag_default_on(monkeypatch):
    mod = _load_rmod()
    # Fresh fetch — flag not in env, default should be True.
    assert mod._capability_credits_enabled() is True


def test_feature_flag_respects_false(monkeypatch):
    mod = _load_rmod()
    import src.core.flags as flags
    monkeypatch.setattr(flags, "get_flag",
                        lambda key, default: False if key == "FEATURE_CAPABILITY_CREDITS" else default)
    assert mod._capability_credits_enabled() is False


# ── E2E: renders on card with history, suppressed without ─────────────────────

def test_e2e_card_with_reytech_history_shows_credit_block(
    auth_client, tmp_path, monkeypatch
):
    data_dir = tmp_path / "credits_e2e"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    db = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE IF NOT EXISTS quotes ("
        "id INTEGER PRIMARY KEY, quote_number TEXT, agency TEXT, "
        "status TEXT, source TEXT, created_at TEXT, is_test INTEGER DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS email_outbox ("
        "id TEXT PRIMARY KEY, to_address TEXT, status TEXT, sent_at TEXT)"
    )
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 24)[2])
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 25)[2])

    today = date.today()

    # Prospect buy pattern: CCHCS buys nitrile gloves from Medline (today)
    cur = conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "agency_code, supplier, grand_total, start_date, buyer_name, "
        "buyer_email) VALUES (?,?,?,?,?,?,?,?,?)",
        ("PO-CCHCS-MED", "4700", "CCHCS", "4700", "Medline", 12000.0,
         today.isoformat(), "Jane", "jane@cchcs.gov"),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
        "description, category, quantity, unit_price, line_total, "
        "reytech_sells, opportunity_flag) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (cur.lastrowid, "PO-CCHCS-MED", 0, "nitrile gloves M",
         "exam_gloves", 500, 8.0, 4000.0, 1, "WIN_BACK"),
    )

    # Reytech's credential at CCHCS in the same category.
    recent = (today - timedelta(days=60)).isoformat()
    cur = conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "supplier, grand_total, start_date) VALUES (?,?,?,?,?,?)",
        ("R26Q0321", "4700", "CCHCS", "Reytech Inc.", 3840.0, recent),
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
    body = resp.data.decode("utf-8", errors="replace")

    # Credit block renders + cites the specific PO and per-unit price.
    assert "Reytech capability credits" in body
    assert "R26Q0321" in body
    assert "$7.68/unit" in body
    assert "nitrile exam gloves medium" in body


def test_e2e_card_without_reytech_history_suppresses_credit_block(
    auth_client, tmp_path, monkeypatch
):
    """When the prospect's agency has no Reytech win in their categories,
    the credit block is SUPPRESSED (not shown with a 'no history'
    placeholder). Per product-engineer review: empty blocks become
    wallpaper; suppress to keep signal high."""
    data_dir = tmp_path / "credits_empty"
    data_dir.mkdir(exist_ok=True)
    import src.agents.scprs_universal_pull as sup
    monkeypatch.setattr(sup, "DATA_DIR", str(data_dir))
    sup._ensure_schema()
    db = str(data_dir / "reytech.db")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE IF NOT EXISTS quotes ("
        "id INTEGER PRIMARY KEY, quote_number TEXT, agency TEXT, "
        "status TEXT, source TEXT, created_at TEXT, is_test INTEGER DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS email_outbox ("
        "id TEXT PRIMARY KEY, to_address TEXT, status TEXT, sent_at TEXT)"
    )
    from src.core.migrations import MIGRATIONS
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 24)[2])
    conn.executescript(next(m for m in MIGRATIONS if m[0] == 25)[2])
    today = date.today()
    cur = conn.execute(
        "INSERT INTO scprs_po_master (po_number, dept_code, dept_name, "
        "agency_code, supplier, grand_total, start_date, buyer_name, "
        "buyer_email) VALUES (?,?,?,?,?,?,?,?,?)",
        ("PO-NEW", "9999", "AGENCY-NEW", "9999", "Grainger", 8000.0,
         today.isoformat(), "Alice", "alice@new.gov"),
    )
    conn.execute(
        "INSERT INTO scprs_po_lines (po_id, po_number, line_num, "
        "description, category, quantity, unit_price, line_total, "
        "reytech_sells, opportunity_flag) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (cur.lastrowid, "PO-NEW", 0, "some niche item",
         "niche", 200, 40.0, 8000.0, 0, "GAP_ITEM"),
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
    body = resp.data.decode("utf-8", errors="replace")
    # No Reytech wins in DB → no credit block.
    assert "Reytech capability credits" not in body
