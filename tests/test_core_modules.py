"""
Test coverage for core modules — atomic saves, email forwarding detection,
supplier inference, item link extraction, institution resolver, and DB pooling.
"""
import json
import os
import threading


# ═══════════════════════════════════════════════════════════════════════════════
# 1. atomic_json_save (data_guard.py)
# ═══════════════════════════════════════════════════════════════════════════════

def test_atomic_json_save_creates_file(tmp_path):
    from src.core.data_guard import atomic_json_save
    path = str(tmp_path / "test.json")
    atomic_json_save(path, {"key": "value"})
    with open(path) as f:
        assert json.load(f) == {"key": "value"}


def test_atomic_json_save_overwrites(tmp_path):
    from src.core.data_guard import atomic_json_save
    path = str(tmp_path / "test.json")
    atomic_json_save(path, {"v": 1})
    atomic_json_save(path, {"v": 2})
    with open(path) as f:
        assert json.load(f) == {"v": 2}


def test_atomic_json_save_preserves_on_error(tmp_path):
    """If write fails, original file should be preserved."""
    from src.core.data_guard import atomic_json_save
    path = str(tmp_path / "test.json")
    atomic_json_save(path, {"original": True})

    # Try to save something that will fail serialization
    class BadObj:
        pass

    try:
        atomic_json_save(path, {"bad": BadObj()})
    except Exception:
        pass

    with open(path) as f:
        data = json.load(f)
    assert data == {"original": True}  # Original preserved


def test_atomic_json_save_creates_parent_dirs(tmp_path):
    from src.core.data_guard import atomic_json_save
    path = str(tmp_path / "nested" / "deep" / "test.json")
    atomic_json_save(path, [1, 2, 3])
    with open(path) as f:
        assert json.load(f) == [1, 2, 3]


# ═══════════════════════════════════════════════════════════════════════════════
# 2. _extract_forwarded_original (email_poller.py)
# ═══════════════════════════════════════════════════════════════════════════════

def test_forward_detection_gmail():
    from src.agents.email_poller import _extract_forwarded_original
    subject = "Fwd: Price Check Request - Medical Supplies"
    body = """Hey, can you handle this?

---------- Forwarded message ---------
From: Valentina Demidenko <vdemidenko@cdcr.ca.gov>
Date: Mon, Apr 1, 2026
Subject: Price Check Request - Medical Supplies
To: mike@reytechinc.com

Please provide pricing for the attached items."""

    sender, subj, clean_body, was_fwd = _extract_forwarded_original(
        subject, body, "mike@reytechinc.com")
    assert was_fwd is True
    assert sender == "vdemidenko@cdcr.ca.gov"
    assert "Fwd:" not in subj
    assert "Price Check Request" in subj
    assert "Forwarded message" not in clean_body


def test_forward_detection_outlook():
    from src.agents.email_poller import _extract_forwarded_original
    subject = "FW: RFQ for Office Supplies"
    body = """FYI

-----Original Message-----
From: Jane Doe <jane@state.ca.gov>
Sent: Monday, April 1, 2026
Subject: RFQ for Office Supplies

Need quotes by Friday."""

    sender, subj, clean_body, was_fwd = _extract_forwarded_original(
        subject, body, "mike@reytechinc.com")
    assert was_fwd is True
    assert sender == "jane@state.ca.gov"
    assert "FW:" not in subj


def test_non_forward_passthrough():
    from src.agents.email_poller import _extract_forwarded_original
    sender, subj, body, was_fwd = _extract_forwarded_original(
        "Price Check - Gloves", "Please quote attached items", "buyer@agency.gov")
    assert was_fwd is False
    assert sender == "buyer@agency.gov"
    assert subj == "Price Check - Gloves"


# ═══════════════════════════════════════════════════════════════════════════════
# 3. infer_supply_chain (supplier_inference.py)
# ═══════════════════════════════════════════════════════════════════════════════

def test_manufacturer_detection():
    from src.agents.supplier_inference import infer_supply_chain
    r = infer_supply_chain("Cardinal Health", 12.50, our_cost=14.70)
    assert r["channel"] == "manufacturer"
    assert r["confidence"] == "high"


def test_distributor_detection():
    from src.agents.supplier_inference import infer_supply_chain
    r = infer_supply_chain("Henry Schein", 25.00, our_cost=22.00)
    assert r["channel"] == "distributor"
    assert r["confidence"] == "high"
    assert r["actionable"] is True


def test_below_cost_inference():
    from src.agents.supplier_inference import infer_supply_chain
    r = infer_supply_chain("Smith Supplies", 8.00, our_cost=13.60)
    # Way below our cost = likely has direct/manufacturer account
    assert r["channel"] == "manufacturer"
    assert r["actionable"] is True


def test_similar_pricing_inference():
    from src.agents.supplier_inference import infer_supply_chain
    r = infer_supply_chain("ABC Gov", 15.50, our_cost=13.60)
    assert r["channel"] == "gov_reseller"


def test_no_price_returns_unknown():
    from src.agents.supplier_inference import infer_supply_chain
    r = infer_supply_chain("Some Corp", 0)
    assert r["channel"] == "unknown"
    assert r["confidence"] == "low"


# ═══════════════════════════════════════════════════════════════════════════════
# 4. _extract_target_tcin and _extract_asin (item_link_lookup.py)
# ═══════════════════════════════════════════════════════════════════════════════

def test_target_tcin():
    from src.agents.item_link_lookup import _extract_target_tcin
    assert _extract_target_tcin(
        "https://www.target.com/p/something/-/A-1005734521") == "1005734521"
    assert _extract_target_tcin("https://www.target.com/p/foo") == ""


def test_amazon_asin_dp():
    from src.agents.item_link_lookup import _extract_asin
    assert _extract_asin("https://www.amazon.com/dp/B0D45DB4BK") == "B0D45DB4BK"


def test_amazon_asin_mobile():
    from src.agents.item_link_lookup import _extract_asin
    assert _extract_asin(
        "https://www.amazon.com/gp/aw/d/B0D45DB4BK") == "B0D45DB4BK"


def test_amazon_asin_gp_product():
    from src.agents.item_link_lookup import _extract_asin
    assert _extract_asin(
        "https://www.amazon.com/gp/product/B0D45DB4BK") == "B0D45DB4BK"


def test_amazon_asin_no_match():
    from src.agents.item_link_lookup import _extract_asin
    assert _extract_asin("https://www.amazon.com/s?k=bandages") == ""


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Institution resolver
# ═══════════════════════════════════════════════════════════════════════════════

def test_csp_sacramento():
    from src.core.institution_resolver import resolve
    r = resolve("CSP-Sacramento")
    assert r["canonical"] == "California State Prison, Sacramento"
    assert r["agency"] == "cchcs"


def test_csp_sacramento_with_unit():
    from src.core.institution_resolver import resolve
    r = resolve("CSP-Sacramento/Medical")
    assert "Sacramento" in r["canonical"]
    assert r["agency"] == "cchcs"


def test_ciw():
    from src.core.institution_resolver import resolve
    r = resolve("CIW")
    assert r["canonical"] == "California Institution for Women"
    assert r["agency"] == "cchcs"


def test_empty_input():
    from src.core.institution_resolver import resolve
    r = resolve("")
    assert r["canonical"] == ""
    assert r["agency"] == ""


def test_unknown_institution():
    from src.core.institution_resolver import resolve
    r = resolve("Some Random Place")
    assert r["canonical"] == "Some Random Place"
    assert r["original"] == "Some Random Place"


# ═══════════════════════════════════════════════════════════════════════════════
# 6. DB thread-local connection pooling
# ═══════════════════════════════════════════════════════════════════════════════

def test_get_db_returns_connection(tmp_path, monkeypatch):
    """get_db() yields a working SQLite connection."""
    import src.core.db as db_mod
    monkeypatch.setattr(db_mod, "DB_PATH", str(tmp_path / "test.db"))
    # Reset thread-local so it picks up new DB_PATH
    db_mod._local.conn = None

    with db_mod.get_db() as conn:
        conn.execute("CREATE TABLE t (id INTEGER)")
        conn.execute("INSERT INTO t VALUES (1)")
    with db_mod.get_db() as conn:
        row = conn.execute("SELECT id FROM t").fetchone()
        assert row[0] == 1

    db_mod.close_thread_db()


def test_get_db_reuses_connection(tmp_path, monkeypatch):
    """Same thread should get the same connection object (thread-local reuse)."""
    import src.core.db as db_mod
    monkeypatch.setattr(db_mod, "DB_PATH", str(tmp_path / "test.db"))
    db_mod._local.conn = None

    with db_mod.get_db() as conn1:
        pass
    with db_mod.get_db() as conn2:
        pass
    assert conn1 is conn2

    db_mod.close_thread_db()


def test_get_db_different_threads_different_connections(tmp_path, monkeypatch):
    """Different threads should get different connection objects."""
    import src.core.db as db_mod
    monkeypatch.setattr(db_mod, "DB_PATH", str(tmp_path / "test.db"))
    db_mod._local.conn = None

    conns = []

    def grab_conn():
        with db_mod.get_db() as c:
            conns.append(id(c))
        db_mod.close_thread_db()

    with db_mod.get_db() as main_conn:
        conns.append(id(main_conn))

    t = threading.Thread(target=grab_conn)
    t.start()
    t.join()

    assert len(conns) == 2
    assert conns[0] != conns[1]  # Different connection objects

    db_mod.close_thread_db()


def test_close_thread_db(tmp_path, monkeypatch):
    """close_thread_db() clears the thread-local connection."""
    import src.core.db as db_mod
    monkeypatch.setattr(db_mod, "DB_PATH", str(tmp_path / "test.db"))
    db_mod._local.conn = None

    with db_mod.get_db() as conn:
        pass
    assert db_mod._local.conn is not None

    db_mod.close_thread_db()
    assert db_mod._local.conn is None


def test_get_db_rollback_on_error(tmp_path, monkeypatch):
    """On exception, transaction should be rolled back."""
    import src.core.db as db_mod
    monkeypatch.setattr(db_mod, "DB_PATH", str(tmp_path / "test.db"))
    db_mod._local.conn = None

    with db_mod.get_db() as conn:
        conn.execute("CREATE TABLE t (id INTEGER)")

    try:
        with db_mod.get_db() as conn:
            conn.execute("INSERT INTO t VALUES (1)")
            raise ValueError("test error")
    except ValueError:
        pass

    with db_mod.get_db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        assert count == 0  # Insert was rolled back

    db_mod.close_thread_db()
