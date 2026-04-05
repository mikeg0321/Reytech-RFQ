"""
Shared pytest fixtures for Reytech RFQ test suite.

IMPORTANT: Stub modules (rfq_parser, reytech_filler_v4, email_poller) are
injected into sys.path BEFORE the project root, so dashboard.py can import them.
"""
import json
import os
import sys
import base64
import tempfile
import pytest

# ── Inject stubs FIRST so dashboard can import rfq_parser etc. ────────────────
_STUBS_DIR = os.path.join(os.path.dirname(__file__), "stubs")
_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

# stubs must precede project root
if _STUBS_DIR not in sys.path:
    sys.path.insert(0, _STUBS_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(1, _PROJECT_ROOT)


# ── Temp data directory (per-test isolation) ──────────────────────────────────

@pytest.fixture(autouse=True)
def temp_data_dir(tmp_path, monkeypatch):
    """Redirect ALL module data/output dirs to an isolated tmp directory."""
    data = str(tmp_path / "data")
    os.makedirs(data, exist_ok=True)

    # Seed minimal customers.json for CRM tests
    _write_json(os.path.join(data, "customers.json"), [
        {"qb_name": "Folsom State Prison", "display_name": "Folsom State Prison",
         "company": "Folsom State Prison", "parent": "Dept of Corrections and Rehabilitation",
         "agency": "CDCR", "abbreviation": "FSP", "address": "300 Prison Road",
         "city": "Represa", "state": "CA", "zip": "95671", "phone": "",
         "email": "timothy.anderson@cdcr.ca.gov", "open_balance": 0, "source": "cdcr.ca.gov"},
        {"qb_name": "California State Prison, Sacramento", "display_name": "California State Prison, Sacramento",
         "company": "CSP-Sacramento", "parent": "Dept of Corrections and Rehabilitation",
         "agency": "CDCR", "abbreviation": "SAC", "address": "100 Prison Road",
         "city": "Represa", "state": "CA", "zip": "95671", "phone": "",
         "email": "", "open_balance": 0, "source": "cdcr.ca.gov"},
        {"qb_name": "Veterans Home of California - Fresno", "display_name": "Veterans Home of California - Fresno",
         "company": "Veterans Home of California - Fresno", "parent": "Dept of Veterans Affairs",
         "agency": "CalVet", "address": "", "city": "Fresno", "state": "CA",
         "zip": "", "phone": "", "email": "", "open_balance": 0, "source": "calvet.ca.gov"},
    ])

    # Patch every module that has DATA_DIR
    _module_map = {
        "src.knowledge.won_quotes_db": "won_quotes_db",
        "src.agents.product_research": "product_research",
        "src.forms.quote_generator": "quote_generator",
        "src.forms.price_check": "price_check",
        "src.auto.auto_processor": "auto_processor",
        "src.agents.scprs_lookup": "scprs_lookup",
    }
    for mod_path, mod_name in _module_map.items():
        try:
            mod = __import__(mod_path, fromlist=[mod_name.split(".")[-1]])
        except ImportError:
            try:
                mod = __import__(mod_name)
            except ImportError:
                continue
        if hasattr(mod, "DATA_DIR"):
            monkeypatch.setattr(mod, "DATA_DIR", data)
        if hasattr(mod, "WON_QUOTES_FILE"):
            monkeypatch.setattr(mod, "WON_QUOTES_FILE",
                                os.path.join(data, "won_quotes.json"))
        if hasattr(mod, "CACHE_FILE"):
            monkeypatch.setattr(mod, "CACHE_FILE",
                                os.path.join(data, "product_research_cache.json"))
        if hasattr(mod, "QUOTES_LOG_FILE"):
            monkeypatch.setattr(mod, "QUOTES_LOG_FILE",
                                os.path.join(data, "quotes_log.json"))

    # Patch DB_PATH so get_all_quotes() reads from an isolated test DB
    # (the app fixture also does this, but tests that don't use app need it too)
    try:
        import sqlite3
        import src.core.db as _db_mod
        # Use reytech.db (not reytech_test.db) so _next_quote_number() and
        # _load_counter() both read from the same file
        _db_path = os.path.join(data, "reytech.db")
        monkeypatch.setattr(_db_mod, "DB_PATH", _db_path)
        _db_mod.close_thread_db()  # Force new connection with patched path
        # Also patch src.core.paths.DATA_DIR so _next_quote_number() uses test DB
        try:
            import src.core.paths as _paths_mod
            monkeypatch.setattr(_paths_mod, "DATA_DIR", data)
        except ImportError:
            pass
        # Create minimal schema in both test DBs so counter and settings work
        _MINIMAL_TABLES = [
            """CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY, value TEXT, updated_at TEXT, updated_by TEXT DEFAULT 'system')""",
            """CREATE TABLE IF NOT EXISTS quotes (
                quote_number TEXT PRIMARY KEY, status TEXT, total REAL, agency TEXT,
                institution TEXT, po_number TEXT, contact_name TEXT, contact_email TEXT,
                subtotal REAL, tax REAL, created_at TEXT, updated_at TEXT, is_test INTEGER DEFAULT 0,
                source TEXT, sent_at TEXT, line_items TEXT, ship_to_name TEXT, ship_to_address TEXT,
                pdf_path TEXT, source_pc_id TEXT, source_rfq_id TEXT, status_notes TEXT,
                requestor TEXT, notes TEXT, status_history TEXT, expires_at TEXT,
                closed_by_agent TEXT, close_reason TEXT, revision_count INTEGER DEFAULT 0,
                win_probability REAL, last_follow_up TEXT, follow_up_count INTEGER DEFAULT 0,
                received_at TEXT, first_opened_at TEXT, priced_at TEXT, generated_at TEXT,
                time_to_price_mins REAL, time_to_send_mins REAL)""",
        ]
        _conn = sqlite3.connect(_db_path)
        for _sql in _MINIMAL_TABLES:
            _conn.execute(_sql)
        # Seed counter so _load_counter() and _next_quote_number() agree on initial value
        import datetime as _dt
        _now = _dt.datetime.now().isoformat()
        _year = _dt.datetime.now().year
        for _key, _val in [("quote_counter_seq", "0"), ("quote_counter", "0"),
                           ("quote_counter_year", str(_year)), ("quote_counter_last_good", "0")]:
            _conn.execute(
                "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?,?,?)",
                (_key, _val, _now))
        _conn.commit()
        _conn.close()
        # Run full init_db() to create all tables (email_outbox, price_checks, etc.)
        # Tests like test_manager_agent need these tables to exist.
        try:
            _db_mod.init_db()
        except Exception:
            pass
        _db_mod.close_thread_db()  # Reset connection after init
    except Exception:
        pass

    return data


# ── Flask test client ─────────────────────────────────────────────────────────

def _basic_auth_header(user="reytech", pw="changeme"):
    creds = base64.b64encode(f"{user}:{pw}".encode()).decode()
    return {"Authorization": f"Basic {creds}"}


class AuthenticatedClient:
    """Wraps Flask test client to add Basic Auth headers to every request."""
    def __init__(self, client, headers):
        self._client = client
        self._headers = headers

    def _merge(self, kwargs):
        h = dict(self._headers)
        h.update(kwargs.pop("headers", {}))
        # Set Origin for CSRF bypass in tests
        h.setdefault("Origin", "http://localhost")
        kwargs["headers"] = h
        return kwargs

    def get(self, *args, **kwargs):
        return self._client.get(*args, **self._merge(kwargs))

    def post(self, *args, **kwargs):
        return self._client.post(*args, **self._merge(kwargs))

    def put(self, *args, **kwargs):
        return self._client.put(*args, **self._merge(kwargs))

    def delete(self, *args, **kwargs):
        return self._client.delete(*args, **self._merge(kwargs))


@pytest.fixture
def app(temp_data_dir, monkeypatch):
    """Create Flask app configured for testing."""
    monkeypatch.setenv("DASH_USER", "reytech")
    monkeypatch.setenv("DASH_PASS", "changeme")
    monkeypatch.setenv("SECRET_KEY", "test-secret-key-for-pytest")

    try:
        from src.api import dashboard
    except ImportError:
        import dashboard
    monkeypatch.setattr(dashboard, "DATA_DIR", temp_data_dir)
    monkeypatch.setattr(dashboard, "OUTPUT_DIR",
                        os.path.join(temp_data_dir, "output"))
    monkeypatch.setattr(dashboard, "UPLOAD_DIR",
                        os.path.join(temp_data_dir, "uploads"))
    # Clear price check cache so tests get fresh data from temp_data_dir
    monkeypatch.setattr(dashboard, "_pc_cache", None)
    monkeypatch.setattr(dashboard, "_pc_cache_time", 0)
    # Ensure auth vars match test credentials (patch both dashboard + shared)
    monkeypatch.setattr(dashboard, "DASH_USER", "reytech")
    monkeypatch.setattr(dashboard, "DASH_PASS", "changeme")
    try:
        from src.api import shared
        monkeypatch.setattr(shared, "DASH_USER", "reytech")
        monkeypatch.setattr(shared, "DASH_PASS", "changeme")
        monkeypatch.setattr(shared, "check_auth",
                            lambda u, p: u == "reytech" and p == "changeme")
    except ImportError:
        pass
    for d in ("output", "uploads"):
        os.makedirs(os.path.join(temp_data_dir, d), exist_ok=True)

    monkeypatch.setenv("ENABLE_EMAIL_POLLING", "false")
    monkeypatch.setenv("ENABLE_BACKGROUND_AGENTS", "false")

    # Patch DB_PATH so get_db() uses an isolated test database
    # Use reytech.db (same as temp_data_dir fixture) so all code paths share one DB
    try:
        import src.core.db as _db_mod
        _db_path = os.path.join(temp_data_dir, "reytech.db")
        monkeypatch.setattr(_db_mod, "DB_PATH", _db_path)
        _db_mod.close_thread_db()  # Force new connection with patched path
    except Exception:
        pass

    from app import create_app
    _app = create_app()
    _app.config["TESTING"] = True

    # Clear rate limiter between tests — module-level dict accumulates across tests
    try:
        from src.api import dashboard
        dashboard._rate_limiter.clear()
    except (ImportError, AttributeError):
        pass

    return _app


@pytest.fixture
def client(app):
    """Authenticated Flask test client (HTTP Basic Auth on every request)."""
    with app.test_client() as c:
        yield AuthenticatedClient(c, _basic_auth_header())


@pytest.fixture
def auth_client(app):
    """Alias — authenticated Flask test client."""
    with app.test_client() as c:
        yield AuthenticatedClient(c, _basic_auth_header())


@pytest.fixture
def anon_client(app):
    """Unauthenticated test client."""
    with app.test_client() as c:
        yield c


# ── Seed helpers ──────────────────────────────────────────────────────────────

def _write_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, default=str)


@pytest.fixture
def seed_pc(temp_data_dir, sample_pc):
    """Write sample PC to data dir, return its id."""
    pcs = {sample_pc["id"]: sample_pc}
    _write_json(os.path.join(temp_data_dir, "price_checks.json"), pcs)
    return sample_pc["id"]


@pytest.fixture
def seed_rfq(temp_data_dir, sample_rfq):
    """Write sample RFQ to data dir, return its id."""
    rfqs = {sample_rfq["id"]: sample_rfq}
    _write_json(os.path.join(temp_data_dir, "rfqs.json"), rfqs)
    return sample_rfq["id"]


# ── Sample data factories ─────────────────────────────────────────────────────

@pytest.fixture
def sample_pc_items():
    """Typical Price Check line items with Amazon results."""
    return [
        {
            "item_number": "1",
            "qty": 22,
            "uom": "EA",
            "description": "Engraved two line name tag, black/white",
            "no_bid": False,
            "supplier_cost": 12.58,
            "unit_price": 15.72,
            "pricing": {
                "amazon_price": 12.58,
                "amazon_title": "Custom Engraved Name Tag",
                "amazon_url": "https://amazon.com/dp/B07TEST123",
                "amazon_asin": "B07TEST123",
                "recommended_price": 15.72,
                "price_source": "amazon",
            },
        },
        {
            "item_number": "2",
            "qty": 5,
            "uom": "BOX",
            "description": "Copy paper, 8.5x11, 20lb, white, 10 reams",
            "no_bid": False,
            "supplier_cost": 42.99,
            "unit_price": 53.74,
            "pricing": {
                "amazon_price": 42.99,
                "amazon_title": "Amazon Basics Copy Paper",
                "amazon_url": "https://amazon.com/dp/B00TEST456",
                "amazon_asin": "B00TEST456",
                "recommended_price": 53.74,
                "price_source": "amazon",
            },
        },
    ]


@pytest.fixture
def sample_pc(sample_pc_items):
    """Full Price Check record."""
    return {
        "id": "test-pc-001",
        "pc_number": "OS - Den - Feb",
        "institution": "CSP-Sacramento",
        "ship_to": "CSP-Sacramento, 300 Prison Road, Represa, CA 95671",
        "status": "priced",
        "tax_enabled": False,
        "tax_rate": 0.0,
        "delivery_option": "5-7 business days",
        "custom_notes": "",
        "price_buffer": 0,
        "default_markup": 25,
        "parsed": {
            "header": {"institution": "CSP-Sacramento"},
            "line_items": sample_pc_items,
        },
        "items": sample_pc_items,
        "source_pdf": "/tmp/test.pdf",
    }


@pytest.fixture
def sample_rfq():
    """Full RFQ record."""
    return {
        "id": "test-rfq-001",
        "solicitation_number": "RFQ-2026-TEST",
        "requestor_name": "Jane Smith",
        "requestor_email": "jane@state.ca.gov",
        "due_date": "2026-03-15",
        "department": "CDCR - California Institution for Men",
        "delivery_location": "CIM, 14901 Central Ave, Chino, CA 91710",
        "ship_to": "CIM, 14901 Central Ave, Chino, CA 91710",
        "status": "new",
        "source": "email",
        "award_method": "all_or_none",
        "templates": {},
        "output_files": [],
        "line_items": [
            {
                "line_number": "1",
                "item_number": "6500-001-430",
                "qty": 2,
                "uom": "SET",
                "description": "X-RESTRAINT PACKAGE by Stryker Medical",
                "supplier_cost": 350.00,
                "scprs_last_price": 475.00,
                "price_per_unit": 454.40,
            },
        ],
    }


@pytest.fixture
def sample_stryker_quote():
    """Quote data matching the R26Q14 Stryker test case."""
    return {
        "institution": "SCC - Sierra Conservation Center",
        "ship_to_name": "SCC - Sierra Conservation Center",
        "ship_to_address": ["5100 O'Byrnes Ferry Road", "Jamestown, CA 95327"],
        "rfq_number": "10838043",
        "line_items": [
            {"line_number": 1, "part_number": "6500-001-430", "qty": 2, "uom": "SET",
             "description": "X-RESTRAINT PACKAGE by Stryker Medical\nNew OEM Original Outright\nOEM#: 6500001430",
             "unit_price": 454.40},
            {"line_number": 2, "part_number": "6250-001-125", "qty": 2, "uom": "EACH",
             "description": "RESTRAINT STRAP, CHEST, GREEN",
             "unit_price": 69.12},
            {"line_number": 3, "part_number": "6250-001-126", "qty": 2, "uom": "EACH",
             "description": "RESTRAINT STRAP, CHEST, BLACK",
             "unit_price": 69.12},
        ],
    }


# ── Disable rate limiting in tests ────────────────────────────────────────────
@pytest.fixture(autouse=True)
def disable_rate_limit(monkeypatch):
    """Prevent 429 responses during rapid test execution."""
    try:
        import src.api.dashboard as dash
        monkeypatch.setattr(dash, "_check_rate_limit", lambda *a, **kw: True)
        # Accept any non-empty credentials in tests
        monkeypatch.setattr(dash, "check_auth",
                            lambda u, p: bool(u and p))
    except Exception:
        pass
    try:
        import src.api.shared as shared
        monkeypatch.setattr(shared, "_check_rate_limit", lambda *a, **kw: True)
        monkeypatch.setattr(shared, "check_auth",
                            lambda u, p: bool(u and p))
    except Exception:
        pass
