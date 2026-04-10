"""
Shared pytest fixtures for Reytech RFQ test suite.

IMPORTANT: Stub modules (rfq_parser, reytech_filler_v4, email_poller) are
injected into sys.path BEFORE the project root, so dashboard.py can import them.

Provides:
  - Per-test temp data isolation (autouse)
  - Flask test client with auth bypass
  - PDF assertion helpers (assert_pdf_fields, extract_pdf_text)
  - External API mocking (Gmail, Claude, SerpApi, SCPRS, Twilio)
  - Database seed functions (seed_db_quote, seed_db_contact, seed_db_price_history)
  - Sample data factories (sample_pc, sample_rfq, sample_stryker_quote)
"""
import json
import os
import sys
import base64
import sqlite3
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import pytest

# ── Paths ────────────────────────────────────────────────────────────────────
_STUBS_DIR = os.path.join(os.path.dirname(__file__), "stubs")
_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

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
        "src.agents.product_validator": "product_validator",
        "src.agents.item_identifier": "item_identifier",
        "src.agents.web_price_research": "web_price_research",
        "src.agents.tax_agent": "tax_agent",
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
            # Each module's cache gets its own isolated file in test tmp dir
            _cache_name = getattr(mod, "CACHE_FILE", "")
            _cache_basename = os.path.basename(_cache_name) if _cache_name else "cache.json"
            monkeypatch.setattr(mod, "CACHE_FILE",
                                os.path.join(data, _cache_basename))
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


# ── Block real API calls in tests ─────────────────────────────────────────────
@pytest.fixture(autouse=True)
def mock_api_keys(monkeypatch):
    """Set fake API keys so modules import cleanly, but block real HTTP calls."""
    monkeypatch.setenv("XAI_API_KEY", "xai-test-fake-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-fake-key")
    # Patch module-level vars that were read at import time
    for mod_path in ("src.agents.item_identifier", "src.agents.product_research",
                     "src.agents.product_validator"):
        try:
            mod = __import__(mod_path, fromlist=[mod_path.split(".")[-1]])
            if hasattr(mod, "XAI_API_KEY"):
                monkeypatch.setattr(mod, "XAI_API_KEY", "xai-test-fake-key")
        except ImportError:
            pass


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


# ── Fixture paths ────────────────────────────────────────────────────────────

@pytest.fixture
def fixtures_dir():
    """Return path to tests/fixtures/ directory."""
    return _FIXTURES_DIR


@pytest.fixture
def blank_704_path():
    """Path to the blank AMS 704 template in fixtures."""
    p = os.path.join(_FIXTURES_DIR, "ams_704_blank.pdf")
    if not os.path.exists(p):
        # Fallback to data/templates/
        p = os.path.join(_PROJECT_ROOT, "data", "templates", "ams_704_blank.pdf")
    return p


# ── PDF assertion helpers ────────────────────────────────────────────────────

def assert_pdf_fields(pdf_path, expected_values, msg=""):
    """Assert that a filled PDF contains the expected field values.

    Args:
        pdf_path: Path to the PDF file to check.
        expected_values: Dict of {field_name: expected_value}.
        msg: Optional message prefix for assertion errors.
    """
    from pypdf import PdfReader
    reader = PdfReader(pdf_path)
    fields = reader.get_fields() or {}
    # Also check annotations on each page for filled values
    all_values = {}
    for field_name, field_obj in fields.items():
        val = field_obj.get("/V", "")
        if hasattr(val, "replace"):
            all_values[field_name] = val
        else:
            all_values[field_name] = str(val) if val else ""

    for key, expected in expected_values.items():
        actual = all_values.get(key, "")
        prefix = f"{msg}: " if msg else ""
        assert str(actual) == str(expected), (
            f"{prefix}PDF field '{key}' expected '{expected}', got '{actual}'. "
            f"Available fields: {sorted(all_values.keys())[:20]}..."
        )


def extract_pdf_text(pdf_path, page_num=None):
    """Extract text from a PDF file using pdfplumber.

    Args:
        pdf_path: Path to the PDF.
        page_num: Specific page (0-indexed) or None for all pages.

    Returns:
        str: Extracted text.
    """
    import pdfplumber
    texts = []
    with pdfplumber.open(pdf_path) as pdf:
        pages = [pdf.pages[page_num]] if page_num is not None else pdf.pages
        for page in pages:
            text = page.extract_text() or ""
            texts.append(text)
    return "\n".join(texts)


def get_pdf_field_names(pdf_path):
    """Return sorted list of all form field names in a PDF."""
    from pypdf import PdfReader
    reader = PdfReader(pdf_path)
    fields = reader.get_fields() or {}
    return sorted(fields.keys())


def get_pdf_page_count(pdf_path):
    """Return number of pages in a PDF."""
    from pypdf import PdfReader
    return len(PdfReader(pdf_path).pages)


# ── External API mocking fixtures ────────────────────────────────────────────

@pytest.fixture
def mock_gmail(monkeypatch):
    """Mock Gmail API — prevents real API calls, returns fixture data.

    Usage:
        def test_email(mock_gmail):
            mock_gmail.set_messages([{...}])
            # code under test calls gmail_api functions
    """
    class GmailMock:
        def __init__(self):
            self._messages = []
            self._service = MagicMock()
            self._configured = True

        def set_messages(self, messages):
            self._messages = messages

        def set_configured(self, val):
            self._configured = val

    mock = GmailMock()

    try:
        import src.core.gmail_api as gmail_mod
        monkeypatch.setattr(gmail_mod, "is_configured", lambda: mock._configured)
        monkeypatch.setattr(gmail_mod, "get_service", lambda *a, **kw: mock._service)
        monkeypatch.setattr(gmail_mod, "list_message_ids",
                            lambda *a, **kw: [m.get("id", f"msg_{i}") for i, m in enumerate(mock._messages)])
        monkeypatch.setattr(gmail_mod, "get_message_metadata",
                            lambda svc, msg_id: next(
                                (m for m in mock._messages if m.get("id") == msg_id),
                                {}))
    except ImportError:
        pass

    return mock


@pytest.fixture
def mock_vision_parser(monkeypatch):
    """Mock Claude vision parser — returns canned parse results.

    Usage:
        def test_parse(mock_vision_parser):
            mock_vision_parser.set_result({"line_items": [...]})
            # code under test calls parse_with_vision()
    """
    class VisionMock:
        def __init__(self):
            self._result = None
            self._available = True

        def set_result(self, result):
            self._result = result

        def set_available(self, val):
            self._available = val

    mock = VisionMock()

    try:
        import src.forms.vision_parser as vp_mod
        monkeypatch.setattr(vp_mod, "is_available", lambda: mock._available)
        monkeypatch.setattr(vp_mod, "parse_with_vision",
                            lambda *a, **kw: mock._result)
        monkeypatch.setattr(vp_mod, "parse_from_text",
                            lambda *a, **kw: mock._result)
    except ImportError:
        pass

    return mock


@pytest.fixture
def mock_product_research(monkeypatch):
    """Mock product research (SerpApi/Amazon/Grok) — no real HTTP calls.

    Usage:
        def test_research(mock_product_research):
            mock_product_research.set_search_results([{...}])
    """
    class ResearchMock:
        def __init__(self):
            self._search_results = []
            self._product = None
            self._cache = {}

        def set_search_results(self, results):
            self._search_results = results

        def set_product(self, product):
            self._product = product

    mock = ResearchMock()

    try:
        import src.agents.product_research as pr_mod
        monkeypatch.setattr(pr_mod, "search_amazon",
                            lambda *a, **kw: mock._search_results)
        monkeypatch.setattr(pr_mod, "lookup_amazon_product",
                            lambda *a, **kw: mock._product)
        monkeypatch.setattr(pr_mod, "research_product",
                            lambda *a, **kw: mock._product or {})
    except ImportError:
        pass

    return mock


@pytest.fixture
def mock_scprs(monkeypatch):
    """Mock SCPRS/FI$Cal scraper — no real web scraping.

    Usage:
        def test_scprs(mock_scprs):
            mock_scprs.set_price({"unit_price": 475.00, "vendor": "Stryker"})
    """
    class ScprsMock:
        def __init__(self):
            self._price = None
            self._bulk = {}

        def set_price(self, price):
            self._price = price

        def set_bulk(self, results):
            self._bulk = results

    mock = ScprsMock()

    try:
        import src.agents.scprs_lookup as scprs_mod
        monkeypatch.setattr(scprs_mod, "lookup_price",
                            lambda *a, **kw: mock._price)
        monkeypatch.setattr(scprs_mod, "bulk_lookup",
                            lambda items: mock._bulk)
        monkeypatch.setattr(scprs_mod, "test_connection",
                            lambda: {"ok": True, "message": "mocked"})
    except ImportError:
        pass

    return mock


@pytest.fixture
def mock_twilio(monkeypatch):
    """Mock Twilio SMS — captures sent messages without real API calls.

    Usage:
        def test_sms(mock_twilio):
            # code under test sends SMS
            assert mock_twilio.sent[0]["to"] == "+15551234567"
    """
    class TwilioMock:
        def __init__(self):
            self.sent = []

    mock = TwilioMock()

    try:
        import src.core.notify as notify_mod
        original_send = getattr(notify_mod, "send_sms", None)
        if original_send:
            def fake_send(to, body, **kwargs):
                mock.sent.append({"to": to, "body": body, **kwargs})
                return {"ok": True, "sid": "SM_test_mock"}
            monkeypatch.setattr(notify_mod, "send_sms", fake_send)
    except ImportError:
        pass

    return mock


# ── Database seed helpers ────────────────────────────────────────────────────

@pytest.fixture
def seed_db_quote(temp_data_dir):
    """Insert a quote into the test database.

    Usage:
        def test_quote(seed_db_quote):
            qn = seed_db_quote("R26Q099", agency="CDCR", total=1234.56)
    """
    def _seed(quote_number, agency="CDCR", institution="CSP-Sacramento",
              status="generated", total=0.0, subtotal=0.0, tax=0.0,
              source_pc_id=None, source_rfq_id=None, line_items=None):
        db_path = os.path.join(temp_data_dir, "reytech.db")
        conn = sqlite3.connect(db_path)
        now = datetime.now().isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO quotes
               (quote_number, agency, institution, status, total, subtotal, tax,
                created_at, updated_at, source_pc_id, source_rfq_id, line_items)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (quote_number, agency, institution, status, total, subtotal, tax,
             now, now, source_pc_id, source_rfq_id,
             json.dumps(line_items or [])))
        conn.commit()
        conn.close()
        return quote_number
    return _seed


@pytest.fixture
def seed_db_contact(temp_data_dir):
    """Insert a contact into the test database.

    Usage:
        def test_contact(seed_db_contact):
            cid = seed_db_contact("buyer1", "Jane Smith", "jane@cdcr.ca.gov")
    """
    def _seed(contact_id, buyer_name, buyer_email, agency="CDCR",
              department="", total_spend=0.0, po_count=0):
        db_path = os.path.join(temp_data_dir, "reytech.db")
        conn = sqlite3.connect(db_path)
        now = datetime.now().isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO contacts
               (id, created_at, buyer_name, buyer_email, agency, department,
                total_spend, po_count)
               VALUES (?,?,?,?,?,?,?,?)""",
            (contact_id, now, buyer_name, buyer_email, agency, department,
             total_spend, po_count))
        conn.commit()
        conn.close()
        return contact_id
    return _seed


@pytest.fixture
def seed_db_price_history(temp_data_dir):
    """Insert price history records into the test database.

    Usage:
        def test_prices(seed_db_price_history):
            seed_db_price_history("Nitrile Gloves", 8.49, source="amazon", asin="B09TEST")
    """
    def _seed(description, unit_price, source="amazon", part_number="",
              manufacturer="", quantity=1, source_url="", source_id="",
              agency="", quote_number=""):
        db_path = os.path.join(temp_data_dir, "reytech.db")
        conn = sqlite3.connect(db_path)
        now = datetime.now().isoformat()
        conn.execute(
            """INSERT INTO price_history
               (found_at, description, part_number, manufacturer, quantity,
                unit_price, source, source_url, source_id, agency, quote_number)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (now, description, part_number, manufacturer, quantity,
             unit_price, source, source_url, source_id, agency, quote_number))
        conn.commit()
        conn.close()
    return _seed


@pytest.fixture
def seed_db_price_check(temp_data_dir):
    """Insert a price check into the test database.

    Usage:
        def test_pc(seed_db_price_check):
            seed_db_price_check("pc-001", items=[...])
    """
    def _seed(pc_id, pc_number="OS - Test - Apr", agency="CDCR",
              institution="CSP-Sacramento", status="parsed", items=None,
              requestor="buyer@cdcr.ca.gov"):
        db_path = os.path.join(temp_data_dir, "reytech.db")
        conn = sqlite3.connect(db_path)
        now = datetime.now().isoformat()
        conn.execute(
            """INSERT OR REPLACE INTO price_checks
               (id, created_at, requestor, agency, institution, items,
                pc_number, status, total_items)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (pc_id, now, requestor, agency, institution,
             json.dumps(items or []), pc_number, status,
             len(items or [])))
        conn.commit()
        conn.close()
        return pc_id
    return _seed


# ── Test data loading helpers ────────────────────────────────────────────────

def load_fixture_json(filename):
    """Load a JSON file from tests/fixtures/."""
    path = os.path.join(_FIXTURES_DIR, filename)
    with open(path) as f:
        return json.load(f)


@pytest.fixture
def fixture_json():
    """Fixture that returns a loader function for JSON fixtures.

    Usage:
        def test_something(fixture_json):
            data = fixture_json("serpapi_responses/amazon_search_gloves.json")
    """
    return load_fixture_json
