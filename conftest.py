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
    for mod_name in ("won_quotes_db", "product_research", "quote_generator",
                     "price_check", "auto_processor", "scprs_lookup"):
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

    def get(self, *args, **kwargs):
        kwargs.setdefault("headers", {}).update(self._headers)
        return self._client.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        kwargs.setdefault("headers", {}).update(self._headers)
        return self._client.post(*args, **kwargs)

    def put(self, *args, **kwargs):
        kwargs.setdefault("headers", {}).update(self._headers)
        return self._client.put(*args, **kwargs)

    def delete(self, *args, **kwargs):
        kwargs.setdefault("headers", {}).update(self._headers)
        return self._client.delete(*args, **kwargs)


@pytest.fixture
def app(temp_data_dir, monkeypatch):
    """Create Flask app configured for testing."""
    monkeypatch.setenv("DASH_USER", "reytech")
    monkeypatch.setenv("DASH_PASS", "changeme")

    import dashboard
    monkeypatch.setattr(dashboard, "DATA_DIR", temp_data_dir)
    monkeypatch.setattr(dashboard, "OUTPUT_DIR",
                        os.path.join(temp_data_dir, "output"))
    monkeypatch.setattr(dashboard, "UPLOAD_DIR",
                        os.path.join(temp_data_dir, "uploads"))
    for d in ("output", "uploads"):
        os.makedirs(os.path.join(temp_data_dir, d), exist_ok=True)

    dashboard.app.config["TESTING"] = True
    return dashboard.app


@pytest.fixture
def client(app):
    """Authenticated Flask test client (HTTP Basic Auth on every request)."""
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
        "ship_to": "CSP-Sacramento, 100 Prison Road, Represa, CA 95671",
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
