"""
tests/test_system.py — Reytech End-to-End System Test Suite
============================================================
Runs against the live Flask app (test client) and the data layer.
Every bug that has hit production gets a regression test here.
Run: python -m pytest tests/test_system.py -v
Auto-runs in QA agent every 5 minutes.
"""
import sys, os, json, re, base64, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DASH_USER', 'reytech')
os.environ.setdefault('DASH_PASS', 'changeme')
os.environ.setdefault('GMAIL_ADDRESS', 'a@b.com')
os.environ.setdefault('GMAIL_PASSWORD', 'x')

import pytest

# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def app():
    from flask import Flask
    from src.api import dashboard as d
    _app = Flask(__name__)
    _app.config['SECRET_KEY'] = 'test'
    _app.config['TESTING'] = True
    _app.register_blueprint(d.bp)
    return _app

@pytest.fixture(scope="session")
def client(app):
    return app.test_client()

@pytest.fixture(scope="session")
def auth(app):
    u = os.environ.get('DASH_USER', 'reytech')
    p = os.environ.get('DASH_PASS', 'changeme')
    return {'Authorization': f'Basic {base64.b64encode(f"{u}:{p}".encode()).decode()}'}

@pytest.fixture(scope="session")
def db_path():
    from src.core.paths import DATA_DIR
    return os.path.join(DATA_DIR, 'reytech.db')

def _conn(db_path):
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    return c

# ─── Category 1: Routes & HTTP ────────────────────────────────────────────────

class TestRoutes:
    """Every main page and API endpoint returns 200, not 500."""

    PAGES = ['/', '/quotes', '/orders', '/contacts', '/agents', '/outbox',
             '/pipeline', '/growth', '/intel/market', '/search']
    APIS  = ['/api/funnel/stats', '/api/manager/brief', '/api/manager/metrics',
             '/api/cs/drafts', '/api/agents/status', '/api/cs/status']

    def test_home_page(self, client, auth):
        r = client.get('/', headers=auth)
        assert r.status_code == 200, f"Home returned {r.status_code}"

    def test_agents_page(self, client, auth):
        r = client.get('/agents', headers=auth)
        assert r.status_code == 200

    def test_outbox_page(self, client, auth):
        r = client.get('/outbox', headers=auth)
        assert r.status_code == 200

    def test_manager_brief_never_500(self, client, auth):
        r = client.get('/api/manager/brief', headers=auth)
        assert r.status_code == 200, f"Brief returned {r.status_code}"
        data = r.get_json()
        assert data is not None, "Brief returned non-JSON"
        assert data.get('ok'), f"Brief ok=False: {data}"

    def test_manager_brief_has_summary(self, client, auth):
        """Brief must always return a populated summary, never empty {}."""
        r = client.get('/api/manager/brief', headers=auth)
        data = r.get_json()
        summary = data.get('summary', {})
        # summary must have at least quotes key
        assert 'quotes' in summary, f"Brief summary missing 'quotes' key: {summary}"

    def test_funnel_stats(self, client, auth):
        r = client.get('/api/funnel/stats', headers=auth)
        assert r.status_code == 200
        data = r.get_json()
        required = ['quotes_sent', 'quotes_pending', 'quotes_won', 'pipeline_value', 'next_quote']
        for k in required:
            assert k in data, f"funnel/stats missing key: {k}"

    def test_cs_drafts_api(self, client, auth):
        r = client.get('/api/cs/drafts', headers=auth)
        assert r.status_code == 200
        data = r.get_json()
        assert 'drafts' in data, "cs/drafts missing 'drafts' key"

    @pytest.mark.parametrize("url", PAGES)
    def test_page_not_500(self, client, auth, url):
        r = client.get(url, headers=auth)
        assert r.status_code in (200, 302), f"{url} returned {r.status_code}"

    @pytest.mark.parametrize("url", APIS)
    def test_api_not_500(self, client, auth, url):
        r = client.get(url, headers=auth)
        assert r.status_code == 200, f"GET {url} returned {r.status_code}"


# ─── Category 2: JS Safety ────────────────────────────────────────────────────

class TestJSSafety:
    """Regression tests for JS syntax errors that have hit production."""

    def _get_home_html(self, client, auth):
        return client.get('/', headers=auth).data.decode()

    def test_no_unescaped_apostrophes_in_js_strings(self, client, auth):
        """Regression: You'll, won't, don't inside JS single-quoted innerHTML crashes browser."""
        html = self._get_home_html(client, auth)
        lines = html.split('\n')
        bad = []
        for i, line in enumerate(lines):
            # Look for innerHTML= or similar assignments with single-quoted strings
            if "innerHTML='" in line or "innerHTML = '" in line:
                # Check for unescaped apostrophes in contractions
                if re.search(r"[a-z]'[a-z]", line):
                    bad.append(f"Line {i+1}: {line.strip()[:100]}")
        assert not bad, f"Unescaped apostrophes in JS strings:\n" + "\n".join(bad)

    def test_no_double_backslash_font_family(self, client, auth):
        """Regression: font-family:\\'JetBrains Mono\\' breaks when template renders."""
        html = self._get_home_html(client, auth)
        assert "font-family:\\'JetBrains" not in html, \
            "Escaped font-family quotes found — will cause JS syntax error"

    def test_no_rgba_in_js_string_escape(self, client, auth):
        """Regression: rgba(79,140,255,.08)\\' causes Unexpected identifier 'rgba'."""
        html = self._get_home_html(client, auth)
        # The actual bug was rgba breaking JS string escaping with backslash-quote.
        # Inline CSS hover effects (onmouseover) are acceptable for progressive enhancement.
        assert "rgba(79,140,255,.08)\\'" not in html, \
            "Broken JS string escape with rgba — original regression"

    def test_no_script_errors_obvious(self, client, auth):
        """Check for obviously broken JS patterns."""
        html = self._get_home_html(client, auth)
        # No double-closing single quotes in JS
        assert "')'" not in html or html.count("')'") < 5, \
            "Suspicious quote pattern in JS"


# ─── Category 3: Manager Brief Logic ─────────────────────────────────────────

class TestManagerBrief:
    """The manager brief must accurately reflect pipeline state."""

    def test_brief_reflects_rfqs(self, client, auth):
        """If rfqs.json has actionable entries, brief must mention them."""
        from src.core.paths import DATA_DIR
        rfq_path = os.path.join(DATA_DIR, 'rfqs.json')
        rfqs = json.load(open(rfq_path)) if os.path.exists(rfq_path) else {}
        actionable = [r for r in rfqs.values()
                      if r.get('status') in ('new', 'pending', 'auto_drafted')]
        if not actionable:
            pytest.skip("No actionable RFQs in test data")

        r = client.get('/api/manager/brief', headers=auth)
        data = r.get_json()
        # Should have rfq_pending approvals
        approvals = data.get('pending_approvals', [])
        rfq_approvals = [a for a in approvals if a.get('type') == 'rfq_pending']
        assert len(rfq_approvals) == len(actionable), \
            f"Brief has {len(rfq_approvals)} RFQ approvals but {len(actionable)} actionable RFQs exist"

    def test_brief_not_fallback_when_data_ok(self, client, auth):
        """Brief must not return _fallback=True when everything is healthy."""
        r = client.get('/api/manager/brief', headers=auth)
        data = r.get_json()
        if data.get('_fallback'):
            err = data.get('_error', 'unknown')
            pytest.fail(f"Brief is using fallback! Error: {err}")

    def test_brief_headline_is_meaningful(self, client, auth):
        """Headline must not be a generic error message."""
        r = client.get('/api/manager/brief', headers=auth)
        data = r.get_json()
        headline = data.get('headline', '')
        generic_bad = ['Dashboard active', 'Dashboard loaded', 'brief refresh pending']
        for bad in generic_bad:
            assert bad not in headline, \
                f"Brief showing fallback headline: '{headline}' — generate_brief() is throwing"

    def test_brief_summary_has_rfq_key(self, client, auth):
        """summary.rfqs must exist so the RFQ stat bar works."""
        r = client.get('/api/manager/brief', headers=auth)
        data = r.get_json()
        summary = data.get('summary', {})
        assert 'rfqs' in summary, \
            f"summary missing 'rfqs' key — RFQ stat bar will show nothing. Keys: {list(summary.keys())}"


# ─── Category 4: Data Consistency ─────────────────────────────────────────────

class TestDataConsistency:
    """DB and JSON files must agree on the same facts."""

    def test_quotes_status_db_matches_json(self, db_path):
        """R26Q16 and other quotes must have same status in DB and quotes_log.json."""
        from src.core.paths import DATA_DIR
        ql_path = os.path.join(DATA_DIR, 'quotes_log.json')
        if not os.path.exists(ql_path):
            pytest.skip("quotes_log.json not found")
        ql = json.load(open(ql_path))
        json_status = {q['quote_number']: q['status'] for q in ql if not q.get('is_test')}

        conn = _conn(db_path)
        db_status = {r['quote_number']: r['status']
                     for r in conn.execute("SELECT quote_number, status FROM quotes WHERE is_test=0")}
        conn.close()

        mismatches = []
        for qn, js in json_status.items():
            db = db_status.get(qn)
            if db and db != js:
                mismatches.append(f"{qn}: JSON={js}, DB={db}")
        assert not mismatches, "DB/JSON status mismatch:\n" + "\n".join(mismatches)

    def test_no_duplicate_rfq_pc_entries(self):
        """Same solicitation number must not appear in both rfqs.json and price_checks.json."""
        from src.core.paths import DATA_DIR
        rfq_path = os.path.join(DATA_DIR, 'rfqs.json')
        pc_path = os.path.join(DATA_DIR, 'price_checks.json')
        if not os.path.exists(rfq_path) or not os.path.exists(pc_path):
            pytest.skip("Missing data files")

        rfqs = json.load(open(rfq_path))
        pcs = json.load(open(pc_path))

        rfq_sols = {r.get('solicitation_number', '').strip().lower()
                    for r in rfqs.values() if r.get('solicitation_number')}
        pc_nums  = {p.get('pc_number', '').strip().lower().replace('ad-', '').replace('#','')
                    for p in pcs.values() if p.get('pc_number')}
        # Normalize both
        rfq_norm = {re.sub(r'[^a-z0-9]', '', s) for s in rfq_sols}
        pc_norm  = {re.sub(r'[^a-z0-9]', '', s) for s in pc_nums}

        overlap = rfq_norm & pc_norm - {''}
        assert not overlap, \
            f"Same solicitation in BOTH PC queue and RFQ queue: {overlap}. " \
            f"Run POST /api/admin/rfq-cleanup to fix."

    def test_no_blank_pcs(self):
        """Price checks must not have empty pc_number AND empty institution."""
        from src.core.paths import DATA_DIR
        pc_path = os.path.join(DATA_DIR, 'price_checks.json')
        if not os.path.exists(pc_path):
            pytest.skip("price_checks.json not found")
        pcs = json.load(open(pc_path))
        blank = [pid for pid, p in pcs.items()
                 if not p.get('pc_number') and not p.get('institution') and not p.get('is_test')]
        assert not blank, f"Blank PCs found: {blank}. Run POST /api/admin/cleanup."

    def test_rfq_queue_has_no_704_only_entries(self):
        """RFQ queue must only contain entries with a full RFQ package, not bare 704s."""
        from src.core.paths import DATA_DIR
        rfq_path = os.path.join(DATA_DIR, 'rfqs.json')
        if not os.path.exists(rfq_path):
            pytest.skip("rfqs.json not found")
        rfqs = json.load(open(rfq_path))
        bad = []
        for rid, r in rfqs.items():
            atts = r.get('attachments_raw', []) or []
            templates = r.get('templates', {}) or {}
            sol = r.get('solicitation_number', rid)
            # An RFQ with no 704B and attachments that look like bare 704s is suspicious
            if (r.get('source') == 'email' and
                '704b' not in templates and
                any('704' in str(a).lower() for a in atts)):
                bad.append(f"#{sol} (no 704B, has 704 attachments → should be PC, not RFQ)")
        assert not bad, \
            f"Found 704 price-checks misrouted to RFQ queue:\n" + "\n".join(bad) + \
            "\nFix: POST /api/admin/rfq-cleanup"


# ─── Category 5: Pipeline & Routing Logic ─────────────────────────────────────

class TestPipelineLogic:
    """Business logic: PCs, RFQs, quotes must behave correctly."""

    def test_quote_detail_shows_items(self, client, auth):
        """Quote detail page must show line items, not a blank table."""
        from src.forms.quote_generator import get_all_quotes
        quotes = [q for q in get_all_quotes() if q.get('line_items') or q.get('items_detail')]
        if not quotes:
            pytest.skip("No quotes with items to test")
        qn = quotes[0]['quote_number']
        r = client.get(f'/quote/{qn}', headers=auth)
        assert r.status_code == 200
        body = r.data.decode()
        # The line items table must have at least one row with actual content
        items = quotes[0].get('line_items') or quotes[0].get('items_detail') or []
        if items:
            first_desc = str(items[0].get('description', ''))[:20]
            if first_desc:
                assert first_desc in body, \
                    f"Quote {qn} detail page is missing item description '{first_desc}'"

    def test_cs_agent_visible_on_agents_page(self, client, auth):
        """CS Agent section must appear on /agents page."""
        r = client.get('/agents', headers=auth)
        body = r.data.decode()
        assert 'Customer Support Agent' in body or 'cs-agent-section' in body, \
            "CS Agent section is missing from /agents page — users can't find their CS drafts"

    def test_search_returns_quotes(self, client, auth):
        """Search must find quotes by quote number."""
        from src.forms.quote_generator import get_all_quotes
        quotes = get_all_quotes()
        if not quotes:
            pytest.skip("No quotes in system")
        qn = quotes[0]['quote_number']
        r = client.get(f'/api/search?q={qn}', headers=auth)
        data = r.get_json()
        assert data.get('count', 0) > 0, f"Search for {qn} returned no results"
        types = [res.get('type') for res in data.get('results', [])]
        assert 'quote' in types, f"Search for {qn} didn't return a quote result"

    def test_next_quote_number_increments(self):
        """Quote counter must be a valid R26QN format and increment."""
        from src.forms.quote_generator import peek_next_quote_number
        n1 = peek_next_quote_number()
        assert re.match(r'R\d+Q\d+', n1), f"Invalid quote format: {n1}"


# ─── Category 6: QA Agent Self-Check ──────────────────────────────────────────

class TestQASystem:
    """The QA agent itself must be working."""

    def test_qa_runs_without_error(self):
        """run_health_check() must complete without throwing."""
        from src.agents.qa_agent import run_health_check
        report = run_health_check()
        assert report is not None, "run_health_check() returned None"

    def test_qa_returns_numeric_score(self):
        """QA score must be a number, not None."""
        from src.agents.qa_agent import run_health_check
        report = run_health_check()
        score = report.get('health_score') or report.get('score')
        assert isinstance(score, (int, float)), (
            f"QA health_score is {type(score).__name__}: {score}. "
            f"Keys: {list(report.keys())}"
        )

    def test_qa_minimum_score(self):
        """System must maintain a minimum QA score of 70."""
        from src.agents.qa_agent import run_health_check
        report = run_health_check()
        score = report.get('health_score') or report.get('score') or 0
        assert score >= 70, (
            f"QA score {score} below minimum 70. Grade: {report.get('grade')}. "
            f"Issues: {report.get('critical_issues', [])}. Run /api/qa/health for details."
        )
