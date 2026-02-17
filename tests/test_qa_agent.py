"""Tests for the QA Agent — validates scanning capabilities."""
import pytest
import os
import sys

# Setup path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.agents.qa_agent import (
    scan_html, scan_python_source, agent_status,
    _check_js_string_escaping, _check_fetch_credentials,
    _check_empty_catch_handlers, _check_onclick_handlers,
    _check_responsive, _check_accessibility,
)


# ─── Test JS String Escaping ─────────────────────────────────────────────────

class TestJSEscaping:
    def test_clean_html_passes(self):
        html = "<script>el.innerHTML='<div>hello world</div>';</script>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_js_string_escaping(html, findings)
        assert len(findings["critical"]) == 0

    def test_unescaped_apostrophe_detected(self):
        # This is the exact bug that broke the manager dashboard
        html = "<script>el.innerHTML='<div>you're caught up</div>';</script>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_js_string_escaping(html, findings)
        assert len(findings["critical"]) > 0
        assert findings["critical"][0]["type"] == "js_unescaped_quote"

    def test_safe_contraction_no_false_positive(self):
        # Properly escaped or avoided apostrophes should NOT trigger
        html = "<script>el.innerHTML='<div>all caught up</div>';</script>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_js_string_escaping(html, findings)
        assert len(findings["critical"]) == 0

    def test_double_quote_in_double_string(self):
        html = '<script>el.innerHTML="<div class=\\"test\\">ok</div>";</script>'
        findings = {"critical": [], "warning": [], "info": []}
        _check_js_string_escaping(html, findings)
        assert len(findings["critical"]) == 0


# ─── Test Fetch Credentials ──────────────────────────────────────────────────

class TestFetchCredentials:
    def test_fetch_with_credentials_passes(self):
        html = "<script>fetch('/api/test',{credentials:'same-origin'}).then(r=>r.json())</script>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_fetch_credentials(html, findings)
        assert len(findings["warning"]) == 0

    def test_fetch_without_credentials_flagged(self):
        html = "<script>fetch('/api/test').then(r=>r.json())</script>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_fetch_credentials(html, findings)
        assert len(findings["warning"]) > 0
        assert findings["warning"][0]["url"] == "/api/test"

    def test_non_api_fetch_ignored(self):
        html = "<script>fetch('https://cdtfa.ca.gov/tax').then(r=>r.json())</script>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_fetch_credentials(html, findings)
        assert len(findings["warning"]) == 0


# ─── Test Empty Catch Handlers ────────────────────────────────────────────────

class TestEmptyCatch:
    def test_empty_catch_detected(self):
        html = "<script>fetch('/x').catch(function(){})</script>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_empty_catch_handlers(html, findings)
        assert len(findings["warning"]) > 0

    def test_catch_with_body_passes(self):
        html = "<script>fetch('/x').catch(function(e){console.error(e)})</script>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_empty_catch_handlers(html, findings)
        assert len(findings["warning"]) == 0


# ─── Test Onclick Handlers ────────────────────────────────────────────────────

class TestOnclickHandlers:
    def test_defined_function_passes(self):
        html = """
        <button onclick="doSave()">Save</button>
        <script>function doSave() { alert('saved'); }</script>
        """
        findings = {"critical": [], "warning": [], "info": []}
        _check_onclick_handlers(html, findings)
        assert len(findings["critical"]) == 0

    def test_undefined_function_flagged(self):
        html = """
        <button onclick="missingFunc()">Click</button>
        <script>function otherFunc() {}</script>
        """
        findings = {"critical": [], "warning": [], "info": []}
        _check_onclick_handlers(html, findings)
        assert len(findings["critical"]) > 0
        assert findings["critical"][0]["function"] == "missingFunc"

    def test_builtin_functions_not_flagged(self):
        html = """<button onclick="alert('hi')">Alert</button><script></script>"""
        findings = {"critical": [], "warning": [], "info": []}
        _check_onclick_handlers(html, findings)
        assert len(findings["critical"]) == 0


# ─── Test Responsive CSS ─────────────────────────────────────────────────────

class TestResponsive:
    def test_missing_viewport_flagged(self):
        html = "<html><head></head><body>Hello</body></html>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_responsive(html, findings)
        assert any(f["type"] == "no_viewport" for f in findings["warning"])

    def test_viewport_present_passes(self):
        html = '<html><head><meta name="viewport" content="width=device-width"></head></html>'
        findings = {"critical": [], "warning": [], "info": []}
        _check_responsive(html, findings)
        assert not any(f["type"] == "no_viewport" for f in findings["warning"])

    def test_no_media_queries_flagged(self):
        html = "<html><head></head><body>No CSS</body></html>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_responsive(html, findings)
        assert any(f["type"] == "no_media_queries" for f in findings["warning"])

    def test_media_queries_present_passes(self):
        html = "<html><style>@media(max-width:768px){.x{display:none}}</style></html>"
        findings = {"critical": [], "warning": [], "info": []}
        _check_responsive(html, findings)
        assert not any(f["type"] == "no_media_queries" for f in findings["warning"])


# ─── Test Accessibility ──────────────────────────────────────────────────────

class TestAccessibility:
    def test_img_without_alt(self):
        html = '<img src="logo.png"><img src="other.png" alt="Other">'
        findings = {"critical": [], "warning": [], "info": []}
        _check_accessibility(html, findings)
        assert any(f["type"] == "img_no_alt" for f in findings["info"])


# ─── Test Python Source Scanning ──────────────────────────────────────────────

class TestPythonScan:
    def test_bare_except_detected(self, tmp_path):
        source = tmp_path / "bad.py"
        source.write_text("try:\n    x = 1\nexcept:\n    pass\n")
        result = scan_python_source(str(source))
        assert result["stats"]["critical_count"] > 0

    def test_clean_code_passes(self, tmp_path):
        source = tmp_path / "good.py"
        source.write_text("try:\n    x = 1\nexcept ValueError as e:\n    log.error(e)\n")
        result = scan_python_source(str(source))
        assert result["stats"]["critical_count"] == 0

    def test_except_without_logging_warned(self, tmp_path):
        source = tmp_path / "quiet.py"
        source.write_text("try:\n    x = 1\nexcept Exception as e:\n    pass\n")
        result = scan_python_source(str(source))
        assert result["stats"]["warning_count"] > 0


# ─── Test Full HTML Scan ──────────────────────────────────────────────────────

class TestFullScan:
    def test_clean_page_passes(self):
        html = """
        <html><head>
        <meta name="viewport" content="width=device-width">
        <style>@media(max-width:768px){.x{flex:1}}</style>
        </head><body>
        <button onclick="doIt()">Go</button>
        <script>
        function doIt() { fetch('/api/test',{credentials:'same-origin'}).then(r=>r.json()).catch(e=>console.error(e)); }
        </script>
        </body></html>
        """
        result = scan_html(html)
        assert result["stats"]["critical_count"] == 0

    def test_stats_populated(self):
        result = scan_html("<html><body>simple</body></html>")
        assert "total_issues" in result["stats"]
        assert "pass" in result["stats"]


# ─── Test Agent Status ────────────────────────────────────────────────────────

class TestAgentStatus:
    def test_returns_status(self):
        status = agent_status()
        assert status["name"] == "QA Agent"
        assert status["status"] == "ready"
        assert len(status["capabilities"]) > 0


# ─── Integration: Scan Actual Codebase ────────────────────────────────────────

class TestRealCodebase:
    """These tests scan the actual Reytech codebase for issues."""
    
    def test_dashboard_no_bare_excepts(self):
        """dashboard.py should have zero bare except: blocks."""
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "dashboard.py")
        if os.path.exists(path):
            result = scan_python_source(path)
            bare = [f for f in result["critical"] if f["type"] == "bare_except"]
            assert len(bare) == 0, f"Found bare excepts: {bare}"

    def test_templates_no_bare_excepts(self):
        """templates.py should have zero bare except: blocks."""
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "templates.py")
        if os.path.exists(path):
            result = scan_python_source(path)
            bare = [f for f in result["critical"] if f["type"] == "bare_except"]
            assert len(bare) == 0, f"Found bare excepts: {bare}"

    def test_no_unescaped_apostrophes_in_js(self):
        """All pages should have properly escaped JS strings.
        
        Regression test for the v9.1.4 apostrophe bug that killed
        the entire manager dashboard.
        """
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "templates.py")
        if not os.path.exists(path):
            pytest.skip("templates.py not found")
        with open(path) as f:
            content = f.read()
        
        # Find all innerHTML='...' assignments in script blocks
        import re
        scripts = re.findall(r'<script[^>]*>(.*?)</script>', content, re.DOTALL)
        for script in scripts:
            for match in re.finditer(r"innerHTML\s*=\s*'([^']*)'", script):
                inner = match.group(1)
                assert "'" not in inner, (
                    f"Unescaped apostrophe in innerHTML: ...{inner[:60]}..."
                )

    def test_all_fetch_calls_have_credentials(self):
        """Every fetch('/api/...') should include credentials:'same-origin'.
        
        Without this, the browser won't send the auth cookie on async
        requests, causing silent 401s that show as empty/broken sections.
        """
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "templates.py")
        if not os.path.exists(path):
            pytest.skip("templates.py not found")
        with open(path) as f:
            lines = f.readlines()
        
        missing = []
        for i, line in enumerate(lines, 1):
            if "fetch(" in line and "/api/" in line:
                chunk = "".join(lines[i-1:i+7])
                if "credentials" not in chunk:
                    url_match = __import__('re').search(r"fetch\(['\"]([^'\"]+)", line)
                    if url_match:
                        missing.append(f"Line {i}: {url_match.group(1)}")
        
        assert len(missing) == 0, f"fetch() calls missing credentials: {missing}"

    def test_responsive_breakpoints_exist(self):
        """Pages should have responsive CSS for mobile/tablet."""
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "templates.py")
        if not os.path.exists(path):
            pytest.skip("templates.py not found")
        with open(path) as f:
            content = f.read()
        
        import re
        media_queries = re.findall(r'@media\s*\(max-width:\s*(\d+)px\)', content)
        breakpoints = sorted(set(int(m) for m in media_queries))
        
        # Should have at least 2 breakpoints (tablet + mobile)
        assert len(breakpoints) >= 2, (
            f"Only {len(breakpoints)} breakpoint(s): {breakpoints}. "
            f"Need at least tablet (768px) and mobile (600px)"
        )

    def test_viewport_meta_in_base_css(self):
        """Base CSS/HTML should include viewport meta tag."""
        path = os.path.join(os.path.dirname(__file__), "..", "src", "api", "templates.py")
        if not os.path.exists(path):
            pytest.skip("templates.py not found")
        with open(path) as f:
            content = f.read()
        assert "viewport" in content, "Missing viewport meta tag"
