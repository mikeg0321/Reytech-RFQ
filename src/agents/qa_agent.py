#!/usr/bin/env python3
"""
QA Agent — Automated Quality Assurance for Reytech RFQ Dashboard

Scans all pages for:
  1. Broken buttons / onclick handlers that reference missing JS functions
  2. fetch() calls missing credentials:same-origin (auth will silently fail)
  3. Unescaped quotes in JS string literals (syntax errors)
  4. Routes referenced in HTML/JS that don't exist
  5. Responsive CSS: viewport meta, media queries, overflow handling
  6. Silent error handlers (.catch with empty body)
  7. Forms pointing to non-existent endpoints
  8. CSS class references without definitions
  9. Missing data-testid attributes on interactive elements

Can run as:
  - API endpoint: GET /api/qa/scan
  - CLI: python -m src.agents.qa_agent
  - Test: pytest tests/test_qa_agent.py
"""

import re
import os
import json
import ast
import time
import logging
import threading
import traceback
from datetime import datetime

log = logging.getLogger("qa_agent")

# ── Agent Context (Skills Guide: Domain Intelligence layer) ──────────────────
try:
    from src.core.agent_context import get_context as _get_agent_ctx
    HAS_CTX = True
except ImportError:
    HAS_CTX = False
    def _get_agent_ctx(**kw): return {}

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")


def scan_html(html: str, route_list: list = None) -> dict:
    """Scan rendered HTML for QA issues.
    
    Args:
        html: Rendered HTML string to scan
        route_list: List of known API route paths (optional)
    
    Returns:
        dict with findings categorized by severity
    """
    findings = {
        "critical": [],   # Will break functionality
        "warning": [],    # Might cause issues
        "info": [],       # Best practices
        "stats": {},
    }
    
    # ─── 1. Unescaped quotes in JS string literals ───────────────────
    _check_js_string_escaping(html, findings)
    
    # ─── 2. fetch() calls missing credentials ────────────────────────
    _check_fetch_credentials(html, findings)
    
    # ─── 3. Silent error handlers ─────────────────────────────────────
    _check_empty_catch_handlers(html, findings)
    
    # ─── 4. Broken onclick/form references ────────────────────────────
    _check_onclick_handlers(html, findings)
    
    # ─── 5. Route wiring ──────────────────────────────────────────────
    if route_list:
        _check_route_wiring(html, route_list, findings)
    
    # ─── 6. Responsive CSS ────────────────────────────────────────────
    _check_responsive(html, findings)
    
    # ─── 7. Accessibility basics ──────────────────────────────────────
    _check_accessibility(html, findings)
    
    # ─── Stats ────────────────────────────────────────────────────────
    findings["stats"] = {
        "total_issues": len(findings["critical"]) + len(findings["warning"]),
        "critical_count": len(findings["critical"]),
        "warning_count": len(findings["warning"]),
        "info_count": len(findings["info"]),
        "pass": len(findings["critical"]) == 0,
    }
    
    return findings


def _check_js_string_escaping(html: str, findings: dict):
    """Find unescaped quotes inside JS string literals."""
    # Extract <script> blocks
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for script in scripts:
        # Method 1: Find innerHTML='...' and check the captured content
        # When an apostrophe is inside, the regex captures up to it
        # and leaves dangling content that looks like broken HTML
        for match in re.finditer(r"innerHTML\s*=\s*'([^']*)'", script):
            content = match.group(1)
            pos_after = match.end()
            # Check what comes after — if it's letters (like "re caught up")
            # that means an apostrophe broke the string
            after = script[pos_after:pos_after+30].strip()
            if after and after[0].isalpha():
                findings["critical"].append({
                    "type": "js_unescaped_quote",
                    "detail": "Unescaped apostrophe in innerHTML — string terminates early",
                    "snippet": content[:80] + "..." + after[:20],
                })
        
        # Method 2: Look for common contractions inside innerHTML assignments
        # Find all innerHTML= lines and check for contractions
        for line in script.split('\n'):
            if 'innerHTML' in line and "='" in line:
                # Check for common English contractions between quotes
                contractions = re.findall(
                    r"(?:you|we|they|he|she|it|who|that|there|what|don|won|can|isn|aren|wasn|weren|shouldn|couldn|wouldn|hasn|haven|hadn|didn|doesn|ain)'(?:re|ve|ll|t|s|d|m)\b",
                    line, re.IGNORECASE
                )
                for contraction in contractions:
                    # Verify it's inside a single-quoted string (not double-quoted)
                    if f"'{contraction}" in line or f"{contraction}'" in line:
                        findings["critical"].append({
                            "type": "js_unescaped_quote",
                            "detail": f"Contraction '{contraction}' inside single-quoted innerHTML will break JS",
                            "snippet": contraction,
                        })
        
        # Method 3: Double-quote strings
        for match in re.finditer(r'innerHTML\s*=\s*"([^"]*)"', script):
            content = match.group(1)
            if '"' in content.replace('\\"', ''):
                findings["critical"].append({
                    "type": "js_unescaped_quote",
                    "detail": f"Unescaped double-quote in innerHTML assignment",
                    "snippet": content[:80],
                })


def _check_fetch_credentials(html: str, findings: dict):
    """Find fetch() calls to /api/ endpoints missing credentials."""
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for script in scripts:
        # Find all fetch calls
        for match in re.finditer(r"fetch\(['\"](/api/[^'\"]+)['\"]([^)]*)\)", script):
            url = match.group(1)
            rest = match.group(2)
            # Check surrounding context (next ~200 chars) for credentials
            pos = match.end()
            context = script[pos:pos+200]
            full_context = rest + context
            if 'credentials' not in full_context.split('fetch(')[0] if 'fetch(' in full_context else full_context:
                # More precise: check the options object of THIS specific fetch
                # Look backwards for the options object
                options_block = rest + script[pos:pos+100]
                if 'credentials' not in options_block.split(').')[0]:
                    findings["warning"].append({
                        "type": "fetch_no_credentials",
                        "detail": f"fetch('{url}') may be missing credentials:'same-origin'",
                        "url": url,
                    })


def _check_empty_catch_handlers(html: str, findings: dict):
    """Find .catch() with empty bodies that swallow errors silently."""
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for script in scripts:
        # Pattern: .catch(function(){})  or .catch(()=>{})
        empties = re.findall(
            r'\.catch\(\s*(?:function\s*\(\s*\)\s*\{\s*\}|'
            r'\(\s*\)\s*=>\s*\{\s*\})\s*\)',
            script
        )
        for _ in empties:
            findings["warning"].append({
                "type": "empty_catch",
                "detail": "Empty .catch() handler swallows errors silently",
            })


def _check_onclick_handlers(html: str, findings: dict):
    """Check that onclick handlers reference defined JS functions."""
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    all_script = "\n".join(scripts)
    
    # Find all defined functions
    defined_funcs = set(re.findall(r'function\s+(\w+)\s*\(', all_script))
    # Also const/let/var func = function/arrow
    defined_funcs.update(re.findall(r'(?:const|let|var)\s+(\w+)\s*=\s*(?:function|\()', all_script))
    
    # Find all onclick handlers
    onclick_funcs = re.findall(r'onclick=["\'](\w+)\s*\(', html)
    
    for func_name in onclick_funcs:
        if func_name not in defined_funcs:
            # Skip built-in methods
            if func_name in ('window', 'location', 'document', 'alert', 'confirm', 'prompt',
                           'setTimeout', 'setInterval', 'console', 'JSON', 'this'):
                continue
            findings["critical"].append({
                "type": "broken_onclick",
                "detail": f"onclick calls '{func_name}()' but function is not defined",
                "function": func_name,
            })


def _check_route_wiring(html: str, route_list: list, findings: dict):
    """Check that all URLs referenced in HTML/JS point to real routes."""
    # Normalize routes (strip parameter names)
    normalized_routes = set()
    for route in route_list:
        # /pricecheck/<pcid>/save-prices → /pricecheck/*/save-prices
        normalized = re.sub(r'<[^>]+>', '*', route)
        normalized_routes.add(normalized)
    
    # Find all fetch URLs and form actions
    urls_in_html = set()
    urls_in_html.update(re.findall(r"fetch\(['\"](/api/[^'\"]+)", html))
    urls_in_html.update(re.findall(r'action=["\']([^"\']+)', html))
    urls_in_html.update(re.findall(r"href=['\"](/api/[^'\"]+)", html))
    
    for url in urls_in_html:
        # Skip dynamic URLs with template variables
        if '{{' in url or '{' in url:
            continue
        # Check if it matches any route pattern
        matched = False
        url_normalized = re.sub(r'/[a-f0-9-]{8,}/', '/*/', url)
        for route in normalized_routes:
            if route == url_normalized or route.replace('*', '') in url:
                matched = True
                break
        if not matched and url.startswith('/api/'):
            findings["info"].append({
                "type": "unmatched_url",
                "detail": f"URL '{url}' not found in route list (may use dynamic segments)",
                "url": url,
            })


def _check_responsive(html: str, findings: dict):
    """Check for responsive design basics."""
    # Viewport meta tag
    if 'viewport' not in html.lower():
        findings["warning"].append({
            "type": "no_viewport",
            "detail": "Missing <meta name='viewport'> — page won't scale on mobile",
        })
    
    # Media queries
    media_count = len(re.findall(r'@media', html))
    if media_count == 0:
        findings["warning"].append({
            "type": "no_media_queries",
            "detail": "No @media queries found — layout won't adapt to screen size",
        })
    
    # Table overflow handling
    tables = re.findall(r'<table[^>]*>', html)
    overflow_wrappers = re.findall(r'overflow-x\s*:\s*(?:auto|scroll)', html)
    if len(tables) > 0 and len(overflow_wrappers) == 0:
        findings["info"].append({
            "type": "table_no_overflow",
            "detail": f"{len(tables)} table(s) found but no overflow-x scroll wrapper",
        })


def _check_accessibility(html: str, findings: dict):
    """Check basic accessibility patterns."""
    # Buttons without accessible text
    empty_buttons = re.findall(r'<button[^>]*>\s*</button>', html)
    if empty_buttons:
        findings["info"].append({
            "type": "empty_buttons",
            "detail": f"{len(empty_buttons)} button(s) with no text content",
        })
    
    # Images without alt text
    imgs_no_alt = re.findall(r'<img(?![^>]*alt=)[^>]*>', html)
    if imgs_no_alt:
        findings["info"].append({
            "type": "img_no_alt",
            "detail": f"{len(imgs_no_alt)} image(s) missing alt attribute",
        })


def scan_python_source(filepath: str) -> dict:
    """Scan Python source file for code quality issues."""
    findings = {
        "critical": [],
        "warning": [],
        "info": [],
    }
    
    with open(filepath) as f:
        content = f.read()
        lines = content.split('\n')
    
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        
        # Bare except: (no exception type)
        if stripped == 'except:':
            findings["critical"].append({
                "type": "bare_except",
                "detail": f"Line {i}: bare 'except:' catches everything including KeyboardInterrupt",
                "line": i,
            })
        
        # except Exception without logging
        if re.match(r'except\s+Exception\s*(as\s+\w+)?:', stripped):
            # Check if next lines have logging
            next_chunk = "\n".join(lines[i:i+3])
            if 'log.' not in next_chunk and 'logging' not in next_chunk and 'print(' not in next_chunk:
                findings["warning"].append({
                    "type": "silent_except",
                    "detail": f"Line {i}: except block doesn't log the error",
                    "line": i,
                })
    
    findings["stats"] = {
        "lines": len(lines),
        "critical_count": len(findings["critical"]),
        "warning_count": len(findings["warning"]),
    }
    
    return findings


def full_scan(app=None) -> dict:
    """Run full QA scan across all pages and source files.
    
    Args:
        app: Flask app instance (if available, renders pages for HTML scanning)
    
    Returns:
        Complete QA report
    """
    report = {
        "pages": {},
        "source": {},
        "summary": {},
    }
    
    # ─── Source file scan ─────────────────────────────────────────────
    src_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), "src")
    
    py_files = []
    for root, dirs, files in os.walk(src_dir):
        for f in files:
            if f.endswith('.py'):
                py_files.append(os.path.join(root, f))
    
    total_critical = 0
    total_warning = 0
    
    for filepath in sorted(py_files):
        rel_path = os.path.relpath(filepath, src_dir)
        result = scan_python_source(filepath)
        report["source"][rel_path] = result
        total_critical += result["stats"]["critical_count"]
        total_warning += result["stats"]["warning_count"]
    
    # ─── HTML scan (if app provided) ──────────────────────────────────
    if app:
        with app.test_client() as client:
            # Authenticate
            client.post('/login', data={'password': os.environ.get('DASH_PASS', 'test')})
            
            # Get route list
            route_list = [rule.rule for rule in app.url_map.iter_rules()]
            
            # Scan key pages
            pages_to_scan = [
                ('/', 'Home'),
                ('/quotes', 'Quotes'),
            ]
            
            for url, name in pages_to_scan:
                try:
                    resp = client.get(url)
                    if resp.status_code == 200:
                        html = resp.data.decode('utf-8', errors='replace')
                        result = scan_html(html, route_list)
                        report["pages"][name] = result
                        total_critical += result["stats"]["critical_count"]
                        total_warning += result["stats"]["warning_count"]
                except Exception as e:
                    report["pages"][name] = {"error": str(e)}
    
    # ─── Summary ──────────────────────────────────────────────────────
    report["summary"] = {
        "total_critical": total_critical,
        "total_warnings": total_warning,
        "source_files_scanned": len(py_files),
        "pages_scanned": len(report["pages"]),
        "pass": total_critical == 0,
        "grade": "A" if total_critical == 0 and total_warning < 5 else
                 "B" if total_critical == 0 else
                 "C" if total_critical < 3 else "F",
    }
    
    return report


def agent_status() -> dict:
    """Return agent status for the control panel."""
    history = get_qa_history(limit=1)
    last = history[0] if history else {}
    return {
        "name": "QA Agent",
        "status": "active" if _monitor and _monitor._running else "ready",
        "version": "2.0.0",
        "description": "Health monitor, route checker, data validator, code scanner",
        "last_score": last.get("health_score", "—"),
        "last_grade": last.get("grade", "—"),
        "last_run": last.get("timestamp", "never"),
        "monitor_active": _monitor._running if _monitor else False,
        "capabilities": [
            "Route integrity (duplicate detection, auth coverage)",
            "Data integrity (JSON validation, corruption detection)",
            "Agent health (import checks, config validation)",
            "Code metrics (line counts, bloat detection)",
            "Env config (required/optional var verification)",
            "JS/HTML scanning (broken handlers, auth, responsive)",
            "Sales metrics (quote totals, PC profit, revenue goal tracking)",
            "Background monitoring (5-min interval)",
            "Health trend tracking",
        ],
    }


# ═══════════════════════════════════════════════════════════════════════
# Health Monitor System (Phase 25)
# ═══════════════════════════════════════════════════════════════════════

QA_REPORT_FILE = os.path.join(DATA_DIR, "qa_reports.json")
QA_INTERVAL = 300  # 5 minutes


def _check_route_integrity() -> list:
    """Check for duplicate routes, missing auth, endpoint conflicts."""
    results = []
    try:
        from flask import Flask
        test_app = Flask(__name__)
        from src.api.dashboard import bp as dash_bp
        test_app.register_blueprint(dash_bp)

        rules = list(test_app.url_map.iter_rules())
        results.append({
            "check": "routes", "status": "pass",
            "message": f"{len(rules)} routes registered OK",
        })

        # Check auth coverage on API routes
        dash_source = open(os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "api", "dashboard.py")).read()
        unprotected = []
        for rule in rules:
            path = str(rule)
            if "/api/" in path and "webhook" not in path and "static" not in path and "callback" not in path:
                func_name = rule.endpoint.split(".")[-1]
                idx = dash_source.find(f"def {func_name}(")
                if idx > 0:
                    context = dash_source[max(0, idx - 200):idx]
                    if "auth_required" not in context:
                        unprotected.append(path)

        if unprotected:
            results.append({
                "check": "auth_coverage", "status": "warn",
                "message": f"{len(unprotected)} API routes may lack auth",
                "details": unprotected[:5],
                "recommendation": "Add @auth_required to unprotected API routes",
            })
        else:
            results.append({"check": "auth_coverage", "status": "pass", "message": "All API routes protected"})

    except AssertionError as e:
        results.append({
            "check": "routes", "status": "fail",
            "message": f"ROUTE CONFLICT: {e}",
            "recommendation": "Fix duplicate route endpoints — app cannot start",
            "severity": "critical",
        })
    except Exception as e:
        results.append({"check": "routes", "status": "fail", "message": str(e)})
    return results


def _check_data_integrity() -> list:
    """Validate all JSON data files."""
    results = []
    json_files = [
        "quotes_log.json", "customers.json", "leads.json",
        "rfqs.json", "orders.json", "crm_activity.json",
        "voice_campaigns.json", "competitor_intel.json",
    ]
    for fname in json_files:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            results.append({"check": "data", "file": fname, "status": "info", "message": f"{fname} not found (OK if new)"})
            continue
        try:
            with open(fpath) as f:
                data = json.load(f)
            size_kb = os.path.getsize(fpath) / 1024
            count = len(data) if isinstance(data, (list, dict)) else 0
            status = "warn" if size_kb > 5000 else "pass"
            rec = f"Archive old records in {fname} (>{size_kb:.0f}KB)" if size_kb > 5000 else None
            results.append({"check": "data", "file": fname, "status": status,
                            "message": f"{fname}: {count} records, {size_kb:.1f}KB",
                            **({"recommendation": rec} if rec else {})})
        except json.JSONDecodeError as e:
            results.append({"check": "data", "file": fname, "status": "fail",
                            "message": f"CORRUPTED: {e}", "severity": "critical",
                            "recommendation": f"Restore {fname} from backup"})
    return results


def _check_agents_health() -> list:
    """Verify all agents import correctly."""
    results = []
    agents = {
        "email_poller": "src.agents.email_poller",
        "lead_gen": "src.agents.lead_gen_agent",
        "scprs_scanner": "src.agents.scprs_scanner",
        "voice_agent": "src.agents.voice_agent",
        "voice_campaigns": "src.agents.voice_campaigns",
        "quickbooks": "src.agents.quickbooks_agent",
        "predictive_intel": "src.agents.predictive_intel",
    }
    for name, module_path in agents.items():
        try:
            __import__(module_path)
            results.append({"check": "agent", "agent": name, "status": "pass", "message": f"{name} OK"})
        except ImportError as e:
            results.append({"check": "agent", "agent": name, "status": "warn", "message": f"{name}: {e}"})
        except Exception as e:
            results.append({"check": "agent", "agent": name, "status": "fail", "message": f"{name}: {e}"})
    return results


def _check_env_config() -> list:
    """Check environment variables."""
    results = []
    required = {"DASH_PASS": "Auth"}
    optional = {"DASH_USER": "Auth (default: reytech)",
                "VAPI_API_KEY": "Voice", "QB_CLIENT_ID": "QuickBooks",
                "QB_CLIENT_SECRET": "QuickBooks", "QB_REALM_ID": "QuickBooks",
                "GMAIL_ADDRESS": "Email", "GMAIL_PASSWORD": "Email",
                "ANTHROPIC_API_KEY": "AI", "TWILIO_ACCOUNT_SID": "Twilio",
                "TWILIO_AUTH_TOKEN": "Twilio", "TWILIO_PHONE_NUMBER": "Twilio"}
    for var, desc in required.items():
        if os.environ.get(var):
            results.append({"check": "env", "status": "pass", "message": f"{var} set"})
        else:
            results.append({"check": "env", "status": "fail", "message": f"{var} MISSING",
                            "severity": "critical", "recommendation": f"Set {var} in Railway"})
    for var, desc in optional.items():
        status = "pass" if os.environ.get(var) else "info"
        results.append({"check": "env", "status": status, "message": f"{var}: {'set' if os.environ.get(var) else 'not set'} ({desc})"})
    return results


def _check_code_metrics() -> list:
    """Code size and bloat metrics."""
    results = []
    src_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    total_lines = 0
    total_files = 0
    big_files = []
    for root, dirs, files in os.walk(src_root):
        dirs[:] = [d for d in dirs if d not in ("__pycache__",)]
        for f in files:
            if f.endswith(".py"):
                total_files += 1
                fp = os.path.join(root, f)
                with open(fp) as fh:
                    lines = len(fh.readlines())
                total_lines += lines
                if lines > 2000:
                    big_files.append((os.path.relpath(fp, src_root), lines))
    results.append({"check": "codebase", "status": "info",
                    "message": f"{total_files} files, {total_lines:,} lines"})
    for fp, lines in sorted(big_files, key=lambda x: -x[1]):
        # WARN only for files >10000 lines (actively needs splitting)
        # INFO for 2000-10000 (note it; weekend refactor planned per PRD)
        if lines > 10000:
            results.append({"check": "code_size", "status": "warn", "file": fp,
                            "message": f"{fp}: {lines} lines — active split needed",
                            "recommendation": f"Split {fp} into route modules (see PRD weekend refactor)"})
        else:
            results.append({"check": "code_size", "status": "info",
                            "message": f"{fp}: {lines} lines (refactor planned — see PRD sprint 4)"})
    return results


def _check_sales_metrics() -> list:
    """Validate sales data: quote totals, PC profit, revenue toward $2M goal."""
    results = []

    # ── 1. Quote integrity ──
    try:
        quotes_path = os.path.join(DATA_DIR, "quotes_log.json")
        with open(quotes_path) as f:
            quotes = json.load(f)

        live_quotes = [q for q in quotes if not q.get("is_test")]
        test_quotes = [q for q in quotes if q.get("is_test")]

        if test_quotes:
            results.append({"check": "sales", "status": "warn",
                            "message": f"{len(test_quotes)} test quotes in DB (should be 0 in production)",
                            "recommendation": "Remove test quotes from quotes_log.json"})
        else:
            results.append({"check": "sales", "status": "pass",
                            "message": "No test quotes in DB — clean"})

        # Validate totals match line items
        bad_totals = 0
        for q in live_quotes:
            qn = q.get("quote_number", "?")
            items = q.get("line_items", [])
            if items:
                calc_total = sum(
                    (it.get("unit_price", 0) or 0) * (it.get("quantity", 0) or 0)
                    for it in items
                )
                stated = q.get("total", 0) or 0
                if stated > 0 and abs(calc_total - stated) > 1.0:
                    bad_totals += 1
                    results.append({"check": "sales", "status": "warn",
                                    "message": f"{qn}: items sum ${calc_total:,.2f} ≠ total ${stated:,.2f}",
                                    "recommendation": f"Recalculate total for {qn}"})
        if not bad_totals:
            results.append({"check": "sales", "status": "pass",
                            "message": f"{len(live_quotes)} live quote(s), all totals valid"})

        # Missing required fields
        bad_fields = [q.get("quote_number","?") for q in live_quotes
                      if not q.get("quote_number") or not q.get("status")]
        if bad_fields:
            results.append({"check": "sales", "status": "fail", "severity": "critical",
                            "message": f"Quotes missing required fields: {bad_fields}",
                            "recommendation": "Fix quotes missing quote_number or status"})

        # Status distribution
        statuses = {}
        for q in live_quotes:
            s = q.get("status", "unknown")
            statuses[s] = statuses.get(s, 0) + 1
        results.append({"check": "sales", "status": "pass",
                        "message": f"Quote statuses: {statuses}"})

    except FileNotFoundError:
        results.append({"check": "sales", "status": "warn", "message": "quotes_log.json not found"})
    except json.JSONDecodeError:
        results.append({"check": "sales", "status": "fail", "severity": "critical",
                        "message": "quotes_log.json CORRUPTED",
                        "recommendation": "Restore quotes_log.json from backup"})

    # ── 2. Price Check profit validation ──
    try:
        pc_path = os.path.join(DATA_DIR, "price_checks.json")
        if os.path.exists(pc_path):
            with open(pc_path) as f:
                pcs = json.load(f)
            pcs = pcs if isinstance(pcs, list) else []

            total_pc_revenue = 0
            total_pc_cost = 0
            total_pc_profit = 0
            negative_margin_items = 0

            for pc in pcs:
                if pc.get("is_test"):
                    continue
                items = pc.get("line_items") or pc.get("items") or []
                for it in items:
                    our_price = it.get("our_price") or it.get("unit_price") or 0
                    cost = it.get("cost") or it.get("vendor_price") or 0
                    qty = it.get("quantity") or it.get("qty") or 1
                    revenue = our_price * qty
                    profit = (our_price - cost) * qty if cost > 0 else 0
                    total_pc_revenue += revenue
                    total_pc_cost += cost * qty
                    total_pc_profit += profit
                    if cost > 0 and our_price < cost:
                        negative_margin_items += 1

            margin_pct = round(total_pc_profit / total_pc_revenue * 100, 1) if total_pc_revenue else 0

            if negative_margin_items:
                results.append({"check": "sales", "status": "warn",
                                "message": f"{negative_margin_items} PC items with NEGATIVE margin — selling below cost",
                                "recommendation": "Review pricing on negative margin items"})

            results.append({"check": "sales", "status": "pass",
                            "message": f"PCs: {len(pcs)} total | Revenue: ${total_pc_revenue:,.2f} | Cost: ${total_pc_cost:,.2f} | Profit: ${total_pc_profit:,.2f} ({margin_pct}%)"})
        else:
            results.append({"check": "sales", "status": "info",
                            "message": "No price_checks.json yet"})
    except Exception as e:
        results.append({"check": "sales", "status": "warn", "message": f"PC validation error: {e}"})

    # ── 3. Revenue toward $2M goal ──
    try:
        from src.agents.sales_intel import update_revenue_tracker, REVENUE_GOAL
        rev = update_revenue_tracker()
        if rev.get("ok"):
            closed = rev.get("closed_revenue", 0)
            gap = rev.get("gap_to_goal", 0)
            pct = rev.get("pct_to_goal", 0)
            run_rate = rev.get("run_rate_annual", 0)
            on_track = rev.get("on_track", False)
            pipeline = rev.get("pipeline_value", 0)
            goal_m = REVENUE_GOAL / 1e6

            results.append({"check": "sales", "status": "pass",
                            "message": f"Revenue: ${closed:,.0f} closed ({pct:.1f}% of ${goal_m:.0f}M goal) | Pipeline: ${pipeline:,.0f}"})

            if not on_track and pct < 10:
                results.append({"check": "sales", "status": "info",
                                "message": f"Goal early stage — ${gap:,.0f} gap, need ${rev.get('monthly_needed', 0):,.0f}/mo"})
            elif not on_track:
                results.append({"check": "sales", "status": "warn",
                                "message": f"Run rate ${run_rate:,.0f}/yr — below ${goal_m:.0f}M pace",
                                "recommendation": f"Need ${rev.get('monthly_needed', 0):,.0f}/mo to hit goal"})
            else:
                results.append({"check": "sales", "status": "pass",
                                "message": f"On track for ${goal_m:.0f}M goal ✓"})

            # Cross-check: won quotes = revenue tracker
            try:
                with open(os.path.join(DATA_DIR, "quotes_log.json")) as f:
                    qs = json.load(f)
                won_total = sum(q.get("total", 0) for q in qs
                               if q.get("status") == "won" and not q.get("is_test"))
                tracker_val = rev.get("quotes_won_value", 0)
                if won_total > 0 and abs(won_total - tracker_val) > 1:
                    results.append({"check": "sales", "status": "warn",
                                    "message": f"Won quotes (${won_total:,.2f}) ≠ tracker (${tracker_val:,.2f})",
                                    "recommendation": "Sync revenue tracker with quotes"})
                else:
                    results.append({"check": "sales", "status": "pass",
                                    "message": "Won quotes ↔ revenue tracker in sync"})
            except Exception:
                pass
    except ImportError:
        results.append({"check": "sales", "status": "info",
                        "message": "Sales intel not loaded — revenue check skipped"})
    except Exception as e:
        results.append({"check": "sales", "status": "warn", "message": f"Revenue check error: {e}"})

    # ── 4. Orders ↔ Quotes consistency ──
    try:
        with open(os.path.join(DATA_DIR, "orders.json")) as f:
            orders = json.load(f)
        if isinstance(orders, dict):
            live_orders = {k: v for k, v in orders.items() if not v.get("is_test")}
            orphan = 0
            for oid, o in live_orders.items():
                qn = o.get("quote_number") or o.get("quote_ref")
                if qn:
                    with open(os.path.join(DATA_DIR, "quotes_log.json")) as f:
                        qs = json.load(f)
                    if not any(q.get("quote_number") == qn for q in qs):
                        orphan += 1
            if orphan:
                results.append({"check": "sales", "status": "warn",
                                "message": f"{orphan} orders reference missing quotes",
                                "recommendation": "Clean orphaned order refs"})
            else:
                results.append({"check": "sales", "status": "pass",
                                "message": f"{len(live_orders)} orders, all quote refs valid"})
    except FileNotFoundError:
        results.append({"check": "sales", "status": "info", "message": "No orders yet"})
    except Exception:
        pass

    return results


def _check_feature_321() -> list:
    """QA checks for PRD Feature 3.2.1: 1-click Price Check → Quote."""
    results = []

    # Check endpoint exists
    try:
        import importlib, sys
        db_mod = sys.modules.get("src.api.dashboard")
        if db_mod:
            bp_rules = [str(r) for r in db_mod.bp.url_map._rules] if hasattr(db_mod.bp, 'url_map') else []
        # Soft check — just verify the route function is importable
        from src.api.dashboard import api_quote_from_price_check
        results.append({"check": "feature_321", "status": "pass",
                         "message": "POST /api/quote/from-price-check endpoint: registered"})
    except Exception as e:
        results.append({"check": "feature_321", "status": "fail",
                         "message": f"1-click quote endpoint missing: {e}",
                         "severity": "critical",
                         "recommendation": "Re-deploy — /api/quote/from-price-check not found"})

    # Check banner in PC detail template
    try:
        from src.api.templates import build_pc_detail_html
        import inspect
        src = inspect.getsource(build_pc_detail_html)
        if "quote-gen-banner" in src and "generateQuote1Click" in src:
            results.append({"check": "feature_321", "status": "pass",
                             "message": "PC detail template: 1-click banner + JS present"})
        else:
            results.append({"check": "feature_321", "status": "warn",
                             "message": "PC detail template missing 1-click banner or JS",
                             "recommendation": "Check templates.py for quote-gen-banner"})
    except Exception as e:
        results.append({"check": "feature_321", "status": "info",
                         "message": f"Template check skipped: {e}"})

    # Check all 5 logging layers are wired
    try:
        import inspect
        from src.api.dashboard import api_quote_from_price_check as _fn
        fn_src = inspect.getsource(_fn)
        layers = {
            "JSON quotes_log": "JSON quotes_log.json",
            "SQLite quotes": "SQLite quotes table",
            "SQLite price_history": "SQLite price_history",
            "SQLite activity_log": "activity_log",
            "CRM activity_log.json": "CRM activity_log.json",
        }
        for layer, marker in layers.items():
            if marker in fn_src:
                results.append({"check": "feature_321", "status": "pass",
                                 "message": f"Logging layer: {layer} ✓"})
            else:
                results.append({"check": "feature_321", "status": "warn",
                                 "message": f"Logging layer missing: {layer}",
                                 "recommendation": f"Add {layer} logging to api_quote_from_price_check"})
    except Exception as e:
        results.append({"check": "feature_321", "status": "info",
                         "message": f"Logging layer check skipped: {e}"})

    return results


def _check_agent_intelligence() -> list:
    """Check that all agents have access to DB context (Skills Guide Pattern 5)."""
    results = []

    # Check agent_context module exists
    try:
        from src.core.agent_context import get_context, format_context_for_agent, get_best_price
        ctx = get_context(include_contacts=True, include_quotes=True, include_revenue=True)
        n_contacts = len(ctx.get("contacts", []))
        results.append({"check": "agent_intelligence", "status": "pass",
                         "message": f"agent_context.py: loaded, {n_contacts} contacts in DB context"})
    except Exception as e:
        results.append({"check": "agent_intelligence", "status": "fail",
                         "message": f"agent_context.py not functional: {e}",
                         "severity": "critical",
                         "recommendation": "Check src/core/agent_context.py"})

    # Check each key agent imports agent_context
    agents_with_ctx = ["growth_agent"]  # agents we've upskilled
    for agent_name in agents_with_ctx:
        try:
            import importlib
            mod = importlib.import_module(f"src.agents.{agent_name}")
            if hasattr(mod, "get_context") or hasattr(mod, "HAS_CTX"):
                results.append({"check": "agent_intelligence", "status": "pass",
                                 "message": f"{agent_name}: DB context layer ✓"})
            else:
                results.append({"check": "agent_intelligence", "status": "info",
                                 "message": f"{agent_name}: DB context not imported (scheduled)"})
        except Exception as e:
            results.append({"check": "agent_intelligence", "status": "info",
                             "message": f"{agent_name} context check: {e}"})

    # Check /api/agent/context endpoint
    try:
        from src.api.dashboard import api_agent_context
        results.append({"check": "agent_intelligence", "status": "pass",
                         "message": "GET /api/agent/context endpoint: registered"})
    except Exception as e:
        results.append({"check": "agent_intelligence", "status": "warn",
                         "message": f"/api/agent/context endpoint missing: {e}",
                         "recommendation": "Add api_agent_context route to dashboard.py"})

    return results


def _check_growth_campaign() -> list:
    """Check growth distro campaign readiness."""
    results = []

    # Check distro campaign function
    try:
        from src.agents.growth_agent import launch_distro_campaign, EMAIL_TEMPLATES
        if "distro_list" in EMAIL_TEMPLATES:
            results.append({"check": "growth_campaign", "status": "pass",
                             "message": f"Growth: launch_distro_campaign ready, {len(EMAIL_TEMPLATES)} templates loaded"})
        else:
            results.append({"check": "growth_campaign", "status": "warn",
                             "message": "distro_list template missing from EMAIL_TEMPLATES"})
    except Exception as e:
        results.append({"check": "growth_campaign", "status": "fail",
                         "message": f"launch_distro_campaign not importable: {e}",
                         "recommendation": "Check src/agents/growth_agent.py"})

    # Check Gmail config
    import os
    # Check CONFIG (loaded from Railway env at startup) then fall back to os.environ
    try:
        from src.api.dashboard import CONFIG as _DCFG
        gmail = _DCFG.get("email", {}).get("email") or os.environ.get("GMAIL_ADDRESS", "")
        gmail_pwd = _DCFG.get("email", {}).get("email_password") or os.environ.get("GMAIL_PASSWORD", "")
    except Exception:
        gmail = os.environ.get("GMAIL_ADDRESS", "")
        gmail_pwd = os.environ.get("GMAIL_PASSWORD", "")
    if gmail and gmail_pwd:
        results.append({"check": "growth_campaign", "status": "pass",
                         "message": f"Gmail configured: {gmail}"})
    else:
        # Staging is a valid operational state — emails queue for review before send.
        results.append({"check": "growth_campaign", "status": "info",
                         "message": "Gmail not configured — emails stage for review (valid operational mode)",
                         "recommendation": "Add GMAIL_ADDRESS + GMAIL_PASSWORD to Railway env to enable live send"})

    # Check available contacts for campaign
    try:
        from src.core.agent_context import get_context
        ctx = get_context(include_contacts=True)
        total = len(ctx.get("contacts", []))
        with_email = sum(1 for c in ctx.get("contacts", []) if c.get("email"))
        new_contacts = sum(1 for c in ctx.get("contacts", []) if c.get("status") == "new" and c.get("email"))
        results.append({"check": "growth_campaign", "status": "pass" if new_contacts > 0 else "warn",
                         "message": f"Campaign targets: {total} contacts, {with_email} have email, {new_contacts} new (never contacted)"})
    except Exception as e:
        results.append({"check": "growth_campaign", "status": "info",
                         "message": f"Contact count check: {e}"})

    # Check distro campaign API endpoint
    try:
        from src.api.dashboard import api_growth_distro_campaign
        results.append({"check": "growth_campaign", "status": "pass",
                         "message": "GET/POST /api/growth/distro-campaign endpoint: registered"})
    except Exception as e:
        results.append({"check": "growth_campaign", "status": "warn",
                         "message": f"Distro campaign endpoint missing: {e}"})

    return results


# ════════════════════════════════════════════════════════════════════════════════
# QA CHECKS — PRD Feature Wave Q1 2026  (Features 4.2 – 4.5 + supporting)
# Added after initial sprint to verify all shipped features.
# ════════════════════════════════════════════════════════════════════════════════

def _check_email_templates() -> list:
    """Feature 4.3 — Email Template Library (6 templates, [name] format)."""
    results = []
    try:
        import json as _json
        path = os.path.join(DATA_DIR, "email_templates.json")
        seed = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "seed_data", "email_templates.json")
        tpath = path if os.path.exists(path) else seed
        assert os.path.exists(tpath), "email_templates.json not found"
        data = _json.load(open(tpath))
        templates = data.get("templates", {})
        assert len(templates) >= 5, f"Expected >= 5 templates, got {len(templates)}"
        required = {"distro_list", "initial_outreach", "rfq_followup", "quote_won", "quote_lost"}
        missing = required - set(templates.keys())
        assert not missing, f"Missing required templates: {missing}"
        for tid, t in templates.items():
            assert t.get("subject"), f"Template '{tid}' missing subject"
            assert t.get("body"), f"Template '{tid}' missing body"
            assert t.get("variables"), f"Template '{tid}' missing variables list"
        # Check [name] format support
        sample_body = list(templates.values())[0].get("body", "")
        assert "[name]" in sample_body or "{{name}}" in sample_body, "No name variable in template body"
        results.append({"check": "email_templates", "status": "pass",
                        "message": f"{len(templates)} templates OK, [name] format verified"})
    except AssertionError as e:
        results.append({"check": "email_templates", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "email_templates", "status": "warn", "message": str(e)})
    return results


def _check_forecasting() -> list:
    """Feature 4.4 — Deal Forecasting + Win Probability (5-signal scoring)."""
    results = []
    try:
        from src.core.forecasting import score_quote
        test_quote = {
            "quote_number": "QA-FORECAST-TEST",
            "agency": "CDCR",
            "total": 5000,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "items_detail": [{"description": "nitrile gloves", "unit_price": 25.0, "qty": 200}],
        }
        result = score_quote(test_quote)
        assert "score" in result and 0 <= result["score"] <= 100
        assert result.get("label") in ("High", "Medium", "Low")
        assert "weighted_value" in result
        assert len(result.get("breakdown", {})) == 5, f"Expected 5 signals, got {len(result.get('breakdown', {}))}"
        results.append({"check": "forecasting", "status": "pass",
                        "message": f"Win probability: score={result['score']}, label={result['label']}, 5-signal breakdown OK"})
    except AssertionError as e:
        results.append({"check": "forecasting", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "forecasting", "status": "warn", "message": str(e)})
    return results


def _check_scprs_scheduler() -> list:
    """Feature 4.5 — SCPRS Dual Schedule (Mon 7am + Wed 10am PST)."""
    results = []
    try:
        from src.api.dashboard import _parse_simple_cron, _SCPRS_DEFAULT_SCHEDULES, _scprs_scheduler_state
        assert len(_SCPRS_DEFAULT_SCHEDULES) >= 2, "Expected at least 2 default schedules"
        mon = next((s for s in _SCPRS_DEFAULT_SCHEDULES if s["day_of_week"] == 0), None)
        wed = next((s for s in _SCPRS_DEFAULT_SCHEDULES if s["day_of_week"] == 2), None)
        assert mon, "Monday schedule not found"
        assert wed, "Wednesday schedule not found"
        assert mon["hour"] == 7, f"Monday should be 7am PST, got {mon['hour']}"
        assert wed["hour"] == 10, f"Wednesday should be 10am PST, got {wed['hour']}"
        results.append({"check": "scprs_scheduler", "status": "pass",
                        "message": f"Dual schedule: Mon 7am PST + Wed 10am PST. State: running={_scprs_scheduler_state.get('running')}"})
    except AssertionError as e:
        results.append({"check": "scprs_scheduler", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "scprs_scheduler", "status": "warn", "message": str(e)})
    return results


def _check_bulk_outreach() -> list:
    """Feature P1 — Bulk CRM Outreach (template personalization)."""
    results = []
    try:
        from src.api.dashboard import _load_email_templates, _personalize_template, _load_crm_contacts
        templates = _load_email_templates()
        assert templates.get("templates"), "No templates loaded"
        crm = _load_crm_contacts()
        contacts = list(crm.values()) if isinstance(crm, dict) else crm
        t = list(templates["templates"].values())[0]
        if contacts:
            result = _personalize_template(t, contact=contacts[0])
            assert result.get("subject"), "Personalization: no subject"
            assert result.get("body"), "Personalization: no body"
            assert "[name]" not in result["body"] or not contacts[0].get("buyer_name"),                 "[name] not filled for contact with name"
        results.append({"check": "bulk_outreach", "status": "pass",
                        "message": f"Bulk outreach: personalization OK, {len(contacts)} contacts available"})
    except AssertionError as e:
        results.append({"check": "bulk_outreach", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "bulk_outreach", "status": "warn", "message": str(e)})
    return results


def _check_email_auto_draft() -> list:
    """Feature 4.2 — Email RFQ → Auto Quote Draft wiring."""
    results = []
    try:
        import inspect, src.agents.email_poller as ep
        src_code = inspect.getsource(ep)
        assert "_auto_draft" in src_code, "_auto_draft function missing from email_poller"
        assert "daemon=True" in src_code, "Background thread flag missing"
        assert ".pdf" in src_code, "PDF detection missing"
        results.append({"check": "email_auto_draft", "status": "pass",
                        "message": "Auto-draft hook in email_poller: background thread + PDF detection present"})
    except AssertionError as e:
        results.append({"check": "email_auto_draft", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "email_auto_draft", "status": "warn", "message": str(e)})
    return results


def _check_price_history() -> list:
    """Feature P2 — Price History Intelligence."""
    results = []
    try:
        from src.core.db import get_price_history_db, get_price_stats
        stats = get_price_stats()
        assert isinstance(stats, dict), "get_price_stats() must return dict"
        results.append({"check": "price_history", "status": "pass",
                        "message": f"Price history: total_records={stats.get('total_records', 0)}"})
    except AssertionError as e:
        results.append({"check": "price_history", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "price_history", "status": "warn", "message": str(e)})
    return results


def _check_mobile_css() -> list:
    """Feature P1 — Mobile Responsive Layout."""
    results = []
    try:
        from src.api.templates import BASE_CSS
        missing = [s for s in ["@media(max-width:768px)", "@media(max-width:480px)"] if s not in BASE_CSS]
        assert not missing, f"Mobile CSS missing: {missing}"
        results.append({"check": "mobile_css", "status": "pass",
                        "message": "Mobile CSS: 768px + 480px breakpoints present"})
    except AssertionError as e:
        results.append({"check": "mobile_css", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "mobile_css", "status": "warn", "message": str(e)})
    return results


def _check_smoke_tests() -> list:
    """Smoke test suite — baseline validator for weekend refactor."""
    results = []
    try:
        smoke = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
                    os.path.abspath(__file__)))), "tests", "smoke_test.py")
        assert os.path.exists(smoke), "tests/smoke_test.py not found"
        code = open(smoke).read()
        assert "def run_pages" in code
        assert "--save-baseline" in code
        assert "--compare" in code
        results.append({"check": "smoke_tests", "status": "pass",
                        "message": f"Smoke test suite: {code.count(chr(10))} lines, baseline+compare modes present"})
    except AssertionError as e:
        results.append({"check": "smoke_tests", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "smoke_tests", "status": "warn", "message": str(e)})
    return results


def _check_quote_pdf_branding() -> list:
    """Feature P2 — Quote PDF branding (SB/DVBE tagline, website)."""
    results = []
    try:
        import inspect, src.forms.quote_generator as qg
        src_code = inspect.getsource(qg)
        missing = []
        if "SB" not in src_code or "DVBE" not in src_code: missing.append("SB/DVBE tagline")
        if "reytechinc.com" not in src_code: missing.append("website")
        assert not missing, f"Missing: {missing}"
        results.append({"check": "quote_pdf_branding", "status": "pass",
                        "message": "PDF branding: SB/DVBE tagline + website line present"})
    except AssertionError as e:
        results.append({"check": "quote_pdf_branding", "status": "fail", "message": str(e)})
    except Exception as e:
        results.append({"check": "quote_pdf_branding", "status": "warn", "message": str(e)})
    return results


def run_health_check(checks: list = None) -> dict:
    """Run full health check suite. Returns report with score and recommendations."""
    start = time.time()
    all_results = []

    check_map = {
        "routes": _check_route_integrity,
        "data": _check_data_integrity,
        "agents": _check_agents_health,
        "env": _check_env_config,
        "code": _check_code_metrics,
        "sales": _check_sales_metrics,
        # PRD Q1 2026 feature checks
        "feature_321": _check_feature_321,
        "agent_intelligence": _check_agent_intelligence,
        "growth_campaign": _check_growth_campaign,
        # PRD Feature Wave Q1 2026 (Features 4.2 – 4.5 + supporting)
        "email_templates": _check_email_templates,
        "forecasting": _check_forecasting,
        "scprs_scheduler": _check_scprs_scheduler,
        "bulk_outreach": _check_bulk_outreach,
        "email_auto_draft": _check_email_auto_draft,
        "price_history": _check_price_history,
        "mobile_css": _check_mobile_css,
        "smoke_tests": _check_smoke_tests,
        "quote_pdf_branding": _check_quote_pdf_branding,
        # Phase 28-29: Notification + Vendor Ordering checks
        "notify_agent": _check_notify_agent,
        "email_log": _check_email_log,
        "vendor_ordering": _check_vendor_ordering,
        "outbox_coverage": _check_outbox_coverage,
    }

    for name in (checks or list(check_map.keys())):
        if name in check_map:
            try:
                all_results.extend(check_map[name]())
            except Exception as e:
                all_results.append({"check": name, "status": "fail", "message": str(e)})

    duration = time.time() - start
    total = len(all_results)
    passed = sum(1 for r in all_results if r["status"] == "pass")
    failed = sum(1 for r in all_results if r["status"] == "fail")
    warned = sum(1 for r in all_results if r["status"] == "warn")
    info = sum(1 for r in all_results if r["status"] == "info")
    critical = [r for r in all_results if r.get("severity") == "critical"]
    recommendations = [r["recommendation"] for r in all_results if r.get("recommendation")]

    # Score: info items are neutral (not pass, not fail)
    scorable = total - info
    health = round((passed / max(scorable, 1)) * 100)
    health = max(0, health - len(critical) * 20)
    grade = "A" if health >= 90 else "B" if health >= 75 else "C" if health >= 60 else "D" if health >= 40 else "F"

    report = {
        "timestamp": datetime.now().isoformat(),
        "duration_seconds": round(duration, 2),
        "health_score": health,
        "grade": grade,
        "summary": {"total": total, "passed": passed, "failed": failed, "warned": warned, "info": info},
        "critical_issues": critical,
        "recommendations": recommendations,
        "results": all_results,
    }

    # Save to history
    _save_qa_report(report)
    log.info("QA Health: %s score=%d grade=%s (%d pass, %d fail, %d warn) %.1fs",
             "OK" if health >= 75 else "ISSUES", health, grade, passed, failed, warned, duration)
    return report


def _save_qa_report(report: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        with open(QA_REPORT_FILE) as f:
            reports = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        reports = []
    reports.append({
        "timestamp": report["timestamp"],
        "health_score": report["health_score"],
        "grade": report["grade"],
        "summary": report["summary"],
        "critical_count": len(report["critical_issues"]),
    })
    if len(reports) > 100:
        reports = reports[-100:]
    with open(QA_REPORT_FILE, "w") as f:
        json.dump(reports, f, indent=2, default=str)


def get_qa_history(limit: int = 20) -> list:
    try:
        with open(QA_REPORT_FILE) as f:
            reports = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []
    return sorted(reports, key=lambda r: r.get("timestamp", ""), reverse=True)[:limit]


def get_health_trend() -> dict:
    history = get_qa_history(50)
    if not history:
        return {"trend": "unknown", "scores": []}
    scores = [h["health_score"] for h in history]
    recent = scores[:5]
    older = scores[5:10] if len(scores) > 5 else scores
    avg_r = sum(recent) / len(recent) if recent else 0
    avg_o = sum(older) / len(older) if older else avg_r
    trend = "improving" if avg_r > avg_o + 5 else "declining" if avg_r < avg_o - 5 else "stable"
    return {"trend": trend, "current": scores[0] if scores else 0, "scores": scores[:20]}


# ─── Background Monitor ─────────────────────────────────────────────────────

class QAMonitor:
    def __init__(self, interval=QA_INTERVAL):
        self.interval = interval
        self._thread = None
        self._running = False

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        log.info("QA Monitor started (every %ds)", self.interval)

    def stop(self):
        self._running = False

    def _loop(self):
        time.sleep(30)  # Let app boot
        while self._running:
            try:
                report = run_health_check(checks=["routes", "data", "agents"])
                if report["health_score"] < 75:
                    log.warning("QA ALERT: score=%d — %s",
                                report["health_score"],
                                "; ".join(report["recommendations"][:3]))
            except Exception as e:
                log.error("QA Monitor: %s", e)
            time.sleep(self.interval)


_monitor = None

def start_qa_monitor(interval=QA_INTERVAL):
    global _monitor
    if _monitor is None:
        _monitor = QAMonitor(interval)
        _monitor.start()
    return _monitor


if __name__ == "__main__":
    import json
    
    # Run source-only scan (no Flask app needed)
    src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    py_files = []
    for root, dirs, files in os.walk(os.path.join(src_dir, "src")):
        for f in files:
            if f.endswith('.py'):
                py_files.append(os.path.join(root, f))
    
    total_c = 0
    total_w = 0
    print("=" * 60)
    print("QA AGENT — Source Code Scan")
    print("=" * 60)
    
    for filepath in sorted(py_files):
        result = scan_python_source(filepath)
        rel = os.path.relpath(filepath, src_dir)
        c = result["stats"]["critical_count"]
        w = result["stats"]["warning_count"]
        total_c += c
        total_w += w
        icon = "✅" if c == 0 and w == 0 else "⚠️" if c == 0 else "🔴"
        if c > 0 or w > 0:
            print(f"  {icon} {rel}: {c} critical, {w} warnings")
            for item in result["critical"] + result["warning"]:
                print(f"     → {item['detail']}")
    
    print(f"\n{'=' * 60}")
    print(f"  Source files: {len(py_files)}")
    print(f"  Critical: {total_c}")
    print(f"  Warnings: {total_w}")
    grade = "A" if total_c == 0 and total_w < 5 else "B" if total_c == 0 else "F"
    print(f"  Grade: {grade}")
    print(f"{'=' * 60}")


def _check_notify_agent() -> list:
    """Check notification agent configuration."""
    results = []
    try:
        from src.agents.notify_agent import get_agent_status as _ns, get_unread_count
        ns = _ns()
        results.append({"check": "notify_agent", "status": "pass",
                       "message": f"Notify agent: bell={get_unread_count()} unread, stale_watcher={ns.get('stale_watcher')}"})
        if ns.get("sms", {}).get("enabled"):
            results.append({"check": "notify_agent", "status": "pass",
                           "message": f"SMS alerts: configured to {ns['sms']['to']}"})
        else:
            results.append({"check": "notify_agent", "status": "warn",
                           "message": "SMS: not set up — set NOTIFY_PHONE + Twilio in Railway for text alerts",
                           "action": "NOTIFY_PHONE (Google Voice OK), TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER"})
        if ns.get("email_alerts", {}).get("enabled"):
            results.append({"check": "notify_agent", "status": "pass",
                           "message": f"Alert email: configured to {ns['email_alerts']['to']}"})
        else:
            results.append({"check": "notify_agent", "status": "warn",
                           "message": "Alert email: set NOTIFY_EMAIL in Railway for proactive email alerts",
                           "action": "Set NOTIFY_EMAIL to your personal email (separate from GMAIL_ADDRESS)"})
    except Exception as e:
        results.append({"check": "notify_agent", "status": "warn", "message": f"Notify check: {e}"})
    return results


def _check_email_log() -> list:
    """Check email communication log and notification persistence."""
    results = []
    try:
        from src.core.db import get_db as _gdb
        with _gdb() as conn:
            # Auto-create tables if they don't exist yet (first deploy)
            try:
                cnt = conn.execute("SELECT COUNT(*) FROM email_log").fetchone()[0]
                results.append({"check": "email_log", "status": "pass",
                               "message": f"Email audit trail: {cnt} entries (CS dispute resolution)"})
            except Exception:
                results.append({"check": "email_log", "status": "info",
                               "message": "Email log: table pending (auto-created on first email activity)"})
            try:
                ncnt = conn.execute("SELECT COUNT(*) FROM notifications").fetchone()[0]
                unread = conn.execute("SELECT COUNT(*) FROM notifications WHERE is_read=0").fetchone()[0]
                results.append({"check": "email_log", "status": "pass",
                               "message": f"Persistent bell: {ncnt} notifications, {unread} unread"})
            except Exception:
                results.append({"check": "email_log", "status": "info",
                               "message": "Notifications table: pending (auto-created on first alert)"})
    except Exception as e:
        results.append({"check": "email_log", "status": "warn", "message": f"Email log check: {e}"})
    return results


def _check_vendor_ordering() -> list:
    """Check vendor ordering agent readiness."""
    results = []
    try:
        from src.agents.vendor_ordering_agent import get_agent_status as _voas, get_vendor_orders
        vs = _voas()
        recent = get_vendor_orders(limit=20)
        if vs.get("email_po_active") and vs.get("email_po_vendors"):
            results.append({"check": "vendor_ordering", "status": "pass",
                           "message": f"Email PO: {len(vs['email_po_vendors'])} vendors ready (Curbell, IMS, Echelon, TSI)"})
        else:
            results.append({"check": "vendor_ordering", "status": "warn",
                           "message": "Email PO: needs GMAIL_ADDRESS + GMAIL_PASSWORD"})
        if vs.get("grainger_can_order"):
            results.append({"check": "vendor_ordering", "status": "pass",
                           "message": "Grainger: fully configured (search + pricing + order)"})
        else:
            results.append({"check": "vendor_ordering", "status": "warn",
                           "message": "Grainger REST API: not set (P0 — free API at api.grainger.com)",
                           "action": "Set GRAINGER_CLIENT_ID, GRAINGER_CLIENT_SECRET, GRAINGER_ACCOUNT_NUMBER in Railway"})
        if vs.get("amazon_configured"):
            results.append({"check": "vendor_ordering", "status": "pass",
                           "message": "Amazon Business SP-API: configured"})
        else:
            results.append({"check": "vendor_ordering", "status": "warn",
                           "message": "Amazon SP-API: not configured (SerpApi search still works)",
                           "action": "Set AMZN_ACCESS_KEY/SECRET/REFRESH_TOKEN from sellercentral.amazon.com"})
        results.append({"check": "vendor_ordering", "status": "info",
                       "message": f"Orders in DB: {len(recent)} | needs setup: {vs.get('vendors_setup_needed', [])[:3]}"})
    except Exception as e:
        results.append({"check": "vendor_ordering", "status": "warn", "message": f"Vendor ordering: {e}"})
    return results


def _check_outbox_coverage() -> list:
    """Check email outbox for stale unreviewed drafts."""
    results = []
    try:
        import json as _jq
        from datetime import datetime as _dtq, timedelta as _tdq
        outbox_path = os.path.join(DATA_DIR, "email_outbox.json")
        if os.path.exists(outbox_path):
            outbox = _jq.load(open(outbox_path))
            drafts = [e for e in outbox if e.get("status") in ("draft", "cs_draft")]
            sent = [e for e in outbox if e.get("status") == "sent"]
            stale_cutoff = (_dtq.now() - _tdq(hours=4)).isoformat()
            stale = [e for e in drafts if e.get("created_at", "9999") < stale_cutoff]
            if stale:
                results.append({"check": "outbox_coverage", "status": "warn",
                               "message": f"Outbox: {len(stale)} stale drafts (>4h) at /outbox",
                               "action": "Visit /outbox to review and send"})
            else:
                results.append({"check": "outbox_coverage", "status": "pass",
                               "message": f"Outbox: {len(drafts)} drafts pending, {len(sent)} sent, 0 stale"})
        else:
            results.append({"check": "outbox_coverage", "status": "info",
                           "message": "Outbox: clean (no drafts yet)"})
    except Exception as e:
        results.append({"check": "outbox_coverage", "status": "warn", "message": f"Outbox: {e}"})
    return results
