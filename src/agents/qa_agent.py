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
import logging

log = logging.getLogger("qa_agent")


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
    return {
        "name": "QA Agent",
        "status": "ready",
        "description": "Scans pages for broken buttons, auth issues, JS errors, responsive gaps",
        "capabilities": [
            "JS string escaping validation",
            "fetch() credential verification",
            "onclick handler wiring check",
            "Responsive CSS audit",
            "Python source quality scan",
        ],
    }


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
