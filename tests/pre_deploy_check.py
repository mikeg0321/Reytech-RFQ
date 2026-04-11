#!/usr/bin/env python3
"""Pre-deploy validation — catches crashes BEFORE they hit production.

Run: python tests/pre_deploy_check.py
Exit 0 = safe to deploy, Exit 1 = broken.
"""
import ast, sys, os, importlib, traceback

# Unbuffered output — os._exit() doesn't flush buffers, so CI loses error messages
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

# Fix Windows console encoding for emoji output
if sys.stdout.encoding and sys.stdout.encoding.lower().startswith("cp"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("FLASK_ENV", "testing")
os.environ.setdefault("SECRET_KEY", "test")
# Suppress background threads during pre-deploy validation
os.environ["TESTING"] = "1"
os.environ["ENABLE_BACKGROUND_AGENTS"] = "false"
os.environ["ENABLE_EMAIL_POLLING"] = "false"

# Ensure data directory and critical JSON files exist for CI
_data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(_data_dir, exist_ok=True)
for _f in ["rfqs.json", "price_checks.json"]:
    _fpath = os.path.join(_data_dir, _f)
    if not os.path.exists(_fpath):
        with open(_fpath, "w") as _fp:
            _fp.write("{}")

ERRORS = []

def check(label, fn):
    try:
        fn()
        print(f"  ✅ {label}")
    except Exception as e:
        ERRORS.append((label, e))
        print(f"  ❌ {label}: {e}")

print("\n═══ PRE-DEPLOY CHECK ═══\n")

# 1. Syntax check all Python files
print("[1] Syntax check...")
for root, dirs, files in os.walk("src"):
    dirs[:] = [d for d in dirs if d != "__pycache__"]
    for f in files:
        if f.endswith(".py"):
            path = os.path.join(root, f)
            def _check_syntax(p=path):
                ast.parse(open(p, encoding="utf-8").read())
            check(f"syntax: {path}", _check_syntax)

# 2. Import core modules
print("\n[2] Import check...")
core_modules = [
    "src.core.paths",
    "src.agents.email_poller",
    "src.forms.generic_rfq_parser",
    "src.forms.quote_generator",
]
for mod in core_modules:
    def _import(m=mod):
        importlib.import_module(m)
    check(f"import: {mod}", _import)

# 3. Dashboard app loads without crash
print("\n[3] App load check...")
def _load_app():
    # Suppress background threads
    os.environ["TESTING"] = "1"
    from src.api.dashboard import bp
    assert bp is not None, "Blueprint is None"
    # Check all route functions exist and are callable
    rules = []
    for rule in bp.deferred_functions:
        rules.append(str(rule))
    assert len(rules) > 0, "No routes registered on blueprint"
check("dashboard blueprint loads", _load_app)

# 4. Check render_page calls match template variables
print("\n[4] Template variable check...")
import re

def _check_render_calls():
    """Find render_page() calls and verify passed kwargs exist as local variables."""
    issues = []
    for root, dirs, files in os.walk("src"):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            source = open(path, encoding="utf-8").read()
            tree = ast.parse(source)

            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef):
                    continue
                # Get all local variable names in function
                local_names = set()
                for child in ast.walk(node):
                    if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store):
                        local_names.add(child.id)
                    elif isinstance(child, ast.arg):
                        local_names.add(child.arg)
                    elif isinstance(child, ast.For):
                        if isinstance(child.target, ast.Name):
                            local_names.add(child.target.id)
                
                # Find render_page calls
                for child in ast.walk(node):
                    if isinstance(child, ast.Call):
                        func = child.func
                        func_name = ""
                        if isinstance(func, ast.Name):
                            func_name = func.id
                        elif isinstance(func, ast.Attribute):
                            func_name = func.attr
                        
                        if func_name == "render_page":
                            for kw in child.keywords:
                                if kw.arg and isinstance(kw.value, ast.Name):
                                    var_name = kw.value.id
                                    # Skip builtins and common globals
                                    skip = {"f", "str", "int", "len", "dict", "list", "set", "True", "False", "None",
                                            "request", "session", "redirect", "flash", "jsonify", "render_page",
                                            "log", "json", "os", "re", "datetime",
                                            "BASE_CSS", "AVAILABLE_FORMS", "BRIEF_HTML", "BRIEF_JS", "POLL_STATUS", "CONFIG", "DATA_DIR"}
                                    if var_name not in local_names and var_name not in skip:
                                        issues.append(f"{path}:{node.name}() passes '{kw.arg}={var_name}' but '{var_name}' not defined locally")
    
    if issues:
        raise ValueError("\n  ".join(["Undefined render variables:"] + issues))

check("render_page variable check", _check_render_calls)

# 5. Check no duplicate global declarations
print("\n[5] Global declaration check...")
def _check_globals():
    issues = []
    for root, dirs, files in os.walk("src"):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            tree = ast.parse(open(path, encoding="utf-8").read())
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    globals_found = []
                    for child in ast.walk(node):
                        if isinstance(child, ast.Global):
                            globals_found.extend(child.names)
                    dupes = [g for g in set(globals_found) if globals_found.count(g) > 1]
                    if dupes:
                        issues.append(f"{path}:{node.name}() has duplicate globals: {dupes}")
    if issues:
        raise ValueError("\n  ".join(issues))

check("no duplicate globals", _check_globals)

# 6. Check @bp.route not @app.route in modules
print("\n[6] Route decorator check...")
def _check_routes():
    issues = []
    for root, dirs, files in os.walk("src/api"):
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            source = open(path, encoding="utf-8").read()
            for i, line in enumerate(source.split("\n"), 1):
                if "@app.route" in line and "# noqa" not in line:
                    # Check if 'app' is actually defined in this file
                    if "app = Flask" not in source and "app = create_app" not in source:
                        issues.append(f"{path}:{i}: @app.route used but 'app' not defined — use @bp.route")
    if issues:
        raise ValueError("\n  ".join(issues))

check("route decorators use @bp.route", _check_routes)

# 7. Jinja2 template compilation check
print("\n[7] Template compilation check...")
def _check_templates():
    from jinja2 import Environment, FileSystemLoader
    template_dir = os.path.join("src", "templates")
    if not os.path.isdir(template_dir):
        return
    env = Environment(loader=FileSystemLoader(template_dir))
    issues = []
    for fname in os.listdir(template_dir):
        if not fname.endswith(".html"):
            continue
        try:
            env.get_template(fname)
        except Exception as e:
            issues.append(f"{fname} line {getattr(e, 'lineno', '?')}: {getattr(e, 'message', str(e))}")
    if issues:
        raise ValueError("\n  ".join(["Template errors:"] + issues))

check("Jinja2 templates compile", _check_templates)

# 8. Duplicate function names across route modules (Flask AssertionError)
print("\n[8] Duplicate endpoint check...")
def _check_duplicate_endpoints():
    import re
    from collections import defaultdict
    funcs = defaultdict(list)
    modules_dir = os.path.join("src", "api", "modules")
    if not os.path.isdir(modules_dir):
        return
    for fname in sorted(os.listdir(modules_dir)):
        if not fname.endswith(".py"):
            continue
        fpath = os.path.join(modules_dir, fname)
        with open(fpath, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                m = re.match(r'^def (\w+)\(', line)
                if m:
                    funcs[m.group(1)].append(f"{fname}:{i}")
    dupes = {k: v for k, v in funcs.items() if len(v) > 1}
    if dupes:
        issues = []
        for name, locs in sorted(dupes.items()):
            issues.append(f"DUPLICATE def {name}() in: {', '.join(locs)}")
        raise ValueError("\n  ".join(["Flask will crash on duplicate endpoints:"] + issues))

check("no duplicate endpoint functions", _check_duplicate_endpoints)

# 9. Package generation smoke test — actually generate PDFs from templates
print("\n[9] Package generation smoke test...")

def _check_form_fillers():
    """Generate each form from templates — catches field name mismatches,
    missing imports, signature bugs, and PDF corruption BEFORE deploy."""
    import tempfile, shutil
    tmpdir = tempfile.mkdtemp(prefix="predeploy_pkg_")
    try:
        from src.forms.reytech_filler_v4 import (
            fill_std204, fill_calrecycle_standalone, fill_cv012_cuf,
            fill_bidder_declaration, fill_darfur_standalone, fill_std1000,
            generate_dvbe_843, generate_darfur_act, generate_bidder_declaration,
            generate_drug_free, fill_and_sign_pdf, _sanitize_for_pdf, SIGN_FIELDS,
        )
        from src.forms.quote_generator import generate_quote, peek_next_quote_number
        from pypdf import PdfReader

        config = {
            "company": {
                "name": "Reytech Inc.", "address": "PO Box 1234, San Diego, CA 92101",
                "owner": "Michael Gutierrez", "title": "President",
                "phone": "(619) 555-1234", "email": "sales@reytechinc.com",
                "fein": "12-3456789", "sellers_permit": "SR ABC 12-345678",
                "cert_number": "2012345", "cert_expiration": "12/31/2026",
                "sb_cert": "2012345", "dvbe_cert": "2012345",
            }
        }
        rfq = {
            "solicitation_number": "PREDEPLOY-001", "sign_date": "03/10/2026",
            "release_date": "03/01/2026", "due_date": "03/15/2026",
            "delivery_days": "30", "delivery_location": "CIW, Corona, CA 92878",
            "requestor_name": "Test User", "requestor_email": "test@cdcr.ca.gov",
            "line_items": [
                {"description": "Test Item", "quantity": 10, "uom": "EA",
                 "unit_price": 5.00, "bid_price": 7.50, "supplier_cost": 5.00},
            ],
        }
        template_dir = os.path.join("data", "templates")
        generated = 0
        # Template-based fillers
        fillers = [
            ("std204", fill_std204, "std204_blank.pdf"),
            ("calrecycle", fill_calrecycle_standalone, "calrecycle_74_blank.pdf"),
            ("cv012_cuf", fill_cv012_cuf, "cv012_cuf_blank.pdf"),
            ("bidder_decl", fill_bidder_declaration, "bidder_declaration_blank.pdf"),
            ("darfur", fill_darfur_standalone, "darfur_act_blank.pdf"),
            ("std1000", fill_std1000, "std1000_blank.pdf"),
        ]
        for name, fn, template_name in fillers:
            tpl = os.path.join(template_dir, template_name)
            if not os.path.exists(tpl):
                continue
            out = os.path.join(tmpdir, f"{name}.pdf")
            fn(tpl, rfq, config, out)
            assert os.path.exists(out), f"{name} PDF not created"
            r = PdfReader(out)
            assert len(r.pages) >= 1, f"{name} PDF has 0 pages"
            generated += 1

        # ReportLab generators (no template needed)
        generators = [
            ("dvbe843", generate_dvbe_843),
            ("darfur_gen", generate_darfur_act),
            ("bidder_gen", generate_bidder_declaration),
            ("drug_free", generate_drug_free),
        ]
        for name, fn in generators:
            out = os.path.join(tmpdir, f"{name}.pdf")
            fn(rfq, config, out)
            assert os.path.exists(out), f"{name} PDF not created"
            generated += 1

        # Quote
        out = os.path.join(tmpdir, "quote.pdf")
        result = generate_quote(rfq, out, agency="CDCR", quote_number="R26QPREDEPLOY")
        assert result.get("ok"), f"Quote failed: {result}"
        assert os.path.exists(out), "Quote PDF not created"
        generated += 1

        # Sanitizer
        s = _sanitize_for_pdf("He said \u201cyes\u201d \u2014 it\u2019s fine")
        assert "\u201c" not in s, "Smart quotes not sanitized"

        # SIGN_FIELDS
        assert len(SIGN_FIELDS) >= 5, "SIGN_FIELDS whitelist too small"

        # peek idempotent
        q1 = peek_next_quote_number()
        q2 = peek_next_quote_number()
        assert q1 == q2, f"Peek burned counter: {q1} → {q2}"

        print(f"    Generated {generated} PDFs successfully")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

check("form fillers + quote generation", _check_form_fillers)

def _check_agency_configs():
    """Verify agency form sets are correct (CCHCS/CDCR are minimal)."""
    from src.core.agency_config import load_agency_configs
    configs = load_agency_configs()
    issues = []
    for agency in ("cchcs", "cdcr"):
        cfg = configs.get(agency, {})
        forms = cfg.get("required_forms", [])
        for bad in ("dvbe843", "calrecycle74", "sellers_permit"):
            if bad in forms:
                issues.append(f"{agency.upper()} has standalone '{bad}' — should be inside bid package")
    # All agencies must have quote
    for key, cfg in configs.items():
        forms = cfg.get("required_forms", [])
        if "quote" not in forms:
            issues.append(f"Agency '{key}' missing 'quote' in required_forms")
    if issues:
        raise ValueError("\n  ".join(issues))

check("agency config form sets", _check_agency_configs)

# Summary
print(f"\n{'═'*40}")
sys.stdout.flush()
sys.stderr.flush()
if ERRORS:
    print(f"❌ {len(ERRORS)} ISSUES FOUND — DO NOT DEPLOY")
    for label, err in ERRORS:
        print(f"  • {label}: {err}")
    sys.stdout.flush()
    os._exit(1)  # Force exit — background daemon threads from app import would hang sys.exit
else:
    print("✅ ALL CHECKS PASSED — safe to deploy")
    sys.stdout.flush()
    os._exit(0)  # Force exit — background daemon threads from app import would hang sys.exit
