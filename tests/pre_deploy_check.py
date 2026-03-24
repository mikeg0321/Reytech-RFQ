#!/usr/bin/env python3
"""Pre-deploy validation — catches crashes BEFORE they hit production.

Run: python tests/pre_deploy_check.py
Exit 0 = safe to deploy, Exit 1 = broken.
"""
import ast, sys, os, importlib, traceback

# Fix Windows console encoding for emoji output
if sys.stdout.encoding and sys.stdout.encoding.lower().startswith("cp"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("FLASK_ENV", "testing")
os.environ.setdefault("SECRET_KEY", "test")

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

# Summary
print(f"\n{'═'*40}")
if ERRORS:
    print(f"❌ {len(ERRORS)} ISSUES FOUND — DO NOT DEPLOY")
    for label, err in ERRORS:
        print(f"  • {label}")
    sys.exit(1)
else:
    print("✅ ALL CHECKS PASSED — safe to deploy")
    sys.exit(0)
