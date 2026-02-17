#!/usr/bin/env python3
"""
scripts/qa_check.py — Pre-Deploy Quality Assurance Script

Run before every push to refactor-v7.2 or before merging to main.
Catches the classes of bugs that static analysis (grep, py_compile) misses:

  - DATA_DIR resolution (the bug that broke CRM, search, and quotes)
  - Route decorator completeness (the bug that broke Send Email)
  - Form action → route wiring
  - JS fetch → route wiring
  - Import path resolution
  - Data file integrity

Usage:
    python3 scripts/qa_check.py         # from project root
    python3 scripts/qa_check.py -v      # verbose mode

Exit codes:
    0 = all pass
    1 = failures detected
"""

import json
import os
import re
import sys

VERBOSE = "-v" in sys.argv

# Resolve project root from this script
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)

passed = 0
failed = 0
warnings = 0


def _pass(msg):
    global passed
    passed += 1
    if VERBOSE:
        print(f"  ✅ {msg}")


def _fail(msg):
    global failed
    failed += 1
    print(f"  ❌ FAIL: {msg}")


def _warn(msg):
    global warnings
    warnings += 1
    if VERBOSE:
        print(f"  ⚠️  {msg}")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. COMPILATION — Every .py file must compile
# ═══════════════════════════════════════════════════════════════════════════════
print("\n1. COMPILATION")
import py_compile
py_files = []
for root, dirs, files in os.walk("src"):
    dirs[:] = [d for d in dirs if d != "__pycache__"]
    for f in files:
        if f.endswith(".py"):
            py_files.append(os.path.join(root, f))
py_files.append("app.py")

compile_ok = 0
for pf in py_files:
    try:
        py_compile.compile(pf, doraise=True)
        compile_ok += 1
    except py_compile.PyCompileError as e:
        _fail(f"Compile error: {pf} — {e}")
_pass(f"{compile_ok}/{len(py_files)} files compile")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. DATA_DIR RESOLUTION — Every module must resolve to project_root/data/
# ═══════════════════════════════════════════════════════════════════════════════
print("\n2. DATA_DIR RESOLUTION")

canonical = os.path.abspath(os.path.join(PROJECT_ROOT, "data"))

modules_with_datadir = [
    "src/forms/quote_generator.py",
    "src/forms/price_check.py",
    "src/agents/product_research.py",
    "src/agents/tax_agent.py",
    "src/auto/auto_processor.py",
    "src/knowledge/won_quotes_db.py",
    "src/api/dashboard.py",
]

for mod_file in modules_with_datadir:
    with open(mod_file) as f:
        content = f.read()
    
    # Check if module imports from centralized paths.py
    if "from src.core.paths import" in content and "DATA_DIR" in content.split("from src.core.paths import")[1].split("\n")[0]:
        _pass(f"{mod_file} → imports from src.core.paths")
        continue
    
    # Find DATA_DIR assignments (skip comments)
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("except"):
            continue
        if re.match(r'DATA_DIR\s*=\s*os\.path\.join', stripped):
            # Evaluate the path expression
            try:
                fake_file = os.path.abspath(mod_file)
                expr = stripped.split("=", 1)[1].strip()
                expr = expr.replace("__file__", f"'{fake_file}'")
                resolved = os.path.abspath(eval(expr))
                if resolved == canonical:
                    _pass(f"{mod_file} → {resolved}")
                else:
                    _fail(f"{mod_file} → {resolved} (expected {canonical})")
            except Exception as e:
                _warn(f"{mod_file} — could not evaluate: {e}")
            break

# Check scprs_lookup DB_PATH
with open("src/agents/scprs_lookup.py") as f:
    content = f.read()

if "from src.core.paths import" in content and "DB_PATH" in content.split("from src.core.paths import")[1].split("\n")[0]:
    _pass(f"scprs_lookup DB_PATH → imports from src.core.paths")
else:
    for line in content.split("\n"):
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("except"):
            continue
        if re.match(r'DB_PATH\s*=\s*os\.path\.join', stripped):
            try:
                fake_file = os.path.abspath("src/agents/scprs_lookup.py")
                expr = stripped.split("=", 1)[1].strip()
                expr = expr.replace("__file__", f"'{fake_file}'")
                resolved = os.path.abspath(eval(expr))
                expected = os.path.join(canonical, "scprs_prices.json")
                if resolved == expected:
                    _pass(f"scprs_lookup DB_PATH → {resolved}")
                else:
                    _fail(f"scprs_lookup DB_PATH → {resolved} (expected {expected})")
            except Exception as e:
                _warn(f"scprs_lookup DB_PATH — could not evaluate: {e}")
            break


# ═══════════════════════════════════════════════════════════════════════════════
# 3. ROUTE INTEGRITY — Every @auth_required MUST have a preceding @bp.route
# ═══════════════════════════════════════════════════════════════════════════════
print("\n3. ROUTE INTEGRITY")
with open("src/api/dashboard.py") as f:
    dash_lines = f.readlines()

route_count = 0
auth_count = 0
missing_routes = []

for i, line in enumerate(dash_lines):
    if "@auth_required" in line:
        auth_count += 1
        # Walk backward to find @bp.route
        found_route = False
        for j in range(i - 1, max(i - 5, 0), -1):
            if "@bp.route" in dash_lines[j]:
                found_route = True
                break
        if not found_route:
            # Find the function name
            for j in range(i, min(i + 3, len(dash_lines))):
                if "def " in dash_lines[j]:
                    func = dash_lines[j].strip()
                    missing_routes.append(f"Line {i+1}: {func}")
                    break
    if "@bp.route" in line:
        route_count += 1

if missing_routes:
    for mr in missing_routes:
        _fail(f"@auth_required without @bp.route: {mr}")
else:
    _pass(f"All {auth_count} @auth_required have @bp.route ({route_count} routes)")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. FORM ACTION → ROUTE WIRING
# ═══════════════════════════════════════════════════════════════════════════════
print("\n4. FORM → ROUTE WIRING")

actions = set()
for fname in ["src/api/templates.py", "src/api/dashboard.py"]:
    with open(fname) as f:
        content = f.read()
    for m in re.findall(r'action="(/[^"]*)"', content):
        actions.add(re.sub(r'\{\{[^}]+\}\}', '<var>', m))

routes = set()
with open("src/api/dashboard.py") as f:
    for line in f:
        m = re.search(r'@bp\.route\("(/[^"]*)"', line)
        if m:
            routes.add(re.sub(r'<[^>]+>', '<var>', m.group(1)))

form_ok = 0
for a in sorted(actions):
    if a in routes:
        form_ok += 1
    else:
        _fail(f"Form action {a} → NO ROUTE")
_pass(f"{form_ok}/{len(actions)} form actions resolve") if form_ok == len(actions) else None


# ═══════════════════════════════════════════════════════════════════════════════
# 5. JS FETCH → ROUTE WIRING
# ═══════════════════════════════════════════════════════════════════════════════
print("\n5. JS FETCH → ROUTE WIRING")

urls = set()
for fname in ["src/api/templates.py", "src/api/dashboard.py"]:
    with open(fname) as f:
        content = f.read()
    for m in re.findall(r"fetch\(['\"](/[^'\"?]+)", content):
        normalized = re.sub(r'\{\{[^}]+\}\}', '<var>', m)
        normalized = re.sub(r'\{[a-z]+\}', '<var>', normalized)
        urls.add(normalized)

fetch_ok = 0
for u in sorted(urls):
    if u in routes:
        fetch_ok += 1
    elif any(u.startswith(r.rsplit("<", 1)[0]) for r in routes if "<" in r):
        fetch_ok += 1  # Partial match with dynamic segment
    else:
        _fail(f"JS fetch {u} → NO ROUTE")
_pass(f"{fetch_ok}/{len(urls)} JS fetch URLs resolve") if fetch_ok == len(urls) else None


# ═══════════════════════════════════════════════════════════════════════════════
# 6. IMPORT PATH RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════
print("\n6. IMPORT PATHS")

import_ok = 0
import_fail = 0
with open("src/api/dashboard.py") as f:
    content = f.read()
imports = re.findall(r'from (src\.\S+) import', content)
for imp in sorted(set(imports)):
    path = imp.replace('.', '/') + '.py'
    pkg_path = imp.replace('.', '/') + '/__init__.py'
    if os.path.exists(path) or os.path.exists(pkg_path):
        import_ok += 1
    else:
        _fail(f"Import {imp} → neither {path} nor {pkg_path} exist")
        import_fail += 1
if import_fail == 0:
    _pass(f"{import_ok}/{import_ok} import paths resolve")


# ═══════════════════════════════════════════════════════════════════════════════
# 7. DATA FILE INTEGRITY
# ═══════════════════════════════════════════════════════════════════════════════
print("\n7. DATA FILES")

data_files = {
    "customers.json": "data/customers.json",
    "quotes_log.json": "data/quotes_log.json",
    "quote_counter.json": "data/quote_counter.json",
}
for name, path in data_files.items():
    if os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
            count = len(data) if isinstance(data, (list, dict)) else "?"
            _pass(f"{name}: {count} records")
        except json.JSONDecodeError:
            _fail(f"{name}: corrupt JSON")
    else:
        _warn(f"{name}: not found (will auto-create)")


# ═══════════════════════════════════════════════════════════════════════════════
# 8. CONFIG FILES
# ═══════════════════════════════════════════════════════════════════════════════
print("\n8. CONFIG FILES")

required_configs = [
    "reytech_config.json", "Procfile", "railway.json",
    "requirements.txt", ".gitignore", "README.md",
    "src/forms/reytech_config.json", "src/forms/signature_transparent.png",
]
for cfg in required_configs:
    if os.path.exists(cfg):
        _pass(cfg)
    else:
        _fail(f"Missing: {cfg}")


# ═══════════════════════════════════════════════════════════════════════════════
# 9. BARE EXCEPT CHECK
# ═══════════════════════════════════════════════════════════════════════════════
print("\n9. CODE HYGIENE")

bare_excepts = 0
for root, dirs, files in os.walk("src"):
    dirs[:] = [d for d in dirs if d != "__pycache__"]
    for f in files:
        if f.endswith(".py"):
            fp = os.path.join(root, f)
            with open(fp) as fh:
                for ln, line in enumerate(fh, 1):
                    stripped = line.strip()
                    if stripped == "except:" or stripped.startswith("except: "):
                        bare_excepts += 1
                        _fail(f"Bare except: at {fp}:{ln}")
if bare_excepts == 0:
    _pass("No bare except: blocks")


# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
total = passed + failed + warnings
print(f"\n{'═'*60}")
print(f"QA RESULTS: {passed} passed, {failed} FAILED, {warnings} warnings")
print(f"{'═'*60}")

if failed > 0:
    print("\n🔴 DEPLOY BLOCKED — fix failures before pushing")
    sys.exit(1)
else:
    print("\n🟢 ALL CLEAR — safe to push/deploy")
    sys.exit(0)
