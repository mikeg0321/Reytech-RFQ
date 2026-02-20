#!/usr/bin/env python3
"""Pre-push build validation — run before every git push to catch issues early.

Usage: python scripts/validate_build.py

Checks:
  1. All Python files compile (no syntax errors)
  2. All imports resolve (no missing modules)
  3. Flask app creates successfully
  4. Classification tests pass
  5. Key routes respond 200
"""
import os
import sys
import py_compile
import glob

# Ensure repo root is in path
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)
os.chdir(REPO_ROOT)

os.environ.setdefault("SECRET_KEY", "test")

def check_syntax():
    """Check all .py files for syntax errors."""
    errors = []
    for f in glob.glob("src/**/*.py", recursive=True):
        try:
            py_compile.compile(f, doraise=True)
        except py_compile.PyCompileError as e:
            errors.append(f"{f}: {e}")
    for f in ["app.py"]:
        if os.path.exists(f):
            try:
                py_compile.compile(f, doraise=True)
            except py_compile.PyCompileError as e:
                errors.append(f"{f}: {e}")
    return errors


def check_app_creates():
    """Check Flask app creates without crash."""
    try:
        from app import create_app
        app = create_app()
        routes = len(list(app.url_map.iter_rules()))
        return None, routes, app
    except Exception as e:
        return str(e), 0, None


def check_classification(app):
    """Run classification tests."""
    import base64
    with app.test_client() as c:
        creds = base64.b64encode(b"reytech:changeme").decode()
        h = {"Authorization": f"Basic {creds}"}
        r = c.get("/api/qa/classification-test", headers=h)
        if r.status_code != 200:
            return f"Classification endpoint returned {r.status_code}"
        d = r.get_json()
        if not d.get("ok"):
            return f"Classification failed: {d}"
        if d.get("passed") != d.get("total_tests"):
            failures = [t["label"] for t in d.get("results", []) if not t.get("passed")]
            return f"Classification: {d['passed']}/{d['total_tests']} — failures: {failures}"
    return None


def check_routes(app):
    """Check key routes respond."""
    import base64
    errors = []
    with app.test_client() as c:
        creds = base64.b64encode(b"reytech:changeme").decode()
        h = {"Authorization": f"Basic {creds}"}
        for path in ["/", "/qa/email-pipeline", "/api/qa/trends"]:
            r = c.get(path, headers=h)
            if r.status_code not in (200, 302):
                errors.append(f"{path} → {r.status_code}")
    return errors


if __name__ == "__main__":
    print("=" * 60)
    print("BUILD VALIDATION")
    print("=" * 60)
    
    all_ok = True
    
    # 1. Syntax
    print("\n1. Syntax check...")
    errs = check_syntax()
    if errs:
        print(f"   FAIL: {len(errs)} syntax errors")
        for e in errs:
            print(f"   - {e}")
        all_ok = False
    else:
        py_count = len(glob.glob("src/**/*.py", recursive=True))
        print(f"   OK: {py_count} files compiled")
    
    # 2. App creation
    print("\n2. App creation...")
    err, routes, app = check_app_creates()
    if err:
        print(f"   FAIL: {err}")
        all_ok = False
    else:
        print(f"   OK: {routes} routes")
    
    if app:
        # 3. Classification
        print("\n3. Classification tests...")
        err = check_classification(app)
        if err:
            print(f"   FAIL: {err}")
            all_ok = False
        else:
            print("   OK: all tests pass")
        
        # 4. Routes
        print("\n4. Route checks...")
        errs = check_routes(app)
        if errs:
            print(f"   FAIL: {len(errs)} routes broken")
            for e in errs:
                print(f"   - {e}")
            all_ok = False
        else:
            print("   OK: key routes respond 200")
    
    print("\n" + "=" * 60)
    if all_ok:
        print("BUILD VALIDATION: ALL PASSED")
        print("=" * 60)
        sys.exit(0)
    else:
        print("BUILD VALIDATION: FAILED — do not push")
        print("=" * 60)
        sys.exit(1)
