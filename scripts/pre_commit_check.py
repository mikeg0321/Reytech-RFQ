#!/usr/bin/env python3
"""
Pre-commit safety checks — run before every push.
Catches the classes of bugs that have crashed production.

Usage: python scripts/pre_commit_check.py
Exit code 0 = safe to commit, 1 = fix issues first.
"""
import subprocess
import sys
import os

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
errors = []
warnings = []


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True, shell=True)


def check(name, passed, detail=""):
    if passed:
        print(f"  OK  {name}")
    else:
        print(f"  FAIL  {name}  {detail}")
        errors.append(f"{name}: {detail}")


def warn(name, detail=""):
    print(f"  WARN  {name}  {detail}")
    warnings.append(f"{name}: {detail}")


print("=" * 60)
print("PRE-COMMIT SAFETY CHECKS")
print("=" * 60)

# 1. All changed Python files compile
print("\n1. Compile check...")
r = run("git diff --name-only HEAD --diff-filter=ACMR")
changed = [f.strip() for f in r.stdout.strip().split("\n") if f.strip().endswith(".py")]
if not changed:
    r = run("git diff --cached --name-only --diff-filter=ACMR")
    changed = [f.strip() for f in r.stdout.strip().split("\n") if f.strip().endswith(".py")]
for f in changed:
    if os.path.exists(f):
        r = run(f"python -m py_compile {f}")
        check(f"compile {f}", r.returncode == 0, r.stderr.strip()[:100])

# 2. No module-level blocking code in dashboard.py
print("\n2. No blocking boot code...")
if os.path.exists("src/api/dashboard.py"):
    with open("src/api/dashboard.py", encoding="utf-8", errors="replace") as f:
        content = f.read()
    # Check for json.load at module level (outside functions/classes)
    lines = content.split("\n")
    in_function = False
    for i, line in enumerate(lines, 1):
        stripped = line.lstrip()
        if stripped.startswith("def ") or stripped.startswith("class "):
            in_function = True
        if not line.startswith(" ") and not line.startswith("\t") and stripped and not stripped.startswith("#") and not stripped.startswith("@"):
            if "json.load(" in stripped and "def " not in stripped:
                warn(f"dashboard.py:{i}", f"json.load at module level may block boot: {stripped[:60]}")
            if "get_db()" in stripped and "def " not in stripped and "import" not in stripped:
                warn(f"dashboard.py:{i}", f"get_db() at module level may block boot: {stripped[:60]}")

# 3. No PC deletion
print("\n3. No PC deletion (Law 22)...")
r = run('grep -rn "del pcs\\[" src/ --include="*.py"')
lines = [l for l in r.stdout.strip().split("\n") if l and "__pycache__" not in l]
check("no 'del pcs[' in codebase", len(lines) == 0, f"found {len(lines)}: {lines[:2]}")

# 4. No dangerous window.fetch override (CSRF patcher is OK)
print("\n4. No dangerous fetch override...")
r = run('grep -c "window.fetch=" src/templates/base.html')
count = int(r.stdout.strip()) if r.stdout.strip() else 0
check("fetch overrides in base.html", count <= 1, f"found {count} — only CSRF patcher is allowed")

# 5. safe_save_json has callers
print("\n5. safe_save_json wired (Law 20)...")
r = run('grep -rn "safe_save_json" src/ --include="*.py"')
lines = [l for l in r.stdout.strip().split("\n") if l and "__pycache__" not in l and "def safe_save_json" not in l]
check("safe_save_json has callers", len(lines) >= 2, f"only {len(lines)} callers")

# 6. No save_rfqs in GET handlers
print("\n6. No save_rfqs in GET handlers (Law 14)...")
if os.path.exists("src/api/modules/routes_rfq.py"):
    with open("src/api/modules/routes_rfq.py", encoding="utf-8", errors="replace") as f:
        code = f.read()
    import re
    m = re.search(r'def detail\(rid\):.*?(?=\n@bp\.route|\ndef [a-z])', code, re.DOTALL)
    if m:
        saves = [l.strip() for l in m.group().split("\n") if "save_rfqs" in l and not l.strip().startswith("#")]
        check("no save_rfqs in detail()", len(saves) == 0, f"found: {saves}")

# 7. Boot checks are deferred
print("\n7. Boot checks deferred...")
if os.path.exists("src/api/dashboard.py"):
    with open("src/api/dashboard.py", encoding="utf-8", errors="replace") as f:
        content = f.read()
    check("boot checks in background thread",
          "_deferred_boot_checks" in content and "Thread" in content,
          "boot checks must run in background thread, not at module level")

# 8. Home page fetch count
print("\n8. Home page fetch count...")
if os.path.exists("src/templates/home.html"):
    with open("src/templates/home.html", encoding="utf-8", errors="replace") as f:
        content = f.read()
    # Count non-deferred fetches (not inside setTimeout)
    import re
    fetches = re.findall(r'fetch\(', content)
    check(f"home.html has {len(fetches)} fetch calls", len(fetches) <= 15,
          f"{len(fetches)} fetches — consider deferring non-critical ones")

# 9. Data guard size limit
print("\n9. Data guard size limit...")
if os.path.exists("src/core/data_guard.py"):
    with open("src/core/data_guard.py", encoding="utf-8", errors="replace") as f:
        content = f.read()
    check("size guard exists", "size_mb > 10" in content or "size_mb > 5" in content,
          "safe_save_json must block saves >10MB to prevent disk blowup")

# 10. Snapshot throttling
print("\n10. Snapshot throttling...")
if os.path.exists("src/core/data_guard.py"):
    check("snapshot throttle exists", "SNAPSHOT_THROTTLE" in content,
          "snapshots must be throttled to prevent 85/day")
    check("max snapshots per file", "MAX_SNAPSHOTS_PER_FILE" in content,
          "must cap snapshots per file")

print("\n" + "=" * 60)
if errors:
    print(f"BLOCKED: {len(errors)} error(s) — fix before committing")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)
elif warnings:
    print(f"OK with {len(warnings)} warning(s)")
    for w in warnings:
        print(f"  {w}")
    sys.exit(0)
else:
    print("ALL CHECKS PASSED")
    sys.exit(0)
