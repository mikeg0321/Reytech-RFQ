#!/usr/bin/env python3
"""Pipeline status dashboard — shows exactly where your code is in the deploy pipeline."""
import subprocess, json, sys, time, os

# Fix Windows console encoding
if sys.stdout.encoding and sys.stdout.encoding.lower().startswith("cp"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

def run(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15)
    return r.stdout.strip(), r.returncode

def main():
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    branch, _ = run("git branch --show-current")
    commit, _ = run('git log -1 --format="%h %s"')

    # ── Gather state ──
    # 1. Uncommitted code files
    dirty_out, _ = run("git status --porcelain -- '*.py' '*.html' '*.yml' '*.toml' 'Makefile'")
    dirty_files = [l for l in dirty_out.split("\n") if l.strip()] if dirty_out else []

    # 2. Unpushed commits
    unpushed_out = ""
    unpushed = []
    if branch and branch != "main":
        unpushed_out, rc = run(f"git log --oneline origin/{branch}..{branch}")
        if rc == 0 and unpushed_out:
            unpushed = unpushed_out.split("\n")
    on_main = branch == "main"

    # 3. Open PRs with check status
    prs_json, _ = run('gh pr list --limit 10 --json number,title,headRefName,statusCheckRollup,url,createdAt')
    prs = json.loads(prs_json) if prs_json else []

    # 4. Last deploy on main
    last_deploy, _ = run('git log origin/main -1 --format="%h %s (%ar)"')

    # 5. Figure out which step WE are on (matches STAGES index)
    # 0=Edit, 1=Commit, 2=Push, 3=CI, 4=Merge, 5=Live
    step = 0
    our_pr = None
    our_checks = {}

    if branch and branch != "main":
        if dirty_files:
            step = 0  # Edit — uncommitted changes
        elif unpushed:
            step = 1  # Commit — committed but not pushed
        else:
            step = 2  # Push — pushed, waiting for PR or CI
        # Check if we have a PR
        for pr in prs:
            if pr.get("headRefName") == branch:
                our_pr = pr
                step = 3  # CI
                checks = pr.get("statusCheckRollup", [])
                if checks:
                    for c in checks:
                        name = c.get("name") or c.get("context", "?")
                        conclusion = c.get("conclusion")
                        status = c.get("status", "")
                        if conclusion == "SUCCESS":
                            our_checks[name] = "pass"
                        elif conclusion in ("FAILURE", "CANCELLED"):
                            our_checks[name] = "FAIL"
                        elif status in ("IN_PROGRESS", "QUEUED"):
                            our_checks[name] = "running"
                        else:
                            our_checks[name] = status or "pending"
                    if all(v == "pass" for v in our_checks.values()):
                        step = 4  # Merge — CI passed, ready to promote
                break
    elif on_main:
        step = -1  # on main, need to branch

    # ── Render ──
    STAGES = [
        ("Edit",       "make changes to code"),
        ("Commit",     "git add + git commit"),
        ("Push",       "make ship (tests + push + PR)"),
        ("CI",         "4 checks: static, build, tests, pre-deploy"),
        ("Merge",      "make promote"),
        ("Live",       "Railway auto-deploys"),
    ]

    # Estimate: CI takes ~17 min from push. Checks we know timing for:
    CI_TIMES = {"static-checks": 0.2, "build-checks": 3.5, "tests": 11, "pre-deploy": 3, "validate": 3.5}

    print()
    print("  ════════════════════════════════════════════")
    print("  PIPELINE STATUS")
    print(f"  Branch: {branch or 'detached'}  ({commit})")
    print("  ════════════════════════════════════════════")
    print()

    # You-are-here indicator
    for i, (name, desc) in enumerate(STAGES):
        if step == -1:
            marker = "  "
        elif i < step:
            marker = " \u2713"  # checkmark
        elif i == step:
            marker = ">>"
        else:
            marker = "  "

        if i == step:
            print(f"  {marker} [{name:8s}]  {desc}  <-- YOU ARE HERE")
        else:
            print(f"  {marker}  {name:8s}   {desc}")

    if step == -1:
        print()
        print("  \u26a0  You're on main. Create a branch first:")
        print("     make branch name=feat/your-feature")

    # ── Details per stage ──
    print()
    print("  ──────────────────────────────────────────")

    # Uncommitted
    if dirty_files:
        print(f"  UNCOMMITTED: {len(dirty_files)} file(s)")
        for f in dirty_files[:5]:
            print(f"    {f}")
        if len(dirty_files) > 5:
            print(f"    ... and {len(dirty_files) - 5} more")
    else:
        print("  UNCOMMITTED: clean")

    # Unpushed
    if unpushed:
        print(f"  UNPUSHED: {len(unpushed)} commit(s) — run: make ship")
        for c in unpushed[:3]:
            print(f"    {c}")
    elif not on_main:
        print("  UNPUSHED: synced with remote")

    print()

    # CI status
    if our_checks:
        print("  CI CHECKS:")
        total_est = 0
        elapsed_est = 0
        for name in ["static-checks", "build-checks", "tests", "pre-deploy", "validate"]:
            status = our_checks.get(name)
            if status is None:
                continue
            est = CI_TIMES.get(name, 3)
            total_est += est
            if status == "pass":
                icon = "\u2705"
                elapsed_est += est
            elif status == "FAIL":
                icon = "\u274c"
                elapsed_est += est
            elif status == "running":
                icon = "\u23f3"
                elapsed_est += est * 0.5  # assume halfway
            else:
                icon = "\u2b1c"
            print(f"    {icon} {name:20s} {status}")

        remaining = total_est - elapsed_est
        if remaining > 0 and step == 3:
            print(f"\n  EST. REMAINING: ~{remaining:.0f} min")
        elif step == 4:
            print(f"\n  \u2705 ALL CHECKS PASSED — ready to merge (make promote)")
    elif prs:
        # Show other open PRs
        print("  OPEN PRs:")
        for pr in prs:
            checks = pr.get("statusCheckRollup", [])
            passed = sum(1 for c in checks if c.get("conclusion") == "SUCCESS")
            total = len(checks)
            print(f"    PR #{pr['number']} [{pr['headRefName']}] {pr['title']}")
            if total:
                print(f"      checks: {passed}/{total} passed")
    else:
        print("  No open PRs")

    print()
    print(f"  LAST DEPLOY: {last_deploy}")
    print()

if __name__ == "__main__":
    main()
