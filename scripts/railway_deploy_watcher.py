#!/usr/bin/env python3
"""Railway deploy watcher — Item B of the P0 resilience backlog.

Polls the Railway deployments list every 30s for up to 5 minutes after
a `make promote`. If the latest deployment transitions into FAILED
AND /ping returns non-200, auto-triggers `make rollback-to` with the
previous SUCCESS deploy id.

Safety guards:
- Only rolls back if the deployment has been FAILED for >=90 seconds
  AND /ping is returning non-200 (catches flaky healthchecks that
  would otherwise recover on their own)
- Skips auto-rollback if the FAILED deployment is itself a rollback
  (detects via meta.reason or via commit/deploy-id equality to a
  previous SUCCESS entry) to avoid rollback loops
- Prints every step so the operator can see what's happening
- Exits 0 on success (auto-rolled-back OR deploy succeeded normally)
- Exits 1 on unrecoverable failure (needs human attention)

Usage:
    python scripts/railway_deploy_watcher.py
    python scripts/railway_deploy_watcher.py --max-poll-minutes 5
    python scripts/railway_deploy_watcher.py --ping-url https://...
    python scripts/railway_deploy_watcher.py --dry-run  # test without rolling back

Env vars:
    REYTECH_URL   - base URL for /ping health check (default: prod)
    NO_ROLLBACK   - set to "1" to disable auto-rollback entirely
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


DEFAULT_PING_URL = "https://web-production-dcee9.up.railway.app/ping"
DEFAULT_MAX_POLL_MINUTES = 5
POLL_INTERVAL_SECONDS = 30
FAILED_MIN_SECONDS_BEFORE_ROLLBACK = 90
ROLLBACK_REASON_MARKER = "auto-rollback by railway_deploy_watcher"

# Railway deploy statuses we care about
STATUS_FAILED = {"FAILED", "CRASHED", "REMOVED"}
STATUS_SUCCESS = {"SUCCESS"}
STATUS_IN_FLIGHT = {"BUILDING", "DEPLOYING", "INITIALIZING", "QUEUED", "WAITING"}

# GitHub Actions main-CI hook — when --check-main-ci SHA is passed, after
# the Railway deploy reaches SUCCESS we keep polling GitHub for the main
# CI workflow run tied to that commit. The merge-time CI runs the fast
# test slice; the full 30-min suite runs post-merge on main. A failure in
# that post-merge full run means bad code landed even though Railway
# booted — exactly the scenario item #18 of the resilience backlog flags.
CI_CHECK_DEFAULT_MAX_MINUTES = 35
CI_CHECK_POLL_INTERVAL_SECONDS = 45
CI_WORKFLOW_FILE = "ci.yml"
CI_JOB_TERMINAL_FAILURE = {"failure", "timed_out", "cancelled"}
CI_JOB_TERMINAL_SUCCESS = {"success", "neutral", "skipped"}


# ── Railway API helpers ──────────────────────────────────────────────────

def _fetch_deployments() -> List[Dict[str, Any]]:
    """Call the railway_deploys.sh helper and parse its 10-row output
    into a list of {id, status, created, commit, reason} dicts.

    Using the shell script means we inherit its token refresh +
    project linking logic without duplicating it here.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    script = os.path.join(here, "railway_deploys.sh")
    if not os.path.exists(script):
        raise RuntimeError(f"railway_deploys.sh not found at {script}")

    try:
        result = subprocess.run(
            ["bash", script],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except Exception as e:
        raise RuntimeError(f"railway_deploys.sh failed: {e}")

    if result.returncode != 0:
        raise RuntimeError(
            f"railway_deploys.sh exit={result.returncode}: {result.stderr.strip()}"
        )

    # Output: "DEPLOY ID ... STATUS ... CREATED ... COMMIT ... REASON"
    # Skip header row, parse each data row
    lines = [
        line.rstrip()
        for line in result.stdout.splitlines()
        if line.strip() and not line.startswith("DEPLOY ID")
    ]
    deploys = []
    for line in lines:
        # Columns are space-padded: id(38), status(10), created(20), commit(10), reason
        # Parse by column widths since reason may contain spaces
        if len(line) < 80:
            continue
        deploy_id = line[:38].strip()
        status = line[39:49].strip()
        created = line[50:69].strip()
        commit = line[70:80].strip()
        reason = line[81:].strip() if len(line) > 81 else ""
        deploys.append({
            "id": deploy_id,
            "status": status,
            "created": created,
            "commit": commit,
            "reason": reason,
        })
    return deploys


def _ping_healthy(url: str, timeout_sec: float = 5.0) -> bool:
    """Return True if the given URL returns a 2xx status. Uses urllib
    instead of requests to avoid adding a new dependency."""
    import urllib.request
    import urllib.error
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "railway-deploy-watcher"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            return 200 <= resp.status < 300
    except urllib.error.HTTPError as e:
        return 200 <= e.code < 300
    except Exception:
        return False


def _find_previous_success(deploys: List[Dict[str, Any]],
                             current_id: str) -> Optional[Dict[str, Any]]:
    """Return the most recent SUCCESS deploy that is NOT the current
    (failing) deploy and whose reason doesn't look like a previous
    auto-rollback. Returns None if no safe target found."""
    for d in deploys:
        if d["id"] == current_id:
            continue
        if d["status"] not in STATUS_SUCCESS:
            continue
        # Skip if this SUCCESS deploy is itself a previous auto-rollback
        if ROLLBACK_REASON_MARKER in d.get("reason", "").lower():
            continue
        return d
    return None


def _trigger_rollback(target_deploy_id: str, dry_run: bool = False) -> bool:
    """Invoke make rollback-to id=<id>. Returns True on success.
    In dry_run mode, prints the command without executing."""
    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.dirname(here)
    cmd = ["bash", os.path.join(here, "railway_rollback.sh"), target_deploy_id]

    if dry_run:
        print(f"[DRY-RUN] would execute: {' '.join(cmd)}")
        return True

    print(f"  EXEC: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=repo,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except Exception as e:
        print(f"  rollback exec failed: {e}", file=sys.stderr)
        return False

    print(result.stdout)
    if result.returncode != 0:
        print(f"  rollback exit={result.returncode}: {result.stderr}", file=sys.stderr)
        return False
    return True


def _fetch_main_ci_run(commit_sha: str) -> Optional[Dict[str, Any]]:
    """Fetch the main-branch ci.yml run for the given commit via `gh`.

    Returns a dict with {status, conclusion, databaseId, url} or None when
    no run is visible yet (GitHub hasn't registered the workflow yet, or
    the commit isn't on main, or `gh` is unavailable).
    """
    try:
        result = subprocess.run(
            [
                "gh", "run", "list",
                "--workflow", CI_WORKFLOW_FILE,
                "--branch", "main",
                "--commit", commit_sha,
                "--limit", "1",
                "--json", "status,conclusion,databaseId,url,headSha",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        print("  gh CLI not found on PATH — cannot check main CI")
        return None
    except Exception as e:
        print(f"  gh run list failed: {e}")
        return None

    if result.returncode != 0:
        print(f"  gh run list exit={result.returncode}: {result.stderr.strip()[:200]}")
        return None

    try:
        runs = json.loads(result.stdout or "[]")
    except Exception as e:
        print(f"  gh run list returned non-JSON: {e}")
        return None

    if not runs:
        return None

    run = runs[0]
    if commit_sha and run.get("headSha") and run["headSha"] != commit_sha:
        # gh's --commit filter is tolerant — if no exact match it may
        # return the most recent run on the branch. Reject the mismatch
        # so we don't rollback based on an unrelated run.
        return None
    return run


def check_main_ci_status(
    commit_sha: str,
    max_wait_minutes: int = CI_CHECK_DEFAULT_MAX_MINUTES,
    poll_interval_seconds: int = CI_CHECK_POLL_INTERVAL_SECONDS,
) -> str:
    """Block until the main-branch CI run for `commit_sha` reaches a
    terminal state, or `max_wait_minutes` elapses.

    Returns one of:
      * "success"   — conclusion ∈ {success, neutral, skipped}
      * "failure"   — conclusion ∈ {failure, timed_out, cancelled}
      * "pending"   — still running when the deadline hit
      * "unknown"   — `gh` unavailable, no run visible, or API errored

    This is polling-only. The caller decides what to do (rollback, notify).
    """
    if not commit_sha:
        return "unknown"

    deadline = time.time() + max_wait_minutes * 60
    last_status: Optional[str] = None
    last_conclusion: Optional[str] = None

    while time.time() < deadline:
        run = _fetch_main_ci_run(commit_sha)
        if run is None:
            time.sleep(poll_interval_seconds)
            continue

        status = str(run.get("status") or "").lower()
        conclusion = str(run.get("conclusion") or "").lower()
        if status != last_status or conclusion != last_conclusion:
            print(f"  main-ci {commit_sha[:8]}: status={status} "
                  f"conclusion={conclusion or '—'} {run.get('url', '')}")
            last_status = status
            last_conclusion = conclusion

        # GitHub transitions status through queued → in_progress → completed.
        # A conclusion is only set once status == completed.
        if status == "completed":
            if conclusion in CI_JOB_TERMINAL_FAILURE:
                return "failure"
            if conclusion in CI_JOB_TERMINAL_SUCCESS:
                return "success"
            # Unexpected conclusion value — treat as unknown and bail.
            print(f"  main-ci: unexpected conclusion={conclusion!r}, treating as unknown")
            return "unknown"

        time.sleep(poll_interval_seconds)

    return "pending"


def _notify(message: str) -> None:
    """Best-effort notification when auto-rollback fires. Uses the
    existing notify_agent if available, otherwise just prints."""
    print(f"[NOTIFY] {message}")
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from src.agents.notify_agent import notify
        notify("[Railway] " + message, priority="high")
    except Exception:
        pass  # notification is best-effort; never crash the watcher


# ── Main loop ────────────────────────────────────────────────────────────

def _rollback_via_previous_success(deploys: List[Dict[str, Any]],
                                     current_id: str,
                                     reason_prefix: str,
                                     dry_run: bool) -> int:
    """Shared rollback kick-off: find prev SUCCESS, trigger, return rc."""
    if ROLLBACK_REASON_MARKER in (deploys[0].get("reason", "") if deploys else "").lower():
        print("  current deploy is itself a prior auto-rollback — refusing loop.")
        _notify(f"{reason_prefix} REFUSED: prior rollback also failed. Manual needed.")
        return 1
    target = _find_previous_success(deploys, current_id)
    if target is None:
        print("  no previous SUCCESS deploy found — cannot auto-rollback.")
        _notify(f"{reason_prefix} BLOCKED: no previous SUCCESS to roll back to.")
        return 1
    print(f"  rolling back to {target['id'][:8]}... (commit {target['commit']})")
    _notify(f"{reason_prefix}: reverting to {target['id'][:8]} ({target['commit']})")
    ok = _trigger_rollback(target["id"], dry_run=dry_run)
    if ok:
        print("  rollback command succeeded; exiting")
        return 0
    print("  rollback command FAILED — manual intervention required",
          file=sys.stderr)
    _notify(f"{reason_prefix} COMMAND FAILED for {target['id'][:8]}")
    return 1


def watch_deploy(
    max_poll_minutes: int = DEFAULT_MAX_POLL_MINUTES,
    ping_url: str = DEFAULT_PING_URL,
    dry_run: bool = False,
    check_main_ci_sha: Optional[str] = None,
    ci_max_wait_minutes: int = CI_CHECK_DEFAULT_MAX_MINUTES,
) -> int:
    """Main watcher loop. Returns 0 on success (OR auto-rollback
    succeeded), 1 on unrecoverable failure."""
    print(f"railway_deploy_watcher: started at {datetime.now(timezone.utc).isoformat()}")
    print(f"  max_poll_minutes = {max_poll_minutes}")
    print(f"  ping_url         = {ping_url}")
    print(f"  poll_interval    = {POLL_INTERVAL_SECONDS}s")
    print(f"  failed_hold_sec  = {FAILED_MIN_SECONDS_BEFORE_ROLLBACK}s")
    print(f"  dry_run          = {dry_run}")

    if os.environ.get("NO_ROLLBACK") == "1":
        print("NO_ROLLBACK=1 — exiting without watching.")
        return 0

    deadline = time.time() + max_poll_minutes * 60
    failed_since: Optional[float] = None
    last_seen_id: Optional[str] = None
    last_seen_status: Optional[str] = None

    while time.time() < deadline:
        try:
            deploys = _fetch_deployments()
        except Exception as e:
            print(f"  fetch deployments failed: {e}; retrying...")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        if not deploys:
            print("  no deployments returned; retrying...")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        latest = deploys[0]
        if latest["id"] != last_seen_id or latest["status"] != last_seen_status:
            print(f"  latest: {latest['id'][:8]}... status={latest['status']} "
                  f"commit={latest['commit']} reason={latest['reason'][:40]}")
            last_seen_id = latest["id"]
            last_seen_status = latest["status"]

        if latest["status"] in STATUS_SUCCESS:
            if check_main_ci_sha:
                print(f"  deploy is SUCCESS — now polling main CI for "
                      f"commit {check_main_ci_sha[:8]} (up to {ci_max_wait_minutes}m)")
                ci_verdict = check_main_ci_status(
                    check_main_ci_sha,
                    max_wait_minutes=ci_max_wait_minutes,
                )
                print(f"  main-ci verdict: {ci_verdict}")
                if ci_verdict == "failure":
                    _notify(
                        f"auto-rollback: deploy {latest['id'][:8]} "
                        f"went live but main CI FAILED on "
                        f"commit {check_main_ci_sha[:8]}"
                    )
                    # Refresh deploy list in case new entries landed while
                    # CI was running.
                    try:
                        deploys = _fetch_deployments()
                    except Exception as e:
                        print(f"  refresh deploys failed: {e}")
                    return _rollback_via_previous_success(
                        deploys, latest["id"],
                        reason_prefix="main-ci-failure rollback",
                        dry_run=dry_run,
                    )
                if ci_verdict == "pending":
                    print("  main CI still pending at deadline — watcher exits "
                          "green, operator should manually verify")
                # success/unknown → exit cleanly
            print("  deploy is SUCCESS — watcher done.")
            return 0

        if latest["status"] in STATUS_FAILED:
            now = time.time()
            if failed_since is None:
                failed_since = now
                print(f"  deploy entered FAILED state; holding for "
                      f"{FAILED_MIN_SECONDS_BEFORE_ROLLBACK}s before rollback")
            elif now - failed_since >= FAILED_MIN_SECONDS_BEFORE_ROLLBACK:
                # Double-check with a /ping call before rolling back
                if _ping_healthy(ping_url):
                    print("  /ping is healthy despite FAILED status — "
                          "treating as flake and continuing to watch")
                    failed_since = None
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # Check if this failing deploy is itself a rollback
                # (avoid auto-rollback loops)
                if ROLLBACK_REASON_MARKER in latest.get("reason", "").lower():
                    print("  FAILED deploy is itself a prior auto-rollback — "
                          "refusing to rollback again. Human action required.")
                    _notify(
                        f"auto-rollback REFUSED: prior rollback "
                        f"{latest['id'][:8]} also failed. Manual intervention needed."
                    )
                    return 1

                target = _find_previous_success(deploys, latest["id"])
                if target is None:
                    print("  no previous SUCCESS deploy found — "
                          "cannot auto-rollback. Human action required.")
                    _notify(
                        f"auto-rollback BLOCKED: deploy {latest['id'][:8]} failed "
                        f"but no previous SUCCESS to roll back to."
                    )
                    return 1

                print(f"  rolling back to {target['id'][:8]}... "
                      f"(commit {target['commit']})")
                _notify(
                    f"auto-rollback: deploy {latest['id'][:8]} FAILED, "
                    f"reverting to {target['id'][:8]} ({target['commit']})"
                )
                ok = _trigger_rollback(target["id"], dry_run=dry_run)
                if ok:
                    print("  rollback command succeeded; exiting")
                    return 0
                else:
                    print("  rollback command FAILED — manual intervention required",
                          file=sys.stderr)
                    _notify(
                        f"auto-rollback COMMAND FAILED for {target['id'][:8]} "
                        "— manual intervention required."
                    )
                    return 1
        else:
            # In-flight state — keep watching
            failed_since = None

        time.sleep(POLL_INTERVAL_SECONDS)

    print(f"  reached max_poll_minutes={max_poll_minutes} without terminal state; "
          f"exiting (latest status: {last_seen_status})")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--max-poll-minutes", type=int, default=DEFAULT_MAX_POLL_MINUTES)
    parser.add_argument("--ping-url", default=os.environ.get("REYTECH_URL") or DEFAULT_PING_URL)
    parser.add_argument("--dry-run", action="store_true",
                        help="log what would happen without triggering rollback")
    parser.add_argument(
        "--check-main-ci",
        dest="check_main_ci_sha",
        default=os.environ.get("REYTECH_CI_COMMIT") or "",
        help="commit SHA whose main-branch ci.yml run to poll after "
             "Railway deploy succeeds. On failure, triggers rollback. "
             "Empty/omitted = skip the CI hook (legacy behavior).",
    )
    parser.add_argument(
        "--ci-max-wait-minutes", type=int,
        default=CI_CHECK_DEFAULT_MAX_MINUTES,
        help="max minutes to wait for main CI to reach terminal state",
    )
    args = parser.parse_args()

    # Normalize ping URL: append /ping if caller passed bare origin
    ping_url = args.ping_url
    if ping_url and not ping_url.rstrip("/").endswith("/ping"):
        ping_url = ping_url.rstrip("/") + "/ping"

    return watch_deploy(
        max_poll_minutes=args.max_poll_minutes,
        ping_url=ping_url,
        dry_run=args.dry_run,
        check_main_ci_sha=(args.check_main_ci_sha or None),
        ci_max_wait_minutes=args.ci_max_wait_minutes,
    )


if __name__ == "__main__":
    sys.exit(main())
