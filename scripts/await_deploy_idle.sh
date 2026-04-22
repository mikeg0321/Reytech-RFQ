#!/usr/bin/env bash
# await_deploy_idle.sh — block until no Railway deploy is in-flight.
#
# Problem: when two PRs auto-merge within Railway's build window (~6-8 min), the
# second merge's build preempts the first. The first PR's code still ships (it
# becomes part of main), but any author watching their specific commit sees a
# multi-minute wall-clock wait while their build gets REMOVED mid-flight.
#
# Fix: opt-in serialization. Before pushing a new PR, ask this script to wait
# until Railway is idle. The result is each PR's build completes cleanly.
#
# Usage:
#   scripts/await_deploy_idle.sh [MAX_WAIT_SECONDS]
#
# Exit codes:
#   0 — Railway is idle (or timeout / script error → fail-safe; caller proceeds)
#       A non-failing exit means "safe to push now," including the timeout
#       case. The philosophy is never to block the release pipeline on an
#       introspection tool — the existing smoke + deploy watcher are the real
#       gates.
#   never 1 — we don't want this script to be a new outage vector.
#
# Poll cadence: 20s. Railway's API is cached at the edge so more aggressive
# polling just wastes requests without getting fresher data.

set -u
MAX_WAIT="${1:-600}"   # 10 min default — longer than a normal build (~7 min)
                       # but shorter than the 15-min cache-miss ceiling.
POLL_EVERY=20

# ACTIVE = any status that will eventually produce a swap. Railway emits these
# statuses for in-flight work:
#   INITIALIZING  — queued, about to build
#   BUILDING      — running the build
#   DEPLOYING     — image built, rolling to replicas
# REMOVED/SUCCESS/FAILED/CRASHED are terminal and safe to push behind.
ACTIVE_REGEX='INITIALIZING|BUILDING|DEPLOYING'

# Locate railway_deploys.sh relative to this script so it works from any CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOYS_SH="$SCRIPT_DIR/railway_deploys.sh"

if [ ! -x "$DEPLOYS_SH" ]; then
    # Fail-safe: if the lister is missing or not executable we can't introspect,
    # so just don't block. Print a note so the user notices over time.
    echo "await_deploy_idle: $DEPLOYS_SH not found/executable — skipping wait" >&2
    exit 0
fi

start=$(date +%s)
first=1
while true; do
    # Capture the lister output once per loop. Silence stderr so a transient
    # auth blip (e.g. token refresh race) doesn't print a scary error on every
    # iteration; the loop will recover next tick.
    raw=$("$DEPLOYS_SH" 2>/dev/null || true)
    # Skip the header row. Status is column 2.
    active_count=$(printf '%s\n' "$raw" \
        | awk 'NR > 1 {print $2}' \
        | grep -cE "^($ACTIVE_REGEX)$" || true)

    elapsed=$(( $(date +%s) - start ))

    if [ "$active_count" -eq 0 ]; then
        if [ "$first" -eq 0 ]; then
            echo "await_deploy_idle: Railway idle after ${elapsed}s, safe to push"
        fi
        exit 0
    fi

    if [ "$first" -eq 1 ]; then
        echo "await_deploy_idle: ${active_count} deploy(s) in flight, waiting up to ${MAX_WAIT}s..."
        first=0
    fi

    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        echo "await_deploy_idle: timeout after ${elapsed}s with ${active_count} still active — proceeding anyway" >&2
        echo "  (your push will preempt the in-flight build; its code still reaches main so nothing is lost)" >&2
        exit 0
    fi

    sleep "$POLL_EVERY"
done
