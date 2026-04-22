#!/usr/bin/env bash
# Poll the deployed /version endpoint until it reports the expected commit SHA.
#
# Replaces the old hardcoded `sleep 90` in `make promote`: if Railway finishes
# rolling the new replica in 30s we stop waiting in 30s, and if it takes 5min
# we don't blindly run smoke tests against the old replica (which would falsely
# pass because the previous image is still healthy during build).
#
# Usage:
#   scripts/wait_for_deploy.sh <PROD_URL> <EXPECTED_SHA> [MAX_WAIT_SECONDS]
#
# Exit codes:
#   0 — /version reported EXPECTED_SHA within MAX_WAIT_SECONDS (new deploy live)
#   1 — timed out; caller should fall back to conservative behavior (run smoke
#       anyway — smoke hits authed endpoints that will catch a broken deploy)
#
# Fallback philosophy: this script NEVER blocks the promote flow on its own.
# If Railway API / the /version endpoint is flaky, we print a warning and exit
# non-zero; the Makefile treats that as "proceed with smoke" rather than abort.
# Smoke + the background railway_deploy_watcher.py are the real gates.

set -u

PROD_URL="${1:-}"
EXPECTED="${2:-}"
MAX_WAIT="${3:-420}"   # 7 min — covers normal 5-8 min deploys with headroom

if [ -z "$PROD_URL" ] || [ -z "$EXPECTED" ]; then
    echo "usage: wait_for_deploy.sh <PROD_URL> <EXPECTED_SHA> [MAX_WAIT_SECONDS]" >&2
    exit 2
fi

SHORT=$(echo "$EXPECTED" | cut -c1-7)
echo "Waiting for Railway to swap to commit ${SHORT} (max ${MAX_WAIT}s)..."

start=$(date +%s)
last_seen=""
while true; do
    now=$(date +%s)
    elapsed=$((now - start))
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        echo "  timeout after ${elapsed}s (last saw: ${last_seen:-none})" >&2
        echo "  proceeding to smoke test — smoke + deploy watcher will catch a bad deploy" >&2
        exit 1
    fi

    # -m 5: per-request timeout so a hung connection does not stall the poll.
    # -s: silent; -f: fail on 4xx/5xx so curl returns non-zero instead of html.
    body=$(curl -sf -m 5 "${PROD_URL}/version" 2>/dev/null || true)
    # Extract "commit":"<sha>" without jq dependency. Tolerate optional
    # whitespace after the colon (Flask `jsonify` is compact, but other
    # servers/proxies may reformat).
    remote=$(printf '%s' "$body" | sed -n 's/.*"commit"[[:space:]]*:[[:space:]]*"\([a-f0-9]*\)".*/\1/p')

    if [ -n "$remote" ] && [ "$remote" != "$last_seen" ]; then
        last_seen="$remote"
    fi

    if [ "$remote" = "$EXPECTED" ]; then
        echo "  new deploy live after ${elapsed}s (commit ${SHORT})"
        exit 0
    fi

    sleep 5
done
