#!/usr/bin/env bash
# railway_rollback.sh — roll back to a specific Railway deployment without rebuilding.
#
# Usage: ./scripts/railway_rollback.sh <deployment-id>
#
# Reads the Railway CLI access token from ~/.railway/config.json and calls the
# Railway GraphQL `deploymentRedeploy` mutation with usePreviousImageTag=true.
# This reuses the cached Docker image, so the redeploy completes in ~60s instead
# of the ~5min full rebuild.
#
# Find a deployment id by running `make deploys`.

set -euo pipefail

DEPLOY_ID="${1:-}"
if [ -z "$DEPLOY_ID" ]; then
  echo "Usage: $0 <deployment-id>" >&2
  exit 1
fi

CONFIG="$HOME/.railway/config.json"
if [ ! -f "$CONFIG" ]; then
  echo "ERROR: $CONFIG not found. Run 'railway login' first." >&2
  exit 1
fi

# Force the Railway CLI to refresh its access token before we read it.
railway whoami >/dev/null 2>&1 || {
  echo "ERROR: 'railway whoami' failed. Run 'railway login'." >&2
  exit 1
}

TOKEN=$(python -c "import json,os;print(json.load(open(os.path.expanduser('~/.railway/config.json')))['user']['accessToken'])" | tr -d '\r')
if [ -z "$TOKEN" ] || [ "${#TOKEN}" -lt 20 ]; then
  echo "ERROR: failed to read accessToken from $CONFIG" >&2
  exit 1
fi
DEPLOY_ID=$(printf '%s' "$DEPLOY_ID" | tr -d '\r')

echo "Rolling back to deployment $DEPLOY_ID (no rebuild)..."

RESPONSE=$(curl -sS -X POST https://backboard.railway.com/graphql/v2 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"mutation { deploymentRedeploy(id: \\\"$DEPLOY_ID\\\", usePreviousImageTag: true) { id status } }\"}")

echo "$RESPONSE" | python -c "
import json, sys
d = json.load(sys.stdin)
if d.get('errors'):
    print('FAILED:')
    for e in d['errors']:
        print(' ', e.get('message'))
    sys.exit(1)
node = d.get('data', {}).get('deploymentRedeploy') or {}
print(f\"OK — new deployment {node.get('id')} status={node.get('status')}\")
print('Verify with: make smoke (after ~60s)')
"
