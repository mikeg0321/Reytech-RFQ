#!/usr/bin/env bash
# railway_deploys.sh — list the 10 most recent Railway deploys for the linked service.
#
# Reads project/environment/service ids from ~/.railway/config.json (set by `railway link`).
# Output format: <id>  <status>  <created>  <commit>  <reason>

set -euo pipefail

CONFIG="$HOME/.railway/config.json"
if [ ! -f "$CONFIG" ]; then
  echo "ERROR: $CONFIG not found. Run 'railway login' and 'railway link' first." >&2
  exit 1
fi

# Force the Railway CLI to refresh its access token before we read it.
# Without this, the cached accessToken in config.json may have expired and the
# GraphQL API returns "Not Authorized" with no useful error.
railway whoami >/dev/null 2>&1 || {
  echo "ERROR: 'railway whoami' failed. Run 'railway login'." >&2
  exit 1
}

# Resolve project/service/env ids from the linked project.
# IMPORTANT: pipe through `tr -d '\r'` because Python's print() on Windows emits
# CRLF, and bash `read` would otherwise leave a trailing \r on SERVICE_ID. The
# Railway API maps the resulting "service not found" to a misleading "Not
# Authorized" error.
read PROJECT_ID ENV_ID SERVICE_ID < <(python -c "
import json, os
d = json.load(open(os.path.expanduser('~/.railway/config.json')))
projects = d.get('projects', {})
# Prefer a fully-linked project (has a service id) named humble-vitality.
match = None
for path, p in projects.items():
    if p.get('name') == 'humble-vitality' and p.get('service'):
        match = p
        break
if not match:
    # Fall back to any fully-linked project entry
    for p in projects.values():
        if p.get('service'):
            match = p
            break
if not match:
    raise SystemExit('No linked Railway project found. Run: railway link')
print(match['project'], match['environment'], match['service'])
" | tr -d '\r')

# Also strip CR from token in case of CRLF write
TOKEN_RAW=$(python -c "import json, os; print(json.load(open(os.path.expanduser('~/.railway/config.json')))['user']['accessToken'])")
TOKEN=$(printf '%s' "$TOKEN_RAW" | tr -d '\r')

# Build the GraphQL payload via Python (bullet-proof against shell quoting).
PAYLOAD_FILE=$(mktemp)
trap 'rm -f "$PAYLOAD_FILE"' EXIT
PROJECT_ID="$PROJECT_ID" ENV_ID="$ENV_ID" SERVICE_ID="$SERVICE_ID" python -c '
import json, os, sys
print(json.dumps({
    "query": "query Q($input: DeploymentListInput!) { deployments(input: $input, first: 10) { edges { node { id status createdAt meta } } } }",
    "variables": {"input": {
        "projectId": os.environ["PROJECT_ID"],
        "environmentId": os.environ["ENV_ID"],
        "serviceId": os.environ["SERVICE_ID"],
    }},
}))
' > "$PAYLOAD_FILE"

curl -sS -X POST https://backboard.railway.com/graphql/v2 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data-binary "@$PAYLOAD_FILE" \
  | python -c "
import json, sys
d = json.load(sys.stdin)
if d.get('errors'):
    print('GraphQL error:', file=sys.stderr)
    for e in d['errors']:
        print(' ', e.get('message'), file=sys.stderr)
    sys.exit(1)
edges = d.get('data', {}).get('deployments', {}).get('edges', [])
if not edges:
    print('No deployments found.')
    sys.exit(0)
print(f\"{'DEPLOY ID':38} {'STATUS':10} {'CREATED':20} {'COMMIT':10} REASON\")
for e in edges:
    n = e['node']
    meta = n.get('meta') or {}
    sha = (meta.get('commitHash') or '')[:8]
    reason = meta.get('reason') or ''
    print(f\"{n['id']:38} {n['status']:10} {n['createdAt'][:19]:20} {sha:10} {reason}\")
"
