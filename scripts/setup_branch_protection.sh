#!/bin/bash
# setup_branch_protection.sh — Configure GitHub branch protection for main.
#
# NOTE: Branch protection requires GitHub Pro for private repos.
# For free private repos, the pre-push hook + make ship enforce the same gates locally.
#
# If you upgrade to Pro or make the repo public, run this script once.
#
# Usage: ./scripts/setup_branch_protection.sh
# Requires: gh CLI authenticated (gh auth login)

set -e

REPO=$(gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null)
if [ -z "$REPO" ]; then
    echo "ERROR: Not in a GitHub repo or gh CLI not authenticated."
    echo "Run: gh auth login"
    exit 1
fi

IS_PRIVATE=$(gh repo view --json isPrivate -q '.isPrivate' 2>/dev/null)

echo "Configuring branch protection for $REPO/main..."
echo ""

if [ "$IS_PRIVATE" = "true" ]; then
    echo "NOTE: $REPO is a PRIVATE repo."
    echo "Branch protection requires GitHub Pro ($4/mo) for private repos."
    echo ""
    echo "Attempting anyway (will fail on free plan)..."
    echo ""
fi

gh api repos/$REPO/branches/main/protection \
  --method PUT \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["static-checks", "build-checks", "tests"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 0,
    "dismiss_stale_reviews": true
  },
  "restrictions": null
}
EOF

RESULT=$?

if [ $RESULT -eq 0 ]; then
    echo ""
    echo "Branch protection configured:"
    echo "  - PRs required to merge into main (no direct push)"
    echo "  - CI jobs must pass: static-checks, build-checks, tests"
    echo "  - Stale reviews dismissed on new pushes"
else
    echo ""
    echo "Branch protection API failed (expected on free private repos)."
    echo ""
    echo "YOUR LOCAL ENFORCEMENT IS STILL ACTIVE:"
    echo "  - Pre-push hook runs 8 critical test files before every push"
    echo "  - 'make ship' blocks pushes from main branch"
    echo "  - 'make ship' runs tests + pre-deploy checks before pushing"
    echo "  - CI runs on every push/PR (tests must pass)"
    echo ""
    echo "To enable server-side protection:"
    echo "  Option A: Upgrade to GitHub Pro ($4/mo)"
    echo "  Option B: Make the repo public"
    echo "  Then re-run this script."
fi
