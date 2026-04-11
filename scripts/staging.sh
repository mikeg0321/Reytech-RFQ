#!/bin/bash
# staging.sh — Railway staging environment management.
#
# Creates/manages a staging service in the existing Railway project.
# Staging mirrors production config but uses an isolated volume.
#
# Usage:
#   ./scripts/staging.sh setup      # One-time: create staging service
#   ./scripts/staging.sh deploy     # Deploy current branch to staging
#   ./scripts/staging.sh smoke      # Run smoke tests against staging
#   ./scripts/staging.sh promote    # After smoke passes, merge PR to prod
#   ./scripts/staging.sh logs       # Tail staging logs
#   ./scripts/staging.sh status     # Show staging state
#
# Requires: railway CLI (npm i -g @railway/cli), authenticated

set -e

PROJECT="${RAILWAY_PROJECT:-humble-vitality}"
STAGING_SERVICE="web-staging"
BRANCH=$(git branch --show-current)

case "${1:-help}" in

  setup)
    echo "Creating staging environment..."
    railway environment create staging 2>/dev/null || echo "  (staging env may already exist)"

    echo "Linking to staging environment..."
    railway link --environment staging

    echo ""
    echo "Staging environment created."
    echo ""
    echo "Next steps:"
    echo "  1. In Railway dashboard -> project -> staging environment"
    echo "  2. Add the same env vars as production:"
    echo "     SECRET_KEY, DASH_USER, DASH_PASS, ANTHROPIC_API_KEY"
    echo "  3. Set FLASK_ENV=staging"
    echo "  4. Attach a NEW volume (don't share prod volume)"
    echo "  5. Run: ./scripts/staging.sh deploy"
    ;;

  deploy)
    # Verify railway CLI is available
    if ! command -v railway &>/dev/null; then
        echo "ERROR: railway CLI not found."
        echo "Install: npm i -g @railway/cli"
        echo "Auth:    railway login"
        exit 1
    fi

    echo "Deploying branch '$BRANCH' to staging..."

    # Run tests first
    echo "Running tests before staging deploy..."
    SECRET_KEY=test DASH_USER=test DASH_PASS=test FLASK_ENV=testing \
    python -m pytest tests/test_ams704_helpers.py \
        tests/test_template_registry.py \
        tests/test_pc_generation.py \
        tests/test_rfq_generation.py \
        -x -q --tb=short

    echo ""
    echo "Tests passed. Deploying to staging..."
    railway up --environment staging --detach
    echo ""
    echo "Staging deploy triggered."
    echo "Wait ~60s, then run: ./scripts/staging.sh smoke"
    ;;

  smoke)
    STAGING_URL="${STAGING_URL:-}"
    if [ -z "$STAGING_URL" ]; then
        echo "ERROR: STAGING_URL not set."
        echo "Find it in Railway dashboard -> staging -> Networking -> Public URL"
        echo "Then: STAGING_URL=https://your-staging.up.railway.app ./scripts/staging.sh smoke"
        exit 1
    fi
    echo "Running smoke tests against $STAGING_URL..."
    REYTECH_URL="$STAGING_URL" python tests/smoke_test.py
    RESULT=$?
    if [ $RESULT -eq 0 ]; then
        echo ""
        echo "Staging smoke tests PASSED."
        echo ""
        echo "Ready to promote. Options:"
        echo "  make promote                    # Merge PR + smoke test prod"
        echo "  ./scripts/staging.sh promote    # Legacy: push main directly"
    else
        echo ""
        echo "Staging smoke tests FAILED. Do NOT promote."
        echo "Check: ./scripts/staging.sh logs"
    fi
    exit $RESULT
    ;;

  promote)
    echo "Promoting to production..."
    echo ""

    if [ "$BRANCH" != "main" ]; then
        # Try PR-based promotion first
        echo "Checking for open PR..."
        PR_URL=$(gh pr view --json url -q '.url' 2>/dev/null || echo "")

        if [ -n "$PR_URL" ]; then
            echo "Found PR: $PR_URL"
            echo "Merging via PR (preferred)..."
            gh pr merge --squash --delete-branch
            git checkout main
            git pull origin main
        else
            echo "No PR found. Merging branch to main locally..."
            git checkout main
            git pull origin main
            git merge "$BRANCH" --no-ff -m "Promote $BRANCH to production"
            git push origin main
            git branch -d "$BRANCH" 2>/dev/null || true
        fi
    else
        echo "Already on main. Pushing to trigger Railway deploy..."
        git push origin main
    fi

    echo ""
    echo "Pushed to main. Railway auto-deploying..."
    echo ""
    echo "Run smoke test in ~90s:"
    echo "  make smoke"
    ;;

  logs)
    echo "Tailing staging logs..."
    railway logs --environment staging
    ;;

  status)
    echo "Staging Status"
    echo "=============="
    echo "Branch: $BRANCH"
    echo "Commit: $(git log -1 --format='%h %s')"
    echo ""
    echo "Staging URL: ${STAGING_URL:-'(not set — check Railway dashboard)'}"
    echo ""
    railway status --environment staging 2>/dev/null || echo "(railway CLI not connected to staging)"
    ;;

  teardown)
    echo "Removing staging environment..."
    echo "WARNING: This deletes the staging service and its volume."
    read -p "Type 'DELETE' to confirm: " confirm
    if [ "$confirm" = "DELETE" ]; then
        railway environment delete staging 2>/dev/null || echo "  (staging env not found or already deleted)"
        echo "Staging environment removed."
    else
        echo "Cancelled."
    fi
    ;;

  *)
    echo "Usage: ./scripts/staging.sh {setup|deploy|smoke|promote|logs|status|teardown}"
    echo ""
    echo "  setup    - Create staging environment (one-time)"
    echo "  deploy   - Deploy current branch to staging (runs tests first)"
    echo "  smoke    - Run smoke tests against staging"
    echo "  promote  - Merge to main (prod) after staging passes"
    echo "  logs     - Tail staging logs"
    echo "  status   - Show staging state"
    echo "  teardown - Delete staging environment"
    ;;
esac
