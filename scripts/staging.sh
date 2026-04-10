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
#   ./scripts/staging.sh promote    # After smoke passes, deploy to prod
#   ./scripts/staging.sh logs       # Tail staging logs
#
# Requires: railway CLI (npm i -g @railway/cli), authenticated

set -e

PROJECT="${RAILWAY_PROJECT:-humble-vitality}"
STAGING_SERVICE="web-staging"
PROD_SERVICE="web"

case "${1:-help}" in

  setup)
    echo "▶ Creating staging environment..."
    # Create a staging environment in the Railway project
    railway environment create staging 2>/dev/null || echo "  (staging env may already exist)"

    echo "▶ Link to staging environment..."
    railway link --environment staging

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Staging environment created."
    echo ""
    echo "  Next steps:"
    echo "  1. In Railway dashboard → project → staging environment"
    echo "  2. Add the same env vars as production:"
    echo "     SECRET_KEY, DASH_USER, DASH_PASS, ANTHROPIC_API_KEY"
    echo "  3. Set FLASK_ENV=staging"
    echo "  4. Attach a NEW volume (don't share prod volume)"
    echo "  5. Run: ./scripts/staging.sh deploy"
    echo "═══════════════════════════════════════════════════"
    ;;

  deploy)
    echo "▶ Deploying to staging..."
    # Push current branch to staging
    railway up --environment staging --detach
    echo "✅ Staging deploy triggered"
    echo "   Wait ~60s, then run: ./scripts/staging.sh smoke"
    ;;

  smoke)
    STAGING_URL="${STAGING_URL:-}"
    if [ -z "$STAGING_URL" ]; then
        echo "❌ STAGING_URL not set."
        echo "   Find it in Railway dashboard → staging → Networking → Public URL"
        echo "   Then: STAGING_URL=https://your-staging.up.railway.app ./scripts/staging.sh smoke"
        exit 1
    fi
    echo "▶ Running smoke tests against $STAGING_URL..."
    REYTECH_URL="$STAGING_URL" python tests/smoke_test.py
    RESULT=$?
    if [ $RESULT -eq 0 ]; then
        echo "✅ Staging smoke tests passed — safe to promote"
        echo "   Run: ./scripts/staging.sh promote"
    else
        echo "❌ Staging smoke tests FAILED — do NOT promote"
        echo "   Check staging logs: ./scripts/staging.sh logs"
    fi
    exit $RESULT
    ;;

  promote)
    echo "▶ Promoting staging to production..."
    echo "   This pushes to main, which auto-deploys to prod."
    read -p "   Confirm? (y/N) " confirm
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        git push origin main
        echo "✅ Pushed to main → Railway auto-deploys to production"
        echo ""
        echo "   After ~60s, verify with:"
        echo "   python tests/smoke_test.py"
    else
        echo "   Cancelled."
    fi
    ;;

  logs)
    echo "▶ Tailing staging logs..."
    railway logs --environment staging
    ;;

  *)
    echo "Usage: ./scripts/staging.sh {setup|deploy|smoke|promote|logs}"
    echo ""
    echo "  setup   — Create staging environment (one-time)"
    echo "  deploy  — Deploy current code to staging"
    echo "  smoke   — Run smoke tests against staging"
    echo "  promote — Push to main (prod) after staging passes"
    echo "  logs    — Tail staging logs"
    ;;
esac
