#!/bin/bash
# post_deploy_verify.sh — Post-deploy smoke test with auto-rollback.
#
# Run after Railway deploys. If smoke tests fail, automatically reverts
# the last commit and pushes, triggering a rollback deploy.
#
# Usage:
#   ./scripts/post_deploy_verify.sh                    # Test production
#   ./scripts/post_deploy_verify.sh --url=https://...  # Test custom URL
#   ./scripts/post_deploy_verify.sh --no-rollback      # Test only, don't revert
#
# Environment:
#   REYTECH_URL — Production URL (default: Railway app URL)

set -e

PROD_URL="${REYTECH_URL:-https://web-production-dcee9.up.railway.app}"
AUTO_ROLLBACK=true
WAIT_SECONDS=90
COMMIT=$(git log -1 --format="%h %s")
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Parse args
for arg in "$@"; do
    case $arg in
        --url=*) PROD_URL="${arg#*=}" ;;
        --no-rollback) AUTO_ROLLBACK=false ;;
        --wait=*) WAIT_SECONDS="${arg#*=}" ;;
    esac
done

log() {
    echo "[$(date '+%H:%M:%S')] $1"
}

echo ""
echo "Post-Deploy Verification"
echo "========================"
echo "Target:  $PROD_URL"
echo "Commit:  $COMMIT"
echo "Time:    $TIMESTAMP"
echo "Auto-rollback: $AUTO_ROLLBACK"
echo ""

# Step 1: Wait for Railway to finish deploying
log "Waiting ${WAIT_SECONDS}s for Railway deploy to complete..."
sleep "$WAIT_SECONDS"

# Step 2: Basic health check (is the app responding?)
log "Health check..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$PROD_URL/" 2>/dev/null || echo "000")

if [ "$HTTP_CODE" = "000" ]; then
    log "CRITICAL: App is not responding at all!"

    if [ "$AUTO_ROLLBACK" = true ]; then
        log "Initiating auto-rollback..."
        git revert HEAD --no-edit
        git push origin main
        log "Rollback pushed. Railway will redeploy in ~60s."
        log "Verify manually: make smoke"
    fi
    exit 1
fi

log "App responding (HTTP $HTTP_CODE)"

# Step 3: Run smoke tests
log "Running smoke tests..."
REYTECH_URL="$PROD_URL" python tests/smoke_test.py 2>&1
SMOKE_EXIT=$?

if [ $SMOKE_EXIT -ne 0 ]; then
    log "SMOKE TESTS FAILED!"
    echo ""

    if [ "$AUTO_ROLLBACK" = true ]; then
        echo ""
        log "Auto-rollback: reverting last commit..."
        git revert HEAD --no-edit
        git push origin main

        log "Rollback pushed. Railway will redeploy in ~60s."
        log ""
        log "Next steps:"
        log "  1. Wait 90s for rollback deploy"
        log "  2. Run: make smoke"
        log "  3. Investigate the failed commit on a feature branch"
    else
        log ""
        log "Auto-rollback disabled. Manual action required:"
        log "  make rollback    # Revert last commit"
        log "  make smoke       # Verify recovery"
    fi
    exit 1
fi

# Step 4: Success
echo ""
log "ALL CHECKS PASSED"
log "Deploy verified: $COMMIT"

exit 0
