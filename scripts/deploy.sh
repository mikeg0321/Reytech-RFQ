#!/bin/bash
# deploy.sh — Safe deployment pipeline for Reytech-RFQ.
#
# Flow: tests → push → wait for Railway deploy → smoke test → alert
#
# Usage:
#   ./scripts/deploy.sh              # Deploy to production
#   ./scripts/deploy.sh --staging    # Deploy to staging only
#   ./scripts/deploy.sh --skip-tests # Skip pre-push tests (dangerous)
#
# Requires: git, python, curl
# Optional: railway CLI (for staging), ALERT_WEBHOOK_URL env var

set -e

PROD_URL="${REYTECH_URL:-https://web-production-dcee9.up.railway.app}"
STAGING_URL="${STAGING_URL:-}"
ALERT_WEBHOOK="${ALERT_WEBHOOK_URL:-}"
DEPLOY_TARGET="production"
SKIP_TESTS=false

# Parse args
for arg in "$@"; do
    case $arg in
        --staging) DEPLOY_TARGET="staging" ;;
        --skip-tests) SKIP_TESTS=true ;;
    esac
done

echo "═══════════════════════════════════════════════════"
echo "  Reytech-RFQ Deploy Pipeline"
echo "  Target: $DEPLOY_TARGET"
echo "═══════════════════════════════════════════════════"

# Step 1: Pre-deploy tests
if [ "$SKIP_TESTS" = false ]; then
    echo ""
    echo "▶ Step 1: Running pre-deploy tests..."
    python -m pytest tests/test_pricing_math.py tests/test_api_contracts.py \
        tests/test_compile_safety.py tests/test_pc_generation.py \
        tests/test_rfq_generation.py -x -q --tb=short
    if [ $? -ne 0 ]; then
        echo "❌ DEPLOY BLOCKED: Tests failed. Fix before deploying."
        exit 1
    fi
    echo "✅ Pre-deploy tests passed"
else
    echo "⚠️  Skipping pre-deploy tests (--skip-tests)"
fi

# Step 2: Push to trigger Railway deploy
echo ""
echo "▶ Step 2: Pushing to main..."
git push origin main
echo "✅ Push complete"

# Step 3: Wait for Railway to build and deploy
echo ""
echo "▶ Step 3: Waiting for Railway deploy (60s)..."
sleep 60

# Step 4: Smoke test against live app
TARGET_URL="$PROD_URL"
if [ "$DEPLOY_TARGET" = "staging" ] && [ -n "$STAGING_URL" ]; then
    TARGET_URL="$STAGING_URL"
fi

echo ""
echo "▶ Step 4: Running smoke tests against $TARGET_URL..."
REYTECH_URL="$TARGET_URL" python tests/smoke_test.py
SMOKE_EXIT=$?

if [ $SMOKE_EXIT -ne 0 ]; then
    echo "❌ SMOKE TESTS FAILED on $TARGET_URL"
    echo "   Check logs: railway logs --latest"

    # Alert via Slack
    if [ -n "$ALERT_WEBHOOK" ]; then
        curl -s -X POST "$ALERT_WEBHOOK" \
            -H 'Content-Type: application/json' \
            -d "{\"text\":\"🚨 *Deploy FAILED* — smoke tests failed on $DEPLOY_TARGET\\nCheck: railway logs --latest\"}" \
            > /dev/null 2>&1
    fi
    exit 1
fi

echo "✅ Smoke tests passed on $TARGET_URL"

# Step 5: Notify success
if [ -n "$ALERT_WEBHOOK" ]; then
    COMMIT=$(git log -1 --format="%h %s")
    curl -s -X POST "$ALERT_WEBHOOK" \
        -H 'Content-Type: application/json' \
        -d "{\"text\":\"✅ *Deploy successful* to $DEPLOY_TARGET\\n\`$COMMIT\`\"}" \
        > /dev/null 2>&1
fi

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✅ Deploy complete: $DEPLOY_TARGET"
echo "═══════════════════════════════════════════════════"
