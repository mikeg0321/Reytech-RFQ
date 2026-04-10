#!/bin/bash
# deploy.sh — Safe deployment pipeline for Reytech-RFQ.
#
# PRIMARY FLOW (feature branch):
#   ./scripts/deploy.sh              # Test -> push branch -> create PR
#
# LEGACY FLOW (direct to main):
#   ./scripts/deploy.sh --direct     # Test -> push main -> smoke
#
# STAGING:
#   ./scripts/deploy.sh --staging    # Deploy current branch to staging
#
# Requires: git, python, gh CLI

set -e

PROD_URL="${REYTECH_URL:-https://web-production-dcee9.up.railway.app}"
STAGING_URL="${STAGING_URL:-}"
DEPLOY_TARGET="production"
DIRECT_PUSH=false
SKIP_TESTS=false

# Parse args
for arg in "$@"; do
    case $arg in
        --staging) DEPLOY_TARGET="staging" ;;
        --direct) DIRECT_PUSH=true ;;
        --skip-tests) SKIP_TESTS=true ;;
    esac
done

BRANCH=$(git branch --show-current)
COMMIT=$(git log -1 --format="%h %s")

echo ""
echo "Reytech-RFQ Deploy Pipeline"
echo "==========================="
echo "  Branch: $BRANCH"
echo "  Target: $DEPLOY_TARGET"
echo "  Mode:   $([ "$DIRECT_PUSH" = true ] && echo 'DIRECT (legacy)' || echo 'PR-based')"
echo ""

# ── Step 1: Pre-deploy tests ─────────────────────────────────────────────────
if [ "$SKIP_TESTS" = false ]; then
    echo "Step 1: Running tests..."
    SECRET_KEY=test DASH_USER=test DASH_PASS=test FLASK_ENV=testing \
    python -m pytest tests/test_ams704_helpers.py \
        tests/test_template_registry.py \
        tests/test_pc_generation.py \
        tests/test_rfq_generation.py \
        tests/test_multipage_704.py \
        tests/test_compile_safety.py \
        tests/test_api_contracts.py \
        tests/test_pricing_math.py \
        -x -q --tb=short

    echo ""
    echo "Step 1b: Running pre-deploy checks..."
    SECRET_KEY=test FLASK_ENV=testing python tests/pre_deploy_check.py

    echo "Tests + checks passed"
else
    echo "WARNING: Skipping tests (--skip-tests). You are on your own."
fi

# ── Step 2: Push / Deploy ────────────────────────────────────────────────────
echo ""

if [ "$DEPLOY_TARGET" = "staging" ]; then
    echo "Step 2: Deploying to staging..."
    railway up --environment staging --detach
    echo "Staging deploy triggered"
    echo ""
    echo "Next: wait ~60s, then run:"
    echo "  STAGING_URL=<url> make smoke-staging"
    exit 0
fi

if [ "$DIRECT_PUSH" = true ]; then
    # Legacy direct-to-main flow
    if [ "$BRANCH" != "main" ]; then
        echo "ERROR: --direct requires being on main branch."
        echo "Current branch: $BRANCH"
        exit 1
    fi
    echo "Step 2: Pushing directly to main (legacy mode)..."
    git push origin main
    echo "Push complete. Railway auto-deploying..."
    echo ""
    echo "Step 3: Waiting 90s for Railway deploy..."
    sleep 90
    echo ""
    echo "Step 4: Smoke testing..."
    ./scripts/post_deploy_verify.sh --no-rollback
else
    # PR-based flow (preferred)
    if [ "$BRANCH" = "main" ]; then
        echo "ERROR: PR-based deploy requires a feature branch."
        echo ""
        echo "Create one:"
        echo "  make branch name=feat/my-feature"
        echo ""
        echo "Or use legacy mode (not recommended):"
        echo "  ./scripts/deploy.sh --direct"
        exit 1
    fi

    echo "Step 2: Pushing branch '$BRANCH'..."
    git push origin HEAD -u
    echo ""

    echo "Step 3: Creating pull request..."
    gh pr create --fill 2>/dev/null && echo "PR created" || echo "PR may already exist"
    echo ""

    echo "Done. CI will run on the PR."
    echo ""
    echo "When CI passes:"
    echo "  make promote           # Merge + smoke test production"
    echo "  gh pr merge --squash   # Or merge manually"
fi

echo ""
echo "Deploy pipeline complete."
