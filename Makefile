# Reytech RFQ — Unified Build & Deploy Pipeline
# Usage: make help
#
# Workflow:
#   make branch name=feat/my-feature   # Create feature branch
#   <do work>
#   make ship                           # Test + push + create PR
#   <CI passes>
#   make promote                        # Merge PR + smoke test prod
#
# Direct deploy (legacy, use only when branch protection is not yet enabled):
#   make deploy                         # test + check + push main

.PHONY: test test-quick test-full check lint run routes deploy ship promote branch health status help

# ── Configuration ───────────────────────────────────────────────────────────

PROD_URL ?= https://web-production-dcee9.up.railway.app
STAGING_URL ?=
BRANCH := $(shell git branch --show-current 2>/dev/null)
COMMIT := $(shell git log -1 --format="%h %s" 2>/dev/null)

# ── Testing ─────────────────────────────────────────────────────────────────

test:  ## Run critical test suite (pre-push gate)
	@echo "Running critical tests..."
	@SECRET_KEY=test DASH_USER=test DASH_PASS=test FLASK_ENV=testing \
		python -m pytest tests/test_ams704_helpers.py \
		tests/test_template_registry.py \
		tests/test_pc_generation.py \
		tests/test_rfq_generation.py \
		tests/test_multipage_704.py \
		tests/test_compile_safety.py \
		tests/test_api_contracts.py \
		tests/test_pricing_math.py \
		-x -q --tb=short

test-quick:  ## Run fast subset (compile + imports only)
	@echo "Running quick checks..."
	@SECRET_KEY=test DASH_USER=test DASH_PASS=test FLASK_ENV=testing \
		python -m pytest tests/test_compile_safety.py tests/test_api_contracts.py -x -q --tb=short

test-full:  ## Run ALL tests (49 files, comprehensive)
	@echo "Running full test suite..."
	@SECRET_KEY=test DASH_USER=test DASH_PASS=test FLASK_ENV=testing \
		python -m pytest tests/ \
		--ignore=tests/test_award_intelligence.py \
		--ignore=tests/smoke_test.py \
		-v --tb=short

# ── Pre-deploy ──────────────────────────────────────────────────────────────

check:  ## Run pre-deploy validation (security, routes, templates)
	@SECRET_KEY=test FLASK_ENV=testing python tests/pre_deploy_check.py

lint:  ## Syntax check all Python files
	@python -c "import py_compile, os; \
	[py_compile.compile(os.path.join(r,f), doraise=True) for r, d, fs in os.walk('src') for f in fs if f.endswith('.py') and '__pycache__' not in r]; \
	py_compile.compile('app.py', doraise=True); \
	print('All files compile clean')"

# ── Branch Workflow (PRIMARY — use this) ────────────────────────────────────

branch:  ## Create feature branch: make branch name=feat/my-feature
	@if [ -z "$(name)" ]; then \
		echo "Usage: make branch name=feat/my-feature"; \
		echo ""; \
		echo "Naming conventions:"; \
		echo "  feat/description    — new feature"; \
		echo "  fix/description     — bug fix"; \
		echo "  refactor/description — code improvement"; \
		echo "  hotfix/description  — urgent production fix"; \
		exit 1; \
	fi
	@if [ "$(BRANCH)" != "main" ]; then \
		echo "WARNING: Not on main. Current branch: $(BRANCH)"; \
		echo "Switch to main first: git checkout main && git pull"; \
		exit 1; \
	fi
	git pull origin main
	git checkout -b $(name)
	@echo ""
	@echo "Branch '$(name)' created from latest main."
	@echo "When ready: make ship"

ship: test check  ## Test + push branch + create PR (the safe way to deploy)
	@if [ "$(BRANCH)" = "main" ]; then \
		echo ""; \
		echo "ERROR: Cannot ship directly from main."; \
		echo ""; \
		echo "Create a feature branch first:"; \
		echo "  make branch name=feat/my-feature"; \
		echo ""; \
		echo "Or for a quick hotfix:"; \
		echo "  make branch name=hotfix/fix-description"; \
		echo ""; \
		exit 1; \
	fi
	@echo ""
	@echo "Tests passed. Pushing branch..."
	git add -A
	@echo ""
	@echo "Staged files:"
	@git diff --cached --stat
	@echo ""
	@echo "Review the staged changes above."
	git push origin HEAD -u
	@echo ""
	@echo "Creating pull request..."
	@gh pr create --fill 2>/dev/null || echo "PR may already exist. Check: gh pr view"
	@echo ""
	@echo "Branch pushed + PR created."
	@echo "CI will run automatically. When green, run: make promote"

promote:  ## Merge current PR after CI passes, then smoke test production
	@if [ "$(BRANCH)" = "main" ]; then \
		echo "ERROR: Already on main. Nothing to promote."; \
		exit 1; \
	fi
	@echo "Checking CI status..."
	@gh pr checks 2>/dev/null || echo "WARNING: Could not verify CI status"
	@echo ""
	@echo "Merging PR for branch: $(BRANCH)..."
	gh pr merge --squash --delete-branch
	git checkout main
	git pull origin main
	@echo ""
	@echo "Merged to main. Railway auto-deploying..."
	@echo "Waiting 90s for Railway build..."
	@sleep 90
	@echo ""
	@echo "Running smoke tests against production..."
	@REYTECH_URL=$(PROD_URL) python tests/smoke_test.py && \
		echo "" && \
		echo "DEPLOY SUCCESSFUL: $(COMMIT)" || \
		(echo "" && \
		echo "SMOKE TESTS FAILED!" && \
		echo "Check production immediately: $(PROD_URL)" && \
		echo "Rollback: make rollback" && \
		exit 1)

# ── Emergency Operations ────────────────────────────────────────────────────

rollback:  ## Revert last commit on main and push (emergency only)
	@if [ "$(BRANCH)" != "main" ]; then \
		echo "ERROR: Must be on main to rollback. Run: git checkout main"; \
		exit 1; \
	fi
	@echo "EMERGENCY ROLLBACK"
	@echo "This will revert the last commit on main and push to Railway."
	@echo ""
	@echo "Last commit: $(COMMIT)"
	@echo ""
	@read -p "Type 'ROLLBACK' to confirm: " confirm; \
	if [ "$$confirm" = "ROLLBACK" ]; then \
		git revert HEAD --no-edit && \
		git push origin main && \
		echo "" && \
		echo "Rollback pushed. Railway will redeploy in ~60s." && \
		echo "Verify: make smoke"; \
	else \
		echo "Cancelled."; \
	fi

smoke:  ## Run smoke tests against production
	@echo "Smoke testing $(PROD_URL)..."
	@REYTECH_URL=$(PROD_URL) python tests/smoke_test.py

smoke-staging:  ## Run smoke tests against staging
	@if [ -z "$(STAGING_URL)" ]; then \
		echo "ERROR: STAGING_URL not set."; \
		echo "Usage: STAGING_URL=https://... make smoke-staging"; \
		exit 1; \
	fi
	@echo "Smoke testing staging: $(STAGING_URL)..."
	@REYTECH_URL=$(STAGING_URL) python tests/smoke_test.py

# ── Development ─────────────────────────────────────────────────────────────

run:  ## Start local development server
	SECRET_KEY=dev-only DASH_USER=reytech DASH_PASS=changeme python app.py

routes:  ## List all API routes
	@SECRET_KEY=test python -c "import os; os.environ['SECRET_KEY']='test'; \
	from app import create_app; a=create_app(); \
	rules=sorted(a.url_map.iter_rules(), key=lambda r:r.rule); \
	api=[r for r in rules if r.rule.startswith('/api/')]; \
	print(f'{len(rules)} total routes ({len(api)} API endpoints)'); \
	[print(f'  {\" | \".join(sorted(r.methods-{\"HEAD\",\"OPTIONS\"})):10s} {r.rule}') for r in api]" 2>/dev/null

# ── Status & Info ───────────────────────────────────────────────────────────

status:  ## Show current branch, PR status, and pipeline state
	@echo "Branch:  $(BRANCH)"
	@echo "Commit:  $(COMMIT)"
	@echo ""
	@echo "--- Git Status ---"
	@git status -s
	@echo ""
	@echo "--- Open PRs ---"
	@gh pr list --limit 5 2>/dev/null || echo "(gh CLI not available)"
	@echo ""
	@echo "--- Recent CI Runs ---"
	@gh run list --limit 3 2>/dev/null || echo "(gh CLI not available)"

# ── Legacy Deploy (direct push, before branch protection) ──────────────────

deploy: check test  ## Legacy: test + check + push main directly
	@echo ""
	@echo "WARNING: Direct push to main. Prefer 'make ship' with feature branches."
	@echo ""
	git push origin main

# ── Help ────────────────────────────────────────────────────────────────────

help:  ## Show this help
	@echo "Reytech RFQ — Build & Deploy Pipeline"
	@echo ""
	@echo "PRIMARY WORKFLOW:"
	@echo "  make branch name=feat/x  Create feature branch from main"
	@echo "  make ship                Test + push + create PR"
	@echo "  make promote             Merge PR + smoke test production"
	@echo ""
	@echo "ALL TARGETS:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
