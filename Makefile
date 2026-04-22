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

.PHONY: test test-quick test-full check lint run routes deploy ship promote branch health status help staging-setup staging-deploy staging-smoke staging-promote worktree worktree-remove worktree-list require-smoke-creds

# ── Configuration ───────────────────────────────────────────────────────────

PROD_URL ?= https://web-production-dcee9.up.railway.app
STAGING_URL ?=
BRANCH := $(shell git branch --show-current 2>/dev/null)
COMMIT := $(shell git log -1 --format="%h %s" 2>/dev/null)

# Smoke credentials — pulled from the shell or from a local .env file so
# make smoke / make promote actually authenticate. Before 2026-04-12 these
# were not forwarded, which made smoke_test.py default to a fake "changeme"
# password and silently return 401 on every authed endpoint. That turned
# the deploy gate into a rubber stamp. Fail loud instead of falling through.
-include .env
SMOKE_USER := $(or $(REYTECH_USER),$(DASH_USER))
SMOKE_PASS := $(or $(REYTECH_PASS),$(DASH_PASS))

# ── Testing ─────────────────────────────────────────────────────────────────

test:  ## Run critical test suite (pre-push gate) — parallelized via pytest-xdist
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
		-n auto -q --tb=short

test-quick:  ## Run fast subset (compile + imports only)
	@echo "Running quick checks..."
	@SECRET_KEY=test DASH_USER=test DASH_PASS=test FLASK_ENV=testing \
		python -m pytest tests/test_compile_safety.py tests/test_api_contracts.py -x -q --tb=short

test-full:  ## Run ALL tests (49 files, comprehensive) — parallelized via pytest-xdist
	@echo "Running full test suite..."
	@SECRET_KEY=test DASH_USER=test DASH_PASS=test FLASK_ENV=testing \
		python -m pytest tests/ \
		--ignore=tests/test_award_intelligence.py \
		--ignore=tests/smoke_test.py \
		-n auto -q --tb=short

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

worktree:  ## Create an isolated worktree: make worktree name=feat/my-topic
	@if [ -z "$(name)" ]; then \
		echo "Usage: make worktree name=feat/my-topic"; \
		echo ""; \
		echo "Creates ../rfq-<topic> on a new branch from latest origin/main."; \
		echo "Use this for every parallel Claude Code window to avoid silent"; \
		echo "overwrites from a shared working tree (see CLAUDE.md)."; \
		echo ""; \
		echo "Branch name must start with: feat/ fix/ refactor/ hotfix/ chore/"; \
		exit 1; \
	fi
	@case "$(name)" in \
		feat/*|fix/*|refactor/*|hotfix/*|chore/*) : ;; \
		*) echo "ERROR: branch name must start with feat/ fix/ refactor/ hotfix/ or chore/"; exit 1 ;; \
	esac
	@topic=$$(echo "$(name)" | sed 's|.*/||'); \
	dest="../rfq-$$topic"; \
	if [ -e "$$dest" ]; then \
		echo "ERROR: $$dest already exists. Pick a different name or:"; \
		echo "  make worktree-remove name=$(name)"; \
		exit 1; \
	fi; \
	echo "Fetching latest origin/main..."; \
	git fetch origin main --quiet && \
	git worktree add -b $(name) "$$dest" origin/main && \
	echo "" && \
	echo "Worktree ready: $$dest" && \
	echo "Branch:         $(name) (from origin/main)" && \
	echo "" && \
	echo "Next steps:" && \
	echo "  cd $$dest" && \
	echo "  # launch Claude Code here — isolated working tree, shared .git" && \
	echo "  # update .claude/WORKSTREAMS.md with your branch + worktree path" && \
	echo "  # when done: make ship   (from inside the worktree)" && \
	echo "  # after merge: make worktree-remove name=$(name)"

worktree-remove:  ## Remove a worktree: make worktree-remove name=feat/my-topic
	@if [ -z "$(name)" ]; then \
		echo "Usage: make worktree-remove name=feat/my-topic"; \
		exit 1; \
	fi
	@topic=$$(echo "$(name)" | sed 's|.*/||'); \
	dest="../rfq-$$topic"; \
	if [ ! -e "$$dest" ]; then \
		echo "Nothing to remove: $$dest does not exist."; \
		echo "Active worktrees:"; \
		git worktree list; \
		exit 0; \
	fi; \
	git worktree remove "$$dest" && \
	echo "Removed worktree: $$dest" && \
	echo "Branch $(name) still exists locally — delete with: git branch -D $(name)"

worktree-list:  ## List all active worktrees
	@git worktree list

ship: check  ## Push branch + create PR (pre-push hook runs tests; pass auto=1 to auto-merge on green)
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
	@if [ -n "$$(git status --porcelain | grep -v '^.. data/')" ]; then \
		echo ""; \
		echo "WARNING: Uncommitted code changes detected:"; \
		git status --short | grep -v '^.. data/'; \
		echo ""; \
		echo "Commit your changes first, then re-run make ship."; \
		echo "  git add <files>"; \
		echo "  git commit -m 'your message'"; \
		echo "  make ship"; \
		exit 1; \
	fi
	@echo ""
	@echo "Pushing branch (pre-push hook runs critical tests)..."
	git push origin HEAD -u
	@echo ""
	@echo "Creating pull request..."
	@gh pr create --fill 2>/dev/null || echo "PR may already exist. Check: gh pr view"
	@if [ "$(auto)" = "1" ]; then \
		echo ""; \
		echo "Enabling auto-merge (squash) — PR merges the moment CI goes green..."; \
		gh pr merge --squash --auto --delete-branch && \
		echo "Auto-merge armed. Watch with: gh pr checks --watch"; \
	else \
		echo ""; \
		echo "Branch pushed + PR created."; \
		echo "CI will run automatically. When green, run: make promote"; \
		echo "(Or re-run with auto=1 next time to skip the manual promote step)"; \
	fi

promote: require-smoke-creds  ## Merge current PR after CI passes, then smoke test production
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
	@# Poll /version until the new replica reports our HEAD commit, instead of
	@# a blind sleep. Old sleep was 90s; a healthy rolling swap happens in ~30s
	@# but Railway's build phase runs 3-7 min, so the old flow sometimes ran
	@# smoke against the OLD replica (it still answers /ping during build).
	@# On timeout we still run smoke — smoke + the background deploy watcher
	@# are the real gates; this poll is just a "ready sooner" signal.
	@./scripts/wait_for_deploy.sh $(PROD_URL) $$(git rev-parse HEAD) 420 || true
	@echo ""
	@echo "Running smoke tests against production..."
	@REYTECH_URL=$(PROD_URL) REYTECH_USER=$(SMOKE_USER) REYTECH_PASS=$(SMOKE_PASS) \
		python tests/smoke_test.py && \
		echo "" && \
		echo "DEPLOY SUCCESSFUL: $(COMMIT)" && \
		echo "Launching deploy watcher in background (auto-rollback on FAILED or main-CI failure)..." && \
		(REYTECH_URL=$(PROD_URL) REYTECH_CI_COMMIT=$$(git rev-parse HEAD) \
			nohup python scripts/railway_deploy_watcher.py \
			--max-poll-minutes 5 \
			--ci-max-wait-minutes 35 \
			> /tmp/reytech_deploy_watcher.log 2>&1 &) \
		|| \
		(echo "" && \
		echo "SMOKE TESTS FAILED — triggering auto-rollback..." && \
		REYTECH_URL=$(PROD_URL) python scripts/railway_deploy_watcher.py \
			--max-poll-minutes 3 && \
		echo "" && \
		echo "Auto-rollback flow complete. Verify: make smoke" && \
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

rollback-to:  ## Roll back Railway to a specific deploy id (no rebuild, ~60s): make rollback-to id=<deployment-id>
	@if [ -z "$(id)" ]; then \
		echo "Usage: make rollback-to id=<railway-deployment-id>"; \
		echo ""; \
		echo "Find a deployment id with:"; \
		echo "  make deploys                # list recent Railway deploys"; \
		echo ""; \
		echo "Uses Railway GraphQL deploymentRedeploy with usePreviousImageTag=true,"; \
		echo "so it skips the build step entirely and reuses the cached image."; \
		exit 1; \
	fi
	@./scripts/railway_rollback.sh "$(id)"

deploys:  ## List the 10 most recent Railway deploys with status + commit
	@./scripts/railway_deploys.sh

require-smoke-creds:
	@if [ -z "$(SMOKE_USER)" ] || [ -z "$(SMOKE_PASS)" ]; then \
		echo ""; \
		echo "ERROR: smoke credentials not configured."; \
		echo ""; \
		echo "Set REYTECH_USER/REYTECH_PASS (or DASH_USER/DASH_PASS) in your"; \
		echo "shell or in a local .env file at the repo root. Example:"; \
		echo ""; \
		echo "  echo 'REYTECH_USER=Reytech'     >> .env"; \
		echo "  echo 'REYTECH_PASS=<password>'  >> .env   # .env is gitignored"; \
		echo ""; \
		echo "Without this, smoke_test.py authenticates as 'changeme' and"; \
		echo "every authed endpoint returns 401 — the deploy gate becomes"; \
		echo "a rubber stamp."; \
		exit 1; \
	fi

smoke: require-smoke-creds  ## Run smoke tests against production
	@echo "Smoke testing $(PROD_URL) as $(SMOKE_USER)..."
	@REYTECH_URL=$(PROD_URL) REYTECH_USER=$(SMOKE_USER) REYTECH_PASS=$(SMOKE_PASS) \
		python tests/smoke_test.py

smoke-staging:  ## Run smoke tests against staging
	@if [ -z "$(STAGING_URL)" ]; then \
		echo "ERROR: STAGING_URL not set."; \
		echo "Usage: STAGING_URL=https://... make smoke-staging"; \
		exit 1; \
	fi
	@echo "Smoke testing staging: $(STAGING_URL)..."
	@REYTECH_URL=$(STAGING_URL) python tests/smoke_test.py

# ── Staging ─────────────────────────────────────────────────────────────────

staging-setup:  ## Set up Railway staging environment (one-time)
	@./scripts/staging.sh setup

staging-deploy: test check  ## Deploy current branch to staging
	@./scripts/staging.sh deploy

staging-smoke:  ## Run smoke tests against staging
	@./scripts/staging.sh smoke

staging-promote:  ## Promote staging to production (merge to main)
	@./scripts/staging.sh promote

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

status:  ## Pipeline dashboard — where your code is in the deploy pipeline
	@python scripts/pipeline_status.py

db-diag:  ## Read-only reytech.db bloat diagnostic (pass db=<path> for non-default)
	@python scripts/db_bloat_diagnostic.py $(if $(db),--db $(db),)

db-diag-json:  ## Machine-readable JSON bloat report (pass db=<path> for non-default)
	@python scripts/db_bloat_diagnostic.py --json $(if $(db),--db $(db),)

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
