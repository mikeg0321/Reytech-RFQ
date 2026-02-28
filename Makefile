# Reytech RFQ — Common Operations
# Usage: make test | make check | make deploy | make routes

.PHONY: test check deploy routes lint health

# ── Testing ──────────────────────────────────────────────────────────────────

test:  ## Run all tests
	SECRET_KEY=test python3 -m pytest tests/test_sprints.py tests/test_critical_paths.py -v --tb=short

test-quick:  ## Run only sprint smoke tests
	SECRET_KEY=test python3 -m pytest tests/test_sprints.py -v --tb=short

# ── Pre-deploy ───────────────────────────────────────────────────────────────

check:  ## Run pre-deploy validation
	SECRET_KEY=test python3 tests/pre_deploy_check.py

lint:  ## Syntax check all Python files
	@python3 -c "import py_compile, os; \
	count = 0; \
	[py_compile.compile(os.path.join(r,f), doraise=True) or setattr(type('',(),{'c':0}), 'c', 0) for r, d, fs in os.walk('src') for f in fs if f.endswith('.py') and '__pycache__' not in r]; \
	py_compile.compile('app.py', doraise=True); \
	print('✅ All files compile clean')"

# ── Development ──────────────────────────────────────────────────────────────

run:  ## Start local development server
	SECRET_KEY=dev-only DASH_USER=reytech DASH_PASS=changeme python3 app.py

routes:  ## List all API routes
	@SECRET_KEY=test python3 -c "import os; os.environ['SECRET_KEY']='test'; \
	from app import create_app; a=create_app(); \
	rules=sorted(a.url_map.iter_rules(), key=lambda r:r.rule); \
	api=[r for r in rules if r.rule.startswith('/api/')]; \
	print(f'{len(rules)} total routes ({len(api)} API endpoints)'); \
	[print(f'  {\" | \".join(sorted(r.methods-{\"HEAD\",\"OPTIONS\"})):10s} {r.rule}') for r in api]" 2>/dev/null

# ── Deployment ───────────────────────────────────────────────────────────────

deploy: check test  ## Full pre-deploy: check + test, then push
	git push origin main

# ── Monitoring ───────────────────────────────────────────────────────────────

health:  ## Check production health (requires RAILWAY_URL)
	@echo "Local health check..."
	@SECRET_KEY=test python3 -c "import os; os.environ['SECRET_KEY']='test'; \
	from app import create_app; a=create_app(); \
	c=a.test_client(); \
	import base64; h={'Authorization':'Basic '+base64.b64encode(b'reytech:changeme').decode()}; \
	r=c.get('/api/system/preflight', headers=h); \
	import json; print(json.dumps(r.get_json(), indent=2))" 2>/dev/null

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
