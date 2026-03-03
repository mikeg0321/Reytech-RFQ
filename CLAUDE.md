# CLAUDE.md — Reytech RFQ Project Rules

## System Context

**What this is:** End-to-end RFQ automation + business intelligence for Reytech Inc., a California SB/DVBE government reseller. 101K lines, 695 routes, 46 templates, deployed on Railway.

**Stack:** Python 3.12 / Flask / SQLite (WAL mode) / Jinja2 / Gunicorn. No frontend framework — all server-rendered HTML with inline JS.

**Deploy:** Push to `main` → Railway auto-deploys. Persistent volume at `/data`. Domain: `web-production-dcee9.up.railway.app`.

**Module loading:** Route modules in `src/api/modules/` are loaded via `exec()` into `dashboard.py` namespace. This means all modules share globals. Be aware of name collisions.

## Workflow Orchestration

### 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately — don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Verification Before Done
- Never mark a task complete without proving it works
- **Always compile-check** Python: `python -c "import py_compile; py_compile.compile('file.py', doraise=True)"`
- **Always render-test** templates with all required variables after changes
- Test with realistic data structures — production data may differ from dev assumptions
- Ask yourself: "Would a staff engineer approve this?"

### 3. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests — then resolve them
- Trace the full call chain: route → function → template → data structure
- Check for type mismatches (dict vs list, missing keys, None values)

### 4. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- Skip this for simple, obvious fixes — don't over-engineer
- Challenge your own work before presenting it

## Code Patterns

### Route Pattern
```python
@bp.route("/api/example", methods=["POST"])
@auth_required
def api_example():
    """Docstring with purpose."""
    try:
        # business logic
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        log.error("Example error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
```

### Template Variable Safety
Always use `|default()` for any variable that might not exist:
```jinja2
{{ value|default(0) }}
{{ obj.key|default('fallback') }}
{% for item in items|default([]) %}
```

### Defensive Data Loading
```python
try:
    data = some_function()
    if not isinstance(data, dict):
        data = {}
except Exception as e:
    log.error("Load error: %s", e)
    data = {}
data.setdefault("required_key", default_value)
```

### Growth Agent Functions
All in `src/agents/growth_agent.py` (104 functions). Key patterns:
- `_load_json(path)` / `_save_json(path, data)` for all JSON file I/O
- `_load_prospects_list()` returns list of prospect dicts
- Status dicts (`PULL_STATUS`, `BUYER_STATUS`, `INTEL_STATUS`) for long-running ops
- Thread-based async for SCPRS scraping — poll status endpoints for progress

## Known Issues (Production Audit)

### Critical — SQL Injection
16 files contain f-string SQL queries. These are behind auth but still injection vectors:
- `src/core/db.py` (3 instances)
- `src/agents/product_catalog.py` (7 instances)
- `src/agents/scprs_universal_pull.py` (4 instances)
- Fix: Convert all to parameterized queries (`cursor.execute("... WHERE x = ?", (value,))`)

### Warning — Unprotected Routes
13 routes lack `@auth_required`. Most are intentional (health check, webhooks, email tracking pixels) but 2 admin routes need auth:
- `/api/email-trace` — should have auth
- `/api/disk-cleanup` — should have auth

### Info — Code Quality
- 74 bare `except:` clauses across 6 files (should specify exception types)
- 9 TODO/FIXME/HACK comments remaining
- 230 POST endpoints rely on session auth only (no explicit CSRF tokens)

## File Layout Rules

- **Routes:** `src/api/modules/routes_*.py` — one file per domain area
- **Agents:** `src/agents/*.py` — one file per external integration or intelligence engine
- **Templates:** `src/templates/*.html` — extends `base.html`, uses `render_page()`
- **Data:** `data/*.json` and `data/*.db` — persisted on Railway volume
- **Forms:** `src/forms/*.py` — PDF generation and form filling

## Testing Checklist

Before pushing any change:
1. `python -c "py_compile.compile('changed_file.py', doraise=True)"` for each modified Python file
2. If template changed: render test with all required variables (check for `UndefinedError`)
3. If route changed: verify `@auth_required` decorator is present
4. If data structure changed: check all templates that consume it for type assumptions
5. `git diff --stat` to verify only intended files are modified

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.
- **Defensive Programming**: Every data access should handle None, wrong type, missing keys.
- **Production First**: This is a live business system. Every commit deploys automatically.
