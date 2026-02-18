# Reytech Smoke Tests

Before/after validator for the weekend refactor.

## Quick Start

```bash
# Install requests if needed
pip install requests --break-system-packages

# Run against local dev server (in another terminal: python -m flask run)
python tests/smoke_test.py

# Run against Railway prod
REYTECH_URL=https://your-app.railway.app \
REYTECH_USER=reytech \
REYTECH_PASS=yourpass \
python tests/smoke_test.py
```

## Weekend Refactor Workflow

```bash
# 1. Before refactor — save baseline
REYTECH_URL=https://prod.railway.app python tests/smoke_test.py --save-baseline tests/baseline_pre_refactor.json

# 2. Do the refactor

# 3. After refactor — check for regressions
REYTECH_URL=https://prod.railway.app python tests/smoke_test.py --compare tests/baseline_pre_refactor.json
```

## Categories

| Category | What it tests |
|---|---|
| `pages` | All 9+ pages return HTTP 200 |
| `auth` | Unauthenticated → 401, bad creds → 403 |
| `api` | Core APIs: /api/db, /api/funnel/stats, /api/qa/health, CRM, Intel |
| `feature_321` | 1-click PC→Quote endpoint + banner |
| `templates` | Email template library page + API |
| `growth` | Distro campaign endpoint |
| `forecasting` | Win probability + weighted pipeline |
| `prices` | Price history API |
| `data` | Contacts seeded, quote counter present, volume status |
| `errors` | Missing pages handled gracefully |

## Exit Codes

- `0` — all pass
- `1` — failures exist
- `2` — regressions vs baseline
