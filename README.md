# Reytech RFQ Automation System

**Version:** 7.2 | **Owner:** Michael Guadan, Reytech Inc.
**Deployment:** Railway → `virtuous-education / production`

## Overview

End-to-end RFQ automation for a California SB/DVBE government reseller.
Automates the pipeline from email ingestion through bid document generation:

**Email → Parse → Price Lookup → Competitive Pricing → Bid Package / Quote PDF → Email Draft**

## Quick Start

```bash
# Clone
git clone https://github.com/mikeg0321/Reytech-RFQ.git
cd Reytech-RFQ

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GMAIL_ADDRESS=rfq@reytechinc.com
export GMAIL_PASSWORD=<app-password>
export DASH_USER=reytech
export DASH_PASS=<password>
export SECRET_KEY=<random-secret>

# Run locally
gunicorn app:app --bind 0.0.0.0:8080 --workers 2 --timeout 120
```

## Project Structure

```
├── app.py                      # Entry point — Flask app factory + startup checks
├── scripts/
│   └── qa_check.py             # Pre-deploy QA gate (28 automated checks)
├── src/
│   ├── api/
│   │   ├── dashboard.py        # Blueprint: 60 routes, all UI (2,504 lines)
│   │   └── templates.py        # All HTML templates + page builders (1,427 lines)
│   ├── agents/
│   │   ├── scprs_lookup.py     # FI$Cal SCPRS price scraper
│   │   ├── email_poller.py     # Gmail IMAP/SMTP
│   │   ├── product_research.py # AI product sourcing + MFG extraction
│   │   └── tax_agent.py        # CDTFA tax rate lookups
│   ├── forms/
│   │   ├── rfq_parser.py       # RFQ PDF attachment parser
│   │   ├── reytech_filler_v4.py# Bid package PDF form filler
│   │   ├── price_check.py      # AMS 704 price check processor
│   │   └── quote_generator.py  # Reytech-branded quote PDFs
│   ├── auto/
│   │   └── auto_processor.py   # Zero-touch processing pipeline
│   ├── knowledge/
│   │   ├── pricing_oracle.py   # Multi-factor pricing engine
│   │   └── won_quotes_db.py    # Historical pricing KB
│   └── core/
│       ├── paths.py            # ★ Single source of truth for ALL directory paths
│       ├── startup_checks.py   # ★ Runtime self-test on every app boot
│       └── logging_config.py   # Structured logging
├── data/
│   ├── customers.json          # CRM contacts (63 records)
│   ├── quote_counter.json      # Sequential quote numbering (next: R26Q16)
│   └── quotes_log.json         # Quote audit trail (43 quotes)
├── tests/                      # 110 tests across 3 files (all passing)
│   ├── conftest.py             # Fixtures: temp data dirs, auth client, seed data
│   ├── test_dashboard_routes.py# 53 route tests
│   ├── test_quote_generator.py # 42 unit tests
│   └── test_pipeline.py        # 15 integration tests
├── reytech_config.json         # Company info, pricing rules, email settings
├── requirements.txt            # Python dependencies
├── Procfile                    # Gunicorn start command
└── railway.json                # Railway deploy config
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `GMAIL_ADDRESS` | Email for IMAP polling |
| `GMAIL_PASSWORD` | Gmail App Password (16-char) |
| `DASH_USER` | Dashboard login username |
| `DASH_PASS` | Dashboard login password |
| `SECRET_KEY` | Flask session secret |
| `PORT` | Auto-set by Railway |
| `SERPAPI_KEY` | (Optional) Product research API key |

## Key API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/` | Dashboard home — RFQ queue |
| `/rfq/<id>` | RFQ detail view |
| `/pricecheck/<id>` | Price check detail |
| `/quotes` | Quote history |
| `/api/status` | System status |
| `/api/health` | Health check |
| `/api/diag` | Full diagnostics |
| `/api/poll-now` | Manual email poll |
| `/api/auto-process/pc` | Auto-process pipeline |

## Deploy to Railway

1. Run `python3 scripts/qa_check.py` — must show 0 FAILED
2. Push to `refactor-v7.2` branch (or `main` after merge)
3. Railway auto-deploys via GitHub integration
4. Verify: visit `/api/health` — checks paths, data files, and modules

## Quality Infrastructure

### Pre-Deploy QA Gate
```bash
python3 scripts/qa_check.py      # 28 automated checks, exit 1 = blocked
python3 scripts/qa_check.py -v   # verbose mode
```
Checks: compilation, DATA_DIR resolution, route integrity, form/JS wiring, import paths, data file integrity, config files, code hygiene, test suite, template separation, import hygiene.

### Runtime Startup Checks
Every deploy runs `startup_checks.py` automatically:
- Validates DATA_DIR consistency across all modules
- Verifies data file readability (customers, quotes, counter)
- Checks config integrity and route registration
- Results visible in Railway logs

### Centralized Paths (`src/core/paths.py`)
Single source of truth for all directory paths. Never compute your own DATA_DIR:
```python
from src.core.paths import DATA_DIR, PROJECT_ROOT, UPLOAD_DIR
```

## Adding New Modules

```python
# 1. Import paths — never compute your own DATA_DIR
try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

# 2. Use src.* prefix for sibling imports
try:
    from src.knowledge.won_quotes_db import find_similar_items
except ImportError:
    from won_quotes_db import find_similar_items

# 3. Add module to startup_checks.py validation list
# 4. Add module to scripts/qa_check.py module list
# 5. Run: python3 scripts/qa_check.py before pushing
```

## Tech Stack

- **Runtime:** Python 3.12 on Railway (Nixpacks)
- **Web:** Flask + Gunicorn (2 workers)
- **PDF Read:** pypdf (form field extraction)
- **PDF Write:** ReportLab (quotes), pypdf (form filling)
- **Email:** imaplib (IMAP PEEK), smtplib (SMTP)
- **Scraping:** requests + BeautifulSoup (FI$Cal PeopleSoft)
- **Tax:** CDTFA Tax Rate REST API
- **Storage:** JSON files (no database)
- **Auth:** HTTP Basic Auth (env var credentials)
