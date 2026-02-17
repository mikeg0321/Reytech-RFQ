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
├── app.py                      # Entry point — Flask app factory
├── src/
│   ├── api/
│   │   ├── dashboard.py        # Blueprint: 59 routes, all UI
│   │   └── templates.py        # HTML template strings
│   ├── agents/
│   │   ├── scprs_lookup.py     # FI$Cal SCPRS price scraper
│   │   ├── email_poller.py     # Gmail IMAP/SMTP
│   │   ├── product_research.py # AI product sourcing
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
│       └── logging_config.py   # Structured logging
├── data/
│   ├── customers.json          # CRM contacts (63 records)
│   ├── quote_counter.json      # Sequential quote numbering
│   └── quotes_log.json         # Quote audit trail
├── reytech_config.json         # Company info, pricing rules, email settings
├── requirements.txt            # Python dependencies
├── Procfile                    # Gunicorn start command
├── railway.json                # Railway deploy config
└── tests/                      # 110 tests across 3 files
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

1. Push to `refactor-v7.2` branch (or `main` after merge)
2. Railway auto-deploys via GitHub integration
3. Verify: visit `/api/status` and `/api/health`

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
