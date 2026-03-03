# Reytech RFQ Automation System

**Version:** 8.0 | **Owner:** Michael Guadan, Reytech Inc.
**Deployment:** Railway → `web-production-dcee9.up.railway.app`
**Stack:** Python 3.12 / Flask / SQLite (WAL) / Jinja2 / Gunicorn
**Codebase:** 101,613 lines across 105 Python files, 46 templates, 695 routes

## Overview

End-to-end RFQ automation and business intelligence platform for a California SB/DVBE government reseller. Covers the full lifecycle from email ingestion through bid generation, order tracking, growth prospecting, and revenue analytics.

**Core Pipeline:**
Email → Parse → Price Lookup (SCPRS + Web) → Competitive Pricing → Bid Package / Quote PDF → Email Draft → Follow-Up → Order Tracking → Revenue

## Quick Start

```bash
git clone https://github.com/mikeg0321/Reytech-RFQ.git
cd Reytech-RFQ
pip install -r requirements.txt

# Required environment variables
export GMAIL_ADDRESS=rfq@reytechinc.com
export GMAIL_PASSWORD=<app-password>
export DASH_USER=reytech
export DASH_PASS=<password>
export SECRET_KEY=<random-secret>

gunicorn app:app --bind 0.0.0.0:8080 --workers 2 --timeout 120
```

## Architecture

```
app.py                          Flask factory, startup, polling init
src/
├── api/
│   ├── dashboard.py            Main Blueprint (4,166 lines), module loader
│   ├── render.py               Template rendering with shared context
│   ├── shared.py               Blueprint instance, auth_required decorator
│   └── modules/                16 route files
│       ├── routes_intel.py         Growth, SCPRS, revenue, QB (6,281 lines)
│       ├── routes_pricecheck.py    Price check workflow (5,518 lines)
│       ├── routes_crm.py           CRM, contacts, activity (3,935 lines)
│       ├── routes_rfq.py           RFQ processing (3,287 lines)
│       ├── routes_orders_full.py   Order management (1,705 lines)
│       └── ... (11 more modules)
├── agents/                     41 agent modules
│   ├── growth_agent.py             Growth Engine — 104 functions (4,179 lines)
│   ├── email_poller.py             Dual Gmail IMAP polling (2,526 lines)
│   ├── product_catalog.py          Product catalog (2,711 lines)
│   ├── scprs_lookup.py             FI$Cal SCPRS price scraper
│   ├── qa_agent.py                 QA intelligence (2,490 lines)
│   └── ... (36 more agents)
├── core/
│   ├── db.py                       SQLite DAL, 14 tables (2,496 lines)
│   ├── security.py                 Rate limiting, CSRF, headers
│   └── paths.py                    Railway volume path resolution
├── forms/                      PDF fillers, quote gen, price check
├── knowledge/                  Pricing oracle, won quotes, margins
└── templates/                  46 Jinja2 HTML templates
```

## Pages (86 routes)

| Page | Route | Purpose |
|------|-------|---------|
| Home | `/` | Dashboard: KPIs, activity feed, manager brief, growth summary |
| Price Checks | `/pricechecks` | Incoming price check archive |
| PC Detail | `/pricecheck/<id>` | Full workflow: parse → lookup → price → generate |
| Quotes | `/quotes` | Quote management with KPIs and status tracking |
| Orders | `/orders` | Order lifecycle: timeline, margins, delivery tracking |
| PO Tracking | `/po-tracking` | Purchase order tracking and delivery status |
| Follow-Ups | `/follow-ups` | Follow-up queue with cohort analysis |
| CRM | `/contacts` | Contact management: 7 KPIs, bulk actions, sorting |
| Catalog | `/catalog` | Product catalog with supplier pricing and freshness |
| Analytics | `/analytics` | Business analytics with growth metrics |
| Pipeline | `/pipeline` | Sales pipeline: velocity, stale quotes, revenue goals |
| Growth | `/growth` | Growth Engine V3 — 12-tab outreach platform |
| Search | `/search` | Universal search across all entities |
| Settings | `/settings` | System health, data storage, nav config |

## Growth Engine

3-phase buyer intelligence and outreach platform built on SCPRS data:

- **Phase 1 — Same-Agency:** Finds new buyers at agencies already served
- **Phase 2 — Cross-Sell:** Other agencies buying items Reytech sells
- **Phase 3 — New Products:** Medical advantage items (med director + license)

Features: 6 email templates, weighted scoring, win probability, kanban, workflows, calendar, A/B testing, campaign tracking, bulk ops, CSV/Excel/PDF export.

## Environment Variables

**Required:** `SECRET_KEY`, `DASH_USER`, `DASH_PASS`, `GMAIL_ADDRESS`, `GMAIL_PASSWORD`

**Integrations:** `ANTHROPIC_API_KEY`, `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_FROM_NUMBER`, `VAPI_API_KEY`, `SERPAPI_KEY`, `QB_CLIENT_ID`, `QB_CLIENT_SECRET`

See `DEPLOY.md` for full setup (80 env vars documented).

## Development

```bash
python app.py                    # Local dev
python -m py_compile src/api/modules/routes_intel.py   # Syntax check
```

Auto-deploys from GitHub `main` via Railway. 826 commits to date.
