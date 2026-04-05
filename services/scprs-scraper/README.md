# SCPRS Scraper Service

Playwright-based SCPRS scraping microservice. Extracted from the main Reytech RFQ app to reduce container size by ~500MB.

## Deploy on Railway

1. Create a new service in the Railway project
2. Point it to this directory (`services/scprs-scraper/`)
3. Set env vars: `SCRAPER_SECRET=<shared-secret>`, `PORT=8001`
4. In the main app, set: `SCRAPER_SERVICE_URL=http://<scraper-service>.railway.internal:8001`

## API

All endpoints accept POST with JSON body and require `X-Scraper-Secret` header.

- `POST /scrape/details` — FI$Cal supplier detail scrape
- `POST /scrape/po` — Single PO detail extraction
- `POST /scrape/exhaustive` — Date-range exhaustive scrape
- `POST /scrape/public-search` — Public CaleProcure search
- `POST /scrape/intercept` — CaleProcure API discovery via network interception
- `GET /health` — Liveness check
