# SCPRS Harvest Plan

## Agent Inventory

### 1. scprs_universal_pull.py
**Writes to:** `scprs_po_master`, `scprs_po_lines`, `scprs_pull_log`
**Entry point:** `run_universal_pull(priority="P0"|"P1"|"all")`
**Input:** Uses `FiscalSession` from `scprs_lookup.py` to search SCPRS public site
**Search terms:** `UNIVERSAL_SEARCH_TERMS` list — product categories + competitor names
**Date range:** Last 365 days from run date (hardcoded)
**Already has:** 2,225 POs from prior runs

### 2. scprs_intelligence_engine.py
**Writes to:** `scprs_po_master`, `scprs_po_lines`, `price_history`
**Entry points:**
- `pull_agency(agency_key, search_terms)` — pulls one agency
- `run_po_award_monitor()` — matches open quotes against SCPRS POs, marks lost
- `get_growth_intelligence()` — reads from scprs tables, returns analytics
- `pull_all_agencies_background()` — runs all agencies in sequence
- `backfill_historical(year)` — pulls a specific fiscal year
**Input:** Reads from `scprs_po_master` (after universal_pull fills it) AND directly from SCPRS via `FiscalSession`
**Note:** `pull_agency()` hits SCPRS directly — does NOT depend on universal_pull data

### 3. scprs_lookup.py
**Provides:** `FiscalSession` class — session-based SCPRS web scraper
**Auth:** No credentials needed. Public access to fiscal.ca.gov SCPRS search
**Env vars:** None required (public site)
**Methods:** `init_session()`, `search(description, from_date, to_date, supplier_name)`, `get_detail()`

### 4. scprs_scanner.py
**Writes to:** `scprs_scan_log.json`, `scprs_seen_pos.json` (JSON files)
**Purpose:** Polls for new POs periodically (opportunity scanner)
**Not needed for bulk harvest** — this is for ongoing monitoring

### 5. scprs_public_search.py
**Provides:** Playwright-based public search (alternative to FiscalSession)
**Purpose:** CCHCS-specific purchases, keyword search
**Not needed for bulk harvest** — FiscalSession handles this

## Credentials
- **FiscalSession:** NO credentials needed (public SCPRS search)
- **SCPRS_USERNAME/SCPRS_PASSWORD:** Not used by any current agent
- All agents connect to `data/reytech.db` via DATA_DIR

## Fiscal Years Available
SCPRS typically has data going back 4-5 years. Available fiscal years:
- FY2025-26 (current)
- FY2024-25
- FY2023-24
- FY2022-23
- FY2021-22

## Correct Execution Order

1. **scprs_universal_pull.run_universal_pull("all")** — Pull raw POs for all search terms
2. **scprs_intelligence_engine.pull_all_agencies_background()** — Pull per-agency data
3. **Process raw data into intelligence tables** — Extract vendor_intel, buyer_intel, competitors, won_quotes_kb from scprs_po_master/lines
4. **scprs_intelligence_engine.run_po_award_monitor()** — Match open quotes against awards

## What Harvest Runner Needs to Do
Since the intelligence engine writes to `price_history` but NOT to `vendor_intel`, `buyer_intel`, `competitors`, or `won_quotes_kb`, the harvest runner must:
1. Run the existing pull agents to populate `scprs_po_master`/`scprs_po_lines`
2. Process the raw PO data into the intelligence tables (custom aggregation)
3. Tag Reytech's own wins in the data
