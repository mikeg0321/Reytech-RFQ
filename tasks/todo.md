# Reytech RFQ — Task Tracker

## Active Sprint (2026-02-22) — PRD-28 WI-2 + WI-4 Integration

### WI-2: Email Outbox Overhaul — Wire tracking into send flow
- [ ] **2a** send_email() → inject tracking pixel + wrap links via email_lifecycle
- [ ] **2b** follow_up_engine → check engagement data (opens/clicks) to prioritize
- [ ] **2c** Outbox summary widget on home page (drafts, failed, open rate)

### WI-4: Revenue Dashboard — Wire data flows
- [ ] **4a** quote won → auto-log to revenue_log (quote_lifecycle already does this ✓, verify)
- [ ] **4b** Auto-reconcile on revenue page load (revenue_engine.reconcile_revenue)
- [ ] **4c** Margin calc: backfill existing quotes with cost data from catalog_price_history

## Completed (2026-02-22)
- [x] WI-1: Quote Lifecycle — all bridges wired + pushed
- [x] WI-3: Lead Nurture — all bridges wired + pushed
- [x] Template extraction: 5 pages (growth_intel, scprs_intel, cchcs_intel, contacts, prospect_detail)

## Sprint: Product Catalog + Pricing Intelligence (2026-02-23)

### Sprint 1: Product Catalog DB + QB Import — ACTIVE
- [ ] Create `products` table in SQLite
- [ ] Create `product_suppliers` table
- [ ] Build QB CSV importer
- [ ] Import 841 products from QB export
- [ ] Add `/api/catalog/search` endpoint (fuzzy match)
- [ ] Verify: products queryable, descriptions cleaned

### Sprint 2: Auto-Match Engine (pending S1)
### Sprint 3: Multi-Supplier Sweep (pending S2)
### Sprint 4: Win/Loss Tracker (pending S1)
### Sprint 5: Dynamic Markup Optimizer (pending S4)
