# Product Catalog + Pricing Intelligence — Sprint Status

## ✅ SPRINT 1: Product Catalog Foundation (COMPLETE)

### Infrastructure
- [x] product_catalog table in reytech.db (842 products, 44 columns)
- [x] catalog_price_history table (1602+ records)
- [x] product_suppliers table
- [x] QB CSV import route `/api/catalog/import` + `/api/catalog/reimport`
- [x] QB data imported from `ProductsServicesList_Reytech_Inc_2_20_2026.csv`
- [x] Auto-categorization (15 categories)
- [x] Search tokens + FTS for fast matching

### Sprint 1 Foundation Fixes
- [x] `fix_catalog_names()` — convert part-number names to descriptive product names
- [x] `_extract_manufacturer()` — extract brands from 765+ products (KNOWN_BRANDS dict)
- [x] `_make_product_name()` — clean QB description → proper product name
- [x] `bulk_calculate_recommended()` — calculate recommended_price for 766 products
- [x] `run_sprint1_fixes()` — orchestrates all fixes in order
- [x] `/api/catalog/run-fixes` route + 🔧 Run Fixes button on /catalog page
- [x] `/api/catalog/reimport` route — improved QB import with name/brand extraction
- [x] Auto-run fixes on startup if >50 unpriced products detected

### Matching Engine
- [x] `match_item()` — 4-tier: exact part# → part# in desc → token overlap → LIKE
- [x] `match_items_batch()` — batch match with freshness + recommended_price
- [x] Auto-match on PC detail page load (600ms delay, silent fetch)
- [x] Freshness badges (🟢 fresh ≤7d, 🟡 recent ≤14d, 🟠 stale ≤30d, 🔴 expired >30d)
- [x] Manufacturer badge (🏭) on catalog match results
- [x] Recommended price badge (🧠 $X.XX) on catalog match results
- [x] Win rate badge (🏆 X%) on catalog match results
- [x] "✅ Use" button applies best_cost + recommended_price (not just QB sell)

### Win/Loss Feedback
- [x] `record_outcome_to_catalog()` — updates times_won/lost, win_rate, avg_margin_won
- [x] mark-won → catalog feedback
- [x] mark-lost → catalog feedback (competitor name + price)

### Smart Pricing
- [x] `get_smart_price()` / `bulk_smart_price()` — per-item intelligent pricing
- [x] `/api/pricecheck/<pcid>/auto-price` + 🧠 Auto-Price button
- [x] Freshness-aware pricing (stale cost → flag for re-check)

### Multi-Supplier Sweep
- [x] `/api/pricecheck/<pcid>/price-sweep` — Google Shopping via SerpApi
- [x] 🛒 Sweep button on PC detail

## ✅ SPRINT 2: Advanced Intelligence (COMPLETE)

- [x] Portfolio Optimizer — loss-leader/balanced/profit-center classification
- [x] `optimize_portfolio()` — quote-level margin optimization
- [x] Competitor Intelligence — loss history + SCPRS/competitor data
- [x] Photo Capture — JSON-LD, OG tags, meta extraction
- [x] UOM Auto-Detection — CS/BX/PK/EA/PR/RL patterns
- [x] `get_freshness_report()` — per-item freshness with price history context

## ✅ SPRINT 3: Catalog Growth Loop (COMPLETE)

- [x] `add_to_catalog()` — creates new product from any PC line item
- [x] `save_pc_items_to_catalog()` — batch-add all PC items on save
- [x] Auto-add on PC save: unmatched items with cost/price → catalog
- [x] Auto-add on mark-won/lost: creates entries for new items
- [x] '📋 Add to Catalog' button for unmatched items
- [x] Duplicate detection (exact name + token overlap 60%+)
- [x] POST /api/pricecheck/<pcid>/save-to-catalog
- [x] POST /api/catalog/add-item

## 🔲 SPRINT 4: Catalog UI + Product Pages (NEXT)
- [ ] 4a. Enhanced catalog browse page with sorting, pagination
- [ ] 4b. Individual product detail page with price history chart
- [ ] 4c. Product edit/merge/dedup tool
- [ ] 4d. Catalog health dashboard (stale prices, missing data, margin alerts)
- [ ] 4e. QB CSV diff — show what changed since last import

## 🔲 SPRINT 5: Win/Loss Analytics Dashboard
- [ ] 5a. Win/Loss dashboard with category breakdown
- [ ] 5b. Margin analysis by category, agency, time period
- [ ] 5c. "Should have won" detector (lost by <5%)
- [ ] 5d. Pricing trend charts per product
- [ ] 5e. Revenue opportunity calculator
