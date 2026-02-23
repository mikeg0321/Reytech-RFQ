# Sprint 1: Core Intelligence Wiring

## What Exists (already built)
- [x] product_catalog table (839 products, 44 columns)
- [x] catalog_price_history (1602 records)  
- [x] product_suppliers table (schema ready, 1 row)
- [x] match_item / match_items_batch in product_catalog.py
- [x] runCatalogMatch UI button + badges + "Use" button
- [x] mark-won / mark-lost endpoints
- [x] calculate_recommended_price in product_catalog.py
- [x] pricing_intel.py with get_price_recommendation
- [x] award_monitor.py with competitor tracking

## Sprint 1 Tasks

### 1. Win/Loss → Catalog Feedback Loop  
- [ ] On mark-won: update product_catalog (times_won++, avg_margin_won, last_sold_price)
- [ ] On mark-lost: update product_catalog (times_lost++, competitor pricing)
- [ ] Record to catalog_price_history (won/lost prices)
- [ ] Update win_rate = times_won / (times_won + times_lost)

### 2. Smart Auto-Price Route + Button
- [ ] New route: /api/pricecheck/<pcid>/auto-price
- [ ] Per-item: catalog recommended, SCPRS ceiling, win history, margin targets
- [ ] Return: {recommended, aggressive, safe, reasoning, win_probability}
- [ ] UI: "🧠 Auto-Price" button on PC detail

### 3. Auto-Match on PC Load
- [ ] Fire catalog match on page load (JS)
- [ ] Show inline badges for matched items
- [ ] Pre-fill option for high-confidence matches (>85%)

### 4. Price Freshness Indicators
- [ ] Check catalog last_checked per matched product
- [ ] Show ⚠️ if >14d old, 🔴 if >30d old
- [ ] "Re-check" link per stale item

### 5. Multi-Supplier Google Shopping Sweep
- [ ] New route: /api/pricecheck/<pcid>/price-sweep
- [ ] SerpApi google_shopping engine for ALL retailers
- [ ] Save results to product_suppliers
- [ ] UI: price comparison per item
