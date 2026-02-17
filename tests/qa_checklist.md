# Reytech RFQ System — QA Checklist v2.0

## How to Use
Run through before each deploy. Mark P(ass)/F(ail)/S(kip).
🤖 = Automated (covered by pytest) | 👁️ = Manual verification needed

**Quick run:** `pytest tests/ -v --tb=short`
**With coverage:** `pytest tests/ --cov=. --cov-report=term-missing`

---

## 1. Price Check Workflow (Upload → Price → Generate → Download)

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 1.1 | Upload AMS 704 PDF → PC appears in queue with correct institution | 👁️ | |
| 1.2 | PC detail page loads with correct items, qtys, UOMs | 🤖 | ✅ |
| 1.3 | Description cleaner strips dimensions (3/4"x3") | 🤖 | ✅ |
| 1.4 | Description cleaner strips font specs (Arial, 18pt) | 🤖 | ✅ |
| 1.5 | Description cleaner strips material specs (magnetic, glossy) | 🤖 | ✅ |
| 1.6 | Description cleaner preserves normal descriptions | 🤖 | ✅ |
| 1.7 | SCPRS Lookup button → populates SCPRS column | 👁️ | |
| 1.8 | Amazon Lookup button → populates Amazon price + title | 👁️ | |
| 1.9 | ASIN shows in blue monospace below Amazon match link | 🤖 | ✅ |
| 1.10 | data-testid="pc-scprs-lookup" present on button | 🤖 | ✅ |
| 1.11 | data-testid="pc-amazon-lookup" present on button | 🤖 | ✅ |
| 1.12 | data-testid="pc-preview-quote" present on button | 🤖 | ✅ |
| 1.13 | data-testid="pc-generate-704" present on button | 🤖 | ✅ |
| 1.14 | data-testid="pc-generate-reytech-quote" present on button | 🤖 | ✅ |
| 1.15 | data-testid="pc-auto-process" present on button | 🤖 | ✅ |
| 1.16 | Cost input → markup % → price auto-calculates | 👁️ | |
| 1.17 | Per-item profit column updates live | 👁️ | |
| 1.18 | Tier comparison table shows all 4 tiers | 👁️ | |
| 1.19 | Save Prices → persists on page reload | 👁️ | |
| 1.20 | Preview Quote → modal shows AMS 704 layout | 👁️ | |
| 1.21 | Generate Completed 704 → downloads filled PDF | 👁️ | |
| 1.22 | Generated 704 has correct prices in form fields | 👁️ | |
| 1.23 | Generated 704 strips stamp annotations | 🤖 | ✅ |
| 1.24 | Reytech Quote PDF button → generates branded quote | 🤖 | ✅ |
| 1.25 | Quote PDF has ASIN in MFG PART # column | 🤖 | ✅ |
| 1.26 | Quote PDF has "Ref ASIN: xxx" in description | 🤖 | ✅ |
| 1.27 | Quote PDF To: matches Ship To: addresses | 🤖 | ✅ |
| 1.28 | Quote PDF has Reytech logo (not black box) | 👁️ | |
| 1.29 | No-bid checkbox → item excluded from totals + quote | 🤖 | ✅ |
| 1.30 | Tax toggle → enables/disables tax row in totals | 👁️ | |
| 1.31 | Auto-Process button runs full pipeline | 👁️ | |

## 2. RFQ Workflow (Email Import → Price → Generate → Send)

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 2.1 | Email poller imports RFQ with correct metadata | 👁️ | |
| 2.2 | RFQ detail page shows all columns (Cost/SCPRS/Amazon/Bid/Margin/Profit) | 🤖 | ✅ |
| 2.3 | data-testid attributes present on all RFQ buttons | 🤖 | ✅ |
| 2.4 | Quick Markup buttons (+10/15/20/25/30%) apply to all rows | 👁️ | |
| 2.5 | SCPRS Undercut buttons (-1/2/5%) apply correctly | 👁️ | |
| 2.6 | Per-item profit shows in Profit column with color coding | 🤖 | ✅ |
| 2.7 | Revenue total and profit summary at bottom | 🤖 | ✅ |
| 2.8 | Save Pricing persists on reload | 👁️ | |
| 2.9 | Preview Quote button → modal with 704B layout | 👁️ | |
| 2.10 | Generate State Forms → produces 704B + Package | 👁️ | |
| 2.11 | Generate Reytech Quote → branded PDF | 🤖 | ✅ |
| 2.12 | Template status shows ✅/❌ for each form type | 🤖 | ✅ |
| 2.13 | Delete RFQ → removes from queue | 🤖 | ✅ |
| 2.14 | Update pricing saves correctly | 🤖 | ✅ |

## 3. Quote Generator Output Quality

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 3.1 | Quote # format R{YY}Q{seq} — sequential, no gaps | 🤖 | ✅ |
| 3.2 | Peek next quote number is idempotent | 🤖 | ✅ |
| 3.3 | CDCR layout: Bill To shown, Sellers Permit shown | 🤖 | ✅ |
| 3.4 | CCHCS layout: No Bill To, no Sellers Permit | 🤖 | ✅ |
| 3.5 | CalVet layout: Bill To with correct address | 🤖 | ✅ |
| 3.6 | Agency auto-detected from institution name (case-insensitive) | 🤖 | ✅ |
| 3.7 | Empty items list → valid PDF with $0 total | 🤖 | ✅ |
| 3.8 | Tax calculation correct (subtotal × rate) | 🤖 | ✅ |
| 3.9 | Shipping added to total | 🤖 | ✅ |
| 3.10 | Items count matches line_items count | 🤖 | ✅ |
| 3.11 | PDF file >1KB created on disk | 🤖 | ✅ |
| 3.12 | Quotes logged to quotes_log.json | 🤖 | ✅ |
| 3.13 | Search quotes by number | 🤖 | ✅ |
| 3.14 | Search quotes by agency | 🤖 | ✅ |

## 4. Quotes Database Page

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 4.1 | /quotes page loads | 🤖 | ✅ |
| 4.2 | Search bar present | 🤖 | ✅ |
| 4.3 | Agency dropdown filter works | 🤖 | ✅ |
| 4.4 | Logo upload form present | 🤖 | ✅ |

## 5. Auto-Processor / Confidence Scoring

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 5.1 | Grade A for item with both Amazon + SCPRS | 🤖 | ✅ |
| 5.2 | Grade F for item with no pricing | 🤖 | ✅ |
| 5.3 | Grade F for empty pricing dict | 🤖 | ✅ |
| 5.4 | Score is float between 0 and 1 | 🤖 | ✅ |
| 5.5 | Result has all required keys (score, grade, factors, notes) | 🤖 | ✅ |
| 5.6 | Quote confidence returns all keys | 🤖 | ✅ |
| 5.7 | Empty items → overall grade F | 🤖 | ✅ |
| 5.8 | Grade distribution sums to item count | 🤖 | ✅ |
| 5.9 | Recommendation is non-empty string | 🤖 | ✅ |
| 5.10 | Response time tracking returns minutes | 🤖 | ✅ |
| 5.11 | System health check returns dict | 🤖 | ✅ |

## 6. Pricing Oracle

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 6.1 | Basic markup above supplier cost | 🤖 | ✅ |
| 6.2 | Returns recommended/aggressive/safe tiers | 🤖 | ✅ |
| 6.3 | SCPRS data influences result | 🤖 | ✅ |
| 6.4 | Zero cost handled gracefully | 🤖 | ✅ |
| 6.5 | None cost handled gracefully | 🤖 | ✅ |
| 6.6 | High-value items still profitable | 🤖 | ✅ |
| 6.7 | Aggressive ≤ Recommended ≤ Safe | 🤖 | ✅ |
| 6.8 | Oracle price feeds into quote generator correctly | 🤖 | ✅ |

## 7. Won Quotes Knowledge Base

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 7.1 | Text normalization is deterministic | 🤖 | ✅ |
| 7.2 | Tokenization returns set of strings | 🤖 | ✅ |
| 7.3 | Category classification returns string | 🤖 | ✅ |
| 7.4 | Medical items classified correctly | 🤖 | ✅ |
| 7.5 | Record ID is deterministic | 🤖 | ✅ |
| 7.6 | Freshness weight: recent dates near 1.0 | 🤖 | ✅ |
| 7.7 | Freshness weight: old dates lower | 🤖 | ✅ |
| 7.8 | Token overlap: identical sets = 1.0 | 🤖 | ✅ |
| 7.9 | Token overlap: no overlap = 0.0 | 🤖 | ✅ |
| 7.10 | Single item ingestion persists to disk | 🤖 | ✅ |
| 7.11 | Deduplication on same PO+item+desc | 🤖 | ✅ |
| 7.12 | Bulk ingestion skips zero-price items | 🤖 | ✅ |
| 7.13 | find_similar_items: empty KB → empty list | 🤖 | ✅ |
| 7.14 | find_similar_items: exact match found | 🤖 | ✅ |
| 7.15 | find_similar_items respects max_results | 🤖 | ✅ |
| 7.16 | Price history returns expected keys | 🤖 | ✅ |
| 7.17 | KB stats returns dict | 🤖 | ✅ |

## 8. Product Research (Amazon)

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 8.1 | Query builder returns string from description | 🤖 | ✅ |
| 8.2 | Query builder with item number | 🤖 | ✅ |
| 8.3 | Empty description → empty query | 🤖 | ✅ |
| 8.4 | Price extraction from dollar string | 🤖 | ✅ |
| 8.5 | Price extraction from numeric | 🤖 | ✅ |
| 8.6 | Missing price → None | 🤖 | ✅ |
| 8.7 | Cache key is deterministic | 🤖 | ✅ |
| 8.8 | Cache key is case-insensitive | 🤖 | ✅ |
| 8.9 | research_product with mock Amazon → found=True | 🤖 | ✅ |
| 8.10 | research_product no results → found=False | 🤖 | ✅ |
| 8.11 | research_product caches results | 🤖 | ✅ |
| 8.12 | research_product returns alternatives | 🤖 | ✅ |

## 9. System / API / Auth

| # | Test Case | Type | Status |
|---|-----------|------|--------|
| 9.1 | /api/health returns ok/degraded | 🤖 | ✅ |
| 9.2 | /api/status returns system info | 🤖 | ✅ |
| 9.3 | Unauthenticated request → 401 | 🤖 | ✅ |
| 9.4 | Wrong password → 401 | 🤖 | ✅ |
| 9.5 | Correct auth → 200 | 🤖 | ✅ |
| 9.6 | Nonexistent PC ID → error JSON (not crash) | 🤖 | ✅ |
| 9.7 | Nonexistent RFQ ID → error/redirect (not crash) | 🤖 | ✅ |
| 9.8 | Request logging middleware doesn't break responses | 🤖 | ✅ |
| 9.9 | Home page loads with Reytech branding | 🤖 | ✅ |
| 9.10 | Home page has upload form with data-testid | 🤖 | ✅ |

---

## Running Tests

```bash
# All tests (fast, ~3s)
pytest tests/ -v --tb=short

# With coverage
pytest tests/ --cov=. --cov-report=term-missing

# Only unit tests (no Flask client)
pytest tests/test_pricing_oracle.py tests/test_price_check.py tests/test_won_quotes_db.py tests/test_product_research.py -v

# Only integration/route tests
pytest tests/test_dashboard_routes.py tests/test_pipeline.py tests/test_integration_expanded.py -v

# Skip slow tests
pytest tests/ -v -m "not slow"
```

## Coverage Targets
| Module | Target | Actual |
|--------|--------|--------|
| quote_generator.py | 90%+ | ✅ 90.4% |
| won_quotes_db.py | 70%+ | ✅ 69.8% |
| pricing_oracle.py | 60%+ | ✅ 63.3% |
| product_research.py | 45%+ | ✅ 48.9% |
| auto_processor.py | 35%+ | ✅ 36.5% |
| dashboard.py (routes) | 35%+ | ✅ 36.6% |

## Total: 104 test cases | 181 automated tests | 42% code coverage
