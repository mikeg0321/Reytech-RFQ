# Reytech RFQ Automation System — PRD v6.0 Delta

**Version:** 6.0 | **Date:** February 13, 2026
**Owner:** Michael Guadan, Reytech Inc.
**Architect:** VIBE-4.6 (Claude Opus 4.6)

---

## CHANGE SUMMARY

This update introduces two foundational modules that transform Reytech from a
**dashboard-assisted workflow** into a **pricing intelligence engine** — the critical
prerequisite for full end-to-end automation (Phase 8).

| Component | Status | Purpose |
|-----------|--------|---------|
| Won Quotes Knowledge Base (`won_quotes_db.py`) | 🔧 Building | Persistent store of all SCPRS historical wins, fuzzy-matched to new RFQ items |
| Dynamic Pricing Oracle (`pricing_oracle.py`) | 🔧 Building | Multi-factor pricing engine replacing static markup rules |
| Auto-Process Pipeline (`/api/auto-process/`) | 📋 Designed | Zero-touch RFQ→Quote→Draft route (next sprint) |

---

## NEW MODULE: Won Quotes Knowledge Base (`won_quotes_db.py`)

### Purpose
Transform SCPRS from a "search on demand" tool into a **persistent competitive
intelligence database**. Every price lookup enriches the knowledge base. Over time,
this becomes Reytech's unfair advantage — instant pricing for any item class the
state has ever bought.

### Architecture

```
scprs_lookup.py (existing)
       │
       ▼
won_quotes_db.py ◄──── data/won_quotes.json (persistent store)
       │
       ├── ingest_scprs_result()    ← Auto-stores every SCPRS lookup result
       ├── find_similar_items()     ← Fuzzy + token match against KB
       ├── get_price_history()      ← Time-series of prices for an item class
       ├── win_probability()        ← Probability estimate based on historical wins
       └── enrich_from_bulk_scrape() ← Background job to pre-populate KB
```

### Data Schema: Won Quote Record

```json
{
  "id": "wq_20260213_001",
  "po_number": "4500012345",
  "item_number": "6500-001-430",
  "description": "X-RESTRAINT PACKAGE by Stryker Medical",
  "normalized_description": "x restraint package stryker medical",
  "tokens": ["restraint", "package", "stryker", "medical", "x"],
  "category": "medical_equipment",
  "supplier": "Medline Industries",
  "department": "CCHCS",
  "unit_price": 1245.00,
  "quantity": 2,
  "total": 2490.00,
  "award_date": "2025-09-15",
  "po_start_date": "2025-10-01",
  "source": "scprs_live",
  "confidence": 0.95,
  "freshness_days": 151,
  "ingested_at": "2026-02-13T14:30:00-08:00"
}
```

### Matching Algorithm

1. **Exact item number match** → confidence 1.0
2. **Token overlap ≥ 70%** on normalized description → confidence 0.7–0.95
3. **Category + keyword match** → confidence 0.4–0.7
4. **No match** → returns empty, triggers supplier research (Phase 6)

Matches are weighted by freshness: awards within 6 months get 1.0x weight,
6–12 months get 0.8x, 12–24 months get 0.5x, older than 24 months get 0.2x.

### Enrichment Strategy

- **Passive enrichment:** Every SCPRS lookup auto-stores results
- **Active enrichment (future):** Nightly bulk scrape of SCPRS for common categories
- **Manual enrichment:** Import historical won-quote data from spreadsheets

---

## NEW MODULE: Dynamic Pricing Oracle (`pricing_oracle.py`)

### Purpose
Replace the static pricing rules in `reytech_filler_v4.py` with an intelligent,
multi-factor pricing engine that produces tiered bid recommendations with
win-probability estimates.

### Replaces (in `reytech_filler_v4.py`)

```python
# OLD — static rules
if scprs_price:
    price = scprs_price * (1 - 0.01)  # undercut by 1%
else:
    price = cost * 1.25  # 25% markup
```

### New Pricing Algorithm

```
INPUTS:
  - supplier_cost: float (from Phase 6 research or manual entry)
  - scprs_matches: List[WonQuote] (from Won Quotes KB)
  - agency: str (CCHCS, CDCR, CalVet)
  - item_category: str (medical, industrial, office, general)
  - urgency: str (standard, rush)

WEIGHTS:
  - Historical SCPRS wins:  60%
  - Real-time supplier cost: 30%
  - Margin goals + patterns: 10%

OUTPUT:
  PricingRecommendation {
    recommended: {price, margin_pct, win_probability}
    aggressive:  {price, margin_pct, win_probability}
    safe:        {price, margin_pct, win_probability}
    flags: ["price_above_recent_wins", "thin_margin", "no_scprs_data"]
    reasoning: str
  }
```

### Pricing Tiers

| Tier | Strategy | Typical Win Rate |
|------|----------|-----------------|
| **Recommended** | SCPRS median minus 1–3%, minimum $100 profit | 65–75% |
| **Aggressive** | SCPRS lowest minus 2–5%, minimum $50 profit | 80–90% |
| **Safe** | Cost + 25–35% markup, stays below SCPRS median | 40–55% |

### Guardrails

- **Hard floor:** Never bid below cost + $25 (prevents money-losing bids)
- **Ceiling alert:** If recommended price > 110% of SCPRS median → auto-flag
- **Stale data warning:** If best SCPRS match is >18 months old → flag + suggest research
- **No-data fallback:** If no SCPRS matches AND no supplier cost → block auto-generation, require human input

---

## INTEGRATION PLAN

### Phase 1 (This Sprint): Foundation
1. Build `won_quotes_db.py` with JSON storage
2. Build `pricing_oracle.py` with three-tier output
3. Wire into `dashboard.py` — new API routes:
   - `POST /api/pricing/recommend` — get pricing recommendation for an RFQ
   - `GET /api/won-quotes/stats` — KB statistics
   - `GET /api/won-quotes/search?q=` — search the knowledge base
4. Update dashboard UI to show pricing tiers instead of single price
5. Backfill: run bulk SCPRS lookups for top 50 item categories

### Phase 2 (Next Sprint): Auto-Process Pipeline
1. `POST /api/auto-process/<rid>` — full pipeline with pause-for-approval
2. Confidence scoring on the full bid package
3. Email draft with pricing tier annotation
4. Human approval UI with one-click send

### Phase 3 (Sprint +2): Self-Learning
1. Track win/loss outcomes (did we win the bid?)
2. Adjust pricing weights based on actual outcomes
3. Win-rate dashboard by agency, category, margin

---

## IMPACT ON EXISTING MODULES

| Module | Change | Risk |
|--------|--------|------|
| `dashboard.py` | New API routes, updated UI for pricing tiers | Low — additive only |
| `scprs_lookup.py` | Add `ingest` hooks after every lookup | Low — wrapper calls |
| `reytech_filler_v4.py` | Pricing logic delegates to oracle | Medium — must maintain backward compat |
| `quote_generator.py` | No change this sprint | None |
| `tax_agent.py` | No change | None |
| `email_poller.py` | No change | None |
| `rfq_parser.py` | No change | None |

---

## MIGRATION NOTES

- JSON storage continues for v6.0 (PostgreSQL migration planned for v7.0)
- `won_quotes.json` will grow — implement 10,000-record cap with LRU eviction
- All new code is backward-compatible — existing workflow unchanged
- Pricing oracle is opt-in until validated (existing static rules still available as fallback)

---

*Generated: February 13, 2026 | PRD v6.0 | Architect: VIBE-4.6*
