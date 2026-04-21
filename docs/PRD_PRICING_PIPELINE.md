# PRD: Automated Pricing Pipeline

> ## Current State — 2026-04-21
>
> This doc is the original vision (2026-04-08). Code has sprinted past it.
> For live state, check `project_pricing_pipeline_prd.md` in memory, the
> `/health/quoting` dashboard, and git log. Highlights:
>
> **Shipped:**
> - Phase 1: UPC/MFG#/ASIN identifier waterfall, supplier SKU reverse lookup,
>   match-rejection blocklist, confidence guardrails — live.
> - Phase 2: Grok LLM validator (`src/agents/product_validator.py`) with 14-day
>   cache, rate limits, retries — live. **Gap:** no circuit breaker wrapping
>   the API call despite `get_breaker("grok")` predefined in
>   `src/core/circuit_breaker.py`.
> - Phase 3 (partial): READY/REVIEW/MANUAL/SKIP tier classification live at
>   `routes_pricecheck.py:819-836`. Row color-coding shipped. Chrome-MCP
>   visual regression not yet added.
> - Phase 4 (partial): `match_feedback` + `rejected_matches` blocklist live;
>   Oracle V5 `calibrate_from_outcome` wired at mark-won. Weekly
>   match-quality report not yet built.
>
> **Known gaps (PRD review 2026-04-21):**
> - ~~"EXACT" badge overclaims~~ — ✅ fixed PR #310 (UPC/identifier-verified only, ≥0.99).
> - ~~Grok circuit breaker missing~~ — ✅ fixed PR #311 (`get_breaker("grok").call(...)`).
> - ~~Confidence threshold hardcoded at 0.75~~ — ✅ now flag-tunable via
>   `pipeline.confidence_threshold` (`/api/admin/flags`). Default 0.75 preserved.
> - Shadow mode for new cost sources — not built. No concrete new source today;
>   will add when a new source (scraper, API) is introduced.
> - Chrome-MCP color-coding regression — visual audit done 2026-04-21 against
>   prod PC pc_711f47d6 (no color-only signals found); automated regression
>   not yet captured in a pytest-chrome fixture.
> - Evaluator-Critic loop around Grok validator output quality — not built.
> - Weekly match-quality report (Phase 4) — not yet built.

### Agentic Gaps (patterns not yet adopted)

- **Shadow mode** — pattern for introducing a new cost source: run it
  beside catalog/SCPRS for N quotes, log deltas to `match_feedback` without
  affecting quoted price. Adopt when the next cost source lands.
- **Evaluator-Critic** — Grok's validation output isn't graded. Consider a
  second-pass critic (Grok + cheaper model, or Grok vs catalog consensus)
  that scores confidence in Grok's answer before it flips `llm_validated=True`.
- **Proactive Watcher** — nothing monitors catalog drift, price staleness,
  or UPC resolution accuracy over time. A weekly agent pass comparing
  last-30-days matches against current-day matches would surface rot.

### Verification Plan (Stage 5 UI)

Chrome MCP selectors for the tier/badge rollout — tie these into a
pytest-chrome fixture when the regression suite needs lockdown:

- **EXACT badge** — `b` elements with text `EXACT` on `/pricecheck/<id>`.
  Count per item should be 0 unless a source chip has `match_confidence ≥ 0.99`
  (UPC-verified or explicit identifier).
- **~FUZZY badge** — `span` elements with text `~FUZZY`. Renders when
  `0.80 ≤ match_confidence < 0.99`.
- **Review tier pill** — text content in `{READY, REVIEW, MANUAL, SKIP}`.
  Colorblind guard: text is the primary signal; color is secondary.
- **Flag knob** — setting `pipeline.confidence_threshold=0.70` via
  `/api/admin/flags` must move items from REVIEW to READY on the next
  PC render.
>
> Treat the sections below as the architectural vision, not the as-built
> state. Do not scope new work from this doc alone — grep the code first.

---

## Vision

A buyer sends a 704 PDF. The system reads it, identifies every product, finds the best price, applies intelligent markup, and presents a ready-to-send quote. Human time per PC: **under 2 minutes** — review and click send.

```
EMAIL IN → PARSE → IDENTIFY → PRICE → MARKUP → REVIEW → SEND
   (0s)     (5s)    (30s)      (30s)    (auto)   (2min)   (click)
```

---

## The Problem Today

Each step was built independently. There's no hierarchy, no decision tree, no clear "what happens when X fails." The result:
- UPCs on the 704 get ignored, system does fuzzy matching instead
- S&S items can't be priced (Cloudflare), no automatic Amazon cross-ref
- 24% match confidence fills $35.24 as cost — no guard rail
- User spends 15+ minutes per PC manually looking up products
- Corrections aren't fed back — same bad match appears next time

---

## Pipeline Architecture

### Stage 1: RECEIVE + PARSE (0-5 seconds)

**Trigger:** Email arrives OR user uploads PDF

**What happens:**
1. PDF is classified (AMS 704, 704B, generic RFQ, Excel, Word)
2. Form fields are extracted (pdfplumber + form field reader)
3. Line items parsed: item#, qty, UOM, qty_per_uom, description, substituted item column

**Identifiers extracted from EACH line item:**
| Source | What | Example | Priority |
|--------|------|---------|----------|
| Description trailing number | UPC/EAN barcode | `Monopoly Game - 195166217604` → `195166217604` | 1 |
| Substituted Item column | MFG#, UPC, model# | `630509288762` or `Item Model #: 60002` | 1 |
| Description embedded | MFG#, ASIN, SKU | `MFG# CYO588200` or `B00006IFJ7` | 2 |
| Description context | Supplier name | `S&S Worldwide` → flag for S&S resolution | 3 |

**Output per item:**
```
{
  description: "Hasbro Gaming Connect 4",
  description_raw: "Hasbro Gaming Connect 4  - 630509940448",
  upc: "630509940448",
  mfg_number: "",
  asin: "",
  supplier_skus: {},
  substituted: "630509940448",
  qty: 1, uom: "EA", qty_per_uom: 1
}
```

**Safeguards:**
- Never discard the raw description — store as `description_raw`
- All 12-13 digit numbers extracted as potential UPCs (but validated before use)
- Substituted Item column always parsed even if empty (many 704s populate it)
- If parse fails: flag as `parse_error`, show to user, never silently drop items

---

### Stage 2: IDENTIFY (5-30 seconds)

**Goal:** For each item, answer: "What exact product is this?"

**Decision tree — executed IN ORDER, stops at first success:**

```
Step 2a: UPC/Barcode Lookup
  ├─ Search product_catalog WHERE upc = ?
  ├─ Search product_suppliers WHERE sku = ?
  ├─ Search won_quotes WHERE upc = ?
  ├─ Search Amazon by UPC (search_amazon(upc))
  └─ If found: IDENTIFIED (confidence 0.99)
       Store: ASIN, product title, supplier URLs, all known SKUs

Step 2b: MFG# / Part Number Lookup
  ├─ Search product_catalog WHERE mfg_number = ?
  ├─ Search product_suppliers WHERE sku = ? (supplier SKU reverse)
  ├─ Search won_quotes WHERE item_number = ?
  └─ If found: IDENTIFIED (confidence 0.95-0.98)

Step 2c: Supplier SKU Resolution
  ├─ Detect supplier from description context (S&S, Uline, Grainger)
  ├─ Extract supplier-specific SKU pattern
  ├─ Reverse lookup in product_suppliers
  ├─ If S&S: search Amazon by full description (can't scrape S&S)
  └─ If found: IDENTIFIED (confidence 0.90-0.98)

Step 2d: ASIN Direct Lookup
  ├─ If ASIN in description: lookup_amazon_product(asin)
  ├─ Get title, list_price, sale_price, manufacturer
  └─ If found: IDENTIFIED (confidence 0.95)

Step 2e: Description Search (Amazon)
  ├─ Only if steps 2a-2d returned nothing
  ├─ search_amazon(description, max_results=3)
  ├─ Compare each result title to PC description
  ├─ Accept only if match score >= 70%
  └─ If found: IDENTIFIED (confidence = match_score / 100)

Step 2f: Catalog Token Match
  ├─ Only if steps 2a-2e returned nothing
  ├─ Token overlap matching against product_catalog
  ├─ Minimum Jaccard similarity: 0.50
  ├─ Cross-category penalty: -0.30 if categories differ
  └─ If found: TENTATIVE (confidence = token_score)

Step 2g: LLM Validator (NEW — Grok/Claude)
  ├─ Only for items that are TENTATIVE or UNIDENTIFIED
  ├─ Send: {description, upc, mfg#, best_match_title, best_match_price}
  ├─ Ask: "Is this the correct product? If not, what is?"
  ├─ LLM can search the web (Grok) or use provided context (Claude)
  └─ If confirmed: upgrade confidence. If corrected: apply correction.

Step 2h: UNIDENTIFIED
  ├─ Flag item as needs_manual_lookup
  ├─ Show empty cost field with "No match found" badge
  ├─ Provide Google Shopping search link for manual fallback
  └─ User must look this one up manually
```

**Confidence tiers after identification:**
| Tier | Confidence | Source | Auto-fill cost? | Show badge |
|------|-----------|--------|----------------|------------|
| EXACT | 0.95-1.0 | UPC, ASIN, part# exact | YES | `EXACT` green |
| STRONG | 0.75-0.94 | Description match >=70%, catalog token >=0.75 | YES | none |
| TENTATIVE | 0.50-0.74 | Catalog fuzzy, low description match | NO — show as reference only | `~FUZZY` yellow |
| REJECTED | <0.50 | Below threshold | NO — don't even show | hidden |

**Safeguards:**
- UPC validation: check digit (GS1 algorithm) before trusting
- NEVER fill cost from a match below 0.50 confidence
- NEVER fill cost from a match below 0.75 if user already has a cost
- Rejection blocklist: previously rejected (query, match) pairs are skipped
- Rate limits: max 5 Amazon API calls per PC, max 3 LLM calls per PC
- All identification results stored: ASIN, supplier URLs, confidence, match reason

---

### Stage 3: PRICE (5-30 seconds)

**Goal:** For each identified item, determine our cost basis and reference prices.

**Price sources — ranked by reliability:**

| Priority | Source | Field | Role | Trust level |
|----------|--------|-------|------|-------------|
| 1 | **Catalog (our cost)** | `catalog_cost` | What WE pay our supplier | Highest — this is our actual cost |
| 2 | **Web/supplier scrape** | `web_price` | Live price from supplier page | High — real-time, but may be retail |
| 3 | **Amazon MSRP** | `list_price` | List price (NOT sale price) | Medium — stable reference |
| 4 | **Amazon sale price** | `sale_price` | Current discounted price | Low — may expire in days |
| 5 | **SCPRS (state contract)** | `scprs_price` | What the STATE paid last time | REFERENCE ONLY — never use as our cost |
| 6 | **Oracle recommendation** | `oracle_price` | AI-calculated bid price | REFERENCE ONLY — this is a sell price, not cost |

**Cost selection logic:**
```python
def determine_cost(item):
    p = item["pricing"]
    
    # Priority 1: Our known supplier cost
    cost = p.get("catalog_cost") or p.get("web_cost") or 0
    
    # Priority 2: Amazon MSRP (list price, NOT sale)
    if not cost:
        cost = p.get("list_price") or 0
    
    # Priority 3: Amazon current price (if no list available)
    if not cost:
        cost = p.get("amazon_price") or 0
    
    # NEVER use SCPRS or Oracle as cost
    # SCPRS = what the state paid (a ceiling)
    # Oracle = what we should charge (a sell price)
    
    return cost
```

**For EACH identified item, collect:**
```
{
  unit_cost: 9.99,           ← our supplier cost (from catalog or web)
  list_price: 12.99,         ← MSRP (for "if discount holds" calc)
  sale_price: 9.99,          ← current sale (for discount tracking)
  scprs_price: 8.50,         ← state contract ref (ceiling)
  oracle_price: 11.58,       ← AI recommendation (sell price target)
  sources: [                 ← all price references for display
    {source: "catalog", price: 9.99, confidence: 0.99, url: "..."},
    {source: "amazon", price: 9.99, confidence: 0.90, url: "..."},
    {source: "scprs", price: 8.50, confidence: 0.95}
  ]
}
```

**Safeguards:**
- 3x rule: if cost > 3x any reference price, flag as suspicious
- SCPRS is a CEILING, never a cost — if our cost > SCPRS, we're overpriced
- Amazon sale prices expire — always prefer list_price for quoting
- If cost = 0 after all sources: flag as `needs_pricing`, don't guess
- Store ALL source prices even if not used as cost — for human review

---

### Stage 4: MARKUP (automatic)

**Goal:** Apply intelligent markup based on item cost, category, and competitive positioning.

**Markup decision tree:**
```
1. Oracle recommendation exists and confidence = "high"?
   → Use oracle markup (calibrated from win/loss history)

2. SCPRS ceiling exists?
   → Calculate max markup: (scprs_price - cost) / cost
   → Apply: min(target_markup, scprs_ceiling_markup - 5%)
   → Never bid above SCPRS (we'd lose)

3. Default tier markup:
   → Items < $10: 25-30%
   → Items $10-50: 20-25%
   → Items $50-200: 15-20%
   → Items > $200: 10-15%

4. Apply safety buffer from tier selection (None/+10%/+15%/+20%)
```

**Safeguards:**
- NEVER auto-markup above SCPRS price (guaranteed loss)
- If cost * markup > SCPRS: reduce markup to fit under SCPRS with 5% buffer
- Minimum margin: $0.50 per item or 5%, whichever is greater
- Maximum markup: 500% cap (prevents runaway pricing from bad cost data)
- All markup decisions logged with reason for audit

---

### Stage 5: HUMAN REVIEW (target: under 2 minutes)

**What the user sees:**

For each item, a single row with:
- **Description** (from buyer's 704 — never modified)
- **Sources column**: All price references as chips with confidence badges
  - `EXACT` = locked in, don't look at this
  - `~FUZZY` = needs a glance, might be wrong match
  - `reject ×` button on non-EXACT matches
- **Cost field**: Pre-filled if confidence >= 0.75, empty if not
- **Markup %**: Pre-filled from oracle/tier logic
- **Our Price**: Calculated (cost * markup)
- **Profit**: Per-item and total

**User actions needed:**
1. **Scan for red flags**: Any `~FUZZY` badges? Any empty costs? Any 500% markups?
2. **Fix exceptions**: Click reject × on bad matches. Paste URLs for unpriced items.
3. **Adjust markup**: Slide tier or edit individual percentages.
4. **Click "Save & Generate"**: One button → fills 704 PDF → ready to send.

**Items should be color-coded by action needed:**
| State | Color | User action |
|-------|-------|-------------|
| Fully priced (EXACT match, cost + markup set) | Green border | None — just review |
| Priced (STRONG match) | No border | Quick glance |
| Needs review (FUZZY match or low confidence) | Yellow border | Verify match, may need manual lookup |
| Unpriced (no match found) | Red border | Must look up manually |

**Target distribution for a typical 20-item PC:**
- 12-15 items: Green (auto-priced, no action)
- 3-5 items: Yellow (quick verify)
- 1-3 items: Red (manual lookup)
- 0 items: 500% markup disasters

---

### Stage 6: SEND (one click)

**What happens:**
1. Save all prices
2. Fill AMS 704 PDF with pricing
3. Generate Reytech formal quote (if applicable)
4. Attach to email draft
5. User clicks send

---

## LLM Validator Design (Stage 2g)

**When it fires:**
- Items with confidence < 0.75 after all other identification steps
- Items where the best match has a red "Low match" badge
- Items where cost = 0 (no pricing found)
- Max 5 LLM calls per PC (budget control)

**What it receives:**
```json
{
  "pc_description": "S&S Worldwide Mini Velvet Art Posters - 840614150049",
  "upc": "840614150049",
  "mfg_number": "",
  "qty": 4, "uom": "pk", "qty_per_uom": 100,
  "best_match": {
    "title": "Some Wrong Product",
    "price": 35.24,
    "confidence": 0.24,
    "source": "amazon"
  },
  "instruction": "Is this the correct product? Search for the UPC or description. Return the correct product name, a purchase URL (prefer Amazon), and the current price."
}
```

**What it returns:**
```json
{
  "is_correct_match": false,
  "correct_product": "S&S Worldwide Mini Velvet Art Posters, 4x6, Pack of 100",
  "correct_url": "https://www.amazon.com/dp/B07663Q1KX",
  "correct_price": 27.99,
  "correct_asin": "B07663Q1KX",
  "confidence": 0.92,
  "reasoning": "UPC 840614150049 maps to S&S item PS1399. Found on Amazon as pack of 100."
}
```

**API choice:** xAI Grok (has built-in web search — can verify products in real-time without SerpApi)

**Safeguards:**
- LLM results are TENTATIVE until user confirms (show as "AI suggested" chip)
- If LLM and existing match disagree, show BOTH — let user pick
- Rate limit: 5 calls per PC, timeout 15s per call
- Cost tracking: log tokens used per PC for budget monitoring
- Fallback: if Grok API is down, skip — don't block the pipeline

---

## Feedback Loop (Continuous Improvement)

### Explicit Feedback
- **Reject ×**: User rejects a match → stored in blocklist → never re-suggested
- **Use ⬇**: User accepts a match → stored as positive signal → boosts confidence
- **Price override**: User changes cost >40% from match → stored as implicit reject

### Win/Loss Feedback
- **Won quote**: Oracle calibration updated — this price/markup worked
- **Lost quote**: Oracle adjusts down — we were too expensive
- **No response**: Track as stale — may indicate wrong match or wrong pricing

### Catalog Learning
- Every confirmed match adds to `product_suppliers` (SKU cross-reference)
- Every new URL lookup writes back to catalog (price + supplier + SKU)
- Over time: more UPC hits, fewer fuzzy matches, less manual work

---

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Items auto-priced (no human intervention) | ~40% | 80%+ |
| Items needing manual lookup | ~30% | <10% |
| Time per PC (human) | 15-20 min | <2 min |
| Bad match rate (wrong product) | ~15% | <3% |
| Cost auto-fill accuracy | ~60% | 95%+ |
| Average confidence score | ~0.65 | 0.85+ |

---

## Implementation Priority

### Phase 1: Foundation (THIS SESSION — mostly done)
- [x] UPC extraction from descriptions + substituted column
- [x] Identifier-first matching (UPC → part# → fuzzy)
- [x] Supplier SKU reverse lookup
- [x] S&S → Amazon auto-resolution
- [x] Confidence badges (EXACT/FUZZY text labels)
- [x] Cost guard rail (no fill below 40% match)
- [x] Match rejection system (blocklist + penalty)

### Phase 2: LLM Validator
- [ ] xAI Grok API integration (`product_validator.py`)
- [ ] Confidence routing: items < 0.75 → LLM
- [ ] LLM result display as "AI suggested" chip
- [ ] Rate limiting + cost tracking

### Phase 3: Review UX
- [ ] Row color-coding by action needed (green/yellow/red)
- [ ] "X items need review" summary at top
- [ ] One-click "approve all green" action
- [ ] Inline quick-lookup for red items (search without leaving page)

### Phase 4: Feedback + Learning
- [ ] Win/loss outcome tracking → oracle calibration
- [ ] Catalog auto-enrichment from every confirmed lookup
- [ ] UPC/SKU cross-reference table growth tracking
- [ ] Weekly match quality report
