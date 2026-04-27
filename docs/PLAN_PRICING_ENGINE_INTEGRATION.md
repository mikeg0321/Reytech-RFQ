# Plan — wire category-intel `danger` signal into pricing engine

**Created:** 2026-04-27 (overnight after Phase 4.6 ship)
**Status:** Design — awaiting Mike's pick on aggressiveness flavor before build.
**Why now:** Phase 4.6 made the category loss/win signal *available* and *visible*. The next compounding step is making the engine *use* it — turn surfaced intel into adjusted pricing recommendations.

---

## What's already in place

After PRs #573-#580 (live in prod 2026-04-27):

- `intel_category(description) → (category_id, label)` — fine-grained classifier
- `/api/oracle/category-intel?description=X[&agency=Y]` — returns `{danger, win_rate_pct, warning_text, ...}`
- `/api/oracle/item-history` — already returns `category_intel` sub-object inline (PR #578)
- PC-detail 📊 Hist modal now renders the banner (PR #580 — pending Mike's visual verify)

The signal is reachable from any caller. The pricing engine isn't a caller yet.

## The pricing engine entry point

`src/core/pricing_oracle_v2.py:1047`:

```python
def _calculate_recommendation(cost, market, quantity,
                              category=None, agency=None, _db=None)
```

This is the function that returns the recommended sale price for a line item. It currently:
1. Reads `oracle_calibration` table for (coarse_category × agency)
2. Optionally blends with `institution_pricing_profile`
3. Returns `{markup_pct, recommended_price, confidence, scope, sample_size, rationale}`

The `category` arg here is the COARSE classifier from `_classify_item_category()` (medical/office/janitorial/...). The fine `intel_category()` from Phase 4.6 is NOT consulted.

## Three flavors of integration

### Flavor A — Auto-lower markup when `danger=true`

**Behavior:** When `intel_category(description)` returns a danger=true bucket, the engine multiplies its computed markup by a damping factor (e.g., 0.5x, capped at +5% over cost).

**Example:**
- Engine computes: 22% markup → $122.00 on $100 cost
- Intel says: footwear-orthopedic, danger=true (12.9% bucket rate)
- Engine RETURNS: 11% markup → $111.00

**Pros:**
- Zero operator effort — engine recommendation already reflects the intel
- Direct KPI lever — competitive bids on known loss buckets

**Cons:**
- Mike loses visibility into WHY the markup dropped (banner shows it, but tight integration could mask the reason)
- Risk of overcorrection on small-n buckets that crossed the n=5 floor
- Changes the existing pricing function's contract — many callers depend on its current shape

### Flavor B — Suggest alternative markup, Mike accepts

**Behavior:** Engine returns its current recommendation UNCHANGED, plus a NEW `suggested_alternative` field that contains the danger-adjusted markup. UI shows both side-by-side; Mike clicks to swap.

**Example response shape:**
```json
{
  "markup_pct": 22.0,
  "recommended_price": 122.00,
  "suggested_alternative": {
    "markup_pct": 11.0,
    "recommended_price": 111.00,
    "rationale": "Category bucket has 4/31 wins. Suggest tightening to ~10%."
  }
}
```

**Pros:**
- Doesn't change existing function contract — additive only
- Mike retains full control + sees the rationale
- Reversible if the suggestion is wrong (just don't accept)
- Naturally surfaces the "two-price" decision at the moment that matters

**Cons:**
- Requires UI render in PC-detail (each row's price cell needs a "swap" button)
- Multiple buckets per quote → dozens of decisions per session → friction
- Mike's existing autosave logic must handle the new field gracefully

### Flavor C — Block bid entirely on known losers

**Behavior:** When opening a PC with multiple footwear items, engine refuses to compute a markup and surfaces a hard "DO NOT BID — historical loss bucket" gate. Operator must explicitly override.

**Example:**
- PC has 5 Propet shoes → engine returns `block: true`, no price computed
- Modal shows: "5 of 5 items in known loss bucket. Bidding here historically loses 87% of the time. Override?"
- Mike can override per-item or skip the whole bid

**Pros:**
- Strongest KPI lever — prevents bidding on losers entirely
- Matches the "stop quoting orthopedic footwear" actionable insight Mike already articulated

**Cons:**
- Risk of false positives blocking legitimate work
- Adds friction at the worst possible moment (operator already invested time)
- Needs a clean override path that doesn't itself add 30s/quote

## Recommended sequencing

1. **Flavor B first.** Lowest risk, additive-only response shape, retains operator control. Build it backend-only first (no UI change), expose via response field, then wire UI. ~3-4 PRs.
2. **Then a per-buyer toggle for Flavor C.** Some buyers (CDCR Sacramento, footwear-heavy) get the hard block; others get the suggestion. Use `agency_config.py` to gate behavior by agency.
3. **Flavor A (auto-lower) only after 30+ days of B telemetry.** Need to see whether Mike actually accepts the suggestions before letting the engine apply them silently.

## Open questions

1. **Damping factor for B's suggested alternative:** linear (markup × 0.5)? Match the median competitor price from prior losses (use `competitor_winning_prices` from item-history)? Use the WINNER price from the last loss?
2. **Multi-bucket quotes:** if a PC has 2 footwear losses + 8 incontinence wins, does the engine apply suggestion only to the loss items? Yes — per-item granularity, not per-quote.
3. **Trailing window vs all-time:** category-intel currently uses ALL quotes (back to 2022). Mike's recent rate is dropping (4.8% YTD 2026). Should the danger signal weight recent quotes more? Recommended: add a `?trailing_days=N` param, default 730 (2 years), let UI swap to "all time" via toggle.
4. **Override audit log:** every time Mike rejects a danger suggestion, log it. After 30 days, retrain thresholds against rejection patterns.

## Files that would change

- `src/core/pricing_oracle_v2.py` — add `intel_category` lookup + alternative markup calc inside `_calculate_recommendation()`
- `src/api/modules/routes_oracle_*.py` — endpoints that surface the rec already exist (4.6.1 inline + 4.6 dedicated) — possibly just add an `engine_alternative_markup` field
- `src/templates/pc_detail.html` — UI for "swap to suggestion" button
- `tests/test_oracle_recommendation.py` — new test file for the flavor B logic

## What NOT to do (yet)

- Don't ship flavor A immediately. Live pricing changes are reversal-expensive and Mike has been clear about the "fix at architecture layer" rule — making the engine quietly re-price would mask operator visibility.
- Don't add a 4th tier ("severe loss bucket" with rate < 5%). Two thresholds is already a lot of mental overhead. Hold the line at danger / WIN.
- Don't try to learn the damping factor from the data. Hand-tune first, observe acceptance rate, then consider learning. Premature optimization will hide bugs in the simpler hand-tuned version.

---

**For the next session:** Mike picks A/B/C aggressiveness. I build flavor B as default (3-4 PRs), wire the response field, and ship a feature-flagged UI render. Telemetry on acceptance rate informs whether to upgrade to A/C later.
