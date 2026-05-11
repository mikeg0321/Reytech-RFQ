# Quote.set_price — Retire vs Commit vs Hybrid

**Status:** Decision pending. Operator (Mike) decides.
**Author:** AI session 2026-05-11.
**Predecessor:** `docs/PLATFORM_QUOTING.md`, `docs/PLAN_ONCE_AND_FOR_ALL.md`, `project_renderer_canonical_subtotal_2026_05_11.md`.

---

## TL;DR

The pydantic `Quote` model + `Quote.set_price()` was designed as the canonical write path. **It never reached canonical status.** After tonight's PRs (#874/#881/#883/#884), `reconcile_items` (dict-side, `src/core/pricing_math.py`) is the operational substrate — called by 5 sites end-to-end (3 routes + enrichment + ingest reparse).

`Quote.set_price` has **exactly 1 production caller** (`quote_engine.apply_oracle_pricing`). The Quote model + adapter + 700 LOC of supporting code primarily serve PR #876's render-divergence test and the `quote_model_v2_enabled` flag (flipped True at 07:50Z tonight after a 6-day off-period).

**Three paths forward:**
1. **Retire** Quote.set_price + adapter. Simplest. Loses pydantic type safety.
2. **Commit** to it. Migrate `_do_save_prices` (~200 LOC refactor). Worth it only if a future surface needs typed Quote.
3. **Hybrid** (recommended). Keep Quote model + adapter as a READ-time consistency check. Don't migrate writes. Best of both — minimal maintenance, divergence test still pins coherence.

---

## Current state (2026-05-11 23:30Z, post-PR #884)

### What exists

| Component | Path | LOC | Status |
|---|---|---|---|
| `Quote` pydantic model | `src/core/quote_model.py` | ~700 | Live; flag-gated reads |
| `Quote.set_price()` | `quote_model.py:259` | ~70 | 1 caller (`quote_engine.apply_oracle_pricing`) |
| Adapter `from_legacy_dict`/`to_legacy_dict` | `quote_model.py:421-680` | ~260 | Hot path when flag on |
| `quote_adapter._is_enabled()` | `src/core/quote_adapter.py:35` | ~10 | Reads `quote_model_v2_enabled` flag |
| Divergence test | `tests/test_adapter_render_divergence.py` | ~110 | Pins subtotal_of agreement |
| `quote_model_v2_enabled` flag | `feature_flags` table | n/a | **True since 2026-05-11 07:50Z** |

### What actually drives prod accuracy

| Path | Caller count | What it does |
|---|---|---|
| `reconcile_items` (dict-side) | 5 callers | Forward-compute price from cost+markup; reverse-derive markup from cost+price; mirror to all 3 aliases |
| `canonical_unit_price` | 4 renderers (post PR #874) | Read-time canonical math; closes drift class at PDF stamp |
| `subtotal_of` | 4 renderers + 2 helpers | Single source of truth for billable subtotal |
| `Quote.set_price` | 1 caller | Same math as reconcile, plus pydantic validation + bid-price preservation semantic |

The dict-side path is the operational reality. Quote.set_price duplicates the math via pydantic Decimal precision + audit logging.

---

## Option 1: Retire (simplest)

### Scope
- Delete `src/core/quote_model.py` (~700 LOC).
- Delete `src/core/quote_adapter.py` (~120 LOC).
- Delete `tests/test_adapter_canonical_items_first.py` + `tests/test_adapter_render_divergence.py` (~250 LOC test).
- Refactor `quote_engine.apply_oracle_pricing` to call `reconcile_items` directly (~5 LOC change).
- Drop `quote_model_v2_enabled` flag from `feature_flags` table + remove the flag-check from any remaining caller.
- Update audit memory + KPI dashboard scope.

### Pros
- **One canonical substrate.** Mental model: pricing math lives in `pricing_math.py`. Done.
- **~1,000 LOC removed.** Reduces surface area for new contributors + agents.
- **No adapter conversion cost** on hot read paths (PC detail, RFQ detail, quote PDF generation).
- **Eliminates flag drift risk.** No `quote_model_v2_enabled` to flip back off if anything goes wrong.
- **Aligns architecture with operational reality.** The dict-side path is what production has been exercising for months.

### Cons
- **Loses pydantic type safety** as a future capability. If we later want API v2 with typed contracts or a structured tool surface for an LLM/MCP agent, we'd need to rebuild the model.
- **Loses `Quote._audit()` structured log.** Currently the only place emitting price-change audit events with full provenance. `reconcile_items` could grow this responsibility but doesn't today.
- **Sunk-cost waste.** PRs #826 (canonical-first read) and #876 (divergence test) and #874 (render canon) all built around the assumption that Quote-model is the formal contract. Retiring those investments.
- **Removes the divergence test.** That test currently catches any drift between dict-side `subtotal_of` and Quote-side rendering — losing it means a future bug could regress silently.

### Blind spots
- Are there any agents (`product_research`, `pc_enrichment_pipeline`, `growth_agent`) importing Quote types we'd miss? Need grep.
- Does any test fixture depend on Quote serialization? Need grep.
- The MCP server (`mcp_server.py`) — does it touch Quote types? Need check.

### Estimated effort
~1 day. Mostly mechanical deletion + 1 caller refactor + test cleanup. Risk: low (Quote model wasn't load-bearing).

---

## Option 2: Commit (heavy)

### Scope
- Migrate `_do_save_prices` (`routes_pricecheck.py:1973`) to use `Quote.set_price`. Same for `routes_rfq.update` + `routes_rfq_gen` autosave.
- That requires buffering per-item field changes (cost, markup, price, qty) into one Quote.set_price call per item per save.
- Audit `to_legacy_dict` coverage for every PC/RFQ field that survives the roundtrip (notes, supplier metadata, audit trail, enrichment summary, etc.).
- Build round-trip test fixtures pinning that every field a renderer reads survives the adapter conversion.
- Replace `reconcile_items` calls at enrichment + ingest reparse boundaries with Quote.set_price equivalents.
- Remove the flag — Quote becomes always-on canonical.

### Pros
- **Type safety end-to-end.** Pydantic catches decimal precision bugs, missing-field bugs, invalid-state transitions at write time.
- **Structured audit log.** Every price change emits a Quote._audit() event with full provenance — feeds the KPI dashboard's pricing-accuracy strip naturally.
- **Bid-price preservation baked into the model.** The `bid_price` parameter (PR #831) becomes the canonical way operators set bids; reconcile_items doesn't currently support this idiom.
- **Future API/MCP surface ready.** Typed Quote model exposes a stable contract for external integrations.
- **Aligns with the original architecture vision** in PLAN_ONCE_AND_FOR_ALL.

### Cons
- **~200 LOC refactor in `_do_save_prices`** alone. Plus equivalent in 2 RFQ save paths. Plus reconcile call-site migrations at 5 sites.
- **Adapter coverage gaps are the real risk.** `to_legacy_dict` today writes a Quote-shaped subset. Fields like `enrichment_summary`, `requirements_json`, `gmail_message_ids`, `_classification`, `tax_enabled`, `delivery_option`, `custom_notes`, `price_buffer` either need to be added to Quote or preserved-through-roundtrip via a passthrough mechanism. Hidden bugs here would silently drop operator data — exactly the failure mode that triggered the 2026-05-05 incident.
- **Adapter conversion cost on every read** (currently flag-gated; commit-mode makes it always-on). For a PC with 30 items, that's 30 LineItem constructions + 30 Decimal coercions per render. Not a bottleneck today but a non-trivial cost.
- **Test surface explosion.** Every PC/RFQ field that survives the roundtrip needs a pin. Probably 80+ new tests.
- **Lock-in.** Once committed, retiring becomes hard. Future contributors see the typed model + assume it's foundational.

### Blind spots
- `_classification` is a free-form dict written by `src/core/ingest_pipeline.py`. Does Quote.to_legacy_dict preserve it? Unverified.
- `tax_enabled` / `tax_rate` are PC-specific. Quote treats them as line-item or quote-level? Unverified.
- The `parsed.header` + `parsed.line_items` shape is buyer-original-from-PDF. Quote model collapses it. Is that lossless? Unverified.
- Concurrent operator + daemon writes: pydantic Quote is not thread-safe. The adapter must serialize. Today's dict-side path serializes via `_save_pcs_lock`. Quote-side equivalent?

### Estimated effort
~1-2 weeks. Phase 1 (audit coverage gaps) is the biggest unknown — could expand to a full audit doc of every PC/RFQ field.

---

## Option 3: Hybrid (recommended)

### Scope
- **Keep** Quote model + adapter + divergence test.
- **Don't migrate writes.** `_do_save_prices` and friends keep using `reconcile_items`.
- **Adapter stays flag-on** as a READ-TIME consistency check.
- The divergence test (PR #876) is the canonical pin: any drift between dict-side `subtotal_of` and Quote-side rendering trips CI.
- `Quote.set_price` retains 1 caller (quote_engine.apply_oracle_pricing) — it's the typed model for ORACLE-driven pricing, not operator-driven pricing.
- Document this division explicitly in `quote_model.py` docstring.

### Pros
- **Best of both.** Type safety as a verification lens, not a write-path requirement.
- **No write-path refactor risk.** Operator + daemon write paths stay on the proven `reconcile_items` substrate.
- **Divergence test catches drift cheaply.** Adapter-on render must equal subtotal_of(items) — invariant violations surface in CI before they reach prod.
- **Future flexibility.** If a need for typed quotes emerges (API v2, MCP tool surface), Quote model is still there.
- **Minimal maintenance.** Quote model exists; we don't actively grow it; we don't break it.
- **Aligns with the post-incident reality.** Tonight's heal + PRs proved `reconcile_items` is the canonical substrate. Don't fight that.

### Cons
- **Two substrates coexist.** Mental model has to hold both: writes via `reconcile_items`, reads validated via Quote adapter.
- **Quote.set_price stays at 1 caller.** Doesn't grow into a foundational role.
- **Some sunk-cost remains.** The 700 LOC + adapter machinery stays around without being a primary path.
- **Adapter is the runtime cost.** With flag on, every PC/RFQ read converts through pydantic. For prod's 27 active PCs + 8 active RFQs, negligible. For a future scaled tenant, would re-evaluate.

### Blind spots
- The adapter's read-time conversion is unobserved today. Does it silently strip fields the renderer would otherwise see? Render-divergence test catches subtotal drift but not field-presence drift. Add a complementary test that asserts every field in `pc.json` survives `from_legacy_dict → to_legacy_dict`.
- If `quote_engine.apply_oracle_pricing` is rarely called in prod, Quote.set_price's 1-caller status may shrink to 0. Then it's truly dead code in an active model.

### Estimated effort
**~0.** This is the current state. Just document the doctrine.

---

## Recommendation: Hybrid

**Why:** Tonight's PRs make `reconcile_items` the operational canon. The Quote model has built-in pydantic validation that's valuable as a verification lens — keep the lens, don't fight the operational reality.

**Concrete actions if you accept Hybrid:**
1. Add a docstring to `src/core/quote_model.py` explicitly stating: *"This module is a VERIFICATION lens, not the canonical write path. Use `pricing_math.reconcile_items` for write paths. The Quote model + adapter validate read-time consistency via `tests/test_adapter_render_divergence.py`."*
2. Add a complementary test: `test_adapter_field_preservation.py` — asserts every field in a representative PC `data_json` survives `from_legacy_dict → to_legacy_dict`. Catches silent field-strip bugs.
3. Update `MEMORY.md` index entry for the 2026-06-07 routine (which you already cancelled) — note that the Quote.set_price 14-site rollout was a misframing; the right substrate (`reconcile_items`) is now end-to-end.
4. Don't migrate `_do_save_prices` to Quote.set_price.
5. Don't grow new callers of Quote.set_price beyond `apply_oracle_pricing`.

**When to revisit:**
- If you build an API v2 with typed contracts → reconsider Commit.
- If the adapter starts producing false divergences in prod → reconsider Retire.
- If you build an MCP/LLM tool surface that needs typed quotes → reconsider Commit.

---

## Decision framework

| Question | Retire | Commit | Hybrid |
|---|---|---|---|
| Do you want pydantic type safety today? | No | Yes | Read-only |
| Do you have ~1-2 weeks for the migration? | n/a | Yes | n/a |
| Is API v2 or MCP tool surface on the roadmap soon? | No | Yes | Maybe |
| Are you OK with two substrates coexisting? | No (one path) | No (one path) | Yes |
| What's your tolerance for sunk-cost decisions? | High (delete) | Low (keep) | Medium (keep, don't grow) |
| Render-divergence test value? | Lose it | Keep it | Keep it |

---

## What this analysis does NOT cover

- The 64 orphan orders surfaced tonight — those are a separate substrate problem (SCPRS + Gmail + Drive cross-reference). See task #26.
- The KPI dashboard build — independent of this decision. See `docs/KPI_DASHBOARD_SCOPE.md`.
- The remaining 5 lint-exempt files (routes_v1, revenue_engine, quotes_backfill) — those are status writers, not price writers. See PR-η Phases 5+.

---

**My recommendation is Hybrid.** If you choose Retire, I can ship that as 2-3 PRs over a half-day. If Commit, I'd start with a field-coverage audit before any refactor — that audit might surface enough adapter gaps to flip the decision.
