# Plan — Coleman multi-facility distribution list (punch-list #3 ship-to + #4 combined tax)

**Status:** proposed — Architect (LAW 4) sign-off required before Phase 1.
**Motivated by:** Coleman 10842771 punch-list #3 + #4; operator directive 2026-05-28.
**Governs alongside:** §0 LAW 6 amendment "READ THE WHOLE CONTRACT" (PR #1190).

---

## 1. The bug, stated correctly

#3 ("ship-to should say *Various Locations, CA* + a distribution list") and #4
("combined per-facility weighted tax") are **two consumers of one missing
substrate: per-facility allocation data.** That data was **never missing from
the contract** — it is the buyer's supplemental **AMS 701B "Purchase Order
Distribution List"** attachment. The 704B even points at it: row 4 reads
`***PLEASE SEE ATTACHED DISTRIBUTION LIST`.

The failure is a **LAW 6 ingest mis-classification**, not a data gap:

- The 701B's columns are literally labeled `LINE ITEM NUMBER / QUANTITY / UNIT
  OF MEASURE / DESCRIPTION`, so the multi-attachment Vision union
  (`src/core/ingest_pipeline.py:1077` `_multi_attachment_vision_union`)
  ingested its **21 rows as 21 phantom line items** (sequential `line_number`
  1..21, qty 1 each).
- **That is exactly the 21 per-facility line items Option D (PR #1179) has to
  re-aggregate back to 2 by MFG#.**
- The `Facility / Address / Zip / Contact` columns had nowhere to land in the
  line-item schema, so they were **discarded** — producing a single-facility
  ship-to and a single-jurisdiction tax on a 21-jurisdiction order.

Ground truth committed alongside this plan:
`tests/fixtures/coleman_10842771/ams701b_distribution_list.pdf` (+ `.parsed.json`).
21 rows · 19× `8700-0893-01` + 2× `LF03699` · 21 distinct facilities, each with a
full address (→ zip → tax jurisdiction).

A new/better parser is **not** required: `pdfplumber.extract_tables()` reads the
701B cleanly (it is a ruled table). The fix is classification + a schema slot to
hold what we already read.

## 2. Substrate seams (verified, with citations)

**Ship-to (singular today):**
- `src/spine_bridge/ingest.py:371` `_resolve_canonical_ship_to(facility)` — one
  facility per contract.
- `EmailContract.ship_to_facility` / `ship_to_address` (`src/spine/email_contract.py:179`).
- No per-line ship_to on any line-item model (`email_contract.py:87`
  `ContractLineItem`; `src/spine/model.py:171` `LineItem`;
  `src/core/quote_contract.py:85` legacy `LineItem`).
- Render seam: `src/spine/quote_pdf.py:567` `_addresses_block()` — single block.

**Tax (single rate today):**
- `Quote.tax_rate_bps` (`src/spine/model.py:310`); `tax_cents` derived
  (`model.py:433`, banker's round of `subtotal × bps / 10_000`).
- Resolved once from one facility at `quote_contract.py:644`
  `_resolve_tax_for_facility`. `src/core/tax_resolver.py:118` `resolve_tax(address)`
  already works per-arbitrary-address — **reusable per facility as-is**.
- Legacy `LineItem.tax_rate_bps_override` exists (`quote_contract.py:96`) but is
  unused; this plan does **not** revive it.
- Render: `quote_pdf.py:276` pulls the derived `tax_cents` — **no render change**
  if we store one (weighted) `tax_rate_bps`.

**Facility resolution check (done):** 13/15 sampled codes resolve with zip;
**`LAC` and `SAC` return None** (missing bare-code aliases). All have `tax=None`
(no operator-verified rate seeded). → key tax + schedule off the **701B address
column** (robust), not the bare code; patch the two aliases for display.

## 3. Phased implementation

### Phase 0 — Fix the ingest mis-classification (the LAW 6 fix; the teeth)
- **Classify** the AMS 701B: header match (`PURCHASE ORDER DISTRIBUTION LIST` /
  `AMS 701B`) **and** the cross-reference trigger — any parsed form whose text
  says "see attached distribution list" / "supplemental" / "schedule" forces a
  locate-and-parse of the referenced attachment (this is now §0 LAW 6).
- **Parse** it via `pdfplumber.extract_tables()` (no Vision) into structured
  rows: `{line, qty, uom, mfg_number, facility_code, facility_address, zip,
  contact, email}`.
- **Stop** emitting those rows as line items — they are allocations, not SKUs.
  Likely lets **Option D's 21→2 re-aggregation retire** (the 2 real SKU rows
  come from the RFQ/704B form). **Verify before removing — do not auto-delete.**
- **Disposition manifest:** record, per attachment, parsed-or-classified-non-RFQ.
  This is the artifact the LAW 6 forcing function asserts on.

### Phase 1 — Schema (LAW 4 — Architect sign-off in the PR)
- Add `EmailContract.distribution: list[DistributionRow]` (carry through Spine
  `Quote`). Additive field; no migration of existing quotes.
- Sanction it in `tests/spine/test_spine_architecture.py`.

### Phase 2 — #3 ship-to "Various Locations"
- `quote_pdf.py:567` `_addresses_block()`: `len({d.facility for d in
  distribution}) > 1` → header `"Various Locations, CA"`; append a
  **Distribution Schedule** page that re-renders the 701B (facility · address ·
  SKU · qty). Presentation only — we already hold the data verbatim.

### Phase 3 — #4 weighted tax
- `compute_weighted_tax_bps(distribution)`: per row `resolve_tax(row.address)` →
  weight by `row.qty × unit_price` → banker's-round to bps → store the single
  result in `Quote.tax_rate_bps` (render unchanged). Unresolvable address →
  **block the quote** (LAW 6), name the facility.

### Phase 0.5 — facility_registry alias patch
- Add `LAC` / `SAC` aliases so the schedule shows canonical names (tax already
  covered via address).

## 4. Scope boundary (what does NOT change)
- **704B untouched** — already handles multi-facility via the buyer's "see
  distribution list" note + Option D MFG# aggregation. This feature is the
  **formal Reytech Quote PDF + contract substrate only.**
- Tax compute/render math unchanged — we only feed `tax_rate_bps` a weighted value.

## 5. Decisions still open for the operator/Architect
1. **Does Option D retire** once the 701B parses as distribution (verify, then confirm)?
2. **Distribution schedule rendering:** appended page on the quote (recommended)
   vs inline vs re-attach the buyer's 701B.
3. **Weighted-rate display:** single blended line (e.g. `Tax (8.41% blended)`,
   recommended) vs per-facility tax lines.

## 6. Tests / forcing function (LAW 6)
- **`test_ingest_reads_all_attachments`** (the LAW 6 forcing function): every
  attachment has a recorded disposition; a parsed form whose cross-reference
  target was never parsed FAILS THE BUILD.
- **Canary** (`tests/test_coleman_10842771_canary.py`): feed
  `ams701b_distribution_list.pdf` → assert `EmailContract.distribution` has 21
  rows matching `ams701b_distribution_list.parsed.json`, **zero phantom line
  items**, ship-to == "Various Locations, CA", blended rate == a hand-computed
  figure.
- Unit: `compute_weighted_tax_bps` worked example; "Various" trigger at 1 vs 2
  facilities; unresolvable facility → block.
- Chrome: rendered quote header + Distribution Schedule page (UI verify rule).

## 7. Risks / notes
- **Biggest risk is the ingest reshaping** (turning 21 phantom items into a
  distribution + 2 real SKU rows) — guard with the canary before touching Option D.
- District add-ons CDTFA misses → operator-verified `facility.tax_rate` override
  already honored (`tax_resolver.py:162`); pre-seed the 21 CDCR facility rates or
  the blended number is slightly off.
- This is a **multi-PR, Architect-gated** effort (Phase 1 schema = the LAW 4 PR).

## 8. Parked options (do not adopt yet)
- **LiteParse 2.0** (run-llama OSS local parser): on ice. pdfplumber + Vision
  cover the structured CA forms we receive today; LiteParse's layout-preserving
  approach gives *less* structure for clean ruled tables like the 701B, and its
  recommended path for hard tables is the **cloud** LlamaParse (buyer-doc egress
  — operator decision). **Pull out only if/when we start receiving scanned,
  photographed, or odd-layout attachments that pdfplumber + Vision fail on.**
