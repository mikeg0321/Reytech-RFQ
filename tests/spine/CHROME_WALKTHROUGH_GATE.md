# CHROME_WALKTHROUGH_GATE.md

The mandatory pre-ship gate for any quote-related code change in the
Spine substrate (or legacy quoting path) — by directive of Mike,
2026-05-15, after the substrate-meltdown that took out the CCHCS
$50K bid and burned the rfq_0ebe242f Russ RFQ.

> *"Build the catalog but not just this test, ALL TESTs. I'm willing
> to bet once you have the final result, one night of testing in
> chrome dev with mcp vision, you would find all bugs and the app
> would work as intended."* — Mike, 2026-05-15

## Why This Exists

| Date | Failure mode | What it cost |
|------|--------------|--------------|
| 2026-05-15 | 19 substrate failure classes in one day; both shipped quotes had `Tax $0.00` and required hand-overlay PDF edit; cost-basis on Item 2555 was $20.85 in app vs $6.68 online | likely $50K CCHCS bid lost |
| 2026-05-15 P0 | Single qty 2→3 edit on row 5 of e02b7fa6 flipped markup 35%→16% and mangled the whole quote | quote re-priced 3× |
| 2026-05-13 | "PR-I" looked complete in tests + memory; digest was empty in prod because it was wired to wrong endpoint | walkthrough found the gap that grep missed |
| 2026-05-05 | `pc.items` / `pc.line_items` alias drift dropped Mike's "Nads Hair Removal" line from UI even though it persisted in JSON | live UI data loss on a real Mike quote |
| 2026-04-23 | Bundle-5 + Bundle-6 shipped to prod with pytest template-render assertions as the only UI proof; `readonly` lock, modal submit, live math, KPI strip — none exercised in real Chrome | spawned the CHROME-VERIFIED hard rule |
| 2026-04-16 | Test-Simple-Submit flow burned 10 real quote numbers (R26Q34–R26Q43) | counter manually reset |
| 2026-04-16 | Fill engine shipped blank PDFs to prod for days because headless Chrome MCP couldn't render PDF form fields — every screenshot showed blank | upgraded to Opus 4.7 vision for PDF verification |
| 2026-04-03 | Multi-page 704 incident — 11 consecutive fix commits patching symptoms instead of diagnosing the shared root cause (hardcoded 8 rows vs actual 11) | spawned the three-strikes rule |

The pattern: **tests passed + grep was clean + the substrate was wrong.**
This gate is the operational fix — every quote-related ship now runs
the catalog in real Chrome (with Vision watching) before merge.

## Rules of the Gate

1. **The gate fires on ANY quote-related change.** Includes: Spine
   substrate, legacy `routes_rfq*.py`, `routes_pricecheck*.py`,
   `quote_generator.py`, form fillers (`reytech_filler_v4.py`,
   `cchcs_packet_filler.py`), template files under `src/templates/`,
   PDF rendering, ingest pipeline.
2. **Auto-tests are necessary but insufficient.** The pytest suite is
   the floor; the Chrome walkthrough is the ceiling. Both must pass.
3. **Pick the relevant W-### IDs** for your change, walk each one in
   Chrome DevTools MCP, take a screenshot per pass/fail.
4. **Commit footer must reference verified IDs:** e.g.
   `WALKTHROUGH-VERIFIED: W-M-001, W-S-003, W-U-007 (screenshots in _diag/walk_2026_05_16/).`
5. **PDF rendering changes MUST use real Chrome, not headless MCP.**
   Headless can't render form fields; vision can't catch what it
   can't see. Open the PDF in real Chrome on Mike's machine.
6. **No CHROME_VERIFIED_SKIP=1 to bypass this gate.** Per
   `feedback_chrome_mcp_deconflict`: if Chrome MCP is locked,
   *deconflict and wait*, don't bypass.
7. **No real quote numbers burned during testing.** Use the Russ Test
   fixture (`tests/spine/fixtures/legacy_russ_no_bid_test.json`) or
   any `is_test=True` record. Per `feedback_no_test_quotes`.

## How to Run

```bash
# 1. Boot the isolated local Spine server with seeded test fixture.
py -3.14 scripts/spine_serve_local.py
# Server runs on http://127.0.0.1:5057; Russ Test seeded at rfq_PREQ10846581_test

# 2. Open Chrome MCP, navigate to the editor:
#    http://127.0.0.1:5057/spine/quotes/rfq_PREQ10846581_test/edit

# 3. Walk the scenarios in the relevant W-### sections below.
#    For each: setup, action, snapshot, assert visible pass criteria,
#    save to _diag/walk_YYYY_MM_DD/W-###.png.

# 4. For PDF rendering scenarios: open the rendered PDF in real
#    Chrome (NOT MCP headless) and verify visually with vision.

# 5. For shadow-mode diff against legacy: run
#    py scripts/spine_shadow_diff.py --legacy-json <path>
#    Exit 0 = match.
```

---

# CATEGORIES

- **W-M**: Math / totals correctness
- **W-A**: Alias / field integrity (extra=forbid boundary)
- **W-S**: State machine / status transitions
- **W-P**: Persistence / event log / audit
- **W-I**: Ingest (email contract → Spine Quote)
- **W-U**: UI visual / operator surface
- **W-Q**: Quote PDF render
- **W-T**: Translator / legacy → Spine bridge
- **W-O**: Operator workflow / human factors
- **W-R**: Process invariants (every walk verifies these)

---

# W-M — Math / Totals Correctness

### W-M-001 — Tax = subtotal × rate, no zeroing branch
**Source:** 5/15 findings #13, #18 · `project_qa_quote_system_substrate_findings_2026_05_15`
**Bug class:** `shipping_option=included` branch zeroed `tax_cents` regardless of operator-set rate
**5/15 impact:** Both Mohammed e02b7fa6 + Today's CCHCS Quote PDFs shipped with `TAX $0.00`. Hand-overlay edit required to ship.
**Setup:** Open editor for `rfq_PREQ10846581_test`. tax_rate_bps = 775.
**Steps:** Verify KPI block shows `TAX (7.75%) $784.69`, not `$0.00`.
**Pass:** Tax > 0 whenever subtotal > 0. No combination of fields zeroes tax.
**Spine status:** ✅ Closed by construction (no `shipping_option` field exists).
**Automated:** `test_tax_cents_is_subtotal_times_rate_with_bankers_rounding`, `test_no_shipping_field_can_zero_tax`.

### W-M-002 — Banker's-rounded integer tax
**Source:** 5/15 finding #18 · 9e63456e manifest math
**Bug class:** Floor division produced $3,863.98 instead of manifest's $3,863.99
**Setup:** Quote with subtotal $46,836.20 at 8.25% (the 9e63456e fixture).
**Pass:** Tax displays $3,863.99 (not $3,863.98). `(4683620 × 825) // 10000` with banker's rounding.
**Spine status:** ✅ `Quote.tax_cents` uses round-half-to-even integer math.
**Automated:** `test_tax_cents_rounds_half_to_even`, `test_9e63456e_model_math_matches_manifest`.

### W-M-003 — Subtotal = Σ(qty × unit_price), nothing else
**Setup:** 7-row quote with mixed qtys.
**Pass:** Subtotal in KPI block = sum of Extension column values.
**Spine status:** ✅ `Quote.subtotal_cents = sum(li.extension_cents)`.

### W-M-004 — Total = subtotal + tax (NO shipping addend)
**Source:** Charter rule #7 · `feedback_pc_price_is_commitment`
**Bug class:** Legacy added shipping_amount to total in some code paths
**Pass:** KPI Total = Subtotal + Tax. Shipping line always $0.00.
**Spine status:** ✅ `Quote.total_cents = subtotal_cents + tax_cents`. No shipping field exists in the model.

### W-M-005 — qty edit does NOT touch markup
**Source:** P0 2026-05-15 · `project_qty_change_clobbers_markup_p0_2026_05_15`
**Bug class:** Legacy JS onChange recomputed markup from bid/cost when qty changed
**5/15 impact:** qty 2→3 on row 5 of e02b7fa6 flipped markup 35%→16%; whole quote mangled
**Setup:** Quote with cost $250, price $337.50, markup column shows 35.0%. Note KPI subtotal.
**Steps:** Change qty in row (e.g., 30→60). Save. Wait for reload.
**Pass:** Markup column STILL shows 35.0%. KPI subtotal/tax/total scaled by qty change.
**Spine status:** ✅ `markup_pct_display` is `@computed_field` from `(unit_price - cost)/cost`; qty is not in the formula.
**Automated:** `test_markup_pct_display_is_derived_not_stored`.

### W-M-006 — cost edit does NOT touch unit_price (operator-typed wins)
**Source:** 5/15 finding #2 · `canonical_unit_price` prefers `cost × markup_pct`
**Bug class:** Setting cost recomputed unit_price via stored markup_pct → silent override of operator's typed price
**Setup:** Quote with cost $250, price $337.50.
**Steps:** Change cost to $260. Save. Reload.
**Pass:** unit_price still $337.50 (operator-typed value preserved). Markup display updates to reflect new ratio.
**Spine status:** ✅ No `markup_pct` stored; `unit_price_cents` is the only sell field; cost edits don't reach unit_price.

### W-M-007 — Negative margin renders cleanly (doesn't crash)
**Setup:** Cost > unit_price (e.g., cost $400, price $337.50).
**Pass:** Markup column shows negative percentage (-16.9% or similar). No exception. KPI math still computes.

### W-M-008 — Zero-cost row shows markup as `—` not `inf%`
**Source:** `feedback_assert_sentinel_value_not_just_shape`
**Setup:** cost_cents = 0.
**Pass:** Markup column shows `—` (em-dash). No `Infinity`, no `NaN`, no Python error.
**Spine status:** ✅ `markup_pct_display` returns None when cost is 0.

---

# W-A — Alias / Field Integrity

### W-A-001 — POST with `bid_price` → 422
**Source:** 5/15 finding #1 · `project_persistence_p0_class_2026_05_12`
**Bug class:** Legacy autosave merged 4 unit_price aliases silently into state
**Test:** Open editor → DevTools console → manually POST a state with `bid_price: 99999` in line_items[0].
**Pass:** HTTP 422 with detail mentioning `bid_price` and `extra_forbidden`. Quote state in DB unchanged.
**Spine status:** ✅ Pydantic `extra="forbid"` boundary.
**Automated:** `test_extra_field_on_line_item_raises`.

### W-A-002 — POST with `price_per_unit` → 422
**Same setup as W-A-001 with `price_per_unit` field.**
**Pass:** 422 + state untouched.

### W-A-003 — POST with `our_price` → 422
**Pass:** 422.

### W-A-004 — POST with `shipping_option=included` → 422
**Source:** 5/15 finding #18 · `project_handoff_2026_05_15_substrate_meltdown_to_new_session`
**Bug class:** Field zeroed tax_cents in legacy QuoteContract; no operator-side write could clear it
**Pass:** 422 — field doesn't exist in Spine model.

### W-A-005 — POST with `tax_rate_pct` (legacy alias) → 422
**Source:** 5/15 finding #16
**Pass:** 422 — only `tax_rate_bps` accepted.

### W-A-006 — POST with `markup_pct` on line item → 422
**Source:** Charter rule #4
**Bug class:** Storing markup created the qty-clobbers-markup class. Spine derives markup, never stores.
**Pass:** 422 — `markup_pct` is a banned field on `LineItem`.

### W-A-007 — POST injecting `subtotal_cents`/`tax_cents`/`total_cents` → 422
**Bug class:** Legacy stored subtotal in dict; tampering propagated to PDF.
**Pass:** 422 — computed fields cannot appear in POST body.
**Automated:** `test_hand_injected_computed_field_in_db_raises_on_load`.

### W-A-008 — pc.items / pc.line_items / parsed.line_items 3-way drift impossible
**Source:** `feedback_pc_items_line_items_alias_drift` · 2026-05-05 incident
**Bug class:** Legacy stored line items under 3 alias names; writer that touched only one caused silent UI data loss
**Pass:** Spine model has ONE field: `line_items`. Cannot drift because there are no aliases.

### W-A-009 — GET response excludes computed fields
**Test:** `curl /spine/quotes/<id>` → parse JSON.
**Pass:** Response JSON does NOT contain `subtotal_cents`, `tax_cents`, `total_cents`, `extension_cents`, `markup_pct_display`. Only stored fields.
**Spine status:** ✅ `to_persisted_dict()` excludes computed fields.

### W-A-010 — UOM not in allowlist → 422
**Source:** 5/15 finding #6 · UOM ambiguity
**Test:** POST with `uom: "WIDGETS"`.
**Pass:** 422 with message pointing to SUPPORTED_UOM list.

---

# W-S — State Machine / Status Transitions

### W-S-001 — Status transition does NOT mutate line items (THE 5/15 P0)
**Source:** 5/15 finding #9 · `project_qa_quote_system_substrate_findings_2026_05_15`
**Bug class:** Legacy "Finalize Pricing" button reverted operator edits to a stale snapshot
**5/15 impact:** Mike typed markup=35 on row 5 of e02b7fa6, clicked Finalize, all rows reverted to mangled state (5-16% markups)
**Setup:** Editor on a `parsed` quote with cost $250, price $337.50, qty 30.
**Steps:** Click `Mark Priced`. Wait for reload. Note line item values. Click `Mark Finalized`. Reload.
**Pass:** Cost, price, qty, source URL all byte-identical to before each transition. Status pill updates.
**Spine status:** ✅ `with_status()` builds a new Quote with status changed and nothing else; line items pass through.
**Automated:** `test_with_status_does_not_mutate_line_items`.

### W-S-002 — `parsed → priced` requires `tax_rate_bps > 0`
**Source:** 5/15 finding #15 · MANDATORY INGEST GATE
**Bug class:** Legacy allowed quotes to reach `priced` without tax resolved; PDF rendered $0 tax silently
**Test:** POST a quote with `tax_rate_bps: 0` and `status: "priced"`.
**Pass:** 422 with message `tax_rate_bps > 0` required.

### W-S-003 — `priced → finalized` blocks on cost ≥ $100 without source URL
**Source:** 5/15 finding #19 · Item 2555 phantom cost class
**5/15 impact:** Bid item 2555 at $20.85 cost basis (no URL) vs $6.68 actual online; $14k phantom on $28k line; likely lost $50K bid
**Setup:** Editor on priced quote with cost $250, source URL cleared.
**Steps:** Click `Mark Finalized`.
**Pass:** Red error box: `state_transition_rejected — cost source on line_no=N (cost_cents=X >= 10000). Set cost_source_url OR cost_hand_validated_note.` Status stays at `priced`. Line items unchanged.
**Spine status:** ✅ Model validator fires on PRICED→FINALIZED.
**Automated:** `test_finalize_rejects_expensive_line_without_source`.

### W-S-004 — `priced → finalized` blocks on stale cost (> 30 days)
**Source:** Charter rule #8 · cost freshness
**Setup:** Quote with `cost_validated_at` 60 days ago.
**Pass:** Finalize blocked with `fresh cost_validated_at on line_no=N (within 30 days)` message.
**Automated:** `test_finalize_rejects_stale_cost_validation`.

### W-S-005 — `sent` is terminal — POST to sent quote → 409
**Setup:** Quote in `sent` status.
**Test:** POST any state change.
**Pass:** 409 — `state_transition_rejected — quote_id is already sent — terminal`.
**Spine status:** ✅ `write_quote` checks for prior sent state.
**Automated:** `test_sent_quotes_are_immutable`.

### W-S-006 — Illegal transition (`parsed → finalized` skipping priced) → 409
**Pass:** 409 with explicit list of allowed transitions.

### W-S-007 — `priced → parsed` allowed (rebid reopen)
**Source:** Rebid arc · `project_rebid_automation_queued_2026_05_15`
**Setup:** Priced quote.
**Pass:** Reopen-to-Parsed button works; status flips; line items intact.

### W-S-008 — `finalized → priced` allowed (operator catches mistake)
**Setup:** Finalized quote.
**Pass:** Reopen-to-Priced button works; status flips; line items intact.

### W-S-009 — `finalized → sent` REQUIRES an approved snapshot matching current state
**Source:** Mr. Wolf 2026-05-15 substrate fix · the structural enforcement of "the bytes shipped to the buyer are the bytes the operator approved"
**Bug class:** Operator edits a value AFTER finalize, hits Send, ships
the edited version. The buyer receives bytes that nobody explicitly
approved. The 5/15 hand-overlay-PDF workflow was the operator's
manual workaround for this gap.
**Three failure modes the gate closes:**

| Case | Pre-fix behavior | Post-fix behavior |
|---|---|---|
| No snapshot exists | sent succeeds, ships re-rendered bytes | 409 `state_transition_rejected — snapshot required` |
| Snapshot exists, state edited after | sent succeeds with edited bytes | 409 `state has diverged from latest approved snapshot` |
| Snapshot exists, state unchanged | sent succeeds (correct) | sent succeeds (correct, and bytes pulled from snapshot) |

**Setup:** Finalize a quote without snapshotting. Try `finalized → sent`.
**Pass:** 409 with detail mentioning "snapshot required". Snapshot.
Edit unit price. Try sent. 409 with detail "diverged from latest
approved snapshot (snap_X)". Re-snapshot. Try sent. 200.
**Spine status:** ✅ Enforced in `routes_spine.post_quote_state` via
`_identity_matches(latest_snapshot, quote)`.
**Automated:** `test_send_without_snapshot_rejected`,
`test_send_with_diverged_state_rejected`,
`test_send_after_resnapshot_succeeds`.

---

# W-P — Persistence / Event Log / Audit

### W-P-001 — Single Save = single event log entry (no fan-out)
**Source:** 5/15 finding #8 · no atomic save endpoint
**Bug class:** Legacy autosave fired multiple POSTs per row per save
**Test:** Edit cost, hit Save. Check `/events` endpoint.
**Pass:** Exactly one new event appended per Save click.
**Automated:** `test_one_post_writes_exactly_one_db_row`.

### W-P-002 — Event log is append-only
**Test:** Make 4 status transitions. GET events.
**Pass:** 4 events in order, oldest first. No edits or deletions to prior entries.
**Automated:** `test_event_log_appends_on_every_write`.

### W-P-003 — Event records actor + note + full state snapshot
**Setup:** POST with headers `X-Spine-Actor: operator`, `X-Spine-Note: priced at 35%`.
**Pass:** Event entry contains both header values + a full state dict.

### W-P-004 — Hand-edited DB row with injected computed field → load raises
**Source:** Defense in depth · finding #11
**Test:** SQLite-edit `state_json` to add `"subtotal_cents": 9999999`. GET the quote.
**Pass:** HTTP 500 / Pydantic ValidationError on load. Model refuses tampered state.

### W-P-005 — Silent state mutation impossible
**Source:** 5/15 finding #4 · `project_qa_quote_system_substrate_findings_2026_05_15`
**Bug class:** rfq_9e63456e had 5 of 7 rows wiped to $0 between morning and afternoon with zero operator activity
**Test:** Walk away for 30 min. Refresh. Compare state.
**Pass:** State identical. No background daemon mutates Spine rows. Event log shows no new entries.

### W-P-006 — Computed fields not in persisted JSON
**Test:** Save a quote, sqlite3-query the state_json column.
**Pass:** JSON dict does not contain subtotal_cents/tax_cents/total_cents/extension_cents/markup_pct_display.
**Automated:** `test_computed_fields_not_persisted_in_state_json`.

---

# W-I — Ingest

### W-I-001 — Tax-at-ingest is mandatory
**Source:** Charter rule #6 · 5/15 finding #15
**Bug class:** Legacy allowed parsed quotes to exist without tax; deferred resolution to operator
**Test:** Call ingest with a CDTFA resolver stub that returns None.
**Pass:** `IngestResult.ok == False`, error message references `tax_rate_bps` mandatory.
**Automated:** `test_ingest_fails_when_tax_resolver_returns_none`.

### W-I-002 — CDTFA resolver exception → ingest fails
**Pass:** Exception caught, recorded as `tax_resolver raised an exception`, ingest returns error.

### W-I-003 — PREQ prefix stripped from solicitation
**Source:** AV-1 substrate fix · `project_email_contract_substrate_pr_av_2026_05_14`
**Test:** Contract with `solicitation_number: "PREQ 10847262"`.
**Pass:** Spine Quote stores `10847262` (no prefix).

### W-I-004 — Agency literal enforced (CCHCS only in v1)
**Test:** Contract with `agency: "CalVet"`.
**Pass:** Ingest fails with agency-not-supported error.

### W-I-005 — Empty line items → ingest fails
**Pass:** Refuses to create a quote with zero rows.

### W-I-006 — Line item without description → ingest fails
**Pass:** Per-row description required.

### W-I-007 — UOM coercion handles legacy variants
**Test:** Line item with `uom: "Each"` / `"ea."` / `"EACH"`.
**Pass:** All coerce to `"EA"` in the Spine.

### W-I-008 — Vision parser → Spine Quote round-trip
**Setup:** Real PDF parsed by Vision → email contract dict.
**Pass:** Contract feeds cleanly into `ingest_email_contract`; produces parsed Quote.
**Status:** ⚠️ Vision wiring not yet built in src/spine_bridge/; deferred.

---

# W-U — UI Visual / Operator Surface

### W-U-001 — Description column wraps, does NOT overflow into QTY
**Source:** `feedback_text_width_overflow_check`
**Bug class:** Long product names overflowed cell, colliding with QTY values
**Setup:** Line item with description `"GLOVES, EXAM, NITRILE, POWDER-FREE, LARGE, 100/BOX"`.
**Pass:** Description wraps to multiple lines inside its cell. QTY column shows only the qty integer.
**Automated:** `test_long_descriptions_do_not_overflow_into_qty_column`.

### W-U-002 — KPI block accurate after Save
**Steps:** Edit unit_price. Save. Reload.
**Pass:** Subtotal/Tax/Total in KPI block match `qty × new_price` math.

### W-U-003 — Status pill matches current status
**Pass:** `parsed` = yellow, `priced` = blue, `finalized` = green, `sent` = gray. Text matches.

### W-U-004 — Status transition buttons match current status
| Current | Visible buttons |
|---------|-----------------|
| parsed | `Mark Priced` |
| priced | `Mark Finalized`, `Reopen to Parsed` |
| finalized | `Mark Sent`, `Reopen to Priced` |
| sent | (none — terminal) |

### W-U-005 — Editable inputs: qty, cost, source URL, unit_price
**Pass:** Each has an `<input>` element, not static text.

### W-U-006 — Read-only display: MFG#, description, UOM, markup, extension
**Pass:** These show as text. No input element. Cannot be edited directly (markup is derived; description/MFG#/UOM are buyer-given for now).

### W-U-007 — Save button shows toast/feedback + reloads
**Pass:** Click Save. "Saving…" appears. After ~400ms, page reloads with new state.

### W-U-008 — Source URL validates http/https shape
**Test:** Type `"not-a-url"` in source URL field. Save.
**Pass:** 422 with URL-shape error. State unchanged.
**Automated:** `test_url_shape_validated`.

### W-U-009 — Single POST per Save (no per-keystroke autosave)
**Source:** Charter rule #2 · 5/15 finding #8
**Test:** Open DevTools Network tab. Type in cost field. Tab away. Count POSTs.
**Pass:** Zero POSTs while typing. One POST when Save is clicked.
**Automated:** `test_no_autosave_hooks_in_spine_template`.

### W-U-010 — Reopen button reachable (not buried)
**Source:** 5/15 finding #12 · AC13 (PR #1028)
**Pass:** Reopen button in top action bar, not buried mid-page.

### W-U-011 — Action footer disclosure text present
**Pass:** Page footer reads "The Spine writes ONE row per Save. No per-keystroke autosave. Status transitions do NOT recompute line items. Tax and markup are derived from your typed values — they are never stored. Shipping is always $0.00 by design."

### W-U-012 — DOM access is null-safe
**Source:** JS guard rails 2026-03-31
**Pass:** Editor doesn't break on quote variants where optional fields are missing.

### W-U-013 — scrollIntoView does NOT yank focus
**Source:** `feedback_visual_verify_always`
**Pass:** During Save, page reloads without snapping the viewport to unexpected locations.

### W-U-014 — Live PDF preview pane reloads on Save
**Source:** Mr. Wolf 2026-05-15 substrate fix · industry pattern (Stripe, QuickBooks, Zoho)
**Bug class:** Operator edits a value, hits Save, has no in-app way
to see what the buyer will receive. The 5/15 incidents lived in this
blindspot — by the time anyone realized the PDF was wrong, the email
was sent.
**Setup:** Open editor on parsed quote. Right pane shows live PDF
preview with the current state. Note the displayed extension on
row 1 ($10,125.00 at qty=30).
**Steps:** Change qty to 60 in the left pane. Click Save.
**Pass:** Right pane reloads automatically. New extension $20,250.00
visible. KPI block on the left ALSO shows $20,250.00. They match.
**Spine status:** ✅ Iframe with cache-busting nonce on every save.

### W-U-015 — Locked banner on sent quotes; iframe points at snapshot bytes
**Source:** Mr. Wolf 2026-05-15 substrate fix
**Setup:** Quote in `sent` status with a snapshot.
**Pass:** Purple lock banner at top: "📌 LOCKED — ships snapshot
`snap_xxx_yyyy` · sha256 `abc123…` · captured … by operator". Right
pane iframe `src` is `/snapshot/<sid>/pdf`, not `/pdf`. Save button
disabled with tooltip "Sent is terminal — no further edits". Mark
Sent button absent (no transitions from sent in v1).
**Spine status:** ✅ Template branches on status; iframe URL set
server-side from `latest_snap`.

---

# W-Q — Quote PDF Render

### W-Q-001 — Render to valid PDF bytes
**Test:** `GET /spine/quotes/<id>/pdf`.
**Pass:** Response is `application/pdf`, starts with `%PDF-`, pypdf parses without exception.
**Automated:** `test_minimum_quote_renders_valid_pdf`.

### W-Q-002 — Reytech identity present
**Pass:** PDF contains "Reytech Inc.", "rfq@reytechinc.com", "949-229-1575".

### W-Q-003 — Solicitation #, facility, buyer agency rendered
**Pass:** "SATF Corcoran 93212" / "Test - CCWF Chowchilla" / "CCHCS" / "10846581" all visible.

### W-Q-004 — SHIPPING line always renders $0.00
**Source:** Charter rule #7
**Pass:** Every Quote PDF, regardless of quote, shows "SHIPPING $0.00". Template-level literal.

### W-Q-005 — TAX line shows rate% AND amount (never $0.00 on non-zero subtotal)
**Pass:** Tax row reads "TAX (X.XX%)" and the calculated cents.

### W-Q-006 — 9e63456e replay produces $50,700.19 total (manifest match)
**Setup:** Build the 9e63456e fixture (7 rows, subtotal $46,836.20, tax 8.25%).
**Pass:** PDF extracts back: `$46,836.20 / $3,863.99 / $0.00 / $50,700.19`.
**Automated:** `test_9e63456e_pdf_round_trip_renders_correct_totals`.

### W-Q-007 — Description text reflows in cell (no overflow into QTY)
**Pass:** Long descriptions wrap to multiple lines inside the cell. QTY column extracts cleanly as integers.

### W-Q-008 — XML-special chars (`&`, `<`, `>`) in description don't crash
**Source:** ReportLab Paragraph parses inline XML
**Test:** Description = `"STAPLER & PUNCH <heavy-duty>"`.
**Pass:** PDF renders. Text shows literal `&` and brackets.
**Automated:** `test_description_with_xml_special_chars_renders_safely`.

### W-Q-009 — Boundary cases: 1, 8, 9, 16, 17, 25 items
**Source:** CLAUDE.md PDF guard rails · 2026-04-03 incident
**Pass:** Each item count renders without overflow / missing rows / page break errors.
**Automated:** `test_renders_at_item_count_boundaries`.

### W-Q-010 — Anti-regression: TAX line never $0.00 on non-zero subtotal
**Source:** 5/15 5pm post-send catastrophe
**Test:** Render the 9e63456e PDF.
**Pass:** No line in the PDF text reads `TAX (X.XX%) $0.00` when subtotal > 0.
**Automated:** `test_9e63456e_pdf_has_no_zero_tax_pathology`.

### W-Q-011 — PDF visual eyeball (real Chrome, not headless)
**Source:** `feedback_visual_verify_always`
**Test:** Open PDF in real Chrome on Mike's machine. Apply Vision.
**Pass:** All fields filled. Money columns right-aligned. Cells contain only their intended content. No overlapping text. Date format readable. Signature block where expected.

### W-Q-012 — Print fidelity (letter-size, 0.75in margins)
**Pass:** Open in Chrome print preview. Page boundaries clean. No content clipped.

### W-Q-013 — Render-matching gate refuses lying bytes
**Source:** Mr. Wolf 2026-05-15 substrate fix · 5/15 TAX $0.00 incident
**Bug class:** Renderer emits a money string that disagrees with the
model (the literal `$0.00` shipped on both 5/15 quotes despite tax
being computed correctly in the model). No upstream code change
caught this — the bytes left the function and were emailed.
**Setup:** Build a Quote with subtotal $10,125 at 7.75% tax.
Monkey-patch `_totals_block` to hard-code `$0.00` for the TAX cell.
Call `render_quote_pdf(quote)`.
**Pass:** `SpineRenderMismatchError` raised. Error names the TAX
cell, shows expected `$784.69`, shows displayed `$0.00`. NO bytes
returned.
**Spine status:** ✅ `_verify_render_matches_model` re-extracts via
pdfplumber and compares cent-for-cent. Function is structurally
incapable of returning lying bytes.
**Automated:** `test_render_matching_gate_catches_zero_tax_injection`,
`test_render_matching_gate_catches_wrong_extension`,
`test_render_matching_gate_passes_on_correct_render`.

### W-Q-014 — Snapshot is byte-identical + immutable
**Source:** Stripe void-and-replace pattern · CHARTER `displayed == persisted == delivered`
**Bug class:** Re-render at send-time can drift from approved render
if upstream state changed silently (the 5/15 `rfq_9e63456e` 5-of-7-
rows-wiped class). Snapshot table holds the bytes that were
**approved**.
**Setup:** Finalize a quote. POST `/snapshot`. Get `snapshot_id`
and `sha256`. GET `/snapshot/<sid>/pdf`. Compute sha256 of returned
bytes.
**Pass:** Same sha256 as the snapshot row. `Content-Disposition`
inline. `X-Spine-Snapshot-Sha256` and `X-Spine-Snapshot-CreatedAt`
headers present. POST `/snapshot` again on unchanged state returns
the same `snapshot_id` (idempotent on identity).
**Spine status:** ✅ `write_snapshot` runs the render gate, then
INSERTs `pdf_bytes` + `sha256` + `state_json`. Append-only.
**Automated:** `test_snapshot_writes_one_row`,
`test_snapshot_is_idempotent_on_unchanged_state`,
`test_snapshot_bytes_are_byte_identical_on_read`,
`test_snapshot_changes_when_state_changes`,
`test_get_snapshot_pdf_returns_immutable_bytes`,
`test_get_snapshot_pdf_scope_check`.

### W-Q-015 — Sent quote ships the snapshot bytes, not a re-render
**Source:** Mr. Wolf 2026-05-15 substrate fix
**Bug class:** Even with a snapshot in hand, if the send path
calls `render_quote_pdf(read_quote(...))` it can produce different
bytes than what the operator approved (state changed between
approve and send; timestamp drift; render-engine non-determinism).
**Setup:** Finalize quote, snapshot, transition to sent. Open the
preview iframe.
**Pass:** Iframe `src` points at `/snapshot/<sid>/pdf`, not `/pdf`.
The bytes displayed are the exact snapshot bytes.
**Spine status:** ✅ Editor template branches on `quote.status ==
"sent"` and renders the snapshot URL. Sending logic (when wired)
will pull bytes via `read_snapshot` not `render_quote_pdf`.

---

# W-T — Translator / Legacy → Spine Bridge

### W-T-001 — Russ Test fixture translates cleanly
**Test:** `py scripts/spine_shadow_diff.py --legacy-json tests/spine/fixtures/legacy_russ_no_bid_test.json`.
**Pass:** Exit code 0. Output reads `CLEAN — Spine total matches legacy.` Subtotal/Tax/Total all delta $0.00.
**Automated:** `test_russ_fixture_translates_cleanly`.

### W-T-002 — `unit_price` wins over `bid_price` / `price_per_unit` / `our_price`
**Test:** Legacy dict with all 4 aliases set to different values.
**Pass:** Spine line item uses `unit_price` value; other aliases recorded as divergence warnings.

### W-T-003 — Derives unit_price from cost × (1 + markup/100) when no explicit price
**Pass:** Translation succeeds; issue recorded explaining the derivation.

### W-T-004 — Refuses to invent zero
**Test:** Legacy line item with no cost, no price, no markup.
**Pass:** Translation FAILS (not "produced unit_price=0"). Error explicitly says "Spine refuses to invent a zero".

### W-T-005 — `shipping_option` / `shipping_amount` / `delivery_option` dropped
**Pass:** Recorded as info-level audit notes. Not stored in Spine.

### W-T-006 — `markup_pct` dropped from every line item
**Pass:** info-level note per line. Spine's `markup_pct_display` is derived.

### W-T-007 — Translator is pure (no mutation of input)
**Test:** Deep-copy legacy dict, translate, compare.
**Pass:** Original dict byte-identical to copy.

### W-T-008 — UOM legacy variants canonicalize
**Pass:** "Each" / "EA." / "EACH" all map to "EA". "Pack" → "PK". "Dozen" → "DZ".

---

# W-O — Operator Workflow / Human Factors

### W-O-001 — Quote drafted faster in Spine UI than legacy
**Pass:** Operator timing — same fixture, draft + price + finalize. Spine should be ≤ legacy time. Pure UX comparison.

### W-O-002 — No test quotes burn real R26Q numbers
**Source:** `feedback_no_test_quotes`
**Pass:** Spine local server seeds `rfq_PREQ10846581_test`; counter is isolated. Verify no R26Q counter increment in legacy DB during Spine testing.

### W-O-003 — Operator can paste a vendor URL → cost field updates? (DEFERRED)
**Status:** ⚠️ URL-paste not yet built in Spine. Charter says atomic refresh OR no-op when implemented. Tracked in `project_url_paste_substrate_macro_2026_05_12`.

### W-O-004 — Operator can rebid (parsed → priced → ... → sent → reopen?) (DEFERRED)
**Status:** Spine v1 makes sent terminal. Rebid arc is `project_rebid_automation_queued_2026_05_15`; v2 deliverable.

### W-O-005 — Operator can see event log audit
**Test:** Click "View event log (JSON)" link.
**Pass:** Returns full append-only history. Each entry has timestamp + actor + status + state.

---

# W-R — Process Invariants (Every Walk Verifies)

### W-R-001 — Real Chrome verification, not headless MCP for PDFs
**Source:** `feedback_visual_verify_always` · 2026-04-16 blank-PDF incident
**Pass:** PDF rendering scenarios use real Chrome on Mike's machine, not MCP headless screenshot.

### W-R-002 — Chrome MCP for UI scenarios — deconflict, don't bypass
**Source:** `feedback_chrome_mcp_deconflict`
**Pass:** When MCP is locked, wait for the other window. Do NOT skip with `CHROME_VERIFIED_SKIP=1`.

### W-R-003 — Walkthrough beats abstract review
**Source:** `feedback_walkthrough_audit_beats_abstract_review_2026_05_13`
**Pass:** Tests passing + grep clean + memory says "done" → walk it anyway. PR-I shipped to prod with the digest wired to the wrong endpoint despite passing those gates.

### W-R-004 — Three strikes on a fix → STOP
**Source:** CLAUDE.md three-strikes rule · 2026-04-03 11-commit-spiral
**Pass:** If a single walkthrough scenario fails 3× in a row, stop trying to fix it. Audit the root cause from scratch.

### W-R-005 — Audit every file in the commit before pushing
**Source:** CLAUDE.md · 2026-04-10 7-files-committed-with-DOCX-fix
**Pass:** `git show --stat HEAD` after commit. Verify no unintended files (.env, runtime JSON, agent files) snuck in.

### W-R-006 — App is source of truth, not memory
**Source:** `feedback_app_is_source_of_truth`, `feedback_memory_decay_verify_first`
**Pass:** Before claiming a deadline or status, verify against the live record, not a memory entry. The 5/15 "Russ missed" memory was wrong and burned 6 hours; the actual PDF said 4pm today.

### W-R-007 — Displayed == Persisted == Delivered
**Source:** `feedback_production_ready_definition`
**Pass:** What the operator sees in the UI is what's in the DB is what the buyer receives via email. Any divergence is a bug.

### W-R-008 — Boundary tests at 1, 8, 9, 16, 17, 25 items
**Source:** AMS 704 multi-page incident
**Pass:** Every PDF / table renderer change runs the 6 boundary item counts. Page breaks especially.

### W-R-009 — Never burn real quote numbers in tests
**Source:** `feedback_no_test_quotes`
**Pass:** Test fixtures only. If a test must touch prod counter, reset immediately via admin endpoint.

### W-R-010 — Worktree required for parallel Claude windows
**Source:** CLAUDE.md `feedback_use_worktrees`
**Pass:** Each parallel Claude session owns its own worktree directory. WORKSTREAMS.md row updated.

---

## Coverage Audit

| Category | Scenarios | Automated | Manual-only |
|----------|-----------|-----------|-------------|
| W-M Math | 8 | 7 | 1 (negative margin display) |
| W-A Alias | 10 | 9 | 1 (3-way drift) |
| W-S State | 9 | 8 | 1 (status pill colors) |
| W-P Persistence | 6 | 5 | 1 (silent mutation 30-min wait) |
| W-I Ingest | 8 | 6 | 2 (Vision wiring DEFERRED) |
| W-U UI | 15 | 6 | 9 (visual) |
| W-Q PDF | 15 | 11 | 4 (real Chrome + visual) |
| W-T Translator | 8 | 8 | 0 |
| W-O Workflow | 5 | 1 | 4 (operator timing/feel) |
| W-R Process | 10 | 0 | 10 (gates on the gate itself) |
| **TOTAL** | **94** | **61** | **33** |

**The bet:** if the 33 manual-only scenarios all pass in one night of
Chrome MCP + Vision testing, the Spine substrate has structurally
closed every known bug class. Anything that fails is a real bug
caught before prod.

---

## Adding New Scenarios

When a new bug class surfaces:
1. Add a `W-X-###` entry in the relevant category.
2. Reference the source memory file.
3. Write the steps + pass criteria.
4. Add an automated test under `tests/spine/` if possible.
5. Update the coverage table.

This catalog is **append-only**. Bug classes that are structurally
closed by Spine v2/v3/etc. stay listed with status `✅ Closed by
construction` — the entry documents what the substrate prevents and
how to verify the prevention still holds.
