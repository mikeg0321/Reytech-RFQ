# SAC Walkthrough — 2026-05-16

Chrome MCP walkthrough for the first live Spine bid (CCHCS SAC,
sol# 10847457, due 5/18 5pm PT). Closes Mike's mandatory Chrome
walkthrough gate ([[feedback-chrome-walkthrough-gate-before-quote-ship-2026-05-15]])
before any quote ships.

Screenshots → `_diag/walk_2026_05_16/`. Commit footer on Phase 4 ship:

    WALKTHROUGH-VERIFIED: W-PKG-001..W-PKG-006 (screenshots in _diag/walk_2026_05_16/)

## Prereqs

- Gmail OAuth circuit CLOSED (prod) ✅ verified via `railway logs --filter gmail`
- 4 .emls in `_diag/bids_2026_05_16/` ✅ pulled via /api/admin/export_eml
- `python scripts/parse_bid_emls.py` run → 4 EmailContract candidates surfaced + reviewed
- SAC EmailContract written to spine_email_contracts via /api/admin/spine/seed-contract (or direct script)
- SAC Quote written to spine_quotes (parsed status) via ingest_email_contract

## Scenarios

### W-PKG-001 — Contract review
1. Navigate `/spine/quotes/<rid_sac>/contract` (auth via DASH_PASS).
2. **Screenshot** `01_contract.png`
3. Verify: solicitation_number, buyer_name/email, facility=SAC, required_forms=["703b","704b","bidpkg","quote"], parse_confidence="high".
4. If any field is wrong → STOP. Don't ship. Re-run parser; update contract.

### W-PKG-002 — Contract-vs-Quote diff (initial)
1. Navigate `/spine/quotes/<rid_sac>/contract-diff`.
2. **Screenshot** `02_diff_initial.png`
3. Verify: deltas list shows operator hasn't priced yet (every line `info` severity, cost=0, unit_price=0).
4. No `error`/`warning` severities at this point.

### W-PKG-003 — 2-pane editor — price all lines
1. Navigate `/spine/quotes/<rid_sac>/edit`.
2. **Screenshot** `03_editor_empty.png`
3. For each line:
   - Operator types cost (cents), unit_price (cents), pastes source URL or hand-validated note.
   - Markup column displays derived markup_pct (computed, never stored).
4. Verify: changing qty does NOT change markup column (closes 5/15 qty-clobbers-markup class).
5. Click Save → single POST → `/spine/quotes/<id>/state`.
6. **Screenshot** `04_editor_priced.png`
7. Status transitions parsed → priced (validated by Quote model — tax_rate_bps must be > 0).

### W-PKG-004 — /package gate
1. Navigate `/spine/quotes/<rid_sac>/package`.
2. **Screenshot** `05_package_response.png`
3. Verify JSON:
   - `quote_id` matches
   - `contract_id` matches
   - `required_forms`: ["703b","704b","bidpkg","quote"]
   - `response_packaging`: "separate_pdfs"
   - `files`: 4 entries, each with form_code + filename + url
4. Click each `url` in turn (open in new tab) — verify each PDF renders:
   - 703B: Reytech identity in headers, sol# 10847457
   - 704B: per-line items with operator-priced values
   - bidpkg: 14-page bundle, identity fields filled
   - quote: Reytech Quote PDF, tax line non-zero, total = subtotal + tax
5. **Screenshots** `06_703b.png`, `07_704b.png`, `08_bidpkg.png`, `09_quote.png`

### W-PKG-005 — Status → finalized + snapshot
1. Back in editor, click Finalize → POST /state with status="finalized".
2. Quote model fires preconditions: every line cost_source present, cost_validated_at within 30 days.
3. **Screenshot** `10_finalized.png` (lock banner appears)
4. Click "Snapshot" → POST /snapshot.
5. Renderer's matching gate runs (`_verify_render_matches_model`); pdfplumber re-parses bytes; cent-for-cent comparison vs Quote model.
6. **Screenshot** `11_snapshot_event.png` (event log entry with snap URL + sha256)
7. Verify snapshot bytes are byte-identical to what the operator approved.

### W-PKG-006 — Mark Sent (snapshot precondition fires)
1. Click "Mark Sent" → POST /state with status="sent".
2. Backend checks `_identity_matches(latest_snapshot, quote)` → must pass.
3. Snapshot bytes (NOT a re-render) become the canonical sent artifact.
4. **Screenshot** `12_sent.png`
5. Verify Gmail compose URL surfaced with snapshot PDF as attachment.
6. Open URL in Gmail, attach the bytes, send to argarin@cchcs.ca.gov.

## Pass criteria

- All 12 screenshots saved
- Every PDF visually matches the Quote model on screen
- /package response.files length == contract.required_forms length (4)
- Tax line on Quote PDF == subtotal × tax_rate_bps / 10000 (banker's rounded)
- No SpineRenderMismatchError, no SpineFormFillError, no 409s
- Gmail Sent thread has the snapshot PDF (visually verify it's the same bytes)

## Fail responses

- Wrong field on contract → re-run parser, update EmailContract via /contract POST (NOT a contract-as-spec violation; the contract is THE spec — if Vision parsed wrong, we fix the parser).
- /package returns 409 renderer_missing → known-deferred form requested; update contract.required_forms to subset Spine can render, OR register the missing renderer (substrate work).
- /package returns 409 output_contract_mismatch → substrate invariant violation; STOP, file as bug, never paper over.
- SpineRenderMismatchError on snapshot → the renderer is lying about its bytes; do NOT bypass the gate; investigate the divergence first.

## After SAC ships clean

Repeat the same script for VSP, CHCF, Hooks PC (Phases 5, 6).
Update memory with first-live-Spine-bid date + any ergonomic fixes
discovered. Cannibalization roadmap day-21 gate fulfilled.
