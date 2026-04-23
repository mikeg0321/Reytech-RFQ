# Bid Package Generation Audit — 2026-04-23

## Finding

`generate_bid_package()` at `src/forms/reytech_filler_v4.py:3255` ONLY calls
three fillers (703B, 704B, bid-package-buyer-template). It does NOT append
the standalone state forms that Mike's north-star submission includes:

- **DVBE 843** — 4 cycles (Signature1-4_PD843, Date1-4_PD843, Text1-17_PD843, Check1-4_PD843). Generator exists: `generate_dvbe_843`.
- **STD 21** (bidder declaration) — 9 text fields + 1 signature. Fillers exist: `fill_bidder_declaration` / `generate_bidder_declaration`.
- **CalVet CUF** (Commercially Useful Function) — 12 check boxes + signature + date + DBA name. Fillers exist: `fill_cv012_cuf` / `generate_barstow_cuf`.
- **CalRecycle 074 item table** — Item Rows 1-6 + Product/SABRC columns. Filler exists: `fill_calrecycle_standalone`.
- **Darfur Act** — standalone `generate_darfur_act`.
- **STD 204/205** — fillers exist: `fill_std204`, `fill_std205`, `generate_std205`.
- **GenAI 708** — `fill_genai_708`.
- **OBS 1600** — `fill_obs1600`.
- **STD 1000** — `fill_std1000`.
- **Drug Free** — `generate_drug_free`.

Current generated bid package: **6 pages** (intro + state-agency header + bid number + sign page + CERTIFICATION + blank).
North star bid content: **~14 pages** including all the standalone forms above.

## Root cause

Two compounding issues:

1. **`generate_bid_package()` has a narrow scope.** It fills the buyer-provided
   bid package template (one PDF covering intro/certification/misc) but does
   NOT assemble the full set of standalone forms Reytech submits.

2. **`routes_rfq_gen.py` generate-package route DOES call the standalone
   fillers** (grep shows `fill_std204`, `fill_bidder_declaration`, etc.
   referenced throughout), but those calls are gated on:
   - `_include(form_id)` — which depends on agency `required_forms`
   - Agency config for CCHCS lists `[703b, 704b, bidpkg, quote]` as required
     and `[sellers_permit, dvbe843, std204, calrecycle74, bidder_decl,
     darfur_act, obs_1600, drug_free, std1000, cchcs_it_rfq]` as OPTIONAL
   - Per `feedback_form_filling`: "Optional forms are OPTIONAL. Never auto-
     include based on item count or heuristics."

The operator must have historically been checking the optional-forms boxes
manually to include DVBE/STD21/CUF/CalRecycle. Without explicit operator
opt-in, those forms don't get added → generated bundle is sparse vs north
star.

## Proposed fix (PR-B scope)

**Option A (minimum blast radius):** Auto-check the CCHCS standard subset
(DVBE 843, STD 21, CalRecycle 074) for shape=cchcs_it_rfq when operator
hasn't explicitly opted out. Keep CUF / Darfur / OBS 1600 opt-in (they
are contextual — CUF for CalVet, OBS for food, etc.).

**Option B (bigger lift):** Redefine required_forms per SHAPE not per
AGENCY. `cchcs_it_rfq` shape's required forms = `[703b-equivalent (the LPA
itself), dvbe_843, std_21, calrecycle_074, bidpkg-filler, quote]`. This
collapses with audit items K/N/W-shape-not-agency. Higher leverage, more
regression surface.

## Page-by-page north star inventory (for fixture-backed regression)

From `tests/fixtures/rfq_packages/10840486_rfq_package_NORTHSTAR.pdf`:

| Page | Content |
|------|---------|
| 1-5 | LPA IT RFQ (filled Reytech form) — fixable by Bundle-4 + PR-A |
| 6 | Solicitation header / Attachment index |
| 7 | Attachment 1 — Bidder Certification |
| 8 | Attachment 6 — AMS 708 GenAI Notification |
| 9-12 | CCHCS Certifications |
| 13 | Attachment 8 |
| 14 | STD 21 — Bidder Declaration |
| 15 | STD 21 CalVet CUF (?) — To be completed by State agency |
| 16 | Bid number / vendor block |
| 17 | (mostly blank — might be wrap page) |
| 18 | CERTIFICATION |
| 19 | (blank) |
| 20 | CA Sellers Permit (from sellers_permit filler) |

## Recommendation

**Do NOT blindly modify `generate_bid_package()` tonight.** Every change
here touches form-fill code which has historically caused 11-commit
failure spirals (2026-04-03 incident). Instead:

1. **This PR (PR-B):** ship the audit findings + enhanced regression tests
   that lock in current behavior (so future changes measure deltas clearly).
2. **Follow-up PR:** Implement Option A or B with fixture-backed test per
   standalone form (assert page count, assert field presence, assert
   signature placement).
3. **Mike reviews** this audit before code PR to decide which standalone
   forms auto-include for cchcs_it_rfq.

## Code-change scope when approved

- **`generate_bid_package()`** — extend to call `generate_dvbe_843`,
  `generate_bidder_declaration`, `fill_calrecycle_standalone` for
  `shape=cchcs_it_rfq`. Concatenate with `pypdf.PdfWriter.append`.
- **`routes_rfq_gen.py`** generate-package — auto-check the subset when
  `_classification.shape == "cchcs_it_rfq"` (and flag CCHCS agency).
- **yaml** `cchcs_it_rfq_reytech_standard.yaml` — add field mappings for
  DVBE 843 / STD 21 cycle fields so fill values come from the profile.
- **`agency_config.py`** — move DVBE 843 / STD 21 / CalRecycle 074 from
  `optional_forms` to `required_forms` for CCHCS (per Option B).

Regression tests MUST compare generated bundle to north-star fixture
page-by-page (extend `scripts/northstar_gap_test_10840486.py` as a
pytest fixture, not just a one-shot script).

## Dependencies

Blocks audit item BB (signature placement) — those signatures live on the
standalone forms that aren't currently included. PR-B must ship first,
then PR-C (signature placement per-page) can compare to north-star
confidently.
