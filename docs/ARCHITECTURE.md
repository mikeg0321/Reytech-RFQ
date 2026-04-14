# Reytech-RFQ Architecture

*Updated 2026-04-14 after the unified-ingest refactor (PR #47).*

## One-sentence summary

A Flask app on Railway that takes buyer requests from California state
agencies (CCHCS, CalVet, CDCR, DGS, DSH, CalFire), prices the items
via a layered identification pipeline, and generates signed
compliance packages ready to email back.

---

## Top-level flow

```
┌──────────────────────────────────────────────────────────────────┐
│  INPUT: email (with attachments) | manual upload | API post     │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                             ▼
            ┌──────────────────────────────┐
            │  classify_request()           │  ← PR #47 Phase 1
            │  src/core/request_classifier │    feature-flagged
            │                               │
            │  returns:                     │
            │   shape (cchcs_packet /       │
            │          pc_704_docx /        │
            │          pc_704_pdf_docusign /│
            │          pc_704_pdf_fillable /│
            │          generic_rfq_xlsx /   │
            │          generic_rfq_pdf /    │
            │          generic_rfq_docx /   │
            │          email_only)          │
            │   agency (cchcs, calvet, ...) │
            │   required_forms              │
            │   confidence                  │
            │   reasons (audit trail)       │
            └────────────┬─────────────────┘
                         │
                         ▼
      ┌─────────────────────────────────────┐
      │  process_buyer_request()             │  ← PR #47 Phase 2
      │  src/core/ingest_pipeline            │
      │                                      │
      │  1. classify                         │
      │  2. dispatch parser by shape         │
      │  3. create PC or RFQ record with     │
      │     _classification stored on it     │
      │  4. triangulated linker (requires    │
      │     >=2 of agency/sol/inst/items)    │
      └────────────┬────────────────────────┘
                   │
       ┌───────────┴───────────┐
       │                       │
       ▼                       ▼
┌──────────────┐         ┌──────────────┐
│  PC record   │         │  RFQ record  │
│  (quote      │         │  (full       │
│   only)      │         │   package)   │
└──────┬───────┘         └──────┬───────┘
       │                        │
       ▼                        ▼
   ENRICHMENT PIPELINE (both paths)
   ┌─────────────────────────────────┐
   │ UPC lookup                       │
   │ ↓                                │
   │ MFG# lookup                      │
   │ ↓                                │
   │ Supplier SKU                     │
   │ ↓                                │
   │ ASIN / Amazon                    │
   │ ↓                                │
   │ Catalog fuzzy match              │
   │ ↓                                │
   │ Grok LLM validator  ◄─ flag-gated│
   │ ↓                                │
   │ SCPRS ceiling                    │
   │ ↓                                │
   │ Pricing Oracle V5 recommendation │
   │ ↓                                │
   │ Markup + buffer                  │
   └─────────────────────────────────┘
       │                        │
       ▼                        ▼
   FILL + PACKAGE GENERATION
   ┌─────────────────────────────────┐
   │ PC: 704 fill                     │
   │ RFQ: 704B + compliance forms +   │
   │      quote PDF                   │
   │ CCHCS: 22-page packet w/ 7       │
   │        attachments spliced       │
   └─────────────────────────────────┘
       │
       ▼
   VALIDATION GATES
   ┌─────────────────────────────────┐
   │ form_qa.run_form_qa               │
   │  - field verification             │
   │  - signature check                │
   │  - 704B computation audit         │
   │  - buyer-field contamination      │
   │  - overlay bounds self-check      │
   │  - Email-as-Contract validation   │
   │                                   │
   │ package_completeness_gate         │
   │  - every required form present    │
   │  - every required form passes QA  │
   │                                   │
   │ CCHCS-specific gate (9 checks)    │
   │  - pricing floor/ceiling          │
   │  - extension arithmetic           │
   │  - attachment completeness        │
   │  - preference pair correctness    │
   │  - Amount field regression guard  │
   │  - cert validity                  │
   │  - signature overlays present     │
   └─────────────────────────────────┘
       │
       ▼
   OUTPUT: filled PDFs + draft email
       │
       ▼
   OPERATOR REVIEW on /pricecheck/<id> or /rfq/<id>
   (classification banner + Oracle badge now visible)
       │
       ▼
   SEND → mark won/lost → pricing_oracle_v2.calibrate_from_outcome
                                            │
                                            ▼
                              OUTCOME FEEDBACK LOOP (Oracle V5)
                               - won_quotes table
                               - buyer_preferences
                               - confidence-weighted pricing
                               ↑
                               └─ feeds back into next enrichment
```

---

## Key module ownership

| Module | Owns |
|---|---|
| `src/core/request_classifier.py` | Single source of truth for "what is this request?" |
| `src/core/ingest_pipeline.py` | Classify → parse → create → link orchestration |
| `src/core/quote_request.py` | Canonical wrapper over PC/RFQ records (fixes field-path drift) |
| `src/core/pricing_oracle_v2.py` | Oracle recommendations, win/loss calibration |
| `src/core/flags.py` | Runtime feature flags (30-second threshold hotfixes) |
| `src/core/utilization.py` | Event recorder for the internal dashboard |
| `src/core/agency_config.py` | **Authoritative** required-forms set per agency |
| `src/core/pc_rfq_linker.py` | Legacy fuzzy linker (still used when classifier flag is off) |
| `src/forms/price_check.py` | AMS 704 parser + filler (PC path) |
| `src/forms/cchcs_packet_parser.py` | CCHCS 18-page packet parser |
| `src/forms/cchcs_packet_filler.py` | CCHCS packet filler + attachment splicer |
| `src/forms/reytech_filler_v4.py` | Generic form fillers (STD 204, CV 012, DVBE 843, etc.) |
| `src/forms/form_qa.py` | Field + signature + overlay verification |
| `src/forms/package_completeness.py` | Agency-agnostic completeness gate |
| `src/agents/pc_enrichment_pipeline.py` | Layered pricing identification (PC path) |
| `src/agents/product_validator.py` | Grok LLM validator (confidence < 0.75 fallback) |
| `src/agents/email_poller.py` | Gmail polling + initial classification |

---

## Where bugs historically hide

| Symptom | Usual root cause |
|---|---|
| Wrong PC linked to RFQ | Fuzzy linker matched on single signal (agency OR sol OR items, not AND) — fixed by triangulated linker in PR #47 |
| Form fields empty on output | pypdf appearance stream not regenerated — need `/NeedAppearances=True` on AcroForm |
| Overlay text 20-40pt off | DOCX→LibreOffice producer uses different cell geometry than DocuSign — fixed by `col_rects` detection method + regression tests in PR #43 |
| Signature missing | `_add_signature_to_pdf` dispatched to wrong page OR `fill_and_sign_pdf` already ran and the overlay path double-fired |
| Required form missing from package | Agency config / `_include()` branch disagreed with dispatcher — fixed by completeness gate in PR #46 |
| Prices wildly wrong | QPU vs per-unit confusion OR stale Oracle price OR SCPRS line-total not divided by qty — all caught by the CCHCS gate + pricing tests |
| Email poller crashed on a new buyer format | Parser chain fell through to vision parser which hit an API key issue — feature-flag the vision path |
| Bulk fix "broke something on the other side" | PC and RFQ parallel paths diverged — classifier_v2 unifies them at the source |

---

## Deployment + resilience

- **Ship path:** `make ship` (pre-push tests + push + auto PR) → CI (~3-5 min for checks, ~12-13 min for tests, 20-min ceiling) → `make promote` (merge + Railway auto-deploy + smoke test)
- **Auto-rollback:** `scripts/railway_deploy_watcher.py` launched in background by `make promote` — if Railway reports the deploy FAILED for >=90s AND /ping returns non-200, auto-triggers `make rollback-to` to the previous SUCCESS deploy
- **Feature flags:** `POST /api/admin/flags` — 30-second hotfixes for thresholds, kill switches, behavior toggles
- **Smoke test:** 49 assertions against prod after every promote, runs in ~12s

---

## What's explicitly NOT in scope

- Buyer-side frontends (Reytech is the supplier, not the buyer)
- PO fulfillment / carrier tracking (separate Orders V2 workstream)
- Invoicing / AR / QuickBooks (manual)
- User auth beyond Basic Auth (single-operator app)
- Non-CA state procurement

---

## Next reads for new sessions

1. `project_rfq_remaining_deliverables.md` — current backlog snapshot
2. `project_unified_ingest_live.md` — the refactor context
3. `project_reytech_canonical_identity.md` — canonical values
4. `project_packet_formats.md` — which agencies issue packets vs individual forms
5. `feedback_*` files — guard rails from prior incidents
