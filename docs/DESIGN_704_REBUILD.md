# Design: 704 Fill Path Rebuild

**Date:** 2026-04-15
**Status:** Approved for Phase 1 implementation (pending Mike review)
**Prerequisite:** Read `docs/HANDOFF_2026-04-15_704_REBUILD.md` first — this doc builds on it.
**Depends on:** `docs/DESIGN_QUOTE_MODEL_V2.md` (canonical Quote model)

---

## Problem Statement

The AMS 704 fill path has been the source of 80% of rewrites and issues. Seven PRs
shipped in one day (#88–#94) chasing serial bugs across rendering, parsing, template
detection, and QA — each fix breaking something else. The root cause is architectural:
15+ modules independently guessing form structure, field conventions, and coordinate
math against buyer-variant PDFs.

**Impact:** Mike is missing quote deadlines. In a side business with <48h turnarounds
where the goal is 2–4 quotes per hour, a broken fill path is a direct revenue blocker.

---

## Design Principles (from Q&A decisions)

1. **PCs use clean Reytech template.** Form-field fill only. No overlay, no detection,
   no coordinate math. PyPDFForm with `flatten=True` for universal viewer compatibility.

2. **RFQs use minimal overlay on buyer's form.** Buyers want zero disruption to their
   process. Write ONLY: unit prices, extensions, subtotal, company info block, signature.
   Preserve everything else in the buyer's original file.

3. **Auto-fill 90%.** Reytech vendor info (name, address, FEIN, cert, contact, signature)
   is always auto-filled. Buyer info pre-filled from parser. Only pricing is manual review.

4. **Profile-driven, not code-driven.** Each form variant is a YAML profile declaring
   field mappings. The fill engine reads the profile — no hardcoded field names in Python.

5. **Content-based detection, not filename-based.** SHA-256 of sorted AcroForm field
   names identifies the profile. `identify_attachments` is deleted.

6. **Boot-time validation.** Every profile validates against its blank PDF at startup.
   Missing fields = app refuses traffic. No more silent "35/35 fields not found."

---

## What Changes

### PC Path (clean template fill)

**Before:** `fill_ams704()` → 2500 lines → `_fill_pdf_fields()` + `_fill_pdf_text_overlay()`
+ `_detect_page_layout()` + `_detect_row_y_positions()` + `_append_overflow_pages()` with
reportlab canvas drawing.

**After:** `fill_engine.fill(quote, profile) → bytes`. One function, profile-driven.

- Input: `Quote` object (pydantic, from `DESIGN_QUOTE_MODEL_V2.md`) + `FormProfile` (YAML)
- PyPDFForm fills the Reytech blank template using profile field map
- `flatten=True` generates appearance streams (every viewer renders correctly)
- Overflow: pre-add `Row1_3..Row11_4` to blank, clone pages as needed
- Output: flat PDF bytes, ready to serve

### RFQ Path (minimal overlay on buyer's form)

**Before:** Same `fill_ams704()` with `original_mode=True` → reportlab overlay on buyer PDF
→ coordinate detection → hardcoded fallbacks → font rendering bugs.

**After:** `fill_engine.fill(quote, profile) → bytes` with `fill_mode: overlay`.

- Profile declares the ~15 fields to write (prices, subtotal, company block)
- Profile declares exact coordinates per field (measured from the actual buyer PDF)
- Reportlab draws ONLY those declared fields — no detection, no guessing
- Everything else in the buyer's PDF is untouched
- If no profile matches the buyer's form → fallback to Simple Submit (Phase 0)

### What Gets Deleted (Phase 3, after shadow-mode burn-in)

| Function/Constant | File | Lines | Why |
|---|---|---|---|
| `_fill_pdf_text_overlay()` | `price_check.py` | ~700 | Replaced by profile-driven overlay |
| `_detect_page_layout()` | `price_check.py` | ~200 | Replaced by profile field fingerprint |
| `_detect_row_y_positions()` | `price_check.py` | ~300 | Coordinates in profile YAML |
| All `_HC_PG1_*` / `_HC_PG2_*` constants | `price_check.py` | ~50 | Coordinates in profile YAML |
| `_cell()`, `_cell_right()`, `_multiline()` | `price_check.py` | ~100 | Reportlab helpers → profile coords |
| PR #89/90 overlay tricks | `price_check.py` | ~80 | Symptoms of wrong approach |
| `identify_attachments()` | `rfq_parser.py` | ~150 | Content-based fingerprint replaces it |
| `fill_and_sign_pdf()` | `reytech_filler_v4.py` | ~400 | Profile-driven sign mode |
| `ROW_FIELD_TEMPLATES_704A/B` | `ams704_helpers.py` | ~40 | Move to profile YAMLs |

**Total: ~2000 lines deleted.** Replaced by ~200 lines in `fill_engine.py` + ~50 lines
per YAML profile.

### What Gets Kept

| Component | File | Why |
|---|---|---|
| PDF item parsing | `parse_price_check.py` | Solid, handles buyer variants |
| `_fill_pdf_fields()` | `price_check.py` | The actual pypdf form-field fill — works |
| PR #88 AcroForm preservation | `price_check.py` | Correct fix, needed for any multi-page |
| `ams_704_blank.pdf` | fixtures | Extended with `_3`/`_4` fields |
| `REYTECH_INFO` | `price_check.py` | Vendor constants (move to config later) |
| `HEADER_FIELDS`, `SUPPLIER_FIELDS` | `price_check.py` | Move to profile YAML |

---

## Template Extension

The Reytech blank template (`ams_704_blank.pdf`) currently has:
- Page 1: 11 unsuffixed row fields (Row1–Row11)
- Page 2: 8 suffixed row fields (Row1_2–Row8_2)
- Total capacity: 19 items

**Extension:** Pre-add `Row1_3..Row11_3` and `Row1_4..Row8_4` on cloned page 2 copies.
New capacity: 38 items before needing dynamic overflow. Covers 95%+ of real bids.

Done programmatically with PyPDFForm at build time, committed as a new fixture.

---

## Library: PyPDFForm

**Decision:** Add PyPDFForm (Q8 answer). Key capabilities:

- `PdfWrapper("blank.pdf").fill({"field": "value"})` — clean field API
- `flatten=True` — generates appearance streams, every viewer renders correctly
- Built-in field introspection — powers boot-time profile validator
- ~2k lines, maintained, PyPI available

**Integration:**
- `pip install PyPDFForm` → `constraints.txt`
- PyPDFForm for fill + flatten on AcroForm profiles
- pypdf stays for page manipulation / merging
- reportlab stays for overlay-mode profiles (flat/scanned buyer PDFs)

---

## Feature Flag

`QUOTE_MODEL_V2` environment variable:
- `false` (default): legacy path runs, new path shadow-runs and logs diffs
- `true`: new path runs, legacy path shadow-runs for rollback safety

Flip after 20 consecutive real bids with zero divergence in shadow mode.

---

## Test Plan

1. **Unit:** Every profile validates against its blank PDF (boot-time + pre-push)
2. **Contract:** Every profile parses + fills + re-parses a golden Quote round-trip
3. **Integration:** `fill_engine.fill()` for every profile produces a QA-passing PDF
4. **Regression:** Golden fixtures from real incidents (CCHCS 704B + morning PCs)
5. **Visual:** pdfplumber text snapshot vs committed expected text

Items 1–3 run in pre-push hook. Items 4–5 run in CI.

---

## Migration Sequence

1. Phase 1: Quote model + profile registry skeleton (no fill changes)
2. Phase 2: Parse engine + fill engine in shadow mode (both paths run, legacy serves)
3. Phase 3: QA engine + package engine + route migration (new path serves, legacy shadows)
4. Phase 6: Burn-in (2 weeks, 20+ real bids clean)
5. Phase 7: Delete old path (separate PR)

**At no point does the user see degraded output.** Shadow mode catches divergence
before any user-facing change.
