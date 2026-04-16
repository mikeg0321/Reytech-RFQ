# Design: Canonical Quote Model + Form Profile Registry

**Date:** 2026-04-15
**Status:** Approved for Phase 1 implementation (pending Mike review)
**Supersedes:** Loose `rfq` / `pc` dicts throughout the codebase
**References:**
- `docs/HANDOFF_2026-04-15_704_REBUILD.md` (root cause analysis)
- `docs/PRD_PRICING_PIPELINE.md` (pricing stages — unchanged)
- `docs/PC_TO_RFQ_WORKFLOW.md` (F1–F11 gap list — reconciled below)
- `docs/SYSTEM_ARCHITECTURE.md` (domain boundaries — unchanged)

---

## Problem Statement

Every consumer of PC/RFQ data does `r.get("field_name", "")` with different
fallback semantics. There is no schema, no validation, no computed fields.
Consequences from the last month:

- PR #94: `NameError: _package_incomplete_reasons` — undefined variable in a 2000-line function
- PR #92: `templates` dict not populated on re-upload — one code path sets it, another doesn't
- `update-field` endpoint omits `payment_terms`, `bill_to`, `contact_name` — hardcoded allowlist
- Re-uploading a buyer PDF nearly wiped operator pricing (dict overwrite, no merge)
- `tax_enabled` checked in 3 separate paths (no single source of truth)
- PC→RFQ conversion bugs from field-name drift between schemas

---

## Layer 1: Canonical Quote Model

`src/core/quote_model.py` — pydantic v2 hierarchy.

```
Quote
├── header        QuoteHeader
│   ├── solicitation_number: str
│   ├── agency_key: str
│   ├── institution_key: str
│   ├── due_date: date | None
│   ├── due_time: time | None
│   ├── due_time_explicit: bool = False
│   ├── payment_terms: str = "Net 45"
│   ├── shipping_terms: str = "FOB Destination"
│   └── delivery_days: str = "7-14 business days"
│
├── buyer         BuyerInfo
│   ├── requestor_name: str
│   ├── requestor_email: str
│   ├── requestor_phone: str
│   ├── department: str = ""
│   └── notes: str = ""
│
├── ship_to       Address
├── bill_to       Address
│
├── vendor        VendorInfo  (Reytech static, from config)
│   ├── name: str = "Reytech Inc."
│   ├── representative: str = "Michael Guadan"
│   ├── email: str = "sales@reytechinc.com"
│   ├── address: Address
│   ├── phone: str
│   ├── fein: str
│   ├── sellers_permit: str
│   ├── sb_cert: str = "2002605"
│   ├── dvbe_cert: str = "2002605"
│   └── signature_image_path: str
│
├── line_items    list[LineItem]
│   └── LineItem
│       ├── line_no: int
│       ├── item_no: str           # MFG#, not ASIN
│       ├── description: str       # Buyer's original (PC) or catalog (RFQ)
│       ├── qty: Decimal
│       ├── uom: str = "EA"
│       ├── qty_per_uom: Decimal = 1
│       ├── unit_cost: Decimal = 0  # Real supplier cost
│       ├── markup_pct: Decimal = 35
│       ├── unit_price: Decimal     # @computed: cost * (1 + markup/100)
│       ├── extension: Decimal      # @computed: price * qty
│       ├── price_source: str = ""  # catalog / amazon / scprs / manual
│       ├── confidence: float = 1.0
│       ├── supplier: str = ""
│       ├── source_url: str = ""
│       ├── asin: str = ""
│       └── notes: str = ""
│
├── totals        QuoteTotals  (@computed from line_items)
│   ├── subtotal: Decimal
│   ├── tax_rate: Decimal = 0
│   ├── tax: Decimal
│   ├── shipping: Decimal = 0
│   └── total: Decimal
│
├── compliance    ComplianceInfo
│   ├── sb_cert_submitted: bool = True
│   ├── dvbe_cert_submitted: bool = True
│   ├── darfur_compliant: bool = True
│   ├── drug_free_workplace: bool = True
│   └── std204_submitted: bool = False
│
├── status        QuoteStatus (enum)
│   DRAFT → PRICED → QA_PASS → GENERATED → SENT → WON | LOST
│
└── provenance    Provenance
    ├── source: str              # "email" | "manual" | "paste_url"
    ├── parsed_from_files: list[str]
    ├── classifier_shape: str
    ├── parse_warnings: list[str]
    ├── last_modified_by: str
    ├── audit_trail: list[AuditEntry]
    └── created_at: datetime
```

### Rules

- `unit_price` and `extension` are `@computed_field` — operators cannot set them directly.
  They set `unit_cost` and `markup_pct`; the model computes the rest.
- `totals` is `@computed_field` — derived from `line_items`. Cannot be set.
- Status transitions go through `Quote.transition(new_status)` which validates prerequisites.
- All mutations go through typed methods: `quote.set_price(line_no, cost, markup)`,
  `quote.update_buyer(name=, phone=)`, `quote.add_item(...)`, `quote.remove_item(line_no)`.
- Serialization: `quote.model_dump(mode="json")` / `Quote.model_validate(json_data)`.

### Migration Adapter

```python
Quote.from_legacy_dict(d: dict, doc_type: str = "pc") -> Quote
Quote.to_legacy_dict() -> dict
```

- `from_legacy_dict` handles both PC and RFQ field names (`items` vs `line_items`,
  `solicitation_number` vs `rfq_number`, etc.)
- `to_legacy_dict` produces the format existing route handlers expect
- Round-trip test: every PC/RFQ in production must survive `dict → Quote → dict`
  with zero data loss

### Feature Flag

`QUOTE_MODEL_V2=true` (env var):
- When `true`: route handlers load dict → convert to Quote → operate → convert back → save
- When `false`: handlers operate on raw dicts (current behavior)
- Instant rollback: flip to `false`, no code revert needed

---

## Layer 2: Form Profile Registry

`src/forms/profiles/` — one YAML file per form variant.

### Profile Schema

```yaml
id: 704a_reytech_standard
form_type: 704a
blank_pdf: templates/blank/ams_704_blank.pdf
fingerprint_sha: <sha256 of sorted field names>
fill_mode: acroform                    # acroform | overlay | hybrid
page_row_capacities: [11, 8, 11, 8]   # rows per page (extended template)

fields:
  vendor.name:
    pdf_field: "COMPANY NAME"
  vendor.representative:
    pdf_field: "COMPANY REPRESENTATIVE print name"
  vendor.address:
    pdf_field: "Address"
  vendor.phone:
    pdf_field: "Phone Number_2"
  vendor.email:
    pdf_field: "EMail Address"
  vendor.sb_cert:
    pdf_field: "Certified SBMB"
  vendor.dvbe_cert:
    pdf_field: "Certified DVBE"
  header.solicitation_number:
    pdf_field: "Text1"
  header.due_date:
    pdf_field: "Text2"
  header.due_time:
    pdf_field: "Time"
  buyer.requestor_name:
    pdf_field: "Requestor"
  buyer.institution:
    pdf_field: "Institution or HQ Program"
  ship_to.zip_code:
    pdf_field: "Delivery Zip Code"
  items[n].description:
    pdf_field: "ITEM DESCRIPTION NOUN FIRST ACCEPTABLE BRAND NAME AND OR EQUIVALENTRow{n}"
  items[n].qty:
    pdf_field: "QTYRow{n}"
  items[n].uom:
    pdf_field: "UNIT OF ISSUERow{n}"
  items[n].unit_price:
    pdf_field: "PRICE PER UNITRow{n}"
  items[n].extension:
    pdf_field: "EXTENSIONRow{n}"
  totals.subtotal:
    pdf_field: "fill_70"

signature:
  mode: image_stamp
  page: 1
  box: [450, 120, 590, 155]
  image_field: vendor.signature_image_path
```

### Profile Selection

1. Open uploaded PDF
2. Extract sorted AcroForm field names → SHA-256
3. Look up fingerprint in profile registry
4. If match: use that profile
5. If no match: fall through to shape classifier (XFA / DocuSign / text-only)
6. If no match at all: flag for operator review, offer Simple Submit as fallback

### Boot-Time Validator

`tools/validate_profiles.py` — runs at app startup AND in `make ship` pre-push hook.

For every YAML profile:
1. Load the profile
2. Open its `blank_pdf`
3. For every `pdf_field` in `fields:`, verify it exists in the PDF's AcroForm
4. If any field missing → **boot fails loudly** with profile ID + field path + expected field name

This turns today's silent "35/35 fields not found" into a startup error.

### Initial Profiles (Phase 1)

| Profile ID | Form | Fill Mode | Source |
|---|---|---|---|
| `704a_reytech_standard` | AMS 704 (Reytech blank) | acroform | PCs |
| `704b_cchcs_standard` | CCHCS "Acquisition Quote Worksheet" | acroform | RFQs (CCHCS) |
| `703b_reytech_rev_2025` | Reytech 703B template | acroform | RFQs |
| `quote_reytech_standard` | Reytech Quote PDF | html_to_pdf | All |
| `bid_package_cchcs` | CCHCS Non-IT Bid Package | acroform | RFQs (CCHCS) |
| `sellers_permit_reytech` | Seller's Permit | acroform | All |

More profiles added as buyer variants are encountered. The corpus mining script
(Phase 0.5) will identify the full set from 2 years of historical data.

---

## Layer 3: Thin Engines

Four modules, each ≤300 lines.

### Parse Engine (`src/forms/parse_engine.py`)

```python
def parse(pdf_path: str) -> tuple[Quote, list[ParseWarning]]:
    """Parse a buyer PDF into a Quote object.

    1. Open PDF, extract sorted field names, compute SHA-256 fingerprint
    2. Look up profile by fingerprint
    3. Read fields using profile's semantic map
    4. Return Quote + any parse warnings
    """
```

Replaces: `parse_ams704`, `_parse_ams704_ocr`, `_dispatch_parser`.

### Fill Engine (`src/forms/fill_engine.py`)

```python
def fill(quote: Quote, profile: FormProfile) -> bytes:
    """Fill a form using the profile's fill mode.

    fill_mode:
      - acroform: PyPDFForm fill + flatten
      - overlay: reportlab at profile-declared coordinates
      - hybrid: acroform for fields that exist, overlay for the rest
      - html_to_pdf: Jinja2 + WeasyPrint (for Reytech quote)

    Returns: filled PDF as bytes
    """
```

Replaces: `fill_ams704`, `_fill_pdf_text_overlay`, `fill_and_sign_pdf`.

### QA Engine (`src/forms/qa_engine.py`)

```python
def validate(filled_pdf: bytes, quote: Quote, profile: FormProfile) -> ValidationReport:
    """Read back the filled PDF using the SAME profile the filler used.

    Diffs every field against the input Quote. Cannot drift from filler
    because both use the same profile YAML as their field map.

    Returns: ValidationReport with pass/fail per field + overall verdict.
    """
```

Replaces: `qa_agent.py`, `form_qa.py`. The critical difference: QA reads fields
using the **same profile** the filler wrote — no more 704A/704B convention mismatch.

### Package Engine (`src/forms/package_engine.py`)

```python
def assemble(quote: Quote, profiles: list[FormProfile]) -> PackageResult:
    """Fill each profile, merge via pypdf, return assembled package.

    ~150 lines. Drive upload, email draft, and activity logging are
    pluggable post-assembly hooks — not inline in this function.
    """
```

Replaces: ~90% of `routes_rfq_gen.py::generate_rfq_package` (2000-line monolith).

---

## Golden Path: Paste URL → Quote

The primary operator workflow (from Q&A decisions):

```
Paste URL → Ingest Engine → Quote Model → UI (pre-filled) → Review Pricing → Send
```

- Ingest engine is a single function: `ingest(source: str) → Quote`
- Source can be: Cal eProcure URL, Gmail thread URL, direct PDF URL, raw email paste
- Every module in the chain tested individually AND end-to-end via golden fixtures
- If any module fails, Quote still populates with whatever succeeded + yellow badges
- Golden fixtures include "paste this URL, expect this Quote" regression cases
- Target: paste → draft quote ready in <60s, pricing review + send in <5min

---

## F1–F11 Reconciliation

From `docs/PC_TO_RFQ_WORKFLOW.md`:

| Feature | Status | Rebuild Phase |
|---|---|---|
| F1: Auto-link RFQ↔PC on import | NOT BUILT | Phase 3 (Quote provenance tracks linked PCs) |
| F2: Price port with diff detection | NOT BUILT | Phase 1 (Quote.from_legacy_dict handles both) |
| F3: QA gate before package generation | UNFINISHED | Phase 3 (qa_engine.validate is the gate) |
| F4: Freshness re-check | NOT BUILT | Phase 5 (post-rebuild) |
| F5: Price recommendations | PARTIAL | Phase 1 (Oracle output → LineItem.confidence) |
| F6: Price conflict resolution UI | NOT BUILT | Phase 3 (confidence badges on pre-filled items) |
| F7: Pricing audit trail | PARTIAL | Phase 1 (Quote.provenance.audit_trail) |
| F8: Stale price alerts | NOT BUILT | Phase 5 (post-rebuild) |
| F9: Duplicate item detection | NOT BUILT | Phase 5 (post-rebuild) |
| F10: Auto-price new RFQs | NOT BUILT | Phase 5 (post-rebuild) |
| F11: Margin guardrails | NOT BUILT | Phase 1 (Quote.set_price validates 3x rule) |

---

## Reply Thread Handling (from Q2 decision)

```
Buyer reply → Gmail API detects thread → Ingest engine extracts diff →
Draft update panel shows old → new → Operator approves →
Quote updated + acknowledgment reply sent
```

- Thread detection via `References` / `In-Reply-To` headers
- Draft update panel on PC/RFQ detail page: "Pending buyer updates"
- One click: approve + send acknowledgment
- `provenance.audit_trail` records every reply ingest
- Human-in-the-loop required — no auto-apply

---

## Due Date Lifecycle (from Q5 decision)

```
Extracted from email+attachments → Quote.header.due_date/due_time →
  Countdown widget (homepage) + sidebar (every page) + hard alert (<4h) →
    AM/PM checkbox on 704 → scheduling + alerting
```

- Due date is a **required field** (status cannot advance to PRICED without it)
- Default to 2:00 PM PST when no time specified (Q5 incident)
- Hard alert modal blocks UI when any bid is <4h out (already shipped in Phase 0.5)
- `due_time_explicit: bool` tracks whether time was extracted or assumed

---

## Confidence System (from Q6 decision)

Every extracted field carries a confidence score (0.0–1.0):

- ≥0.95: rendered normally
- <0.95: yellow "unconfirmed" badge + tooltip showing extraction source
- One-click confirm: tap badge → badge disappears, field locked
- One-click correct: tap field → inline edit
- Never block quote progression on low-confidence fields
- All confidence scores + operator corrections logged to audit trail

---

## Success Criteria

From `HANDOFF_2026-04-15_704_REBUILD.md` §A12, verified on 10 consecutive real bids:

1. Zero Claude-in-the-loop patches during bid submission
2. Failing bids show operator the specific field that failed QA
3. Parse → fill → QA round-trip is idempotent
4. Re-uploading a buyer PDF never wipes operator pricing
5. `make ship` pre-push blocks any commit that drops a profile or breaks a fixture
6. Boot-time validator catches profile/PDF mismatch before serving traffic
7. Admin UI lets operator override any Quote field without touching code
8. Every incident becomes a one-PR fix with a new golden fixture
9. Total LOC in `src/forms/` + `src/api/modules/routes_rfq*` < 4k (currently ~12k)
10. Email → draft ready in <5 min on well-formed CCHCS RFQ
11. Zero rendering incidents (decimal drops, supplier header missing)
12. Zero "wrong convention" incidents (QA/filler/parser disagreeing on field names)
