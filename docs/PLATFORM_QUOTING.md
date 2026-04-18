# Platform: QuoteOrchestrator + FormProfiler + ComplianceValidator

**Status:** Built 2026-04-18. Shipped as `feat/quote-orchestrator`. All 46 platform tests pass.

This doc is the operator playbook for the quoting platform. The principle:
new agencies are a **data task** (YAML profile + rows in `agency_rules`),
not a **code task**. No more "hand-wired route per agency."

---

## Mental model

One connector (`QuoteOrchestrator`) drives every quote through an
explicit, ordered state machine:

```
draft → parsed → priced → qa_pass → generated → sent
```

Every stage transition checks preconditions. Every attempt — success,
block, error, skip — is written to `quote_audit_log`. The orchestrator
refuses to skip stages; you can only advance one at a time.

The three platform components:

| Component | File | Job |
|---|---|---|
| **QuoteOrchestrator** | `src/core/quote_orchestrator.py` | Runs the state machine, persists audit log, resolves agency + profiles |
| **FormProfiler** | `src/agents/form_profiler.py` | Turns a blank buyer PDF into a YAML profile draft (new agency onboarding) |
| **ComplianceValidator** | `src/agents/compliance_validator.py` | Blocks `priced → qa_pass` until required forms are filled + QA-passed |
| **/quoting/status** | `src/api/modules/routes_quoting_status.py` | Single pane of glass for the audit trail + operator override |

---

## Public API

Every caller uses one entry point:

```python
from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest

result = QuoteOrchestrator().run(QuoteRequest(
    source="uploads/buyer_rfq.pdf",   # path | dict | None
    doc_type="rfq",                    # "pc" | "rfq"
    agency_key="calvet",               # optional; inferred if blank
    buyer_email_text="",               # optional; fed to ComplianceValidator
    target_stage="qa_pass",            # where to stop (operator reviews)
))

if not result.ok:
    print("blockers:", result.blockers)
    print("warnings:", result.warnings)
else:
    print("final stage:", result.final_stage)
    print("package:", result.package)
```

Routes, pollers, and jobs call `.run()` and nothing else. Any per-agency
branching lives in data (agency_config + profile YAML + agency_rules),
not Python.

---

## Onboarding a new agency

This is the whole playbook. ~1–2 days per agency end-to-end.

### 1) Inventory the buyer's forms

Ask the buyer (or pull from a prior quote) the **blank** PDFs for every
form they require. Save them under `tests/fixtures/`.

### 2) Add the agency to `agency_config`

```python
# src/core/agency_config.py
"newagency": {
    "name": "New Agency",
    "match_patterns": ["NEW AGENCY", "newagency.ca.gov", ...],
    "required_forms": ["quote", "std204", "some_newagency_form"],
    "optional_forms": [],
    "default_markup_pct": 25,
}
```

If `some_newagency_form` is a new form type, add it to `AVAILABLE_FORMS`
at the bottom of the file AND to `_FORM_ID_TO_PROFILE_ID` in
`src/core/quote_orchestrator.py`.

### 3) Profile each new form

For every form we don't already have a profile for:

```bash
python scripts/profile_form.py \
    --blank tests/fixtures/some_newagency_form_blank.pdf \
    --form-id some_newagency_form \
    --out src/forms/profiles/some_newagency_form_reytech_draft.yaml
```

The script:
- extracts AcroForm field names via pypdf
- derives row capacities from `…Row<n>` / `…Row<n>_<page>` patterns
- calls Claude Sonnet with tool-use-forced classification to map header
  fields to canonical semantic names (`vendor.name`, `header.due_date`,
  etc.)
- emits YAML with `# TODO (auto)` comments over any field Claude was not
  confident about
- validates the draft against the blank PDF and reports issues

Open the generated YAML, fill in the TODOs, rename to
`<form_id>_reytech_standard.yaml` once clean, re-run validation.

**Row mapping is deterministic — never LLM-guessed.** Only header/footer
fields go through the LLM. A hallucinated row mapping corrupts every quote.

### 4) Load rules from buyer emails

```bash
python -m src.agents.agency_rules_extractor --agency newagency --days 730
```

This pulls the buyer's past emails from `sales@reytechinc.com`, extracts
durable rules (signature requirements, form-inclusion preferences, past
rejections), and upserts them into `agency_rules`. The ComplianceValidator
reads these at QA time.

### 5) Smoke test

```python
from src.core.quote_orchestrator import QuoteOrchestrator, QuoteRequest

# Build a minimal legacy-dict source for smoke tests
src = {
    "pc_id": "pc_smoke",
    "pc_number": "R26Q9999",
    "agency": "newagency",
    "items": [{"description": "test item", "qty": 1, "unit_cost": 10.00}],
    "requestor": "buyer@newagency.ca.gov",
}
r = QuoteOrchestrator(persist_audit=False).run(
    QuoteRequest(source=src, doc_type="rfq", target_stage="qa_pass",
                 agency_key="newagency")
)
assert r.ok, f"smoke failed: {r.blockers}"
```

### 6) Ship

- Add a golden fixture in `tests/fixtures/golden/` (real buyer data, no
  ghost data — see `feedback_no_ghost_data.md`)
- Add an end-to-end test in `tests/test_quote_orchestrator.py` that runs
  the orchestrator on the golden fixture and asserts the target stage
- `make ship`

---

## Operator dashboard: `/quoting/status`

Single page showing every quote's latest transition + full audit trail
for any doc_id. Features:

- **List view** (`/quoting/status`): last 50 quotes with stage + outcome
  + reasons + KPI tiles (advanced / blocked / error / override counts).
- **Detail view** (`/quoting/status/<doc_id>`): full chronological
  stage timeline, per-transition reasons, override form.
- **Override endpoint** (`POST /api/quoting/override/<doc_id>`): records
  an operator override as an `outcome=override` audit row. Override does
  NOT advance the quote — fix the root cause, then re-run.
- **JSON API** (`/api/quoting/status` + `/api/quoting/status/<doc_id>`):
  same data for CLI / scripts.

Auth: `@auth_required` (Basic Auth on every request, same as rest of app).

---

## Audit log schema (migration 21)

```sql
CREATE TABLE quote_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    quote_doc_id TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    agency_key TEXT DEFAULT '',
    stage_from TEXT DEFAULT '',
    stage_to TEXT NOT NULL,
    outcome TEXT NOT NULL,          -- advanced | blocked | skipped | error | override
    reasons_json TEXT DEFAULT '[]',
    actor TEXT DEFAULT 'system',
    at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

A JSON-validation trigger guards `reasons_json` writes. Indexes cover
`quote_doc_id`, `agency_key`, `outcome`, and `at`.

---

## What NOT to add

- **Per-agency route modules.** If you catch yourself writing
  `routes_<agency>.py`, stop. The right move is a new profile YAML +
  agency_config entry.
- **Hardcoded field names.** Every PDF field name lives in a profile's
  `pdf_field:` string, never in Python.
- **LLM-assigned row fields.** Rows are structurally derived. Ever.
- **"Skip profile validation for just this quote."** If a profile has
  validation issues, fix the YAML. The strict boot gate (shipped in
  PR #141) blocks boot on bad profiles for a reason.

---

## Next: CalVet integration test

CalVet R25Q86 (Fresno) is the first real integration for the new
platform. Steps (left for the next session — requires blank CalVet
RFQBriefs PDF):

1. Drop blank PDFs under `tests/fixtures/calvet_*_blank.pdf`.
2. Run `scripts/profile_form.py` for each.
3. Curate the drafts, rename to `<form>_reytech_standard.yaml`.
4. Create a golden fixture from the real R25Q86 email chain.
5. Add an E2E test that runs the orchestrator end-to-end on it.
6. Verify the `/quoting/status` dashboard shows the quote and its stages.

If any step takes more than an hour, that's a platform bug, not an
agency bug. Fix the platform.
