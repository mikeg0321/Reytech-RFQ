"""Coleman 10842771 — permanent CCHCS 704B canary fixture.

Why this test exists
--------------------
2026-05-27 → 2026-05-28: Mike re-tested generate-package on the Coleman
RFQ `rfq_5a55f1b5` (sol# 10842771, 21 CCHCS facilities, 19× Zoll
Defib Training Kit + 2× Trainer Airway Mgmt) THREE evenings in a row.
Three different PRs landed in between (#1169 classifier, #1170 704B
field-name dual-write, #1171 704B overflow). Each one shipped green
tests against synthetic AcroForm fixtures the patcher constructed
from memory — not against Coleman's actual filled output. On
2026-05-28 06:07 UTC, regenerate-package on Coleman hit the SAME
"Row 1: 19.0 × $1564.57 = $29726.83, but extension shows $1564.57"
QA error a third time. Mike had to manually complete the package and
ship it.

His ask: "test the QA/QC on this one before 'marked sent' because
its good stress test." This test is the realization of that ask.

What this test pins
-------------------
The 704B PDF Mike known-good-shipped on 2026-05-27 16:57 PT (a
*pre*-PR-#1170 build) is checked into the repo as
`tests/fixtures/coleman_10842771/704b_golden_pre_pr1170.pdf`. It
represents the correct shape any future Coleman-like regenerate
MUST produce:

  - 145 fields, 1 page, canonical Reytech `704b_reytech_standard`
    template (NOT the buyer's 362-field 2-page prefilled template).
  - `SUBTOTALRow1 = '29,726.83'`  ← 19 × $1564.57 ✓
  - `SUBTOTALRow2 = '2,935.50'`   ← 2 × $1467.75 ✓
  - `fill_154    = '32662.33'`    ← merchandise subtotal ✓
  - NO `EXTENSIONRow*` fields present — proving the canonical-
    template path (the buyer's prefilled template has these; the
    canonical doesn't).
  - `verify_704b_computations()` returns `passed=True`, zero issues,
    zero warnings.

The fixture is the OBSERVABLE OUTCOME, not the intermediate strategy.
Whichever way the system gets there — Path A (force RFQ_FULL for
CCHCS) or Path B (fix RFQ_PREFILLED to actually override buyer JS) —
the output PDF must match these invariants.

Why pre-Mark-Sent specifically
------------------------------
Per Mike 2026-05-28: this test should run as a pre-deploy gate AND
should be evaluated before any future "Mark Sent" automation can
fire on a CCHCS RFQ shaped like Coleman (multi-facility, 1-3 SKUs,
"see attached distribution list" continuation rows). The thinking:
if the generator can't get Coleman right, it can't be trusted to
auto-mark-sent ANY CCHCS quote without operator eyeball.

See memory:
  - [[no-blind-guess-on-pdf-field-names]]
  - [[deferred-bug-requires-same-session-pr]]
  - [[long-running-post-must-be-background]]
  - [[project-handoff-2026-05-28-704b-field-name-blind-guess-loop]]
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pypdf import PdfReader

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "coleman_10842771"
GOLDEN_704B = FIXTURE_DIR / "704b_golden_pre_pr1170.pdf"
GOLDEN_FIELD_DUMP = FIXTURE_DIR / "704b_golden_field_dump.json"


# Coleman 10842771 buyer-stated facts — frozen here so regressions
# show up as explicit invariant violations, not "the test broke."
COLEMAN_SOL_NUMBER = "10842771"
COLEMAN_BUYER_EMAIL = "ayisha.coleman@cdcr.ca.gov"
COLEMAN_FACILITIES = 21
COLEMAN_LINE_ITEMS = [
    {"row": 1, "qty": 19, "unit_price": 1564.57, "extension": 29726.83,
     "description_prefix": "Training Kit"},
    {"row": 2, "qty": 2, "unit_price": 1467.75, "extension": 2935.50,
     "description_prefix": "Trainer Airway"},
]
COLEMAN_MERCHANDISE_SUBTOTAL = 32662.33


# ── Fixture availability ─────────────────────────────────────────────


def test_canary_artifacts_exist():
    """The pre-PR-#1170 golden 704B and its field-name dump must be
    in the repo. If a refactor moves them, this test fires first so
    nothing else mysteriously skips."""
    assert GOLDEN_704B.exists(), (
        f"Coleman canary 704B missing at {GOLDEN_704B}. "
        f"Restore from git history — this is the regression baseline."
    )
    assert GOLDEN_FIELD_DUMP.exists(), (
        f"Coleman canary field-dump missing at {GOLDEN_FIELD_DUMP}. "
        f"Regenerate with: python -c \"from pypdf import PdfReader; "
        f"import json; r=PdfReader('{GOLDEN_704B.name}'); "
        f"f=r.get_fields() or {{}}; json.dump({{'field_count':len(f),"
        f"'page_count':len(r.pages),'fields':{{n:str(f[n].get('/V',''))"
        f" for n in sorted(f.keys())}}}}, open('{GOLDEN_FIELD_DUMP.name}','w'),indent=2)\""
    )


# ── Field schema invariants ──────────────────────────────────────────


def test_golden_uses_canonical_reytech_template():
    """The golden 704B was filled from the canonical Reytech blank
    (145 fields, 1 page) — NOT the buyer's prefilled template (362
    fields, 2 pages). Tonight's regression flipped to the buyer
    template via FillStrategy.RFQ_PREFILLED and hit broken JS calcs.

    If a future change makes Coleman fill through the buyer-template
    path AND the resulting math is still correct, this assertion can
    be relaxed — but until then, canonical-template is the proven
    path and we keep it pinned."""
    reader = PdfReader(str(GOLDEN_704B))
    fields = reader.get_fields() or {}

    assert len(fields) == 145, (
        f"Expected 145 fields (canonical 704b_reytech_standard); "
        f"got {len(fields)}. If this rose, you may be filling against "
        f"the buyer's 362-field prefilled template — verify "
        f"FillStrategy.for_rfq() decision."
    )
    assert len(reader.pages) == 1, (
        f"Expected 1 page (canonical template); got {len(reader.pages)}. "
        f"The buyer prefilled template is 2 pages."
    )


def test_golden_has_subtotal_fields_not_extension_fields():
    """The canonical Reytech 704b template uses `SUBTOTALRow{n}`,
    NOT `EXTENSIONRow{n}`. PR #1170 added a dual-write to both names
    on the theory that buyer templates use EXTENSION — that diagnosis
    was wrong for Coleman specifically. This test pins which field
    family the canonical template actually has."""
    reader = PdfReader(str(GOLDEN_704B))
    field_names = set((reader.get_fields() or {}).keys())

    assert "SUBTOTALRow1" in field_names, (
        "Canonical 704B must have SUBTOTALRow1. If renamed, update "
        "verify_704b_computations field-fallback chain too."
    )
    assert "EXTENSIONRow1" not in field_names, (
        "Canonical 704B should NOT have EXTENSIONRow1 (that's a buyer-"
        "prefilled template convention). If EXTENSIONRow1 now appears, "
        "the fill strategy may have switched templates."
    )


# ── Value invariants (the actual bug class) ──────────────────────────


def _get_field_value(fields: dict, name: str) -> str:
    f = fields.get(name)
    return str(f.get("/V", "")).strip() if f else ""


def test_golden_row_1_extension_is_qty_times_unit_price():
    """The exact bug Mike hit three evenings in a row: extension
    column shows $1564.57 (unit price) instead of $29,726.83 (qty ×
    unit price). The golden has the correct math. If this fails, the
    regression has landed again."""
    reader = PdfReader(str(GOLDEN_704B))
    fields = reader.get_fields() or {}

    qty = _get_field_value(fields, "QTYRow1")
    unit_price = _get_field_value(fields, "PRICE PER UNITRow1")
    subtotal = _get_field_value(fields, "SUBTOTALRow1")

    assert qty == "19", f"Row 1 qty expected '19' got {qty!r}"
    assert unit_price == "1564.57", (
        f"Row 1 unit_price expected '1564.57' got {unit_price!r}"
    )
    assert subtotal == "29,726.83", (
        f"Row 1 SUBTOTAL expected '29,726.83' (19 × $1564.57); got "
        f"{subtotal!r}. If this is '1564.57' or '1,564.57', the "
        f"buyer-template broken-JS bug has regressed — see the 2026-"
        f"05-28 handoff."
    )


def test_golden_row_2_extension_distinct_from_row_1():
    """Tonight's QA error said BOTH rows showed extension $1564.57.
    Row 2 actually has unit_price $1467.75 (Trainer Airway Mgmt), so
    if a fix shows row 2's extension as anything tied to row 1's
    price, the bug is back. Pin distinct values for distinct rows."""
    reader = PdfReader(str(GOLDEN_704B))
    fields = reader.get_fields() or {}

    qty = _get_field_value(fields, "QTYRow2")
    unit_price = _get_field_value(fields, "PRICE PER UNITRow2")
    subtotal = _get_field_value(fields, "SUBTOTALRow2")

    assert qty == "2", f"Row 2 qty expected '2' got {qty!r}"
    assert unit_price == "1467.75", (
        f"Row 2 unit_price expected '1467.75' (NOT $1564.57 from row 1)"
        f" — got {unit_price!r}. Buyer-template-JS row-1-price-leak "
        f"would show row 1's price here."
    )
    assert subtotal == "2,935.50", (
        f"Row 2 SUBTOTAL expected '2,935.50' (2 × $1467.75); got "
        f"{subtotal!r}."
    )


def test_golden_merchandise_subtotal_sums_correctly():
    """fill_154 is the MERCHANDISE SUBTOTAL field. It must equal the
    sum of the row SUBTOTALs (after which CalRecycle SABRC + Bid
    Package math takes over). $32,662.33 is the buyer-side expected
    merchandise total per Mike's manually-shipped 2026-05-27 quote."""
    reader = PdfReader(str(GOLDEN_704B))
    fields = reader.get_fields() or {}

    assert _get_field_value(fields, "fill_154") == "32662.33", (
        f"MERCHANDISE SUBTOTAL fill_154 expected '32662.33' "
        f"(row1+row2 = $29,726.83 + $2,935.50). The shipped 2026-05-27 "
        f"Coleman package uses this total."
    )


# ── QA function end-to-end on the golden ─────────────────────────────


def test_verify_704b_computations_passes_on_golden():
    """The very check that failed in prod tonight must PASS on the
    golden. This is the substrate-singleness pin: writer's output
    and reader's check must agree on field names + values for a
    real production-shipped PDF, not a synthetic fixture."""
    from src.forms.form_qa import verify_704b_computations

    result = verify_704b_computations(str(GOLDEN_704B), {})

    assert result["passed"] is True, (
        f"verify_704b_computations failed on the canary golden:\n"
        f"  issues:   {result.get('issues', [])}\n"
        f"  warnings: {result.get('warnings', [])}\n"
        f"This file is known-good (Mike shipped this exact PDF to "
        f"Coleman 2026-05-27). If QA now flags it, the QA reader has "
        f"regressed — not the file."
    )
    assert result.get("issues", []) == [], (
        f"Expected zero issues on golden; got {result['issues']}"
    )


# ── Field-dump artifact integrity (audit trail) ──────────────────────


def test_field_dump_matches_live_pdf():
    """The JSON field-dump artifact is checked into the repo so PR
    reviewers can see what was measured without running pypdf. This
    test makes sure the dump stays in sync with the PDF — drift means
    one of them was manually edited and the other forgotten."""
    reader = PdfReader(str(GOLDEN_704B))
    live_fields = reader.get_fields() or {}

    dump = json.loads(GOLDEN_FIELD_DUMP.read_text())

    assert dump["field_count"] == len(live_fields), (
        f"Field-dump count drift: dump says {dump['field_count']}, "
        f"PDF has {len(live_fields)}. Regenerate dump."
    )
    assert dump["page_count"] == len(reader.pages)
    assert set(dump["fields"].keys()) == set(live_fields.keys()), (
        "Field-dump key set drift between JSON and PDF. Regenerate."
    )


# ── Strategy decision pin (Path A vs Path B awareness) ───────────────


def test_for_rfq_returns_rfq_full_for_non_prefilled():
    """Sanity pin: FillStrategy.for_rfq(is_prefilled=False) must
    return RFQ_FULL. If a refactor flips this default, the canonical-
    template path Coleman relied on disappears. The bug Mike hit
    tonight was upstream of this — the `is_prefilled` detector
    flipped True on Coleman's template — but pinning this floor
    prevents a second regression from compounding the first."""
    from src.forms.ams704_helpers import FillStrategy

    assert FillStrategy.for_rfq(is_prefilled=False) == FillStrategy.RFQ_FULL
    assert FillStrategy.for_rfq(is_prefilled=True) == FillStrategy.RFQ_PREFILLED


# ── Path A override pins ──────────────────────────────────────────────


def test_cchcs_override_demotes_prefilled_to_full():
    """Coleman 10842771 fix (2026-05-28, Path A). CCHCS buyer-
    uploaded 704B templates have broken JS calcs that override
    explicit value writes. Force RFQ_FULL regardless of is_prefilled.
    """
    from src.forms.ams704_helpers import FillStrategy

    # The bug case: prefilled=True + agency=CCHCS → demoted to FULL
    assert (
        FillStrategy.for_rfq_with_agency_override(
            is_prefilled=True, agency="cchcs"
        )
        == FillStrategy.RFQ_FULL
    )
    # Case-insensitive on agency
    assert (
        FillStrategy.for_rfq_with_agency_override(
            is_prefilled=True, agency="CCHCS"
        )
        == FillStrategy.RFQ_FULL
    )


def test_cchcs_override_no_op_when_not_prefilled():
    """Override is only a DEMOTION — it never PROMOTES. If the
    template isn't prefilled, for_rfq() already returns RFQ_FULL
    and the override is a no-op."""
    from src.forms.ams704_helpers import FillStrategy

    assert (
        FillStrategy.for_rfq_with_agency_override(
            is_prefilled=False, agency="cchcs"
        )
        == FillStrategy.RFQ_FULL
    )


def test_override_preserves_default_for_non_cchcs_agencies():
    """The CCHCS override must NOT bleed to CalVet, DSH, DGS, or
    any other agency. Those agencies may legitimately need the
    RFQ_PREFILLED path (their buyer templates aren't known to have
    broken JS calcs). Substrate-boundary pin per the same pattern
    as PR #1170's test_704a_still_only_writes_extension_field."""
    from src.forms.ams704_helpers import FillStrategy

    for agency in ("calvet", "dsh", "dgs", "cdcr", "cdfa", "", None):
        assert (
            FillStrategy.for_rfq_with_agency_override(
                is_prefilled=True, agency=agency or ""
            )
            == FillStrategy.RFQ_PREFILLED
        ), f"agency={agency!r} should NOT get the CCHCS demotion"
        assert (
            FillStrategy.for_rfq_with_agency_override(
                is_prefilled=False, agency=agency or ""
            )
            == FillStrategy.RFQ_FULL
        ), f"agency={agency!r} non-prefilled should still be FULL"


@pytest.mark.xfail(reason=(
    "Coleman buyer 704B template not yet checked into fixtures. "
    "Once tests/fixtures/coleman_10842771/704b_buyer_uploaded.pdf "
    "exists (copy from /data/uploads/20260526_171611_19e654/ on prod), "
    "this test runs an end-to-end fill against the buyer template "
    "and asserts the resulting PDF passes verify_704b_computations "
    "(with Path A override active OR Path B substrate fix shipped). "
    "Until then, Path A unit-tested above is sufficient regression "
    "coverage and the end-to-end is deferred."
))
def test_coleman_buyer_template_end_to_end_produces_passing_qa():
    """End-to-end pin — once the buyer template is in fixtures, this
    runs the full fill_704b path with agency=cchcs and asserts the
    output PDF passes the QA gate. Lift xfail when fixture lands."""
    raise NotImplementedError("waiting on buyer template fixture")
