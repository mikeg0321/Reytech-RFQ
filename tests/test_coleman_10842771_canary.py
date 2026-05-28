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


# ── Falsification pins ────────────────────────────────────────────────
# PR #1170 + #1173 commit messages claimed (a) buyer templates use
# `EXTENSION{suffix}` fields and (b) buyer templates carry a JS calc on
# the EXTENSION column. Direct measurement of the golden file
# (committed by PR #1173 itself) falsifies both claims. These tests
# pin those falsifications against the in-repo golden so no future PR
# can re-fabricate the hypothesis without measurement.
# See feedback memory: [[no-blind-guess-on-pdf-field-names]] +
# [[claimed-pdf-js-must-be-dumped]].


def test_canonical_704b_template_has_no_extension_row_fields():
    """Pin the field-name falsification.

    Canonical CCHCS 704B (and the buyer-prefilled variants Mike's
    desktop dump measured 2026-05-28) use `SUBTOTAL{suffix}` for the
    row-level extension column. There is no `EXTENSION{suffix}`. PR
    #1170's dual-write to both was therefore writing to a non-existent
    field every fill. A PR adding `EXTENSION{suffix}` back to the
    writer must commit a fixture proving such a template exists.
    """
    reader = PdfReader(str(GOLDEN_704B))
    fields = reader.get_fields() or {}

    extension_fields = sorted(k for k in fields if k.startswith("EXTENSION"))
    assert extension_fields == [], (
        f"Golden 704B must contain ZERO EXTENSION* row fields "
        f"(canonical template uses SUBTOTAL{{suffix}} exclusively). "
        f"Found: {extension_fields}. If a new template variant "
        f"introduces EXTENSION* fields, that is its own fixture — "
        f"do NOT add EXTENSION* to this golden."
    )
    subtotal_fields = sorted(k for k in fields if k.startswith("SUBTOTAL"))
    assert len(subtotal_fields) >= 15, (
        f"Golden 704B must carry the SUBTOTAL{{suffix}} family used by "
        f"verify_704b_computations + ams704_helpers writer. Found "
        f"{len(subtotal_fields)}: {subtotal_fields}"
    )


def test_canonical_704b_template_has_no_unexpected_javascript():
    """Pin the JS falsification.

    PR #1173 commit message: "buyer-uploaded CCHCS 704B variants ...
    carry a JS calc on the EXTENSION column that defaults to row-1's
    unit_price for every row." Deep /JS scan of three real buyer
    templates + this golden showed only 2 entries: `AFDate_FormatEx`
    and `AFDate_KeystrokeEx` on `Date1_af_date`. The "JS overrides
    on viewer re-render" mechanism does not exist.

    Per [[claimed-pdf-js-must-be-dumped]]: any future PR claiming
    JS-side cause for a 704B bug must commit a fresh deep_js_scan
    that surfaces the alleged code. This test is the floor that
    fires first.
    """
    import collections

    reader = PdfReader(str(GOLDEN_704B))
    seen: set[int] = set()
    js_entries: list[str] = []
    queue = collections.deque([reader.trailer])
    while queue:
        obj = queue.popleft()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        try:
            obj = obj.get_object() if hasattr(obj, "get_object") else obj
        except Exception:
            continue
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == "/JS":
                    try:
                        code = v.get_object() if hasattr(v, "get_object") else v
                    except Exception:
                        code = ""
                    js_entries.append(str(code)[:200])
                queue.append(v)
        elif isinstance(obj, list):
            for v in obj:
                queue.append(v)

    # Acrobat's date-field default formatter pair is allowed; anything
    # else means a PR shipped a real JS hypothesis and must be checked.
    disallowed = [j for j in js_entries if "AFDate_" not in j]
    assert disallowed == [], (
        f"Golden 704B must contain NO JavaScript except Acrobat's "
        f"default AFDate_FormatEx / AFDate_KeystrokeEx pair on the "
        f"Date1_af_date field. Found {len(disallowed)} disallowed "
        f"entries: {disallowed!r}. If a PR commit message ever claims "
        f"JavaScript causes a 704B bug, that PR MUST commit a fresh "
        f"deep_js_scan artifact proving the JS exists "
        f"(see [[claimed-pdf-js-must-be-dumped]])."
    )


# ── Option C substrate-fix invariant ──────────────────────────────────


def test_for_rfq_returns_rfq_prefilled_when_template_is_prefilled():
    """Strategy floor. Buyer-prefilled templates take the
    RFQ_PREFILLED path so we never overwrite buyer-owned QTY,
    descriptions, or ITEM NUMBER. The agency-aware override added by
    PR #1173 (`for_rfq_with_agency_override`) has been reverted; the
    substrate fix in `build_704_item_fields` (reading buyer's QTY for
    the extension computation) closes the bug class instead.
    """
    from src.forms.ams704_helpers import FillStrategy

    assert FillStrategy.for_rfq(is_prefilled=True) == FillStrategy.RFQ_PREFILLED
    assert FillStrategy.for_rfq(is_prefilled=False) == FillStrategy.RFQ_FULL
    assert not hasattr(FillStrategy, "for_rfq_with_agency_override"), (
        "PR #1173's agency override was reverted because its premise "
        "(JS calc on buyer templates) was falsified — see the JS test "
        "above. The substrate fix in build_704_item_fields supersedes it."
    )


def test_build_704_item_fields_uses_buyer_qty_under_rfq_prefilled():
    """Option C — substrate bug class closed.

    Pre-fix: under RFQ_PREFILLED, `build_704_item_fields` computed
    extension = li.qty × li.unit_price. When upstream sent per-facility
    line_items (qty=1) but buyer's template carried the true line-item
    quantity in `QTYRow{n}` (e.g. Coleman QTYRow1=19), the SUBTOTAL
    field was written as 1 × unit_price. verify_704b_computations
    then read buyer's QTYRow1=19 alongside SUBTOTALRow1=unit_price and
    correctly flagged the mismatch — the verifier was honest, the
    writer was wrong.

    Post-fix: under RFQ_PREFILLED + profile.is_prefilled, the writer
    reads `profile.field_values[f"QTY{row_suffix}"]` and uses it in
    place of li.qty for the extension. li.unit_price is still our
    write; buyer's QTY{row_suffix} is preserved (writes_qty_uom=False
    for RFQ_PREFILLED). Result matches the golden subtotals.
    """
    from src.forms.template_registry import get_profile
    from src.forms.ams704_helpers import build_704_item_fields, FillStrategy

    profile = get_profile(str(GOLDEN_704B))
    assert profile.is_prefilled, (
        "Golden 704B has QTYRow1/QTYRow2 set — _detect_prefill should "
        "flag is_prefilled=True"
    )
    assert profile.field_values.get("QTYRow1") == "19", (
        f"Expected buyer QTYRow1=19, got {profile.field_values.get('QTYRow1')!r}"
    )
    assert profile.field_values.get("QTYRow2") == "2"

    # Simulate the per-facility upstream shape that triggered the bug:
    # 2 line_items, each with qty=1 (= 1 per facility, NOT buyer's total).
    raw_items = [
        {"line_number": 1, "qty": 1, "unit_price": 1564.57,
         "description": "Training Kit, for Zoll R Series Defibrillators"},
        {"line_number": 2, "qty": 1, "unit_price": 1467.75,
         "description": "Trainer Airway Mgmt W/ Stand"},
    ]

    result = build_704_item_fields(
        profile, raw_items, FillStrategy.RFQ_PREFILLED, convention="704b",
    )

    assert result.field_values.get("SUBTOTALRow1") == "29726.83", (
        f"Row1 SUBTOTAL must reflect buyer's QTYRow1=19 × $1564.57 = "
        f"$29726.83, got {result.field_values.get('SUBTOTALRow1')!r}. "
        f"If this is back to '1564.57', Option C regressed."
    )
    assert result.field_values.get("SUBTOTALRow2") == "2935.50", (
        f"Row2 SUBTOTAL must reflect buyer's QTYRow2=2 × $1467.75 = "
        f"$2935.50, got {result.field_values.get('SUBTOTALRow2')!r}."
    )
    # Per-unit price is OUR write, unchanged by Option C.
    assert result.field_values.get("PRICE PER UNITRow1") == "1564.57"
    assert result.field_values.get("PRICE PER UNITRow2") == "1467.75"
    # Strategy contract — QTYRow{n} must NOT be in the values dict
    # under RFQ_PREFILLED. writes_qty_uom=False for PREFILLED.
    assert "QTYRow1" not in result.field_values, (
        "RFQ_PREFILLED must not overwrite buyer's QTYRow1. If this "
        "fires, FillStrategy.writes_qty_uom regressed."
    )
    assert "QTYRow2" not in result.field_values
    assert "ITEM NUMBERRow1" not in result.field_values, (
        "RFQ_PREFILLED must not overwrite buyer's ITEM NUMBER. "
        "writes_item_numbers regressed."
    )


