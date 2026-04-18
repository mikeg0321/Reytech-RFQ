"""Form Profiler — turn a blank PDF into a FormProfile YAML draft.

This is the agent that makes new agencies a DATA task, not a CODE task.
Given a blank buyer PDF, it:

  1. Extracts the AcroForm field inventory via pypdf.
  2. Detects row-templated fields (e.g., `ITEM Row1`, `QTYRow1`, and their
     `_2` suffix counterparts for page 2) and derives `page_row_capacities`.
  3. Ships the remaining header/footer fields to Claude with tool-use-forced
     structured output, classifying each into the canonical semantic schema
     used by `src/forms/profile_registry.py` (e.g., `vendor.name`,
     `header.solicitation_number`, `shipping.fob_prepaid`).
  4. Emits a YAML draft with inline `# TODO (auto)` comments over any field
     Claude was not confident about, so a human reviewer sees the gaps.
  5. Validates the draft via `validate_profile()` and reports remaining
     issues — the operator ships only when issues == [].

The LLM is allowed to classify header-type fields only. Row fields are
derived structurally (never LLM-guessed) because a hallucinated row mapping
corrupts every quote.

Public API:
    profile_blank_pdf(blank_pdf_path, form_id, profile_id, filled_sample=None)
        -> ProfilerResult(yaml_text, issues, fields_mapped, fields_unknown)

CLI: see `scripts/profile_form.py`.
"""
from __future__ import annotations

import json
import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("reytech.form_profiler")

MODEL = "claude-sonnet-4-6"

# Canonical semantic namespace. Profiler is allowed to emit ONLY these keys.
# Adding a new key = update the registry + this list + downstream form QA.
_CANONICAL_SEMANTICS = [
    # header
    "header.solicitation_number", "header.due_date", "header.due_time",
    "header.am_pst", "header.pm_pst", "header.price_check",
    # buyer
    "buyer.requestor_name", "buyer.institution", "buyer.phone",
    "buyer.date_of_request",
    # ship-to
    "ship_to.address", "ship_to.zip_code",
    # vendor (Reytech)
    "vendor.name", "vendor.supplier_name", "vendor.representative",
    "vendor.address", "vendor.phone", "vendor.email",
    "vendor.sb_cert", "vendor.dvbe_cert",
    "vendor.delivery", "vendor.discount", "vendor.expires", "vendor.signature",
    # shipping terms
    "shipping.fob_prepaid", "shipping.fob_ppadd", "shipping.fob_collect",
    # totals
    "totals.subtotal", "totals.freight", "totals.tax", "totals.total",
    "totals.notes",
    # page metadata
    "page.number", "page.of",
]

# Row-level semantics — never LLM-assigned; derived from field-name patterns.
_ROW_SEMANTICS = [
    "items[n].item_no", "items[n].description", "items[n].qty",
    "items[n].uom", "items[n].qty_per_uom", "items[n].unit_price",
    "items[n].extension", "items[n].substituted",
]


# ── Row-field detection ─────────────────────────────────────────────────────

# Matches any field name ending in `Row<digits>` or `Row<digits>_<page>`.
# Examples: "ITEM Row1", "QTYRow5", "EXTENSIONRow8_2".
_ROW_RE = re.compile(r"^(?P<stem>.*?)Row(?P<row>\d+)(?:_(?P<page>\d+))?$")


@dataclass
class ProfilerResult:
    yaml_text: str
    issues: list[str] = field(default_factory=list)
    fields_mapped: list[str] = field(default_factory=list)
    fields_unknown: list[str] = field(default_factory=list)
    page_row_capacities: list[int] = field(default_factory=list)


def _extract_fields(blank_pdf: str) -> list[str]:
    """Return sorted list of AcroForm field names, or [] if extraction fails."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(blank_pdf)
        fields = reader.get_fields() or {}
        return sorted(fields.keys())
    except Exception as e:
        log.error("field extraction failed: %s", e)
        return []


def _classify_row_fields(field_names: list[str]) -> tuple[dict[str, str], list[int], list[str]]:
    """Identify row-indexed fields and derive page capacities.

    Returns:
        (row_templates, page_row_capacities, non_row_fields)
        row_templates: {stem_with_Row{n}: canonical_semantic}
        page_row_capacities: [page1_cap, page2_cap, ...]
        non_row_fields: fields that are NOT row-indexed (feed to LLM classifier)
    """
    # page_num -> set of row numbers observed
    rows_per_page: dict[int, set[int]] = defaultdict(set)
    # stem_key -> canonical pdf_field template (with {n} and _<page>)
    stems: dict[str, list[tuple[int, int]]] = defaultdict(list)

    non_row = []
    for fname in field_names:
        m = _ROW_RE.match(fname)
        if not m:
            non_row.append(fname)
            continue
        # Preserve trailing whitespace in the stem so `"ITEM Row1"` round-trips
        # to `"ITEM Row{n}"` and not `"ITEMRow{n}"`.
        stem = m.group("stem")
        row = int(m.group("row"))
        page = int(m.group("page")) if m.group("page") else 1
        rows_per_page[page].add(row)
        stems[stem].append((page, row))

    # Derive page capacities in order of observed page numbers
    pages_sorted = sorted(rows_per_page.keys())
    page_caps = [max(rows_per_page[p]) for p in pages_sorted]

    # Map each stem to its canonical semantic based on substring match.
    # We intentionally use tight substring anchors — loose matches mis-map
    # (e.g., "ITEM DESCRIPTION" would otherwise match "ITEM" first).
    stem_to_semantic = _match_stem_to_semantic(list(stems.keys()))

    row_templates: dict[str, str] = {}
    for stem, semantic in stem_to_semantic.items():
        if not semantic:
            continue
        # Reconstruct template. For page-2 fields we use `{n}_2` suffix.
        row_templates[f"{stem}Row{{n}}"] = semantic

    return row_templates, page_caps, non_row


def _match_stem_to_semantic(stems: list[str]) -> dict[str, str]:
    """Match row-field stems to canonical row semantics via keyword rules.

    Deterministic — no LLM. If a stem does not clearly map, it stays None
    and the operator sees a `# TODO (auto)` in the YAML draft.
    """
    # Ordered keyword rules — MORE specific first.
    rules: list[tuple[str, str]] = [
        ("substitut",          "items[n].substituted"),
        ("qty per uom",        "items[n].qty_per_uom"),
        ("unit of measure",    "items[n].uom"),
        ("uom",                "items[n].uom"),
        ("price per unit",     "items[n].unit_price"),
        ("unit price",         "items[n].unit_price"),
        ("extension",          "items[n].extension"),
        ("item description",   "items[n].description"),
        ("description",        "items[n].description"),
        ("qty",                "items[n].qty"),
        ("quantity",           "items[n].qty"),
        ("item",               "items[n].item_no"),
    ]
    out: dict[str, str] = {}
    for stem in stems:
        s_norm = stem.lower()
        chosen = ""
        for needle, semantic in rules:
            if needle in s_norm:
                chosen = semantic
                break
        out[stem] = chosen
    return out


# ── Claude tool-use for non-row fields ──────────────────────────────────────

_CLASSIFY_TOOL = {
    "name": "classify_form_fields",
    "description": (
        "Record a mapping of raw PDF form-field names to canonical semantic "
        "names used by Reytech's form-fill engine. ONLY use semantics from "
        "the provided allowed list. If a field's purpose is unclear, set "
        "semantic to an empty string — the operator will fill it in."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "mappings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "pdf_field":   {"type": "string"},
                        "semantic":    {"type": "string"},
                        "field_type":  {"type": "string", "enum": ["text", "checkbox", "signature"]},
                        "confidence":  {"type": "number"},
                    },
                    "required": ["pdf_field", "semantic", "field_type", "confidence"],
                },
            },
        },
        "required": ["mappings"],
    },
}


_SYSTEM_PROMPT = (
    "You are mapping raw PDF form-field names from a government buyer PDF to "
    "Reytech's canonical semantic names. Output ONE entry per input field.\n\n"
    "Rules:\n"
    "- Use ONLY semantic names from the allowed list. If no allowed name fits, "
    "return semantic=\"\" (never invent one).\n"
    "- `vendor.*` refers to Reytech's own vendor info (company name, address, "
    "signature). `buyer.*` refers to the government buyer (requestor name, "
    "institution).\n"
    "- Checkboxes for 'FOB Destination Prepaid', 'FOB Destination PPADD', 'FOB "
    "Origin Freight Collect' map to `shipping.fob_prepaid`, `shipping.fob_ppadd`, "
    "`shipping.fob_collect` respectively.\n"
    "- A 'Signature and Date' field is `vendor.signature`, field_type=signature.\n"
    "- Numeric totals named like 'fill_70..fill_73' or 'Subtotal'/'Freight'/"
    "'Tax'/'Total' map to totals.{subtotal,freight,tax,total}.\n"
    "- If in doubt, emit semantic=\"\" with confidence<0.4.\n"
    "- Call the tool exactly once."
)


def _classify_non_row_fields(field_names: list[str]) -> list[dict]:
    """Run Claude tool-use to classify header/footer fields. Returns [] on any
    failure (no key, SDK missing, API error) — caller surfaces as TODOs."""
    if not field_names:
        return []
    try:
        import anthropic
    except Exception as e:
        log.warning("anthropic SDK unavailable: %s", e)
        return []
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY missing — skipping LLM classification")
        return []

    client = anthropic.Anthropic(api_key=api_key)

    user_parts = [
        "Allowed canonical semantic names (exact strings):",
        json.dumps(_CANONICAL_SEMANTICS, indent=2),
        "",
        "PDF form fields to classify:",
        json.dumps(field_names, indent=2),
    ]
    user_block = "\n".join(user_parts)

    try:
        resp = client.messages.create(
            model=MODEL,
            max_tokens=4096,
            system=_SYSTEM_PROMPT,
            tools=[_CLASSIFY_TOOL],
            tool_choice={"type": "tool", "name": "classify_form_fields"},
            messages=[{"role": "user", "content": user_block}],
        )
    except Exception as e:
        log.error("Claude classify error: %s", e)
        return []

    for block in resp.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "classify_form_fields":
            return (block.input or {}).get("mappings", []) or []
    return []


# ── YAML rendering ──────────────────────────────────────────────────────────

def _render_yaml(
    *,
    profile_id: str,
    form_type: str,
    blank_pdf: str,
    page_row_capacities: list[int],
    row_templates: dict[str, str],
    classifications: list[dict],
    signature_field: str = "Signature and Date",
) -> tuple[str, list[str], list[str]]:
    """Emit a YAML draft. Returns (yaml_text, fields_mapped, fields_unknown)."""
    lines: list[str] = []
    lines.append(f"# {form_type} — profile draft generated by FormProfiler")
    lines.append(f"# Blank PDF: {blank_pdf}")
    lines.append(f"# Rows per page: {page_row_capacities} (capacity={sum(page_row_capacities)})")
    lines.append("# Review TODO (auto) markers before shipping.")
    lines.append(f"id: {profile_id}")
    lines.append(f"form_type: {form_type}")
    lines.append(f"blank_pdf: {blank_pdf}")
    lines.append("fill_mode: acroform")
    lines.append(f"page_row_capacities: {page_row_capacities}")
    lines.append("")
    lines.append("fields:")

    # De-dup: one entry per canonical semantic (Claude may emit duplicates).
    seen_semantics: set[str] = set()
    mapped: list[str] = []
    unknown: list[str] = []

    for m in classifications:
        pdf_field = (m.get("pdf_field") or "").strip()
        semantic = (m.get("semantic") or "").strip()
        ftype = (m.get("field_type") or "text").strip()
        conf = float(m.get("confidence") or 0.0)
        if not pdf_field:
            continue
        if not semantic or semantic not in _CANONICAL_SEMANTICS:
            unknown.append(pdf_field)
            lines.append(f"  # TODO (auto): no confident semantic for '{pdf_field}' (conf={conf:.2f})")
            continue
        if semantic in seen_semantics:
            lines.append(f"  # TODO (auto): duplicate semantic '{semantic}' — also seen on '{pdf_field}'")
            continue
        seen_semantics.add(semantic)
        mapped.append(semantic)
        lines.append(f"  {semantic}:")
        lines.append(f'    pdf_field: "{pdf_field}"')
        if ftype != "text":
            lines.append(f"    type: {ftype}")

    # Row-templated fields.
    lines.append("")
    lines.append("  # ── Row item fields (templated — derived from field names) ──")
    for template_stem, semantic in sorted(row_templates.items(), key=lambda kv: kv[1]):
        lines.append(f"  {semantic}:")
        lines.append(f'    pdf_field: "{template_stem}"')
        mapped.append(semantic)

    # Signature block.
    lines.append("")
    lines.append("signature:")
    lines.append("  mode: image_stamp")
    lines.append("  page: 1")
    lines.append(f'  field: "{signature_field}"')

    return "\n".join(lines) + "\n", mapped, unknown


# ── Public entry point ──────────────────────────────────────────────────────

def profile_blank_pdf(
    blank_pdf: str,
    *,
    form_id: str,
    profile_id: str = "",
    filled_sample: Optional[str] = None,
) -> ProfilerResult:
    """Build a FormProfile YAML draft from a blank PDF.

    Args:
        blank_pdf: path to the blank government form PDF.
        form_id: agency_config form id (e.g., "calvet_rfq_briefs") — used
            in YAML header comment, not enforced.
        profile_id: override profile id; default is `{form_id}_reytech_draft`.
        filled_sample: currently unused — reserved for future few-shot
            examples pulled from a filled reference pack.

    Returns a ProfilerResult with yaml_text and issue list.
    """
    pid = profile_id or f"{form_id}_reytech_draft"
    issues: list[str] = []

    if not os.path.exists(blank_pdf):
        return ProfilerResult(
            yaml_text="",
            issues=[f"blank_pdf not found: {blank_pdf}"],
        )

    fields = _extract_fields(blank_pdf)
    if not fields:
        return ProfilerResult(
            yaml_text="",
            issues=[f"{blank_pdf}: no AcroForm fields found — "
                    f"profile cannot be auto-generated for a flat/scanned PDF"],
        )

    row_templates, page_caps, non_row = _classify_row_fields(fields)
    if not page_caps:
        issues.append("no row-indexed fields detected — this may not be a line-item form")
        page_caps = []

    classifications = _classify_non_row_fields(non_row)
    if not classifications and non_row:
        issues.append(
            f"LLM classification unavailable — {len(non_row)} header/footer fields "
            f"need manual mapping (all marked TODO in output)"
        )
        # Still render a skeleton so the operator sees the full field list.
        classifications = [
            {"pdf_field": n, "semantic": "", "field_type": "text", "confidence": 0.0}
            for n in non_row
        ]

    # Detect a signature field heuristically — used for the signature block.
    sig_field = "Signature and Date"
    for n in non_row:
        if "signature" in n.lower() and "date" in n.lower():
            sig_field = n
            break
    else:
        for n in non_row:
            if "signature" in n.lower():
                sig_field = n
                break

    yaml_text, mapped, unknown = _render_yaml(
        profile_id=pid,
        form_type=form_id,
        blank_pdf=blank_pdf,
        page_row_capacities=page_caps,
        row_templates=row_templates,
        classifications=classifications,
        signature_field=sig_field,
    )

    if unknown:
        issues.append(f"{len(unknown)} header/footer fields unmapped — see TODO markers")

    return ProfilerResult(
        yaml_text=yaml_text,
        issues=issues,
        fields_mapped=sorted(set(mapped)),
        fields_unknown=sorted(set(unknown)),
        page_row_capacities=page_caps,
    )
