"""Form registry — single source of truth for per-form metadata used
by both code-fillers and the mirror-fill substrate.

PR mr-wolf #4. Closes Pattern 4 (template fallback drift). Before
this module, three different parts of the codebase carried
overlapping knowledge about each form:

  - `src/forms/form_classifier.py` — knows the AcroForm field-name
    prefix per slot (data-driven from profile YAML).
  - `src/forms/reytech_filler_v4.py` — has hardcoded fill_* functions
    per form (`fill_703b`, `fill_704b`, `fill_bid_package`, etc.).
  - `src/core/agency_config.py` — knows which agencies require which
    forms.

When a buyer sent a form variant no fill_* function existed for
(703A, AMS 708 before fill_genai_708, etc.), the dispatcher fell
through to a coincidentally-overlapping filler — silent drift. This
registry surfaces the mapping explicitly: every form_type names its
prefix, its mirror-fill fallback (which other form's prior submission
can populate it via suffix translation), and whether a code-fill
function exists.

Read by:
  - `src/forms/mirror_fill.py` — gets prefix mapping for translation
  - `reytech_filler_v4.fill_703a` (new in this PR) — finds the
    fallback form to mirror from
  - Future dispatchers + classifiers that need a one-stop lookup
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FormDefinition:
    """One row of the registry — describes a single buyer-form variant.

    `form_id` — the canonical slot id ("703a", "703b", "703c", "704b",
    "bidpkg", "ams708", "dsh_attA", "cv012_cuf", etc.). Same id space
    `form_classifier.TEMPLATE_SLOTS` and `agency_config.required_forms`
    use.

    `field_prefix` — the AcroForm field-name prefix the form uses
    ("703B_", "703A_", "703C_", "708_", ""). Empty string means the
    form has unprefixed fields (legacy 703B variant).

    `code_filler` — module:function name of the canonical code-fill
    path ("src.forms.reytech_filler_v4:fill_703b"). None when no
    code-fill exists today (mirror-fill is the only path).

    `mirror_fallback` — when this form has no code_filler OR the
    code_filler fails, the dispatcher mirror-fills from a prior
    submission of THIS form_id. Example: form_id="703a" sets
    `mirror_fallback="703b"` — when a buyer sends a 703A, the
    dispatcher looks up the latest stored 703B submission and
    suffix-translates 703B_ → 703A_. None when no mirror is viable
    (form has unique fields not in any sibling).

    `human_label` — short string for logs / operator UI ("AMS 703A
    SB-DVBE Option Form"). Not load-bearing; just legibility.
    """
    form_id: str
    field_prefix: str = ""
    code_filler: Optional[str] = None
    mirror_fallback: Optional[str] = None
    human_label: str = ""


# ── Registry ────────────────────────────────────────────────────────


_REGISTRY: dict[str, FormDefinition] = {
    "703a": FormDefinition(
        form_id="703a",
        field_prefix="703A_",
        code_filler=None,  # PR #4: no code-fill; mirror_fallback handles it.
        mirror_fallback="703b",  # 703B and 703A share field-name suffixes.
        human_label="AMS 703A — RFQ SB-DVBE Option",
    ),
    "703b": FormDefinition(
        form_id="703b",
        field_prefix="703B_",
        code_filler="src.forms.reytech_filler_v4:fill_703b",
        mirror_fallback=None,
        human_label="AMS 703B — RFQ Informal Competitive Attachment 1",
    ),
    "703c": FormDefinition(
        form_id="703c",
        field_prefix="703C_",
        code_filler="src.forms.reytech_filler_v4:fill_703c",
        mirror_fallback="703b",  # 703C ↔ 703B suffix-compatible in practice.
        human_label="AMS 703C — RFQ Multi-Step (CDCR variant)",
    ),
    "704b": FormDefinition(
        form_id="704b",
        field_prefix="",  # Unprefixed Row{N} / Row{N}_2 fields.
        code_filler="src.forms.reytech_filler_v4:fill_704b",
        mirror_fallback=None,
        human_label="AMS 704B — CCHCS Acquisition Quote Worksheet",
    ),
    "bidpkg": FormDefinition(
        form_id="bidpkg",
        field_prefix="",  # Multi-form aggregator — many internal prefixes.
        code_filler="src.forms.reytech_filler_v4:fill_bid_package",
        mirror_fallback=None,  # Too compound to mirror as a single doc.
        human_label="CCHCS Bid Package (multi-form Attachment 3)",
    ),
    "ams708": FormDefinition(
        form_id="ams708",
        field_prefix="708_",
        code_filler="src.forms.reytech_filler_v4:fill_genai_708",
        mirror_fallback=None,
        human_label="AMS 708 — GenAI Disclosure",
    ),
    "cv012_cuf": FormDefinition(
        form_id="cv012_cuf",
        field_prefix="CV012_",
        code_filler=None,  # CalVet uses a profile YAML + fill_engine; no direct fn.
        mirror_fallback=None,
        human_label="CV 012 — Commercially Useful Function (CalVet)",
    ),
    "dsh_attA": FormDefinition(
        form_id="dsh_attA",
        field_prefix="",
        code_filler="src.forms.dsh_attachment_fillers:fill_dsh_attA",
        mirror_fallback=None,
        human_label="DSH Attachment A — Bidder Identity",
    ),
    "dsh_attB": FormDefinition(
        form_id="dsh_attB",
        field_prefix="",
        code_filler="src.forms.dsh_attachment_fillers:fill_dsh_attB",
        mirror_fallback=None,
        human_label="DSH Attachment B — Pricing",
    ),
    "dsh_attC": FormDefinition(
        form_id="dsh_attC",
        field_prefix="",
        code_filler="src.forms.dsh_attachment_fillers:fill_dsh_attC",
        mirror_fallback=None,
        human_label="DSH Attachment C — Forms Checklist",
    ),
    "cchcs_it_rfq": FormDefinition(
        form_id="cchcs_it_rfq",
        field_prefix="",  # LPA template has unprefixed fields.
        code_filler=None,  # Profile-driven via fill_engine; no direct fn.
        mirror_fallback=None,
        human_label="CCHCS IT Goods/Services RFQ (LPA non-cloud)",
    ),
    "quote": FormDefinition(
        form_id="quote",
        field_prefix="",
        code_filler="src.forms.quote_generator:generate_quote",
        mirror_fallback=None,
        human_label="Reytech Quote Letterhead PDF",
    ),
}


# ── Public API ──────────────────────────────────────────────────────


def get_form_definition(form_id: str) -> Optional[FormDefinition]:
    """Return the FormDefinition for `form_id`, or None when not
    registered. Whitespace-tolerant; tries the input verbatim first
    (preserves `dsh_attA/B/C` mixed-case convention from
    `form_classifier.TEMPLATE_SLOTS`), then lowercase as fallback so
    callers passing `"703A"` still hit `"703a"` cleanly."""
    if not form_id:
        return None
    key = str(form_id).strip()
    fd = _REGISTRY.get(key)
    if fd is not None:
        return fd
    return _REGISTRY.get(key.lower())


def all_form_ids() -> list[str]:
    """Every registered form_id, stable order. Used by tests + the
    architecture-contract ratchet to enumerate the form surface."""
    return sorted(_REGISTRY.keys())


def field_prefix(form_id: str) -> str:
    """Convenience accessor — return the AcroForm prefix for
    `form_id`. Returns "" when the form is unregistered OR has
    unprefixed fields. Matches the semantics
    `form_classifier.get_field_prefix` uses (so the two helpers can
    co-exist; this module is the canonical home as profiles migrate)."""
    fd = get_form_definition(form_id)
    return fd.field_prefix if fd else ""


def mirror_fallback_form(form_id: str) -> Optional[str]:
    """Return the form_id whose prior submission can populate
    `form_id` via suffix translation, or None when no fallback is
    registered (e.g., the buyer's form is sui generis or compound).
    Used by `mirror_fill.lookup_prior_submission` to find the prior
    PDF to read from."""
    fd = get_form_definition(form_id)
    return fd.mirror_fallback if fd else None


def has_code_filler(form_id: str) -> bool:
    """True if `form_id` has a registered code-fill function. False
    means mirror-fill (or operator hand-fill) is the only path —
    surface this to the dispatcher so it can route appropriately."""
    fd = get_form_definition(form_id)
    return bool(fd and fd.code_filler)
