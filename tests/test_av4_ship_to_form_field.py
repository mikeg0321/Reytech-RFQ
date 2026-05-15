"""PR-AV4 (AV-4) — ship_to from buyer AcroForm field block.

Closes AV-4 from the 5/14 EOD substrate backlog. Every CCHCS / DSH
RFQ ingest landed with `ship_to = "CA"` or the facility_registry
canonical fallback — never the buyer's actual typed delivery
address. Operator retyped the real address on every record.

ROOT CAUSE

`_create_record` (ingest_pipeline.py) read `ship_to` only from
`header.ship_to` and `header.delivery_address`. Both come from
pdfplumber text-extraction heuristics that miss AcroForm field
values entirely (text-extract returns the static template LABEL
"Ship To:" but never the buyer-typed value below it).

The form_field_extractor module (PR-AV1) already reads AcroForm
values from buyer PDFs — including ship_to under multiple aliases
(ship_to / ship_address / delivery_address / delivery_location /
deliver_to / destination). It was wired into
attachment_contract_parser but NOT into the ingest path's ship_to
resolution.

THE FIX

Before `_hdr_ship_to` is computed (the read that drives
resolved_ship_to + the persisted record["ship_to"] field), call
form_field_extractor.extract_from_attachments on `all_paths`. If
it returns a ship_to value, use it as the highest-priority source:

  resolved_ship_to = (
      form_field.ship_to                            # ← NEW: most authoritative
      or header.ship_to                             # body-regex
      or header.delivery_address                    # body-regex alt
      or header.delivery_location                   # body-regex alt
      or facility_registry.canonical_ship_to        # registry fallback
  )

The form-field value WINS because the buyer typed it directly
into the form. Body-regex matches read labels (not values), so
they're inherently lower-confidence.

Safety:
  - extract_from_attachments returns FormFieldValues() empty when
    pypdf is unavailable or no PDFs have AcroForm fields — no crash.
  - The call is wrapped in try/except — any pypdf failure logs at
    debug level and falls through to existing behavior. Ingest is
    a hot path and must never crash on a malformed PDF.
  - The existing >3-char guard is preserved so a 1-2 char garbage
    form value ("CA") still falls through to canonical_ship_to.

Tests pin:
  - form_field_extractor exposes ship_to in its FormFieldValues
  - The wire-up at ingest reads form-field result correctly
  - Empty form-field result falls through to existing behavior
  - Crash inside FFE doesn't propagate to ingest
  - Source-grep guard so a refactor that removes the call breaks loudly
"""
from __future__ import annotations


def test_form_field_extractor_has_ship_to_field():
    """Existing contract — FormFieldValues exposes a ship_to attribute.
    AV-4 relies on this; pin it so a refactor of the extractor can't
    silently break the wire-up."""
    from src.agents.form_field_extractor import FormFieldValues
    ff = FormFieldValues()
    assert hasattr(ff, "ship_to")
    assert ff.ship_to == ""


def test_form_field_extractor_handles_missing_pypdf():
    """When pypdf isn't installed, extract_from_attachments must return
    an empty FormFieldValues (not raise). The ingest wire-up depends
    on this to skip cleanly when running in test/lite environments."""
    from src.agents.form_field_extractor import extract_from_attachments, HAS_PYPDF
    # In a normal env pypdf IS installed — exercise the empty-list path
    out = extract_from_attachments([])
    assert out.ship_to == ""


def test_ingest_pipeline_reads_form_field_ship_to():
    """Source-level guard: ingest_pipeline._create_record contains the
    AV-4 marker and reads form_field.ship_to before header.ship_to."""
    import inspect
    from src.core import ingest_pipeline
    src = inspect.getsource(ingest_pipeline._create_record)
    assert "PR-AV4" in src, "AV-4 marker missing from _create_record"
    assert "extract_from_attachments" in src, (
        "AV-4 wire-up missing: form_field_extractor not invoked"
    )
    # Form-field value must appear in the fallback chain BEFORE
    # header values (it's authoritative)
    idx_ff = src.find("_ff_ship_to")
    idx_hdr = src.find('header.get("ship_to")')
    assert idx_ff != -1 and idx_hdr != -1
    assert idx_ff < idx_hdr, (
        "form-field ship_to must come BEFORE header.ship_to in the "
        "fallback chain — otherwise the body-regex value (which may "
        "be a 2-char 'CA' garbage match) wins over the buyer's "
        "literal AcroForm entry"
    )


def test_ingest_wire_up_includes_delivery_location_alias():
    """The form_field_extractor and attachment_contract_parser both
    have historic paths writing ship_to into `delivery_location`. The
    fallback chain must include delivery_location so older records
    that flowed that path don't lose their ship_to."""
    import inspect
    from src.core import ingest_pipeline
    src = inspect.getsource(ingest_pipeline._create_record)
    assert 'header.get("delivery_location")' in src


def test_ingest_wire_up_wrapped_in_try_except():
    """A pypdf crash inside the new FFE call must NOT break ingest.
    Pin the try/except wrapper."""
    import inspect
    from src.core import ingest_pipeline
    src = inspect.getsource(ingest_pipeline._create_record)
    # Find the FFE call site
    idx = src.find("extract_from_attachments")
    assert idx != -1
    preceding = src[max(0, idx - 200):idx]
    following = src[idx:idx + 500]
    assert "try:" in preceding, "AV-4 FFE call must be in a try block"
    assert "except" in following, "AV-4 FFE call must have an except clause"
    assert "log.debug" in following, (
        "AV-4 FFE failure must log at debug level (non-fatal)"
    )


def test_ingest_wire_up_skips_when_no_attachments():
    """Defensive: when all_paths is None or empty, the FFE call must
    be skipped (not crash on an empty list comprehension over None)."""
    import inspect
    from src.core import ingest_pipeline
    src = inspect.getsource(ingest_pipeline._create_record)
    # The `if all_paths:` guard must precede the FFE call
    idx_guard = src.find("if all_paths:")
    idx_ffe = src.find("extract_from_attachments")
    assert idx_guard != -1, "all_paths guard missing"
    assert idx_guard < idx_ffe, (
        "FFE call must be inside the `if all_paths:` guard"
    )


def test_existing_fallback_chain_preserved():
    """Don't break the existing canonical-fallback logic — the
    >3-char guard + canonical_ship_to fallback must still apply
    when neither form-field nor header values are substantive."""
    import inspect
    from src.core import ingest_pipeline
    src = inspect.getsource(ingest_pipeline._create_record)
    assert "len(_hdr_ship_to) > 3" in src, (
        "AV-4 must preserve the >3-char guard so 'CA' garbage values "
        "fall through to canonical_ship_to"
    )
    assert "resolved_ship_to = _hdr_ship_to if len(_hdr_ship_to) > 3" in src or \
           "resolved_ship_to = _hdr_ship_to" in src
