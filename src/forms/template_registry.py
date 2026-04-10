"""
Template Registry — Single source of truth for PDF template structure.

Introspects any PDF template ONCE, caches results, and provides a contract
that all fill functions consume. Replaces the 4+ copies of inline page
detection, row counting, pre-fill detection, and field scanning scattered
across price_check.py and reytech_filler_v4.py.

Usage:
    from src.forms.template_registry import TemplateProfile

    profile = TemplateProfile(pdf_path)
    profile.pg1_row_count          # 11
    profile.pg2_row_count          # 8
    profile.row_capacity           # 19
    profile.is_prefilled           # True if buyer already filled descriptions/qty
    profile.is_flattened           # True if DocuSign-flattened / no fillable fields
    profile.row_field_suffix(14)   # "Row6_2"  (slot 14 → page 2 row 6)
    profile.validate_mapping(fv)   # ["BadField1", "BadField2"]  (fields NOT in template)
"""

import os
import re
import logging
from functools import lru_cache
from typing import Optional

log = logging.getLogger("reytech.template_registry")

# Cache profiles so re-generating the same template doesn't re-read the PDF.
# Key = (absolute path, file mtime) so edits invalidate the cache.
_profile_cache: dict[tuple[str, float], "TemplateProfile"] = {}


def get_profile(pdf_path: str) -> "TemplateProfile":
    """Get or create a cached TemplateProfile for the given PDF path.

    Cache key includes file mtime so a replaced template file gets
    a fresh profile automatically.
    """
    abs_path = os.path.abspath(pdf_path)
    try:
        mtime = os.path.getmtime(abs_path)
    except OSError:
        mtime = 0.0
    key = (abs_path, mtime)
    if key not in _profile_cache:
        _profile_cache[key] = TemplateProfile(abs_path)
        # Keep cache bounded (LRU-ish: just cap size)
        if len(_profile_cache) > 50:
            oldest = next(iter(_profile_cache))
            del _profile_cache[oldest]
    return _profile_cache[key]


class TemplateProfile:
    """Introspect a PDF template and expose its structure.

    All detection runs once at construction time. Consumers call properties
    and methods — never re-read the PDF themselves.
    """

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.page_count: int = 0

        # All field names in the PDF (set for O(1) lookup)
        self.field_names: set[str] = set()

        # All fields with their current values {name: value_str}
        self.field_values: dict[str, str] = {}

        # Row field structure (detected from QTYRowN annotations)
        # pg1_rows: sorted list of row numbers on page 1 (unsuffixed)
        #   e.g. [1, 2, 3, ..., 11]
        self.pg1_rows: list[int] = []
        # pg2_rows_suffixed: sorted list of row numbers with _2 suffix on page 2
        #   e.g. [1, 2, 3, ..., 8]
        self.pg2_rows_suffixed: list[int] = []
        # pg2_rows_plain: unsuffixed QTYRowN fields physically on page 2
        #   e.g. [9, 10, 11] (shared field names but on page 2)
        self.pg2_rows_plain: list[int] = []

        # Pre-fill detection
        self.is_prefilled: bool = False
        # {line_number_int: field_suffix_str}  e.g. {1: "Row1", 2: "Row2_2"}
        self.prefilled_item_rows: dict[int, str] = {}

        # Flattened PDF detection (DocuSign, scanned, etc.)
        self.is_flattened: bool = False

        # FOB checkbox field names found
        self.fob_prepaid_fields: list[str] = []

        # Signature field names found (intersection with any known sig fields)
        self.signature_fields: list[str] = []

        # Whether any _2 suffix fields exist at all
        self.has_suffix_fields: bool = False

        # 703B embedded as page 0 of a 704B
        self.has_embedded_703b: bool = False

        # Run all detection
        self._introspect()

    # ── Derived properties ─────────────────────────────────────────────

    @property
    def pg1_row_count(self) -> int:
        """Number of item rows on page 1 (unsuffixed fields)."""
        return len(self.pg1_rows)

    @property
    def pg2_row_count(self) -> int:
        """Total item rows on page 2 (suffixed + plain unsuffixed)."""
        return len(self.pg2_rows_suffixed) + len(self.pg2_rows_plain)

    @property
    def row_capacity(self) -> int:
        """Total items fillable via form fields (pages 1+2).
        Items beyond this need overflow pages via reportlab overlay."""
        return self.pg1_row_count + self.pg2_row_count

    # ── Row field mapping ──────────────────────────────────────────────

    def row_field_suffix(self, slot: int) -> Optional[str]:
        """Given a 1-based sequential item slot, return the Row field suffix.

        Returns:
            "Row3"    — page 1, unsuffixed
            "Row5_2"  — page 2, _2 suffix
            "Row12"   — page 2, plain unsuffixed (shared name, different page)
            None      — slot exceeds form field capacity (needs overflow page)

        Examples:
            profile.row_field_suffix(1)  → "Row1"     (page 1 slot 1)
            profile.row_field_suffix(12) → "Row1_2"   (page 2 slot 1, if pg1 has 11 rows)
            profile.row_field_suffix(20) → None        (overflow)
        """
        # Page 1: unsuffixed rows
        if slot <= len(self.pg1_rows):
            return f"Row{self.pg1_rows[slot - 1]}"

        # Page 2: _2 suffix rows first
        p2_slot = slot - len(self.pg1_rows)
        if p2_slot <= len(self.pg2_rows_suffixed):
            return f"Row{self.pg2_rows_suffixed[p2_slot - 1]}_2"

        # Page 2: plain unsuffixed continuation rows
        p2_plain_slot = p2_slot - len(self.pg2_rows_suffixed)
        if p2_plain_slot <= len(self.pg2_rows_plain):
            return f"Row{self.pg2_rows_plain[p2_plain_slot - 1]}"

        # Beyond all form fields
        return None

    def row_page_number(self, slot: int) -> int:
        """Return the 1-based page number for a given item slot.
        Returns 0 if the slot exceeds form capacity (overflow)."""
        if slot <= len(self.pg1_rows):
            return 1
        p2_slot = slot - len(self.pg1_rows)
        if p2_slot <= len(self.pg2_rows_suffixed) + len(self.pg2_rows_plain):
            return 2
        return 0  # overflow

    def field_suffix_parts(self, slot: int) -> tuple[Optional[str], int, str]:
        """Return (row_suffix, page_number, bare_suffix) for a slot.

        bare_suffix is "" for unsuffixed or "_2" for suffixed fields.
        Useful for callers that need to append the suffix to field name templates.

        Returns (None, 0, "") if slot exceeds capacity.
        """
        row_suffix = self.row_field_suffix(slot)
        if row_suffix is None:
            return None, 0, ""
        page = self.row_page_number(slot)
        bare = "_2" if row_suffix.endswith("_2") else ""
        return row_suffix, page, bare

    # ── Field validation ───────────────────────────────────────────────

    def validate_mapping(self, field_values: dict) -> list[str]:
        """Return field names from field_values that do NOT exist in the template.

        Call this BEFORE filling to catch mismatches. A non-empty return
        means those fields will be silently ignored by pypdf.
        """
        if not self.field_names:
            # If we couldn't read field names (e.g. flattened PDF), skip validation
            return []
        return sorted(k for k in field_values if k not in self.field_names)

    def has_field(self, field_name: str) -> bool:
        """Check if a specific field name exists in the template."""
        return field_name in self.field_names

    def get_field_value(self, field_name: str) -> Optional[str]:
        """Get the current value of a field, or None if not present."""
        return self.field_values.get(field_name)

    def find_fields(self, pattern: str) -> list[str]:
        """Find all field names matching a regex pattern."""
        compiled = re.compile(pattern)
        return sorted(f for f in self.field_names if compiled.search(f))

    # ── Pre-fill helpers ───────────────────────────────────────────────

    def prefilled_suffix_for_item(self, line_number: int) -> Optional[str]:
        """For pre-filled templates, get the form row suffix for a line item number.

        Returns None if the line number has no corresponding pre-filled row.
        """
        return self.prefilled_item_rows.get(line_number)

    # ── Introspection (runs once) ──────────────────────────────────────

    def _introspect(self):
        """Read the PDF and populate all fields. Called once at construction."""
        try:
            from pypdf import PdfReader
        except ImportError:
            log.warning("TemplateProfile: pypdf not available — profile will be empty")
            return

        if not os.path.exists(self.pdf_path):
            log.warning("TemplateProfile: PDF not found: %s", self.pdf_path)
            return

        try:
            reader = PdfReader(self.pdf_path)
        except Exception as e:
            log.error("TemplateProfile: cannot read PDF %s: %s",
                      os.path.basename(self.pdf_path), e)
            return

        self.page_count = len(reader.pages)

        # ── 1. Read all field names and current values ─────────────────
        try:
            fields = reader.get_fields() or {}
        except Exception:
            fields = {}

        for fname, fobj in fields.items():
            self.field_names.add(fname)
            if isinstance(fobj, dict):
                val = str(fobj.get("/V", "")).strip()
                if val:
                    self.field_values[fname] = val
            else:
                # Some pypdf versions return the value directly
                val = str(fobj).strip() if fobj else ""
                if val:
                    self.field_values[fname] = val

        # ── 2. Detect flattened PDF ────────────────────────────────────
        # A PDF with zero fillable fields is likely DocuSign-flattened or scanned
        if not self.field_names:
            self.is_flattened = True
            log.info("TemplateProfile: %s is flattened (0 fields)",
                     os.path.basename(self.pdf_path))
            return  # No field-based detection possible

        # ── 3. Check for _2 suffix fields ──────────────────────────────
        self.has_suffix_fields = any("_2" in fn for fn in self.field_names)

        # ── 4. Detect row layout from per-page annotations ─────────────
        # This is the critical detection: which QTYRowN fields are on which page.
        # We scan page annotations directly (not get_fields()) because get_fields()
        # doesn't tell us which page a field lives on.
        pg1_unsuffixed: set[int] = set()
        pg2_unsuffixed: set[int] = set()
        pg2_suffixed: set[int] = set()

        for pg_idx, page in enumerate(reader.pages[:2]):
            annots = page.get("/Annots") or []
            for annot_ref in annots:
                try:
                    obj = annot_ref.get_object() if hasattr(annot_ref, "get_object") else annot_ref
                    fname = str(obj.get("/T", ""))
                except Exception:
                    continue

                # Match QTYRowN_2
                m2 = re.match(r"QTYRow(\d+)_2$", fname)
                if m2:
                    pg2_suffixed.add(int(m2.group(1)))
                    continue

                # Match QTYRowN (unsuffixed)
                m1 = re.match(r"QTYRow(\d+)$", fname)
                if m1:
                    row_n = int(m1.group(1))
                    if pg_idx == 0:
                        pg1_unsuffixed.add(row_n)
                    else:
                        pg2_unsuffixed.add(row_n)

        if pg1_unsuffixed:
            self.pg1_rows = sorted(pg1_unsuffixed)
            self.pg2_rows_suffixed = sorted(pg2_suffixed)
            self.pg2_rows_plain = sorted(pg2_unsuffixed)
        else:
            # Fallback: count from field names alone (can't distinguish pages)
            all_unsuffixed: set[int] = set()
            all_suffixed: set[int] = set()
            for fname in self.field_names:
                m2 = re.match(r"QTYRow(\d+)_2$", fname)
                if m2:
                    all_suffixed.add(int(m2.group(1)))
                    continue
                m1 = re.match(r"QTYRow(\d+)$", fname)
                if m1:
                    all_unsuffixed.add(int(m1.group(1)))

            if all_unsuffixed:
                # Can't tell which page — assume all unsuffixed on page 1
                self.pg1_rows = sorted(all_unsuffixed)
                self.pg2_rows_suffixed = sorted(all_suffixed)
                log.info("TemplateProfile: fallback scan — pg1=%d rows, pg2=%d suffixed "
                         "(annotation scan unavailable)",
                         len(self.pg1_rows), len(self.pg2_rows_suffixed))

        # ── 5. Detect pre-filled template ──────────────────────────────
        # Agency 704B templates come with QTY/UOM/Description already filled.
        # We detect this by checking if any QTY row fields have values.
        self._detect_prefill(fields)

        # ── 6. Detect FOB Destination Freight Prepaid checkbox ─────────
        for fname in self.field_names:
            fn_lower = fname.lower().replace(" ", "")
            if "fobdestination" in fn_lower and "prepaid" in fn_lower:
                self.fob_prepaid_fields.append(fname)
        # Also check known static names
        for static_name in ("Check Box4", "CheckBox4", "fob_prepaid"):
            if static_name in self.field_names:
                if static_name not in self.fob_prepaid_fields:
                    self.fob_prepaid_fields.append(static_name)

        # ── 7. Detect embedded 703B on page 0 ─────────────────────────
        if self.page_count > 1:
            try:
                p0_annots = reader.pages[0].get("/Annots") or []
                for annot_ref in p0_annots:
                    obj = annot_ref.get_object() if hasattr(annot_ref, "get_object") else annot_ref
                    fname = str(obj.get("/T", ""))
                    if fname.startswith("703B_") or "Business Name" in fname:
                        self.has_embedded_703b = True
                        break
                if not self.has_embedded_703b:
                    p0_text = (reader.pages[0].extract_text() or "").upper()
                    if "BIDDER INFORMATION" in p0_text and "REQUEST FOR QUOTATION" in p0_text:
                        self.has_embedded_703b = True
            except Exception:
                pass

        # ── 8. Log summary ─────────────────────────────────────────────
        log.info("TemplateProfile: %s — %d pages, %d fields, "
                 "pg1=%d rows, pg2=%d+%d rows, capacity=%d, "
                 "prefilled=%s, flattened=%s, has_703b=%s",
                 os.path.basename(self.pdf_path),
                 self.page_count, len(self.field_names),
                 self.pg1_row_count,
                 len(self.pg2_rows_suffixed), len(self.pg2_rows_plain),
                 self.row_capacity,
                 self.is_prefilled, self.is_flattened, self.has_embedded_703b)

    def _detect_prefill(self, fields: dict):
        """Detect if buyer pre-filled descriptions/qty in the template.

        Checks all QTYRowN and QTYRowN_2 fields for non-empty values.
        If any have values, marks the template as pre-filled and builds
        a mapping of {line_number: row_suffix} for pricing placement.
        """
        for row_n in range(1, 30):
            for sfx in [f"Row{row_n}", f"Row{row_n}_2"]:
                qty_key = f"QTY{sfx}"
                qty_field = fields.get(qty_key)
                if qty_field is None:
                    continue

                # Extract value
                if isinstance(qty_field, dict):
                    qty_val = str(qty_field.get("/V", "")).strip()
                else:
                    qty_val = str(qty_field).strip()

                if not qty_val or qty_val in ("", "0", "/Off"):
                    continue

                self.is_prefilled = True

                # Find the # (line item number) for this row
                hash_val = ""
                for hash_name in [f"#{sfx}", f"Row{row_n}"]:
                    hf = fields.get(hash_name)
                    if hf is None:
                        continue
                    hv = str(hf.get("/V", "")).strip() if isinstance(hf, dict) else str(hf).strip()
                    if hv and hv.isdigit():
                        hash_val = hv
                        break

                if hash_val:
                    self.prefilled_item_rows[int(hash_val)] = sfx
                else:
                    # Infer line number by count
                    inferred_num = len(self.prefilled_item_rows) + 1
                    self.prefilled_item_rows[inferred_num] = sfx

        if self.is_prefilled:
            log.info("TemplateProfile: pre-filled detected (%d item rows: %s)",
                     len(self.prefilled_item_rows), self.prefilled_item_rows)

    # ── String representation ──────────────────────────────────────────

    def __repr__(self) -> str:
        return (f"TemplateProfile({os.path.basename(self.pdf_path)!r}, "
                f"pages={self.page_count}, fields={len(self.field_names)}, "
                f"rows={self.pg1_row_count}+{self.pg2_row_count}, "
                f"prefilled={self.is_prefilled}, flat={self.is_flattened})")
