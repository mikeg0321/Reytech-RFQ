"""Canonical address formatter — single source of truth for every
customer-facing address rendering across the app (quote PDF, 703B/704B/
bid package fillers, PC PDFs, order packets, etc).

Mike P0 2026-05-06 RFQ a5b09b56: the quote PDF's "Ship to Location"
clipped past the right margin because the source address was a
super-long mash-up: `"CIW - California Institution for Women - 16756
Chino Corona Road"` followed by `["Corona", "CA", "92880", "United
States"]` as separate lines. Each caller built addresses its own way;
no shared formatter; no canonical shape.

This module fixes that. Every caller routes through `parse_address_blob`
(messy → canonical) or `format_address_canonical` (parts → canonical),
both returning the same `{"name": str, "lines": list[str]}` shape. PDF
generators consume `lines` as-is, no further surgery.

## Canonical output shape

```python
{
    "name": "CIW - California Institution for Women",
    "lines": [
        "16756 Chino Corona Road",
        "Corona, CA 92880",
    ],
}
```

Country line is dropped when it's "United States" / "USA" / "US" —
implied for every Reytech buyer (CA-only government). Foreign country
lines (rare) are preserved.

## Patterns this parser handles (seen on prod 2026-05-06)

  A. `"INST - STREET\\nCity\\nST\\nZIP\\nCountry"` — the bug pattern.
     Institution name and street concatenated with " - " on line 1;
     city, state, zip each on separate lines.
  B. `"INST\\nSTREET\\nCity, ST ZIP"` — already canonical multi-line.
  C. `"INST, STREET, City, ST ZIP"` — single-line CSV.
  D. `"INST"` — name only, no address.
  E. `""` / None — empty.
"""
from __future__ import annotations

import re
from typing import Optional

# Country tokens to drop from the rendered address (always implied for
# Reytech). Matched case-insensitively against the trailing line.
_IMPLIED_COUNTRIES = {"united states", "usa", "u.s.a.", "us", "u.s."}

# Cheap heuristic for "this token looks like a street number prefix".
# A real street starts with a digit (or compound like "1A"); a real
# institution name starts with a letter.
_STREET_PREFIX_RE = re.compile(r"^\s*\d+[a-z]?\s")

# US state two-letter pattern. Used to detect the "City, ST ZIP" line
# shape so we can re-fold split-out city/state/zip lines.
_US_STATE_RE = re.compile(
    r"\b(A[KLRZ]|C[AOT]|D[CE]|FL|GA|HI|I[ADLN]|K[SY]|LA|"
    r"M[ADEINOST]|N[CDEHJMVY]|O[HKR]|PA|RI|S[CD]|"
    r"T[NX]|UT|V[AT]|W[AIVY])\b",
    re.IGNORECASE,
)

_ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")


def _drop_implied_country(line: str) -> Optional[str]:
    """Return None if `line` is an implied-country token; else the line."""
    if not line:
        return None
    norm = re.sub(r"\s+", " ", line.strip().lower()).rstrip(".")
    if norm in _IMPLIED_COUNTRIES:
        return None
    return line


def _split_inst_and_street(first_line: str) -> tuple[str, Optional[str]]:
    """Detect the `"INST - STREET"` mash-up.

    The split fires when `" - "` exists AND the right side looks like a
    street (begins with a digit). Otherwise leave the line whole — many
    legitimate institution names contain hyphens (`"CIW - California
    Institution for Women"`) that should NOT be split.

    Returns (institution_name, street_or_none).
    """
    if " - " not in first_line:
        return first_line.strip(), None
    # Split only at the LAST " - " — a name like "CIW - California
    # Institution for Women - 16756 Chino Corona Road" must split between
    # the institution and the street, not at the first hyphen inside the
    # name. The street side must start with a digit; otherwise we keep the
    # whole line as the institution name.
    head, _, tail = first_line.rpartition(" - ")
    if _STREET_PREFIX_RE.match(tail):
        return head.strip(), tail.strip()
    return first_line.strip(), None


def _fold_city_state_zip(parts: list[str]) -> list[str]:
    """Given remaining address parts that may have city/state/zip on
    separate lines (`["Corona", "CA", "92880"]`), fold them into a
    single canonical `"City, ST ZIP"` line.

    Pre-folded lines (`"Corona, CA 92880"`) are detected and passed
    through. Foreign country lines are dropped via `_drop_implied_country`.
    """
    cleaned = []
    for p in parts:
        kept = _drop_implied_country(p)
        if kept:
            cleaned.append(kept.strip())
    if not cleaned:
        return []

    # Already-folded — single line containing both a state token and a zip.
    if any(_US_STATE_RE.search(c) and _ZIP_RE.search(c) for c in cleaned):
        return cleaned

    # Try to fold: walk from the end looking for state + zip + city.
    folded: list[str] = []
    leftover: list[str] = list(cleaned)
    zip_idx = next(
        (i for i, p in enumerate(leftover) if _ZIP_RE.fullmatch(p.strip())),
        -1,
    )
    state_idx = next(
        (i for i, p in enumerate(leftover) if _US_STATE_RE.fullmatch(p.strip())),
        -1,
    )
    if zip_idx >= 0 and state_idx >= 0 and state_idx < zip_idx:
        # City is anything before state_idx; combine.
        city = ", ".join(leftover[:state_idx]).strip()
        state = leftover[state_idx].strip()
        zip_code = leftover[zip_idx].strip()
        folded.append(f"{city}, {state} {zip_code}" if city else f"{state} {zip_code}")
        # Anything after the zip_idx is foreign / extra — preserve.
        for extra in leftover[zip_idx + 1:]:
            kept = _drop_implied_country(extra)
            if kept:
                folded.append(kept.strip())
        return folded
    # No clean fold — return as-is, post-country-drop.
    return cleaned


def parse_address_blob(raw: Optional[str]) -> dict:
    """Parse a raw address string (any of the wild shapes) into the
    canonical `{name, lines}` form.

    Empty / None input → `{name: "", lines: []}`.
    """
    if not raw:
        return {"name": "", "lines": []}

    # Normalize line endings, strip, split.
    text = raw.replace("\r\n", "\n").replace("\\r\\n", "\n").replace("\\n", "\n")
    raw_lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    if len(raw_lines) == 0:
        return {"name": "", "lines": []}

    # Single-line input: try comma-split (Pattern C).
    if len(raw_lines) == 1:
        parts = [p.strip() for p in raw_lines[0].split(",") if p.strip()]
        if len(parts) == 1:
            # Just an institution / street.
            head, street = _split_inst_and_street(parts[0])
            if street:
                return {"name": head, "lines": [street]}
            return {"name": parts[0], "lines": []}
        # Multi-CSV: first part is name (if non-numeric), the FIRST
        # numeric-prefixed token is the street, rest is city/state/zip.
        # Without peeling the street, _fold_city_state_zip would absorb
        # it into the "City, ST ZIP" join.
        if _STREET_PREFIX_RE.match(parts[0]):
            # No name; parts[0] is the street.
            return {"name": "", "lines": [parts[0]] + _fold_city_state_zip(parts[1:])}
        name = parts[0]
        rest_parts = parts[1:]
        if rest_parts and _STREET_PREFIX_RE.match(rest_parts[0]):
            street = rest_parts[0]
            tail_parts = rest_parts[1:]
            return {"name": name, "lines": [street] + _fold_city_state_zip(tail_parts)}
        return {"name": name, "lines": _fold_city_state_zip(rest_parts)}

    # Multi-line. Detect Pattern A: first line has " - STREET" mash-up.
    head, embedded_street = _split_inst_and_street(raw_lines[0])
    rest = raw_lines[1:]

    # If the first remaining line itself starts with a digit, it's already
    # the street — head is just the institution name.
    if not embedded_street and rest and _STREET_PREFIX_RE.match(rest[0]):
        street = rest[0]
        tail = rest[1:]
        return {"name": head, "lines": [street] + _fold_city_state_zip(tail)}

    if embedded_street:
        # Pattern A: name unsplit + embedded street + remaining city/state/zip.
        return {"name": head, "lines": [embedded_street] + _fold_city_state_zip(rest)}

    # Default: first line is name, rest is address.
    return {"name": head, "lines": _fold_city_state_zip(rest)}


def format_address_canonical(
    institution: Optional[str] = None,
    street: Optional[str] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    zip_code: Optional[str] = None,
    country: Optional[str] = None,
) -> dict:
    """Build a canonical `{name, lines}` from explicit address parts.

    Empty / None parts are skipped. `country` is dropped when it matches
    an implied-country token.
    """
    name = (institution or "").strip()
    lines: list[str] = []
    if street:
        lines.append(street.strip())
    csz_bits = []
    if city:
        csz_bits.append(city.strip())
    state_zip = " ".join(p for p in (state, zip_code) if p)
    if state_zip:
        if csz_bits:
            csz_bits[-1] = f"{csz_bits[-1]}, {state_zip.strip()}"
        else:
            csz_bits.append(state_zip.strip())
    lines.extend(csz_bits)
    if country:
        kept = _drop_implied_country(country)
        if kept:
            lines.append(kept.strip())
    return {"name": name, "lines": lines}


def format_for_pdf(parsed: dict) -> tuple[str, list[str]]:
    """Convenience: return `(name, lines)` for callers that want a tuple.

    Used by the quote PDF generator and form fillers that consume
    address as `(label_string, [line_string, ...])`.
    """
    return parsed.get("name", "") or "", list(parsed.get("lines") or [])
