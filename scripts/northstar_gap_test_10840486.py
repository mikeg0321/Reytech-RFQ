"""Comprehensive north-star vs generated gap test for RFQ 10840486.

Compares the Reytech-submitted north star (what was actually sent) against
the currently-generated bundle on prod, page-by-page and field-by-field.
Emits a structured report so a dedicated fix PR can target specific gaps
without blind signature-patching (3-strikes guardrail).

What it checks:
  - PAGE COUNT parity
  - AcroForm field name overlap (which fields each has)
  - AcroForm field VALUES on fields both have — emits (field, expected, actual) diffs
  - Signature annotations per page (count + detection via /Subtype=/Widget with /FT=/Sig,
    plus signature-image overlays by scanning XObject resources)
  - Page TEXT headline match (first ~100 chars per page)
  - Total bytes

Exit code:
  0 = no gaps (or gaps all within acceptable categories)
  1 = gaps present — report lists them

Usage:
  railway ssh /opt/venv/bin/python /app/scripts/northstar_gap_test_10840486.py
"""
import json
import os
import sys
from typing import Any

sys.path.insert(0, "/app")

NORTH_STAR = "/app/tests/fixtures/rfq_packages/10840486_rfq_package_NORTHSTAR.pdf"

# Candidate generated files — take whichever exists, preferring the latest v2.
GEN_CANDIDATES = [
    "/data/output/10840486/10840486_RFQ_Package_Reytech_v2.pdf",
    "/data/output/10840486/10840486_RFQ_Package_Reytech_CORRECTED.pdf",
]


def pick_generated() -> str:
    for p in GEN_CANDIDATES:
        if os.path.exists(p):
            return p
    print("ERROR: no generated bundle on disk")
    sys.exit(2)


def get_fields(reader) -> dict[str, str]:
    """Return {field_name: /V value as str or ''}."""
    fields = reader.get_fields() or {}
    out = {}
    for name, spec in fields.items():
        v = ""
        try:
            if isinstance(spec, dict):
                raw = spec.get("/V", "") or spec.get("value", "")
                v = str(raw) if raw else ""
        except Exception:
            v = ""
        out[name] = v
    return out


def count_sig_widgets(reader) -> int:
    """Count /Sig widget annotations across all pages."""
    n = 0
    for page in reader.pages:
        annots = page.get("/Annots") or []
        try:
            annots = list(annots)
        except Exception:
            continue
        for a in annots:
            try:
                obj = a.get_object() if hasattr(a, "get_object") else a
                ft = obj.get("/FT") if hasattr(obj, "get") else None
                if str(ft) == "/Sig":
                    n += 1
            except Exception:
                continue
    return n


def page_text_headline(pdf, page_idx: int, n: int = 120) -> str:
    try:
        return ((pdf.pages[page_idx].extract_text() or "").strip())[:n].replace("\n", " | ")
    except Exception:
        return ""


def page_image_count(page) -> int:
    """Heuristic: count XObjects in /Resources (rough signature/stamp overlay count)."""
    try:
        resources = page.get("/Resources") or {}
        xobj = resources.get("/XObject") or {}
        return len(list(xobj.keys()) if hasattr(xobj, "keys") else [])
    except Exception:
        return 0


def main():
    import pdfplumber
    from pypdf import PdfReader

    generated = pick_generated()
    print("=" * 70)
    print(f"NORTH STAR  : {NORTH_STAR} ({os.path.getsize(NORTH_STAR):,} bytes)")
    print(f"GENERATED   : {generated} ({os.path.getsize(generated):,} bytes)")
    print("=" * 70)

    ns_r = PdfReader(NORTH_STAR)
    gn_r = PdfReader(generated)

    # 1. Page count
    print(f"\n── PAGE COUNT ──")
    print(f"  north star: {len(ns_r.pages)}")
    print(f"  generated : {len(gn_r.pages)}")
    page_count_match = len(ns_r.pages) == len(gn_r.pages)
    print(f"  match: {page_count_match}")

    gaps = []

    # 2. AcroForm fields
    print(f"\n── ACROFORM FIELDS ──")
    ns_fields = get_fields(ns_r)
    gn_fields = get_fields(gn_r)
    print(f"  north star fields: {len(ns_fields)}")
    print(f"  generated  fields: {len(gn_fields)}")
    only_in_ns = set(ns_fields) - set(gn_fields)
    only_in_gn = set(gn_fields) - set(ns_fields)
    shared = set(ns_fields) & set(gn_fields)
    if only_in_ns:
        print(f"  ⚠ only in north star ({len(only_in_ns)}): {sorted(list(only_in_ns))[:10]}")
        gaps.append(("fields_only_in_northstar", sorted(only_in_ns)))
    if only_in_gn:
        print(f"  ⚠ only in generated ({len(only_in_gn)}): {sorted(list(only_in_gn))[:10]}")
        gaps.append(("fields_only_in_generated", sorted(only_in_gn)))

    # 3. Field values on shared fields
    print(f"\n── FIELD VALUE DIFFS (on {len(shared)} shared fields) ──")
    value_diffs = []
    for name in sorted(shared):
        ns_v = (ns_fields[name] or "").strip()
        gn_v = (gn_fields[name] or "").strip()
        if ns_v != gn_v:
            value_diffs.append((name, ns_v, gn_v))
    if value_diffs:
        print(f"  {len(value_diffs)} fields differ.")
        for name, ns_v, gn_v in value_diffs[:50]:
            ns_preview = (ns_v[:50] + "…") if len(ns_v) > 50 else ns_v
            gn_preview = (gn_v[:50] + "…") if len(gn_v) > 50 else gn_v
            print(f"    {name!r:50} NS={ns_preview!r:55} GEN={gn_preview!r}")
        if len(value_diffs) > 50:
            print(f"    ... and {len(value_diffs) - 50} more")
        gaps.append(("value_diffs", [(n, a, b) for n, a, b in value_diffs]))
    else:
        print("  ✓ all shared field values match")

    # 4. Signatures
    print(f"\n── SIGNATURE WIDGETS (/Sig) ──")
    ns_sigs = count_sig_widgets(ns_r)
    gn_sigs = count_sig_widgets(gn_r)
    print(f"  north star: {ns_sigs}")
    print(f"  generated : {gn_sigs}")
    if ns_sigs != gn_sigs:
        gaps.append(("signature_count", {"northstar": ns_sigs, "generated": gn_sigs}))

    # 5. Per-page text headline (rough structure diff)
    print(f"\n── PAGE HEADLINES (first 120 chars each) ──")
    with pdfplumber.open(NORTH_STAR) as ns_pp, pdfplumber.open(generated) as gn_pp:
        max_pages = max(len(ns_pp.pages), len(gn_pp.pages))
        for i in range(max_pages):
            ns_h = page_text_headline(ns_pp, i) if i < len(ns_pp.pages) else "(no page)"
            gn_h = page_text_headline(gn_pp, i) if i < len(gn_pp.pages) else "(no page)"
            match = "✓" if ns_h[:30] == gn_h[:30] else "⚠"
            print(f"  p{i+1:2d} {match}")
            if match == "⚠":
                print(f"         NS : {ns_h}")
                print(f"         GEN: {gn_h}")
                gaps.append(("page_headline_diff", {"page": i + 1, "ns": ns_h, "gen": gn_h}))

    # 6. XObject (image) count per page — signature image overlays show here
    print(f"\n── PER-PAGE XOBJECT COUNT (signature-image overlay proxy) ──")
    ns_xobj = [page_image_count(p) for p in ns_r.pages]
    gn_xobj = [page_image_count(p) for p in gn_r.pages]
    for i in range(max(len(ns_xobj), len(gn_xobj))):
        ns_n = ns_xobj[i] if i < len(ns_xobj) else "-"
        gn_n = gn_xobj[i] if i < len(gn_xobj) else "-"
        if ns_n != gn_n:
            print(f"  p{i+1:2d}: NS={ns_n} GEN={gn_n}  ⚠")
            gaps.append(("xobject_count", {"page": i + 1, "ns": ns_n, "gen": gn_n}))

    # Summary
    print("\n" + "=" * 70)
    print(f"GAP SUMMARY: {len(gaps)} gap(s) found")
    print("=" * 70)
    summary = {
        "north_star": NORTH_STAR,
        "generated": generated,
        "page_count_match": page_count_match,
        "acroform_fields_only_in_northstar": sorted(only_in_ns),
        "acroform_fields_only_in_generated": sorted(only_in_gn),
        "value_diff_count": len(value_diffs),
        "sig_widgets": {"northstar": ns_sigs, "generated": gn_sigs},
        "total_gaps": len(gaps),
    }
    print(json.dumps(summary, indent=2))

    sys.exit(0 if not gaps else 1)


if __name__ == "__main__":
    main()
