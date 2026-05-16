#!/usr/bin/env python3
"""Parse downloaded .eml files into candidate EmailContract dicts.

Phase 1 substrate validation against real 5/18-5/19 CCHCS bids.
Reads every .eml under `_diag/bids_2026_05_16/`, extracts:

    - subject → solicitation_number candidate
    - From header → buyer_name, buyer_email
    - Date header → release_date
    - Attachment filenames → required_forms classification
    - Body excerpt → buyer_hints / parse_confidence signal

Prints one section per .eml with the EmailContract candidate it
would construct. Operator (Mike) reviews + corrects before the
contract is written to the Spine via `write_email_contract`.

Run:
    python scripts/parse_bid_emls.py [path/to/eml_dir]

Default path: C:\\Users\\mikeg\\rfq-spine-foundation\\_diag\\bids_2026_05_16

This script is one-shot scaffolding. The production ingest path is
src.spine_bridge.ingest.ingest_email_contract — once Vision is wired
to produce the contract dict, this script's job is done.
"""
from __future__ import annotations

import email
import email.policy
import json
import re
import sys
from datetime import datetime, timezone
from email.utils import parseaddr, parsedate_to_datetime
from pathlib import Path

# Default bids directory matches the handoff.
DEFAULT_DIR = Path(
    r"C:\Users\mikeg\rfq-spine-foundation\_diag\bids_2026_05_16"
)


# ──────────────────────────────────────────────────────────────────────
# Subject-line solicitation extractor
# ──────────────────────────────────────────────────────────────────────

# CCHCS solicitation numbers are 8 digits (e.g. 10847457). The PREQ
# prefix is sometimes attached ("PREQ 10847457"); the Spine substrate's
# strip_solicitation_prefix helper handles that downstream, but we
# emit both raw and stripped here for operator review.
_SOL_RE = re.compile(r"\b(?:PREQ\s+)?(\d{8})\b", re.IGNORECASE)
_FACILITY_RE = re.compile(
    r"\b(SAC|VSP|CHCF|CHCWF|CCWF|SATF|CIW|CTF|HDSP|PVSP|FOL)\b",
    re.IGNORECASE,
)


def extract_sol(subject: str) -> str | None:
    m = _SOL_RE.search(subject or "")
    return m.group(1) if m else None


def extract_facility_hint(text: str) -> str | None:
    m = _FACILITY_RE.search(text or "")
    return m.group(1).upper() if m else None


# ──────────────────────────────────────────────────────────────────────
# Attachment → FormCode classifier
# ──────────────────────────────────────────────────────────────────────
# Heuristic only — the Spine's parse_confidence field carries forward
# whether this classifier was sure or not. CCHCS bids almost always
# attach a 703B + 704B + bid package (the empirical CCHCS_DEFAULT_REQUIRED_FORMS).

_FORM_HINTS = {
    "703b":          [r"703\s*b\b", r"703-b"],
    "703c":          [r"703\s*c\b", r"703-c"],
    "704b":          [r"704\s*b\b", r"704-b", r"\bAMS[\s_-]*704\b"],
    "704c":          [r"704\s*c\b", r"704-c"],
    "bidpkg":        [r"bid[\s_-]*package", r"bidpkg", r"non[\s_-]*it[\s_-]*rfq[\s_-]*packet"],
    "calrecycle_74": [r"calrecycle[\s_-]*74", r"epp[\s_-]*74"],
    "std_204":       [r"\bSTD[\s_-]*204\b", r"payee[\s_-]*data"],
    "std_1000":      [r"\bSTD[\s_-]*1000\b", r"genAI"],
    "dvbe_843":      [r"\bDVBE[\s_-]*843\b", r"\bSTD[\s_-]*843\b"],
    "darfur":        [r"darfur"],
    "cuf":           [r"\bcv[\s_-]*012\b", r"commercially[\s_-]*useful"],
    "quote":         [r"\bquote\b", r"\bRFQ[\s_-]*response\b"],
}


def classify_attachment(filename: str) -> list[str]:
    """Return FormCodes that this filename likely maps to."""
    matches = []
    name = (filename or "").lower()
    for code, patterns in _FORM_HINTS.items():
        for pat in patterns:
            if re.search(pat, name, re.IGNORECASE):
                matches.append(code)
                break
    return matches


# ──────────────────────────────────────────────────────────────────────
# .eml → candidate EmailContract dict
# ──────────────────────────────────────────────────────────────────────


def parse_eml(eml_path: Path) -> dict:
    """Return a candidate EmailContract-shaped dict (operator reviews)."""
    raw = eml_path.read_bytes()
    msg = email.message_from_bytes(raw, policy=email.policy.default)

    subject = (msg.get("Subject") or "").strip()
    from_hdr = msg.get("From") or ""
    buyer_name, buyer_email = parseaddr(from_hdr)
    date_hdr = msg.get("Date") or ""
    received_at = None
    try:
        if date_hdr:
            received_at = parsedate_to_datetime(date_hdr)
            if received_at and received_at.tzinfo is None:
                received_at = received_at.replace(tzinfo=timezone.utc)
    except Exception:
        received_at = None

    sol = extract_sol(subject)
    facility = extract_facility_hint(subject)

    # Walk attachments
    attachments: list[dict] = []
    body_text = ""
    for part in msg.walk():
        if part.is_multipart():
            continue
        ctype = part.get_content_type()
        disp = (part.get("Content-Disposition") or "").lower()
        fname = part.get_filename()
        if fname:
            try:
                payload = part.get_payload(decode=True)
                size = len(payload) if payload else 0
            except Exception:
                size = 0
            codes = classify_attachment(fname)
            attachments.append({
                "filename": fname,
                "content_type": ctype,
                "size_bytes": size,
                "form_codes": codes,
            })
        elif ctype == "text/plain" and "attachment" not in disp:
            try:
                body_text += (part.get_content() or "")
            except Exception:
                pass

    # Required-forms aggregation: dedupe + sort by first-seen order
    seen: dict[str, None] = {}
    for att in attachments:
        for code in att["form_codes"]:
            seen.setdefault(code, None)
    # CCHCS empirical: ensure 'quote' is in required_forms if it's a
    # CCHCS bid (we always include the Reytech Quote PDF response).
    # Operator can override if the buyer asks differently.
    detected_codes = list(seen.keys())

    # Confidence heuristic:
    #   high   = sol# present + at least one of 703b/704b/bidpkg present
    #   medium = sol# present OR (703b/704b/bidpkg present) but not both
    #   low    = neither
    has_strong_form = any(c in detected_codes for c in ("703b", "703c", "704b", "704c", "bidpkg"))
    if sol and has_strong_form:
        conf = "high"
    elif sol or has_strong_form:
        conf = "medium"
    else:
        conf = "low"

    return {
        "source_eml_path": str(eml_path),
        "subject": subject,
        "buyer_name": buyer_name,
        "buyer_email": buyer_email,
        "received_at": received_at.isoformat() if received_at else None,
        "extracted": {
            "solicitation_number_candidate": sol,
            "facility_candidate": facility,
            "required_forms_candidate": detected_codes,
            "parse_confidence": conf,
        },
        "attachments": attachments,
        "body_excerpt": body_text[:500],
    }


def main(argv: list[str]) -> int:
    target = Path(argv[1]) if len(argv) > 1 else DEFAULT_DIR
    if not target.exists():
        print(f"ERROR: {target} does not exist.", file=sys.stderr)
        return 2

    emls = sorted(target.glob("*.eml"))
    if not emls:
        print(f"No .eml files in {target}", file=sys.stderr)
        return 1

    print(f"# Parsing {len(emls)} .eml file(s) from {target}\n")
    out = []
    for p in emls:
        try:
            candidate = parse_eml(p)
            out.append(candidate)
            print("─" * 70)
            print(f"## {p.name}")
            print(f"  Subject:           {candidate['subject']!r}")
            print(f"  Buyer:             {candidate['buyer_name']!r} <{candidate['buyer_email']}>")
            print(f"  Received:          {candidate['received_at']}")
            ex = candidate["extracted"]
            print(f"  Sol# candidate:    {ex['solicitation_number_candidate']!r}")
            print(f"  Facility candidate: {ex['facility_candidate']!r}")
            print(f"  Required forms:    {ex['required_forms_candidate']!r}")
            print(f"  Parse confidence:  {ex['parse_confidence']!r}")
            print(f"  Attachments ({len(candidate['attachments'])}):")
            for att in candidate["attachments"]:
                print(f"    - {att['filename']} ({att['size_bytes']:,}B)"
                      f" → {att['form_codes']!r}")
        except Exception as e:
            print(f"ERROR parsing {p.name}: {e}", file=sys.stderr)
            out.append({"source_eml_path": str(p), "error": str(e)})

    # Also write a JSON dump alongside the .emls for downstream use.
    json_out = target / "candidates.json"
    json_out.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"\nCandidates JSON written: {json_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
