#!/usr/bin/env python3
"""CLI wrapper around request_classifier.classify_request().

Drop any buyer file on the command line → get a full classification
JSON back. Useful for debugging new file types offline without going
through the web UI.

Usage:
    python scripts/classify_fixture.py <file_path>
    python scripts/classify_fixture.py <file1> <file2> ...
    python scripts/classify_fixture.py --email-sender buyer@cchcs.ca.gov <file>
    python scripts/classify_fixture.py --subject "PREQ..." --sender x@y.com

Exit codes:
    0 — classification succeeded (any shape, including unknown)
    1 — file not found or crashed
"""
from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)

os.environ.setdefault("SECRET_KEY", "diag")
os.environ.setdefault("DASH_USER", "diag")
os.environ.setdefault("DASH_PASS", "diag")
os.environ.setdefault("FLASK_ENV", "testing")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Classify one or more buyer files via request_classifier"
    )
    parser.add_argument("files", nargs="*", help="paths to files to classify")
    parser.add_argument("--subject", default="", help="email subject hint")
    parser.add_argument("--sender", default="", help="email sender hint")
    parser.add_argument("--body", default="", help="email body hint")
    parser.add_argument("--pretty", action="store_true", default=True)
    parser.add_argument("--quiet", "-q", action="store_true", help="JSON only")
    args = parser.parse_args()

    if not args.files and not (args.body or args.subject):
        parser.print_help()
        return 1

    missing = [f for f in args.files if not os.path.exists(f)]
    if missing:
        print(f"ERROR: files not found: {missing}", file=sys.stderr)
        return 1

    try:
        from src.core.request_classifier import classify_request
    except Exception as e:
        print(f"ERROR: could not import classifier: {e}", file=sys.stderr)
        return 1

    try:
        result = classify_request(
            attachments=args.files,
            email_body=args.body,
            email_subject=args.subject,
            email_sender=args.sender,
        )
    except Exception as e:
        print(f"ERROR: classifier crashed: {e}", file=sys.stderr)
        return 1

    d = result.to_dict()
    if args.quiet:
        print(json.dumps(d))
        return 0

    # Pretty-print summary first, then the full JSON
    print("═" * 60)
    print(f"  shape:          {d['shape']}")
    print(f"  agency:         {d['agency']} ({d['agency_name']})")
    print(f"  confidence:     {d['confidence']:.0%}")
    if d["solicitation_number"]:
        print(f"  solicitation:   {d['solicitation_number']}")
    if d["institution"]:
        print(f"  institution:    {d['institution']}")
    print(f"  primary file:   {d['primary_file']}")
    print(f"  quote only:     {d['is_quote_only']}")
    print(f"  needs overlay:  {d['needs_overlay_fill']}")
    print(f"  required forms: {', '.join(d['required_forms']) or '(none)'}")
    print()
    print("  reasons:")
    for r in d["reasons"]:
        print(f"    - {r}")
    print("═" * 60)
    print()
    print(json.dumps(d, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
