#!/usr/bin/env python3
"""
Pre-deploy safety check: detect duplicate Flask routes before pushing.
Run: python3 scripts/check_routes.py
"""
import re
import sys
import os

def find_duplicate_routes():
    routes = []
    src_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src", "api", "modules")
    
    for fname in sorted(os.listdir(src_dir)):
        if not fname.endswith(".py"):
            continue
        fpath = os.path.join(src_dir, fname)
        with open(fpath, encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f, 1):
                m = re.match(r'^@bp\.route\((.+?)(?:,|\))', line)
                if m:
                    route = m.group(1).strip('"\'')
                    routes.append((route, fname, i))

    # Check for duplicates (same route string)
    seen = {}
    dupes = []
    for route, fname, line in routes:
        # Normalize: strip methods for comparison
        if route in seen:
            dupes.append((route, seen[route], (fname, line)))
        else:
            seen[route] = (fname, line)

    return dupes


if __name__ == "__main__":
    dupes = find_duplicate_routes()
    if dupes:
        print(f"❌ DUPLICATE ROUTES FOUND ({len(dupes)}):")
        for route, (f1, l1), (f2, l2) in dupes:
            print(f"  {route}")
            print(f"    1st: {f1}:{l1}")
            print(f"    2nd: {f2}:{l2}")
        sys.exit(1)
    else:
        print("✅ No duplicate routes found")
        sys.exit(0)
