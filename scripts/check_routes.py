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
                m = re.match(r'^@bp\.route\((.+)\)', line)
                if m:
                    args = m.group(1)
                    # Extract route path
                    pm = re.match(r'["\']([^"\']+)["\']', args)
                    if not pm:
                        continue
                    route = pm.group(1)
                    # Extract methods if specified
                    mm = re.search(r'methods\s*=\s*\[([^\]]+)\]', args)
                    if mm:
                        methods = frozenset(
                            s.strip().strip('"\'').upper()
                            for s in mm.group(1).split(",")
                        )
                    else:
                        methods = frozenset(["GET"])
                    routes.append((route, methods, fname, i))

    # Check for duplicates (same route path AND overlapping methods)
    seen = {}  # key: route path -> list of (methods, fname, line)
    dupes = []
    for route, methods, fname, line in routes:
        if route in seen:
            for prev_methods, prev_fname, prev_line in seen[route]:
                overlap = methods & prev_methods
                if overlap:
                    dupes.append((route, (prev_fname, prev_line), (fname, line),
                                  sorted(overlap)))
                    break
            seen[route].append((methods, fname, line))
        else:
            seen[route] = [(methods, fname, line)]

    return dupes


if __name__ == "__main__":
    dupes = find_duplicate_routes()
    if dupes:
        print(f"❌ DUPLICATE ROUTES FOUND ({len(dupes)}):")
        for entry in dupes:
            route, (f1, l1), (f2, l2) = entry[0], entry[1], entry[2]
            methods = entry[3] if len(entry) > 3 else []
            print(f"  {route} [{', '.join(methods)}]")
            print(f"    1st: {f1}:{l1}")
            print(f"    2nd: {f2}:{l2}")
        sys.exit(1)
    else:
        print("✅ No duplicate routes found")
        sys.exit(0)
