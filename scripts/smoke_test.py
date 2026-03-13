#!/usr/bin/env python3
"""
Pre-deploy smoke tests — validates critical business logic before push.
Run: python3 scripts/smoke_test.py

Tests:
1. Route dedup check (no duplicate Flask routes)
2. Echelon quote parser → 7 items with MFG refs
3. Supplier matching → 7/7 against known RFQ data
4. PC detection → known senders classified correctly
5. Flattened 704 parser → items extracted
6. Syntax check on all Python files
"""

import sys
import os
import re
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PASS = 0
FAIL = 0
WARN = 0


def ok(msg):
    global PASS
    PASS += 1
    print(f"  ✅ {msg}")


def fail(msg):
    global FAIL
    FAIL += 1
    print(f"  ❌ {msg}")


def warn(msg):
    global WARN
    WARN += 1
    print(f"  ⚠️  {msg}")


# ═══════════════════════════════════════════════════════════════════════
# Test 1: Route Dedup
# ═══════════════════════════════════════════════════════════════════════

def test_route_dedup():
    print("\n🔍 Test 1: Route Dedup Check")
    routes = []
    src_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "src", "api", "modules")
    
    for fname in sorted(os.listdir(src_dir)):
        if not fname.endswith(".py"):
            continue
        fpath = os.path.join(src_dir, fname)
        with open(fpath) as f:
            for i, line in enumerate(f, 1):
                m = re.match(r'^@bp\.route\((.+?)(?:,|\))', line)
                if m:
                    route = m.group(1).strip('"\'')
                    # Check methods to distinguish GET vs POST on same route
                    methods_m = re.search(r'methods=\[([^\]]+)\]', line)
                    methods = methods_m.group(1) if methods_m else "GET"
                    routes.append((route, methods, fname, i))

    # Check for TRUE duplicates (same route + same methods)
    seen = {}
    dupes = []
    for route, methods, fname, line in routes:
        key = f"{route}|{methods}"
        if key in seen:
            dupes.append((route, methods, seen[key], (fname, line)))
        else:
            seen[key] = (fname, line)

    if dupes:
        for route, methods, (f1, l1), (f2, l2) in dupes:
            fail(f"Duplicate route: {route} [{methods}] in {f1}:{l1} AND {f2}:{l2}")
    else:
        ok("No duplicate routes with same methods")


# ═══════════════════════════════════════════════════════════════════════
# Test 2: Echelon Quote Parser
# ═══════════════════════════════════════════════════════════════════════

def test_echelon_parser():
    print("\n🔍 Test 2: Echelon Quote Parser")
    
    # Find test PDF
    test_pdf = None
    for path in [
        "/mnt/user-data/uploads/ECHQ1223298.pdf",
        "tests/fixtures/ECHQ1223298.pdf",
    ]:
        if os.path.exists(path):
            test_pdf = path
            break
    
    if not test_pdf:
        warn("Echelon test PDF not found — skipping")
        return
    
    from src.forms.supplier_quote_parser import parse_supplier_quote
    result = parse_supplier_quote(test_pdf)
    
    if not result.get("ok"):
        fail(f"Parser failed: {result.get('error', 'unknown')}")
        return
    
    items = result.get("items", [])
    if len(items) != 7:
        fail(f"Expected 7 items, got {len(items)}")
    else:
        ok(f"Parsed {len(items)} items")
    
    if result.get("supplier") != "Echelon Distribution":
        fail(f"Supplier={result.get('supplier')}, expected Echelon Distribution")
    else:
        ok("Supplier detected: Echelon Distribution")
    
    if result.get("quote_number") != "ECHQ1223298":
        fail(f"Quote#={result.get('quote_number')}, expected ECHQ1223298")
    else:
        ok("Quote# detected: ECHQ1223298")
    
    # Check specific items
    pns = [it.get("item_number", "") for it in items]
    expected_pns = ["449317", "172018", "EQX7044", "MDS098001Z", "826991", "666134", "NON03005"]
    for epn in expected_pns:
        if epn in pns:
            ok(f"Part# {epn} found")
        else:
            fail(f"Part# {epn} MISSING from parsed items")
    
    # Check MFG refs preserved in descriptions (critical for matching)
    for it in items:
        desc = it.get("description", "")
        pn = it.get("item_number", "")
        if pn in ("449317", "172018", "826991", "666134"):
            if "Manufacturer" in desc or "McKesson" in desc or "Mc Kesson" in desc:
                ok(f"{pn}: MFG/McKesson refs preserved in description")
            else:
                fail(f"{pn}: MFG/McKesson refs STRIPPED from description — matching will break")


# ═══════════════════════════════════════════════════════════════════════
# Test 3: Supplier Matching
# ═══════════════════════════════════════════════════════════════════════

def test_supplier_matching():
    print("\n🔍 Test 3: Supplier Quote → RFQ Matching")
    
    test_pdf = None
    for path in [
        "/mnt/user-data/uploads/ECHQ1223298.pdf",
        "tests/fixtures/ECHQ1223298.pdf",
    ]:
        if os.path.exists(path):
            test_pdf = path
            break
    
    if not test_pdf:
        warn("Echelon test PDF not found — skipping")
        return
    
    from src.forms.supplier_quote_parser import parse_supplier_quote, match_quote_to_rfq
    parsed = parse_supplier_quote(test_pdf)
    quote_items = parsed.get("items", [])
    
    # Simulated CalVet RFQ items
    rfq_items = [
        {"description": "Promogran Collagen Matrix Wound Dressing # J-JPG004Z", "item_number": "J-JPG004Z", "qty": 40, "uom": "BX"},
        {"description": "Drainable Pouch, ActiveLife # SQU022767", "item_number": "SQU022767", "qty": 10, "uom": "BX"},
        {"description": 'Calcium Alginate Wound Dressing, 4" x 4" 50/CS # EQX7044', "item_number": "EQX7044", "qty": 8, "uom": "CS"},
        {"description": "Hydrogen Peroxide, 3%, 16 oz. 12/CS # MDS098001Z", "item_number": "MDS098001Z", "qty": 2, "uom": "CS"},
        {"description": "Emery Board, 2-Sided, 4.5\", 24/CS # DYA4895", "item_number": "DYA4895", "qty": 2, "uom": "CS"},
        {"description": "VanishPoint 0.5 mL Insulin Syringe # RT115221", "item_number": "RT115221", "qty": 12, "uom": "CS"},
        {"description": "Plastic Drinking Cup, 5 oz. 2500/CS # NON03005", "item_number": "NON03005", "qty": 40, "uom": "CS"},
    ]
    
    matches = match_quote_to_rfq(quote_items, rfq_items)
    matched = sum(1 for m in matches if m.get("matched"))
    
    if matched == 7:
        ok(f"All 7/7 items matched")
    else:
        fail(f"Only {matched}/7 matched — cross-reference broken")
        for m in matches:
            if not m.get("matched"):
                fail(f"  Unmatched: Q[{m['quote_idx']}] pn={m['quote_pn']} conf={m.get('confidence',0):.2f}")


# ═══════════════════════════════════════════════════════════════════════
# Test 4: PC Detection (email classification)
# ═══════════════════════════════════════════════════════════════════════

def test_pc_detection():
    print("\n🔍 Test 4: Price Check Email Detection")
    
    from src.agents.email_poller import is_price_check_email, is_rfq_email
    
    # Katrina Valencia — should be PC
    result = is_price_check_email(
        "Price Quote Request",
        "Can you please review the attached order and let me know if you will be able to provide a price quote?",
        "Katrina Valencia <Katrina.Valencia@cdcr.ca.gov>",
        ["PIP Hygiene Order Form.pdf"]
    )
    if result and result.get("is_price_check"):
        ok(f"Katrina Valencia detected as PC (score={result['score']})")
    else:
        fail("Katrina Valencia NOT detected as PC")
    
    rfq = is_rfq_email(
        "Price Quote Request", "price quote", ["PIP Hygiene Order Form.pdf"],
        sender_email="Katrina.Valencia@cdcr.ca.gov"
    )
    if not rfq:
        ok("Katrina's email correctly NOT classified as RFQ")
    else:
        fail("Katrina's email incorrectly classified as RFQ (should be PC)")
    
    # Grace Pfost — should be RFQ (has 703B + 704B)
    rfq2 = is_rfq_email(
        "Request for Bid: 10840673 -CTF",
        "Hello, Please sign and quote",
        ["AMS 703B.pdf", "AMS 704B.pdf", "BID PACKAGE.pdf"],
        sender_email="grace.pfost@cdcr.ca.gov"
    )
    if rfq2:
        ok("Grace Pfost 'Request for Bid' classified as RFQ")
    else:
        fail("Grace Pfost 'Request for Bid' NOT classified as RFQ")
    
    # Valentina Demidenko — should be PC (known sender)
    result3 = is_price_check_email(
        "Quote - Airway Adapter",
        "Please email me a quote",
        "valentina.demidenko@cdcr.ca.gov",
        ["AMS_704.pdf"]
    )
    if result3 and result3.get("is_price_check"):
        ok(f"Valentina detected as PC (score={result3['score']})")
    else:
        fail("Valentina NOT detected as PC")


# ═══════════════════════════════════════════════════════════════════════
# Test 5: Flattened 704 Parser
# ═══════════════════════════════════════════════════════════════════════

def test_flattened_704():
    print("\n🔍 Test 5: DocuSign-Flattened 704 Parser")
    
    test_pdf = None
    for path in [
        "/mnt/user-data/uploads/PIP_Hygiene_Order_3_12_2026.pdf",
        "tests/fixtures/PIP_Hygiene_Order.pdf",
    ]:
        if os.path.exists(path):
            test_pdf = path
            break
    
    if not test_pdf:
        warn("Flattened 704 test PDF not found — skipping")
        return
    
    from src.forms.price_check import parse_ams704
    result = parse_ams704(test_pdf)
    
    if result.get("error"):
        fail(f"Parser error: {result['error']}")
        return
    
    items = result.get("line_items", [])
    header = result.get("header", {})
    method = result.get("parse_method", "?")
    
    if len(items) >= 15:
        ok(f"Extracted {len(items)} items (method={method})")
    elif len(items) > 0:
        warn(f"Only {len(items)} items extracted (expected 27, method={method})")
    else:
        fail("Zero items extracted from flattened 704")
    
    if header.get("requestor"):
        ok(f"Requestor: {header['requestor']}")
    else:
        fail("Requestor not extracted")
    
    if header.get("institution"):
        ok(f"Institution: {header['institution']}")
    else:
        fail("Institution not extracted")


# ═══════════════════════════════════════════════════════════════════════
# Test 6: Syntax Check
# ═══════════════════════════════════════════════════════════════════════

def test_syntax():
    print("\n🔍 Test 6: Python Syntax Check")
    
    import py_compile
    errors = []
    checked = 0
    
    for root, dirs, files in os.walk("src"):
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(root, f)
                try:
                    py_compile.compile(path, doraise=True)
                    checked += 1
                except py_compile.PyCompileError as e:
                    errors.append(f"{path}: {e}")
    
    if errors:
        for e in errors:
            fail(f"Syntax error: {e}")
    else:
        ok(f"All {checked} Python files compile cleanly")


# ═══════════════════════════════════════════════════════════════════════
# Run All Tests
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("🧪 REYTECH PRE-DEPLOY SMOKE TESTS")
    print("=" * 60)
    
    test_route_dedup()
    test_syntax()
    test_pc_detection()
    test_echelon_parser()
    test_supplier_matching()
    test_flattened_704()
    
    print("\n" + "=" * 60)
    total = PASS + FAIL + WARN
    if FAIL == 0:
        print(f"✅ ALL CLEAR: {PASS} passed, {WARN} warnings, 0 failures")
        sys.exit(0)
    else:
        print(f"❌ FAILURES: {FAIL} failed, {PASS} passed, {WARN} warnings")
        print(f"   DO NOT PUSH until failures are fixed.")
        sys.exit(1)
