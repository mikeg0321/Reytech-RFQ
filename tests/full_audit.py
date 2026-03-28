#!/usr/bin/env python3
"""Full system audit — tests every component end-to-end."""
import os, sys, json, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("TESTING", "1")
os.environ.setdefault("SECRET_KEY", "test")
os.environ.setdefault("FLASK_ENV", "testing")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

errors = []
print("=" * 70)
print("FULL SYSTEM AUDIT")
print("=" * 70)

# 1. Database
print("\n[1] DATABASE")
conn = sqlite3.connect("data/reytech.db", timeout=10)
for t in ["won_quotes","product_catalog","quotes","price_checks","orders","scprs_po_master","scprs_po_lines","contacts","price_history"]:
    try:
        c = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:25} {c:>8} rows")
    except Exception as e:
        print(f"  {t:25} ERROR: {e}")
        errors.append(f"DB {t}: {e}")
conn.close()

# 2. Imports
print("\n[2] IMPORTS")
for name, mod, fn in [
    ("circuit_breaker", "src.core.circuit_breaker", "get_breaker"),
    ("tracing", "src.core.tracing", "set_trace_id"),
    ("enrichment", "src.agents.pc_enrichment_pipeline", "enrich_pc_background"),
    ("won_quotes", "src.knowledge.won_quotes_db", "sync_from_scprs_tables"),
    ("item_enricher", "src.agents.item_enricher", "parse_identifiers"),
    ("form_qa", "src.forms.form_qa", "run_form_qa"),
    ("pricing_oracle", "src.knowledge.pricing_oracle", "recommend_price"),
]:
    try:
        m = __import__(mod, fromlist=[fn])
        assert hasattr(m, fn), f"{fn} not found"
        print(f"  {name:25} OK")
    except Exception as e:
        print(f"  {name:25} FAIL: {e}")
        errors.append(f"Import {name}: {e}")

# 3. Enrichment matching
print("\n[3] ENRICHMENT MATCHING")
from src.knowledge.won_quotes_db import find_similar_items
matched = 0
for desc in ["GLOVE EXAM NITRILE", "Book Paperback", "Razor Shave Cream", "Pen Pencil Office"]:
    r = find_similar_items("", desc, max_results=1, min_confidence=0.3)
    if r:
        matched += 1
        q = r[0].get("quote", {})
        conf = r[0].get("match_confidence", 0)
        print(f"  {desc:25} MATCH ${q.get('unit_price',0):.2f} conf={conf:.0%}")
    else:
        print(f"  {desc:25} no match")
print(f"  Result: {matched}/4 matched")

# 4. Identifiers
print("\n[4] IDENTIFIER EXTRACTION")
from src.agents.item_enricher import parse_identifiers
for desc in ["NITRILE GLOVES MFG# MDS195175", "B07TEST123 Widget", "Plain no ids"]:
    r = parse_identifiers(desc)
    mfg = r.get("primary_mfg_number", "")
    asin = r.get("primary_asin", "")
    ids = f"mfg={mfg}" if mfg else (f"asin={asin}" if asin else "none")
    print(f"  {desc[:35]:35} {ids}")

# 5. Circuit breaker
print("\n[5] CIRCUIT BREAKER")
from src.core.circuit_breaker import get_breaker
b = get_breaker("audit")
assert b.call(lambda: 42) == 42
print(f"  State: {b.state} | OK")

# 6. Tracing
print("\n[6] TRACING")
from src.core.tracing import set_trace_id, get_trace_id, trace_context
tid = set_trace_id(operation="audit")
with trace_context("inner"):
    pass
assert get_trace_id() == tid
print(f"  Trace: {tid} | context restore OK")

# 7. URL extraction
print("\n[7] URL EXTRACTION")
from src.agents.pc_enrichment_pipeline import _extract_urls_from_items
items = [
    {"description": "Wipes https://www.amazon.com/dp/B123", "pricing": {}},
    {"description": "No URL here", "pricing": {}},
]
c = _extract_urls_from_items(items)
print(f"  Extracted: {c}/2 | link={items[0].get('item_link','')[:40]}")

# 8. Templates
print("\n[8] TEMPLATES")
from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader("src/templates"))
for t in ["home.html","analytics.html","business_intel.html","pc_detail.html","rfq_review.html","po_upload.html","base.html"]:
    try:
        env.get_template(t)
        print(f"  {t:30} OK")
    except Exception as e:
        print(f"  {t:30} FAIL")
        errors.append(f"Template {t}: {e}")

# 9. Python compilation
print("\n[9] COMPILATION")
import py_compile
for f in ["src/api/dashboard.py","src/api/shared.py","src/api/modules/routes_rfq.py",
          "src/api/modules/routes_pricecheck.py","src/api/modules/routes_analytics.py",
          "src/api/modules/routes_orders_full.py","src/core/circuit_breaker.py",
          "src/core/tracing.py","src/agents/pc_enrichment_pipeline.py",
          "src/knowledge/won_quotes_db.py","src/core/db.py","app.py"]:
    try:
        py_compile.compile(f, doraise=True)
        print(f"  {f:50} OK")
    except Exception as e:
        print(f"  {f:50} FAIL")
        errors.append(f"Compile {f}: {e}")

# Summary
print("\n" + "=" * 70)
if errors:
    print(f"RESULT: {len(errors)} ERRORS")
    for e in errors:
        print(f"  FAIL: {e}")
else:
    print("RESULT: ALL CHECKS PASSED")
print("=" * 70)
sys.exit(1 if errors else 0)
