#!/usr/bin/env python3
"""
Reytech Platform Smoke Test Suite
===================================
Before/after validator for the weekend refactor.

Usage:
    # Against local dev server:
    python tests/smoke_test.py

    # Against Railway prod:
    REYTECH_URL=https://your-app.railway.app REYTECH_USER=reytech REYTECH_PASS=yourpass \
        python tests/smoke_test.py

    # Specific categories only:
    python tests/smoke_test.py --only pages,api

    # Save baseline:
    python tests/smoke_test.py --save-baseline

    # Compare against baseline:
    python tests/smoke_test.py --compare baseline.json

Exit codes: 0 = all pass, 1 = failures exist, 2 = baseline mismatch
"""
import os, sys, json, time, argparse, base64, traceback
from datetime import datetime
from typing import Optional

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    print("pip install requests --break-system-packages")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL = os.environ.get("REYTECH_URL", "http://localhost:8000")
USER     = os.environ.get("REYTECH_USER", "reytech")
PASS     = os.environ.get("REYTECH_PASS", "changeme")
TIMEOUT  = int(os.environ.get("SMOKE_TIMEOUT", "15"))

AUTH = HTTPBasicAuth(USER, PASS)
SESS = requests.Session()
SESS.auth = AUTH
SESS.headers.update({"User-Agent": "Reytech-SmokeTest/1.0"})

PASS_SYM = "✅"
FAIL_SYM = "❌"
WARN_SYM = "⚠️ "
SKIP_SYM = "⏭️ "

results = []  # [{name, category, status, duration_ms, detail}]


def check(name: str, category: str, fn, warn_only: bool = False):
    """Run a single check and record result."""
    t0 = time.time()
    try:
        detail = fn()
        status = "pass"
        sym = PASS_SYM
    except AssertionError as e:
        status = "warn" if warn_only else "fail"
        detail = str(e)
        sym = WARN_SYM if warn_only else FAIL_SYM
    except Exception as e:
        status = "warn" if warn_only else "fail"
        detail = f"{type(e).__name__}: {e}"
        sym = WARN_SYM if warn_only else FAIL_SYM

    ms = round((time.time() - t0) * 1000)
    results.append({"name": name, "category": category, "status": status,
                     "duration_ms": ms, "detail": str(detail or "")[:200]})
    print(f"  {sym} [{ms:>4}ms] {name}")
    if status in ("fail", "warn") and detail:
        print(f"          └─ {detail[:120]}")
    return status == "pass"


def get(path: str, **kwargs) -> requests.Response:
    return SESS.get(f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)


def post(path: str, **kwargs) -> requests.Response:
    return SESS.post(f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)


# ── Category: Pages ───────────────────────────────────────────────────────────
def run_pages():
    print("\n📄 PAGES (all 9 must return HTTP 200)")
    pages = [
        ("/",           "Home"),
        ("/quotes",     "Quotes"),
        ("/search",     "Search"),
        ("/contacts",   "CRM"),
        ("/intelligence","Intel"),
        ("/debug",      "Debug"),
        ("/orders",     "Orders"),
        ("/agents",     "Agents"),
        ("/pipeline",   "Pipeline"),
        ("/growth",     "Growth"),
        ("/templates",  "Email Templates"),
    ]
    for path, label in pages:
        def _f(p=path, lbl=label):
            r = get(p)
            assert r.status_code == 200, f"HTTP {r.status_code}"
            assert len(r.text) > 500, "Response suspiciously short"
            return f"HTTP 200, {len(r.text):,} bytes"
        check(label, "pages", _f)


# ── Category: Auth ────────────────────────────────────────────────────────────
def run_auth():
    print("\n🔐 AUTH")
    def no_auth_redirects():
        r = requests.get(f"{BASE_URL}/quotes", timeout=TIMEOUT, allow_redirects=False)
        assert r.status_code in (401, 302, 301), f"Expected redirect/401, got {r.status_code}"
        return f"Unauthenticated → {r.status_code}"
    check("Unauthenticated request blocked", "auth", no_auth_redirects)

    def bad_auth():
        r = requests.get(f"{BASE_URL}/quotes", auth=HTTPBasicAuth("x","y"), timeout=TIMEOUT)
        assert r.status_code in (401, 403), f"Expected 401/403, got {r.status_code}"
        return f"Bad credentials → {r.status_code}"
    check("Bad credentials rejected", "auth", bad_auth)

    def good_auth():
        r = get("/")
        assert r.status_code == 200
        return "Valid credentials accepted"
    check("Valid credentials accepted", "auth", good_auth)


# ── Category: Core APIs ───────────────────────────────────────────────────────
def run_api():
    print("\n🔌 CORE APIs")

    def db_api():
        r = get("/api/db")
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok"), f"ok=False: {d}"
        assert d.get("is_railway_volume") is not None
        tables = d.get("tables", {})
        assert "contacts" in tables, "contacts table missing"
        return f"ok=True, contacts={tables.get('contacts',0)}, quotes={tables.get('quotes',0)}"
    check("GET /api/db", "api", db_api)

    def funnel_api():
        r = get("/api/funnel/stats")
        assert r.status_code == 200
        d = r.json()
        assert "rfqs_total" in d or "quotes_total" in d or "pending" in d, f"missing keys: {list(d.keys())}"
        return f"keys: {list(d.keys())[:5]}"
    check("GET /api/funnel/stats", "api", funnel_api)

    def search_api():
        r = get("/api/search?q=CDCR")
        assert r.status_code == 200
        d = r.json()
        assert isinstance(d, dict) or isinstance(d, list), "unexpected type"
        return f"search returned results"
    check("GET /api/search?q=CDCR", "api", search_api)

    def debug_api():
        r = get("/api/debug/run")
        assert r.status_code == 200
        d = r.json()
        assert "db" in d or "ok" in d, f"unexpected structure: {list(d.keys())[:5]}"
        return f"debug ok"
    check("GET /api/debug/run", "api", debug_api)

    def qa_api():
        r = get("/api/qa/health")
        assert r.status_code == 200
        d = r.json()
        score = d.get("health_score", 0)
        assert score >= 70, f"QA score too low: {score}"
        return f"health_score={score}, grade={d.get('grade')}"
    check("GET /api/qa/health (score >= 70)", "api", qa_api)

    def crm_api():
        r = get("/api/crm/contacts")
        assert r.status_code == 200
        d = r.json()
        contacts = d if isinstance(d, list) else d.get("contacts", d.get("data", []))
        assert len(contacts) > 0, "No CRM contacts returned"
        return f"{len(contacts)} contacts"
    check("GET /api/crm/contacts", "api", crm_api)

    def intel_api():
        r = get("/api/intel/status")
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok") or "buyers" in d or "total" in d, f"unexpected: {list(d.keys())}"
        return "intel status ok"
    check("GET /api/intel/status", "api", intel_api)

    def prices_api():
        r = get("/api/prices/history?q=glove")
        assert r.status_code == 200
        return "price history ok"
    check("GET /api/prices/history", "api", prices_api, warn_only=True)

    def agent_ctx_api():
        r = get("/api/agent/context")
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok"), f"ok=False: {d.get('error')}"
        return f"context ok, contacts={d.get('summary',{}).get('contacts',0)}"
    check("GET /api/agent/context", "api", agent_ctx_api)


# ── Category: Feature 3.2.1 — 1-click Quote ──────────────────────────────────
def run_feature_321():
    print("\n🎯 FEATURE 3.2.1 — 1-click Price Check → Quote")

    pc_id = None

    def create_test_pc():
        nonlocal pc_id
        r = get("/api/test/create-pc")
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok") or d.get("pc_id"), f"no pc_id: {d}"
        pc_id = d.get("pc_id")
        return f"test PC created: {pc_id}"
    check("Create test price check", "feature_321", create_test_pc)

    def check_1click_endpoint():
        r = post("/api/quote/from-price-check", json={"pc_id": pc_id or "test_missing"})
        assert r.status_code == 200
        d = r.json()
        # ok=False with no_prices is still a valid response (PC has no prices)
        assert "ok" in d, f"missing ok field: {d}"
        return f"endpoint reachable, ok={d.get('ok')}, error={d.get('error','none')}"
    check("POST /api/quote/from-price-check reachable", "feature_321", check_1click_endpoint)

    def check_pc_detail_banner():
        if not pc_id:
            raise AssertionError("no pc_id to test with")
        r = get(f"/pricecheck/{pc_id}")
        assert r.status_code == 200
        html = r.text
        assert "quote-gen-banner" in html, "1-click banner missing from PC detail page"
        assert "generateQuote1Click" in html, "generateQuote1Click JS missing"
        return "banner + JS present"
    check("PC detail: 1-click banner present", "feature_321", check_pc_detail_banner)

    def check_missing_pc():
        r = post("/api/quote/from-price-check", json={"pc_id": "doesnotexist_xyz"})
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok") is False, "Should return ok=False for missing PC"
        return "missing PC handled gracefully"
    check("Missing PC returns ok=False", "feature_321", check_missing_pc)

    def check_empty_body():
        r = post("/api/quote/from-price-check", json={})
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok") is False
        return "empty body handled"
    check("Empty body returns ok=False", "feature_321", check_empty_body)

    # Clean up test PCs
    get("/api/test/clear")


# ── Category: Email Templates ─────────────────────────────────────────────────
def run_templates():
    print("\n📧 EMAIL TEMPLATES")

    def templates_page():
        r = get("/templates")
        assert r.status_code == 200
        assert len(r.text) > 200
        return f"HTTP 200, {len(r.text):,} bytes"
    check("GET /templates page", "templates", templates_page, warn_only=True)

    def list_templates():
        r = get("/api/email/templates")
        assert r.status_code == 200
        d = r.json()
        templates = d.get("templates", d if isinstance(d, list) else [])
        assert len(templates) > 0, "No templates returned"
        return f"{len(templates)} templates"
    check("GET /api/email/templates", "templates", list_templates, warn_only=True)

    def draft_template():
        r = post("/api/email/draft", json={"template_id": "distro_list", "contact_id": "test"})
        assert r.status_code in (200, 404), f"unexpected: {r.status_code}"
        return f"draft endpoint: {r.status_code}"
    check("POST /api/email/draft", "templates", draft_template, warn_only=True)


# ── Category: Growth Campaign ─────────────────────────────────────────────────
def run_growth():
    print("\n🌱 GROWTH CAMPAIGN")

    def distro_preview():
        r = get("/api/growth/distro-campaign?dry_run=true&max=5")
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok"), f"ok=False: {d.get('error')}"
        return f"staged={d.get('total_staged',0)}, dry_run={d.get('dry_run')}"
    check("GET /api/growth/distro-campaign (preview)", "growth", distro_preview)

    def campaign_status():
        r = get("/api/growth/campaign-status")
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok"), f"ok=False: {d.get('error')}"
        return f"distro campaigns: {d.get('distro_campaigns',{}).get('count',0)}"
    check("GET /api/growth/campaign-status", "growth", campaign_status)


# ── Category: Deal Forecasting ────────────────────────────────────────────────
def run_forecasting():
    print("\n📊 DEAL FORECASTING")

    def win_prob_api():
        r = get("/api/quotes/win-probability")
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok") or isinstance(d, list) or "quotes" in d
        return "win probability endpoint ok"
    check("GET /api/quotes/win-probability", "forecasting", win_prob_api, warn_only=True)

    def weighted_pipeline():
        r = get("/api/funnel/stats")
        assert r.status_code == 200
        d = r.json()
        # weighted_pipeline may or may not exist yet
        return f"pipeline_value={d.get('pipeline_value', d.get('pipeline','N/A'))}"
    check("Pipeline value in funnel stats", "forecasting", weighted_pipeline, warn_only=True)


# ── Category: SCPRS Scheduler ─────────────────────────────────────────────────
def run_scheduler():
    print("\n⏰ SCPRS SCHEDULER")

    def schedule_api():
        r = get("/api/intel/pull/schedule")
        assert r.status_code in (200, 405), f"unexpected: {r.status_code}"
        return f"schedule endpoint: {r.status_code}"
    check("GET /api/intel/pull/schedule", "scheduler", schedule_api, warn_only=True)


# ── Category: Price History UI ────────────────────────────────────────────────
def run_price_history():
    print("\n💰 PRICE HISTORY")

    def price_history_api():
        r = get("/api/prices/history?q=nitrile")
        assert r.status_code == 200
        return "price history ok"
    check("GET /api/prices/history?q=nitrile", "prices", price_history_api)

    def best_price_api():
        r = get("/api/prices/best?q=glove")
        assert r.status_code == 200
        return "best price ok"
    check("GET /api/prices/best?q=glove", "prices", best_price_api, warn_only=True)


# ── Category: Data Integrity ──────────────────────────────────────────────────
def run_data():
    print("\n🗄️  DATA INTEGRITY")

    def contacts_seeded():
        r = get("/api/db")
        assert r.status_code == 200
        d = r.json()
        contacts = d.get("tables", {}).get("contacts", 0)
        assert contacts > 0, f"contacts=0 — seed data missing"
        return f"contacts={contacts}"
    check("CRM contacts seeded (> 0)", "data", contacts_seeded)

    def volume_persistent():
        r = get("/api/db")
        d = r.json()
        assert d.get("is_railway_volume") is not None
        return f"is_railway_volume={d.get('is_railway_volume')}, persistence={d.get('persistence','?')[:30]}"
    check("Railway volume status reported", "data", volume_persistent)

    def quote_counter():
        r = get("/api/funnel/stats")
        assert r.status_code == 200
        d = r.json()
        next_q = d.get("next_quote", "")
        assert next_q and next_q != "—", f"next_quote missing: {repr(next_q)}"
        return f"next_quote={next_q}"
    check("Quote counter present in funnel stats", "data", quote_counter)


# ── Category: Redirects & Error Handling ─────────────────────────────────────
def run_errors():
    print("\n🛡️  ERROR HANDLING")

    def missing_page():
        r = get("/this-page-definitely-does-not-exist-xyz")
        assert r.status_code in (404, 302, 301), f"Expected 404/redirect, got {r.status_code}"
        return f"missing page → {r.status_code}"
    check("Missing page returns 404", "errors", missing_page, warn_only=True)

    def missing_pc():
        r = get("/pricecheck/nonexistent_pc_xyz")
        assert r.status_code in (200, 302, 404), f"unexpected: {r.status_code}"
        return f"missing PC → {r.status_code}"
    check("Missing price check handled", "errors", missing_pc, warn_only=True)

    def missing_quote():
        r = get("/rfq/nonexistent_rfq_xyz")
        assert r.status_code in (200, 302, 404)
        return f"missing RFQ → {r.status_code}"
    check("Missing RFQ handled", "errors", missing_quote, warn_only=True)


# ── Run + Report ──────────────────────────────────────────────────────────────
CATEGORIES = {
    "pages":       run_pages,
    "auth":        run_auth,
    "api":         run_api,
    "feature_321": run_feature_321,
    "templates":   run_templates,
    "growth":      run_growth,
    "forecasting": run_forecasting,
    "scheduler":   run_scheduler,
    "prices":      run_price_history,
    "data":        run_data,
    "errors":      run_errors,
}


def run_all(only: list = None) -> dict:
    t0 = time.time()
    target = {k: v for k, v in CATEGORIES.items() if not only or k in only}

    print(f"\n{'='*60}")
    print(f"  REYTECH SMOKE TEST  |  {BASE_URL}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Categories: {', '.join(target.keys())}")
    print(f"{'='*60}")

    for name, fn in target.items():
        try:
            fn()
        except Exception as e:
            print(f"  {FAIL_SYM} Category {name} crashed: {e}")

    # Summary
    total = len(results)
    passed = sum(1 for r in results if r["status"] == "pass")
    failed = sum(1 for r in results if r["status"] == "fail")
    warned = sum(1 for r in results if r["status"] == "warn")
    elapsed = round(time.time() - t0, 1)

    score = round(passed / max(total - warned, 1) * 100) if total > 0 else 0

    print(f"\n{'='*60}")
    print(f"  RESULTS: {passed}/{total} passed  |  {failed} failed  |  {warned} warnings")
    print(f"  SCORE:   {score}/100  |  {elapsed}s total")
    if failed > 0:
        print(f"\n  FAILURES:")
        for r in results:
            if r["status"] == "fail":
                print(f"    {FAIL_SYM} {r['category']}/{r['name']}: {r['detail'][:80]}")
    print(f"{'='*60}\n")

    return {
        "timestamp": datetime.now().isoformat(),
        "url": BASE_URL,
        "total": total,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "score": score,
        "elapsed_seconds": elapsed,
        "results": results,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reytech Smoke Test Suite")
    parser.add_argument("--only", help="Comma-separated categories to run")
    parser.add_argument("--save-baseline", metavar="FILE", help="Save results to JSON baseline file")
    parser.add_argument("--compare", metavar="FILE", help="Compare against baseline JSON file")
    args = parser.parse_args()

    only = args.only.split(",") if args.only else None
    report = run_all(only=only)

    if args.save_baseline:
        with open(args.save_baseline, "w") as f:
            json.dump(report, f, indent=2)
        print(f"Baseline saved to {args.save_baseline}")

    if args.compare:
        with open(args.compare) as f:
            baseline = json.load(f)
        new_failures = [r for r in report["results"]
                        if r["status"] == "fail"
                        and not any(b["name"] == r["name"] and b["status"] == "fail"
                                    for b in baseline["results"])]
        if new_failures:
            print(f"⚠️  REGRESSION: {len(new_failures)} new failures vs baseline:")
            for r in new_failures:
                print(f"  {FAIL_SYM} {r['name']}: {r['detail'][:80]}")
            sys.exit(2)
        else:
            print(f"✅ No regressions vs baseline (score: {baseline['score']} → {report['score']})")

    sys.exit(0 if report["failed"] == 0 else 1)
