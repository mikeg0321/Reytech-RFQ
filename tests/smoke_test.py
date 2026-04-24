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

# Force UTF-8 stdout on Windows (cp1252 can't encode emoji)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    print("pip install requests --break-system-packages")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL = os.environ.get("REYTECH_URL", "http://localhost:8000")
USER     = os.environ.get("REYTECH_USER") or os.environ.get("DASH_USER") or ""
PASS     = os.environ.get("REYTECH_PASS") or os.environ.get("DASH_PASS") or ""
TIMEOUT  = int(os.environ.get("SMOKE_TIMEOUT", "15"))

if not USER or not PASS:
    print("ERROR: smoke test credentials missing.", file=sys.stderr)
    print("Set REYTECH_USER/REYTECH_PASS (or DASH_USER/DASH_PASS) before running.", file=sys.stderr)
    print("Example: REYTECH_PASS='...' make smoke", file=sys.stderr)
    sys.exit(2)

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
        assert "rfqs_active" in d or "quotes_sent" in d or "ok" in d, f"missing keys: {list(d.keys())}"
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
    test_routes_enabled = True

    def create_test_pc():
        nonlocal pc_id, test_routes_enabled
        r = get("/api/test/create-pc")
        # Production deployments set ENABLE_TEST_ROUTES=0 to gate test fixtures.
        # That's the correct posture — treat the gate response as a clean pass
        # and skip downstream checks that depend on the fixture.
        if r.status_code == 403:
            try:
                err = r.json().get("error", "")
            except Exception:
                err = ""
            if err == "disabled_on_production":
                test_routes_enabled = False
                return "test routes disabled on prod (gate working as intended)"
        assert r.status_code == 200, f"unexpected: HTTP {r.status_code}"
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
        # On prod (ENABLE_TEST_ROUTES=0) we have no test PC to probe the detail
        # template against. Pick any real PC from /api/pricechecks instead so we
        # still validate that the 1-click banner template fragment ships in
        # production HTML — not just locally.
        target_pc = pc_id
        if not target_pc:
            try:
                lr = get("/api/pricechecks?limit=1")
                if lr.status_code == 200:
                    body = lr.json()
                    items = body.get("pcs") or body.get("items") or body.get("price_checks") or []
                    if items:
                        target_pc = items[0].get("id") or items[0].get("pc_id")
            except Exception as _e:
                pass
        if not target_pc:
            return "no PC available to probe (prod has no PCs and test routes disabled)"
        r = get(f"/pricecheck/{target_pc}")
        assert r.status_code == 200
        html = r.text
        assert "quote-gen-banner" in html, "1-click banner missing from PC detail page"
        assert "generateQuote1Click" in html, "generateQuote1Click JS missing"
        return f"banner + JS present (probed PC {target_pc})"
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

    # Clean up the test PC we just created so prod doesn't collect 20+ of
    # them across deploy runs. The old call hit /api/test/clear which
    # doesn't exist — 14 test PCs were found in the UI on 2026-04-12
    # because of this silent 404. Delete the specific PC we made first,
    # then fall back to the bulk cleanup endpoint for any stragglers.
    if pc_id:
        try:
            post(f"/api/pricecheck/{pc_id}/delete", json={})
        except Exception as _cleanup_err:
            print(f"  cleanup: per-PC delete failed: {_cleanup_err}")
    try:
        get("/api/test/cleanup")
    except Exception as _cleanup_err:
        print(f"  cleanup: bulk cleanup failed: {_cleanup_err}")


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

# ── Category: Email Poller Health ─────────────────────────────────────────────
def run_poll_health():
    print("\n📬 EMAIL POLLER HEALTH")

    def poll_status_running():
        r = get("/api/status")
        assert r.status_code == 200
        d = r.json()
        poll = d.get("poll", {})
        assert poll.get("running") is True, \
            f"Poller not running. running={poll.get('running')}, error={poll.get('error')}"
        return f"running=True, last_check={poll.get('last_check','never')}"
    check("Poller thread is running", "poll", poll_status_running)

    def poll_has_checked():
        r = get("/api/status")
        d = r.json()
        last = d.get("poll", {}).get("last_check")
        assert last is not None, (
            "last_check is null — poller has never completed a cycle. "
            "Check Railway logs for NameError or IMAP failures."
        )
        return f"last_check={last}"
    check("Poller has completed at least one cycle", "poll", poll_has_checked)

    def poll_no_error():
        r = get("/api/status")
        d = r.json()
        err = d.get("poll", {}).get("error")
        assert not err, f"Poller error: {err}"
        return "no error"
    check("Poller has no active error", "poll", poll_no_error)

    def poll_now_reachable():
        r = get("/api/poll-now")
        assert r.status_code == 200, f"HTTP {r.status_code}"
        d = r.json()
        assert d.get("ok") is True, f"poll-now returned ok=False: {d.get('error')}"
        return f"ok=True, found={d.get('found', 0)}, error={d.get('error')}"
    check("GET /api/poll-now returns ok=True (no NameError)", "poll", poll_now_reachable)

    def inbox_peek_reachable():
        r = get("/api/diag/inbox-peek")
        assert r.status_code == 200
        d = r.json()
        assert d.get("ok") is True, f"inbox-peek error: {d.get('error')}"
        total = d.get("total_in_window", 0)
        return f"ok=True, emails_in_window={total}"
    check("GET /api/diag/inbox-peek returns ok=True", "poll", inbox_peek_reachable)


def run_classifier_v2_health():
    """Post-deploy verification for the unified ingest pipeline.

    The /api/health/quoting endpoint aggregates utilization_events +
    feature_flags + quotes into a single observability view. Every
    deploy should prove, before we walk away from it:

      1. The feature flag state is readable — catches a regression in
         flags.get_flag / feature_flags table.
      2. `ingest.classify_crashed` count in the window is 0 — catches
         a classifier crash bug landing in production.
      3. `ingest.process_buyer_request` is NOT in the top_errors list
         with a non-zero error count — catches the slow-failure mode
         where classification succeeds but downstream save/link fails.

    These are all "zero-tolerance" checks — any regression here should
    block promote and trigger rollback, not show up as a warning.
    """
    print("\n🧠 CLASSIFIER V2 HEALTH")

    def health_api_reachable():
        r = get("/api/health/quoting?days=1")
        assert r.status_code == 200, f"HTTP {r.status_code}"
        d = r.json()
        assert d.get("ok") is True, f"health API returned ok=False: {d}"
        return "endpoint live"
    check("GET /api/health/quoting returns ok=True", "classifier_v2", health_api_reachable)

    def flag_card_shape():
        r = get("/api/health/quoting?days=1")
        d = r.json()
        fc = d.get("flag_card") or {}
        assert "classifier_v2_on" in fc, "flag_card missing classifier_v2_on"
        assert isinstance(fc["classifier_v2_on"], bool), \
            f"classifier_v2_on should be bool, got {type(fc['classifier_v2_on']).__name__}"
        return f"classifier_v2_on={fc['classifier_v2_on']}"
    check("flag_card.classifier_v2_on is a bool", "classifier_v2", flag_card_shape)

    def no_classifier_crashes():
        r = get("/api/health/quoting?days=1")
        d = r.json()
        crashes_1d = d.get("classifier_1d", {}).get("crashes", 0)
        assert crashes_1d == 0, (
            f"classifier_v2 crashed {crashes_1d}x in the last 24h. "
            f"Check /health/quoting or grep Railway logs for "
            f"'ingest.classify_crashed'. Recent crashes: "
            f"{d.get('recent_crashes', [])[:3]}"
        )
        return f"0 crashes in 24h (invocations={d.get('classifier_1d', {}).get('invocations', 0)})"
    check("Zero classifier crashes in last 24h", "classifier_v2", no_classifier_crashes)

    def ingest_not_in_top_errors():
        r = get("/api/health/quoting?days=7")
        d = r.json()
        errored_features = {e.get("feature") for e in (d.get("top_errors") or [])}
        bad = [f for f in errored_features if f and f.startswith("ingest.")]
        assert not bad, (
            f"Ingest features showing errors in 7d window: {bad}. "
            f"Check /health/quoting top_errors table."
        )
        return f"no ingest.* features in top_errors ({len(errored_features)} total)"
    check("No ingest.* features in top_errors (7d)", "classifier_v2", ingest_not_in_top_errors)

    def recent_crashes_table_sane():
        """The recent_crashes table should always be readable even when
        empty — catches a regression in the context-JSON parser."""
        r = get("/api/health/quoting?days=1")
        d = r.json()
        recent = d.get("recent_crashes")
        assert isinstance(recent, list), f"recent_crashes should be list, got {type(recent).__name__}"
        for row in recent:
            assert "created_at" in row
            assert "error_type" in row
        return f"recent_crashes list is sane ({len(recent)} entries)"
    check("recent_crashes list parses cleanly", "classifier_v2", recent_crashes_table_sane)


def run_catalog_health():
    """Post-deploy check for the catalog observability surface shipped in
    PR #227: UNIQUE(name) + UPC column presence, enrichment error table.
    Lights up a regression if the endpoint drops or the index is lost."""
    print("\n📚 CATALOG HEALTH")

    def endpoint_reachable():
        r = get("/api/health/catalog")
        assert r.status_code == 200, f"HTTP {r.status_code}"
        d = r.json()
        assert d.get("ok") is True, f"returned ok=False: {d}"
        return "endpoint live"
    check("GET /api/health/catalog returns ok=True", "catalog_health", endpoint_reachable)

    def unique_name_index_present():
        d = get("/api/health/catalog").json()
        assert d.get("unique_name_index") is True, (
            "UNIQUE(name) index missing on prod — possible duplicate rows in "
            "product_catalog blocking the CREATE UNIQUE INDEX. Check logs for "
            "'UNIQUE(name) enforcement blocked'."
        )
        return "idx_catalog_name_unique present"
    check("product_catalog UNIQUE(name) index enforced", "catalog_health",
          unique_name_index_present)

    def upc_column_and_index_present():
        d = get("/api/health/catalog").json()
        assert d.get("upc_column") is True, "upc column missing"
        assert d.get("upc_index") is True, "idx_catalog_upc missing"
        return "upc + idx_catalog_upc present"
    check("product_catalog upc column + index present", "catalog_health",
          upc_column_and_index_present)


def run_profiles_health():
    """Post-deploy check for the form profile registry.

    Guards the fingerprint gate (PR #251) and the manifest drift detector
    (PR #254) against silent regressions. If registry.yml ships out of
    sync with the profile YAMLs, the /api/health/profiles endpoint will
    flag drift — and rfq package generation for that form will hard-fail
    in routes_rfq_gen with a 422. Catching drift here, right after deploy,
    means ops sees it before a real RFQ hits the gate.
    """
    print("\n🗂  PROFILE REGISTRY HEALTH")

    def endpoint_reachable():
        r = get("/api/health/profiles")
        assert r.status_code == 200, f"HTTP {r.status_code}"
        d = r.json()
        assert d.get("ok") is True, f"returned ok=False: {d}"
        return f"endpoint live ({d.get('profile_count')} profiles)"
    check("GET /api/health/profiles returns ok=True", "profiles_health",
          endpoint_reachable)

    def no_manifest_drift():
        d = get("/api/health/profiles").json()
        drift = d.get("drift") or []
        assert not drift, (
            f"profile manifest drift on prod — regenerate registry.yml: {drift}"
        )
        return "manifest matches runtime"
    check("/api/health/profiles reports zero manifest drift", "profiles_health",
          no_manifest_drift)

    def manifest_count_matches_runtime():
        d = get("/api/health/profiles").json()
        assert d.get("manifest_count") == d.get("profile_count"), (
            f"manifest_count ({d.get('manifest_count')}) != "
            f"profile_count ({d.get('profile_count')})"
        )
        return f"manifest/runtime agree at {d.get('profile_count')} profiles"
    check("manifest_count == profile_count", "profiles_health",
          manifest_count_matches_runtime)


CATEGORIES = {
    "pages":            run_pages,
    "auth":             run_auth,
    "api":              run_api,
    "feature_321":      run_feature_321,
    "templates":        run_templates,
    "growth":           run_growth,
    "forecasting":      run_forecasting,
    "scheduler":        run_scheduler,
    "prices":           run_price_history,
    "data":             run_data,
    "errors":           run_errors,
    "poll":             run_poll_health,
    "classifier_v2":    run_classifier_v2_health,
    "catalog_health":   run_catalog_health,
    "profiles_health":  run_profiles_health,
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
    parser.add_argument("--min-score", type=int, default=None,
                        help="Exit 1 if score < min-score (used by post-deploy gate)")
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

    if args.min_score is not None and report["score"] < args.min_score:
        print(f"FAIL: score {report['score']} < min-score {args.min_score}", file=sys.stderr)
        sys.exit(1)

    sys.exit(0 if report["failed"] == 0 else 1)
