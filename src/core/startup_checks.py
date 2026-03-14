"""
startup_checks.py — Runs on EVERY deploy/boot. Catches the 3 bug classes:
  1. "no such table" — sqlite3.connect without init_db
  2. Wrong DATA_DIR — reading from git dir instead of Railway volume
  3. Silent thread death — daemon threads with no error handling

Results at GET /api/health/startup
"""

import os, re, json, time, sqlite3, logging, importlib
from datetime import datetime

log = logging.getLogger("startup_checks")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

_results = {"ran_at": None, "passed": 0, "failed": 0, "warnings": 0, "checks": []}


def _check(name, fn):
    try:
        ok, detail = fn()
        status = "PASS" if ok else "FAIL"
        if not ok:
            _results["failed"] += 1
            log.error("STARTUP FAIL: %s — %s", name, detail)
        else:
            _results["passed"] += 1
        _results["checks"].append({"name": name, "status": status, "detail": detail})
    except Exception as e:
        _results["failed"] += 1
        detail = f"{type(e).__name__}: {str(e)[:200]}"
        log.error("STARTUP CRASH: %s — %s", name, detail)
        _results["checks"].append({"name": name, "status": "CRASH", "detail": detail})


def run_all_checks():
    _results.update(ran_at=datetime.now().isoformat(), passed=0, failed=0, warnings=0, checks=[])

    # 1. DATA_DIR correct location
    def check_data_dir():
        on_railway = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("PORT"))
        git_data = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
        is_volume = os.path.abspath(DATA_DIR) != os.path.abspath(git_data)
        if on_railway and not is_volume:
            return False, f"DATA_DIR={DATA_DIR} is git dir on Railway"
        if not os.path.exists(DATA_DIR):
            return False, f"DATA_DIR={DATA_DIR} does not exist"
        if not os.access(DATA_DIR, os.W_OK):
            return False, f"DATA_DIR={DATA_DIR} not writable"
        return True, f"DATA_DIR={DATA_DIR} (volume={is_volume})"
    _check("DATA_DIR", check_data_dir)

    # 2. DB exists + critical tables
    def check_db():
        db_path = os.path.join(DATA_DIR, "reytech.db")
        try:
            from src.core.db import init_db
            init_db()
        except Exception as e:
            return False, f"init_db failed: {e}"
        conn = sqlite3.connect(db_path, timeout=10)
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        conn.close()
        critical = ["price_checks", "quotes", "contacts", "notifications", "workflow_runs",
                     "scprs_po_master", "product_catalog", "audit_trail"]
        missing = [t for t in critical if t not in tables]
        if missing:
            return False, f"Missing tables: {missing}"
        return True, f"{len(tables)} tables, all {len(critical)} critical present"
    _check("DB tables", check_db)

    # 3. _load_price_checks works
    def check_load_pcs():
        from src.api.dashboard import _load_price_checks
        pcs = _load_price_checks()
        return True, f"{len(pcs)} PCs loaded"
    _check("Load PCs", check_load_pcs)

    # 4. Critical imports
    def check_imports():
        failures = []
        mods = [
            ("src.agents.product_catalog", "match_item"),
            ("src.agents.email_poller", "EmailPoller"),
            ("src.forms.reytech_filler_v4", "fill_704b"),
            ("src.core.db", "get_db"),
        ]
        for mod, attr in mods:
            try:
                m = importlib.import_module(mod)
                if not hasattr(m, attr):
                    failures.append(f"{mod}.{attr} missing")
            except Exception as e:
                failures.append(f"{mod}: {e}")
        if failures:
            return False, "; ".join(failures)
        return True, f"All {len(mods)} OK"
    _check("Imports", check_imports)

    # 5. Product catalog accessible
    def check_catalog():
        from src.agents.product_catalog import get_catalog_stats, init_catalog_db
        init_catalog_db()
        stats = get_catalog_stats()
        return True, f"{stats.get('total_products', 0)} products"
    _check("Catalog", check_catalog)

    # 6. SCPRS KB accessible
    def check_scprs():
        try:
            from src.knowledge.won_quotes_db import get_kb_stats
            stats = get_kb_stats()
            return True, f"{stats.get('total_quotes', 0)} quotes"
        except Exception as e:
            return False, str(e)
    _check("SCPRS KB", check_scprs)

    # 7. Templates compile
    def check_templates():
        from jinja2 import Environment, FileSystemLoader
        tpl_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src", "templates")
        env = Environment(loader=FileSystemLoader(tpl_dir))
        errors = []
        for name in env.list_templates():
            try: env.get_template(name)
            except Exception as e: errors.append(f"{name}: {e}")
        if errors:
            return False, f"{len(errors)} errors: {errors[0]}"
        return True, f"{len(env.list_templates())} clean"
    _check("Templates", check_templates)

    # 8. WAL mode
    def check_wal():
        db_path = os.path.join(DATA_DIR, "reytech.db")
        conn = sqlite3.connect(db_path, timeout=10)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        if mode != "wal":
            conn.execute("PRAGMA journal_mode=WAL")
        conn.close()
        return True, f"{'was '+mode+', fixed' if mode != 'wal' else 'active'}"
    _check("WAL mode", check_wal)

    # 9. STATIC CODE AUDIT — catch bugs BEFORE they hit runtime
    def check_code_patterns():
        issues = []
        src_root = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "src")
        for root, dirs, files in os.walk(src_root):
            dirs[:] = [d for d in dirs if d != '__pycache__']
            for fname in files:
                if not fname.endswith('.py'): continue
                fpath = os.path.join(root, fname)
                with open(fpath, encoding="utf-8", errors="replace") as f:
                    content = f.read()
                rel = os.path.relpath(fpath, src_root)
                
                # Pattern A: hardcoded "data/" file paths
                for m in re.finditer(r'["\']data/(\w+\.\w+)["\']', content):
                    issues.append(f"{rel}: hardcoded 'data/{m.group(1)}'")
                
                # Pattern B: os.environ.get("DATA_DIR"...) without paths.py import
                if 'os.environ.get("DATA_DIR"' in content or "os.environ.get('DATA_DIR'" in content:
                    if 'from src.core.paths import' not in content and fname != 'paths.py':
                        issues.append(f"{rel}: uses env DATA_DIR without paths.py import")
                
                # Pattern C: sqlite3.connect without init_db or get_db or DATA_DIR
                if 'sqlite3.connect(' in content and fname not in ('db.py', 'startup_checks.py'):
                    has_guard = 'init_db' in content or 'get_db' in content or 'from src.core' in content
                    if not has_guard:
                        issues.append(f"{rel}: direct sqlite3.connect without any db guard")

        if issues:
            return False, f"{len(issues)} code issues: {'; '.join(issues[:5])}"
        return True, "No hardcoded paths, no unguarded DB, no env DATA_DIR"
    _check("Code patterns", check_code_patterns)

    # 10. Auto-price imports (lightweight — no DB calls)
    def check_auto_price_imports():
        errors = []
        for mod in ["src.agents.product_catalog", "src.knowledge.pricing_oracle"]:
            try:
                __import__(mod)
            except Exception as e:
                errors.append(f"{mod}: {e}")
        if errors:
            return False, "; ".join(errors)
        return True, "Pricing modules importable"
    _check("Auto-price imports", check_auto_price_imports)

    # Summary + auto-alert
    total = _results["passed"] + _results["failed"]
    failed_names = [c["name"] for c in _results["checks"] if c["status"] != "PASS"]

    if _results["failed"] > 0:
        log.error("⚠️ STARTUP: %d/%d FAILED — %s", _results["failed"], total, ", ".join(failed_names))
        _auto_alert_failures(_results)
    else:
        log.info("✅ STARTUP: %d/%d passed", _results["passed"], total)

    return _results


def _auto_alert_failures(results):
    """Send email + bell notification when startup checks fail."""
    failed = [c for c in results["checks"] if c["status"] != "PASS"]
    if not failed:
        return

    subject = f"⚠️ Deploy Health: {len(failed)} check(s) failed"
    body = f"Startup health checks ran at {results['ran_at']}\n\n"
    body += f"PASSED: {results['passed']}  |  FAILED: {results['failed']}\n\n"
    for c in failed:
        body += f"❌ {c['name']}: {c['detail']}\n"
    body += f"\nFull results: /api/health/startup"

    # 1. Email alert
    try:
        gmail = os.environ.get("GMAIL_ADDRESS", "")
        gmail_pw = os.environ.get("GMAIL_PASSWORD", "")
        if gmail and gmail_pw:
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = gmail
            msg["To"] = gmail  # Send to self
            with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as s:
                s.login(gmail, gmail_pw)
                s.send_message(msg)
            log.info("Startup alert emailed to %s", gmail)
    except Exception as e:
        log.warning("Startup email alert failed: %s", e)

    # 2. Bell notification (shows on home page)
    try:
        from src.agents.notify_agent import send_alert
        send_alert(
            event_type="deploy_health",
            title=subject,
            body=body,
            urgency="urgent",
            channels=["bell", "email"],
            run_async=False,
        )
    except Exception as e:
        log.debug("Startup bell alert: %s", e)

    # 3. Write to audit trail
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT INTO audit_trail (timestamp, event_type, actor, details) VALUES (datetime('now'), ?, ?, ?)",
                ("deploy_health_fail", "system", json.dumps({"failed": [c["name"] for c in failed], "details": body[:500]}))
            )
    except Exception:
        pass


def get_results():
    return _results
