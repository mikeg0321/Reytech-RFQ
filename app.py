#!/usr/bin/env python3
"""
Reytech RFQ — Application Entry Point
Creates Flask app and registers the dashboard Blueprint.
"""

# ── Clear stale bytecode BEFORE any imports ──
# Railway's persistent volume caches .pyc across deploys, causing old code to run.
# ONLY clean src/ — don't walk the 4GB data volume!
import sys, pathlib
sys.dont_write_bytecode = True
_src_dir = pathlib.Path(__file__).parent / "src"
if _src_dir.exists():
    for _pyc in _src_dir.rglob("*.pyc"):
        try:
            _pyc.unlink()
        except OSError:
            pass
    for _cache in _src_dir.rglob("__pycache__"):
        try:
            _cache.rmdir()
        except OSError:
            pass

import os
import gzip as _gzip
import logging
import time
from flask import Flask, request, jsonify

print(f"[BOOT] app.py loading at {time.time():.0f}", flush=True)

def create_app():
    """Application factory — optimized for fast startup."""
    t0 = time.time()
    print("[BOOT] create_app() called", flush=True)
    _app_dir = os.path.dirname(os.path.abspath(__file__))
    app = Flask(
        __name__,
        template_folder=os.path.join(_app_dir, "src", "templates"),
        static_folder=os.path.join(_app_dir, "src", "static"),
        static_url_path="/static",
    )
    _secret = os.environ.get("SECRET_KEY") or os.environ.get("APP_SECRET")
    if not _secret:
        raise RuntimeError(
            "SECRET_KEY environment variable is required. "
            "Set it in Railway: Settings → Variables → SECRET_KEY = <random 32+ char string>"
        )
    app.secret_key = _secret
    app.config["MAX_CONTENT_LENGTH"] = 20 * 1024 * 1024  # 20 MB upload limit
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["PERMANENT_SESSION_LIFETIME"] = 86400  # 24 hours
    app.config["SESSION_PERMANENT"] = True

    # ── Structured logging — MUST be first, before any other init ─────────
    try:
        from src.core.structured_log import setup_structured_logging
        setup_structured_logging()
        print("[BOOT] Structured logging active", flush=True)
    except Exception as e:
        # Logging setup failure is serious — print to stderr so it's visible
        import sys as _sys
        print(f"[BOOT] WARNING: structured logging setup failed: {e}", file=_sys.stderr, flush=True)

    # ── FORCE_CLEAN_BOOT: nuke corrupted volume data ──────────────────────
    if os.environ.get("FORCE_CLEAN_BOOT"):
        print("[BOOT] FORCE_CLEAN_BOOT: clearing corrupted files...", flush=True)
        import glob
        data_dir = os.path.join(_app_dir, "data")
        for pattern in ["*.db-journal", "*.db-wal", "*.db-shm"]:
            for f in glob.glob(os.path.join(data_dir, pattern)):
                try:
                    os.remove(f)
                except Exception:
                    pass
        db_file = os.path.join(data_dir, "reytech.db")
        if os.path.exists(db_file):
            db_size = os.path.getsize(db_file) / 1024 / 1024
            os.remove(db_file)
            print(f"[BOOT] Removed reytech.db ({db_size:.0f} MB)", flush=True)
        for f in ["processed_emails.json"]:
            p = os.path.join(data_dir, f)
            if os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    # ── CRITICAL PATH: DB schema + data sync + blueprint ──
    print("[BOOT] DB schema init...", flush=True)
    _db_degraded = False
    _db_degraded_error = None
    try:
        if os.environ.get("FORCE_CLEAN_BOOT"):
            from src.core.db import get_db, SCHEMA, DB_PATH, _is_railway_volume
            with get_db() as conn:
                conn.executescript(SCHEMA)
        else:
            import signal
            _has_alarm = hasattr(signal, 'SIGALRM')
            if _has_alarm:
                def _timeout_handler(signum, frame):
                    raise TimeoutError("DB init >30s")
                old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
                signal.alarm(30)
            try:
                from src.core.db import (get_db, SCHEMA, DB_PATH, init_db, _is_railway_volume,
                                         _fix_data_on_boot, get_db_stats, migrate_json_to_db,
                                         init_db_deferred)
                init_db()
                init_db_deferred()  # DAL migration
                _fix_data_on_boot()
                stats = get_db_stats()
                if stats.get("quotes", 0) == 0 and stats.get("contacts", 0) == 0:
                    migrate_json_to_db()
            except TimeoutError:
                print("[BOOT] DB TIMEOUT — minimal schema", flush=True)
                from src.core.db import get_db, SCHEMA, DB_PATH, _is_railway_volume
                with get_db() as conn:
                    conn.executescript(SCHEMA)
            finally:
                if _has_alarm:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
    except Exception as e:
        _db_degraded = True
        _db_degraded_error = str(e)
        logging.getLogger("reytech").error("DB INIT FAILED — entering degraded mode: %s", e)
        print(f"[BOOT] DB FAILED — degraded mode: {e}", flush=True)
    app.config["DB_DEGRADED"] = _db_degraded
    app.config["DB_DEGRADED_ERROR"] = _db_degraded_error
    if not _db_degraded:
        print(f"[BOOT] DB ready ({time.time()-t0:.1f}s)", flush=True)
    else:
        print(f"[BOOT] DB DEGRADED ({time.time()-t0:.1f}s)", flush=True)

    # ── Schema validation — fix missing tables/columns before first request ──
    try:
        from src.core.startup_checks import run_schema_checks
        schema_result = run_schema_checks()
        fixes = schema_result.get("issues_fixed", [])
        if fixes:
            print(f"[BOOT] Schema repaired: {len(fixes)} fix(es) — {', '.join(fixes[:5])}", flush=True)
        else:
            print(f"[BOOT] Schema OK — {schema_result['tables_checked']} tables, 0 issues", flush=True)
    except Exception as e:
        logging.getLogger("reytech").warning("Schema check: %s", e)

    # ── Run migrations (always — creates intelligence tables on first deploy) ──
    try:
        from src.core.migrations import run_migrations
        mig_result = run_migrations()
        if mig_result.get("applied", 0) > 0:
            print(f"[BOOT] Migrations applied: {mig_result['applied']} (now at v{mig_result['version']})", flush=True)
        else:
            print(f"[BOOT] Migrations OK (v{mig_result.get('version', '?')})", flush=True)
    except Exception as e:
        logging.getLogger("reytech").warning("Migrations: %s", e)

    # ── Seed agency registry (idempotent) ──
    try:
        from src.core.ca_agencies import seed_agency_registry
        from src.core.db import get_db
        with get_db() as _seed_conn:
            seed_agency_registry(_seed_conn)
    except Exception:
        pass

    # Register blueprint (all routes)
    from src.api.dashboard import bp
    try:
        from src.api.dashboard import start_polling
    except ImportError:
        start_polling = None
    # Prevent "overwriting existing endpoint" crash — bp is a module-level
    # singleton whose deferred_functions can accumulate across worker reloads.
    # Clear any pre-existing dashboard endpoints before registering.
    for _k in list(app.view_functions.keys()):
        if _k.startswith("dashboard."):
            del app.view_functions[_k]
    app.register_blueprint(bp)
    print(f"[BOOT] Routes registered ({time.time()-t0:.1f}s)", flush=True)

    # Canonical agency casing filter — templates render {{ agency|agency_display }}
    # instead of each site carrying its own agency_map dict. Closes O-8 / RFQ-3
    # / PC-14 lowercase-leak class.
    try:
        from src.core.agency_display import agency_display
        app.jinja_env.filters["agency_display"] = agency_display
    except Exception as e:
        logging.getLogger("reytech").warning("agency_display filter: %s", e)

    # ── Request tracing ──
    try:
        from src.core.tracing import install_tracing
        install_tracing()
    except Exception as e:
        logging.getLogger("reytech").debug("Tracing init: %s", e)

    # ── Security middleware ──
    try:
        from src.core.security import init_security
        init_security(app)
    except Exception as e:
        logging.getLogger("reytech").warning("Security init: %s", e)

    # ── Request timing middleware (P2.1) ──
    try:
        from src.core.ops_monitor import install_request_timing
        install_request_timing(app)
    except Exception as e:
        logging.getLogger("reytech").debug("Request timing init: %s", e)

    # ── Secrets validation ──
    try:
        from src.core.secrets import startup_check
        startup_check()
    except Exception as e:
        logging.getLogger("reytech").warning("Secrets check: %s", e)

    # ── Error handlers ──
    # Bare connectivity test — no auth, no DB, no templates
    @app.route("/ping")
    def _ping():
        return "pong", 200, {"Content-Type": "text/plain"}

    # /health is defined in routes_rfq.py with real DB + disk checks

    @app.errorhandler(413)
    def _request_too_large(e):
        if request.path.startswith("/api/"):
            return {"ok": False, "error": "File too large (max 20 MB)"}, 413
        return "<h1>413 — File Too Large</h1><p>Maximum upload size is 20 MB. <a href='/'>Go home</a></p>", 413

    @app.errorhandler(404)
    def _not_found(e):
        if request.path.startswith("/api/"):
            return {"ok": False, "error": "Not found"}, 404
        return app.send_static_file("404.html") if os.path.exists(
            os.path.join(app.static_folder or "", "404.html")
        ) else ("<h1>404 — Page Not Found</h1><p><a href='/'>Go home</a></p>", 404)

    def _send_error_alert(error, route_info):
        """Fire alerts for server errors via notify_agent (non-blocking).
        Routes to email + SMS + dashboard bell based on notify_agent config."""
        try:
            from src.agents.notify_agent import send_alert
            send_alert(
                event_type="server_error",
                title=f"500 Error: {type(error).__name__}",
                body=f"{route_info}\n{str(error)[:300]}",
                urgency="warning",
                channels=["email", "bell"],
                cooldown_key=f"500:{route_info}",
            )
        except Exception:
            pass  # Alert infra failure must never block error response
        # Also fire webhook (routes to any configured webhook URL)
        try:
            from src.core.webhooks import fire_event
            fire_event("server_error", {
                "route": route_info,
                "error": f"{type(error).__name__}: {str(error)[:200]}",
            })
        except Exception:
            pass

    @app.errorhandler(500)
    def _server_error(e):
        _route = f"{request.method} {request.path}"
        logging.getLogger("reytech").error("500 error: %s | %s", e, _route)
        _send_error_alert(e, _route)
        try:
            from src.core.ops_monitor import record_error
            record_error(e, request.path, request.method)
        except Exception:
            pass
        if request.path.startswith("/api/") or request.path.startswith("/pricecheck/"):
            return {"ok": False, "error": "Internal server error"}, 500
        return "<h1>500 — Server Error</h1><p>Something went wrong. <a href='/'>Go home</a></p>", 500

    @app.errorhandler(Exception)
    def _unhandled_exception(e):
        """Catch-all: any unhandled exception returns JSON for API routes."""
        _route = f"{request.method} {request.path}"
        logging.getLogger("reytech").error("Unhandled: %s → %s: %s",
            _route, type(e).__name__, str(e)[:200])
        _send_error_alert(e, _route)
        try:
            from src.core.ops_monitor import record_error
            record_error(e, request.path, request.method)
        except Exception:
            pass
        if request.path.startswith("/api/") or request.path.startswith("/pricecheck/"):
            return {"ok": False, "error": "Internal server error"}, 500
        return "<h1>500 — Server Error</h1><p>Something went wrong. <a href='/'>Go home</a></p>", 500

    # ── Degraded mode middleware — returns maintenance page when DB is down ──
    _DEGRADED_EXEMPT = {"/ping", "/api/system/status", "/static/"}

    @app.before_request
    def _check_degraded_mode():
        if not app.config.get("DB_DEGRADED"):
            return None
        # Allow health checks and static files through
        for exempt in _DEGRADED_EXEMPT:
            if request.path.startswith(exempt) or request.path == exempt:
                return None
        if request.path.startswith("/api/"):
            return jsonify({
                "ok": False,
                "error": "System in maintenance mode — database unavailable",
                "degraded": True,
                "detail": app.config.get("DB_DEGRADED_ERROR", "unknown"),
            }), 503
        return (
            "<html><head><title>Maintenance</title>"
            "<style>body{font-family:system-ui;display:flex;justify-content:center;"
            "align-items:center;min-height:100vh;margin:0;background:#0d1117;color:#c9d1d9}"
            ".box{text-align:center;padding:40px;border:1px solid #30363d;border-radius:12px;"
            "background:#161b22;max-width:500px}"
            "h1{color:#f0883e;margin:0 0 16px}p{color:#8b949e;line-height:1.6}"
            "</style></head><body><div class='box'>"
            "<h1>System Maintenance</h1>"
            "<p>The database is temporarily unavailable. The system will automatically "
            "recover when the issue is resolved.</p>"
            "<p style='font-size:13px;margin-top:20px'>Error: "
            + (app.config.get("DB_DEGRADED_ERROR") or "unknown") +
            "</p></div></body></html>"
        ), 503

    # ── Response compression + caching ──
    @app.after_request
    def _optimize_response(response):
        # Cache static assets for 1 hour
        if request.path.startswith("/static/"):
            response.cache_control.max_age = 3600
            response.cache_control.public = True
        # Gzip HTML/JSON responses (skip if upstream proxy already compressed)
        if (response.content_type and response.status_code == 200
            and not response.headers.get("Content-Encoding")
            and any(ct in response.content_type for ct in ("text/html", "application/json"))
            and response.content_length and response.content_length > 500
            and "gzip" in request.headers.get("Accept-Encoding", "")):
            try:
                data = response.get_data()
                compressed = _gzip.compress(data, compresslevel=4)
                if len(compressed) < len(data):
                    response.set_data(compressed)
                    response.headers["Content-Encoding"] = "gzip"
                    response.headers["Content-Length"] = len(compressed)
            except Exception:
                pass
        return response

    # ── DEFERRED: only non-critical tasks in background ──────────────────
    def _deferred_init():
        """Non-critical startup tasks — app already serves requests."""
        time.sleep(2)
        try:
            from src.core.catalog import init_catalog
            init_catalog()
        except Exception:
            pass
        try:
            from src.core.db import _dedup_price_checks_on_boot
            _dedup_price_checks_on_boot()
        except Exception:
            pass
        # Boot disk cleanup — prune old backups and stale files
        try:
            from src.core.paths import DATA_DIR as _data
            import glob as _glob
            # Delete corrupt DB files
            for _cf in _glob.glob(os.path.join(_data, "reytech.db.corrupt.*")):
                try:
                    os.remove(_cf)
                    logging.getLogger("reytech").info("CLEANUP: removed corrupt DB: %s", os.path.basename(_cf))
                except OSError:
                    pass
            # Force rotation on hourly backups (in case old 24-keep left extras)
            _hourly = os.path.join(_data, "backups", "hourly")
            if os.path.isdir(_hourly):
                from src.core.ops_monitor import _rotate_files
                _rotate_files(_hourly, prefix="reytech_", suffix=".db", keep=6)
        except Exception as e:
            logging.getLogger("reytech").warning("Boot disk cleanup: %s", e)

        # Reconciliation tasks — moved off critical startup path (not needed for first request)
        try:
            from src.core.db import _reconcile_quotes_json, _boot_sync_quotes, _boot_sync_pcs
            _reconcile_quotes_json()
            _boot_sync_quotes()
            _boot_sync_pcs()
        except Exception:
            pass
        # Recover PCs stuck in 'enriching' state from interrupted deploys
        try:
            from src.agents.pc_enrichment_pipeline import recover_stuck_enrichments
            recover_stuck_enrichments()
        except Exception as e:
            logging.getLogger("reytech").warning("Enrichment recovery on boot: %s", e)

        # Structured logging already initialized in create_app()
        try:
            from src.core.scheduler import start_backup_scheduler, register_job, start_watchdog
            start_backup_scheduler(interval_hours=24)
            for job_name, interval in [
                ("email-poller", 300), ("award-tracker", 3600),
                ("follow-up-engine", 3600), ("quote-lifecycle", 3600),
                ("email-retry", 900), ("lead-nurture", 86400),
                ("qa-monitor", 900), ("growth-agent", 86400),
            ]:
                register_job(job_name, interval_sec=interval)
            start_watchdog(check_interval=300)
        except Exception:
            pass

        # Initialize task queue and start consumer
        try:
            from src.core.task_queue import init_task_queue, reset_stale_running
            init_task_queue()
            reset_stale_running()
            from src.core.task_consumer import start_task_consumer
            start_task_consumer(poll_interval=10)
        except Exception as e:
            logging.getLogger("reytech").warning("Task consumer startup: %s", e)

        # Full FI$Cal exhaustive scrape at 2:00 AM PST
        try:
            from src.agents.scprs_browser import schedule_full_fiscal_scrape
            schedule_full_fiscal_scrape(target_hour_pst=2)
            logging.getLogger("reytech").info("FI$Cal exhaustive scrape scheduled for 2:00 AM PST")
        except ImportError:
            pass

        # System audit after data pull
        try:
            from src.agents.system_auditor import schedule_system_audit
            schedule_system_audit()
            logging.getLogger("reytech").info("System audit scheduled for 5:30 AM PST (after data pull)")
        except ImportError:
            pass

        # Sync SCPRS harvest data → won_quotes KB (bridge between tables)
        try:
            from src.knowledge.won_quotes_db import sync_from_scprs_tables
            sync_result = sync_from_scprs_tables()
            if sync_result.get("synced", 0) > 0:
                logging.getLogger("reytech").info("SCPRS→won_quotes sync: %d items", sync_result["synced"])
        except Exception as _sync_e:
            logging.getLogger("reytech").debug("SCPRS sync: %s", _sync_e)

        # Auto-populate catalog from won_quotes if empty
        try:
            from src.core.db import get_db
            with get_db() as _db:
                _cat_count = _db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0]
            if _cat_count == 0:
                logging.getLogger("reytech").info("Catalog empty — populating from won_quotes...")
                try:
                    from src.knowledge.won_quotes_db import get_db as _get_wq, _ensure_won_quotes_table
                    _ensure_won_quotes_table()
                    _wq = _get_wq()
                    _rows = _wq.execute(
                        "SELECT description, unit_price, quantity, supplier, "
                        "department, po_number, award_date FROM won_quotes WHERE unit_price > 0"
                    ).fetchall()
                    with get_db() as _db2:
                        for _r in _rows:
                            _desc = (_r[0] or "")[:500]
                            if not _desc or not _r[1] or _r[1] <= 0:
                                continue
                            try:
                                _db2.execute("""
                                    INSERT OR IGNORE INTO scprs_catalog
                                    (description, last_unit_price, last_quantity,
                                     last_supplier, last_department, last_po_number,
                                     last_date, times_seen, updated_at)
                                    VALUES (?,?,?,?,?,?,?,1,datetime('now'))
                                """, (_desc, _r[1], _r[2] or 1, _r[3] or "", _r[4] or "", _r[5] or "", _r[6] or ""))
                            except Exception:
                                pass
                        _new_count = _db2.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0]
                    logging.getLogger("reytech").info("Catalog populated: %d items", _new_count)
                except Exception as _e:
                    logging.getLogger("reytech").warning("Catalog population failed: %s", _e)
        except Exception:
            pass

        # Ops monitor — hourly backups, health checks, synthetic tests (P1.3, P2.2-P2.5)
        try:
            from src.core.ops_monitor import start_ops_monitor
            start_ops_monitor()
        except Exception as _ops_e:
            logging.getLogger("reytech").warning("Ops monitor init: %s", _ops_e)

        logging.getLogger("reytech").info("Deferred init complete")

        # Pre-warm expensive caches so first user request is fast
        try:
            import base64 as _b64
            _dash_user = os.environ.get("DASH_USER", "reytech")
            _dash_pass = os.environ.get("DASH_PASS", "")
            _auth_header = {"Authorization": "Basic " + _b64.b64encode(
                f"{_dash_user}:{_dash_pass}".encode()).decode()} if _dash_pass else {}
            with app.app_context():
                client = app.test_client()
                for endpoint in ["/api/dashboard/init", "/api/manager/brief"]:
                    try:
                        client.get(endpoint, headers=_auth_header)
                    except Exception:
                        pass
                logging.getLogger("reytech").info("Cache pre-warmed: dashboard/init + manager/brief")
        except Exception as _e:
            logging.getLogger("reytech").debug("Cache pre-warm failed: %s", _e)

    import threading
    if os.environ.get("ENABLE_BACKGROUND_AGENTS", "true").lower() not in ("false", "0", "off"):
        threading.Thread(target=_deferred_init, daemon=True, name="deferred-init").start()

    # Start email polling (production only)
    _email_polling_env = os.environ.get("ENABLE_EMAIL_POLLING", "").lower()
    if _email_polling_env == "true" and start_polling:
        with app.app_context():
            start_polling(app)
    elif _email_polling_env != "true":
        logging.getLogger("reytech").warning(
            "EMAIL POLLING DISABLED: ENABLE_EMAIL_POLLING=%r (need 'true'). "
            "PO detection via email will NOT work. Check /api/email/health for diagnostics.",
            os.environ.get("ENABLE_EMAIL_POLLING", "(not set)")
        )

    # ── Strict form-profile validation (synchronous, blocks boot on failure) ──
    # Any profile/blank-PDF mismatch raises RuntimeError -> gunicorn worker exits
    # -> Railway deploy fails -> bad profile never serves traffic.
    # Opt-out only for local dev: STRICT_PROFILE_BOOT=0
    if os.environ.get("STRICT_PROFILE_BOOT", "1") != "0":
        from src.core.quote_engine import boot_validate_profiles
        boot_validate_profiles(strict=True)

    elapsed = time.time() - t0
    try:
        print(f"[BOOT] create_app() complete \u2705 ({elapsed:.1f}s)", flush=True)
    except UnicodeEncodeError:
        print(f"[BOOT] create_app() complete OK ({elapsed:.1f}s)", flush=True)

    # ── Run startup health checks (background, non-blocking) ──
    try:
        from src.core.startup_checks import run_all_checks, get_results
        import threading as _thr
        if os.environ.get("ENABLE_BACKGROUND_AGENTS", "true").lower() not in ("false", "0", "off"):
            _thr.Thread(target=run_all_checks, daemon=True, name="startup-checks").start()

        @app.route("/api/health/startup")
        def _startup_health():
            r = get_results()
            return r, 200 if r.get("failed", 0) == 0 else 503
    except Exception as e:
        logging.getLogger("reytech").warning("Startup checks init: %s", e)

    return app


# ── SIGTERM handler for graceful shutdown ──
import signal as _signal

def _handle_sigterm(signum, frame):
    _log = logging.getLogger("reytech")
    _log.info("SIGTERM received — starting graceful shutdown")

    # 1. Stop all background threads
    try:
        from src.core.scheduler import request_shutdown
        request_shutdown()
    except Exception:
        pass

    # 2. WAL checkpoint — flush pending writes to main DB file
    try:
        import sqlite3
        from src.core.db import DB_PATH
        if os.path.exists(DB_PATH):
            _wal_conn = sqlite3.connect(DB_PATH, timeout=5)
            _wal_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            _wal_conn.close()
            _log.info("WAL checkpoint complete — database is consistent")
    except Exception as _wal_err:
        _log.error("WAL checkpoint failed during shutdown: %s", _wal_err)

    # 3. Close all thread-local DB connections
    try:
        from src.core.db import close_thread_db
        close_thread_db()
    except Exception:
        pass

    _log.info("Graceful shutdown complete — safe to terminate")

_signal.signal(_signal.SIGTERM, _handle_sigterm)
# Also catch SIGINT (Ctrl+C) for local development
try:
    _signal.signal(_signal.SIGINT, _handle_sigterm)
except (OSError, ValueError):
    pass  # SIGINT can't be set in some contexts (e.g., non-main thread)

# For gunicorn: gunicorn app:app
print("[BOOT] Creating app at module level...", flush=True)
app = create_app()
try:
    print("[BOOT] Module ready \u2705", flush=True)
except UnicodeEncodeError:
    print("[BOOT] Module ready OK", flush=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
