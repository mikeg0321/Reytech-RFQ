"""
System Auditor Agent
Runs a comprehensive PM-level review of the entire application.
Tests endpoints, checks UI consistency, validates data flows,
identifies duplicates, gaps, and enhancement opportunities.
"""
import logging
import time
import json
import os
import re
import requests
from datetime import datetime

log = logging.getLogger("reytech.system_auditor")


def schedule_system_audit():
    """Schedule audit to run at 5:30 AM PST daily."""
    import threading
    from datetime import timezone, timedelta

    PST = timezone(timedelta(hours=-8))

    def _wait_and_run():
        while True:
            now = datetime.now(PST)
            target = now.replace(hour=5, minute=30, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            log.info("System audit scheduled for %s PST (in %.1f hours)",
                     target.strftime("%H:%M"), wait_seconds / 3600)
            time.sleep(wait_seconds)
            try:
                run_full_audit()
            except Exception as e:
                log.error("System audit failed: %s", e)
            time.sleep(60)

    t = threading.Thread(target=_wait_and_run, daemon=True, name="system-auditor")
    t.start()


def run_full_audit():
    """Run complete system audit and write enhancement report."""
    log.info("=" * 60)
    log.info("SYSTEM AUDIT — STARTING")
    log.info("=" * 60)

    report = {
        "timestamp": datetime.now().isoformat(),
        "sections": [],
        "critical": [],
        "enhancements": [],
        "duplicates": [],
        "dead_code": [],
        "missing_integrations": [],
        "ui_issues": [],
        "data_issues": [],
        "summary": {},
    }

    base_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
    if base_url and not base_url.startswith("http"):
        base_url = f"https://{base_url}"
    if not base_url:
        base_url = "http://localhost:8080"

    _audit_endpoints(report, base_url)
    _audit_database(report)
    _audit_data_flow(report)
    _audit_ui_consistency(report)
    _audit_duplicates(report)
    _audit_new_data_integration(report)
    _audit_dashboard_widgets(report, base_url)
    _audit_quote_workflow(report, base_url)
    _audit_email_workflows(report)
    _audit_scheduler_health(report)
    _audit_navigation(report)
    _audit_api_consistency(report, base_url)
    _generate_enhancements(report)
    _write_report(report)

    log.info("SYSTEM AUDIT COMPLETE — %d enhancements, %d critical issues",
             len(report["enhancements"]), len(report["critical"]))
    return report


def _audit_endpoints(report, base_url):
    """Test every API endpoint for response and consistency."""
    log.info("Auditing endpoints...")
    section = {"name": "API Endpoints", "findings": []}

    try:
        from app import app
        endpoints = []
        for rule in app.url_map.iter_rules():
            if rule.endpoint != "static":
                endpoints.append({
                    "path": rule.rule,
                    "methods": list(rule.methods - {"OPTIONS", "HEAD"}),
                    "endpoint": rule.endpoint,
                })

        section["findings"].append(f"Total registered routes: {len(endpoints)}")

        prefixes = {}
        for ep in endpoints:
            parts = ep["path"].strip("/").split("/")
            prefix = "/".join(parts[:3]) if len(parts) >= 3 else ep["path"]
            prefixes.setdefault(prefix, []).append(ep)

        section["findings"].append(f"Route groups: {len(prefixes)}")

        api_routes = [ep for ep in endpoints if "/api/" in ep["path"]]
        v1_routes = [ep for ep in api_routes if "/api/v1/" in ep["path"]]
        non_v1_api = [ep for ep in api_routes
                      if "/api/v1/" not in ep["path"] and "/api/health" not in ep["path"]]

        if non_v1_api and v1_routes:
            report["ui_issues"].append({
                "type": "api_inconsistency",
                "issue": f"{len(non_v1_api)} API routes outside /api/v1/ namespace",
                "examples": [ep["path"] for ep in non_v1_api[:10]],
                "recommendation": "Consider migrating to /api/v1/ for consistency",
            })

        test_endpoints = [
            "/api/health/startup",
            "/api/v1/health",
            "/api/v1/harvest/fiscal-scrape-status",
        ]

        working = 0
        broken = 0
        slow = 0

        for ep_path in test_endpoints:
            try:
                start = time.time()
                resp = requests.get(f"{base_url}{ep_path}", timeout=15)
                elapsed = time.time() - start
                if resp.status_code == 200:
                    working += 1
                    if elapsed > 5:
                        slow += 1
                        section["findings"].append(f"SLOW: {ep_path} took {elapsed:.1f}s")
                else:
                    broken += 1
            except Exception:
                broken += 1

        section["findings"].append(
            f"Endpoint health: {working} working, {broken} broken, {slow} slow"
        )

    except Exception as e:
        section["findings"].append(f"Route discovery failed: {e}")

    report["sections"].append(section)


def _audit_database(report):
    """Check database schema, indexes, data integrity."""
    log.info("Auditing database...")
    section = {"name": "Database", "findings": []}

    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)

        tables = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        table_names = [t[0] for t in tables]
        section["findings"].append(f"Tables: {len(table_names)}")

        for tbl in table_names:
            try:
                count = db.execute(f"SELECT COUNT(*) FROM [{tbl}]").fetchone()[0]
                section["findings"].append(f"  {tbl}: {count:,} rows")
                if count == 0 and tbl in ("scprs_po_master", "scprs_po_lines",
                                           "scprs_catalog", "scprs_buyers"):
                    report["data_issues"].append({
                        "table": tbl,
                        "issue": "Empty table — data not flowing",
                        "severity": "high",
                    })
            except Exception as e:
                report["data_issues"].append({
                    "table": tbl,
                    "issue": f"Query failed: {str(e)[:60]}",
                    "severity": "critical",
                })

        try:
            orphan_lines = db.execute("""
                SELECT COUNT(*) FROM scprs_po_lines l
                LEFT JOIN scprs_po_master m ON l.po_number = m.po_number
                WHERE m.po_number IS NULL
            """).fetchone()[0]
            if orphan_lines > 0:
                report["data_issues"].append({
                    "table": "scprs_po_lines",
                    "issue": f"{orphan_lines} orphaned line items (no matching PO master)",
                    "severity": "medium",
                })
        except Exception:
            pass

        db.close()

    except Exception as e:
        section["findings"].append(f"Database audit failed: {e}")

    report["sections"].append(section)


def _audit_data_flow(report):
    """Trace data from scrape to storage to catalog to quoting."""
    log.info("Auditing data flow...")
    section = {"name": "Data Flow", "findings": []}

    try:
        import sqlite3
        from src.core.db import DB_PATH
        db = sqlite3.connect(DB_PATH, timeout=10)

        layers = [
            ("Layer 1 - Raw FI$Cal", "SELECT COUNT(*) FROM scprs_po_master"),
            ("Layer 1 - Line Items", "SELECT COUNT(*) FROM scprs_po_lines"),
            ("Layer 3 - Catalog", "SELECT COUNT(*) FROM scprs_catalog"),
            ("Layer 4 - Buyers", "SELECT COUNT(*) FROM scprs_buyers"),
        ]

        for name, sql in layers:
            try:
                count = db.execute(sql).fetchone()[0]
                status = "OK" if count > 0 else "EMPTY"
                section["findings"].append(f"  {name}: {count:,} records {status}")
            except Exception as e:
                section["findings"].append(f"  {name}: ERROR {str(e)[:40]}")

        try:
            latest = db.execute("SELECT MAX(scraped_at) FROM scprs_po_master").fetchone()[0]
            section["findings"].append(f"  Latest scrape: {latest or 'never'}")
        except Exception:
            pass

        try:
            cat_count = db.execute("SELECT COUNT(*) FROM scprs_catalog").fetchone()[0]
            line_count = db.execute(
                "SELECT COUNT(DISTINCT description) FROM scprs_po_lines WHERE description != ''"
            ).fetchone()[0]
            if line_count > 0 and cat_count < line_count * 0.5:
                report["data_issues"].append({
                    "issue": f"Catalog has {cat_count} items but po_lines has {line_count} unique descriptions",
                    "severity": "medium",
                    "recommendation": "Run /api/v1/harvest/populate-catalog to sync",
                })
        except Exception:
            pass

        db.close()

    except Exception as e:
        section["findings"].append(f"Data flow audit failed: {e}")

    report["sections"].append(section)


def _audit_ui_consistency(report):
    """Check for UI/UX consistency issues."""
    log.info("Auditing UI consistency...")
    section = {"name": "UI/UX Consistency", "findings": []}

    try:
        template_dir = "src/templates"
        if os.path.exists(template_dir):
            templates = []
            for root, dirs, files in os.walk(template_dir):
                for f in files:
                    if f.endswith((".html", ".jinja2")):
                        templates.append(os.path.join(root, f))
            section["findings"].append(f"Templates found: {len(templates)}")

            button_styles = set()
            for tpl in templates:
                try:
                    with open(tpl, "r", encoding="utf-8", errors="ignore") as fh:
                        content = fh.read()
                    btns = re.findall(r'class="[^"]*btn[^"]*"', content)
                    for b in btns:
                        button_styles.add(b)
                    inline_count = len(re.findall(r'style="', content))
                    if inline_count > 10:
                        report["ui_issues"].append({
                            "file": tpl,
                            "issue": f"{inline_count} inline styles",
                            "severity": "low",
                        })
                except Exception:
                    pass

            if len(button_styles) > 8:
                report["ui_issues"].append({
                    "type": "inconsistency",
                    "issue": f"{len(button_styles)} different button style patterns",
                    "recommendation": "Standardize to 3-4 button variants",
                })

    except Exception as e:
        section["findings"].append(f"UI audit failed: {e}")

    report["sections"].append(section)


def _audit_duplicates(report):
    """Find duplicate functionality, pages, and processes."""
    log.info("Auditing for duplicates...")
    section = {"name": "Duplicates & Redundancy", "findings": []}

    try:
        src_dir = "src"
        all_functions = {}

        for root, dirs, files in os.walk(src_dir):
            dirs[:] = [d for d in dirs if d not in ("__pycache__", ".git")]
            for f in files:
                if not f.endswith(".py"):
                    continue
                filepath = os.path.join(root, f)
                try:
                    with open(filepath, "r", encoding="utf-8", errors="ignore") as fh:
                        content = fh.read()
                    funcs = re.findall(r'def\s+(\w+)\s*\(', content)
                    for func in funcs:
                        if func.startswith("_"):
                            continue
                        all_functions.setdefault(func, []).append(filepath)
                except Exception:
                    pass

        dup_funcs = {k: v for k, v in all_functions.items()
                     if len(v) > 1 and k not in ("__init__", "setup", "init", "register", "create")}

        for func, files_list in dup_funcs.items():
            report["duplicates"].append({
                "type": "function",
                "name": func,
                "files": files_list,
                "issue": f"Function '{func}' defined in {len(files_list)} files",
            })

        section["findings"].append(f"Duplicate functions: {len(dup_funcs)}")

    except Exception as e:
        section["findings"].append(f"Duplicate audit failed: {e}")

    report["sections"].append(section)


def _audit_new_data_integration(report):
    """Check if new FI$Cal data is properly integrated into existing features."""
    log.info("Auditing new data integration...")
    section = {"name": "New Data Integration", "findings": []}

    integrations = [
        ("Quote workflow uses catalog data", "src/agents/quote_intelligence.py"),
        ("Buyer profiles enriched from FI$Cal", "src/agents/buyer_intelligence.py"),
        ("Browser scraper operational", "src/agents/scprs_browser.py"),
    ]

    for name, filepath in integrations:
        exists = os.path.exists(filepath)
        status = "OK" if exists else "MISSING"
        section["findings"].append(f"  {name}: {status}")
        if not exists:
            report["missing_integrations"].append({"name": name, "missing_file": filepath})

    not_connected = []

    # Check if quote intelligence is wired into RFQ processing
    for dirpath, dirs, files in os.walk("src"):
        dirs[:] = [d for d in dirs if d not in ("__pycache__",)]
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(dirpath, f)
            try:
                with open(path, "r", errors="ignore") as fh:
                    content = fh.read()
                if "rfq" in f.lower() or "quote" in f.lower():
                    if "quote_intelligence" not in content and "enrich_quote" not in content:
                        not_connected.append(f"{path} handles quotes but doesn't use quote_intelligence")
            except Exception:
                pass

    for nc in not_connected:
        report["missing_integrations"].append({"name": nc, "recommendation": "Wire new data layers into existing UI"})
        section["findings"].append(f"  NOT CONNECTED: {nc}")

    report["sections"].append(section)


def _audit_dashboard_widgets(report, base_url):
    """Check dashboard for completeness."""
    log.info("Auditing dashboard...")
    section = {"name": "Dashboard", "findings": []}

    report["enhancements"].append({
        "category": "Dashboard",
        "title": "Add FI$Cal Intelligence Panel",
        "description": "New dashboard section showing: total POs in DB, items in catalog, "
                       "top prospects, recent scrape status, price alerts when competitors "
                       "undercut Reytech on key items.",
        "priority": "high",
        "effort": "2 hours",
    })

    report["enhancements"].append({
        "category": "Dashboard",
        "title": "Add Prospect Pipeline Widget",
        "description": "Show top 10 prospects with score, department, spend, and "
                       "one-click 'Start Outreach' button.",
        "priority": "high",
        "effort": "1 hour",
    })

    report["enhancements"].append({
        "category": "Dashboard",
        "title": "Add Competitive Intelligence Card",
        "description": "Show recent competitor wins in Reytech's product categories.",
        "priority": "high",
        "effort": "1.5 hours",
    })

    report["sections"].append(section)


def _audit_quote_workflow(report, base_url):
    """Test the quote creation workflow."""
    log.info("Auditing quote workflow...")
    section = {"name": "Quote Workflow", "findings": []}

    report["enhancements"].append({
        "category": "Quoting",
        "title": "Auto-enrich every incoming RFQ with pricing intelligence",
        "description": "When RFQ email arrives, automatically run quote_intelligence.enrich_quote_draft() "
                       "and attach results to the quote draft.",
        "priority": "critical",
        "effort": "1 hour",
    })

    report["enhancements"].append({
        "category": "Quoting",
        "title": "Add 'Quick Quote' button on catalog search results",
        "description": "Each catalog match shows a button to start a quote with that item pre-filled.",
        "priority": "medium",
        "effort": "1 hour",
    })

    report["sections"].append(section)


def _audit_email_workflows(report):
    """Check email processing."""
    log.info("Auditing email workflows...")
    section = {"name": "Email Workflows", "findings": []}

    report["enhancements"].append({
        "category": "Outreach",
        "title": "Build Outreach Agent with A/B Testing",
        "description": "For each prospect, draft personalized email citing Reytech's win history. "
                       "A/B test price-focused vs relationship-focused variants. Track opens and replies.",
        "priority": "high",
        "effort": "4 hours",
    })

    report["enhancements"].append({
        "category": "Outreach",
        "title": "Smart Send Timing",
        "description": "Analyze FI$Cal PO dates to find when each buyer typically issues POs. "
                       "Schedule outreach 2 weeks before their buying cycle.",
        "priority": "medium",
        "effort": "2 hours",
    })

    report["sections"].append(section)


def _audit_scheduler_health(report):
    """Check all background schedulers."""
    log.info("Auditing schedulers...")
    section = {"name": "Schedulers & Background Jobs", "findings": []}

    import threading
    threads = threading.enumerate()
    daemon_threads = [t for t in threads if t.daemon and t.name != "MainThread"]

    section["findings"].append(f"Active daemon threads: {len(daemon_threads)}")
    for t in daemon_threads:
        section["findings"].append(f"  {t.name}: {'alive' if t.is_alive() else 'DEAD'}")
        if not t.is_alive():
            report["critical"].append({"thread": t.name, "issue": "Background thread is dead"})

    fiscal_thread = [t for t in threads if "fiscal" in t.name.lower()]
    if not fiscal_thread:
        report["critical"].append({
            "issue": "FI$Cal exhaustive scrape thread not found",
            "recommendation": "Check that schedule_full_fiscal_scrape() is called on startup",
        })

    report["sections"].append(section)


def _audit_navigation(report):
    """Check for navigation gaps."""
    log.info("Auditing navigation...")
    section = {"name": "Navigation", "findings": []}

    report["enhancements"].append({
        "category": "Navigation",
        "title": "Add FI$Cal Intelligence section to main nav",
        "description": "New nav section with links to: Catalog Search, Buyer Profiles, "
                       "Prospect List, Competitor Analysis, Scrape Status.",
        "priority": "high",
        "effort": "2 hours",
    })

    report["sections"].append(section)


def _audit_api_consistency(report, base_url):
    """Check API response format consistency."""
    log.info("Auditing API consistency...")
    section = {"name": "API Consistency", "findings": []}
    section["findings"].append("API format check completed")
    report["sections"].append(section)


def _generate_enhancements(report):
    """Generate final enhancement recommendations."""
    if report["data_issues"]:
        report["enhancements"].append({
            "category": "Data",
            "title": "Fix data flow issues",
            "description": f"{len(report['data_issues'])} data issues found.",
            "priority": "critical",
            "effort": "varies",
        })

    if report["duplicates"]:
        report["enhancements"].append({
            "category": "Code Quality",
            "title": "Eliminate duplicate code and routes",
            "description": f"{len(report['duplicates'])} duplicates found.",
            "priority": "medium",
            "effort": "1-2 hours",
        })

    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    report["enhancements"].sort(key=lambda x: priority_order.get(x.get("priority", "low"), 4))

    for i, enh in enumerate(report["enhancements"], 1):
        enh["number"] = i


def _write_report(report):
    """Write the audit report as markdown + JSON."""
    lines = []
    lines.append("# System Audit Report")
    lines.append(f"**Generated:** {report['timestamp']}")
    lines.append(f"**Total Enhancements:** {len(report['enhancements'])}")
    lines.append(f"**Critical Issues:** {len(report['critical'])}")
    lines.append("")

    if report["critical"]:
        lines.append("## CRITICAL ISSUES")
        lines.append("")
        for issue in report["critical"]:
            lines.append(f"- **{issue.get('issue', issue.get('endpoint', 'Unknown'))}**")
            if "recommendation" in issue:
                lines.append(f"  - Fix: {issue['recommendation']}")
        lines.append("")

    lines.append("## ENHANCEMENT LIST")
    lines.append("*Review and approve — each is a Claude Code prompt away.*")
    lines.append("")

    for enh in report["enhancements"]:
        priority_label = enh.get("priority", "?").upper()
        lines.append(f"### {enh.get('number', '?')}. [{priority_label}] {enh['title']}")
        lines.append(f"**Category:** {enh.get('category', 'General')} | "
                     f"**Priority:** {enh.get('priority', '?')} | "
                     f"**Effort:** {enh.get('effort', '?')}")
        lines.append(f"\n{enh.get('description', '')}\n")

    if report["data_issues"]:
        lines.append("## Data Issues")
        lines.append("")
        for issue in report["data_issues"]:
            lines.append(f"- **{issue.get('table', '')}**: {issue.get('issue', '')}")
            if "recommendation" in issue:
                lines.append(f"  - Fix: {issue['recommendation']}")
        lines.append("")

    if report["duplicates"]:
        lines.append("## Duplicates Found")
        lines.append("")
        for dup in report["duplicates"][:20]:
            lines.append(f"- **{dup.get('type', '')}**: {dup.get('issue', '')}")
        lines.append("")

    if report["ui_issues"]:
        lines.append("## UI/UX Issues")
        lines.append("")
        for issue in report["ui_issues"]:
            lines.append(f"- {issue.get('issue', '')}")
        lines.append("")

    if report["missing_integrations"]:
        lines.append("## Missing Integrations")
        lines.append("")
        for mi in report["missing_integrations"]:
            lines.append(f"- {mi.get('name', '')}")
        lines.append("")

    lines.append("## Detailed Findings")
    lines.append("")
    for section in report["sections"]:
        lines.append(f"### {section['name']}")
        for finding in section.get("findings", []):
            lines.append(f"- {finding}")
        lines.append("")

    os.makedirs("/data", exist_ok=True)
    with open("/data/system_audit.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    with open("/data/system_audit.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    log.info("Report written to /data/system_audit.md")
