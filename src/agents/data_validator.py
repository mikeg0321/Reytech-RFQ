"""
Data Validator
Tests all data paths to ensure records are persisted correctly.
"""
import logging
import json
import os
from datetime import datetime

log = logging.getLogger("reytech.data_validator")


def validate_all():
    """Run all data validation checks. Returns report."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)

    report = {"timestamp": datetime.now().isoformat(), "checks": [],
              "passed": 0, "failed": 0, "warnings": 0}

    def _check(name, query, expected_min, severity="fail"):
        try:
            result = db.execute(query).fetchone()[0]
            passed = result >= expected_min
            report["checks"].append({"name": name, "result": result,
                                     "expected_min": expected_min, "passed": passed, "severity": severity})
            if passed:
                report["passed"] += 1
            elif severity == "fail":
                report["failed"] += 1
            else:
                report["warnings"] += 1
        except Exception as e:
            report["checks"].append({"name": name, "error": str(e)[:80], "passed": False, "severity": "fail"})
            report["failed"] += 1

    _check("PO master has records", "SELECT COUNT(*) FROM scprs_po_master", 1000)
    _check("PO lines has records", "SELECT COUNT(*) FROM scprs_po_lines", 5000)
    _check("PO master has suppliers", "SELECT COUNT(DISTINCT supplier) FROM scprs_po_master", 50)
    _check("PO master has departments", "SELECT COUNT(DISTINCT dept_name) FROM scprs_po_master", 10)
    _check("PO master has buyer emails", "SELECT COUNT(*) FROM scprs_po_master WHERE buyer_email != ''", 100)
    _check("PO lines have descriptions", "SELECT COUNT(*) FROM scprs_po_lines WHERE description != ''", 5000)
    _check("PO lines have unit prices", "SELECT COUNT(*) FROM scprs_po_lines WHERE unit_price != '' AND unit_price != '0'", 1000)
    _check("Reytech POs in master", "SELECT COUNT(*) FROM scprs_po_master WHERE UPPER(supplier) LIKE '%REYTECH%'", 100)
    _check("Catalog has items", "SELECT COUNT(*) FROM scprs_catalog", 1000)
    _check("Catalog items have prices", "SELECT COUNT(*) FROM scprs_catalog WHERE last_unit_price > 0", 500)
    _check("Buyer profiles exist", "SELECT COUNT(*) FROM scprs_buyers", 100)
    _check("Buyers have prospect scores", "SELECT COUNT(*) FROM scprs_buyers WHERE prospect_score > 0", 50)
    _check("Buyer items tracked", "SELECT COUNT(*) FROM scprs_buyer_items", 1000)
    _check("Catalog items enriched", "SELECT COUNT(*) FROM scprs_catalog WHERE enrichment_status = 'enriched'", 100, "warn")
    _check("Items with MFG numbers", "SELECT COUNT(*) FROM scprs_catalog WHERE mfg_number != ''", 50, "warn")

    try:
        orphans = db.execute("""
            SELECT COUNT(*) FROM scprs_po_lines l
            LEFT JOIN scprs_po_master m ON l.po_number = m.po_number
            WHERE m.po_number IS NULL
        """).fetchone()[0]
        report["checks"].append({"name": "No orphaned line items", "result": orphans,
                                 "expected_min": 0, "passed": orphans == 0, "severity": "warn"})
        if orphans == 0:
            report["passed"] += 1
        else:
            report["warnings"] += 1
    except Exception:
        pass

    # PO records
    po_dir = "/data/po_records"
    if os.path.exists(po_dir):
        files = os.listdir(po_dir)
        png_count = len([f for f in files if f.endswith(".png")])
        report["checks"].append({"name": "PO screenshots", "result": png_count,
                                 "expected_min": 1000, "passed": png_count >= 1000, "severity": "warn"})
        if png_count >= 1000:
            report["passed"] += 1
        else:
            report["warnings"] += 1

    # Intelligence function test
    try:
        from src.agents.quote_intelligence import enrich_extracted_items
        test = enrich_extracted_items([{"description": "nitrile gloves", "quantity": 100}])
        has_intel = bool(test and test[0].get("intelligence"))
        report["checks"].append({"name": "Quote intelligence returns data", "result": 1 if has_intel else 0,
                                 "expected_min": 1, "passed": has_intel, "severity": "fail"})
        if has_intel:
            report["passed"] += 1
        else:
            report["failed"] += 1
    except Exception as e:
        report["checks"].append({"name": "Quote intelligence returns data", "error": str(e)[:80],
                                 "passed": False, "severity": "fail"})
        report["failed"] += 1

    db.close()

    total = report["passed"] + report["failed"] + report["warnings"]
    report["summary"] = {
        "total_checks": total, "passed": report["passed"],
        "failed": report["failed"], "warnings": report["warnings"],
        "health": "OK" if report["failed"] == 0 else ("DEGRADED" if report["failed"] < 3 else "CRITICAL"),
    }

    log.info("Data validation: %d/%d passed, %d failed, %d warnings",
             report["passed"], total, report["failed"], report["warnings"])

    try:
        os.makedirs("/data", exist_ok=True)
        with open("/data/data_validation.json", "w") as f:
            json.dump(report, f, indent=2, default=str)
    except Exception:
        pass

    return report
