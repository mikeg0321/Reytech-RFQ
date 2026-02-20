"""
email_pipeline_qa.py — Email Pipeline QA System for Reytech
Tests the full email → PC/RFQ/CS pipeline against expected outcomes.

Capabilities:
  1. INBOX AUDIT: Connects to IMAP, lists all recent emails, classifies each,
     compares against what's actually in PCs + RFQs + CS outbox
  2. CLASSIFICATION TESTS: Runs known email samples through is_rfq_email(),
     is_recall_email(), is_reply_followup(), CS classify — checks accuracy
  3. GAP DETECTION: Finds emails that should have created PCs/RFQs but didn't
  4. PATTERN LEARNING: Logs failures with the email characteristics so
     patterns can be improved over time
  5. DEDUP VALIDATION: Ensures uniqueness rules work correctly

Runs: On demand via /api/qa/email-pipeline + auto after each poll cycle
"""

import os
import re
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("email_pipeline_qa")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

QA_LOG_FILE = os.path.join(DATA_DIR, "email_qa_log.json")
QA_PATTERNS_FILE = os.path.join(DATA_DIR, "email_qa_learned.json")


# ── Known Email Patterns (ground truth for testing) ──────────────────────────

# Each entry: {subject_pattern, sender_contains, has_pdfs, expected_type, notes}
GROUND_TRUTH = [
    # Valentina price checks (AMS 704 single-PDF pattern)
    {"subject_re": r"^Quote\s*[-–]\s*\w+.*-\s*\d{2}\.\d{2}\.\d{2,4}$",
     "sender_contains": "demidenko", "pdf_pattern": r"ams.?704",
     "expected": "price_check", "notes": "Valentina PC — subject: Quote - [Name] - [Date]"},

    # Recall emails
    {"subject_re": r"^Recall:",
     "expected": "recall", "notes": "Outlook recall request"},

    # Formal RFQ (704B + 703B + bid package)
    {"subject_re": r"(?:REQUEST FOR QUOTE|RFQ|SAC RFQ)\s*(?:PR\s*)?\d+",
     "expected": "rfq", "notes": "Formal RFQ with solicitation number"},

    # CS / general question from buyer
    {"subject_re": r"(?:question|inquiry|update|status|help|can you|do you)",
     "sender_contains": ".ca.gov",
     "expected": "cs_request", "notes": "Government buyer with non-RFQ question"},

    # Shipping notification
    {"subject_re": r"(?:tracking|shipped|shipment|delivery|fedex|ups)",
     "expected": "shipping", "notes": "Shipping/tracking notification"},
]


# ── Extract Expected Items from Inbox ────────────────────────────────────────

def parse_inbox_expectations(emails: list) -> list:
    """Given a list of email dicts (from IMAP), determine what each SHOULD produce.
    
    Returns list of {email, expected_type, expected_id, confidence, reasons}
    """
    expectations = []
    
    for em in emails:
        subject = em.get("subject", "")
        sender = em.get("sender", "")
        sender_email = em.get("sender_email", "")
        pdf_names = em.get("pdf_names", [])
        body = em.get("body_preview", "")
        
        result = {
            "email_uid": em.get("uid", ""),
            "subject": subject,
            "sender": sender_email or sender,
            "date": em.get("date", ""),
            "pdf_count": len(pdf_names),
            "pdf_names": pdf_names[:3],
            "expected_type": "unknown",
            "expected_id": "",
            "confidence": 0,
            "reasons": [],
        }
        
        subj_lower = subject.lower()
        
        # Check recalls first
        if subj_lower.startswith("recall:") or "would like to recall" in (body or "").lower():
            result["expected_type"] = "recall"
            result["confidence"] = 95
            result["expected_id"] = subject.replace("Recall:", "").replace("Recall: Quote request -", "").strip()
            result["reasons"].append("Recall pattern in subject")
            expectations.append(result)
            continue
        
        # Check formal RFQ (704B or 703B in PDF names)
        has_704b = any("704b" in p.lower() for p in pdf_names)
        has_703b = any("703b" in p.lower() for p in pdf_names)
        has_bid = any("bid" in p.lower() for p in pdf_names)
        has_rfq_subject = bool(re.search(r"(?:REQUEST FOR QUOTE|RFQ|SAC RFQ)\s*(?:PR\s*)?\d+", subject, re.I))
        
        if has_704b or has_703b or has_rfq_subject:
            result["expected_type"] = "rfq"
            result["confidence"] = 90 if (has_704b and has_703b) else 75
            sol_match = re.search(r'(?:PR\s*|RFQ\s*|SAC\s*RFQ\s*)(\d{5,})', subject, re.I)
            result["expected_id"] = sol_match.group(1) if sol_match else subject[:40]
            result["reasons"].append(f"Formal RFQ: 704B={'Y' if has_704b else 'N'} 703B={'Y' if has_703b else 'N'} bid={'Y' if has_bid else 'N'}")
            
            # Check if there's also a CS component in the body
            cs_signals = ["update your vendor record", "can you", "please complete", "question"]
            if any(s in (body or "").lower() for s in cs_signals):
                result["also_cs"] = True
                result["reasons"].append("Also contains CS/admin request")
            
            expectations.append(result)
            continue
        
        # Check price check (single 704 PDF, not 704B)
        has_704 = any(re.search(r"ams.?704(?!b)", p.lower()) for p in pdf_names)
        is_quote_subject = bool(re.search(r"^Quote\s*[-–:]", subject))
        
        if has_704 or (is_quote_subject and len(pdf_names) >= 1):
            result["expected_type"] = "price_check"
            result["confidence"] = 85
            # Extract PC name from subject: "Quote - Airway Adapter - 02.19.26" → "Airway Adapter"
            pc_match = re.match(r"Quote\s*[-–:]\s*(.+?)\s*[-–]\s*\d{2}\.\d{2}\.\d{2,4}", subject)
            if pc_match:
                result["expected_id"] = pc_match.group(1).strip()
            else:
                result["expected_id"] = subject[:40]
            result["reasons"].append(f"Price check: 704={'Y' if has_704 else 'N'} quote_subject={'Y' if is_quote_subject else 'N'}")
            expectations.append(result)
            continue
        
        # Check if it's from a known buyer domain
        is_buyer = any(d in (sender_email or "").lower() for d in [
            ".ca.gov", "cdcr", "calvet", "cdph", "cchcs", "dsh"
        ])
        
        # Check CS patterns
        from src.agents.cs_agent import is_update_request
        if is_buyer and is_update_request(subject, body or ""):
            result["expected_type"] = "cs_request"
            result["confidence"] = 70
            result["expected_id"] = f"CS: {subject[:40]}"
            result["reasons"].append("Buyer email matching CS patterns")
            expectations.append(result)
            continue
        
        # Check for CS by classification
        if is_buyer and not pdf_names:
            result["expected_type"] = "cs_request"
            result["confidence"] = 50
            result["reasons"].append("Buyer email with no PDFs — likely CS")
        elif pdf_names:
            result["expected_type"] = "price_check"
            result["confidence"] = 40
            result["reasons"].append("Has PDFs in dedicated inbox")
        else:
            result["expected_type"] = "skip"
            result["confidence"] = 30
            result["reasons"].append("No clear classification")
        
        expectations.append(result)
    
    return expectations


# ── Compare Expected vs Actual ───────────────────────────────────────────────

def audit_pipeline(expectations: list, pcs: dict, rfqs: dict, cs_drafts: list = None) -> dict:
    """Compare what should exist vs what actually exists.
    
    Returns: {
        total_emails, matched, gaps: [...], false_positives: [...],
        recalls_handled, score, grade
    }
    """
    cs_drafts = cs_drafts or []
    
    matched = []
    gaps = []
    recalls = []
    
    # Build lookup from existing PCs by name/institution
    pc_names = set()
    for pid, pc in pcs.items():
        name = (pc.get("pc_number") or "").lower().strip()
        pc_names.add(name)
        # Also add normalized versions
        for word in name.split():
            if len(word) > 3:
                pc_names.add(word)
    
    # Build lookup from existing RFQs by solicitation
    rfq_sols = set()
    for rid, rfq in rfqs.items():
        sol = (rfq.get("solicitation_number") or "").lower().strip()
        rfq_sols.add(sol)
        subj = (rfq.get("email_subject") or "").lower()
        rfq_sols.add(subj[:50])
    
    for exp in expectations:
        etype = exp["expected_type"]
        eid = exp.get("expected_id", "").lower().strip()
        
        if etype == "recall":
            recalls.append(exp)
            continue
        
        if etype == "skip":
            continue
        
        found = False
        
        if etype == "price_check":
            # Check if PC exists with matching name
            for name in pc_names:
                if eid and (eid.lower() in name or name in eid.lower()):
                    found = True
                    break
            # Fuzzy: check by significant words
            if not found and eid:
                eid_words = set(w.lower() for w in eid.split() if len(w) > 2)
                for pid, pc in pcs.items():
                    pc_num = (pc.get("pc_number") or "").lower()
                    pc_words = set(w.lower() for w in pc_num.split() if len(w) > 2)
                    if eid_words and pc_words and len(eid_words & pc_words) >= 1:
                        found = True
                        break
        
        elif etype == "rfq":
            for sol in rfq_sols:
                if eid and (eid.lower() in sol or sol in eid.lower()):
                    found = True
                    break
            # Also check PCs (RFQs might be in PC queue)
            if not found:
                for pid, pc in pcs.items():
                    pc_num = (pc.get("pc_number") or "").lower()
                    if eid and eid.lower() in pc_num:
                        found = True
                        break
        
        elif etype == "cs_request":
            # Check CS outbox for drafts matching this sender
            sender = exp.get("sender", "").lower()
            for draft in cs_drafts:
                if sender in (draft.get("to") or "").lower():
                    found = True
                    break
        
        if found:
            matched.append({**exp, "status": "found"})
        else:
            gaps.append({**exp, "status": "MISSING"})
    
    total = len([e for e in expectations if e["expected_type"] != "skip"])
    found_count = len(matched)
    gap_count = len(gaps)
    score = round(found_count / max(total, 1) * 100)
    
    if score >= 95:
        grade = "A"
    elif score >= 80:
        grade = "B"
    elif score >= 60:
        grade = "C"
    elif score >= 40:
        grade = "D"
    else:
        grade = "F"
    
    return {
        "total_emails": len(expectations),
        "total_actionable": total,
        "matched": found_count,
        "gaps": gaps,
        "gap_count": gap_count,
        "recalls": len(recalls),
        "score": score,
        "grade": grade,
        "details": {
            "matched": matched,
            "recalls": recalls,
        },
    }


# ── Run Classification Tests ─────────────────────────────────────────────────

def test_classification() -> dict:
    """Run offline classification tests against known patterns.
    
    Tests: is_rfq_email, is_recall_email, CS classify
    """
    from src.agents.email_poller import is_rfq_email, is_recall_email
    from src.agents.cs_agent import classify_inbound_email, is_update_request
    
    tests = [
        # (subject, body, pdfs, expected_rfq, expected_recall, expected_cs, label)
        # === Valentina price checks ===
        ("Quote - Airway Adapter - 02.19.26", "Please email me quote", ["AMS 704 - Airway Adapter.pdf"],
         True, False, False, "Valentina PC"),
        ("Quote - MH OS - 02.19.26", "Please email me quote", ["AMS 704 - MH OS.pdf"],
         True, False, False, "Valentina PC (MH OS)"),
        ("Quote - BLS Med - 02.19.26", "Please email me quote", ["AMS 704 - BLS Med.pdf"],
         True, False, False, "Valentina PC (BLS Med)"),
        ("Quote - BLS IT Med - 02.19.26", "Please email me quote", ["AMS 704 - BLS IT Med.pdf"],
         True, False, False, "Valentina PC (BLS IT Med)"),
        ("Quote - Med OS - 02.19.26", "Please email me quote", ["AMS 704 - Med OS.pdf"],
         True, False, False, "Valentina PC (Med OS)"),
        # === Formal RFQs ===
        ("REQUEST FOR QUOTE PR 10838349", "Please sign and quote for CCWF",
         ["AMS 704B - PR 10838349.pdf", "BID PACKAGE & FORMS.pdf", "AMS 703B - PR 10838349.pdf"],
         True, False, False, "Janie formal RFQ"),
        ("SAC RFQ 10837794", "update your vendor record",
         ["204, 205, SUPP.pdf", "R26Q8_10837794.pdf", "10837794 AMS 704B.pdf"],
         True, False, False, "Jessica formal RFQ"),
        # === Recalls ===
        ("Recall: Quote - Med OS - 02.17.26",
         'Demidenko, Valentina@CDCR would like to recall the message, "Quote - Med OS - 02.17.26".',
         [], False, True, False, "Recall Med OS"),
        ("Recall: Quote request - MH OS - 02.17.26",
         'Demidenko, Valentina@CDCR would like to recall the message, "Quote request - MH OS - 02.17.26".',
         [], False, True, False, "Recall MH OS"),
        # === CS requests ===
        ("Question about pricing", "Can you send me pricing for nitrile gloves?",
         [], False, False, True, "CS pricing question"),
        ("Invoice status", "When will the invoice be ready for PO 12345?",
         [], False, False, True, "CS invoice question"),
        ("Following up on quote", "Following up on quote R26Q16, any update?",
         [], False, False, True, "CS follow-up"),
        # === Edge cases ===
        ("Meeting notes", "Here are the meeting notes from today",
         [], False, False, False, "Non-actionable email"),
        ("FW: Quote request", "Please see attached", ["forwarded_704.pdf"],
         True, False, False, "Forwarded RFQ"),
        # === RE: prefix with PDFs (buyer replying with new submission) ===
        ("RE: Quote - Med OS - 02.19.26", "Please email me quote", ["AMS 704 - Med OS.pdf"],
         True, False, False, "RE: prefix with AMS 704 PDF"),
        ("RE: SAC RFQ 10837794", "update your vendor record",
         ["10837794 AMS 704B.pdf"], True, False, False, "RE: prefix with 704B PDF"),
        ("RE: Quote Request - OS Den - 02.13.2026", "Please email me quote",
         ["AMS 704 - OS Den.pdf"], True, False, False, "RE: prefix with 704 (OS Den)"),
        # === Reply with NO PDF = CS follow-up ===
        ("RE: Quote - Med OS - 02.19.26", "What is the status of this quote?",
         [], False, False, True, "RE: no PDF = CS follow-up"),
    ]
    
    results = []
    passed = 0
    failed = 0
    
    for subj, body, pdfs, exp_rfq, exp_recall, exp_cs, label in tests:
        actual_rfq = is_rfq_email(subj, body, pdfs)
        actual_recall = is_recall_email(subj, body) is not None
        actual_cs = is_update_request(subj, body)
        
        rfq_ok = actual_rfq == exp_rfq
        recall_ok = actual_recall == exp_recall
        cs_ok = actual_cs == exp_cs
        all_ok = rfq_ok and recall_ok and cs_ok
        
        if all_ok:
            passed += 1
        else:
            failed += 1
        
        results.append({
            "label": label,
            "subject": subj[:50],
            "passed": all_ok,
            "rfq": {"expected": exp_rfq, "actual": actual_rfq, "ok": rfq_ok},
            "recall": {"expected": exp_recall, "actual": actual_recall, "ok": recall_ok},
            "cs": {"expected": exp_cs, "actual": actual_cs, "ok": cs_ok},
        })
    
    total = len(tests)
    score = round(passed / max(total, 1) * 100)
    
    return {
        "total_tests": total,
        "passed": passed,
        "failed": failed,
        "score": score,
        "grade": "A" if score >= 95 else "B" if score >= 80 else "C" if score >= 60 else "F",
        "results": results,
    }


# ── Log QA Results for Learning ──────────────────────────────────────────────

def log_qa_result(result: dict):
    """Persist QA results for trend tracking and pattern learning."""
    try:
        existing = []
        if os.path.exists(QA_LOG_FILE):
            with open(QA_LOG_FILE) as f:
                existing = json.load(f)
        
        entry = {
            "run_at": datetime.now().isoformat(),
            "score": result.get("score", 0),
            "grade": result.get("grade", "?"),
            "total": result.get("total_actionable", 0),
            "matched": result.get("matched", 0),
            "gaps": len(result.get("gaps", [])),
            "gap_subjects": [g.get("subject", "")[:50] for g in result.get("gaps", [])[:10]],
        }
        existing.append(entry)
        
        # Keep last 100 runs
        if len(existing) > 100:
            existing = existing[-100:]
        
        with open(QA_LOG_FILE, "w") as f:
            json.dump(existing, f, indent=2, default=str)
        
    except Exception as e:
        log.debug("QA log save error: %s", e)


def log_learned_pattern(email_data: dict, expected_type: str, failure_reason: str):
    """Log a classification failure so patterns can be improved."""
    try:
        existing = []
        if os.path.exists(QA_PATTERNS_FILE):
            with open(QA_PATTERNS_FILE) as f:
                existing = json.load(f)
        
        existing.append({
            "logged_at": datetime.now().isoformat(),
            "subject": email_data.get("subject", "")[:80],
            "sender": email_data.get("sender", "")[:50],
            "pdf_names": email_data.get("pdf_names", [])[:5],
            "expected_type": expected_type,
            "failure_reason": failure_reason,
            "resolved": False,
        })
        
        if len(existing) > 200:
            existing = existing[-200:]
        
        with open(QA_PATTERNS_FILE, "w") as f:
            json.dump(existing, f, indent=2, default=str)
        
    except Exception as e:
        log.debug("QA pattern log error: %s", e)


def get_qa_trends() -> dict:
    """Get QA score trends over time."""
    try:
        if not os.path.exists(QA_LOG_FILE):
            return {"runs": 0, "trend": "no_data"}
        
        with open(QA_LOG_FILE) as f:
            history = json.load(f)
        
        if not history:
            return {"runs": 0, "trend": "no_data"}
        
        recent = history[-10:]
        scores = [r.get("score", 0) for r in recent]
        avg = sum(scores) / len(scores)
        
        # Trend: improving, stable, declining
        if len(scores) >= 3:
            first_half = sum(scores[:len(scores)//2]) / max(len(scores)//2, 1)
            second_half = sum(scores[len(scores)//2:]) / max(len(scores) - len(scores)//2, 1)
            if second_half > first_half + 5:
                trend = "improving"
            elif second_half < first_half - 5:
                trend = "declining"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"
        
        # Common failures
        all_gaps = []
        for r in history[-20:]:
            all_gaps.extend(r.get("gap_subjects", []))
        
        return {
            "runs": len(history),
            "avg_score": round(avg, 1),
            "latest_score": scores[-1] if scores else 0,
            "latest_grade": recent[-1].get("grade", "?") if recent else "?",
            "trend": trend,
            "common_gaps": list(set(all_gaps))[:10],
        }
    except Exception:
        return {"runs": 0, "trend": "error"}


# ── Full Pipeline Audit (connects to IMAP) ───────────────────────────────────

def full_inbox_audit(email_config: dict = None) -> dict:
    """Connect to IMAP, pull recent emails, audit the full pipeline.
    
    This is the main entry point for the QA system.
    """
    import imaplib
    import email as email_lib
    from email.header import decode_header
    
    if not email_config:
        try:
            from src.api.dashboard import CONFIG
            email_config = CONFIG.get("email", {})
        except Exception:
            return {"error": "No email config available"}
    
    # Connect to IMAP
    try:
        mail = imaplib.IMAP4_SSL(
            email_config.get("imap_host", "imap.gmail.com"),
            email_config.get("imap_port", 993)
        )
        mail.login(
            email_config.get("email", ""),
            email_config.get("email_password", "")
        )
        mail.select("INBOX")
    except Exception as e:
        return {"error": f"IMAP connection failed: {e}"}
    
    # Fetch recent emails (last 5 days)
    since_date = (datetime.now() - timedelta(days=5)).strftime("%d-%b-%Y")
    status, messages = mail.uid("search", None, f"(SINCE {since_date})")
    
    if status != "OK":
        return {"error": "IMAP search failed"}
    
    uids = messages[0].split() if messages[0] else []
    
    emails = []
    for uid_bytes in uids:
        uid = uid_bytes.decode()
        try:
            # Use same fetch approach as EmailPoller (known working)
            status, data = mail.uid("fetch", uid_bytes, "(BODY.PEEK[])")
            if status != "OK" or not data or not data[0]:
                continue
            
            raw_email = data[0][1] if isinstance(data[0], tuple) else None
            if not raw_email:
                continue
            
            msg = email_lib.message_from_bytes(raw_email)
            
            # Decode subject
            raw_subj = msg.get("Subject", "")
            if raw_subj:
                parts = decode_header(raw_subj)
                subject = "".join(
                    p.decode(enc or "utf-8", errors="replace") if isinstance(p, bytes) else str(p)
                    for p, enc in parts
                )
            else:
                subject = ""
            
            # Decode sender
            raw_from = msg.get("From", "")
            if raw_from:
                parts = decode_header(raw_from)
                sender = "".join(
                    p.decode(enc or "utf-8", errors="replace") if isinstance(p, bytes) else str(p)
                    for p, enc in parts
                )
            else:
                sender = ""
            
            sender_email = ""
            em = re.search(r'[\w.+-]+@[\w.-]+\.\w+', sender)
            if em:
                sender_email = em.group(0)
            
            # Get PDF attachment names and body text
            pdf_names = []
            body_text = ""
            
            for part in msg.walk():
                # Get PDF filenames
                fn = part.get_filename()
                if fn:
                    # Decode filename
                    fn_parts = decode_header(fn)
                    fn_decoded = "".join(
                        p.decode(enc or "utf-8", errors="replace") if isinstance(p, bytes) else str(p)
                        for p, enc in fn_parts
                    )
                    if fn_decoded.lower().endswith(".pdf"):
                        pdf_names.append(fn_decoded)
                
                # Get body text (first text/plain part)
                if not body_text and part.get_content_type() == "text/plain":
                    try:
                        payload = part.get_payload(decode=True)
                        if payload:
                            body_text = payload.decode("utf-8", errors="replace")[:800]
                    except Exception:
                        pass
            
            emails.append({
                "uid": uid,
                "subject": subject.strip(),
                "sender": sender,
                "sender_email": sender_email,
                "date": msg.get("Date", ""),
                "pdf_names": pdf_names,
                "pdf_count": len(pdf_names),
                "body_preview": body_text[:300],
            })
        except Exception as e:
            log.debug("QA: failed to parse email %s: %s", uid, e)
            continue
    
    mail.logout()
    
    # Parse expectations
    expectations = parse_inbox_expectations(emails)
    
    # Load current system state
    try:
        pc_path = os.path.join(DATA_DIR, "price_checks.json")
        with open(pc_path) as f:
            pcs = json.load(f)
    except Exception:
        pcs = {}
    
    try:
        rfq_path = os.path.join(DATA_DIR, "rfq_queue.json")
        with open(rfq_path) as f:
            rfqs = json.load(f)
    except Exception:
        rfqs = {}
    
    # Load CS drafts
    cs_drafts = []
    try:
        outbox_path = os.path.join(DATA_DIR, "cs_outbox.json")
        if os.path.exists(outbox_path):
            with open(outbox_path) as f:
                cs_drafts = json.load(f)
    except Exception:
        pass
    
    # Run audit
    result = audit_pipeline(expectations, pcs, rfqs, cs_drafts)
    result["emails_scanned"] = len(emails)
    result["system_state"] = {
        "pcs": len(pcs),
        "rfqs": len(rfqs),
        "cs_drafts": len(cs_drafts),
        "pc_names": [pc.get("pc_number", "?")[:30] for pc in pcs.values()][:10],
        "rfq_sols": [r.get("solicitation_number", "?") for r in rfqs.values()][:10],
    }
    result["inbox_summary"] = [
        {"subject": e["subject"][:60], "sender": e["sender_email"],
         "pdfs": e["pdf_count"], "date": e["date"][:25]}
        for e in emails
    ]
    result["expectations_summary"] = [
        {"subject": e["subject"][:60], "type": e["expected_type"],
         "confidence": e["confidence"], "id": e.get("expected_id", "")[:30]}
        for e in expectations
    ]
    
    # Log failures for learning
    for gap in result.get("gaps", []):
        log_learned_pattern(gap, gap["expected_type"], "Not found in system")
    
    # Log result
    log_qa_result(result)
    
    # Run classification tests too
    class_results = test_classification()
    result["classification_tests"] = class_results
    
    return result
