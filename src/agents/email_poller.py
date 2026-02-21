


#!/usr/bin/env python3
"""
Email Poller v2 — monitors IMAP inbox for RFQ emails.
Improved: broader detection (dedicated inbox assumption), forwarded email handling,
robust reconnection, manual trigger support.
"""

import imaplib
import email
from email.header import decode_header
import os, time, json, re, logging
from datetime import datetime, timedelta

log = logging.getLogger("email_poller")

# ── Shared DB Context (Anthropic Skills Guide: Pattern 5 — Domain Intelligence) ──
# Full access to live CRM, quotes, revenue, price history, voice calls from SQLite.
try:
    from src.core.agent_context import (
        get_context, format_context_for_agent,
        get_contact_by_agency, get_best_price,
    )
    HAS_AGENT_CTX = True
except ImportError:
    HAS_AGENT_CTX = False
    def get_context(**kw): return {}
    def format_context_for_agent(c, **kw): return ""
    def get_contact_by_agency(a): return []
    def get_best_price(d): return None

# ═══════════════════════════════════════════════════════════════════════════════
# RFQ Detection
# ═══════════════════════════════════════════════════════════════════════════════

# Strong indicators — if any match, definitely an RFQ
RFQ_STRONG = [
    "request for quotation", "rfq", "703b", "704b", "bid package",
    "cchcs", "cdcr", "solicitation", "informal competitive",
    "acquisition quote", "quote worksheet", "bid response",
]

# Known Price Check sender patterns (first/last name fragments)
# These senders always send AMS 704 price checks, NOT RFQs
PC_KNOWN_SENDERS = [
    "demidenko", "valentina.demidenko",   # Valentina Demidenko — CSP-Sacramento
    "delgado", "matt.delgado",            # Matt Delgado
]

# Subject patterns that indicate a Price Check (not an RFQ)
PC_SUBJECT_PATTERNS = [
    r"^quote\s*-\s*",                     # "Quote - Airway Adapter - 02.19.26"
    r"^price\s*quote\s*\d*",              # "Price Quote 001"
    r"^pc\s*[-#]",                        # "PC - Something" or "PC #123"
    r"^price\s*check\s*[-#]",             # "Price Check - Something"
    r"^ams\s*704\s*[-#]",                 # "AMS 704 - Something"
]

# PDF filename patterns that indicate RFQ attachments
RFQ_PDF_PATTERNS = [
    r"703b", r"704b", r"bid.?package", r"rfq", r"solicitation",
    r"quote.?worksheet", r"attachment.?\d", r"ams.?7\d\d",
    r"informal.?competitive", r"acquisition",
]

ATTACHMENT_PATTERNS = {
    "703b": ["703b", "rfq", "request_for_quotation", "informal_competitive", "attachment_1", "attachment1"],
    "704b": ["704b", "quote_worksheet", "acquisition_quote", "attachment_2", "attachment2"],
    "bidpkg": ["bid_package", "bid package", "forms", "attachment_3", "attachment3", "under_100k", "under 100k"],
}

# ═══════════════════════════════════════════════════════════════════════════════
# Purchase Order (PO) / Award Detection
# ═══════════════════════════════════════════════════════════════════════════════

# Strong PO indicators in subject or body
PO_STRONG_SUBJECT = [
    "purchase order", "p.o.", "po #", "po#", "po number",
    "notice of award", "award notice", "award notification",
    "std 65", "std65", "std-65",
    "you have been awarded", "contract award",
    "order confirmation",
    "fi$cal", "fiscal", "po distribution",
    "po dist", "encumbrance",
]

PO_BODY_PHRASES = [
    "purchase order number", "po number", "purchase order #",
    "you have been awarded", "notice of award", "award notification",
    "pleased to inform you", "contract has been awarded",
    "std 65", "std65", "order is confirmed",
    "purchase order is attached", "attached purchase order",
    "po is attached", "attached po",
    "fi$cal", "po distribution", "encumbrance",
    "award has been made", "contract executed",
]

# PDF filenames that suggest a PO document
PO_PDF_PATTERNS = [
    r"purchase.?order", r"^po[_\-\s]", r"std.?65", r"award",
    r"p\.?o\.?\s*\d", r"order.?confirm",
]


def _parse_po_pdf(pdf_path: str) -> dict:
    """Parse a Purchase Order PDF (STD-65, Fi$cal, or similar) for line items and metadata.
    
    Uses pypdf (already in requirements.txt) for text extraction.
    Multiple line-item parsing patterns for California state PO formats.
    
    Returns: {po_number, agency, institution, ship_to_address, items: [{description, qty, unit_price, part_number, extended}], total}
    """
    # Extract text using pypdf (installed on Railway)
    text = ""
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        for page in reader.pages:
            page_text = page.extract_text() or ""
            text += page_text + "\n"
    except Exception as e:
        log.error("pypdf extraction failed: %s", e)
    
    # Fallback: pdftotext (poppler, if available)
    if not text or len(text.strip()) < 50:
        try:
            import subprocess
            result = subprocess.run(
                ["pdftotext", "-layout", pdf_path, "-"],
                capture_output=True, text=True, timeout=15
            )
            if result.stdout and len(result.stdout.strip()) > len(text.strip()):
                text = result.stdout
        except Exception:
            pass
    
    # OCR fallback: scanned/image PDFs (e.g. "Microsoft Print to PDF" state forms)
    if not text or len(text.strip()) < 50:
        try:
            import subprocess, tempfile, glob
            with tempfile.TemporaryDirectory() as tmpdir:
                # Convert PDF pages to images with pdftoppm
                subprocess.run(
                    ["pdftoppm", "-r", "300", "-png", pdf_path, os.path.join(tmpdir, "page")],
                    capture_output=True, timeout=60
                )
                page_images = sorted(glob.glob(os.path.join(tmpdir, "page-*.png")))
                if not page_images:
                    # Try without -png flag (older pdftoppm)
                    subprocess.run(
                        ["pdftoppm", "-r", "300", pdf_path, os.path.join(tmpdir, "page")],
                        capture_output=True, timeout=60
                    )
                    page_images = sorted(glob.glob(os.path.join(tmpdir, "page-*.ppm")))
                
                ocr_text = ""
                for img_path in page_images:
                    result = subprocess.run(
                        ["tesseract", img_path, "stdout"],
                        capture_output=True, text=True, timeout=30
                    )
                    ocr_text += result.stdout + "\n"
                
                if len(ocr_text.strip()) > len(text.strip()):
                    text = ocr_text
                    log.info("PO PDF: OCR extracted %d chars from %d pages", len(text), len(page_images))
        except Exception as e:
            log.warning("OCR fallback failed: %s", e)
    
    if not text or len(text.strip()) < 30:
        log.warning("PO PDF: no text extracted from %s", pdf_path)
        return {}
    
    log.info("PO PDF text extracted: %d chars from %s", len(text), os.path.basename(pdf_path))
    result = {"items": [], "po_number": "", "agency": "", "institution": "", 
              "ship_to_address": [], "total": 0, "_raw_text": text[:3000]}
    
    # ── Extract PO / Encumbrance Number ──
    # STD-65 format: "PURCHASE ORDER NUMBER\n00015 02/19/2026 00000000 4500750017"
    # The PO number is the last (longest) number on the line after the header
    po_header = re.search(r'PURCHASE\s*ORDER\s*NUMBER\s*\n([^\n]+)', text, re.IGNORECASE)
    if po_header:
        # Find all 7+ digit numbers on that line — PO is typically the longest/last
        nums = re.findall(r'\b(\d{7,13})\b', po_header.group(1))
        if nums:
            result["po_number"] = nums[-1]  # Last long number is the PO
    
    if not result["po_number"]:
        for pat in [
            r'(?:Purchase\s*Order|P\.?O\.?)\s*(?:Number|No\.?|#)?\s*:?\s*(\d{7,13})',
            r'(?:PO|STD\s*65)\s*#?\s*:?\s*(\d{7,13})',
            r'Order\s*(?:Number|No\.?)\s*:?\s*(\d{7,13})',
            r'Encumbrance\s*(?:Number|No\.?|#)?\s*:?\s*(\d{7,13})',
            r'Document\s*(?:Number|No\.?|#|ID)\s*:?\s*(\d{7,13})',
        ]:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                result["po_number"] = m.group(1)
                break
    
    # ── Extract Agency / Institution / Ship-To ──
    # STD-65: "CA State Prison Sacramento Dept. of Corrections..."
    # Look for California prison/facility names first
    inst_patterns = [
        r'((?:CA|California)\s+State\s+Prison[\w\s,\-]*?)(?:\s+Dept|\s+AGENCY)',
        r'((?:CSP|CCI|CCWF|CMC|CMF|CRC|CIW|CTF|CHCF|DVI|FSP|HDSP|ISP|KVSP|LAC|MCSP|NKSP|PBSP|RJD|SAC|SCC|SOL|SQ|SVSP|VSP|WSP)\b[\w\s,\-]{3,50}?)(?:\s+Dept|\s+AGENCY|\s+Attn)',
        r'(?:Ship\s*To|Deliver\s*To)\s*[:\n]\s*([^\n]{5,80})',
        r'(Correctional\s+(?:Training\s+Facility|Institution|Center)[\w\s,\-]*?)(?:\s+Dept|\s+AGENCY)',
    ]
    for pat in inst_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = m.group(1).strip().rstrip(",")
            if len(val) > 3:
                result["institution"] = val
                break
    
    # Agency abbreviation
    text_low = text.lower()
    if not result["agency"]:
        if "cdcr" in text_low or "corrections" in text_low:
            result["agency"] = "CDCR"
        elif "cchcs" in text_low or "correctional health" in text_low:
            result["agency"] = "CCHCS"
        elif "calvet" in text_low or "veterans" in text_low:
            result["agency"] = "CalVet"
        elif "dsh" in text_low or "state hospital" in text_low:
            result["agency"] = "DSH"
    
    # Ship-to address: look for address lines near SHIP TO
    ship_match = re.search(
        r'(?:SHIP|Ship\s*To)\s*\n?\s*([^\n]*?(?:Road|Ave|St|Blvd|Dr|Way|Hwy)[^\n]*)\s*\n\s*([^\n]*?(?:CA|California)\s+\d{5}[^\n]*)',
        text, re.IGNORECASE
    )
    if ship_match:
        result["ship_to_address"] = [ship_match.group(1).strip(), ship_match.group(2).strip()]
    elif not result["ship_to_address"]:
        ship_match2 = re.search(r'(?:Ship\s*To|Deliver\s*To)\s*:?\s*\n((?:[^\n]+\n){1,4})', text, re.IGNORECASE)
        if ship_match2:
            addr_lines = [l.strip() for l in ship_match2.group(1).split("\n") if l.strip()]
            result["ship_to_address"] = addr_lines[:4]
    
    # ── Extract Line Items — multiple strategies ──
    lines = text.split("\n")
    items_found = []
    
    # Strategy 0: California STD-65 PO format (OCR output)
    # "1 10 EA | 60121702 Pad, Replacement - Fits Stamp R-532 Taxable 13.92 139.20"
    # Next line often has part number: "R-532-7"
    for i, line in enumerate(lines):
        m = re.match(
            r'\s*(\d{1,3})\s+'                              # line number
            r'(\d+)\s+'                                      # quantity
            r'([A-Z]{2,4})\s*'                               # unit (EA, CS, PAC, RL, CAR, BX)
            r'[|}\]]\s*'                                     # pipe separator (OCR: | } ])
            r'(\d{5,8})?\s*'                                 # UNSPSC code (optional)
            r'(.+?)\s+'                                      # description
            r'(?:Taxable|Non-?Taxable|Tax(?:able)?)\s+'      # tax category
            r'([\d,]+\.?\d{0,2})\s+'                         # unit price
            r'([\d,]+\.?\d{0,2})',                           # extension total
            line, re.IGNORECASE
        )
        if m:
            desc = m.group(5).strip()
            # Check next line for part number or description continuation
            part_number = ""
            desc_continuation = ""
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                # Skip if it's another item line, header, or has prices
                if (next_line and len(next_line) < 80 
                    and not re.match(r'\s*\d{1,3}\s+\d+\s+[A-Z]', next_line)
                    and not re.search(r'(?:Page|STATE|NUMBER|QUANTITY|PURCHASE)', next_line, re.IGNORECASE)
                    and not re.search(r'[\d,]+\.\d{2}\s+[\d,]+\.\d{2}', next_line)):
                    
                    raw = next_line.strip()
                    # Classify: part number vs description continuation
                    # Part numbers: "R-532-7", "6/4911", "S-6261; 8/CS", "17051", "S-852"
                    # Description: "with handle", "UNDER BED", "*SEE ATTACHED*"
                    is_part_num = bool(re.match(
                        r'^[A-Z0-9][\w\-/\.]*(?:\s*;\s*\d+/[A-Z]+)?$',
                        raw.split(";")[0].strip(), re.IGNORECASE
                    )) and len(raw.split()) <= 3
                    
                    if is_part_num:
                        part_number = raw.split(";")[0].strip()
                        desc = f"{desc} — {raw}"
                    else:
                        # Description continuation (e.g., "with handle", "UNDER BED")
                        desc = f"{desc}, {raw}"
            
            items_found.append({
                "description": desc,
                "qty": int(m.group(2)),
                "unit_price": float(m.group(6).replace(",", "")),
                "extended": float(m.group(7).replace(",", "")),
                "part_number": part_number,
            })
    
    # Strategy 1: Numbered line items — "1  description  5  EA  $12.50  $62.50"
    if not items_found:
        for line in lines:
            m = re.match(
                r'\s*(\d{1,3})\s+'                              # line number
                r'(.{8,150}?)\s+'                                 # description (greedy-ish)
                r'(\d+(?:\.\d+)?)\s+'                             # quantity
                r'(?:ea|each|bx|box|pk|cs|case|bn|pc|set|kit|pr|dz|gal|lb)\.?\s+'  # unit
                r'\$?([\d,]+\.?\d{0,2})\s*'                       # unit price
                r'(?:\$?([\d,]+\.?\d{0,2}))?',                    # extended (optional)
                line, re.IGNORECASE
            )
            if m:
                items_found.append({
                    "description": m.group(2).strip(),
                    "qty": int(float(m.group(3))),
                    "unit_price": float(m.group(4).replace(",", "")),
                    "extended": float(m.group(5).replace(",", "")) if m.group(5) else round(int(float(m.group(3))) * float(m.group(4).replace(",", "")), 2),
                })
    
    # Strategy 2: "qty  unit  description  $price  $ext" (no line number)
    if not items_found:
        for line in lines:
            m = re.match(
                r'\s*(\d+(?:\.\d+)?)\s+'                             # quantity
                r'(?:ea|each|bx|box|pk|cs|case|bn|pc|set|kit|pr)\.?\s+'  # unit
                r'(.{8,150}?)\s+'                                     # description
                r'\$?([\d,]+\.?\d{0,2})\s*'                           # unit price
                r'(?:\$?([\d,]+\.?\d{0,2}))?',                        # extended
                line, re.IGNORECASE
            )
            if m:
                items_found.append({
                    "description": m.group(2).strip(),
                    "qty": int(float(m.group(1))),
                    "unit_price": float(m.group(3).replace(",", "")),
                    "extended": float(m.group(4).replace(",", "")) if m.group(4) else round(int(float(m.group(1))) * float(m.group(3).replace(",", "")), 2),
                })
    
    # Strategy 3: Description on one line, qty + price on next/same — look for $ amounts
    if not items_found:
        for i, line in enumerate(lines):
            # Look for lines with dollar amounts that look like line items
            m = re.search(
                r'(\d+)\s+(?:ea|each|bx|box|pk|cs|case|pc|set)\.?\s+.*?\$\s*([\d,]+\.?\d{2})\s+\$\s*([\d,]+\.?\d{2})',
                line, re.IGNORECASE
            )
            if m:
                qty = int(m.group(1))
                up = float(m.group(2).replace(",", ""))
                ext = float(m.group(3).replace(",", ""))
                # Description might be earlier in same line or previous line
                desc_part = line[:m.start()].strip()
                if len(desc_part) < 5 and i > 0:
                    desc_part = lines[i-1].strip()
                items_found.append({
                    "description": desc_part[:150],
                    "qty": qty,
                    "unit_price": up,
                    "extended": ext,
                })
    
    # Strategy 4: Ultra-flexible — any line with qty + two dollar amounts
    if not items_found:
        for line in lines:
            dollars = re.findall(r'\$\s*([\d,]+\.?\d{2})', line)
            qty_match = re.search(r'\b(\d{1,5})\s+(?:ea|each|bx|box|pk|pc|cs|set|kit)\b', line, re.IGNORECASE)
            if len(dollars) >= 2 and qty_match:
                qty = int(qty_match.group(1))
                up = float(dollars[0].replace(",", ""))
                ext = float(dollars[-1].replace(",", ""))
                # Get description — text before the quantity
                desc = line[:qty_match.start()].strip()
                # Remove leading line numbers
                desc = re.sub(r'^\d{1,3}\s+', '', desc)
                if desc and len(desc) > 3:
                    items_found.append({
                        "description": desc[:150],
                        "qty": qty,
                        "unit_price": up,
                        "extended": ext,
                    })
    
    # Extract part numbers from descriptions (only if not already set by Strategy 0)
    for it in items_found:
        if it.get("part_number"):
            continue  # Already extracted by STD-65 parser
        desc = it.get("description", "")
        pn_match = re.search(r'(?:P/?N|Part|#|PN|ASIN|Item\s*#?)\s*:?\s*([\w\-]{4,30})', desc, re.IGNORECASE)
        it["part_number"] = pn_match.group(1) if pn_match else ""
    
    result["items"] = items_found
    
    # ── Extract Total ──
    # Prefer GRAND TOTAL (includes tax) over SUBTOTAL
    for pat in [
        r'GRAND\s*TOTAL\s*\n?\s*\$?\s*([\d,]+\.?\d{0,2})',
        r'(?:Grand\s*)?Total\s*(?:Amount)?\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
        r'Amount\s*(?:Due|Payable)\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
        r'Total\s*(?:Price|Cost|Value)\s*:?\s*\$?\s*([\d,]+\.?\d{0,2})',
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = float(m.group(1).replace(",", ""))
            if val > 0:
                result["total"] = val
                break
    
    if not result["total"] and items_found:
        result["total"] = sum(it.get("extended", 0) for it in items_found)
    
    # Extract subtotal and tax separately
    sub_match = re.search(r'SUBTOTAL\s*\n?\s*\$?\s*([\d,]+\.?\d{0,2})', text, re.IGNORECASE)
    if sub_match:
        result["subtotal"] = float(sub_match.group(1).replace(",", ""))
    else:
        result["subtotal"] = sum(it.get("extended", 0) for it in items_found)
    
    tax_match = re.search(r'SALES\s*TAX\s*\n?\s*\$?\s*([\d,]+\.?\d{0,2})', text, re.IGNORECASE)
    if tax_match:
        result["tax"] = float(tax_match.group(1).replace(",", ""))
    
    log.info("PO PDF parsed: po=%s, agency=%s, institution=%s, items=%d, total=$%.2f",
             result["po_number"], result["agency"], result.get("institution", ""),
             len(items_found), result["total"])
    return result


def is_purchase_order_email(subject, body, sender, pdf_names):
    """
    Detect incoming Purchase Order / Award emails.
    Fires BEFORE reply and RFQ detection to catch POs in existing threads.
    
    Returns dict with classification or None if not a PO.
    """
    subj_lower = subject.lower()
    body_lower = (body or "").lower()[:3000]
    combined = f"{subj_lower} {body_lower}"
    signals = []
    
    # 1. Subject match — strongest signal
    for kw in PO_STRONG_SUBJECT:
        if kw in subj_lower:
            signals.append(f"subject_kw:{kw}")
            break
    
    # 2. Body phrase match
    body_hits = 0
    for phrase in PO_BODY_PHRASES:
        if phrase in body_lower:
            body_hits += 1
            if body_hits <= 2:
                signals.append(f"body_phrase:{phrase}")
    
    # 3. PDF filename match
    for pdf in (pdf_names or []):
        pdf_low = pdf.lower()
        for pat in PO_PDF_PATTERNS:
            if re.search(pat, pdf_low):
                signals.append(f"pdf:{pdf}")
                break
    
    # 4. Sender is .gov or known buyer domain (boosts confidence)
    sender_email = ""
    if "<" in sender:
        sender_email = sender.split("<")[-1].split(">")[0].lower()
    else:
        sender_email = sender.lower()
    is_gov = any(d in sender_email for d in [".ca.gov", ".gov", "cdcr", "cchcs", "calvet", "cdph", "dsh"])
    if is_gov:
        signals.append("gov_sender")
    
    # Must NOT be a recall
    if "recall:" in subj_lower or "would like to recall" in combined:
        return None
    
    # Scoring: need at least subject match OR (body + pdf) OR (body + gov sender)
    has_subject = any(s.startswith("subject_kw") for s in signals)
    has_body = body_hits > 0
    has_pdf = any(s.startswith("pdf:") for s in signals)
    has_gov = "gov_sender" in signals
    
    if has_subject:
        pass  # Subject match alone is sufficient
    elif has_body and (has_pdf or has_gov):
        pass  # Body phrase + supporting evidence
    elif has_pdf and has_gov:
        pass  # PO PDF from gov sender
    else:
        return None
    
    # Extract PO number
    po_number = None
    po_patterns = [
        r'(?:purchase\s*order|p\.?o\.?)\s*#?\s*(\d[\w\-]{3,20})',
        r'(?:po\s*number|po#|po\s*#)\s*:?\s*(\d[\w\-]{3,20})',
        r'std\s*65\s*#?\s*(\d[\w\-]{3,20})',
        r'po\s*distribution\s*:?\s*(\d{7,13})',           # "PO Distribution: 4500750017"
        r'fi\$cal.*?(\d{7,13})',                           # "Fi$cal ... 4500750017"
        r'encumbrance.*?(\d{7,13})',                       # "Encumbrance 4500750017"
    ]
    for pat in po_patterns:
        m = re.search(pat, combined, re.IGNORECASE)
        if m:
            po_number = m.group(1)
            break
    
    log.info("🏆 PO/Award detected: subject='%s' signals=%s po=%s", subject[:60], signals, po_number)
    return {
        "is_po": True,
        "signals": signals,
        "po_number": po_number,
        "sender_email": sender_email,
        "confidence": "high" if has_subject else "medium",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Reply / Follow-Up Detection — must fire BEFORE is_rfq_email()
# ═══════════════════════════════════════════════════════════════════════════════

# Conversational reply indicators — buyer confirming, clarifying, or asking about
# an EXISTING PC/RFQ thread, not submitting a new one.
REPLY_BODY_PATTERNS = [
    # Confirmations
    r"(?:yes|yeah|correct|confirmed?|that(?:'s| is) (?:correct|right|it))",
    r"(?:go\s+(?:ahead|with)|sounds?\s+good|works?\s+for (?:me|us)|approved?)",
    r"(?:please\s+)?proceed",
    # Clarifications
    r"(?:to clarify|just to confirm|clarification|clarifying|fyi|for your info)",
    r"(?:i|we) (?:meant|mean|need|want|prefer|would like)\b",
    r"(?:the correct|the right|the actual) (?:item|part|product|quantity|color|size|spec)",
    r"(?:instead of|rather than|not the .+? but the)",
    r"(?:should be|needs to be|it(?:'s| is) actually)",
    # Quick answers / short responses
    r"^(?:yes|no|correct|will do|ok|okay|sure|thanks|thank you|got it|noted)[\.\!\s]*$",
    # Questions about existing request
    r"(?:did you|have you|can you).{0,40}(?:receive|get|see|process|send|ship|quote)",
    r"(?:any update|update on|status (?:of|on)|following up|checking (?:in|on))",
    r"(?:when (?:can|will|would)|how (?:soon|long|quickly))\b",
    # Attachments that are supporting docs, not new RFQs
    r"(?:attached|here(?:'s| is)|see attached|sending).{0,40}(?:spec|photo|picture|image|catalog|detail)",
]

REPLY_PATTERNS_COMPILED = [re.compile(p, re.I | re.M) for p in REPLY_BODY_PATTERNS]


def _extract_email_addr(sender_str):
    """Pull bare email from 'Name <email@example.com>' format."""
    m = re.search(r'[\w.+-]+@[\w.-]+\.\w+', sender_str or "")
    return m.group(0).lower() if m else ""


def _sender_has_active_item(sender_email):
    """Check if this sender has any active PC, RFQ, or sent quote.
    Returns dict with match info or None."""
    if not sender_email:
        return None
    try:
        from src.core.db import get_db, DB_PATH
        import sqlite3
        conn = sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # Check quotes — any sent/pending to this email
        q_rows = conn.execute(
            "SELECT quote_number, status, requestor, contact_email FROM quotes "
            "WHERE (lower(contact_email) = ? OR lower(requestor) LIKE ?) "
            "AND status IN ('sent','pending','draft') AND is_test=0 "
            "ORDER BY created_at DESC LIMIT 3",
            (sender_email, f"%{sender_email}%")
        ).fetchall()

        # Check RFQs — from rfqs.json (not in SQLite yet typically)
        rfq_match = None
        try:
            rfqs_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                "data", "rfqs.json")
            if os.path.exists(rfqs_path):
                with open(rfqs_path) as f:
                    rfqs = json.load(f)
                for rid, rfq in rfqs.items():
                    rfq_email = (rfq.get("contact_email") or rfq.get("requestor") or "").lower()
                    if sender_email in rfq_email or rfq_email in sender_email:
                        if rfq.get("status", "").lower() in ("new", "pending", "auto_drafted", "in_progress"):
                            rfq_match = {"rfq_id": rid, "status": rfq.get("status"), "sol": rfq.get("solicitation_number", "")}
                            break
        except Exception:
            pass

        # Check price checks
        pc_rows = conn.execute(
            "SELECT pc_number, status, requestor FROM price_checks "
            "WHERE (lower(requestor) LIKE ? OR lower(contact_email) LIKE ?) "
            "AND status NOT IN ('completed','cancelled','closed') AND is_test=0 "
            "ORDER BY created_at DESC LIMIT 3",
            (f"%{sender_email}%", f"%{sender_email}%")
        ).fetchall()
        conn.close()

        matches = []
        for r in q_rows:
            matches.append({"type": "quote", "ref": r["quote_number"], "status": r["status"]})
        if rfq_match:
            matches.append({"type": "rfq", **rfq_match})
        for r in pc_rows:
            matches.append({"type": "pc", "ref": r["pc_number"], "status": r["status"]})

        return matches if matches else None
    except Exception as e:
        log.debug("_sender_has_active_item error: %s", e)
        return None


def is_recall_email(subject, body):
    """Detect Outlook/Exchange recall requests.
    
    Pattern: Subject starts with 'Recall:' and body contains 'would like to recall'.
    Returns the original subject being recalled, or None if not a recall.
    
    Examples:
      Subject: "Recall: Quote - Med OS - 02.17.26"
      Body: 'Demidenko, Valentina@CDCR would like to recall the message, "Quote - Med OS - 02.17.26".'
    """
    subj = (subject or "").strip()
    body_text = (body or "")[:500].lower()
    
    # Must have Recall: prefix
    recall_match = re.match(r'^Recall:\s*(.+)', subj, re.IGNORECASE)
    if not recall_match:
        return None
    
    original_subject = recall_match.group(1).strip()
    
    # Body confirmation (optional but strengthens detection)
    if "would like to recall" in body_text or "recall the message" in body_text:
        log.info("📨 Recall email detected: original subject = '%s'", original_subject)
        return original_subject
    
    # Even without body match, Recall: prefix is strong enough
    log.info("📨 Recall email detected (subject only): original subject = '%s'", original_subject)
    return original_subject


def handle_recall(original_subject):
    """Process a recall: find matching PCs and delete them + free quote numbers.
    
    Matches by comparing the original recalled subject against PC numbers and
    filenames in existing price checks.
    
    Returns list of deleted PC IDs.
    """
    deleted = []
    try:
        from src.api.dashboard import _load_price_checks, _save_price_checks
        pcs = _load_price_checks()
        
        # Normalize the recalled subject for fuzzy matching
        # "Quote - Med OS - 02.17.26" → extract the PC identifier part
        recall_clean = original_subject.lower().strip()
        # Remove common prefixes: "Quote - ", "Quote request - "
        for prefix in ["quote request - ", "quote - ", "price check - ", "pc - "]:
            if recall_clean.startswith(prefix):
                recall_clean = recall_clean[len(prefix):]
                break
        recall_clean = recall_clean.strip()
        
        if not recall_clean:
            log.warning("Recall: could not extract identifier from '%s'", original_subject)
            return deleted
        
        # Find matching PCs
        to_delete = []
        for pcid, pc in pcs.items():
            pc_num = (pc.get("pc_number") or "").lower().strip()
            # Direct match on PC number
            if recall_clean and recall_clean in pc_num:
                to_delete.append(pcid)
                continue
            # Match on source PDF filename
            source = (pc.get("source_pdf") or "").lower()
            if recall_clean and recall_clean in source:
                to_delete.append(pcid)
                continue
            # Match on original email subject
            email_subject = (pc.get("email_subject") or "").lower()
            if recall_clean and recall_clean in email_subject:
                to_delete.append(pcid)
                continue
        
        if not to_delete:
            log.info("Recall: no matching PCs found for '%s'", original_subject)
            return deleted
        
        # Delete each matching PC + cascade (quote, counter)
        for pcid in to_delete:
            result = _delete_price_check_cascade(pcid, pcs, reason=f"recalled: {original_subject}")
            if result:
                deleted.append(result)
        
        # Save updated PCs
        _save_price_checks(pcs)
        
        # Recalculate counter after all deletes
        _recalc_quote_counter()
        
        log.info("📨 Recall processed: deleted %d PCs for '%s': %s",
                 len(deleted), original_subject, [d["pcid"] for d in deleted])
        
        # Also clean matching RFQs
        try:
            from src.api.dashboard import load_rfqs, save_rfqs
            rfqs = load_rfqs()
            rfq_deleted = []
            for rid in list(rfqs.keys()):
                r = rfqs[rid]
                searchable = f"{r.get('requestor','')} {r.get('email_subject','')} {r.get('solicitation','')}".lower()
                if recall_clean and recall_clean in searchable:
                    rfq_deleted.append(rid)
                    del rfqs[rid]
            if rfq_deleted:
                save_rfqs(rfqs)
                log.info("📨 Recall also removed %d RFQs: %s", len(rfq_deleted), rfq_deleted)
        except Exception as e2:
            log.warning("Recall RFQ cleanup error: %s", e2)
        
    except Exception as e:
        log.error("Recall handling error: %s", e, exc_info=True)
    
    return deleted


def _delete_price_check_cascade(pcid, pcs_dict, reason=""):
    """Delete a PC from dict + SQLite + linked draft quote. 
    Does NOT save pcs_dict or recalc counter (caller handles batch).
    Returns info dict or None.
    """
    if pcid not in pcs_dict:
        return None
    
    pc = pcs_dict[pcid]
    pc_num = pc.get("pc_number", pcid)
    linked_qn = pc.get("reytech_quote_number", "") or pc.get("linked_quote_number", "")
    
    # Remove from dict
    del pcs_dict[pcid]
    
    # Remove from SQLite
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM price_checks WHERE id=?", (pcid,))
    except Exception as e:
        log.debug("SQLite PC delete: %s", e)
    
    # Remove linked draft quote
    quote_removed = False
    if linked_qn:
        try:
            from src.forms.quote_generator import get_all_quotes, _save_all_quotes
            all_quotes = get_all_quotes()
            before = len(all_quotes)
            all_quotes = [q for q in all_quotes
                          if not (q.get("quote_number") == linked_qn
                                  and q.get("status") in ("draft", "pending"))]
            if len(all_quotes) < before:
                _save_all_quotes(all_quotes)
                quote_removed = True
                try:
                    from src.core.db import get_db
                    with get_db() as conn:
                        conn.execute("DELETE FROM quotes WHERE quote_number=? AND status IN ('draft','pending')", (linked_qn,))
                except Exception:
                    pass
        except Exception as e:
            log.debug("Quote cleanup: %s", e)
    
    log.info("DELETED PC %s (%s) — %s%s", pcid, pc_num, reason,
             f" + quote {linked_qn}" if quote_removed else "")
    
    return {
        "pcid": pcid,
        "pc_number": pc_num,
        "quote_removed": linked_qn if quote_removed else None,
    }


def _recalc_quote_counter():
    """Recalculate quote counter to highest remaining quote number."""
    try:
        from src.forms.quote_generator import get_all_quotes, _load_counter, _save_counter
        from src.api.dashboard import _load_price_checks
        all_quotes = get_all_quotes()
        max_seq = 0
        for q in all_quotes:
            qn = q.get("quote_number", "")
            m = re.search(r'R\d{2}Q(\d+)', qn)
            if m and not q.get("is_test"):
                max_seq = max(max_seq, int(m.group(1)))
        for rpc in _load_price_checks().values():
            qn = rpc.get("reytech_quote_number", "") or ""
            m = re.search(r'R\d{2}Q(\d+)', qn)
            if m:
                max_seq = max(max_seq, int(m.group(1)))
        old = _load_counter()
        if max_seq < old.get("seq", 0):
            _save_counter({"year": old.get("year", 2026), "seq": max_seq})
            log.info("Quote counter reset: Q%d → Q%d", old["seq"], max_seq)
    except Exception as e:
        log.debug("Counter recalc: %s", e)


def is_reply_followup(msg, subject, body, sender, pdf_names):
    """Detect if this email is a REPLY/FOLLOW-UP to an existing thread,
    not a new RFQ submission.

    Returns dict with classification or None if it's genuinely new.
    This MUST fire before is_rfq_email() to prevent pipeline pollution.

    Logic:
      1. Must have reply indicators (Re: subject, In-Reply-To header, References header)
      2. Sender must match an existing active PC/RFQ/quote
      3. Must NOT carry new RFQ form attachments (704A/704B/703B)
      4. Body should be conversational (short or matches reply patterns)

    If ALL conditions met → route to CS agent, not PC/RFQ queue.
    """
    # ── Step 1: Reply indicators ──
    is_reply = False
    reply_signals = []

    # Check headers
    in_reply_to = msg.get("In-Reply-To", "") if msg else ""
    references = msg.get("References", "") if msg else ""
    if in_reply_to:
        is_reply = True
        reply_signals.append("In-Reply-To header present")
    if references:
        is_reply = True
        reply_signals.append("References header present")

    # Check subject prefix
    subj_clean = (subject or "").strip()
    if re.match(r'^(?:Re|RE|re|Fwd|FW|fw)\s*:\s*', subj_clean):
        is_reply = True
        reply_signals.append(f"Subject starts with reply/forward prefix")

    if not is_reply:
        return None  # Not a reply — let is_rfq_email() handle it

    # ── Step 2: Check for attachments ──
    # If the reply carries ANY PDF attachments, it's a new submission
    # (buyer sending a new PC/RFQ even if replying to old thread).
    # Follow-ups/clarifications never carry PDF attachments.
    if pdf_names:
        pdf_count = len(pdf_names)
        log.info("Reply has %d PDF attachment(s) — treating as NEW submission: %s",
                 pdf_count, subject[:60])
        return None  # Let is_rfq_email() process it

    # ── Step 3: Sender has active item? ──
    sender_email = _extract_email_addr(sender)
    active_items = _sender_has_active_item(sender_email)

    if not active_items:
        # Reply thread but unknown sender — could be a new buyer replying to a
        # forwarded RFQ. Let is_rfq_email() decide.
        log.debug("Reply from unknown sender %s — passing to is_rfq_email()", sender_email)
        return None

    # ── Step 4: Body analysis — conversational vs. new request ──
    body_text = (body or "")[:1500]
    body_score = 0

    # Short body = almost certainly a reply (full RFQs are long)
    body_lines = [l.strip() for l in body_text.split("\n") if l.strip() and not l.strip().startswith(">")]
    original_body = "\n".join(body_lines)
    if len(original_body) < 300:
        body_score += 3

    # Pattern match for conversational content
    for pat in REPLY_PATTERNS_COMPILED:
        if pat.search(original_body):
            body_score += 2
            break

    # No PDFs at all = very likely a reply
    if not pdf_names:
        body_score += 2

    # If body contains strong NEW RFQ indicators, override
    combined = f"{subject} {original_body}".lower()
    new_rfq_signals = ["solicitation", "bid package", "quote worksheet", "informal competitive", "acquisition quote"]
    for sig in new_rfq_signals:
        # Only count if it's NOT in the quoted/forwarded part
        if sig in original_body.lower() and not any(l.strip().startswith(">") for l in body_text.split("\n") if sig in l.lower()):
            body_score -= 3
            break

    if body_score < 2:
        log.debug("Reply body score too low (%d) — passing to is_rfq_email(): %s", body_score, subject[:60])
        return None

    # ── All checks passed: this is a follow-up, not a new RFQ ──
    result = {
        "is_followup": True,
        "sender_email": sender_email,
        "reply_signals": reply_signals,
        "active_items": active_items,
        "body_score": body_score,
        "subject": subject,
    }
    log.info("🔄 FOLLOW-UP detected (not new RFQ): sender=%s items=%s signals=%s score=%d subj='%s'",
             sender_email, [i.get("ref") for i in active_items[:3]],
             reply_signals, body_score, subject[:60])
    return result


def is_price_check_email(subject, body, sender, pdf_names):
    """Detect if this email is a Price Check (AMS 704, no B).
    
    Fires BEFORE is_rfq_email() to route PCs directly to the PC queue
    instead of accidentally creating broken RFQs.
    
    Returns dict with classification or None if not a PC.
    """
    subj_lower = (subject or "").strip().lower()
    sender_lower = (sender or "").lower()
    combined = f"{subject} {body}".lower()
    
    signals = []
    score = 0
    
    # ── Signal 1: Known PC sender ──
    sender_email = _extract_email_addr(sender).lower()
    for pattern in PC_KNOWN_SENDERS:
        if pattern in sender_email or pattern in sender_lower:
            signals.append(f"known_sender:{pattern}")
            score += 3
            break
    
    # ── Signal 2: Subject matches PC pattern ──
    for pat in PC_SUBJECT_PATTERNS:
        if re.match(pat, subj_lower):
            signals.append(f"subject_pattern:{pat}")
            score += 3
            break
    
    # ── Signal 3: PDF filename contains "704" but NOT "704b" ──
    for pdf in pdf_names:
        pl = pdf.lower()
        if "704" in pl and "704b" not in pl:
            if "ams" in pl or "quote" in combined:
                signals.append(f"pdf_ams704:{pdf}")
                score += 4
                break
    
    # ── Signal 4: Body contains PC-like phrases ──
    pc_phrases = ["please email me a quote", "price your response",
                  "price check", "please quote", "email me a quote",
                  "attached request", "attached items"]
    for phrase in pc_phrases:
        if phrase in combined:
            signals.append(f"body_phrase:{phrase}")
            score += 1
            break
    
    # ── Negative: Has 703B/704B/Bid Package forms → NOT a PC ──
    for pdf in pdf_names:
        pl = pdf.lower()
        if "703b" in pl or "704b" in pl or "bid package" in pl or "bid_package" in pl:
            log.debug("PC check: has RFQ form (%s) → not a PC", pdf)
            return None
    
    # Threshold: need at least 4 points to be confident it's a PC
    if score >= 4:
        log.info("📋 PRICE CHECK detected (score=%d signals=%s): %s",
                 score, signals, subject[:60])
        return {
            "is_price_check": True,
            "score": score,
            "signals": signals,
            "sender_email": sender_email,
        }
    
    return None


def is_rfq_email(subject, body, attachments):
    """
    Determine if an email is an RFQ. Uses tiered detection:
    1. Strong keyword match in subject/body → definitely RFQ
    2. PDF attachments with RFQ-like filenames → likely RFQ
    3. Any email with 2+ PDF attachments → probable RFQ (dedicated inbox)
    4. Forwarded email with PDF → probable RFQ
    5. Single PDF in dedicated inbox → still probably RFQ
    """
    combined = f"{subject} {body}".lower()
    
    # Guard: recall emails are NOT RFQs even if they contain keywords like "cdcr"
    if subject.lower().startswith("recall:") or "would like to recall" in combined:
        return False
    
    # Tier 1: Strong keyword match
    if any(kw in combined for kw in RFQ_STRONG):
        log.info(f"RFQ detected (keyword match): {subject[:60]}")
        return True
    
    # Tier 2: PDF filenames look like RFQ forms
    pdf_names = [a.lower().replace(" ", ".").replace("-", ".") for a in attachments]
    for name in pdf_names:
        if any(re.search(p, name) for p in RFQ_PDF_PATTERNS):
            log.info(f"RFQ detected (PDF filename match): {subject[:60]}")
            return True
    
    # Tier 3: Multiple PDFs = likely RFQ (this is a dedicated RFQ inbox)
    if len(attachments) >= 2:
        log.info(f"RFQ detected (multiple PDFs in dedicated inbox): {subject[:60]}")
        return True
    
    # Tier 4: Forwarded email with any PDF attachment
    fwd_indicators = ["fwd:", "fw:", "forwarded", "---------- forwarded"]
    if any(ind in combined for ind in fwd_indicators) and len(attachments) >= 1:
        log.info(f"RFQ detected (forwarded with PDF): {subject[:60]}")
        return True
    
    # Tier 5: Single PDF in dedicated inbox — still probably an RFQ
    if len(attachments) >= 1:
        log.info(f"RFQ detected (PDF in dedicated inbox): {subject[:60]}")
        return True
    
    log.debug(f"Skipped (no PDFs, no keywords): {subject[:60]}")
    return False


def extract_solicitation_number(subject, body, attachments=None):
    """Extract solicitation number from subject, body, or filenames. CCHCS uses 7-8 digit numbers."""
    combined = f"{subject} {body}"
    
    # Look for explicit "solicitation #12345678" patterns first
    explicit = re.search(r'(?:solicitation|sol\.?)\s*#?\s*(\d{7,8})', combined, re.IGNORECASE)
    if explicit:
        return explicit.group(1)
    
    # Look for 7-8 digit numbers near RFQ keywords
    for kw in ["rfq", "solicitation", "703b", "704b", "bid"]:
        idx = combined.lower().find(kw)
        if idx >= 0:
            nearby = combined[max(0, idx-50):idx+100]
            match = re.search(r'(\d{7,8})', nearby)
            if match:
                return match.group(1)
    
    # Check PDF filenames
    if attachments:
        for att_name in attachments:
            match = re.search(r'(\d{7,8})', att_name)
            if match:
                return match.group(1)
    
    # Fallback: any 7-8 digit number in subject
    match = re.search(r'(\d{7,8})', subject)
    if match:
        return match.group(1)
    
    # Last resort: any 7-8 digit number anywhere
    match = re.search(r'(\d{7,8})', combined)
    if match:
        return match.group(1)
    
    return "unknown"


# ═══════════════════════════════════════════════════════════════════════════════
# Email Poller
# ═══════════════════════════════════════════════════════════════════════════════

class EmailPoller:
    def __init__(self, config):
        self.host = config.get("imap_host", "imap.gmail.com")
        self.port = config.get("imap_port", 993)
        self.email_addr = config.get("email", os.environ.get("GMAIL_ADDRESS", ""))
        self.password = config.get("email_password", os.environ.get("GMAIL_PASSWORD", ""))
        self.folder = config.get("imap_folder", "INBOX")
        self.processed_file = config.get("processed_file", "data/processed_emails.json")
        self._processed = self._load_processed()
        self.mail = None
        self._connected = False

    def _load_processed(self):
        if os.path.exists(self.processed_file):
            try:
                with open(self.processed_file) as f:
                    return set(json.load(f))
            except (json.JSONDecodeError, IOError):
                log.warning("Corrupt processed_emails.json — starting fresh")
                return set()
        return set()

    def _save_processed(self):
        d = os.path.dirname(self.processed_file)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(self.processed_file, "w") as f:
            json.dump(list(self._processed), f)

    def connect(self):
        """Connect to IMAP server. Returns True on success."""
        try:
            if self.mail and self._connected:
                try:
                    self.mail.noop()
                    return True
                except Exception:
                    self._connected = False
            
            self.mail = imaplib.IMAP4_SSL(self.host, self.port)
            self.mail.login(self.email_addr, self.password)
            self.mail.select(self.folder)
            self._connected = True
            log.info(f"Connected to {self.host} as {self.email_addr}")
            return True
        except imaplib.IMAP4.error as e:
            log.error(f"IMAP auth failed: {e}")
            self._connected = False
            return False
        except Exception as e:
            log.error(f"IMAP connection failed: {e}")
            self._connected = False
            return False

    def check_for_rfqs(self, save_dir="uploads"):
        """Check inbox for new RFQ emails. Returns list of parsed RFQ dicts.
        Uses UID tracking + date search so Gmail read status doesn't matter.
        Uses BODY.PEEK[] to avoid marking emails as read.
        """
        results = []
        
        try:
            # Search last 3 days by date — doesn't matter if read or unread
            since_date = (datetime.now() - timedelta(days=3)).strftime("%d-%b-%Y")
            status, messages = self.mail.uid("search", None, f"(SINCE {since_date})")
            if status != "OK":
                log.warning(f"IMAP UID search failed: {status}")
                return results

            uids = messages[0].split() if messages[0] else []
            new_uids = [u for u in uids if u.decode() not in self._processed]
            if uids:
                log.info(f"Found {len(uids)} emails from last 3 days, {len(new_uids)} new to process")
            
            # Diagnostic counters
            self._diag = {
                "total_uids": len(uids),
                "new_uids": len(new_uids),
                "recalled": 0,
                "followup": 0,
                "not_rfq": 0,
                "rfq_captured": 0,
                "no_attachments": 0,
                "parse_errors": 0,
                "subjects_seen": [],
                "_po_numbers_seen": set(),  # Dedup PO within same poll cycle
            }

            for uid_bytes in new_uids:
                uid = uid_bytes.decode()

                try:
                    # BODY.PEEK[] = fetch without marking as read
                    status, data = self.mail.uid("fetch", uid_bytes, "(BODY.PEEK[])")
                    if status != "OK":
                        continue

                    msg = email.message_from_bytes(data[0][1])
                    
                    subject = self._decode_header(msg["Subject"]) or ""
                    sender = self._decode_header(msg["From"]) or ""
                    body = self._get_body(msg)
                    
                    # Track for diagnostics
                    self._diag["subjects_seen"].append(subject[:60])
                    
                    # ── SELF-EMAIL FILTER — skip our own sent emails ──────────
                    # Gmail threads sent replies into INBOX view. The poller must
                    # never process emails FROM our own address.
                    sender_email_raw = self._extract_email(sender).lower()
                    our_email = self.email_addr.lower()
                    our_domains = ["reytechinc.com", "reytech.com"]
                    is_self = (
                        sender_email_raw == our_email
                        or any(sender_email_raw.endswith(f"@{d}") for d in our_domains)
                    )
                    if is_self:
                        log.debug("Skipping own email: %s — %s", sender_email_raw, subject[:50])
                        self._diag.setdefault("self_skipped", 0)
                        self._diag["self_skipped"] = self._diag.get("self_skipped", 0) + 1
                        self._processed.add(uid)
                        continue
                    # ── END SELF-EMAIL FILTER ──────────────────────────────────
                    
                    # Get PDF names without saving
                    pdf_names = self._get_pdf_names(msg)
                    
                    # ── RECALL DETECTION — fires FIRST ─────────────────────
                    # Outlook/Exchange recall requests delete the original PC
                    # and free the quote number for reuse.
                    recalled_subject = is_recall_email(subject, body)
                    if recalled_subject:
                        log.info("📨 Processing recall: '%s' from %s", recalled_subject, sender[:40])
                        deleted_pcs = handle_recall(recalled_subject)
                        if deleted_pcs:
                            try:
                                from src.agents.notify_agent import send_alert
                                deleted_nums = [d["pc_number"] for d in deleted_pcs]
                                freed_quotes = [d["quote_removed"] for d in deleted_pcs if d.get("quote_removed")]
                                send_alert(
                                    event_type="pc_recalled",
                                    title=f"📨 PC recalled: {', '.join(deleted_nums)}",
                                    body=f"Recall from {sender[:40]}. Deleted {len(deleted_pcs)} PC(s). "
                                         f"Quote numbers freed: {', '.join(freed_quotes) or 'none'}.",
                                    urgency="normal",
                                    cooldown_key=f"recall_{uid}",
                                )
                            except Exception:
                                pass
                        self._processed.add(uid)
                        self._diag["recalled"] += 1
                        continue
                    # ── END RECALL DETECTION ──────────────────────────────────

                    # ── PO / AWARD DETECTION — fires BEFORE reply + RFQ ───────
                    # A Purchase Order email in a reply thread is still a PO.
                    # Auto-marks quote as Won and triggers vendor ordering.
                    po_detect = is_purchase_order_email(subject, body, sender, pdf_names)
                    if po_detect:
                        log.info("🏆 PO/Award email: %s from %s", subject[:60], sender[:40])
                        po_number = po_detect.get("po_number", "")
                        sol_number = extract_solicitation_number(subject, body or "", pdf_names)
                        
                        # Special handling for "PO Distribution: PO#, SOL#, FACILITY, VENDOR" format
                        po_dist_match = re.match(
                            r'(?:re:\s*)?po\s*distribution\s*:?\s*(\d+)\s*,\s*(\d{7,8})',
                            subject, re.IGNORECASE
                        )
                        if po_dist_match:
                            sol_number = po_dist_match.group(2)
                            if not po_number:
                                po_number = po_dist_match.group(1)
                        
                        # ── DEDUP: skip if this PO number already seen this cycle ──
                        if po_number and po_number in self._diag.get("_po_numbers_seen", set()):
                            log.info("PO %s already processed this cycle (RE: thread), skipping", po_number)
                            self._processed.add(uid)
                            continue
                        if po_number:
                            self._diag.setdefault("_po_numbers_seen", set()).add(po_number)
                        
                        # Save attachments for PO records
                        po_rfq_id = "PO_" + datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uid[:6]
                        po_dir = os.path.join(save_dir, po_rfq_id)
                        os.makedirs(po_dir, exist_ok=True)
                        po_attachments = self._save_attachments(msg, po_dir)
                        po_attachments.extend(self._extract_forwarded_attachments(msg, po_dir))
                        
                        # Try to match to existing quote/RFQ and mark as won
                        matched_quote = None
                        po_items = []
                        po_total = 0
                        po_agency = ""
                        po_institution = ""
                        try:
                            from src.core.paths import DATA_DIR
                            import json as _json
                            
                            # Parse PO PDF for line items if we have attachments
                            if po_attachments:
                                try:
                                    po_parsed = _parse_po_pdf(po_attachments[0])
                                    if po_parsed:
                                        po_items = po_parsed.get("items", [])
                                        po_total = po_parsed.get("total", 0)
                                        po_agency = po_parsed.get("agency", "")
                                        po_institution = po_parsed.get("institution", "")
                                        if not po_number and po_parsed.get("po_number"):
                                            po_number = po_parsed["po_number"]
                                        log.info("PO PDF parsed: %d items, $%.2f, PO#%s",
                                                 len(po_items), po_total, po_number)
                                except Exception as _ppe:
                                    log.debug("PO PDF parse error: %s", _ppe)
                            
                            # 1. Match by solicitation number to RFQ → quote
                            if sol_number:
                                rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
                                try:
                                    with open(rfqs_path) as _rf:
                                        _rfqs = _json.load(_rf)
                                    for rid, rfq in (_rfqs.items() if isinstance(_rfqs, dict) else []):
                                        if rfq.get("solicitation_number") == sol_number:
                                            matched_quote = rfq.get("reytech_quote_number", "")
                                            log.info("PO matched to RFQ sol#%s → quote %s", sol_number, matched_quote)
                                            break
                                except (FileNotFoundError, _json.JSONDecodeError):
                                    pass
                            
                            # 2. Also check quotes_log directly by solicitation
                            if not matched_quote:
                                quotes_path = os.path.join(DATA_DIR, "quotes_log.json")
                                try:
                                    with open(quotes_path) as _qf:
                                        _quotes = _json.load(_qf)
                                    for q in (_quotes if isinstance(_quotes, list) else []):
                                        q_sol = q.get("solicitation_number", "") or q.get("sol", "")
                                        if sol_number and q_sol == sol_number:
                                            matched_quote = q.get("quote_number", "")
                                            log.info("PO matched to quote %s via sol#%s", matched_quote, sol_number)
                                            break
                                except (FileNotFoundError, _json.JSONDecodeError):
                                    pass
                            
                            # 3. Mark quote as won + trigger vendor ordering
                            _all_quotes = []
                            if matched_quote:
                                log.info("🏆 Auto-marking quote %s as WON (PO: %s)", matched_quote, po_number)
                                # Update quotes_log.json
                                try:
                                    with open(os.path.join(DATA_DIR, "quotes_log.json")) as _qf2:
                                        _all_quotes = _json.load(_qf2)
                                    for q in _all_quotes:
                                        if q.get("quote_number") == matched_quote:
                                            q["status"] = "won"
                                            q["won_at"] = datetime.now().isoformat()
                                            q["po_number"] = po_number or ""
                                            q["won_source"] = "email_auto"
                                            # Get items from quote if PO parse didn't yield any
                                            if not po_items:
                                                po_items = q.get("items_detail", q.get("items", []))
                                            if not po_total:
                                                po_total = q.get("total", 0)
                                            if not po_agency:
                                                po_agency = q.get("agency", "")
                                            if not po_institution:
                                                po_institution = q.get("institution", "") or q.get("ship_to_name", "")
                                            break
                                    with open(os.path.join(DATA_DIR, "quotes_log.json"), "w") as _qf3:
                                        _json.dump(_all_quotes, _qf3, indent=2, default=str)
                                    log.info("Quote %s marked WON in quotes_log.json", matched_quote)
                                except Exception as _qe:
                                    log.error("Failed to update quote status: %s", _qe)
                                
                                # Update SQLite too
                                try:
                                    from src.core.db_dal import update_quote_status
                                    update_quote_status(matched_quote, "won")
                                except Exception:
                                    pass
                                
                                # Trigger vendor ordering pipeline
                                try:
                                    from src.agents.vendor_ordering_agent import process_won_quote_ordering
                                    if po_items:
                                        process_won_quote_ordering(
                                            quote_number=matched_quote,
                                            items=po_items,
                                            agency=po_agency,
                                            po_number=po_number or "",
                                            run_async=True,
                                        )
                                        log.info("Vendor ordering triggered for %s", matched_quote)
                                except Exception as _voe:
                                    log.debug("Vendor ordering trigger: %s", _voe)
                                
                                # Log revenue
                                try:
                                    from src.core.db_dal import log_revenue
                                    if po_total:
                                        log_revenue(
                                            amount=po_total,
                                            source="quote_won",
                                            quote_number=matched_quote,
                                            po_number=po_number or "",
                                            agency=po_agency,
                                            date=datetime.now().strftime("%Y-%m-%d"),
                                        )
                                except Exception:
                                    pass
                            
                            # 4. Create order record (whether or not we matched a quote)
                            try:
                                from src.api.dashboard import _create_order_from_po_email
                                order_data = {
                                    "po_number": po_number or "",
                                    "sender_email": po_detect.get("sender_email", ""),
                                    "subject": subject,
                                    "sol_number": sol_number or "",
                                    "items": po_items,
                                    "total": po_total,
                                    "agency": po_agency,
                                    "institution": po_institution,
                                    "po_pdf_path": po_attachments[0] if po_attachments else "",
                                    "matched_quote": matched_quote or "",
                                }
                                new_order = _create_order_from_po_email(order_data)
                                log.info("Order created: %s", new_order.get("order_id", ""))
                            except Exception as _oe:
                                log.error("Order creation from PO email failed: %s", _oe)
                            
                            # 5. Update pricing intelligence
                            try:
                                from src.core.db_dal import record_price
                                for it in po_items:
                                    desc = it.get("description", "")
                                    up = it.get("unit_price", 0)
                                    if desc and up:
                                        record_price(desc, up, source="po_won", quote_number=matched_quote or po_number or "")
                            except Exception:
                                pass
                                
                        except Exception as _pe:
                            log.error("PO processing error: %s", _pe)
                        
                        # Notify Mike
                        try:
                            from src.agents.notify_agent import send_alert
                            title = f"🏆 Purchase Order received!"
                            body_msg = f"PO: {po_number or 'see email'}"
                            if matched_quote:
                                body_msg += f" · Quote {matched_quote} auto-marked WON"
                                body_msg += " · Vendor ordering triggered"
                            else:
                                body_msg += f" · Sol: {sol_number or 'unknown'}"
                                body_msg += " · ⚠️ Could not match to existing quote — review manually"
                            body_msg += f"\nFrom: {po_detect.get('sender_email','')}"
                            body_msg += f"\nSubject: {subject[:80]}"
                            send_alert(
                                event_type="po_received",
                                title=title,
                                body=body_msg,
                                urgency="deal",
                                cooldown_key=f"po_{uid}",
                            )
                        except Exception:
                            pass
                        
                        # Log to activity
                        try:
                            from src.core.db_dal import log_activity
                            log_activity(
                                event_type="po_received",
                                ref_type="quote",
                                ref_id=matched_quote or sol_number or "",
                                detail=f"PO {po_number or '?'} from {po_detect.get('sender_email','')}. "
                                       f"{'Auto-marked ' + matched_quote + ' as WON.' if matched_quote else 'No matching quote found.'}",
                            )
                        except Exception:
                            pass
                        
                        # Auto reply-all: confirm PO receipt
                        try:
                            send_po_confirmation_reply(msg, po_number)
                        except Exception as _rpe:
                            log.debug("PO confirmation reply error: %s", _rpe)
                        
                        self._processed.add(uid)
                        self._diag.setdefault("po_received", 0)
                        self._diag["po_received"] = self._diag.get("po_received", 0) + 1
                        continue
                    # ── END PO / AWARD DETECTION ──────────────────────────────

                    # ── REPLY DETECTION — fires BEFORE is_rfq_email() ──────────
                    # Prevents pipeline pollution from buyer follow-ups/clarifications
                    # being logged as new PCs/RFQs.
                    followup = is_reply_followup(msg, subject, body, sender, pdf_names)
                    if followup:
                        # Route to CS Agent with context about which item they're replying to
                        log.info("🔄 Routing follow-up to CS Agent (not PC/RFQ queue): %s", subject[:60])
                        try:
                            from src.agents.cs_agent import classify_inbound_email, build_cs_response_draft
                            cs_class = classify_inbound_email(subject, body, sender)
                            # Enrich with thread context even if CS patterns didn't match
                            cs_class["is_update_request"] = True
                            cs_class["is_followup"] = True
                            cs_class["linked_items"] = followup.get("active_items", [])
                            if not cs_class.get("intent"):
                                cs_class["intent"] = "followup_clarification"
                            def _cs_followup(cls=cs_class, subj=subject, bdy=body, snd=sender):
                                try:
                                    result = build_cs_response_draft(cls, subj, bdy, snd)
                                    log.info("CS follow-up draft: ok=%s intent=%s linked=%s draft_id=%s",
                                             result.get("ok"), cls.get("intent"),
                                             [i.get("ref") for i in cls.get("linked_items",[])[:2]],
                                             result.get("draft",{}).get("id",""))
                                except Exception as _ce:
                                    log.debug("CS follow-up draft error: %s", _ce)
                            import threading as _ft
                            _ft.Thread(target=_cs_followup, daemon=True, name="cs-followup").start()
                        except Exception as _fse:
                            log.debug("Follow-up CS routing error: %s", _fse)
                        # Notify about the follow-up (don't silently swallow it)
                        try:
                            from src.agents.notify_agent import send_alert
                            linked = followup.get("active_items", [])
                            ref_str = ", ".join(i.get("ref","?") for i in linked[:3])
                            send_alert(
                                event_type="buyer_followup",
                                title=f"📩 Buyer follow-up: {subject[:50]}",
                                body=f"From {followup.get('sender_email','')} re: {ref_str}. Routed to CS — check /outbox.",
                                urgency="normal",
                                cooldown_key=f"followup_{uid}",
                            )
                        except Exception:
                            pass
                        self._processed.add(uid)
                        self._diag["followup"] += 1
                        continue
                    # ── END REPLY DETECTION ────────────────────────────────────

                    # ── EARLY PC DETECTION — fires BEFORE is_rfq_email() ──────
                    # Catches known sender + subject patterns (Valentina "Quote - ..." emails)
                    # to route directly to PC queue, avoiding broken RFQ creation.
                    pc_detect = is_price_check_email(subject, body, sender, pdf_names)
                    if pc_detect:
                        log.info("📋 Routing to PC queue via early detection: %s", subject[:60])
                        # Save attachments for PC processing
                        pc_rfq_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uid[:6]
                        pc_rfq_dir = os.path.join(save_dir, pc_rfq_id)
                        os.makedirs(pc_rfq_dir, exist_ok=True)
                        pc_attachments = self._save_attachments(msg, pc_rfq_dir)
                        pc_attachments.extend(self._extract_forwarded_attachments(msg, pc_rfq_dir))
                        
                        # Build minimal rfq_email dict for process_rfq_email's PC path
                        pc_email_info = {
                            "id": pc_rfq_id,
                            "email_uid": uid,
                            "message_id": msg.get("Message-ID", ""),
                            "subject": subject,
                            "sender": sender,
                            "sender_email": self._extract_email(sender),
                            "date": msg.get("Date"),
                            "solicitation_hint": extract_solicitation_number(subject, body, pdf_names),
                            "attachments": pc_attachments,
                            "rfq_dir": pc_rfq_dir,
                            "body_preview": body[:500] if body else "",
                            "_pc_early_detect": True,  # Flag for process_rfq_email
                            "_pc_signals": pc_detect.get("signals", []),
                        }
                        results.append(pc_email_info)
                        self._processed.add(uid)
                        self._diag["rfq_captured"] += 1
                        log.info("PC captured (early): %s (%d PDFs)", subject[:60], len(pc_attachments))
                        continue
                    # ── END EARLY PC DETECTION ─────────────────────────────────

                    if not is_rfq_email(subject, body, pdf_names):
                        # Not an RFQ — classify what kind of email it is
                        email_handled = False

                        # Check if it's a shipping/tracking email
                        try:
                            from src.agents.predictive_intel import detect_shipping_email
                            ship_info = detect_shipping_email(subject, body, sender)
                            if ship_info.get("is_shipping") and ship_info.get("tracking_numbers"):
                                log.info("Shipping email detected: %s tracking=%s",
                                         subject[:60], ship_info["tracking_numbers"][:2])
                                _ship_file = os.path.join(os.path.dirname(os.path.dirname(
                                    os.path.dirname(os.path.abspath(__file__)))), "data", "detected_shipments.json")
                                try:
                                    with open(_ship_file) as _sf:
                                        _ships = json.load(_sf)
                                except (FileNotFoundError, json.JSONDecodeError):
                                    _ships = []
                                _ships.append({
                                    **ship_info,
                                    "subject": subject,
                                    "sender": sender,
                                    "detected_at": datetime.now().isoformat(),
                                })
                                if len(_ships) > 500:
                                    _ships = _ships[-500:]
                                with open(_ship_file, "w") as _sf:
                                    json.dump(_ships, _sf, indent=2, default=str)
                                
                                # Auto-update order tracking — find active orders with pending/ordered items
                                try:
                                    from src.api.dashboard import _load_orders, _save_orders, _update_order_status
                                    _orders = _load_orders()
                                    tracking_nums = ship_info.get("tracking_numbers", [])
                                    carrier = ship_info.get("carrier", "")
                                    updated_order = None
                                    for _oid, _ord in _orders.items():
                                        if _ord.get("status") in ("new", "sourcing", "shipped", "partial_delivery"):
                                            for _it in _ord.get("line_items", []):
                                                if _it.get("sourcing_status") in ("ordered", "pending") and not _it.get("tracking_number"):
                                                    # Assign tracking to first untracked item
                                                    if tracking_nums:
                                                        _it["tracking_number"] = tracking_nums[0]
                                                        _it["carrier"] = carrier
                                                        _it["sourcing_status"] = "shipped"
                                                        _it["ship_date"] = datetime.now().strftime("%Y-%m-%d")
                                                        updated_order = _oid
                                                        log.info("Auto-assigned tracking %s to order %s line %s",
                                                                 tracking_nums[0], _oid, _it.get("line_id"))
                                                        break
                                            if updated_order:
                                                _orders[_oid]["updated_at"] = datetime.now().isoformat()
                                                break
                                    if updated_order:
                                        _save_orders(_orders)
                                        _update_order_status(updated_order)
                                except Exception as _ote:
                                    log.debug("Shipping→order tracking update: %s", _ote)
                                
                                email_handled = True
                        except Exception as _e:
                            pass

                        # CS Agent: check for customer service requests
                        try:
                            from src.agents.cs_agent import classify_inbound_email, build_cs_response_draft
                            cs_class = classify_inbound_email(subject, body, sender)
                            if cs_class.get("is_update_request") or cs_class.get("intent") != "general":
                                log.info("CS request detected: intent=%s from=%s subject=%s",
                                         cs_class.get("intent"), sender[:40], subject[:50])
                                def _cs_draft(cls=cs_class, subj=subject, bdy=body, snd=sender):
                                    try:
                                        result = build_cs_response_draft(cls, subj, bdy, snd)
                                        log.info("CS auto-draft: ok=%s intent=%s draft_id=%s",
                                                 result.get("ok"), cls.get("intent"),
                                                 result.get("draft",{}).get("id",""))
                                    except Exception as _ce:
                                        log.debug("CS draft error: %s", _ce)
                                import threading as _cst
                                _cst.Thread(target=_cs_draft, daemon=True, name="cs-draft").start()
                                email_handled = True
                        except Exception as _cse:
                            log.debug("CS classification error: %s", _cse)

                        # SAFETY NET: Never silently drop emails from .gov / known buyer domains
                        sender_email = self._extract_email(sender).lower()
                        is_buyer = any(d in sender_email for d in [
                            ".ca.gov", "cdcr", "calvet", "cdph", "cchcs", "dsh",
                            "calfire", "caltrans", "chp", "dgs",
                        ])
                        if not email_handled and is_buyer:
                            log.warning("UNCLASSIFIED buyer email — notifying: from=%s subj=%s",
                                        sender[:40], subject[:60])
                            try:
                                from src.agents.notify_agent import send_alert
                                send_alert(
                                    event_type="unclassified_buyer_email",
                                    title=f"Unclassified email from buyer",
                                    body=f"From: {sender[:50]} — Subject: {subject[:60]}. "
                                         f"Not RFQ, not CS pattern. Needs manual review.",
                                    urgency="urgent",
                                    context={"sender": sender, "subject": subject},
                                    cooldown_key=f"unclass_{uid}",
                                )
                            except Exception:
                                pass
                        elif not email_handled:
                            log.debug("Non-buyer email skipped: %s — %s", sender[:30], subject[:50])

                        self._processed.add(uid)
                        self._diag["not_rfq"] += 1
                        continue
                    # Save attachments
                    rfq_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uid[:6]
                    rfq_dir = os.path.join(save_dir, rfq_id)
                    os.makedirs(rfq_dir, exist_ok=True)
                    
                    attachments = self._save_attachments(msg, rfq_dir)
                    fwd_attachments = self._extract_forwarded_attachments(msg, rfq_dir)
                    attachments.extend(fwd_attachments)
                    
                    if attachments:
                        sol_num = extract_solicitation_number(
                            subject, body, 
                            [a["filename"] for a in attachments]
                        )
                        
                        rfq_info = {
                            "id": rfq_id,
                            "email_uid": uid,
                            "message_id": msg.get("Message-ID", ""),
                            "subject": subject,
                            "sender": sender,
                            "sender_email": self._extract_email(sender),
                            "date": msg["Date"],
                            "solicitation_hint": sol_num,
                            "attachments": attachments,
                            "rfq_dir": rfq_dir,
                            "body_preview": body[:500] if body else "",
                        }
                        results.append(rfq_info)
                        self._diag["rfq_captured"] += 1
                        log.info(f"RFQ captured: {subject[:60]} ({len(attachments)} PDFs, sol #{sol_num})")

                        # PC creation is handled by process_rfq_email() → _trigger_auto_price()
                        # in dashboard.py. Do NOT create PCs here to avoid duplicates.

                        # 🔔 RFQ arrival alert
                        try:
                            from src.agents.notify_agent import send_alert, log_email_event
                            send_alert(
                                event_type="rfq_arrived",
                                title=f"New RFQ: {subject[:50]}",
                                body=f"From: {sender} — auto-pricing started.",
                                urgency="urgent",
                                context={"contact": sender, "entity_id": rfq_id},
                                cooldown_key=f"rfq_{rfq_id}",
                            )
                            log_email_event(
                                direction="received",
                                sender=sender,
                                recipient=self.email_addr,
                                subject=subject,
                                body_preview=(body or "")[:500],
                                rfq_id=rfq_id,
                                intent="rfq",
                                status="received",
                            )
                        except Exception as _ne:
                            pass
                    else:
                        log.info(f"RFQ email but no PDFs saved: {subject[:60]}")
                    
                    self._processed.add(uid)
                    
                except Exception as e:
                    log.error(f"Error processing email {uid}: {e}")
                    continue

            self._save_processed()
            
        except imaplib.IMAP4.abort:
            log.warning("IMAP connection aborted — will reconnect next cycle")
            self._connected = False
        except Exception as e:
            log.error(f"Error checking emails: {e}")
            self._connected = False
        
        return results

    def _get_pdf_names(self, msg):
        """Get list of PDF filenames without saving them."""
        names = []
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            filename = part.get_filename()
            if filename and filename.lower().endswith(".pdf"):
                names.append(self._decode_header(filename) if isinstance(filename, str) else filename)
            if part.get_content_type() == "message/rfc822":
                payload = part.get_payload()
                if isinstance(payload, list):
                    for inner_msg in payload:
                        for inner_part in inner_msg.walk():
                            fn = inner_part.get_filename()
                            if fn and fn.lower().endswith(".pdf"):
                                names.append(self._decode_header(fn) if isinstance(fn, str) else fn)
        return names

    def _save_attachments(self, msg, save_dir):
        """Save PDF attachments and identify them."""
        saved = []
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            
            filename = part.get_filename()
            if not filename or not filename.lower().endswith(".pdf"):
                continue
            
            filename = self._decode_header(filename) if isinstance(filename, str) else filename
            safe_name = re.sub(r'[^\w\-_. ()]+', '_', filename)
            
            filepath = os.path.join(save_dir, safe_name)
            payload = part.get_payload(decode=True)
            if payload:
                with open(filepath, "wb") as f:
                    f.write(payload)
                
                form_type = self._identify_form(safe_name)
                saved.append({"path": filepath, "filename": safe_name, "type": form_type})
        
        return saved

    def _extract_forwarded_attachments(self, msg, save_dir):
        """Extract PDFs from forwarded/nested message parts."""
        saved = []
        for part in msg.walk():
            if part.get_content_type() == "message/rfc822":
                payload = part.get_payload()
                if isinstance(payload, list):
                    for inner_msg in payload:
                        inner_saved = self._save_attachments(inner_msg, save_dir)
                        saved.extend(inner_saved)
                elif hasattr(payload, 'walk'):
                    inner_saved = self._save_attachments(payload, save_dir)
                    saved.extend(inner_saved)
        return saved

    def _identify_form(self, filename):
        """Identify if a PDF is 703B, 704B, or Bid Package."""
        name_lower = filename.lower().replace(" ", "_").replace("-", "_")
        for form_type, patterns in ATTACHMENT_PATTERNS.items():
            if any(p.replace(" ", "_") in name_lower or p in filename.lower() for p in patterns):
                return form_type
        return "unknown"

    def _decode_header(self, header):
        if not header:
            return ""
        try:
            parts = decode_header(header)
            result = ""
            for content, charset in parts:
                if isinstance(content, bytes):
                    result += content.decode(charset or "utf-8", errors="replace")
                else:
                    result += content
            return result
        except Exception:
            return str(header)

    def _get_body(self, msg):
        """Extract plain text body (handles forwarded messages too)."""
        bodies = []
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        bodies.append(payload.decode("utf-8", errors="replace"))
                elif part.get_content_type() == "message/rfc822":
                    inner = part.get_payload()
                    if isinstance(inner, list):
                        for inner_msg in inner:
                            inner_body = self._get_body(inner_msg)
                            if inner_body:
                                bodies.append(inner_body)
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                bodies.append(payload.decode("utf-8", errors="replace"))
        return "\n".join(bodies)

    def _extract_email(self, from_str):
        match = re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', from_str)
        return match.group(0) if match else from_str

    def disconnect(self):
        try:
            if self.mail:
                self.mail.logout()
        except Exception:
            pass
        self._connected = False


# ═══════════════════════════════════════════════════════════════════════════════
# Email Sender
# ═══════════════════════════════════════════════════════════════════════════════

class EmailSender:
    """Send bid package response emails via SMTP."""
    
    def __init__(self, config):
        self.smtp_host = config.get("smtp_host", "smtp.gmail.com")
        self.smtp_port = config.get("smtp_port", 587)
        self.email_addr = config.get("email", os.environ.get("GMAIL_ADDRESS", "sales@reytechinc.com"))
        self.password = config.get("email_password", os.environ.get("GMAIL_PASSWORD", ""))
        self.from_name = config.get("from_name", "Michael Guadan - Reytech Inc.")
    
    def create_draft_email(self, rfq_data, output_files):
        sol = rfq_data.get("solicitation_number", "")
        requestor = rfq_data.get("requestor_name", "")
        requestor_email = rfq_data.get("requestor_email", "")
        
        subject = f"Reytech Inc. - Bid Response - Solicitation #{sol}"
        
        body = f"""Dear {requestor},

Please find attached our bid response for Solicitation #{sol}.

Bid Package includes:
- AMS 703B - Request for Quotation (signed)
- AMS 704B - CCHCS Acquisition Quote Worksheet (with pricing)
- Bid Package & Forms (all required forms completed)

All items are quoted F.O.B. Destination, freight prepaid and included. 
Pricing is valid for 45 calendar days from the due date.

Please let us know if you need any additional information.

Best regards,
Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com
SB/DVBE Cert #2002605"""
        
        return {
            "to": requestor_email,
            "subject": subject,
            "body": body,
            "attachments": output_files,
            "solicitation": sol,
        }
    
    def send(self, draft):
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
        
        msg = MIMEMultipart("mixed")
        msg["From"] = f"{self.from_name} <{self.email_addr}>"
        msg["To"] = draft["to"]
        msg["Subject"] = draft["subject"]
        
        # CC / BCC
        cc_list = [x.strip() for x in draft.get("cc", "").split(",") if x.strip()]
        bcc_list = [x.strip() for x in draft.get("bcc", "").split(",") if x.strip()]
        if cc_list:
            msg["Cc"] = ", ".join(cc_list)
        
        # Threading — reply to original email thread
        if draft.get("in_reply_to"):
            msg["In-Reply-To"] = draft["in_reply_to"]
            msg["References"] = draft.get("references", draft["in_reply_to"])

        # Build alternative part (plain + HTML)
        body_html = draft.get("body_html", "")
        body_plain = draft.get("body", "")
        
        # Auto-generate HTML version with signature if not provided
        if not body_html and body_plain:
            try:
                from src.core.email_signature import wrap_html_email
                body_html = wrap_html_email(body_plain)
            except ImportError:
                pass
        
        if body_html:
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(body_plain, "plain"))
            alt.attach(MIMEText(body_html, "html"))
            msg.attach(alt)
        else:
            msg.attach(MIMEText(body_plain, "plain"))
        
        for filepath in draft.get("attachments", []):
            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={os.path.basename(filepath)}")
                msg.attach(part)
        
        # All recipients for SMTP envelope
        all_recipients = [draft["to"]] + cc_list + bcc_list
        
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            server.login(self.email_addr, self.password)
            server.send_message(msg, to_addrs=all_recipients)
        
        return True


def send_po_confirmation_reply(msg_obj, po_number: str, gmail_addr: str = "", gmail_pwd: str = ""):
    """
    Draft a Reply-All PO confirmation email → saved to outbox for review.
    Mike verifies PO number, then sends from /agents outbox.
    
    Args:
        msg_obj: The original email.message.Message object
        po_number: Extracted PO number (or 'your recent purchase order')
    """
    # Build reply-all recipients
    original_from = msg_obj.get("From", "")
    original_to = msg_obj.get("To", "")
    original_cc = msg_obj.get("Cc", "")
    original_message_id = msg_obj.get("Message-ID", "")
    original_references = msg_obj.get("References", "")
    original_subject = msg_obj.get("Subject", "")
    
    # Extract all email addresses for reply-all (excluding ourselves)
    import re as _re
    all_addrs = set()
    for field in [original_from, original_to, original_cc]:
        if field:
            found = _re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', field)
            all_addrs.update(a.lower() for a in found)
    
    # Remove our own address
    our_domains = ["reytechinc.com", "reytech.com"]
    all_addrs = {a for a in all_addrs if not any(a.endswith(f"@{d}") for d in our_domains)}
    
    if not all_addrs:
        log.warning("PO reply-all: no recipients found")
        return False
    
    # Primary recipient = original sender, CC = everyone else
    from_email = _re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', original_from)
    primary_to = from_email[0] if from_email else list(all_addrs)[0]
    cc_addrs = [a for a in all_addrs if a != primary_to]
    
    # PO number display
    po_display = f"PO {po_number}" if po_number else "your recent purchase order"
    
    # Compose reply
    reply_subject = original_subject
    if not reply_subject.lower().startswith("re:"):
        reply_subject = f"Re: {reply_subject}"
    
    reply_body = f"""Hello,

This email confirms receipt of {po_display}. We will begin to process this order immediately. Should you have any further questions or need assistance, please let us know."""

    # Generate HTML version with signature
    try:
        from src.core.email_signature import wrap_html_email, get_plain_signature
        reply_body_html = wrap_html_email(reply_body)
        reply_body += "\n\n" + get_plain_signature()
    except ImportError:
        reply_body_html = ""
        reply_body += f"""

Respectfully,

Michael Guadan
Reytech Inc.
949-229-1575
sales@reytechinc.com"""
    
    # Build threading headers
    references = original_references or ""
    if original_message_id:
        references = f"{references} {original_message_id}".strip()
    
    # Save as DRAFT to outbox — Mike reviews PO# then sends
    draft = {
        "id": f"po_confirm_{po_number or 'unknown'}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "to": primary_to,
        "cc": ", ".join(cc_addrs),
        "subject": reply_subject,
        "body": reply_body,
        "body_html": reply_body_html,
        "in_reply_to": original_message_id,
        "references": references,
        "attachments": [],
        "status": "draft",
        "source": "po_auto_confirm",
        "po_number": po_number or "",
        "created_at": datetime.now().isoformat(),
        "priority": "high",
    }
    
    try:
        from src.core.paths import DATA_DIR
        outbox_path = os.path.join(DATA_DIR, "email_outbox.json")
        try:
            with open(outbox_path) as f:
                outbox = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            outbox = []
        outbox.append(draft)
        with open(outbox_path, "w") as f:
            json.dump(outbox, f, indent=2, default=str)
        
        log.info("📝 PO confirmation DRAFT saved: to=%s cc=%s po=%s — review in /agents outbox",
                 primary_to, cc_addrs[:3], po_number)
        return True
    except Exception as e:
        log.error("PO confirmation draft save failed: %s", e)
        return False
