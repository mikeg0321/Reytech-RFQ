


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

    # ── Step 2: Check for NEW RFQ form attachments ──
    # If the reply carries fresh 704B/703B forms, it IS a new submission
    # even though it's threaded as a reply (buyer sometimes replies with new RFQ)
    has_new_forms = False
    if pdf_names:
        for name in pdf_names:
            name_lower = name.lower().replace(" ", ".").replace("-", ".")
            if any(re.search(p, name_lower) for p in [r"703b", r"704b", r"bid.?package", r"quote.?worksheet"]):
                has_new_forms = True
                break

    if has_new_forms:
        log.info("Reply has new RFQ form attachments — treating as NEW RFQ: %s", subject[:60])
        return None  # Let is_rfq_email() process it as a genuine new submission

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
                    
                    # Get PDF names without saving
                    pdf_names = self._get_pdf_names(msg)
                    
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
                        continue
                    # ── END REPLY DETECTION ────────────────────────────────────

                    if not is_rfq_email(subject, body, pdf_names):
                        # Check if it's a shipping/tracking email
                        try:
                            from src.agents.predictive_intel import detect_shipping_email
                            ship_info = detect_shipping_email(subject, body, sender)
                            if ship_info.get("is_shipping") and ship_info.get("tracking_numbers"):
                                log.info("📦 Shipping email detected: %s tracking=%s",
                                         subject[:60], ship_info["tracking_numbers"][:2])
                                # Save for order matching
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
                        except Exception as _e:
                            pass  # Non-critical

                        # ── CS Agent: Inbound Update Request Detection ──────────────
                        # After ruling out RFQ + shipping, check if this is a customer
                        # asking for an order/quote/delivery/invoice update.
                        # Auto-drafts a professional CS reply for Mike to review.
                        try:
                            from src.agents.cs_agent import classify_inbound_email, build_cs_response_draft
                            cs_class = classify_inbound_email(subject, body, sender)
                            if cs_class.get("is_update_request"):
                                log.info("📬 CS update request detected: intent=%s from=%s subject=%s",
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
                        except Exception as _cse:
                            pass  # Non-critical — CS agent is additive

                        self._processed.add(uid)
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
                        log.info(f"RFQ captured: {subject[:60]} ({len(attachments)} PDFs, sol #{sol_num})")

                        # ── PRD Feature 4.2: Auto Price Check + Draft Quote ──
                        # Trigger in background thread so polling doesn't block.
                        # Creates a draft quote the user can review and approve.
                        def _auto_draft(rfq=rfq_info):
                            """PRD Feature 4.2: Email RFQ → Auto Price Check → Draft Quote."""
                            try:
                                from src.api.dashboard import _handle_price_check_upload, _push_notification
                                import uuid as _uuid
                                pc_id = f"auto_{_uuid.uuid4().hex[:8]}"
                                pdfs = [a["path"] for a in rfq.get("attachments", [])
                                        if a.get("path") and a["path"].endswith(".pdf")]
                                if not pdfs:
                                    log.info("Auto-draft: no PDFs in RFQ — skipping")
                                    return

                                # ── DEDUP: Check if this PDF was already processed ──
                                # Parse first to get pc_number, then check existing PCs
                                try:
                                    from src.parsers.ams704_parser import parse_ams704
                                    pre_parsed = parse_ams704(pdfs[0])
                                    pre_pc_num = pre_parsed.get("header", {}).get("price_check_number", "")
                                    pre_inst = pre_parsed.get("header", {}).get("institution", "")
                                    if pre_pc_num and pre_pc_num != "unknown" and pre_inst:
                                        from src.api.modules.routes_rfq import _load_price_checks
                                        existing_pcs = _load_price_checks()
                                        for eid, epc in existing_pcs.items():
                                            if (epc.get("pc_number", "").strip() == pre_pc_num.strip()
                                                    and epc.get("institution", "").strip().lower() == pre_inst.strip().lower()):
                                                log.info("Auto-draft dedup: PC #%s from %s already exists as %s — skipping",
                                                         pre_pc_num, pre_inst, eid)
                                                return
                                except Exception as _dp:
                                    log.debug("Auto-draft dedup pre-check failed (non-fatal): %s", _dp)

                                # Step 1: Create price check from PDF
                                pc_result = _handle_price_check_upload(pdfs[0], pc_id)
                                log.info("Auto-draft [Feature 4.2]: PC %s created from %s", pc_id, rfq.get("subject","")[:50])
                                # Step 2: Auto-run price lookup
                                try:
                                    from src.auto.auto_processor import auto_process_price_check
                                    auto_process_price_check(pdfs[0], pc_id=pc_id)
                                    log.info("Auto-draft: price lookup complete for %s", pc_id)
                                except Exception as _ape:
                                    log.debug("Auto-draft price lookup skipped: %s", _ape)
                                # Step 3: Create draft quote
                                try:
                                    from src.api.dashboard import _create_quote_from_pc
                                    q_result = _create_quote_from_pc(pc_id, status="draft")
                                    if q_result and q_result.get("ok"):
                                        qnum = q_result.get("quote_number","")
                                        log.info("Auto-draft: quote %s created (draft) from RFQ", qnum)
                                        # Step 4: Push dashboard notification
                                        agency = rfq.get("agency","") or rfq.get("institution","") or "Unknown Agency"
                                        _push_notification({
                                            "type": "auto_draft",
                                            "title": f"New draft quote from {agency}",
                                            "message": f"Quote {qnum} ready to review",
                                            "quote_number": qnum,
                                            "pc_id": pc_id,
                                            "url": f"/quotes",
                                            "feature": "PRD 4.2",
                                        })
                                except Exception as _qe:
                                    log.debug("Auto-draft quote creation skipped: %s", _qe)
                            except Exception as _ae:
                                log.debug("Auto-draft pipeline failed: %s", _ae)
                        # 🔔 RFQ arrival alert (before spawning auto-draft thread)
                        try:
                            from src.agents.notify_agent import send_alert, log_email_event
                            send_alert(
                                event_type="rfq_arrived",
                                title=f"🚨 New RFQ: {subject[:50]}",
                                body=f"From: {sender} — {len(rfq_info.get('items',[]))} line items. Auto-draft starting.",
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
                        import threading as _t
                        _t.Thread(target=_auto_draft, daemon=True, name="auto-draft").start()
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

        # Build alternative part (plain + HTML) if HTML body provided
        body_html = draft.get("body_html", "")
        body_plain = draft.get("body", "")
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
        
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            server.login(self.email_addr, self.password)
            server.send_message(msg)
        
        return True
