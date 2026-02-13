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
                except:
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
                    
                    if not is_rfq_email(subject, body, pdf_names):
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
        except:
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
        
        msg = MIMEMultipart()
        msg["From"] = f"{self.from_name} <{self.email_addr}>"
        msg["To"] = draft["to"]
        msg["Subject"] = draft["subject"]
        msg.attach(MIMEText(draft["body"], "plain"))
        
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
