#!/usr/bin/env python3
"""
Email Poller — monitors IMAP inbox for new RFQ emails from CCHCS.
Extracts PDF attachments, identifies 703B/704B/BidPackage, triggers parsing.
"""

import imaplib
import email
from email.header import decode_header
import os, time, json, re, logging
from datetime import datetime

log = logging.getLogger("email_poller")

# Patterns that indicate an RFQ email from CCHCS
RFQ_INDICATORS = [
    "request for quotation",
    "rfq", "703b", "704b", "bid package",
    "cchcs", "cdcr", "solicitation",
    "informal competitive",
]

ATTACHMENT_PATTERNS = {
    "703b": ["703b", "rfq", "request_for_quotation", "informal_competitive", "attachment_1"],
    "704b": ["704b", "quote_worksheet", "acquisition_quote", "attachment_2"],
    "bidpkg": ["bid_package", "forms", "attachment_3"],
}


class EmailPoller:
    def __init__(self, config):
        self.host = config.get("imap_host", "imap.gmail.com")
        self.port = config.get("imap_port", 993)
        self.email_addr = config.get("email", "rfq@reytechinc.com")
        self.password = config.get("email_password", "")
        self.folder = config.get("imap_folder", "INBOX")
        self.processed_file = config.get("processed_file", "data/processed_emails.json")
        self._processed = self._load_processed()

    def _load_processed(self):
        if os.path.exists(self.processed_file):
            with open(self.processed_file) as f:
                return set(json.load(f))
        return set()

    def _save_processed(self):
        os.makedirs(os.path.dirname(self.processed_file), exist_ok=True)
        with open(self.processed_file, "w") as f:
            json.dump(list(self._processed), f)

    def connect(self):
        """Connect to IMAP server."""
        try:
            self.mail = imaplib.IMAP4_SSL(self.host, self.port)
            self.mail.login(self.email_addr, self.password)
            self.mail.select(self.folder)
            log.info(f"Connected to {self.host} as {self.email_addr}")
            return True
        except Exception as e:
            log.error(f"IMAP connection failed: {e}")
            return False

    def check_for_rfqs(self, save_dir="uploads"):
        """Check inbox for new RFQ emails. Returns list of parsed RFQ dicts."""
        results = []
        
        try:
            # Search for unread emails
            status, messages = self.mail.search(None, "UNSEEN")
            if status != "OK":
                return results

            for msg_id in messages[0].split():
                uid = msg_id.decode()
                if uid in self._processed:
                    continue

                status, data = self.mail.fetch(msg_id, "(RFC822)")
                if status != "OK":
                    continue

                msg = email.message_from_bytes(data[0][1])
                
                # Check if it looks like an RFQ
                subject = self._decode_header(msg["Subject"]) or ""
                sender = self._decode_header(msg["From"]) or ""
                body = self._get_body(msg)
                
                combined = f"{subject} {body}".lower()
                is_rfq = any(ind in combined for ind in RFQ_INDICATORS)
                
                if not is_rfq:
                    continue

                # Extract PDF attachments
                rfq_id = uid[:8] + datetime.now().strftime("%H%M%S")
                rfq_dir = os.path.join(save_dir, rfq_id)
                os.makedirs(rfq_dir, exist_ok=True)
                
                attachments = self._save_attachments(msg, rfq_dir)
                
                if attachments:
                    # Extract solicitation number from subject/body
                    sol_match = re.search(r'(\d{7,8})', subject)
                    sol_num = sol_match.group(1) if sol_match else "unknown"
                    
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
                    }
                    results.append(rfq_info)
                    log.info(f"Found RFQ email: {subject} ({len(attachments)} PDFs)")
                
                self._processed.add(uid)

            self._save_processed()
            
        except Exception as e:
            log.error(f"Error checking emails: {e}")
        
        return results

    def _save_attachments(self, msg, save_dir):
        """Save PDF attachments and identify them."""
        saved = []
        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            
            filename = part.get_filename()
            if not filename or not filename.lower().endswith(".pdf"):
                continue
            
            filepath = os.path.join(save_dir, filename)
            with open(filepath, "wb") as f:
                f.write(part.get_payload(decode=True))
            
            # Identify which form this is
            form_type = self._identify_form(filename)
            saved.append({"path": filepath, "filename": filename, "type": form_type})
        
        return saved

    def _identify_form(self, filename):
        """Identify if a PDF is 703B, 704B, or Bid Package."""
        name_lower = filename.lower().replace(" ", "_").replace("-", "_")
        for form_type, patterns in ATTACHMENT_PATTERNS.items():
            if any(p in name_lower for p in patterns):
                return form_type
        return "unknown"

    def _decode_header(self, header):
        if not header:
            return ""
        parts = decode_header(header)
        result = ""
        for content, charset in parts:
            if isinstance(content, bytes):
                result += content.decode(charset or "utf-8", errors="replace")
            else:
                result += content
        return result

    def _get_body(self, msg):
        """Extract plain text body from email."""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        return payload.decode("utf-8", errors="replace")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                return payload.decode("utf-8", errors="replace")
        return ""

    def _extract_email(self, from_str):
        """Extract email address from 'Name <email>' format."""
        match = re.search(r'[\w.+-]+@[\w-]+\.[\w.]+', from_str)
        return match.group(0) if match else from_str

    def disconnect(self):
        try:
            self.mail.logout()
        except:
            pass


class EmailSender:
    """Send bid package response emails via SMTP."""
    
    def __init__(self, config):
        self.smtp_host = config.get("smtp_host", "smtp.gmail.com")
        self.smtp_port = config.get("smtp_port", 587)
        self.email_addr = config.get("email", "sales@reytechinc.com")
        self.password = config.get("email_password", "")
        self.from_name = config.get("from_name", "Michael Guadan - Reytech Inc.")
    
    def create_draft_email(self, rfq_data, output_files):
        """Create a draft email response dict (for preview before sending)."""
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
        """Actually send the email."""
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
