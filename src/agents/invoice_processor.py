"""
invoice_processor.py — Polls Gmail for QB invoice emails, enhances PDF with UOM + PO#.

Flow:
  1. QB creates invoice → emails PDF to sales@reytechinc.com
  2. This agent polls Gmail for "Invoice from Reytech" subjects
  3. Downloads PDF attachment
  4. Matches to order by invoice number
  5. Enhances PDF: adds UOM column + PO number overlay
  6. Stores enhanced PDF on the order
  7. Order status: awaiting_email → ready_to_send

The user then clicks "Send to Customer" on the order page.
"""

import os
import re
import json
import time
import email
import logging
import imaplib
import threading
from datetime import datetime
from typing import Optional

log = logging.getLogger("invoice_processor")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

POLL_INTERVAL = 300  # 5 minutes
_running = False


def _get_email_config():
    """Get Gmail IMAP config from env."""
    return {
        "email": os.environ.get("GMAIL_ADDRESS", ""),
        "password": os.environ.get("GMAIL_PASSWORD", ""),
        "imap_server": "imap.gmail.com",
    }


def poll_for_qb_invoices():
    """Check Gmail for QuickBooks invoice emails, process attachments."""
    cfg = _get_email_config()
    if not cfg["email"] or not cfg["password"]:
        return {"ok": False, "error": "Email not configured"}

    try:
        mail = imaplib.IMAP4_SSL(cfg["imap_server"])
        mail.login(cfg["email"], cfg["password"])
        mail.select("INBOX")

        # Search for QB invoice emails not yet processed
        # QB subjects: "Invoice #25-040 from Reytech Inc."
        _, msg_ids = mail.search(None, '(SUBJECT "Invoice" SUBJECT "Reytech" UNSEEN)')
        ids = msg_ids[0].split() if msg_ids[0] else []

        processed = 0
        for mid in ids[-10:]:  # Process up to 10 at a time
            _, msg_data = mail.fetch(mid, "(BODY.PEEK[])")
            raw = msg_data[0][1]
            msg = email.message_from_bytes(raw)

            subject = str(msg.get("Subject", ""))
            sender = str(msg.get("From", ""))

            # Must be from QB (intuit/quickbooks) or our own address
            if not any(k in sender.lower() for k in ["intuit", "quickbooks", "reytechinc"]):
                continue

            # Extract invoice number from subject
            inv_match = re.search(r'Invoice\s*#?\s*(\d{2}-\d{3})', subject)
            if not inv_match:
                continue
            inv_number = inv_match.group(1)
            log.info("Found QB invoice email: #%s from %s", inv_number, sender)

            # Extract PDF attachment
            pdf_data = None
            pdf_name = ""
            for part in msg.walk():
                ct = part.get_content_type()
                fn = part.get_filename() or ""
                if ct == "application/pdf" or fn.lower().endswith(".pdf"):
                    pdf_data = part.get_payload(decode=True)
                    pdf_name = fn or f"Invoice_{inv_number}.pdf"
                    break

            if not pdf_data:
                log.warning("QB invoice #%s email has no PDF attachment", inv_number)
                continue

            # Save raw PDF
            raw_path = os.path.join(DATA_DIR, f"qb_invoice_{inv_number}_raw.pdf")
            with open(raw_path, "wb") as f:
                f.write(pdf_data)

            # Match to order by invoice number
            order_id = _find_order_by_invoice(inv_number)
            if order_id:
                # Enhance PDF with UOM + PO#
                enhanced_path = _enhance_invoice_pdf(raw_path, order_id, inv_number)
                _update_order_invoice_status(order_id, raw_path, enhanced_path, inv_number)
                processed += 1
                log.info("QB invoice #%s → order %s: enhanced PDF ready", inv_number, order_id)
            else:
                log.info("QB invoice #%s: no matching order found, PDF saved at %s", inv_number, raw_path)

            # Mark as read
            mail.store(mid, "+FLAGS", "\\Seen")

        mail.logout()
        return {"ok": True, "processed": processed, "checked": len(ids)}

    except Exception as e:
        log.error("Invoice poll error: %s", e)
        return {"ok": False, "error": str(e)}


def _load_orders_sqlite() -> dict:
    """Load orders — delegates to order_dal (V2)."""
    try:
        from src.core.order_dal import load_orders_dict
        return load_orders_dict()
    except Exception as e:
        log.warning("_load_orders_sqlite via order_dal failed: %s", e)
        return {}


def _save_single_order_sqlite(order_id, order):
    """Save a single order — delegates to order_dal (V2)."""
    try:
        from src.core.order_dal import save_order, save_line_items_batch
        save_order(order_id, order, actor="invoice_processor")
        items = order.get("line_items", order.get("items", []))
        if items and isinstance(items, list):
            save_line_items_batch(order_id, items)
    except Exception as e:
        log.error("_save_single_order_sqlite via order_dal failed for %s: %s", order_id, e)


def _find_order_by_invoice(inv_number: str) -> Optional[str]:
    """Find which order this QB invoice belongs to."""
    try:
        orders = _load_orders_sqlite()
        for oid, order in orders.items():
            if order.get("qb_invoice_number") == inv_number:
                return oid
            if order.get("invoice_number") == inv_number:
                return oid
    except Exception as e:
        log.debug("Order lookup: %s", e)
    return None


def _enhance_invoice_pdf(raw_pdf_path: str, order_id: str, inv_number: str) -> str:
    """Add UOM column and PO number to the QB invoice PDF.
    
    Uses reportlab to overlay text on existing PDF pages.
    """
    enhanced_path = os.path.join(DATA_DIR, f"invoice_{inv_number}.pdf")

    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import letter
        from PyPDF2 import PdfReader, PdfWriter
        import io

        # Load order data for UOM + PO
        orders = _load_orders_sqlite()
        order = orders.get(order_id, {})
        po_number = order.get("po_number", "") or order.get("invoice_po_number", "")
        uom_data = order.get("invoice_items_uom", [])
        items = order.get("line_items", [])

        # Read the QB PDF
        reader = PdfReader(raw_pdf_path)
        writer = PdfWriter()

        for page_num, page in enumerate(reader.pages):
            page_width = float(page.mediabox.width)
            page_height = float(page.mediabox.height)

            # Create overlay
            overlay_buf = io.BytesIO()
            c = canvas.Canvas(overlay_buf, pagesize=(page_width, page_height))

            if page_num == 0:
                # Add PO number if not already on the invoice
                if po_number:
                    c.setFont("Helvetica-Bold", 9)
                    c.drawString(72, page_height - 310, "P.O. NUMBER")
                    c.setFont("Helvetica", 9)
                    c.drawString(72, page_height - 322, po_number)

                # Add UOM next to each line item's QTY
                # QB invoice layout: QTY is around x=72, UOM would go at x=105
                c.setFont("Helvetica-Bold", 8)
                c.drawString(108, page_height - 355, "UOM")  # Header

                c.setFont("Helvetica", 8)
                # Approximate line item positions (QB uses ~20pt line spacing)
                y_start = page_height - 372
                line_height = 55  # Approximate spacing between items

                for i, it in enumerate(items[:10]):  # Max 10 items per page
                    uom = it.get("uom", "EA") or "EA"
                    if i < len(uom_data):
                        uom = uom_data[i].get("uom", uom) or uom
                    y = y_start - (i * line_height)
                    if y < 100:
                        break  # Don't go below totals area
                    c.drawString(108, y, uom.upper())

            c.save()

            # Merge overlay onto page
            overlay_buf.seek(0)
            overlay_reader = PdfReader(overlay_buf)
            if overlay_reader.pages:
                page.merge_page(overlay_reader.pages[0])

            writer.add_page(page)

        with open(enhanced_path, "wb") as f:
            writer.write(f)

        log.info("Enhanced invoice PDF: %s (added UOM + PO# %s)", enhanced_path, po_number)
        return enhanced_path

    except ImportError as ie:
        log.warning("PDF enhancement needs reportlab + PyPDF2: %s. Using raw PDF.", ie)
        # Fallback: just use the raw PDF as-is
        import shutil
        shutil.copy2(raw_pdf_path, enhanced_path)
        return enhanced_path
    except Exception as e:
        log.error("PDF enhancement error: %s", e)
        import shutil
        shutil.copy2(raw_pdf_path, enhanced_path)
        return enhanced_path


def _update_order_invoice_status(order_id: str, raw_path: str, enhanced_path: str, inv_number: str):
    """Update order with invoice PDF paths and status."""
    try:
        orders = _load_orders_sqlite()
        order = orders.get(order_id, {})
        order["invoice_pdf_raw"] = raw_path
        order["invoice_pdf_enhanced"] = enhanced_path
        order["invoice_pdf"] = enhanced_path  # For backward compat
        order["invoice_status"] = "ready_to_send"
        order["invoice_received_at"] = datetime.now().isoformat()
        _save_single_order_sqlite(order_id, order)
    except Exception as e:
        log.error("Order invoice status update error: %s", e)


# ── Background poller ──────────────────────────────────────────────────────

def _poll_loop():
    """Background thread: polls Gmail for QB invoice emails."""
    global _running
    _running = True
    time.sleep(60)  # Let app boot
    while _running:
        try:
            result = poll_for_qb_invoices()
            if result.get("processed", 0) > 0:
                log.info("Invoice poller: processed %d invoices", result["processed"])
        except Exception as e:
            log.error("Invoice poller error: %s", e)
        try:
            time.sleep(POLL_INTERVAL)
        except Exception:
            break


def start_invoice_poller():
    """Start background thread to poll for QB invoice emails."""
    cfg = _get_email_config()
    if not cfg["email"]:
        log.info("Invoice poller: not configured (no GMAIL_ADDRESS)")
        return
    t = threading.Thread(target=_poll_loop, daemon=True, name="invoice-poller")
    t.start()
    log.info("Invoice poller started (checks every %ds)", POLL_INTERVAL)
