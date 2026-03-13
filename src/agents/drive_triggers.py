"""
Drive Triggers — Hooks into app workflow events to push files to Google Drive.

All triggers are async (enqueued to background worker) to never block the user.
Each trigger determines the correct folder path and uploads relevant files.
"""

import os
import logging
from datetime import datetime
from typing import Optional

log = logging.getLogger("reytech.drive_triggers")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")


def _is_ready() -> bool:
    """Check if Drive is configured before attempting any operation."""
    try:
        from src.core.gdrive import is_configured
        return is_configured()
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════
# Trigger 1: Package Generated
# ═══════════════════════════════════════════════════════════════════════

def on_package_generated(rfq: dict, output_dir: str, output_files: list):
    """Called when Generate Package completes. Uploads all files to Pending."""
    if not _is_ready():
        return

    sol = rfq.get("solicitation_number", "")
    if not sol:
        return

    try:
        from src.core.gdrive import get_folder_path, enqueue, _get_or_create_folder

        year = str(datetime.now().year)
        pending_id = get_folder_path(year, category="Pending")
        sol_folder_id = _get_or_create_folder(sol, pending_id)

        for fname in output_files:
            fpath = os.path.join(output_dir, fname)
            if os.path.exists(fpath):
                drive_name = f"{sol}_{fname}" if not fname.startswith(sol) else fname
                enqueue({
                    "action": "upload_file",
                    "local_path": fpath,
                    "folder_id": sol_folder_id,
                    "filename": drive_name,
                })

        log.info("Drive trigger: package_generated → %d files queued for %s", len(output_files), sol)
    except Exception as e:
        log.error("Drive trigger on_package_generated failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# Trigger 2: Quote Sent
# ═══════════════════════════════════════════════════════════════════════

def on_quote_sent(rfq: dict, email_body: str = "", to_email: str = ""):
    """Called when a quote email is sent. Saves email body as PDF in Pending."""
    if not _is_ready():
        return

    sol = rfq.get("solicitation_number", "")
    if not sol:
        return

    try:
        from src.core.gdrive import get_folder_path, enqueue, _get_or_create_folder

        year = str(datetime.now().year)
        pending_id = get_folder_path(year, category="Pending")
        sol_folder_id = _get_or_create_folder(sol, pending_id)

        # Save email body as text file for contract record
        if email_body:
            email_path = os.path.join(DATA_DIR, "uploads", f"{sol}_QuoteEmail.txt")
            os.makedirs(os.path.dirname(email_path), exist_ok=True)
            with open(email_path, "w") as f:
                f.write(f"To: {to_email}\n")
                f.write(f"Date: {datetime.now().isoformat()}\n")
                f.write(f"Subject: Quote — Solicitation #{sol}\n\n")
                f.write(email_body)

            enqueue({
                "action": "upload_file",
                "local_path": email_path,
                "folder_id": sol_folder_id,
                "filename": f"{sol}_QuoteEmail.txt",
            })

        log.info("Drive trigger: quote_sent → email archived for %s", sol)
    except Exception as e:
        log.error("Drive trigger on_quote_sent failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# Trigger 3: Supplier Quote Uploaded
# ═══════════════════════════════════════════════════════════════════════

def on_supplier_quote_uploaded(rfq: dict, pdf_path: str, supplier: str, quote_number: str):
    """Called when a supplier quote PDF is uploaded. Archives to vendor folder + Pending."""
    if not _is_ready():
        return

    sol = rfq.get("solicitation_number", "")
    if not pdf_path or not os.path.exists(pdf_path):
        return

    try:
        from src.core.gdrive import get_folder_path, enqueue, _get_or_create_folder

        year = str(datetime.now().year)
        fname = f"{supplier}_{quote_number or os.path.basename(pdf_path)}"
        if not fname.endswith(".pdf"):
            fname += ".pdf"

        # Upload to Supplier_Quotes/Vendor/
        vendor_root = get_folder_path(category="Supplier_Quotes")
        safe_supplier = supplier.replace("/", "_").replace("\\", "_") or "Unknown"
        vendor_folder = _get_or_create_folder(safe_supplier, vendor_root)
        enqueue({
            "action": "upload_file",
            "local_path": pdf_path,
            "folder_id": vendor_folder,
            "filename": fname,
        })

        # Also upload to Pending/solicitation/ if we have one
        if sol:
            pending_id = get_folder_path(year, category="Pending")
            sol_folder_id = _get_or_create_folder(sol, pending_id)
            enqueue({
                "action": "upload_file",
                "local_path": pdf_path,
                "folder_id": sol_folder_id,
                "filename": f"{sol}_{fname}",
            })

        log.info("Drive trigger: supplier_quote → %s from %s", quote_number, supplier)
    except Exception as e:
        log.error("Drive trigger on_supplier_quote_uploaded failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# Trigger 4: PO Received
# ═══════════════════════════════════════════════════════════════════════

def on_po_received(order: dict):
    """Called when a PO is detected/created. Creates PO folder and copies from Pending."""
    if not _is_ready():
        return

    po = order.get("po_number", "")
    sol = order.get("solicitation_number", "") or order.get("rfq_number", "")
    if not po:
        return

    try:
        from src.core.gdrive import enqueue, _current_quarter

        year = str(datetime.now().year)
        quarter = _current_quarter()

        enqueue({
            "action": "create_po_folder",
            "po_number": f"PO-{po}" if not po.startswith("PO") else po,
            "year": year,
            "quarter": quarter,
            "solicitation_number": sol,
        })

        log.info("Drive trigger: po_received → creating PO folder for %s", po)
    except Exception as e:
        log.error("Drive trigger on_po_received failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# Trigger 5: Order Document Added (supplier order, tracking, POD, invoice)
# ═══════════════════════════════════════════════════════════════════════

def on_order_document(order: dict, doc_path: str, doc_type: str, filename: str = ""):
    """
    Called when any document is added to an order.
    doc_type: "supplier" | "delivery" | "invoice" | "misc"
    """
    if not _is_ready():
        return

    po = order.get("po_number", "")
    if not po or not doc_path:
        return

    try:
        from src.core.gdrive import get_folder_path, enqueue, _current_quarter

        year = str(datetime.now().year)
        quarter = _current_quarter()
        po_name = f"PO-{po}" if not po.startswith("PO") else po

        subfolder_map = {
            "supplier": "Supplier",
            "delivery": "Delivery",
            "invoice": "Invoice",
            "misc": "Misc",
            "rfq": "RFQ",
        }
        subfolder = subfolder_map.get(doc_type, "Misc")

        folder_id = get_folder_path(year, quarter, po_name, subfolder=subfolder)

        drive_name = filename or os.path.basename(doc_path)
        if not drive_name.startswith(po.replace("PO-", "")):
            drive_name = f"{po.replace('PO-', '')}_{drive_name}"

        if os.path.exists(doc_path):
            enqueue({
                "action": "upload_file",
                "local_path": doc_path,
                "folder_id": folder_id,
                "filename": drive_name,
            })
        elif isinstance(doc_path, bytes):
            enqueue({
                "action": "upload_bytes",
                "data": doc_path,
                "folder_id": folder_id,
                "filename": drive_name,
            })

        log.info("Drive trigger: order_document → %s to %s/%s", drive_name, po_name, subfolder)
    except Exception as e:
        log.error("Drive trigger on_order_document failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# Trigger 6: Loss Detected
# ═══════════════════════════════════════════════════════════════════════

def on_loss_detected(solicitation: str, agency: str, loss_summary_path: str,
                     year: str = "", quarter: str = ""):
    """Called when growth agent detects a SCPRS loss. Uploads loss summary to Lost/."""
    if not _is_ready():
        return

    try:
        from src.core.gdrive import get_folder_path, enqueue, _current_quarter

        year = year or str(datetime.now().year)
        quarter = quarter or _current_quarter()

        lost_folder = get_folder_path(year, quarter, category="Lost")
        fname = f"{solicitation}_LossSummary.pdf"

        if os.path.exists(loss_summary_path):
            enqueue({
                "action": "upload_file",
                "local_path": loss_summary_path,
                "folder_id": lost_folder,
                "filename": fname,
            })

        log.info("Drive trigger: loss_detected → %s to %s/Lost/", solicitation, quarter)
    except Exception as e:
        log.error("Drive trigger on_loss_detected failed: %s", e)


# ═══════════════════════════════════════════════════════════════════════
# Trigger 7: PC Completed (no RFQ)
# ═══════════════════════════════════════════════════════════════════════

def on_pc_completed(pc: dict, pdf_path: str = ""):
    """Called when a Price Check is completed without becoming an RFQ."""
    if not _is_ready():
        return

    pc_number = pc.get("pc_number", "")
    institution = pc.get("institution", "")
    if not pc_number:
        return

    try:
        from src.core.gdrive import get_folder_path, enqueue

        year = str(datetime.now().year)
        pc_folder = get_folder_path(year, category="Price_Checks")

        fname = f"PC-{pc_number}_{institution[:20]}.pdf" if pdf_path else f"PC-{pc_number}.json"

        if pdf_path and os.path.exists(pdf_path):
            enqueue({
                "action": "upload_file",
                "local_path": pdf_path,
                "folder_id": pc_folder,
                "filename": fname,
            })
        else:
            # Save PC data as JSON
            import json
            json_path = os.path.join(DATA_DIR, "uploads", f"pc_{pc_number}.json")
            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            with open(json_path, "w") as f:
                json.dump(pc, f, indent=2, default=str)
            enqueue({
                "action": "upload_file",
                "local_path": json_path,
                "folder_id": pc_folder,
                "filename": fname,
            })

        log.info("Drive trigger: pc_completed → %s", pc_number)
    except Exception as e:
        log.error("Drive trigger on_pc_completed failed: %s", e)
