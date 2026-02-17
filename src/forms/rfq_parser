import sys
from pathlib import Path

# Compatibility for refactored structure
sys.path.insert(0, str(Path(__file__).parent.parent))

#!/usr/bin/env python3
"""
RFQ Parser — extracts structured data from CCHCS RFQ PDF attachments.
Reads pre-filled fields from the 703B (solicitation details) and 704B (line items).
"""

from pypdf import PdfReader
import re, os, json
from datetime import datetime


def parse_703b(pdf_path):
    """Extract solicitation details + requestor info from AMS 703B."""
    reader = PdfReader(pdf_path)
    fields = reader.get_fields() or {}
    
    def fv(name):
        f = fields.get(name)
        if not f:
            return ""
        v = f.get("/V", "")
        return str(v).strip() if v else ""
    
    data = {
        "solicitation_number": fv("703B_Solicitation Number"),
        "release_date": fv("703B_Release Date"),
        "due_date": fv("703B_Due Date"),
        "delivery_days": fv("703B_Deliveries must be completed within"),
        "requestor_name": fv("703B_Name"),
        "requestor_email": fv("703B_Email_2"),
        "requestor_phone": fv("703B_Phone_2"),
    }
    
    # Delivery location from dropdown
    loc = fv("703B_Dropdown2")
    if loc:
        data["delivery_location"] = loc
    
    # Award method
    if fv("703B_Check Box5") == "/Yes":
        data["award_method"] = "all_or_none"
    elif fv("703B_Check Box6") == "/Yes":
        data["award_method"] = "individual_items"
    
    # Freight
    if fv("703B_Check Box7") == "/Yes":
        data["freight"] = "prepaid_included"
    elif fv("703B_Check Box8") == "/Yes":
        data["freight"] = "prepaid_add_or_prepaid"
    
    return data


def parse_704b(pdf_path):
    """Extract line items from AMS 704B. State pre-fills QTY, UOM, descriptions, item numbers."""
    reader = PdfReader(pdf_path)
    fields = reader.get_fields() or {}
    
    def fv(name):
        f = fields.get(name)
        if not f:
            return ""
        v = f.get("/V", "")
        return str(v).strip() if v else ""
    
    # Header
    header = {
        "solicitation_number": fv("SOLICITATION"),
        "date": fv("Date1_af_date"),
        "requestor": fv("REQUESTOR"),
        "department": fv("DEPARTMENT"),
        "phone_email": fv("PHONEEMAIL"),
    }
    
    # Parse line items from rows 1-15
    # The state sometimes uses multiple rows for one line item (multi-line descriptions)
    # We detect a new item by: Row# has a value (the line number) and QTY has a value
    raw_rows = []
    for i in range(1, 16):
        row = {
            "row_index": i,
            "line_number": fv(f"Row{i}"),
            "qty": fv(f"QTYRow{i}"),
            "uom": fv(f"UOMRow{i}"),
            "qty_per_uom": fv(f"QTY PER UOMRow{i}"),
            "unspsc": fv(f"UNSPSCRow{i}"),
            "description": fv(f"ITEM DESCRIPTION PRODUCT SPECIFICATIONRow{i}"),
            "item_number": fv(f"ITEM NUMBERRow{i}"),
            "price_per_unit": fv(f"PRICE PER UNITRow{i}"),
            "subtotal": fv(f"SUBTOTALRow{i}"),
        }
        raw_rows.append(row)
    
    # Consolidate: group continuation rows with their parent line item
    items = []
    current = None
    
    for row in raw_rows:
        is_new_item = bool(row["qty"])  # New item has a QTY
        
        if is_new_item:
            if current:
                items.append(current)
            
            # Parse numeric values
            qty = 0
            try:
                qty = int(float(row["qty"]))
            except:
                pass
            
            qty_per = 1
            try:
                qty_per = int(float(row["qty_per_uom"])) if row["qty_per_uom"] else 1
            except:
                pass
            
            line_num = 0
            try:
                line_num = int(float(row["line_number"])) if row["line_number"] else len(items) + 1
            except:
                line_num = len(items) + 1
            
            current = {
                "line_number": line_num,
                "form_row": row["row_index"],  # Which row in the 704B this starts at
                "qty": qty,
                "uom": row["uom"],
                "qty_per_uom": qty_per,
                "unspsc": row["unspsc"],
                "description": row["description"],
                "item_number": row["item_number"],
                # Pricing — to be filled by agent or user
                "supplier_cost": 0.0,
                "scprs_last_price": None,
                "source_type": "general",
                "price_per_unit": 0.0,
            }
        elif current and row["description"]:
            # Continuation row — append description
            current["description"] += "\n" + row["description"]
            # Pick up item number from continuation if first row didn't have it
            if row["item_number"] and not current["item_number"]:
                current["item_number"] = row["item_number"]
    
    if current:
        items.append(current)
    
    return {"header": header, "line_items": items}


def parse_rfq_attachments(paths):
    """
    Parse a complete RFQ from its 3 PDF attachments.
    Returns a unified RFQ data structure ready for the filler.
    
    paths: dict with keys '703b', '704b', 'bidpkg' pointing to file paths
    """
    result = {}
    
    # Parse 703B for solicitation details
    if "703b" in paths and os.path.exists(paths["703b"]):
        sol_data = parse_703b(paths["703b"])
        result.update(sol_data)
    
    # Parse 704B for line items
    if "704b" in paths and os.path.exists(paths["704b"]):
        quote_data = parse_704b(paths["704b"])
        result["line_items"] = quote_data["line_items"]
        
        # Use 704B header as fallback for missing 703B data
        h = quote_data["header"]
        if not result.get("solicitation_number"):
            result["solicitation_number"] = h.get("solicitation_number", "")
        if not result.get("requestor_name"):
            result["requestor_name"] = h.get("requestor", "")
        if not result.get("requestor_phone"):
            result["requestor_phone"] = h.get("phone_email", "")
    
    # Store template paths for the filler
    result["templates"] = paths
    
    # Add metadata
    result["parsed_at"] = datetime.now().isoformat()
    result["status"] = "pending_pricing"
    
    return result


def identify_attachments(file_paths):
    """
    Given a list of PDF file paths from an email, identify which is 703B, 704B, and Bid Package.
    Uses filename patterns.
    """
    templates = {}
    for path in file_paths:
        name = os.path.basename(path).upper()
        if "703B" in name or "RFQ" in name:
            templates["703b"] = path
        elif "704B" in name or "QUOTE_WORKSHEET" in name or "WORKSHEET" in name:
            templates["704b"] = path
        elif "BID_PACKAGE" in name or "PACKAGE" in name or "FORMS" in name:
            templates["bidpkg"] = path
    return templates


if __name__ == "__main__":
    # Test with the uploaded blank forms
    paths = identify_attachments([
        "/mnt/user-data/uploads/10838043_AMS_703B_-_RFQ_-_Informal_Competitive_-_Attachment_1.pdf",
        "/mnt/user-data/uploads/10838043_AMS_704B_-_CCHCS_Acquisition_Quote_Worksheet_-_Attachment_2.pdf",
        "/mnt/user-data/uploads/10838043_BID_PACKAGE___FORMS__Under_100k___-_Attachment_3.pdf",
    ])
    
    print("Identified templates:", json.dumps({k: os.path.basename(v) for k, v in paths.items()}, indent=2))
    
    rfq = parse_rfq_attachments(paths)
    
    print(f"\nSolicitation: #{rfq['solicitation_number']}")
    print(f"Due: {rfq['due_date']}")
    print(f"Requestor: {rfq['requestor_name']} ({rfq['requestor_email']})")
    print(f"Delivery: {rfq.get('delivery_location', 'N/A')}")
    print(f"Award: {rfq.get('award_method', 'N/A')}")
    print(f"\nLine Items ({len(rfq['line_items'])}):")
    for item in rfq["line_items"]:
        desc = item["description"].split("\n")[0]
        print(f"  #{item['line_number']} | Qty {item['qty']} {item['uom']} | {desc} | Part: {item['item_number']}")
