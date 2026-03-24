"""
PC-RFQ Linker — Automatically connects Price Checks to RFQs.
When an RFQ arrives for the same buyer/items as a sent PC,
auto-import all pricing data.
"""
import logging
import json
from difflib import SequenceMatcher

log = logging.getLogger("reytech.linker")


def find_matching_pc(rfq_data, pcs):
    """Find the best matching PC for an RFQ.
    Returns (pc_id, pc_data, match_reason) or (None, None, None)
    """
    rfq_email = (rfq_data.get("requestor_email") or "").lower().strip()
    rfq_sol = (rfq_data.get("solicitation_number") or "").strip()
    rfq_items = rfq_data.get("line_items", rfq_data.get("items", []))

    best_match = None
    best_score = 0
    best_reason = ""

    for pcid, pc in pcs.items():
        if not isinstance(pc, dict):
            continue

        pc_data = pc.get("pc_data", pc)
        if isinstance(pc_data, str):
            try:
                pc_data = json.loads(pc_data)
            except Exception:
                continue

        score = 0
        reasons = []

        # Match by email
        pc_email = (pc_data.get("requestor", pc.get("requestor", "")) or "").lower()
        if rfq_email and pc_email and rfq_email == pc_email:
            score += 50
            reasons.append("same_requestor")

        # Match by solicitation number
        pc_sol = (pc_data.get("pc_number", pc.get("pc_number", "")) or "").strip()
        if rfq_sol and pc_sol and (rfq_sol in pc_sol or pc_sol in rfq_sol):
            score += 40
            reasons.append("same_solicitation")

        # Match by institution (uses resolver for canonical name comparison)
        rfq_inst = (rfq_data.get("institution") or "").strip()
        pc_inst = (pc_data.get("institution", pc.get("institution", "")) or "").strip()
        if rfq_inst and pc_inst and len(rfq_inst) >= 3 and len(pc_inst) >= 3:
            try:
                from src.core.institution_resolver import same_institution
                if same_institution(rfq_inst, pc_inst):
                    score += 20
                    reasons.append("same_institution")
            except ImportError:
                # Fallback to substring match if resolver unavailable
                if rfq_inst.lower() in pc_inst.lower() or pc_inst.lower() in rfq_inst.lower():
                    score += 20
                    reasons.append("same_institution")

        # Match by item descriptions
        pc_items = pc_data.get("items", pc.get("items", []))
        if rfq_items and pc_items:
            item_matches = 0
            for rfq_item in rfq_items:
                rfq_desc = (rfq_item.get("description", "") or "").lower()
                if not rfq_desc or len(rfq_desc) < 5:
                    continue
                for pc_item in pc_items:
                    pc_desc = (pc_item.get("description", pc_item.get("desc", "")) or "").lower()
                    if not pc_desc:
                        continue
                    sim = SequenceMatcher(None, rfq_desc, pc_desc).ratio()
                    if sim > 0.6:
                        item_matches += 1
                        break
            if item_matches > 0:
                score += min(30, item_matches * 10)
                reasons.append(f"matched_{item_matches}_items")

        if score > best_score and score >= 40:
            best_score = score
            best_match = pcid
            best_reason = "+".join(reasons)

    if best_match:
        return best_match, pcs[best_match], best_reason
    return None, None, None


def auto_link_rfq_to_pc(rfq_data, pc_id, pc_data):
    """Import all pricing data from PC into RFQ.
    Copies EVERY field from PC items to RFQ items.
    """
    pc_inner = pc_data.get("pc_data", pc_data)
    if isinstance(pc_inner, str):
        try:
            pc_inner = json.loads(pc_inner)
        except Exception:
            pc_inner = {}

    pc_items = pc_inner.get("items", pc_data.get("items", []))
    if not pc_items:
        return 0

    rfq_items = rfq_data.get("line_items", rfq_data.get("items", []))

    imported = 0

    if not rfq_items:
        # No RFQ items yet — import all PC items directly
        new_items = []
        for pc_item in pc_items:
            rfq_item = {}
            for key, val in pc_item.items():
                rfq_item[key] = val
            rfq_item.setdefault("description", pc_item.get("desc", ""))
            rfq_item.setdefault("quantity", pc_item.get("qty", 1))
            rfq_item.setdefault("uom", "EACH")
            rfq_item.setdefault("supplier_cost",
                pc_item.get("cost", pc_item.get("unit_cost",
                pc_item.get("unit_price"))))
            rfq_item.setdefault("price_per_unit",
                pc_item.get("bid_price", pc_item.get("sell_price")))
            rfq_item.setdefault("item_supplier",
                pc_item.get("supplier", ""))
            rfq_item.setdefault("item_link",
                pc_item.get("url", pc_item.get("product_url",
                pc_item.get("amazon_url", ""))))
            rfq_item["source_pc"] = pc_id
            rfq_item["imported_from_pc"] = True
            new_items.append(rfq_item)
            imported += 1
        rfq_data["line_items"] = new_items
        rfq_data["items"] = new_items
    else:
        # RFQ has items — match by description and fill pricing
        for rfq_item in rfq_items:
            rfq_desc = (rfq_item.get("description", "") or "").lower()
            if not rfq_desc:
                continue
            cost = rfq_item.get("supplier_cost", rfq_item.get("cost"))
            try:
                if cost and float(str(cost).replace("$", "").replace(",", "")) > 0:
                    continue
            except (ValueError, TypeError):
                pass

            best_sim = 0
            best_pc_item = None
            for pc_item in pc_items:
                pc_desc = (pc_item.get("description", pc_item.get("desc", "")) or "").lower()
                sim = SequenceMatcher(None, rfq_desc, pc_desc).ratio()
                if sim > best_sim and sim > 0.5:
                    best_sim = sim
                    best_pc_item = pc_item

            if best_pc_item:
                for key in ["supplier_cost", "cost", "unit_cost", "unit_price",
                           "price_per_unit", "bid_price", "sell_price",
                           "item_supplier", "supplier", "item_link", "url",
                           "product_url", "amazon_url", "amazon_price",
                           "scprs_last_price", "catalog_match", "oracle",
                           "intelligence", "markup_pct", "item_number",
                           "mfg_number", "asin", "upc"]:
                    if best_pc_item.get(key) and not rfq_item.get(key):
                        rfq_item[key] = best_pc_item[key]
                rfq_item["source_pc"] = pc_id
                rfq_item["imported_from_pc"] = True
                imported += 1

    rfq_data["linked_pc_id"] = pc_id
    rfq_data["linked_pc_number"] = pc_inner.get("pc_number", pc_data.get("pc_number", ""))
    rfq_data["linked_pc_match_reason"] = "auto_linked"

    log.info("Auto-linked RFQ to PC %s: %d items imported", pc_id, imported)
    return imported
