"""
growth_agent.py — Proactive Growth Engine for Reytech
Phase 26 | Version: 2.0.0

Workflow:
  1. Pull ALL Reytech POs from SCPRS (2022 → present)
  2. Drill into each PO → get items, prices, buyer info
  3. Categorize items into product groups
  4. Search SCPRS for ALL buyers of those categories
  5. Build prospect list with contact info (name, email, agency)
  6. Launch email outreach → voice follow-up if no response in 3-5 days
"""

import json, os, re, logging, time, threading, uuid
from datetime import datetime, timedelta
from collections import defaultdict

log = logging.getLogger("growth")

try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

HISTORY_FILE = os.path.join(DATA_DIR, "growth_reytech_history.json")
CATEGORIES_FILE = os.path.join(DATA_DIR, "growth_categories.json")
PROSPECTS_FILE = os.path.join(DATA_DIR, "growth_prospects.json")
OUTREACH_FILE = os.path.join(DATA_DIR, "growth_outreach.json")

try:
    from src.agents.scprs_lookup import _get_session
    HAS_SCPRS = True
except ImportError:
    HAS_SCPRS = False


def _load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


# ─── Item Category Mapping ───────────────────────────────────────────────

CATEGORY_KEYWORDS = {
    "Medical Supplies": [
        "glove", "nitrile", "exam", "syringe", "needle", "catheter", "bandage",
        "gauze", "surgical", "gown", "mask", "face shield", "medical", "patient",
        "restraint", "stryker", "wheelchair", "first aid",
    ],
    "Janitorial & Cleaning": [
        "trash bag", "liner", "mop", "broom", "disinfect", "sanitizer",
        "bleach", "cleaner", "detergent", "soap", "wipe", "paper towel",
        "toilet paper", "tissue", "janitorial", "cleaning", "floor",
    ],
    "Office Supplies": [
        "pen", "pencil", "paper", "copy paper", "toner", "cartridge", "ink",
        "folder", "binder", "staple", "tape", "envelope", "marker",
        "highlighter", "office", "label",
    ],
    "IT & Electronics": [
        "battery", "cable", "usb", "adapter", "charger", "keyboard",
        "mouse", "monitor", "printer", "laptop", "computer", "hard drive",
    ],
    "Safety & PPE": [
        "safety glass", "ear plug", "hard hat", "vest", "boot",
        "fire extinguisher", "safety", "protective", "respirator", "goggles",
    ],
    "Food Service": [
        "food", "beverage", "cup", "plate", "napkin", "utensil",
        "container", "tray", "coffee",
    ],
    "Facility Maintenance": [
        "light bulb", "led", "bulb", "filter", "hvac", "paint",
        "tool", "hardware", "plumbing", "electrical", "maintenance", "lock",
    ],
}

def categorize_item(description: str) -> str:
    desc_lower = (description or "").lower()
    scores = {}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in desc_lower)
        if score > 0:
            scores[cat] = score
    return max(scores, key=scores.get) if scores else "General Supplies"


# ═══════════════════════════════════════════════════════════════════════
# STEP 1: Pull Reytech History from SCPRS
# ═══════════════════════════════════════════════════════════════════════

PULL_STATUS = {
    "running": False, "phase": "", "progress": "",
    "pos_found": 0, "pos_detailed": 0, "items_total": 0,
    "errors": [], "started_at": None, "finished_at": None,
}

def pull_reytech_history(from_date="01/01/2022", to_date=""):
    """Search SCPRS for all Reytech POs from 2022 to present.
    Drills into each for line items + buyer info."""
    if not HAS_SCPRS:
        return {"ok": False, "error": "SCPRS not available (needs requests + bs4)"}
    if PULL_STATUS["running"]:
        return {"ok": False, "error": "Already running", "status": PULL_STATUS}
    if not to_date:
        to_date = datetime.now().strftime("%m/%d/%Y")

    PULL_STATUS.update({
        "running": True, "phase": "searching",
        "progress": f"Searching SCPRS for Reytech ({from_date} to {to_date})...",
        "pos_found": 0, "pos_detailed": 0, "items_total": 0,
        "errors": [], "started_at": datetime.now().isoformat(), "finished_at": None,
    })

    try:
        session = _get_session()
        if not session.initialized and not session.init_session():
            PULL_STATUS.update({"running": False, "phase": "error"})
            return {"ok": False, "error": "SCPRS session init failed"}

        results = session.search(supplier_name="Reytech", from_date=from_date, to_date=to_date)
        if not results:
            PULL_STATUS["progress"] = "Trying 'Rey Tech'..."
            results = session.search(supplier_name="Rey Tech", from_date=from_date, to_date=to_date)
        if not results:
            PULL_STATUS["progress"] = "Trying 'REYTECH'..."
            results = session.search(supplier_name="REYTECH", from_date=from_date, to_date=to_date)

        PULL_STATUS["pos_found"] = len(results)
        PULL_STATUS["progress"] = f"Found {len(results)} POs — getting details..."
        log.info(f"Growth: Found {len(results)} Reytech POs")

        history = []
        for idx, r in enumerate(results):
            PULL_STATUS["phase"] = "detailing"
            PULL_STATUS["progress"] = f"PO {idx+1}/{len(results)}: {r.get('po_number', '?')}"

            po = {
                "po_number": r.get("po_number", ""),
                "dept": r.get("dept", ""),
                "supplier_name": r.get("supplier_name", ""),
                "start_date": r.get("start_date", ""),
                "grand_total": r.get("grand_total", ""),
                "grand_total_num": r.get("grand_total_num"),
                "buyer_email": r.get("buyer_email", ""),
                "acq_type": r.get("acq_type", ""),
                "status": r.get("status", ""),
                "first_item": r.get("first_item", ""),
                "line_items": [],
                "buyer_name": "",
                "detail_fetched": False,
            }

            if r.get("_results_html") and r.get("_row_index") is not None:
                try:
                    detail = session.get_detail(r["_results_html"], r["_row_index"], r.get("_click_action"))
                    if detail:
                        po["detail_fetched"] = True
                        hdr = detail.get("header", {}) if isinstance(detail.get("header"), dict) else {}
                        po["buyer_name"] = hdr.get("buyer_name", "")
                        po["buyer_email"] = hdr.get("buyer_email", "") or po["buyer_email"]
                        po["line_items"] = detail.get("line_items", [])
                        PULL_STATUS["items_total"] += len(po["line_items"])
                    time.sleep(0.5)
                except Exception as e:
                    PULL_STATUS["errors"].append(f"{po['po_number']}: {e}")

            PULL_STATUS["pos_detailed"] = idx + 1
            history.append(po)

        _save_json(HISTORY_FILE, {
            "supplier": "Reytech", "from_date": from_date, "to_date": to_date,
            "pulled_at": datetime.now().isoformat(),
            "total_pos": len(history), "total_items": PULL_STATUS["items_total"],
            "purchase_orders": history,
        })

        categories = _categorize_history(history)

        PULL_STATUS.update({
            "running": False, "phase": "complete",
            "progress": f"Done: {len(history)} POs, {PULL_STATUS['items_total']} items, {len(categories)} categories",
            "finished_at": datetime.now().isoformat(),
        })

        return {
            "ok": True, "total_pos": len(history),
            "total_items": PULL_STATUS["items_total"],
            "categories": len(categories),
            "date_range": f"{from_date} to {to_date}",
        }

    except Exception as e:
        PULL_STATUS.update({"running": False, "phase": "error", "progress": str(e)})
        return {"ok": False, "error": str(e)}


def _categorize_history(history):
    categories = defaultdict(lambda: {"items": [], "total_value": 0, "po_count": 0, "search_terms": set()})
    for po in history:
        for item in po.get("line_items", []):
            desc = item.get("description", "")
            cat = categorize_item(desc)
            price = item.get("unit_price_num") or 0
            qty = item.get("quantity_num") or 1
            categories[cat]["items"].append({"description": desc, "unit_price": price, "po_number": po.get("po_number")})
            categories[cat]["total_value"] += price * qty
            categories[cat]["po_count"] += 1
            words = set(re.findall(r'\b[a-zA-Z]{3,}\b', desc.lower()))
            categories[cat]["search_terms"].update(words - {"the", "and", "for", "with", "each", "per", "box", "case", "pack"})
        if not po.get("line_items") and po.get("first_item"):
            cat = categorize_item(po["first_item"])
            categories[cat]["items"].append({"description": po["first_item"], "po_number": po.get("po_number")})
            categories[cat]["po_count"] += 1

    result = {}
    for cat, data in categories.items():
        result[cat] = {
            "item_count": len(data["items"]),
            "total_value": round(data["total_value"], 2),
            "po_count": data["po_count"],
            "search_terms": sorted(list(data["search_terms"]))[:20],
            "sample_items": [i["description"][:80] for i in data["items"][:10]],
        }
    _save_json(CATEGORIES_FILE, {"generated_at": datetime.now().isoformat(), "total_categories": len(result), "categories": result})
    return result


# ═══════════════════════════════════════════════════════════════════════
# STEP 2: Find ALL Buyers of Those Categories
# ═══════════════════════════════════════════════════════════════════════

BUYER_STATUS = {"running": False, "phase": "", "progress": "", "prospects_found": 0, "errors": []}

def find_category_buyers(max_categories=10, from_date="01/01/2024"):
    """For each category Reytech sells, find all SCPRS buyers."""
    if not HAS_SCPRS:
        return {"ok": False, "error": "SCPRS not available"}
    if BUYER_STATUS["running"]:
        return {"ok": False, "error": "Already running"}

    cat_data = _load_json(CATEGORIES_FILE)
    if not isinstance(cat_data, dict) or not cat_data.get("categories"):
        return {"ok": False, "error": "No categories. Run pull_reytech_history first."}

    cats = sorted(cat_data["categories"].items(), key=lambda x: x[1].get("total_value", 0), reverse=True)[:max_categories]

    BUYER_STATUS.update({"running": True, "phase": "searching", "progress": "Starting...", "prospects_found": 0, "errors": []})

    try:
        session = _get_session()
        if not session.initialized and not session.init_session():
            BUYER_STATUS.update({"running": False})
            return {"ok": False, "error": "SCPRS session init failed"}

        to_date = datetime.now().strftime("%m/%d/%Y")
        prospects = {}

        for cat_idx, (cat_name, cat_info) in enumerate(cats):
            BUYER_STATUS["progress"] = f"[{cat_idx+1}/{len(cats)}] {cat_name}"

            # Build search queries from sample items
            queries = []
            for item in cat_info.get("sample_items", [])[:2]:
                words = item.split()[:3]
                queries.append(" ".join(words))
            terms = cat_info.get("search_terms", [])[:2]
            if terms:
                queries.append(" ".join(terms))

            for query in queries[:2]:
                try:
                    results = session.search(description=query, from_date=from_date, to_date=to_date)
                    for r in results[:20]:
                        supplier = (r.get("supplier_name") or "").lower()
                        if "reytech" in supplier or "rey tech" in supplier:
                            continue

                        email = (r.get("buyer_email") or "").strip()
                        dept = (r.get("dept") or "").strip()
                        if not email and not dept:
                            continue

                        key = email or f"{dept}_{r.get('po_number', '')}"
                        if key not in prospects:
                            prospects[key] = {
                                "id": f"PRO-{uuid.uuid4().hex[:8]}",
                                "buyer_email": email, "buyer_name": "",
                                "agency": dept, "categories_matched": [],
                                "purchase_orders": [], "total_spend": 0,
                                "outreach_status": "new",
                            }

                        p = prospects[key]
                        if cat_name not in p["categories_matched"]:
                            p["categories_matched"].append(cat_name)

                        po_num = r.get("po_number", "")
                        existing_pos = [x["po_number"] for x in p["purchase_orders"]]
                        if po_num and po_num not in existing_pos:
                            p["purchase_orders"].append({
                                "po_number": po_num, "date": r.get("start_date", ""),
                                "total_num": r.get("grand_total_num"),
                                "items": r.get("first_item", "")[:100], "category": cat_name,
                            })
                            p["total_spend"] += (r.get("grand_total_num") or 0)

                    time.sleep(1)
                except Exception as e:
                    BUYER_STATUS["errors"].append(f"{cat_name}: {e}")

            # Get buyer names from detail on a few results
            for r in (results if 'results' in dir() else [])[:2]:
                if r.get("_results_html") and r.get("_row_index") is not None:
                    try:
                        detail = session.get_detail(r["_results_html"], r["_row_index"], r.get("_click_action"))
                        if detail:
                            hdr = detail.get("header", {}) if isinstance(detail.get("header"), dict) else {}
                            em = hdr.get("buyer_email", "")
                            if em and em in prospects:
                                prospects[em]["buyer_name"] = hdr.get("buyer_name", "")
                        time.sleep(0.5)
                    except Exception:
                        pass

        prospect_list = sorted(prospects.values(), key=lambda p: p["total_spend"], reverse=True)
        _save_json(PROSPECTS_FILE, {
            "generated_at": datetime.now().isoformat(),
            "total_prospects": len(prospect_list),
            "from_date": from_date,
            "prospects": prospect_list,
        })

        BUYER_STATUS.update({"running": False, "phase": "complete", "prospects_found": len(prospect_list)})

        return {
            "ok": True, "prospects_found": len(prospect_list),
            "categories_searched": len(cats),
            "top_prospects": [{"agency": p["agency"], "email": p["buyer_email"], "spend": p["total_spend"]} for p in prospect_list[:10]],
        }

    except Exception as e:
        BUYER_STATUS.update({"running": False, "phase": "error"})
        return {"ok": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════
# STEP 3: Email Outreach
# ═══════════════════════════════════════════════════════════════════════

EMAIL_TEMPLATE = """Hi{name_greeting},

This is Mike from Reytech Inc. We noticed that {agency} purchased {items_mention} on {purchase_date}. {pricing_line}

We've been serving California state agencies for several years and would love the opportunity to get on your RFQ distribution list. We're a certified Small Business (SB) and Disabled Veteran Business Enterprise (DVBE), which helps meet your procurement mandates.

Please consider us for your next order — we'd appreciate the chance to quote.

Best regards,
Mike
Reytech Inc.
sales@reytechinc.com
949-229-1575
reytechinc.com"""


def launch_outreach(max_prospects=50, dry_run=True):
    """Send personalized emails. dry_run=True builds but doesn't send."""
    prospect_data = _load_json(PROSPECTS_FILE)
    if not isinstance(prospect_data, dict) or not prospect_data.get("prospects"):
        return {"ok": False, "error": "No prospects. Run find_category_buyers first."}

    outreach = _load_json(OUTREACH_FILE)
    if not isinstance(outreach, dict):
        outreach = {"campaigns": [], "total_sent": 0}

    contacted = set()
    for c in outreach.get("campaigns", []):
        for o in c.get("outreach", []):
            if o.get("email_sent"):
                contacted.add(o.get("email", ""))

    new = [p for p in prospect_data["prospects"] if p.get("buyer_email") and p["buyer_email"] not in contacted][:max_prospects]
    if not new:
        return {"ok": True, "message": "All prospects already contacted", "new_to_contact": 0}

    campaign = {"id": f"GC-{datetime.now().strftime('%Y%m%d-%H%M')}", "created_at": datetime.now().isoformat(), "dry_run": dry_run, "outreach": []}
    sent = 0

    for p in new:
        name = p.get("buyer_name", "")
        name_greeting = f" {name.split()[0]}" if name else ""
        agency = p.get("agency", "your agency")
        pos = p.get("purchase_orders", [])
        items_mention = pos[0].get("items", "items we also carry")[:80] if pos else "items we also carry"
        purchase_date = pos[0].get("date", "recently") if pos else "recently"
        cats = p.get("categories_matched", [])
        pricing_line = f"We specialize in {', '.join(cats[:3])} and often offer more competitive rates — typically 10-30% below contract pricing." if cats else "We often offer more competitive rates on these items."

        body = EMAIL_TEMPLATE.format(name_greeting=name_greeting, agency=agency, items_mention=items_mention, purchase_date=purchase_date, pricing_line=pricing_line)
        subject = f"Reytech Inc. — {', '.join(cats[:2]) if cats else 'Supply'} Vendor Introduction"

        entry = {
            "prospect_id": p["id"], "email": p["buyer_email"], "name": name,
            "agency": agency, "categories": cats,
            "email_subject": subject, "email_body": body,
            "email_sent": False, "email_sent_at": None,
            "voice_follow_up_date": (datetime.now() + timedelta(days=4)).isoformat(),
            "voice_called": False, "response_received": False,
        }

        if not dry_run and p.get("buyer_email"):
            try:
                from src.agents.email_poller import send_email
                send_email(to=p["buyer_email"], subject=subject, body=body)
                entry["email_sent"] = True
                entry["email_sent_at"] = datetime.now().isoformat()
                sent += 1
                log.info(f"Growth email → {p['buyer_email']} ({agency})")
                time.sleep(1)
            except Exception as e:
                entry["error"] = str(e)

        campaign["outreach"].append(entry)

    outreach.setdefault("campaigns", []).append(campaign)
    outreach["total_sent"] = outreach.get("total_sent", 0) + sent
    _save_json(OUTREACH_FILE, outreach)

    return {
        "ok": True, "campaign_id": campaign["id"], "dry_run": dry_run,
        "emails_built": len(new), "emails_sent": sent,
        "follow_up_date": (datetime.now() + timedelta(days=4)).strftime("%Y-%m-%d"),
        "preview": [{"to": o["email"], "agency": o["agency"], "subject": o["email_subject"]} for o in campaign["outreach"][:5]],
    }


# ═══════════════════════════════════════════════════════════════════════
# STEP 4: Voice Follow-Up
# ═══════════════════════════════════════════════════════════════════════

def check_follow_ups():
    """Find prospects who haven't responded after 3-5 business days."""
    outreach = _load_json(OUTREACH_FILE)
    if not isinstance(outreach, dict):
        return {"ok": True, "ready": [], "count": 0}

    now = datetime.now()
    ready = []
    for c in outreach.get("campaigns", []):
        if c.get("dry_run"):
            continue
        for o in c.get("outreach", []):
            if o.get("email_sent") and not o.get("response_received") and not o.get("voice_called"):
                try:
                    fdate = datetime.fromisoformat(o.get("voice_follow_up_date", ""))
                    if now >= fdate:
                        ready.append({"prospect_id": o["prospect_id"], "email": o["email"], "agency": o.get("agency", ""), "categories": o.get("categories", [])})
                except Exception:
                    pass

    return {"ok": True, "ready": ready, "count": len(ready)}


def launch_voice_follow_up(max_calls=10):
    """Auto-dial non-responders using voice agent."""
    fu = check_follow_ups()
    if not fu.get("ready"):
        return {"ok": True, "message": "No follow-ups due", "calls_made": 0}

    prospect_data = _load_json(PROSPECTS_FILE)
    pmap = {}
    if isinstance(prospect_data, dict):
        for p in prospect_data.get("prospects", []):
            pmap[p["id"]] = p

    calls = 0
    for target in fu["ready"][:max_calls]:
        prospect = pmap.get(target["prospect_id"], {})
        phone = prospect.get("buyer_phone", "")
        if not phone:
            continue
        try:
            from src.agents.voice_agent import place_call
            result = place_call(phone_number=phone, script_key="lead_intro", variables={
                "institution": target["agency"],
                "top_items": ", ".join(target.get("categories", ["supplies"])[:3]),
            })
            if result.get("ok"):
                calls += 1
                _mark_called(target["prospect_id"])
        except Exception as e:
            log.warning(f"Voice failed {target['agency']}: {e}")

    return {"ok": True, "calls_made": calls, "remaining": fu["count"] - calls}


def _mark_called(prospect_id):
    outreach = _load_json(OUTREACH_FILE)
    if not isinstance(outreach, dict):
        return
    for c in outreach.get("campaigns", []):
        for o in c.get("outreach", []):
            if o.get("prospect_id") == prospect_id:
                o["voice_called"] = True
                o["voice_called_at"] = datetime.now().isoformat()
    _save_json(OUTREACH_FILE, outreach)


# ═══════════════════════════════════════════════════════════════════════
# Status
# ═══════════════════════════════════════════════════════════════════════

def get_growth_status():
    history = _load_json(HISTORY_FILE)
    cats = _load_json(CATEGORIES_FILE)
    prospects = _load_json(PROSPECTS_FILE)
    outreach = _load_json(OUTREACH_FILE)
    h = history if isinstance(history, dict) else {}
    c = cats if isinstance(cats, dict) else {}
    p = prospects if isinstance(prospects, dict) else {}
    o = outreach if isinstance(outreach, dict) else {}
    return {
        "ok": True,
        "history": {"total_pos": h.get("total_pos", 0), "total_items": h.get("total_items", 0), "pulled_at": h.get("pulled_at")},
        "categories": {"total": c.get("total_categories", 0), "names": list(c.get("categories", {}).keys())[:7]},
        "prospects": {"total": p.get("total_prospects", 0), "generated_at": p.get("generated_at")},
        "outreach": {"total_sent": o.get("total_sent", 0), "campaigns": len(o.get("campaigns", []))},
        "pull_status": PULL_STATUS,
        "buyer_status": BUYER_STATUS,
    }


# Legacy compatibility
def generate_recommendations():
    return get_growth_status()

def full_report():
    return get_growth_status()

def lead_funnel():
    return get_growth_status()
