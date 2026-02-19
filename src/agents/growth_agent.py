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

# ── Agent Context (Anthropic Skills Guide: Pattern 5 — Domain Intelligence) ──
try:
    from src.core.agent_context import get_context, get_contact_by_agency, format_context_for_agent
    HAS_CTX = True
except ImportError:
    HAS_CTX = False
    def get_context(**kw): return {}
    def get_contact_by_agency(a): return []
    def format_context_for_agent(c, **kw): return ""


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

def pull_reytech_history(from_date="01/01/2019", to_date=""):
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
                        po["buyer_phone"] = hdr.get("buyer_phone", "")
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

def find_category_buyers(max_categories=10, from_date="01/01/2019"):
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
                                "buyer_phone": "",
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

            # Get buyer names + phone from detail on a few results
            try:
                detail_results = results if results else []
            except NameError:
                detail_results = []
            for r in detail_results[:3]:
                if r.get("_results_html") and r.get("_row_index") is not None:
                    try:
                        detail = session.get_detail(r["_results_html"], r["_row_index"], r.get("_click_action"))
                        if detail:
                            hdr = detail.get("header", {}) if isinstance(detail.get("header"), dict) else {}
                            em = hdr.get("buyer_email", "")
                            if em and em in prospects:
                                prospects[em]["buyer_name"] = hdr.get("buyer_name", "") or prospects[em]["buyer_name"]
                                prospects[em]["buyer_phone"] = hdr.get("buyer_phone", "") or prospects[em]["buyer_phone"]
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

# ── Email Template Library (PRD Feature 4.3) ─────────────────────────────
# Implements Anthropic Skills Guide Pattern 5: Domain-specific intelligence
# Templates are personalized from CRM/intel DB context

EMAIL_TEMPLATES = {
    "distro_list": """Hi{name_greeting},

My name is Mike from Reytech Inc. I wanted to reach out because {agency} recently purchased {items_mention} — we supply those same categories and consistently offer competitive pricing on SCPRS.

We're a California-certified Small Business (SB) and Disabled Veteran Business Enterprise (DVBE), which helps meet your procurement mandates under CalTrans and DGS guidelines. We've been serving CA state agencies for several years.

I'd love the opportunity to get on {agency}'s RFQ distribution list so we can submit quotes for your next procurement. We turn around quotes quickly and our prices are typically 10–30% below current contract rates.

You can add us at sales@reytechinc.com or call/text 949-229-1575. I'm happy to provide our vendor registration, certifications, or a product catalog on request.

Thank you for your time.

Best regards,
Mike
Reytech Inc. | SB/DVBE Certified
sales@reytechinc.com | 949-229-1575
www.reytechinc.com""",

    "initial_outreach": """Hi{name_greeting},

This is Mike from Reytech Inc. We noticed that {agency} recently purchased {items_mention} ({purchase_date}). We carry those same types of items and often have more competitive pricing — typically 10–30% below current contract rates.

We're a certified Small Business (SB) and Disabled Veteran Business Enterprise (DVBE), which helps meet your procurement mandates. We'd love to get on your RFQ distribution list for future bids.

Please reach us at sales@reytechinc.com or 949-229-1575.

Best regards,
Mike | Reytech Inc. | SB/DVBE
sales@reytechinc.com | 949-229-1575""",

    "follow_up": """Hi{name_greeting},

Following up on my recent email about Reytech Inc. We specialize in {items_mention} for California state agencies and are a certified SB/DVBE vendor.

I wanted to make sure you received my previous note and reiterate our interest in being added to {agency}'s RFQ distribution list. We're ready to quote on your next procurement.

Feel free to reply or call/text 949-229-1575.

Best,
Mike | Reytech Inc. | SB/DVBE""",

    "quote_won": """Hi{name_greeting},

Thank you for the award on {items_mention}. Your order is being processed and you'll receive tracking information once shipped.

We look forward to continuing to serve {agency}. For your next procurement, please keep us on your distribution list — we're always competitive on pricing and delivery.

Best regards,
Mike | Reytech Inc.
sales@reytechinc.com | 949-229-1575""",
}

# Keep backward-compat alias
EMAIL_TEMPLATE = EMAIL_TEMPLATES["initial_outreach"]




def launch_distro_campaign(
    max_contacts: int = 100,
    dry_run: bool = True,
    template: str = "distro_list",
    source_filter: str = "",
) -> dict:
    """Phase 1 Growth Campaign: Email CA state agency buyers to get on RFQ distro lists.

    This is the primary growth lever per the PRD:
      'Get on RFQ distribution list — Phase 1'
      Target: ~100 buyers from SCPRS intel, personalized by agency/category.

    Uses the Anthropic Skills Guide Pattern 5 (Domain Intelligence):
      Pulls live DB context → personalizes each email from actual purchase history →
      logs every touch to activity_log → deduplicates against already-contacted.

    Args:
        max_contacts: Max emails to send (or stage in dry_run)
        dry_run: True = build emails but DO NOT send (review first)
        template: Template key from EMAIL_TEMPLATES
        source_filter: Only contact buyers from this agency/source substring

    Returns:
        {ok, campaign_id, total_staged, total_sent, emails[], dry_run, context_used}
    """
    log.info("launch_distro_campaign: dry_run=%s template=%s max=%d", dry_run, template, max_contacts)

    # ── Pull DB context (agent intelligence layer) ─────────────────────────
    ctx = get_context(include_contacts=True, include_revenue=True, include_quotes=True)
    ctx_summary = format_context_for_agent(ctx, focus="crm")

    # ── Load all buyers from intel + CRM ───────────────────────────────────
    all_buyers = []

    # Source 1: Intel buyers (SCPRS-sourced)
    intel_data = _load_json(os.path.join(DATA_DIR, "intel_buyers.json"))
    if isinstance(intel_data, dict):
        for b in intel_data.get("buyers", []):
            if b.get("buyer_email") and "@" in b.get("buyer_email", ""):
                all_buyers.append({
                    "id": b.get("id", f"intel_{len(all_buyers)}"),
                    "name": b.get("buyer_name", ""),
                    "email": b.get("buyer_email", ""),
                    "agency": b.get("agency", ""),
                    "categories": b.get("categories", []),
                    "spend": b.get("annual_spend", 0),
                    "source": "intel",
                    "items_mention": ", ".join(list(b.get("categories", []) or [])[:2]) or "medical supplies",
                    "purchase_date": "recently",
                })

    # Source 2: Growth prospects
    prospect_data = _load_json(PROSPECTS_FILE)
    if isinstance(prospect_data, dict):
        for p in prospect_data.get("prospects", []):
            if p.get("buyer_email") and "@" in p.get("buyer_email", ""):
                pos = p.get("purchase_orders", [])
                items = pos[0].get("items", ", ".join(p.get("categories_matched", [])))[:80] if pos else ", ".join(p.get("categories_matched", []))[:80]
                date = pos[0].get("date", "recently") if pos else "recently"
                all_buyers.append({
                    "id": p.get("id", f"prospect_{len(all_buyers)}"),
                    "name": p.get("buyer_name", ""),
                    "email": p.get("buyer_email", ""),
                    "agency": p.get("agency", ""),
                    "categories": p.get("categories_matched", []),
                    "spend": p.get("estimated_spend", 0),
                    "source": "prospect",
                    "items_mention": items or "supplies",
                    "purchase_date": date,
                })

    # Source 3: CRM contacts with email not yet emailed
    for c in ctx.get("contacts", []):
        if c.get("email") and "@" in c.get("email", "") and c.get("status") == "new":
            if not any(b["email"] == c["email"] for b in all_buyers):
                cats = list(c.get("categories", []) or [])
                all_buyers.append({
                    "id": c["id"],
                    "name": c.get("name", ""),
                    "email": c.get("email", ""),
                    "agency": c.get("agency", ""),
                    "categories": cats,
                    "spend": c.get("spend", 0),
                    "source": "crm",
                    "items_mention": ", ".join(list(cats or [])[:2]) or "your recent purchases",
                    "purchase_date": "recently",
                })

    # ── Deduplicate against previously contacted ───────────────────────────
    outreach_data = _load_json(OUTREACH_FILE)
    if not isinstance(outreach_data, dict):
        outreach_data = {"campaigns": [], "total_sent": 0}

    already_contacted = set()
    for camp in outreach_data.get("campaigns", []):
        for o in camp.get("outreach", []):
            if o.get("email_sent") or o.get("staged"):
                already_contacted.add(o.get("email", ""))

    # Apply filters
    if source_filter:
        sf = source_filter.lower()
        all_buyers = [b for b in all_buyers if sf in (b.get("agency") or "").lower()
                      or sf in (b.get("source") or "").lower()]

    new_buyers = [b for b in all_buyers if b["email"] not in already_contacted]
    new_buyers = new_buyers[:max_contacts]

    if not new_buyers:
        return {
            "ok": True,
            "message": f"All {len(all_buyers)} contacts already contacted (or no contacts found). Run Intel Deep Pull to find more.",
            "total_staged": 0,
            "total_sent": 0,
            "dry_run": dry_run,
        }

    # ── Build campaign ─────────────────────────────────────────────────────
    campaign_id = f"DISTRO-{datetime.now().strftime('%Y%m%d-%H%M')}"
    email_template = EMAIL_TEMPLATES.get(template, EMAIL_TEMPLATES["distro_list"])
    campaign = {
        "id": campaign_id,
        "type": "distro_list_phase1",
        "created_at": datetime.now().isoformat(),
        "dry_run": dry_run,
        "template": template,
        "context_summary": ctx_summary[:500],
        "outreach": [],
    }
    staged = 0
    sent = 0
    emails = []

    for b in new_buyers:
        name = b.get("name", "")
        name_greeting = f" {name.split()[0]}" if name else ""
        agency = b.get("agency", "your agency")
        items_mention = b.get("items_mention", "supplies")
        purchase_date = b.get("purchase_date", "recently")
        cats = list(b.get("categories", []) or [])
        if isinstance(cats, str): cats = [cats] if cats else []

        # Personalize subject by category
        if "Medical" in str(cats) or "medical" in items_mention.lower():
            subject = f"Reytech Inc. — Competitive Pricing on Medical Supplies for {agency}"
        elif "Janitorial" in str(cats) or "cleaning" in items_mention.lower():
            subject = f"Reytech Inc. — Janitorial & Cleaning Supplies — SB/DVBE Vendor"
        elif cats:
            subject = f"Reytech Inc. — {cats[0]} Pricing — SB/DVBE — Get on RFQ Distro"
        else:
            subject = f"Reytech Inc. — CA State Vendor Introduction — SB/DVBE Certified"

        body = email_template.format(
            name_greeting=name_greeting,
            agency=agency,
            items_mention=items_mention,
            purchase_date=purchase_date,
        )

        entry = {
            "buyer_id": b["id"],
            "email": b["email"],
            "name": name,
            "agency": agency,
            "categories": cats,
            "subject": subject,
            "body": body,
            "staged": True,
            "email_sent": False,
            "email_sent_at": None,
            "campaign_id": campaign_id,
            "follow_up_date": (datetime.now() + timedelta(days=5)).isoformat(),
        }

        if not dry_run and b.get("email"):
            try:
                from src.agents.email_poller import EmailSender
                gmail = os.environ.get("GMAIL_ADDRESS", "")
                pwd = os.environ.get("GMAIL_PASSWORD", "")
                if gmail and pwd:
                    sender = EmailSender({"email": gmail, "email_password": pwd})
                    sender.send({"to": b["email"], "subject": subject, "body": body, "attachments": []})
                    entry["email_sent"] = True
                    entry["email_sent_at"] = datetime.now().isoformat()
                    sent += 1
                    log.info("Distro email SENT → %s (%s)", b["email"], agency)
                    _add_event(b["id"], "distro_email_sent", f"Phase 1 distro campaign: {subject}")
                    time.sleep(1.2)  # rate limit
                else:
                    entry["error"] = "GMAIL_ADDRESS / GMAIL_PASSWORD not set in Railway env"
            except Exception as e:
                entry["error"] = str(e)
                log.error("Email send failed %s: %s", b["email"], e)
        else:
            log.info("Distro email STAGED → %s (%s) [dry_run]", b["email"], agency)

        campaign["outreach"].append(entry)
        emails.append({
            "to": b["email"],
            "name": name,
            "agency": agency,
            "subject": subject,
            "body_preview": body[:120] + "...",
            "sent": entry["email_sent"],
        })
        staged += 1

    # ── Save campaign ──────────────────────────────────────────────────────
    outreach_data.setdefault("campaigns", []).append(campaign)
    outreach_data["total_sent"] = outreach_data.get("total_sent", 0) + sent
    outreach_data["last_distro_campaign"] = campaign_id
    _save_json(OUTREACH_FILE, outreach_data)

    # Log to DB activity
    try:
        from src.core.db import log_activity as _la
        _la(
            contact_id="growth_agent",
            event_type="distro_campaign_launched",
            detail=f"Campaign {campaign_id}: {staged} staged, {sent} sent, dry_run={dry_run}",
            actor="growth_agent",
            metadata={"campaign_id": campaign_id, "staged": staged, "sent": sent, "template": template},
        )
    except Exception:
        pass

    log.info("launch_distro_campaign: %s — staged=%d sent=%d dry_run=%s",
             campaign_id, staged, sent, dry_run)

    result = {
        "ok": True,
        "campaign_id": campaign_id,
        "total_available": len(all_buyers),
        "already_contacted": len(already_contacted),
        "total_staged": staged,
        "total_sent": sent,
        "dry_run": dry_run,
        "template": template,
        "emails": emails,
        "follow_up_in_days": 5,
        "next_step": "Review emails in growth outreach dashboard, then set dry_run=False to send" if dry_run else f"Sent {sent} emails. Follow up in 5 days.",
        "context_used": bool(ctx.get("contacts")),
    }
    return result


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

        body = EMAIL_TEMPLATE.format(name_greeting=name_greeting, agency=agency, items_mention=items_mention, purchase_date=purchase_date)
        cats_list = list(cats or []) if not isinstance(cats, str) else [cats]
        subject = f"Reytech Inc. — Competitive Pricing on {', '.join(cats_list[:2])}" if cats_list else "Reytech Inc. — CA State Vendor Introduction"

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
                from src.agents.email_poller import EmailSender
                config = {"email": os.environ.get("GMAIL_ADDRESS", ""), "email_password": os.environ.get("GMAIL_PASSWORD", "")}
                if config["email"] and config["email_password"]:
                    sender = EmailSender(config)
                    sender.send({"to": p["buyer_email"], "subject": subject, "body": body, "attachments": []})
                    entry["email_sent"] = True
                    entry["email_sent_at"] = datetime.now().isoformat()
                    _update_prospect_status(p["id"], "emailed")
                    _add_event(p["id"], "email_sent", f"Sent: {subject}")
                    sent += 1
                    log.info(f"Growth email → {p['buyer_email']} ({agency})")
                    time.sleep(1)
                else:
                    entry["error"] = "Gmail not configured"
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
            result = place_call(phone_number=phone, script_key="growth_email_follow_up", variables={
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


# ═══════════════════════════════════════════════════════════════════════
# PROSPECT CRM — Contact Management + Timeline
# ═══════════════════════════════════════════════════════════════════════

TIMELINE_FILE = os.path.join(DATA_DIR, "growth_timeline.json")

# Status flow: new → emailed → follow_up_due → called → responded | bounced | dead
VALID_STATUSES = ["new", "emailed", "follow_up_due", "called", "responded", "bounced", "dead", "won"]

def _load_timeline() -> dict:
    """Load timeline events keyed by prospect_id."""
    data = _load_json(TIMELINE_FILE)
    return data if isinstance(data, dict) else {}

def _save_timeline(data: dict):
    _save_json(TIMELINE_FILE, data)

def _add_event(prospect_id: str, event_type: str, detail: str = "", metadata: dict = None):
    """Add a timeline event for a prospect."""
    timeline = _load_timeline()
    events = timeline.setdefault(prospect_id, [])
    events.append({
        "type": event_type,
        "detail": detail,
        "timestamp": datetime.now().isoformat(),
        "metadata": metadata or {},
    })
    _save_timeline(timeline)


def _update_prospect_status(prospect_id: str, new_status: str):
    """Update a prospect's outreach_status in the prospects file."""
    data = _load_json(PROSPECTS_FILE)
    if not isinstance(data, dict):
        return
    for p in data.get("prospects", []):
        if p.get("id") == prospect_id:
            old = p.get("outreach_status", "new")
            p["outreach_status"] = new_status
            p["status_updated_at"] = datetime.now().isoformat()
            _save_json(PROSPECTS_FILE, data)
            _add_event(prospect_id, "status_change", f"{old} → {new_status}")
            return
    log.warning(f"Prospect {prospect_id} not found for status update")


def get_prospect(prospect_id: str) -> dict:
    """Get a single prospect with full timeline."""
    data = _load_json(PROSPECTS_FILE)
    if not isinstance(data, dict):
        return {"ok": False, "error": "No prospects data"}
    for p in data.get("prospects", []):
        if p.get("id") == prospect_id:
            timeline = _load_timeline().get(prospect_id, [])
            # Also pull outreach records
            outreach = _load_json(OUTREACH_FILE)
            outreach_records = []
            if isinstance(outreach, dict):
                for c in outreach.get("campaigns", []):
                    for o in c.get("outreach", []):
                        if o.get("prospect_id") == prospect_id:
                            outreach_records.append(o)
            return {
                "ok": True, "prospect": p,
                "timeline": sorted(timeline, key=lambda e: e.get("timestamp", ""), reverse=True),
                "outreach_records": outreach_records,
            }
    return {"ok": False, "error": "Prospect not found"}


def update_prospect(prospect_id: str, updates: dict) -> dict:
    """Update prospect fields (name, phone, email, status, notes)."""
    data = _load_json(PROSPECTS_FILE)
    if not isinstance(data, dict):
        return {"ok": False, "error": "No prospects data"}
    for p in data.get("prospects", []):
        if p.get("id") == prospect_id:
            changed = []
            for key in ["buyer_name", "buyer_phone", "buyer_email", "outreach_status", "notes"]:
                if key in updates and updates[key] != p.get(key):
                    old_val = p.get(key, "")
                    p[key] = updates[key]
                    changed.append(f"{key}: {old_val} → {updates[key]}")
            if changed:
                p["updated_at"] = datetime.now().isoformat()
                _save_json(PROSPECTS_FILE, data)
                _add_event(prospect_id, "updated", "; ".join(changed))
            return {"ok": True, "changed": changed}
    return {"ok": False, "error": "Prospect not found"}


def add_prospect_note(prospect_id: str, note: str) -> dict:
    """Add a manual note to a prospect's timeline."""
    if not note.strip():
        return {"ok": False, "error": "Note cannot be empty"}
    _add_event(prospect_id, "note", note)
    return {"ok": True}


def mark_responded(prospect_id: str, response_type: str = "email_reply", detail: str = "") -> dict:
    """Mark a prospect as having responded."""
    _update_prospect_status(prospect_id, "responded")
    _add_event(prospect_id, "response_received", detail or response_type, {"response_type": response_type})
    # Update outreach records
    outreach = _load_json(OUTREACH_FILE)
    if isinstance(outreach, dict):
        for c in outreach.get("campaigns", []):
            for o in c.get("outreach", []):
                if o.get("prospect_id") == prospect_id:
                    o["response_received"] = True
                    o["response_at"] = datetime.now().isoformat()
                    o["response_type"] = response_type
        _save_json(OUTREACH_FILE, outreach)
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════════════
# BOUNCEBACK HANDLING
# ═══════════════════════════════════════════════════════════════════════

BOUNCE_KEYWORDS = [
    "delivery status notification", "undeliverable", "mail delivery failed",
    "returned mail", "delivery failure", "address rejected",
    "mailbox not found", "user unknown", "no such user",
    "mailbox unavailable", "permanent failure", "550 ",
    "message not delivered", "delivery has failed",
]

def detect_bounceback(subject: str, body: str, sender: str = "") -> dict:
    """Check if an email is a bounceback notification."""
    text = f"{subject} {body}".lower()
    sender_lower = sender.lower()

    # Check sender patterns
    is_bounce_sender = any(s in sender_lower for s in [
        "mailer-daemon", "postmaster", "mail-daemon", "noreply",
        "no-reply", "bounce", "delivery",
    ])

    # Check content
    bounce_signals = sum(1 for kw in BOUNCE_KEYWORDS if kw in text)

    if bounce_signals >= 2 or (is_bounce_sender and bounce_signals >= 1):
        # Try to extract the original recipient
        recipient = ""
        # Common patterns: "The email to <user@example.com> failed"
        import re
        email_match = re.search(r'[\w.+-]+@[\w-]+\.[\w.-]+', body)
        if email_match:
            recipient = email_match.group(0)

        return {
            "is_bounce": True,
            "confidence": min(1.0, bounce_signals * 0.3 + (0.4 if is_bounce_sender else 0)),
            "recipient": recipient,
            "signals": bounce_signals,
        }

    return {"is_bounce": False, "confidence": 0}


def process_bounceback(email_address: str, reason: str = "") -> dict:
    """Mark a prospect as bounced and exclude from future outreach."""
    data = _load_json(PROSPECTS_FILE)
    if not isinstance(data, dict):
        return {"ok": False, "error": "No prospects data"}

    found = None
    for p in data.get("prospects", []):
        if p.get("buyer_email", "").lower() == email_address.lower():
            found = p
            break

    if not found:
        return {"ok": False, "error": f"No prospect with email {email_address}"}

    found["outreach_status"] = "bounced"
    found["bounced_at"] = datetime.now().isoformat()
    found["bounce_reason"] = reason
    _save_json(PROSPECTS_FILE, data)

    _add_event(found["id"], "email_bounced", reason or f"Bounce: {email_address}")

    # Mark in outreach campaigns too
    outreach = _load_json(OUTREACH_FILE)
    if isinstance(outreach, dict):
        for c in outreach.get("campaigns", []):
            for o in c.get("outreach", []):
                if o.get("email", "").lower() == email_address.lower():
                    o["bounced"] = True
                    o["bounce_reason"] = reason
        _save_json(OUTREACH_FILE, outreach)

    log.info(f"Bounce processed: {email_address} ({reason})")
    return {"ok": True, "prospect_id": found["id"], "agency": found.get("agency")}


def scan_inbox_for_bounces() -> dict:
    """Check inbox for bounceback emails and process them."""
    try:
        from src.agents.email_poller import EmailSender
        config = {
            "email": os.environ.get("GMAIL_ADDRESS", ""),
            "email_password": os.environ.get("GMAIL_PASSWORD", ""),
        }
        if not config["email"] or not config["email_password"]:
            return {"ok": False, "error": "Gmail not configured"}
        sender = EmailSender(config)
    except ImportError:
        return {"ok": False, "error": "EmailSender not available"}

    bounces_found = 0
    try:
        # Check recent emails for bouncebacks
        import imaplib, email as email_lib
        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(config["email"], config["email_password"])
        imap.select("INBOX")

        # Search last 7 days
        from_date = (datetime.now() - timedelta(days=7)).strftime("%d-%b-%Y")
        _, msg_ids = imap.search(None, f'(SINCE "{from_date}" FROM "mailer-daemon")')

        # Also check for delivery failure subjects
        _, msg_ids2 = imap.search(None, f'(SINCE "{from_date}" SUBJECT "delivery")')

        all_ids = set()
        for ids in [msg_ids[0], msg_ids2[0]]:
            if ids:
                all_ids.update(ids.split())

        for msg_id in list(all_ids)[:50]:
            _, data = imap.fetch(msg_id, "(RFC822)")
            msg = email_lib.message_from_bytes(data[0][1])
            subject = str(msg.get("Subject", ""))
            sender_addr = str(msg.get("From", ""))
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        try:
                            body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                        except Exception:
                            pass
                        break
            else:
                try:
                    body = msg.get_payload(decode=True).decode("utf-8", errors="replace")
                except Exception:
                    pass

            bounce = detect_bounceback(subject, body, sender_addr)
            if bounce.get("is_bounce") and bounce.get("recipient"):
                result = process_bounceback(bounce["recipient"], f"Auto-detected: {subject[:60]}")
                if result.get("ok"):
                    bounces_found += 1

        imap.logout()

    except Exception as e:
        log.warning(f"Bounce scan error: {e}")
        return {"ok": False, "error": str(e), "bounces_found": bounces_found}

    return {"ok": True, "bounces_found": bounces_found}


# ═══════════════════════════════════════════════════════════════════════
# AUTO-SCHEDULER — Background Follow-Up Engine
# ═══════════════════════════════════════════════════════════════════════

_scheduler_thread = None
_scheduler_stop = threading.Event()
SCHEDULER_INTERVAL = 3600  # Check every hour

def _scheduler_loop():
    """Background loop: check bounces + initiate voice follow-ups."""
    log.info("Growth scheduler started (every %ds)", SCHEDULER_INTERVAL)
    while not _scheduler_stop.is_set():
        try:
            # 1. Scan for bouncebacks
            bounce_result = scan_inbox_for_bounces()
            if bounce_result.get("bounces_found", 0) > 0:
                log.info(f"Scheduler: {bounce_result['bounces_found']} bounces processed")

            # 2. Update statuses for follow-up-due prospects
            outreach = _load_json(OUTREACH_FILE)
            now = datetime.now()
            due_count = 0
            if isinstance(outreach, dict):
                for c in outreach.get("campaigns", []):
                    if c.get("dry_run"):
                        continue
                    for o in c.get("outreach", []):
                        if (o.get("email_sent")
                            and not o.get("response_received")
                            and not o.get("voice_called")
                            and not o.get("bounced")
                            and o.get("voice_follow_up_date")):
                            try:
                                fdate = datetime.fromisoformat(o["voice_follow_up_date"])
                                if now >= fdate:
                                    due_count += 1
                                    _update_prospect_status(o["prospect_id"], "follow_up_due")
                            except Exception:
                                pass

            if due_count > 0:
                log.info(f"Scheduler: {due_count} prospects now due for voice follow-up")

        except Exception as e:
            log.debug(f"Scheduler error: {e}")

        _scheduler_stop.wait(SCHEDULER_INTERVAL)


def start_scheduler():
    """Start background CRM scheduler."""
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _scheduler_stop.clear()
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True, name="growth-scheduler")
    _scheduler_thread.start()


def get_campaign_dashboard() -> dict:
    """Campaign management overview with full metrics."""
    outreach = _load_json(OUTREACH_FILE)
    if not isinstance(outreach, dict):
        return {"ok": True, "campaigns": [], "totals": {}}

    prospects_data = _load_json(PROSPECTS_FILE)
    prospect_map = {}
    if isinstance(prospects_data, dict):
        for p in prospects_data.get("prospects", []):
            prospect_map[p["id"]] = p

    # Per-status counts
    status_counts = defaultdict(int)
    for p in prospect_map.values():
        status_counts[p.get("outreach_status", "new")] += 1

    campaigns = []
    total_sent = 0
    total_bounced = 0
    total_responded = 0
    total_called = 0
    total_pending = 0

    for c in outreach.get("campaigns", []):
        camp_sent = sum(1 for o in c.get("outreach", []) if o.get("email_sent"))
        camp_bounced = sum(1 for o in c.get("outreach", []) if o.get("bounced"))
        camp_responded = sum(1 for o in c.get("outreach", []) if o.get("response_received"))
        camp_called = sum(1 for o in c.get("outreach", []) if o.get("voice_called"))
        camp_pending = camp_sent - camp_bounced - camp_responded - camp_called

        campaigns.append({
            "id": c.get("id"),
            "created_at": c.get("created_at"),
            "dry_run": c.get("dry_run", True),
            "total": len(c.get("outreach", [])),
            "sent": camp_sent,
            "bounced": camp_bounced,
            "responded": camp_responded,
            "called": camp_called,
            "pending": max(0, camp_pending),
        })

        total_sent += camp_sent
        total_bounced += camp_bounced
        total_responded += camp_responded
        total_called += camp_called
        total_pending += max(0, camp_pending)

    return {
        "ok": True,
        "campaigns": campaigns,
        "totals": {
            "sent": total_sent,
            "bounced": total_bounced,
            "responded": total_responded,
            "called": total_called,
            "pending_follow_up": total_pending,
            "total_prospects": len(prospect_map),
        },
        "status_breakdown": dict(status_counts),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SCPRS-POWERED GROWTH INTELLIGENCE  (Phase 32 upgrade)
# Reads live SCPRS data → gap analysis → ranked recommendations → outreach actions
# ═══════════════════════════════════════════════════════════════════════════════

def get_scprs_growth_intelligence() -> dict:
    """
    Primary intelligence function. Reads SCPRS DB data and returns:
    - Ranked action list (what Mike should do TODAY vs this week vs this month)
    - Gap analysis (what agencies buy that we don't sell them)
    - Win-back targets (we sell it, they're buying from Cardinal/McKesson)
    - Pricing lessons from auto-closed lost quotes
    - Agency expansion opportunities
    """
    try:
        from src.agents.scprs_intelligence_engine import get_growth_intelligence
        data = get_growth_intelligence()
    except Exception as e:
        return {"ok": False, "error": str(e), "recommendations": [], "gaps": [], "win_back": []}

    recs = data.get("recommendations", [])
    gaps = data.get("top_gaps", [])
    win_back = data.get("win_back", [])
    competitors = data.get("competitors", [])
    losses = data.get("recent_losses", [])
    by_agency = data.get("by_agency", [])

    # Compute total opportunity value
    total_gap = sum(g.get("total_spend") or 0 for g in gaps)
    total_wb = sum(w.get("total_spend") or 0 for w in win_back)

    # Compute pull coverage
    agencies_with_data = [a for a in by_agency if (a.get("po_count") or 0) > 0]

    return {
        "ok": True,
        "recommendations": recs,
        "gaps": gaps,
        "win_back": win_back,
        "competitors": competitors,
        "recent_losses": losses,
        "by_agency": by_agency,
        "summary": {
            "total_gap_opportunity": total_gap,
            "total_win_back": total_wb,
            "total_opportunity": total_gap + total_wb,
            "agencies_with_data": len(agencies_with_data),
            "rec_count": len(recs),
        },
        "pull_schedule": data.get("pull_schedule", []),
    }


def generate_recommendations() -> dict:
    """Upgraded: returns SCPRS-powered recommendations, falls back to legacy."""
    scprs = get_scprs_growth_intelligence()
    if scprs.get("ok") and scprs.get("recommendations"):
        return scprs
    return get_growth_status()


def full_report() -> dict:
    """Full growth report with SCPRS intelligence."""
    return generate_recommendations()


def get_plain_english_brief() -> str:
    """
    Returns a plain English brief for the manager dashboard.
    Answers: 'What should I do today to grow revenue?'
    Includes DVBE angle and unconstrained sourcing opportunities.
    """
    intel = get_scprs_growth_intelligence()
    if not intel.get("ok"):
        return "Run a full SCPRS pull to generate growth insights."

    lines = []
    recs = intel.get("recommendations", [])
    summary = intel.get("summary", {})

    total_opp = summary.get("total_opportunity", 0)
    agencies = summary.get("agencies_with_data", 0)
    if total_opp > 0:
        lines.append(
            f"💰 ${total_opp:,.0f} in identified opportunities across {agencies} agencies. "
            f"This includes items you already sell AND items you can source. "
            f"Your DVBE cert is the wildcard — use it."
        )

    # P0 actions — what to do today
    p0_recs = [r for r in recs if r.get("priority") == "P0"][:4]
    icons = {"dvbe_displace": "🏅", "win_back": "⚔️", "add_product": "📦",
             "pricing": "💲", "dvbe_partner": "🤝", "source_anything": "🔍"}
    for r in p0_recs:
        val = r.get("estimated_annual_value", 0)
        icon = icons.get(r.get("type", ""), "🎯")
        lines.append(f"{icon} {r.get('action','')} — ${val:,.0f}/yr estimate. {r.get('how','')[:90]}")

    # DVBE summary
    dvbe_recs = [r for r in recs if r.get("dvbe_angle")]
    if dvbe_recs:
        dvbe_val = sum(r.get("estimated_annual_value",0) for r in dvbe_recs)
        lines.append(
            f"🏅 {len(dvbe_recs)} DVBE opportunities worth ~${dvbe_val:,.0f}/yr — "
            f"Cardinal Health, McKesson and others don't have your DVBE cert. "
            f"Lead with it in every proposal to CCHCS, CalVet, DSH."
        )

    # Pricing lessons from auto-closed losses
    losses = intel.get("recent_losses", [])
    if losses:
        l = losses[0]
        delta = (l.get("total") or 0) - (l.get("scprs_total") or 0)
        if delta > 0:
            lines.append(
                f"📉 Lost {l.get('quote_number','')} to {l.get('scprs_supplier','')} "
                f"— overpriced by ${delta:,.0f}. SCPRS saved the intel. Fix pricing before next RFQ."
            )

    if not lines:
        lines.append(
            "No SCPRS data yet. Hit 'Pull All Agencies' on the Growth Intel page — "
            "it searches every PO every CA agency placed and shows you exactly what to sell."
        )

    return "\n".join(lines)

