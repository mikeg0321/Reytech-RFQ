"""
sales_intel.py — Sales Intelligence Engine for Reytech
Phase 26 | Version: 1.0.0

The brain behind the $2M revenue target.

Data Architecture:
  1. SCPRS Deep Pull: Every buyer, every agency, every item, every dollar
  2. Buyer Database: Contacts tagged by item categories + spend amounts
  3. Priority Scoring: High spend + NOT our customer = top priority
  4. SB Admin Lookup: Small Business liaison contacts at each agency
  5. Revenue Tracker: YTD pipeline vs $2M goal
  6. Outreach Queue: Prioritized list of who to contact next

Storage:
  data/intel_buyers.json       — Master buyer DB (agency → contacts → items → spend)
  data/intel_agencies.json     — Agency profiles (total spend, categories, SB contacts)
  data/intel_revenue.json      — Revenue tracking toward goal
"""

import json, os, re, logging, time, uuid
from datetime import datetime, timedelta
from collections import defaultdict

log = logging.getLogger("sales_intel")
# ── Shared DB Context (Anthropic Skills Guide: Pattern 5 — Domain Intelligence) ──
# Gives this agent access to live CRM, quotes, revenue, price history from SQLite.
# Eliminates file loading duplication and ensures consistent ground truth.
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



try:
    from src.core.paths import DATA_DIR
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")

BUYERS_FILE = os.path.join(DATA_DIR, "intel_buyers.json")  # Legacy - contacts now in SQLite
AGENCIES_FILE = os.path.join(DATA_DIR, "intel_agencies.json")
REVENUE_FILE = os.path.join(DATA_DIR, "intel_revenue.json")

try:
    from src.agents.scprs_lookup import _get_session
    HAS_SCPRS = True
except ImportError:
    HAS_SCPRS = False

try:
    from src.agents.growth_agent import (
        categorize_item, CATEGORY_KEYWORDS, _load_json, _save_json,
        PROSPECTS_FILE, OUTREACH_FILE, HISTORY_FILE,
    )
    HAS_GROWTH = True
except ImportError:
    HAS_GROWTH = False
    def categorize_item(d): return "General"
    CATEGORY_KEYWORDS = {}
    def _load_json(p):
        try:
            with open(p) as f: return json.load(f)
        except: return []
    def _save_json(p, d):
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w") as f: json.dump(d, f, indent=2, default=str)

REVENUE_GOAL = 2_000_000  # $2M annual target


# ═══════════════════════════════════════════════════════════════════════
# SCPRS DEEP PULL — All Buyers, All Items, All Spend
# ═══════════════════════════════════════════════════════════════════════

# Search categories that cover Reytech's addressable market
SEARCH_QUERIES = [
    # Medical / PPE
    "gloves nitrile", "exam gloves", "surgical gown", "face mask N95",
    "bandage gauze", "syringe needle", "catheter", "first aid kit",
    "hand sanitizer", "disinfectant wipes", "thermometer",
    # Janitorial
    "trash bags liners", "cleaning supplies", "paper towels",
    "toilet paper tissue", "mop broom", "floor cleaner",
    "soap dispenser", "bleach disinfectant",
    # Office
    "copy paper", "toner cartridge", "ink cartridge",
    "pens pencils", "file folders binders", "envelopes",
    "labels", "stapler tape", "sticky notes",
    # IT / Electronics
    "batteries alkaline", "USB cable", "keyboard mouse",
    "printer paper", "toner HP", "toner Brother",
    # Safety
    "safety glasses", "ear plugs", "hard hat",
    "fire extinguisher", "safety vest",
    # Facility
    "light bulbs LED", "HVAC filter", "paint supplies",
    "plumbing supplies", "tools hardware",
]

DEEP_PULL_STATUS = {
    "running": False, "phase": "", "progress": "",
    "queries_done": 0, "queries_total": 0,
    "total_pos": 0, "total_buyers": 0, "total_agencies": 0,
    "errors": [], "started_at": None, "finished_at": None,
}


def deep_pull_all_buyers(from_date="01/01/2019", max_queries=None, max_detail_per_query=20):
    """
    Massive SCPRS pull: search every product category + Reytech supplier history,
    drill into POs, extract every buyer + agency + item + price.

    Strategy:
      1. First search by supplier_name="Reytech" to get OUR full win history
      2. Then search all product category keywords to find competitor buyers
      3. Tag buyers by item type, annual spend, and agency
      4. Score and rank — highest spend + not our customer = top priority

    Builds the master intelligence database going back to 2019.
    """
    if not HAS_SCPRS:
        return {"ok": False, "error": "SCPRS not available"}
    if DEEP_PULL_STATUS["running"]:
        return {"ok": False, "error": "Already running", "status": DEEP_PULL_STATUS}

    queries = SEARCH_QUERIES[:max_queries] if max_queries else SEARCH_QUERIES
    to_date = datetime.now().strftime("%m/%d/%Y")

    # Reytech supplier name variants to search our own win history
    REYTECH_NAMES = ["Reytech", "Rey Tech", "REYTECH"]

    DEEP_PULL_STATUS.update({
        "running": True, "phase": "init", "progress": "Connecting to SCPRS (suppliers.fiscal.ca.gov)...",
        "queries_done": 0, "queries_total": len(queries) + len(REYTECH_NAMES),
        "total_pos": 0, "total_buyers": 0, "total_agencies": 0,
        "errors": [], "started_at": datetime.now().isoformat(), "finished_at": None,
    })

    try:
        session = _get_session()
        if not session.initialized and not session.init_session():
            err_msg = "SCPRS connection blocked — Railway static IP not enabled. Go to Railway → Settings → Networking → Static IP → Enable."
            DEEP_PULL_STATUS.update({
                "running": False, "phase": "error",
                "progress": err_msg,
                "errors": [err_msg],
                "finished_at": datetime.now().isoformat(),
            })
            return {"ok": False, "error": err_msg}

        # Master collections
        buyers = {}     # email → buyer record
        agencies = {}   # dept_code → agency record
        all_pos = []

        def _process_results(results, q_idx, total_q, label):
            """Shared result processor for both Reytech and category searches."""
            DEEP_PULL_STATUS["total_pos"] += len(results)
            for r_idx, r in enumerate(results):
                po_num = r.get("po_number", "")
                dept = r.get("dept", "").strip()
                supplier = r.get("supplier_name", "")
                email = (r.get("buyer_email") or "").strip().lower()
                total = r.get("grand_total_num") or 0
                first_item = r.get("first_item", "")
                date = r.get("start_date", "")
                is_reytech = "reytech" in (supplier or "").lower() or "rey tech" in (supplier or "").lower()
                category = categorize_item(first_item)

                # Drill detail for more POs to get buyer names
                buyer_name = ""
                line_items = []
                if r_idx < max_detail_per_query and r.get("_results_html") and r.get("_row_index") is not None:
                    try:
                        detail = session.get_detail(r["_results_html"], r["_row_index"], r.get("_click_action"))
                        if detail:
                            hdr = detail.get("header", {}) if isinstance(detail.get("header"), dict) else {}
                            buyer_name = hdr.get("buyer_name", "")
                            email = (hdr.get("buyer_email") or email or "").strip().lower()
                            line_items = detail.get("line_items", [])
                        time.sleep(0.2)
                    except Exception as e:
                        DEEP_PULL_STATUS["errors"].append(f"Detail {po_num}: {e}")

                # Build agency record
                if dept and dept not in agencies:
                    agencies[dept] = {
                        "dept_code": dept,
                        "total_spend": 0, "po_count": 0,
                        "categories": {}, "buyers": {}, "suppliers": {},
                        "is_customer": False, "reytech_spend": 0,
                    }
                if dept:
                    ag = agencies[dept]
                    ag["total_spend"] += total
                    ag["po_count"] += 1
                    ag["categories"][category] = ag["categories"].get(category, 0) + total
                    if is_reytech:
                        ag["is_customer"] = True
                        ag["reytech_spend"] += total
                    if supplier:
                        ag["suppliers"][supplier] = ag["suppliers"].get(supplier, 0) + total

                # Build buyer record
                buyer_key = email or f"anon_{dept}_{po_num}"
                if buyer_key not in buyers:
                    buyers[buyer_key] = {
                        "id": f"BUY-{uuid.uuid4().hex[:8]}",
                        "email": email, "name": buyer_name, "agency": dept,
                        "total_spend": 0, "po_count": 0,
                        "categories": {}, "items_purchased": [], "purchase_orders": [],
                        "is_reytech_customer": False, "reytech_spend": 0,
                        "last_purchase": "",
                    }
                b = buyers[buyer_key]
                b["total_spend"] += total
                b["po_count"] += 1
                b["categories"][category] = b["categories"].get(category, 0) + total
                if not b["name"] and buyer_name:
                    b["name"] = buyer_name
                if is_reytech:
                    b["is_reytech_customer"] = True
                    b["reytech_spend"] += total
                if date > b.get("last_purchase", ""):
                    b["last_purchase"] = date
                if len(b["purchase_orders"]) < 20:
                    b["purchase_orders"].append({
                        "po_number": po_num, "date": date,
                        "total": total, "supplier": supplier,
                        "items": first_item[:100], "category": category,
                    })
                for li in line_items[:5]:
                    desc = li.get("description", "")[:80]
                    if desc and len(b["items_purchased"]) < 50:
                        b["items_purchased"].append({
                            "description": desc,
                            "unit_price": li.get("unit_price_num"),
                            "category": categorize_item(desc),
                        })
                if dept and email:
                    agencies[dept]["buyers"][email] = {
                        "name": buyer_name or b.get("name", ""),
                        "spend": b["total_spend"],
                    }

        # ── Phase 1: Pull Reytech's own win history (marks our customers) ──
        DEEP_PULL_STATUS["phase"] = "reytech_history"
        for rt_idx, rt_name in enumerate(REYTECH_NAMES):
            DEEP_PULL_STATUS["progress"] = f"[Phase 1] Pulling Reytech PO history: '{rt_name}'..."
            try:
                results = session.search(supplier_name=rt_name, from_date=from_date, to_date=to_date)
                log.info(f"Reytech '{rt_name}': {len(results)} POs found")
                _process_results(results, rt_idx, len(REYTECH_NAMES), f"Reytech:{rt_name}")
                time.sleep(0.5)
            except Exception as e:
                log.warning(f"Reytech search '{rt_name}' failed: {e}")
                DEEP_PULL_STATUS["errors"].append(f"Reytech:{rt_name}: {e}")

        reytech_agencies = [a for a in agencies.values() if a.get("is_customer")]
        log.info(f"Phase 1 done: {len(reytech_agencies)} agencies are Reytech customers")
        DEEP_PULL_STATUS["progress"] = f"Phase 1 done — {len(reytech_agencies)} Reytech customer agencies found. Starting category scan..."

        # ── Phase 2: Search all product categories (finds competitor buyers) ──
        DEEP_PULL_STATUS["phase"] = "category_scan"
        for q_idx, query in enumerate(queries):
            DEEP_PULL_STATUS.update({
                "progress": f"[Phase 2 — {q_idx+1}/{len(queries)}] Category scan: '{query}'",
                "queries_done": q_idx + len(REYTECH_NAMES),
            })

            try:
                results = session.search(description=query, from_date=from_date, to_date=to_date)
                _process_results(results, q_idx, len(queries), query)
                time.sleep(0.6)  # Be polite to SCPRS
            except Exception as e:
                log.warning(f"Query '{query}' failed: {e}")
                DEEP_PULL_STATUS["errors"].append(f"{query}: {e}")

        DEEP_PULL_STATUS["total_buyers"] = len(buyers)
        DEEP_PULL_STATUS["total_agencies"] = len(agencies)

        # Score and rank buyers
        buyer_list = _score_buyers(list(buyers.values()))

        # Score agencies
        agency_list = _score_agencies(list(agencies.values()))

        # Save
        _save_json(BUYERS_FILE, {
            "generated_at": datetime.now().isoformat(),
            "from_date": from_date,
            "total_buyers": len(buyer_list),
            "total_agencies": len(agency_list),
            "queries_searched": len(queries),
            "buyers": buyer_list,
        })

        _save_json(AGENCIES_FILE, {
            "generated_at": datetime.now().isoformat(),
            "total_agencies": len(agency_list),
            "agencies": agency_list,
        })

        DEEP_PULL_STATUS.update({
            "running": False, "phase": "complete",
            "queries_done": len(queries),
            "progress": f"Done: {len(buyer_list)} buyers, {len(agency_list)} agencies from {DEEP_PULL_STATUS['total_pos']} POs",
            "finished_at": datetime.now().isoformat(),
        })

        return {
            "ok": True,
            "buyers": len(buyer_list),
            "agencies": len(agency_list),
            "total_pos_scanned": DEEP_PULL_STATUS["total_pos"],
            "queries_searched": len(queries),
            "top_opportunity_agencies": [
                {"agency": a["dept_code"], "spend": a["total_spend"], "score": a.get("opportunity_score", 0)}
                for a in agency_list[:10] if not a.get("is_customer")
            ],
        }

    except Exception as e:
        err_str = str(e)
        if "ProxyError" in err_str or "403" in err_str or "ConnectionPool" in err_str or "Max retries" in err_str:
            err_msg = f"SCPRS network blocked ({err_str[:80]}). Enable Railway static IP: Settings → Networking → Static IP."
        else:
            err_msg = err_str
        DEEP_PULL_STATUS.update({
            "running": False, "phase": "error",
            "progress": err_msg,
            "errors": DEEP_PULL_STATUS.get("errors", []) + [err_msg],
            "finished_at": datetime.now().isoformat(),
        })
        return {"ok": False, "error": err_msg}


def _score_buyers(buyers):
    """Score buyers by opportunity value."""
    for b in buyers:
        score = 0
        spend = b.get("total_spend", 0)

        # Spend score (0-40 pts)
        if spend >= 100000: score += 40
        elif spend >= 50000: score += 30
        elif spend >= 20000: score += 25
        elif spend >= 10000: score += 20
        elif spend >= 5000: score += 15
        elif spend >= 1000: score += 10
        else: score += 5

        # Category overlap with Reytech (0-25 pts)
        reytech_cats = set(CATEGORY_KEYWORDS.keys())
        buyer_cats = set(b.get("categories", {}).keys())
        overlap = len(reytech_cats & buyer_cats)
        score += min(25, overlap * 5)

        # NOT our customer = bigger opportunity (0-20 pts)
        if not b.get("is_reytech_customer"):
            score += 20
        else:
            score += 5  # Upsell opportunity

        # Has email (0-10 pts) — can actually reach them
        if b.get("email"):
            score += 10

        # Recency (0-5 pts)
        lp = b.get("last_purchase", "")
        if lp:
            try:
                from datetime import datetime as dt
                lpd = dt.strptime(lp, "%m/%d/%Y")
                days = (dt.now() - lpd).days
                if days < 90: score += 5
                elif days < 180: score += 3
                elif days < 365: score += 1
            except: pass

        b["opportunity_score"] = score

    buyers.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
    return buyers


def _score_agencies(agencies):
    """Score agencies by total opportunity."""
    for a in agencies:
        score = 0
        spend = a.get("total_spend", 0)

        # Total spend (0-40 pts)
        if spend >= 500000: score += 40
        elif spend >= 200000: score += 35
        elif spend >= 100000: score += 30
        elif spend >= 50000: score += 25
        elif spend >= 20000: score += 20
        else: score += 10

        # Category diversity (0-20 pts)
        score += min(20, len(a.get("categories", {})) * 4)

        # NOT our customer = biggest opportunity (0-25 pts)
        if not a.get("is_customer"):
            score += 25
        else:
            # Upsell: gap between their spend and our spend
            gap = spend - a.get("reytech_spend", 0)
            if gap > 100000: score += 15
            elif gap > 50000: score += 10
            elif gap > 10000: score += 5

        # Multiple buyers = easier to get in (0-10 pts)
        score += min(10, len(a.get("buyers", {})) * 2)

        # Has buyer emails (0-5 pts)
        has_emails = sum(1 for e in a.get("buyers", {}).keys() if "@" in str(e))
        if has_emails: score += 5

        a["opportunity_score"] = score

    agencies.sort(key=lambda x: x.get("opportunity_score", 0), reverse=True)
    return agencies


# ═══════════════════════════════════════════════════════════════════════
# SB ADMIN / LIAISON CONTACTS
# ═══════════════════════════════════════════════════════════════════════

# Known SB liaison contacts by department (seed data — expand via web scrape)
SB_ADMIN_CONTACTS = {
    "CDCR": {"name": "Small Business Office", "email": "CDCR.SmallBusiness@cdcr.ca.gov", "phone": "", "title": "SB/DVBE Advocate"},
    "Caltrans": {"name": "Small Business Program", "email": "sb.advocate@dot.ca.gov", "phone": "", "title": "SB Advocate"},
    "DGS": {"name": "Office of Small Business and DVBE Services", "email": "osbds@dgs.ca.gov", "phone": "(916) 375-4940", "title": "OSDS"},
    "CalVet": {"name": "Small Business Office", "email": "", "phone": "", "title": "SB Coordinator"},
    "DMV": {"name": "Small Business Program", "email": "", "phone": "", "title": "SB Advocate"},
    "CHP": {"name": "Small Business Office", "email": "", "phone": "", "title": "SB Coordinator"},
    "DSH": {"name": "Small Business Office", "email": "", "phone": "", "title": "SB Coordinator"},
}

# Agency name patterns → department code
AGENCY_CODE_MAP = {
    "cdcr": "CDCR", "csp": "CDCR", "cim": "CDCR", "scc": "CDCR",
    "prison": "CDCR", "corrections": "CDCR", "rehabilitation": "CDCR",
    "calvet": "CalVet", "veterans": "CalVet",
    "caltrans": "Caltrans", "transportation": "Caltrans",
    "dgs": "DGS", "general services": "DGS",
    "dmv": "DMV", "motor vehicles": "DMV",
    "chp": "CHP", "highway patrol": "CHP",
    "dsh": "DSH", "state hospital": "DSH",
    "calfire": "CalFire", "fire": "CalFire",
    "cdph": "CDPH", "public health": "CDPH",
    "dof": "DOF", "finance": "DOF",
}


def get_sb_admin(agency_name: str) -> dict:
    """Find the SB admin/liaison for a given agency."""
    name_lower = agency_name.lower()

    # Direct match
    if agency_name in SB_ADMIN_CONTACTS:
        return {"ok": True, "contact": SB_ADMIN_CONTACTS[agency_name], "agency": agency_name}

    # Pattern match
    for pattern, code in AGENCY_CODE_MAP.items():
        if pattern in name_lower:
            if code in SB_ADMIN_CONTACTS:
                return {"ok": True, "contact": SB_ADMIN_CONTACTS[code], "agency": code}
            return {"ok": True, "contact": {"name": f"{code} Small Business Office", "email": "", "phone": "", "title": "SB Advocate"}, "agency": code}

    return {"ok": False, "message": f"No SB admin found for '{agency_name}'. Try searching DGS OSDS for a referral.", "suggestion": SB_ADMIN_CONTACTS.get("DGS")}


def find_sb_admin_for_agencies() -> dict:
    """Match SB admin contacts to all agencies in the intel database."""
    data = _load_json(AGENCIES_FILE)
    if not isinstance(data, dict):
        return {"ok": False, "error": "No agency data. Run deep_pull first."}

    matched = 0
    unmatched = []
    for ag in data.get("agencies", []):
        result = get_sb_admin(ag.get("dept_code", ""))
        if result.get("ok"):
            ag["sb_admin"] = result["contact"]
            ag["sb_agency_code"] = result.get("agency", "")
            matched += 1
        else:
            unmatched.append(ag.get("dept_code", ""))
            ag["sb_admin"] = None

    _save_json(AGENCIES_FILE, data)
    return {"ok": True, "matched": matched, "unmatched": len(unmatched), "unmatched_agencies": unmatched[:20]}


# ═══════════════════════════════════════════════════════════════════════
# REVENUE TRACKER — $2M Goal
# ═══════════════════════════════════════════════════════════════════════

def update_revenue_tracker() -> dict:
    """Aggregate all revenue data toward the $2M goal."""
    # Sources: quotes won, QB data, manual entries
    revenue = _load_json(REVENUE_FILE)
    if not isinstance(revenue, dict):
        revenue = {"goal": REVENUE_GOAL, "year": 2026, "entries": [], "manual_entries": []}

    # Pull from quotes
    try:
        quotes_data = _load_json(os.path.join(DATA_DIR, "quotes_log.json"))
        if isinstance(quotes_data, list):
            won = [q for q in quotes_data if q.get("status") == "won" and not q.get("is_test")]
            quotes_revenue = sum(q.get("total", 0) for q in won)
        else:
            quotes_revenue = 0
    except:
        quotes_revenue = 0

    # Pull from QB if available
    qb_revenue = 0
    try:
        from src.agents.qb_agent import get_financial_context, qb_configured
        if qb_configured():
            ctx = get_financial_context()
            if ctx.get("ok"):
                qb_revenue = ctx.get("total_collected", 0)
    except:
        pass

    # Manual entries
    manual_total = sum(e.get("amount", 0) for e in revenue.get("manual_entries", []))

    # Pipeline value (pending + sent quotes)
    try:
        if isinstance(quotes_data, list):
            pipeline = sum(q.get("total", 0) for q in quotes_data
                          if q.get("status") in ("pending", "sent") and not q.get("is_test"))
        else:
            pipeline = 0
    except:
        pipeline = 0

    # Growth prospects pipeline
    prospects_data = _load_json(PROSPECTS_FILE) if HAS_GROWTH else {}
    growth_pipeline = 0
    if isinstance(prospects_data, dict):
        for p in prospects_data.get("prospects", []):
            if p.get("outreach_status") in ("responded", "won"):
                growth_pipeline += p.get("total_spend", 0) * 0.1  # 10% capture estimate

    closed = max(quotes_revenue, qb_revenue, manual_total)
    total_pipeline = pipeline + growth_pipeline

    now = datetime.now()
    days_in_year = 366 if now.year % 4 == 0 else 365
    day_of_year = now.timetuple().tm_yday
    pct_year_elapsed = day_of_year / days_in_year
    run_rate = (closed / pct_year_elapsed) if pct_year_elapsed > 0 else 0
    gap = REVENUE_GOAL - closed
    monthly_needed = gap / max(1, 12 - now.month + 1)

    tracker = {
        "goal": REVENUE_GOAL,
        "year": now.year,
        "closed_revenue": round(closed, 2),
        "pipeline_value": round(total_pipeline, 2),
        "growth_pipeline": round(growth_pipeline, 2),
        "quotes_won_value": round(quotes_revenue, 2),
        "qb_collected": round(qb_revenue, 2),
        "manual_entries_total": round(manual_total, 2),
        "pct_to_goal": round(closed / REVENUE_GOAL * 100, 1),
        "gap_to_goal": round(gap, 2),
        "monthly_needed": round(monthly_needed, 2),
        "run_rate_annual": round(run_rate, 2),
        "on_track": run_rate >= REVENUE_GOAL * 0.9,
        "pct_year_elapsed": round(pct_year_elapsed * 100, 1),
        "updated_at": now.isoformat(),
    }

    revenue.update(tracker)
    _save_json(REVENUE_FILE, revenue)
    return {"ok": True, **tracker}


def add_manual_revenue(amount: float, description: str, date: str = "") -> dict:
    """Add a manual revenue entry (for deals closed outside the system)."""
    revenue = _load_json(REVENUE_FILE)
    if not isinstance(revenue, dict):
        revenue = {"goal": REVENUE_GOAL, "year": 2026, "entries": [], "manual_entries": []}
    revenue.setdefault("manual_entries", []).append({
        "id": f"REV-{uuid.uuid4().hex[:6]}",
        "amount": float(amount),
        "description": description,
        "date": date or datetime.now().strftime("%Y-%m-%d"),
        "added_at": datetime.now().isoformat(),
    })
    _save_json(REVENUE_FILE, revenue)
    return {"ok": True, "message": f"Added ${amount:,.2f}: {description}"}


# ═══════════════════════════════════════════════════════════════════════
# MANUAL BUYER MANAGEMENT — Works without SCPRS
# ═══════════════════════════════════════════════════════════════════════

def add_manual_buyer(agency: str, buyer_email: str, buyer_name: str = "",
                     buyer_phone: str = "", categories: list = None,
                     annual_spend: float = 0, notes: str = "") -> dict:
    """Manually add a buyer to the intel database without needing SCPRS.
    Useful when you already know a contact at an agency.
    """
    buyers_data = _load_json(BUYERS_FILE)
    if not isinstance(buyers_data, dict):
        buyers_data = {"buyers": [], "total_buyers": 0, "updated_at": datetime.now().isoformat()}

    buyer_email = (buyer_email or "").strip().lower()
    if not buyer_email or not agency:
        return {"ok": False, "error": "agency and buyer_email are required"}

    # Check for existing
    buyers = buyers_data.get("buyers", [])
    for b in buyers:
        if b.get("email", "").lower() == buyer_email:
            # Update existing
            b.update({
                "name": buyer_name or b.get("name", ""),
                "phone": buyer_phone or b.get("phone", ""),
                "agency": agency,
                "total_spend": annual_spend or b.get("total_spend", 0),
                "notes": notes or b.get("notes", ""),
                "updated_at": datetime.now().isoformat(),
            })
            if categories:
                for cat in categories:
                    b.setdefault("categories", {})[cat] = annual_spend / max(len(categories), 1)
            buyers_data["updated_at"] = datetime.now().isoformat()
            _save_json(BUYERS_FILE, buyers_data)
            return {"ok": True, "action": "updated", "buyer_email": buyer_email}

    # Create new
    bid = f"manual-{uuid.uuid4().hex[:8]}"
    cat_dict = {}
    if categories:
        per_cat = annual_spend / max(len(categories), 1)
        for cat in categories:
            cat_dict[cat] = per_cat

    new_buyer = {
        "id": bid,
        "email": buyer_email,
        "buyer_email": buyer_email,
        "name": buyer_name,
        "buyer_name": buyer_name,
        "phone": buyer_phone,
        "buyer_phone": buyer_phone,
        "agency": agency,
        "total_spend": annual_spend,
        "categories": cat_dict,
        "items_purchased": [],
        "purchase_orders": [],
        "po_count": 0,
        "last_purchase": "",
        "opportunity_score": min(100, int(annual_spend / 1000)),
        "score": min(1.0, annual_spend / 100000),
        "is_reytech_customer": False,
        "outreach_status": "new",
        "notes": notes,
        "source": "manual",
        "added_at": datetime.now().isoformat(),
    }
    buyers.append(new_buyer)
    buyers_data["buyers"] = buyers
    buyers_data["total_buyers"] = len(buyers)
    buyers_data["updated_at"] = datetime.now().isoformat()
    _save_json(BUYERS_FILE, buyers_data)

    # Also update agencies file
    agencies_data = _load_json(AGENCIES_FILE)
    if not isinstance(agencies_data, dict):
        agencies_data = {"agencies": [], "total_agencies": 0}
    agencies = agencies_data.get("agencies", [])
    existing_ag = next((a for a in agencies if a.get("dept_code") == agency), None)
    if existing_ag:
        existing_ag.setdefault("buyers", {})[buyer_email] = True
        existing_ag["total_spend"] = existing_ag.get("total_spend", 0) + annual_spend
    else:
        agencies.append({
            "dept_code": agency, "total_spend": annual_spend,
            "buyers": {buyer_email: True},
            "categories": cat_dict,
            "is_customer": False, "opportunity_score": new_buyer["opportunity_score"],
        })
    agencies_data["agencies"] = agencies
    agencies_data["total_agencies"] = len(agencies)
    _save_json(AGENCIES_FILE, agencies_data)

    # Auto-sync new buyer to CRM immediately
    sync_buyers_to_crm()
    return {"ok": True, "action": "created", "buyer_id": bid, "buyer_email": buyer_email}


def sync_buyers_to_crm() -> dict:
    """Sync all intel buyers into crm_contacts.json.
    Preserves manual fields (phone, title, linkedin, notes, activity).
    Updates SCPRS intel fields (spend, categories, items, POs).
    Called automatically after seed, add, import, and deep pull.
    """
    buyers_data = _load_json(BUYERS_FILE)
    if not isinstance(buyers_data, dict):
        return {"ok": False, "error": "No buyer data to sync"}

    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(crm_path) as f:
            contacts = json.load(f)
        if not isinstance(contacts, dict):
            contacts = {}
    except (FileNotFoundError, json.JSONDecodeError):
        contacts = {}

    created = 0
    updated = 0
    for b in buyers_data.get("buyers", []):
        bid = b.get("id", "")
        if not bid:
            continue
        email = (b.get("email","") or b.get("buyer_email","")).strip().lower()
        if bid in contacts:
            # Update intel fields only — never touch manual fields
            c = contacts[bid]
            c["total_spend"]      = b.get("total_spend", c.get("total_spend", 0))
            c["po_count"]         = b.get("po_count", c.get("po_count", 0))
            c["categories"]       = b.get("categories", c.get("categories", {}))
            c["items_purchased"]  = b.get("items_purchased", c.get("items_purchased", []))
            c["purchase_orders"]  = b.get("purchase_orders", c.get("purchase_orders", []))
            c["last_purchase"]    = b.get("last_purchase", c.get("last_purchase", ""))
            c["score"]            = b.get("score", c.get("score", 0))
            c["opportunity_score"]= b.get("opportunity_score", c.get("opportunity_score", 0))
            c["is_reytech_customer"] = b.get("is_reytech_customer", False)
            c["intel_synced_at"]  = datetime.now().isoformat()
            c["updated_at"]       = datetime.now().isoformat()
            # Update name/email/phone only if currently empty
            if not c.get("buyer_name") and b.get("name"):
                c["buyer_name"] = b["name"]
            if not c.get("buyer_email") and email:
                c["buyer_email"] = email
            if not c.get("buyer_phone") and b.get("phone"):
                c["buyer_phone"] = b["phone"]
            updated += 1
        else:
            # Create new CRM record from buyer
            contacts[bid] = {
                "id": bid,
                "created_at": datetime.now().isoformat(),
                "buyer_name": b.get("name","") or b.get("buyer_name",""),
                "buyer_email": email,
                "buyer_phone": b.get("phone","") or b.get("buyer_phone",""),
                "agency": b.get("agency",""),
                "title": "",
                "linkedin": "",
                "notes": b.get("notes",""),
                "tags": [],
                "total_spend": b.get("total_spend", 0),
                "po_count": b.get("po_count", 0),
                "categories": b.get("categories", {}),
                "items_purchased": b.get("items_purchased", []),
                "purchase_orders": b.get("purchase_orders", []),
                "last_purchase": b.get("last_purchase",""),
                "score": b.get("score", 0),
                "opportunity_score": b.get("opportunity_score", 0),
                "is_reytech_customer": b.get("is_reytech_customer", False),
                "outreach_status": b.get("outreach_status","new"),
                "activity": [],
                "intel_synced_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
                "source": b.get("source","intel"),
            }
            created += 1

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(crm_path, "w") as f:
        json.dump(contacts, f, indent=2, default=str)

    return {
        "ok": True,
        "created": created,
        "updated": updated,
        "total_contacts": len(contacts),
        "message": f"Synced {created + updated} buyers → CRM ({created} new, {updated} updated)",
    }


def import_buyers_csv(csv_text: str) -> dict:
    """Import buyers from CSV text.
    Expected columns (any order, case-insensitive):
    agency, email, name, phone, categories, annual_spend, notes
    """
    import csv, io
    lines = csv_text.strip().splitlines()
    if not lines:
        return {"ok": False, "error": "Empty CSV"}

    reader = csv.DictReader(io.StringIO(csv_text))
    created = 0
    updated = 0
    errors = []

    for row_num, row in enumerate(reader, 2):
        # Normalize keys
        r = {k.strip().lower().replace(" ", "_"): v.strip() for k, v in row.items() if v}
        agency = r.get("agency", "")
        email = r.get("email", "")
        if not agency or not email:
            errors.append(f"Row {row_num}: missing agency or email")
            continue
        try:
            cats_raw = r.get("categories", "")
            cats = [c.strip() for c in cats_raw.split(",") if c.strip()] if cats_raw else []
            spend = float(r.get("annual_spend", "0").replace(",","").replace("$","") or 0)
            result = add_manual_buyer(
                agency=agency, buyer_email=email,
                buyer_name=r.get("name","") or r.get("buyer_name",""),
                buyer_phone=r.get("phone","") or r.get("buyer_phone",""),
                categories=cats, annual_spend=spend,
                notes=r.get("notes",""),
            )
            if result.get("action") == "created": created += 1
            elif result.get("action") == "updated": updated += 1
            else: errors.append(f"Row {row_num}: {result.get('error','')}")
        except Exception as e:
            errors.append(f"Row {row_num}: {e}")

    result = {
        "ok": True,
        "created": created,
        "updated": updated,
        "errors": errors,
        "message": f"Imported {created} new + {updated} updated buyers. {len(errors)} errors.",
    }
    # Auto-sync to CRM
    sync = sync_buyers_to_crm()
    result["crm_synced"] = sync.get("created", 0) + sync.get("updated", 0)
    result["message"] += f" Synced {result['crm_synced']} to CRM."
    return result


def seed_demo_data() -> dict:
    """Seed the intel database with realistic CA state agency demo data.
    Used when SCPRS is unreachable — shows the full UI working with real-looking data.
    Clears existing data first.
    """
    demo_buyers = [
        {"agency":"CDCR","email":"james.chen@cdcr.ca.gov","name":"James Chen","phone":"916-445-2182","categories":["Medical","Safety"],"annual_spend":142000},
        {"agency":"CDCR","email":"sarah.morales@cdcr.ca.gov","name":"Sarah Morales","phone":"916-445-3391","categories":["Janitorial","Facility"],"annual_spend":89500},
        {"agency":"CalTrans","email":"m.nguyen@dot.ca.gov","name":"Mike Nguyen","phone":"916-654-2852","categories":["Safety","Office"],"annual_spend":215000},
        {"agency":"CalTrans","email":"linda.park@dot.ca.gov","name":"Linda Park","phone":"916-654-5000","categories":["IT","Office"],"annual_spend":78000},
        {"agency":"CDPH","email":"r.thompson@cdph.ca.gov","name":"Robert Thompson","phone":"916-558-1784","categories":["Medical","Safety"],"annual_spend":330000},
        {"agency":"CDPH","email":"amy.wilson@cdph.ca.gov","name":"Amy Wilson","phone":"916-558-2900","categories":["Medical","Janitorial"],"annual_spend":145000},
        {"agency":"DSS","email":"kevin.ortiz@dss.ca.gov","name":"Kevin Ortiz","phone":"916-651-8848","categories":["Office","Facility"],"annual_spend":67000},
        {"agency":"CHP","email":"t.garcia@chp.ca.gov","name":"Tony Garcia","phone":"916-657-7095","categories":["Safety","Medical"],"annual_spend":198000},
        {"agency":"CalFire","email":"b.johnson@fire.ca.gov","name":"Bill Johnson","phone":"916-653-5123","categories":["Safety","Facility"],"annual_spend":412000},
        {"agency":"DGS","email":"patricia.lee@dgs.ca.gov","name":"Patricia Lee","phone":"916-375-4100","categories":["Office","IT","Facility"],"annual_spend":95000},
        {"agency":"CDFA","email":"d.martinez@cdfa.ca.gov","name":"Diana Martinez","phone":"916-900-5022","categories":["Safety","Janitorial"],"annual_spend":53000},
        {"agency":"DMV","email":"e.robinson@dmv.ca.gov","name":"Eric Robinson","phone":"916-657-6565","categories":["Office","IT"],"annual_spend":44000},
        {"agency":"EDD","email":"c.white@edd.ca.gov","name":"Chris White","phone":"916-654-7000","categories":["Office","Facility"],"annual_spend":118000},
        {"agency":"OSHPD","email":"jennifer.brown@oshpd.ca.gov","name":"Jennifer Brown","phone":"916-326-3000","categories":["Medical","Safety"],"annual_spend":267000},
        {"agency":"Caltrans-D4","email":"m.taylor@dot.ca.gov","name":"Michael Taylor","phone":"510-286-5600","categories":["Safety","Facility"],"annual_spend":88000},
    ]

    created = 0
    for b in demo_buyers:
        result = add_manual_buyer(
            agency=b["agency"], buyer_email=b["email"],
            buyer_name=b["name"], buyer_phone=b["phone"],
            categories=b["categories"], annual_spend=b["annual_spend"],
        )
        if result.get("ok"): created += 1

    # Auto-sync to CRM immediately
    sync = sync_buyers_to_crm()
    return {
        "ok": True,
        "created": created,
        "crm_synced": sync.get("created", 0),
        "message": f"Seeded {created} demo CA state agency buyers + synced {sync.get('created',0)} to CRM. Run Deep Pull on Railway to replace with real SCPRS data.",
        "note": "demo_data",
    }


def delete_buyer(buyer_id: str = None, buyer_email: str = None) -> dict:
    """Delete a buyer from the intel database by ID or email."""
    buyers_data = _load_json(BUYERS_FILE)
    if not isinstance(buyers_data, dict):
        return {"ok": False, "error": "No buyer data"}
    buyers = buyers_data.get("buyers", [])
    original_len = len(buyers)
    if buyer_id:
        buyers = [b for b in buyers if b.get("id") != buyer_id]
    elif buyer_email:
        buyers = [b for b in buyers if b.get("email","").lower() != buyer_email.lower()]
    else:
        return {"ok": False, "error": "buyer_id or buyer_email required"}
    if len(buyers) == original_len:
        return {"ok": False, "error": "Buyer not found"}
    buyers_data["buyers"] = buyers
    buyers_data["total_buyers"] = len(buyers)
    _save_json(BUYERS_FILE, buyers_data)
    return {"ok": True, "deleted": original_len - len(buyers)}




def get_priority_queue(limit=25) -> dict:
    """Generate prioritized outreach queue from all intelligence data."""
    buyers_data = _load_json(BUYERS_FILE)
    if not isinstance(buyers_data, dict):
        return {"ok": False, "error": "No buyer data. Run deep_pull first."}

    # Load existing growth prospects to avoid duplicates
    contacted = set()
    if HAS_GROWTH:
        prospects = _load_json(PROSPECTS_FILE)
        if isinstance(prospects, dict):
            for p in prospects.get("prospects", []):
                if p.get("buyer_email"):
                    contacted.add(p["buyer_email"].lower())
        outreach = _load_json(OUTREACH_FILE)
        if isinstance(outreach, dict):
            for c in outreach.get("campaigns", []):
                for o in c.get("outreach", []):
                    if o.get("email"):
                        contacted.add(o["email"].lower())

    queue = []
    for b in buyers_data.get("buyers", []):
        email = (b.get("email") or "").lower()
        if not email or email in contacted:
            continue
        if b.get("is_reytech_customer"):
            continue  # Focus on new business

        # Build priority entry
        top_cats = sorted(b.get("categories", {}).items(), key=lambda x: x[1], reverse=True)[:3]
        queue.append({
            "buyer_id": b.get("id"),
            "email": b.get("email"),
            "name": b.get("name", ""),
            "agency": b.get("agency"),
            "total_spend": b.get("total_spend", 0),
            "opportunity_score": b.get("opportunity_score", 0),
            "top_categories": [c[0] for c in top_cats],
            "top_category_spend": {c[0]: round(c[1], 2) for c in top_cats},
            "po_count": b.get("po_count", 0),
            "last_purchase": b.get("last_purchase", ""),
            "items_sample": [i.get("description", "")[:60] for i in b.get("items_purchased", [])[:3]],
        })

    queue.sort(key=lambda x: x["opportunity_score"], reverse=True)

    # Revenue potential estimate
    total_addressable = sum(q["total_spend"] for q in queue)
    capture_10pct = total_addressable * 0.10

    return {
        "ok": True,
        "queue": queue[:limit],
        "total_in_queue": len(queue),
        "total_addressable_spend": round(total_addressable, 2),
        "estimated_capture_10pct": round(capture_10pct, 2),
        "already_contacted": len(contacted),
    }


def push_to_growth_prospects(buyer_ids: list = None, top_n: int = 50) -> dict:
    """Push top priority buyers into the Growth Agent prospect pipeline for outreach."""
    if not HAS_GROWTH:
        return {"ok": False, "error": "Growth agent not available"}

    from src.agents.growth_agent import _save_json as save_prospects

    buyers_data = _load_json(BUYERS_FILE)
    if not isinstance(buyers_data, dict):
        return {"ok": False, "error": "No buyer data"}

    # Load existing prospects
    prospects_data = _load_json(PROSPECTS_FILE)
    if not isinstance(prospects_data, dict):
        prospects_data = {"prospects": [], "generated_at": datetime.now().isoformat(), "total_prospects": 0}
    existing_emails = {p.get("buyer_email", "").lower() for p in prospects_data.get("prospects", [])}

    added = 0
    buyers = buyers_data.get("buyers", [])

    for b in buyers:
        if buyer_ids and b.get("id") not in buyer_ids:
            continue
        if not buyer_ids and added >= top_n:
            break

        email = (b.get("email") or "").lower()
        if not email or email in existing_emails:
            continue
        if b.get("is_reytech_customer"):
            continue

        prospect = {
            "id": f"PRO-{uuid.uuid4().hex[:8]}",
            "buyer_email": email,
            "buyer_name": b.get("name", ""),
            "buyer_phone": "",
            "agency": b.get("agency", ""),
            "categories_matched": list(b.get("categories", {}).keys())[:5],
            "purchase_orders": b.get("purchase_orders", [])[:10],
            "total_spend": b.get("total_spend", 0),
            "outreach_status": "new",
            "source": "sales_intel",
            "opportunity_score": b.get("opportunity_score", 0),
        }
        prospects_data["prospects"].append(prospect)
        existing_emails.add(email)
        added += 1

    prospects_data["total_prospects"] = len(prospects_data["prospects"])
    save_prospects(PROSPECTS_FILE, prospects_data)

    return {"ok": True, "added": added, "total_prospects": prospects_data["total_prospects"]}


# ═══════════════════════════════════════════════════════════════════════
# STATUS & DASHBOARD
# ═══════════════════════════════════════════════════════════════════════

def get_intel_status() -> dict:
    buyers = _load_json(BUYERS_FILE)
    agencies = _load_json(AGENCIES_FILE)
    revenue = update_revenue_tracker()

    b = buyers if isinstance(buyers, dict) else {}
    a = agencies if isinstance(agencies, dict) else {}

    # Top opportunity agencies (not our customers)
    top_opps = []
    if isinstance(a, dict):
        for ag in a.get("agencies", [])[:20]:
            if not ag.get("is_customer"):
                top_opps.append({
                    "agency": ag.get("dept_code"),
                    "spend": ag.get("total_spend", 0),
                    "score": ag.get("opportunity_score", 0),
                    "categories": list(ag.get("categories", {}).keys())[:3],
                    "sb_admin": ag.get("sb_admin"),
                })
            if len(top_opps) >= 5:
                break

    return {
        "ok": True,
        "buyers": {"total": b.get("total_buyers", 0), "generated_at": b.get("generated_at")},
        "agencies": {"total": a.get("total_agencies", 0)},
        "revenue": revenue,
        "top_opportunity_agencies": top_opps,
        "pull_status": DEEP_PULL_STATUS,
    }
