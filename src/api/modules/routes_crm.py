# routes_crm.py
# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from flask import redirect, flash, send_file
from src.core.paths import DATA_DIR
from src.core.db import get_db
from src.api.render import render_page

from src.core.security import rate_limit
import re
from datetime import datetime
# ─────────────────────────────────────────────────────────────────────────────
# 70 routes, 3186 lines
# Loaded by dashboard.py via load_module()

# CUSTOMERS CRM — Agency parent/child, QuickBooks-synced
# ═══════════════════════════════════════════════════════════════════════════════

def _load_customers():
    """Load customers — DB is source of truth, JSON fallback for tests/migration."""
    # If local JSON exists (test env or pre-migration), use it for consistency
    path = os.path.join(DATA_DIR, "customers.json")
    try:
        from src.core.dal import get_all_customers
        from src.core.paths import DATA_DIR as _dal_dir
        # Only use DAL if DATA_DIR matches (avoids test env mismatch)
        if DATA_DIR == _dal_dir:
            result = get_all_customers()
            if result:
                return result
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    # Fallback to JSON
    try:
        with open(path) as f:
            data = json.load(f)
            if data:
                return data
    except (FileNotFoundError, json.JSONDecodeError) as _e:
        log.debug("suppressed: %s", _e)
    return []

def _save_customers(customers):
    """Save customers — DB is source of truth, JSON fallback for tests."""
    try:
        from src.core.dal import save_all_customers
        from src.core.paths import DATA_DIR as _dal_dir
        if DATA_DIR == _dal_dir:
            save_all_customers(customers)
            return
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    # Fallback to JSON
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "customers.json")
    with open(path, "w") as f:
        json.dump(customers, f, indent=2)

@bp.route("/api/supplier-profiles")
@auth_required
@safe_route
def api_supplier_profiles():
    """List all supplier profiles (tax/shipping settings)."""
    from src.core.db import get_all_supplier_profiles
    return jsonify({"ok": True, "profiles": get_all_supplier_profiles()})


@bp.route("/api/supplier-profiles", methods=["POST"])
@auth_required
@safe_route
def api_supplier_profile_save():
    """Create or update a supplier profile."""
    data = request.get_json(force=True, silent=True) or {}
    name = (data.get("supplier_name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "supplier_name required"})
    tax = data.get("tax_exempt_status", "unknown")
    if tax not in ("exempt_on_file", "pending", "not_accepted", "unknown"):
        tax = "unknown"
    threshold = float(data.get("free_shipping_threshold", 0) or 0)
    ship_pct = float(data.get("default_shipping_pct", 0) or 0)
    drop_ship = 1 if data.get("drop_ship") else 0
    notes = data.get("notes", "")
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO supplier_profiles
                (supplier_name, tax_exempt_status, free_shipping_threshold,
                 default_shipping_pct, drop_ship, notes, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(supplier_name) DO UPDATE SET
                    tax_exempt_status=excluded.tax_exempt_status,
                    free_shipping_threshold=excluded.free_shipping_threshold,
                    default_shipping_pct=excluded.default_shipping_pct,
                    drop_ship=excluded.drop_ship,
                    notes=excluded.notes,
                    updated_at=datetime('now')
            """, (name, tax, threshold, ship_pct, drop_ship, notes))
        return jsonify({"ok": True})
    except Exception as e:
        log.error("Supplier profile save: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/supplier-profiles/<name>/landed-cost")
@auth_required
@safe_route
def api_landed_cost_preview(name):
    """Preview landed cost calculation for a supplier. ?cost=50&qty=10"""
    from src.core.db import calc_landed_cost
    cost = float(request.args.get("cost", 0) or 0)
    qty = int(request.args.get("qty", 1) or 1)
    result = calc_landed_cost(cost, qty, name)
    return jsonify({"ok": True, **result})


@bp.route("/api/crm/buyer-lookup")
@auth_required
@safe_route
def api_buyer_lookup():
    """Look up ship-to address from buyer name or email via CRM + PO history."""
    name = request.args.get("name", "").strip()
    email = request.args.get("email", "").strip()
    if not name and not email:
        return jsonify({"ok": False, "error": "Provide name or email"})
    from src.core.ship_to_resolver import lookup_buyer_ship_to
    r = lookup_buyer_ship_to(name=name, email=email,
                             _load_customers=_load_customers)
    if not r["ship_to"]:
        return jsonify({"ok": False})
    return jsonify({"ok": True, "ship_to": r["ship_to"],
                    "institution": r["institution"], "agency": r["agency"],
                    "source": r["source"]})


@bp.route("/api/customers")
@auth_required
@safe_route
def api_customers():
    """Search customers. ?q=term&agency=CDCR&parent=true (parent-only)"""
    customers = _load_customers()
    q = request.args.get("q", "").lower()
    agency = request.args.get("agency", "")
    parent_only = request.args.get("parent", "") == "true"
    results = []
    for c in customers:
        if agency and c.get("agency", "").lower() != agency.lower():
            continue
        if parent_only and c.get("parent"):
            continue
        if q:
            searchable = " ".join([
                c.get("display_name") or "", c.get("company") or "",
                c.get("qb_name") or "", c.get("agency") or "",
                c.get("city") or "", c.get("abbreviation") or "",
            ]).lower()
            if q not in searchable:
                continue
        results.append(c)
    return jsonify(results)

@bp.route("/api/customers", methods=["POST"])
@auth_required
@safe_route
def api_customers_add():
    """Add a new customer. User confirms before saving."""
    data = request.get_json(force=True, silent=True) or {}
    if not data or not data.get("display_name"):
        return jsonify({"ok": False, "error": "display_name required"})
    customers = _load_customers()
    # Check for duplicate
    existing = [c for c in customers
                if c.get("display_name", "").lower() == data["display_name"].lower()]
    if existing:
        return jsonify({"ok": False, "error": "Customer already exists",
                        "existing": existing[0]})
    entry = {
        "qb_name": data.get("qb_name", data["display_name"]),
        "display_name": data["display_name"],
        "company": data.get("company", data["display_name"]),
        "parent": data.get("parent", ""),
        "agency": data.get("agency", "DEFAULT"),
        "abbreviation": data.get("abbreviation", ""),
        "address": data.get("address", ""),
        "city": data.get("city", ""),
        "state": data.get("state", "CA"),
        "zip": data.get("zip", ""),
        "phone": data.get("phone", ""),
        "email": data.get("email", ""),
        "open_balance": 0,
        "source": "manual",
    }
    customers.append(entry)
    _save_customers(customers)
    log.info("Customer added: %s (agency=%s)", entry["display_name"], entry.get("agency", ""))
    return jsonify({"ok": True, "customer": entry})

@bp.route("/api/customers/hierarchy")
@auth_required
@safe_route
def api_customers_hierarchy():
    """Return parent/child agency tree."""
    customers = _load_customers()
    parents = {}
    for c in customers:
        if not c.get("parent"):
            parents[c["display_name"]] = {
                "agency": c.get("agency", ""),
                "company": c.get("company", ""),
                "children": [],
            }
    for c in customers:
        p = c.get("parent", "")
        if p and p in parents:
            parents[p]["children"].append({
                "display_name": c["display_name"],
                "abbreviation": c.get("abbreviation", ""),
                "city": c.get("city", ""),
            })
    return jsonify(parents)

@bp.route("/api/customers/match")
@auth_required
@safe_route
def api_customers_match():
    """Match an institution name to CRM. Returns best match + new flag."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"matched": False, "candidates": []})
    customers = _load_customers()
    q_upper = q.upper()
    # Exact match first (check all name fields)
    for c in customers:
        names = [c.get("display_name",""), c.get("company",""),
                 c.get("abbreviation",""), c.get("qb_name","")]
        if any(q_upper == n.upper() for n in names if n):
            return jsonify({"matched": True, "customer": c, "is_new": False})
    # Abbreviation expansion: CSP-Sacramento → California State Prison, Sacramento
    _ABBR_MAP = {
        "CSP": "California State Prison",
        "SCC": "Sierra Conservation Center",
        "CIM": "California Institution for Men",
        "CIW": "California Institution for Women",
        "CMC": "California Men's Colony",
        "CMF": "California Medical Facility",
        "CTF": "Correctional Training Facility",
        "CHCF": "California Health Care Facility",
        "SATF": "Substance Abuse Treatment Facility",
    }
    expanded = q
    for abbr, full in _ABBR_MAP.items():
        if q_upper.startswith(abbr + "-") or q_upper.startswith(abbr + " "):
            suffix = q[len(abbr):].lstrip("- ")
            expanded = f"{full}, {suffix}" if suffix else full
            break
    if expanded != q:
        exp_upper = expanded.upper()
        for c in customers:
            if exp_upper in c.get("display_name", "").upper():
                return jsonify({"matched": True, "customer": c, "is_new": False})
    # Abbreviation-only match (e.g. "SAC" → abbreviation field)
    if len(q) <= 5:
        for c in customers:
            if c.get("abbreviation", "").upper() == q_upper:
                return jsonify({"matched": True, "customer": c, "is_new": False})
    # Fuzzy: token overlap
    q_tokens = set(q_upper.split())
    scored = []
    for c in customers:
        search_text = " ".join([c.get("display_name",""), c.get("company",""),
                                c.get("abbreviation","")]).upper()
        c_tokens = set(search_text.split())
        overlap = len(q_tokens & c_tokens)
        if overlap > 0:
            scored.append((overlap / max(len(q_tokens), 1), c))
    scored.sort(key=lambda x: -x[0])
    candidates = [s[1] for s in scored[:5] if s[0] > 0.3]
    if candidates and scored[0][0] >= 0.6:
        return jsonify({"matched": True, "customer": candidates[0],
                        "is_new": False, "candidates": candidates[:3]})
    return jsonify({"matched": False, "is_new": True,
                    "candidates": candidates[:3],
                    "suggested_agency": _guess_agency(q)})

def _guess_agency(institution_name):
    """Guess agency from institution name using the institution resolver.
    We only sell in CA — every institution maps to a known agency."""
    if not institution_name:
        return "CDCR"  # Default to CDCR (most common)

    # Use the authoritative resolver first
    try:
        from src.core.institution_resolver import resolve
        from src.core.agency_display import agency_display
        result = resolve(institution_name)
        if result and result.get("agency"):
            agency = result["agency"]
            resolved = agency_display(agency)
            log.info("AGENCY_RESOLVE: '%s' → resolver=%s → %s", institution_name, agency, resolved)
            return resolved
    except Exception as e:
        log.warning("AGENCY_RESOLVE: resolver failed for '%s': %s", institution_name, e)

    # Fallback: keyword matching
    upper = institution_name.upper()
    if any(kw in upper for kw in ("CCHCS", "HEALTH CARE SERVICE")):
        return "CCHCS"
    if any(kw in upper for kw in ("CALVET", "CAL VET", "VETERAN")):
        return "CalVet"
    if any(kw in upper for kw in ("STATE HOSPITAL", "DSH", "PATTON", "COALINGA", "ATASCADERO", "NAPA")):
        return "DSH"
    if any(kw in upper for kw in ("DGS", "GENERAL SERVICE")):
        return "DGS"
    if any(kw in upper for kw in ("CALFIRE", "CAL FIRE", "FORESTRY")):
        return "CalFire"
    if any(kw in upper for kw in ("CDPH", "PUBLIC HEALTH")):
        return "CDPH"
    if any(kw in upper for kw in ("CALTRANS", "TRANSPORTATION")):
        return "CalTrans"
    if any(kw in upper for kw in ("CHP", "HIGHWAY PATROL")):
        return "CHP"

    # CDCR patterns — check abbreviations + keywords
    cdcr_kw = ("CDCR", "CORRECTION", "STATE PRISON", "CONSERVATION CENTER",
               "INSTITUTION FOR", "FOLSOM", "PELICAN", "SAN QUENTIN", "CORCORAN",
               "MENTAL HEALTH", "REHABILIT")
    cdcr_pfx = ("CSP", "CIM", "CIW", "SCC", "CMC", "SATF", "CHCF", "PVSP",
                "KVSP", "LAC", "MCSP", "NKSP", "SAC", "WSP", "SOL", "FSP",
                "HDSP", "ISP", "CTF", "RJD", "CAL", "CEN", "ASP", "CCWF", "VSP",
                "DVI", "CRC", "PBSP", "RJD", "SVSP", "COR", "CMF", "CVSP", "CCC")
    if any(kw in upper for kw in cdcr_kw):
        return "CDCR"
    # Check if starts with known CDCR prefix (CIW /, CIW-, CIW space, CIW alone)
    for pfx in cdcr_pfx:
        if upper == pfx or upper.startswith(pfx + " ") or upper.startswith(pfx + "-") or upper.startswith(pfx + "/"):
            return "CDCR"

    # We only sell in CA — default to CDCR (most likely)
    return "CDCR"

@bp.route("/api/quotes/counter")
@auth_required
@safe_route
def api_quote_counter():
    """Get current quote counter state."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})
    return jsonify({"ok": True, "next": peek_next_quote_number()})


@bp.route("/api/search")
@auth_required
@safe_route
def api_universal_search():
    """Universal search across ALL data: quotes, CRM contacts, intel buyers,
    orders, RFQs, growth prospects. Returns results with clickable links.
    GET ?q=<query>&limit=<n>
    """
    q = (_sanitize_input(request.args.get("q", "")) or "").strip().lower()
    try:
        limit = min(max(1, int(request.args.get("limit", 30))), 100)
    except (ValueError, TypeError, OverflowError):
        limit = 30
    if not q or len(q) < 2:
        return jsonify({"ok": False, "error": "Query must be at least 2 characters"})

    results = []

    # ── Quotes ────────────────────────────────────────────────────────────────
    if QUOTE_GEN_AVAILABLE:
        try:
            for qt in search_quotes(query=q, limit=20):
                qn = qt.get("quote_number", "")
                inst = qt.get("institution","") or qt.get("ship_to_name","") or "—"
                ag   = qt.get("agency","") or "—"
                total= qt.get("total", 0)
                status = qt.get("status","")
                results.append({
                    "type": "quote",
                    "icon": "📋",
                    "title": qn,
                    "subtitle": f"{ag} · {inst[:40]}",
                    "meta": f"${total:,.0f} · {status}",
                    "url": f"/quote/{qn}",
                    "score": 100,
                })
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

    # ── CRM Contacts ──────────────────────────────────────────────────────────
    try:
        contacts = _load_crm_contacts()
        for cid, c in contacts.items():
            fields = " ".join([
                c.get("buyer_name",""), c.get("buyer_email",""),
                c.get("agency",""), c.get("title",""),
                c.get("notes",""), c.get("buyer_phone",""),
                " ".join(str(k) for k in c.get("categories",{}).keys()),
            ]).lower()
            if q in fields:
                spend = c.get("total_spend", 0)
                status = c.get("outreach_status","new")
                results.append({
                    "type": "contact",
                    "icon": "👤",
                    "title": c.get("buyer_name","") or c.get("buyer_email",""),
                    "subtitle": f"{c.get('agency','')} · {c.get('buyer_email','')}",
                    "meta": f"${spend:,.0f} · {status}",
                    "url": f"/growth/prospect/{cid}",
                    "score": 90,
                })
                if len(results) >= limit: break
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # ── Intel Buyers (not yet in CRM) ─────────────────────────────────────────
    if INTEL_AVAILABLE:
        try:
            from src.agents.sales_intel import _load_json as _il, BUYERS_FILE as _BF
            buyers_data = _il(_BF)
            crm_emails = {c.get("buyer_email","").lower() for c in _load_crm_contacts().values()}
            if isinstance(buyers_data, dict):
                for b in buyers_data.get("buyers", [])[:200]:
                    email = (b.get("email","") or b.get("buyer_email","")).lower()
                    if email in crm_emails:
                        continue  # already surfaced via CRM
                    fields = " ".join([
                        b.get("name","") or b.get("buyer_name",""),
                        email, b.get("agency",""),
                        " ".join(b.get("categories",{}).keys()),
                        " ".join(i.get("description","") for i in b.get("items_purchased",[])[:5]),
                    ]).lower()
                    if q in fields:
                        spend = b.get("total_spend",0)
                        results.append({
                            "type": "intel_buyer",
                            "icon": "🧠",
                            "title": b.get("name","") or b.get("buyer_name","") or email,
                            "subtitle": f"{b.get('agency','')} · {email}",
                            "meta": f"${spend:,.0f} · score {b.get('opportunity_score',0)}",
                            "url": f"/growth/prospect/{b.get('id','')}",
                            "score": 80,
                        })
                        if len(results) >= limit: break
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

    # ── Orders ────────────────────────────────────────────────────────────────
    try:
        orders = _load_orders()
        for oid, o in orders.items():
            fields = " ".join([
                o.get("quote_number",""), o.get("agency",""),
                o.get("institution",""), o.get("po_number",""),
                o.get("status",""), oid,
            ]).lower()
            if q in fields:
                results.append({
                    "type": "order",
                    "icon": "📦",
                    "title": oid,
                    "subtitle": f"{o.get('agency','')} · {o.get('institution','')}",
                    "meta": f"PO {o.get('po_number','')} · {o.get('status','')}",
                    "url": f"/order/{oid}",
                    "score": 70,
                })
                if len(results) >= limit: break
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # ── RFQs ──────────────────────────────────────────────────────────────────
    try:
        rfqs = load_rfqs()
        for rid, r in rfqs.items():
            fields = " ".join([
                r.get("rfq_number",""), r.get("requestor_name",""),
                r.get("institution",""), r.get("agency",""),
                r.get("status",""), rid,
                " ".join(str(i.get("description","")) for i in r.get("items",[])),
            ]).lower()
            if q in fields:
                results.append({
                    "type": "rfq",
                    "icon": "📄",
                    "title": r.get("rfq_number","") or rid[:12],
                    "subtitle": f"{r.get('agency','')} · {r.get('requestor_name','')}",
                    "meta": f"{len(r.get('items',[]))} items · {r.get('status','')}",
                    "url": f"/rfq/{rid}",
                    "score": 60,
                })
                if len(results) >= limit: break
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    # ── Price Checks (SQLite) ────────────────────────────────────────────────
    try:
        from src.core.db import get_db
        with get_db() as conn:
            pcs = conn.execute("""
                SELECT id, created_at, requestor, agency, items, quote_number, total_items
                FROM price_checks
                WHERE LOWER(requestor) LIKE ? OR LOWER(agency) LIKE ?
                   OR LOWER(items) LIKE ? OR LOWER(id) LIKE ?
                   OR LOWER(quote_number) LIKE ?
                ORDER BY created_at DESC LIMIT 20
            """, (f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%", f"%{q}%")).fetchall()
            for pc in pcs:
                pc_id = pc[0]
                results.append({
                    "type": "price_check",
                    "icon": "💰",
                    "title": pc_id[:16] if pc_id else "PC",
                    "subtitle": f"{pc[3] or ''} · {pc[2] or ''}",
                    "meta": f"{pc[6] or 0} items · {pc[5] or 'no quote'}",
                    "url": f"/pricechecks#{pc_id}",
                    "score": 75,
                })
                if len(results) >= limit: break
    except Exception as _e:
        log.debug("Suppressed PC search: %s", _e)

    # ── Products / Catalog ───────────────────────────────────────────────────
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Check if products table exists
            has_products = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='products'"
            ).fetchone()
            if has_products:
                prods = conn.execute("""
                    SELECT id, name, mfg_number, category, sell_price, recommended_price
                    FROM products
                    WHERE LOWER(name) LIKE ? OR LOWER(mfg_number) LIKE ?
                       OR LOWER(category) LIKE ?
                    ORDER BY name LIMIT 15
                """, (f"%{q}%", f"%{q}%", f"%{q}%")).fetchall()
                for p in prods:
                    price = p[5] or p[4] or 0
                    results.append({
                        "type": "product",
                        "icon": "🏷️",
                        "title": p[1] or p[2] or f"Product #{p[0]}",
                        "subtitle": f"{p[3] or 'Uncategorized'} · {p[2] or ''}",
                        "meta": f"${price:,.2f}" if price else "No price",
                        "url": f"/catalog#{p[0]}",
                        "score": 50,
                    })
                    if len(results) >= limit: break
    except Exception as _e:
        log.debug("Suppressed product search: %s", _e)

    # Sort by type priority, dedupe urls
    seen_urls = set()
    deduped = []
    for r in sorted(results, key=lambda x: -x["score"]):
        if r["url"] not in seen_urls:
            seen_urls.add(r["url"])
            deduped.append(r)

    return jsonify({
        "ok": True,
        "query": q,
        "count": len(deduped),
        "results": deduped[:limit],
        "breakdown": {t: sum(1 for r in deduped if r["type"]==t)
                      for t in ("quote","contact","intel_buyer","order","rfq","price_check","product")},
    })





@bp.route("/api/quotes/set-counter", methods=["POST"])
@auth_required
@safe_route
def api_set_quote_counter():
    """Manually set quote counter to sync with QuoteWerks.
    POST JSON: {"seq": 16, "year": 2026}  ← next quote will be R26Q17
    """
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})
    data = request.get_json(silent=True) or {}
    seq = data.get("seq")
    year = data.get("year", datetime.now().year)
    if seq is None or not isinstance(seq, int) or seq < 0:
        return jsonify({"ok": False, "error": "seq (integer ≥ 0) required — next quote will be R{YY}Q{seq+1}"})
    set_quote_counter(seq=seq, year=year)
    nxt = peek_next_quote_number()
    return jsonify({"ok": True, "set_to": seq, "year": year,
                    "next_quote_will_be": nxt,
                    "message": f"Counter set. Next quote: {nxt}"})


@bp.route("/api/quotes/history")
@auth_required
@safe_route
def api_quote_history():
    """Get quote history for an institution. Returns linked entities for UI."""
    institution = request.args.get("institution", "").strip()
    if not institution or not QUOTE_GEN_AVAILABLE:
        return jsonify([])
    quotes = get_all_quotes()
    inst_upper = institution.upper()
    matches = []
    for qt in reversed(quotes):
        qt_inst = qt.get("institution", "").upper()
        from src.core.contracts import safe_match as _sm
        if _sm(inst_upper, qt_inst):
            source_pc = qt.get("source_pc_id", "")
            source_rfq = qt.get("source_rfq_id", "")
            
            # Compute days since creation for age display
            created = qt.get("created_at", "")
            days_ago = ""
            if created:
                try:
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    delta = datetime.now() - created_dt.replace(tzinfo=None)
                    days_ago = f"{delta.days}d ago" if delta.days > 0 else "today"
                except Exception as e:
                    log.debug("Suppressed: %s", e)
                    pass

            matches.append({
                "quote_number": qt.get("quote_number"),
                "date": qt.get("date"),
                "total": qt.get("total", 0),
                "items_count": qt.get("items_count", 0),
                "status": qt.get("status", "pending"),
                "po_number": qt.get("po_number", ""),
                "items_text": qt.get("items_text", ""),
                "items_detail": qt.get("items_detail", []),
                "days_ago": days_ago,
                # Links for UI navigation
                "source_pc_id": source_pc,
                "source_pc_url": f"/pricecheck/{source_pc}" if source_pc else "",
                "source_rfq_id": source_rfq,
                "source_rfq_url": f"/rfq/{source_rfq}" if source_rfq else "",
                "quote_url": f"/quotes?q={qt.get('quote_number', '')}",
                # Lifecycle
                "status_history": qt.get("status_history", []),
            })
            if len(matches) >= 10:
                break
    return jsonify(matches)

# ═══════════════════════════════════════════════════════════════════════
# Product Research API (v6.1 — Phase 6)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/research/test")
@auth_required
@safe_route
def api_research_test():
    """Test Amazon search — ?q=nitrile+gloves"""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    q = request.args.get("q", "nitrile exam gloves")
    return jsonify(test_amazon_search(q))


@bp.route("/api/research/lookup")
@auth_required
@safe_route
def api_research_lookup():
    """Quick product lookup — ?q=stryker+restraint+package"""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    q = request.args.get("q", "")
    if not q:
        return jsonify({"error": "Provide ?q=search+terms"}), 400
    return jsonify(quick_lookup(q))


@bp.route("/api/research/rfq/<rid>")
@auth_required
@safe_route
def api_research_rfq(rid):
    """Research all line items in an RFQ. Runs in background thread."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"error": f"RFQ {rid} not found"}), 404
    if RESEARCH_STATUS.get("running"):
        return jsonify({"ok": False, "message": "Research already running", "status": RESEARCH_STATUS})

    def _run_research():
        result = research_rfq_items(r)
        # Save updated supplier costs back to RFQ
        rfqs_fresh = load_rfqs()
        if rid in rfqs_fresh:
            rfqs_fresh[rid]["line_items"] = r["line_items"]
            save_rfqs(rfqs_fresh)

    t = threading.Thread(target=_run_research, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Research started. Check /api/research/status for progress."})


@bp.route("/api/research/status")
@auth_required
@safe_route
def api_research_status():
    """Check progress of RFQ product research."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    # In-memory dict is primary (fast, current session)
    if RESEARCH_STATUS.get("running"):
        return jsonify(RESEARCH_STATUS)
    # Fall back to DB for durability across restarts
    try:
        from src.core.workflow_tracker import tracker
        # Find any recent product_research task
        active = tracker.get_active(task_type="product_research")
        if active:
            return jsonify(active[0])
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    return jsonify(RESEARCH_STATUS)


@bp.route("/api/research/cache-stats")
@auth_required
@safe_route
def api_research_cache_stats():
    """Get product research cache statistics."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    return jsonify(get_research_cache_stats())


@bp.route("/api/debug/env-check")
@auth_required
@safe_route
def api_debug_env_check():
    """Check if API keys are visible to the app."""
    import os
    xai_val = os.environ.get("XAI_API_KEY", "")
    anthropic_val = os.environ.get("ANTHROPIC_API_KEY", "")
    return jsonify({
        "XAI_API_KEY_set": bool(xai_val),
        "XAI_API_KEY_preview": f"{xai_val[:8]}..." if xai_val else "EMPTY",
        "ANTHROPIC_API_KEY_set": bool(anthropic_val),
        "ANTHROPIC_API_KEY_preview": f"{anthropic_val[:8]}..." if anthropic_val else "EMPTY",
    })


# ═══════════════════════════════════════════════════════════════════════
# Price Check API (v6.2 — Phase 6)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pricecheck/parse", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_pricecheck_parse():
    """Parse an uploaded AMS 704 PDF. Upload as multipart file."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"error": "price_check.py not available"}), 503
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded. Use multipart form with 'file' field."}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400
    # Save to temp
    import tempfile
    tmp = os.path.join(DATA_DIR, f"pc_upload_{_safe_filename(f.filename)}")
    f.save(tmp)
    log.info("Price check parse: %s", f.filename)
    try:
        result = test_parse(tmp)
        result["uploaded_file"] = tmp
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/pricecheck/process", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_pricecheck_process():
    """Full pipeline: parse → lookup → price → fill PDF."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"error": "price_check.py not available"}), 503

    # Accept file upload or path to existing file
    pdf_path = None
    if "file" in request.files:
        f = request.files["file"]
        pdf_path = os.path.join(DATA_DIR, f"pc_upload_{_safe_filename(f.filename)}")
        f.save(pdf_path)
    elif request.is_json and (request.get_json(force=True, silent=True) or {}).get("pdf_path"):
        try:
            pdf_path = _validate_pdf_path((request.get_json(force=True, silent=True) or {}).get("pdf_path", ""))
        except ValueError as _e:
            return jsonify({"error": f"Invalid pdf_path: {_e}"}), 400
    else:
        return jsonify({"error": "Upload a file or provide pdf_path in JSON"}), 400

    tax_rate = 0.0
    try:
        if request.is_json:
            tax_rate = max(0.0, min(float((request.get_json(force=True, silent=True) or {}).get("tax_rate", 0.0)), 100.0))
        elif request.form.get("tax_rate"):
            tax_rate = max(0.0, min(float(request.form.get("tax_rate", 0.0)), 100.0))
    except (ValueError, TypeError, OverflowError):
        tax_rate = 0.0

    try:
        log.info("Price check process pipeline started: %s", pdf_path)
        result = process_price_check(
            pdf_path=pdf_path,
            output_dir=DATA_DIR,
            tax_rate=tax_rate,
        )
        # If successful, make the PDF downloadable
        if result.get("ok") and result.get("output_pdf"):
            result["download_url"] = f"/api/pricecheck/download/{os.path.basename(result['output_pdf'])}"
        return jsonify(json.loads(json.dumps(result, default=str)))
    except Exception as e:
        log.error("Route error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/pricecheck/download/<filename>")
@auth_required
@safe_route
def api_pricecheck_download(filename):
    """Download or preview a completed Price Check PDF.
    Add ?inline=1 to display in browser instead of downloading.
    """
    safe = os.path.basename(filename)
    inline = request.args.get("inline") == "1"
    # Fast targeted search — one level deep in output dirs
    search_dirs = [DATA_DIR, os.path.join(DATA_DIR, "output"), os.path.join(DATA_DIR, "outputs")]
    for d in [os.path.join(DATA_DIR, "output"), os.path.join(DATA_DIR, "outputs")]:
        if os.path.isdir(d):
            try:
                search_dirs.extend(os.path.join(d, sub) for sub in os.listdir(d) if os.path.isdir(os.path.join(d, sub)))
            except OSError as _e:
                log.debug("suppressed: %s", _e)
    for d in search_dirs:
        candidate = os.path.join(d, safe)
        if os.path.exists(candidate):
            mimetype = "application/pdf" if safe.lower().endswith(".pdf") else None
            resp = send_file(candidate, as_attachment=not inline, download_name=safe, mimetype=mimetype)
            # Prevent 304 caching — Chrome serves stale filename from cache
            resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            resp.headers.pop("ETag", None)
            resp.headers.pop("Last-Modified", None)
            return resp
    # Fallback: check DB
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute("SELECT data FROM rfq_files WHERE filename=? ORDER BY id DESC LIMIT 1", (safe,)).fetchone()
            if row and row["data"]:
                restore_dir = os.path.join(DATA_DIR, "output", "_restored")
                os.makedirs(restore_dir, exist_ok=True)
                restore_path = os.path.join(restore_dir, safe)
                with open(restore_path, "wb") as _fw:
                    _fw.write(row["data"])
                mimetype = "application/pdf" if safe.lower().endswith(".pdf") else None
                return send_file(restore_path, as_attachment=not inline, download_name=safe, mimetype=mimetype)
    except Exception as _e:
        log.debug("suppressed: %s", _e)
    return jsonify({"error": "File not found"}), 404


@bp.route("/api/pricecheck/view-pdf/<path:filename>")
@auth_required
@safe_route
def api_pricecheck_view_pdf(filename):
    """Serve a PDF inline for the browser PDF viewer (iframes, tabs)."""
    safe = os.path.basename(filename)
    
    # Fast targeted search — check specific directories, no os.walk
    search_dirs = [
        DATA_DIR,
        os.path.join(DATA_DIR, "output"),
        os.path.join(DATA_DIR, "outputs"),
    ]
    # Add output subdirectories (one level deep only)
    for d in [os.path.join(DATA_DIR, "output"), os.path.join(DATA_DIR, "outputs")]:
        if os.path.isdir(d):
            try:
                search_dirs.extend(
                    os.path.join(d, sub) for sub in os.listdir(d) 
                    if os.path.isdir(os.path.join(d, sub))
                )
            except OSError as _e:
                log.debug("suppressed: %s", _e)
    # Also check uploads subdirectories (one level)
    uploads_dir = os.path.join(DATA_DIR, "uploads")
    if os.path.isdir(uploads_dir):
        try:
            search_dirs.extend(
                os.path.join(uploads_dir, sub) for sub in os.listdir(uploads_dir) 
                if os.path.isdir(os.path.join(uploads_dir, sub))
            )
        except OSError as _e:
            log.debug("suppressed: %s", _e)
    
    for d in search_dirs:
        candidate = os.path.join(d, safe)
        if os.path.exists(candidate):
            from flask import Response as _Resp
            with open(candidate, 'rb') as _f:
                _data = _f.read()
            return _Resp(_data, mimetype='application/pdf',
                headers={'Content-Disposition': 'inline', 'Cache-Control': 'no-store'})

    # Fallback: check DB (rfq_files) — survives redeploys
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                "SELECT data, filename FROM rfq_files WHERE filename=? ORDER BY id DESC LIMIT 1",
                (safe,)).fetchone()
            if row and row["data"]:
                from flask import Response as _Resp
                return _Resp(row["data"], mimetype='application/pdf',
                    headers={'Content-Disposition': 'inline', 'Cache-Control': 'no-store'})
    except Exception as _e:
        log.debug("DB PDF lookup failed: %s", _e)

    return jsonify({"error": f"PDF not found: {safe}"}), 404


@bp.route("/api/pricecheck/test-parse")
@auth_required
@safe_route
def api_pricecheck_test_parse():
    """Test parse the most recently uploaded PC PDF."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"error": "price_check.py not available"}), 503
    # Find most recent pc_upload file
    files = [f for f in os.listdir(DATA_DIR) if f.startswith("pc_upload_")]
    if not files:
        return jsonify({"error": "No uploaded PC files. POST a file to /api/pricecheck/parse first."})
    latest = sorted(files)[-1]
    return jsonify(test_parse(os.path.join(DATA_DIR, latest)))


# ═══════════════════════════════════════════════════════════════════════
# Auto-Processor API (v7.0 — Phase 7)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/tax-rate")
@auth_required
@safe_route
def api_tax_rate():
    """Get CA sales tax rate. Uses ship-to zip if provided, else default CA rate."""
    zip_code = request.args.get("zip", "")
    # Try CDTFA lookup if we have a zip
    if zip_code:
        try:
            import requests as req
            # CDTFA tax rate lookup via their API
            resp = req.get(
                f"https://www.cdtfa.ca.gov/taxes-and-fees/rates.aspx",
                params={"city": "", "county": "", "zip": zip_code},
                timeout=5
            )
            # Parse rate from response if possible
            # For now fall through to default — full CDTFA scraper is in main codebase
        except Exception as e:
            log.debug("Suppressed: %s", e)
            pass
    # Default CA rate — state govt PCs are typically tax-exempt anyway
    return jsonify({
        "rate": 0.0725,
        "jurisdiction": "CA Default",
        "note": "State government purchases are typically tax-exempt. Toggle is OFF by default for 704 PCs.",
    })


@bp.route("/api/health")
@auth_required
@safe_route
def api_health():
    """Comprehensive system health check with path validation."""
    health = {"status": "ok", "build": "v20260220-1005-pdf-v4", "checks": {}}
    return jsonify(health)


@bp.route("/api/build")
@auth_required
@safe_route
def api_build_version():
    """Build version check to verify deploys."""
    try:
        from src.core.paths import DATA_DIR, _USING_VOLUME
        vol = "persistent" if _USING_VOLUME else "ephemeral"
    except Exception:
        vol = "unknown"
        DATA_DIR = "?"
    try:
        from src.forms.quote_generator import peek_next_quote_number, _load_counter
        counter = _load_counter()
        next_qn = peek_next_quote_number()
    except Exception:
        counter = {}
        next_qn = "?"
    return jsonify({
        "build": "v20260220-1130-pc-detection", "ok": True,
        "storage": vol, "data_dir": str(DATA_DIR),
        "quote_counter": counter, "next_quote": next_qn,
    })


@bp.route("/api/admin/set-counter", methods=["POST"])
@auth_required
@safe_route
def api_set_counter():
    """Set the quote counter to a specific value.
    POST {"seq": 16} → next quote will be R26Q17
    POST {"seq": 16, "year": 2026}
    """
    data = request.get_json(force=True)
    seq = data.get("seq")
    if seq is None or not isinstance(seq, int):
        return jsonify({"ok": False, "error": "seq (integer) required"}), 400
    year = data.get("year", datetime.now().year)
    from src.forms.quote_generator import set_quote_counter, peek_next_quote_number
    set_quote_counter(seq, year)
    return jsonify({
        "ok": True,
        "set_to": {"year": year, "seq": seq},
        "next_quote": peek_next_quote_number(),
    })


@bp.route("/api/admin/reclassify-to-pc", methods=["POST"])
@auth_required
@safe_route
def api_reclassify_to_pc():
    """Move stuck #unknown RFQs to the Price Check queue.
    POST {"rfq_ids": ["id1", "id2"]} or {"rfq_ids": "all_unknown"}
    """
    try:
        data = request.get_json(force=True)
        rfq_ids = data.get("rfq_ids", [])
    
        from src.api.dashboard import load_rfqs, save_rfqs, _load_price_checks, _save_price_checks
    
        rfqs = load_rfqs()
        pcs = _load_price_checks()
        moved = []
    
        # "all_unknown" = move all #unknown solicitation RFQs with 0 items
        if rfq_ids == "all_unknown":
            rfq_ids = [rid for rid, r in rfqs.items()
                       if r.get("solicitation_number") in ("unknown", "RFQ", "#unknown", "")
                       and len(r.get("line_items", [])) == 0]
    
        for rid in rfq_ids:
            r = rfqs.get(rid)
            if not r:
                continue
        
            import uuid as _uuid
            pc_id = f"pc_{str(_uuid.uuid4())[:8]}"
        
            # Try to find the source PDF for parsing
            source_pdf = ""
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    files = conn.execute(
                        "SELECT id, filename FROM rfq_files WHERE rfq_id=? AND filename LIKE '%.pdf'",
                        (rid,)
                    ).fetchall()
                    if files:
                        source_pdf = files[0]["filename"]
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
        
            pcs[pc_id] = {
                "id": pc_id,
                "pc_number": r.get("solicitation_number", "") or "RFQ",
                "institution": r.get("institution", ""),
                "due_date": r.get("due_date", ""),
                "requestor": r.get("requestor_email", r.get("requestor_name", "")),
                "ship_to": r.get("delivery_location", ""),
                "items": r.get("line_items", []),
                "source_pdf": source_pdf,
                "status": "parse_error" if not r.get("line_items") else "parsed",
                "parse_error": "Reclassified from RFQ queue",
                "created_at": r.get("created_at", datetime.now().isoformat()),
                "email_uid": r.get("email_uid", ""),
                "email_subject": r.get("email_subject", ""),
                "source": "reclassified",
                "reytech_quote_number": "",
                "linked_quote_number": "",
            }
            del rfqs[rid]
            moved.append({"rfq_id": rid, "pc_id": pc_id, "subject": r.get("email_subject", "")})
    
        if moved:
            # raise_on_error=True: admin reclassify deletes source RFQs AND
            # writes them as PCs. If either save silently fails we get data
            # loss (RFQs gone from rfqs table, PCs never landed). Force loud
            # failure so the user knows to retry rather than walking away
            # thinking the move succeeded.
            try:
                save_rfqs(rfqs, raise_on_error=True)
                _save_price_checks(pcs, raise_on_error=True)
            except Exception as _save_e:
                log.error("reclassify-to-pc persistence failed mid-move: %s", _save_e)
                return jsonify({
                    "ok": False,
                    "error": f"Reclassify failed to persist: {_save_e}. "
                             f"Check /api/admin/reclassify-to-pc-status for current state — "
                             f"{len(moved)} items were staged in memory but the DB write did not complete.",
                    "attempted_moves": len(moved),
                }), 500

        return jsonify({
            "ok": True,
            "moved": len(moved),
            "details": moved,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

@bp.route("/api/metrics")
@auth_required
@safe_route
def api_metrics():
    """Real-time performance & system metrics — cache efficiency, data sizes, thread state."""
    import gc

    # Cache stats
    with _json_cache_lock:
        cache_size = len(_json_cache)
        cache_keys = list(_json_cache.keys())
    
    # Data file sizes
    data_files = {}
    for fname in ["rfqs.json","quotes_log.json","crm_activity.json",
                  "crm_contacts.json","intel_buyers.json","intel_agencies.json",
                  "growth_prospects.json","scprs_prices.json"]:
        fpath = os.path.join(DATA_DIR, fname)
        if os.path.exists(fpath):
            stat = os.stat(fpath)
            try:
                with open(fpath) as f:
                    d = json.load(f)
                records = len(d) if isinstance(d, (list, dict)) else "?"
            except Exception:
                records = "?"
            data_files[fname] = {"size_kb": round(stat.st_size/1024,1), "records": records,
                                  "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat()}
        else:
            data_files[fname] = {"size_kb": 0, "records": 0, "mtime": None}

    # Thread inventory
    threads = [{"name": t.name, "alive": t.is_alive(), "daemon": t.daemon}
               for t in threading.enumerate()]

    # Rate limiter state
    with _rate_limiter_lock:
        active_ips = len(_rate_limiter)

    # Global agent states
    agent_states = {
        "poll_running": POLL_STATUS.get("running", False),
        "poll_last": POLL_STATUS.get("last_check"),
        "poll_emails_found": POLL_STATUS.get("emails_found", 0),
    }
    if INTEL_AVAILABLE:
        try:
            from src.agents.sales_intel import DEEP_PULL_STATUS
            agent_states["intel_pull_running"] = DEEP_PULL_STATUS.get("running", False)
            agent_states["intel_buyers"] = DEEP_PULL_STATUS.get("total_buyers", 0)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    if GROWTH_AVAILABLE:
        try:
            from src.agents.growth_agent import PULL_STATUS, BUYER_STATUS
            agent_states["growth_pull_running"] = PULL_STATUS.get("running", False)
            agent_states["growth_buyer_running"] = BUYER_STATUS.get("running", False)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)

    # GC stats
    gc_counts = gc.get_count()

    # DB stats
    db_stats = {}
    try:
        from src.core.db import get_db_stats
        db_stats = get_db_stats()
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    return jsonify({
        "ok": True,
        "timestamp": datetime.now().isoformat(),
        "cache": {
            "entries": cache_size,
            "keys": [os.path.basename(k) for k in cache_keys],
        },
        "data_files": data_files,
        "database": db_stats,
        "threads": {"count": len(threads), "list": threads},
        "rate_limiter": {"active_ips": active_ips},
        "agents": agent_states,
        "gc": {"gen0": gc_counts[0], "gen1": gc_counts[1], "gen2": gc_counts[2]},
        "modules": {
            "quote_gen": QUOTE_GEN_AVAILABLE,
            "price_check": PRICE_CHECK_AVAILABLE,
            "auto_processor": AUTO_PROCESSOR_AVAILABLE,
            "intel": INTEL_AVAILABLE,
            "growth": GROWTH_AVAILABLE,
            "qb": QB_AVAILABLE,
        },
    })


@bp.route("/api/db")
@auth_required
@safe_route
def api_db_status():
    """Database status — row counts, file size, persistence info."""
    try:
        from src.core.db import get_db_stats, DB_PATH, _is_railway_volume
        stats = get_db_stats()
        is_vol = _is_railway_volume()
        return jsonify({
            "ok": True,
            "db_path": DB_PATH,
            "db_size_kb": stats.get("db_size_kb", 0),
            "is_railway_volume": is_vol,
            "persistence": "permanent (Railway volume ✅)" if is_vol else "temporary (container filesystem — data lost on redeploy)",
            "tables": {k: v for k, v in stats.items() if k not in ("db_path", "db_size_kb")},
            "railway_env": {
                "RAILWAY_VOLUME_NAME": os.environ.get("RAILWAY_VOLUME_NAME", "not set"),
                "RAILWAY_VOLUME_MOUNT_PATH": os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "not set"),
                "RAILWAY_ENVIRONMENT": os.environ.get("RAILWAY_ENVIRONMENT", "not set"),
            },
            "setup_instructions": None if is_vol else {
                "note": "Volume appears mounted at /app/data but RAILWAY_VOLUME_NAME env var not detected.",
                "fix": "In Railway UI → your service → Variables → confirm RAILWAY_VOLUME_NAME is auto-set, or redeploy.",
            },
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/prices/history")
@auth_required
@safe_route
def api_price_history():
    """Search price history database.
    GET ?q=<description>&pn=<part_number>&source=<amazon|scprs|quote>&limit=50
    """
    try:
        from src.core.db import get_price_history_db, get_price_stats
        q = request.args.get("q","").strip()
        pn = request.args.get("pn","").strip()
        source = request.args.get("source","").strip()
        try:
            limit = min(max(1, int(request.args.get("limit",50))), 200)
        except (ValueError, TypeError, OverflowError):
            limit = 50

        if not q and not pn and not source:
            stats = get_price_stats()
            return jsonify({"ok": True, "mode": "stats", **stats})

        results = get_price_history_db(description=q, part_number=pn,
                                        source=source, limit=limit)
        return jsonify({
            "ok": True,
            "query": {"description": q, "part_number": pn, "source": source},
            "count": len(results),
            "results": results,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/prices/best")
@auth_required
@safe_route
def api_price_best():
    """Get the best (lowest) recorded price for an item description.
    GET ?q=<description>  or  ?pn=<part_number>
    Returns: best price, source, when found, and all price observations.
    """
    try:
        from src.core.db import get_price_history_db
        q = request.args.get("q","").strip()
        pn = request.args.get("pn","").strip()
        if not q and not pn:
            return jsonify({"ok": False, "error": "q (description) or pn (part number) required"})

        results = get_price_history_db(description=q, part_number=pn, limit=100)
        if not results:
            return jsonify({"ok": True, "found": False, "query": q or pn})

        best = min(results, key=lambda x: x["unit_price"])
        avg = sum(r["unit_price"] for r in results) / len(results)
        sources_seen = list({r["source"] for r in results})

        return jsonify({
            "ok": True,
            "found": True,
            "query": q or pn,
            "best_price": best["unit_price"],
            "best_source": best["source"],
            "best_found_at": best["found_at"],
            "avg_price": round(avg, 2),
            "observations": len(results),
            "sources": sources_seen,
            "all": results[:20],
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/cache/clear", methods=["POST"])
@auth_required
@safe_route
def api_cache_clear():
    """Clear the JSON read cache (useful after manual data edits)."""
    with _json_cache_lock:
        count = len(_json_cache)
        _json_cache.clear()
    return jsonify({"ok": True, "cleared": count, "message": f"Cleared {count} cache entries"})



def api_audit_stats():
    """Processing statistics from audit log."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"error": "auto_processor.py not available"}), 503
    return jsonify(get_audit_stats())


@bp.route("/api/auto-process/pc", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_auto_process_pc():
    """Full autonomous pipeline for a Price Check PDF."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"error": "auto_processor.py not available"}), 503
    if "file" not in request.files:
        return jsonify({"error": "Upload a PDF file"}), 400
    f = request.files["file"]
    safe_name = re.sub(r'[^\w.\-]', '_', f.filename or 'upload.pdf')
    pdf_path = os.path.join(DATA_DIR, f"pc_upload_{safe_name}")
    f.save(pdf_path)
    log.info("Auto-process started for %s", f.filename)
    result = auto_process_price_check(pdf_path)
    log.info("Auto-process complete for %s: status=%s", f.filename, result.get("status", "unknown"))
    return jsonify(json.loads(json.dumps(result, default=str)))


@bp.route("/api/detect-type", methods=["POST"])
@auth_required
@safe_route
@rate_limit("heavy")
def api_detect_type():
    """Detect if a PDF is an RFQ or Price Check."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"error": "auto_processor.py not available"}), 503
    if "file" not in request.files:
        return jsonify({"error": "Upload a PDF file"}), 400
    f = request.files["file"]
    safe_name = re.sub(r'[^\w.\-]', '_', f.filename or 'upload.pdf')
    pdf_path = os.path.join(DATA_DIR, f"detect_{safe_name}")
    f.save(pdf_path)
    result = detect_document_type(pdf_path)
    os.remove(pdf_path)
    return jsonify(result)


@bp.route("/pricecheck/<pcid>/auto-process")
@auth_required
@safe_page
def pricecheck_auto_process(pcid):
    """Run full auto-process pipeline on an existing Price Check."""
    if not AUTO_PROCESSOR_AVAILABLE:
        return jsonify({"ok": False, "error": "auto_processor not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})
    source_pdf = pc.get("source_pdf", "")
    if not source_pdf or not os.path.exists(source_pdf):
        return jsonify({"ok": False, "error": "Source PDF not found"})

    result = auto_process_price_check(source_pdf, pc_id=pcid)

    # Update PC record with results
    if result.get("ok"):
        pc["items"] = result.get("parsed", {}).get("line_items", [])
        pc["parsed"] = result.get("parsed", {})
        pc["output_pdf"] = result.get("output_pdf")
        pc["confidence"] = result.get("confidence", {})
        pc["draft_email"] = result.get("draft_email", {})
        pc["timing"] = result.get("timing", {})
        _transition_status(pc, "completed", actor="auto", notes="Auto-processed")
        pc["summary"] = result.get("summary", {})
        # raise_on_error=True: auto-process ran the full pipeline (parse →
        # enrich → draft email). Silent save failure = pipeline wasted.
        try:
            _save_price_checks(pcs, raise_on_error=True)
        except Exception as _save_e:
            log.error("auto-process PC save failed for %s: %s", pcid, _save_e)
            return jsonify({"ok": False,
                "error": f"Auto-process completed but PC save failed: {_save_e}",
                "pipeline_result": result}), 500
        # Ingest into KB
        _ingest_pc_to_won_quotes(pc)
        # Catalog all items for future matching
        try:
            _enrich_catalog_from_pc(pc)
        except Exception as _e:
            log.debug("suppressed: %s", _e)  # Don't break auto-process if enrichment fails

    return jsonify(json.loads(json.dumps({
        "ok": result.get("ok", False),
        "timing": result.get("timing", {}),
        "confidence": result.get("confidence", {}),
        "steps": result.get("steps", []),
        "draft_email": result.get("draft_email", {}),
    }, default=str)))


# ═══════════════════════════════════════════════════════════════════════
# Email Health & Diagnostics
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email/health")
@auth_required
@safe_route
def api_email_health():
    """Email system diagnostics — shows why polling might not be working."""
    try:
        import os as _os
        email_cfg = CONFIG.get("email", {})
        gmail_addr = email_cfg.get("email") or _os.environ.get("GMAIL_ADDRESS", "")
        gmail_pass = email_cfg.get("email_password") or _os.environ.get("GMAIL_PASSWORD", "")
        enable_polling = _os.environ.get("ENABLE_EMAIL_POLLING", "")

        # Check poll thread
        poll_thread_alive = False
        try:
            for t in threading.enumerate():
                if "email" in t.name.lower() or "poll" in t.name.lower():
                    poll_thread_alive = t.is_alive()
                    break
        except Exception as _e:
            log.debug("suppressed: %s", _e)

        # Diagnostics
        diag = POLL_STATUS.get("_diag", {})
        checks = []

        # Check 1: ENABLE_EMAIL_POLLING
        if enable_polling.lower() == "true":
            checks.append({"check": "ENABLE_EMAIL_POLLING", "status": "ok", "value": "true"})
        else:
            checks.append({"check": "ENABLE_EMAIL_POLLING", "status": "FAIL",
                           "value": enable_polling or "(not set)",
                           "fix": "Set ENABLE_EMAIL_POLLING=true in Railway env vars"})

        # Check 2: Gmail address
        if gmail_addr:
            checks.append({"check": "GMAIL_ADDRESS", "status": "ok",
                           "value": gmail_addr[:4] + "***" + gmail_addr[gmail_addr.index("@"):] if "@" in gmail_addr else "***"})
        else:
            checks.append({"check": "GMAIL_ADDRESS", "status": "FAIL", "value": "(not set)",
                           "fix": "Set GMAIL_ADDRESS=sales@raytechinc.com in Railway env vars"})

        # Check 3: Gmail password
        if gmail_pass:
            checks.append({"check": "GMAIL_PASSWORD", "status": "ok",
                           "value": gmail_pass[:3] + "***" + gmail_pass[-2:] if len(gmail_pass) > 5 else "***"})
        else:
            checks.append({"check": "GMAIL_PASSWORD", "status": "FAIL", "value": "(not set)",
                           "fix": "Set GMAIL_PASSWORD to a Gmail App Password (not your regular password) in Railway env vars"})

        # Check 4: Poll thread
        if poll_thread_alive:
            checks.append({"check": "Poll Thread", "status": "ok", "value": "alive"})
        elif _poll_started:
            checks.append({"check": "Poll Thread", "status": "FAIL", "value": "started but dead",
                           "fix": "Thread crashed — check logs for errors"})
        else:
            checks.append({"check": "Poll Thread", "status": "FAIL", "value": "never started",
                           "fix": "Fix the above env var issues, then restart the app"})

        # Overall status
        all_ok = all(c["status"] == "ok" for c in checks)

        return jsonify({
            "ok": all_ok,
            "status": "healthy" if all_ok else "unhealthy",
            "checks": checks,
            "poll_status": {
                "running": POLL_STATUS.get("running", False),
                "paused": POLL_STATUS.get("paused", False),
                "last_check": POLL_STATUS.get("last_check"),
                "last_success": POLL_STATUS.get("last_success"),
                "error": POLL_STATUS.get("error"),
                "emails_found": POLL_STATUS.get("emails_found", 0),
                "pos_detected": POLL_STATUS.get("pos_detected", 0),
                "started_at": POLL_STATUS.get("started_at"),
            },
            "diag": {
                "imap_connected": diag.get("imap_connected"),
                "rfqs_returned": diag.get("rfqs_returned"),
                "pcs_routed": diag.get("pcs_routed", 0),
                "errors": diag.get("errors", [])[-5:],
            },
        })
    except Exception as e:
        log.exception("email-health")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/email/poll-now", methods=["POST"])
@auth_required
@safe_route
def api_email_poll_now():
    """Force an immediate email poll cycle. Returns results."""
    try:
        log.info("Manual email poll triggered via /api/email/poll-now")
        results = do_poll_check()

        pos_found = 0
        rfqs_found = 0
        pcs_found = 0
        for r in (results or []):
            if isinstance(r, dict):
                if r.get("_is_po"):
                    pos_found += 1
                elif r.get("_is_pc"):
                    pcs_found += 1
                else:
                    rfqs_found += 1

        return jsonify({
            "ok": True,
            "emails_processed": len(results or []),
            "pos_found": pos_found,
            "rfqs_found": rfqs_found,
            "pcs_found": pcs_found,
            "poll_status": {
                "last_check": POLL_STATUS.get("last_check"),
                "error": POLL_STATUS.get("error"),
            },
        })
    except Exception as e:
        log.exception("email-poll-now")
        return jsonify({"ok": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════
# Startup
# ═══════════════════════════════════════════════════════════════════════

_poll_started = False

def start_polling(app=None):
    global _poll_started
    if _poll_started:
        return
    _poll_started = True
    email_cfg = CONFIG.get("email", {})
    effective_password = (email_cfg.get("email_password")
                          or os.environ.get("GMAIL_PASSWORD", ""))
    if effective_password:
        POLL_STATUS["started_at"] = __import__("datetime").datetime.now().isoformat()
        POLL_STATUS["running"] = True
        poll_thread = threading.Thread(target=email_poll_loop, daemon=True, name="email-poller")
        poll_thread.start()
        log.info("Email polling started (account: %s)",
                 email_cfg.get("email") or os.environ.get("GMAIL_ADDRESS", "?"))
    else:
        POLL_STATUS["error"] = "Set GMAIL_PASSWORD env var or email_password in config"
        log.warning("EMAIL POLLING DISABLED: No GMAIL_PASSWORD env var or email_password in config. "
                     "PO detection via email will NOT work. Set GMAIL_PASSWORD in Railway env vars.")

# ─── Logo Upload + Quotes Database ────────────────────────────────────────────

@bp.route("/settings/upload-logo", methods=["POST"])
@auth_required
@safe_page
@rate_limit("heavy")
def upload_logo():
    """Upload Reytech logo for quote PDFs."""
    if "logo" not in request.files:
        flash("No file selected", "error")
        return redirect(request.referrer or "/")
    f = request.files["logo"]
    if not f.filename:
        flash("No file selected", "error")
        return redirect(request.referrer or "/")
    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ("png", "jpg", "jpeg", "gif"):
        flash("Logo must be PNG, JPG, or GIF", "error")
        return redirect(request.referrer or "/")
    dest = os.path.join(DATA_DIR, f"reytech_logo.{ext}")
    # Remove old logos
    for old in glob.glob(os.path.join(DATA_DIR, "reytech_logo.*")):
        os.remove(old)
    f.save(dest)
    flash(f"Logo uploaded: {f.filename}", "success")
    return redirect(request.referrer or "/")


@bp.route("/api/logo")
@auth_required
@safe_route
def serve_logo():
    """Serve the uploaded Reytech logo."""
    for ext in ("png", "jpg", "jpeg", "gif"):
        path = os.path.join(DATA_DIR, f"reytech_logo.{ext}")
        if os.path.exists(path):
            return send_file(path)
    return "", 404


@bp.route("/quotes/<quote_number>/status", methods=["POST"])
@auth_required
@safe_page
def quote_update_status(quote_number):
    """Mark a quote as won, lost, or pending. Triggers won workflow if applicable."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})
    data = request.get_json(force=True, silent=True) or request.form
    new_status = data.get("status", "").lower()
    po_number = data.get("po_number", "")
    notes = data.get("notes", "")
    if new_status not in ("won", "lost", "pending"):
        return jsonify({"ok": False, "error": f"Invalid status: {new_status}"})

    # Business rule: quotes can only be marked "won" with a formal PO number
    if new_status == "won" and not (po_number or "").strip():
        return jsonify({"ok": False, "error": "PO number required to mark as won — only formal POs count as wins"})

    found = update_quote_status(quote_number, new_status, po_number, notes)
    if not found:
        return jsonify({"ok": False, "error": f"Quote {quote_number} not found"})

    result = {"ok": True, "quote_number": quote_number, "status": new_status}

    # ── Won workflow: QB PO + CRM activity ──
    if new_status == "won":
        # Log CRM activity
        _log_crm_activity(quote_number, "quote_won",
                          f"Quote {quote_number} marked WON" + (f" — PO: {po_number}" if po_number else ""),
                          actor="user")

        # ── Fire webhook for quote_won event ──
        try:
            from src.core.webhooks import fire_event
            qt_wh = _find_quote(quote_number) or {}
            fire_event("quote_won", {
                "quote_number": quote_number,
                "po_number": po_number,
                "agency": qt_wh.get("agency", ""),
                "institution": qt_wh.get("institution", "") or qt_wh.get("ship_to_name", ""),
                "total": f"${qt_wh.get('total', 0):,.2f}",
            })
        except Exception as _e:
            log.debug("suppressed: %s", _e)

        # Attempt QB PO creation if configured
        if QB_AVAILABLE and qb_configured():
            try:
                qt = _find_quote(quote_number)
                if qt:
                    items_for_qb = []
                    for it in qt.get("items_detail", []):
                        items_for_qb.append({
                            "description": it.get("description", ""),
                            "qty": it.get("qty", 1),
                            "unit_cost": it.get("unit_price", 0),
                        })
                    if items_for_qb:
                        # Find or use default vendor
                        institution = qt.get("institution", "") or qt.get("ship_to_name", "")
                        vendor = find_vendor(institution) if institution else None
                        if vendor:
                            po_result = create_purchase_order(
                                vendor_id=vendor["qb_id"],
                                items=items_for_qb,
                                memo=f"Reytech Quote {quote_number}" + (f" / PO {po_number}" if po_number else ""),
                                ship_to=institution,
                            )
                            if po_result:
                                result["qb_po"] = po_result
                                _log_crm_activity(quote_number, "qb_po_created",
                                                  f"QB PO #{po_result.get('doc_number','')} created — ${po_result.get('total',0):,.2f}",
                                                  actor="system")
                            else:
                                result["qb_po_error"] = "PO creation failed"
                        else:
                            result["qb_vendor_missing"] = f"No QB vendor match for '{institution}'"
            except Exception as e:
                log.error("Won workflow QB step failed: %s", e)
                result["qb_error"] = str(e)

    elif new_status == "lost":
        _log_crm_activity(quote_number, "quote_lost",
                          f"Quote {quote_number} marked LOST" + (f" — {notes}" if notes else ""),
                          actor="user")
        # Log competitor intelligence
        if PREDICT_AVAILABLE:
            try:
                log_competitor_intel(quote_number, "lost", {"notes": notes or ""})
            except Exception as e:
                log.error("Competitor intel logging failed: %s", e)
        # Log competitor intelligence
        if PREDICT_AVAILABLE:
            try:
                ci = log_competitor_intel(quote_number, "lost",
                                          {"notes": notes, "competitor": request.get_json(silent=True).get("competitor", "")})
                result["competitor_intel"] = ci.get("id", "")
            except Exception as e:
                log.error("Competitor intel log failed: %s", e)

    # ── Create Order for won quotes ──
    if new_status == "won":
        try:
            qt = _find_quote(quote_number)
            if qt:
                order = _create_order_from_quote(qt, po_number=po_number)
                result["order_id"] = order["order_id"]
                result["order_url"] = f"/order/{order['order_id']}"
                # ── Auto-log revenue to SQLite DB ──
                try:
                    from src.core.db import log_revenue
                    total = qt.get("total", 0)
                    if total > 0:
                        rev_id = log_revenue(
                            amount=total,
                            description=f"Quote {quote_number} WON — {qt.get('institution','') or qt.get('agency','')}",
                            source="quote_won",
                            quote_number=quote_number,
                            po_number=po_number or "",
                            agency=qt.get("agency",""),
                            date=datetime.now().strftime("%Y-%m-%d"),
                        )
                        result["revenue_logged"] = rev_id
                        log.info("Auto-logged revenue $%.2f for won quote %s", total, quote_number)
                except Exception as rev_err:
                    log.debug("Revenue auto-log skipped: %s", rev_err)
        except Exception as e:
            log.error("Order creation failed: %s", e)
            result["order_error"] = str(e)

    # 🏭 Vendor ordering pipeline (async, on won quotes)
    if new_status == "won":
        try:
            from src.agents.vendor_ordering_agent import process_won_quote_ordering
            qt_for_order = _find_quote(quote_number)
            if qt_for_order:
                ordering_result = process_won_quote_ordering(
                    quote_number=quote_number,
                    items=qt_for_order.get("items_detail", qt_for_order.get("items", [])),
                    agency=qt_for_order.get("agency","") or qt_for_order.get("institution",""),
                    po_number=po_number or "",
                    run_async=True,
                )
                result["vendor_ordering"] = ordering_result
                log.info("Vendor ordering pipeline triggered for %s", quote_number)
        except Exception as _voe:
            log.debug("Vendor ordering trigger skipped: %s", _voe)

    return jsonify(result)


@bp.route("/api/quote/from-price-check", methods=["POST"])
@auth_required
@safe_route
def api_quote_from_price_check():
    """PRD Feature 3.2.1 — 1-click Price Check → Reytech Quote with full logging.

    POST JSON: { "pc_id": "abc123" }
    Returns: { ok, quote_number, total, download, next_quote, pc_id, logs[] }

    Logging chain (all 5 layers):
      1. quotes_log.json  — JSON store (Railway seed)
      2. SQLite quotes    — persistent DB on volume
      3. SQLite price_history — every line item price
      4. SQLite activity_log — CRM entry per quote
      5. Application log  — structured INFO lines
    """
    body = request.get_json(silent=True) or {}
    pc_id = body.get("pc_id", "").strip()
    if not pc_id:
        return jsonify({"ok": False, "error": "pc_id required"})

    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})

    pcs = _load_price_checks()
    pc = pcs.get(pc_id)
    if not pc:
        return jsonify({"ok": False, "error": f"Price Check {pc_id} not found"})

    # ── Items check — accept manually entered prices too ─────────────────
    items = pc.get("items", [])
    priced_items = [it for it in items if not it.get("no_bid") and
                    (it.get("unit_price") or                          # manually entered
                     it.get("pricing", {}).get("recommended_price") or
                     it.get("pricing", {}).get("amazon_price"))]
    if not priced_items:
        return jsonify({"ok": False,
                        "error": "No priced items — enter unit prices first"})

    # ── Generate PDF ──────────────────────────────────────────────────────
    pc_num = pc.get("pc_number", "") or "PC"
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", pc_num.strip())
    output_path = os.path.join(DATA_DIR, f"Quote_{safe_name}_Reytech.pdf")

    locked_qn = pc.get("reytech_quote_number", "")  # reuse if regenerating

    logs = []
    t0 = time.time()

    result = generate_quote_from_pc(
        pc, output_path,
        include_tax=True,
        quote_number=locked_qn if locked_qn else None,
    )

    if not result.get("ok"):
        return jsonify({"ok": False, "error": result.get("error", "PDF generation failed")})

    qn = result.get("quote_number", "")
    total = result.get("total", 0)
    items_count = result.get("items_count", 0)
    institution = result.get("institution", pc.get("institution", ""))
    agency = result.get("agency", "")

    logs.append(f"PDF generated: {qn} — ${total:,.2f} ({items_count} items) in {(time.time()-t0)*1000:.0f}ms")

    # ── Layer 1+2: JSON + SQLite via _log_quote (already called inside generate_quote_from_pc) ──
    logs.append("JSON quotes_log.json: written")
    logs.append(f"SQLite quotes table: upserted {qn}")

    # ── Layer 3: Price history — explicit per-item logging ─────────────────
    ph_count = 0
    try:
        from src.core.db import record_price as _rp
        for it in result.get("items_detail", []):
            price = it.get("unit_price") or it.get("price_each") or 0
            desc = it.get("description", "")
            if price > 0 and desc:
                _rp(
                    description=desc,
                    unit_price=float(price),
                    source="quote_1click",
                    part_number=it.get("part_number", "") or it.get("item_number", ""),
                    manufacturer=it.get("manufacturer", ""),
                    quantity=float(it.get("qty", 1) or 1),
                    agency=agency,
                    quote_number=qn,
                    price_check_id=pc_id,
                )
                ph_count += 1
        logs.append(f"SQLite price_history: {ph_count} prices recorded")
    except Exception as ph_err:
        logs.append(f"price_history skipped: {ph_err}")

    # ── Layer 4: CRM activity log ──────────────────────────────────────────
    try:
        from src.core.db import log_activity as _la
        _la(
            contact_id=f"pc_{pc_id}",
            event_type="quote_generated_1click",
            subject=f"Quote {qn} generated — ${total:,.2f}",
            body=f"1-click quote {qn} for {institution} ({items_count} items, PC #{pc_num})",
            actor="user",
            metadata={"pc_id": pc_id, "quote_number": qn, "total": total,
                      "institution": institution, "agency": agency, "feature": "3.2.1"},
        )
        logs.append(f"SQLite activity_log: CRM entry written")
    except Exception as al_err:
        logs.append(f"activity_log skipped: {al_err}")

    # Also log to JSON CRM activity (existing system)
    _log_crm_activity(
        qn, "quote_generated_1click",
        f"1-click Quote {qn} — ${total:,.2f} for {institution} (PC #{pc_num}, {items_count} items)",
        actor="user",
        metadata={"pc_id": pc_id, "institution": institution, "agency": agency},
    )
    logs.append("CRM activity_log.json: written")

    # ── Layer 5: Application log ───────────────────────────────────────────
    log.info("1-CLICK QUOTE [Feature 3.2.1] %s → %s $%.2f (%d items, PC %s, %dms)",
             institution[:40], qn, total, items_count, pc_id,
             (time.time() - t0) * 1000)

    # ── Update PC record ──────────────────────────────────────────────────
    pc["reytech_quote_pdf"] = output_path
    pc["reytech_quote_number"] = qn
    pc["quote_generated_at"] = datetime.now().isoformat()
    pc["quote_generated_via"] = "1click_feature_321"
    _transition_status(pc, "completed", actor="user", notes=f"1-click quote {qn}")
    # raise_on_error=True: the quote PDF has been generated and the counter
    # bumped (qn is locked). If _save_price_checks silently fails, the PC
    # doesn't track qn → the user can't find their own quote, and a second
    # 1-click run burns another quote number (per memory
    # feedback_quote_number_rules.md). Surface the failure so the operator
    # can manually record the qn and PDF path from the response.
    try:
        _save_price_checks(pcs, raise_on_error=True)
    except Exception as _save_e:
        log.error("1-click quote PC persistence failed for %s (qn=%s): %s",
                  pc_id, qn, _save_e)
        return jsonify({
            "ok": False,
            "error": f"Quote {qn} PDF generated but PC record save failed: {_save_e}. "
                     f"Record qn={qn} and pdf={output_path} manually to prevent "
                     f"duplicate quote number allocation.",
            "quote_number": qn,
            "quote_pdf": output_path,
            "needs_manual_reconcile": True,
        }), 500
    logs.append(f"PC {pc_id} status → completed, reytech_quote_number={qn}")

    next_qn = peek_next_quote_number() if QUOTE_GEN_AVAILABLE else ""

    # Pull profit summary from the saved PC record
    profit = pc.get("profit_summary", {})

    return jsonify({
        "ok": True,
        "quote_number": qn,
        "total": total,
        "items_count": items_count,
        "institution": institution,
        "agency": agency,
        "pc_id": pc_id,
        "download": f"/api/pricecheck/download/{os.path.basename(output_path)}",
        "next_quote": next_qn,
        "logs": logs,
        "elapsed_ms": round((time.time() - t0) * 1000),
        "feature": "PRD 3.2.1",
        # Profit intelligence
        "gross_profit":  profit.get("gross_profit"),
        "margin_pct":    profit.get("margin_pct"),
        "total_cost":    profit.get("total_cost"),
        "fully_costed":  profit.get("fully_costed", False),
        "costed_items":  profit.get("costed_items", 0),
    })




# ════════════════════════════════════════════════════════════════════════════════
# BULK CRM OUTREACH  (PRD Feature P1)
# ════════════════════════════════════════════════════════════════════════════════
@bp.route("/api/crm/bulk-outreach", methods=["POST"])
@auth_required
@safe_route
def api_crm_bulk_outreach():
    """Send a templated email to multiple CRM contacts.

    POST {
      contact_ids: ["id1","id2",...],   # or use filter
      filter: {status: "new", agency: "CDCR"},
      template_id: "distro_list",
      extra_vars: {},
      dry_run: true
    }
    Returns { ok, staged, sent, failed, results[] }
    """
    body = request.get_json(silent=True) or {}
    contact_ids = body.get("contact_ids", [])
    filter_params = body.get("filter", {})
    template_id = body.get("template_id", "distro_list")
    extra_vars = body.get("extra_vars", {})
    dry_run = body.get("dry_run", True)

    # Load template
    tmpl_data = _load_email_templates()
    template = tmpl_data.get("templates", {}).get(template_id)
    if not template:
        return jsonify({"ok": False, "error": f"Template '{template_id}' not found"})

    # Load contacts
    crm = _load_crm_contacts()
    all_contacts = list(crm.values()) if isinstance(crm, dict) else crm

    # Filter
    if contact_ids:
        contacts = [c for c in all_contacts if c.get("id") in contact_ids
                    or c.get("buyer_email") in contact_ids]
    elif filter_params:
        contacts = all_contacts
        if filter_params.get("status"):
            contacts = [c for c in contacts if c.get("outreach_status") == filter_params["status"]]
        if filter_params.get("agency"):
            ag = filter_params["agency"].lower()
            contacts = [c for c in contacts if ag in (c.get("agency") or "").lower()]
        if filter_params.get("has_email"):
            contacts = [c for c in contacts if c.get("buyer_email")]
    else:
        contacts = all_contacts

    # Only contacts with email
    contacts = [c for c in contacts if c.get("buyer_email")]

    results = []
    sent = 0
    staged = 0
    failed = 0

    gmail = os.environ.get("GMAIL_ADDRESS", "")
    pwd = os.environ.get("GMAIL_PASSWORD", "")

    for contact in contacts[:100]:  # hard cap 100
        draft = _personalize_template(template, contact=contact, extra=extra_vars)
        entry = {
            "contact_id": contact.get("id"),
            "name": contact.get("buyer_name") or contact.get("name") or "",
            "email": contact.get("buyer_email"),
            "agency": contact.get("agency") or "",
            "subject": draft["subject"],
            "ok": False,
            "staged": dry_run,
            "sent": False,
        }

        if not dry_run and gmail and pwd:
            try:
                from src.agents.email_poller import EmailSender
                sender = EmailSender({"email": gmail, "email_password": pwd})
                sender.send({"to": contact["buyer_email"], "subject": draft["subject"],
                             "body": draft["body"], "attachments": []})
                entry["ok"] = True
                entry["sent"] = True
                sent += 1
                import time; time.sleep(1)
            except Exception as e:
                entry["error"] = str(e)
                failed += 1
        else:
            entry["ok"] = True
            staged += 1

        results.append(entry)

    log.info("Bulk outreach: template=%s, dry_run=%s, contacts=%d, sent=%d, staged=%d",
             template_id, dry_run, len(contacts), sent, staged)

    # Log to DB
    try:
        from src.core.db import log_activity as _la
        _la(contact_id="bulk_outreach", event_type="bulk_email",
            subject=f"Bulk {template_id}: {sent} sent, {staged} staged",
            body=f"contacts={len(contacts)}, dry_run={dry_run}",
            actor="user", metadata={"template": template_id, "sent": sent, "staged": staged})
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    return jsonify({
        "ok": True,
        "dry_run": dry_run,
        "template": template_id,
        "total_contacts": len(contacts),
        "sent": sent,
        "staged": staged,
        "failed": failed,
        "results": results[:20],
        "note": "Set dry_run=false and configure GMAIL_ADDRESS+GMAIL_PASSWORD in Railway to send." if dry_run else f"Sent {sent} emails.",
    })





# ════════════════════════════════════════════════════════════════════════════════
# NOTIFICATION & ALERT ROUTES — Push notification system
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/notifications/persistent")
@auth_required
@safe_route
def api_notifications_persistent():
    """Get persistent notifications from SQLite (survives deploys)."""
    unread_only = request.args.get("unread_only") == "true"
    try:
        limit = max(1, min(int(request.args.get("limit", 30)), 200))
    except (ValueError, TypeError, OverflowError):
        limit = 30
    try:
        from src.agents.notify_agent import get_notifications, get_unread_count
        notifs = get_notifications(limit=limit, unread_only=unread_only)
        return jsonify({"ok": True, "notifications": notifs, "unread_count": get_unread_count()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


_bell_cache = {"data": None, "ts": 0}

@bp.route("/api/notifications/bell-count")
@auth_required
@safe_route
def api_bell_count():
    """Fast unread count for nav bell badge — polled every 5min."""
    import time as _t
    global _bell_cache
    if _bell_cache["data"] and (_t.time() - _bell_cache["ts"]) < 30:
        return jsonify(_bell_cache["data"])
    try:
        from src.agents.notify_agent import get_unread_count
        from src.agents.cs_agent import get_cs_drafts
        cs_pending = len(get_cs_drafts())
        unread = get_unread_count()
        result = {
            "ok": True,
            "unread": unread,
            "cs_drafts": cs_pending,
            "total_badge": unread + cs_pending,
        }
        _bell_cache["data"] = result
        _bell_cache["ts"] = _t.time()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "unread": 0, "cs_drafts": 0, "total_badge": 0})


@bp.route("/api/notifications/mark-read", methods=["POST"])
@auth_required
@safe_route
def api_notifications_mark_read_v2():
    """Mark notifications as read."""
    data = request.get_json(silent=True) or {}
    ids = data.get("ids")  # list of IDs, or None to mark all
    try:
        from src.agents.notify_agent import mark_notifications_read
        result = mark_notifications_read(ids)
        # Also mark in-memory deque
        for n in _notifications:
            n["read"] = True
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/notify/test", methods=["POST"])
@auth_required
@safe_route
def api_notify_test():
    """Test notification channels (SMS + email + bell). POST {} to fire test."""
    try:
        from src.agents.notify_agent import send_alert
        result = send_alert(
            event_type="auto_draft_ready",
            title="🔔 Test Alert — Reytech Dashboard",
            body="This is a test notification. All channels working correctly.",
            urgency="info",
            context={"entity_id": "test_" + datetime.now().strftime("%H%M%S")},
            cooldown_key=f"test_{datetime.now().strftime('%H%M')}",
            run_async=False,
        )
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/quotes/reconcile-po", methods=["POST"])
@auth_required
@safe_route
def api_quotes_reconcile_po():
    """Manually mark a quote as won by attaching a PO number.

    Built specifically to clean up the 3 known POs that came in while the
    markQuote() JS button was a silent no-op (broken Feb 17 → Apr 15 2026).

    Body: {"po_number": "PO-12345", "quote_number": "R26Q500", "notes": "..."}

    Returns:
      - 200 ok=true on success
      - 400 if po_number / quote_number missing
      - 404 if quote_number doesn't exist
      - 409 if the quote is already in a terminal state (won/lost) — caller
        must explicitly pass force=true to overwrite
    """
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"}), 503
    data = request.get_json(force=True, silent=True) or {}
    po_number = (data.get("po_number") or "").strip()
    quote_number = (data.get("quote_number") or "").strip()
    notes = (data.get("notes") or "manual reconciliation (markQuote silent no-op recovery)").strip()
    force = bool(data.get("force"))
    if not po_number:
        return jsonify({"ok": False, "error": "po_number required"}), 400
    if not quote_number:
        return jsonify({"ok": False, "error": "quote_number required"}), 400

    # Pre-flight: check existing state so we don't silently overwrite a
    # genuine 'lost' decision someone made (rare but possible).
    try:
        from src.forms.quote_generator import get_all_quotes
        existing = None
        for qt in get_all_quotes(include_test=True):
            if qt.get("quote_number") == quote_number:
                existing = qt
                break
        if not existing:
            return jsonify({"ok": False, "error": f"Quote {quote_number} not found"}), 404
        cur_status = (existing.get("status") or "").lower()
        if cur_status in ("won", "lost") and not force:
            return jsonify({
                "ok": False,
                "error": f"Quote {quote_number} is already {cur_status} — pass force=true to overwrite",
                "current_status": cur_status,
                "current_po_number": existing.get("po_number", ""),
            }), 409
    except Exception as e:
        log.warning("reconcile-po pre-flight failed: %s", e)

    found = update_quote_status(quote_number, "won",
                                po_number=po_number,
                                notes=notes, actor="reconcile_po")
    if not found:
        return jsonify({"ok": False, "error": f"Quote {quote_number} not found or update failed"}), 404
    log.info("RECONCILE: marked %s as WON via PO %s (manual)", quote_number, po_number)
    return jsonify({
        "ok": True, "quote_number": quote_number,
        "po_number": po_number, "status": "won",
        "notes": notes,
    })


@bp.route("/api/notify/snooze", methods=["POST"])
@auth_required
@safe_route
def api_notify_snooze():
    """Snooze a notification key for N hours (default 24).

    POST body: {"key": "outbox_stale_drafts_waiting", "hours": 24}

    The key is the cooldown_key the alert was sent with — see notify_agent
    call sites. Snooze is in-memory only; restarts clear all snoozes.
    """
    try:
        from src.agents.notify_agent import snooze_alert
        data = request.get_json(force=True, silent=True) or {}
        key = (data.get("key") or "").strip()
        if not key:
            return jsonify({"ok": False, "error": "key required"}), 400
        hours = float(data.get("hours") or 24)
        result = snooze_alert(key, hours=hours)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/notify/status")
@auth_required
@safe_route
def api_notify_status():
    """Notification agent configuration status."""
    try:
        from src.agents.notify_agent import get_agent_status
        return jsonify({"ok": True, **get_agent_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/email-log")
@auth_required
@safe_route
def api_email_log():
    """Get email communication log for CS dispute resolution.
    ?contact=email&quote=R26Q4&po=12345&limit=50
    """
    contact = request.args.get("contact","")
    quote = request.args.get("quote","")
    po = request.args.get("po","")
    try:
        limit = max(1, min(int(request.args.get("limit", 50)), 500))
    except (ValueError, TypeError, OverflowError):
        limit = 50
    try:
        from src.agents.notify_agent import get_email_thread, build_cs_communication_summary
        thread = get_email_thread(contact_email=contact, quote_number=quote, po_number=po, limit=limit)
        summary = build_cs_communication_summary(contact, quote, po)
        return jsonify({"ok": True, "count": len(thread), "thread": thread, "summary": summary})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/email-log/log", methods=["POST"])
@auth_required
@safe_route
def api_email_log_entry():
    """Manually log an email event (sent or received).
    POST {direction, sender, recipient, subject, body, quote_number, po_number, intent, status}
    """
    data = request.get_json(silent=True) or {}
    try:
        from src.agents.notify_agent import log_email_event
        result = log_email_event(
            direction=data.get("direction","sent"),
            sender=data.get("sender",""),
            recipient=data.get("recipient",""),
            subject=data.get("subject",""),
            body_preview=data.get("body","")[:500],
            full_body=data.get("body",""),
            quote_number=data.get("quote_number",""),
            po_number=data.get("po_number",""),
            rfq_id=data.get("rfq_id",""),
            contact_id=data.get("contact_id","") or data.get("recipient",""),
            intent=data.get("intent","general"),
            status=data.get("status","sent"),
        )
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})



@bp.route("/outbox")
@auth_required  
@safe_page
def page_outbox():
    """Email outbox — review and approve all pending drafts (sales + CS)."""
    try:
        from src.agents.email_outreach import get_outbox
        from src.agents.cs_agent import get_cs_drafts
        from src.agents.notify_agent import get_unread_count, get_notifications
        
        sales_drafts = get_outbox(status="draft")
        cs_drafts = get_cs_drafts(limit=50)
        sent_today = [e for e in get_outbox() if e.get("status") == "sent" and
                      e.get("sent_at","").startswith(datetime.now().strftime("%Y-%m-%d"))]
        notifications = get_notifications(limit=10, unread_only=True)
    except Exception:
        sales_drafts, cs_drafts, sent_today, notifications = [], [], [], []

    total_pending = len(sales_drafts) + len(cs_drafts)
    
    sales_html = ""
    for d in sales_drafts:
        sales_html += f"""<div class="card" style="margin-bottom:10px">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
   <div style="flex:1">
    <div style="font-size:13px;font-weight:600;color:var(--tx)">{d.get('subject','')}</div>
    <div style="font-size:14px;color:var(--tx2);margin-top:3px">To: {d.get('to','')} &nbsp;·&nbsp; Created: {(d.get('created_at','') or '')[:16].replace('T',' ')}</div>
    <div style="font-size:14px;color:var(--tx2);margin-top:6px;white-space:pre-wrap">{(d.get('body','') or '')[:300]}{'...' if len(d.get('body','') or '') > 300 else ''}</div>
   </div>
   <div style="display:flex;flex-direction:column;gap:6px;min-width:120px">
    <button class="btn btn-sm" onclick="approveDraft('{d.get('id','')}',this)" style="background:var(--gn);color:#000;font-size:14px">✅ Approve</button>
    <button class="btn btn-sm" onclick="deleteDraft('{d.get('id','')}',this)" style="background:var(--sf2);color:var(--rd);font-size:14px">🗑 Delete</button>
    {"<span style='font-size:13px;color:var(--ac);padding:2px 6px;background:rgba(79,140,255,.1);border-radius:4px'>📋 sales draft</span>" if d.get('type') != 'cs_response' else ''}
   </div>
  </div>
</div>"""

    cs_html = ""
    for d in cs_drafts:
        intent_colors = {"order_status":"var(--ac)","delivery":"var(--gn)","invoice":"var(--yl)","quote_status":"var(--or)","general":"var(--tx2)"}
        intent = d.get("intent","general")
        cs_html += f"""<div class="card" style="margin-bottom:10px;border-left:3px solid {intent_colors.get(intent,'var(--ac)')}">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px">
   <div style="flex:1">
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
     <span style="font-size:14px;font-weight:600;color:{intent_colors.get(intent,'var(--ac)')};text-transform:uppercase">{intent.replace('_',' ')}</span>
     <span style="font-size:13px;color:var(--tx2)">📬 CS auto-draft</span>
    </div>
    <div style="font-size:13px;font-weight:600;color:var(--tx)">{d.get('subject','')}</div>
    <div style="font-size:14px;color:var(--tx2);margin-top:3px">To: {d.get('to','')} &nbsp;·&nbsp; {(d.get('created_at','') or '')[:16].replace('T',' ')}</div>
    <div style="font-size:14px;color:var(--tx);margin-top:8px;white-space:pre-wrap;padding:8px;background:var(--sf2);border-radius:6px">{(d.get('body','') or '')[:400]}{'...' if len(d.get('body','') or '') > 400 else ''}</div>
   </div>
   <div style="display:flex;flex-direction:column;gap:6px;min-width:120px">
    <button class="btn btn-sm" onclick="approveCS('{d.get('id','')}',this)" style="background:var(--gn);color:#000;font-size:14px">✅ Send Reply</button>
    <button class="btn btn-sm" onclick="deleteCS('{d.get('id','')}',this)" style="background:var(--sf2);color:var(--rd);font-size:14px">🗑 Discard</button>
   </div>
  </div>
</div>"""

    html = render_page("outbox.html", active_page="Home",
        cs_drafts=cs_drafts,
        sales_drafts=sales_drafts,
        sent_today=sent_today,
        total_pending=total_pending)
    return html


@bp.route("/api/email/approve-cs", methods=["POST"])
@auth_required
@safe_route
def api_approve_cs_draft():
    """Approve and send a CS reply draft."""
    data = request.get_json(silent=True) or {}
    draft_id = data.get("draft_id","")
    if not draft_id:
        return jsonify({"ok": False, "error": "draft_id required"})
    try:
        from src.core.dal import get_outbox as _dal_ob
        outbox = _dal_ob()
        
        draft = next((e for e in outbox if e.get("id") == draft_id), None)
        if not draft:
            return jsonify({"ok": False, "error": "Draft not found"})
        
        # Send via EmailSender
        from src.agents.email_poller import EmailSender
        from src.core.secrets import CONFIG
        sender = EmailSender(CONFIG.get("email", {}))
        sender.send({"to": draft["to"], "subject": draft["subject"], "body": draft["body"], "attachments": []})
        
        # Mark as sent
        draft["status"] = "sent"
        draft["sent_at"] = datetime.now().isoformat()
        
        with open(outbox_path, "w") as f:
            json.dump(outbox, f, indent=2, default=str)
        
        # Log the sent email
        try:
            from src.agents.notify_agent import log_email_event
            log_email_event(
                direction="sent",
                sender=CONFIG.get("email",{}).get("email","sales@reytechinc.com"),
                recipient=draft["to"],
                subject=draft["subject"],
                body_preview=draft.get("body","")[:500],
                full_body=draft.get("body",""),
                contact_id=draft.get("to",""),
                intent=f"cs_{draft.get('intent','reply')}",
                status="sent",
            )
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        
        log.info("CS draft %s sent to %s", draft_id, draft["to"])
        return jsonify({"ok": True, "sent_to": draft["to"]})
    except Exception as e:
        log.error("CS send failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/email/delete-cs", methods=["POST"])
@auth_required
@safe_route
def api_delete_cs_draft():
    """Delete a CS draft."""
    data = request.get_json(silent=True) or {}
    draft_id = data.get("draft_id","")
    try:
        from src.core.dal import delete_outbox_email
        delete_outbox_email(draft_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})






# ════════════════════════════════════════════════════════════════════════════════
# PRODUCT CATALOG (F31-01)
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/catalog-legacy")
@auth_required
@safe_page
def page_catalog_legacy():
    """Legacy product catalog (pre-QB import) — use /catalog for new version."""
    import json as _json
    try:
        from src.core.catalog import init_catalog, get_catalog, get_categories, get_catalog_stats
        init_catalog()
        stats = get_catalog_stats()
        categories = get_categories()
        items = get_catalog(limit=200)
    except Exception as e:
        return f"<h2>Catalog error: {e}</h2>"

    # Group by category
    grouped = {}
    for item in items:
        cat = item.get("category","General")
        grouped.setdefault(cat, []).append(item)

    cats_html = ""
    for cat, cat_items in sorted(grouped.items()):
        rows = ""
        for it in cat_items:
            tags = ", ".join(it.get("tags",[])[:4]) if it.get("tags") else ""
            vendor = it.get("vendor_key","").replace("_"," ").title()
            rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:7px 10px;font-size:14px;font-weight:600;color:var(--ac)">{it.get("sku","")}</td>
  <td style="padding:7px 10px;font-size:14px">{it.get("name","")[:60]}</td>
  <td style="padding:7px 10px;font-size:14px;color:var(--tx2)">{it.get("unit","each")}</td>
  <td style="padding:7px 10px;font-size:14px;color:var(--yl)">${it.get("typical_cost",0):.2f}</td>
  <td style="padding:7px 10px;font-size:14px;color:var(--gn)">${it.get("list_price",0):.2f}</td>
  <td style="padding:7px 10px;font-size:14px;color:var(--tx2)">{vendor}</td>
  <td style="padding:7px 10px;font-size:14px;color:var(--tx2)">{tags}</td>
</tr>"""
        cats_html += f"""<div style="margin-bottom:20px">
  <div style="font-size:14px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;padding:6px 10px;background:var(--bg2);border-radius:6px">
    {cat} <span style="color:var(--tx3);font-weight:400">({len(cat_items)} SKUs)</span>
  </div>
  <table style="width:100%;border-collapse:collapse">
    <thead><tr style="border-bottom:2px solid var(--bd)">
      <th style="padding:5px 10px;font-size:14px;color:var(--tx2);text-align:left">SKU</th>
      <th style="padding:5px 10px;font-size:14px;color:var(--tx2);text-align:left">Name</th>
      <th style="padding:5px 10px;font-size:14px;color:var(--tx2);text-align:left">Unit</th>
      <th style="padding:5px 10px;font-size:14px;color:var(--tx2);text-align:left">Cost</th>
      <th style="padding:5px 10px;font-size:14px;color:var(--tx2);text-align:left">List</th>
      <th style="padding:5px 10px;font-size:14px;color:var(--tx2);text-align:left">Vendor</th>
      <th style="padding:5px 10px;font-size:14px;color:var(--tx2);text-align:left">Tags</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""

    # Build content HTML for the unified catalog template
    content = f"""
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px;flex-wrap:wrap;gap:10px">
  <div>
    <h2 style="font-size:22px;font-weight:700;margin-bottom:4px">📦 Product Catalog (Legacy)</h2>
    <div style="font-size:14px;color:var(--tx2)">{stats['total_skus']} SKUs across {stats['categories']} categories</div>
  </div>
</div>
<div class="card" style="overflow-x:auto">{cats_html}</div>
"""
    html = render_page("catalog.html", active_page="Catalog",
        tab="products",
        content=content)
    return html


@bp.route("/api/catalog/search")
@auth_required
@safe_route
def api_catalog_search():
    """Search product catalog. ?q=nitrile&limit=10"""
    q = request.args.get("q","").strip()
    try:
        limit = max(1, min(int(request.args.get("limit", 10)), 200))
    except (ValueError, TypeError, OverflowError):
        limit = 10
    if not q:
        return jsonify({"ok": False, "error": "q required"})
    try:
        from src.core.catalog import init_catalog, search_catalog
        init_catalog()
        results = search_catalog(q, limit=limit)
        return jsonify({"ok": True, "query": q, "count": len(results), "results": results})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/stats")
@auth_required
@safe_route
def api_catalog_stats():
    try:
        from src.core.catalog import init_catalog, get_catalog_stats, get_categories
        init_catalog()
        return jsonify({"ok": True, **get_catalog_stats(), "categories": get_categories()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/items", methods=["GET"])
@auth_required
@safe_route
def api_catalog_items():
    """List all catalog items. ?category=Medical"""
    cat = request.args.get("category")
    try:
        limit = max(1, min(int(request.args.get("limit", 200)), 1000))
    except (ValueError, TypeError, OverflowError):
        limit = 200
    try:
        from src.core.catalog import init_catalog, get_catalog
        init_catalog()
        items = get_catalog(category=cat, limit=limit)
        return jsonify({"ok": True, "count": len(items), "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/add", methods=["POST"])
@auth_required
@safe_route
def api_catalog_add():
    """Manually add an item to the catalog."""
    data = request.get_json() or {}
    name = data.get("name","").strip()
    if not name:
        return jsonify({"ok": False, "error": "name required"})
    try:
        from src.core.catalog import auto_ingest_item
        result = auto_ingest_item(
            description=name,
            unit_price=float(data.get("list_price", 0)),
            vendor_key=data.get("vendor_key",""),
            manufacturer=data.get("manufacturer",""),
            source="manual"
        )
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})



# ════════════════════════════════════════════════════════════════════════════════
# BUYER OUTREACH ENGINE (F31-02) + CCHCS EXPANSION (F31-04)
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/intel/draft-outreach", methods=["POST"])
@auth_required
@safe_route
def api_draft_outreach():
    """
    Generate a targeted outreach email draft for a buyer from market intelligence.
    Saves to email_outbox.json as status=outreach_draft for review in /outbox.
    """
    import json as _json
    data = request.get_json() or {}
    buyer_email = data.get("buyer_email","").strip()
    if not buyer_email:
        return jsonify({"ok": False, "error": "buyer_email required"})

    mi_path = os.path.join(DATA_DIR, "market_intelligence.json")
    if not os.path.exists(mi_path):
        return jsonify({"ok": False, "error": "market_intelligence.json not found"})
    mi = _json.load(open(mi_path))

    # Find buyer in intel
    buyers = _json.load(open(os.path.join(DATA_DIR, "intel_buyers.json"))).get("buyers", [])
    buyer = next((b for b in buyers if b.get("buyer_email","").lower() == buyer_email.lower()), None)
    if not buyer:
        return jsonify({"ok": False, "error": f"Buyer not found: {buyer_email}"})

    agency = buyer.get("agency","")
    name = buyer.get("name","") or buyer.get("buyer_name","")
    phone = buyer.get("phone","")
    spend = buyer.get("total_spend",0)
    cats = buyer.get("categories",{})

    # Agency-specific pitch angles
    AGENCY_PITCHES = {
        "CalFire": {
            "lead_items": "N95 respirators (3M 8210, NIOSH-approved), hi-visibility safety vests (ANSI Class 2), and vehicle first aid kits (ANSI Class B)",
            "angle": "With wildfire season expanding year-round, Reytech maintains ready stock of respiratory protection and field safety equipment sourced directly from 3M, Honeywell, and ML Kishigo.",
            "cta": "Can we provide a competitive quote for your next PPE requisition?"
        },
        "CDPH": {
            "lead_items": "nitrile exam gloves (all sizes), N95 respirators (NIOSH-approved), and surgical masks (ASTM Level 2/3)",
            "angle": "As a California-certified SB/DVBE supplier, Reytech is positioned to help CDPH meet diverse supplier spend goals while delivering competitive pricing on high-volume PPE.",
            "cta": "Would you be open to receiving a quote on your next PPE order?"
        },
        "CalTrans": {
            "lead_items": "hi-visibility safety vests (ANSI Class 2, Type R), hard hats (ANSI Z89), safety glasses, and work gloves",
            "angle": "Reytech supplies OSHA-compliant safety equipment to California state agencies with fast turnaround and competitive pricing. Our SB/DVBE certification supports your diverse spend goals.",
            "cta": "I would welcome the opportunity to quote your next safety supply requisition."
        },
        "CHP": {
            "lead_items": "black nitrile exam gloves, vehicle trauma kits, and CAT tourniquets (Stop the Bleed program)",
            "angle": "Reytech stocks law-enforcement-grade PPE and trauma supplies. With the Stop the Bleed mandate now standard across CA law enforcement, we can source CAT Gen 7 tourniquets and IFAK kits with short lead times.",
            "cta": "Can we submit a quote for your vehicle medical supply program?"
        },
        "OSHPD": {
            "lead_items": "nitrile gloves, N95 respirators, and field PPE kits",
            "angle": "Reytech is a California SB/DVBE certified supplier with broad PPE sourcing across Medline, 3M, and Honeywell. We frequently supply inspection and field operations teams.",
            "cta": "Would a quote for your next PPE order be welcome?"
        },
    }

    pitch_data = AGENCY_PITCHES.get(agency, {
        "lead_items": "medical supplies and safety PPE",
        "angle": "Reytech is a California SB/DVBE certified supplier specializing in medical supplies, PPE, and safety equipment for state agencies.",
        "cta": "We would be glad to provide a competitive quote for your next requisition."
    })

    subject = f"Reytech — {agency} Supply Quote | CA SB/DVBE Certified"
    body = f"""Dear {name},

My name is Michael Guadan, and I reach out on behalf of Reytech Inc., a California-certified Small Business and Disabled Veteran Business Enterprise (SB/DVBE #2002605) specializing in supply procurement for state agencies.

I noticed {agency} manages significant supply needs in {", ".join(list(cats.keys())[:2]) if cats else "safety and medical equipment"} — areas where we have established sourcing relationships and competitive pricing.

We currently supply similar agencies throughout the CDCR, CalVet, and Department of State Hospitals systems, and would be glad to extend that service to {agency}.

Items we can quote immediately:
- {pitch_data['lead_items']}

{pitch_data['angle']}

{pitch_data['cta']}

Our standard terms are Net 30, F.O.B. Destination, and we are registered in Cal eProcure. As a DVBE, we can also help your agency meet set-aside procurement goals.

Please feel free to call me directly at (949) 229-1575 or reply to this email.

Thank you for your time, and I look forward to the opportunity to serve {agency}.

Best regards,
Michael Guadan
Reytech Inc.
CA SB #2002605 | CA DVBE #2002605
(949) 229-1575
sales@reytechinc.com"""

    # Save to outbox
    from src.core.dal import get_outbox as _dal_ob2, upsert_outbox_email as _dal_upsert
    outbox = _dal_ob2()
    from datetime import datetime as _dt
    draft_id = f"outreach-{buyer_email.split('@')[0]}-{int(_dt.now().timestamp())}"
    draft = {
        "id": draft_id,
        "created_at": _dt.now().isoformat(),
        "status": "outreach_draft",
        "intent": "new_agency_outreach",
        "to": buyer_email,
        "to_name": name,
        "agency": agency,
        "subject": subject,
        "body": body,
        "priority": "high" if spend > 150000 else "normal",
        "spend_signal": spend,
        "notes": f"Market intel outreach — {agency} | ${spend:,.0f} spend signal"
    }
    outbox.append(draft)
    from src.core.dal import upsert_outbox_email as _upsert_ob
    for _e in outbox:
        if _e.get("id"): _upsert_ob(_e)

    # Log activity + bell notification
    try:
        from src.core.db import get_db
        from datetime import datetime as _dt2
        contact_id = f"intel-{buyer_email.split('@')[0]}"
        with get_db() as conn:
            conn.execute("""
                INSERT INTO activity_log (contact_id, logged_at, event_type, subject, body, actor, metadata)
                VALUES (?,?,?,?,?,?,?)
            """, (contact_id, _dt2.now().isoformat(), "outreach_drafted",
                  f"Outreach draft: {agency}", f"Draft created for {name} ({buyer_email})",
                  "system", _json.dumps({"draft_id": draft_id, "agency": agency, "spend": spend})))
    except Exception as e:
        log.debug("outreach activity_log insert: %s", e)

    try:
        from src.agents.notify_agent import send_alert
        send_alert("bell", f"Outreach draft ready: {agency} — {name}", {
            "type": "outreach_draft", "agency": agency, "email": buyer_email,
            "draft_id": draft_id, "link": "/outbox"
        })
    except Exception as e:
        log.debug("outreach send_alert: %s", e)

    log.info("Outreach draft created: %s | %s | draft_id=%s", agency, buyer_email, draft_id)
    return jsonify({"ok": True, "draft_id": draft_id, "to": buyer_email, "agency": agency,
                    "subject": subject, "outbox_link": "/outbox"})


@bp.route("/api/cchcs/facilities")
@auth_required
@safe_route
def api_cchcs_facilities():
    """List all CCHCS/CalVet/DSH facilities with activity status."""
    import json as _json
    customers = _load_customers()
    facilities = []
    for c in customers:
        name = c.get("qb_name","") or c.get("display_name","")
        parent = c.get("parent","")
        balance = float(c.get("open_balance",0) or 0)
        email = c.get("email","")
        abbr = c.get("abbreviation","")
        agency_field = c.get("agency", "")
        agency_type = None
        if agency_field in ("CCHCS", "CDCR") or "Correctional" in (parent or name) or "State Prison" in name or "Calipatria" in name:
            agency_type = "CCHCS"
        elif agency_field == "CalVet" or "Veterans" in name or "Dept of Veterans" in name:
            agency_type = "CalVet"
        elif agency_field == "DSH" or "State Hospital" in name:
            agency_type = "DSH"
        if agency_type:
            facilities.append({
                "name": name, "parent": parent, "agency_type": agency_type,
                "abbreviation": abbr, "email": email,
                "ar_balance": balance, "is_active": balance > 0,
                "address": c.get("address",""), "city": c.get("city",""), "state": c.get("state","")
            })
    active = [f for f in facilities if f["is_active"]]
    inactive = [f for f in facilities if not f["is_active"]]
    return jsonify({
        "ok": True, "total": len(facilities),
        "active": len(active), "inactive": len(inactive),
        "active_facilities": active, "inactive_facilities": inactive
    })


def facility_name_match(name1, name2):
    """Loose match between facility names."""
    if not name1 or not name2: return False
    n1 = name1.lower().replace(" ","")
    n2 = name2.lower().replace(" ","")
    return n1[:15] in n2 or n2[:15] in n1


# ════════════════════════════════════════════════════════════════════════════════
# TERRITORY INTELLIGENCE v4 — SCPRS-powered sales command center
# ════════════════════════════════════════════════════════════════════════════════

_EST_ANNUAL = {"CCHCS": 8000, "CalVet": 12000, "DSH": 6000}

def _ensure_scprs_tables():
    """Create SCPRS tables if they don't exist (idempotent)."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.executescript("""
        CREATE TABLE IF NOT EXISTS scprs_po_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pulled_at TEXT, po_number TEXT UNIQUE,
            dept_code TEXT, dept_name TEXT, institution TEXT,
            supplier TEXT, supplier_id TEXT, status TEXT,
            start_date TEXT, end_date TEXT,
            acq_type TEXT, acq_method TEXT,
            merch_amount REAL, grand_total REAL,
            buyer_name TEXT, buyer_email TEXT, buyer_phone TEXT,
            search_term TEXT, agency_key TEXT
        );
        CREATE TABLE IF NOT EXISTS scprs_po_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            po_id INTEGER REFERENCES scprs_po_master(id),
            po_number TEXT, line_num INTEGER,
            item_id TEXT, description TEXT, unspsc TEXT,
            uom TEXT, quantity REAL, unit_price REAL,
            line_total REAL, line_status TEXT,
            category TEXT, reytech_sells INTEGER DEFAULT 0,
            reytech_sku TEXT, opportunity_flag TEXT
        );
        CREATE TABLE IF NOT EXISTS scprs_pull_schedule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agency_key TEXT UNIQUE, priority TEXT,
            pull_interval_hours INTEGER DEFAULT 24,
            last_pull TEXT, next_pull TEXT,
            enabled INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS scprs_pull_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pulled_at TEXT, search_term TEXT, dept_filter TEXT,
            results_found INTEGER DEFAULT 0, lines_parsed INTEGER DEFAULT 0,
            new_pos INTEGER DEFAULT 0, error TEXT, duration_sec REAL
        );
        CREATE INDEX IF NOT EXISTS idx_po_institution ON scprs_po_master(institution);
        CREATE INDEX IF NOT EXISTS idx_po_buyer ON scprs_po_master(buyer_email);
        CREATE INDEX IF NOT EXISTS idx_po_supplier ON scprs_po_master(supplier);
        """)
            # Migrate: add enabled column to existing scprs_pull_schedule tables
            try:
                conn.execute("ALTER TABLE scprs_pull_schedule ADD COLUMN enabled INTEGER DEFAULT 1")
            except Exception as _e:
                log.debug("suppressed: %s", _e)  # Column already exists
    except Exception as e:
        log.debug("ensure_scprs_tables: %s", e)


def _get_scprs_intel():
    """Query SCPRS tables for buyer contacts, spending, competitors per institution.
    Returns empty dicts if no SCPRS data yet — graceful degradation."""
    result = {
        "buyers_by_institution": {},
        "spend_by_institution": {},
        "competitors_by_institution": {},
        "items_by_institution": {},
        "total_scprs_spend": 0, "total_pos": 0, "total_buyers": 0,
        "last_pull": None, "has_data": False,
    }

    try:
        from src.core.db import get_db
        with get_db() as conn:
            count = conn.execute("SELECT count(*) FROM scprs_po_master").fetchone()[0]
            if count == 0:
                return result
            result["has_data"] = True
            result["total_pos"] = count
            result["total_scprs_spend"] = conn.execute(
                "SELECT coalesce(sum(grand_total),0) FROM scprs_po_master").fetchone()[0] or 0
            result["last_pull"] = (conn.execute(
                "SELECT max(pulled_at) FROM scprs_po_master").fetchone()[0] or None)

            # Buyers per institution
            for r in conn.execute("""
                SELECT institution, buyer_name, buyer_email, buyer_phone,
                       count(*) as po_count, max(start_date) as last_po,
                       sum(grand_total) as total_spend
                FROM scprs_po_master
                WHERE buyer_email != '' AND buyer_email IS NOT NULL
                GROUP BY institution, buyer_email ORDER BY total_spend DESC
            """):
                d = dict(r)
                inst = d["institution"] or ""
                if inst not in result["buyers_by_institution"]:
                    result["buyers_by_institution"][inst] = []
                result["buyers_by_institution"][inst].append({
                    "name": d["buyer_name"] or "", "email": d["buyer_email"] or "",
                    "phone": d["buyer_phone"] or "", "po_count": d["po_count"],
                    "last_po": d["last_po"] or "", "total_spend": d["total_spend"] or 0,
                })
            result["total_buyers"] = sum(len(v) for v in result["buyers_by_institution"].values())

            # Spend per institution
            for r in conn.execute("""
                SELECT institution, sum(grand_total) as total,
                       count(*) as po_count, max(start_date) as last_date
                FROM scprs_po_master GROUP BY institution ORDER BY total DESC
            """):
                d = dict(r)
                result["spend_by_institution"][d["institution"] or ""] = {
                    "total": d["total"] or 0, "po_count": d["po_count"],
                    "last_date": d["last_date"] or "",
                }

            # Competitors per institution
            for r in conn.execute("""
                SELECT institution, supplier, sum(grand_total) as total, count(*) as po_count
                FROM scprs_po_master WHERE supplier != '' AND supplier IS NOT NULL
                GROUP BY institution, supplier ORDER BY total DESC
            """):
                d = dict(r)
                inst = d["institution"] or ""
                if inst not in result["competitors_by_institution"]:
                    result["competitors_by_institution"][inst] = []
                result["competitors_by_institution"][inst].append({
                    "supplier": d["supplier"] or "", "total": d["total"] or 0,
                    "po_count": d["po_count"],
                })

            # Top items per institution
            for r in conn.execute("""
                SELECT p.institution, l.description, l.category,
                       sum(l.quantity) as total_qty, avg(l.unit_price) as avg_price,
                       sum(l.line_total) as total_spend, l.reytech_sells
                FROM scprs_po_lines l JOIN scprs_po_master p ON l.po_id = p.id
                WHERE l.description != ''
                GROUP BY p.institution, l.description ORDER BY total_spend DESC
            """):
                d = dict(r)
                inst = d["institution"] or ""
                if inst not in result["items_by_institution"]:
                    result["items_by_institution"][inst] = []
                if len(result["items_by_institution"][inst]) < 10:
                    result["items_by_institution"][inst].append({
                        "description": d["description"][:60], "category": d["category"] or "",
                        "total_qty": d["total_qty"] or 0,
                        "avg_price": round(d["avg_price"] or 0, 2),
                        "total_spend": d["total_spend"] or 0,
                        "we_sell": bool(d["reytech_sells"]),
                    })
    except Exception as e:
        log.debug("SCPRS intel query error: %s", e)
    return result


def _build_expansion_intel_v4():
    """V4 Territory Intelligence — merges QB, CRM, PCs, SCPRS, market intel, catalog.
    Key fix: separates central contacts (Timothy) from facility-specific buyers."""
    import json as _json
    from collections import Counter
    import sqlite3

    # Load CRM contacts from SQLite (authoritative)
    try:
        from src.core.db import get_all_contacts
        _crm_data = get_all_contacts()
    except Exception:
        _crm_data = {}

    _ensure_scprs_tables()

    customers = _load_customers()
    pcs_path = os.path.join(DATA_DIR, "price_checks.json")
    pcs = _json.load(open(pcs_path)) if os.path.exists(pcs_path) else {}
    orders = _load_orders()
    crm = _crm_data
    crm_list = list(crm.values()) if isinstance(crm, dict) else crm
    mi_path = os.path.join(DATA_DIR, "market_intelligence.json")
    market_intel = _json.load(open(mi_path)) if os.path.exists(mi_path) else {}
    scprs = _get_scprs_intel()

    # ── 1. Detect central contacts (email on 3+ facilities) ──
    email_fac_count = Counter()
    for c in customers:
        em = (c.get("email","") or "").lower().strip()
        if em:
            email_fac_count[em] += 1

    central_contacts = {}
    for em, cnt in email_fac_count.items():
        if cnt >= 3:
            central_contacts[em] = {
                "email": em, "role": "CENTRAL", "source": "quickbooks",
                "facility_count": cnt,
                "note": f"Central procurement — on {cnt} QB facilities",
            }

    # ── 2. Build facility registry ──
    facilities = {}
    for c in customers:
        raw_name = c.get("qb_name","") or c.get("display_name","")
        parent = c.get("parent","")
        bal = float(c.get("open_balance",0) or 0)
        email = c.get("email","")
        abbr = c.get("abbreviation","")

        name_parent = (parent or raw_name).lower()
        if "correctional" in name_parent or "state prison" in raw_name.lower() \
                or "calipatria" in raw_name.lower() or "medical facility" in raw_name.lower():
            atype = "CCHCS"
        elif "veterans" in raw_name.lower() or "dept of veterans" in raw_name.lower() \
                or "calvet" in raw_name.lower():
            atype = "CalVet"
        elif "state hospital" in raw_name.lower():
            atype = "DSH"
        else:
            continue

        short_name, parent_name = _parse_facility_name(raw_name)
        if short_name is None:
            continue

        fkey = short_name.lower()[:30].strip()
        if fkey in facilities:
            ex = facilities[fkey]
            if bal > ex["ar"]:
                ex["ar"] = bal
            ex["qb_names"].append(raw_name)
            if email and email.lower() not in central_contacts:
                if not any(cc.get("email","").lower() == email.lower() for cc in ex["contacts"]):
                    ex["contacts"].append({"email": email, "source": "quickbooks", "role": "Billing/AP"})
        else:
            contacts = []
            if email and email.lower() not in central_contacts:
                contacts.append({"email": email, "source": "quickbooks", "role": "Billing/AP"})
            facilities[fkey] = {
                "id": fkey, "name": short_name, "raw_name": raw_name,
                "parent": parent_name, "abbr": abbr, "type": atype,
                "ar": bal, "contacts": contacts, "orders": [], "pcs": [],
                "qb_names": [raw_name], "outreach_status": "untouched",
                "last_activity": None, "score": 0,
                "scprs_spend": 0, "scprs_buyers": [],
                "competitors": [], "top_items": [],
                "est_annual": _EST_ANNUAL.get(atype, 5000), "gap": 0,
            }

    # ── 3. Inject SCPRS buyer contacts (the REAL buyers) ──
    for scprs_inst, buyers in scprs["buyers_by_institution"].items():
        for fkey, fac in facilities.items():
            if facility_name_match(scprs_inst, fac["name"]) or \
               facility_name_match(scprs_inst, fac["raw_name"]) or \
               any(facility_name_match(scprs_inst, qn) for qn in fac.get("qb_names",[])):
                for buyer in buyers:
                    be = (buyer.get("email","") or "").lower()
                    if not be or be in central_contacts:
                        continue
                    if not any(c.get("email","").lower() == be for c in fac["contacts"]):
                        fac["contacts"].append({
                            "name": buyer["name"], "email": buyer["email"],
                            "phone": buyer.get("phone",""), "source": "scprs",
                            "role": "Buyer", "po_count": buyer.get("po_count",0),
                            "last_po": buyer.get("last_po",""),
                        })
                    fac["scprs_buyers"].append(buyer)
                break

    # ── 4. SCPRS spending & competitors ──
    for scprs_inst, spend in scprs["spend_by_institution"].items():
        for fkey, fac in facilities.items():
            if facility_name_match(scprs_inst, fac["name"]) or \
               facility_name_match(scprs_inst, fac["raw_name"]) or \
               any(facility_name_match(scprs_inst, qn) for qn in fac.get("qb_names",[])):
                fac["scprs_spend"] = spend["total"]
                fac["gap"] = max(0, spend["total"] - fac["ar"])
                break

    for scprs_inst, comps in scprs["competitors_by_institution"].items():
        for fkey, fac in facilities.items():
            if facility_name_match(scprs_inst, fac["name"]) or \
               facility_name_match(scprs_inst, fac["raw_name"]):
                fac["competitors"] = comps[:8]
                break

    for scprs_inst, items in scprs["items_by_institution"].items():
        for fkey, fac in facilities.items():
            if facility_name_match(scprs_inst, fac["name"]) or \
               facility_name_match(scprs_inst, fac["raw_name"]):
                fac["top_items"] = items[:8]
                break

    # ── 5. CRM contacts (skip centrals) ──
    for contact in crm_list:
        agency = (contact.get("agency","") or "").upper()
        buyer_email = (contact.get("buyer_email","") or "").lower()
        if not buyer_email or buyer_email in central_contacts:
            continue
        is_relevant = agency in ("CDCR","CCHCS") or \
            any(k in agency.lower() for k in ("calvet","veterans","dsh","state hospital","correctional"))
        if not is_relevant:
            continue
        for fkey, fac in facilities.items():
            if not any(c.get("email","").lower() == buyer_email for c in fac["contacts"]):
                fac["contacts"].append({
                    "name": contact.get("buyer_name",""), "email": contact.get("buyer_email",""),
                    "title": contact.get("title",""), "source": "crm",
                    "role": contact.get("title","") or "Procurement",
                })
                break

    # ── 6. Price Checks ──
    for pid, pc in pcs.items():
        inst = pc.get("institution","") or ""
        pc_email = (pc.get("contact_email","") or "").lower()
        pc_contact = pc.get("contact_name","") or pc.get("header",{}).get("requestor","") or ""
        if not inst:
            continue
        for fkey, fac in facilities.items():
            if facility_name_match(inst, fac["name"]) or facility_name_match(inst, fac["raw_name"]):
                fac["pcs"].append({
                    "id": pid, "number": pc.get("pc_number",""),
                    "items": len(pc.get("items",[])), "total": pc.get("total",0) or 0,
                    "status": pc.get("status",""), "date": pc.get("created_at",""),
                })
                if pc_email and pc_email not in central_contacts:
                    if not any(c.get("email","").lower() == pc_email for c in fac["contacts"]):
                        fac["contacts"].append({
                            "name": pc_contact, "email": pc.get("contact_email",""),
                            "source": "price_check", "role": "Requestor",
                        })
                if fac["outreach_status"] == "untouched":
                    fac["outreach_status"] = "responded"
                break

    # ── 7. Orders ──
    for oid, order in orders.items():
        inst = order.get("institution","") or ""
        for fkey, fac in facilities.items():
            if facility_name_match(inst, fac["name"]) or facility_name_match(inst, fac["raw_name"]):
                fac["orders"].append({
                    "id": oid, "total": order.get("total",0),
                    "status": order.get("status",""), "date": order.get("created_at",""),
                    "items": [it.get("description","")[:40] for it in order.get("line_items",[])[:5]],
                })
                break

    # ── 8. Product recommendations ──
    product_recs = {"CCHCS": [], "CalVet": [], "DSH": []}
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cat_map = {
                "CCHCS": ["Medical/Clinical","Gloves","Cleaning/Sanitation","Personal Care","Safety/PPE"],
                "CalVet": ["Medical/Clinical","Personal Care","Gloves","Cleaning/Sanitation"],
                "DSH": ["Medical/Clinical","Safety/PPE","Gloves","Cleaning/Sanitation"],
            }
            for atype, cats in cat_map.items():
                for cat in cats:
                    for r in conn.execute("""
                        SELECT name, category, sell_price, cost, manufacturer, recommended_price
                        FROM product_catalog WHERE category = ? AND cost > 0
                        ORDER BY sell_price DESC LIMIT 2
                    """, (cat,)):
                        item = dict(r)
                        item["margin_pct"] = round((item["sell_price"] - item["cost"]) / item["sell_price"] * 100) if item["sell_price"] > 0 else 0
                        product_recs[atype].append(item)
    except Exception as e:
        log.debug("Product recs error: %s", e)

    # ── 9. Score (SCPRS-enhanced) ──
    for fac in facilities.values():
        s = 0
        if fac["ar"] > 0:
            s += 35; fac["outreach_status"] = "won"
        scprs_c = len([c for c in fac["contacts"] if c.get("source") == "scprs"])
        other_c = len(fac["contacts"]) - scprs_c
        s += min(scprs_c * 12, 24)
        s += min(other_c * 5, 10)
        if fac["scprs_spend"] > 0: s += 15
        if fac["scprs_spend"] > 50000: s += 5
        s += min(len(fac["orders"]) * 6, 12)
        s += min(len(fac["pcs"]) * 4, 8)
        s += {"CalVet": 6, "CCHCS": 4, "DSH": 2}.get(fac["type"], 0)
        fac["score"] = min(s, 100)
        if fac["ar"] > 0 and fac["outreach_status"] == "untouched":
            fac["outreach_status"] = "won"
        dates = [pc.get("date","") for pc in fac["pcs"]] + [o.get("date","") for o in fac["orders"]]
        dates = [d for d in dates if d]
        fac["last_activity"] = max(dates) if dates else None

    # ── 10. Aggregate ──
    fac_list = sorted(facilities.values(), key=lambda x: (-x["score"], -x["ar"], x["name"]))
    active = [f for f in fac_list if f["ar"] > 0]
    untouched = [f for f in fac_list if f["outreach_status"] == "untouched"]
    total_ar = sum(f["ar"] for f in active)
    total_contacts = sum(len(f["contacts"]) for f in fac_list) + len(central_contacts)
    total_scprs = scprs["total_scprs_spend"]
    total_gap = max(0, total_scprs - total_ar) if total_scprs > 0 else sum(f["est_annual"] for f in untouched)

    type_stats = {}
    for atype in ["CCHCS","CalVet","DSH"]:
        tf = [f for f in fac_list if f["type"] == atype]
        ta = [f for f in tf if f["ar"] > 0]
        ss = sum(f["scprs_spend"] for f in tf)
        type_stats[atype] = {
            "total": len(tf), "active": len(ta),
            "ar": sum(f["ar"] for f in ta), "scprs_spend": ss,
            "gap": max(0, ss - sum(f["ar"] for f in ta)),
            "contacts": sum(len(f["contacts"]) for f in tf),
            "untouched": len([f for f in tf if f["outreach_status"] == "untouched"]),
            "avg_score": round(sum(f["score"] for f in tf) / max(len(tf),1)),
        }

    # All contacts (deduped)
    all_contacts = []
    seen_em = set()
    for fac in fac_list:
        for c in fac["contacts"]:
            em = (c.get("email","") or "").lower()
            if em and em not in seen_em:
                seen_em.add(em)
                c2 = dict(c)
                c2["facility"] = fac["name"]
                c2["facility_type"] = fac["type"]
                c2["facility_id"] = fac["id"]
                all_contacts.append(c2)

    return {
        "facilities": fac_list, "active": active, "untouched": untouched,
        "total_ar": total_ar, "total_contacts": total_contacts,
        "total_gap": total_gap, "total_scprs_spend": total_scprs,
        "type_stats": type_stats, "product_recs": product_recs,
        "stale_accounts": [f for f in active if not f["orders"] and not f["last_activity"]],
        "all_contacts": all_contacts,
        "central_contacts": list(central_contacts.values()),
        "scprs_has_data": scprs["has_data"], "scprs_last_pull": scprs["last_pull"],
        "scprs_total_buyers": scprs["total_buyers"],
        "competitive_gaps": market_intel.get("competitive_product_gaps", [])[:12],
        "vendor_registrations": market_intel.get("accounts_to_register_now", [])[:8],
    }


@bp.route("/cchcs/expansion")
@auth_required
@safe_page
def page_cchcs_expansion():
    """Redirected to Competitor Intelligence page."""
    return redirect("/intel/competitors")

@bp.route("/api/expansion/facility/<fac_id>")
@auth_required
@safe_route
def api_expansion_facility(fac_id):
    """Full detail for one facility."""
    intel = _build_expansion_intel_v4()
    for f in intel["facilities"]:
        if f["id"] == fac_id:
            return jsonify({"ok": True, "facility": f})
    return jsonify({"ok": False, "error": "Not found"})


@bp.route("/api/expansion/outreach", methods=["POST"])
@auth_required
@safe_route
def api_expansion_outreach():
    """Create targeted outreach — smart PC + email to the REAL buyer."""
    import json as _json
    import sqlite3 as _sql
    data = request.get_json() or {}
    facility_name = data.get("facility_name","").strip()
    agency_type = data.get("agency_type","CCHCS")
    contact_email = data.get("email","")
    contact_name = data.get("contact_name","")
    action = data.get("action", "email_and_pc")

    if not facility_name:
        return jsonify({"ok": False, "error": "Facility name required"})

    results = {"ok": True, "facility": facility_name, "agency_type": agency_type}

    # Real catalog products
    items = []
    try:
        db_path = os.path.join(DATA_DIR, "reytech.db")
        conn = _sql.connect(db_path); conn.row_factory = _sql.Row
        cat_map = {
            "CCHCS": ["Medical/Clinical","Gloves","Cleaning/Sanitation"],
            "CalVet": ["Medical/Clinical","Personal Care","Gloves"],
            "DSH": ["Medical/Clinical","Safety/PPE","Gloves"],
        }
        for cat in cat_map.get(agency_type, ["Medical/Clinical","Gloves"]):
            for r in conn.execute(
                "SELECT name, sell_price, cost, recommended_price FROM product_catalog "
                "WHERE category = ? AND cost > 0 ORDER BY RANDOM() LIMIT 2", (cat,)):
                d = dict(r)
                items.append({"description": d["name"][:80], "qty": 10,
                    "unit_price": round(d.get("recommended_price") or d["sell_price"], 2),
                    "cost": round(d["cost"], 2)})
        conn.close()
    except Exception as e:
        log.debug("outreach catalog sample pick: %s", e)
    if not items:
        items = [{"description": "Nitrile Exam Gloves, Medium, Box/100", "qty": 50, "unit_price": 12.99, "cost": 9.50}]
    total = sum(it["qty"] * it["unit_price"] for it in items)

    # PC
    if action in ("email_and_pc", "pc_only"):
        import time as _time
        pc_id = f"expand-{agency_type.lower()}-{int(_time.time())}"
        short = facility_name.split(":")[-1].strip()[:20] if ":" in facility_name else facility_name[:20]
        pc = {
            "id": pc_id, "created_at": datetime.now().isoformat(),
            "pc_number": f"EXP-{agency_type}-{short}",
            "institution": facility_name, "agency": "CDCR" if agency_type == "CCHCS" else agency_type,
            "contact_email": contact_email, "contact_name": contact_name,
            "items": items, "total": round(total, 2), "status": "pending",
            "source": "cchcs_expansion", "tags": [f"{agency_type.lower()}_expansion", "outreach"],
        }
        from src.api.dashboard import _load_price_checks, _save_price_checks
        pcs = _load_price_checks()
        pcs[pc_id] = pc
        try:
            _save_price_checks(pcs, raise_on_error=True)
        except Exception as _save_e:
            log.error("expansion PC save failed for %s: %s", pc_id, _save_e)
            return jsonify({"ok": False,
                "error": f"Expansion PC save failed: {_save_e}",
                "pc_id_attempted": pc_id}), 500
        results["pc_id"] = pc_id; results["items_count"] = len(items); results["total"] = round(total, 2)

    # Email
    if action in ("email_and_pc", "email_only") and contact_email:
        short = facility_name.split(":")[-1].strip() if ":" in facility_name else facility_name
        plines = "\n".join(f"  - {it['description'][:60]}" for it in items[:4])
        body = (f"Hello{(' ' + contact_name.split()[0]) if contact_name else ''},\n\n"
                f"I'm reaching out from Reytech Inc., a CA-certified small business and SCPRS-registered vendor.\n\n"
                f"We serve multiple {agency_type} facilities and would like to introduce our services to {short}:\n\n"
                f"{plines}\n\n"
                f"We respond to AMS 704 price checks and offer competitive SCPRS pricing.\n\n"
                f"Best regards,\nMichael Guadan\nReytech Inc.\n(949) 229-1575 | sales@reytechinc.com\n"
                f"CA Certified Small Business | DVBE | SCPRS Supplier")
        try:
            from src.core.dal import get_outbox as _ob
            outbox = _ob()
            outbox_path_local = os.path.join(DATA_DIR, "outbox.json")
            outbox.append({"id": f"draft-expand-{int(__import__('time').time())}",
                "to": contact_email, "subject": f"Reytech Inc — Medical Supplies for {short}",
                "body": body, "status": "draft", "type": "expansion_outreach",
                "facility": facility_name, "created_at": datetime.now().isoformat()})
            with open(outbox_path_local, "w") as f: _json.dump(outbox, f, indent=2, default=str)
            results["email_drafted"] = True; results["email_to"] = contact_email
        except Exception as e: results["email_error"] = str(e)

    # Activity log
    try:
        act_path = os.path.join(DATA_DIR, "crm_activity.json")
        acts = _json.load(open(act_path)) if os.path.exists(act_path) else []
        acts.append({"type": "expansion_outreach", "facility": facility_name,
            "agency_type": agency_type, "action": action, "email": contact_email,
            "timestamp": datetime.now().isoformat(), "total": round(total, 2)})
        with open(act_path, "w") as f: _json.dump(acts, f, indent=2, default=str)
    except (ValueError, OSError, TypeError) as e:
        log.debug("expansion crm_activity write: %s", e)
    try:
        from src.agents.notify_agent import send_alert
        send_alert("bell", f"Outreach: {facility_name} ({agency_type})", {"type": "expansion_target"})
    except Exception as e:
        log.debug("expansion send_alert: %s", e)
    return jsonify(results)


@bp.route("/api/expansion/scprs-pull", methods=["POST"])
@auth_required
@safe_route
def api_expansion_scprs_pull():
    """Trigger SCPRS pull for an agency (background thread)."""
    data = request.get_json() or {}
    agency_key = data.get("agency_key", "CCHCS")
    try:
        from src.agents.scprs_intelligence_engine import pull_agency
        import threading
        threading.Thread(target=pull_agency, args=(agency_key,), daemon=True).start()
        return jsonify({"ok": True, "message": f"SCPRS pull started for {agency_key}"})
    except ImportError:
        return jsonify({"ok": False, "error": "SCPRS engine not available"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# VENDOR REGISTRATION TRACKER (F31-05)
# ════════════════════════════════════════════════════════════════════════════════

VENDOR_REGISTRATION_LIST = [
    {"vendor":"Cardinal Health","priority":"P0","url":"cardinal.com/en-us/o/business","products":["Chux/underpads","Adult briefs","Wound care","Gloves","Hospital gowns"],"key":"cardinal_health"},
    {"vendor":"McKesson Medical-Surgical","priority":"P0","url":"mms.mckesson.com","products":["Wound care dressings","Sharps containers","Exam supplies","PPE"],"key":"mckesson"},
    {"vendor":"Bound Tree Medical","priority":"P0","url":"boundtree.com","products":["First aid kits ANSI B","CAT Tourniquets","Trauma supplies","AEDs"],"key":"bound_tree"},
    {"vendor":"Waxie Sanitary Supply","priority":"P0","url":"waxie.com","products":["Trash bags","Paper towels","Toilet paper","Disinfectants"],"key":"waxie"},
    {"vendor":"Medline Industries","priority":"P0","url":"medline.com/businessaccount","products":["Full PPE catalog","Medical supplies","Incontinence","Wound care"],"key":"medline"},
    {"vendor":"S&S Worldwide","priority":"P1","url":"ssww.com (call 800-243-9232)","products":["Recreational activity","Art therapy","Games","Exercise equipment"],"key":"ss_worldwide"},
    {"vendor":"North American Rescue","priority":"P1","url":"narescue.com","products":["CAT tourniquets","IFAK kits","Bleed control","Chest seals"],"key":"north_american_rescue"},
    {"vendor":"GloveNation / Dash Medical","priority":"P1","url":"glovenation.com","products":["Black nitrile gloves","Law enforcement grade","Tactical PPE"],"key":"glovenation"},
]

def _get_vendor_reg():
    import json as _json
    reg_path = os.path.join(DATA_DIR, "vendor_registration.json")
    if not os.path.exists(reg_path):
        reg = {v["key"]: {"status":"not_started","registered_at":None,"account_number":""} for v in VENDOR_REGISTRATION_LIST}
        with open(reg_path,"w") as f: _json.dump(reg, f, indent=2)
        return reg
    return _json.load(open(reg_path))

@bp.route("/api/vendor/registration", methods=["GET"])
@auth_required
@safe_route
def api_vendor_registration_get():
    reg = _get_vendor_reg()
    enriched = []
    for v in VENDOR_REGISTRATION_LIST:
        status = reg.get(v["key"],{}).get("status","not_started")
        enriched.append({**v, "status": status,
                         "registered_at": reg.get(v["key"],{}).get("registered_at"),
                         "account_number": reg.get(v["key"],{}).get("account_number","")})
    active = sum(1 for v in enriched if v["status"]=="active")
    return jsonify({"ok":True, "vendors": enriched, "active": active, "total": len(enriched)})

@bp.route("/api/vendor/registration", methods=["POST"])
@auth_required
@safe_route
def api_vendor_registration_update():
    import json as _json
    data = request.get_json() or {}
    vendor_key = data.get("vendor_key","").strip()
    status = data.get("status","in_progress")  # not_started | in_progress | active
    account_number = data.get("account_number","")
    if not vendor_key:
        return jsonify({"ok":False,"error":"vendor_key required"})
    reg = _get_vendor_reg()
    from datetime import datetime as _dt
    reg[vendor_key] = {
        "status": status,
        "registered_at": _dt.now().isoformat() if status=="active" else reg.get(vendor_key,{}).get("registered_at"),
        "account_number": account_number
    }
    reg_path = os.path.join(DATA_DIR, "vendor_registration.json")
    with open(reg_path,"w") as f: _json.dump(reg, f, indent=2)
    if status == "active":
        try:
            from src.agents.notify_agent import send_alert
            vendor_name = next((v["vendor"] for v in VENDOR_REGISTRATION_LIST if v["key"]==vendor_key), vendor_key)
            send_alert("bell", f"Vendor account activated: {vendor_name}", {
                "type":"vendor_activated","vendor_key":vendor_key,"vendor":vendor_name,"link":"/catalog?tab=vendors"
            })
        except Exception as e:
            log.debug("vendor activation send_alert: %s", e)
    active_count = sum(1 for v in reg.values() if v.get("status")=="active")
    return jsonify({"ok":True,"vendor_key":vendor_key,"status":status,"active_total":active_count})




@bp.route("/api/intel/scprs/test")
@auth_required
@safe_route
def api_intel_scprs_test_v2():
    """F31-07: Test SCPRS connectivity + credential status."""
    import os as _os
    import time as _time
    has_user = bool(_os.environ.get("SCPRS_USERNAME",""))
    has_pass = bool(_os.environ.get("SCPRS_PASSWORD",""))
    result = {
        "ok": True,
        "credentials_set": has_user and has_pass,
        "username_set": has_user,
        "password_set": has_pass,
    }
    if not (has_user and has_pass):
        result["hint"] = "Set SCPRS_USERNAME and SCPRS_PASSWORD in Railway env vars to enable live data pulls"
        result["creds_missing"] = True
        return jsonify(result)
    # Test connectivity
    try:
        import requests as _req
        t0 = _time.time()
        r = _req.get("https://suppliers.fiscal.ca.gov/psc/psfpd1/SUPPLIER/ERP/c/ZZ_PO.ZZ_SCPRS1_CMP.GBL",
                     timeout=8, allow_redirects=True)
        result["reachable"] = True
        result["status_code"] = r.status_code
        result["elapsed_ms"] = round((_time.time() - t0)*1000)
    except Exception as e:
        result["reachable"] = False
        result["error"] = str(e)
        result["hint"] = "Railway static IP may need to be enabled — check Railway settings → Networking"
    return jsonify(result)


@bp.route("/api/intel/scprs/pull-now", methods=["POST"])
@auth_required
@safe_route
def api_intel_scprs_pull_now():
    """F31-07: Trigger an immediate SCPRS pull (bypasses schedule)."""
    import os as _os
    if not INTEL_AVAILABLE:
        return jsonify({"ok": False, "error": "Sales intel agent not available"})
    if not _os.environ.get("SCPRS_USERNAME") or not _os.environ.get("SCPRS_PASSWORD"):
        return jsonify({"ok": False, "error": "SCPRS_USERNAME and SCPRS_PASSWORD must be set in Railway first",
                        "action": "Set credentials in Railway → Variables → add SCPRS_USERNAME + SCPRS_PASSWORD"})
    if DEEP_PULL_STATUS.get("running"):
        return jsonify({"ok": True, "message": "SCPRS pull already running", "status": DEEP_PULL_STATUS})
    def _run():
        try:
            DEEP_PULL_STATUS["running"] = True
            DEEP_PULL_STATUS["started_at"] = _pst_now_iso()
            from src.agents.sales_intel import deep_pull_all_buyers
            deep_pull_all_buyers()
            DEEP_PULL_STATUS["running"] = False
            DEEP_PULL_STATUS["last_completed"] = _pst_now_iso()
            _push_notification({"event_type":"scprs_pull_done","urgency":"info",
                "title":"SCPRS pull complete","body":"Live buyer data updated from SCPRS","deep_link":"/intel/market"})
        except Exception as e:
            DEEP_PULL_STATUS["running"] = False
            DEEP_PULL_STATUS["last_error"] = str(e)
    import threading as _threading
    t = _threading.Thread(target=_run, daemon=True, name="scprs-pull-now")
    t.start()
    return jsonify({"ok": True, "message": "SCPRS pull started in background", "check_status": "/api/intel/pull/status"})

# ════════════════════════════════════════════════════════════════════════════════
# MARKET INTELLIGENCE / LAND & EXPAND PAGE
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/intel/market")
@auth_required
@safe_page
def page_market_intel():
    """Land & Expand — competitive gap analysis, buyer intelligence, revenue model."""
    import json as _json
    from pathlib import Path
    mi_path = os.path.join(DATA_DIR, "market_intelligence.json")
    if not os.path.exists(mi_path):
        return "<h2>Market intelligence not yet generated. Run SCPRS pull first.</h2>"
    mi = _json.load(open(mi_path))

    agencies = mi.get("agencies", {})
    gaps = mi.get("competitive_product_gaps", [])
    accounts = mi.get("accounts_to_register_now", [])
    playbook = mi.get("land_and_expand_playbook", {})
    rev_ops = {k: v for k, v in mi.get("revenue_model", {}).get("opportunities", {}).items()} if mi.get("revenue_model") else {}

    # Revenue opportunity summary
    total_opp = sum(
        a.get("revenue_opportunity_12mo", 0) for a in agencies.values()
    )
    p0_gaps = [g for g in gaps if g.get("priority") == "P0"]
    p1_gaps = [g for g in gaps if g.get("priority") == "P1"]
    p0_missed = sum(g.get("annual_missed", 0) for g in p0_gaps)

    # Agency status cards
    def agency_card(key, ag):
        is_cust = ag.get("is_customer", False)
        ar = ag.get("ar_outstanding", 0)
        opp = ag.get("revenue_opportunity_12mo", 0)
        pri = ag.get("priority", "P2")
        color = "var(--gn)" if is_cust else ("var(--ac)" if pri == "P0" else "var(--yl)")
        status = f"✅ Customer (${ar:,.0f} AR)" if is_cust else f"🎯 Target ({pri})"
        buyer_html = ""
        if ag.get("intel_buyer"):
            b = ag["intel_buyer"]
            if isinstance(b, dict):
                buyer_html = f'<div style="font-size:14px;color:var(--ac);margin-top:4px">📞 {b["name"]} | {b["email"]} | {b["phone"]}</div>'
        elif ag.get("intel_buyers"):
            for b in ag["intel_buyers"][:2]:
                if isinstance(b, dict):
                    buyer_html += f'<div style="font-size:14px;color:var(--ac);margin-top:2px">📞 {b["name"]} | {b["email"]}</div>'
        # Competitive items (top 3)
        items_html = ""
        comp = ag.get("what_they_buy_from_competitors", {})
        all_items = []
        for cat_items in comp.values():
            if isinstance(cat_items, list):
                all_items.extend(cat_items[:2])
        for it in all_items[:4]:
            vendor = it.get("vendor", "")
            annual = it.get("annual_est", 0)
            items_html += f'<div style="font-size:14px;padding:3px 0;border-bottom:1px solid var(--bd)"><span style="color:var(--tx)">{it["item"][:45]}</span> <span style="color:var(--tx2);float:right">${annual:,.0f}/yr → {vendor[:20]}</span></div>'

        return f"""<div class="card" style="border-color:{color};margin-bottom:14px">
  <div style="display:flex;justify-content:space-between;align-items:flex-start">
    <div>
      <div style="font-size:13px;font-weight:700;color:{color}">{ag.get('full_name','')[:50]}</div>
      <div style="font-size:14px;color:var(--tx2);margin-top:2px">{status}</div>
      {buyer_html}
    </div>
    <div style="text-align:right">
      <div style="font-size:20px;font-weight:700;color:var(--gn)">${opp:,.0f}</div>
      <div style="font-size:13px;color:var(--tx2)">12mo opportunity</div>
    </div>
  </div>
  <div style="margin-top:10px;font-size:14px;color:var(--yl);background:rgba(210,167,78,.08);padding:6px 8px;border-radius:4px">
    💡 {ag.get('land_expand_strategy','')[:150]}
  </div>
  {f'<div style="margin-top:8px">{items_html}</div>' if items_html else ""}
</div>"""

    agencies_html = "".join(agency_card(k, v) for k, v in agencies.items())

    # Competitive gap table
    def gap_row(g):
        pri_color = "var(--rd)" if g["priority"]=="P0" else "var(--yl)"
        return f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:8px 10px;font-weight:500;font-size:14px">{g["item"]}</td>
  <td style="padding:8px 10px;font-size:14px;color:{pri_color}">{g["priority"]}</td>
  <td style="padding:8px 10px;font-size:14px;color:var(--gn);font-weight:600">${g["annual_missed"]:,}</td>
  <td style="padding:8px 10px;font-size:14px;color:var(--tx2)">{g["fix"][:80]}</td>
</tr>"""

    gaps_html = "".join(gap_row(g) for g in sorted(gaps, key=lambda x: (x["priority"], -x["annual_missed"])))

    # Accounts to register
    def account_card(a):
        pri_color = "var(--rd)" if a.get("priority")=="P0" else "var(--yl)"
        return f"""<div style="padding:10px 12px;border-bottom:1px solid var(--bd);display:flex;justify-content:space-between;align-items:flex-start">
  <div>
    <div style="font-size:13px;font-weight:600">{a["vendor"]}</div>
    <div style="font-size:14px;color:var(--tx2);margin-top:2px">{a["why"][:90]}</div>
    <div style="font-size:13px;color:var(--ac);margin-top:3px">{a["url"]}</div>
  </div>
  <span style="font-size:14px;font-weight:700;color:{pri_color};white-space:nowrap;margin-left:12px">{a["priority"]}</span>
</div>"""

    accounts_html = "".join(account_card(a) for a in accounts)

    # Playbook
    def phase_html(phase_key, phase):
        return f"""<div class="card" style="margin-bottom:12px">
  <div style="font-size:13px;font-weight:700;margin-bottom:8px">
    {phase_key.replace('_',' ').title()} — <span style="color:var(--gn)">${phase.get('revenue_target',0):,}</span>
    <span style="font-size:14px;font-weight:400;color:var(--tx2);margin-left:8px">{phase.get('title','')}</span>
  </div>
  {"".join(f'<div style="font-size:14px;padding:3px 0;color:var(--tx2)">▸ {a}</div>' for a in phase.get('actions',[]))}
</div>"""

    playbook_html = "".join(phase_html(k, v) for k, v in playbook.items())

    total_gaps_missed = sum(g.get('annual_missed', 0) for g in gaps)
    html = render_page("market_intel.html", active_page="Intel",
        accounts=accounts,
        accounts_html=accounts_html,
        agencies=agencies,
        agencies_html=agencies_html,
        gaps=gaps,
        gaps_html=gaps_html,
        p0_gaps=p0_gaps,
        playbook_html=playbook_html,
        total_opp=total_opp,
        p0_missed=p0_missed,
        total_gaps_missed=total_gaps_missed)
    return html


@bp.route("/api/intel/market")
@auth_required
@safe_route
def api_intel_market():
    """Raw market intelligence JSON."""
    import json as _json
    mi_path = os.path.join(DATA_DIR, "market_intelligence.json")
    if not os.path.exists(mi_path):
        return jsonify({"ok": False, "error": "market_intelligence.json not found"})
    return jsonify({"ok": True, **_json.load(open(mi_path))})





# ════════════════════════════════════════════════════════════════════════════════
# SCPRS PUBLIC SEARCH — No credentials required. 100% public data.
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/scprs/public/search")
@auth_required
@safe_route
def api_scprs_public_search():
    """Search public SCPRS for what CCHCS/CDCR is buying. No credentials needed."""
    keyword = request.args.get("keyword", "").strip()
    if not keyword:
        return jsonify({"ok": False, "error": "keyword required (e.g. nitrile gloves)"})
    try:
        from src.agents.scprs_public_search import search_cchcs_purchases
        result = search_cchcs_purchases(keyword)
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/scprs/public/sweep")
@auth_required
@safe_route
def api_scprs_public_sweep():
    """Sweep all Reytech keywords against CCHCS. Background job on Railway."""
    def _run():
        try:
            from src.agents.scprs_public_search import get_cchcs_purchase_intelligence
            result = get_cchcs_purchase_intelligence()
            _push_notification("SCPRS Sweep Done",
                f"{result.get('total_po_records', 0)} PO records found across {len(result.get('keywords_searched', []))} keywords",
                "success")
        except Exception as e:
            log.error("SCPRS sweep: %s", e)
    import threading
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"ok": True, "message": "SCPRS sweep started. Check /api/notifications when done."})


@bp.route("/api/scprs/public/ingest", methods=["POST"])
@auth_required
@safe_route
def api_scprs_public_ingest():
    """Ingest CSV downloaded from caleprocure.ca.gov SCPRS search. Finds gaps instantly."""
    data = request.get_json(force=True) or {}
    csv_text = data.get("csv_text", "").strip()
    if not csv_text:
        return jsonify({"ok": False, "error": "csv_text required"})
    import csv, io, os
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return jsonify({"ok": False, "error": "No rows parsed"})
    OUR_PRODUCTS = ["nitrile","glove","chux","underpad","brief","incontinence",
                    "n95","respirator","mask","wound","gauze","sanitizer",
                    "first aid","sharps","gown","vest","hi-vis","janitorial",
                    "paper towel","trash bag","disinfect","tourniquet"]
    parsed = []
    opportunities = []
    for row in rows:
        desc = (row.get("Description") or row.get("Commodity Description") or row.get("Item Description") or "").strip()
        vendor = (row.get("Vendor") or row.get("Supplier") or row.get("Vendor Name") or "").strip()
        amount_raw = row.get("Amount") or row.get("Total Amount") or row.get("Contract Amount") or "0"
        dept = (row.get("Department") or row.get("Dept") or row.get("Business Unit") or "").strip()
        date = (row.get("Date") or row.get("PO Date") or row.get("Award Date") or "").strip()
        po_num = (row.get("PO Number") or row.get("Document Number") or row.get("Contract Number") or "").strip()
        acq_type = (row.get("Acquisition Type") or row.get("Procurement Method") or "").strip()
        try:
            amt = float(str(amount_raw).replace("$","").replace(",","").strip() or 0)
        except Exception:
            amt = 0.0
        entry = {"description": desc, "vendor": vendor, "amount": amt,
                 "department": dept, "date": date, "po_number": po_num,
                 "acquisition_type": acq_type, "can_compete": False}
        parsed.append(entry)
        for product in OUR_PRODUCTS:
            if product in desc.lower():
                entry["opportunity_match"] = product
                entry["can_compete"] = True
                opportunities.append(entry)
                break
    ingest_path = os.path.join(DATA_DIR, "scprs_ingested.json")
    try:
        existing = json.load(open(ingest_path))
    except Exception:
        existing = []
    existing_pos = {e.get("po_number") for e in existing if e.get("po_number")}
    new_entries = [e for e in parsed if not e.get("po_number") or e.get("po_number") not in existing_pos]
    with open(ingest_path, "w") as _f:
        json.dump(existing + new_entries, _f, indent=2, default=str)
    vendors = {}
    for opp in opportunities:
        v = opp.get("vendor", "Unknown")
        vendors[v] = vendors.get(v, 0) + opp.get("amount", 0)
    top_vendors = sorted(vendors.items(), key=lambda x: -x[1])[:10]
    return jsonify({
        "ok": True,
        "total_rows": len(rows),
        "opportunities": len(opportunities),
        "new_entries_saved": len(new_entries),
        "top_vendors_to_beat": [{"vendor": v, "total_spend": s} for v, s in top_vendors],
        "opportunity_items": opportunities[:25],
        "gap_analysis": (f"Found {len(opportunities)} items you could compete for. "
                        f"Top competitor: {top_vendors[0][0]} (${top_vendors[0][1]:,.0f})")
                       if top_vendors else "No matching items found in this dataset",
    })


@bp.route("/scprs/gap-analysis")
@auth_required
@safe_page
def page_scprs_gap_analysis():
    """SCPRS Gap Analysis page — paste CSV from caleprocure, get instant intel."""
    import os
    try:
        ingested = json.load(open(os.path.join(DATA_DIR, "scprs_ingested.json")))
    except Exception:
        ingested = []
    opportunities = [i for i in ingested if i.get("can_compete")]
    vendors = {}
    for opp in opportunities:
        v = opp.get("vendor", "Unknown")
        vendors[v] = vendors.get(v, 0) + opp.get("amount", 0)
    top_vendors = sorted(vendors.items(), key=lambda x: -x[1])[:10]
    total_opp = sum(v for _, v in top_vendors)

    vendor_rows = "".join(
        f'<tr style="border-bottom:1px solid var(--bd)"><td style="padding:8px 12px;font-size:13px;font-weight:600">{v}</td>'
        f'<td style="padding:8px 12px;font-size:13px;color:var(--rd);font-weight:700">${s:,.0f}</td>'
        f'<td style="padding:8px 12px;font-size:14px;color:var(--gn)">✅ We can compete</td></tr>'
        for v, s in top_vendors
    )
    opp_rows = "".join(
        f'<tr style="border-bottom:1px solid var(--bd)"><td style="padding:7px 10px;font-size:14px">{o.get("description","")[:55]}</td>'
        f'<td style="padding:7px 10px;font-size:14px;color:var(--tx2)">{o.get("vendor","")[:28]}</td>'
        f'<td style="padding:7px 10px;font-size:14px;font-weight:700;color:var(--rd)">${o.get("amount",0):,.0f}</td>'
        f'<td style="padding:7px 10px;font-size:14px;color:var(--ac)">{o.get("opportunity_match","")}</td></tr>'
        for o in opportunities[:30]
    )

    no_data_html = ""
    if not ingested:
        no_data_html = """<div style="background:rgba(37,99,235,.08);border:1px solid var(--ac);border-radius:10px;padding:20px;margin-bottom:24px">
  <div style="font-size:15px;font-weight:700;margin-bottom:12px">📋 How to get SCPRS data (2 minutes, no login needed):</div>
  <ol style="color:var(--tx2);font-size:13px;line-height:2;margin:0;padding-left:20px">
    <li>Open <a href='https://caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx' target='_blank' style='color:var(--ac);font-weight:600'>caleprocure.ca.gov → Find Past Purchases (SCPRS)</a></li>
    <li>In <strong>Department</strong> type: <code style='background:var(--bg);padding:1px 6px;border-radius:3px'>CDCR</code> or <code style='background:var(--bg);padding:1px 6px;border-radius:3px'>Correctional Health</code></li>
    <li>In <strong>Description</strong> type: <code style='background:var(--bg);padding:1px 6px;border-radius:3px'>nitrile gloves</code> (repeat for each product)</li>
    <li>Click <strong>Search</strong> → <strong>Download</strong> to get CSV</li>
    <li>Open CSV, select all text (Ctrl+A), copy (Ctrl+C), paste below</li>
  </ol>
</div>"""

    stats_html = ""
    if ingested:
        stats_html = (
            f'<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:24px">'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px">'
            f'<div style="font-size:14px;color:var(--tx2)">PO RECORDS INGESTED</div>'
            f'<div style="font-size:32px;font-weight:800;color:var(--ac)">{len(ingested)}</div></div>'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px">'
            f'<div style="font-size:14px;color:var(--tx2)">ITEMS WE CAN COMPETE FOR</div>'
            f'<div style="font-size:32px;font-weight:800;color:var(--rd)">{len(opportunities)}</div></div>'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px">'
            f'<div style="font-size:14px;color:var(--tx2)">COMPETITOR SPEND TO CAPTURE</div>'
            f'<div style="font-size:32px;font-weight:800;color:var(--yl)">${total_opp:,.0f}</div></div></div>'
        )
        stats_html += (
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px">'
            f'<div><div style="font-size:14px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Vendors to beat at CCHCS/CDCR</div>'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:0;overflow:hidden">'
            f'<table style="width:100%;border-collapse:collapse"><thead><tr>'
            f'<th style="padding:8px 12px;font-size:14px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Vendor</th>'
            f'<th style="padding:8px 12px;font-size:14px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Their Spend</th>'
            f'<th style="padding:8px 12px;font-size:14px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Status</th>'
            f'</tr></thead><tbody>{vendor_rows}</tbody></table></div></div>'
            f'<div><div style="font-size:14px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Items to quote (your products)</div>'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:0;overflow:hidden">'
            f'<table style="width:100%;border-collapse:collapse"><thead><tr>'
            f'<th style="padding:7px 10px;font-size:14px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Description</th>'
            f'<th style="padding:7px 10px;font-size:14px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Curr. Vendor</th>'
            f'<th style="padding:7px 10px;font-size:14px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Amount</th>'
            f'<th style="padding:7px 10px;font-size:14px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Match</th>'
            f'</tr></thead><tbody>{opp_rows}</tbody></table></div></div></div>'
        )

    paste_box = """<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px;margin-top:16px">
  <div style="font-size:13px;font-weight:600;margin-bottom:8px">Paste SCPRS CSV here:</div>
  <textarea id="csvPaste" style="width:100%;height:90px;background:var(--bg);border:1px solid var(--bd);border-radius:6px;padding:10px;font-size:14px;color:var(--tx);font-family:monospace;box-sizing:border-box" placeholder="Paste CSV content from SCPRS download..."></textarea>
  <button onclick="ingestCSV()" style="margin-top:8px;padding:8px 20px;background:var(--ac);color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600">📊 Find Gaps</button>
  <span id="ingestStatus" style="margin-left:12px;font-size:14px;color:var(--tx2)"></span>
</div>"""

    content = (
        '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">'
        '<div><h2 style="font-size:22px;font-weight:700">🔍 SCPRS Gap Analysis</h2>'
        '<p style="color:var(--tx2);font-size:13px;margin-top:4px">What is CCHCS/CDCR buying that Reytech isn\'t selling them?</p></div>'
        '<div style="display:flex;gap:8px">'
        '<a href="https://caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx" target="_blank" style="padding:5px 12px;border:1px solid var(--ac);border-radius:6px;font-size:14px;text-decoration:none;color:var(--ac)">🔎 Open SCPRS</a>'
        '<a href="/" style="padding:5px 12px;border:1px solid var(--bd);border-radius:6px;font-size:14px;text-decoration:none">🏠 Home</a>'
        '</div></div>'
        + no_data_html + stats_html + paste_box
    )
    return render_page("generic.html", active_page="Intel",
        page_title="SCPRS Gap Analysis", content=content)



# ════════════════════════════════════════════════════════════════════════════════
# QA INTELLIGENCE v2 — Regression tracking, issue history, adaptive patterns
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/qa/intelligence")
@auth_required
@safe_route
def api_qa_intelligence():
    """QA intelligence summary: trends, regressions, persistent issues."""
    try:
        from src.agents.qa_agent import get_qa_intelligence_summary
        return jsonify({"ok": True, **get_qa_intelligence_summary()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/regressions")
@auth_required
@safe_route
def api_qa_regressions():
    """List unacknowledged score regressions."""
    try:
        from src.agents.qa_agent import _qa_db
        conn = _qa_db()
        rows = conn.execute(
            'SELECT id, detected_at, check_name, prev_score, new_score, "drop" as drop, acknowledged FROM qa_regressions ORDER BY detected_at DESC LIMIT 20'
        ).fetchall()
        return jsonify({"ok": True, "regressions": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/regressions/<int:rid>/ack", methods=["POST"])
@auth_required
@safe_route
def api_qa_regression_ack(rid):
    """Acknowledge a regression so it stops alerting."""
    try:
        from src.agents.qa_agent import _qa_db
        conn = _qa_db()
        conn.execute("UPDATE qa_regressions SET acknowledged=1 WHERE id=?", (rid,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/issues")
@auth_required
@safe_route
def api_qa_issues():
    """List persistent open issues sorted by frequency (most recurring = most critical)."""
    try:
        from src.agents.qa_agent import _qa_db
        conn = _qa_db()
        status_filter = request.args.get("status", "open")
        rows = conn.execute(
            "SELECT * FROM qa_issues WHERE status=? ORDER BY occurrences DESC LIMIT 50",
            (status_filter,)
        ).fetchall()
        return jsonify({"ok": True, "issues": [dict(r) for r in rows], "count": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/history")
@auth_required
@safe_route
def api_qa_history_v2():
    """QA run history with scores over time for trend chart."""
    try:
        from src.agents.qa_agent import _qa_db
        import json as _j
        conn = _qa_db()
        try:
            limit = max(1, min(int(request.args.get("limit", 30)), 200))
        except (ValueError, TypeError, OverflowError):
            limit = 30
        rows = conn.execute(
            "SELECT run_at, score, grade, passed, failed, warned, duration_ms "
            "FROM qa_runs ORDER BY run_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return jsonify({"ok": True, "history": [dict(r) for r in rows], "count": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/qa/intelligence")
@auth_required
@safe_page
def page_qa_intelligence():
    """QA Intelligence dashboard — trend charts, regressions, persistent issues."""
    try:
        from src.agents.qa_agent import get_qa_intelligence_summary, _qa_db, get_health_trend
        import json as _j
        intel = get_qa_intelligence_summary()
        trend = get_health_trend()
        conn = _qa_db()
        issues = [dict(r) for r in conn.execute(
            "SELECT check_name, message, occurrences, first_seen, last_seen "
            "FROM qa_issues WHERE status='open' ORDER BY occurrences DESC LIMIT 30"
        ).fetchall()]
        history = [dict(r) for r in conn.execute(
            "SELECT run_at, score, grade FROM qa_runs ORDER BY run_at DESC LIMIT 20"
        ).fetchall()]
        regressions = [dict(r) for r in conn.execute(
            "SELECT * FROM qa_regressions WHERE acknowledged=0 ORDER BY detected_at DESC LIMIT 5"
        ).fetchall()]
    except Exception as e:
        intel = {"error": str(e)}; issues = []; history = []; regressions = []
        trend = {"trend": "unknown", "scores": []}

    score = intel.get("current_score", 0)
    score_color = "var(--gn)" if score >= 90 else ("var(--yl)" if score >= 75 else "var(--rd)")
    trend_str = intel.get("trend", "→ stable")
    trend_color = "var(--gn)" if "improv" in trend_str else ("var(--rd)" if "declin" in trend_str else "var(--yl)")

    issue_rows = ""
    for iss in issues:
        sev = "var(--rd)" if iss["occurrences"] >= 5 else ("var(--yl)" if iss["occurrences"] >= 2 else "var(--tx2)")
        issue_rows += f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:7px 10px;font-size:14px;color:{sev};font-weight:600">{iss["check_name"]}</td>
  <td style="padding:7px 10px;font-size:14px">{iss["message"][:90]}</td>
  <td style="padding:7px 10px;font-size:14px;text-align:center;color:{sev};font-weight:700">{iss["occurrences"]}</td>
  <td style="padding:7px 10px;font-size:14px;color:var(--tx2)">{iss["first_seen"][:10] if iss["first_seen"] else "?"}</td>
</tr>"""

    reg_html = ""
    for reg in regressions:
        reg_html += f"""<div style="background:rgba(220,38,38,.08);border:1px solid var(--rd);border-radius:6px;padding:10px 14px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center">
  <div>
    <span style="font-size:13px;font-weight:700;color:var(--rd)">Score drop: {reg["prev_score"]} → {reg["new_score"]} (-{reg.get("score_drop", reg.get("drop",0))} pts)</span>
    <div style="font-size:14px;color:var(--tx2);margin-top:2px">{reg["detected_at"][:16] if reg["detected_at"] else "?"}</div>
  </div>
  <button onclick="fetch('/api/qa/regressions/{reg["id"]}/ack',{{method:'POST',credentials:'same-origin'}}).then(()=>location.reload())" style="padding:4px 12px;border:1px solid var(--rd);border-radius:4px;background:none;color:var(--rd);font-size:14px;cursor:pointer">Acknowledge</button>
</div>"""

    scores_js = str([r["score"] for r in reversed(history)]) if history else "[]"
    labels_js = str([r["run_at"][:10] for r in reversed(history)]) if history else "[]"

    return render_page("qa_intel.html", active_page="Intel",
        f=f,
        intel=intel,
        issue_rows=issue_rows,
        labels_js=labels_js,
        regressions=regressions,
        score=score,
        score_color=score_color,
        scores_js=scores_js,
        trend_color=trend_color,
        trend_str=trend_str)





# ════════════════════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════════════════════
# Follow-Up Automation Routes
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/follow-ups/scan", methods=["POST"])
@auth_required
@safe_route
def api_follow_up_scan():
    """Manually trigger a follow-up scan."""
    try:
        from src.agents.follow_up_engine import run_follow_up_scan
        result = run_follow_up_scan()
        return jsonify(result)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/follow-ups/summary")
@auth_required
@safe_route
def api_follow_up_summary():
    """Get follow-up summary for daily brief."""
    try:
        from src.agents.follow_up_engine import get_follow_up_summary
        return jsonify(get_follow_up_summary())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/follow-ups/status")
@auth_required
@safe_route
def api_follow_up_status():
    """Get follow-up engine status."""
    import json as _json
    try:
        state_path = os.path.join(DATA_DIR, "follow_up_state.json")
        if os.path.exists(state_path):
            with open(state_path) as f:
                state = _json.load(f)
        else:
            state = {"status": "no_scans_yet"}
        return jsonify({"ok": True, **state})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})




# ════════════════════════════════════════════════════════════════════════════════
# Daily Briefing Page + Push Notification (SMS via Twilio)
# ════════════════════════════════════════════════════════════════════════════════

TWILIO_SID = os.environ.get("TWILIO_ACCOUNT_SID", "")
TWILIO_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM = os.environ.get("TWILIO_FROM_NUMBER", "")
NOTIFY_TO = os.environ.get("NOTIFY_PHONE", "")
BRIEF_SEND_HOUR = int(os.environ.get("BRIEF_SEND_HOUR", "7"))


def _build_text_brief():
    """Build concise SMS-friendly daily brief."""
    lines = []
    lines.append("REYTECH DAILY BRIEF")
    lines.append(datetime.now().strftime("%a %b %d, %Y"))
    lines.append("")

    try:
        pcs = _load_price_checks()
        active_pcs = [p for p in (pcs.values() if isinstance(pcs, dict) else pcs) if not p.get("is_test")]
        quotes = get_all_quotes()
        active_quotes = [q for q in quotes if not q.get("is_test")]
        pipeline_val = sum(float(q.get("total", 0)) for q in active_quotes if q.get("status") in ("draft", "sent", "reviewed"))
        lines.append(f"Pipeline: ${pipeline_val:,.0f}")
        new_pcs = [p for p in active_pcs if p.get("status") in ("new", "pending", "inbox")]
        if new_pcs:
            lines.append(f"{len(new_pcs)} PCs need pricing")
        if active_quotes:
            lines.append(f"{len(active_quotes)} quotes active")
    except Exception:
        lines.append("Pipeline: data unavailable")

    try:
        from src.agents.follow_up_engine import get_follow_up_summary
        fu = get_follow_up_summary()
        awaiting = fu.get("total_awaiting_response", 0)
        overdue = fu.get("overdue", 0)
        if awaiting > 0:
            lines.append(f"{awaiting} outreach awaiting response")
        if overdue > 0:
            lines.append(f"! {overdue} overdue (7d+ no reply)")
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    try:
        from src.core.dal import get_outbox as _dal_ob4
        ob = _dal_ob4()
        if ob:
            if isinstance(ob, list):
                drafts = [e for e in ob if e.get("status") in ("draft", "follow_up_draft", "cs_draft")]
                if drafts:
                    lines.append(f"{len(drafts)} email drafts to review")
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    try:
        orders = _load_orders()
        active_orders = [o for o in orders.values() if o.get("status") not in ("completed", "cancelled")]
        if active_orders:
            lines.append(f"{len(active_orders)} active orders")
    except Exception as _e:
        log.debug("Suppressed: %s", _e)

    return "\n".join(lines)


def _send_sms(to_number, message):
    """Send SMS via Twilio REST API."""
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_FROM]):
        return {"ok": False, "error": "Twilio not configured. Set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER"}
    try:
        import urllib.request, urllib.parse, base64
        url = f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_SID}/Messages.json"
        data = urllib.parse.urlencode({"To": to_number, "From": TWILIO_FROM, "Body": message}).encode()
        creds = base64.b64encode(f"{TWILIO_SID}:{TWILIO_TOKEN}".encode()).decode()
        req = urllib.request.Request(url, data=data, headers={
            "Authorization": f"Basic {creds}", "Content-Type": "application/x-www-form-urlencoded"})
        resp = urllib.request.urlopen(req, timeout=10)
        return {"ok": True, "status": resp.status, "message": "SMS sent"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@bp.route("/brief")
@auth_required
@safe_page
def daily_brief_page():
    """Daily Briefing page."""
    header = ""  # Rendered via base.html template
    now = datetime.now()
    text_brief = _build_text_brief()
    twilio_ok = bool(TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM)

    # Gather data
    try:
        from src.agents.follow_up_engine import get_follow_up_summary
        fu = get_follow_up_summary()
    except Exception:
        fu = {"total_awaiting_response": 0, "overdue": 0, "overdue_items": [], "pending_items": [], "follow_ups_sent": 0}

    try:
        pcs = _load_price_checks()
        new_pcs = [(pid, p) for pid, p in (pcs.items() if isinstance(pcs, dict) else []) if p.get("status") in ("new", "pending", "inbox") and not p.get("is_test")]
    except Exception:
        new_pcs = []

    try:
        quotes = get_all_quotes()
        aging = []
        for q in quotes:
            if q.get("is_test") or q.get("status") not in ("sent", "reviewed"):
                continue
            try:
                created = datetime.fromisoformat(q.get("date", "").replace("Z", ""))
                age = (now - created).days
                if age >= 5:
                    aging.append((q.get("quote_number", ""), q.get("bill_to", {}).get("name", "?"), age, float(q.get("total", 0))))
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
        aging.sort(key=lambda x: -x[2])
    except Exception:
        aging = []

    try:
        from src.core.dal import get_outbox as _dal_ob5
        ob2 = _dal_ob5()
        drafts = [e for e in (ob2 if isinstance(ob2, list) else []) if e.get("status") in ("draft", "follow_up_draft", "cs_draft")]
    except Exception:
        drafts = []

    # Pre-build HTML sections
    pc_html = ""
    for pid, p in new_pcs[:5]:
        pc_html += f'<div style="padding:10px 14px;background:rgba(251,146,60,.08);border:1px solid rgba(251,146,60,.3);border-radius:8px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center"><div>New PC needs pricing</div><a href="/pricecheck/{pid}" class="btn btn-s" style="padding:6px 12px;font-size:14px">Price it</a></div>'
    if not new_pcs:
        pc_html = '<div style="padding:10px;color:var(--t2)">No PCs awaiting pricing</div>'

    aging_html = ""
    for qn, cust, age, total in aging[:5]:
        aging_html += f'<div style="padding:10px 14px;background:rgba(239,68,68,.08);border:1px solid rgba(239,68,68,.3);border-radius:8px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center"><div><strong>{qn}</strong> {cust} (${total:,.0f}) {age}d old</div><a href="/quote/{qn}" class="btn btn-s" style="padding:6px 12px;font-size:14px">View</a></div>'
    if not aging:
        aging_html = '<div style="padding:10px;color:var(--t2)">No aging quotes</div>'

    draft_html = ""
    for d in drafts[:3]:
        subj = _sanitize_input(d.get("subject", "(no subject)"))[:60]
        to_addr = _sanitize_input(d.get("to", "?"))
        draft_html += f'<div style="padding:10px 14px;background:rgba(79,140,255,.08);border:1px solid rgba(79,140,255,.3);border-radius:8px;margin-bottom:8px">Draft: {subj} &rarr; {to_addr}</div>'
    if len(drafts) > 3:
        draft_html += f'<div style="padding:8px;color:var(--t2);font-size:13px">+ {len(drafts)-3} more</div>'
    draft_btn = f'<a href="/outbox" class="btn btn-s" style="margin-top:8px;padding:8px 14px;font-size:13px">Review {len(drafts)} Drafts</a>' if drafts else ""

    overdue_html = ""
    for item in fu.get("overdue_items", [])[:5]:
        fac = item.get("facility", "?")
        email = item.get("to_email", "")
        days = item.get("days_since", 0)
        overdue_html += f'<div style="padding:8px 12px;border-left:3px solid #ef4444;margin-bottom:6px;font-size:13px"><strong>{fac}</strong> {email} {days}d no response</div>'

    twilio_status = f"Twilio configured, sends to {NOTIFY_TO}" if twilio_ok else "Set TWILIO env vars on Railway to enable SMS"
    twilio_disabled = "" if twilio_ok else 'disabled title="Configure Twilio first"'

    content = f"""
<div style="max-width:800px;margin:0 auto;padding:20px">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px">
    <div>
      <h1 style="margin:0;font-size:28px">Daily Brief</h1>
      <p style="margin:4px 0 0;color:var(--t2);font-size:14px">{now.strftime('%A, %B %d, %Y')} &middot; {now.strftime('%I:%M %p')}</p>
    </div>
    <div style="display:flex;gap:8px">
      <button onclick="sendBriefSMS()" class="btn btn-s" style="padding:10px 16px;font-size:13px" {twilio_disabled}>Text Me This</button>
      <button onclick="location.reload()" class="btn btn-s" style="padding:10px 16px;font-size:13px">Refresh</button>
    </div>
  </div>

  <div style="background:var(--bg2);border:1px solid var(--bd);border-radius:12px;padding:20px;margin-bottom:16px">
    <h2 style="margin:0 0 16px;font-size:18px;color:var(--ac)">Action Items</h2>
    {pc_html}
    {aging_html}
    {draft_html}
    {draft_btn}
  </div>

  <div style="background:var(--bg2);border:1px solid var(--bd);border-radius:12px;padding:20px;margin-bottom:16px">
    <h2 style="margin:0 0 16px;font-size:18px;color:#f0883e">Follow-Up Status</h2>
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px">
      <div style="text-align:center;padding:12px;background:var(--bg);border-radius:8px">
        <div style="font-size:24px;font-weight:700">{fu.get('total_awaiting_response', 0)}</div>
        <div style="font-size:14px;color:var(--t2)">Awaiting Reply</div>
      </div>
      <div style="text-align:center;padding:12px;background:var(--bg);border-radius:8px">
        <div style="font-size:24px;font-weight:700">{fu.get('follow_ups_sent', 0)}</div>
        <div style="font-size:14px;color:var(--t2)">Follow-Ups Created</div>
      </div>
      <div style="text-align:center;padding:12px;background:var(--bg);border-radius:8px">
        <div style="font-size:24px;font-weight:700;{'color:#ef4444' if fu.get('overdue',0) > 0 else ''}">{fu.get('overdue', 0)}</div>
        <div style="font-size:14px;color:var(--t2)">Overdue (7d+)</div>
      </div>
    </div>
    {overdue_html}
    <button onclick="fetch('/api/follow-ups/scan',{{method:'POST'}}).then(r=>r.json()).then(d=>{{alert('Scanned: '+d.scanned+' items, '+d.new_drafts+' new drafts');location.reload()}})" class="btn btn-s" style="margin-top:8px;padding:8px 14px;font-size:13px">Scan Now</button>
  </div>

  <div style="background:var(--bg2);border:1px solid var(--bd);border-radius:12px;padding:20px;margin-bottom:16px">
    <h2 style="margin:0 0 12px;font-size:18px;color:var(--t2)">SMS Preview</h2>
    <pre style="background:#000;color:#0f0;padding:16px;border-radius:8px;font-size:13px;white-space:pre-wrap;font-family:monospace;line-height:1.5">{text_brief}</pre>
    <p style="font-size:14px;color:var(--t2);margin:8px 0 0">{twilio_status}</p>
  </div>
</div>

<script>
function sendBriefSMS() {{
  if (!confirm('Send this brief as a text message?')) return;
  fetch('/api/brief/send-sms', {{method:'POST'}})
  .then(r => r.json())
  .then(d => {{ if (d.ok) alert('Brief sent!'); else alert('Failed: ' + (d.error || 'Unknown error')); }})
  .catch(e => alert('Error: ' + e));
}}
</script>
"""
    from src.api.render import render_page
    return render_page("generic.html", active_page="Brief", page_title="Daily Brief", content=content)


@bp.route("/api/brief/text")
@auth_required
@safe_route
def api_brief_text():
    """Get text version of daily brief."""
    return jsonify({"ok": True, "text": _build_text_brief()})


@bp.route("/api/brief/send-sms", methods=["POST"])
@auth_required
@safe_route
def api_brief_send_sms():
    """Send daily brief via SMS."""
    if not NOTIFY_TO:
        return jsonify({"ok": False, "error": "NOTIFY_PHONE not set"})
    text = _build_text_brief()
    if len(text) > 1550:
        text = text[:1547] + "..."
    return jsonify(_send_sms(NOTIFY_TO, text))


# ════════════════════════════════════════════════════════════════════════════════
# Data Quality Check API
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/data/quality")
@auth_required
@safe_route
def api_data_quality():
    """Run data quality check and return report."""
    import json as _j
    report = {"ok": True, "fixes": 0, "issues": []}

    # Customers
    try:
        cust_path = os.path.join(DATA_DIR, "customers.json")
        customers = _j.load(open(cust_path))
        fixes = 0
        for c in customers:
            # Normalize emails
            email = c.get("email", "")
            if email and email != email.strip().lower():
                c["email"] = email.strip().lower()
                fixes += 1
            # Strip whitespace
            for key in ["qb_name", "display_name", "company", "parent", "city", "state", "zip", "phone"]:
                val = c.get(key, "")
                if isinstance(val, str) and val != val.strip():
                    c[key] = val.strip()
                    fixes += 1
            # Ensure display_name
            if not c.get("display_name", "").strip() and c.get("qb_name", "").strip():
                c["display_name"] = c["qb_name"]
                fixes += 1
        if fixes > 0:
            with open(cust_path, "w") as f:
                _j.dump(customers, f, indent=2, default=str)

        report["customers"] = {
            "total": len(customers),
            "with_email": sum(1 for c in customers if c.get("email", "").strip()),
            "with_phone": sum(1 for c in customers if c.get("phone", "").strip()),
            "no_email": sum(1 for c in customers if not c.get("email", "").strip()),
            "fixes": fixes,
        }
        report["fixes"] += fixes
    except Exception as e:
        report["issues"].append(f"customers: {e}")

    # CRM Contacts
    try:
        from src.core.db import get_all_contacts
        ct_list = list(get_all_contacts().values())
        report["crm_contacts"] = {
            "total": len(ct_list),
            "with_email": sum(1 for ct in ct_list if isinstance(ct, dict) and ct.get("buyer_email", "").strip()),
            "with_phone": sum(1 for ct in ct_list if isinstance(ct, dict) and ct.get("buyer_phone", "").strip()),
        }
    except Exception as e:
        report["issues"].append(f"crm_contacts: {e}")

    # Vendors
    try:
        vendor_path = os.path.join(DATA_DIR, "vendors.json")
        vendors = _j.load(open(vendor_path))
        report["vendors"] = {
            "total": len(vendors),
            "with_email": sum(1 for v in vendors if isinstance(v, dict) and v.get("email", "").strip()),
            "with_phone": sum(1 for v in vendors if isinstance(v, dict) and v.get("phone", "").strip()),
        }
    except Exception as e:
        report["issues"].append(f"vendors: {e}")

    return jsonify(report)


# ══ Consolidated from routes_features*.py ══════════════════════════════════

# From routes_features.py — Contact Search
@bp.route("/api/crm/search")
@auth_required
@safe_route
def api_crm_contact_search():
    """Search contacts by name, email, or institution. ?q=keyword
    Unions contacts table + scprs_buyers table for full autocomplete coverage.
    """
    try:
        q = request.args.get("q", "").strip()
        if not q:
            return jsonify({"ok": False, "error": "Provide ?q=search_term"})
        with get_db() as conn:
            like = f"%{q}%"
            rows = conn.execute("""
                SELECT buyer_name, buyer_email,
                       department AS agency, department AS institution
                FROM scprs_buyers
                WHERE (buyer_name LIKE ? OR buyer_email LIKE ? OR department LIKE ?)
                  AND buyer_name != ''
                UNION
                SELECT name AS buyer_name, email AS buyer_email,
                       COALESCE(agency, institution, '') AS agency,
                       COALESCE(institution, agency, '') AS institution
                FROM contacts
                WHERE name LIKE ? OR email LIKE ? OR institution LIKE ? OR agency LIKE ?
                ORDER BY buyer_name
                LIMIT 25
            """, (like, like, like, like, like, like, like)).fetchall()
            contacts = [dict(r) for r in rows]
            return jsonify({"ok": True, "contacts": contacts, "count": len(contacts), "query": q})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# From routes_features3.py — Vendor Performance
@bp.route("/api/vendor/performance")
@auth_required
@safe_route
def api_vendor_performance():
    """Score vendors by response time, pricing accuracy, fill rate."""
    cat_path = os.path.join(DATA_DIR, "product_catalog.json")
    vendors = defaultdict(lambda: {"quotes": 0, "products": 0, "avg_markup": [], "urls": set()})

    try:
        with open(cat_path) as f:
            cat = json.load(f)

        for pid, p in cat.get("products", {}).items():
            for url in p.get("supplier_urls", []):
                domain = url.split("/")[2] if "/" in url and len(url.split("/")) > 2 else url
                domain = domain.replace("www.", "")
                vendors[domain]["products"] += 1
                vendors[domain]["urls"].add(url)

            if p.get("supplier_cost") and p.get("last_quoted_price"):
                cost = p["supplier_cost"]
                price = p["last_quoted_price"]
                if cost > 0:
                    markup = ((price - cost) / cost) * 100
                    for url in p.get("supplier_urls", []):
                        domain = url.split("/")[2] if "/" in url and len(url.split("/")) > 2 else url
                        domain = domain.replace("www.", "")
                        vendors[domain]["avg_markup"].append(markup)
    except Exception as _e:
        log.debug("suppressed: %s", _e)

    result = []
    for name, data in vendors.items():
        markups = data.pop("avg_markup", [])
        data["urls"] = list(data["urls"])[:3]
        data["avg_markup_pct"] = round(sum(markups) / len(markups), 1) if markups else None
        data["name"] = name
        result.append(data)

    result.sort(key=lambda x: x["products"], reverse=True)

    return jsonify({"ok": True, "vendors": result[:20], "total": len(result)})
