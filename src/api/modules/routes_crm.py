# routes_crm.py
import re
from datetime import datetime

# ── JSON→SQLite compatibility (Phase 32c migration) ──────────────────────────
try:
    from src.core.db import (
        get_all_customers, get_all_vendors, get_all_price_checks, get_price_check,
        upsert_price_check, get_outbox, upsert_outbox_email, update_outbox_status,
        get_email_templates, upsert_email_template, get_vendor_registrations,
        upsert_vendor_registration, get_market_intelligence, upsert_market_intelligence,
        get_intel_agencies, upsert_intel_agency, get_growth_outreach, save_growth_campaign,
        get_qa_reports, save_qa_report, get_latest_qa_report,
        upsert_customer, upsert_vendor,
    )
    _HAS_DB_DAL = True
except ImportError:
    _HAS_DB_DAL = False
# ─────────────────────────────────────────────────────────────────────────────
# 70 routes, 3186 lines
# Loaded by dashboard.py via load_module()

# CUSTOMERS CRM — Agency parent/child, QuickBooks-synced
# ═══════════════════════════════════════════════════════════════════════════════

def _load_customers():
    """Load customers CRM database. Auto-seeds from bundled file if missing."""
    path = os.path.join(DATA_DIR, "customers.json")
    try:
        with open(path) as f:
            data = json.load(f)
            if data:
                return data
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    # Auto-seed: Railway volume may not have customers.json yet
    # Check for seed file in repo root (not overridden by volume mount)
    seed_path = os.path.join(BASE_DIR, "customers_seed.json")
    if os.path.exists(seed_path):
        try:
            with open(seed_path) as f:
                data = json.load(f)
            if data:
                log.info(f"Auto-seeding {len(data)} customers from seed file")
                os.makedirs(DATA_DIR, exist_ok=True)
                with open(path, "w") as f:
                    json.dump(data, f, indent=2)
                return data
        except Exception as e:
            log.warning(f"Failed to seed customers: {e}")
    return []

def _save_customers(customers):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, "customers.json")
    with open(path, "w") as f:
        json.dump(customers, f, indent=2)

@bp.route("/api/customers")
@auth_required
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
                c.get("display_name", ""), c.get("company", ""),
                c.get("qb_name", ""), c.get("agency", ""),
                c.get("city", ""), c.get("abbreviation", ""),
            ]).lower()
            if q not in searchable:
                continue
        results.append(c)
    return jsonify(results)

@bp.route("/api/customers", methods=["POST"])
@auth_required
def api_customers_add():
    """Add a new customer. User confirms before saving."""
    data = request.json
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
    """Guess agency from institution name for new customers."""
    upper = institution_name.upper()
    if any(kw in upper for kw in ("CCHCS", "HEALTH CARE SERVICE")):
        return "CCHCS"
    if any(kw in upper for kw in ("CALVET", "CAL VET", "VETERAN")):
        return "CalVet"
    if any(kw in upper for kw in ("STATE HOSPITAL", "DSH")):
        return "DSH"
    if any(kw in upper for kw in ("DGS", "GENERAL SERVICE")):
        return "DGS"
    # CDCR patterns
    cdcr_kw = ("CDCR", "CORRECTION", "STATE PRISON", "CONSERVATION CENTER",
               "INSTITUTION FOR", "FOLSOM", "PELICAN", "SAN QUENTIN", "CORCORAN")
    cdcr_pfx = ("CSP", "CIM", "CIW", "SCC", "CMC", "SATF", "CHCF", "PVSP",
                "KVSP", "LAC", "MCSP", "NKSP", "SAC", "WSP", "SOL", "FSP",
                "HDSP", "ISP", "CTF", "RJD", "CAL", "CEN", "ASP", "CCWF", "VSP")
    if any(kw in upper for kw in cdcr_kw):
        return "CDCR"
    for pfx in cdcr_pfx:
        if upper.startswith(pfx + "-") or upper.startswith(pfx + " ") or upper == pfx:
            return "CDCR"
    return "DEFAULT"

@bp.route("/api/quotes/counter")
@auth_required
def api_quote_counter():
    """Get current quote counter state."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})
    return jsonify({"ok": True, "next": peek_next_quote_number()})


@bp.route("/api/search")
@auth_required
def api_universal_search():
    """Universal search across ALL data: quotes, CRM contacts, intel buyers,
    orders, RFQs, growth prospects. Returns results with clickable links.
    GET ?q=<query>&limit=<n>
    """
    q = (_sanitize_input(request.args.get("q", "")) or "").strip().lower()
    limit = min(int(request.args.get("limit", 30)), 100)
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
        except Exception:
            pass

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
    except Exception:
        pass

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
        except Exception:
            pass

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
    except Exception:
        pass

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
    except Exception:
        pass

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
                      for t in ("quote","contact","intel_buyer","order","rfq")},
    })





@bp.route("/api/quotes/set-counter", methods=["POST"])
@auth_required
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
        if inst_upper in qt_inst or qt_inst in inst_upper:
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
def api_research_test():
    """Test Amazon search — ?q=nitrile+gloves"""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    q = request.args.get("q", "nitrile exam gloves")
    return jsonify(test_amazon_search(q))


@bp.route("/api/research/lookup")
@auth_required
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
def api_research_status():
    """Check progress of RFQ product research."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    return jsonify(RESEARCH_STATUS)


@bp.route("/api/research/cache-stats")
@auth_required
def api_research_cache_stats():
    """Get product research cache statistics."""
    if not PRODUCT_RESEARCH_AVAILABLE:
        return jsonify({"error": "product_research.py not available"}), 503
    return jsonify(get_research_cache_stats())


@bp.route("/api/debug/env-check")
@auth_required
def api_debug_env_check():
    """Check if SERPAPI_KEY is visible to the app."""
    import os
    serp_val = os.environ.get("SERPAPI_KEY", "")
    all_keys = sorted(os.environ.keys())
    serp_matches = [k for k in all_keys if "SERP" in k.upper()]
    return jsonify({
        "SERPAPI_KEY_set": bool(serp_val),
        "SERPAPI_KEY_preview": f"{serp_val[:8]}..." if serp_val else "EMPTY",
        "serp_matching_keys": serp_matches,
        "all_env_keys": all_keys,
    })


@bp.route("/api/config/set-serpapi-key", methods=["GET", "POST"])
@auth_required
def api_set_serpapi_key():
    """Store SerpApi key on persistent volume (bypasses Railway env var issues)."""
    if request.method == "POST":
        key = request.json.get("key", "") if request.is_json else request.args.get("key", "")
    else:
        key = request.args.get("key", "")
    if not key:
        return jsonify({"error": "Add ?key=YOUR_KEY to the URL"}), 400
    key_file = os.path.join(DATA_DIR, ".serpapi_key")
    with open(key_file, "w") as f:
        f.write(key.strip())
    return jsonify({"ok": True, "message": "SerpApi key saved to volume", "preview": f"{key[:8]}..."})


@bp.route("/api/config/check-serpapi-key")
@auth_required
def api_check_serpapi_key():
    """Check if SerpApi key is stored on volume."""
    key_file = os.path.join(DATA_DIR, ".serpapi_key")
    if os.path.exists(key_file):
        with open(key_file) as f:
            key = f.read().strip()
        return jsonify({"stored": True, "preview": f"{key[:8]}..." if key else "EMPTY"})
    return jsonify({"stored": False})


# ═══════════════════════════════════════════════════════════════════════
# Price Check API (v6.2 — Phase 6)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pricecheck/parse", methods=["POST"])
@auth_required
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
    elif request.is_json and request.json.get("pdf_path"):
        try:
            pdf_path = _validate_pdf_path(request.json["pdf_path"])
        except ValueError as _e:
            return jsonify({"error": f"Invalid pdf_path: {_e}"}), 400
    else:
        return jsonify({"error": "Upload a file or provide pdf_path in JSON"}), 400

    tax_rate = 0.0
    if request.is_json:
        tax_rate = float(request.json.get("tax_rate", 0.0))
    elif request.form.get("tax_rate"):
        tax_rate = float(request.form.get("tax_rate", 0.0))

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
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


@bp.route("/api/pricecheck/download/<filename>")
@auth_required
def api_pricecheck_download(filename):
    """Download a completed Price Check PDF."""
    safe = os.path.basename(filename)
    path = os.path.join(DATA_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path, as_attachment=True, download_name=safe)


@bp.route("/api/pricecheck/view-pdf/<path:filename>")
@auth_required
def api_pricecheck_view_pdf(filename):
    """Serve a PDF inline for the browser PDF viewer (iframes, tabs)."""
    import mimetypes
    safe = os.path.basename(filename)
    # Search: data dir, outputs subfolders, uploads subfolders
    search_paths = [os.path.join(DATA_DIR, safe)]
    for search_root in [os.path.join(DATA_DIR, "outputs"), os.path.join(DATA_DIR, "uploads"), DATA_DIR]:
        if os.path.isdir(search_root):
            for root, dirs, files in os.walk(search_root):
                for f in files:
                    if f == safe:
                        search_paths.append(os.path.join(root, f))
    for path in search_paths:
        if os.path.exists(path):
            return send_file(path, mimetype="application/pdf", download_name=safe)
    return jsonify({"error": f"PDF not found: {safe}"}), 404


@bp.route("/api/pricecheck/test-parse")
@auth_required
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
def api_health():
    """Comprehensive system health check with path validation."""
    health = {"status": "ok", "build": "v20260220-1005-pdf-v4", "checks": {}}


@bp.route("/api/build")
def api_build_version():
    """Quick build version check (no auth) to verify deploys."""
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
def api_reclassify_to_pc():
    """Move stuck #unknown RFQs to the Price Check queue.
    POST {"rfq_ids": ["id1", "id2"]} or {"rfq_ids": "all_unknown"}
    """
    data = request.get_json(force=True)
    rfq_ids = data.get("rfq_ids", [])
    
    from src.api.dashboard import load_rfqs, save_rfqs, _load_price_checks, _save_price_checks
    
    rfqs = load_rfqs()
    pcs = _load_price_checks()
    moved = []
    
    # "all_unknown" = move all #unknown solicitation RFQs with 0 items
    if rfq_ids == "all_unknown":
        rfq_ids = [rid for rid, r in rfqs.items()
                   if r.get("solicitation_number") in ("unknown", "#unknown", "")
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
        except Exception:
            pass
        
        pcs[pc_id] = {
            "id": pc_id,
            "pc_number": r.get("solicitation_number", "unknown"),
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
        save_rfqs(rfqs)
        _save_price_checks(pcs)
    
    return jsonify({
        "ok": True,
        "moved": len(moved),
        "details": moved,
    })


@bp.route("/api/metrics")
@auth_required
def api_metrics():
    """Real-time performance & system metrics — cache efficiency, data sizes, thread state."""
    import gc

    # Cache stats
    with _json_cache_lock:
        cache_size = len(_json_cache)
        cache_keys = list(_json_cache.keys())
    
    # Data file sizes
    data_files = {}
    for fname in ["rfqs.json","quotes_log.json","orders.json","crm_activity.json",
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
        except Exception:
            pass
    if GROWTH_AVAILABLE:
        try:
            from src.agents.growth_agent import PULL_STATUS, BUYER_STATUS
            agent_states["growth_pull_running"] = PULL_STATUS.get("running", False)
            agent_states["growth_buyer_running"] = BUYER_STATUS.get("running", False)
        except Exception:
            pass

    # GC stats
    gc_counts = gc.get_count()

    # DB stats
    db_stats = {}
    try:
        from src.core.db import get_db_stats
        db_stats = get_db_stats()
    except Exception:
        pass

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
def api_price_history():
    """Search price history database.
    GET ?q=<description>&pn=<part_number>&source=<amazon|scprs|quote>&limit=50
    """
    try:
        from src.core.db import get_price_history_db, get_price_stats
        q = request.args.get("q","").strip()
        pn = request.args.get("pn","").strip()
        source = request.args.get("source","").strip()
        limit = min(int(request.args.get("limit",50)), 200)

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
        _save_price_checks(pcs)
        # Ingest into KB
        _ingest_pc_to_won_quotes(pc)

    return jsonify(json.loads(json.dumps({
        "ok": result.get("ok", False),
        "timing": result.get("timing", {}),
        "confidence": result.get("confidence", {}),
        "steps": result.get("steps", []),
        "draft_email": result.get("draft_email", {}),
    }, default=str)))


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
    if email_cfg.get("email_password"):
        poll_thread = threading.Thread(target=email_poll_loop, daemon=True)
        poll_thread.start()
        log.info("Email polling started")
    else:
        POLL_STATUS["error"] = "Set GMAIL_PASSWORD env var or email_password in config"
        log.info("Email polling disabled — no password configured")

# ─── Logo Upload + Quotes Database ────────────────────────────────────────────

@bp.route("/settings/upload-logo", methods=["POST"])
@auth_required
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
def serve_logo():
    """Serve the uploaded Reytech logo."""
    for ext in ("png", "jpg", "jpeg", "gif"):
        path = os.path.join(DATA_DIR, f"reytech_logo.{ext}")
        if os.path.exists(path):
            return send_file(path)
    return "", 404


@bp.route("/quotes/<quote_number>/status", methods=["POST"])
@auth_required
def quote_update_status(quote_number):
    """Mark a quote as won, lost, or pending. Triggers won workflow if applicable."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"})
    data = request.json or request.form
    new_status = data.get("status", "").lower()
    po_number = data.get("po_number", "")
    notes = data.get("notes", "")
    if new_status not in ("won", "lost", "pending"):
        return jsonify({"ok": False, "error": f"Invalid status: {new_status}"})
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
    pc_num = pc.get("pc_number", "unknown")
    safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", pc_num.strip())
    output_path = os.path.join(DATA_DIR, f"Quote_{safe_name}_Reytech.pdf")

    locked_qn = pc.get("reytech_quote_number", "")  # reuse if regenerating

    logs = []
    t0 = time.time()

    result = generate_quote_from_pc(
        pc, output_path,
        include_tax=pc.get("tax_enabled", False),
        tax_rate=pc.get("tax_rate", 0.0725) if pc.get("tax_enabled") else 0.0,
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
    _save_price_checks(pcs)
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
    except Exception:
        pass

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
def api_notifications_persistent():
    """Get persistent notifications from SQLite (survives deploys)."""
    unread_only = request.args.get("unread_only") == "true"
    limit = int(request.args.get("limit", 30))
    try:
        from src.agents.notify_agent import get_notifications, get_unread_count
        notifs = get_notifications(limit=limit, unread_only=unread_only)
        return jsonify({"ok": True, "notifications": notifs, "unread_count": get_unread_count()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/notifications/bell-count")
@auth_required
def api_bell_count():
    """Fast unread count for nav bell badge — polled every 30s."""
    try:
        from src.agents.notify_agent import get_unread_count
        from src.agents.cs_agent import get_cs_drafts
        cs_pending = len(get_cs_drafts())
        return jsonify({
            "ok": True,
            "unread": get_unread_count(),
            "cs_drafts": cs_pending,
            "total_badge": get_unread_count() + cs_pending,
        })
    except Exception as e:
        return jsonify({"ok": False, "unread": 0, "cs_drafts": 0, "total_badge": 0})


@bp.route("/api/notifications/mark-read", methods=["POST"])
@auth_required
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


@bp.route("/api/notify/status")
@auth_required
def api_notify_status():
    """Notification agent configuration status."""
    try:
        from src.agents.notify_agent import get_agent_status
        return jsonify({"ok": True, **get_agent_status()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/email-log")
@auth_required
def api_email_log():
    """Get email communication log for CS dispute resolution.
    ?contact=email&quote=R26Q4&po=12345&limit=50
    """
    contact = request.args.get("contact","")
    quote = request.args.get("quote","")
    po = request.args.get("po","")
    limit = int(request.args.get("limit", 50))
    try:
        from src.agents.notify_agent import get_email_thread, build_cs_communication_summary
        thread = get_email_thread(contact_email=contact, quote_number=quote, po_number=po, limit=limit)
        summary = build_cs_communication_summary(contact, quote, po)
        return jsonify({"ok": True, "count": len(thread), "thread": thread, "summary": summary})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/email-log/log", methods=["POST"])
@auth_required
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
    <div style="font-size:11px;color:var(--tx2);margin-top:3px">To: {d.get('to','')} &nbsp;·&nbsp; Created: {(d.get('created_at','') or '')[:16].replace('T',' ')}</div>
    <div style="font-size:12px;color:var(--tx2);margin-top:6px;white-space:pre-wrap">{(d.get('body','') or '')[:300]}{'...' if len(d.get('body','') or '') > 300 else ''}</div>
   </div>
   <div style="display:flex;flex-direction:column;gap:6px;min-width:120px">
    <button class="btn btn-sm" onclick="approveDraft('{d.get('id','')}',this)" style="background:var(--gn);color:#000;font-size:11px">✅ Approve</button>
    <button class="btn btn-sm" onclick="deleteDraft('{d.get('id','')}',this)" style="background:var(--sf2);color:var(--rd);font-size:11px">🗑 Delete</button>
    {"<span style='font-size:10px;color:var(--ac);padding:2px 6px;background:rgba(79,140,255,.1);border-radius:4px'>📋 sales draft</span>" if d.get('type') != 'cs_response' else ''}
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
     <span style="font-size:11px;font-weight:600;color:{intent_colors.get(intent,'var(--ac)')};text-transform:uppercase">{intent.replace('_',' ')}</span>
     <span style="font-size:10px;color:var(--tx2)">📬 CS auto-draft</span>
    </div>
    <div style="font-size:13px;font-weight:600;color:var(--tx)">{d.get('subject','')}</div>
    <div style="font-size:11px;color:var(--tx2);margin-top:3px">To: {d.get('to','')} &nbsp;·&nbsp; {(d.get('created_at','') or '')[:16].replace('T',' ')}</div>
    <div style="font-size:12px;color:var(--tx);margin-top:8px;white-space:pre-wrap;padding:8px;background:var(--sf2);border-radius:6px">{(d.get('body','') or '')[:400]}{'...' if len(d.get('body','') or '') > 400 else ''}</div>
   </div>
   <div style="display:flex;flex-direction:column;gap:6px;min-width:120px">
    <button class="btn btn-sm" onclick="approveCS('{d.get('id','')}',this)" style="background:var(--gn);color:#000;font-size:11px">✅ Send Reply</button>
    <button class="btn btn-sm" onclick="deleteCS('{d.get('id','')}',this)" style="background:var(--sf2);color:var(--rd);font-size:11px">🗑 Discard</button>
   </div>
  </div>
</div>"""

    html = _header("Outbox") + f"""
<style>
.btn{{padding:5px 12px;border:1px solid var(--bd);border-radius:6px;cursor:pointer;font-family:'DM Sans',sans-serif;font-weight:500;transition:.15s;text-decoration:none}}
.btn:hover{{opacity:.8}}
.btn-sm{{font-size:12px;padding:4px 10px}}
</style>
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
 <div>
  <h2 style="font-size:22px;font-weight:700">📬 Email Outbox</h2>
  <p style="color:var(--tx2);font-size:13px;margin-top:4px">{total_pending} pending · {len(sent_today)} sent today — Review all drafts before sending</p>
 </div>
 <div style="display:flex;gap:8px">
  <a href="/" class="btn">🏠 Home</a>
  <button class="btn" onclick="sendAllApproved(this)" style="background:rgba(52,211,153,.1);border-color:var(--gn);color:var(--gn)">📤 Send All Approved</button>
 </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:24px">
 <div class="card"><div style="font-size:11px;color:var(--tx2);margin-bottom:4px">PENDING DRAFTS</div><div style="font-size:28px;font-weight:700;color:var(--yl)">{len(sales_drafts)}</div><div style="font-size:11px;color:var(--tx2)">sales outreach</div></div>
 <div class="card"><div style="font-size:11px;color:var(--tx2);margin-bottom:4px">CS REPLY DRAFTS</div><div style="font-size:28px;font-weight:700;color:{'var(--rd)' if cs_drafts else 'var(--gn)'}">{len(cs_drafts)}</div><div style="font-size:11px;color:var(--tx2)">customer service</div></div>
</div>

<h3 style="font-size:14px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">📬 Customer Service Replies ({len(cs_drafts)})</h3>
{'<p style="color:var(--tx2);font-size:13px;padding:20px 0">No CS drafts pending. 👍</p>' if not cs_drafts else cs_html}

<h3 style="font-size:14px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin:24px 0 12px">📋 Sales Drafts ({len(sales_drafts)})</h3>
{'<p style="color:var(--tx2);font-size:13px;padding:20px 0">No sales drafts pending.</p>' if not sales_drafts else sales_html}

<script>
function approveDraft(id,btn){{
  btn.disabled=true;btn.textContent='Sending...';
  fetch('/api/email/approve',{{method:'POST',credentials:'same-origin',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{email_id:id}})}})
  .then(r=>r.json()).then(d=>{{
    if(d.ok){{btn.textContent='✅ Sent!';btn.style.background='var(--gn)';setTimeout(()=>location.reload(),1200)}}
    else{{btn.disabled=false;btn.textContent='❌ Failed: '+d.error}}
  }}).catch(()=>{{btn.disabled=false;btn.textContent='Error'}});
}}
function approveCS(id,btn){{
  btn.disabled=true;btn.textContent='Sending...';
  fetch('/api/email/approve-cs',{{method:'POST',credentials:'same-origin',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{draft_id:id}})}})
  .then(r=>r.json()).then(d=>{{
    if(d.ok){{btn.textContent='✅ Sent!';setTimeout(()=>location.reload(),1200)}}
    else{{btn.disabled=false;btn.textContent='Error: '+(d.error||'unknown')}}
  }}).catch(()=>{{btn.disabled=false;btn.textContent='Error'}});
}}
function deleteDraft(id,btn){{
  if(!confirm('Delete this draft?'))return;
  fetch('/api/email/delete',{{method:'POST',credentials:'same-origin',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{email_id:id}})}})
  .then(()=>btn.closest('.card').remove());
}}
function deleteCS(id,btn){{
  if(!confirm('Discard this CS draft?'))return;
  fetch('/api/email/delete-cs',{{method:'POST',credentials:'same-origin',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{draft_id:id}})}})
  .then(()=>btn.closest('.card').remove());
}}
function sendAllApproved(btn){{
  btn.disabled=true;btn.textContent='⏳ Sending...';
  fetch('/api/email/send-approved',{{method:'POST',credentials:'same-origin'}})
  .then(r=>r.json()).then(d=>{{
    btn.textContent=(d.sent||0)+' sent';setTimeout(()=>location.reload(),1500);
  }}).catch(()=>{{btn.disabled=false;btn.textContent='Error'}});
}}
</script>
</div></body></html>"""
    return html


@bp.route("/api/email/approve-cs", methods=["POST"])
@auth_required
def api_approve_cs_draft():
    """Approve and send a CS reply draft."""
    data = request.get_json(silent=True) or {}
    draft_id = data.get("draft_id","")
    if not draft_id:
        return jsonify({"ok": False, "error": "draft_id required"})
    try:
        outbox_path = os.path.join(DATA_DIR, "email_outbox.json")
        with open(outbox_path) as f:
            outbox = json.load(f)
        
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
        except Exception:
            pass
        
        log.info("CS draft %s sent to %s", draft_id, draft["to"])
        return jsonify({"ok": True, "sent_to": draft["to"]})
    except Exception as e:
        log.error("CS send failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/email/delete-cs", methods=["POST"])
@auth_required
def api_delete_cs_draft():
    """Delete a CS draft."""
    data = request.get_json(silent=True) or {}
    draft_id = data.get("draft_id","")
    try:
        outbox_path = os.path.join(DATA_DIR, "email_outbox.json")
        with open(outbox_path) as f:
            outbox = json.load(f)
        outbox = [e for e in outbox if e.get("id") != draft_id]
        with open(outbox_path, "w") as f:
            json.dump(outbox, f, indent=2, default=str)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})






# ════════════════════════════════════════════════════════════════════════════════
# PRODUCT CATALOG (F31-01)
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/catalog-legacy")
@auth_required
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
  <td style="padding:7px 10px;font-size:12px;font-weight:600;color:var(--ac)">{it.get("sku","")}</td>
  <td style="padding:7px 10px;font-size:12px">{it.get("name","")[:60]}</td>
  <td style="padding:7px 10px;font-size:12px;color:var(--tx2)">{it.get("unit","each")}</td>
  <td style="padding:7px 10px;font-size:12px;color:var(--yl)">${it.get("typical_cost",0):.2f}</td>
  <td style="padding:7px 10px;font-size:12px;color:var(--gn)">${it.get("list_price",0):.2f}</td>
  <td style="padding:7px 10px;font-size:11px;color:var(--tx2)">{vendor}</td>
  <td style="padding:7px 10px;font-size:11px;color:var(--tx2)">{tags}</td>
</tr>"""
        cats_html += f"""<div style="margin-bottom:20px">
  <div style="font-size:12px;font-weight:700;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;padding:6px 10px;background:var(--bg2);border-radius:6px">
    {cat} <span style="color:var(--tx3);font-weight:400">({len(cat_items)} SKUs)</span>
  </div>
  <table style="width:100%;border-collapse:collapse">
    <thead><tr style="border-bottom:2px solid var(--bd)">
      <th style="padding:5px 10px;font-size:11px;color:var(--tx2);text-align:left">SKU</th>
      <th style="padding:5px 10px;font-size:11px;color:var(--tx2);text-align:left">Name</th>
      <th style="padding:5px 10px;font-size:11px;color:var(--tx2);text-align:left">Unit</th>
      <th style="padding:5px 10px;font-size:11px;color:var(--tx2);text-align:left">Cost</th>
      <th style="padding:5px 10px;font-size:11px;color:var(--tx2);text-align:left">List</th>
      <th style="padding:5px 10px;font-size:11px;color:var(--tx2);text-align:left">Vendor</th>
      <th style="padding:5px 10px;font-size:11px;color:var(--tx2);text-align:left">Tags</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""

    html = _header("Catalog") + f"""
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
  <div>
    <h2 style="font-size:22px;font-weight:700">📦 Product Catalog</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:4px">{stats['total_skus']} SKUs · {stats['categories']} categories · {stats['p0_skus_loaded']} P0 gap items loaded</p>
  </div>
  <div style="display:flex;gap:8px">
    <input id="cat-search" type="text" placeholder="Search catalog..." onkeyup="filterCatalog(this.value)"
      style="padding:7px 12px;background:var(--bg2);border:1px solid var(--bd);border-radius:6px;color:var(--tx1);font-size:13px;width:220px">
    <a href="/" style="padding:7px 12px;border:1px solid var(--bd);border-radius:6px;font-size:12px;text-decoration:none">🏠 Home</a>
  </div>
</div>

<div id="catalog-content">{cats_html}</div>

<script>
function filterCatalog(q) {{
  q = q.toLowerCase();
  document.querySelectorAll('#catalog-content tr[style*="border-bottom"]').forEach(row => {{
    row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
  }});
}}
</script>
</div></body></html>"""
    return html


@bp.route("/api/catalog/search")
@auth_required
def api_catalog_search():
    """Search product catalog. ?q=nitrile&limit=10"""
    q = request.args.get("q","").strip()
    limit = int(request.args.get("limit", 10))
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
def api_catalog_stats():
    try:
        from src.core.catalog import init_catalog, get_catalog_stats, get_categories
        init_catalog()
        return jsonify({"ok": True, **get_catalog_stats(), "categories": get_categories()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/items", methods=["GET"])
@auth_required
def api_catalog_items():
    """List all catalog items. ?category=Medical"""
    cat = request.args.get("category")
    limit = int(request.args.get("limit", 200))
    try:
        from src.core.catalog import init_catalog, get_catalog
        init_catalog()
        items = get_catalog(category=cat, limit=limit)
        return jsonify({"ok": True, "count": len(items), "items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/catalog/add", methods=["POST"])
@auth_required
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
    outbox_path = os.path.join(DATA_DIR, "email_outbox.json")
    outbox = _json.load(open(outbox_path)) if os.path.exists(outbox_path) else []
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
    with open(outbox_path, "w") as f:
        _json.dump(outbox, f, indent=2)

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
    except Exception: pass

    try:
        from src.agents.notify_agent import send_alert
        send_alert("bell", f"Outreach draft ready: {agency} — {name}", {
            "type": "outreach_draft", "agency": agency, "email": buyer_email,
            "draft_id": draft_id, "link": "/outbox"
        })
    except Exception: pass

    log.info("Outreach draft created: %s | %s | draft_id=%s", agency, buyer_email, draft_id)
    return jsonify({"ok": True, "draft_id": draft_id, "to": buyer_email, "agency": agency,
                    "subject": subject, "outbox_link": "/outbox"})


@bp.route("/api/cchcs/facilities")
@auth_required
def api_cchcs_facilities():
    """List all CCHCS/CalVet/DSH facilities with activity status."""
    import json as _json
    customers = _json.load(open(os.path.join(DATA_DIR, "customers.json")))
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


@bp.route("/api/cchcs/create-target", methods=["POST"])
@auth_required
def api_cchcs_create_target():
    """
    Create a pre-populated price check targeting a specific facility.
    Includes 6 highest-probability items based on facility type.
    """
    import json as _json
    data = request.get_json() or {}
    facility_name = data.get("facility_name","").strip()
    agency_type = data.get("agency_type","CCHCS")  # CCHCS, CalVet, DSH
    facility_email = data.get("email","")

    # Item sets by agency type
    ITEM_SETS = {
        "CCHCS": [
            {"description":"Nitrile Exam Gloves, Small, Box/100","qty":20,"unit_price":13.99,"sku":"NIT-EXAM-SM"},
            {"description":"Nitrile Exam Gloves, Medium, Box/100","qty":50,"unit_price":12.99,"sku":"NIT-EXAM-MD"},
            {"description":"Nitrile Exam Gloves, Large, Box/100","qty":30,"unit_price":13.99,"sku":"NIT-EXAM-LG"},
            {"description":"Disposable Underpads, 23x36 in, Case/100","qty":10,"unit_price":28.99,"sku":"CHUX-23X36"},
            {"description":"Adult Incontinence Briefs, Medium, Case/80","qty":10,"unit_price":29.99,"sku":"BRIEF-MD"},
            {"description":"Hand Sanitizer, 8oz Pump Bottle, 75% Alcohol","qty":50,"unit_price":6.99,"sku":"SANIT-8OZ"},
        ],
        "CalVet": [
            {"description":"Nitrile Exam Gloves, Medium, Box/100","qty":30,"unit_price":12.99,"sku":"NIT-EXAM-MD"},
            {"description":"Nitrile Exam Gloves, Large, Box/100","qty":20,"unit_price":13.99,"sku":"NIT-EXAM-LG"},
            {"description":"Disposable Underpads, 30x36 in, Case/90","qty":8,"unit_price":36.99,"sku":"CHUX-30X36"},
            {"description":"Adult Incontinence Briefs, Medium, Case/80","qty":10,"unit_price":29.99,"sku":"BRIEF-MD"},
            {"description":"Adult Incontinence Briefs, Large, Case/80","qty":8,"unit_price":31.99,"sku":"BRIEF-LG"},
            {"description":"Hand Sanitizer, 1 Gallon Jug, 70% Alcohol","qty":12,"unit_price":19.99,"sku":"SANIT-GAL"},
        ],
        "DSH": [
            {"description":"Nitrile Exam Gloves, Medium, Box/100","qty":20,"unit_price":12.99,"sku":"NIT-EXAM-MD"},
            {"description":"Stryker Patient Restraint Package, Standard","qty":5,"unit_price":69.99,"sku":"STRYKER-RESTRAINT-STD"},
            {"description":"Hand Sanitizer, 8oz Pump Bottle, 75% Alcohol","qty":30,"unit_price":6.99,"sku":"SANIT-8OZ"},
            {"description":"Sharps Container, 1 Quart, Red Lid","qty":20,"unit_price":4.99,"sku":"SHARPS-1QT"},
            {"description":"Gauze Pads, 4x4 in, Non-Sterile, Box/200","qty":10,"unit_price":10.99,"sku":"GAUZE-4X4"},
            {"description":"Adult Incontinence Briefs, Medium, Case/80","qty":6,"unit_price":29.99,"sku":"BRIEF-MD"},
        ],
    }

    items = ITEM_SETS.get(agency_type, ITEM_SETS["CCHCS"])
    total = sum(it["qty"] * it["unit_price"] for it in items)

    # Create price check record
    pc_id = f"expand-{agency_type.lower()}-{int(__import__('time').time())}"
    pc = {
        "id": pc_id,
        "created_at": __import__('datetime').datetime.now().isoformat(),
        "institution": facility_name,
        "agency": "CDCR" if agency_type == "CCHCS" else agency_type,
        "contact_email": facility_email,
        "items": items,
        "total": round(total, 2),
        "status": "pending",
        "tags": [f"{agency_type.lower()}_expansion"],
        "source": "cchcs_expansion",
        "notes": f"Expansion target: {facility_name} ({agency_type})"
    }

    pcs_path = os.path.join(DATA_DIR, "price_checks.json")
    pcs = _json.load(open(pcs_path)) if os.path.exists(pcs_path) else {}
    pcs[pc_id] = pc
    with open(pcs_path, "w") as f:
        _json.dump(pcs, f, indent=2)

    # Bell notification
    try:
        from src.agents.notify_agent import send_alert
        send_alert("bell", f"Expansion target created: {facility_name}", {
            "type": "expansion_target", "facility": facility_name, "agency": agency_type,
            "pc_id": pc_id, "total": total, "link": f"/price-check/{pc_id}"
        })
    except Exception: pass

    log.info("CCHCS expansion target created: %s | %s | $%.2f", agency_type, facility_name, total)
    return jsonify({
        "ok": True, "pc_id": pc_id, "facility": facility_name, "agency_type": agency_type,
        "items_count": len(items), "total": round(total,2),
        "link": f"/price-check/{pc_id}"
    })


@bp.route("/cchcs/expansion")
@auth_required
def page_cchcs_expansion():
    """CCHCS/CalVet expansion dashboard."""
    import json as _json
    customers = _json.load(open(os.path.join(DATA_DIR, "customers.json")))
    pcs = _json.load(open(os.path.join(DATA_DIR, "price_checks.json"))) if os.path.exists(os.path.join(DATA_DIR, "price_checks.json")) else {}

    # Categorize facilities
    fac_list = []
    for c in customers:
        name = c.get("qb_name","") or c.get("display_name","")
        parent = c.get("parent","")
        balance = float(c.get("open_balance",0) or 0)
        abbr = c.get("abbreviation","")
        email = c.get("email","")
        if "Correctional" in (parent or name) or "State Prison" in name or "Calipatria" in name:
            atype = "CCHCS"
        elif "Veterans" in name or "Dept of Veterans" in name:
            atype = "CalVet"
        elif "State Hospital" in name:
            atype = "DSH"
        else:
            continue
        # Check if has expansion target
        has_target = any(
            atype.lower() in str(pc.get("tags",[])).lower() and facility_name_match(name, pc.get("institution",""))
            for pc in pcs.values()
        )
        fac_list.append({
            "name": name, "abbr": abbr, "email": email, "type": atype,
            "ar": balance, "active": balance > 0, "has_target": has_target
        })

    active = [f for f in fac_list if f["active"]]
    inactive = [f for f in fac_list if not f["active"]]
    expansion_targets = [pc for pc in pcs.values() if pc.get("source") == "cchcs_expansion"]

    def fac_row(f):
        status = "🟢 Active" if f["active"] else ("📋 Targeted" if f["has_target"] else "⚪ Untouched")
        ar = f"${f['ar']:,.2f}" if f["ar"] else "—"
        btn = "" if f["active"] else f"""
<button onclick="createTarget('{f['name'].replace("'","''")}','{f['type']}','{f['email']}')"
  style="padding:3px 10px;font-size:11px;border:1px solid var(--ac);border-radius:4px;color:var(--ac);cursor:pointer;background:transparent">
  + Target
</button>"""
        return f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:8px 10px;font-size:12px">{f['name'][:50]}</td>
  <td style="padding:8px 10px;font-size:11px;color:var(--tx2)">{f['type']}</td>
  <td style="padding:8px 10px;font-size:11px">{f['abbr']}</td>
  <td style="padding:8px 10px;font-size:12px;color:var(--gn)">{ar}</td>
  <td style="padding:8px 10px;font-size:11px;color:var(--tx2)">{status}</td>
  <td style="padding:8px 10px">{btn}</td>
</tr>"""

    rows_html = "".join(fac_row(f) for f in sorted(fac_list, key=lambda x: (-x["ar"], x["type"], x["name"])))

    html = _header("Expand") + f"""
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
  <div>
    <h2 style="font-size:22px;font-weight:700">🏥 Facility Expansion</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:4px">
      {len(active)} active · {len(inactive)} untouched · {len(expansion_targets)} targeted
    </p>
  </div>
  <a href="/intel/market" style="padding:7px 14px;border:1px solid var(--bd);border-radius:6px;font-size:12px;text-decoration:none">📊 Market Intel</a>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:20px">
  <div style="background:var(--bg2);border:1px solid var(--gn);border-radius:10px;padding:16px">
    <div style="font-size:11px;color:var(--tx2)">ACTIVE FACILITIES</div>
    <div style="font-size:28px;font-weight:800;color:var(--gn)">{len(active)}</div>
    <div style="font-size:11px;color:var(--tx2)">${sum(f['ar'] for f in active):,.2f} total AR</div>
  </div>
  <div style="background:var(--bg2);border:1px solid var(--ac);border-radius:10px;padding:16px">
    <div style="font-size:11px;color:var(--tx2)">UNTOUCHED</div>
    <div style="font-size:28px;font-weight:800;color:var(--ac)">{len(inactive)}</div>
    <div style="font-size:11px;color:var(--tx2)">$0 AR — ready to target</div>
  </div>
  <div style="background:var(--bg2);border:1px solid var(--yl);border-radius:10px;padding:16px">
    <div style="font-size:11px;color:var(--tx2)">EXPANSION TARGETS</div>
    <div style="font-size:28px;font-weight:800;color:var(--yl)">{len(expansion_targets)}</div>
    <div style="font-size:11px;color:var(--tx2)">price checks created</div>
  </div>
</div>

<div id="status-msg" style="display:none;padding:10px;background:var(--gn);color:#fff;border-radius:6px;margin-bottom:12px"></div>

<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;overflow:hidden">
  <div style="padding:12px 16px;border-bottom:1px solid var(--bd);font-size:13px;font-weight:600">
    All Facilities ({len(fac_list)} total)
    <span style="font-size:11px;font-weight:400;color:var(--tx2);margin-left:8px">Click "+ Target" to create pre-populated price check</span>
  </div>
  <table style="width:100%;border-collapse:collapse">
    <thead><tr style="border-bottom:2px solid var(--bd)">
      <th style="padding:8px 10px;font-size:11px;color:var(--tx2);text-align:left">Facility</th>
      <th style="padding:8px 10px;font-size:11px;color:var(--tx2);text-align:left">Type</th>
      <th style="padding:8px 10px;font-size:11px;color:var(--tx2);text-align:left">Code</th>
      <th style="padding:8px 10px;font-size:11px;color:var(--tx2);text-align:left">AR Balance</th>
      <th style="padding:8px 10px;font-size:11px;color:var(--tx2);text-align:left">Status</th>
      <th style="padding:8px 10px;font-size:11px;color:var(--tx2);text-align:left">Action</th>
    </tr></thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>

<script>
function createTarget(facilityName, agencyType, email) {{
  const msg = document.getElementById('status-msg');
  msg.style.display = 'block';
  msg.style.background = 'var(--ac)';
  msg.textContent = 'Creating expansion target for ' + facilityName + '...';
  fetch('/api/cchcs/create-target', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json','Authorization': document.cookie.split('=')[1] || ''}},
    body: JSON.stringify({{facility_name: facilityName, agency_type: agencyType, email: email}})
  }})
  .then(r => r.json())
  .then(d => {{
    if (d.ok) {{
      msg.style.background = 'var(--gn)';
      msg.textContent = 'Target created! ' + facilityName + ' — $' + d.total.toFixed(2) + ' | ' + d.items_count + ' items';
      setTimeout(() => location.reload(), 2000);
    }} else {{
      msg.style.background = 'var(--rd)';
      msg.textContent = 'Error: ' + d.error;
    }}
  }}).catch(e => {{ msg.style.background='var(--rd)'; msg.textContent = 'Request failed: ' + e; }});
}}
</script>
</div></body></html>"""
    return html


def facility_name_match(name1, name2):
    """Loose match between facility names."""
    if not name1 or not name2: return False
    n1 = name1.lower().replace(" ","")
    n2 = name2.lower().replace(" ","")
    return n1[:15] in n2 or n2[:15] in n1



# ════════════════════════════════════════════════════════════════════════════════
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
                "type":"vendor_activated","vendor_key":vendor_key,"vendor":vendor_name,"link":"/vendors"
            })
        except Exception: pass
    active_count = sum(1 for v in reg.values() if v.get("status")=="active")
    return jsonify({"ok":True,"vendor_key":vendor_key,"status":status,"active_total":active_count})




@bp.route("/api/intel/scprs/test")
@auth_required
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
                buyer_html = f'<div style="font-size:11px;color:var(--ac);margin-top:4px">📞 {b["name"]} | {b["email"]} | {b["phone"]}</div>'
        elif ag.get("intel_buyers"):
            for b in ag["intel_buyers"][:2]:
                if isinstance(b, dict):
                    buyer_html += f'<div style="font-size:11px;color:var(--ac);margin-top:2px">📞 {b["name"]} | {b["email"]}</div>'
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
            items_html += f'<div style="font-size:11px;padding:3px 0;border-bottom:1px solid var(--bd)"><span style="color:var(--tx1)">{it["item"][:45]}</span> <span style="color:var(--tx2);float:right">${annual:,.0f}/yr → {vendor[:20]}</span></div>'

        return f"""<div class="card" style="border-color:{color};margin-bottom:14px">
  <div style="display:flex;justify-content:space-between;align-items:flex-start">
    <div>
      <div style="font-size:13px;font-weight:700;color:{color}">{ag.get('full_name','')[:50]}</div>
      <div style="font-size:11px;color:var(--tx2);margin-top:2px">{status}</div>
      {buyer_html}
    </div>
    <div style="text-align:right">
      <div style="font-size:20px;font-weight:700;color:var(--gn)">${opp:,.0f}</div>
      <div style="font-size:10px;color:var(--tx2)">12mo opportunity</div>
    </div>
  </div>
  <div style="margin-top:10px;font-size:11px;color:var(--yl);background:rgba(210,167,78,.08);padding:6px 8px;border-radius:4px">
    💡 {ag.get('land_expand_strategy','')[:150]}
  </div>
  {f'<div style="margin-top:8px">{items_html}</div>' if items_html else ""}
</div>"""

    agencies_html = "".join(agency_card(k, v) for k, v in agencies.items())

    # Competitive gap table
    def gap_row(g):
        pri_color = "var(--rd)" if g["priority"]=="P0" else "var(--yl)"
        return f"""<tr style="border-bottom:1px solid var(--bd)">
  <td style="padding:8px 10px;font-weight:500;font-size:12px">{g["item"]}</td>
  <td style="padding:8px 10px;font-size:11px;color:{pri_color}">{g["priority"]}</td>
  <td style="padding:8px 10px;font-size:12px;color:var(--gn);font-weight:600">${g["annual_missed"]:,}</td>
  <td style="padding:8px 10px;font-size:11px;color:var(--tx2)">{g["fix"][:80]}</td>
</tr>"""

    gaps_html = "".join(gap_row(g) for g in sorted(gaps, key=lambda x: (x["priority"], -x["annual_missed"])))

    # Accounts to register
    def account_card(a):
        pri_color = "var(--rd)" if a.get("priority")=="P0" else "var(--yl)"
        return f"""<div style="padding:10px 12px;border-bottom:1px solid var(--bd);display:flex;justify-content:space-between;align-items:flex-start">
  <div>
    <div style="font-size:13px;font-weight:600">{a["vendor"]}</div>
    <div style="font-size:11px;color:var(--tx2);margin-top:2px">{a["why"][:90]}</div>
    <div style="font-size:10px;color:var(--ac);margin-top:3px">{a["url"]}</div>
  </div>
  <span style="font-size:11px;font-weight:700;color:{pri_color};white-space:nowrap;margin-left:12px">{a["priority"]}</span>
</div>"""

    accounts_html = "".join(account_card(a) for a in accounts)

    # Playbook
    def phase_html(phase_key, phase):
        return f"""<div class="card" style="margin-bottom:12px">
  <div style="font-size:13px;font-weight:700;margin-bottom:8px">
    {phase_key.replace('_',' ').title()} — <span style="color:var(--gn)">${phase.get('revenue_target',0):,}</span>
    <span style="font-size:11px;font-weight:400;color:var(--tx2);margin-left:8px">{phase.get('title','')}</span>
  </div>
  {"".join(f'<div style="font-size:12px;padding:3px 0;color:var(--tx2)">▸ {a}</div>' for a in phase.get('actions',[]))}
</div>"""

    playbook_html = "".join(phase_html(k, v) for k, v in playbook.items())

    html = _header("Market Intel") + f"""
<style>
.card{{background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px;margin-bottom:0}}
th{{padding:8px 10px;font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;text-align:left;border-bottom:1px solid var(--bd)}}
table{{width:100%;border-collapse:collapse}}
</style>

<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
  <div>
    <h2 style="font-size:22px;font-weight:700">📊 Land & Expand Intelligence</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:4px">Competitive gaps · Buyer contacts · Revenue model</p>
  </div>
  <div style="display:flex;gap:8px">
    <a href="/" style="padding:5px 12px;border:1px solid var(--bd);border-radius:6px;font-size:12px;text-decoration:none">🏠 Home</a>
    <a href="/vendors" style="padding:5px 12px;border:1px solid var(--bd);border-radius:6px;font-size:12px;text-decoration:none">🏭 Vendors</a>
  </div>
</div>

<!-- Revenue opportunity summary -->
<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:14px;margin-bottom:24px">
  <div class="card">
    <div style="font-size:11px;color:var(--tx2)">12-MONTH OPPORTUNITY</div>
    <div style="font-size:26px;font-weight:800;color:var(--gn)">${total_opp:,.0f}</div>
    <div style="font-size:11px;color:var(--tx2)">across {len(agencies)} agencies</div>
  </div>
  <div class="card">
    <div style="font-size:11px;color:var(--tx2)">P0 PRODUCT GAPS</div>
    <div style="font-size:26px;font-weight:800;color:var(--rd)">{len(p0_gaps)}</div>
    <div style="font-size:11px;color:var(--tx2)">${p0_missed:,.0f}/yr being lost to competitors</div>
  </div>
  <div class="card">
    <div style="font-size:11px;color:var(--tx2)">EXISTING CUSTOMERS</div>
    <div style="font-size:26px;font-weight:800;color:var(--ac)">3</div>
    <div style="font-size:11px;color:var(--tx2)">CCHCS · CalVet · DSH (32+ untapped facilities)</div>
  </div>
  <div class="card">
    <div style="font-size:11px;color:var(--tx2)">ACCOUNTS TO REGISTER</div>
    <div style="font-size:26px;font-weight:800;color:var(--yl)">{len(accounts)}</div>
    <div style="font-size:11px;color:var(--tx2)">Cardinal · McKesson · Bound Tree · Waxie + more</div>
  </div>
</div>

<div style="display:grid;grid-template-columns:1.5fr 1fr;gap:20px">
  <div>
    <!-- Agency intelligence -->
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Agency Intelligence</div>
    {agencies_html}
  </div>
  <div>
    <!-- Accounts to register -->
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">Accounts to Register Now</div>
    <div class="card" style="padding:0;margin-bottom:20px">
      {accounts_html}
    </div>

    <!-- Land & Expand Playbook -->
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:12px">30/60/90 Day Playbook</div>
    {playbook_html}
  </div>
</div>

<!-- Competitive product gap table -->
<div class="card" style="margin-top:20px;padding:0">
  <div style="padding:14px 16px;border-bottom:1px solid var(--bd)">
    <span style="font-size:13px;font-weight:700">🎯 Items Competitors Are Selling To Your Customers</span>
    <span style="font-size:11px;color:var(--tx2);margin-left:8px">{len(gaps)} gaps · ${sum(g.get('annual_missed',0) for g in gaps):,.0f}/yr being captured by others</span>
  </div>
  <table>
    <thead><tr><th>Product / Item</th><th>Priority</th><th>Annual Missed</th><th>Fix</th></tr></thead>
    <tbody>{gaps_html}</tbody>
  </table>
</div>

</div></body></html>"""
    return html


@bp.route("/api/intel/market")
@auth_required
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
        f'<td style="padding:8px 12px;font-size:12px;color:var(--gn)">✅ We can compete</td></tr>'
        for v, s in top_vendors
    )
    opp_rows = "".join(
        f'<tr style="border-bottom:1px solid var(--bd)"><td style="padding:7px 10px;font-size:12px">{o.get("description","")[:55]}</td>'
        f'<td style="padding:7px 10px;font-size:12px;color:var(--tx2)">{o.get("vendor","")[:28]}</td>'
        f'<td style="padding:7px 10px;font-size:12px;font-weight:700;color:var(--rd)">${o.get("amount",0):,.0f}</td>'
        f'<td style="padding:7px 10px;font-size:11px;color:var(--ac)">{o.get("opportunity_match","")}</td></tr>'
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
            f'<div style="font-size:11px;color:var(--tx2)">PO RECORDS INGESTED</div>'
            f'<div style="font-size:32px;font-weight:800;color:var(--ac)">{len(ingested)}</div></div>'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px">'
            f'<div style="font-size:11px;color:var(--tx2)">ITEMS WE CAN COMPETE FOR</div>'
            f'<div style="font-size:32px;font-weight:800;color:var(--rd)">{len(opportunities)}</div></div>'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px">'
            f'<div style="font-size:11px;color:var(--tx2)">COMPETITOR SPEND TO CAPTURE</div>'
            f'<div style="font-size:32px;font-weight:800;color:var(--yl)">${total_opp:,.0f}</div></div></div>'
        )
        stats_html += (
            f'<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px">'
            f'<div><div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Vendors to beat at CCHCS/CDCR</div>'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:0;overflow:hidden">'
            f'<table style="width:100%;border-collapse:collapse"><thead><tr>'
            f'<th style="padding:8px 12px;font-size:11px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Vendor</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Their Spend</th>'
            f'<th style="padding:8px 12px;font-size:11px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Status</th>'
            f'</tr></thead><tbody>{vendor_rows}</tbody></table></div></div>'
            f'<div><div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Items to quote (your products)</div>'
            f'<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:0;overflow:hidden">'
            f'<table style="width:100%;border-collapse:collapse"><thead><tr>'
            f'<th style="padding:7px 10px;font-size:11px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Description</th>'
            f'<th style="padding:7px 10px;font-size:11px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Curr. Vendor</th>'
            f'<th style="padding:7px 10px;font-size:11px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Amount</th>'
            f'<th style="padding:7px 10px;font-size:11px;color:var(--tx2);text-align:left;border-bottom:1px solid var(--bd)">Match</th>'
            f'</tr></thead><tbody>{opp_rows}</tbody></table></div></div></div>'
        )

    paste_box = """<div style="background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px;margin-top:16px">
  <div style="font-size:13px;font-weight:600;margin-bottom:8px">Paste SCPRS CSV here:</div>
  <textarea id="csvPaste" style="width:100%;height:90px;background:var(--bg);border:1px solid var(--bd);border-radius:6px;padding:10px;font-size:12px;color:var(--tx);font-family:monospace;box-sizing:border-box" placeholder="Paste CSV content from SCPRS download..."></textarea>
  <button onclick="ingestCSV()" style="margin-top:8px;padding:8px 20px;background:var(--ac);color:#fff;border:none;border-radius:6px;font-size:13px;cursor:pointer;font-weight:600">📊 Find Gaps</button>
  <span id="ingestStatus" style="margin-left:12px;font-size:12px;color:var(--tx2)"></span>
</div>"""

    return _header("SCPRS Gap Analysis") + f"""
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
  <div>
    <h2 style="font-size:22px;font-weight:700">🔍 SCPRS Gap Analysis</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:4px">What is CCHCS/CDCR buying that Reytech isn't selling them?</p>
  </div>
  <div style="display:flex;gap:8px">
    <a href="https://caleprocure.ca.gov/pages/SCPRSSearch/scprs-search.aspx" target="_blank" style="padding:5px 12px;border:1px solid var(--ac);border-radius:6px;font-size:12px;text-decoration:none;color:var(--ac)">🔎 Open SCPRS</a>
    <a href="/" style="padding:5px 12px;border:1px solid var(--bd);border-radius:6px;font-size:12px;text-decoration:none">🏠 Home</a>
  </div>
</div>
{no_data_html}{stats_html}{paste_box}
<script>
async function ingestCSV() {{
  const csv = document.getElementById("csvPaste").value.trim();
  const st = document.getElementById("ingestStatus");
  if (!csv) {{ alert("Paste SCPRS CSV first"); return; }}
  st.textContent = "Analyzing...";
  try {{
    const r = await fetch("/api/scprs/public/ingest", {{
      method: "POST", credentials: "same-origin",
      headers: {{"Content-Type": "application/json"}},
      body: JSON.stringify({{csv_text: csv}})
    }});
    const d = await r.json();
    if (d.ok) {{
      st.textContent = d.gap_analysis;
      setTimeout(() => location.reload(), 1500);
    }} else {{
      st.textContent = "Error: " + d.error;
    }}
  }} catch(e) {{ st.textContent = "Error: " + e.message; }}
}}
</script>
</div></body></html>"""



# ════════════════════════════════════════════════════════════════════════════════
# QA INTELLIGENCE v2 — Regression tracking, issue history, adaptive patterns
# ════════════════════════════════════════════════════════════════════════════════

@bp.route("/api/qa/intelligence")
@auth_required
def api_qa_intelligence():
    """QA intelligence summary: trends, regressions, persistent issues."""
    try:
        from src.agents.qa_agent import get_qa_intelligence_summary
        return jsonify({"ok": True, **get_qa_intelligence_summary()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/qa/regressions")
@auth_required
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
def api_qa_history_v2():
    """QA run history with scores over time for trend chart."""
    try:
        from src.agents.qa_agent import _qa_db
        import json as _j
        conn = _qa_db()
        limit = int(request.args.get("limit", 30))
        rows = conn.execute(
            "SELECT run_at, score, grade, passed, failed, warned, duration_ms "
            "FROM qa_runs ORDER BY run_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return jsonify({"ok": True, "history": [dict(r) for r in rows], "count": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/qa/intelligence")
@auth_required
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
  <td style="padding:7px 10px;font-size:12px;color:{sev};font-weight:600">{iss["check_name"]}</td>
  <td style="padding:7px 10px;font-size:11px">{iss["message"][:90]}</td>
  <td style="padding:7px 10px;font-size:12px;text-align:center;color:{sev};font-weight:700">{iss["occurrences"]}</td>
  <td style="padding:7px 10px;font-size:11px;color:var(--tx2)">{iss["first_seen"][:10] if iss["first_seen"] else "?"}</td>
</tr>"""

    reg_html = ""
    for reg in regressions:
        reg_html += f"""<div style="background:rgba(220,38,38,.08);border:1px solid var(--rd);border-radius:6px;padding:10px 14px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center">
  <div>
    <span style="font-size:13px;font-weight:700;color:var(--rd)">Score drop: {reg["prev_score"]} → {reg["new_score"]} (-{reg.get("score_drop", reg.get("drop",0))} pts)</span>
    <div style="font-size:11px;color:var(--tx2);margin-top:2px">{reg["detected_at"][:16] if reg["detected_at"] else "?"}</div>
  </div>
  <button onclick="fetch('/api/qa/regressions/{reg["id"]}/ack',{{method:'POST',credentials:'same-origin'}}).then(()=>location.reload())" style="padding:4px 12px;border:1px solid var(--rd);border-radius:4px;background:none;color:var(--rd);font-size:11px;cursor:pointer">Acknowledge</button>
</div>"""

    scores_js = str([r["score"] for r in reversed(history)]) if history else "[]"
    labels_js = str([r["run_at"][:10] for r in reversed(history)]) if history else "[]"

    return _header("QA Intelligence") + f"""
<style>
.card{{background:var(--bg2);border:1px solid var(--bd);border-radius:10px;padding:16px}}
th{{padding:7px 10px;font-size:11px;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;text-align:left;border-bottom:1px solid var(--bd)}}
table{{width:100%;border-collapse:collapse}}
</style>
<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px">
  <div>
    <h2 style="font-size:22px;font-weight:700">🧠 QA Intelligence Engine</h2>
    <p style="color:var(--tx2);font-size:13px;margin-top:4px">Regression detection · Persistent issues · Score trend · Full agent coverage</p>
  </div>
  <div style="display:flex;gap:8px">
    <a href="/api/qa/health" style="padding:5px 12px;border:1px solid var(--bd);border-radius:6px;font-size:12px;text-decoration:none">Run Full Check</a>
    <a href="/" style="padding:5px 12px;border:1px solid var(--bd);border-radius:6px;font-size:12px;text-decoration:none">🏠 Home</a>
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:14px;margin-bottom:24px">
  <div class="card">
    <div style="font-size:11px;color:var(--tx2)">CURRENT SCORE</div>
    <div style="font-size:32px;font-weight:800;color:{score_color}">{score}/100</div>
    <div style="font-size:12px;color:{trend_color};margin-top:4px">{trend_str}</div>
  </div>
  <div class="card">
    <div style="font-size:11px;color:var(--tx2)">OPEN ISSUES</div>
    <div style="font-size:32px;font-weight:800;color:{'var(--rd)' if intel.get('open_issue_count',0)>3 else 'var(--yl)'}">{intel.get('open_issue_count', 0)}</div>
    <div style="font-size:11px;color:var(--tx2);margin-top:4px">Persistent across runs</div>
  </div>
  <div class="card">
    <div style="font-size:11px;color:var(--tx2)">REGRESSIONS</div>
    <div style="font-size:32px;font-weight:800;color:{'var(--rd)' if regressions else 'var(--gn)'}">{len(regressions)}</div>
    <div style="font-size:11px;color:var(--tx2);margin-top:4px">Unacknowledged score drops</div>
  </div>
  <div class="card">
    <div style="font-size:11px;color:var(--tx2)">TOTAL QA RUNS</div>
    <div style="font-size:32px;font-weight:800;color:var(--ac)">{intel.get('total_runs', 0)}</div>
    <div style="font-size:11px;color:var(--tx2);margin-top:4px">Every 5min background checks</div>
  </div>
</div>

{f'<div style="margin-bottom:16px">{reg_html}</div>' if regressions else ''}

<div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">
  <div>
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Persistent Issues (most recurring)</div>
    <div class="card" style="padding:0">
      <table>
        <thead><tr><th>Check</th><th>Message</th><th style="text-align:center">Seen</th><th>Since</th></tr></thead>
        <tbody>{issue_rows if issue_rows else '<tr><td colspan="4" style="padding:16px;text-align:center;color:var(--gn);font-size:13px">✅ No persistent open issues</td></tr>'}</tbody>
      </table>
    </div>
  </div>
  <div>
    <div style="font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">Score History</div>
    <div class="card">
      <canvas id="scoreChart" height="180"></canvas>
    </div>
    <div style="margin-top:16px;font-size:12px;font-weight:600;color:var(--tx2);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px">QA Check Coverage (38 checks, 23 agents)</div>
    <div class="card">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:12px">
        <div style="color:var(--gn)">✅ Routes &amp; auth (257 routes)</div>
        <div style="color:var(--gn)">✅ All 23 agents covered</div>
        <div style="color:var(--gn)">✅ DB schema (10 tables)</div>
        <div style="color:var(--gn)">✅ Data files integrity</div>
        <div style="color:var(--gn)">✅ Critical route coverage</div>
        <div style="color:var(--gn)">✅ Regression detection</div>
        <div style="color:var(--gn)">✅ Issue deduplication</div>
        <div style="color:var(--gn)">✅ Score trend analysis</div>
        <div style="color:var(--gn)">✅ Vendor registration</div>
        <div style="color:var(--gn)">✅ SCPRS credentials</div>
        <div style="color:var(--gn)">✅ Product catalog</div>
        <div style="color:var(--gn)">✅ Market scope</div>
      </div>
    </div>
  </div>
</div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
<script>
const scores = {scores_js};
const labels = {labels_js};
if (scores.length > 0) {{
  new Chart(document.getElementById('scoreChart'), {{
    type: 'line',
    data: {{
      labels,
      datasets: [{{
        label: 'QA Score', data: scores,
        borderColor: '#2563EB', backgroundColor: 'rgba(37,99,235,.1)',
        tension: 0.3, fill: true, pointRadius: 3
      }}]
    }},
    options: {{
      scales: {{ y: {{ min: 60, max: 100, grid: {{ color: 'rgba(255,255,255,.05)' }} }},
                 x: {{ display: false }} }},
      plugins: {{ legend: {{ display: false }} }},
      responsive: true, maintainAspectRatio: false
    }}
  }});
}}
</script>
</div></body></html>"""





# ════════════════════════════════════════════════════════════════════════════════