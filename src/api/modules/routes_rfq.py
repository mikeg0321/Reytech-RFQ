# RFQ + Quote Routes
# 9 routes, 484 lines
# Loaded by dashboard.py via load_module()

# ── Explicit imports (S11 refactor: no longer relying solely on injection) ──
from flask import request, jsonify
from src.api.shared import bp, auth_required
import logging
log = logging.getLogger("reytech")
from src.core.error_handler import safe_route
from flask import redirect, flash
from src.core.paths import DATA_DIR, UPLOAD_DIR, OUTPUT_DIR
from src.core.db import get_db
from src.api.render import render_page
from datetime import datetime, timezone, timedelta


# ═══════════════════════════════════════════════════════════════════════
# Pricing Intelligence — catalog + price history integration
# ═══════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════
# F11: Margin Guardrails — configurable pricing rules
# ═══════════════════════════════════════════════════════════════════════

MARGIN_RULES = {
    "min_margin_pct": 15,       # Warn if margin below this
    "critical_margin_pct": 5,   # Fail if margin below this
    "max_over_scprs_pct": 10,   # Warn if bid > SCPRS by this much
    "max_under_scprs_pct": 15,  # Warn if bid < SCPRS by this much (leaving money)
    "require_cost_source": True, # Warn if cost has no backing URL/SCPRS
}


def _check_guardrails(items):
    """F11: Check margin guardrails on all items. Returns list of warnings."""
    warnings = []
    for i, item in enumerate(items):
        bid = item.get("price_per_unit") or 0
        cost = item.get("supplier_cost") or 0
        scprs = item.get("scprs_last_price") or 0
        desc = (item.get("description", "") or "")[:40]
        if not bid or bid <= 0:
            continue

        # Margin check
        if cost > 0:
            margin = (bid - cost) / bid * 100
            if margin < MARGIN_RULES["critical_margin_pct"]:
                warnings.append({
                    "idx": i, "desc": desc, "level": "critical",
                    "msg": f"Margin {margin:.1f}% is below {MARGIN_RULES['critical_margin_pct']}% minimum"
                })
            elif margin < MARGIN_RULES["min_margin_pct"]:
                warnings.append({
                    "idx": i, "desc": desc, "level": "warn",
                    "msg": f"Margin {margin:.1f}% is below {MARGIN_RULES['min_margin_pct']}% target"
                })

        # SCPRS comparison
        if scprs > 0 and bid > 0:
            diff_pct = (bid - scprs) / scprs * 100
            if diff_pct > MARGIN_RULES["max_over_scprs_pct"]:
                warnings.append({
                    "idx": i, "desc": desc, "level": "warn",
                    "msg": f"Bid is {diff_pct:.0f}% above SCPRS — may lose"
                })

        # Cost without source
        if cost > 0 and MARGIN_RULES["require_cost_source"]:
            if not item.get("item_link") and not scprs:
                warnings.append({
                    "idx": i, "desc": desc, "level": "info",
                    "msg": "Cost has no backing source (no URL or SCPRS)"
                })

    return warnings


def _recommend_price(item):
    """Lightweight price recommendation (mirrors routes_analytics logic).
    Returns {recommended, aggressive, safe} tiers or None."""
    scprs = item.get("scprs_last_price") or 0
    amazon = item.get("amazon_price") or 0
    cost = item.get("supplier_cost") or 0

    base = 0
    reason = ""

    if scprs > 0:
        base = scprs
        reason = f"SCPRS ${scprs:.2f}"
    elif amazon > 0:
        base = amazon * 1.15
        reason = f"Amazon ${amazon:.2f}+15%"
    elif cost > 0:
        base = cost * 1.25
        reason = f"Cost ${cost:.2f}+25%"
    else:
        return None

    # Ensure minimum margin over cost
    if cost > 0 and base < cost * 1.10:
        base = cost * 1.10

    return {
        "recommended": round(base * 0.98, 2),  # Undercut by 2%
        "aggressive": round(base * 0.93, 2),    # Undercut by 7%
        "safe": round(base * 1.05, 2),           # 5% above
        "reason": reason,
    }


def _enrich_items_with_intel(items, rfq_number="", agency=""):
    """Enrich line items with catalog matches and price history.
    Called on RFQ detail load to surface pricing intelligence."""
    for item in items:
        desc = item.get("description", "")
        pn = item.get("item_number", "") or ""
        if not desc and not pn:
            continue

        # 1. Catalog match
        if not item.get("catalog_match"):
            try:
                from src.core.catalog import search_catalog
                matches = search_catalog(pn or desc[:40], limit=1)
                if matches:
                    m = matches[0]
                    item["catalog_match"] = {
                        "sku": m.get("sku", ""),
                        "name": m.get("name", ""),
                        "typical_cost": m.get("typical_cost", 0),
                        "list_price": m.get("list_price", 0),
                        "category": m.get("category", ""),
                    }
            except Exception:
                pass

        # 2. Price history (last 5 observations)
        if not item.get("price_intel"):
            try:
                from src.core.db import get_price_history_db
                history = get_price_history_db(
                    description=desc[:60] if not pn else "",
                    part_number=pn,
                    limit=5
                )
                if history:
                    prices = [h["unit_price"] for h in history if h.get("unit_price")]
                    item["price_intel"] = {
                        "history_count": len(history),
                        "avg_price": round(sum(prices) / len(prices), 2) if prices else 0,
                        "min_price": round(min(prices), 2) if prices else 0,
                        "max_price": round(max(prices), 2) if prices else 0,
                        "last_price": prices[0] if prices else 0,
                        "last_source": history[0].get("source", "") if history else "",
                        "last_date": history[0].get("found_at", "")[:10] if history else "",
                        "last_quote": history[0].get("quote_number", "") if history else "",
                    }
            except Exception:
                pass


def _record_rfq_prices(rfq_data, source="rfq_save"):
    """Record all priced items to price_history + auto-ingest to catalog."""
    sol = rfq_data.get("solicitation_number", "")
    agency = rfq_data.get("agency", "")
    for item in rfq_data.get("line_items", []):
        desc = item.get("description", "")
        pn = item.get("item_number", "") or ""
        if not desc:
            continue

        # Record supplier cost
        cost = item.get("supplier_cost") or 0
        if cost and cost > 0:
            try:
                from src.core.db import record_price
                record_price(
                    description=desc, unit_price=cost, source=source,
                    part_number=pn, agency=agency, quote_number=sol,
                    source_url=item.get("item_link", ""),
                    notes=f"Supplier cost from RFQ {sol}"
                )
            except Exception:
                pass

        # Record bid price
        bid = item.get("price_per_unit") or 0
        if bid and bid > 0:
            try:
                from src.core.db import record_price
                record_price(
                    description=desc, unit_price=bid, source=f"{source}_bid",
                    part_number=pn, agency=agency, quote_number=sol,
                    notes=f"Bid price from RFQ {sol}"
                )
            except Exception:
                pass

        # Record SCPRS price
        scprs = item.get("scprs_last_price") or 0
        if scprs and scprs > 0:
            try:
                from src.core.db import record_price
                record_price(
                    description=desc, unit_price=scprs, source="scprs",
                    part_number=pn, agency=agency, quote_number=sol,
                )
            except Exception:
                pass

        # Record Amazon price
        amz = item.get("amazon_price") or 0
        if amz and amz > 0:
            try:
                from src.core.db import record_price
                record_price(
                    description=desc, unit_price=amz, source="amazon",
                    part_number=pn, source_url=item.get("item_link", ""),
                )
            except Exception:
                pass

        # Auto-ingest to product_catalog (same table PC uses + auto-price reads)
        if cost > 0 or bid > 0:
            try:
                from src.agents.product_catalog import add_to_catalog, init_catalog_db
                init_catalog_db()
                add_to_catalog(
                    description=desc,
                    part_number=pn,
                    cost=float(cost) if cost else 0,
                    sell_price=float(bid) if bid else 0,
                    source=f"rfq_{sol}",
                    supplier_name=item.get("item_supplier", ""),
                    supplier_url=item.get("item_link", ""),
                )
            except Exception:
                pass

@bp.route("/health")
def health_check():
    """Health check endpoint for Railway/load balancers. No auth required."""
    checks = {"status": "ok", "timestamp": datetime.now().isoformat()}
    # Check SQLite
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("SELECT 1")
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"
        checks["status"] = "degraded"
    # Check data dir writable
    try:
        test_path = os.path.join(DATA_DIR, ".health_check")
        with open(test_path, "w") as f: f.write("ok")
        os.remove(test_path)
        checks["disk"] = "ok"
    except Exception as e:
        checks["disk"] = f"error: {e}"
        checks["status"] = "degraded"
    code = 200 if checks["status"] == "ok" else 503
    return jsonify(checks), code

@bp.route("/")
@auth_required
def home():
    import time as _ht
    _t0 = _ht.time()
    log.info("HOME: request started")
    try:
        all_pcs = _load_price_checks()
    except Exception:
        all_pcs = {}
    log.info("HOME: PCs loaded (%d) in %.0fms", len(all_pcs), (_ht.time()-_t0)*1000)
    from src.api.dashboard import _is_user_facing_pc
    user_pcs = {k: v for k, v in all_pcs.items() if _is_user_facing_pc(v)}
    # PST "today" for California-based due date comparisons
    _pst = timezone(timedelta(hours=-8))
    _today = datetime.now(_pst).replace(tzinfo=None)
    # Sort by URGENCY: overdue first, then soonest due date, then newest
    def _pc_sort_key(item):
        pc = item[1]
        due = pc.get("due_date", "") or ""
        status = pc.get("status", "")
        # Terminal statuses go to bottom
        if status in ("won", "lost", "dismissed", "archived", "expired"):
            return (3, "9999-99-99", "")
        # Parse due date and compute urgency
        urgency = 1  # default: normal
        try:
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    d = datetime.strptime(due.strip(), fmt)
                    days_left = (d - _today).days
                    if days_left < 0:
                        urgency = 0  # OVERDUE — top of queue
                    elif days_left <= 2:
                        urgency = 0  # Due within 48h — also top
                    due_sort = d.strftime("%Y-%m-%d")
                    return (urgency, due_sort, pc.get("created_at", ""))
                except ValueError:
                    continue
        except Exception:
            pass
        # No parseable due date — sort by creation
        return (2, "", pc.get("created_at", ""))
    sorted_pcs = dict(sorted(user_pcs.items(), key=_pc_sort_key))
    
    # Also compute urgency metadata for template
    for pid, pc in sorted_pcs.items():
        due = pc.get("due_date", "") or ""
        pc["_days_left"] = None
        pc["_urgency"] = "normal"
        try:
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    d = datetime.strptime(due.strip(), fmt)
                    days = (d - _today).days
                    pc["_days_left"] = days
                    if days < 0: pc["_urgency"] = "overdue"
                    elif days <= 1: pc["_urgency"] = "critical"
                    elif days <= 3: pc["_urgency"] = "soon"
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    # Same for RFQs
    active_rfqs = {k: v for k, v in load_rfqs().items() if v.get("status") not in ("dismissed", "sent")}
    for rid, r in active_rfqs.items():
        due = r.get("due_date", "") or ""
        r["_days_left"] = None
        r["_urgency"] = "normal"
        try:
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    d = datetime.strptime(due.strip(), fmt)
                    days = (d - _today).days
                    r["_days_left"] = days
                    if days < 0: r["_urgency"] = "overdue"
                    elif days <= 1: r["_urgency"] = "critical"
                    elif days <= 3: r["_urgency"] = "soon"
                    break
                except ValueError:
                    continue
        except Exception:
            pass
    # Sort RFQs by urgency too
    active_rfqs = dict(sorted(active_rfqs.items(), key=lambda x: (
        3 if x[1].get("status") in ("sent","generated") else 0 if x[1].get("_urgency") in ("overdue","critical") else 1,
        x[1].get("due_date", "9999"),
    )))
    log.info("HOME: rendering template, %d PCs + %d RFQs, total %.0fms", 
             len(sorted_pcs), len(active_rfqs), (_ht.time()-_t0)*1000)
    return render_page("home.html", active_page="Home", rfqs=active_rfqs, price_checks=sorted_pcs)

@bp.route("/upload", methods=["POST"])
@auth_required
def upload():
    files = request.files.getlist("files")
    if not files:
        flash("No files uploaded", "error"); return redirect("/")
    
    rfq_id = str(uuid.uuid4())[:8]
    rfq_dir = os.path.join(UPLOAD_DIR, rfq_id)
    os.makedirs(rfq_dir, exist_ok=True)
    
    saved = []
    for f in files:
        safe_fn = _safe_filename(f.filename)
        if safe_fn and safe_fn.lower().endswith(".pdf"):
            p = os.path.join(rfq_dir, safe_fn)
            f.save(p); saved.append(p)
    
    if not saved:
        flash("No PDFs found", "error"); return redirect("/")
    
    log.info("Upload: %d PDFs saved to %s", len(saved), rfq_id)
    
    # Check if this is a Price Check (AMS 704) instead of an RFQ
    if PRICE_CHECK_AVAILABLE and len(saved) == 1:
        if _is_price_check(saved[0]):
            return _handle_price_check_upload(saved[0], rfq_id)

    templates = identify_attachments(saved)
    if "704b" not in templates:
        flash("Could not identify 704B", "error"); return redirect("/")
    
    rfq = parse_rfq_attachments(templates)
    rfq["id"] = rfq_id
    rfq["source"] = "upload"
    
    # Auto SCPRS lookup
    rfq["line_items"] = bulk_lookup(rfq.get("line_items", []))
    
    # Store lookup results summary
    items = rfq.get("line_items", [])
    priced_count = sum(1 for i in items if i.get("price_per_unit") or i.get("scprs_last_price"))
    rfq["auto_lookup_results"] = {
        "scprs_found": sum(1 for i in items if i.get("scprs_last_price")),
        "amazon_found": sum(1 for i in items if i.get("amazon_price")),
        "catalog_found": 0,
        "priced": priced_count,
        "total": len(items),
        "ran_at": datetime.now().isoformat(),
    }
    
    # Set status based on whether prices were actually found
    if priced_count > 0:
        _transition_status(rfq, "priced", actor="system", notes=f"Parsed + {priced_count}/{len(items)} items priced")
    else:
        _transition_status(rfq, "draft", actor="system", notes="Parsed from upload — no prices found yet")
    
    rfqs = load_rfqs()
    rfqs[rfq_id] = rfq
    save_rfqs(rfqs)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rfq_id, rfq.get("status", "draft"))
    except Exception:
        pass

    scprs_found = sum(1 for i in rfq["line_items"] if i.get("scprs_last_price"))
    msg = f"RFQ #{rfq['solicitation_number']} parsed — {len(rfq['line_items'])} items"
    if scprs_found:
        msg += f", {scprs_found} SCPRS prices found"
    flash(msg, "success")
    return redirect(f"/rfq/{rfq_id}")


def _is_price_check(pdf_path):
    """Detect if a PDF is an AMS 704 Price Check (NOT 704B quote worksheet).
    
    Uses filename first (fast, reliable), falls back to PDF content parsing.
    """
    basename = os.path.basename(pdf_path).lower()
    
    # ── Filename-based detection (fast path) ──
    # Exclude 704B / 703B / bid package by filename
    if any(x in basename for x in ["704b", "703b", "bid package", "bid_package", "quote worksheet"]):
        return False
    
    # Positive filename match: "AMS 704" or "ams704" in filename (but NOT 704B)
    if "704" in basename and "ams" in basename:
        return True
    # Also match "Quote - [Name] - [Date]" pattern (Valentina's format)
    # These always carry a single AMS 704 attachment
    if basename.startswith("quote") and basename.endswith(".pdf") and "704b" not in basename:
        # Only if filename looks like a price check attachment, not a generated quote
        if any(x in basename for x in ["ams", "704", "price"]):
            return True
    
    # ── PDF content fallback ──
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        text = reader.pages[0].extract_text() or ""
        text_lower = text.lower()
        
        # Exclude 704B forms
        if any(marker in text_lower for marker in ["704b", "quote worksheet", "acquisition quote"]):
            return False
        
        if "price check" in text_lower and ("ams 704" in text_lower or "worksheet" in text_lower):
            return True
        # Check form fields for AMS 704 patterns
        fields = reader.get_fields()
        if fields:
            field_names = set(fields.keys())
            ams704_markers = {"COMPANY NAME", "Requestor", "PRICE PER UNITRow1", "EXTENSIONRow1"}
            if len(ams704_markers & field_names) >= 3:
                return True
    except Exception as e:
        log.debug("PDF parse fallback failed for %s: %s", basename, e)
    return False


# ═══════════════════════════════════════════════════════════════════════
# Status Lifecycle — tracks every transition for PCs and RFQs
# ═══════════════════════════════════════════════════════════════════════

# PC lifecycle: parsed → priced → completed → won/lost/expired
# RFQ lifecycle: new → pending → ready → generated → sent → won/lost
PC_LIFECYCLE = ["parsed", "priced", "completed", "won", "lost", "expired"]
RFQ_LIFECYCLE = ["new", "pending", "ready", "generated", "sent", "won", "lost"]


def _transition_status(record, new_status, actor="system", notes=""):
    """Record a status transition with full history.

    Mutates record in place. Returns the record for chaining.
    """
    old_status = record.get("status", "")

    # Validate transition
    try:
        from src.core.quote_validator import validate_transition
        check = validate_transition(old_status, new_status)
        if not check["ok"]:
            log.warning("BLOCKED transition: %s -> %s (%s)",
                       old_status, new_status, check["error"])
            try:
                from flask import flash as _flash
                _flash(f"Unusual status change: {old_status} -> {new_status}", "warning")
            except Exception:
                pass
    except Exception:
        pass

    record["status"] = new_status
    now = datetime.now().isoformat()
    record["status_updated"] = now

    # Build status_history (create if missing for legacy records)
    history = record.get("status_history", [])
    entry = {"from": old_status, "to": new_status, "timestamp": now, "actor": actor}
    if notes:
        entry["notes"] = notes
    history.append(entry)
    record["status_history"] = history

    # Speed clock tracking
    try:
        from src.core.pricing_oracle_v2 import record_speed_event
        record_id = record.get("id") or record.get("pc_id") or ""
        record_type = "pc" if "pc_data" in record or "pc_number" in record else "quote"
        speed_map = {"parsed": "received", "draft": "received", "new": "received",
                     "priced": "priced", "ready": "priced", "generated": "generated",
                     "sent": "sent", "submitted": "sent"}
        event = speed_map.get(new_status)
        if event and record_id:
            record_speed_event(record_type, record_id, event)
    except Exception:
        pass

    # On win: confirm item mappings + lock costs
    if new_status in ("won", "awarded"):
        try:
            from src.core.pricing_oracle_v2 import confirm_item_mapping, lock_cost
            items = record.get("line_items", record.get("items", []))
            if isinstance(items, str):
                import json as _json
                items = _json.loads(items)
            for item in (items or []):
                desc = item.get("description", "")
                cost = item.get("supplier_cost") or item.get("unit_cost") or item.get("cost")
                sell = item.get("unit_price") or item.get("sell_price") or item.get("price")
                if desc and sell:
                    confirm_item_mapping(
                        original_description=desc, canonical_description=desc,
                        item_number=item.get("item_number", ""),
                        supplier=item.get("item_supplier", ""),
                        cost=float(str(cost or 0).replace("$", "").replace(",", "")) if cost else None,
                    )
                if desc and cost:
                    try:
                        lock_cost(desc, float(str(cost).replace("$", "").replace(",", "")),
                                  supplier=item.get("item_supplier", ""),
                                  source="won_quote", expires_days=60,
                                  item_number=item.get("item_number", ""))
                    except Exception:
                        pass
        except Exception:
            pass

    # Post-send pipeline: schedule follow-ups and tracking
    if new_status in ("sent", "submitted"):
        try:
            from src.agents.post_send_pipeline import on_quote_sent
            record_type = "pc" if "pc_data" in record or "pc_number" in record else "rfq"
            on_quote_sent(record_type,
                         record.get("id", record.get("pc_id", "")),
                         record)
        except Exception as _e:
            log.warning("Post-send pipeline: %s", _e)

    return record


def _handle_price_check_upload(pdf_path, pc_id, from_email=False):
    """Process an uploaded Price Check PDF.
    
    Full pipeline:
    1. Parse PDF → extract header + line items
    2. Dedup check
    3. Catalog matching → pull costs, MFG#, UOM from known products
    4. Save with status 'new' (ready for work in queue)
    5. Return/redirect to PC detail page
    
    Args:
        from_email: If True, returns dict instead of redirect (email pipeline call)
    """
    # Save to data dir for persistence
    pc_file = os.path.join(DATA_DIR, f"pc_upload_{os.path.basename(pdf_path)}")
    shutil.copy2(pdf_path, pc_file)

    # Parse
    parsed = parse_ams704(pc_file)
    parse_error = parsed.get("error")
    now = datetime.now().isoformat()
    source = "email_auto" if from_email else "manual_upload"
    
    if parse_error:
        if from_email:
            # Still create a minimal PC so the email isn't lost
            log.warning("PC parse failed for %s: %s — creating minimal PC with PDF attached",
                        os.path.basename(pdf_path), parse_error)
            pcs = _load_price_checks()
            pcs[pc_id] = {
                "id": pc_id,
                "pc_number": os.path.basename(pdf_path).replace(".pdf", "").replace("pc_upload_", "")[:40],
                "institution": "",
                "due_date": "",
                "requestor": "",
                "ship_to": "",
                "items": [],
                "source_pdf": pc_file,
                "status": "parse_error",
                "status_history": [{"from": "", "to": "parse_error", "timestamp": now, "actor": "system"}],
                "created_at": now,
                "source": source,
                "parsed": {"error": parse_error},
                "parse_error": parse_error,
                "reytech_quote_number": "",
                "linked_quote_number": "",
            }
            _save_price_checks(pcs)
            return {"ok": True, "pc_id": pc_id, "parse_error": parse_error, "items": 0}
        flash(f"Price Check parse error: {parse_error}", "error")
        return redirect("/")

    items = parsed.get("line_items", [])
    header = parsed.get("header", {})
    pc_num = header.get("price_check_number", "unknown")
    institution = header.get("institution", "")
    due_date = header.get("due_date", "")

    # ── DEDUP CHECK: same PC number + institution + due date = true duplicate ──
    pcs = _load_price_checks()
    for existing_id, existing_pc in pcs.items():
        if (existing_pc.get("pc_number", "").strip() == pc_num.strip()
                and existing_pc.get("institution", "").strip().lower() == institution.strip().lower()
                and existing_pc.get("due_date", "").strip() == due_date.strip()
                and pc_num != "unknown"):
            log.info("Dedup: PC #%s from %s (due %s) already exists as %s — skipping",
                     pc_num, institution, due_date, existing_id)
            if from_email:
                return {"dedup": True, "existing_id": existing_id}
            return redirect(f"/pricecheck/{existing_id}")

    # ── CATALOG MATCHING: enrich items with known costs, MFG#, UOM ──
    try:
        from src.agents.product_catalog import match_item as _cat_match, init_catalog_db as _cat_init
        _cat_init()
        for item in items:
            desc = (item.get("description") or "").strip()
            mfg = (item.get("mfg_number") or "").strip()
            if not desc and not mfg:
                continue
            matches = _cat_match(desc, mfg, top_n=1)
            if matches and matches[0].get("match_confidence", 0) >= 0.50:
                best = matches[0]
                # Initialize pricing dict
                pricing = item.get("pricing", {})
                if not pricing:
                    item["pricing"] = pricing
                # Pull catalog data into the item
                if best.get("cost") and not pricing.get("unit_cost"):
                    pricing["unit_cost"] = best["cost"]
                    pricing["price_source"] = "catalog"
                if best.get("sell_price") and not pricing.get("recommended_price"):
                    pricing["recommended_price"] = best["sell_price"]
                if best.get("mfg_number") and not item.get("mfg_number"):
                    item["mfg_number"] = best["mfg_number"]
                if best.get("uom"):
                    item["uom"] = best["uom"]
                if best.get("manufacturer"):
                    pricing["manufacturer"] = best["manufacturer"]
                pricing["catalog_match"] = best.get("name", "")[:50]
                pricing["catalog_id"] = best.get("id")
                pricing["catalog_confidence"] = best.get("match_confidence", 0)
                log.info("  catalog match for '%s': %s (%.0f%%) cost=$%.2f",
                         desc[:30], best.get("name", "")[:30],
                         best.get("match_confidence", 0) * 100,
                         best.get("cost", 0))
    except Exception as e:
        log.debug("Catalog matching on upload failed (non-fatal): %s", e)

    # ── Save PC Record ──
    pcs = _load_price_checks()
    pcs[pc_id] = {
        "id": pc_id,
        "pc_number": pc_num,
        "institution": institution,
        "due_date": due_date,
        "requestor": header.get("requestor", ""),
        "ship_to": parsed.get("ship_to", ""),
        "phone": header.get("phone", ""),
        "agency": institution,
        "items": items,
        "source_pdf": pc_file,
        "status": "new",
        "status_history": [
            {"from": "", "to": "parsed", "timestamp": now, "actor": "system", "notes": f"Parsed {len(items)} items"},
            {"from": "parsed", "to": "new", "timestamp": now, "actor": "system", "notes": f"Source: {source}"},
        ],
        "created_at": now,
        "source": source,
        "parsed": parsed,
        "reytech_quote_number": "",
        "linked_quote_number": "",
    }
    _save_price_checks(pcs)

    log.info("PC #%s created (%s) — %d items from %s, due %s, status=new",
             pc_num, source, len(items), institution, due_date)
    
    if from_email:
        return {"ok": True, "pc_id": pc_id, "pc_number": pc_num, "items": len(items)}
    
    flash(f"Price Check #{pc_num} — {len(items)} items from {institution}. Due {due_date}", "success")
    return redirect(f"/pricecheck/{pc_id}")


def _load_price_checks():
    """Delegate to dashboard's DB-primary implementation."""
    from src.api.dashboard import _load_price_checks as _db_load
    return _db_load()


def _save_price_checks(pcs):
    """Delegate to dashboard's DB-primary implementation."""
    from src.api.dashboard import _save_price_checks as _db_save
    _db_save(pcs)


@bp.route("/rfq/<rid>")
@auth_required
@safe_route
def detail(rid):
    # WARNING: GET handler — must NEVER call save_rfqs() or modify data.
    # Data loss incident 2026-03-16: save_rfqs in GET handler corrupted items.
    # Check if this is actually a price check
    pcs = _load_price_checks()
    if rid in pcs:
        return redirect(f"/pricecheck/{rid}")
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: flash("Not found", "error"); return redirect("/")

    # Ensure r is a plain dict (not a Jinja2-aware object)
    if not isinstance(r, dict):
        r = dict(r) if hasattr(r, 'items') else {}

    # ── Restore template paths from DB if files missing from disk (post-redeploy) ──
    tmpl = r.get("templates", {})
    db_files = list_rfq_files(rid, category="template")
    restored = False
    for db_f in db_files:
        ft = db_f.get("file_type", "").lower().replace("template_", "")
        fname = db_f.get("filename", "").lower()
        ttype = None
        if "703b" in ft or "703b" in fname:
            ttype = "703b"
        elif "704b" in ft or "704b" in fname:
            ttype = "704b"
        elif "bid" in ft or "bid" in fname:
            ttype = "bidpkg"
        if ttype and (ttype not in tmpl or not os.path.exists(tmpl.get(ttype, ""))):
            full_f = get_rfq_file(db_f["id"])
            if full_f and full_f.get("data"):
                restore_dir = os.path.join(DATA_DIR, "rfq_templates", rid)
                os.makedirs(restore_dir, exist_ok=True)
                restore_path = os.path.join(restore_dir, db_f["filename"])
                with open(restore_path, "wb") as _fw:
                    _fw.write(full_f["data"])
                tmpl[ttype] = restore_path
                restored = True
    if restored:
        r["templates"] = tmpl
        rfqs[rid] = r
        r["_needs_save"] = True  # Deferred to POST /rfq/{rid}/save-restore
    
    # ── Restore output_files from DB if empty (post-redeploy) ──
    if not r.get("output_files") and r.get("status") in ("generated", "sent", "won", "lost"):
        db_gen_files = list_rfq_files(rid, category="generated")
        if db_gen_files:
            r["output_files"] = [f["filename"] for f in db_gen_files]
            # Also restore files to disk for download
            for db_f in db_gen_files:
                fname = db_f.get("filename", "")
                sol = r.get("solicitation_number", rid)
                restore_dir = os.path.join(OUTPUT_DIR, sol)
                restore_path = os.path.join(restore_dir, fname)
                if not os.path.exists(restore_path):
                    full_f = get_rfq_file(db_f["id"])
                    if full_f and full_f.get("data"):
                        os.makedirs(restore_dir, exist_ok=True)
                        with open(restore_path, "wb") as _fw:
                            _fw.write(full_f["data"])
            rfqs[rid] = r
            r["_needs_save"] = True  # Deferred to POST /rfq/{rid}/save-restore

    # ── Enrichment DISABLED — was crashing page with Undefined serialization ──
    # TODO: re-enable after fixing stored Undefined values in rfqs.json
    pass  # enrichment disabled

    # Map items → line_items (SQLite column is "items", template expects "line_items")
    # Also handle: items might be a JSON string, a list, or missing
    if "line_items" not in r or not r["line_items"]:
        items_data = r.get("items", [])
        if isinstance(items_data, str):
            try:
                import json as _json
                items_data = _json.loads(items_data)
            except Exception:
                items_data = []
        if isinstance(items_data, list) and items_data:
            r["line_items"] = items_data

    if not isinstance(r.get("line_items"), list):
        r["line_items"] = []

    # Also map solicitation_number from rfq_number (SQLite vs JSON field names)
    if not r.get("solicitation_number") and r.get("rfq_number"):
        r["solicitation_number"] = r["rfq_number"]

    # Show link suggestion if unlinked (read-only — no save_rfqs here)
    if not r.get("linked_pc_id"):
        try:
            from src.core.pc_rfq_linker import find_matching_pc
            from src.api.dashboard import _load_price_checks
            pcs = _load_price_checks()
            pc_id, pc_data, reason = find_matching_pc(r, pcs)
            if pc_id:
                r["_suggested_pc"] = pc_id
                r["_suggested_pc_reason"] = reason
                pc_inner = pc_data.get("pc_data", pc_data)
                if isinstance(pc_inner, str):
                    try:
                        import json as _json
                        pc_inner = _json.loads(pc_inner)
                    except Exception:
                        pc_inner = {}
                r["_suggested_pc_number"] = pc_inner.get("pc_number", pc_data.get("pc_number", ""))
                r["_suggested_pc_items"] = len(pc_inner.get("items", pc_data.get("items", [])))
        except Exception:
            pass

    log.info("RFQ detail render: rid=%s, line_items=%d, has_items_key=%s",
             rid, len(r.get("line_items", [])), "items" in r)

    return render_page("rfq_detail.html", active_page="Home", r=r, rid=rid)


@bp.route("/rfq/<rid>/save-restore", methods=["POST"])
@auth_required
@safe_route
def rfq_save_restore(rid):
    """Save template/file restorations. Called via POST from rfq_detail.html, not GET."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False})
    if r.pop("_needs_save", False):
        save_rfqs(rfqs)
        return jsonify({"ok": True, "saved": True})
    return jsonify({"ok": True, "saved": False})


@bp.route("/rfq/<rid>/update", methods=["POST"])
@auth_required
@safe_route
def update(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    
    for i, item in enumerate(r["line_items"]):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit"), ("markup", "markup_pct")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try: item[key] = float(v)
                except Exception as e:

                    log.debug("Suppressed: %s", e)
        # Save qty and uom from separate inputs
        qty_val = request.form.get(f"qty_{i}")
        if qty_val:
            try: item["qty"] = int(float(qty_val))
            except Exception: pass
        uom_val = request.form.get(f"uom_{i}")
        if uom_val is not None:
            item["uom"] = uom_val.strip().upper()
        # Save edited description
        desc_val = request.form.get(f"desc_{i}")
        if desc_val is not None:
            item["description"] = desc_val
        # Save part number
        part_val = request.form.get(f"part_{i}")
        if part_val is not None:
            item["item_number"] = part_val.strip()
        # Save item link and auto-detect supplier
        link_val = request.form.get(f"link_{i}", "").strip()
        item["item_link"] = link_val
        if link_val:
            try:
                from src.agents.item_link_lookup import detect_supplier
                item["item_supplier"] = detect_supplier(link_val)
            except Exception as _e:
                log.debug("Suppressed: %s", _e)
    
    _transition_status(r, "ready", actor="user", notes="Pricing updated")
    save_rfqs(rfqs)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "ready")
    except Exception:
        pass

    # Save SCPRS prices for future lookups
    save_prices_from_rfq(r)
    
    # Record ALL prices to history + auto-ingest to catalog
    try:
        _record_rfq_prices(r, source="rfq_finalize")
    except Exception as _e:
        log.debug("Price recording: %s", _e)
    
    # Sync all priced items to product catalog
    cat_added, cat_updated = 0, 0
    try:
        from src.agents.product_catalog import match_item, add_to_catalog, add_supplier_price, init_catalog_db
        init_catalog_db()
        for item in r.get("line_items", []):
            desc = item.get("description", "")
            pn = item.get("item_number", "") or ""
            cost = item.get("supplier_cost") or 0
            bid = item.get("price_per_unit") or 0
            supplier = item.get("item_supplier", "")
            uom = item.get("uom", "EA")
            url = item.get("item_link", "")
            if not desc or (not cost and not bid):
                continue
            cat_matches = match_item(desc, pn, top_n=1)
            if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                pid = cat_matches[0]["id"]
                if cost > 0 and supplier:
                    add_supplier_price(pid, supplier, cost, url=url)
                if url:
                    try:
                        from src.agents.product_catalog import _get_conn
                        conn = _get_conn()
                        conn.execute(
                            "UPDATE product_catalog SET photo_url=COALESCE(NULLIF(photo_url,''),?) WHERE id=?",
                            (url, pid))
                        conn.commit(); conn.close()
                    except Exception:
                        pass
                cat_updated += 1
            else:
                pid = add_to_catalog(
                    description=desc, part_number=pn,
                    cost=cost if cost > 0 else 0,
                    sell_price=bid if bid > 0 else 0,
                    supplier_name=supplier, uom=uom,
                    supplier_url=url,
                    source=f"rfq_finalize_{r.get('solicitation_number', '')}",
                )
                if pid and cost > 0 and supplier:
                    add_supplier_price(pid, supplier, cost, url=url)
                    cat_added += 1
        if cat_added or cat_updated:
            log.info("Finalize catalog sync: +%d new, ~%d updated", cat_added, cat_updated)
    except Exception as _ce:
        log.debug("Finalize catalog sync: %s", _ce)
    
    # Auto-learn item mappings + lock costs from user pricing
    try:
        from src.core.pricing_oracle_v2 import auto_learn_mapping, lock_cost
        for _item in r.get("line_items", []):
            _desc = _item.get("description", "")
            _cost = _item.get("supplier_cost") or _item.get("unit_price")
            if _desc and _cost:
                try:
                    _cv = float(str(_cost).replace("$", "").replace(",", ""))
                except (ValueError, TypeError):
                    _cv = 0
                if _cv > 0:
                    auto_learn_mapping(_desc, _item.get("catalog_match", {}).get("name", _desc),
                                       item_number=_item.get("item_number", ""), confidence=0.7)
                    lock_cost(_desc, _cv, supplier=_item.get("item_supplier", ""),
                              source="user_pricing", expires_days=30,
                              item_number=_item.get("item_number", ""))
    except Exception:
        pass

    _log_rfq_activity(rid, "pricing_finalized",
        f"Pricing finalized for #{r.get('solicitation_number','?')} ({len(r.get('line_items',[]))} items, catalog +{cat_added}/~{cat_updated})",
        actor="user")
    
    flash("Pricing finalized — saved to catalog", "success")
    return redirect(f"/rfq/{rid}")


@bp.route("/api/rfq/<rid>/autosave", methods=["POST"])
@auth_required
def api_rfq_autosave(rid):
    """AJAX auto-save: persist line item edits without page reload."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "not found"}), 404

    data = request.get_json(force=True, silent=True) or {}
    items_data = data.get("items", [])

    for update in items_data:
        idx = update.get("idx")
        if idx is None or idx >= len(r["line_items"]):
            continue
        item = r["line_items"][idx]
        if "supplier_cost" in update and update["supplier_cost"] is not None:
            try: item["supplier_cost"] = float(update["supplier_cost"])
            except Exception: pass
        if "price_per_unit" in update and update["price_per_unit"] is not None:
            try: item["price_per_unit"] = float(update["price_per_unit"])
            except Exception: pass
        if "qty" in update and update["qty"] is not None:
            try: item["qty"] = int(float(update["qty"]))
            except Exception: pass
        if "uom" in update:
            item["uom"] = str(update["uom"]).strip().upper()
        if "description" in update:
            item["description"] = str(update["description"])
        if "item_number" in update:
            item["item_number"] = str(update["item_number"]).strip()
        if "item_link" in update:
            item["item_link"] = str(update["item_link"]).strip()
            if item["item_link"]:
                try:
                    from src.agents.item_link_lookup import detect_supplier
                    item["item_supplier"] = detect_supplier(item["item_link"])
                except Exception:
                    pass
        if "scprs_last_price" in update and update["scprs_last_price"] is not None:
            try: item["scprs_last_price"] = float(update["scprs_last_price"])
            except Exception: pass
        if "markup_pct" in update and update["markup_pct"] is not None:
            try: item["markup_pct"] = float(update["markup_pct"])
            except Exception: pass

    save_rfqs(rfqs)

    # F11: Check guardrails on saved data
    guardrail_warnings = _check_guardrails(r.get("line_items", []))

    # F6: Record price audits for changed items
    try:
        from src.core.db import record_audit
        sol = r.get("solicitation_number", "")
        for update in items_data:
            idx = update.get("idx")
            if idx is None or idx >= len(r["line_items"]):
                continue
            item = r["line_items"][idx]
            desc = (item.get("description", "") or "")[:60]
            # Record cost changes
            if "supplier_cost" in update and update["supplier_cost"]:
                record_audit(
                    item_description=desc, field_changed="supplier_cost",
                    old_value=0, new_value=float(update["supplier_cost"]),
                    source="manual_edit", rfq_id=rid, actor="user"
                )
            # Record bid changes
            if "price_per_unit" in update and update["price_per_unit"]:
                record_audit(
                    item_description=desc, field_changed="price_per_unit",
                    old_value=0, new_value=float(update["price_per_unit"]),
                    source="manual_edit", rfq_id=rid, actor="user"
                )
    except Exception:
        pass

    # Write priced items to catalog (same as full save does)
    try:
        sol = r.get("solicitation_number", "")
        for update in items_data:
            idx = update.get("idx")
            if idx is None or idx >= len(r["line_items"]):
                continue
            item = r["line_items"][idx]
            cost = item.get("supplier_cost") or 0
            bid = item.get("price_per_unit") or 0
            desc = item.get("description", "")
            if desc and (cost > 0 or bid > 0):
                from src.agents.product_catalog import add_to_catalog, init_catalog_db
                init_catalog_db()
                add_to_catalog(
                    description=desc,
                    part_number=item.get("item_number", ""),
                    cost=float(cost) if cost else 0,
                    sell_price=float(bid) if bid else 0,
                    source=f"rfq_autosave_{sol}",
                    supplier_name=item.get("item_supplier", ""),
                    supplier_url=item.get("item_link", ""),
                )
    except Exception:
        pass

    return jsonify({
        "ok": True, "saved": len(items_data),
        "guardrails": guardrail_warnings if guardrail_warnings else None,
    })


@bp.route("/rfq/<rid>/add-item", methods=["POST"])
@auth_required
@safe_route
def rfq_add_item(rid):
    """Add a line item to an RFQ (for generic/Cal Vet RFQs or manual entry)."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")

    if "line_items" not in r:
        r["line_items"] = []

    next_num = len(r["line_items"]) + 1

    new_item = {
        "line_number": next_num,
        "qty": int(request.form.get("qty", 1)),
        "uom": request.form.get("uom", "EA").strip().upper(),
        "description": request.form.get("description", "").strip(),
        "item_number": request.form.get("item_number", "").strip(),
        "supplier_cost": 0,
        "scprs_last_price": None,
        "source_type": "manual",
        "price_per_unit": 0,
    }

    r["line_items"].append(new_item)
    save_rfqs(rfqs)
    _log_rfq_activity(rid, "item_added",
        f"Line item #{next_num} added: {new_item['description'][:60]}",
        actor="user")
    flash(f"Item #{next_num} added", "success")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/remove-item/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def rfq_remove_item(rid, idx):
    """Remove a line item from an RFQ by index."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return _item_response(rid, False, "RFQ not found")

    items = r.get("line_items", [])
    if 0 <= idx < len(items):
        removed = items.pop(idx)
        _renumber_items(items)
        save_rfqs(rfqs)
        _log_rfq_activity(rid, "item_removed",
            f"Line item removed: {removed.get('description','')[:60]}",
            actor="user")
        return _item_response(rid, True, "Item removed")
    return _item_response(rid, False, "Invalid item index")


@bp.route("/rfq/<rid>/duplicate-item/<int:idx>", methods=["POST"])
@auth_required
@safe_route
def rfq_duplicate_item(rid, idx):
    """Duplicate a line item (insert copy right after the original)."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return _item_response(rid, False, "RFQ not found")

    items = r.get("line_items", [])
    if 0 <= idx < len(items):
        import copy
        dupe = copy.deepcopy(items[idx])
        dupe.pop("_catalog_product_id", None)
        items.insert(idx + 1, dupe)
        _renumber_items(items)
        save_rfqs(rfqs)
        return _item_response(rid, True, f"Item duplicated at #{idx + 2}")
    return _item_response(rid, False, "Invalid item index")


@bp.route("/rfq/<rid>/move-item/<int:idx>/<direction>", methods=["POST"])
@auth_required
@safe_route
def rfq_move_item(rid, idx, direction):
    """Move a line item up or down."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return _item_response(rid, False, "RFQ not found")

    items = r.get("line_items", [])
    if direction == "up" and idx > 0:
        items[idx], items[idx - 1] = items[idx - 1], items[idx]
    elif direction == "down" and idx < len(items) - 1:
        items[idx], items[idx + 1] = items[idx + 1], items[idx]
    else:
        return _item_response(rid, False, "Cannot move")

    _renumber_items(items)
    save_rfqs(rfqs)
    return _item_response(rid, True, f"Item moved {direction}")


@bp.route("/rfq/<rid>/reset-items", methods=["POST"])
@auth_required
@safe_route
def rfq_reset_items(rid):
    """Clear all line items from an RFQ so it can be re-imported."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return _item_response(rid, False, "RFQ not found")

    old_count = len(r.get("line_items", []))
    r["line_items"] = []
    r.pop("linked_pc_id", None)
    r.pop("linked_pc_number", None)
    r.pop("linked_pc_match_reason", None)
    r.pop("uploaded_pc_pdf", None)
    save_rfqs(rfqs)
    _log_rfq_activity(rid, "items_reset",
        f"All {old_count} line items cleared for re-import",
        actor="user")
    return _item_response(rid, True, f"Cleared {old_count} items")


@bp.route("/rfq/<rid>/lookup-item/<int:idx>", methods=["POST"])
@auth_required
def rfq_lookup_single_item(rid, idx):
    """Run SCPRS + Catalog + Amazon lookup on a single line item by index."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    items = r.get("line_items", [])
    if idx < 0 or idx >= len(items):
        return jsonify({"ok": False, "error": "Invalid item index"})

    item = items[idx]
    desc = item.get("description", "")
    pn = item.get("item_number", "")
    results = {"scprs": None, "catalog": None, "amazon": None}

    # 1. SCPRS lookup
    try:
        from src.agents.scprs_lookup import bulk_lookup
        wrapped = bulk_lookup([item])
        if wrapped and wrapped[0].get("scprs_last_price"):
            items[idx] = wrapped[0]
            results["scprs"] = {
                "price": wrapped[0]["scprs_last_price"],
                "vendor": wrapped[0].get("scprs_vendor", ""),
                "source": wrapped[0].get("scprs_source", ""),
            }
    except Exception as e:
        results["scprs"] = {"error": str(e)[:80]}
        log.debug("Single item SCPRS error: %s", e)

    # 2. Catalog lookup
    try:
        from src.agents.product_catalog import match_item, init_catalog_db
        init_catalog_db()
        matches = match_item(desc, pn, top_n=3)
        if matches and matches[0].get("match_confidence", 0) >= 0.3:
            best = matches[0]
            items[idx]["catalog_match"] = best
            cost = best.get("best_cost") or best.get("cost") or 0
            results["catalog"] = {
                "name": best.get("name", "")[:60],
                "cost": cost,
                "supplier": best.get("best_supplier", ""),
                "confidence": round(best.get("match_confidence", 0), 2),
                "sell_price": best.get("sell_price") or best.get("recommended_price") or 0,
            }
            # Fill cost if empty
            if cost and not items[idx].get("supplier_cost"):
                items[idx]["supplier_cost"] = round(float(cost), 2)
                items[idx]["item_supplier"] = best.get("best_supplier", "")
    except Exception as e:
        results["catalog"] = {"error": str(e)[:80]}
        log.debug("Single item catalog error: %s", e)

    # 3. Amazon/web lookup
    try:
        from src.agents.web_price_research import research_items
        wrapped = research_items([items[idx]])
        if wrapped and wrapped[0].get("amazon_price"):
            items[idx] = wrapped[0]
            results["amazon"] = {
                "price": wrapped[0].get("amazon_price"),
                "url": wrapped[0].get("item_link", ""),
                "source": wrapped[0].get("item_supplier", ""),
            }
    except Exception as e:
        results["amazon"] = {"error": str(e)[:80]}
        log.debug("Single item Amazon error: %s", e)

    # 4. Unified Pricing Oracle
    try:
        from src.core.pricing_oracle_v2 import get_pricing
        oracle = get_pricing(
            description=desc,
            quantity=item.get("quantity", item.get("qty", 1)),
            cost=item.get("supplier_cost") or item.get("unit_price"),
            item_number=pn,
        )
        items[idx]["oracle"] = oracle
        results["oracle"] = {
            "quote_price": oracle.get("recommendation", {}).get("quote_price"),
            "confidence": oracle.get("recommendation", {}).get("confidence"),
            "market_avg": oracle.get("market", {}).get("weighted_avg"),
            "competitors": len(oracle.get("competitors", [])),
            "cross_sell": len(oracle.get("cross_sell", [])),
            "sources": oracle.get("sources_used", []),
        }
    except Exception as e:
        results["oracle"] = {"error": str(e)[:80]}

    save_rfqs(rfqs)

    # Build summary
    found = []
    if results["scprs"] and not results["scprs"].get("error"):
        found.append(f"SCPRS: ${results['scprs']['price']:.2f}")
    if results["catalog"] and not results["catalog"].get("error") and results["catalog"].get("cost"):
        found.append(f"Catalog: ${results['catalog']['cost']:.2f} ({results['catalog']['supplier']})")
    if results["amazon"] and not results["amazon"].get("error"):
        found.append(f"Amazon: ${results['amazon']['price']:.2f}")
    if results.get("oracle") and not results["oracle"].get("error") and results["oracle"].get("quote_price"):
        found.append(f"Oracle: ${results['oracle']['quote_price']:.2f} ({results['oracle'].get('confidence','?')})")

    return jsonify({
        "ok": True,
        "idx": idx,
        "description": desc[:60],
        "results": results,
        "summary": " | ".join(found) if found else "No prices found",
    })


@bp.route("/rfq/<rid>/upload-supplier-quote", methods=["POST"])
@auth_required
def rfq_upload_supplier_quote(rid):
    """Upload a supplier quote PDF → parse → match to RFQ items → fill costs + update catalog."""
    import os
    from src.core.paths import DATA_DIR

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    f = request.files.get("file")
    if not f or not f.filename.lower().endswith(".pdf"):
        return jsonify({"ok": False, "error": "Upload a PDF file"})

    # Save uploaded file
    upload_dir = os.path.join(DATA_DIR, "uploads", "supplier_quotes")
    os.makedirs(upload_dir, exist_ok=True)
    pdf_path = os.path.join(upload_dir, f"sq_{rid}_{f.filename}")
    f.save(pdf_path)

    # Parse the quote
    try:
        from src.forms.supplier_quote_parser import parse_supplier_quote, match_quote_to_rfq
    except ImportError as e:
        return jsonify({"ok": False, "error": f"Parser not available: {e}"})

    parsed = parse_supplier_quote(pdf_path)
    if not parsed.get("ok"):
        return jsonify({"ok": False, "error": parsed.get("error", "Parse failed"),
                        "raw_text": parsed.get("raw_text", "")[:500]})

    quote_items = parsed.get("items", [])
    if not quote_items:
        return jsonify({"ok": False, "error": "No priced items found in PDF",
                        "raw_text": parsed.get("raw_text", "")[:500]})

    supplier = parsed.get("supplier", "Unknown")
    quote_num = parsed.get("quote_number", "")

    # Match to RFQ line items
    rfq_items = r.get("line_items", [])
    
    # ── If RFQ has NO items, create them from supplier quote directly ──
    if not rfq_items and quote_items:
        log.info("RFQ has 0 items — creating %d items from supplier quote", len(quote_items))
        for qi, q in enumerate(quote_items):
            new_item = {
                "line_number": qi + 1,
                "qty": q.get("qty", 1),
                "uom": (q.get("uom") or "EA").upper(),
                "description": q.get("description", ""),
                "item_number": q.get("item_number", ""),
                "supplier_cost": round(q.get("unit_price", 0), 2),
                "item_supplier": supplier,
                "price_per_unit": 0,
                "scprs_last_price": 0,
                "amazon_price": 0,
                "_desc_source": "supplier",
            }
            rfq_items.append(new_item)
        r["line_items"] = rfq_items
        applied = len(quote_items)
        desc_upgraded = len(quote_items)
        unmatched = []
        catalog_added = 0
        catalog_updated = 0
        
        # Catalog sync for all items
        try:
            from src.agents.product_catalog import match_item, add_to_catalog, add_supplier_price, init_catalog_db
            init_catalog_db()
            for q in quote_items:
                _desc = q.get("description", "")
                _pn = q.get("item_number", "")
                _cost = q.get("unit_price", 0)
                if not _desc or _cost <= 0:
                    continue
                cat_matches = match_item(_desc, _pn, top_n=1)
                if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                    pid = cat_matches[0]["id"]
                    add_supplier_price(pid, supplier, _cost)
                    catalog_updated += 1
                else:
                    pid = add_to_catalog(description=_desc, part_number=_pn, cost=_cost,
                                         supplier_name=supplier, uom=q.get("uom", "EA"),
                                         source=f"supplier_quote_{quote_num or 'upload'}")
                    if pid:
                        add_supplier_price(pid, supplier, _cost)
                        catalog_added += 1
        except Exception as _ce:
            log.debug("Catalog sync (new items): %s", _ce)
        
        # Skip the matching loop — go straight to save
        r["_last_supplier_quote"] = {
            "supplier": supplier, "quote_number": quote_num, "pdf": pdf_path,
            "items_parsed": len(quote_items), "items_matched": applied,
            "uploaded_at": __import__("datetime").datetime.now().isoformat(),
        }
        save_rfqs(rfqs)
        
        try:
            from src.api.dashboard import save_rfq_file
            with open(pdf_path, "rb") as _qf:
                pdf_data = _qf.read()
            save_rfq_file(rid, f.filename, "application/pdf", pdf_data,
                         category="supplier_quote", uploaded_by="user")
        except Exception as _fe:
            log.warning("Failed to save supplier quote to DB: %s", _fe)
        
        _log_rfq_activity(rid, "supplier_quote_uploaded",
            f"Supplier quote from {supplier} ({quote_num or f.filename}): "
            f"CREATED {len(quote_items)} items (RFQ was empty), catalog +{catalog_added}/~{catalog_updated}",
            actor="user")
        
        try:
            from src.agents.drive_triggers import on_supplier_quote_uploaded
            on_supplier_quote_uploaded(r, pdf_path, supplier, quote_num)
        except Exception:
            pass
        
        return jsonify({
            "ok": True, "supplier": supplier, "quote_number": quote_num,
            "items_parsed": len(quote_items), "items_matched": applied,
            "desc_upgraded": desc_upgraded,
            "unmatched": [], "catalog_added": catalog_added, "catalog_updated": catalog_updated,
            "reconciliation": [{
                "line": i+1, "supplier_desc": q.get("description","")[:60],
                "supplier_pn": q.get("item_number",""), "supplier_qty": q.get("qty",0),
                "supplier_uom": q.get("uom",""), "supplier_price": q.get("unit_price",0),
                "matched": True, "confidence": 1.0, "rfq_line": i+1,
                "qty_match": True, "uom_match": True,
            } for i, q in enumerate(quote_items)],
            "warnings": {"qty_mismatches": 0, "uom_mismatches": 0},
            "created_items": True,
        })

    # ── Diagnostic logging for remote debugging ──
    log.info("SUPPLIER QUOTE MATCH: %d quote items vs %d RFQ items", len(quote_items), len(rfq_items))
    for qi, q in enumerate(quote_items[:5]):
        log.info("  Q[%d] pn='%s' $%.2f desc='%s'", qi, q.get("item_number",""), q.get("unit_price",0), q.get("description","")[:60])
    for ri, r_item in enumerate(rfq_items[:5]):
        log.info("  R[%d] pn='%s' desc='%s'", ri, r_item.get("item_number",""), r_item.get("description","")[:60])
    
    matches = match_quote_to_rfq(quote_items, rfq_items)
    
    matched_count = sum(1 for m in matches if m.get("matched"))
    log.info("MATCH RESULT: %d/%d matched", matched_count, len(matches))
    for m in matches:
        if not m.get("matched"):
            log.info("  UNMATCHED: Q[%d] pn='%s' $%.2f conf=%.2f", m["quote_idx"], m["quote_pn"], m["unit_price"], m.get("confidence",0))

    # Apply matches: fill cost, pick best description, update catalog
    applied = 0
    unmatched = []
    catalog_added = 0
    catalog_updated = 0
    desc_upgraded = 0

    for m in matches:
        cost = m.get("unit_price", 0)
        q_desc = m.get("quote_desc", "")
        q_pn = m.get("quote_pn", "")
        q_qty = m.get("qty", 1)
        q_uom = m.get("uom", "EA")

        if m["matched"] and m["rfq_idx"] is not None and cost > 0:
            idx = m["rfq_idx"]
            if idx < len(rfq_items):
                item = rfq_items[idx]
                item["supplier_cost"] = round(cost, 2)
                item["item_supplier"] = supplier

                # ── Description: ALWAYS use supplier's when matched ──
                # Supplier is the source of truth for what they're selling.
                # Their descriptions include McKesson #, MFG #, pack sizes.
                if q_desc and len(q_desc) > 10:
                    item["description"] = q_desc
                    item["_desc_source"] = "supplier"
                    desc_upgraded += 1

                # Fill part number if empty or from a richer source
                if q_pn and not item.get("item_number"):
                    item["item_number"] = q_pn

                applied += 1

                # ── Catalog update: always save/enrich ──
                try:
                    from src.agents.product_catalog import (
                        match_item, add_to_catalog, add_supplier_price,
                        init_catalog_db
                    )
                    init_catalog_db()
                    pn_search = q_pn or item.get("item_number", "")
                    cat_matches = match_item(q_desc, pn_search, top_n=1)
                    
                    if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                        pid = cat_matches[0]["id"]
                        # Always update catalog description with supplier's (richer)
                        if q_desc and len(q_desc) > len(cat_matches[0].get("description", "") or ""):
                            try:
                                from src.agents.product_catalog import _get_conn, _tokenize
                                conn = _get_conn()
                                conn.execute(
                                    "UPDATE product_catalog SET description=?, search_tokens=?, updated_at=? WHERE id=?",
                                    (q_desc, _tokenize(q_desc),
                                     __import__('datetime').datetime.now().isoformat(), pid)
                                )
                                conn.commit()
                                conn.close()
                            except Exception:
                                pass
                        add_supplier_price(pid, supplier, cost)
                        catalog_updated += 1
                    else:
                        pid = add_to_catalog(
                            description=q_desc,
                            part_number=pn_search,
                            cost=cost,
                            supplier_name=supplier,
                            uom=q_uom,
                            source=f"supplier_quote_{quote_num or 'upload'}",
                        )
                        if pid:
                            add_supplier_price(pid, supplier, cost)
                            catalog_added += 1
                except Exception as _ce:
                    log.debug("Catalog update from supplier quote: %s", _ce)

        else:
            unmatched.append({
                "description": q_desc,
                "part_number": q_pn,
                "unit_price": cost,
                "qty": q_qty,
            })
            # Still add unmatched items to catalog
            if cost > 0 and q_desc:
                try:
                    from src.agents.product_catalog import (
                        match_item, add_to_catalog, add_supplier_price,
                        init_catalog_db
                    )
                    init_catalog_db()
                    cat_matches = match_item(q_desc, q_pn, top_n=1)
                    if cat_matches and cat_matches[0].get("match_confidence", 0) >= 0.5:
                        pid = cat_matches[0]["id"]
                        add_supplier_price(pid, supplier, cost)
                        catalog_updated += 1
                    else:
                        pid = add_to_catalog(
                            description=q_desc,
                            part_number=q_pn,
                            cost=cost,
                            supplier_name=supplier,
                            source=f"supplier_quote_{quote_num or 'upload'}",
                        )
                        if pid:
                            add_supplier_price(pid, supplier, cost)
                            catalog_added += 1
                except Exception as _ce:
                    log.debug("Catalog update (unmatched): %s", _ce)

    # Save RFQ
    r["_last_supplier_quote"] = {
        "supplier": supplier,
        "quote_number": quote_num,
        "pdf": pdf_path,
        "items_parsed": len(quote_items),
        "items_matched": applied,
        "uploaded_at": __import__("datetime").datetime.now().isoformat(),
    }
    save_rfqs(rfqs)

    # Save supplier quote PDF to rfq_files DB (persists across deploys)
    try:
        from src.api.dashboard import save_rfq_file
        with open(pdf_path, "rb") as _qf:
            pdf_data = _qf.read()
        save_rfq_file(
            rid,
            f.filename,
            "application/pdf",
            pdf_data,
            category="supplier_quote",
            uploaded_by="user",
        )
        log.info("Supplier quote saved to DB: %s (%d bytes) for RFQ %s", f.filename, len(pdf_data), rid)
    except Exception as _fe:
        log.warning("Failed to save supplier quote to DB: %s", _fe)

    # Log activity
    sol = r.get("solicitation_number", rid)
    _log_rfq_activity(rid, "supplier_quote_uploaded",
        f"Supplier quote from {supplier} ({quote_num or f.filename}): "
        f"{len(quote_items)} parsed, {applied} matched, {desc_upgraded} desc upgraded, "
        f"catalog +{catalog_added}/~{catalog_updated}",
        actor="user")

    # ── Google Drive: archive supplier quote ──
    try:
        from src.agents.drive_triggers import on_supplier_quote_uploaded
        on_supplier_quote_uploaded(r, pdf_path, supplier, quote_num)
    except Exception as _gde:
        log.debug("Drive trigger (supplier_quote): %s", _gde)

    # ── Build reconciliation table for validation ──
    reconciliation = []
    for m in matches:
        recon = {
            "line": m.get("quote_idx", 0) + 1,
            "supplier_desc": (m.get("quote_desc") or "")[:60],
            "supplier_pn": m.get("quote_pn", ""),
            "supplier_qty": m.get("qty", 0),
            "supplier_uom": m.get("uom", ""),
            "supplier_price": m.get("unit_price", 0),
            "matched": m.get("matched", False),
            "confidence": m.get("confidence", 0),
        }
        if m["matched"] and m["rfq_idx"] is not None and m["rfq_idx"] < len(rfq_items):
            ri = rfq_items[m["rfq_idx"]]
            recon["rfq_line"] = m["rfq_idx"] + 1
            recon["rfq_qty"] = ri.get("qty", 0)
            recon["rfq_uom"] = (ri.get("uom") or "").upper()
            recon["qty_match"] = recon["supplier_qty"] == recon["rfq_qty"]
            recon["uom_match"] = recon["supplier_uom"].upper() == recon["rfq_uom"]
        reconciliation.append(recon)

    # Validation flags
    qty_mismatches = [r for r in reconciliation if r.get("matched") and not r.get("qty_match", True)]
    uom_mismatches = [r for r in reconciliation if r.get("matched") and not r.get("uom_match", True)]

    return jsonify({
        "ok": True,
        "supplier": supplier,
        "quote_number": quote_num,
        "items_parsed": len(quote_items),
        "items_matched": applied,
        "desc_upgraded": desc_upgraded,
        "unmatched": unmatched,
        "catalog_added": catalog_added,
        "catalog_updated": catalog_updated,
        "reconciliation": reconciliation,
        "warnings": {
            "qty_mismatches": len(qty_mismatches),
            "uom_mismatches": len(uom_mismatches),
        },
    })


def _renumber_items(items):
    """Re-number line items sequentially."""
    for i, it in enumerate(items):
        it["line_number"] = i + 1


def _item_response(rid, ok, msg):
    """Return JSON for AJAX or redirect for form POST."""
    if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": ok, "message": msg})
    from flask import flash as _flash
    _flash(msg, "success" if ok else "error")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/upload-templates", methods=["POST"])
@auth_required
@safe_route
def upload_templates(rid):
    """Upload 703B/704B/Bid Package template PDFs for an RFQ."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")

    files = request.files.getlist("templates")
    if not files:
        flash("No files uploaded", "error"); return redirect(f"/rfq/{rid}")

    rfq_dir = os.path.join(UPLOAD_DIR, rid)
    os.makedirs(rfq_dir, exist_ok=True)

    saved = []
    for f in files:
        safe_fn = _safe_filename(f.filename)
        if safe_fn and safe_fn.lower().endswith(".pdf"):
            p = os.path.join(rfq_dir, safe_fn)
            f.save(p)
            saved.append(p)
            # Store in DB
            try:
                f.seek(0)
                file_data = f.read()
                if not file_data:
                    with open(p, "rb") as _rb:
                        file_data = _rb.read()
                save_rfq_file(rid, safe_fn, "template_upload", file_data, category="template", uploaded_by="user")
            except Exception:
                try:
                    with open(p, "rb") as _rb:
                        save_rfq_file(rid, safe_fn, "template_upload", _rb.read(), category="template", uploaded_by="user")
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)

    if not saved:
        flash("No PDFs found in upload", "error"); return redirect(f"/rfq/{rid}")

    # Identify which forms were uploaded
    new_templates = identify_attachments(saved)

    # Merge with existing templates (don't overwrite)
    existing = r.get("templates", {})
    for key, path in new_templates.items():
        existing[key] = path
    r["templates"] = existing

    # If we now have a 704B and didn't have line items, re-parse
    if "704b" in new_templates and not r.get("line_items"):
        try:
            parsed = parse_rfq_attachments(existing)
            r["line_items"] = parsed.get("line_items", r.get("line_items", []))
            r["solicitation_number"] = parsed.get("solicitation_number", r.get("solicitation_number", ""))
            r["delivery_location"] = parsed.get("delivery_location", r.get("delivery_location", ""))
            # Auto SCPRS lookup on new items
            r["line_items"] = bulk_lookup(r.get("line_items", []))
        except Exception as e:
            log.error(f"Re-parse error: {e}")

    save_rfqs(rfqs)

    found = [k for k in new_templates.keys()]
    _log_rfq_activity(rid, "templates_uploaded",
        f"Templates uploaded: {', '.join(found).upper()} for #{r.get('solicitation_number','?')}",
        actor="user", metadata={"templates": found})
    flash(f"Templates uploaded: {', '.join(found).upper()}", "success")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/generate-package", methods=["POST"])
@auth_required
@safe_route
def generate_rfq_package(rid):
    """ONE BUTTON — generates complete RFQ package:
    1. Filled 703B (RFQ form)
    2. Filled 704B (Quote Worksheet)
    3. Filled Bid Package
    4. Reytech Quote on letterhead
    5. Draft email with all attachments
    """
    from src.api.trace import Trace
    t = Trace("rfq_package", rfq_id=rid)
    
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        t.fail("RFQ not found")
        flash("RFQ not found", "error")
        return redirect("/")
    
    sol = r.get("solicitation_number", "unknown")
    t.step("Starting", sol=sol, items=len(r.get("line_items", [])))
    
    # ── Step 1: Save ALL fields from form (not just pricing) ──
    for i, item in enumerate(r.get("line_items", [])):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit"), ("markup", "markup_pct")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try:
                    item[key] = float(v)
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)
        # Save description, qty, uom, part# from form too
        desc_val = request.form.get(f"desc_{i}")
        if desc_val is not None:
            item["description"] = desc_val.strip()
        qty_val = request.form.get(f"qty_{i}")
        if qty_val:
            try: item["qty"] = int(float(qty_val))
            except Exception: pass
        uom_val = request.form.get(f"uom_{i}")
        if uom_val is not None:
            item["uom"] = uom_val.strip().upper()
        part_val = request.form.get(f"part_{i}")
        if part_val is not None:
            item["item_number"] = part_val.strip()
        link_val = request.form.get(f"link_{i}", "").strip()
        if link_val:
            item["item_link"] = link_val
    
    r["sign_date"] = get_pst_date()
    safe_sol = re.sub(r'[^a-zA-Z0-9_-]', '_', sol.strip())
    out_dir = os.path.join(OUTPUT_DIR, sol)
    
    # ── Step 1.5: Clean old generated files on regenerate ──────────────
    if os.path.exists(out_dir):
        import shutil as _sh_clean
        try:
            _old_files = os.listdir(out_dir)
            _sh_clean.rmtree(out_dir)
            t.step(f"Cleaned {len(_old_files)} old files from {sol}/")
        except Exception as _ce:
            t.warn("Cleanup failed", error=str(_ce))
    os.makedirs(out_dir, exist_ok=True)
    
    # Also clear old output_files list and DB-stored generated files
    r["output_files"] = []
    r.pop("draft_email", None)  # Clear stale draft tied to old files
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute(
                "DELETE FROM rfq_files WHERE rfq_id = ? AND category = 'generated'",
                (rid,)
            )
            t.step("Cleared old generated files from DB")
    except Exception as _dbe:
        t.warn("DB cleanup skipped", error=str(_dbe))
    
    output_files = []
    errors = []
    
    # ── Step 2: Fill State Forms (703B, 704B, Bid Package) ──
    try:
        tmpl = r.get("templates", {})
        
        # ── DB Fallback: if template files don't exist on disk (post-redeploy),
        # reconstruct them from rfq_files DB table ──
        db_files = list_rfq_files(rid, category="template")
        type_map = {"703b": "703b", "704b": "704b", "bidpkg": "bidpkg", "bid_package": "bidpkg"}
        for db_f in db_files:
            # Determine template type from filename or file_type
            ft = db_f.get("file_type", "").lower().replace("template_", "")
            fname = db_f.get("filename", "").lower()
            ttype = None
            if "703b" in ft or "703b" in fname:
                ttype = "703b"
            elif "704b" in ft or "704b" in fname:
                ttype = "704b"
            elif "bid" in ft or "bid" in fname:
                ttype = "bidpkg"
            
            if ttype and (ttype not in tmpl or not os.path.exists(tmpl.get(ttype, ""))):
                # Restore from DB to temp location
                full_f = get_rfq_file(db_f["id"])
                if full_f and full_f.get("data"):
                    restore_dir = os.path.join(DATA_DIR, "rfq_templates", rid)
                    os.makedirs(restore_dir, exist_ok=True)
                    restore_path = os.path.join(restore_dir, db_f["filename"])
                    with open(restore_path, "wb") as _fw:
                        _fw.write(full_f["data"])
                    tmpl[ttype] = restore_path
                    t.step(f"Restored {ttype} from DB: {db_f['filename']}")
        
        # ── Auto-fallback: Use saved CDCR bid package template ONLY for CDCR/CCHCS ──
        _agency = (r.get("agency", "") or "").upper()
        _is_cdcr = any(x in _agency for x in ["CDCR", "CCHCS", "CORRECTIONS"])
        if _is_cdcr and ("bidpkg" not in tmpl or not os.path.exists(tmpl.get("bidpkg", ""))):
            default_bidpkg = os.path.join(DATA_DIR, "templates", "cdcr_bid_package_template.pdf")
            if os.path.exists(default_bidpkg):
                tmpl["bidpkg"] = default_bidpkg
                t.step("Using default CDCR bid package template")
        
        # Update templates in RFQ data
        r["templates"] = tmpl
        
        # ── Match agency FIRST — determines which forms to generate ──
        try:
            from src.core.agency_config import match_agency
            _agency_key, _agency_cfg = match_agency(r)
            _req_forms = set(_agency_cfg.get("required_forms", []))
            _opt_forms = set(_agency_cfg.get("optional_forms", []))
            t.step(f"Agency matched: {_agency_key} ({_agency_cfg.get('name','')}), {len(_req_forms)} required forms: {', '.join(sorted(_req_forms))}")
        except Exception as _ae:
            t.warn(f"Agency config load failed, using CCHCS default: {_ae}")
            _req_forms = {"703b", "704b", "bidpkg", "quote", "sellers_permit"}
            _opt_forms = set()
            _agency_key = "cchcs"
        
        # Helper: should this form be included?
        def _include(form_id):
            return form_id in _req_forms
        
        # ── Template-based forms (only if agency requires them) ──
        if _include("703b"):
            if "703b" in tmpl and os.path.exists(tmpl["703b"]):
                try:
                    fill_703b(tmpl["703b"], r, CONFIG, f"{out_dir}/{sol}_703B_Reytech.pdf")
                    output_files.append(f"{sol}_703B_Reytech.pdf")
                    t.step("703B filled")
                except Exception as e:
                    errors.append(f"703B: {e}")
                    t.warn("703B fill failed", error=str(e))
            else:
                t.step("703B skipped — no template")
                errors.append("703B: no template uploaded — upload 703B PDF on this RFQ page")
        
        if _include("704b"):
            if "704b" in tmpl and os.path.exists(tmpl["704b"]):
                try:
                    fill_704b(tmpl["704b"], r, CONFIG, f"{out_dir}/{sol}_704B_Reytech.pdf")
                    output_files.append(f"{sol}_704B_Reytech.pdf")
                    t.step("704B filled")
                except Exception as e:
                    errors.append(f"704B: {e}")
                    t.warn("704B fill failed", error=str(e))
            else:
                t.step("704B skipped — no template")
                errors.append("704B: no template uploaded — upload 704B PDF on this RFQ page")
        
        if _include("bidpkg"):
            if "bidpkg" in tmpl and os.path.exists(tmpl["bidpkg"]):
                try:
                    fill_bid_package(tmpl["bidpkg"], r, CONFIG, f"{out_dir}/{sol}_BidPackage_Reytech.pdf")
                    output_files.append(f"{sol}_BidPackage_Reytech.pdf")
                    t.step("Bid Package filled")
                except Exception as e:
                    errors.append(f"Bid Package: {e}")
                    t.warn("Bid Package fill failed", error=str(e))
            else:
                t.step("Bid Package skipped — no template")
                errors.append("Bid Package: no template uploaded — upload Bid Package PDF on this RFQ page")
        
        # ── AGENCY-GATED FORMS ─────
        
        # STD 204 Payee Data Record
        if _include("std204"):
            try:
                from src.forms.reytech_filler_v4 import fill_std204
                std204_tmpl = os.path.join(DATA_DIR, "templates", "std204_blank.pdf")
                if os.path.exists(std204_tmpl):
                    fill_std204(std204_tmpl, r, CONFIG, f"{out_dir}/{sol}_STD204_Reytech.pdf")
                    output_files.append(f"{sol}_STD204_Reytech.pdf")
                    t.step("STD 204 filled")
            except Exception as e:
                errors.append(f"STD 204: {e}")
        
        # Seller's Permit
        if _include("sellers_permit"):
            try:
                sellers_permit = os.path.join(DATA_DIR, "templates", "sellers_permit_reytech.pdf")
                if os.path.exists(sellers_permit):
                    import shutil
                    shutil.copy2(sellers_permit, f"{out_dir}/{sol}_SellersPermit_Reytech.pdf")
                    output_files.append(f"{sol}_SellersPermit_Reytech.pdf")
                    t.step("Seller's Permit included")
            except Exception as e:
                t.warn("Seller's Permit copy failed", error=str(e))
        
        # DVBE 843
        if _include("dvbe843"):
            try:
                from src.forms.reytech_filler_v4 import generate_dvbe_843
                generate_dvbe_843(r, CONFIG, f"{out_dir}/{sol}_DVBE843_Reytech.pdf")
                output_files.append(f"{sol}_DVBE843_Reytech.pdf")
                t.step("DVBE 843 generated")
            except Exception as e:
                errors.append(f"DVBE 843: {e}")
        
        # CV 012 CUF (Cal Vet)
        if _include("cv012_cuf"):
            try:
                from src.forms.reytech_filler_v4 import fill_cv012_cuf
                cuf_tmpl = os.path.join(DATA_DIR, "templates", "cv012_cuf_blank.pdf")
                if os.path.exists(cuf_tmpl):
                    fill_cv012_cuf(cuf_tmpl, r, CONFIG, f"{out_dir}/{sol}_CV012_CUF_Reytech.pdf")
                    output_files.append(f"{sol}_CV012_CUF_Reytech.pdf")
                    t.step("CV 012 CUF filled")
            except Exception as e:
                t.warn("CV 012 CUF failed", error=str(e))
        
        # Barstow CUF (facility-specific)
        if _include("barstow_cuf") or "barstow_cuf" in _opt_forms:
            try:
                from src.forms.reytech_filler_v4 import generate_barstow_cuf
                _rfq_text = " ".join([
                    str(r.get("ship_to", "")), str(r.get("delivery_location", "")),
                    str(r.get("institution", "")),
                ]).lower()
                if "barstow" in _rfq_text or "92311" in _rfq_text:
                    generate_barstow_cuf(r, CONFIG, f"{out_dir}/{sol}_BarstowCUF_Reytech.pdf")
                    output_files.append(f"{sol}_BarstowCUF_Reytech.pdf")
                    t.step("Barstow CUF generated")
            except Exception as e:
                t.warn("Barstow CUF failed", error=str(e))
        
        # Bidder Declaration
        if _include("bidder_decl"):
            try:
                from src.forms.reytech_filler_v4 import generate_bidder_declaration
                generate_bidder_declaration(r, CONFIG, f"{out_dir}/{sol}_BidderDecl_Reytech.pdf")
                output_files.append(f"{sol}_BidderDecl_Reytech.pdf")
                t.step("Bidder Declaration generated")
            except Exception as e:
                t.warn("Bidder Declaration failed", error=str(e))
        
        # Darfur Act
        if _include("darfur_act"):
            try:
                from src.forms.reytech_filler_v4 import generate_darfur_act
                generate_darfur_act(r, CONFIG, f"{out_dir}/{sol}_DarfurAct_Reytech.pdf")
                output_files.append(f"{sol}_DarfurAct_Reytech.pdf")
                t.step("Darfur Act generated")
            except Exception as e:
                t.warn("Darfur Act failed", error=str(e))
        
        # CalRecycle 74
        if _include("calrecycle74") or ("calrecycle74" in _opt_forms and len(r.get("line_items", [])) > 6):
            try:
                from src.forms.reytech_filler_v4 import fill_calrecycle_standalone
                cr_tmpl = os.path.join(DATA_DIR, "templates", "calrecycle_74_blank.pdf")
                if os.path.exists(cr_tmpl):
                    fill_calrecycle_standalone(cr_tmpl, r, CONFIG, f"{out_dir}/{sol}_CalRecycle74_Reytech.pdf")
                    output_files.append(f"{sol}_CalRecycle74_Reytech.pdf")
                    t.step("CalRecycle 74 filled")
            except Exception as e:
                t.warn("CalRecycle 74 failed", error=str(e))
        
        # STD 1000 GenAI
        if _include("std1000"):
            try:
                from src.forms.reytech_filler_v4 import fill_std1000
                std1000_tmpl = os.path.join(DATA_DIR, "templates", "std1000_blank.pdf")
                if os.path.exists(std1000_tmpl):
                    fill_std1000(std1000_tmpl, r, CONFIG, f"{out_dir}/{sol}_STD1000_Reytech.pdf")
                    output_files.append(f"{sol}_STD1000_Reytech.pdf")
                    t.step("STD 1000 filled")
            except Exception as e:
                t.warn("STD 1000 failed", error=str(e))
        
        # STD 205
        if _include("std205"):
            try:
                from src.forms.reytech_filler_v4 import generate_std205
                generate_std205(r, CONFIG, f"{out_dir}/{sol}_STD205_Reytech.pdf")
                output_files.append(f"{sol}_STD205_Reytech.pdf")
                t.step("STD 205 generated")
            except Exception as e:
                t.warn("STD 205 failed", error=str(e))
        
        # Drug-Free Workplace
        if _include("drug_free"):
            try:
                from src.forms.reytech_filler_v4 import generate_drug_free
                generate_drug_free(r, CONFIG, f"{out_dir}/{sol}_DrugFree_Reytech.pdf")
                output_files.append(f"{sol}_DrugFree_Reytech.pdf")
                t.step("Drug-Free STD 21 generated")
            except Exception as e:
                t.warn("Drug-Free failed", error=str(e))
    except Exception as e:
        errors.append(f"State forms: {e}")
        t.warn("State forms exception", error=str(e))
    
    # ── Step 3: Generate Reytech Quote on letterhead ──
    if QUOTE_GEN_AVAILABLE:
        try:
            quote_path = os.path.join(out_dir, f"{safe_sol}_Quote_Reytech.pdf")
            locked_qn = r.get("reytech_quote_number", "")

            # GUARDRAIL: if this RFQ already has a quote number locked in the JSON,
            # ALWAYS reuse it — never burn a new counter number on regenerate.
            # A new number is only issued when locked_qn is empty (first generation
            # or after an explicit clear-quote).
            if locked_qn:
                t.step(f"Reusing locked quote number: {locked_qn}")
            
            result = generate_quote_from_rfq(
                r, quote_path,
                include_tax=True,
                quote_number=locked_qn if locked_qn else None,
            )
            
            if result.get("ok"):
                qn = result.get("quote_number", "")
                r["reytech_quote_number"] = qn
                output_files.append(f"{safe_sol}_Quote_Reytech.pdf")
                t.step("Reytech Quote generated", quote_number=qn, total=result.get("total", 0))
                # CRM log
                _log_crm_activity(qn, "quote_generated",
                                  f"Quote {qn} generated for RFQ {sol} — ${result.get('total',0):,.2f}",
                                  actor="user", metadata={"rfq_id": rid})
            else:
                errors.append(f"Quote: {result.get('error', 'unknown')}")
                t.warn("Quote generation failed", error=result.get("error", "unknown"))
        except Exception as e:
            errors.append(f"Quote: {e}")
            t.warn("Quote exception", error=str(e))
    else:
        t.step("Quote generator not available — skipped")
    
    if not output_files and not r.get("form_type") == "generic_rfq":
        t.fail("No files generated", errors=errors)
        flash(f"No files generated — {'; '.join(errors) if errors else 'No templates found'}", "error")
        return redirect(f"/rfq/{rid}")
    
    # ── Step 3.5: Collect ALL package PDFs (state forms + original RFQ attachments) ──
    # These will be merged into one single package PDF
    package_pdfs = []  # (filepath, label) — order matters
    
    # ── Split output files into 4 separate attachments ──
    # Attachment 1: 703B (standalone filled bidder form)
    # Attachment 2: 704B (standalone filled pricing worksheet)
    # Attachment 3: Formal quote on Reytech letterhead
    # Attachment 4: RFQ Package — all supporting compliance docs merged in this order:
    #   BidPackage (CDCR Terms + CUF MC-345 + GenAI 708 + Voluntary Stats)
    #   CalRecycle 74, Bidder Declaration, Darfur Act, DVBE 843,
    #   Drug-Free STD 21, Seller's Permit, STD 204
    #
    # RULE: To add a form to the package, add its form_id to the agency config
    # required_forms AND add its filename pattern to _FORM_ORDER below.
    # NEVER remove BidPackage from the package — it contains CUF/708/Stats.

    file_703b = None
    file_704b = None
    quote_file = None

    for f in output_files:
        fpath = os.path.join(out_dir, f)
        if not os.path.exists(fpath):
            continue
        fu = f.upper()
        if "703B" in fu:
            file_703b = f
        elif "704B" in fu:
            file_704b = f
        elif "QUOTE" in fu:
            quote_file = f
        else:
            package_pdfs.append((fpath, f))

    # ── Canonical package order — matches model R25Q120_Reytech_CCHCS_BidPackage ──
    # RULE: This order is locked to the model. Do not reorder without a new sample.
    # BidPackage (pg1 CDCR Terms, pg2 CalRecycle) come first, then standalone forms
    # fill the gap (BidderDecl, Darfur), then rest of BidPackage (CUF, DVBE, GenAI, DrugFree, PD802).
    # BUT since we can't interleave pages from BidPackage with standalone files mid-merge,
    # the actual runtime order is determined by _FORM_ORDER key matching filename patterns.
    # BidPackage sorts first (position 0), then BidderDecl (2), DarfurAct (3), etc.
    # The BidPackage page filter already removed BidDecl from the template output.
    _FORM_ORDER = [
        "BidPackage",    # 0. CDCR Terms + CalRecycle + CUF + DVBE + GenAI + DrugFree + PD802
        "CalRecycle74",  # 1. Standalone CalRecycle (if present — normally inside BidPackage)
        "BidderDecl",    # 2. Standalone Bidder Declaration (ReportLab — cleaner signature)
        "DarfurAct",     # 3. Standalone Darfur Act
        "DVBE843",       # 4. Standalone DVBE (if present — normally inside BidPackage)
        "DrugFree",      # 5. Standalone Drug-Free (if present — normally inside BidPackage)
        "SellersPermit", # 6. CA Seller's Permit
        "STD204",        # 7. STD 204 Payee Data Record (if included)
        "CV012_CUF",     # 8. CUF CV 012 (CalVet only)
        "BarstowCUF",    # 9. Barstow CUF (conditional)
        "STD1000",       # 10. STD 1000 (non-CCHCS agencies)
        "STD205",        # 11. STD 205 Payee Supplement
    ]

    def _form_sort_key(filename):
        fn_upper = filename.upper().replace("-", "").replace("_", "").replace(" ", "")
        for idx, pattern in enumerate(_FORM_ORDER):
            if pattern.upper().replace("_", "") in fn_upper:
                return idx
        return len(_FORM_ORDER)

    package_pdfs.sort(key=lambda pair: _form_sort_key(pair[1]))
    
    # ── Step 4: Merge all package PDFs into ONE file ──
    final_output_files = []
    package_filename = f"RFQ_Package_{safe_sol}_ReytechInc.pdf"
    
    final_output_files = []
    package_filename = f"RFQ_Package_{safe_sol}_ReytechInc.pdf"

    # ── Attachment 1: 703B ──
    if file_703b:
        final_output_files.append(file_703b)
        t.step(f"Attachment 1 — 703B: {file_703b}")

    # ── Attachment 2: 704B ──
    if file_704b:
        final_output_files.append(file_704b)
        t.step(f"Attachment 2 — 704B: {file_704b}")

    # ── Attachment 3: Formal Quote ──
    if quote_file:
        final_output_files.append(quote_file)
        t.step(f"Attachment 3 — Quote: {quote_file}")

    # ── Attachment 4: RFQ Package (supporting compliance docs merged) ──
    if package_pdfs:
        try:
            from pypdf import PdfReader, PdfWriter
            writer = PdfWriter()
            merge_count = 0

            try:
                from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason as _skip_reason
            except Exception:
                _skip_reason = None

            for pdf_path, label in package_pdfs:
                try:
                    reader = PdfReader(pdf_path)
                    pages_added = 0

                    for page_idx, page in enumerate(reader.pages):
                        try:
                            text = page.extract_text() or ""
                        except Exception:
                            text = ""

                        # Skip XFA placeholder pages
                        if text.strip().startswith("Please wait") or (
                                "Please wait" in text and len(text.strip()) < 300):
                            continue

                        # Skip pages flagged by the bid-package page filter
                        if _skip_reason is not None:
                            try:
                                skip = _skip_reason(page)
                            except Exception:
                                skip = None
                            if skip:
                                continue

                        # Do NOT touch /Rotate — form field appearance streams are
                        # positioned in the rotated coordinate system. Changing rotation
                        # here misplaces all field values. PDF viewers handle /Rotate natively.

                        writer.add_page(page)
                        pages_added += 1

                    if pages_added > 0:
                        merge_count += 1
                        t.step(f"  Package includes: {label} ({pages_added} pg)")
                    else:
                        t.step(f"  Package skipped: {label} (all pages filtered)")
                except Exception as _me:
                    t.warn(f"Could not merge {label}", error=str(_me))

            if merge_count > 0:
                merged_path = os.path.join(out_dir, package_filename)
                with open(merged_path, "wb") as _mf:
                    writer.write(_mf)
                final_output_files.append(package_filename)
                t.step(f"Attachment 4 — RFQ Package: {merge_count} docs → {package_filename}")
            else:
                t.warn("RFQ Package: no docs to merge")
        except Exception as _merge_err:
            t.warn("Package merge failed", error=str(_merge_err))
            final_output_files.extend([os.path.basename(p) for p, _ in package_pdfs])
    
    if not final_output_files:
        t.fail("No files generated", errors=errors)
        flash(f"No files generated — {'; '.join(errors) if errors else 'No templates found'}", "error")
        return redirect(f"/rfq/{rid}")
    
    # ── Step 5: Store final files in DB (survive redeploys) ──
    for f in final_output_files:
        fpath = os.path.join(out_dir, f)
        try:
            if os.path.exists(fpath):
                with open(fpath, "rb") as _fb:
                    ftype = "generated_quote" if "Quote" in f else "generated_package"
                    save_rfq_file(rid, f, ftype, _fb.read(), category="generated", uploaded_by="user")
                    t.step(f"DB stored: {f}")
        except Exception as _de:
            t.warn(f"DB store failed: {f}", error=str(_de))
    
    # ── Step 6: Save, transition, create draft email ──
    _transition_status(r, "generated", actor="user", notes=f"Package: {len(final_output_files)} files")
    r["output_files"] = final_output_files
    r["generated_at"] = datetime.now().isoformat()
    
    # ── Google Drive: upload package to Pending ──
    try:
        from src.agents.drive_triggers import on_package_generated
        on_package_generated(r, out_dir, final_output_files)
    except Exception as _gde:
        log.debug("Drive trigger (package_generated): %s", _gde)
    
    # Draft email with final files attached (quote + merged package)
    try:
        sender = EmailSender(CONFIG.get("email", {}))
        all_paths = [os.path.join(out_dir, f) for f in final_output_files]
        r["draft_email"] = sender.create_draft_email(r, all_paths)
        t.step("Draft email created", attachments=len(all_paths))
    except Exception as e:
        t.warn("Draft email failed", error=str(e))
    
    # Save SCPRS prices for history
    try:
        save_prices_from_rfq(r)
    except Exception as _e:
        log.debug("Suppressed: %s", _e)
    
    save_rfqs(rfqs)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "generated")
    except Exception:
        pass

    # Build success message
    parts = []
    for f in final_output_files:
        if "Quote" in f: parts.append(f"Quote #{r.get('reytech_quote_number', '?')}")
        elif "Package" in f: parts.append(f"RFQ Package ({len(package_pdfs)} docs merged)")
        else: parts.append(os.path.basename(f))
    
    msg = f"✅ RFP Package ready: {', '.join(parts)}"
    if errors:
        msg += f" | ⚠️ {'; '.join(errors)}"
    
    # Log activity
    _log_rfq_activity(rid, "package_generated", msg, actor="user",
        metadata={"files": output_files, "quote_number": r.get("reytech_quote_number",""), "errors": errors})
    
    t.ok("Package complete", files=len(output_files), errors=len(errors))
    flash(msg, "success" if not errors else "info")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/generate", methods=["POST"])
@auth_required
def generate(rid):
    log.info("Generate bid package for RFQ %s", rid)
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    
    # Update pricing from form
    for i, item in enumerate(r["line_items"]):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit"), ("markup", "markup_pct")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try: item[key] = float(v)
                except Exception as e:

                    log.debug("Suppressed: %s", e)
    
    r["sign_date"] = get_pst_date()
    sol = r["solicitation_number"]
    out = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out, exist_ok=True)
    
    try:
        t = r.get("templates", {})
        output_files = []
        
        if "703b" in t and os.path.exists(t["703b"]):
            fill_703b(t["703b"], r, CONFIG, f"{out}/{sol}_703B_Reytech.pdf")
            output_files.append(f"{sol}_703B_Reytech.pdf")
        
        if "704b" in t and os.path.exists(t["704b"]):
            fill_704b(t["704b"], r, CONFIG, f"{out}/{sol}_704B_Reytech.pdf")
            output_files.append(f"{sol}_704B_Reytech.pdf")
        
        if "bidpkg" in t and os.path.exists(t["bidpkg"]):
            fill_bid_package(t["bidpkg"], r, CONFIG, f"{out}/{sol}_BidPackage_Reytech.pdf")
            output_files.append(f"{sol}_BidPackage_Reytech.pdf")
        
        if not output_files:
            flash("No template PDFs found — upload the original RFQ PDFs first", "error")
            return redirect(f"/rfq/{rid}")
        
        _transition_status(r, "generated", actor="system", notes="Bid package filled")
        r["output_files"] = output_files
        r["generated_at"] = datetime.now().isoformat()
        
        # Note which forms are missing
        missing = []
        if "703b" not in t: missing.append("703B")
        if "704b" not in t: missing.append("704B")
        if "bidpkg" not in t: missing.append("Bid Package")
        
        # Create draft email
        sender = EmailSender(CONFIG.get("email", {}))
        output_paths = [f"{out}/{f}" for f in r["output_files"]]
        r["draft_email"] = sender.create_draft_email(r, output_paths)
        
        # Save SCPRS prices
        save_prices_from_rfq(r)
        
        save_rfqs(rfqs)
        try:
            from src.core.dal import update_rfq_status as _dal_ur
            _dal_ur(rid, "generated")
        except Exception:
            pass
        msg = f"Generated {len(output_files)} form(s) for #{sol}"
        if missing:
            msg += f" — missing: {', '.join(missing)}"
        else:
            msg += " — draft email ready"
        flash(msg, "success" if not missing else "info")
    except Exception as e:
        flash(f"Error: {e}", "error")
    
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/generate-quote")
@auth_required
@safe_route
def rfq_generate_quote(rid):
    """Generate a standalone Reytech-branded quote PDF from an RFQ."""
    from src.api.trace import Trace
    t = Trace("quote_generation", rfq_id=rid)
    
    if not QUOTE_GEN_AVAILABLE:
        t.fail("Quote generator not available")
        flash("Quote generator not available", "error")
        return redirect(f"/rfq/{rid}")
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        t.fail("RFQ not found")
        flash("RFQ not found", "error"); return redirect("/")

    # Validate before generating
    from src.core.quote_validator import validate_ready_to_generate
    validation = validate_ready_to_generate(r)
    if not validation["ok"]:
        t.fail("Validation failed", errors=validation["errors"])
        flash(f"Cannot generate: {'; '.join(validation['errors'])}", "error")
        return redirect(f"/rfq/{rid}")

    sol = r.get("solicitation_number", "unknown")
    t.step("Starting", sol=sol, items=len(r.get("line_items",[])))
    safe_sol = re.sub(r'[^a-zA-Z0-9_-]', '_', sol.strip())
    out_dir = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f"{safe_sol}_Quote_Reytech.pdf")

    locked_qn = r.get("reytech_quote_number", "")
    
    result = generate_quote_from_rfq(r, output_path,
                                      quote_number=locked_qn if locked_qn else None)

    if result.get("ok"):
        fname = os.path.basename(output_path)
        if "output_files" not in r:
            r["output_files"] = []
        if fname not in r["output_files"]:
            r["output_files"].append(fname)
        r["reytech_quote_number"] = result.get("quote_number", "")
        save_rfqs(rfqs)
        t.ok("Quote generated", quote_number=result.get("quote_number",""), total=result.get("total",0))
        log.info("Quote #%s generated for RFQ %s — $%s", result.get("quote_number"), rid, f"{result['total']:,.2f}")
        flash(f"Reytech Quote #{result['quote_number']} generated — ${result['total']:,.2f}", "success")
        _log_crm_activity(result.get("quote_number", ""), "quote_generated",
                          f"Quote {result.get('quote_number','')} generated from RFQ {sol} — ${result.get('total',0):,.2f}",
                          actor="user", metadata={"rfq_id": rid, "agency": result.get("agency","")})
    else:
        t.fail("Quote generation failed", error=result.get("error","unknown"))
        log.error("Quote generation failed for RFQ %s: %s", rid, result.get("error", "unknown"))
        flash(f"Quote generation failed: {result.get('error', 'unknown')}", "error")

    return redirect(f"/rfq/{rid}")

@bp.route("/rfq/<rid>/send", methods=["POST"])
@auth_required
def send_email(rid):
    from src.api.trace import Trace
    t = Trace("email_send", rfq_id=rid)
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r or not r.get("draft_email"):
        t.fail("No draft to send")
        flash("No draft to send", "error"); return redirect(f"/rfq/{rid}")

    # Validate before sending
    from src.core.quote_validator import validate_ready_to_send
    validation = validate_ready_to_send(r)
    if not validation["ok"]:
        t.fail("Send validation failed", errors=validation["errors"])
        flash(f"Cannot send: {'; '.join(validation['errors'])}", "error")
        return redirect(f"/rfq/{rid}")

    try:
        sender = EmailSender(CONFIG.get("email", {}))
        sender.send(r["draft_email"])
        _transition_status(r, "sent", actor="user", notes="Email sent to buyer")
        r["sent_at"] = datetime.now().isoformat()
        save_rfqs(rfqs)
        try:
            from src.core.dal import update_rfq_status as _dal_ur
            _dal_ur(rid, "sent")
        except Exception:
            pass
        t.ok("Email sent", to=r["draft_email"].get("to",""), sol=r.get("solicitation_number","?"))
        flash(f"Bid response sent to {r['draft_email']['to']}", "success")
        _log_rfq_activity(rid, "email_sent",
            f"Bid response emailed to {r['draft_email'].get('to','')} for #{r.get('solicitation_number','?')}",
            actor="user", metadata={"to": r["draft_email"].get("to",""), "quote": r.get("reytech_quote_number","")})
        qn = r.get("reytech_quote_number", "")
        if qn and QUOTE_GEN_AVAILABLE:
            update_quote_status(qn, "sent", actor="system")
            _log_crm_activity(qn, "email_sent",
                              f"Quote {qn} emailed to {r['draft_email'].get('to','')}",
                              actor="user", metadata={"to": r['draft_email'].get('to','')})
    except Exception as e:
        t.fail("Send failed", error=str(e))
        flash(f"Send failed: {e}. Use 'Open in Mail App' instead.", "error")
    
    return redirect(f"/rfq/{rid}")


@bp.route("/api/quote/<qn>/regenerate", methods=["POST"])
@auth_required
def api_quote_regenerate(qn):
    """Regenerate the formal quote PDF for a given quote number.
    Finds the quote in quotes_log.json, regenerates PDF, and updates pdf_path.
    """
    from src.api.trace import Trace
    t = Trace("quote_regenerate", quote_number=qn)
    
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "Quote generator not available"}), 503
    
    try:
        quotes = get_all_quotes()
        qt = None
        for q in quotes:
            if q.get("quote_number") == qn:
                qt = q
                break
        if not qt:
            return jsonify({"ok": False, "error": f"Quote {qn} not found"}), 404
        
        # Build output path
        rfq_num = qt.get("rfq_number", "") or qn
        safe_rfq = re.sub(r'[^a-zA-Z0-9_-]', '_', str(rfq_num).strip()) or qn
        out_dir = os.path.join(OUTPUT_DIR, safe_rfq)
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, f"{safe_rfq}_Quote_Reytech.pdf")
        
        # Build quote_data from existing quote — map to generate_quote_from_rfq's expected fields
        quote_data = {
            "agency_name": qt.get("institution", ""),
            "institution": qt.get("institution", ""),
            "delivery_location": "",  # Not stored in quote, use ship_to_name instead
            "ship_to": "",
            "ship_to_name": qt.get("ship_to_name", qt.get("institution", "")),
            "ship_to_address": qt.get("ship_to_address", []),
            "agency": qt.get("agency", ""),
            "solicitation_number": rfq_num,
            "rfq_number": rfq_num,
            "requestor_email": qt.get("requestor_email", ""),
            "line_items": qt.get("items_detail", []),
            "source_pc_id": qt.get("source_pc_id", ""),
            "source_rfq_id": qt.get("source_rfq_id", ""),
        }
        
        result = generate_quote_from_rfq(
            quote_data, output_path,
            include_tax=True,
            quote_number=qn,
        )
        
        if result.get("ok"):
            # Update pdf_path in quotes_log.json
            quotes_path = os.path.join(DATA_DIR, "quotes_log.json")
            try:
                import json as _json
                all_quotes = _json.load(open(quotes_path))
                for i, q in enumerate(all_quotes):
                    if q.get("quote_number") == qn:
                        all_quotes[i]["pdf_path"] = output_path
                        break
                with open(quotes_path, "w") as f:
                    _json.dump(all_quotes, f, indent=2)
            except Exception as _e:
                t.warn(f"Could not update quotes_log: {_e}")
            
            # Store in DB for redeploy survival
            fname = os.path.basename(output_path)
            try:
                with open(output_path, "rb") as _fb:
                    save_rfq_file(qn, fname, "generated_quote", _fb.read(),
                                  category="generated", uploaded_by="user")
            except Exception:
                pass
            
            t.ok("Regenerated", path=output_path, total=result.get("total", 0))
            return jsonify({
                "ok": True,
                "quote_number": qn,
                "total": result.get("total", 0),
                "pdf_path": output_path,
                "download_url": f"/api/pricecheck/download/{fname}",
                "view_url": f"/api/pricecheck/view-pdf/{fname}",
            })
        else:
            t.fail(result.get("error", "unknown"))
            return jsonify({"ok": False, "error": result.get("error", "Generation failed")}), 500
    
    except Exception as e:
        import traceback
        t.fail(str(e))
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500


@bp.route("/api/rfq/<rid>/dismiss", methods=["POST"])
@auth_required
def api_rfq_dismiss(rid):
    """Dismiss an RFQ from the active queue with a reason.
    Keeps data for SCPRS intelligence. reason=delete does hard delete."""
    from datetime import datetime
    import os as _os
    import json as _jl
    
    data = request.get_json(force=True) if request.data else {}
    reason = data.get("reason", "other")
    
    # Load RFQs directly from JSON
    rfqs_path = _os.path.join(DATA_DIR, "rfqs.json")
    try:
        with open(rfqs_path) as f:
            rfqs = _jl.load(f)
    except Exception:
        rfqs = {}
    
    if rid not in rfqs:
        return jsonify({"ok": False, "error": "RFQ not found"})
    
    # Hard delete path
    if reason == "delete":
        sol = rfqs[rid].get("solicitation_number", "?")
        del rfqs[rid]
        try:
            with open(rfqs_path, "w") as f:
                _jl.dump(rfqs, f, indent=2)
        except Exception as e:
            log.error("Failed to save rfqs.json: %s", e)
        # Also delete from SQLite via DAL
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM rfqs WHERE id=?", (rid,))
        except Exception as e:
            log.debug("DAL RFQ delete: %s", e)
        log.info("Hard deleted RFQ #%s (id=%s)", sol, rid)
        return jsonify({"ok": True, "deleted": rid})
    
    r = rfqs[rid]
    r["status"] = "dismissed"
    r["dismiss_reason"] = reason
    r["dismissed_at"] = datetime.now().isoformat()
    rfqs[rid] = r
    try:
        with open(rfqs_path, "w") as f:
            _jl.dump(rfqs, f, indent=2)
    except Exception as e:
        log.error("Failed to save rfqs.json: %s", e)
    
    # Also update SQLite status via DAL
    try:
        from src.core.dal import update_rfq_status as _dal_update_rfq
        _dal_update_rfq(rid, "dismissed")
    except Exception as e:
        log.debug("DAL RFQ dismiss update: %s", e)
    
    sol = r.get("solicitation_number", "?")
    log.info("RFQ #%s dismissed: reason=%s", sol, reason)
    
    # Queue SCPRS price intelligence on line items (async)
    scprs_queued = False
    items = r.get("line_items", [])
    if items:
        try:
            from src.agents.scprs_lookup import queue_background_lookup
            for item in items[:20]:
                desc = item.get("description", "")
                if desc and len(desc) > 3:
                    queue_background_lookup(desc, source=f"dismissed_rfq_{rid}")
            scprs_queued = True
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    
    return jsonify({
        "ok": True,
        "dismissed": rid,
        "solicitation": sol,
        "reason": reason,
        "scprs_queued": scprs_queued,
    })


@bp.route("/rfq/<rid>/delete", methods=["POST"])
@auth_required
@safe_route
def delete_rfq(rid):
    """Delete an RFQ from the queue and remove its UID from processed list."""
    rfqs = load_rfqs()
    if rid in rfqs:
        sol = rfqs[rid].get("solicitation_number", "?")
        # Remove this email's UID from processed list so it can be re-imported
        email_uid = rfqs[rid].get("email_uid")
        if email_uid:
            _remove_processed_uid(email_uid)
        del rfqs[rid]
        save_rfqs(rfqs)
        log.info("Deleted RFQ #%s (id=%s)", sol, rid)
        _log_rfq_activity(rid, "deleted", f"RFQ #{sol} deleted", actor="user")
        flash(f"Deleted RFQ #{sol}", "success")
    return redirect("/")


# ═══════════════════════════════════════════════════════════════════════
# RFQ File Management — download from DB
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/file/<file_id>")
@auth_required
def rfq_download_file(rid, file_id):
    """Download an RFQ file from the database."""
    f = get_rfq_file(file_id)
    if not f or f.get("rfq_id") != rid:
        flash("File not found", "error")
        return redirect(f"/rfq/{rid}")
    from flask import Response
    return Response(
        f["data"],
        mimetype=f.get("mime_type", "application/pdf"),
        headers={"Content-Disposition": f"inline; filename=\"{f['filename']}\""}
    )


@bp.route("/api/rfq/<rid>/files")
@auth_required
def api_rfq_files(rid):
    """List all files for an RFQ."""
    category = request.args.get("category")
    files = list_rfq_files(rid, category=category)
    return jsonify({"ok": True, "files": files, "count": len(files)})


# ═══════════════════════════════════════════════════════════════════════
# RFQ Status Management — reopen, edit, resubmit
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/reopen", methods=["POST"])
@auth_required
@safe_route
def rfq_reopen(rid):
    """Reopen an RFQ for editing. Changes status back to 'ready'."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error")
        return redirect("/")
    
    old_status = r.get("status", "?")
    _transition_status(r, "ready", actor="user", notes=f"Reopened from '{old_status}'")
    save_rfqs(rfqs)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "ready")
    except Exception:
        pass

    _log_rfq_activity(rid, "reopened",
        f"RFQ #{r.get('solicitation_number','?')} reopened for editing (was: {old_status})",
        actor="user", metadata={"old_status": old_status})
    
    flash(f"RFQ reopened for editing (was: {old_status})", "info")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/update-status", methods=["POST"])
@auth_required
@safe_route
def rfq_update_status(rid):
    """Change RFQ status to any valid state."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error")
        return redirect("/")
    
    new_status = request.form.get("status", "").strip()
    valid = {"new", "ready", "generated", "sent", "won", "lost", "no_bid", "cancelled"}
    if new_status not in valid:
        flash(f"Invalid status: {new_status}", "error")
        return redirect(f"/rfq/{rid}")
    
    old_status = r.get("status", "?")
    notes = request.form.get("notes", "").strip()
    _transition_status(r, new_status, actor="user", notes=notes or f"Changed from {old_status}")
    save_rfqs(rfqs)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, new_status)
    except Exception:
        pass

    _log_rfq_activity(rid, "status_changed",
        f"RFQ #{r.get('solicitation_number','?')} status: {old_status} → {new_status}" + (f" ({notes})" if notes else ""),
        actor="user", metadata={"old_status": old_status, "new_status": new_status, "notes": notes})

    flash(f"Status changed: {old_status} → {new_status}", "success")
    return redirect(f"/rfq/{rid}")


# ═══════════════════════════════════════════════════════════════════════
# RFQ Activity Log
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/<rid>/activity")
@auth_required
def api_rfq_activity(rid):
    """Get activity log for an RFQ."""
    activities = _get_crm_activity(ref_id=rid, limit=50)
    return jsonify({"ok": True, "activities": activities, "count": len(activities)})


# ═══════════════════════════════════════════════════════════════════════
# Email Templates API
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email-templates")
@auth_required
def api_list_email_templates():
    """List email templates, optionally filtered by category."""
    category = request.args.get("category")
    templates = get_email_templates_db(category)
    return jsonify({"ok": True, "templates": templates})


@bp.route("/api/email-templates/<tid>", methods=["GET"])
@auth_required
def api_get_email_template(tid):
    """Get a single email template by ID."""
    templates = get_email_templates_db()
    t = next((t for t in templates if t["id"] == tid), None)
    if not t:
        return jsonify({"ok": False, "error": "Template not found"}), 404
    return jsonify({"ok": True, "template": t})


@bp.route("/api/email-templates", methods=["POST"])
@auth_required
def api_create_email_template():
    """Create or update an email template."""
    data = request.get_json(force=True, silent=True) or request.form
    tid = save_email_template_db(
        data.get("id", ""), data.get("name", ""), data.get("category", "rfq"),
        data.get("subject", ""), data.get("body", ""), int(data.get("is_default", 0)))
    if tid:
        return jsonify({"ok": True, "id": tid})
    return jsonify({"ok": False, "error": "Save failed"}), 500


@bp.route("/api/email-templates/<tid>", methods=["DELETE"])
@auth_required
def api_delete_email_template(tid):
    """Delete an email template."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM email_templates WHERE id = ?", (tid,))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/email-templates/render", methods=["POST"])
@auth_required
def api_render_email_template():
    """Render a template with variables. POST {template_id, variables: {...}}"""
    data = request.get_json(force=True, silent=True) or {}
    tid = data.get("template_id", "")
    variables = data.get("variables", {})
    
    templates = get_email_templates_db()
    t = next((t for t in templates if t["id"] == tid), None)
    if not t:
        return jsonify({"ok": False, "error": "Template not found"}), 404
    
    subject = t["subject"]
    body = t["body"]
    for key, val in variables.items():
        subject = subject.replace("{{" + key + "}}", str(val))
        body = body.replace("{{" + key + "}}", str(val))
    
    return jsonify({"ok": True, "subject": subject, "body": body})


# ═══════════════════════════════════════════════════════════════════════
# PDF Preview from DB
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/preview/<file_id>")
@auth_required
def rfq_preview_pdf(rid, file_id):
    """Serve a PDF for inline preview (Content-Disposition: inline)."""
    f = get_rfq_file(file_id)
    if not f or f.get("rfq_id") != rid:
        return "File not found", 404
    from flask import Response
    return Response(
        f["data"],
        mimetype="application/pdf",
        headers={"Content-Disposition": f"inline; filename=\"{f['filename']}\""}
    )


# ═══════════════════════════════════════════════════════════════════════
# Email Signature — get/save HTML signature for outbound emails
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email-signature")
@auth_required
def get_email_signature():
    """Get current email signature config."""
    email_cfg = CONFIG.get("email", {})
    sig_html = email_cfg.get("signature_html", "")

    # Auto-generate default signature on first load if empty
    if not sig_html:
        sig_html = _build_default_signature()
        CONFIG.setdefault("email", {})["signature_html"] = sig_html

    return jsonify({
        "ok": True,
        "signature_html": sig_html,
        "signature_enabled": email_cfg.get("signature_enabled", True),
    })

@bp.route("/api/email-signature", methods=["POST"])
@auth_required
def save_email_signature():
    """Save email signature HTML."""
    data = request.get_json(force=True)
    sig_html = data.get("signature_html", "")
    
    CONFIG.setdefault("email", {})["signature_html"] = sig_html
    CONFIG["email"]["signature_enabled"] = True
    
    # Persist to config file
    import json as _json
    for cfg_path in [
        os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "reytech_config.json"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "forms", "reytech_config.json"),
    ]:
        try:
            with open(cfg_path) as f:
                cfg = _json.load(f)
            cfg.setdefault("email", {})["signature_html"] = sig_html
            cfg["email"]["signature_enabled"] = True
            with open(cfg_path, "w") as f:
                _json.dump(cfg, f, indent=2)
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
    
    return jsonify({"ok": True})


@bp.route("/api/upload-sig-logo", methods=["POST"])
@auth_required
def upload_sig_logo():
    """Upload a PNG/JPG logo for the email signature. Returns base64 data URI."""
    import base64 as _b64
    if "logo" not in request.files:
        return jsonify({"ok": False, "error": "No file uploaded"}), 400
    f = request.files["logo"]
    if not f.filename:
        return jsonify({"ok": False, "error": "Empty filename"}), 400

    data = f.read()
    if len(data) > 5_000_000:
        return jsonify({"ok": False, "error": "File too large (max 5MB)"}), 400

    fname = f.filename.lower()
    if fname.endswith(".png"):
        mime = "image/png"
    elif fname.endswith((".jpg", ".jpeg")):
        mime = "image/jpeg"
    elif fname.endswith(".gif"):
        mime = "image/gif"
    else:
        return jsonify({"ok": False, "error": "PNG/JPG/GIF only"}), 400

    # Resize for email if large
    try:
        from PIL import Image
        import io as _io
        img = Image.open(_io.BytesIO(data))
        if img.width > 200:
            ratio = 200 / img.width
            img = img.resize((200, int(img.height * ratio)), Image.LANCZOS)
            buf = _io.BytesIO()
            img.save(buf, "PNG", optimize=True)
            data = buf.getvalue()
            mime = "image/png"
    except Exception:
        pass

    b64 = _b64.b64encode(data).decode()
    data_uri = f"data:{mime};base64,{b64}"

    # Save to data/ for future use
    try:
        save_path = os.path.join(DATA_DIR, "email_logo.png")
        with open(save_path, "wb") as _fw:
            _fw.write(data)
    except Exception:
        pass

    return jsonify({"ok": True, "data_uri": data_uri, "size": len(data)})


def _build_default_signature():
    """Build the default Reytech email signature HTML for compose section."""
    return """<table cellpadding="0" cellspacing="0" style="font-family:'Segoe UI',Arial,sans-serif;margin-top:12px">
 <tr>
  <td style="padding-right:14px;vertical-align:top"><img src="cid:reytech_logo" alt="Reytech Inc." style="width:120px;height:auto;display:block"></td>
  <td style="vertical-align:top;font-size:13px;color:#444;line-height:1.5">
   <strong style="font-size:14px;color:#1a1a2e">Reytech Inc.</strong><br>
   Sales Support<br>
   <a href="https://www.reytechinc.com" style="color:#2563eb;text-decoration:none">www.reytechinc.com</a><br>
   Trabuco Canyon, CA<br>
   949-229-1575
  </td>
 </tr>
</table>
<div style="font-size:11px;color:#999;margin-top:8px;line-height:1.4">
CA MB/SB/SB-PW/DVBE #2002605 &middot; NY SDVOB 221449<br>
DOT DBE #44511 &middot; MBE SC6550 &middot; SBA-SDVOB (FWWSKE9113T7)
</div>"""


# ═══════════════════════════════════════════════════════════════════════
# Enhanced Email Send — DB attachments + email logging + CRM tracking
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/save-draft", methods=["POST"])
@auth_required
def save_gmail_draft(rid):
    """Save email as Gmail draft — user reviews and sends manually from Gmail."""
    from src.api.trace import Trace
    t = Trace("email_draft", rfq_id=rid)

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error")
        return redirect("/")

    to_addr = request.form.get("to", "").strip()
    subject = request.form.get("subject", "").strip()
    body = request.form.get("body", "").strip()
    cc = request.form.get("cc", "").strip()
    attach_ids = [x.strip() for x in request.form.get("attach_files", "").split(",") if x.strip()]

    if not to_addr or not subject:
        flash("Draft requires To and Subject", "error")
        return redirect(f"/rfq/{rid}")

    import tempfile, shutil, imaplib, time as _time
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    tmp_dir = tempfile.mkdtemp(prefix="rfq_draft_")
    try:
        # Build the MIME message
        msg = MIMEMultipart("mixed")
        email_cfg = CONFIG.get("email", {})
        from_name = email_cfg.get("from_name", "Michael Guadan - Reytech Inc.")
        from_addr = email_cfg.get("email", os.environ.get("GMAIL_ADDRESS", "sales@reytechinc.com"))
        password = email_cfg.get("email_password", os.environ.get("GMAIL_PASSWORD", ""))

        msg["From"] = f"{from_name} <{from_addr}>"
        msg["To"] = to_addr
        msg["Subject"] = subject
        if cc:
            msg["Cc"] = cc

        # HTML body with signature
        try:
            from src.core.email_signature import wrap_html_email
            body_html = wrap_html_email(body)
        except Exception:
            body_html = None

        if body_html:
            related = MIMEMultipart("related")
            related.attach(MIMEText(body_html, "html"))
            # Embed logo as CID inline attachment
            try:
                from src.core.paths import DATA_DIR as _dd2
                for _ln in ("reytech_logo_email.png", "email_logo.png", "reytech_logo.png", "logo.png"):
                    _lp = os.path.join(_dd2, _ln)
                    if os.path.exists(_lp):
                        from email.mime.image import MIMEImage
                        with open(_lp, "rb") as _lf2:
                            _lip = MIMEImage(_lf2.read(), _subtype="png")
                        _lip.add_header("Content-ID", "<reytech_logo>")
                        _lip.add_header("Content-Disposition", "inline", filename="reytech_logo.png")
                        related.attach(_lip)
                        break
            except Exception:
                pass
            alt = MIMEMultipart("alternative")
            alt.attach(MIMEText(body, "plain"))
            alt.attach(related)
            msg.attach(alt)
        else:
            msg.attach(MIMEText(body, "plain"))

        # Attach files
        attached = []
        for fid in attach_ids:
            f = get_rfq_file(fid)
            if f and f.get("data"):
                path = os.path.join(tmp_dir, f["filename"])
                with open(path, "wb") as _fw:
                    _fw.write(f["data"])
                with open(path, "rb") as _fr:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(_fr.read())
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f"attachment; filename={f['filename']}")
                msg.attach(part)
                attached.append(f["filename"])

        # Save to Gmail Drafts via IMAP APPEND
        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(from_addr, password)

        saved = False
        for folder in ['"[Gmail]/Drafts"', "[Gmail]/Drafts", "Drafts", "DRAFTS"]:
            try:
                res = imap.append(folder, "", imaplib.Time2Internaldate(_time.time()), msg.as_bytes())
                if res[0] == "OK":
                    saved = True
                    t.ok("Draft saved", folder=folder, attachments=len(attached))
                    break
            except Exception as _fe:
                log.debug("IMAP draft append %s: %s", folder, _fe)

        if not saved:
            # Auto-detect Drafts folder
            _, folders = imap.list()
            import re as _re
            for _raw in (folders or []):
                _s = _raw.decode() if isinstance(_raw, bytes) else str(_raw)
                if "draft" in _s.lower():
                    _m = _re.search(r'"([^"]+)"\s*$', _s) or _re.search(r'(\S+)$', _s)
                    if _m:
                        try:
                            res = imap.append(_m.group(1), "", imaplib.Time2Internaldate(_time.time()), msg.as_bytes())
                            if res[0] == "OK":
                                saved = True
                                t.ok("Draft saved", folder=_m.group(1))
                                break
                        except Exception:
                            pass

        imap.logout()

        if saved:
            flash(f"✅ Draft saved to Gmail — open Gmail to review and send ({len(attached)} attachments)", "success")
        else:
            flash("⚠️ Could not save to Gmail Drafts — check IMAP is enabled in Gmail settings", "error")

    except Exception as e:
        t.fail("Draft save failed", error=str(e))
        flash(f"Draft save failed: {e}", "error")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/send-email", methods=["POST"])
@auth_required
@safe_route
def send_email_enhanced(rid):
    """Send email with editable fields and DB-stored attachments.
    Form fields: to, subject, body, attach_files (comma-separated file IDs)
    """
    from src.api.trace import Trace
    t = Trace("email_send", rfq_id=rid)

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        t.fail("RFQ not found")
        flash("RFQ not found", "error")
        return redirect("/")

    # Validate before sending
    from src.core.quote_validator import validate_ready_to_send
    validation = validate_ready_to_send(r)
    if not validation["ok"]:
        t.fail("Send validation failed", errors=validation["errors"])
        flash(f"Cannot send: {'; '.join(validation['errors'])}", "error")
        return redirect(f"/rfq/{rid}")

    # Get editable fields from form
    to_addr = request.form.get("to", "").strip()
    subject = request.form.get("subject", "").strip()
    body = request.form.get("body", "").strip()
    cc = request.form.get("cc", "").strip()
    bcc = request.form.get("bcc", "").strip()
    attach_ids = [x.strip() for x in request.form.get("attach_files", "").split(",") if x.strip()]
    
    if not to_addr or not subject:
        flash("Email requires To and Subject", "error")
        return redirect(f"/rfq/{rid}")
    
    t.step("Preparing email", to=to_addr, attachments=len(attach_ids))
    
    # Build attachment list from DB files
    import tempfile, shutil
    tmp_dir = tempfile.mkdtemp(prefix="rfq_send_")
    attachment_paths = []
    attachment_names = []
    
    try:
        for fid in attach_ids:
            f = get_rfq_file(fid)
            if f and f.get("data"):
                path = os.path.join(tmp_dir, f["filename"])
                with open(path, "wb") as _fw:
                    _fw.write(f["data"])
                attachment_paths.append(path)
                attachment_names.append(f["filename"])
                t.step(f"Attached: {f['filename']}")
        
        # Also check filesystem for any files not in DB yet
        if not attach_ids and r.get("output_files"):
            out_dir = os.path.join(UPLOAD_DIR, rid)
            for fname in r["output_files"]:
                fpath = os.path.join(out_dir, fname)
                if os.path.exists(fpath):
                    attachment_paths.append(fpath)
                    attachment_names.append(fname)
        
        # Send via SMTP — include HTML signature if enabled
        draft = {
            "to": to_addr,
            "subject": subject,
            "body": body,
            "cc": cc,
            "bcc": bcc,
            "attachments": attachment_paths,
        }
        
        # Threading: if RFQ came from email, reply to that thread
        msg_id = r.get("email_message_id", "")
        if msg_id:
            draft["in_reply_to"] = msg_id
            draft["references"] = msg_id
        
        include_sig = request.form.get("include_signature") == "1"
        email_cfg = CONFIG.get("email", {})
        sig_html = email_cfg.get("signature_html", "")
        
        if include_sig and sig_html:
            # Build HTML body: plain text body + signature
            import html as _html
            body_escaped = _html.escape(body).replace("\n", "<br>")
            draft["body_html"] = f"""<div style="font-family:'Segoe UI',Arial,sans-serif;font-size:14px;color:#222;line-height:1.6">
{body_escaped}
<br><br>
<div style="border-top:1px solid #ddd;padding-top:10px;margin-top:10px">
{sig_html}
</div>
</div>"""
            t.step("HTML signature included")
        
        sender = EmailSender(CONFIG.get("email", {}))
        sender.send(draft)
        
        # Transition status
        _transition_status(r, "sent", actor="user", notes=f"Email sent to {to_addr}")
        r["sent_at"] = datetime.now().isoformat()
        r["draft_email"] = {"to": to_addr, "subject": subject, "body": body, "cc": cc, "bcc": bcc}
        save_rfqs(rfqs)
        try:
            from src.core.dal import update_rfq_status as _dal_ur
            _dal_ur(rid, "sent")
        except Exception:
            pass
        
        # ── Log to email_log table ──
        sol = r.get("solicitation_number", "")
        qn = r.get("reytech_quote_number", "")
        
        # Find contact_id from recipient email
        contact_id = ""
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute("SELECT id FROM contacts WHERE buyer_email = ?", (to_addr.lower(),)).fetchone()
                if row:
                    contact_id = row[0]
        except Exception as _e:
            log.debug("Suppressed: %s", _e)
        
        email_log_id = log_email_sent_db(
            direction="outbound", sender=sender.email_addr, recipient=to_addr,
            subject=subject, body=body, attachments=attachment_names,
            quote_number=qn, rfq_id=rid, contact_id=contact_id)
        t.step(f"Email logged (id={email_log_id})")
        
        # ── CRM activity: log against quote AND contact ──
        _log_rfq_activity(rid, "email_sent",
            f"Bid response emailed to {to_addr} for Sol #{sol} ({len(attachment_names)} attachments)",
            actor="user", metadata={"to": to_addr, "quote": qn, "files": attachment_names, "email_log_id": email_log_id})
        
        if qn:
            _log_crm_activity(qn, "email_sent",
                f"Quote {qn} emailed to {to_addr} for Sol #{sol}",
                actor="user", metadata={"to": to_addr, "rfq_id": rid})
            if QUOTE_GEN_AVAILABLE:
                update_quote_status(qn, "sent", actor="system")
        
        if contact_id:
            _log_crm_activity(contact_id, "email_sent",
                f"Bid response for Sol #{sol} (Quote {qn}) sent to {to_addr}",
                actor="user", metadata={"rfq_id": rid, "quote": qn, "solicitation": sol})
        
        t.ok("Email sent", to=to_addr, attachments=len(attachment_names))
        flash(f"✅ Email sent to {to_addr} with {len(attachment_names)} attachments", "success")
        
    except Exception as e:
        t.fail("Send failed", error=str(e))
        flash(f"Send failed: {e}. Try 'Open in Mail App' instead.", "error")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    
    return redirect(f"/rfq/{rid}")


# ═══════════════════════════════════════════════════════════════════════
# Email History API (for contact/quote level)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email-history")
@auth_required
def api_email_history():
    """Get email history. Filter by ?rfq_id=, ?quote_number=, ?contact_id="""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            query = "SELECT id, logged_at, direction, sender, recipient, subject, body_preview, attachments_json, quote_number, rfq_id, contact_id, status FROM email_log WHERE 1=1"
            params = []
            for field in ("rfq_id", "quote_number", "contact_id"):
                val = request.args.get(field)
                if val:
                    query += f" AND {field} = ?"
                    params.append(val)
            query += " ORDER BY logged_at DESC LIMIT 50"
            rows = conn.execute(query, params).fetchall()
            return jsonify({"ok": True, "emails": [dict(r) for r in rows], "count": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ═══════════════════════════════════════════════════════════════════════
# OBS 1600 — CA Agricultural Food Product Certification
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/food-classify", methods=["POST"])
@auth_required
def api_food_classify():
    """Classify quote/RFQ items into CDCR food category codes.
    Body: {"items": [{"description": "..."}, ...]}
    Returns classified items with food codes.
    """
    try:
        from src.forms.food_classifier import classify_quote_items
        data = request.get_json(force=True)
        items = data.get("items", [])
        classified = classify_quote_items(items)
        food_count = sum(1 for r in classified if r['is_food'])
        return jsonify({"ok": True, "items": classified, "food_count": food_count,
                        "total_count": len(classified)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/api/rfq/<rid>/obs1600", methods=["POST"])
@auth_required
def api_generate_obs1600(rid):
    """Generate filled OBS 1600 food certification form for an RFQ.
    Uses the bid package PDF if available, or a standalone template.
    """
    from src.api.trace import Trace
    t = Trace("obs1600_fill", rfq_id=rid)
    
    try:
        from src.forms.reytech_filler_v4 import load_config, fill_obs1600, get_pst_date
        from src.forms.food_classifier import get_food_items_for_obs1600
        
        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            t.fail("RFQ not found")
            return jsonify({"ok": False, "error": "RFQ not found"}), 404
        
        config = load_config()
        sol = r.get("solicitation_number", "unknown")
        
        # Get items — try line_items first, then items_detail from quote
        items = r.get("line_items", [])
        if not items:
            items = r.get("items_detail", r.get("items", []))
            if isinstance(items, str):
                import json as _json
                try: items = _json.loads(items)
                except Exception: items = []
        
        # Classify food items
        food_items = get_food_items_for_obs1600(items)
        
        if not food_items:
            t.step("No food items found", item_count=len(items))
            return jsonify({"ok": False, "error": "No food items found in this RFQ. Only food products need the OBS 1600 form.",
                            "items_checked": len(items)}), 400
        
        t.step("Classified food items", food_count=len(food_items),
               items=[f"{fi['description'][:40]} → Code {fi['code']}" for fi in food_items[:5]])
        
        # Output directory
        out_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "output", sol)
        os.makedirs(out_dir, exist_ok=True)
        
        # Find bid package template — use RFQ's stored template paths
        bid_pkg = None
        tmpl = r.get("templates", {})
        
        # Check bid package template from RFQ data
        if tmpl.get("bidpkg") and os.path.exists(tmpl["bidpkg"]):
            bid_pkg = tmpl["bidpkg"]
        
        # Try to restore from DB if not on disk
        if not bid_pkg:
            try:
                from src.core.db import get_db
                with get_db() as conn:
                    db_files = conn.execute(
                        "SELECT id, filename, file_type FROM rfq_files WHERE rfq_id=? AND category='template'",
                        (rid,)).fetchall()
                    for db_f in db_files:
                        fname = db_f["filename"].lower()
                        if "bid" in fname or "package" in fname or "form" in fname:
                            full_f_row = conn.execute("SELECT data FROM rfq_files WHERE id=?", (db_f["id"],)).fetchone()
                            if full_f_row and full_f_row["data"]:
                                restore_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "rfq_templates", rid)
                                os.makedirs(restore_dir, exist_ok=True)
                                restore_path = os.path.join(restore_dir, db_f["filename"])
                                with open(restore_path, "wb") as _fw:
                                    _fw.write(full_f_row["data"])
                                bid_pkg = restore_path
                                t.step(f"Restored bid package from DB: {db_f['filename']}")
                                break
            except Exception as db_err:
                t.step(f"DB restore failed: {db_err}")
        
        # Check uploaded files directory
        if not bid_pkg:
            import glob
            for pattern in [f"*{sol}*BID*PACKAGE*", f"*{sol}*bid*pack*", f"*{sol}*form*", f"*{sol}*.pdf"]:
                for search_dir in [os.path.join(DATA_DIR, "uploads"), os.path.join(DATA_DIR, "rfq_templates"), os.path.join(DATA_DIR, "output", sol)]:
                    matches = glob.glob(os.path.join(search_dir, pattern))
                    for m in matches:
                        # Verify it has OBS 1600 fields
                        try:
                            from pypdf import PdfReader
                            _r = PdfReader(m)
                            for page in _r.pages:
                                if "/Annots" in page:
                                    for annot in page["/Annots"]:
                                        obj = annot.get_object()
                                        if "OBS 1600" in str(obj.get("/T", "")):
                                            bid_pkg = m
                                            break
                                if bid_pkg: break
                        except Exception:
                            pass
                        if bid_pkg: break
                    if bid_pkg: break
                if bid_pkg: break
        
        # Fallback: use saved CDCR bid package template
        if not bid_pkg:
            default_tmpl = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "templates", "cdcr_bid_package_template.pdf")
            if os.path.exists(default_tmpl):
                bid_pkg = default_tmpl
                t.step("Using saved CDCR bid package template")
        
        # Build rfq_data for the filler
        rfq_data = {
            "solicitation_number": sol,
            "sign_date": get_pst_date(),
            "line_items": items,
        }
        
        output_path = os.path.join(out_dir, f"{sol}_OBS1600_FoodCert_Reytech.pdf")
        
        if bid_pkg and os.path.exists(bid_pkg):
            # Fill OBS 1600 fields in the existing bid package
            fill_obs1600(bid_pkg, rfq_data, config, output_path, food_items=food_items)
            t.ok(f"Filled from bid package template: {os.path.basename(bid_pkg)}")
        else:
            # Generate standalone OBS 1600 using reportlab
            _generate_standalone_obs1600(food_items, config, rfq_data, output_path)
            t.ok("Generated standalone OBS 1600")
        
        return jsonify({
            "ok": True,
            "file": output_path,
            "filename": os.path.basename(output_path),
            "food_items": food_items,
            "food_count": len(food_items),
            "download_url": f"/api/download/{sol}/{os.path.basename(output_path)}",
        })
        
    except Exception as e:
        import traceback
        t.fail(str(e))
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500


def _generate_standalone_obs1600(food_items, config, rfq_data, output_path):
    """Generate a standalone OBS 1600 PDF using reportlab when no template is available."""
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.units import inch
    
    company = config["company"]
    sol = rfq_data.get("solicitation_number", "")
    sign_date = rfq_data.get("sign_date", "")
    
    c = rl_canvas.Canvas(output_path, pagesize=letter)
    w, h = letter
    
    # Header
    c.setFont("Helvetica-Bold", 9)
    c.drawString(0.5*inch, h - 0.5*inch, "California Department of Corrections and Rehabilitation/California Correctional Health Care Services")
    c.setFont("Helvetica", 8)
    c.drawString(0.5*inch, h - 0.65*inch, "Office of Business Services - Non-IT Goods Procurement/Acquisitions Management Section, Procurement Services")
    c.drawString(0.5*inch, h - 0.8*inch, "OBS 1600 (Rev. 1/26)")
    
    # Title
    c.setFont("Helvetica-Bold", 12)
    c.drawCentredString(w/2, h - 1.2*inch, "California-Grown/Produced Agricultural Food Products Vendor Certification")
    
    # Vendor info
    y = h - 1.6*inch
    c.setFont("Helvetica-Bold", 10)
    c.drawString(0.5*inch, y, f"Vendor Name : {company['name']}")
    y -= 0.25*inch
    c.drawString(0.5*inch, y, f"Solicitation # : {sol}")
    
    # Table header
    y -= 0.45*inch
    c.setFont("Helvetica-Bold", 8)
    col_x = [0.5*inch, 1.2*inch, 4.5*inch, 5.2*inch, 6.2*inch]
    headers = ["Quoted Line\nItem #", "Food Product Description", "Code", "CA-Grown\nor Produced\n(Yes/No)", "If Yes, % of\nProduct"]
    
    # Header row background
    c.setFillColorRGB(0.9, 0.9, 0.9)
    c.rect(0.5*inch, y - 0.15*inch, 7*inch, 0.45*inch, fill=True, stroke=True)
    c.setFillColorRGB(0, 0, 0)
    
    for i, hdr in enumerate(headers):
        lines = hdr.split("\n")
        for j, line in enumerate(lines):
            c.drawString(col_x[i] + 0.05*inch, y + 0.2*inch - j*0.12*inch, line)
    
    # Data rows
    y -= 0.35*inch
    c.setFont("Helvetica", 9)
    for item in food_items[:18]:
        y -= 0.28*inch
        c.line(0.5*inch, y - 0.05*inch, 7.5*inch, y - 0.05*inch)
        c.drawString(col_x[0] + 0.1*inch, y + 0.05*inch, str(item.get("line_number", "")))
        c.drawString(col_x[1] + 0.05*inch, y + 0.05*inch, item.get("description", "")[:55])
        c.drawCentredString(col_x[2] + 0.35*inch, y + 0.05*inch, str(item.get("code", "")))
        c.drawCentredString(col_x[3] + 0.4*inch, y + 0.05*inch, item.get("ca_grown", "No"))
        c.drawCentredString(col_x[4] + 0.4*inch, y + 0.05*inch, item.get("pct", "N/A"))
    
    # Fill remaining empty rows
    for _ in range(18 - len(food_items)):
        y -= 0.28*inch
        c.line(0.5*inch, y - 0.05*inch, 7.5*inch, y - 0.05*inch)
    
    # Table border
    table_top = h - 2.1*inch
    c.rect(0.5*inch, y - 0.05*inch, 7*inch, table_top - (y - 0.05*inch))
    
    # Certification text
    y -= 0.45*inch
    c.setFont("Helvetica", 7.5)
    c.drawString(0.5*inch, y, "Pursuant to California Code, Food and Agricultural Code, Section 58595(a), I certify under the laws of the State of California")
    y -= 0.15*inch
    c.drawString(0.5*inch, y, "that the above information is true and correct.")
    
    # Signature block
    y -= 0.4*inch
    c.setFont("Helvetica", 10)
    c.drawString(0.5*inch, y, company["owner"])
    c.drawString(3.2*inch, y, company["title"])
    c.drawString(5.5*inch, y, sign_date)
    
    y -= 0.15*inch
    c.line(0.5*inch, y, 2.8*inch, y)
    c.line(3.2*inch, y, 4.8*inch, y)
    c.line(5.5*inch, y, 7.5*inch, y)
    
    y -= 0.15*inch
    c.setFont("Helvetica-Bold", 8)
    c.drawString(0.5*inch, y, "Print Name")
    c.drawString(2.2*inch, y, "Signature")
    c.drawString(3.2*inch, y, "Title")
    c.drawString(5.5*inch, y, "Date")
    
    c.save()


@bp.route("/api/download/<sol>/<filename>")
@auth_required
def api_download_file(sol, filename):
    """Download a generated file."""
    import re as _re
    # Sanitize inputs
    sol = _re.sub(r'[^a-zA-Z0-9_-]', '', sol)
    filename = os.path.basename(filename)
    filepath = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "output", sol, filename)
    if not os.path.exists(filepath):
        return jsonify({"ok": False, "error": "File not found"}), 404
    from flask import send_file
    return send_file(filepath, as_attachment=True, download_name=filename)


# ═══════════════════════════════════════════════════════════════════════
# Fill ALL Bid Package Forms (CUF, Darfur, DVBE, CalRecycle, OBS 1600, etc.)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/<rid>/fill-bid-package", methods=["POST"])
@auth_required
def api_fill_bid_package(rid):
    """Fill ALL forms in the CDCR bid package for an RFQ."""
    from src.api.trace import Trace
    t = Trace("fill_bid_package", rfq_id=rid)
    
    try:
        from src.forms.reytech_filler_v4 import load_config, fill_bid_package, get_pst_date
        
        rfqs = load_rfqs()
        r = rfqs.get(rid)
        if not r:
            t.fail("RFQ not found")
            return jsonify({"ok": False, "error": "RFQ not found"}), 404
        
        config = load_config()
        sol = r.get("solicitation_number", "unknown")
        
        # Get items
        items = r.get("line_items", [])
        if not items:
            items = r.get("items_detail", r.get("items", []))
            if isinstance(items, str):
                import json as _json
                try: items = _json.loads(items)
                except Exception: items = []
        
        # Find template
        bid_pkg = None
        tmpl = r.get("templates", {})
        if tmpl.get("bidpkg") and os.path.exists(tmpl["bidpkg"]):
            bid_pkg = tmpl["bidpkg"]
        
        # Fallback to saved template
        if not bid_pkg:
            default_tmpl = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "templates", "cdcr_bid_package_template.pdf")
            if os.path.exists(default_tmpl):
                bid_pkg = default_tmpl
        
        if not bid_pkg:
            t.fail("No bid package template found")
            return jsonify({"ok": False, "error": "No bid package template found. Upload one at /form-filler or place in data/templates/"}), 400
        
        out_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "output", sol)
        os.makedirs(out_dir, exist_ok=True)
        output_path = os.path.join(out_dir, f"{sol}_BidPackage_Reytech.pdf")
        
        rfq_data = {
            "solicitation_number": sol,
            "sign_date": get_pst_date(),
            "line_items": items,
        }
        
        fill_bid_package(bid_pkg, rfq_data, config, output_path)
        
        # Count food items
        from src.forms.food_classifier import get_food_items_for_obs1600
        food_items = get_food_items_for_obs1600(items)
        
        t.ok(f"Filled bid package: {len(items)} items, {len(food_items)} food items")
        
        return jsonify({
            "ok": True,
            "filename": os.path.basename(output_path),
            "download_url": f"/api/download/{sol}/{os.path.basename(output_path)}",
            "total_items": len(items),
            "food_items": len(food_items),
            "forms_filled": ["CUF", "Darfur", "Bidder Declaration", "DVBE", "Drug-Free", "CalRecycle", "OBS 1600"],
        })
    except Exception as e:
        import traceback
        t.fail(str(e))
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500


@bp.route("/api/fill-forms", methods=["POST"])
@auth_required
def api_fill_forms_standalone():
    """Standalone form filler — fill bid package from manually entered items.
    Body: {
        "solicitation_number": "...",
        "items": [{"line_number": 1, "description": "..."}],
        "fill_type": "all" | "obs1600_only"
    }
    """
    from src.api.trace import Trace
    t = Trace("fill_forms_standalone")
    
    try:
        from src.forms.reytech_filler_v4 import load_config, fill_bid_package, fill_obs1600, get_pst_date
        from src.forms.food_classifier import get_food_items_for_obs1600
        
        data = request.get_json(force=True)
        sol = data.get("solicitation_number", "STANDALONE")
        items = data.get("items", [])
        fill_type = data.get("fill_type", "all")
        
        if not items:
            return jsonify({"ok": False, "error": "No items provided"}), 400
        
        config = load_config()
        
        # Find template
        bid_pkg = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "templates", "cdcr_bid_package_template.pdf")
        if not os.path.exists(bid_pkg):
            return jsonify({"ok": False, "error": "No bid package template found. Upload cdcr_bid_package_template.pdf to data/templates/"}), 400
        
        out_dir = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "output", sol)
        os.makedirs(out_dir, exist_ok=True)
        
        rfq_data = {
            "solicitation_number": sol,
            "sign_date": get_pst_date(),
            "line_items": items,
        }
        
        food_items = get_food_items_for_obs1600(items)
        
        if fill_type == "obs1600_only":
            output_path = os.path.join(out_dir, f"{sol}_OBS1600_FoodCert_Reytech.pdf")
            fill_obs1600(bid_pkg, rfq_data, config, output_path, food_items=food_items)
        else:
            output_path = os.path.join(out_dir, f"{sol}_BidPackage_Reytech.pdf")
            fill_bid_package(bid_pkg, rfq_data, config, output_path)
        
        t.ok(f"Filled {fill_type}: {sol}, {len(food_items)} food items")
        
        return jsonify({
            "ok": True,
            "filename": os.path.basename(output_path),
            "download_url": f"/api/download/{sol}/{os.path.basename(output_path)}",
            "food_items": food_items,
            "food_count": len(food_items),
            "total_items": len(items),
        })
    except Exception as e:
        import traceback
        t.fail(str(e))
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500


@bp.route("/api/rfq/<rid>/price-intel")
@auth_required
def api_rfq_price_intel(rid):
    """Return pricing intelligence for all items in an RFQ."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False}), 404

    intel = []
    for item in r.get("line_items", []):
        desc = item.get("description", "")
        pn = item.get("item_number", "") or ""
        current_cost = item.get("supplier_cost") or 0
        current_bid = item.get("price_per_unit") or 0
        result = {"description": desc[:60], "part_number": pn}

        # Price history
        try:
            from src.core.db import get_price_history_db
            history = get_price_history_db(
                description=desc[:60] if not pn else "",
                part_number=pn, limit=10
            )
            if history:
                prices = [h["unit_price"] for h in history if h.get("unit_price")]
                result["history"] = {
                    "count": len(history),
                    "avg": round(sum(prices) / len(prices), 2) if prices else 0,
                    "min": round(min(prices), 2) if prices else 0,
                    "max": round(max(prices), 2) if prices else 0,
                    "entries": [{
                        "price": h["unit_price"],
                        "source": h.get("source", ""),
                        "date": h.get("found_at", "")[:10],
                        "quote": h.get("quote_number", ""),
                        "agency": h.get("agency", ""),
                    } for h in history[:5]]
                }

                # Freshness: compare current cost vs most recent history
                latest = history[0]
                latest_price = latest.get("unit_price", 0)
                latest_source = latest.get("source", "")
                latest_date = latest.get("found_at", "")[:10]
                try:
                    from datetime import datetime as _dt
                    days_old = (_dt.now() - _dt.fromisoformat(
                        latest["found_at"][:19])).days
                except Exception:
                    days_old = 999

                drift = None
                if current_cost > 0 and latest_price > 0 and latest_source not in ("rfq_save", "rfq_save_bid"):
                    diff = latest_price - current_cost
                    pct = diff / current_cost * 100
                    if abs(pct) > 3:  # Only flag >3% drift
                        drift = {
                            "direction": "up" if diff > 0 else "down",
                            "amount": round(abs(diff), 2),
                            "pct": round(pct, 1),
                            "new_price": latest_price,
                            "source": latest_source,
                        }

                result["freshness"] = {
                    "days_old": days_old,
                    "stale": days_old > 90,
                    "last_source": latest_source,
                    "last_date": latest_date,
                    "drift": drift,
                }
        except Exception:
            pass

        # Catalog match
        try:
            from src.core.catalog import search_catalog
            matches = search_catalog(pn or desc[:40], limit=1)
            if matches:
                m = matches[0]
                result["catalog"] = {
                    "sku": m.get("sku", ""),
                    "typical_cost": m.get("typical_cost", 0),
                    "list_price": m.get("list_price", 0),
                    "category": m.get("category", ""),
                }
        except Exception:
            pass

        # Audit trail
        try:
            from src.core.db import get_audit_trail
            audits = get_audit_trail(
                description=desc[:40],
                rfq_id=r.get("solicitation_number", ""), limit=5)
            if audits:
                result["audit"] = [{
                    "field": a["field_changed"],
                    "old": a.get("old_value"),
                    "new": a.get("new_value"),
                    "source": a.get("source", ""),
                    "ts": a.get("ts", "")[:16],
                } for a in audits]
        except Exception:
            pass

        # Pricing recommendation
        rec = _recommend_price(item)
        if rec:
            result["recommendation"] = rec

        # F6: Price conflict resolution — all known sources
        sources = {}
        if item.get("supplier_cost") and item["supplier_cost"] > 0:
            sources["Your Cost"] = round(item["supplier_cost"], 2)
        if item.get("scprs_last_price") and item["scprs_last_price"] > 0:
            sources["SCPRS"] = round(item["scprs_last_price"], 2)
        if item.get("amazon_price") and item["amazon_price"] > 0:
            sources["Amazon"] = round(item["amazon_price"], 2)
        if item.get("price_per_unit") and item["price_per_unit"] > 0:
            sources["Current Bid"] = round(item["price_per_unit"], 2)
        if result.get("catalog") and result["catalog"].get("typical_cost"):
            sources["Catalog"] = round(result["catalog"]["typical_cost"], 2)
        if result.get("catalog") and result["catalog"].get("list_price"):
            sources["Catalog List"] = round(result["catalog"]["list_price"], 2)
        if item.get("_from_pc"):
            sources["_from_pc"] = item["_from_pc"]
        if len(sources) > 1:
            result["sources"] = sources

        # F9: Duplicate item detection — same item quoted recently?
        try:
            from src.core.db import get_price_history_db
            pn = item.get("item_number", "") or ""
            recent = get_price_history_db(
                description=desc[:40] if not pn else "",
                part_number=pn, source="rfq_save_bid", limit=3
            )
            if recent:
                dupes = []
                for rh in recent:
                    dupes.append({
                        "price": rh.get("unit_price", 0),
                        "quote": rh.get("quote_number", ""),
                        "agency": rh.get("agency", ""),
                        "date": rh.get("found_at", "")[:10],
                    })
                if dupes:
                    result["recent_quotes"] = dupes
        except Exception:
            pass

        intel.append(result)

    return jsonify({"ok": True, "intel": intel})


@bp.route("/api/pricing-alerts")
@auth_required
def api_pricing_alerts():
    """F8: Dashboard pricing alerts — stale prices, drift, unpriced items."""
    from datetime import datetime as _dt, timedelta
    rfqs = load_rfqs()
    stale_rfqs = []
    unpriced_rfqs = []
    drift_items = 0
    now = _dt.now()

    for rid, r in rfqs.items():
        if r.get("status") in ("dismissed", "sent", "won", "lost", "cancelled"):
            continue
        items = r.get("line_items", [])
        if not items:
            continue

        # Check for unpriced items
        unpriced = sum(1 for it in items if not (it.get("price_per_unit") or 0) > 0)
        if unpriced == len(items):
            unpriced_rfqs.append({"id": rid, "sol": r.get("solicitation_number", ""), "items": len(items)})
            continue

        # Check for stale pricing (created > 14 days ago, never regenerated)
        try:
            created = r.get("created_at", "")
            if created:
                age = (now - _dt.fromisoformat(created[:19])).days
                if age > 14 and r.get("status") not in ("generated",):
                    stale_rfqs.append({
                        "id": rid, "sol": r.get("solicitation_number", ""),
                        "age_days": age, "items": len(items),
                    })
        except Exception:
            pass

    # Check price_history for recent drift
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Items with multiple prices where latest differs >10% from previous
            rows = conn.execute("""
                SELECT description, COUNT(*) as cnt,
                       MAX(unit_price) as max_p, MIN(unit_price) as min_p
                FROM price_history
                WHERE found_at > ?
                GROUP BY LOWER(SUBSTR(description, 1, 40))
                HAVING cnt > 1 AND (max_p - min_p) / min_p > 0.10
            """, ((now - timedelta(days=30)).isoformat(),)).fetchall()
            drift_items = len(rows)
    except Exception:
        pass

    total_alerts = len(stale_rfqs) + len(unpriced_rfqs) + (1 if drift_items > 0 else 0)
    return jsonify({
        "ok": True,
        "total_alerts": total_alerts,
        "stale_rfqs": stale_rfqs,
        "unpriced_rfqs": unpriced_rfqs,
        "drift_items": drift_items,
    })


@bp.route("/api/rfq/<rid>/qa-check")
@auth_required
def api_rfq_qa_check(rid):
    """QA gate: validate all items before package generation.
    Returns per-item pass/warn/fail with reasons."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False}), 404

    items = r.get("line_items", [])
    results = []
    overall = "pass"
    warnings = 0
    failures = 0

    for i, item in enumerate(items):
        checks = []
        item_status = "pass"
        desc = (item.get("description", "") or "")[:50]
        bid = item.get("price_per_unit") or 0
        cost = item.get("supplier_cost") or 0
        scprs = item.get("scprs_last_price") or 0
        qty = item.get("qty") or 0

        # 1: Bid price > 0
        if not bid or bid <= 0:
            checks.append({"check": "bid_price", "status": "fail", "msg": "No bid price"})
            item_status = "fail"
        else:
            checks.append({"check": "bid_price", "status": "pass", "msg": f"${bid:.2f}"})

        # 2: Supplier cost > 0
        if not cost or cost <= 0:
            checks.append({"check": "cost", "status": "warn", "msg": "No supplier cost"})
            if item_status != "fail":
                item_status = "warn"
        else:
            checks.append({"check": "cost", "status": "pass", "msg": f"${cost:.2f}"})

        # 3: Margin ≥ 15%
        if bid > 0 and cost > 0:
            margin = (bid - cost) / bid * 100
            if margin < 5:
                checks.append({"check": "margin", "status": "fail",
                               "msg": f"{margin:.1f}% — dangerously low"})
                item_status = "fail"
            elif margin < 15:
                checks.append({"check": "margin", "status": "warn",
                               "msg": f"{margin:.1f}% — below 15%"})
                if item_status != "fail":
                    item_status = "warn"
            else:
                checks.append({"check": "margin", "status": "pass",
                               "msg": f"{margin:.1f}%"})

        # 4: Bid vs SCPRS
        if bid > 0 and scprs > 0:
            diff_pct = (bid - scprs) / scprs * 100
            if diff_pct > 10:
                checks.append({"check": "scprs", "status": "warn",
                               "msg": f"{diff_pct:.0f}% above SCPRS ${scprs:.2f}"})
                if item_status != "fail":
                    item_status = "warn"
            elif diff_pct < -15:
                checks.append({"check": "scprs", "status": "warn",
                               "msg": f"{abs(diff_pct):.0f}% below SCPRS — leaving margin?"})
                if item_status != "fail":
                    item_status = "warn"
            else:
                checks.append({"check": "scprs", "status": "pass",
                               "msg": f"OK vs SCPRS ${scprs:.2f}"})

        # 5: Price freshness
        try:
            from src.core.db import get_price_history_db
            pn = item.get("item_number", "") or ""
            history = get_price_history_db(
                description=desc[:40] if not pn else "",
                part_number=pn, limit=1)
            if history:
                from datetime import datetime as _dt
                days = (_dt.now() - _dt.fromisoformat(
                    history[0]["found_at"][:19])).days
                if days > 90:
                    checks.append({"check": "freshness", "status": "warn",
                                   "msg": f"Price data {days}d old"})
                    if item_status != "fail":
                        item_status = "warn"
                else:
                    checks.append({"check": "freshness", "status": "pass",
                                   "msg": f"{days}d ago"})
        except Exception:
            pass

        # 6: Qty > 0
        if not qty or qty <= 0:
            checks.append({"check": "qty", "status": "warn", "msg": "Qty is 0"})
            if item_status != "fail":
                item_status = "warn"

        if item_status == "fail":
            failures += 1
            if overall != "fail":
                overall = "fail"
        elif item_status == "warn":
            warnings += 1
            if overall == "pass":
                overall = "warn"

        results.append({"idx": i, "description": desc,
                        "status": item_status, "checks": checks})

    # PC diff warnings
    diff_notes = []
    pc_diff = r.get("pc_diff")
    if pc_diff:
        if pc_diff.get("added"):
            diff_notes.append(f"{len(pc_diff['added'])} new items not in Price Check")
        if pc_diff.get("removed"):
            diff_notes.append(f"{len(pc_diff['removed'])} PC items not in RFQ")
        if pc_diff.get("qty_changed"):
            diff_notes.append(f"{len(pc_diff['qty_changed'])} qty changes from PC")

    return jsonify({"ok": True, "overall": overall, "failures": failures,
                    "warnings": warnings, "total": len(items), "items": results,
                    "linked_pc": r.get("linked_pc_number", ""),
                    "diff_notes": diff_notes})


@bp.route("/form-filler")
@auth_required
def form_filler_page():
    """Standalone form filler page."""
    return render_page("form_filler.html", active_page="Forms")


# ═══════════════════════════════════════════════════════════════════════
# Admin: Nuke & Re-poll RFQ
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/nuke/<rid>", methods=["POST"])
@auth_required
def api_nuke_rfq(rid):
    """Nuclear delete: wipe RFQ from JSON + SQLite + processed UIDs, then re-poll.
    Usage: POST /api/rfq/nuke/<rfq_id>  or  POST /api/rfq/nuke/<solicitation_number>
    """
    import json as _json
    from src.api.dashboard import load_rfqs, save_rfqs

    rfqs = load_rfqs()
    nuked = []

    # Find by ID or by solicitation number
    targets = {}
    for k, v in rfqs.items():
        if k == rid or v.get("solicitation_number", "") == rid or v.get("rfq_number", "") == rid:
            targets[k] = v

    if not targets:
        return jsonify({"ok": False, "error": f"No RFQ found matching '{rid}'"}), 404

    for rfq_id, rfq in targets.items():
        sol = rfq.get("solicitation_number", rfq.get("rfq_number", "?"))
        email_uid = rfq.get("email_uid", "")

        # 1. Remove from JSON
        if rfq_id in rfqs:
            del rfqs[rfq_id]

        # 2. Remove from SQLite (rfqs, rfq_files, email_log, price_checks)
        try:
            with get_db() as conn:
                conn.execute("DELETE FROM rfq_files WHERE rfq_id = ?", (rfq_id,))
                conn.execute("DELETE FROM rfqs WHERE id = ?", (rfq_id,))
                # email_log by rfq_id
                conn.execute("DELETE FROM email_log WHERE rfq_id = ?", (rfq_id,))
                # price_checks by rfq_id
                conn.execute("DELETE FROM price_checks WHERE rfq_id = ?", (rfq_id,))
                conn.commit()
        except Exception as e:
            log.warning("Nuke SQLite cleanup for %s: %s", rfq_id, e)

        # 3. Remove email UID from processed list
        if email_uid:
            try:
                from src.api.modules.routes_pricecheck import _remove_processed_uid
                _remove_processed_uid(email_uid)
            except Exception:
                # Manual fallback
                proc_file = os.path.join(DATA_DIR, "processed_emails.json")
                try:
                    if os.path.exists(proc_file):
                        with open(proc_file) as f:
                            processed = _json.load(f)
                        if isinstance(processed, list) and email_uid in processed:
                            processed.remove(email_uid)
                        elif isinstance(processed, dict) and email_uid in processed:
                            del processed[email_uid]
                        with open(proc_file, "w") as f:
                            _json.dump(processed, f)
                except Exception:
                    pass

        nuked.append({"id": rfq_id, "sol": sol, "uid": email_uid})
        log.info("NUKED RFQ %s (sol=%s, uid=%s)", rfq_id, sol, email_uid)

    save_rfqs(rfqs)

    # 4. Trigger re-poll
    poll_result = None
    try:
        from src.api.modules.routes_pricecheck import do_poll_check
        imported = do_poll_check()
        poll_result = {"found": len(imported), "rfqs": [r.get("solicitation_number", "?") for r in imported]}
    except Exception as e:
        poll_result = {"error": str(e)}

    return jsonify({
        "ok": True,
        "nuked": nuked,
        "poll": poll_result,
    })


@bp.route("/api/rfq/<rid>/clear-quote", methods=["POST", "GET"])
@auth_required
def api_rfq_clear_quote(rid):
    """Clear the quote number on an RFQ so regeneration assigns a new one."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})
    
    old_qn = r.get("reytech_quote_number", "")
    r["reytech_quote_number"] = ""
    r["linked_quote_number"] = ""
    save_rfqs(rfqs)
    
    return jsonify({"ok": True, "cleared": old_qn, "message": f"Cleared {old_qn}. Regenerate to get a new number."})


@bp.route("/api/rfq/<rid>/clear-generated", methods=["POST", "GET"])
@auth_required
def api_rfq_clear_generated(rid):
    """
    Force-clear all generated files for an RFQ from both DB and JSON.
    Resets status to 'ready' so the full generate-package pipeline re-runs cleanly.
    Use this when Railway redeploys cached the old output and Regenerate doesn't help.
    """
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    # Clear DB generated files
    db_deleted = 0
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cur = conn.execute(
                "DELETE FROM rfq_files WHERE rfq_id = ? AND category = 'generated'",
                (rid,)
            )
            db_deleted = cur.rowcount
    except Exception as _e:
        log.warning("clear-generated DB delete failed for %s: %s", rid, _e)

    # Clear disk output files
    sol = r.get("solicitation_number", rid)
    out_dir = os.path.join(OUTPUT_DIR, sol)
    disk_deleted = 0
    if os.path.exists(out_dir):
        try:
            for fname in os.listdir(out_dir):
                fpath = os.path.join(out_dir, fname)
                if os.path.isfile(fpath):
                    os.remove(fpath)
                    disk_deleted += 1
        except Exception as _de:
            log.warning("clear-generated disk delete failed: %s", _de)

    # Reset JSON state
    old_files = r.get("output_files", [])
    r["output_files"] = []
    r.pop("draft_email", None)
    r.pop("generated_at", None)
    _transition_status(r, "ready", actor="user", notes="Cleared generated files for fresh regeneration")
    save_rfqs(rfqs)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "ready")
    except Exception:
        pass

    msg = f"Cleared {db_deleted} DB files + {disk_deleted} disk files. Status reset to 'ready'. Click Generate Package to rebuild."
    log.info("clear-generated %s: %s", rid, msg)
    return jsonify({"ok": True, "db_deleted": db_deleted, "disk_deleted": disk_deleted,
                    "old_files": old_files, "message": msg})


@bp.route("/api/rfq/<rid>/clean-slate", methods=["POST", "GET"])
@auth_required
def api_rfq_clean_slate(rid):
    """Nuclear clean: keep ONLY line_items with pricing. Clear everything else.
    Use when package is broken — stale templates, wrong forms, old data."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    # Preserve line items with all pricing fields
    items = r.get("line_items", [])
    preserved_items = []
    for it in items:
        preserved_items.append({
            "line_number": it.get("line_number", 0),
            "qty": it.get("qty", 1),
            "uom": it.get("uom", "EA"),
            "description": it.get("description", ""),
            "item_number": it.get("item_number", ""),
            "supplier_cost": it.get("supplier_cost", 0),
            "price_per_unit": it.get("price_per_unit", 0),
            "markup_pct": it.get("markup_pct"),
            "scprs_last_price": it.get("scprs_last_price"),
            "amazon_price": it.get("amazon_price"),
            "item_link": it.get("item_link", ""),
            "item_supplier": it.get("item_supplier", ""),
            "_desc_source": it.get("_desc_source", ""),
        })

    # Preserve core RFQ identity
    sol = r.get("solicitation_number", "")
    identity = {
        "solicitation_number": sol,
        "agency": r.get("agency", ""),
        "requestor_name": r.get("requestor_name", ""),
        "requestor_email": r.get("requestor_email", ""),
        "delivery_location": r.get("delivery_location", ""),
        "due_date": r.get("due_date", ""),
        "institution": r.get("institution", ""),
        "ship_to": r.get("ship_to", ""),
        "created_at": r.get("created_at", ""),
        "source": r.get("source", ""),
        "linked_pc_id": r.get("linked_pc_id", ""),
        "reytech_quote_number": r.get("reytech_quote_number", ""),
    }

    # Clear DB files (generated + templates)
    db_deleted = 0
    try:
        from src.core.db import get_db
        with get_db() as conn:
            cur = conn.execute(
                "DELETE FROM rfq_files WHERE rfq_id = ? AND category IN ('generated', 'template')",
                (rid,)
            )
            db_deleted = cur.rowcount
    except Exception as _e:
        log.warning("clean-slate DB: %s", _e)

    # Clear disk
    disk_deleted = 0
    import shutil as _sh2
    out_dir = os.path.join(OUTPUT_DIR, sol)
    if os.path.exists(out_dir):
        try:
            _sh2.rmtree(out_dir)
            disk_deleted += 1
        except Exception:
            pass
    tmpl_dir = os.path.join(DATA_DIR, "rfq_templates", rid)
    if os.path.exists(tmpl_dir):
        try:
            _sh2.rmtree(tmpl_dir)
            disk_deleted += 1
        except Exception:
            pass

    # Rebuild RFQ with clean state
    r.clear()
    r.update(identity)
    r["line_items"] = preserved_items
    r["templates"] = {}
    r["output_files"] = []
    r["status"] = "ready"

    save_rfqs(rfqs)
    try:
        from src.core.dal import update_rfq_status as _dal_ur
        _dal_ur(rid, "ready")
    except Exception:
        pass

    log.info("clean-slate %s: kept %d items, cleared %d DB + %d disk",
             rid, len(preserved_items), db_deleted, disk_deleted)
    return jsonify({
        "ok": True,
        "items_preserved": len(preserved_items),
        "db_cleared": db_deleted,
        "disk_cleared": disk_deleted,
        "message": f"Clean slate: {len(preserved_items)} items preserved with pricing. All docs/templates cleared. Ready to regenerate.",
    })


@bp.route("/api/rfq/<rid>/debug-pages", methods=["GET"])
@auth_required
def api_rfq_debug_pages(rid):
    """Debug: run page-skip logic against last generated package PDF. Returns per-page decisions."""
    from src.forms.reytech_filler_v4 import _bidpkg_page_skip_reason
    from pypdf import PdfReader

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    sol = r.get("solicitation_number", rid)
    pkg_path = os.path.join(OUTPUT_DIR, sol, f"RFQ_Package_{sol}_ReytechInc.pdf")

    if not os.path.exists(pkg_path):
        return jsonify({"ok": False, "error": f"Not found: {pkg_path}"})

    reader = PdfReader(pkg_path)
    pages = []
    for i, page in enumerate(reader.pages):
        try:
            reason = _bidpkg_page_skip_reason(page)
        except Exception as e:
            reason = f"ERROR: {e}"
        text_snip = (page.extract_text() or "")[:80].replace("\n", " ")
        n_fields = len(page.get("/Annots", [])) if "/Annots" in page else 0
        pages.append({"page": i, "decision": "SKIP" if reason else "KEEP",
                      "reason": reason or "", "fields": n_fields, "text": text_snip})

    return jsonify({"ok": True, "total": len(pages),
                    "kept": sum(1 for p in pages if p["decision"] == "KEEP"),
                    "skipped": sum(1 for p in pages if p["decision"] == "SKIP"),
                    "pages": pages})


@bp.route("/api/rfq/<rid>/debug-templates", methods=["GET"])
@auth_required
def api_rfq_debug_templates(rid):
    """Dump all field names from uploaded 703B/704B/bidpkg templates. Use to diagnose fill mismatches."""
    from pypdf import PdfReader

    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    result = {}
    tmpl = r.get("templates", {})

    # Also restore from DB if needed
    db_files = list_rfq_files(rid, category="template")
    for db_f in db_files:
        ft = db_f.get("file_type", "").lower().replace("template_", "")
        fname = db_f.get("filename", "").lower()
        ttype = None
        if "703b" in ft or "703b" in fname: ttype = "703b"
        elif "704b" in ft or "704b" in fname: ttype = "704b"
        elif "bid" in ft or "bid" in fname: ttype = "bidpkg"
        if ttype and (ttype not in tmpl or not os.path.exists(tmpl.get(ttype, ""))):
            full_f = get_rfq_file(db_f["id"])
            if full_f and full_f.get("data"):
                restore_dir = os.path.join(DATA_DIR, "rfq_templates", rid)
                os.makedirs(restore_dir, exist_ok=True)
                restore_path = os.path.join(restore_dir, db_f["filename"])
                with open(restore_path, "wb") as _fw:
                    _fw.write(full_f["data"])
                tmpl[ttype] = restore_path

    for tname, tpath in tmpl.items():
        if not os.path.exists(tpath):
            result[tname] = {"error": f"file missing: {tpath}"}
            continue
        try:
            rdr = PdfReader(tpath)
            fields = rdr.get_fields() or {}
            sig_fields = []
            all_pages = []
            for i, pg in enumerate(rdr.pages):
                annots = pg.get("/Annots", [])
                pg_fields = []
                for a in (annots or []):
                    obj = a.get_object() if hasattr(a, "get_object") else a
                    name = str(obj.get("/T", ""))
                    ft_val = str(obj.get("/FT", ""))
                    if ft_val == "/Sig" or "sig" in name.lower():
                        sig_fields.append({"name": name, "ft": ft_val, "page": i})
                    if name:
                        pg_fields.append(name)
                all_pages.append({"page": i, "fields": pg_fields[:10]})
            result[tname] = {
                "path": tpath,
                "pages": len(rdr.pages),
                "total_fields": len(fields),
                "sig_fields": sig_fields,
                "all_field_names": sorted(fields.keys())[:50],
                "pages_preview": all_pages,
            }
        except Exception as e:
            result[tname] = {"error": str(e)}

    return jsonify({"ok": True, "templates": result})


@bp.route("/api/rfq/<rid>/diag-package")
@auth_required
def api_diag_package(rid):
    """Diagnostic: test each form generation step and report what works/fails."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        return jsonify({"ok": False, "error": "RFQ not found"})

    results = {"rid": rid, "items": len(r.get("line_items", [])), "steps": []}
    
    # Check agency
    try:
        from src.core.agency_config import match_agency
        _agency_key, _agency_cfg = match_agency(r)
        _req = _agency_cfg.get("required_forms", [])
        results["agency"] = _agency_key
        results["required_forms"] = _req
        results["steps"].append({"step": "agency_match", "ok": True, "agency": _agency_key, "forms": _req})
    except Exception as e:
        results["steps"].append({"step": "agency_match", "ok": False, "error": str(e)})
        return jsonify(results)
    
    # Check templates dir
    import os
    tdir = os.path.join(os.environ.get("DATA_DIR", "data"), "templates")
    if os.path.exists(tdir):
        files = os.listdir(tdir)
        results["steps"].append({"step": "templates_dir", "ok": True, "files": files})
    else:
        results["steps"].append({"step": "templates_dir", "ok": False, "error": f"{tdir} not found"})
    
    # Check quote generator
    try:
        results["steps"].append({"step": "quote_gen_import", "ok": True})
    except Exception as e:
        results["steps"].append({"step": "quote_gen_import", "ok": False, "error": str(e)})
    
    # Check each required form's generator
    form_checks = {
        "calrecycle74": ("src.forms.reytech_filler_v4", "fill_calrecycle_standalone"),
        "std204": ("src.forms.reytech_filler_v4", "fill_std204"),
        "std1000": ("src.forms.reytech_filler_v4", "fill_std1000"),
        "dvbe843": ("src.forms.reytech_filler_v4", "generate_dvbe_843"),
        "bidder_decl": ("src.forms.reytech_filler_v4", "generate_bidder_declaration"),
        "darfur_act": ("src.forms.reytech_filler_v4", "generate_darfur_act"),
        "cv012_cuf": ("src.forms.reytech_filler_v4", "fill_cv012_cuf"),
    }
    for form_id, (mod, func) in form_checks.items():
        if form_id in _req:
            try:
                m = __import__(mod, fromlist=[func])
                fn = getattr(m, func)
                results["steps"].append({"step": f"import_{form_id}", "ok": True, "func": func})
            except Exception as e:
                results["steps"].append({"step": f"import_{form_id}", "ok": False, "error": str(e)})
    
    # Check CONFIG
    try:
        from src.api.modules.routes_rfq import CONFIG
        results["steps"].append({"step": "config", "ok": True, "company": CONFIG.get("company", {}).get("name", "?")})
    except Exception as e:
        results["steps"].append({"step": "config", "ok": False, "error": str(e)})
    
    # Check line items have pricing
    items = r.get("line_items", [])
    priced = sum(1 for i in items if i.get("price_per_unit") and i["price_per_unit"] > 0)
    results["steps"].append({"step": "pricing", "items": len(items), "priced": priced})

    return jsonify(results)


# ══ Consolidated from routes_features*.py ══════════════════════════════════


# ═══════════════════════════════════════════════════════════════════════
# Email Draft Queue Status
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/email/queue-status")
@auth_required
def api_email_queue_status():
    """Status of email drafts: pending, approved, sent."""
    outbox_path = os.path.join(DATA_DIR, "outbox.json")
    try:
        with open(outbox_path) as f:
            outbox = json.load(f)
    except Exception:
        outbox = []

    if isinstance(outbox, dict):
        outbox = list(outbox.values())

    draft = [e for e in outbox if (e.get("status") or "").lower() in ("draft", "pending")]
    approved = [e for e in outbox if (e.get("status") or "").lower() == "approved"]
    sent = [e for e in outbox if (e.get("status") or "").lower() == "sent"]

    return jsonify({
        "ok": True,
        "drafts": len(draft),
        "approved": len(approved),
        "sent": len(sent),
        "total": len(outbox),
        "needs_review": len(draft),
        "ready_to_send": len(approved),
        "recent_drafts": [
            {"to": e.get("to", "?"), "subject": e.get("subject", "?")[:50],
             "created": e.get("created", "?"), "type": e.get("type", "?")}
            for e in sorted(draft, key=lambda x: x.get("created", ""), reverse=True)[:5]
        ]
    })


# ═══════════════════════════════════════════════════════════════════════
# RFQs Ready to Quote
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/rfq/ready-to-quote")
@auth_required
def api_rfq_ready_to_quote():
    """RFQs that need pricing/quoting — prioritized by deadline."""
    rfqs_path = os.path.join(DATA_DIR, "rfqs.json")
    if not os.path.exists(rfqs_path):
        return jsonify({"ok": True, "rfqs": [], "count": 0})

    try:
        with open(rfqs_path) as f:
            rfqs = json.load(f)
    except Exception:
        return jsonify({"ok": True, "rfqs": [], "count": 0})

    today = datetime.now().strftime("%Y-%m-%d")
    ready = []

    for rid, r in rfqs.items():
        status = (r.get("status") or "").lower()
        if status in ("new", "draft", "priced", "inbox"):
            due = r.get("due_date") or r.get("deadline") or ""
            sol = r.get("solicitation_number", rid)
            items = r.get("line_items") or r.get("items_detail") or []
            if isinstance(items, str):
                try: items = json.loads(items)
                except Exception: items = []

            overdue = due and due < today
            days_left = None
            if due:
                try:
                    dd = datetime.strptime(due[:10], "%Y-%m-%d")
                    days_left = (dd - datetime.now()).days
                except Exception: pass

            ready.append({
                "id": rid,
                "solicitation": sol[:30],
                "requestor": r.get("requestor", r.get("buyer_name", "?")),
                "institution": r.get("institution", "?"),
                "status": status.upper(),
                "items": len(items) if isinstance(items, list) else 0,
                "due": due[:10] if due else "TBD",
                "days_left": days_left,
                "overdue": overdue,
                "total": r.get("total_price", 0),
            })

    # Sort: overdue first, then by days_left
    ready.sort(key=lambda x: (not x["overdue"], x["days_left"] if x["days_left"] is not None else 999))

    return jsonify({
        "ok": True,
        "rfqs": ready[:20],
        "count": len(ready),
        "overdue": len([r for r in ready if r["overdue"]]),
        "due_this_week": len([r for r in ready if r.get("days_left") is not None and 0 <= r["days_left"] <= 7])
    })
