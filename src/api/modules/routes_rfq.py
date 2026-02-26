# RFQ + Quote Routes
# 9 routes, 484 lines
# Loaded by dashboard.py via load_module()

@bp.route("/health")
def health_check():
    """Health check endpoint for Railway/load balancers. No auth required."""
    import sqlite3
    checks = {"status": "ok", "timestamp": datetime.now().isoformat()}
    # Check SQLite
    try:
        db_path = os.path.join(DATA_DIR, "reytech.db")
        conn = sqlite3.connect(db_path, timeout=5)
        conn.execute("SELECT 1")
        conn.close()
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
    all_pcs = _load_price_checks()
    # Use canonical filter — auto-price PCs belong to RFQ rows, not PC queue
    from src.api.dashboard import _is_user_facing_pc
    user_pcs = {k: v for k, v in all_pcs.items() if _is_user_facing_pc(v)}
    # Sort by URGENCY: overdue first, then soonest due date, then newest
    def _pc_sort_key(item):
        pc = item[1]
        due = pc.get("due_date", "") or ""
        status = pc.get("status", "")
        now_str = datetime.now().strftime("%m/%d/%y")
        # Terminal statuses go to bottom
        if status in ("won", "lost", "dismissed", "archived", "expired"):
            return (3, "9999-99-99", "")
        # Parse due date and compute urgency
        urgency = 1  # default: normal
        try:
            for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    d = datetime.strptime(due.strip(), fmt)
                    days_left = (d - datetime.now()).days
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
    _today = datetime.now()
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
    active_rfqs = {k: v for k, v in load_rfqs().items() if v.get("status") != "dismissed"}
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
    path = os.path.join(DATA_DIR, "price_checks.json")
    if os.path.exists(path):
        try:
            import fcntl
            with open(path) as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                data = json.load(f)
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                return data
        except ImportError:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            log.debug("Suppressed: %s", e)
            return {}
    return {}


def _save_price_checks(pcs):
    path = os.path.join(DATA_DIR, "price_checks.json")
    try:
        import fcntl
        with open(path, "w") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            json.dump(pcs, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
    except ImportError:
        with open(path, "w") as f:
            json.dump(pcs, f, indent=2, default=str)


@bp.route("/rfq/<rid>")
@auth_required
def detail(rid):
    # Check if this is actually a price check
    pcs = _load_price_checks()
    if rid in pcs:
        return redirect(f"/pricecheck/{rid}")
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: flash("Not found", "error"); return redirect("/")
    
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
        save_rfqs(rfqs)
    
    return render_page("rfq_detail.html", active_page="Home", r=r, rid=rid)


@bp.route("/rfq/<rid>/update", methods=["POST"])
@auth_required
def update(rid):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    
    for i, item in enumerate(r["line_items"]):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit")]:
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
    
    # Save SCPRS prices for future lookups
    save_prices_from_rfq(r)
    
    _log_rfq_activity(rid, "pricing_saved",
        f"Pricing updated for #{r.get('solicitation_number','?')} ({len(r.get('line_items',[]))} items)",
        actor="user")
    
    flash("Pricing saved", "success")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/add-item", methods=["POST"])
@auth_required
def rfq_add_item(rid):
    """Add a line item to an RFQ (for generic/Cal Vet RFQs or manual entry)."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")

    if "line_items" not in r:
        r["line_items"] = []

    next_num = max((it.get("line_number", 0) for it in r["line_items"]), default=0) + 1

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
def rfq_remove_item(rid, idx):
    """Remove a line item from an RFQ by index."""
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r:
        flash("RFQ not found", "error"); return redirect("/")

    items = r.get("line_items", [])
    if 0 <= idx < len(items):
        removed = items.pop(idx)
        # Re-number remaining items
        for i, it in enumerate(items):
            it["line_number"] = i + 1
        save_rfqs(rfqs)
        _log_rfq_activity(rid, "item_removed",
            f"Line item removed: {removed.get('description','')[:60]}",
            actor="user")
        flash("Item removed", "success")
    else:
        flash("Invalid item index", "error")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/upload-templates", methods=["POST"])
@auth_required
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
    
    # ── Step 1: Save pricing from form ──
    for i, item in enumerate(r.get("line_items", [])):
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit")]:
            v = request.form.get(f"{field}_{i}")
            if v:
                try:
                    item[key] = float(v)
                except Exception as _e:
                    log.debug("Suppressed: %s", _e)
    
    r["sign_date"] = get_pst_date()
    safe_sol = re.sub(r'[^a-zA-Z0-9_-]', '_', sol.strip())
    out_dir = os.path.join(OUTPUT_DIR, sol)
    os.makedirs(out_dir, exist_ok=True)
    
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
        
        # Update templates in RFQ data
        r["templates"] = tmpl
        
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
    except Exception as e:
        errors.append(f"State forms: {e}")
        t.warn("State forms exception", error=str(e))
    
    # ── Step 3: Generate Reytech Quote on letterhead ──
    if QUOTE_GEN_AVAILABLE:
        try:
            quote_path = os.path.join(out_dir, f"{safe_sol}_Quote_Reytech.pdf")
            locked_qn = r.get("reytech_quote_number", "")
            
            result = generate_quote_from_rfq(
                r, quote_path,
                include_tax=r.get("tax_enabled", False),
                tax_rate=r.get("tax_rate", 0.0725) if r.get("tax_enabled") else 0.0,
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
    
    if not output_files:
        t.fail("No files generated", errors=errors)
        flash(f"No files generated — {'; '.join(errors) if errors else 'No templates found'}", "error")
        return redirect(f"/rfq/{rid}")
    
    # ── Step 3.5: For generic agencies (Cal Vet, etc.), include original RFQ attachments ──
    # These agencies don't have 704B forms — we send our Reytech quote PLUS their
    # original RFQ package docs back to them.
    if r.get("form_type") == "generic_rfq":
        import shutil as _sh2
        # Include any rfq_package or unknown-type attachments from the original email
        db_attachments = list_rfq_files(rid, category="attachment") + list_rfq_files(rid, category="template")
        for db_f in db_attachments:
            fname = db_f.get("filename", "")
            # Skip if we already generated a file with this name
            if fname in output_files:
                continue
            # Skip non-PDF
            if not fname.lower().endswith(".pdf"):
                continue
            try:
                full_f = get_rfq_file(db_f["id"])
                if full_f and full_f.get("data"):
                    att_path = os.path.join(out_dir, fname)
                    with open(att_path, "wb") as _fw:
                        _fw.write(full_f["data"])
                    output_files.append(fname)
                    t.step(f"Included original: {fname}")
            except Exception as _ae:
                t.warn(f"Could not include {fname}", error=str(_ae))
    
    # ── Step 4: Store generated PDFs in DB (survive redeploys) ──
    for f in output_files:
        fpath = f"{out_dir}/{f}"
        try:
            if os.path.exists(fpath):
                with open(fpath, "rb") as _fb:
                    ftype = "generated_quote" if "Quote" in f else \
                            "generated_703b" if "703B" in f else \
                            "generated_704b" if "704B" in f else \
                            "generated_bidpkg" if "BidPackage" in f else "generated_other"
                    save_rfq_file(rid, f, ftype, _fb.read(), category="generated", uploaded_by="user")
                    t.step(f"DB stored: {f}")
        except Exception as _de:
            t.warn(f"DB store failed: {f}", error=str(_de))
    
    # ── Step 5: Save, transition, create draft email ──
    _transition_status(r, "generated", actor="user", notes=f"Package: {len(output_files)} files")
    r["output_files"] = output_files
    r["generated_at"] = datetime.now().isoformat()
    
    # Draft email with ALL files attached
    try:
        sender = EmailSender(CONFIG.get("email", {}))
        all_paths = [f"{out_dir}/{f}" for f in output_files]
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
    
    # Build success message
    parts = []
    is_generic = r.get("form_type") == "generic_rfq"
    for f in output_files:
        if "703B" in f: parts.append("703B")
        elif "704B" in f: parts.append("704B")
        elif "BidPackage" in f: parts.append("Bid Package")
        elif "Quote" in f: parts.append(f"Quote #{r.get('reytech_quote_number', '?')}")
        else: parts.append(os.path.basename(f))
    
    if is_generic:
        agency = r.get('agency_name', 'Agency')
        msg = f"✅ {agency} quote package ready: {', '.join(parts)}"
    else:
        msg = f"✅ RFQ Package generated: {', '.join(parts)}"
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
        for field, key in [("cost", "supplier_cost"), ("scprs", "scprs_last_price"), ("price", "price_per_unit")]:
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
    
    try:
        sender = EmailSender(CONFIG.get("email", {}))
        sender.send(r["draft_email"])
        _transition_status(r, "sent", actor="user", notes="Email sent to buyer")
        r["sent_at"] = datetime.now().isoformat()
        save_rfqs(rfqs)
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


@bp.route("/api/rfq/<rid>/dismiss", methods=["POST"])
@auth_required
def api_rfq_dismiss(rid):
    """Dismiss an RFQ from the active queue with a reason.
    Keeps data for SCPRS intelligence. reason=delete does hard delete."""
    data = request.get_json(force=True) if request.data else {}
    reason = data.get("reason", "other")
    
    rfqs = load_rfqs()
    if rid not in rfqs:
        return jsonify({"ok": False, "error": "RFQ not found"})
    
    # Hard delete path
    if reason == "delete":
        sol = rfqs[rid].get("solicitation_number", "?")
        email_uid = rfqs[rid].get("email_uid")
        if email_uid:
            _remove_processed_uid(email_uid)
        del rfqs[rid]
        save_rfqs(rfqs)
        log.info("Hard deleted RFQ #%s (id=%s)", sol, rid)
        return jsonify({"ok": True, "deleted": rid})
    
    r = rfqs[rid]
    r["status"] = "dismissed"
    r["dismiss_reason"] = reason
    r["dismissed_at"] = datetime.now().isoformat()
    rfqs[rid] = r
    save_rfqs(rfqs)
    
    sol = r.get("solicitation_number", "?")
    log.info("RFQ #%s dismissed: reason=%s", sol, reason)
    _log_rfq_activity(rid, "dismissed", f"RFQ #{sol} dismissed: {reason}", actor="user")
    
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
    
    _log_rfq_activity(rid, "reopened",
        f"RFQ #{r.get('solicitation_number','?')} reopened for editing (was: {old_status})",
        actor="user", metadata={"old_status": old_status})
    
    flash(f"RFQ reopened for editing (was: {old_status})", "info")
    return redirect(f"/rfq/{rid}")


@bp.route("/rfq/<rid>/update-status", methods=["POST"])
@auth_required
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
    data = request.get_json() or request.form
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
    data = request.get_json()
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
    return jsonify({
        "ok": True,
        "signature_html": email_cfg.get("signature_html", ""),
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

# ═══════════════════════════════════════════════════════════════════════
# Enhanced Email Send — DB attachments + email logging + CRM tracking
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/rfq/<rid>/send-email", methods=["POST"])
@auth_required
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
        from src.forms.food_classifier import classify_quote_items, is_food_item
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
        from src.forms.reytech_filler_v4 import load_config, fill_obs1600, fill_obs1600_fields, get_pst_date, fill_and_sign_pdf
        from src.forms.food_classifier import classify_food_item, get_food_items_for_obs1600
        
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
                except: items = []
        
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
                for search_dir in ["data/uploads", "data/rfq_templates", f"data/output/{sol}"]:
                    matches = glob.glob(os.path.join(os.path.dirname(__file__), "..", "..", "..", search_dir, pattern))
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
                        except:
                            pass
                        if bid_pkg: break
                    if bid_pkg: break
                if bid_pkg: break
        
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
