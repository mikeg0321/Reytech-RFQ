# RFQ + Quote Routes
# 9 routes, 484 lines
# Loaded by dashboard.py via load_module()

@bp.route("/")
@auth_required
def home():
    all_pcs = _load_price_checks()
    # Use canonical filter — auto-price PCs belong to RFQ rows, not PC queue
    from src.api.dashboard import _is_user_facing_pc
    user_pcs = {k: v for k, v in all_pcs.items() if _is_user_facing_pc(v)}
    # Sort by quote number (R26Q17 → 17) descending so newest quotes appear first
    def _pc_sort_key(item):
        pc = item[1]
        qn = pc.get("reytech_quote_number", "") or ""
        # Extract numeric part from R26Q17 → 17
        import re as _re
        m = _re.search(r'Q(\d+)', qn)
        if m:
            return int(m.group(1))
        # Fallback to created_at timestamp
        return pc.get("created_at", "")
    sorted_pcs = dict(sorted(user_pcs.items(), key=_pc_sort_key, reverse=True))
    # Filter dismissed RFQs from active queue
    active_rfqs = {k: v for k, v in load_rfqs().items() if v.get("status") != "dismissed"}
    return render(PAGE_HOME, rfqs=active_rfqs, price_checks=sorted_pcs, include_brief=True)

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
    _transition_status(rfq, "pending", actor="system", notes="Parsed from email")
    rfq["source"] = "upload"
    
    # Auto SCPRS lookup
    rfq["line_items"] = bulk_lookup(rfq.get("line_items", []))
    
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
    
    Option C: Creates PC record ONLY. No quote number assigned.
    User clicks "Generate Quote" when ready → that's when R26Qxx is consumed.
    
    Args:
        from_email: If True, returns dict instead of redirect (email pipeline call)
    """
    # Save to data dir for persistence
    pc_file = os.path.join(DATA_DIR, f"pc_upload_{os.path.basename(pdf_path)}")
    shutil.copy2(pdf_path, pc_file)

    # Parse
    parsed = parse_ams704(pc_file)
    parse_error = parsed.get("error")
    
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
                "status_history": [{"from": "", "to": "parse_error", "timestamp": datetime.now().isoformat(), "actor": "system"}],
                "created_at": datetime.now().isoformat(),
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
    pc_num = parsed.get("header", {}).get("price_check_number", "unknown")
    institution = parsed.get("header", {}).get("institution", "")
    due_date = parsed.get("header", {}).get("due_date", "")

    # ── DEDUP CHECK: same PC number + institution + due date = true duplicate ──
    # Must include due_date because same institution sends different PCs
    # (e.g. Valentina sends Airway Adapter AND BLS Med from CSP-Sacramento)
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

    # Option C: NO quote number assigned here. User generates manually.
    # Save PC record
    pcs = _load_price_checks()
    pcs[pc_id] = {
        "id": pc_id,
        "pc_number": pc_num,
        "institution": institution,
        "due_date": due_date,
        "requestor": parsed.get("header", {}).get("requestor", ""),
        "ship_to": parsed.get("ship_to", ""),
        "items": items,
        "source_pdf": pc_file,
        "status": "parsed",
        "status_history": [{"from": "", "to": "parsed", "timestamp": datetime.now().isoformat(), "actor": "system"}],
        "created_at": datetime.now().isoformat(),
        "parsed": parsed,
        "reytech_quote_number": "",  # Empty until user clicks Generate Quote
        "linked_quote_number": "",
    }
    _save_price_checks(pcs)

    log.info("PC #%s created from %s — %d items, due %s (no quote assigned — Option C)",
             pc_num, institution, len(items), due_date)
    
    if from_email:
        return {"ok": True, "pc_id": pc_id, "pc_number": pc_num, "items": len(items)}
    
    flash(f"Price Check #{pc_num} parsed — {len(items)} items from {institution}. Due {due_date}", "success")
    return redirect(f"/pricecheck/{pc_id}")


def _load_price_checks():
    path = os.path.join(DATA_DIR, "price_checks.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception as e:
            log.debug("Suppressed: %s", e)
            return {}
    return {}


def _save_price_checks(pcs):
    path = os.path.join(DATA_DIR, "price_checks.json")
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
    
    return render(PAGE_DETAIL, r=r, rid=rid)


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
            except Exception:
                pass
    
    _transition_status(r, "ready", actor="user", notes="Pricing updated")
    save_rfqs(rfqs)
    
    # Save SCPRS prices for future lookups
    save_prices_from_rfq(r)
    
    _log_rfq_activity(rid, "pricing_saved",
        f"Pricing updated for #{r.get('solicitation_number','?')} ({len(r.get('line_items',[]))} items)",
        actor="user")
    
    flash("Pricing saved", "success")
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
                except Exception:
                    pass

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
                except Exception:
                    pass
    
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
    except Exception:
        pass
    
    save_rfqs(rfqs)
    
    # Build success message
    parts = []
    for f in output_files:
        if "703B" in f: parts.append("703B")
        elif "704B" in f: parts.append("704B")
        elif "BidPackage" in f: parts.append("Bid Package")
        elif "Quote" in f: parts.append(f"Quote #{r.get('reytech_quote_number', '?')}")
    
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
        except Exception:
            pass
    
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
        except Exception:
            pass
    
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
        except Exception:
            pass
        
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