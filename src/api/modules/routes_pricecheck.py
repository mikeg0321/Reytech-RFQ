import json as _json
# Price Check Routes
# 26 routes, 985 lines
# Loaded by dashboard.py via load_module()

# Price Check Pages (v6.2)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/pricecheck/<pcid>")
@auth_required
def pricecheck_detail(pcid):
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        flash("Price Check not found", "error"); return redirect("/")

    items = pc.get("items", [])
    header = pc.get("parsed", {}).get("header", {})

    items_html = ""
    for idx, item in enumerate(items):
        p = item.get("pricing", {})
        # Clean description for display (strip font specs, dimensions, etc.)
        raw_desc = item.get("description_raw") or item.get("description", "")
        display_desc = item.get("description", raw_desc)
        if PRICE_CHECK_AVAILABLE and raw_desc:
            display_desc = clean_description(raw_desc)
            # Persist cleaned version back
            if display_desc != item.get("description"):
                item["description"] = display_desc
                item["description_raw"] = raw_desc
        # Cost sources
        amazon_cost = p.get("amazon_price")
        scprs_cost = p.get("scprs_price")
        # Best available cost
        unit_cost = p.get("unit_cost") or amazon_cost or scprs_cost or 0
        # Markup and final price
        markup_pct = p.get("markup_pct", 25)
        final_price = p.get("recommended_price") or (round(unit_cost * (1 + markup_pct/100), 2) if unit_cost else 0)

        amazon_str = f"${amazon_cost:.2f}" if amazon_cost else "—"
        amazon_data = f'data-amazon="{amazon_cost:.2f}"' if amazon_cost else 'data-amazon="0"'
        scprs_str = f"${scprs_cost:.2f}" if scprs_cost else "—"
        cost_str = f"{unit_cost:.2f}" if unit_cost else ""
        final_str = f"{final_price:.2f}" if final_price else ""
        qty = item.get("qty", 1)
        ext = f"${final_price * qty:.2f}" if final_price else "—"

        # Amazon match link + ASIN
        title = (p.get("amazon_title") or "")[:40]
        url = p.get("amazon_url", "")
        asin = p.get("amazon_asin", "")
        link_parts = []
        if url and title:
            link_parts.append(f'<a href="{url}" target="_blank" title="{p.get("amazon_title","")}">{title}</a>')
        if asin:
            link_parts.append(f'<span style="color:#58a6ff;font-size:10px;font-family:JetBrains Mono,monospace">ASIN: {asin}</span>')
        link = "<br>".join(link_parts) if link_parts else "—"

        # SCPRS confidence indicator
        scprs_conf = p.get("scprs_confidence", 0)
        scprs_badge = ""
        if scprs_cost:
            color = "#3fb950" if scprs_conf > 0.7 else ("#d29922" if scprs_conf > 0.4 else "#8b949e")
            scprs_badge = f' <span style="color:{color};font-size:10px" title="Confidence: {scprs_conf:.0%}">●</span>'

        # Confidence grade if scored
        conf = item.get("confidence", {})
        grade = conf.get("grade", "")
        grade_color = {"A": "#3fb950", "B": "#58a6ff", "C": "#d29922", "F": "#f85149"}.get(grade, "#8b949e")
        grade_html = f'<span style="color:{grade_color};font-weight:bold">{grade}</span>' if grade else "—"

        # Per-item profit
        item_profit = round((final_price - unit_cost) * qty, 2) if (final_price and unit_cost) else 0
        profit_color = "#3fb950" if item_profit > 0 else ("#f85149" if item_profit < 0 else "#8b949e")
        profit_str = f'<span style="color:{profit_color}">${item_profit:.2f}</span>' if (final_price and unit_cost) else "—"
        
        # Item link
        item_link = item.get("item_link", "")
        item_supplier = item.get("item_supplier", "")
        link_display = f'<a href="{item_link}" target="_blank" style="font-size:11px;color:#58a6ff;word-break:break-all">{item_supplier or item_link[:30]}</a>' if item_link else ""
        supplier_badge = f'<span style="font-size:10px;color:#8b949e;display:block;margin-top:1px">{item_supplier}</span>' if item_supplier else ""

        # No-bid state
        no_bid = item.get("no_bid", False)
        bid_checked = "" if no_bid else "checked"
        row_opacity = "opacity:0.4" if no_bid else ""

        items_html += f"""<tr style="{row_opacity}" data-row="{idx}">
         <td style="text-align:center"><input type="checkbox" name="bid_{idx}" {bid_checked} onchange="toggleBid({idx},this)" style="width:18px;height:18px;cursor:pointer"></td>
         <td><input type="number" name="itemnum_{idx}" value="{item.get('item_number','')}" class="num-in sm" style="width:40px"></td>
         <td><input type="number" name="qty_{idx}" value="{qty}" class="num-in sm" style="width:55px" onchange="recalcPC()"></td>
         <td><input type="text" name="uom_{idx}" value="{item.get('uom','EA').upper()}" class="text-in" style="width:45px;text-transform:uppercase;text-align:center;font-weight:600"></td>
         <td><textarea name="desc_{idx}" class="text-in" style="width:100%;min-height:38px;resize:vertical;font-family:inherit;font-size:13px;line-height:1.4;padding:6px 8px" title="{raw_desc.replace('"','&quot;').replace('<','&lt;')}">{display_desc.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')}</textarea></td>
         <td style="min-width:220px">
          <div style="display:flex;flex-direction:column;gap:3px">
           <input type="text" name="link_{idx}" value="{item_link.replace(chr(34), '&quot;')}" placeholder="Paste supplier URL…" class="text-in" style="width:100%;font-size:12px;color:#58a6ff;padding:5px 7px" oninput="handleLinkInput({idx}, this)" onpaste="setTimeout(()=>handleLinkInput({idx},this),50)">
           <div id="link_meta_{idx}" style="font-size:10px;color:#8b949e">{supplier_badge}</div>
          </div>
         </td>
         <td style="font-weight:600;font-size:14px">{scprs_str}{scprs_badge}</td>
         <td style="font-weight:600;font-size:14px" {amazon_data}>{amazon_str}</td>
         <td style="font-size:12px;max-width:180px">{link}</td>
         <td><input type="number" step="0.01" min="0" name="cost_{idx}" value="{cost_str}" class="num-in" onchange="recalcRow({idx})"></td>
         <td><input type="number" step="1" min="0" max="200" name="markup_{idx}" value="{markup_pct}" class="num-in sm" style="width:48px" onchange="recalcRow({idx})"><span style="color:#8b949e;font-size:13px">%</span></td>
         <td><input type="number" step="0.01" min="0" name="price_{idx}" value="{final_str}" class="num-in" onchange="recalcPC()"></td>
         <td class="ext" style="font-weight:600;font-size:14px">{ext}</td>
         <td class="profit" style="font-size:14px">{profit_str}</td>
         <td style="text-align:center;font-size:15px">{grade_html}</td>
        </tr>"""

    download_html = ""
    if pc.get("output_pdf") and os.path.exists(pc.get("output_pdf", "")):
        fname = os.path.basename(pc["output_pdf"])
        download_html += f'<a href="/api/pricecheck/download/{fname}" class="btn btn-sm btn-g" style="font-size:13px">📥 Download 704</a>'
    if pc.get("reytech_quote_pdf") and os.path.exists(pc.get("reytech_quote_pdf", "")):
        qfname = os.path.basename(pc["reytech_quote_pdf"])
        qnum = pc.get("reytech_quote_number", "")
        download_html += f' <a href="/api/pricecheck/download/{qfname}" class="btn btn-sm" style="background:#1a3a5c;color:#fff;font-size:13px">📥 Quote {qnum}</a>'

    # 45-day expiry from TODAY (not upload date)
    try:
        expiry = datetime.now() + timedelta(days=45)
        expiry_date = expiry.strftime("%m/%d/%Y")
    except Exception as e:
        log.debug("Suppressed: %s", e)
        expiry_date = (datetime.now() + timedelta(days=45)).strftime("%m/%d/%Y")
    today_date = datetime.now().strftime("%m/%d/%Y")

    # Delivery dropdown state
    saved_delivery = pc.get("delivery_option", "5-7 business days")
    preset_options = ("3-5 business days", "5-7 business days", "7-14 business days")
    is_custom = saved_delivery not in preset_options and saved_delivery != ""
    del_sel = {opt: ("selected" if saved_delivery == opt else "") for opt in preset_options}
    del_sel["custom"] = "selected" if is_custom else ""
    # Default to 5-7 if nothing saved
    if not any(del_sel.values()):
        del_sel["5-7 business days"] = "selected"
    custom_val = saved_delivery if is_custom else ""
    custom_display = "inline-block" if is_custom else "none"

    # Pre-compute next quote number preview
    next_quote_preview = peek_next_quote_number() if QUOTE_GEN_AVAILABLE else ""
    
    profit_summary_json = _json.dumps(pc.get("profit_summary")) if pc.get("profit_summary") else "null"
    html = build_pc_detail_html(
        pcid=pcid, pc=pc, items=items, items_html=items_html,
        download_html=download_html, expiry_date=expiry_date,
        header=header, custom_val=custom_val, custom_display=custom_display,
        del_sel=del_sel, next_quote_preview=next_quote_preview,
        today_date=today_date, profit_summary_json=profit_summary_json
    )
    return html


@bp.route("/pricecheck/<pcid>/lookup")
@auth_required
def pricecheck_lookup(pcid):
    """Run Amazon lookup for all items in a Price Check."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"ok": False, "error": "price_check.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    parsed = pc.get("parsed", {})
    parsed = lookup_prices(parsed)
    pc["parsed"] = parsed
    pc["items"] = parsed.get("line_items", [])
    _transition_status(pc, "priced", actor="user", notes="Prices saved")
    _save_price_checks(pcs)

    found = sum(1 for i in pc["items"] if i.get("pricing", {}).get("amazon_price"))
    return jsonify({"ok": True, "found": found, "total": len(pc["items"])})


@bp.route("/pricecheck/<pcid>/scprs-lookup")
@auth_required
def pricecheck_scprs_lookup(pcid):
    """Run SCPRS Won Quotes lookup for all items."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    items = pc.get("items", [])
    found = 0
    if PRICING_ORACLE_AVAILABLE:
        for item in items:
            try:
                matches = find_similar_items(
                    item_number=item.get("item_number", ""),
                    description=item.get("description", ""),
                )
                if matches:
                    best = matches[0]
                    quote = best.get("quote", best)
                    if not item.get("pricing"):
                        item["pricing"] = {}
                    scprs_price = quote.get("unit_price")
                    item["pricing"]["scprs_price"]      = scprs_price
                    item["pricing"]["scprs_match"]      = quote.get("description", "")[:60]
                    item["pricing"]["scprs_confidence"] = best.get("match_confidence", 0)
                    item["pricing"]["scprs_source"]     = quote.get("source", "scprs_kb")
                    item["pricing"]["scprs_po"]         = quote.get("po_number", "")
                    # GAP 4 FIX: record this match to price_history
                    if scprs_price and scprs_price > 0:
                        try:
                            from src.core.db import record_price as _rp3
                            _rp3(
                                description=item.get("description", ""),
                                unit_price=float(scprs_price),
                                source="scprs_kb_match",
                                part_number=str(item.get("item_number", "") or ""),
                                agency=pc.get("institution", ""),
                                price_check_id=pcid,
                                notes=f"conf={best.get('match_confidence',0):.2f}|po={quote.get('po_number','')}",
                            )
                        except Exception:
                            pass
                    found += 1
            except Exception as e:
                log.error(f"SCPRS lookup error: {e}")

    pc["items"] = items
    pc["parsed"]["line_items"] = items
    _save_price_checks(pcs)
    return jsonify({"ok": True, "found": found, "total": len(items)})


@bp.route("/pricecheck/<pcid>/rename", methods=["POST"])
@auth_required
def pricecheck_rename(pcid):
    """Rename a price check's display number."""
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})
    data = request.get_json(silent=True) or {}
    new_name = data.get("pc_number", "").strip()
    if not new_name:
        return jsonify({"ok": False, "error": "Name cannot be empty"})
    pcs[pcid]["pc_number"] = new_name
    _save_price_checks(pcs)
    log.info("RENAME PC %s → %s", pcid, new_name)
    return jsonify({"ok": True, "pc_number": new_name})


@bp.route("/pricecheck/<pcid>/save-prices", methods=["POST"])
@auth_required
def pricecheck_save_prices(pcid):
    """Save manually edited prices, costs, and markups from the UI."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    data = request.json or {}
    items = pc.get("items", [])
    
    # Save tax state
    pc["tax_enabled"] = data.get("tax_enabled", False)
    pc["tax_rate"] = data.get("tax_rate", 0)
    pc["delivery_option"] = data.get("delivery_option", "5-7 business days")
    pc["custom_notes"] = data.get("custom_notes", "")
    pc["price_buffer"] = data.get("price_buffer", 0)
    pc["default_markup"] = data.get("default_markup", 25)
    
    for key, val in data.items():
        try:
            if key in ("tax_enabled", "tax_rate"):
                continue
            parts = key.split("_", 1)
            if len(parts) != 2:
                continue
            field_type = parts[0]
            idx = int(parts[1])
            # Expand items list if new rows were added via UI
            while idx >= len(items):
                items.append({"item_number": "", "qty": 1, "uom": "ea",
                              "description": "", "pricing": {}})
            if 0 <= idx < len(items):
                if field_type in ("price", "cost", "markup"):
                    if not items[idx].get("pricing"):
                        items[idx]["pricing"] = {}
                    if field_type == "price":
                        # Write to both pricing dict (oracle compat) and first-class field
                        v = float(val) if val else None
                        items[idx]["pricing"]["recommended_price"] = v
                        items[idx]["unit_price"] = v
                    elif field_type == "cost":
                        v = float(val) if val else None
                        items[idx]["pricing"]["unit_cost"] = v
                        items[idx]["vendor_cost"] = v
                    elif field_type == "markup":
                        v = float(val) if val else 25
                        items[idx]["pricing"]["markup_pct"] = v
                        items[idx]["markup_pct"] = v
                    # Recalculate derived profit fields whenever any of these change
                    it = items[idx]
                    vc = it.get("vendor_cost") or it["pricing"].get("unit_cost") or 0
                    up = it.get("unit_price") or it["pricing"].get("recommended_price") or 0
                    qty = it.get("qty", 1) or 1
                    if up and vc:
                        it["profit_unit"] = round(up - vc, 4)
                        it["profit_total"] = round((up - vc) * qty, 2)
                        it["margin_pct"] = round((up - vc) / up * 100, 1) if up else 0
                    elif up and not vc:
                        # Cost unknown — can't calculate profit yet
                        it["profit_unit"] = None
                        it["profit_total"] = None
                        it["margin_pct"] = None
                elif field_type == "qty":
                    items[idx]["qty"] = int(val) if val else 1
                    # Recalc profit_total when qty changes
                    it = items[idx]
                    vc = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0
                    up = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
                    qty = it["qty"]
                    if up and vc:
                        it["profit_unit"] = round(up - vc, 4)
                        it["profit_total"] = round((up - vc) * qty, 2)
                elif field_type == "desc":
                    items[idx]["description"] = str(val) if val else ""
                elif field_type == "uom":
                    items[idx]["uom"] = str(val).upper() if val else "EA"
                elif field_type == "itemno":
                    items[idx]["item_number"] = str(val) if val else ""
                elif field_type == "bid":
                    items[idx]["no_bid"] = not bool(val)
                elif field_type == "link":
                    items[idx]["item_link"] = str(val).strip() if val else ""
                    # Auto-detect supplier from the URL when it's saved
                    if items[idx]["item_link"]:
                        try:
                            from src.agents.item_link_lookup import detect_supplier
                            items[idx]["item_supplier"] = detect_supplier(items[idx]["item_link"])
                        except Exception:
                            pass
        except (ValueError, IndexError):
            pass

    pc["items"] = items
    pc["parsed"]["line_items"] = items

    # Compute PC-level profit summary — always kept current
    total_revenue = 0
    total_cost = 0
    total_profit = 0
    costed_items = 0
    for it in items:
        if it.get("no_bid"):
            continue
        up = it.get("unit_price") or it.get("pricing", {}).get("recommended_price") or 0
        vc = it.get("vendor_cost") or it.get("pricing", {}).get("unit_cost") or 0
        qty = it.get("qty", 1) or 1
        total_revenue += up * qty
        if vc:
            total_cost += vc * qty
            total_profit += (up - vc) * qty
            costed_items += 1
    pc["profit_summary"] = {
        "total_revenue":    round(total_revenue, 2),
        "total_cost":       round(total_cost, 2),
        "gross_profit":     round(total_profit, 2),
        "margin_pct":       round(total_profit / total_revenue * 100, 1) if total_revenue else 0,
        "costed_items":     costed_items,
        "total_items":      len([i for i in items if not i.get("no_bid")]),
        "fully_costed":     costed_items == len([i for i in items if not i.get("no_bid")]),
    }

    _save_price_checks(pcs)

    # Also mirror to SQLite
    try:
        upsert_price_check(pcid, pc)
    except Exception:
        pass

    # ── GAP 3 FIX: write confirmed prices to price_history + won_quotes ───────
    institution = pc.get("institution", "")
    pc_num      = pc.get("pc_number", "")
    try:
        from src.core.db import record_price as _rp
        from src.knowledge.won_quotes_db import ingest_scprs_result as _ingest_wq2
        for _item in items:
            if _item.get("no_bid"):
                continue
            _up   = _item.get("unit_price") or _item.get("pricing", {}).get("recommended_price") or 0
            _cost = _item.get("vendor_cost") or _item.get("pricing", {}).get("unit_cost") or 0
            _desc = _item.get("description", "")
            _qty  = _item.get("qty", 1) or 1
            _part = str(_item.get("item_number", "") or "")
            if _up > 0 and _desc:
                _rp(description=_desc, unit_price=float(_up), source="pc_confirmed",
                    part_number=_part, quantity=float(_qty),
                    agency=institution, price_check_id=pcid, notes=f"PC#{pc_num}")
            if _cost > 0 and _desc:
                _rp(description=_desc, unit_price=float(_cost), source="pc_vendor_cost",
                    part_number=_part, quantity=float(_qty),
                    agency=institution, price_check_id=pcid,
                    notes=f"PC#{pc_num} vendor cost")
                _ingest_wq2(
                    po_number=f"PC-{pc_num}",
                    item_number=_part,
                    description=_desc,
                    unit_price=float(_cost),
                    quantity=float(_qty),
                    department=institution,
                    award_date=datetime.now().strftime("%Y-%m-%d"),
                    source="pc_vendor_cost",
                )
    except Exception as _e:
        log.debug("price learning write: %s", _e)

    summary = pc["profit_summary"]
    return jsonify({
        "ok": True,
        "profit_summary": summary,
    })


@bp.route("/pricecheck/<pcid>/generate")
@auth_required
def pricecheck_generate(pcid):
    """Generate completed Price Check PDF and ingest into Won Quotes KB."""
    if not PRICE_CHECK_AVAILABLE:
        return jsonify({"ok": False, "error": "price_check.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    from src.forms.price_check import fill_ams704
    parsed = pc.get("parsed", {})
    source_pdf = pc.get("source_pdf", "")
    if not source_pdf or not os.path.exists(source_pdf):
        return jsonify({"ok": False, "error": "Source PDF not found"})

    pc_num = pc.get("pc_number", "unknown")
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
    output_path = os.path.join(DATA_DIR, f"PC_{safe_name}_Reytech_.pdf")

    result = fill_ams704(
        source_pdf=source_pdf,
        parsed_pc=parsed,
        output_pdf=output_path,
        tax_rate=pc.get("tax_rate", 0) if pc.get("tax_enabled") else 0.0,
        custom_notes=pc.get("custom_notes", ""),
        delivery_option=pc.get("delivery_option", ""),
    )

    if result.get("ok"):
        pc["output_pdf"] = output_path
        _transition_status(pc, "completed", actor="system", notes="704 PDF filled")
        pc["summary"] = result.get("summary", {})
        _save_price_checks(pcs)

        # Ingest completed prices into Won Quotes KB for future reference
        _ingest_pc_to_won_quotes(pc)

        return jsonify({"ok": True, "download": f"/api/pricecheck/download/{os.path.basename(output_path)}"})
    return jsonify({"ok": False, "error": result.get("error", "Unknown error")})


@bp.route("/pricecheck/<pcid>/generate-quote")
@auth_required
def pricecheck_generate_quote(pcid):
    """Generate a standalone Reytech-branded quote PDF from a Price Check."""
    if not QUOTE_GEN_AVAILABLE:
        return jsonify({"ok": False, "error": "quote_generator.py not available"})
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    pc_num = pc.get("pc_number", "unknown")
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', pc_num.strip())
    output_path = os.path.join(DATA_DIR, f"Quote_{safe_name}_Reytech.pdf")

    # Lock-in: reuse existing quote number if already assigned
    locked_qn = pc.get("reytech_quote_number", "")

    result = generate_quote_from_pc(
        pc, output_path,
        include_tax=pc.get("tax_enabled", False),
        tax_rate=pc.get("tax_rate", 0.0725) if pc.get("tax_enabled") else 0.0,
        quote_number=locked_qn if locked_qn else None,
    )

    if result.get("ok"):
        pc["reytech_quote_pdf"] = output_path
        pc["reytech_quote_number"] = result.get("quote_number", "")
        _save_price_checks(pcs)
        # CRM: log quote generation
        _log_crm_activity(result.get("quote_number", ""), "quote_generated",
                          f"Quote {result.get('quote_number','')} generated — ${result.get('total',0):,.2f} for {pc.get('institution','')}",
                          actor="user", metadata={"institution": pc.get("institution",""), "agency": result.get("agency","")})
        return jsonify({
            "ok": True,
            "download": f"/api/pricecheck/download/{os.path.basename(output_path)}",
            "quote_number": result.get("quote_number"),
        })
    return jsonify({"ok": False, "error": result.get("error", "Unknown error")})


def _ingest_pc_to_won_quotes(pc):
    """Ingest completed Price Check pricing into Won Quotes KB."""
    if not PRICING_ORACLE_AVAILABLE:
        return
    try:
        items = pc.get("items", [])
        institution = pc.get("institution", "")
        pc_num = pc.get("pc_number", "")
        for item in items:
            pricing = item.get("pricing", {})
            price = pricing.get("recommended_price")
            if not price:
                continue
            ingest_scprs_result({
                "po_number": f"PC-{pc_num}",
                "item_number": item.get("item_number", ""),
                "description": item.get("description", ""),
                "unit_price": price,
                "supplier": "Reytech Inc.",
                "department": institution,
                "award_date": datetime.now().strftime("%Y-%m-%d"),
                "source": "price_check",
            })
        log.info(f"Ingested {len(items)} items from PC #{pc_num} into Won Quotes KB")
    except Exception as e:
        log.error(f"KB ingestion error: {e}")


@bp.route("/pricecheck/<pcid>/convert-to-quote")
@auth_required
def pricecheck_convert_to_quote(pcid):
    """Convert a Price Check into a full RFQ with 704A/B and Bid Package."""
    pcs = _load_price_checks()
    pc = pcs.get(pcid)
    if not pc:
        return jsonify({"ok": False, "error": "PC not found"})

    items = pc.get("items", [])
    header = pc.get("parsed", {}).get("header", {})

    # Build RFQ record from PC data
    rfq_id = str(uuid.uuid4())[:8]
    line_items = []
    for item in items:
        pricing = item.get("pricing", {})
        # First-class fields take precedence over oracle suggestions
        vendor_cost = item.get("vendor_cost") or pricing.get("unit_cost") or pricing.get("amazon_price") or 0
        unit_price  = item.get("unit_price")  or pricing.get("recommended_price") or 0
        markup_pct  = item.get("markup_pct")  or pricing.get("markup_pct", 25)
        qty         = item.get("qty", 1) or 1
        profit_unit  = round(unit_price - vendor_cost, 4) if (unit_price and vendor_cost) else None
        profit_total = round(profit_unit * qty, 2) if profit_unit is not None else None
        margin_pct   = round((unit_price - vendor_cost) / unit_price * 100, 1) if (unit_price and vendor_cost) else None

        li = {
            "item_number":     item.get("item_number", ""),
            "description":     item.get("description", ""),
            "qty":             qty,
            "uom":             item.get("uom", "ea"),
            "qty_per_uom":     item.get("qty_per_uom", 1),
            # Cost & profit (the fields that matter for business intelligence)
            "vendor_cost":     vendor_cost,
            "markup_pct":      markup_pct,
            "unit_price":      unit_price,
            "extension":       round(unit_price * qty, 2),
            "profit_unit":     profit_unit,
            "profit_total":    profit_total,
            "margin_pct":      margin_pct,
            # Backwards compat names
            "unit_cost":       vendor_cost,
            "supplier_cost":   vendor_cost,
            "our_price":       unit_price,
            # Source intelligence
            "scprs_last_price": pricing.get("scprs_price"),
            "amazon_price":     pricing.get("amazon_price"),
            "price_source":     pricing.get("price_source", "manual"),
            "supplier_source":  pricing.get("price_source", "price_check"),
            "supplier_url":     pricing.get("amazon_url", ""),
        }
        line_items.append(li)

    rfq = {
        "id": rfq_id,
        "solicitation_number": f"PC-{pc.get('pc_number', 'unknown')}",
        "requestor_name": header.get("requestor", pc.get("requestor", "")),
        "requestor_email": "",
        "department": header.get("institution", pc.get("institution", "")),
        "ship_to": pc.get("ship_to", ""),
        "delivery_zip": header.get("zip_code", ""),
        "due_date": pc.get("due_date", ""),
        "phone": header.get("phone", ""),
        "line_items": line_items,
        "status": "pending",
        "source": "price_check",
        "source_pc_id": pcid,
        "is_test": pcid.startswith("test_") or pc.get("is_test", False),
        "award_method": "all_or_none",
        "created_at": datetime.now().isoformat(),
    }

    rfqs = load_rfqs()
    rfqs[rfq_id] = rfq
    save_rfqs(rfqs)

    # Update PC status
    _transition_status(pc, "completed", actor="system", notes="Reytech quote generated")
    pc["converted_rfq_id"] = rfq_id
    _save_price_checks(pcs)

    return jsonify({"ok": True, "rfq_id": rfq_id})


@bp.route("/api/resync")
@auth_required
def api_resync():
    """Clear entire queue + reset processed UIDs + re-poll inbox."""
    log.info("Full resync triggered — clearing queue and re-polling")
    # 1. Clear queue
    save_rfqs({})
    # 2. Reset processed UIDs
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if os.path.exists(proc_file):
        os.remove(proc_file)
        log.info("Cleared processed_emails.json")
    # 3. Reset poller so it rebuilds
    global _shared_poller
    _shared_poller = None
    # 4. Re-poll
    imported = do_poll_check()
    return jsonify({
        "ok": True,
        "cleared": True,
        "found": len(imported),
        "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
        "last_check": POLL_STATUS.get("last_check"),
    })


def _remove_processed_uid(uid):
    """Remove a single UID from processed_emails.json."""
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if not os.path.exists(proc_file):
        return
    try:
        with open(proc_file) as f:
            processed = json.load(f)
        if isinstance(processed, list) and uid in processed:
            processed.remove(uid)
            with open(proc_file, "w") as f:
                json.dump(processed, f)
            log.info(f"Removed UID {uid} from processed list")
        elif isinstance(processed, dict) and uid in processed:
            del processed[uid]
            with open(proc_file, "w") as f:
                json.dump(processed, f)
    except Exception as e:
        log.error(f"Error removing UID: {e}")


@bp.route("/api/clear-queue")
@auth_required
def api_clear_queue():
    """Clear all RFQs from the queue."""
    save_rfqs({})
    return jsonify({"ok": True, "message": "Queue cleared"})


@bp.route("/dl/<rid>/<fname>")
@auth_required
def download(rid, fname):
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return redirect("/")
    p = os.path.join(OUTPUT_DIR, r["solicitation_number"], fname)
    if os.path.exists(p): return send_file(p, as_attachment=True)
    flash("File not found", "error"); return redirect(f"/rfq/{rid}")


@bp.route("/api/scprs/<rid>")
@auth_required
def api_scprs(rid):
    """SCPRS lookup API endpoint for the dashboard JS."""
    log.info("SCPRS lookup requested for RFQ %s", rid)
    rfqs = load_rfqs()
    r = rfqs.get(rid)
    if not r: return jsonify({"error": "not found"})
    
    results = []
    errors = []
    for item in r["line_items"]:
        try:
            from src.agents.scprs_lookup import lookup_price, _build_search_terms
            item_num = item.get("item_number")
            desc = item.get("description")
            search_terms = _build_search_terms(item_num, desc)
            result = lookup_price(item_num, desc)
            if result:
                result["searched"] = search_terms
                results.append(result)
                # v6.0: Auto-ingest into Won Quotes KB
                if PRICING_ORACLE_AVAILABLE and result.get("price"):
                    try:
                        ingest_scprs_result(
                            po_number=result.get("po_number", ""),
                            item_number=item_num or "",
                            description=desc or "",
                            unit_price=result["price"],
                            quantity=1,
                            supplier=result.get("vendor", ""),
                            department=result.get("department", ""),
                            award_date=result.get("date", ""),
                            source=result.get("source", "scprs_live"),
                        )
                    except Exception as e:
                        log.debug("Suppressed: %s", e)
                        pass  # Never let KB ingestion break the lookup flow
            else:
                results.append({
                    "price": None,
                    "note": f"No SCPRS data found",
                    "item_number": item_num,
                    "description": (desc or "")[:80],
                    "searched": search_terms,
                })
        except Exception as e:
            import traceback
            results.append({"price": None, "error": str(e), "traceback": traceback.format_exc()})
            errors.append(str(e))
    
    return jsonify({"results": results, "errors": errors if errors else None})


@bp.route("/api/scprs-test")
@auth_required
def api_scprs_test():
    """SCPRS search test — ?q=stryker+xpr"""
    q = request.args.get("q", "stryker xpr")
    try:
        from src.agents.scprs_lookup import test_search
        return jsonify(test_search(q))
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


@bp.route("/api/scprs-raw")
@auth_required
def api_scprs_raw():
    """Raw SCPRS debug — shows HTML field IDs found in search results."""
    q = request.args.get("q", "stryker xpr")
    try:
        from src.agents.scprs_lookup import _get_session, _discover_grid_ids, SCPRS_SEARCH_URL, SEARCH_BUTTON, ALL_SEARCH_FIELDS, FIELD_DESCRIPTION
        from bs4 import BeautifulSoup
        
        session = _get_session()
        if not session.initialized:
            session.init_session()
        
        # Load search page
        page = session._load_page(2)
        icsid = session._extract_icsid(page)
        if icsid: session.icsid = icsid
        
        # POST search
        sv = {f: "" for f in ALL_SEARCH_FIELDS}
        sv[FIELD_DESCRIPTION] = q
        fd = session._build_form_data(page, SEARCH_BUTTON, sv)
        r = session.session.post(SCPRS_SEARCH_URL, data=fd, timeout=30)
        html = r.text
        soup = BeautifulSoup(html, "html.parser")
        
        import re
        count = re.search(r'(\d+)\s+to\s+(\d+)\s+of\s+(\d+)', html)
        discovered = _discover_grid_ids(soup, "ZZ_SCPR_RD_DVW")
        
        # Sample row 0 values
        row0 = {}
        for suffix in discovered:
            eid = f"ZZ_SCPR_RD_DVW_{suffix}$0"
            el = soup.find(id=eid)
            val = el.get_text(strip=True) if el else None
            row0[eid] = val
        
        # Also check for link-style elements
        link0 = soup.find("a", id="ZZ_SCPR_RD_DVW_CRDMEM_ACCT_NBR$0")
        
        # Broad scan: find ALL element IDs ending in $0
        all_row0_ids = {}
        for el in soup.find_all(id=re.compile(r'\$0$')):
            eid = el.get('id', '')
            if eid and ('SCPR' in eid or 'DVW' in eid or 'RSLT' in eid):
                all_row0_ids[eid] = el.get_text(strip=True)[:80]
        
        # Also discover with correct prefix
        discovered2 = _discover_grid_ids(soup, "ZZ_SCPR_RSLT_VW")
        
        # Table class scan
        tables = [(t.get("class",""), t.get("id",""), len(t.find_all("tr")))
                  for t in soup.find_all("table") if t.get("class")]
        grid_tables = [t for t in tables if "PSLEVEL1GRID" in str(t[0])]
        
        return jsonify({
            "query": q, "status": r.status_code, "size": len(html),
            "result_count": count.group(0) if count else "none",
            "id_discovered_RD_DVW": list(discovered.keys()),
            "id_discovered_RSLT_VW": list(discovered2.keys()),
            "all_row0_ids": all_row0_ids,
            "row0_values": row0,
            "po_link_found": link0.get_text(strip=True) if link0 else None,
            "grid_tables": grid_tables[:5],
        })
    except Exception as e:
        import traceback
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})


@bp.route("/api/status")
@auth_required
def api_status():
    return jsonify({
        "poll": POLL_STATUS,
        "scprs_db": get_price_db_stats(),
        "rfqs": len(load_rfqs()),
    })


@bp.route("/api/poll-now")
@auth_required
def api_poll_now():
    """Manual trigger: check email inbox right now."""
    try:
        imported = do_poll_check()
        return jsonify({
            "ok": True,
            "found": len(imported),
            "rfqs": [{"id": r["id"], "sol": r.get("solicitation_number", "?")} for r in imported],
            "last_check": POLL_STATUS.get("last_check"),
            "error": POLL_STATUS.get("error"),
        })
    except Exception as e:
        return jsonify({"ok": False, "found": 0, "error": str(e)})


@bp.route("/api/diag")
@auth_required
def api_diag():
    """Diagnostic endpoint — shows email config, connection test, and inbox status."""
    import traceback
    try:
        return _api_diag_inner()
    except Exception as e:
        return jsonify({"error": str(e), "traceback": traceback.format_exc()})

def _api_diag_inner():
    import traceback
    email_cfg = CONFIG.get("email", {})
    addr = email_cfg.get("email", "NOT SET")
    has_pw = bool(email_cfg.get("email_password"))
    host = email_cfg.get("imap_host", "imap.gmail.com")
    port = email_cfg.get("imap_port", 993)
    
    diag = {
        "config": {
            "email_address": addr,
            "has_password": has_pw,
            "password_length": len(email_cfg.get("email_password", "")),
            "imap_host": host,
            "imap_port": port,
            "imap_folder": email_cfg.get("imap_folder", "INBOX"),
        },
        "env_vars": {
            "GMAIL_ADDRESS_set": bool(os.environ.get("GMAIL_ADDRESS")),
            "GMAIL_PASSWORD_set": bool(os.environ.get("GMAIL_PASSWORD")),
            "GMAIL_ADDRESS_value": os.environ.get("GMAIL_ADDRESS", "NOT SET"),
        },
        "poll_status": POLL_STATUS,
        "connection_test": None,
        "inbox_test": None,
    }
    
    # Test IMAP connection
    try:
        import imaplib
        mail = imaplib.IMAP4_SSL(host, port)
        diag["connection_test"] = "SSL connected OK"
        
        try:
            mail.login(addr, email_cfg.get("email_password", ""))
            diag["connection_test"] = f"Logged in as {addr} OK"
            
            try:
                mail.select("INBOX")
                # Check total
                status, messages = mail.search(None, "ALL")
                total = len(messages[0].split()) if status == "OK" and messages[0] else 0
                # Check recent (last 3 days) — same as poller
                since_date = (datetime.now() - timedelta(days=3)).strftime("%d-%b-%Y")
                status3, recent = mail.uid("search", None, f"(SINCE {since_date})")
                recent_count = len(recent[0].split()) if status3 == "OK" and recent[0] else 0
                
                # Check how many already processed
                proc_file = os.path.join(DATA_DIR, "processed_emails.json")
                processed_uids = set()
                if os.path.exists(proc_file):
                    try:
                        with open(proc_file) as pf:
                            processed_uids = set(json.load(pf))
                    except Exception as e:

                        log.debug("Suppressed: %s", e)
                
                recent_uids = recent[0].split() if status3 == "OK" and recent[0] else []
                new_to_process = [u.decode() for u in recent_uids if u.decode() not in processed_uids]
                
                diag["inbox_test"] = {
                    "total_emails": total,
                    "recent_3_days": recent_count,
                    "already_processed": recent_count - len(new_to_process),
                    "new_to_process": len(new_to_process),
                }
                
                # Show subjects of emails that would be processed
                if new_to_process:
                    subjects = []
                    for uid_str in new_to_process[:5]:
                        st, data = mail.uid("fetch", uid_str.encode(), "(BODY.PEEK[HEADER.FIELDS (SUBJECT FROM)])")
                        if st == "OK":
                            subjects.append(data[0][1].decode("utf-8", errors="replace").strip())
                    diag["inbox_test"]["new_email_subjects"] = subjects
                
            except Exception as e:
                diag["inbox_test"] = f"SELECT/SEARCH failed: {e}"
            
            mail.logout()
        except imaplib.IMAP4.error as e:
            diag["connection_test"] = f"LOGIN FAILED: {e}"
        except Exception as e:
            diag["connection_test"] = f"LOGIN ERROR: {e}"
    except Exception as e:
        diag["connection_test"] = f"SSL CONNECT FAILED: {e}"
        diag["connection_traceback"] = traceback.format_exc()
    
    # Check processed emails file
    proc_file = email_cfg.get("processed_file", "data/processed_emails.json")
    if os.path.exists(proc_file):
        try:
            with open(proc_file) as f:
                processed = json.load(f)
            diag["processed_emails"] = {"count": len(processed), "ids": processed[-10:] if isinstance(processed, list) else list(processed)[:10]}
        except Exception as e:
            log.debug("Suppressed: %s", e)
            diag["processed_emails"] = "corrupt file"
    else:
        diag["processed_emails"] = "file not found"
    
    # SCPRS diagnostics
    diag["scprs"] = {
        "db_stats": get_price_db_stats(),
        "db_exists": os.path.exists(os.path.join(BASE_DIR, "data", "scprs_prices.json")),
    }
    try:
        from src.agents.scprs_lookup import test_connection
        import threading
        result = [False, "timeout"]
        def _test():
            try:
                result[0], result[1] = test_connection()
            except Exception as ex:
                result[1] = str(ex)
        t = threading.Thread(target=_test, daemon=True)
        t.start()
        t.join(timeout=15)  # Max 15 seconds for connectivity test (may need 2-3 loads)
        diag["scprs"]["fiscal_reachable"] = result[0]
        diag["scprs"]["fiscal_status"] = result[1]
    except Exception as e:
        diag["scprs"]["fiscal_reachable"] = False
        diag["scprs"]["fiscal_error"] = str(e)
    
    return jsonify(diag)


@bp.route("/api/reset-processed")
@auth_required
def api_reset_processed():
    """Clear the processed emails list so all recent emails get re-scanned."""
    global _shared_poller
    proc_file = os.path.join(DATA_DIR, "processed_emails.json")
    if os.path.exists(proc_file):
        os.remove(proc_file)
    _shared_poller = None  # Force new poller instance
    return jsonify({"ok": True, "message": "Processed emails list cleared. Hit Check Now to re-scan."})


# ═══════════════════════════════════════════════════════════════════════
# Pricing Oracle API (v6.0)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/api/pricing/recommend", methods=["POST"])
@auth_required
def api_pricing_recommend():
    """Get three-tier pricing recommendation for an RFQ's line items."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Pricing oracle not available — check won_quotes_db.py and pricing_oracle.py are in repo"}), 503

    data = request.get_json() or {}
    rid = data.get("rfq_id")

    if rid:
        rfqs = load_rfqs()
        rfq = rfqs.get(rid)
        if not rfq:
            return jsonify({"error": f"RFQ {rid} not found"}), 404
        result = recommend_prices_for_rfq(rfq, config_overrides=data.get("config"))
    else:
        result = recommend_prices_for_rfq(data, config_overrides=data.get("config"))

    return jsonify(result)


@bp.route("/api/won-quotes/search")
@auth_required
def api_won_quotes_search():
    """Search the Won Quotes Knowledge Base."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503

    query = request.args.get("q", "")
    item_number = request.args.get("item", "")
    max_results = int(request.args.get("max", 10))

    if not query and not item_number:
        return jsonify({"error": "Provide ?q=description or ?item=number"}), 400

    results = find_similar_items(
        item_number=item_number,
        description=query,
        max_results=max_results,
    )
    return jsonify({"query": query, "item_number": item_number, "results": results})


@bp.route("/api/won-quotes/stats")
@auth_required
def api_won_quotes_stats():
    """Get Won Quotes KB statistics and pricing health check."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503

    stats = get_kb_stats()
    health = pricing_health_check()
    return jsonify({"stats": stats, "health": health})


@bp.route("/api/won-quotes/dump")
@auth_required
def api_won_quotes_dump():
    """Debug: show first 10 raw KB records to verify what's stored."""
    if not PRICING_ORACLE_AVAILABLE:
        return jsonify({"error": "Won Quotes DB not available"}), 503
    from src.knowledge.won_quotes_db import load_won_quotes
    quotes = load_won_quotes()
    return jsonify({"total": len(quotes), "first_10": quotes[:10]})


@bp.route("/api/debug/paths")
@auth_required
def api_debug_paths():
    """Debug: show actual filesystem paths and what exists."""
    try:
        from src.knowledge import won_quotes_db
    except ImportError:
        import won_quotes_db
    results = {
        "dashboard_BASE_DIR": BASE_DIR,
        "dashboard_DATA_DIR": DATA_DIR,
        "won_quotes_DATA_DIR": won_quotes_db.DATA_DIR,
        "won_quotes_FILE": won_quotes_db.WON_QUOTES_FILE,
        "cwd": os.getcwd(),
        "app_file_location": os.path.abspath(__file__),
    }
    # Check what exists
    for path_name, path_val in list(results.items()):
        if path_val and os.path.exists(path_val):
            if os.path.isdir(path_val):
                try:
                    results[f"{path_name}_contents"] = os.listdir(path_val)
                except Exception as e:
                    log.debug("Suppressed: %s", e)
                    results[f"{path_name}_contents"] = "permission denied"
            else:
                results[f"{path_name}_exists"] = True
                results[f"{path_name}_size"] = os.path.getsize(path_val)
        else:
            results[f"{path_name}_exists"] = False
    # Check /app/data specifically
    for check_path in ["/app/data", "/app", DATA_DIR]:
        key = check_path.replace("/", "_")
        results[f"check{key}_exists"] = os.path.exists(check_path)
        if os.path.exists(check_path) and os.path.isdir(check_path):
            try:
                results[f"check{key}_contents"] = os.listdir(check_path)
            except Exception as e:
                log.debug("Suppressed: %s", e)
                results[f"check{key}_contents"] = "permission denied"
    return jsonify(results)


@bp.route("/api/won-quotes/migrate")
@auth_required
def api_won_quotes_migrate():
    """One-time migration: import existing scprs_prices.json into Won Quotes KB."""
    try:
        from src.agents.scprs_lookup import migrate_local_db_to_won_quotes
        result = migrate_local_db_to_won_quotes()
        return jsonify({"ok": True, **result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/won-quotes/seed")
@auth_required
def api_won_quotes_seed():
    """Start bulk SCPRS seed: searches ~20 common categories, drills into PO details,
    ingests unit prices into Won Quotes KB. Runs in background thread (~3-5 min)."""
    try:
        from src.agents.scprs_lookup import bulk_seed_won_quotes, SEED_STATUS
        if SEED_STATUS.get("running"):
            return jsonify({"ok": False, "message": "Seed already running", "status": SEED_STATUS})
        t = threading.Thread(target=bulk_seed_won_quotes, daemon=True)
        t.start()
        return jsonify({"ok": True, "message": "Seed started in background. Check progress at /api/won-quotes/seed-status"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/won-quotes/seed-status")
@auth_required
def api_won_quotes_seed_status():
    """Check progress of bulk SCPRS seed job."""
    try:
        from src.agents.scprs_lookup import SEED_STATUS
        return jsonify(SEED_STATUS)
    except Exception as e:
        return jsonify({"error": str(e)})


@bp.route("/api/pricecheck/<pcid>/delete", methods=["POST"])
@auth_required
def api_pricecheck_delete(pcid):
    """Delete a price check by ID. Also removes linked quote draft and recalculates counter."""
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})

    pc = pcs[pcid]
    pc_num = pc.get("pc_number", pcid)
    linked_qn = pc.get("reytech_quote_number", "") or pc.get("linked_quote_number", "")

    # Remove the PC
    del pcs[pcid]
    _save_price_checks(pcs)

    # Also remove from SQLite
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("DELETE FROM price_checks WHERE id=?", (pcid,))
    except Exception as e:
        log.debug("SQLite PC delete: %s", e)

    # Remove the linked draft quote from quotes_log.json so the number is freed
    quote_removed = False
    if linked_qn:
        try:
            from src.forms.quote_generator import get_all_quotes, _save_all_quotes
            all_quotes = get_all_quotes()
            before = len(all_quotes)
            all_quotes = [q for q in all_quotes
                          if not (q.get("quote_number") == linked_qn
                                  and q.get("status") in ("draft", "pending"))]
            if len(all_quotes) < before:
                _save_all_quotes(all_quotes)
                quote_removed = True
                log.info("Removed draft quote %s (linked to deleted PC %s)", linked_qn, pcid)

                # Also remove from SQLite quotes table
                try:
                    with get_db() as conn:
                        conn.execute("DELETE FROM quotes WHERE quote_number=? AND status IN ('draft','pending')", (linked_qn,))
                except Exception:
                    pass
        except Exception as e:
            log.debug("Quote cleanup: %s", e)

    # Recalculate counter — set to highest remaining quote number
    counter_reset = None
    if quote_removed:
        try:
            import re as _re
            from src.forms.quote_generator import get_all_quotes, _load_counter, _save_counter
            all_quotes = get_all_quotes()
            max_seq = 0
            for q in all_quotes:
                qn = q.get("quote_number", "")
                m = _re.search(r'R\d{2}Q(\d+)', qn)
                if m and not q.get("is_test"):
                    max_seq = max(max_seq, int(m.group(1)))
            # Also check remaining PCs
            remaining_pcs = _load_price_checks()
            for rpc in remaining_pcs.values():
                qn = rpc.get("reytech_quote_number", "") or ""
                m = _re.search(r'R\d{2}Q(\d+)', qn)
                if m:
                    max_seq = max(max_seq, int(m.group(1)))
            old_counter = _load_counter()
            if max_seq < old_counter.get("seq", 0):
                _save_counter({"year": old_counter.get("year", 2026), "seq": max_seq})
                counter_reset = f"Q{old_counter['seq']} → Q{max_seq} (next will be Q{max_seq + 1})"
                log.info("Quote counter reset: %s", counter_reset)
        except Exception as e:
            log.debug("Counter recalc: %s", e)

    log.info("DELETED PC %s (%s)%s", pcid, pc_num,
             f" + quote {linked_qn}" if quote_removed else "")
    return jsonify({
        "ok": True, "deleted": pcid,
        "quote_removed": linked_qn if quote_removed else None,
        "counter_reset": counter_reset,
    })


@bp.route("/api/admin/cleanup", methods=["POST"])
@auth_required
def api_admin_cleanup():
    """
    Fix Railway data issues:
    - Remove duplicate PCs (same pc_number + institution)
    - Remove test/blank PCs that have no real data
    - Reset quote counter to match actual highest quote number
    - Clean up orphaned quote references on PCs
    """
    results = {"removed_pcs": [], "kept_pcs": [], "quote_counter_before": None, "quote_counter_after": None, "errors": []}

    try:
        pcs = _load_price_checks()
        results["total_before"] = len(pcs)

        # --- Step 1: Remove clearly blank/empty PCs ---
        to_delete = []
        for pcid, pc in list(pcs.items()):
            pc_num = pc.get("pc_number", "").strip()
            institution = pc.get("institution", "").strip()
            items = pc.get("items", [])
            # Blank PC number with no institution and no items = junk
            if not pc_num and not institution and len(items) == 0:
                to_delete.append(pcid)
                results["removed_pcs"].append(f"{pcid[:8]}: blank/empty")
        for pcid in to_delete:
            del pcs[pcid]

        # --- Step 2: Deduplicate by (pc_number, institution) ---
        # Keep the most recent one (highest pcid / latest updated_at)
        seen = {}  # key -> best pcid
        for pcid, pc in pcs.items():
            key = (pc.get("pc_number", "").strip(), pc.get("institution", "").strip())
            if key not in seen:
                seen[key] = pcid
            else:
                # Keep whichever was updated more recently or has more data
                existing = pcs[seen[key]]
                existing_items = len(existing.get("items", []))
                this_items = len(pc.get("items", []))
                # Prefer one with more items, then newer by ID string sort
                if this_items > existing_items or (this_items == existing_items and pcid > seen[key]):
                    results["removed_pcs"].append(f"{seen[key][:8]}: dup of {pcid[:8]} ({key[0]})")
                    seen[key] = pcid
                else:
                    results["removed_pcs"].append(f"{pcid[:8]}: dup of {seen[key][:8]} ({key[0]})")

        # Rebuild pcs with only kept entries
        kept_ids = set(seen.values())
        for pcid in list(pcs.keys()):
            if pcid not in kept_ids:
                del pcs[pcid]

        _save_price_checks(pcs)
        results["total_after"] = len(pcs)
        results["kept_pcs"] = [f"{pid[:8]}: {pc.get('pc_number','?')}" for pid, pc in pcs.items()]

        # Also sync to SQLite
        try:
            from src.core.db import get_db
            with get_db() as conn:
                conn.execute("DELETE FROM price_checks WHERE id NOT IN ({})".format(
                    ",".join("?" * len(pcs))
                ), list(pcs.keys()))
        except Exception as e:
            results["errors"].append(f"SQLite sync: {e}")

    except Exception as e:
        results["errors"].append(f"PC cleanup: {e}")

    # --- Step 3: Fix quote counter ---
    try:
        from src.forms.quote_generator import _load_counter, _save_counter
        from src.core.db import get_db
        import re as _re
        from datetime import datetime as _dt

        counter = _load_counter()
        results["quote_counter_before"] = counter.copy()

        # Find highest real (non-test) quote number in DB
        with get_db() as conn:
            rows = conn.execute(
                "SELECT quote_number FROM quotes WHERE is_test=0 OR is_test IS NULL ORDER BY rowid"
            ).fetchall()

        max_seq = counter.get("seq", 16)
        year = _dt.now().year % 100  # 26 for 2026
        for row in rows:
            qn = row[0] or ""
            m = _re.match(r"R\d{2}Q(\d+)", qn)
            if m:
                max_seq = max(max_seq, int(m.group(1)))

        # Also scan price_checks for reytech_quote_numbers (may have test ones like R26Q1-R26Q8)
        # Do NOT update counter based on test PCs — only real quotes count
        new_seq = max_seq  # Already at or beyond highest real quote
        counter["seq"] = new_seq
        _save_counter(counter)
        results["quote_counter_after"] = counter.copy()

    except Exception as e:
        results["errors"].append(f"Counter fix: {e}")

    results["ok"] = True
    return jsonify(results)


@bp.route("/api/admin/status")
@auth_required
def api_admin_status():
    """Quick system status — quote counter, PC count, quote count, full PC detail, RFQ queue."""
    try:
        from src.forms.quote_generator import _load_counter
        from src.core.db import get_db
        pcs = _load_price_checks()
        counter = _load_counter()
        rfqs = load_rfqs()
        with get_db() as conn:
            q_count = conn.execute("SELECT COUNT(*) FROM quotes WHERE is_test=0 OR is_test IS NULL").fetchone()[0]
            quotes = [dict(r) for r in conn.execute(
                "SELECT quote_number, total, status FROM quotes WHERE is_test=0 ORDER BY rowid DESC LIMIT 20"
            ).fetchall()]
        # Full PC detail
        pc_detail = {}
        for pcid, pc in pcs.items():
            pc_detail[pcid] = {
                "pc_number": pc.get("pc_number", "?"),
                "institution": pc.get("institution", "?"),
                "reytech_quote_number": pc.get("reytech_quote_number", ""),
                "status": pc.get("status", "?"),
                "items_count": len(pc.get("items", [])),
                "email_subject": pc.get("email_subject", ""),
            }
        # RFQ detail
        rfq_detail = {}
        for rid, r in rfqs.items():
            rfq_detail[rid] = {
                "solicitation": r.get("solicitation", "?"),
                "requestor": r.get("requestor", "?"),
                "status": r.get("status", "?"),
                "items_count": len(r.get("items", [])),
                "email_uid": r.get("email_uid", ""),
                "email_subject": r.get("email_subject", ""),
            }
        return jsonify({
            "ok": True,
            "pc_count": len(pcs),
            "pcs": pc_detail,
            "rfq_count": len(rfqs),
            "rfqs": rfq_detail,
            "quote_count": q_count,
            "all_quotes": quotes,
            "counter": counter,
            "next_quote": f"R{str(counter.get('year',2026))[-2:]}Q{counter.get('seq',0)+1}",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/counter-set", methods=["POST"])
@auth_required
def api_admin_counter_set():
    """Force-set the quote counter. POST body: {"seq": 16}
    Next quote will be R26Q(seq+1).
    """
    data = request.get_json(silent=True) or {}
    new_seq = data.get("seq")
    if new_seq is None:
        return jsonify({"ok": False, "error": "Missing 'seq' in body"})
    try:
        from src.forms.quote_generator import set_quote_counter, _load_counter
        old = _load_counter()
        set_quote_counter(int(new_seq))
        new = _load_counter()
        log.info("ADMIN counter force-set: Q%d → Q%d (next = Q%d)",
                 old.get("seq", 0), new["seq"], new["seq"] + 1)
        return jsonify({
            "ok": True,
            "before": old,
            "after": new,
            "next_quote": f"R{str(new.get('year',2026))[-2:]}Q{new['seq']+1}",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/delete-quotes", methods=["POST"])
@auth_required
def api_admin_delete_quotes():
    """Delete quotes by number. POST body: {"quote_numbers": ["R26Q9","R26Q10"]}"""
    data = request.get_json(silent=True) or {}
    qns = data.get("quote_numbers", [])
    if not qns:
        return jsonify({"ok": False, "error": "Missing 'quote_numbers' list"})
    deleted = []
    try:
        from src.forms.quote_generator import get_all_quotes, _save_all_quotes
        from src.core.db import get_db
        all_q = get_all_quotes()
        before = len(all_q)
        all_q = [q for q in all_q if q.get("quote_number") not in qns]
        _save_all_quotes(all_q)
        try:
            with get_db() as conn:
                for qn in qns:
                    conn.execute("DELETE FROM quotes WHERE quote_number=?", (qn,))
                    deleted.append(qn)
        except Exception as e:
            log.debug("SQLite quote delete: %s", e)
        log.info("ADMIN delete-quotes: removed %s", qns)
        return jsonify({"ok": True, "deleted": qns, "quotes_before": before, "quotes_after": len(all_q)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@bp.route("/api/admin/recall", methods=["POST"])
@auth_required
def api_admin_recall():
    """Retroactive recall: delete PCs matching a pattern + free quote numbers.
    
    POST body: {"pattern": "02.17.26"} or {"pc_ids": ["auto_xxx", ...]}
    Deletes matching PCs, removes linked draft quotes, resets counter.
    """
    data = request.get_json(silent=True) or {}
    pattern = data.get("pattern", "").strip()
    pc_ids = data.get("pc_ids", [])
    
    results = {"deleted": [], "errors": [], "before": {}, "after": {}}
    
    try:
        from src.forms.quote_generator import get_all_quotes, _save_all_quotes, _load_counter, _save_counter
        from src.core.db import get_db
        
        pcs = _load_price_checks()
        results["before"]["pc_count"] = len(pcs)
        results["before"]["counter"] = _load_counter()
        results["before"]["pc_list"] = {k: {"num": v.get("pc_number",""), "qn": v.get("reytech_quote_number","")} for k,v in pcs.items()}
        
        # Find PCs to delete
        to_delete = []
        if pc_ids:
            to_delete = [pid for pid in pc_ids if pid in pcs]
        elif pattern:
            for pcid, pc in pcs.items():
                searchable = f"{pc.get('pc_number','')} {pc.get('email_subject','')} {pc.get('source_pdf','')}".lower()
                if pattern.lower() in searchable:
                    to_delete.append(pcid)
        
        if not to_delete:
            return jsonify({"ok": False, "error": f"No PCs match pattern='{pattern}' ids={pc_ids}", "pcs": results["before"]["pc_list"]})
        
        # Delete each PC + cascade
        for pcid in to_delete:
            pc = pcs[pcid]
            pc_num = pc.get("pc_number", pcid)
            linked_qn = pc.get("reytech_quote_number", "") or pc.get("linked_quote_number", "")
            
            del pcs[pcid]
            
            # SQLite cleanup
            try:
                with get_db() as conn:
                    conn.execute("DELETE FROM price_checks WHERE id=?", (pcid,))
            except Exception:
                pass
            
            # Remove linked draft quote
            quote_freed = None
            if linked_qn:
                try:
                    all_quotes = get_all_quotes()
                    before_len = len(all_quotes)
                    all_quotes = [q for q in all_quotes
                                  if not (q.get("quote_number") == linked_qn
                                          and q.get("status") in ("draft", "pending"))]
                    if len(all_quotes) < before_len:
                        _save_all_quotes(all_quotes)
                        quote_freed = linked_qn
                        try:
                            with get_db() as conn:
                                conn.execute("DELETE FROM quotes WHERE quote_number=? AND status IN ('draft','pending')", (linked_qn,))
                        except Exception:
                            pass
                except Exception as e:
                    results["errors"].append(f"Quote cleanup for {linked_qn}: {e}")
            
            results["deleted"].append({
                "pcid": pcid, "pc_number": pc_num,
                "quote_freed": quote_freed,
            })
        
        # Save updated PCs
        _save_price_checks(pcs)
        
        # Recalculate counter
        import re as _re
        all_quotes = get_all_quotes()
        max_seq = 0
        for q in all_quotes:
            qn = q.get("quote_number", "")
            m = _re.search(r'R\d{2}Q(\d+)', qn)
            if m and not q.get("is_test"):
                max_seq = max(max_seq, int(m.group(1)))
        for rpc in pcs.values():
            qn = rpc.get("reytech_quote_number", "") or ""
            m = _re.search(r'R\d{2}Q(\d+)', qn)
            if m:
                max_seq = max(max_seq, int(m.group(1)))
        
        old_counter = _load_counter()
        if max_seq < old_counter.get("seq", 0):
            _save_counter({"year": old_counter.get("year", 2026), "seq": max_seq})
        
        results["after"]["pc_count"] = len(pcs)
        results["after"]["counter"] = _load_counter()
        results["after"]["next_quote"] = f"R{str(results['after']['counter'].get('year',2026))[-2:]}Q{results['after']['counter'].get('seq',0)+1}"
        results["after"]["pc_list"] = {k: {"num": v.get("pc_number",""), "qn": v.get("reytech_quote_number","")} for k,v in pcs.items()}
        results["ok"] = True
        
        log.info("ADMIN RECALL: deleted %d PCs matching '%s', counter %s → %s",
                 len(results["deleted"]), pattern or pc_ids,
                 results["before"]["counter"], results["after"]["counter"])
        
    except Exception as e:
        results["ok"] = False
        results["errors"].append(str(e))
    
    return jsonify(results)


@bp.route("/api/admin/purge-rfqs", methods=["POST"])
@auth_required
def api_admin_purge_rfqs():
    """Delete RFQs from the queue.
    
    POST body options:
      {"rfq_ids": ["rfq_0", "rfq_1"]}  — delete specific IDs
      {"empty": true}                   — delete all RFQs with 0 items
      {"pattern": "valentina"}          — delete RFQs matching pattern in requestor/subject
      {"all": true}                     — nuclear: delete ALL RFQs
    Returns before/after counts.
    """
    data = request.get_json(silent=True) or {}
    rfqs = load_rfqs()
    before_count = len(rfqs)
    before_list = {k: {"sol": v.get("solicitation","?"), "req": v.get("requestor","?"),
                       "items": len(v.get("items",[])), "status": v.get("status","?")}
                   for k, v in rfqs.items()}
    
    to_delete = set()
    
    if data.get("rfq_ids"):
        to_delete = {rid for rid in data["rfq_ids"] if rid in rfqs}
    elif data.get("empty"):
        to_delete = {rid for rid, r in rfqs.items() if len(r.get("items", [])) == 0}
    elif data.get("pattern"):
        pat = data["pattern"].lower()
        for rid, r in rfqs.items():
            searchable = f"{r.get('requestor','')} {r.get('email_subject','')} {r.get('solicitation','')}".lower()
            if pat in searchable:
                to_delete.add(rid)
    elif data.get("all"):
        to_delete = set(rfqs.keys())
    else:
        return jsonify({"ok": False, "error": "Provide rfq_ids, empty:true, pattern, or all:true",
                        "rfqs": before_list})
    
    deleted = []
    for rid in to_delete:
        r = rfqs.pop(rid, None)
        if r:
            deleted.append({"id": rid, "sol": r.get("solicitation","?"),
                           "req": r.get("requestor","?"), "items": len(r.get("items",[]))})
    
    save_rfqs(rfqs)
    
    # Also clean SQLite
    try:
        from src.core.db import get_db
        with get_db() as conn:
            for d in deleted:
                conn.execute("DELETE FROM rfqs WHERE id=?", (d["id"],))
    except Exception:
        pass
    
    log.info("ADMIN PURGE-RFQS: deleted %d of %d RFQs", len(deleted), before_count)
    
    return jsonify({
        "ok": True,
        "deleted": deleted,
        "deleted_count": len(deleted),
        "before": before_count,
        "after": len(rfqs),
    })


@bp.route("/api/admin/clean-activity", methods=["POST"])
@auth_required
def api_admin_clean_activity():
    """Remove entries from crm_activity.json.
    
    POST body options:
      {"event_types": ["quote_lost"]}       — remove by event type
      {"pattern": "R26Q19"}                 — remove entries matching pattern in detail
      {"before": "2026-02-18"}              — remove entries before date
      {"all": true}                         — nuclear: clear all activity
    Returns before/after counts.
    """
    data = request.get_json(silent=True) or {}
    crm_path = os.path.join(DATA_DIR, "crm_activity.json")
    try:
        with open(crm_path) as f:
            activities = json.load(f)
    except Exception:
        activities = []
    
    before_count = len(activities)
    
    if data.get("all"):
        activities = []
    elif data.get("event_types"):
        types = set(data["event_types"])
        activities = [a for a in activities if a.get("event_type") not in types]
    elif data.get("pattern"):
        pat = data["pattern"].lower()
        activities = [a for a in activities
                      if pat not in (a.get("detail","") + " " + a.get("event_type","")).lower()]
    elif data.get("before"):
        cutoff = data["before"]
        activities = [a for a in activities if a.get("timestamp","") >= cutoff]
    else:
        return jsonify({"ok": False, "error": "Provide event_types, pattern, before, or all:true"})
    
    with open(crm_path, "w") as f:
        json.dump(activities, f, indent=2, default=str)
    
    log.info("ADMIN CLEAN-ACTIVITY: %d → %d entries", before_count, len(activities))
    
    return jsonify({
        "ok": True,
        "before": before_count,
        "after": len(activities),
        "removed": before_count - len(activities),
    })


@bp.route("/api/admin/backfill-contacts", methods=["POST"])
@auth_required
def api_admin_backfill_contacts():
    """Backfill CRM contacts from existing price checks and RFQ senders.
    Scans all PCs/RFQs for requestor emails and creates CRM contacts.
    """
    import re as _re, hashlib
    from src.core.db import upsert_contact
    
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(crm_path) as f:
            crm = json.load(f)
    except Exception:
        crm = {}
    
    before_count = len(crm)
    created = []
    
    agency_map = {
        "cdcr.ca.gov": "CDCR", "cdph.ca.gov": "CDPH", "dgs.ca.gov": "DGS",
        "dhcs.ca.gov": "DHCS", "cchcs.org": "CCHCS",
    }
    
    def _add_contact(email_raw, name_hint=""):
        if not email_raw:
            return
        m = _re.search(r'[\w.+-]+@[\w.-]+', str(email_raw))
        if not m:
            return
        em = m.group(0).lower().strip()
        cid = hashlib.md5(em.encode()).hexdigest()[:16]
        if cid in crm:
            return  # already exists
        
        # Derive name
        if name_hint and name_hint != em and "@" not in name_hint:
            name = name_hint
        else:
            local = em.split("@")[0]
            name = " ".join(w.capitalize() for w in _re.split(r'[._-]', local))
        
        domain = em.split("@")[-1].lower()
        agency = agency_map.get(domain, domain.split(".")[0].upper() if ".gov" in domain else "")
        
        crm[cid] = {
            "id": cid, "buyer_name": name, "buyer_email": em,
            "buyer_phone": "", "agency": agency, "title": "", "department": "",
            "linkedin": "", "notes": "Backfilled from PC/RFQ records",
            "tags": ["email_sender", "buyer"], "total_spend": 0, "po_count": 0,
            "categories": {}, "items_purchased": [], "purchase_orders": [],
            "last_purchase": "", "score": 50, "opportunity_score": 0,
            "outreach_status": "active", "activity": [],
        }
        upsert_contact({"id": cid, "buyer_name": name, "buyer_email": em,
                        "agency": agency, "source": "backfill",
                        "outreach_status": "active", "is_reytech_customer": True})
        created.append({"email": em, "name": name, "agency": agency})
    
    # Scan price checks
    pcs = _load_price_checks()
    for pc in pcs.values():
        req = pc.get("requestor", "")
        req_email = pc.get("contact_email", "") or pc.get("requestor_email", "")
        if "@" in req:
            _add_contact(req)
        elif req_email:
            _add_contact(req_email, req)
    
    # Scan RFQs
    rfqs = load_rfqs()
    for r in rfqs.values():
        _add_contact(r.get("email_sender", ""), r.get("requestor_name", ""))
        _add_contact(r.get("requestor_email", ""), r.get("requestor_name", ""))
    
    if created:
        with open(crm_path, "w") as f:
            json.dump(crm, f, indent=2, default=str)
    
    log.info("ADMIN BACKFILL-CONTACTS: created %d new contacts from PC/RFQ data", len(created))
    
    return jsonify({
        "ok": True,
        "created": created,
        "created_count": len(created),
        "before": before_count,
        "after": len(crm),
    })


@bp.route("/api/admin/import-contacts", methods=["POST"])
@auth_required
def api_admin_import_contacts():
    """Import contacts from a list.
    
    POST body: {"contacts": [{"email": "...", "name": "...", "agency": "..."}, ...]}
    Deduplicates by email. Merges with existing CRM contacts.
    """
    import re as _re, hashlib
    from src.core.db import upsert_contact
    
    data = request.get_json(silent=True) or {}
    incoming = data.get("contacts", [])
    if not incoming:
        return jsonify({"ok": False, "error": "No contacts provided"})
    
    crm_path = os.path.join(DATA_DIR, "crm_contacts.json")
    try:
        with open(crm_path) as f:
            crm = json.load(f)
    except Exception:
        crm = {}
    
    before_count = len(crm)
    created = []
    skipped = []
    
    for c in incoming:
        em = (c.get("email") or "").lower().strip()
        if not em or "@" not in em:
            continue
        cid = hashlib.md5(em.encode()).hexdigest()[:16]
        
        if cid in crm:
            skipped.append(em)
            continue
        
        name = c.get("name", "")
        agency = c.get("agency", "")
        tags = c.get("tags", ["imported"])
        
        crm[cid] = {
            "id": cid, "buyer_name": name, "buyer_email": em,
            "buyer_phone": c.get("phone", ""), "agency": agency,
            "title": c.get("title", ""), "department": c.get("department", ""),
            "linkedin": "", "notes": c.get("notes", "Imported from Google Contacts"),
            "tags": tags, "total_spend": 0, "po_count": 0,
            "categories": {}, "items_purchased": [], "purchase_orders": [],
            "last_purchase": "", "score": 40, "opportunity_score": 0,
            "outreach_status": "new", "activity": [],
        }
        upsert_contact({"id": cid, "buyer_name": name, "buyer_email": em,
                        "agency": agency, "source": "google_import",
                        "outreach_status": "new", "is_reytech_customer": False})
        created.append({"email": em, "name": name, "agency": agency})
    
    if created:
        with open(crm_path, "w") as f:
            json.dump(crm, f, indent=2, default=str)
    
    log.info("ADMIN IMPORT-CONTACTS: %d created, %d skipped (already exist)", len(created), len(skipped))
    
    return jsonify({
        "ok": True,
        "created": created,
        "created_count": len(created),
        "skipped": skipped,
        "before": before_count,
        "after": len(crm),
    })


@bp.route("/api/pricecheck/<pcid>/clear-quote", methods=["POST"])
@auth_required
def api_pricecheck_clear_quote(pcid):
    """Clear a stale/wrong reytech_quote_number from a PC."""
    pcs = _load_price_checks()
    if pcid not in pcs:
        return jsonify({"ok": False, "error": "PC not found"})
    old_qnum = pcs[pcid].get("reytech_quote_number", "")
    pcs[pcid]["reytech_quote_number"] = ""
    pcs[pcid]["status"] = "parsed"  # Reset to parsed so it can be re-generated
    _save_price_checks(pcs)
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.execute("UPDATE price_checks SET reytech_quote_number=NULL, status='parsed' WHERE id=?", (pcid,))
    except Exception as e:
        log.debug("SQLite clear-quote: %s", e)
    log.info("CLEARED quote number %s from PC %s", old_qnum, pcid)
    return jsonify({"ok": True, "cleared": old_qnum})



@bp.route("/api/admin/rfq-cleanup", methods=["POST"])
@auth_required
def api_admin_rfq_cleanup():
    """Remove AMS 704 price check PDFs that incorrectly landed in the RFQ queue.
    These appear when the same 704 email was processed before the routing fix.
    Moves them to PC queue if not already there, then removes from rfq queue.
    """
    from src.api.dashboard import load_rfqs, save_rfqs
    import uuid as _uuid

    rfqs = load_rfqs()
    removed = []
    kept = []

    for rid, r in list(rfqs.items()):
        # Detect if this RFQ entry is actually a 704 price check:
        # 1. Attachments include a 704 form type
        atts = r.get("attachments_raw", []) or []
        templates = r.get("templates", {}) or {}
        is_704 = (
            "704" in " ".join(str(a) for a in atts).lower() or
            "704a" in templates or
            "704" in str(r.get("email_subject", "")).lower() or
            # Has no 704B (full RFQ requires 704B)
            ("704b" not in templates and r.get("source") == "email" and 
             any("704" in str(a).lower() for a in atts))
        )
        
        # Also flag if it exactly matches a PC we have
        pcs = _load_price_checks()
        sol = r.get("solicitation_number", "")
        matching_pc = any(
            str(pc.get("pc_number","")).replace("-","").replace(" ","").replace("#","") ==
            str(sol).replace("-","").replace(" ","").replace("#","")
            for pc in pcs.values()
        )
        
        if is_704 or matching_pc:
            removed.append({
                "rfq_id": rid,
                "solicitation": sol,
                "requestor": r.get("requestor_name", r.get("requestor_email", "")),
                "reason": "matching_pc" if matching_pc else "detected_704_form",
            })
            del rfqs[rid]
        else:
            kept.append(rid)

    save_rfqs(rfqs)
    log.info("RFQ cleanup: removed %d entries (%s), kept %d",
             len(removed), [r["solicitation"] for r in removed], len(kept))
    return jsonify({
        "ok": True,
        "removed": len(removed),
        "kept": len(kept),
        "removed_entries": removed,
    })



# ═══════════════════════════════════════════════════════════════════════════════

@bp.route("/api/item-link/lookup", methods=["POST"])
@auth_required
def api_item_link_lookup():
    """
    POST { url: "https://grainger.com/product/..." }
    Returns structured product data: title, price, part_number, shipping, supplier.
    Used for the item_link autofill on PC and RFQ line items.
    """
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "url required"})

    try:
        from src.agents.item_link_lookup import lookup_from_url
        result = lookup_from_url(url)
        return jsonify(result)
    except Exception as e:
        log.error("item_link_lookup API error: %s", e)
        return jsonify({"ok": False, "error": str(e)})
