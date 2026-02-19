# routes_agents.py

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
# 7 routes, 506 lines
# Loaded by dashboard.py via load_module()

# Agent Control Panel (Phase 14)
# ═══════════════════════════════════════════════════════════════════════

@bp.route("/agents")
@auth_required
def agents_page():
    """Agent Control Panel — click buttons instead of writing API calls."""
    from src.api.templates import render_agents_page
    return render_agents_page()

# ════════════════════════════════════════════════════════════════════════════════
# EMAIL TEMPLATE LIBRARY  (PRD Feature 4.3 — P0)
# ════════════════════════════════════════════════════════════════════════════════
def _load_email_templates() -> dict:
    path = os.path.join(DATA_DIR, "email_templates.json")
    if not os.path.exists(path):
        seed = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "seed_data", "email_templates.json")
        if os.path.exists(seed):
            import shutil; shutil.copy2(seed, path)
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {"templates": {}}


def _save_email_templates(data: dict):
    path = os.path.join(DATA_DIR, "email_templates.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _get_recent_scprs_pos(agency: str, months: int = 3) -> list:
    """Return SCPRS purchase orders for an agency from the last N months.

    Returns list of {po_number, items, date, link} dicts sorted by date desc.
    Data sourced from intel_buyers.json and activity_log.
    """
    from datetime import timezone
    cutoff = datetime.now(timezone.utc) - timedelta(days=months * 30)
    results = []
    try:
        # Check intel_buyers for SCPRS purchase data
        from src.agents.sales_intel import _load_json as _il, BUYERS_FILE as _BF
        buyers = _il(_BF)
        buyer_list = buyers.get("buyers", []) if isinstance(buyers, dict) else buyers
        agency_lower = agency.lower()
        for b in buyer_list:
            if agency_lower not in (b.get("agency") or "").lower():
                continue
            for po in b.get("recent_pos", b.get("purchase_orders", [])):
                try:
                    po_date = po.get("date") or po.get("purchase_date") or ""
                    if po_date:
                        from dateutil.parser import parse as _dp
                        dt = _dp(po_date).replace(tzinfo=timezone.utc)
                        if dt >= cutoff:
                            results.append({
                                "po_number": po.get("po_number") or po.get("number") or "",
                                "items": po.get("items") or po.get("description") or "",
                                "date": po_date[:10],
                                "amount": po.get("amount") or po.get("total") or 0,
                                "link": po.get("link") or po.get("scprs_link") or
                                        f"https://www.dgsapps.dgs.ca.gov/OA_HTML/OA.jsp?OAFunc=SCPRS_SEARCH&agency={agency.replace(' ', '%20')}",
                            })
                except Exception:
                    pass
            # Also check top_purchases / categories
            for item in b.get("top_purchases", [])[:3]:
                if isinstance(item, dict) and item.get("date"):
                    try:
                        from dateutil.parser import parse as _dp
                        dt = _dp(item["date"]).replace(tzinfo=timezone.utc)
                        if dt >= cutoff:
                            results.append({
                                "po_number": item.get("po_number") or "",
                                "items": item.get("description") or item.get("item") or "",
                                "date": item["date"][:10],
                                "amount": item.get("amount") or 0,
                                "link": item.get("scprs_link") or
                                        f"https://www.dgsapps.dgs.ca.gov/OA_HTML/OA.jsp?OAFunc=SCPRS_SEARCH",
                            })
                    except Exception:
                        pass
    except Exception:
        pass
    # Sort newest first, deduplicate by PO number
    seen = set()
    out = []
    for r in sorted(results, key=lambda x: x.get("date", ""), reverse=True):
        key = r.get("po_number") or r.get("items", "")[:30]
        if key and key not in seen:
            seen.add(key)
            out.append(r)
    return out[:5]  # top 5 most recent


def _personalize_template(template: dict, contact: dict = None, quote: dict = None, extra: dict = None) -> dict:
    """Fill template variables from CRM contact + quote data.

    Supports both [name] and {{name}} variable formats.
    Auto-fetches recent SCPRS POs (<3 months) for the contact's agency
    and injects them as [recent_po_line] and [recent_po_link].
    """
    from datetime import timezone
    expiry = (datetime.now() + timedelta(days=45)).strftime("%B %d, %Y")
    vars_ = {
        "name": "",
        "agency": "",
        "items": "",
        "items_preview": "",
        "items_summary": "",
        "date": datetime.now().strftime("%B %d, %Y"),
        "category": "",
        "quote_number": "",
        "quote_date": "",
        "total": "",
        "po_number": "",
        "delivery_date": "5–7 business days",
        "tax_note": "tax included",
        "expiry_date": expiry,
        "lead_time": "5–7 business days",
        "recent_po_line": "",
        "recent_po_link": "",
        "recent_po_line_warm": "",
    }

    # ── Contact fields ────────────────────────────────────────────────────
    if contact:
        raw_name = contact.get("buyer_name") or contact.get("name") or ""
        vars_["name"] = raw_name.split()[0] if raw_name else ""
        vars_["agency"] = contact.get("agency") or ""
        cats = contact.get("categories") or contact.get("top_categories") or []
        if isinstance(cats, list) and cats:
            vars_["items"] = ", ".join(str(c) for c in cats[:3])
            vars_["category"] = str(cats[0])
        elif isinstance(cats, str):
            vars_["items"] = cats
            vars_["category"] = cats.split(",")[0].strip()

        # ── Recent SCPRS POs (<3 months) ─────────────────────────────────
        agency = vars_["agency"]
        if agency:
            recent_pos = _get_recent_scprs_pos(agency, months=3)
            if recent_pos:
                po = recent_pos[0]  # most recent
                po_num = po.get("po_number") or ""
                po_items = po.get("items") or vars_["items"]
                po_date = po.get("date") or ""
                po_link = po.get("link") or ""
                # Build [recent_po_line] for outreach templates
                if po_num and po_link:
                    vars_["recent_po_line"] = (
                        f"purchased {po_items} on {po_date} "
                        f"(PO {po_num} — {po_link})"
                    )
                    vars_["recent_po_link"] = po_link
                elif po_items:
                    vars_["recent_po_line"] = f"recently purchased {po_items}"
                # Warm/lost variant
                vars_["recent_po_line_warm"] = (
                    f"We previously supplied {agency} with {po_items}"
                    + (f" (PO {po_num})" if po_num else "") + " and delivered on time."
                )
                # Items preview for outreach
                vars_["items_preview"] = "\n".join(
                    f"  • {p.get('items','')[:60]}" + (f" — ${p.get('amount',0):,.0f}" if p.get('amount') else "")
                    for p in recent_pos[:3]
                )
            else:
                # Use fallback text from template definition
                fallback = template.get("recent_po_fallback", f"procures {vars_['category'] or 'supplies'} through SCPRS")
                vars_["recent_po_line"] = fallback.replace("[category]", vars_["category"]).replace("[agency]", agency)
                vars_["recent_po_line_warm"] = f"We're actively monitoring {agency}'s upcoming procurement activity."

    # ── Quote fields ──────────────────────────────────────────────────────
    if quote:
        vars_["quote_number"] = quote.get("quote_number") or ""
        vars_["quote_date"] = (quote.get("created_at") or "")[:10]
        qitems = quote.get("items_text") or vars_["items"]
        vars_["items"] = qitems
        total = quote.get("total") or 0
        vars_["total"] = f"${total:,.2f}" if total else ""
        line_items = quote.get("items_detail") or quote.get("line_items") or []
        vars_["items_summary"] = "\n".join(
            f"  • {it.get('description','')[:60]}"
            f"  ${it.get('unit_price', it.get('our_price', 0)):,.2f} x {it.get('qty', it.get('quantity', 1))}"
            for it in line_items[:8]
        )

    # ── Extra overrides ───────────────────────────────────────────────────
    if extra:
        vars_.update(extra)

    # ── Substitute both [name] and {{name}} formats ───────────────────────
    subject = template.get("subject", "")
    body = template.get("body", "")
    for k, v in vars_.items():
        subject = subject.replace(f"[{k}]", str(v)).replace("{{" + k + "}}", str(v))
        body = body.replace(f"[{k}]", str(v)).replace("{{" + k + "}}", str(v))

    return {"subject": subject, "body": body, "variables_used": vars_}


@bp.route("/templates")
@auth_required
def email_templates_page():
    """Email Template Library — PRD Feature 4.3 (P0)."""
    data = _load_email_templates()
    templates_list = list(data.get("templates", {}).values())
    count = len(templates_list)
    cards = ""
    for t in templates_list:
        tid = t.get("id", "")
        tname = t.get("name", "")
        tcat = t.get("category", "other")
        tsubj = t.get("subject", "")[:80]
        tbody_prev = t.get("body", "")[:180].replace("\n", " ").replace('"', "'").replace("<", "&lt;")
        tags = " ".join(
            f'<span style="background:#21262d;color:#8b949e;padding:2px 6px;border-radius:3px;font-size:11px">{g}</span>'
            for g in t.get("tags", [])
        )
        vars_str = ", ".join("{{" + v + "}}" for v in t.get("variables", []))
        cat_color = {"outreach": "#1f6feb", "followup": "#e3b341",
                     "transaction": "#238636", "nurture": "#8957e5"}.get(tcat, "#484f58")
        cards += f"""<div class="card tmpl-card" data-cat="{tcat}" id="tmpl-{tid}" style="margin-bottom:12px">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap">
            <div style="flex:1;min-width:0">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
                <span style="background:{cat_color}22;color:{cat_color};border:1px solid {cat_color}44;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;text-transform:uppercase">{tcat}</span>
                <h3 style="margin:0;font-size:16px;color:#e6edf3">{tname}</h3>
              </div>
              <div style="font-size:13px;color:#8b949e;margin-bottom:4px">Subject: <b style="color:#c9d1d9">{tsubj}</b></div>
              <div style="font-size:12px;color:#484f58;margin-bottom:6px">{tbody_prev}&#x2026;</div>
              <div style="font-size:11px;color:#3fb950">Variables: {vars_str}</div>
              <div style="margin-top:6px">{tags}</div>
            </div>
            <div style="display:flex;flex-direction:column;gap:6px;min-width:120px">
              <button onclick="previewTmpl('{tid}')" class="btn" style="background:#21262d;color:#c9d1d9;border:1px solid #30363d;font-size:12px;padding:5px 10px">Preview</button>
              <button onclick="composeTmpl('{tid}')" class="btn" style="background:#1f6feb;color:#fff;font-size:12px;padding:5px 10px">Use Template</button>
            </div>
          </div></div>"""

    return f"""<!doctype html><html><head><title>Email Templates</title>
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <style>
    body{{font-family:"Segoe UI",system-ui,sans-serif;background:#0d1117;color:#c9d1d9;margin:0;padding:20px;font-size:15px}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}}
    .btn{{padding:7px 14px;border-radius:6px;border:none;cursor:pointer;font-weight:600;font-size:14px;text-decoration:none;display:inline-block}}
    a{{color:#58a6ff;text-decoration:none}}
    input,textarea,select{{background:#0d1117;border:1px solid #484f58;color:#e6edf3;padding:8px;border-radius:5px;font-size:14px;width:100%;box-sizing:border-box;margin-bottom:10px}}
    textarea{{resize:vertical;min-height:140px;font-family:monospace;line-height:1.5}}
    .modal{{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.75);z-index:100;padding:20px;overflow:auto;align-items:flex-start;justify-content:center}}
    .modal.open{{display:flex}}
    .modal-box{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:24px;max-width:700px;width:100%;margin-top:20px}}
    label{{display:block;font-size:11px;color:#8b949e;margin-bottom:3px;font-weight:700;text-transform:uppercase;letter-spacing:.3px}}
    .msg{{padding:8px 12px;border-radius:6px;font-size:13px;margin-top:8px}}
    .msg-ok{{background:#23863622;color:#3fb950;border:1px solid #23863655}}
    .msg-err{{background:#da363322;color:#f85149;border:1px solid #da363355}}
    @media(max-width:600px){{body{{padding:10px}}}}
    </style></head><body>
    <div style="max-width:900px;margin:0 auto">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap">
        <a href="/">&#8592; Home</a>
        <span style="color:#484f58">/</span>
        <h1 style="margin:0;font-size:22px">Email Templates</h1>
        <span style="font-size:13px;color:#484f58">{count} templates</span>
        <button onclick="composeTmpl('')" class="btn" style="margin-left:auto;background:#238636;color:#fff">+ Compose</button>
      </div>

      <div class="card" style="margin-bottom:16px;background:#0d2137;border-color:#1f6feb">
        <b style="color:#58a6ff">PRD Feature 4.3</b>
        <span style="color:#8b949e;font-size:13px"> — Personalized templates. Use from CRM contact pages or compose below. Target: outreach drafted in &lt;2 min.</span>
      </div>

      <div id="tmpl-list">{cards}</div>
    </div>

    <div class="modal" id="previewModal">
      <div class="modal-box">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
          <h2 style="margin:0;font-size:17px" id="prevTitle">Preview</h2>
          <button onclick="closeModal('previewModal')" style="background:none;border:none;color:#8b949e;font-size:22px;cursor:pointer">x</button>
        </div>
        <label>Subject</label>
        <div id="prevSubj" style="background:#0d1117;border:1px solid #30363d;padding:10px;border-radius:5px;font-size:14px;margin-bottom:10px"></div>
        <label>Body</label>
        <pre id="prevBody" style="background:#0d1117;border:1px solid #30363d;padding:14px;border-radius:5px;font-size:13px;white-space:pre-wrap;line-height:1.6;max-height:400px;overflow:auto;margin:0"></pre>
        <div style="margin-top:12px;display:flex;gap:8px">
          <button onclick="closeModal('previewModal')" class="btn" style="background:#21262d;color:#c9d1d9;border:1px solid #30363d">Close</button>
          <button onclick="navigator.clipboard.writeText(document.getElementById('prevBody').textContent);this.textContent='Copied!'" class="btn" style="background:#1f6feb;color:#fff">Copy Body</button>
        </div>
      </div>
    </div>

    <div class="modal" id="composeModal">
      <div class="modal-box">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px">
          <h2 style="margin:0;font-size:17px">Compose Email</h2>
          <button onclick="closeModal('composeModal')" style="background:none;border:none;color:#8b949e;font-size:22px;cursor:pointer">x</button>
        </div>
        <label>Template</label>
        <select id="cmpTmpl" onchange="draftEmail()">
          <option value="">-- select template --</option>
          {''.join(f'<option value="{t.get("id")}">{t.get("name")}</option>' for t in templates_list)}
        </select>
        <label>Contact email (for personalization)</label>
        <input id="cmpContact" placeholder="buyer@agency.ca.gov" oninput="draftEmail()">
        <label>To</label>
        <input id="cmpTo" placeholder="recipient@agency.ca.gov">
        <label>Subject</label>
        <input id="cmpSubj">
        <label>Body</label>
        <textarea id="cmpBody"></textarea>
        <div id="cmpMsg"></div>
        <div style="display:flex;gap:8px;margin-top:4px">
          <button onclick="closeModal('composeModal')" class="btn" style="background:#21262d;color:#c9d1d9;border:1px solid #30363d">Cancel</button>
          <button onclick="copyAll()" class="btn" style="background:#21262d;color:#58a6ff;border:1px solid #1f6feb">Copy All</button>
          <button onclick="sendEmail()" class="btn" id="sendBtn" style="background:#238636;color:#fff">Send Email</button>
        </div>
      </div>
    </div>

    <script>
    let allTmpls = {{}};
    fetch('/api/email/templates').then(r=>r.json()).then(d=>{{ allTmpls = d.templates || {{}}; }});

    function previewTmpl(id) {{
      const t = allTmpls[id] || {{}};
      document.getElementById('prevTitle').textContent = t.name || id;
      document.getElementById('prevSubj').textContent = t.subject || '';
      document.getElementById('prevBody').textContent = t.body || '';
      document.getElementById('previewModal').classList.add('open');
    }}
    function composeTmpl(id) {{
      if (id) document.getElementById('cmpTmpl').value = id;
      document.getElementById('composeModal').classList.add('open');
      draftEmail();
    }}
    function closeModal(id) {{ document.getElementById(id).classList.remove('open'); }}
    function draftEmail() {{
      const tid = document.getElementById('cmpTmpl').value;
      const contact = document.getElementById('cmpContact').value;
      if (!tid) return;
      fetch('/api/email/draft', {{method:'POST',headers:{{'Content-Type':'application/json'}},
        body: JSON.stringify({{template_id: tid, contact_email: contact}})
      }}).then(r=>r.json()).then(d=>{{
        document.getElementById('cmpSubj').value = d.subject || '';
        document.getElementById('cmpBody').value = d.body || '';
        if (d.to) document.getElementById('cmpTo').value = d.to;
      }});
    }}
    function copyAll() {{
      const txt = 'To: '+document.getElementById('cmpTo').value+'\nSubject: '+document.getElementById('cmpSubj').value+'\n\n'+document.getElementById('cmpBody').value;
      navigator.clipboard.writeText(txt).then(()=>{{ document.getElementById('cmpMsg').innerHTML='<div class="msg msg-ok">Copied to clipboard</div>'; }});
    }}
    function sendEmail() {{
      const btn = document.getElementById('sendBtn');
      btn.disabled = true; btn.textContent = 'Sending...';
      fetch('/api/email/send', {{method:'POST',headers:{{'Content-Type':'application/json'}},
        body: JSON.stringify({{to:document.getElementById('cmpTo').value, subject:document.getElementById('cmpSubj').value, body:document.getElementById('cmpBody').value}})
      }}).then(r=>r.json()).then(d=>{{
        btn.disabled=false; btn.textContent='Send Email';
        document.getElementById('cmpMsg').innerHTML = d.ok
          ? '<div class="msg msg-ok">Sent successfully</div>'
          : '<div class="msg msg-err">' + (d.message||d.error||'Send failed — check Gmail config in Railway env') + '</div>';
      }}).catch(e=>{{btn.disabled=false;btn.textContent='Send Email';document.getElementById('cmpMsg').innerHTML='<div class="msg msg-err">'+e+'</div>';}});
    }}
    </script></body></html>"""


@bp.route("/api/email/templates")
@auth_required
def api_email_templates():
    """Return all email templates."""
    data = _load_email_templates()
    return jsonify({"ok": True, "templates": data.get("templates", {}),
                    "count": len(data.get("templates", {}))})


@bp.route("/api/email/templates/<tid>", methods=["GET"])
@auth_required
def api_email_template_get(tid):
    """Get single template."""
    data = _load_email_templates()
    t = data.get("templates", {}).get(tid)
    if not t:
        return jsonify({"ok": False, "error": "Template not found"})
    return jsonify({"ok": True, "template": t})


@bp.route("/api/email/templates/<tid>", methods=["POST", "PUT"])
@auth_required
def api_email_template_save(tid):
    """Create or update a template."""
    body = request.get_json(silent=True) or {}
    data = _load_email_templates()
    templates = data.setdefault("templates", {})
    templates[tid] = {
        "id": tid,
        "name": body.get("name", tid),
        "category": body.get("category", "outreach"),
        "subject": body.get("subject", ""),
        "body": body.get("body", ""),
        "variables": body.get("variables", []),
        "tags": body.get("tags", []),
        "updated_at": datetime.now().isoformat(),
    }
    data["updated_at"] = datetime.now().isoformat()
    _save_email_templates(data)
    return jsonify({"ok": True, "template": templates[tid]})


@bp.route("/api/email/draft", methods=["POST"])
@auth_required
def api_email_draft():
    """Return a personalized draft from a template + optional contact.

    POST { template_id, contact_id?, contact_email?, quote_number? }
    Returns { ok, subject, body, to, template_name }
    """
    body = request.get_json(silent=True) or {}
    tid = body.get("template_id", "")
    data = _load_email_templates()
    template = data.get("templates", {}).get(tid)
    if not template:
        return jsonify({"ok": False, "error": f"Template '{tid}' not found"})

    contact = None
    to_email = ""
    # Try to find contact by ID or email
    contact_id = body.get("contact_id", "")
    contact_email = body.get("contact_email", "")
    if contact_id or contact_email:
        try:
            crm = _load_crm_contacts()
            if isinstance(crm, dict):
                for cid, c in crm.items():
                    if cid == contact_id or c.get("buyer_email") == contact_email:
                        contact = c
                        to_email = c.get("buyer_email", "")
                        break
        except Exception:
            pass

    quote = None
    qn = body.get("quote_number", "")
    if qn:
        try:
            from src.forms.quote_generator import get_all_quotes
            for q in get_all_quotes():
                if q.get("quote_number") == qn:
                    quote = q
                    break
        except Exception:
            pass

    result = _personalize_template(template, contact=contact, quote=quote)
    return jsonify({
        "ok": True,
        "subject": result["subject"],
        "body": result["body"],
        "to": to_email,
        "template_name": template.get("name", ""),
        "template_id": tid,
    })


@bp.route("/api/email/send", methods=["POST"])
@auth_required
def api_email_send():
    """Send an email. Requires GMAIL_ADDRESS + GMAIL_PASSWORD in Railway env."""
    body = request.get_json(silent=True) or {}
    to = body.get("to", "")
    subject = body.get("subject", "")
    email_body = body.get("body", "")

    if not to:
        return jsonify({"ok": False, "error": "to field required"})

    gmail = os.environ.get("GMAIL_ADDRESS", "")
    pwd = os.environ.get("GMAIL_PASSWORD", "")

    if not gmail or not pwd:
        return jsonify({
            "ok": False,
            "message": "Gmail not configured. Add GMAIL_ADDRESS and GMAIL_PASSWORD to Railway environment variables to enable sending.",
            "staged": True,
            "to": to, "subject": subject,
        })

    try:
        from src.agents.email_poller import EmailSender
        sender = EmailSender({"email": gmail, "email_password": pwd})
        sender.send({"to": to, "subject": subject, "body": email_body, "attachments": []})
        log.info("Email sent via template: to=%s subject=%s", to, subject[:50])
        return jsonify({"ok": True, "message": f"Sent to {to}", "to": to})
    except Exception as e:
        log.error("Email send failed: %s", e)
        return jsonify({"ok": False, "error": str(e)})




# ═══════════════════════════════════════════════════════════════════════