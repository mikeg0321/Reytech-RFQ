"""
Agency package configurations — determines which forms each state agency requires.
Standalone module (no Flask imports) so it can be imported from anywhere.
"""

AVAILABLE_FORMS = [
    {"id": "703b", "name": "AMS 703B", "desc": "RFQ Pricing Form"},
    {"id": "703c", "name": "AMS 703C", "desc": "Fair & Reasonable Form"},
    {"id": "704b", "name": "AMS 704B", "desc": "Quote Worksheet"},
    {"id": "bidpkg", "name": "Bid Package", "desc": "Agency Bid Package"},
    {"id": "quote", "name": "Reytech Quote", "desc": "Formal quote on letterhead"},
    {"id": "std204", "name": "STD 204", "desc": "Payee Data Record"},
    {"id": "std205", "name": "STD 205", "desc": "Payee Supplemental"},
    {"id": "sellers_permit", "name": "Seller's Permit", "desc": "CA Seller's Permit"},
    {"id": "dvbe843", "name": "DVBE 843", "desc": "DVBE Declarations"},
    {"id": "cv012_cuf", "name": "CV 012 CUF", "desc": "CalVet Commercially Useful Function"},
    {"id": "bidder_decl", "name": "Bidder Declaration", "desc": "GSPD-05-105"},
    {"id": "darfur_act", "name": "Darfur Act", "desc": "DGS PD 1"},
    {"id": "calrecycle74", "name": "CalRecycle 74", "desc": "Postconsumer Recycled Content"},
    {"id": "std1000", "name": "STD 1000", "desc": "GenAI Reporting / Disclosure"},
    {"id": "drug_free", "name": "Drug-Free STD 21", "desc": "Drug-Free Workplace"},
    {"id": "barstow_cuf", "name": "Barstow CUF", "desc": "Barstow facility CUF"},
    {"id": "obs_1600", "name": "OBS 1600", "desc": "Food Product Certification"},
    {"id": "w9", "name": "W-9", "desc": "IRS W-9 Tax Form"},
]

# Patterns in email body/PDF text that indicate a specific form is required
# These match what buyers list in their "what to include" instructions
FORM_TEXT_PATTERNS = {
    "std204":       ["STD 204", "STD204", "PAYEE DATA", "PAYEE RECORD", "204/205 PAYEE"],
    "std205":       ["STD 205", "STD205", "PAYEE SUPPLEMENTAL"],
    "dvbe843":      ["STD 843", "DGS PD 843", "DVBE DECLARATION", "DVBE 843", "843"],
    "darfur_act":   ["DARFUR", "DARFUR CONTRACTING", "DARFUR ACT"],
    "cv012_cuf":    ["CV 012", "CV012", "COMMERCIALLY USEFUL FUNCTION", "CUF FORM", "CUF,"],
    "calrecycle74": ["CALRECYCLE", "RECYCLED-CONTENT", "RECYCLED CONTENT", "POSTCONSUMER", "074"],
    "bidder_decl":  ["BIDDER DECLARATION", "GSPD-05-105", "GSPD 05"],
    "std1000":      ["STD 1000", "STD1000", "GENAI", "GEN AI DISCLOSURE", "GEN AI REPORTING"],
    "sellers_permit": ["SELLER'S PERMIT", "SELLERS PERMIT", "SELLER PERMIT"],
    "w9":           ["W-9", "W9", "TAX FORM"],
    "drug_free":    ["DRUG-FREE", "DRUG FREE WORKPLACE"],
    "obs_1600":     ["OBS 1600", "FOOD PRODUCT", "AGRICULTURAL PRODUCT"],
    "quote":        ["YOUR QUOTE", "YOUR BID", "PRICE QUOTE", "QUOTATION"],
    "703b":         ["703B", "703-B", "AMS 703"],
    "703c":         ["703C", "703-C", "FAIR AND REASONABLE"],
    "704b":         ["704B", "704-B", "QUOTE WORKSHEET"],
}

DEFAULT_AGENCY_CONFIGS = {
    "calvet": {
        "name": "Cal Vet / DVA",
        "match_patterns": ["CALVET", "CAL VET", "CVA", "VHC", "VETERANS HOME",
                          "VETERANS AFFAIRS", "CALVET.CA.GOV"],
        "required_forms": ["quote", "calrecycle74", "bidder_decl", "dvbe843", "darfur_act",
                          "cv012_cuf", "std204", "std205", "std1000", "sellers_permit"],
        "optional_forms": ["barstow_cuf", "obs_1600", "drug_free"],
        "notes": "California Department of Veterans Affairs. No AMS 703B/704B — uses Reytech quote + compliance forms. STD 205 supplement required.",
        "default_markup_pct": 25,
        "payment_terms": "Net 30",
        "shipping_terms": "FOB Destination, Freight Prepaid",
        "delivery_days": "3-5 business days",
    },
    "calvet_barstow": {
        "name": "Cal Vet — Barstow",
        "match_patterns": ["BARSTOW", "BARSTOW VETERANS"],
        "required_forms": ["quote", "calrecycle74", "bidder_decl", "dvbe843", "darfur_act",
                          "cv012_cuf", "barstow_cuf", "std204", "std205", "std1000", "sellers_permit"],
        "optional_forms": ["obs_1600", "drug_free"],
        "notes": "Cal Vet Barstow facility — requires BOTH CV 012 CUF AND Barstow-specific CUF.",
        "default_markup_pct": 25,
        "payment_terms": "Net 30",
        "shipping_terms": "FOB Destination, Freight Prepaid",
        "delivery_days": "3-5 business days",
    },
    "cchcs": {
        "name": "CCHCS / CDCR",
        "match_patterns": ["CCHCS", "CDCR", "CORRECTIONS", "CORRECTIONAL",
                          "PRISON", "STATE PRISON",
                          "CIM", "CMC", "CTF", "CIW", "LAC", "SAC", "SQ",
                          "FSP", "SATF", "KVSP", "CRC", "CCWF", "CHCF",
                          "DVI", "MCSP", "NKSP", "PBSP", "RJD", "SCC",
                          "SOL", "SVSP", "VSP", "WSP", "CEN", "ISP",
                          "ASP", "HDSP", "CAL", "PVSP", "CVSP",
                          "PELICAN BAY", "FOLSOM", "CORCORAN", "IRONWOOD",
                          "CENTINELA", "CALIPATRIA", "CHUCKAWALLA", "AVENAL",
                          "SOLANO", "VACAVILLE", "REPRESA", "LANCASTER",
                          "CHOWCHILLA", "IMPERIAL", "BLYTHE", "CORONA",
                          "CDCR.CA.GOV", "CCHCS.CA.GOV"],
        "required_forms": ["703b", "704b", "bidpkg", "quote"],
        "optional_forms": ["703c", "sellers_permit", "dvbe843", "std204", "calrecycle74",
                          "bidder_decl", "darfur_act", "obs_1600", "drug_free", "std1000"],
        "notes": "CCHCS / CDCR. Package: 703B (or 703C) + 704B + CCHCS Bid Package. DVBE 843 and seller's permit are inside the bid package.",
        "default_markup_pct": 25,
        "payment_terms": "Net 45",
        "shipping_terms": "FOB Destination, Freight Prepaid",
        "delivery_days": "5-7 business days",
    },
    "dsh": {
        "name": "DSH — State Hospitals",
        "match_patterns": ["DSH", "STATE HOSPITAL", "DEPARTMENT OF STATE HOSPITALS",
                          "ATASCADERO", "COALINGA", "METROPOLITAN", "NAPA",
                          "PATTON", "DSH.CA.GOV"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843", "bidder_decl",
                          "darfur_act", "calrecycle74"],
        "optional_forms": ["std1000", "drug_free"],
        "notes": "Department of State Hospitals. Similar to DGS but with CalRecycle requirement.",
        "default_markup_pct": 25,
        "payment_terms": "Net 45",
        "shipping_terms": "FOB Destination, Freight Prepaid",
        "delivery_days": "5-7 business days",
    },
    "dgs": {
        "name": "DGS",
        "match_patterns": ["DGS", "GENERAL SERVICES"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843", "bidder_decl", "darfur_act"],
        "optional_forms": ["std1000", "calrecycle74"],
        "notes": "Department of General Services. No AMS forms — uses their own bid format.",
        "default_markup_pct": 25,
        "payment_terms": "Net 30",
        "shipping_terms": "FOB Destination, Freight Prepaid",
        "delivery_days": "5-7 business days",
    },
    "calfire": {
        "name": "CAL FIRE",
        "match_patterns": ["CALFIRE", "CAL FIRE", "FORESTRY", "FIRE PROTECTION"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843"],
        "optional_forms": ["bidder_decl", "darfur_act"],
        "notes": "California Department of Forestry and Fire Protection.",
        "default_markup_pct": 20,
        "payment_terms": "Net 30",
        "shipping_terms": "FOB Destination, Freight Prepaid",
        "delivery_days": "5-7 business days",
    },
    "other": {
        "name": "Other / Unknown",
        "match_patterns": [],
        "required_forms": ["quote", "std204", "sellers_permit"],
        "optional_forms": ["dvbe843", "bidder_decl"],
        "notes": "Default config for unrecognized agencies. Minimal forms.",
        "default_markup_pct": 25,
        "payment_terms": "Net 45",
        "shipping_terms": "FOB Destination, Freight Prepaid",
        "delivery_days": "5-7 business days",
    },
}


def get_agency_config(agency_key):
    """Get the config dict for an agency key (case-insensitive).
    Returns the 'other' config as fallback if key not found."""
    if not agency_key:
        return DEFAULT_AGENCY_CONFIGS.get("other", {})
    key = agency_key.lower().strip()
    # Try direct match first, then mapped aliases
    _alias_map = {"cdcr": "cchcs", "calvet": "calvet", "cal vet": "calvet",
                  "cal fire": "calfire", "cal_fire": "calfire"}
    resolved = _alias_map.get(key, key)
    return DEFAULT_AGENCY_CONFIGS.get(resolved, DEFAULT_AGENCY_CONFIGS.get("other", {}))


def extract_required_forms_from_text(text):
    """Parse email body or PDF text to detect which forms the buyer is asking for.

    Buyers often list required forms like:
        - A completed STD 204/205 Payee Data Record
        - Darfur Contracting Act Certification
        - CV 012, Commercially Useful Function Form
        - CalRecycle 074, Postconsumer Recycled-Content Certification

    Returns: {"forms": ["std204", "darfur_act", ...], "raw_matches": [...]}
    """
    if not text:
        return {"forms": [], "raw_matches": []}

    text_upper = text.upper()
    found = []
    raw = []

    for form_id, patterns in FORM_TEXT_PATTERNS.items():
        for pat in patterns:
            if pat.upper() in text_upper:
                if form_id not in found:
                    found.append(form_id)
                    raw.append({"form": form_id, "matched": pat})
                break

    # Always include quote if any forms were requested
    if found and "quote" not in found:
        found.insert(0, "quote")

    return {"forms": found, "raw_matches": raw}


def load_agency_configs():
    """Load agency configs — defaults merged with DB overrides and learned data."""
    configs = dict(DEFAULT_AGENCY_CONFIGS)
    # Merge DB customizations if available
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT agency_key, required_forms, optional_forms FROM agency_package_configs"
            ).fetchall()
            for r in rows:
                key = r[0]
                if key in configs:
                    import json
                    try:
                        req = json.loads(r[1]) if r[1] else []
                        opt = json.loads(r[2]) if r[2] else []
                        if req:
                            configs[key]["required_forms"] = req
                        if opt:
                            configs[key]["optional_forms"] = opt
                    except Exception:
                        pass
    except Exception:
        pass
    return configs


def match_agency(rfq_data):
    """Match an RFQ to an agency config based on agency name, email, institution, etc.
    Also checks buyer history for learned agency preferences."""
    configs = load_agency_configs()

    search_text = " ".join([
        str(rfq_data.get("agency", "")),
        str(rfq_data.get("agency_name", "")),
        str(rfq_data.get("requestor_email", "")),
        str(rfq_data.get("email_sender", "")),
        str(rfq_data.get("institution", "")),
        str(rfq_data.get("delivery_location", "")),
        str(rfq_data.get("ship_to", "")),
        str(rfq_data.get("solicitation_number", "")),
        str(rfq_data.get("email_subject", "")),
    ]).upper()
    
    # Check Barstow before general CalVet (more specific first)
    for key in ["calvet_barstow", "dsh"]:
        cfg = configs.get(key, {})
        for pattern in cfg.get("match_patterns", []):
            if pattern in search_text:
                return key, cfg

    for key, cfg in configs.items():
        if key in ("other", "calvet_barstow", "dsh"):
            continue
        for pattern in cfg.get("match_patterns", []):
            if pattern in search_text:
                return key, cfg

    # Check buyer history — what agency did this buyer's past RFQs match?
    buyer_email = (rfq_data.get("requestor_email") or rfq_data.get("email_sender") or "").lower()
    if buyer_email:
        try:
            from src.core.db import get_db
            with get_db() as conn:
                row = conn.execute("""
                    SELECT agency FROM rfqs
                    WHERE requestor_email = ? AND agency != '' AND agency != 'unknown'
                    ORDER BY received_at DESC LIMIT 1
                """, (buyer_email,)).fetchone()
                if row and row[0] in configs:
                    return row[0], configs[row[0]]
        except Exception:
            pass

    return "other", configs["other"]


def learn_agency_forms(rfq_id, agency_key, forms_used, buyer_email=""):
    """Record which forms were actually used for an RFQ. Over time this
    builds a per-buyer and per-agency profile of required forms.

    Called after package generation. forms_used is a list of form IDs
    that were successfully generated (not skipped/failed).
    """
    import logging
    log = logging.getLogger("reytech.agency")
    try:
        from src.core.db import get_db
        import json
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS agency_form_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rfq_id TEXT,
                    agency_key TEXT,
                    buyer_email TEXT DEFAULT '',
                    forms_used TEXT DEFAULT '[]',
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("""
                INSERT INTO agency_form_history (rfq_id, agency_key, buyer_email, forms_used)
                VALUES (?, ?, ?, ?)
            """, (rfq_id, agency_key, buyer_email, json.dumps(forms_used)))
        log.debug("Learned agency forms: %s %s -> %s", agency_key, buyer_email, forms_used)
    except Exception as e:
        log.debug("Agency form learning: %s", e)


def get_buyer_form_preferences(buyer_email):
    """Get forms this buyer typically needs, based on past RFQs.
    Returns {"forms": [...], "agency": "...", "confidence": N} or None."""
    if not buyer_email:
        return None
    try:
        from src.core.db import get_db
        import json
        from collections import Counter
        with get_db() as conn:
            rows = conn.execute("""
                SELECT agency_key, forms_used FROM agency_form_history
                WHERE buyer_email = ? ORDER BY created_at DESC LIMIT 10
            """, (buyer_email.lower(),)).fetchall()
        if not rows:
            return None
        # Most common agency
        agencies = Counter(r[0] for r in rows)
        top_agency = agencies.most_common(1)[0][0]
        # Union of all forms used
        all_forms = set()
        for r in rows:
            try:
                all_forms.update(json.loads(r[1]))
            except Exception:
                pass
        return {
            "forms": sorted(all_forms),
            "agency": top_agency,
            "confidence": len(rows),
            "based_on": f"{len(rows)} past RFQs",
        }
    except Exception:
        return None
