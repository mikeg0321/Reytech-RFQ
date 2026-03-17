"""
Agency package configurations — determines which forms each state agency requires.
Standalone module (no Flask imports) so it can be imported from anywhere.
"""

AVAILABLE_FORMS = [
    {"id": "703b", "name": "AMS 703B", "desc": "RFQ Pricing Form"},
    {"id": "704b", "name": "AMS 704B", "desc": "Quote Worksheet"},
    {"id": "bidpkg", "name": "Bid Package", "desc": "Agency Bid Package"},
    {"id": "quote", "name": "Reytech Quote", "desc": "Formal quote on letterhead"},
    {"id": "std204", "name": "STD 204", "desc": "Payee Data Record"},
    {"id": "sellers_permit", "name": "Seller's Permit", "desc": "CA Seller's Permit"},
    {"id": "dvbe843", "name": "DVBE 843", "desc": "DVBE Declarations"},
    {"id": "cv012_cuf", "name": "CV 012 CUF", "desc": "CalVet Commercially Useful Function"},
    {"id": "bidder_decl", "name": "Bidder Declaration", "desc": "GSPD-05-105"},
    {"id": "darfur_act", "name": "Darfur Act", "desc": "DGS PD 1"},
    {"id": "calrecycle74", "name": "CalRecycle 74", "desc": "Postconsumer Recycled Content"},
    {"id": "std1000", "name": "STD 1000", "desc": "GenAI Reporting"},
    {"id": "std205", "name": "STD 205", "desc": "Payee Supplemental"},
    {"id": "drug_free", "name": "Drug-Free STD 21", "desc": "Drug-Free Workplace"},
    {"id": "barstow_cuf", "name": "Barstow CUF", "desc": "Barstow facility CUF"},
]

DEFAULT_AGENCY_CONFIGS = {
    "calvet": {
        "name": "Cal Vet / DVA",
        "match_patterns": ["CALVET", "CAL VET", "CVA", "VHC", "VETERANS HOME",
                          "VETERANS AFFAIRS", "CALVET.CA.GOV"],
        "required_forms": ["quote", "calrecycle74", "bidder_decl", "dvbe843", "darfur_act",
                          "cv012_cuf", "std204", "std1000", "sellers_permit"],
        "optional_forms": ["barstow_cuf", "obs_1600", "drug_free"],
        "notes": "California Department of Veterans Affairs. No AMS 703B/704B — uses Reytech quote + compliance forms.",
    },
    "calvet_barstow": {
        "name": "Cal Vet — Barstow",
        "match_patterns": ["BARSTOW", "BARSTOW VETERANS"],
        "required_forms": ["quote", "calrecycle74", "bidder_decl", "dvbe843", "darfur_act",
                          "cv012_cuf", "barstow_cuf", "std204", "std1000", "sellers_permit"],
        "optional_forms": ["obs_1600", "drug_free"],
        "notes": "Cal Vet Barstow facility — requires BOTH CV 012 CUF AND Barstow-specific CUF.",
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
        "required_forms": ["703b", "704b", "bidpkg", "quote", "sellers_permit", "dvbe843"],
        "optional_forms": ["std204", "calrecycle74", "bidder_decl", "darfur_act",
                          "obs_1600", "drug_free", "std1000"],
        "notes": "California Correctional Health Care Services / CDCR. Requires AMS 703B + 704B + Bid Package.",
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
    },
    "dgs": {
        "name": "DGS",
        "match_patterns": ["DGS", "GENERAL SERVICES"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843", "bidder_decl", "darfur_act"],
        "optional_forms": ["std1000", "calrecycle74"],
        "notes": "Department of General Services. No AMS forms — uses their own bid format.",
    },
    "calfire": {
        "name": "CAL FIRE",
        "match_patterns": ["CALFIRE", "CAL FIRE", "FORESTRY", "FIRE PROTECTION"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843"],
        "optional_forms": ["bidder_decl", "darfur_act"],
        "notes": "California Department of Forestry and Fire Protection.",
    },
    "other": {
        "name": "Other / Unknown",
        "match_patterns": [],
        "required_forms": ["quote", "std204", "sellers_permit"],
        "optional_forms": ["dvbe843", "bidder_decl"],
        "notes": "Default config for unrecognized agencies. Minimal forms.",
    },
}


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
