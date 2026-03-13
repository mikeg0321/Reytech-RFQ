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
        "match_patterns": ["CALVET", "CAL VET", "CVA", "VHC", "VETERANS"],
        "required_forms": ["quote", "calrecycle74", "bidder_decl", "dvbe843", "darfur_act", "cv012_cuf", "std204", "std1000", "sellers_permit"],
        "optional_forms": ["barstow_cuf"],
        "notes": "California Department of Veterans Affairs. No AMS 703B/704B — uses Reytech quote + compliance forms.",
    },
    "cchcs": {
        "name": "CCHCS / CDCR",
        "match_patterns": ["CCHCS", "CDCR", "CORRECTIONS", "PRISON", "CIM", "CMC", "CTF",
                          "CIW", "LAC", "SAC", "SQ", "FSP", "SATF", "KVSP", "CRC",
                          "CCWF", "CHCF", "DVI", "MCSP", "NKSP", "PBSP", "RJD",
                          "SCC", "SOL", "SVSP", "VSP", "WSP", "CEN", "ISP",
                          "ASP", "HDSP"],
        "required_forms": ["703b", "704b", "bidpkg", "quote", "sellers_permit", "dvbe843"],
        "optional_forms": ["std204", "calrecycle74", "bidder_decl", "darfur_act"],
        "notes": "California Correctional Health Care Services / CDCR. Requires AMS 703B + 704B + Bid Package.",
    },
    "dgs": {
        "name": "DGS",
        "match_patterns": ["DGS", "GENERAL SERVICES"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843", "bidder_decl", "darfur_act"],
        "optional_forms": ["std1000"],
        "notes": "Department of General Services. No AMS forms — uses their own bid format.",
    },
    "calfire": {
        "name": "CAL FIRE",
        "match_patterns": ["CALFIRE", "CAL FIRE", "FORESTRY"],
        "required_forms": ["quote", "std204", "sellers_permit", "dvbe843"],
        "optional_forms": [],
        "notes": "California Department of Forestry and Fire Protection.",
    },
    "other": {
        "name": "Other / Unknown",
        "match_patterns": [],
        "required_forms": ["quote", "std204", "sellers_permit"],
        "optional_forms": ["dvbe843"],
        "notes": "Default config for unrecognized agencies. Minimal forms.",
    },
}


def load_agency_configs():
    """Load agency configs. Returns DEFAULT_AGENCY_CONFIGS (could be extended to load from DB)."""
    return DEFAULT_AGENCY_CONFIGS


def match_agency(rfq_data):
    """Match an RFQ to an agency config based on agency name, email, institution, etc."""
    configs = load_agency_configs()
    
    search_text = " ".join([
        str(rfq_data.get("agency", "")),
        str(rfq_data.get("requestor_email", "")),
        str(rfq_data.get("institution", "")),
        str(rfq_data.get("delivery_location", "")),
        str(rfq_data.get("ship_to", "")),
        str(rfq_data.get("solicitation_number", "")),
    ]).upper()
    
    for key, cfg in configs.items():
        if key == "other":
            continue
        for pattern in cfg.get("match_patterns", []):
            if pattern in search_text:
                return key, cfg
    
    return "other", configs["other"]
