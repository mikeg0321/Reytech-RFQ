"""
ca_agencies.py — Complete California state agency registry for SCPRS procurement.

Used by harvest scripts and intelligence engine to pull all CA agencies.
"""

# Comprehensive list of CA state agencies that participate in SCPRS procurement
# Organized by category for prioritization
CA_STATE_AGENCIES = {
    # ── Healthcare / Corrections (high volume, Reytech's core market) ──
    "CCHCS": {"name": "CA Correctional Health Care Services", "dept_code": "5225", "category": "healthcare", "priority": "P0"},
    "CDCR": {"name": "CA Dept of Corrections and Rehabilitation", "dept_code": "4700", "category": "corrections", "priority": "P0"},
    "DSH": {"name": "Dept of State Hospitals", "dept_code": "4440", "category": "healthcare", "priority": "P0"},
    "CDPH": {"name": "CA Dept of Public Health", "dept_code": "4260", "category": "healthcare", "priority": "P0"},
    "DHCS": {"name": "Dept of Health Care Services", "dept_code": "4260", "category": "healthcare", "priority": "P1"},
    "EMSA": {"name": "Emergency Medical Services Authority", "dept_code": "4120", "category": "healthcare", "priority": "P1"},

    # ── Veterans (established customer) ──
    "CalVet": {"name": "CA Dept of Veterans Affairs", "dept_code": "7700", "category": "veterans", "priority": "P0"},

    # ── Emergency / Safety / Law Enforcement ──
    "CalFire": {"name": "CA Dept of Forestry and Fire Protection", "dept_code": "3540", "category": "safety", "priority": "P1"},
    "CHP": {"name": "CA Highway Patrol", "dept_code": "2720", "category": "safety", "priority": "P1"},
    "OES": {"name": "CA Office of Emergency Services", "dept_code": "0690", "category": "safety", "priority": "P1"},
    "DOJ": {"name": "CA Dept of Justice", "dept_code": "0820", "category": "justice", "priority": "P1"},
    "BSCC": {"name": "Board of State and Community Corrections", "dept_code": "5227", "category": "corrections", "priority": "P1"},

    # ── Transportation / Infrastructure ──
    "CalTrans": {"name": "CA Dept of Transportation", "dept_code": "2660", "category": "infrastructure", "priority": "P1"},
    "DMV": {"name": "Dept of Motor Vehicles", "dept_code": "2740", "category": "infrastructure", "priority": "P2"},

    # ── General / Admin ──
    "DGS": {"name": "Dept of General Services", "dept_code": "1760", "category": "admin", "priority": "P2"},
    "DOF": {"name": "Dept of Finance", "dept_code": "1700", "category": "admin", "priority": "P2"},
    "SCO": {"name": "State Controllers Office", "dept_code": "0840", "category": "admin", "priority": "P2"},
    "FTB": {"name": "Franchise Tax Board", "dept_code": "1730", "category": "admin", "priority": "P2"},
    "BOE": {"name": "Board of Equalization", "dept_code": "0860", "category": "admin", "priority": "P2"},

    # ── Education ──
    "CDE": {"name": "CA Dept of Education", "dept_code": "6110", "category": "education", "priority": "P2"},
    "CSU": {"name": "CA State University", "dept_code": "6610", "category": "education", "priority": "P2"},
    "CCC": {"name": "CA Community Colleges", "dept_code": "6870", "category": "education", "priority": "P2"},

    # ── Environment / Resources ──
    "CalRecycle": {"name": "CA Dept of Resources Recycling and Recovery", "dept_code": "3970", "category": "environment", "priority": "P2"},
    "CDFW": {"name": "CA Dept of Fish and Wildlife", "dept_code": "3600", "category": "environment", "priority": "P2"},
    "DWR": {"name": "Dept of Water Resources", "dept_code": "3860", "category": "environment", "priority": "P2"},
    "DPR": {"name": "Dept of Parks and Recreation", "dept_code": "3790", "category": "environment", "priority": "P2"},

    # ── Social Services ──
    "CDSS": {"name": "CA Dept of Social Services", "dept_code": "5180", "category": "social", "priority": "P2"},
    "DDS": {"name": "Dept of Developmental Services", "dept_code": "4300", "category": "social", "priority": "P1"},
    "DOR": {"name": "Dept of Rehabilitation", "dept_code": "5160", "category": "social", "priority": "P2"},

    # ── Labor / Business ──
    "DIR": {"name": "Dept of Industrial Relations", "dept_code": "7350", "category": "labor", "priority": "P2"},
    "EDD": {"name": "Employment Development Department", "dept_code": "7100", "category": "labor", "priority": "P2"},

    # ── Technology ──
    "CDT": {"name": "CA Dept of Technology", "dept_code": "0509", "category": "technology", "priority": "P2"},

    # ── Housing ──
    "HCD": {"name": "Dept of Housing and Community Development", "dept_code": "2240", "category": "housing", "priority": "P2"},
}

# High-value agencies get 3-year lookback; others get 2 years
HIGH_VALUE_AGENCIES = {"CCHCS", "CDCR", "CalVet", "DSH", "CDPH", "DDS"}


def get_agencies_by_priority(max_priority: str = "P2") -> dict:
    """Get agencies up to a given priority level."""
    priority_order = {"P0": 0, "P1": 1, "P2": 2}
    max_val = priority_order.get(max_priority, 2)
    return {k: v for k, v in CA_STATE_AGENCIES.items()
            if priority_order.get(v["priority"], 2) <= max_val}


def seed_agency_registry(conn):
    """Insert all CA agencies into the agency_registry table."""
    for code, info in CA_STATE_AGENCIES.items():
        conn.execute("""
            INSERT OR IGNORE INTO agency_registry
            (agency_name, agency_code, state, jurisdiction, category,
             reytech_customer, active, tenant_id)
            VALUES (?, ?, 'CA', 'state', ?, ?, 1, 'reytech')
        """, (info["name"], code, info["category"],
              1 if code in HIGH_VALUE_AGENCIES else 0))
    conn.commit()
