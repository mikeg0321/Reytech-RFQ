"""
harvest_keywords.py — Keyword lists for SCPRS procurement harvest.

These keywords drive the search_by_keyword() calls that return
PO detail pages with line items (the gold mine for pricing oracle).
"""

# High priority — items most likely in Reytech RFQs. Run first.
HIGH_PRIORITY_KEYWORDS = [
    "restraint", "medical supply", "examination glove",
    "catheter", "wheelchair", "exam table",
    "nitrile glove", "gown", "mask", "ppe",
    "paper towel", "cleaning supply", "copy paper",
    "chair", "mattress", "uniform",
    "wound care", "bandage", "gauze",
    "sharps", "syringe", "sanitizer",
]

# Full keyword list — covers broad government procurement categories
SCPRS_HARVEST_KEYWORDS = HIGH_PRIORITY_KEYWORDS + [
    # Medical / Clinical
    "surgical", "latex glove", "blood pressure",
    "stethoscope", "thermometer", "oxygen", "nebulizer",
    "suction", "iv supply", "needle", "specimen",
    "drape", "face shield", "disinfectant", "antiseptic",
    "incontinence", "brief", "underpad",
    # Facility / Janitorial
    "toilet paper", "tissue", "trash bag", "liner",
    "janitorial", "mop", "broom", "soap", "detergent",
    "bleach", "floor wax",
    # Office
    "toner", "ink cartridge", "binder", "folder",
    "envelope", "label",
    # Furniture / Equipment
    "desk", "table", "cabinet", "shelf", "locker",
    "filing cabinet", "pillow", "blanket", "sheet",
    "clothing", "boot", "shoe",
    # Food service
    "food service", "tray", "utensil", "cafeteria",
    # Technology
    "computer", "laptop", "monitor", "printer",
    "battery", "charger",
    # Safety
    "handcuff", "security", "lock", "badge",
    "hard hat", "eyewear", "safety glasses",
    "flashlight", "first aid", "tourniquet",
    # Maintenance
    "tool", "hardware", "paint", "filter",
    "light bulb", "extension cord",
]
