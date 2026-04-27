"""Fine-grained intel categorizer.

Distinct from src/core/pricing_oracle_v2.py:_classify_item_category() —
that one buckets into broad markup tiers (medical/office/janitorial/...).
This module buckets by **historical win-pattern** so the oracle can warn
Mike when he's about to bid on a known loss class.

Loss-bucket discoveries from 2026-04-26 items-yearly run:
  - "Propet M3705 Life Walker Strap White"  : 0/37 (2024)
  - "Men's Sport Walker SZ 7.5 M White"     : 0/18 (2024)
  - "Diabetic White Velcro Shoe Men"        : 0/9  (2024)
  - "RX Comfort Insoles"                    : 0/9  (2024+2025)
  - 111 footwear losses, 0 wins → "footwear-orthopedic" warrants its
    own bucket.

Win-bucket discoveries from same run:
  - TENA ProSkin Stretch Adult Brief XL     : 11/11 (2023+2024)
  - SoftPro Resting Hand Splint (L+R)       : 18/18 (2023)
  - Bardex IC Foley Catheter 22 Fr          : 8/8   (2023)
  - Tranquility Premium OverNight Pull On   : 8/8   (2024)

The categorizer is keyword-based with explicit precedence so a "Propet
walking shoe" classifies as footwear-orthopedic, not generic medical.
"""

# Categories ordered by precedence — first match wins.
# Each entry is (category_id, display_label, [keyword_list]).
# Keywords are lower-cased substrings tested against the description.
_CATEGORIES = [
    # --- Active loss buckets (warn Mike) ---
    ("footwear-orthopedic", "Orthopedic Footwear / Walkers / Insoles", [
        "propet", "walker", "walking shoe", "insole", "orthotic",
        "diabetic", "velcro shoe", "heat mold", "moldable",
        "men's sport walker", "life walker",
    ]),

    # --- Active win buckets (high confidence) ---
    ("incontinence", "Adult Incontinence (Briefs / Pull-Ons)", [
        "incontinence", "adult brief", "pull on", "pull-on",
        "tena", "tranquility", "depend", "absorbent underwear",
        "overnight pull",
    ]),
    ("splint-brace", "Splints / Braces / Slings", [
        "splint", "softpro", "brace", "sling", "immobilizer",
        "wrist support", "ankle support",
    ]),
    ("catheter-foley", "Catheters (Foley / Urinary)", [
        "catheter", "foley", "urinary drainage", "bardex",
    ]),

    # --- Active middle ground ---
    ("exam-glove", "Exam Gloves (Nitrile / Latex / Vinyl)", [
        "exam glove", "examination glove", "nitrile glove",
        "latex glove", "vinyl glove", "powder free glove",
        "powder-free glove", "medical glove",
    ]),
    ("gauze-wound", "Gauze / Wound Dressing", [
        "gauze", "wound dress", "abdominal pad", "abd pad",
        "non-adherent", "non adherent", "abd-pad",
    ]),
    ("sharps-container", "Sharps Containers", [
        "sharps container", "sharpstar", "biohazard container",
        "needle disposal",
    ]),
    ("snack-food", "Snack Food / Beverages", [
        "oreo", "ruffles", "doritos", "milky way", "milkyway",
        "hostess", "snack pack", "candy bar", "chocolate bar",
        "potato chip", "pretzel", "cookie", "cupcake",
    ]),

    # --- Lower-priority generic categories ---
    ("medical-other", "Medical (other)", [
        "syringe", "needle", "thermometer", "stethoscope",
        "tongue depressor", "alcohol prep", "antiseptic",
    ]),
    ("janitorial", "Janitorial / Cleaning", [
        "trash bag", "garbage bag", "mop", "broom",
        "disinfectant", "sanitizer", "bleach", "deodorizer",
        "paper towel", "toilet paper",
    ]),
    ("office", "Office Supplies", [
        "pen", "pencil", "stapler", "binder clip", "folder",
        "envelope", "post-it", "post it", "highlighter",
        "ink cartridge", "toner cartridge",
    ]),
]


def intel_category(description: str) -> tuple[str, str]:
    """Classify a description into (category_id, category_label).

    Returns ("uncategorized", "Uncategorized") if no rule matches —
    that's the signal that the categorizer needs another keyword,
    NOT a confident "general" verdict.

    First-match-wins: the precedence order in _CATEGORIES is what
    keeps "Propet walking shoe" out of generic categories.
    """
    desc = (description or "").lower().strip()
    if not desc:
        return ("uncategorized", "Uncategorized")
    for cat_id, label, keywords in _CATEGORIES:
        for kw in keywords:
            if kw in desc:
                return (cat_id, label)
    return ("uncategorized", "Uncategorized")


def all_categories():
    """For diagnostic / admin endpoints — returns the full
    {id: label} map of categories the intel layer knows about."""
    return {cat_id: label for (cat_id, label, _) in _CATEGORIES}
