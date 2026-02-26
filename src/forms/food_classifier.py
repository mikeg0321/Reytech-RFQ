#!/usr/bin/env python3
"""
CDCR Agricultural Food Product Category Classifier
OBS 1600 (Rev. 1/26) — Maps food item descriptions to category codes 1-14.

Category Codes:
  1  Coffee, Tea
  2  Dairy
  3  Eggs
  4  Extended Shelf Life Products (flour, sugar)
  5  Fruit - Extended Shelf Life (canned, dried)
  6  Fruit - Fresh, Frozen (processed, whole)
  7  Fungi, Ginger, Herbs (canned, dried, fresh)
  8  Grains, Nuts, Rice, Seeds
  9  Honey, Pollen
  10 Legumes
  11 Meat, Poultry & Seafood
  12 Puree
  13 Vegetables - Extended Shelf Life (canned, dried)
  14 Vegetables - Fresh, Frozen (processed, whole)
"""

import re

# ── Category definitions with keyword patterns ─────────────────────────
# Order matters: more specific categories checked first
FOOD_CATEGORIES = [
    # Code 1: Coffee, Tea
    (1, "Coffee, Tea", [
        r'\bcoffee\b', r'\btea\b', r'\bdecaf', r'\bespresso\b',
        r'\bcappuccino\b', r'\blatte\b', r'\bmatcha\b',
        r'\binstant\s+coffee\b', r'\bground\s+coffee\b',
        r'\bcoffee\s+bean', r'\bherbal\s+tea\b', r'\bgreen\s+tea\b',
        r'\bblack\s+tea\b', r'\biced\s+tea\b',
    ]),
    # Code 9: Honey, Pollen (before dairy/grains since "honey" is specific)
    (9, "Honey, Pollen", [
        r'\bhoney\b', r'\bpollen\b', r'\bhoneycomb\b',
    ]),
    # Code 3: Eggs
    (3, "Eggs", [
        r'\beggs?\b', r'\begg\s+patty', r'\bliquid\s+egg',
        r'\bfrozen\s+egg', r'\bwhole\s+egg', r'\begg\s+white',
        r'\begg\s+yolk', r'\bshell\s+egg',
    ]),
    # Code 2: Dairy
    (2, "Dairy", [
        r'\bmilk\b', r'\bbutter\b', r'\bcheese\b', r'\byogurt\b',
        r'\bcream\b', r'\bbuttermilk\b', r'\bsour\s+cream\b',
        r'\bcottage\s+cheese\b', r'\bice\s+cream\b', r'\bwhipped',
        r'\bcheddar\b', r'\bmozzarella\b', r'\bparmesan\b',
        r'\bprovolone\b', r'\bswiss\s+cheese\b', r'\bjack\s+cheese\b',
        r'\bcream\s+cheese\b', r'\bdairy\b', r'\bhalf\s+and\s+half\b',
        r'\bevaporated\s+milk\b', r'\bcondensed\s+milk\b',
        r'\bpowdered\s+milk\b', r'\bdry\s+milk\b',
    ]),
    # Code 12: Puree (before fruits/vegs since "puree" is specific)
    (12, "Puree", [
        r'\bpuree\b', r'\bpureed\b',
    ]),
    # Code 7: Fungi, Ginger, Herbs
    (7, "Fungi, Ginger, Herbs", [
        r'\bmushroom', r'\bfungi\b', r'\bginger\b', r'\bcilantro\b',
        r'\bparsley\b', r'\bbasil\b', r'\bthyme\b', r'\borgano\b',
        r'\brosemary\b', r'\bsage\b', r'\bdill\b', r'\bmint\b',
        r'\bbay\s+leaf', r'\bbay\s+leaves\b', r'\bcumin\b',
        r'\bturmeric\b', r'\bpaprika\b', r'\bcinnamon\b',
        r'\bnutmeg\b', r'\bcloves?\b', r'\bherb', r'\bspice',
        r'\bseasonin', r'\bpepper\s*corn', r'\bchili\s+powder\b',
        r'\bgarlic\s+powder\b', r'\bonion\s+powder\b',
        r'\bportobello\b', r'\bshiitake\b',
    ]),
    # Code 10: Legumes (before vegetables)
    (10, "Legumes", [
        r'\bbeans?\b', r'\blentil', r'\bpeas?\b(?!\s*nut)',
        r'\bpinto\b', r'\bkidney\s+bean', r'\blima\s+bean',
        r'\bblack\s+bean', r'\bblack[-\s]eyed\s+pea',
        r'\bnavy\s+bean', r'\bgreen\s+bean', r'\brefried\b',
        r'\bchick\s*pea', r'\bgarbanzo\b', r'\bedamame\b',
        r'\bsplit\s+pea',
    ]),
    # Code 11: Meat, Poultry & Seafood
    (11, "Meat, Poultry & Seafood", [
        r'\bbeef\b', r'\bchicken\b', r'\bpork\b', r'\bturkey\b',
        r'\bfish\b', r'\bshrimp\b', r'\btuna\b', r'\bsalmon\b',
        r'\btilapia\b', r'\bcod\b', r'\bpollock\b', r'\bcatfish\b',
        r'\bcrab\b', r'\blobster\b', r'\bshellfish\b', r'\bclam',
        r'\bmussel\b', r'\boycster\b', r'\bsquid\b',
        r'\brabbit\b', r'\blamb\b', r'\bveal\b', r'\bbacon\b',
        r'\bsausage\b', r'\bham\b(?!\s*burger)', r'\bhotdog',
        r'\bhot\s+dog', r'\bfrank\b', r'\bwiener\b',
        r'\bpatty\b', r'\bground\s+beef\b', r'\bground\s+turkey\b',
        r'\bground\s+pork\b', r'\bground\s+chicken\b',
        r'\bsteak\b', r'\broast\b', r'\brib\b', r'\bwing',
        r'\bbreast\b', r'\bthigh\b', r'\bdrum\s*stick',
        r'\bmeat\b', r'\bpoultry\b', r'\bseafood\b',
        r'\bjerky\b', r'\bdeli\s+meat\b', r'\bbolognab',
        r'\bpepperoni\b', r'\bsalami\b', r'\bcanadian\s+bacon\b',
        r'\bhalal\b', r'\bkosher\b',
    ]),
    # Code 8: Grains, Nuts, Rice, Seeds
    (8, "Grains, Nuts, Rice, Seeds", [
        r'\brice\b', r'\boats?\b', r'\boatmeal\b', r'\bcornmeal\b',
        r'\bfarina\b', r'\bgrits\b', r'\bwheat\b', r'\bbarley\b',
        r'\bquinoa\b', r'\bgranola\b', r'\bcereal\b',
        r'\balmond', r'\bpeanut', r'\bwalnut', r'\bpecan',
        r'\bcashew', r'\bpistachio\b', r'\bsunflower\s+seed',
        r'\bsesame\b', r'\bflax\b', r'\bchia\b', r'\bgrain',
        r'\bnuts?\b', r'\bseed\b', r'\bcrackers?\b',
        r'\bbread\b', r'\btortilla', r'\bbun\b', r'\broll\b',
        r'\bnoodle', r'\bpasta\b', r'\bspaghetti\b', r'\bmacaroni\b',
        r'\bpancake\b', r'\bwaffle\b', r'\bbiscuit\b', r'\bmuffin\b',
        r'\bcorn\s+chip', r'\bchip(?!s?\s+(?:beef|pork))\b',
        r'\bpopcorn\b', r'\bpretzel\b', r'\bcookie\b',
        r'\bcandy\b', r'\bchocolate\b', r'\bsnack\b',
    ]),
    # Code 4: Extended Shelf Life Products (flour, sugar)
    (4, "Extended Shelf Life Products", [
        r'\bflour\b', r'\bsugar\b', r'\bbrown\s+sugar\b',
        r'\bpowdered\s+sugar\b', r'\bwhite\s+sugar\b',
        r'\bcorn\s*starch\b', r'\bbaking\s+soda\b', r'\bbaking\s+powder\b',
        r'\byeast\b', r'\bgelatin\b', r'\bpectin\b',
        r'\bvanilla\b', r'\bextract\b', r'\bvinegar\b',
        r'\bsalt\b', r'\bpepper\b(?!\s*corn)', r'\boil\b',
        r'\bshortening\b', r'\bmargarine\b', r'\blard\b',
        r'\bsyrup\b', r'\bmolasses\b', r'\bjelly\b', r'\bjam\b',
        r'\bpreserve', r'\bketchup\b', r'\bmustard\b',
        r'\bmayonnaise\b', r'\bsalad\s+dressing\b', r'\bsoy\s+sauce\b',
        r'\bhot\s+sauce\b', r'\bbarbecue\s+sauce\b', r'\bbbq\s+sauce\b',
        r'\bcondiment', r'\bsauce\b', r'\bdressing\b',
    ]),
    # Code 5: Fruit - Extended Shelf Life (canned, dried)
    (5, "Fruit - Extended Shelf Life", [
        r'\bcanned\s+(?:fruit|peach|pear|pineapple|mandarin|apricot|cherry)',
        r'\bfruit\s+cocktail\b', r'\bdried\s+fruit\b',
        r'\braisin', r'\bprune', r'\bcranberr(?:y|ies)\b.*(?:dried|canned)',
        r'\bapple\s+sauce\b', r'\bapplesauce\b',
        r'\bbanana\s+chip', r'\bfruit\s+cup\b',
        r'\btomato\s+(?:sauce|paste|puree|crushed|diced|stewed)',
        r'\bmarinara\b', r'\btomato\b.*canned', r'\bcanned.*tomato',
        r'\bmandarin\s+orange', r'\bolive', r'\bpickle',
        r'\bsalsa\b', r'\brelish\b',
    ]),
    # Code 6: Fruit - Fresh, Frozen
    (6, "Fruit - Fresh, Frozen", [
        r'\bfresh\s+(?:fruit|apple|banana|orange|grape|melon|berr)',
        r'\bfrozen\s+(?:fruit|berr|strawberr|blueberr|peach|mango)',
        r'\bapple(?![\s-]+sauce)\b', r'\bbanana\b', r'\borange\b',
        r'\bgrape\b', r'\bmelon\b', r'\bwatermelon\b',
        r'\bstrawberr', r'\bblueberr', r'\braspberr', r'\bblackberr',
        r'\bpeach\b', r'\bpear\b', r'\bplum\b', r'\bnectarine\b',
        r'\bkiwi\b', r'\bpineapple\b', r'\bmango\b', r'\bavocado\b',
        r'\blemon\b', r'\blime\b', r'\bgrapefruit\b',
        r'\bfruit\b',
    ]),
    # Code 13: Vegetables - Extended Shelf Life (canned, dried)
    (13, "Vegetables - Extended Shelf Life", [
        r'\bcanned\s+(?:corn|vegeta|carrot|pea|green|tomato|bean)',
        r'\bdried\s+(?:vegeta|onion|garlic|tomato)',
        r'\bpotato\s+flake', r'\binstant\s+potato',
        r'\bdehydrated\b.*(?:vegeta|potato|onion)',
        r'\bcanned.*(?:corn|vegetable|carrot)',
    ]),
    # Code 14: Vegetables - Fresh, Frozen
    (14, "Vegetables - Fresh, Frozen", [
        r'\bfresh\s+(?:vegeta|lettuce|tomato|onion|carrot|celery|broccoli)',
        r'\bfrozen\s+(?:vegeta|corn|pea|broccoli|spinach|carrot|mixed)',
        r'\blettuce\b', r'\bcabbage\b', r'\bbroccoli\b',
        r'\bcauliflower\b', r'\bspinach\b', r'\bkale\b',
        r'\bcelery\b', r'\bcarrot\b', r'\bonion\b',
        r'\bpotato\b(?!\s+flake)', r'\bsweet\s+potato\b', r'\byam\b',
        r'\bcorn\b(?!\s*(?:meal|starch|chip|bread|dog))',
        r'\bpepper\b(?!\s*corn)(?!\s*(?:powder|flake))',
        r'\bjalapeno\b', r'\btomato\b', r'\bsquash\b',
        r'\bzucchini\b', r'\bcucumber\b', r'\bgarlic\b',
        r'\bturnip\b', r'\bcole\s*slaw\b', r'\bcoleslaw\b',
        r'\bmixed\s+vegeta', r'\bgreen\s+salad\b',
        r'\bvegeta', r'\bsalad\b',
    ]),
]

# Keywords that indicate an item is food-related
FOOD_INDICATORS = [
    r'\bfood\b', r'\bedible\b', r'\bconsumab',
    r'\bcanned\b', r'\bfrozen\b', r'\bfresh\b', r'\bdried\b',
    r'\borganic\b', r'\bgraded?\b', r'\busda\b',
    r'\bproduce\b', r'\bgrocery\b', r'\bingredient',
    r'\bflavor', r'\bseason', r'\bspice',
    # Direct food terms
    r'\bcoffee\b', r'\btea\b', r'\bmilk\b', r'\bbutter\b',
    r'\bcheese\b', r'\begg', r'\bflour\b', r'\bsugar\b',
    r'\brice\b', r'\bbeans?\b', r'\bmeat\b', r'\bchicken\b',
    r'\bbeef\b', r'\bpork\b', r'\bfish\b', r'\btuna\b',
    r'\bfruit\b', r'\bveget', r'\bbread\b', r'\bpasta\b',
    r'\bchocolate\b', r'\bcandy\b', r'\bpopcorn\b',
    r'\bhoney\b', r'\bsyrup\b', r'\bsauce\b',
    r'\bcereal\b', r'\boatmeal\b', r'\bnoodle',
    r'\btortilla\b', r'\bcrackers?\b', r'\bcookie',
    r'\bjuice\b', r'\bwater\b.*(?:spring|bottled)',
]


def is_food_item(description: str) -> bool:
    """Check if an item description is a food product."""
    if not description:
        return False
    desc = description.lower().strip()
    for pattern in FOOD_INDICATORS:
        if re.search(pattern, desc, re.IGNORECASE):
            return True
    return False


def classify_food_item(description: str) -> tuple:
    """
    Classify a food item description into a CDCR category code.
    
    Returns:
        (code, category_name) or (None, None) if not classifiable
    """
    if not description:
        return None, None
    desc = description.lower().strip()
    
    # ── Specific product overrides (resolve ambiguity) ──
    OVERRIDES = [
        (r'\bpopcorn\b', 8, "Grains, Nuts, Rice, Seeds"),
        (r'\bbutter\s+flavor', 8, "Grains, Nuts, Rice, Seeds"),
        (r'\bpeanut\s+butter\b', 8, "Grains, Nuts, Rice, Seeds"),
        (r'\bcocoa\s+butter\b', 4, "Extended Shelf Life Products"),
        (r'\bice\s+cream\b', 2, "Dairy"),
        (r'\begg\s+noodle', 8, "Grains, Nuts, Rice, Seeds"),
        (r'\btomato\s+(?:sauce|paste|crushed|diced|stewed)', 5, "Fruit - Extended Shelf Life"),
        (r'\btomato\s+puree\b', 12, "Puree"),
        (r'\bgreen\s+bean', 10, "Legumes"),
    ]
    for pattern, code, name in OVERRIDES:
        if re.search(pattern, desc, re.IGNORECASE):
            return code, name
    
    for code, name, patterns in FOOD_CATEGORIES:
        for pattern in patterns:
            if re.search(pattern, desc, re.IGNORECASE):
                return code, name
    
    # If it's a food item but doesn't match any specific category
    if is_food_item(description):
        return 4, "Extended Shelf Life Products"  # Default to shelf-stable
    
    return None, None


def classify_quote_items(items: list) -> list:
    """
    Classify a list of quote/RFQ items.
    
    Args:
        items: List of dicts with 'description' field
    
    Returns:
        List of dicts with added 'food_code', 'food_category', 'is_food' fields
    """
    results = []
    for i, item in enumerate(items):
        desc = item.get('description', '')
        code, category = classify_food_item(desc)
        results.append({
            'line_number': item.get('line_number', i + 1),
            'description': desc,
            'is_food': code is not None,
            'food_code': code,
            'food_category': category,
            'qty': item.get('qty', 1),
            'unit_price': item.get('unit_price', item.get('price_per_unit', 0)),
        })
    return results


def get_food_items_for_obs1600(items: list) -> list:
    """
    Filter and classify only food items for OBS 1600 form filling.
    
    Returns list of dicts ready for form fields:
        [{'line_number': 1, 'description': 'Chocolate', 'code': 8, 'ca_grown': 'No', 'pct': 'N/A'}, ...]
    """
    classified = classify_quote_items(items)
    food_items = [r for r in classified if r['is_food']]
    
    result = []
    for item in food_items:
        result.append({
            'line_number': item['line_number'],
            'description': item['description'],
            'code': item['food_code'],
            'category_name': item['food_category'],
            'ca_grown': 'No',
            'pct': 'N/A',
        })
    return result


# ── Self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_items = [
        "Chocolate bars, assorted",
        "Popcorn, microwave, butter flavor",
        "Candy, hard, assorted flavors",
        "Ground Coffee, Regular, 2lb",
        "Whole Milk, 1 Gallon",
        "Fresh Eggs, Large, 30 dozen",
        "White Sugar, 50lb bag",
        "Canned Peaches, Halves, #10 can",
        "Frozen Broccoli, IQF, 30lb",
        "Mushrooms, Canned, Sliced, #10",
        "Pinto Beans, Dry, 25lb bag",
        "Ground Beef, 80/20, Halal, 10lb",
        "Brown Rice, Long Grain, 25lb",
        "Honey, Clover, 5lb jug",
        "Vegetable Puree, Mixed",
        "Canned Corn, Whole Kernel, #10",
        "Stryker X-Restraint Package",  # NOT food
        "Nitrile Exam Gloves, Medium",  # NOT food
    ]
    
    print(f"{'Description':45s} | {'Code':>4} | {'Category'}")
    print("-" * 80)
    for desc in test_items:
        code, cat = classify_food_item(desc)
        if code:
            print(f"{desc[:45]:45s} | {code:4d} | {cat}")
        else:
            print(f"{desc[:45]:45s} |  N/A | Not a food item")
