"""
sku_url_resolver.py — Map MFG#/SKU patterns to supplier product URLs

Known supplier SKU formats:
  S-XXXXX     → Uline (uline.com/Product/Detail/S-XXXXX)
  B0XXXXXXXXX → Amazon (amazon.com/dp/B0XXXXXXXXX)
  W#####      → S&S Worldwide (ssww.com search)
  GRA-XXXXX   → Grainger
  FIS-XXXXX   → Fisher Scientific
  MCM-XXXXX   → McMaster-Carr
"""

import re
import logging

log = logging.getLogger("sku_resolver")

# Pattern → (supplier_name, url_template)
# {sku} is replaced with the matched SKU
SKU_PATTERNS = [
    # Uline: S-XXXXX or S-XXXX (always starts with S- followed by digits)
    (r'^(S-\d{4,6})$', "Uline", "https://www.uline.com/Product/Detail/{sku}"),
    # Amazon ASIN: B0 + 8 alphanumeric
    (r'^(B0[A-Z0-9]{8,10})$', "Amazon", "https://www.amazon.com/dp/{sku}"),
    # S&S Worldwide: W + 4-6 digits
    (r'^(W\d{4,6})$', "S&S Worldwide", "https://www.ssww.com/item/{sku}/"),
    # S&S Worldwide: pure 5-digit numeric (some S&S items)
    (r'^(\d{5})$', "S&S Worldwide", "https://www.ssww.com/item/{sku}/"),
    # S&S Worldwide: 2-letter + 3-4 digits (NL304, AP293, PS1465, FN4368)
    (r'^([A-Z]{2}\d{3,5})$', "S&S Worldwide", "https://www.ssww.com/item/{sku}/"),
    # Grainger: 3-letter + digits or pure digits with specific length
    (r'^(\d{1,3}[A-Z]\d{2,4})$', "Grainger", "https://www.grainger.com/product/{sku}"),
    # Medline: 3-letter + digits (DYA3664, BMG354204, RTI10211)
    (r'^([A-Z]{2,4}\d{4,8})$', "Medline", "https://www.medline.com/product/{sku}"),
    # Medline: letter-dash pattern (B-D382534)
    (r'^([A-Z]-[A-Z]?\d{5,8})$', "Medline", "https://www.medline.com/product/{sku}"),
    # Medline: DAF pattern (DAF100585)
    (r'^(DAF\d{5,7})$', "Medline", "https://www.medline.com/product/{sku}"),
    # Trodat/Xstamper stamps: digits-letter pattern (4926-TR0I) or XST prefix
    (r'^(XST[A-Z0-9]{3,8})$', "Xstamper", "https://www.amazon.com/s?k={sku}"),
    (r'^(\d{4}-[A-Z]{2}\d[A-Z])$', "Trodat", "https://www.amazon.com/s?k={sku}"),
    # Generic alphanumeric with dash: search Amazon
    (r'^([A-Z0-9]{2,6}-[A-Z0-9]{2,8})$', "Amazon Search", "https://www.amazon.com/s?k={sku}"),
]


def resolve_sku_url(sku: str) -> dict:
    """Resolve a MFG#/SKU to a supplier URL.

    Args:
        sku: The part number / MFG# to resolve.

    Returns:
        {"supplier": str, "url": str} or {"supplier": "", "url": ""} if no match.
    """
    if not sku or not sku.strip():
        return {"supplier": "", "url": ""}

    clean = sku.strip().upper()

    for pattern, supplier, url_template in SKU_PATTERNS:
        m = re.match(pattern, clean, re.IGNORECASE)
        if m:
            matched_sku = m.group(1)
            url = url_template.replace("{sku}", matched_sku)
            return {"supplier": supplier, "url": url}

    return {"supplier": "", "url": ""}


def resolve_batch(skus: list) -> list:
    """Resolve multiple SKUs at once.

    Args:
        skus: List of {"idx": int, "sku": str} dicts.

    Returns:
        List of {"idx": int, "sku": str, "supplier": str, "url": str} dicts.
    """
    results = []
    for item in skus:
        sku = item.get("sku", "")
        resolved = resolve_sku_url(sku)
        results.append({
            "idx": item.get("idx", 0),
            "sku": sku,
            **resolved,
        })
    return results
