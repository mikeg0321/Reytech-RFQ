"""
Empirical test: run lookup_from_url() against a curated set of URLs for
the top 10 suppliers. Reports per-supplier outcome, latency, and what
fields came back populated. Output is meant to give Mike concrete evidence
of where the URL lookup pipeline is failing.

Usage:  python scripts/test_top10_url_lookup.py
"""
import os, sys, time, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.agents.item_link_lookup import lookup_from_url

# Curated probes — public product pages chosen to exercise each branch.
# When a supplier is login-walled the lookup should report login_required
# (not a silent failure). When public, we should get title + price.
PROBES = [
    ("Amazon",            "https://www.amazon.com/dp/B07ZPKBL9V"),   # nitrile gloves
    ("Amazon (ASIN bare)", "B09V3KXJPB"),                            # bare ASIN path
    ("Grainger",          "https://www.grainger.com/product/3KE39"),  # public SKU page
    ("McMaster-Carr",     "https://www.mcmaster.com/91290A115/"),     # public part page
    ("Fisher Scientific", "https://www.fishersci.com/shop/products/p-4529495"),
    ("Medline",           "https://www.medline.com/product/Nitrile-Exam-Gloves/Z05-PF06700"),
    ("Bound Tree Medical","https://www.boundtree.com/product-detail/curaplex-nitrile-exam-gloves/_/A-CUR-7-1100"),
    ("Henry Schein",      "https://www.henryschein.com/us-en/dental/p/preventive/gloves/exam-gloves-nitrile/9006935"),
    ("Concordance",       "https://www.concordancehealthcare.com/product/12345"),
    ("Waxie",             "https://www.waxie.com/product/12345"),
    ("Staples",           "https://www.staples.com/Staples-Premium-Multipurpose-Paper-8-1-2-x-11-20-lb-White-500-Sheets-Ream-10-Reams-Carton/product_135855"),
    ("Uline",             "https://www.uline.com/Product/Detail/S-13371/Nitrile-Gloves"),
]


def _summarize(out: dict) -> dict:
    """Pull just the fields that matter for the empirical report."""
    return {
        "supplier": out.get("supplier"),
        "title": (out.get("title") or "")[:60],
        "list_price": out.get("list_price"),
        "sale_price": out.get("sale_price"),
        "price": out.get("price"),
        "mfg_number": out.get("mfg_number"),
        "manufacturer": out.get("manufacturer"),
        "error": out.get("error"),
        "login_required": out.get("login_required"),
    }


def main():
    rows = []
    for label, url in PROBES:
        t0 = time.monotonic()
        try:
            out = lookup_from_url(url)
        except Exception as e:
            out = {"error": f"EXCEPTION: {type(e).__name__}: {e}"}
        elapsed = time.monotonic() - t0
        s = _summarize(out)
        s["label"] = label
        s["url"] = url
        s["elapsed_s"] = round(elapsed, 1)
        # Rough verdict
        if s.get("login_required"):
            s["verdict"] = "LOGIN"
        elif s.get("error"):
            s["verdict"] = "ERROR"
        elif s.get("title") and (s.get("list_price") or s.get("price")):
            s["verdict"] = "OK"
        elif s.get("title"):
            s["verdict"] = "TITLE_ONLY"
        elif s.get("list_price") or s.get("price"):
            s["verdict"] = "PRICE_ONLY"
        else:
            s["verdict"] = "EMPTY"
        rows.append(s)
        print(f"[{s['verdict']:10s}] {label:22s} {elapsed:5.1f}s  "
              f"title={s['title']!r:30s}  list={s['list_price']}  sale={s['sale_price']}  "
              f"mfg={s['mfg_number']!r}  err={s['error']!r}")

    # Summary
    print()
    print("=== SUMMARY ===")
    by_verdict = {}
    for r in rows:
        by_verdict.setdefault(r["verdict"], []).append(r["label"])
    for v in ("OK", "TITLE_ONLY", "PRICE_ONLY", "LOGIN", "ERROR", "EMPTY"):
        labels = by_verdict.get(v, [])
        print(f"{v:12s} ({len(labels)}): {', '.join(labels)}")

    # Persist for later inspection
    out_path = os.path.join(os.path.dirname(__file__), "top10_url_lookup_report.json")
    with open(out_path, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"\nWrote: {out_path}")


if __name__ == "__main__":
    main()
