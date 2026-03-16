"""
Unified Pricing Oracle v2
One function for all pricing. Merges all data sources,
applies time decay, returns the definitive answer.
"""
import logging
import re
import math
import json
from datetime import datetime, timedelta
from difflib import SequenceMatcher

log = logging.getLogger("reytech.pricing_oracle")


def get_pricing(description, quantity=1, cost=None, item_number="",
                department="", force_refresh=False):
    """THE pricing function. Call this for everything."""
    import sqlite3
    from src.core.db import DB_PATH

    result = {
        "description": description, "quantity": quantity,
        "matched_item": None, "confidence": 0,
        "cost": {}, "market": {}, "recommendation": {},
        "strategies": [], "tiers": [], "competitors": [],
        "cross_sell": [], "sources_used": [],
    }

    if not description or len(description.strip()) < 3:
        return result

    db = sqlite3.connect(DB_PATH, timeout=10)

    # Step 1: Check item memory
    memory = _check_item_memory(db, description, item_number)
    if memory:
        result["matched_item"] = memory
        result["confidence"] = memory.get("confidence", 0.95)
        result["sources_used"].append("item_memory")
        description = memory.get("canonical_description", description)

    # Step 2: Locked cost
    locked = _get_locked_cost(db, description, item_number)
    if locked:
        result["cost"] = locked
        result["sources_used"].append("supplier_costs")
        cost = locked.get("locked_cost") or cost
    if cost:
        result["cost"]["provided_cost"] = cost

    # Step 3: Gather market prices
    market_prices = []
    for fn, name in [
        (_search_scprs_catalog, "scprs_catalog"),
        (_search_po_lines, "scprs_po_lines"),
        (_search_product_catalog, "product_catalog"),
    ]:
        hits = fn(db, description, item_number)
        if hits:
            market_prices.extend(hits)
            result["sources_used"].append(name)

    # Step 4: Analyze market
    result["market"] = _analyze_market_prices(market_prices, quantity)

    # Step 5: Competitors
    result["competitors"] = _get_competitor_breakdown(market_prices)

    # Step 6: Recommendation
    rec = _calculate_recommendation(cost, result["market"], quantity)
    result["strategies"] = rec.pop("strategies", [])
    result["tiers"] = rec.pop("tiers", [])
    result["recommendation"] = rec

    # Step 7: Cross-sell
    result["cross_sell"] = _get_cross_sell(db, description)

    db.close()
    return result


def _check_item_memory(db, description, item_number=""):
    """Return ALL stored fields for a matched item."""
    _cols = """canonical_description, canonical_item_number, product_url, mfg_number,
               supplier, last_cost, confidence, times_confirmed, asin, uom,
               supplier_url, last_sell_price, mfg_name"""

    def _row_to_dict(row, match_type):
        if not row:
            return None
        return {
            "canonical_description": row[0] or "", "canonical_item_number": row[1] or "",
            "product_url": row[2] or "", "mfg_number": row[3] or "",
            "supplier": row[4] or "", "last_cost": row[5] or 0,
            "confidence": row[6] or 0, "times_confirmed": row[7] or 0,
            "asin": row[8] or "" if len(row) > 8 else "",
            "uom": row[9] or "" if len(row) > 9 else "",
            "supplier_url": row[10] or "" if len(row) > 10 else "",
            "last_sell_price": row[11] or 0 if len(row) > 11 else 0,
            "mfg_name": row[12] or "" if len(row) > 12 else "",
            "match_type": match_type,
        }

    try:
        if item_number:
            row = db.execute(f"""
                SELECT {_cols} FROM item_mappings
                WHERE LOWER(original_item_number)=LOWER(?) AND confirmed=1 LIMIT 1
            """, (item_number,)).fetchone()
            result = _row_to_dict(row, "exact_item")
            if result:
                return result

        row = db.execute(f"""
            SELECT {_cols} FROM item_mappings WHERE confirmed=1
            AND (LOWER(original_description)=LOWER(?) OR LOWER(canonical_description)=LOWER(?)) LIMIT 1
        """, (description, description)).fetchone()
        result = _row_to_dict(row, "exact_desc")
        if result:
            return result
    except Exception:
        pass
    return None


def _get_locked_cost(db, description, item_number=""):
    try:
        params = []
        where = "expires_at > datetime('now')"
        if item_number:
            where += " AND (item_number=? OR LOWER(description) LIKE ?)"
            params.extend([item_number, f"%{description.lower()[:50]}%"])
        else:
            where += " AND LOWER(description) LIKE ?"
            params.append(f"%{description.lower()[:50]}%")

        row = db.execute(f"""
            SELECT cost, supplier, source, confirmed_at, expires_at
            FROM supplier_costs WHERE {where} ORDER BY confirmed_at DESC LIMIT 1
        """, params).fetchone()
        if row:
            return {"locked_cost": row[0], "cost_source": row[2], "cost_supplier": row[1],
                    "cost_confirmed": row[3], "cost_expires": row[4], "cost_fresh": True}
    except Exception:
        pass
    return None


def _search_scprs_catalog(db, description, item_number=""):
    prices = []
    try:
        token_groups = _tokenize(description)[:4]
        if not token_groups:
            return prices
        where_parts = []
        params = []
        for group in token_groups:
            if len(group) == 1:
                where_parts.append("LOWER(description) LIKE ?")
                params.append(f"%{group[0]}%")
            else:
                or_clause = " OR ".join(["LOWER(description) LIKE ?" for _ in group])
                where_parts.append(f"({or_clause})")
                params.extend([f"%{v}%" for v in group])
        where = " AND ".join(where_parts)
        if item_number:
            where = f"({where}) OR LOWER(mfg_number) = ?"
            params.append(item_number.lower())
        rows = db.execute(f"""
            SELECT description, last_unit_price, last_quantity, last_uom,
                   last_supplier, last_department, last_date, times_seen
            FROM scprs_catalog WHERE {where} ORDER BY times_seen DESC LIMIT 20
        """, params).fetchall()
        for r in rows:
            if r[1] and r[1] > 0:
                prices.append({"price": r[1], "description": r[0], "quantity": r[2] or 1,
                               "uom": r[3] or "", "supplier": r[4] or "", "department": r[5] or "",
                               "date": r[6] or "", "source": "scprs_catalog",
                               "is_reytech": "REYTECH" in (r[4] or "").upper()})
    except Exception as e:
        log.debug("scprs_catalog search: %s", e)
    return prices


def _search_po_lines(db, description, item_number=""):
    prices = []
    try:
        token_groups = _tokenize(description)[:3]
        if not token_groups:
            return prices
        where_parts = []
        params = []
        for group in token_groups:
            if len(group) == 1:
                where_parts.append("LOWER(l.description) LIKE ?")
                params.append(f"%{group[0]}%")
            else:
                or_clause = " OR ".join(["LOWER(l.description) LIKE ?" for _ in group])
                where_parts.append(f"({or_clause})")
                params.extend([f"%{v}%" for v in group])
        where = " AND ".join(where_parts)
        rows = db.execute(f"""
            SELECT l.description, l.unit_price, l.quantity, l.uom,
                   m.supplier, m.dept_name, m.start_date, m.buyer_email
            FROM scprs_po_lines l JOIN scprs_po_master m ON l.po_number=m.po_number
            WHERE {where} ORDER BY m.start_date DESC LIMIT 25
        """, params).fetchall()
        for r in rows:
            try:
                p = float(str(r[1] or "0").replace("$", "").replace(",", ""))
            except (ValueError, TypeError):
                continue
            if p > 0:
                prices.append({"price": p, "description": r[0],
                               "quantity": float(str(r[2] or "1").replace(",", "")),
                               "supplier": r[4] or "", "department": r[5] or "",
                               "date": r[6] or "", "buyer_email": r[7] or "",
                               "source": "scprs_po_lines",
                               "is_reytech": "REYTECH" in (r[4] or "").upper()})
    except Exception as e:
        log.debug("po_lines search: %s", e)
    return prices


def _search_product_catalog(db, description, item_number=""):
    prices = []
    try:
        token_groups = _tokenize(description)[:3]
        if not token_groups:
            return prices
        # Use OR across all variant groups for product catalog (broader match)
        all_variants = []
        for group in token_groups:
            all_variants.extend(group)
        where = " OR ".join(["LOWER(name) LIKE ?" for _ in all_variants])
        params = [f"%{v}%" for v in all_variants]
        rows = db.execute(f"""
            SELECT name, sell_price, cost, best_supplier FROM product_catalog WHERE {where} LIMIT 10
        """, params).fetchall()
        for r in rows:
            p = r[1] or r[2]
            if p and p > 0:
                prices.append({"price": p, "description": r[0], "supplier": r[3] or "",
                               "source": "product_catalog", "is_reytech": True})
    except Exception:
        pass
    return prices


def _normalize_to_per_unit(price, description, quantity=1, uom=""):
    """Normalize any price to per-unit (per-each) basis."""
    desc = (description or "").upper()
    total_units = 1
    detected = ""
    # X/BX Y BXS/CS
    m = re.search(r'(\d+)\s*/\s*(?:BX|BOX|PK|PKG)\s+(\d+)\s*(?:BXS?|BOXES|PKS?|PKGS?)\s*/\s*(?:CS|CASE|CT|CTN)', desc)
    if m:
        total_units = int(m.group(1)) * int(m.group(2))
        detected = m.group(0)
    if total_units == 1:
        m = re.search(r'(\d+)\s*/\s*(?:BX|BOX|PK|PKG|BAG|RL|ROLL|BT|BTL|EA|CT|CASE)', desc)
        if m:
            total_units = int(m.group(1))
            detected = m.group(0)
    if total_units == 1:
        m = re.search(r'(?:BOX|CASE|PACK|PKG|PACKAGE)\s+(?:OF\s+)?(\d+)', desc)
        if m:
            total_units = int(m.group(1))
            detected = m.group(0)
    if total_units == 1:
        m = re.search(r'(\d+)\s+PER\s+(?:BOX|BX|CASE|CS|PACK|PK)', desc)
        if m:
            total_units = int(m.group(1))
            detected = m.group(0)
    if total_units == 1:
        m = re.search(r'(\d+)\s*(?:CT|COUNT|EA)\b', desc)
        if m and int(m.group(1)) > 1:
            total_units = int(m.group(1))
            detected = m.group(0)
    if total_units > 10000:
        total_units = 1
        detected = ""
    per_unit = price / total_units if total_units > 0 else price
    return {"per_unit": round(per_unit, 6), "total_units": total_units,
            "detected": detected, "original_price": price}


def _analyze_market_prices(market_prices, request_qty):
    """Time-weighted market analysis with proper UOM normalization."""
    if not market_prices:
        return {"data_points": 0, "freshness": "none"}
    now = datetime.now()
    weighted = []
    for mp in market_prices:
        price = mp.get("price", 0)
        if not price or price <= 0:
            continue
        norm = _normalize_to_per_unit(price, mp.get("description", ""),
                                      mp.get("quantity", 1), mp.get("uom", ""))
        per_unit = norm["per_unit"]
        if per_unit < 0.001 or per_unit > 50000:
            continue
        date_str = mp.get("date", "")
        weight = 0.3
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                dt = datetime.strptime(str(date_str).strip(), fmt)
                weight = math.pow(0.5, max(0, (now - dt).days) / 180)
                break
            except (ValueError, TypeError):
                continue
        covid_cats = ['glove', 'mask', 'gown', 'sanitiz', 'disinfect', 'ppe', 'n95']
        desc_lower = mp.get("description", "").lower()
        if any(c in desc_lower for c in covid_cats):
            for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(str(date_str).strip(), fmt)
                    if datetime(2020, 3, 1) <= dt <= datetime(2021, 12, 31):
                        weight *= 0.05
                    break
                except (ValueError, TypeError):
                    continue
        weighted.append({"price": per_unit, "raw_price": price,
                         "total_units": norm["total_units"], "pack_detected": norm["detected"],
                         "weight": round(weight, 4), "supplier": mp.get("supplier", ""),
                         "date": date_str, "is_reytech": mp.get("is_reytech", False),
                         "source": mp.get("source", "")})

    # Outlier removal: median-based clustering
    # Problem: $652/case and $8/box are in different UOMs.
    # If normalization couldn't detect pack size, raw price is an outlier.
    if len(weighted) >= 3:
        sorted_prices = sorted(w["price"] for w in weighted)
        median = sorted_prices[len(sorted_prices) // 2]
        if median > 0 and max(sorted_prices) / min(sorted_prices) > 50:
            filtered = [w for w in weighted
                        if median / 10 <= w["price"] <= median * 10]
            if len(filtered) >= 2:
                removed = len(weighted) - len(filtered)
                log.info("Oracle: removed %d outliers (median=$%.4f, "
                         "range kept: $%.4f-$%.4f)",
                         removed, median, median / 10, median * 10)
                weighted = filtered

    if not weighted:
        return {"data_points": 0, "freshness": "none"}
    total_w = sum(w["weight"] for w in weighted)
    wavg = sum(w["price"] * w["weight"] for w in weighted) / total_w if total_w > 0 else 0
    reytech = [w for w in weighted if w["is_reytech"]]
    comps = [w for w in weighted if not w["is_reytech"]]
    rt_wavg = None
    if reytech:
        rt_w = sum(w["weight"] for w in reytech)
        rt_wavg = sum(w["price"] * w["weight"] for w in reytech) / rt_w if rt_w > 0 else None
    comp_wavg = comp_low = comp_high = None
    if comps:
        c_w = sum(w["weight"] for w in comps)
        comp_wavg = sum(w["price"] * w["weight"] for w in comps) / c_w if c_w > 0 else None
        hw = [w for w in comps if w["weight"] > 0.3]
        if hw:
            comp_low = min(w["price"] for w in hw)
            comp_high = max(w["price"] for w in hw)
    best_w = max(w["weight"] for w in weighted)
    freshness = "stale" if best_w < 0.3 else ("aging" if best_w < 0.5 else "fresh")
    all_p = [w["price"] for w in weighted]
    return {
        "data_points": len(weighted), "freshness": freshness,
        "weighted_avg": round(wavg, 4) if wavg else None,
        "low": round(min(all_p), 4), "high": round(max(all_p), 4),
        "reytech_avg": round(rt_wavg, 4) if rt_wavg else None,
        "competitor_avg": round(comp_wavg, 4) if comp_wavg else None,
        "competitor_low": round(comp_low, 4) if comp_low else None,
        "competitor_high": round(comp_high, 4) if comp_high else None,
        "unique_suppliers": len(set(w["supplier"] for w in weighted if w["supplier"])),
        "normalization": "All prices normalized to per-unit",
    }


def _calculate_recommendation(cost, market, quantity):
    result = {"strategies": [], "tiers": [], "rationale": "",
              "quote_price": None, "markup_pct": None, "confidence": "low"}
    qty = float(quantity) if quantity else 1
    comp_avg = market.get("competitor_avg")
    comp_low = market.get("competitor_low") or comp_avg
    has_cost = cost is not None and cost > 0
    has_market = market.get("data_points", 0) > 0 and comp_avg

    if has_cost and has_market:
        ceiling = round(comp_avg * 0.98, 2)
        ceiling_m = ((ceiling - cost) / cost * 100) if cost > 0 else 0
        competitive = round(comp_low * 0.98, 2)
        comp_m = ((competitive - cost) / cost * 100) if cost > 0 else 0
        floor = round(cost * 1.15, 2)
        result["strategies"] = [
            {"name": "Maximize Margin", "price": ceiling, "markup_pct": round(ceiling_m, 1),
             "margin_per_unit": round(ceiling - cost, 2), "margin_total": round((ceiling - cost) * qty, 2)},
            {"name": "Undercut All", "price": competitive, "markup_pct": round(comp_m, 1),
             "margin_per_unit": round(competitive - cost, 2), "margin_total": round((competitive - cost) * qty, 2)},
            {"name": "Floor", "price": floor, "markup_pct": 15,
             "margin_per_unit": round(floor - cost, 2), "margin_total": round((floor - cost) * qty, 2)},
        ]
        for pct in [15, 20, 25, 30, 35, 40, 45, 50]:
            tp = round(cost * (1 + pct / 100), 2)
            result["tiers"].append({"pct": pct, "price": tp, "margin_total": round((tp - cost) * qty, 2),
                                     "beats_avg": tp < comp_avg, "beats_low": tp < comp_low})
        if ceiling > floor:
            result.update({"quote_price": ceiling, "markup_pct": round(ceiling_m, 1), "confidence": "high",
                           "rationale": f"Cost ${cost:.2f} -> ${ceiling:.2f} ({ceiling_m:.0f}%). Margin ${(ceiling-cost)*qty:,.2f}"})
        elif competitive > floor:
            result.update({"quote_price": competitive, "markup_pct": round(comp_m, 1), "confidence": "medium",
                           "rationale": f"Tight market. ${competitive:.2f} ({comp_m:.0f}%)"})
        else:
            result.update({"quote_price": floor, "markup_pct": 15, "confidence": "low",
                           "rationale": f"Competitors below floor ${floor:.2f}"})
    elif has_cost:
        result.update({"quote_price": round(cost * 1.25, 2), "markup_pct": 25, "confidence": "low",
                       "rationale": f"No market data. 25% on ${cost:.2f}"})
    elif has_market:
        result.update({"quote_price": round(comp_avg * 0.92, 2), "confidence": "medium",
                       "rationale": f"No cost. 8% under ${comp_avg:.2f}"})
    else:
        result["rationale"] = "No data. Manual research needed."
    return result


def _get_competitor_breakdown(market_prices):
    """Top competitors with normalized per-unit prices."""
    by_sup = {}
    for mp in market_prices:
        s = mp.get("supplier", "")
        if not s or mp.get("is_reytech"):
            continue
        norm = _normalize_to_per_unit(mp.get("price", 0), mp.get("description", ""),
                                      mp.get("quantity", 1), mp.get("uom", ""))
        if norm["per_unit"] < 0.001 or norm["per_unit"] > 50000:
            continue
        if s not in by_sup:
            by_sup[s] = {"supplier": s, "prices": [], "department": mp.get("department", ""),
                         "buyer_email": mp.get("buyer_email", "")}
        by_sup[s]["prices"].append(norm["per_unit"])
    result = []
    for s, data in by_sup.items():
        avg = sum(data["prices"]) / len(data["prices"])
        result.append({"supplier": s, "avg_price": round(avg, 4),
                       "low": round(min(data["prices"]), 4), "high": round(max(data["prices"]), 4),
                       "data_points": len(data["prices"]), "department": data["department"],
                       "buyer_email": data["buyer_email"]})
    result.sort(key=lambda x: x["avg_price"])
    return result[:8]


def _get_cross_sell(db, description):
    """Items commonly ordered on the same PO."""
    try:
        token_groups = _tokenize(description)[:2]
        if not token_groups:
            return []
        flat_kw = [g[0] for g in token_groups]
        where = " AND ".join(["LOWER(description) LIKE ?" for _ in flat_kw])
        params = [f"%{k}%" for k in flat_kw]
        po_rows = db.execute(f"""
            SELECT DISTINCT po_number FROM scprs_po_lines WHERE {where} LIMIT 50
        """, params).fetchall()
        if not po_rows:
            return []
        po_nums = [r[0] for r in po_rows]
        placeholders = ",".join(["?" for _ in po_nums])
        exclude = f"NOT (LOWER(description) LIKE ?)"
        rows = db.execute(f"""
            SELECT description, COUNT(DISTINCT po_number) c FROM scprs_po_lines
            WHERE po_number IN ({placeholders}) AND description!='' AND {exclude}
            GROUP BY description HAVING c>=2 ORDER BY c DESC LIMIT 5
        """, po_nums + [f"%{flat_kw[0]}%"]).fetchall()
        return [{"description": r[0][:100], "co_occurrence": r[1]} for r in rows]
    except Exception:
        return []


def _tokenize(text):
    """Extract keywords with abbreviation expansion. Returns list of lists."""
    stop = {'the', 'a', 'an', 'and', 'or', 'for', 'of', 'to', 'in', 'with',
            'item', 'items', 'each', 'per', 'ea', 'cs', 'bx', 'pk', 'box',
            'case', 'pack', 'qty', 'options', 'option', 'size', 'type'}
    expansions = {
        'medium': ['medium', 'med'], 'large': ['large', 'lg', 'lrg'],
        'small': ['small', 'sm', 'sml'], 'extra': ['extra', 'xtra'],
        'black': ['black', 'blk'], 'blue': ['blue', 'blu'],
        'white': ['white', 'wht'], 'powder': ['powder', 'pwdr', 'pwdrfree'],
        'nitrile': ['nitrile', 'nitrl'], 'gloves': ['gloves', 'glove', 'glov'],
        'exam': ['exam', 'examination'], 'wheelchair': ['wheelchair', 'whlchr'],
        'diabetic': ['diabetic', 'diab'],
    }
    words = [w for w in re.findall(r'[a-zA-Z0-9]{2,}', (text or "").lower()) if w not in stop]
    expanded = []
    for w in words:
        found = False
        for key, variants in expansions.items():
            if w in variants or w == key:
                expanded.append(variants)
                found = True
                break
        if not found:
            expanded.append([w])
    return expanded


# ── Item Memory ─────────────────────────────────────────────────

def _parse_float(val):
    """Safely parse a price/cost value to float."""
    try:
        return float(str(val or 0).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        return 0


def _learn_item(db, item, source="backfill"):
    """Learn ALL fields from a single item dict into item_mappings + supplier_costs."""
    desc = item.get("description", item.get("desc", ""))
    if not desc or len(desc) < 3:
        return False

    item_num = item.get("item_number", item.get("part_number", item.get("mfg_number", "")))
    supplier = item.get("item_supplier", item.get("supplier", ""))
    cost_val = _parse_float(item.get("supplier_cost") or item.get("cost") or item.get("unit_cost"))
    sell_val = _parse_float(item.get("unit_price") or item.get("price") or item.get("bid_price") or
                            item.get("sell_price"))
    url = item.get("item_link", item.get("supplier_url", item.get("product_url", "")))
    uom = item.get("uom", item.get("unit_of_measure", ""))
    mfg = item.get("mfg_number", item.get("part_number", ""))
    asin = item.get("asin", "")

    if cost_val <= 0 and sell_val <= 0 and not url:
        return False

    try:
        db.execute("""
            INSERT INTO item_mappings
            (original_description, original_item_number, canonical_description,
             canonical_item_number, mfg_number, asin, product_url, supplier,
             last_cost, last_sell_price, uom, supplier_url,
             confidence, confirmed, times_confirmed, last_confirmed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0.8,1,1,datetime('now'))
            ON CONFLICT(original_description, original_item_number) DO UPDATE SET
                last_cost = CASE WHEN excluded.last_cost > 0 THEN excluded.last_cost ELSE item_mappings.last_cost END,
                last_sell_price = CASE WHEN excluded.last_sell_price > 0 THEN excluded.last_sell_price ELSE item_mappings.last_sell_price END,
                supplier = COALESCE(NULLIF(excluded.supplier,''), item_mappings.supplier),
                mfg_number = COALESCE(NULLIF(excluded.mfg_number,''), item_mappings.mfg_number),
                asin = COALESCE(NULLIF(excluded.asin,''), item_mappings.asin),
                product_url = COALESCE(NULLIF(excluded.product_url,''), item_mappings.product_url),
                supplier_url = COALESCE(NULLIF(excluded.supplier_url,''), item_mappings.supplier_url),
                uom = COALESCE(NULLIF(excluded.uom,''), item_mappings.uom),
                times_confirmed = item_mappings.times_confirmed + 1,
                last_confirmed = datetime('now'),
                confidence = MIN(0.95, item_mappings.confidence + 0.05)
        """, (desc[:500], item_num, desc[:500], item_num, mfg, asin, url,
              supplier, cost_val, sell_val, uom, url))
    except Exception:
        return False

    # Also lock cost
    if cost_val > 0:
        try:
            expires = (datetime.now() + timedelta(days=60)).isoformat()
            db.execute("""
                INSERT INTO supplier_costs
                (description, item_number, cost, supplier, source, source_url, confirmed_at, expires_at)
                VALUES (?,?,?,?,?,?,datetime('now'),?)
                ON CONFLICT(description, supplier) DO UPDATE SET
                    cost = excluded.cost, source_url = COALESCE(NULLIF(excluded.source_url,''), supplier_costs.source_url),
                    confirmed_at = datetime('now'), expires_at = excluded.expires_at
            """, (desc[:500], item_num, cost_val, supplier, source, url, expires))
        except Exception:
            pass

    return True


def backfill_item_memory():
    """Learn ALL fields from ALL existing priced PCs and quotes."""
    import json
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=30)
    learned = 0

    # From price_checks
    try:
        rows = db.execute("""
            SELECT id, pc_data, items FROM price_checks
            WHERE status IN ('sent','submitted','won','completed','priced')
        """).fetchall()
        for row in rows:
            try:
                pc_data = json.loads(row[1] or "{}") if row[1] else {}
                items = pc_data.get("items", pc_data.get("line_items", []))
                if isinstance(items, str):
                    items = json.loads(items)
                if not items:
                    items = json.loads(row[2] or "[]") if row[2] else []
                for item in (items or []):
                    if _learn_item(db, item, source="backfill_pc"):
                        learned += 1
            except Exception:
                pass
    except Exception as e:
        log.warning("Backfill PCs: %s", e)

    # From quotes
    try:
        rows = db.execute("""
            SELECT id, items_detail, line_items FROM quotes
            WHERE status IN ('sent','submitted','won','completed')
        """).fetchall()
        for row in rows:
            try:
                items = json.loads(row[1] or "[]") if row[1] else []
                if not items:
                    items = json.loads(row[2] or "[]") if row[2] else []
                if isinstance(items, str):
                    items = json.loads(items)
                for item in (items or []):
                    if _learn_item(db, item, source="backfill_quote"):
                        learned += 1
            except Exception:
                pass
    except Exception as e:
        log.warning("Backfill quotes: %s", e)

    db.commit()
    db.close()
    log.info("Item memory backfill: %d items learned (all fields)", learned)
    return learned


def confirm_item_mapping(original_description, canonical_description,
                          item_number="", mfg_number="", product_url="",
                          supplier="", cost=None):
    """User confirms item match. Persists forever."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.execute("""
        INSERT INTO item_mappings
        (original_description, original_item_number, canonical_description,
         canonical_item_number, mfg_number, product_url, supplier,
         last_cost, confidence, confirmed, times_confirmed, last_confirmed)
        VALUES (?,?,?,?,?,?,?,?,0.95,1,1,datetime('now'))
        ON CONFLICT(original_description, original_item_number) DO UPDATE SET
            canonical_description=excluded.canonical_description,
            mfg_number=COALESCE(NULLIF(excluded.mfg_number,''), item_mappings.mfg_number),
            product_url=COALESCE(NULLIF(excluded.product_url,''), item_mappings.product_url),
            supplier=COALESCE(NULLIF(excluded.supplier,''), item_mappings.supplier),
            last_cost=CASE WHEN excluded.last_cost>0 THEN excluded.last_cost ELSE item_mappings.last_cost END,
            confirmed=1, confidence=0.95, times_confirmed=item_mappings.times_confirmed+1,
            last_confirmed=datetime('now')
    """, (original_description[:500], item_number, canonical_description[:500],
          item_number, mfg_number, product_url, supplier, cost or 0))
    db.commit()
    db.close()
    log.info("Item confirmed: '%s' -> '%s'", original_description[:40], canonical_description[:40])


def auto_learn_mapping(original_description, matched_description,
                        item_number="", confidence=0.5):
    """System auto-learns a mapping."""
    import sqlite3
    from src.core.db import DB_PATH
    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        db.execute("""
            INSERT OR IGNORE INTO item_mappings
            (original_description, original_item_number, canonical_description,
             canonical_item_number, confidence, confirmed, times_confirmed)
            VALUES (?,?,?,?,?,0,1)
        """, (original_description[:500], item_number, matched_description[:500],
              item_number, confidence))
        db.commit()
        db.close()
    except Exception:
        pass


# ── Supplier Cost Lock ──────────────────────────────────────────

def lock_cost(description, cost, supplier="", source="manual",
              source_url="", expires_days=30, item_number=""):
    """Lock a supplier cost with auto-expiry."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    expires = (datetime.now() + timedelta(days=expires_days)).isoformat()
    db.execute("""
        INSERT INTO supplier_costs
        (description, item_number, cost, supplier, source, source_url, confirmed_at, expires_at)
        VALUES (?,?,?,?,?,?,datetime('now'),?)
        ON CONFLICT(description, supplier) DO UPDATE SET
            cost=excluded.cost, source=excluded.source, source_url=excluded.source_url,
            confirmed_at=datetime('now'), expires_at=excluded.expires_at
    """, (description[:500], item_number, cost, supplier, source, source_url, expires))
    db.commit()
    db.close()
    log.info("Cost locked: '%s' = $%.2f from %s", description[:40], cost, supplier)


def get_expiring_costs(days=7):
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    rows = db.execute("""
        SELECT description, cost, supplier, source, expires_at FROM supplier_costs
        WHERE expires_at BETWEEN datetime('now') AND datetime('now', ?) ORDER BY expires_at
    """, (f"+{days} days",)).fetchall()
    db.close()
    return [{"description": r[0], "cost": r[1], "supplier": r[2], "source": r[3], "expires": r[4]} for r in rows]


# ── Quote Speed Clock ───────────────────────────────────────────

def record_speed_event(record_type, record_id, event):
    """Record timing event: received, opened, priced, generated, sent."""
    import sqlite3
    from src.core.db import DB_PATH
    table = "price_checks" if record_type == "pc" else "quotes"
    col = f"{event}_at" if event != "opened" else "first_opened_at"
    try:
        db = sqlite3.connect(DB_PATH, timeout=10)
        current = db.execute(f"SELECT [{col}] FROM [{table}] WHERE id=?", (record_id,)).fetchone()
        if current and not current[0]:
            now = datetime.now().isoformat()
            db.execute(f"UPDATE [{table}] SET [{col}]=? WHERE id=?", (now, record_id))
            if event in ("priced", "sent"):
                received = db.execute(f"SELECT received_at, created_at FROM [{table}] WHERE id=?",
                                      (record_id,)).fetchone()
                start = received[0] or received[1] if received else None
                if start:
                    try:
                        elapsed = int((datetime.now() - datetime.fromisoformat(start)).total_seconds() / 60)
                        db.execute(f"UPDATE [{table}] SET time_to_{event}_mins=? WHERE id=?", (elapsed, record_id))
                    except Exception:
                        pass
            db.commit()
        db.close()
    except Exception:
        pass


def get_speed_stats():
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    stats = {}
    for table, name in [("price_checks", "pcs"), ("quotes", "quotes")]:
        try:
            rows = db.execute(f"""
                SELECT time_to_price_mins, time_to_send_mins FROM [{table}]
                WHERE time_to_send_mins > 0 ORDER BY sent_at DESC LIMIT 50
            """).fetchall()
            if rows:
                pt = [r[0] for r in rows if r[0] and r[0] > 0]
                st = [r[1] for r in rows if r[1] and r[1] > 0]
                stats[name] = {
                    "avg_to_price": round(sum(pt) / len(pt)) if pt else None,
                    "avg_to_send": round(sum(st) / len(st)) if st else None,
                    "fastest": min(st) if st else None, "slowest": max(st) if st else None,
                    "sample": len(rows),
                }
            else:
                stats[name] = {"sample": 0}
        except Exception:
            stats[name] = {"sample": 0}
    db.close()
    return stats
