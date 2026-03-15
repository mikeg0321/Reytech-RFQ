"""
Buyer Intelligence Engine
Auto-enriches buyer profiles from FI$Cal data.
Scores prospects for outreach prioritization.
"""
import logging
import re
import math
from datetime import datetime

log = logging.getLogger("reytech.buyer_intelligence")


def refresh_buyer_profiles():
    """Rebuild all buyer profiles from scprs_po_master + scprs_po_lines."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.row_factory = None

    buyers = db.execute("""
        SELECT DISTINCT buyer_email, buyer_name, dept_name, dept_code
        FROM scprs_po_master
        WHERE buyer_email IS NOT NULL AND buyer_email != ''
    """).fetchall()

    log.info("Buyer refresh: processing %d unique buyers", len(buyers))
    updated = 0

    for email, name, dept, dept_code in buyers:
        if not email or "@" not in email:
            continue
        try:
            stats = db.execute("""
                SELECT COUNT(DISTINCT po_number) as total_pos,
                       SUM(CAST(REPLACE(REPLACE(grand_total,'$',''),',','') AS REAL)) as total_spend,
                       MIN(start_date) as first_date,
                       MAX(start_date) as last_date
                FROM scprs_po_master WHERE buyer_email = ?
            """, (email,)).fetchone()

            line_count = db.execute("""
                SELECT COUNT(*) FROM scprs_po_lines l
                JOIN scprs_po_master m ON l.po_number = m.po_number
                WHERE m.buyer_email = ?
            """, (email,)).fetchone()[0]

            reytech = db.execute("""
                SELECT COUNT(DISTINCT po_number),
                       SUM(CAST(REPLACE(REPLACE(grand_total,'$',''),',','') AS REAL)),
                       MAX(start_date)
                FROM scprs_po_master
                WHERE buyer_email = ? AND UPPER(supplier) LIKE '%REYTECH%'
            """, (email,)).fetchone()

            rt_pos = reytech[0] or 0
            rt_spend = reytech[1] or 0
            rt_last = reytech[2] or ""

            items = db.execute("""
                SELECT l.description FROM scprs_po_lines l
                JOIN scprs_po_master m ON l.po_number = m.po_number
                WHERE m.buyer_email = ?
            """, (email,)).fetchall()

            top_cats = _extract_categories([r[0] for r in items if r[0]])
            status = "active_customer" if rt_pos > 0 else "prospect"

            score = _calculate_prospect_score(
                total_pos=stats[0] or 0, total_spend=stats[1] or 0,
                last_date=stats[3] or "", line_items=line_count,
                buys_from_reytech=rt_pos > 0, reytech_spend=rt_spend,
                categories=top_cats,
            )

            db.execute("""
                INSERT INTO scprs_buyers
                (buyer_email, buyer_name, department, dept_code,
                 total_pos, total_spend, total_line_items,
                 first_po_date, last_po_date, top_categories,
                 buys_from_reytech, reytech_spend, reytech_last_date,
                 relationship_status, prospect_score, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                ON CONFLICT(buyer_email) DO UPDATE SET
                    buyer_name = COALESCE(NULLIF(excluded.buyer_name,''), scprs_buyers.buyer_name),
                    department = COALESCE(NULLIF(excluded.department,''), scprs_buyers.department),
                    total_pos = excluded.total_pos,
                    total_spend = excluded.total_spend,
                    total_line_items = excluded.total_line_items,
                    first_po_date = excluded.first_po_date,
                    last_po_date = excluded.last_po_date,
                    top_categories = excluded.top_categories,
                    buys_from_reytech = excluded.buys_from_reytech,
                    reytech_spend = excluded.reytech_spend,
                    reytech_last_date = excluded.reytech_last_date,
                    relationship_status = CASE
                        WHEN scprs_buyers.relationship_status IN ('outreach_sent','replied','meeting_scheduled')
                        THEN scprs_buyers.relationship_status
                        ELSE excluded.relationship_status END,
                    prospect_score = excluded.prospect_score,
                    updated_at = datetime('now')
            """, (
                email, name or "", dept or "", dept_code or "",
                stats[0] or 0, stats[1] or 0, line_count,
                stats[2] or "", stats[3] or "",
                ", ".join(top_cats[:10]),
                1 if rt_pos > 0 else 0, rt_spend, rt_last,
                status, score,
            ))

            # Store buyer's item history
            buyer_items = db.execute("""
                SELECT l.description, l.unit_price, l.quantity,
                       m.supplier, m.start_date, m.po_number
                FROM scprs_po_lines l
                JOIN scprs_po_master m ON l.po_number = m.po_number
                WHERE m.buyer_email = ?
                ORDER BY m.start_date DESC
            """, (email,)).fetchall()

            for bi in buyer_items:
                try:
                    db.execute("""
                        INSERT OR IGNORE INTO scprs_buyer_items
                        (buyer_email, po_number, description, unit_price,
                         quantity, supplier, date)
                        VALUES (?,?,?,?,?,?,?)
                    """, (email, bi[5], bi[0], bi[1], bi[2], bi[3], bi[4]))
                except Exception:
                    pass

            updated += 1
        except Exception as e:
            log.warning("Buyer profile %s failed: %s", email, str(e)[:60])

    db.commit()
    db.close()
    log.info("Buyer refresh complete: %d profiles updated", updated)
    return updated


def _extract_categories(descriptions):
    """Extract top product categories from item descriptions."""
    stop_words = {"item", "items", "each", "per", "ea", "cs", "bx", "pk",
                  "box", "case", "pack", "qty", "the", "and", "for", "with",
                  "size", "type", "color", "inch", "model", "ref", "mfr",
                  "options", "option", "white", "black", "blue", "clear",
                  "large", "medium", "small", "xlg", "pkg", "new"}
    word_counts = {}
    for desc in descriptions:
        words = re.findall(r'[a-zA-Z]{4,}', (desc or "").lower())
        for w in words:
            if w not in stop_words:
                word_counts[w] = word_counts.get(w, 0) + 1
    sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
    return [w[0] for w in sorted_words[:15]]


def _calculate_prospect_score(total_pos, total_spend, last_date,
                               line_items, buys_from_reytech,
                               reytech_spend, categories):
    """Score a buyer as a prospect (0-100)."""
    score = 0

    # Recency (0-30 points)
    try:
        dt = None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(str(last_date).strip(), fmt)
                break
            except ValueError:
                continue
        if dt:
            age_days = (datetime.now() - dt).days
            score += 30 * math.pow(0.5, age_days / 180)
    except Exception:
        pass

    # Volume (0-25 points)
    if total_pos > 0:
        score += min(25, 10 * math.log10(total_pos + 1))

    # Spend (0-25 points)
    if total_spend and total_spend > 0:
        score += min(25, 5 * math.log10(total_spend + 1))

    # Category overlap (0-10 points)
    reytech_categories = ["glove", "radio", "shoe", "diabetic", "wheelchair",
                          "mattress", "label", "stamp", "cart", "sleeve",
                          "knee", "insole", "deodorant", "shampoo", "catheter",
                          "dressing", "syringe", "bandage", "mask", "gown",
                          "wipe", "sanitiz", "paper", "envelope", "notebook"]
    if categories:
        overlap = sum(1 for cat in categories
                      if any(rc in cat for rc in reytech_categories))
        score += min(10, overlap * 2)

    # Existing customer adjustment
    if buys_from_reytech:
        if reytech_spend and reytech_spend > 0 and total_spend and total_spend > 0:
            reytech_share = reytech_spend / total_spend
            score += 10 * (1 - reytech_share)
    else:
        score += 5

    return round(min(100, max(0, score)), 1)


def get_top_prospects(limit=50, min_score=20, exclude_customers=False):
    """Get ranked prospect list for outreach."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)

    where = "WHERE prospect_score >= ?"
    params = [min_score]
    if exclude_customers:
        where += " AND buys_from_reytech = 0"

    rows = db.execute(f"""
        SELECT buyer_email, buyer_name, department,
               total_pos, total_spend, total_line_items,
               last_po_date, top_categories,
               buys_from_reytech, reytech_spend,
               relationship_status, prospect_score,
               outreach_status, outreach_last_date
        FROM scprs_buyers {where}
        ORDER BY prospect_score DESC LIMIT ?
    """, params + [limit]).fetchall()
    db.close()

    return [{
        "email": r[0], "name": r[1], "department": r[2],
        "total_pos": r[3], "total_spend": r[4],
        "total_line_items": r[5], "last_po_date": r[6],
        "top_categories": r[7], "is_reytech_customer": bool(r[8]),
        "reytech_spend": r[9], "relationship_status": r[10],
        "prospect_score": r[11], "outreach_status": r[12],
        "outreach_last_date": r[13],
    } for r in rows]


def get_buyer_profile(email):
    """Get full buyer profile with purchase history."""
    import sqlite3
    from src.core.db import DB_PATH
    db = sqlite3.connect(DB_PATH, timeout=10)
    db.row_factory = sqlite3.Row

    buyer = db.execute("SELECT * FROM scprs_buyers WHERE buyer_email = ?", (email,)).fetchone()
    if not buyer:
        db.close()
        return None

    profile = dict(buyer)

    items = db.execute("""
        SELECT description, unit_price, quantity, supplier, date, po_number
        FROM scprs_buyer_items WHERE buyer_email = ?
        ORDER BY date DESC LIMIT 100
    """, (email,)).fetchall()

    profile["purchase_history"] = [{
        "description": i[0], "unit_price": i[1], "quantity": i[2],
        "supplier": i[3], "date": i[4], "po_number": i[5],
    } for i in items]

    profile["overlap_items"] = _find_reytech_overlap(items, db)
    db.close()
    return profile


def _find_reytech_overlap(buyer_items, db):
    """Find items this buyer purchases that Reytech has won before."""
    overlaps = []
    seen = set()

    for item in buyer_items:
        desc = item[0] or ""
        keywords = re.findall(r'[a-zA-Z]{4,}', desc.lower())[:3]
        if not keywords:
            continue

        key = tuple(sorted(keywords))
        if key in seen:
            continue
        seen.add(key)

        where = " AND ".join(["LOWER(l.description) LIKE ?" for _ in keywords])
        params = [f"%{k}%" for k in keywords]

        reytech_match = db.execute(f"""
            SELECT l.description, l.unit_price, m.start_date, m.po_number
            FROM scprs_po_lines l
            JOIN scprs_po_master m ON l.po_number = m.po_number
            WHERE UPPER(m.supplier) LIKE '%REYTECH%' AND {where}
            ORDER BY m.start_date DESC LIMIT 1
        """, params).fetchone()

        if reytech_match:
            overlaps.append({
                "buyer_item": desc[:100],
                "reytech_item": reytech_match[0][:100] if reytech_match[0] else "",
                "reytech_price": reytech_match[1],
                "reytech_date": reytech_match[2],
                "reytech_po": reytech_match[3],
            })

    return overlaps
