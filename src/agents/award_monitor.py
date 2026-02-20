"""
award_monitor.py — PO Award Monitor for Reytech
Tracks sent quotes through SCPRS to determine win/loss outcomes.

Workflow:
  1. Every check cycle, find all PCs in status "sent" or "pending_award"
  2. If last SCPRS check was < 3 business days ago, skip
  3. Query SCPRS for matching POs (same agency + similar items)
  4. If PO found awarded to another vendor → mark LOST, log competitor + price
  5. If PO found awarded to Reytech → mark WON
  6. If 45+ days since sent with no match → mark EXPIRED (closed_lost)
  7. Log all competitor intel for dashboard + price suggestions

Runs: Background thread, checks once per hour. Actual SCPRS queries only
      fire every 3 business days per PC.
"""

import os
import json
import logging
import threading
import time
import sqlite3
from datetime import datetime, timedelta

log = logging.getLogger("award_monitor")

try:
    from src.core.paths import DATA_DIR
    from src.core.db import get_db
except ImportError:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), "data")
    def get_db():
        return sqlite3.connect(os.path.join(DATA_DIR, "reytech.db"))

# ── Configuration ────────────────────────────────────────────────────────────
CHECK_INTERVAL_HOURS = 1        # How often the monitor thread wakes up
SCPRS_CHECK_INTERVAL_DAYS = 3   # Business days between SCPRS checks per PC
EXPIRY_DAYS = 45                # Days after sent with no award → expired
MAX_CHECKS_PER_RUN = 10         # Don't flood SCPRS

_monitor_thread = None
_monitor_running = False


# ── Business Day Calculator ──────────────────────────────────────────────────

def _business_days_ago(n: int, from_date=None) -> datetime:
    """Calculate the date N business days before from_date."""
    d = from_date or datetime.now()
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            count += 1
    return d


def _business_days_between(d1_str: str, d2: datetime = None) -> int:
    """Count business days between date string and now (or d2)."""
    try:
        d1 = datetime.fromisoformat(d1_str.replace("Z", "+00:00").split("+")[0])
    except Exception:
        return 0
    d2 = d2 or datetime.now()
    count = 0
    current = min(d1, d2)
    end = max(d1, d2)
    while current < end:
        current += timedelta(days=1)
        if current.weekday() < 5:
            count += 1
    return count


# ── Smart Match: Find Existing PC for Incoming Email ─────────────────────────

def smart_match_pc(new_email: dict, existing_pcs: dict) -> dict | None:
    """Check if an incoming email is a revision of an existing PC.
    
    Returns matched PC dict with match details, or None.
    
    Match signals (ranked by strength):
      1. Solicitation number exact match (strongest)
      2. Same institution + >60% item description overlap
      3. Same requestor email within 14 days
    """
    sol_hint = (new_email.get("solicitation_hint") or "").strip()
    sender = (new_email.get("sender_email") or "").lower()
    subject = (new_email.get("subject") or "").lower()
    
    best_match = None
    best_score = 0
    
    for pcid, pc in existing_pcs.items():
        score = 0
        reasons = []
        
        # Signal 1: Solicitation number match (exact)
        pc_sol = (pc.get("pc_number") or "") + " " + (pc.get("solicitation_number") or "")
        if sol_hint and sol_hint != "unknown" and sol_hint in pc_sol:
            score += 50
            reasons.append(f"sol# {sol_hint}")
        
        # Signal 2: Same institution/requestor
        pc_req = (pc.get("requestor") or "").lower()
        pc_email = (pc.get("contact_email") or "").lower()
        if sender and (sender in pc_req or sender in pc_email or sender == pc_email):
            score += 20
            reasons.append("same sender")
        
        # Signal 3: Subject similarity (PC number in subject)
        pc_num = (pc.get("pc_number") or "").lower()
        if pc_num and len(pc_num) > 3 and pc_num in subject:
            score += 30
            reasons.append(f"PC# in subject")
        
        # Signal 4: Item description overlap
        pc_items = pc.get("items", [])
        if pc_items:
            pc_descs = set()
            for it in pc_items:
                desc = (it.get("description") or "").lower()
                # Extract key words (3+ chars)
                words = [w for w in desc.split() if len(w) > 3]
                pc_descs.update(words)
            
            # Check body/subject for overlap
            body = (new_email.get("body_preview") or "").lower()
            if pc_descs and body:
                overlap = sum(1 for w in pc_descs if w in body)
                pct = overlap / max(len(pc_descs), 1)
                if pct > 0.6:
                    score += 25
                    reasons.append(f"{pct:.0%} item overlap")
        
        # Signal 5: Date proximity (within 14 days)
        pc_created = pc.get("created_at", "")
        if pc_created:
            try:
                pc_dt = datetime.fromisoformat(pc_created.split("+")[0])
                days = abs((datetime.now() - pc_dt).days)
                if days <= 14:
                    score += 10
                    reasons.append(f"{days}d ago")
            except Exception:
                pass
        
        if score > best_score and score >= 30:
            best_score = score
            best_match = {
                "pc_id": pcid,
                "pc": pc,
                "score": score,
                "reasons": reasons,
            }
    
    if best_match:
        log.info("Smart match: new email matches PC %s (score=%d: %s)",
                 best_match["pc_id"], best_match["score"], ", ".join(best_match["reasons"]))
    
    return best_match


# ── Item Diff: Compare PC Revisions ──────────────────────────────────────────

def diff_pc_items(old_items: list, new_items: list) -> dict:
    """Compare two PC item lists and return the diff.
    
    Returns: {added: [], removed: [], changed: [], unchanged: int}
    """
    old_map = {}
    for it in old_items:
        key = (it.get("description") or "").lower().strip()[:60]
        old_map[key] = it
    
    new_map = {}
    for it in new_items:
        key = (it.get("description") or "").lower().strip()[:60]
        new_map[key] = it
    
    added = [v for k, v in new_map.items() if k not in old_map]
    removed = [v for k, v in old_map.items() if k not in new_map]
    changed = []
    unchanged = 0
    
    for key in set(old_map.keys()) & set(new_map.keys()):
        old_it = old_map[key]
        new_it = new_map[key]
        old_qty = old_it.get("qty", 1)
        new_qty = new_it.get("qty", 1)
        if old_qty != new_qty:
            changed.append({
                "description": new_it.get("description", ""),
                "old_qty": old_qty,
                "new_qty": new_qty,
            })
        else:
            unchanged += 1
    
    return {
        "added": added,
        "removed": removed,
        "changed": changed,
        "unchanged": unchanged,
    }


# ── SCPRS Award Check ───────────────────────────────────────────────────────

def check_pc_award(pc: dict) -> dict | None:
    """Query SCPRS for PO awards matching this PC.
    
    Returns dict with award info if found, None if no match.
    """
    try:
        conn = get_db()
        conn.row_factory = sqlite3.Row
        
        institution = pc.get("institution", "")
        items = pc.get("items", [])
        if not institution or not items:
            return None
        
        # Build item search terms
        search_terms = []
        for it in items[:5]:
            desc = (it.get("description") or "").strip()
            if len(desc) > 5:
                # Use first significant words
                words = [w for w in desc.split() if len(w) > 3][:2]
                if words:
                    search_terms.append(" ".join(words))
        
        if not search_terms:
            return None
        
        # Find dept_code from institution
        from src.agents.scprs_universal_pull import ALL_AGENCIES
        dept_code = None
        for code, (name, *_) in ALL_AGENCIES.items():
            if institution.upper() in name.upper() or name.upper().split("/")[0].strip() in institution.upper():
                dept_code = code
                break
        
        if not dept_code:
            return None
        
        # Search for matching POs
        term_clauses = " OR ".join([
            f"LOWER(l.description) LIKE '%{t.lower().replace(chr(39), '')}%'"
            for t in search_terms
        ] or ["1=0"])
        
        pc_created = pc.get("created_at", "")[:10] or "2026-01-01"
        
        rows = conn.execute(f"""
            SELECT p.po_number, p.supplier, p.grand_total, p.start_date, p.buyer_email,
                   l.description, l.unit_price, l.quantity
            FROM scprs_po_master p
            JOIN scprs_po_lines l ON l.po_id = p.id
            WHERE p.dept_code = ?
              AND p.start_date >= ?
              AND ({term_clauses})
            ORDER BY p.start_date DESC LIMIT 10
        """, (dept_code, pc_created)).fetchall()
        
        if not rows:
            return None
        
        # Check if Reytech won or competitor won
        for row in rows:
            r = dict(row)
            supplier = (r.get("supplier") or "").lower()
            if "reytech" in supplier or "rey tech" in supplier:
                return {
                    "outcome": "won",
                    "po_number": r["po_number"],
                    "supplier": r["supplier"],
                    "price": r.get("unit_price"),
                    "total": r.get("grand_total"),
                }
        
        # Competitor won
        winner = dict(rows[0])
        return {
            "outcome": "lost",
            "po_number": winner["po_number"],
            "supplier": winner["supplier"],
            "price": winner.get("unit_price"),
            "total": winner.get("grand_total"),
            "item_desc": winner.get("description", ""),
            "buyer_email": winner.get("buyer_email", ""),
        }
        
    except Exception as e:
        log.debug("SCPRS award check failed: %s", e)
        return None


# ── Log Competitor Intel ─────────────────────────────────────────────────────

def log_competitor(pc: dict, award: dict, our_quote_total: float = 0):
    """Record a competitive loss in the competitor_intel table."""
    try:
        conn = get_db()
        now = datetime.now().isoformat()
        
        their_price = award.get("total") or award.get("price") or 0
        delta = their_price - our_quote_total if our_quote_total else 0
        delta_pct = (delta / our_quote_total * 100) if our_quote_total else 0
        
        items_summary = ", ".join(
            (it.get("description") or "")[:40] for it in pc.get("items", [])[:5]
        )
        
        conn.execute("""
            INSERT INTO competitor_intel
            (found_at, pc_id, quote_number, our_price, competitor_name,
             competitor_price, price_delta, price_delta_pct, po_number,
             agency, institution, item_summary, solicitation, outcome, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now,
            pc.get("id", ""),
            pc.get("reytech_quote_number", ""),
            our_quote_total,
            award.get("supplier", "Unknown"),
            their_price,
            delta,
            round(delta_pct, 1),
            award.get("po_number", ""),
            pc.get("agency", ""),
            pc.get("institution", ""),
            items_summary,
            pc.get("solicitation_number", pc.get("pc_number", "")),
            "lost",
            f"SCPRS PO {award.get('po_number','')} awarded to {award.get('supplier','')}",
        ))
        conn.commit()
        
        log.info("Competitor logged: %s won PO %s ($%.2f vs our $%.2f = %+.1f%%)",
                 award.get("supplier"), award.get("po_number"),
                 their_price, our_quote_total, delta_pct)
        
    except Exception as e:
        log.error("Failed to log competitor: %s", e)


# ── Price Suggestions ────────────────────────────────────────────────────────

def get_price_suggestions(items: list, institution: str = "") -> list:
    """For a list of PC items, check competitor_intel for historical losses.
    
    Returns list of suggestions like:
    [{item_desc, last_loss_to, their_price, our_price, suggested_action}]
    """
    suggestions = []
    try:
        conn = get_db()
        conn.row_factory = sqlite3.Row
        
        for item in items:
            desc = (item.get("description") or "").strip()
            if len(desc) < 5:
                continue
            
            # Search for previous losses on similar items
            words = [w for w in desc.lower().split() if len(w) > 3][:3]
            if not words:
                continue
            
            clauses = " AND ".join([f"LOWER(item_summary) LIKE '%{w}%'" for w in words])
            rows = conn.execute(f"""
                SELECT competitor_name, competitor_price, our_price, 
                       price_delta_pct, found_at, agency, po_number
                FROM competitor_intel
                WHERE {clauses} AND outcome='lost'
                ORDER BY found_at DESC LIMIT 3
            """).fetchall()
            
            for row in rows:
                r = dict(row)
                suggestions.append({
                    "item_desc": desc[:60],
                    "competitor": r["competitor_name"],
                    "their_price": r["competitor_price"],
                    "our_price": r["our_price"],
                    "delta_pct": r["price_delta_pct"],
                    "when": r["found_at"][:10],
                    "agency": r["agency"],
                    "suggestion": (
                        f"Lost to {r['competitor_name']} at ${r['competitor_price']:,.2f} "
                        f"(we quoted ${r['our_price']:,.2f}, {r['price_delta_pct']:+.1f}%). "
                        f"Consider pricing ~${r['competitor_price'] * 0.98:,.2f} to be competitive."
                    ),
                })
        
    except Exception as e:
        log.debug("Price suggestions error: %s", e)
    
    return suggestions


# ── Get Competitor Dashboard Data ────────────────────────────────────────────

def get_competitor_dashboard() -> dict:
    """Aggregate competitor intel for the dashboard."""
    try:
        conn = get_db()
        conn.row_factory = sqlite3.Row
        
        # Top competitors by loss count
        top_competitors = [dict(r) for r in conn.execute("""
            SELECT competitor_name, COUNT(*) as losses,
                   AVG(price_delta_pct) as avg_delta_pct,
                   SUM(competitor_price) as total_won,
                   GROUP_CONCAT(DISTINCT agency) as agencies
            FROM competitor_intel WHERE outcome='lost'
            GROUP BY competitor_name ORDER BY losses DESC LIMIT 15
        """).fetchall()]
        
        # Losses by agency
        by_agency = [dict(r) for r in conn.execute("""
            SELECT agency, COUNT(*) as losses,
                   AVG(price_delta_pct) as avg_delta_pct,
                   GROUP_CONCAT(DISTINCT competitor_name) as competitors
            FROM competitor_intel WHERE outcome='lost'
            GROUP BY agency ORDER BY losses DESC LIMIT 10
        """).fetchall()]
        
        # Recent losses
        recent = [dict(r) for r in conn.execute("""
            SELECT * FROM competitor_intel
            WHERE outcome='lost'
            ORDER BY found_at DESC LIMIT 10
        """).fetchall()]
        
        # Overall stats
        stats = dict(conn.execute("""
            SELECT COUNT(*) as total_losses,
                   AVG(price_delta_pct) as avg_delta_pct,
                   COUNT(DISTINCT competitor_name) as unique_competitors,
                   SUM(CASE WHEN price_delta_pct > 0 THEN 1 ELSE 0 END) as times_undercut
            FROM competitor_intel WHERE outcome='lost'
        """).fetchone() or {})
        
        return {
            "top_competitors": top_competitors,
            "by_agency": by_agency,
            "recent_losses": recent,
            "stats": stats,
        }
    except Exception as e:
        log.debug("Competitor dashboard error: %s", e)
        return {"top_competitors": [], "by_agency": [], "recent_losses": [], "stats": {}}


# ── Monitor Run ──────────────────────────────────────────────────────────────

def run_award_check() -> dict:
    """Single check cycle: scan all sent PCs for award status.
    
    Returns: {checked, won, lost, expired, skipped, errors}
    """
    results = {"checked": 0, "won": 0, "lost": 0, "expired": 0, "skipped": 0, "errors": []}
    
    try:
        # Load PCs from JSON (source of truth)
        pc_path = os.path.join(DATA_DIR, "price_checks.json")
        with open(pc_path) as f:
            pcs = json.load(f)
    except Exception:
        pcs = {}
    
    now = datetime.now()
    checked_this_run = 0
    
    for pcid, pc in list(pcs.items()):
        status = pc.get("status", "")
        
        # Only check PCs that have been sent / pending award
        if status not in ("sent", "pending_award", "quoted"):
            continue
        
        # Check if it's time for SCPRS check (3 business days since last check)
        last_check = pc.get("last_scprs_check", "")
        if last_check:
            bdays = _business_days_between(last_check, now)
            if bdays < SCPRS_CHECK_INTERVAL_DAYS:
                results["skipped"] += 1
                continue
        
        # Check expiry: 45+ days since sent → expired
        sent_at = pc.get("sent_at", pc.get("created_at", ""))
        if sent_at:
            try:
                sent_dt = datetime.fromisoformat(sent_at.split("+")[0])
                days_since = (now - sent_dt).days
                if days_since >= EXPIRY_DAYS:
                    pc["status"] = "expired"
                    pc["award_status"] = "expired"
                    pc["closed_at"] = now.isoformat()
                    pc["closed_reason"] = f"No award found after {days_since} days"
                    results["expired"] += 1
                    log.info("PC %s expired: %d days since sent, no award found",
                             pc.get("pc_number", pcid), days_since)
                    continue
            except Exception:
                pass
        
        # Rate limit
        if checked_this_run >= MAX_CHECKS_PER_RUN:
            results["skipped"] += 1
            continue
        
        # Query SCPRS
        award = check_pc_award(pc)
        pc["last_scprs_check"] = now.isoformat()
        pc["scprs_check_count"] = pc.get("scprs_check_count", 0) + 1
        checked_this_run += 1
        results["checked"] += 1
        
        if award:
            if award["outcome"] == "won":
                pc["status"] = "won"
                pc["award_status"] = "won"
                pc["closed_at"] = now.isoformat()
                pc["closed_reason"] = f"Reytech won PO {award.get('po_number','')}"
                results["won"] += 1
                log.info("🏆 PC %s WON! PO %s", pc.get("pc_number", pcid), award.get("po_number"))
                
                # Push notification
                try:
                    from src.api.dashboard import _push_notification
                    _push_notification("bell",
                        f"🏆 WON: {pc.get('pc_number','')} — PO {award.get('po_number','')}",
                        "success")
                except Exception:
                    pass
                
            elif award["outcome"] == "lost":
                pc["status"] = "lost"
                pc["award_status"] = "lost"
                pc["competitor_name"] = award.get("supplier", "")
                pc["competitor_price"] = award.get("total") or award.get("price") or 0
                pc["competitor_po"] = award.get("po_number", "")
                pc["closed_at"] = now.isoformat()
                pc["closed_reason"] = f"Lost to {award['supplier']} — PO {award.get('po_number','')}"
                results["lost"] += 1
                
                # Log competitor intel
                our_total = 0
                try:
                    for it in pc.get("items", []):
                        p = it.get("pricing", {})
                        our_total += (p.get("recommended_price") or 0) * it.get("qty", 1)
                except Exception:
                    pass
                log_competitor(pc, award, our_total)
                
                log.info("❌ PC %s LOST to %s (PO %s, $%.2f vs our $%.2f)",
                         pc.get("pc_number", pcid), award["supplier"],
                         award.get("po_number"), pc.get("competitor_price", 0), our_total)
                
                # Push notification
                try:
                    from src.api.dashboard import _push_notification
                    _push_notification("bell",
                        f"❌ Lost: {pc.get('pc_number','')} to {award['supplier']}",
                        "warn")
                except Exception:
                    pass
        else:
            # No match yet — keep checking
            pc["status"] = "pending_award"
    
    # Save updated PCs
    try:
        with open(pc_path, "w") as f:
            json.dump(pcs, f, indent=2, default=str)
    except Exception as e:
        results["errors"].append(f"Save failed: {e}")
    
    log.info("Award monitor: checked=%d won=%d lost=%d expired=%d skipped=%d",
             results["checked"], results["won"], results["lost"],
             results["expired"], results["skipped"])
    
    return results


# ── Background Thread ────────────────────────────────────────────────────────

def _monitor_loop():
    """Background loop — runs every CHECK_INTERVAL_HOURS."""
    global _monitor_running
    _monitor_running = True
    log.info("Award monitor started (checks every %dh, SCPRS every %d biz days, expires at %dd)",
             CHECK_INTERVAL_HOURS, SCPRS_CHECK_INTERVAL_DAYS, EXPIRY_DAYS)
    
    while _monitor_running:
        try:
            run_award_check()
        except Exception as e:
            log.error("Award monitor error: %s", e, exc_info=True)
        
        time.sleep(CHECK_INTERVAL_HOURS * 3600)


def start_monitor():
    """Start the award monitor background thread."""
    global _monitor_thread
    if _monitor_thread and _monitor_thread.is_alive():
        return
    _monitor_thread = threading.Thread(target=_monitor_loop, daemon=True, name="award-monitor")
    _monitor_thread.start()


def stop_monitor():
    """Stop the monitor (for graceful shutdown)."""
    global _monitor_running
    _monitor_running = False


def get_monitor_status() -> dict:
    """Get monitor status for health checks."""
    return {
        "running": _monitor_thread is not None and _monitor_thread.is_alive() if _monitor_thread else False,
        "check_interval_hours": CHECK_INTERVAL_HOURS,
        "scprs_interval_bdays": SCPRS_CHECK_INTERVAL_DAYS,
        "expiry_days": EXPIRY_DAYS,
    }
