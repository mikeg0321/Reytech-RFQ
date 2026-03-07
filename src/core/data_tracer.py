"""
data_tracer.py — End-to-end document lineage tracing.

Traces any document (RFQ, quote, order, price check) through the full pipeline:
  Email → RFQ → Price Check → Quote → Order → Invoice → Payment

Usage:
    from src.core.data_tracer import trace_document, trace_quote_lineage
    lineage = trace_document("R26Q14")  # by quote number
    lineage = trace_document("rfq-abc123", doc_type="rfq")
"""
import json
import logging
from datetime import datetime

log = logging.getLogger("reytech.tracer")


def trace_document(doc_id: str, doc_type: str = "auto") -> dict:
    """
    Trace a document through the entire pipeline.
    
    Args:
        doc_id: Quote number, RFQ id, order id, or price check id
        doc_type: "quote", "rfq", "order", "price_check", or "auto" to detect
    
    Returns:
        Lineage dict with all related documents and timeline
    """
    lineage = {
        "query": {"doc_id": doc_id, "doc_type": doc_type},
        "traced_at": datetime.now().isoformat(),
        "pipeline": {},
        "timeline": [],
        "warnings": [],
    }
    
    try:
        # Auto-detect type
        if doc_type == "auto":
            doc_type = _detect_type(doc_id)
            lineage["query"]["detected_type"] = doc_type
        
        # Trace from the detected starting point
        if doc_type == "quote":
            _trace_from_quote(doc_id, lineage)
        elif doc_type == "rfq":
            _trace_from_rfq(doc_id, lineage)
        elif doc_type == "order":
            _trace_from_order(doc_id, lineage)
        elif doc_type == "price_check":
            _trace_from_price_check(doc_id, lineage)
        else:
            # Try everything
            _trace_from_quote(doc_id, lineage)
            if not lineage["pipeline"]:
                _trace_from_rfq(doc_id, lineage)
            if not lineage["pipeline"]:
                _trace_from_order(doc_id, lineage)
        
        # Sort timeline
        lineage["timeline"].sort(key=lambda e: e.get("timestamp", ""))
        lineage["ok"] = True
        lineage["stages_found"] = len(lineage["pipeline"])
        
    except Exception as e:
        lineage["ok"] = False
        lineage["error"] = str(e)[:500]
        log.error("Trace failed for %s: %s", doc_id, str(e)[:200])
    
    return lineage


def _detect_type(doc_id: str) -> str:
    """Auto-detect document type from ID format."""
    if not doc_id:
        return "unknown"
    doc_id_lower = doc_id.lower()
    if doc_id.startswith("R") and "Q" in doc_id[:6]:
        return "quote"  # R26Q14 format
    if doc_id_lower.startswith("rfq-") or doc_id_lower.startswith("rfq_"):
        return "rfq"
    if doc_id_lower.startswith("ord-") or doc_id_lower.startswith("order-"):
        return "order"
    if doc_id_lower.startswith("pc-") or doc_id_lower.startswith("pc_"):
        return "price_check"
    return "auto"


def _trace_from_quote(quote_id: str, lineage: dict):
    """Trace starting from a quote number."""
    from src.core.db import get_db
    
    with get_db() as conn:
        # Find the quote
        quote = conn.execute(
            "SELECT * FROM quotes WHERE quote_number = ? OR id = ?",
            (quote_id, quote_id)
        ).fetchone()
        
        if quote:
            q = dict(quote)
            lineage["pipeline"]["quote"] = {
                "id": q.get("id"),
                "quote_number": q.get("quote_number"),
                "status": q.get("status"),
                "total": q.get("total"),
                "agency": q.get("agency"),
                "institution": q.get("institution"),
                "created_at": q.get("created_at"),
                "items_count": q.get("items_count"),
            }
            _add_event(lineage, "quote_created", q.get("created_at"),
                       f"Quote {q.get('quote_number')} created — ${q.get('total', 0):.2f}")
            
            if q.get("status") == "won":
                _add_event(lineage, "quote_won", q.get("updated_at"),
                           f"Quote won — PO: {q.get('po_number', 'unknown')}")
            
            # Trace to source RFQ
            source_rfq = q.get("source_rfq_id") or q.get("rfq_id")
            if source_rfq:
                _trace_rfq_by_id(source_rfq, lineage, conn)
            
            # Trace to source price check
            source_pc = q.get("source_pc_id")
            if source_pc:
                _trace_pc_by_id(source_pc, lineage, conn)
            
            # Trace forward to orders
            _trace_orders_for_quote(q.get("quote_number"), lineage, conn)
            
            # PDF generation history
            _trace_pdf_history(q.get("quote_number"), "quote", lineage, conn)


def _trace_from_rfq(rfq_id: str, lineage: dict):
    """Trace starting from an RFQ."""
    from src.core.db import get_db
    
    with get_db() as conn:
        rfq = conn.execute(
            "SELECT * FROM rfqs WHERE id = ? OR rfq_number = ?",
            (rfq_id, rfq_id)
        ).fetchone()
        
        if rfq:
            r = dict(rfq)
            lineage["pipeline"]["rfq"] = {
                "id": r.get("id"),
                "rfq_number": r.get("rfq_number"),
                "status": r.get("status"),
                "source": r.get("source"),
                "agency": r.get("agency"),
                "institution": r.get("institution"),
                "received_at": r.get("received_at"),
            }
            _add_event(lineage, "rfq_received", r.get("received_at"),
                       f"RFQ received: {r.get('rfq_number')} from {r.get('source', 'unknown')}")
            
            # Trace forward to quotes
            quotes = conn.execute(
                "SELECT * FROM quotes WHERE source_rfq_id = ? OR rfq_id = ?",
                (rfq_id, rfq_id)
            ).fetchall()
            for q in quotes:
                qd = dict(q)
                lineage["pipeline"]["quote"] = {
                    "quote_number": qd.get("quote_number"),
                    "status": qd.get("status"),
                    "total": qd.get("total"),
                    "created_at": qd.get("created_at"),
                }
                _trace_orders_for_quote(qd.get("quote_number"), lineage, conn)


def _trace_from_order(order_id: str, lineage: dict):
    """Trace starting from an order."""
    from src.core.db import get_db
    
    with get_db() as conn:
        order = conn.execute(
            "SELECT * FROM orders WHERE id = ? OR po_number = ?",
            (order_id, order_id)
        ).fetchone()
        
        if order:
            o = dict(order)
            lineage["pipeline"]["order"] = {
                "id": o.get("id"),
                "po_number": o.get("po_number"),
                "status": o.get("status"),
                "total": o.get("total"),
                "customer": o.get("customer"),
                "created_at": o.get("created_at"),
            }
            _add_event(lineage, "order_created", o.get("created_at"),
                       f"Order {o.get('po_number')} — ${o.get('total', 0):.2f}")
            
            # Trace back to quote
            qn = o.get("quote_number")
            if qn:
                _trace_from_quote(qn, lineage)
            
            # Order lifecycle events
            events = conn.execute(
                "SELECT * FROM order_events WHERE order_id = ? ORDER BY created_at",
                (o.get("id"),)
            ).fetchall()
            for ev in events:
                evd = dict(ev)
                _add_event(lineage, f"order_{evd.get('event_type', 'event')}",
                           evd.get("created_at"),
                           evd.get("details", evd.get("event_type", "")))
            
            # Revenue log
            rev = conn.execute(
                "SELECT * FROM revenue_log WHERE order_id = ? OR reference = ?",
                (o.get("id"), o.get("po_number"))
            ).fetchall()
            for r in rev:
                rd = dict(r)
                lineage.setdefault("pipeline", {})["revenue"] = {
                    "amount": rd.get("amount"),
                    "category": rd.get("category"),
                    "recorded_at": rd.get("created_at"),
                }


def _trace_from_price_check(pc_id: str, lineage: dict):
    """Trace starting from a price check."""
    from src.core.db import get_db
    
    with get_db() as conn:
        pc = conn.execute(
            "SELECT * FROM price_checks WHERE id = ?", (pc_id,)
        ).fetchone()
        
        if pc:
            p = dict(pc)
            lineage["pipeline"]["price_check"] = {
                "id": p.get("id"),
                "pc_number": p.get("pc_number"),
                "institution": p.get("institution"),
                "status": p.get("status"),
                "created_at": p.get("created_at"),
            }
            _add_event(lineage, "price_check_received", p.get("created_at"),
                       f"Price check: {p.get('pc_number')} for {p.get('institution')}")
            
            # Forward to quotes
            quotes = conn.execute(
                "SELECT * FROM quotes WHERE source_pc_id = ?", (pc_id,)
            ).fetchall()
            for q in quotes:
                qd = dict(q)
                _trace_from_quote(qd.get("quote_number") or qd.get("id"), lineage)


def _trace_rfq_by_id(rfq_id: str, lineage: dict, conn):
    """Helper: trace an RFQ into lineage."""
    rfq = conn.execute("SELECT * FROM rfqs WHERE id = ?", (rfq_id,)).fetchone()
    if rfq:
        r = dict(rfq)
        lineage["pipeline"]["rfq"] = {
            "id": r.get("id"),
            "rfq_number": r.get("rfq_number"),
            "status": r.get("status"),
            "source": r.get("source"),
            "received_at": r.get("received_at"),
        }
        _add_event(lineage, "rfq_received", r.get("received_at"),
                   f"RFQ: {r.get('rfq_number')}")


def _trace_pc_by_id(pc_id: str, lineage: dict, conn):
    """Helper: trace a price check into lineage."""
    pc = conn.execute("SELECT * FROM price_checks WHERE id = ?", (pc_id,)).fetchone()
    if pc:
        p = dict(pc)
        lineage["pipeline"]["price_check"] = {
            "id": p.get("id"),
            "pc_number": p.get("pc_number"),
            "institution": p.get("institution"),
            "created_at": p.get("created_at"),
        }
        _add_event(lineage, "price_check", p.get("created_at"),
                   f"Source price check: {p.get('pc_number')}")


def _trace_orders_for_quote(quote_number: str, lineage: dict, conn):
    """Helper: find orders linked to a quote."""
    if not quote_number:
        return
    orders = conn.execute(
        "SELECT * FROM orders WHERE quote_number = ?", (quote_number,)
    ).fetchall()
    for o in orders:
        od = dict(o)
        lineage["pipeline"]["order"] = {
            "id": od.get("id"),
            "po_number": od.get("po_number"),
            "status": od.get("status"),
            "total": od.get("total"),
            "customer": od.get("customer"),
            "created_at": od.get("created_at"),
        }
        _add_event(lineage, "order_created", od.get("created_at"),
                   f"Order {od.get('po_number')} — ${od.get('total', 0):.2f}")


def _trace_pdf_history(doc_id: str, template_type: str, lineage: dict, conn):
    """Helper: add PDF generation events."""
    try:
        rows = conn.execute(
            "SELECT * FROM pdf_generation_log WHERE document_id = ? ORDER BY generated_at",
            (doc_id,)
        ).fetchall()
        for r in rows:
            rd = dict(r)
            _add_event(lineage, "pdf_generated", rd.get("generated_at"),
                       f"PDF v{rd.get('template_version')} generated")
            lineage.setdefault("pipeline", {}).setdefault("pdfs", []).append({
                "template_version": rd.get("template_version"),
                "generated_at": rd.get("generated_at"),
                "file_path": rd.get("file_path"),
            })
    except Exception:
        pass  # Table may not exist yet


def _add_event(lineage: dict, event_type: str, timestamp: str, detail: str):
    """Add a timeline event."""
    lineage["timeline"].append({
        "event": event_type,
        "timestamp": timestamp or "",
        "detail": detail,
    })


def get_pipeline_stats() -> dict:
    """Get high-level pipeline statistics for dashboard."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            stats = {}
            for table, label in [
                ("rfqs", "rfqs"), ("price_checks", "price_checks"),
                ("quotes", "quotes"), ("orders", "orders"),
            ]:
                try:
                    row = conn.execute("""
                        SELECT COUNT(*) as total,
                               COUNT(CASE WHEN status IN ('won','completed','shipped','delivered') THEN 1 END) as completed,
                               COUNT(CASE WHEN status IN ('new','draft','pending','sent') THEN 1 END) as active
                        FROM " + re.sub(r"[^a-zA-Z0-9_]", "", table) + "
                    """).fetchone()
                    stats[label] = {
                        "total": row["total"],
                        "completed": row["completed"],
                        "active": row["active"],
                    }
                except Exception:
                    stats[label] = {"total": 0, "error": "table missing"}
            
            # Conversion rates
            q_total = stats.get("quotes", {}).get("total", 0)
            q_won = stats.get("quotes", {}).get("completed", 0)
            stats["conversion"] = {
                "quote_win_rate": round(q_won / q_total * 100, 1) if q_total > 0 else 0,
                "rfq_to_quote": "N/A",  # Would need join
            }
            return {"ok": True, **stats}
    except Exception as e:
        return {"ok": False, "error": str(e)}
