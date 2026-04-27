"""training_corpus.py — Phase 1.6 PR3g.

Bootstrap the per-buyer profile training base from Reytech's own
shipping history. Every won order is a labeled training example:
  - the buyer's incoming blank PDF (input)
  - the contract from the email (intent)
  - the package we shipped (output)
  - the PO number + date (label: 'won')

Walks orders for the last 365 days, joins to quotes/price_checks +
their incoming attachments + the shipped po_pdf_path, and writes one
training pair per won PO to `data/training_pairs/<quote_id>/`.

Reusable from two callers:
  1. scripts/build_training_corpus.py — one-shot bootstrap
  2. post_send_pipeline.on_quote_sent (PR3h) — continuous capture

Idempotent: if the manifest exists for a quote_id, skip unless
--force passed.
"""

import json
import logging
import os
import shutil
from datetime import datetime, timedelta
from typing import Optional

log = logging.getLogger("reytech.training_corpus")


def _data_dir() -> str:
    """Resolve DATA_DIR from the dashboard module (test-isolated)."""
    try:
        from src.api import dashboard
        return getattr(dashboard, "DATA_DIR", "data")
    except Exception:
        return "data"


def _training_root() -> str:
    return os.path.join(_data_dir(), "training_pairs")


# ─── Public API ────────────────────────────────────────────────────────────

def build_training_pair(quote_id: str, quote_type: str,
                        force: bool = False) -> dict:
    """Build a single training-pair manifest + copy artifact files.

    Args:
        quote_id: PC or RFQ id (or order's quote_number)
        quote_type: "pc" | "rfq" | "order" (auto-resolves)
        force: rewrite even if manifest exists

    Returns:
        dict with keys {ok, status, manifest_path, ...}.
        status ∈ {"created", "skipped_exists", "skipped_no_data",
                  "skipped_no_artifacts", "error"}.
    """
    qt = (quote_type or "").lower()
    try:
        record = _load_record(quote_id, qt)
        if not record:
            return {"ok": False, "status": "skipped_no_data",
                    "reason": f"no {qt} row found for {quote_id}"}

        out_dir = os.path.join(_training_root(), record["quote_id"])
        manifest_path = os.path.join(out_dir, "manifest.json")

        if os.path.exists(manifest_path) and not force:
            return {"ok": True, "status": "skipped_exists",
                    "manifest_path": manifest_path}

        os.makedirs(out_dir, exist_ok=True)

        # Copy artifacts
        incoming_files = _copy_incoming_blanks(record, out_dir)
        outgoing_files = _copy_outgoing_fill(record, out_dir)

        if not incoming_files and not outgoing_files:
            # Nothing to learn from — clean up empty dir
            try:
                os.rmdir(out_dir)
            except OSError:
                pass
            return {"ok": False, "status": "skipped_no_artifacts",
                    "reason": "no incoming or outgoing PDFs found"}

        manifest = {
            "quote_id": record["quote_id"],
            "quote_type": record["quote_type"],
            "quote_number": record.get("quote_number", ""),
            "agency": record.get("agency", ""),
            "institution": record.get("institution", ""),
            "po_number": record.get("po_number", ""),
            "po_date": record.get("po_date", ""),
            "won": record.get("won", False),
            "contract": record.get("contract", {}),
            "incoming_blanks": incoming_files,
            "outgoing_fills": outgoing_files,
            "captured_at": datetime.utcnow().isoformat() + "Z",
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        return {"ok": True, "status": "created",
                "manifest_path": manifest_path,
                "incoming_count": len(incoming_files),
                "outgoing_count": len(outgoing_files)}
    except Exception as e:
        log.error("build_training_pair(%s, %s) error: %s", quote_id, qt, e,
                  exc_info=True)
        return {"ok": False, "status": "error", "error": str(e)}


def bootstrap_from_orders(days: int = 365, force: bool = False,
                          limit: Optional[int] = None) -> dict:
    """Walk orders table for last N days, build a training pair per row.

    Args:
        days: lookback window
        force: rewrite existing manifests
        limit: cap orders processed (for dev iteration)

    Returns:
        Coverage report:
          {
            "scanned": int,
            "created": int,
            "skipped_exists": int,
            "skipped_no_data": int,
            "skipped_no_artifacts": int,
            "errors": int,
            "by_agency": {agency_key: {created, won_count, ...}},
            "results_sample": [first 20 results for debugging],
          }
    """
    from src.core.db import get_db
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

    summary = {
        "scanned": 0, "created": 0, "skipped_exists": 0,
        "skipped_no_data": 0, "skipped_no_artifacts": 0,
        "errors": 0, "by_agency": {}, "results_sample": [],
        "lookback_days": days, "cutoff_date": cutoff,
    }

    try:
        with get_db() as conn:
            sql = ("SELECT id, quote_number, agency, institution, po_number, "
                   "po_date FROM orders "
                   "WHERE COALESCE(po_date,'') >= ? "
                   "ORDER BY po_date DESC")
            params = [cutoff]
            if limit:
                sql += " LIMIT ?"
                params.append(limit)
            rows = conn.execute(sql, params).fetchall()
    except Exception as e:
        log.error("bootstrap_from_orders DB error: %s", e)
        return {"error": str(e), **summary}

    for row in rows:
        summary["scanned"] += 1
        order = dict(row)
        agency = order.get("agency", "") or "_unknown"
        summary["by_agency"].setdefault(agency, {
            "scanned": 0, "created": 0, "skipped": 0,
        })
        summary["by_agency"][agency]["scanned"] += 1

        # Resolve to a quote: prefer matching PC by quote_number then RFQ
        target_id, target_type = _find_quote_for_order(order)
        if not target_id:
            summary["skipped_no_data"] += 1
            summary["by_agency"][agency]["skipped"] += 1
            continue

        result = build_training_pair(target_id, target_type, force=force)
        status = result.get("status", "error")
        if status == "created":
            summary["created"] += 1
            summary["by_agency"][agency]["created"] += 1
        elif status == "skipped_exists":
            summary["skipped_exists"] += 1
        elif status == "skipped_no_data":
            summary["skipped_no_data"] += 1
        elif status == "skipped_no_artifacts":
            summary["skipped_no_artifacts"] += 1
        else:
            summary["errors"] += 1

        if len(summary["results_sample"]) < 20:
            summary["results_sample"].append({
                "po_number": order.get("po_number", ""),
                "quote_id": target_id,
                "agency": agency,
                "status": status,
            })

    return summary


def coverage_report() -> dict:
    """Return per-buyer training-pair coverage from the on-disk corpus."""
    root = _training_root()
    by_agency = {}
    total = 0
    if os.path.isdir(root):
        for q_dir in os.listdir(root):
            mp = os.path.join(root, q_dir, "manifest.json")
            if not os.path.isfile(mp):
                continue
            try:
                with open(mp, "r", encoding="utf-8") as f:
                    m = json.load(f)
            except Exception:
                continue
            agency = m.get("agency", "") or "_unknown"
            by_agency.setdefault(agency, {
                "pairs": 0, "won": 0,
                "with_incoming": 0, "with_outgoing": 0,
            })
            by_agency[agency]["pairs"] += 1
            if m.get("won"):
                by_agency[agency]["won"] += 1
            if m.get("incoming_blanks"):
                by_agency[agency]["with_incoming"] += 1
            if m.get("outgoing_fills"):
                by_agency[agency]["with_outgoing"] += 1
            total += 1
    return {"total_pairs": total, "by_agency": by_agency,
            "training_root": root}


# ─── Internals ─────────────────────────────────────────────────────────────

def _load_record(quote_id: str, quote_type: str) -> Optional[dict]:
    """Load + denormalize a quote record into the training-pair shape."""
    from src.core.db import get_db
    try:
        with get_db() as conn:
            if quote_type == "pc":
                row = conn.execute(
                    "SELECT * FROM price_checks WHERE id = ?", (quote_id,)
                ).fetchone()
            elif quote_type == "rfq":
                row = conn.execute(
                    "SELECT * FROM rfqs WHERE id = ?", (quote_id,)
                ).fetchone()
            elif quote_type == "order":
                # Caller passed an order id — find linked quote first
                ord_row = conn.execute(
                    "SELECT * FROM orders WHERE id = ? OR quote_number = ?",
                    (quote_id, quote_id)
                ).fetchone()
                if not ord_row:
                    return None
                ord_dict = dict(ord_row)
                qid, qt = _find_quote_for_order(ord_dict)
                if qid:
                    return _load_record(qid, qt)
                return None
            else:
                return None
            if not row:
                return None
            d = dict(row)
    except Exception as e:
        log.debug("_load_record(%s, %s) error: %s", quote_id, quote_type, e)
        return None

    contract = {}
    raw = d.get("requirements_json") or ""
    if raw:
        try:
            contract = json.loads(raw)
        except Exception:
            pass

    # Look up the order for win/loss + po info
    po_info = _find_order_for_quote(d)

    return {
        "quote_id": d.get("id", ""),
        "quote_type": quote_type,
        "quote_number": (d.get("reytech_quote_number") or
                         d.get("quote_number", "")),
        "agency": d.get("agency", ""),
        "institution": d.get("institution", ""),
        "po_number": po_info.get("po_number", ""),
        "po_date": po_info.get("po_date", ""),
        "po_pdf_path": po_info.get("po_pdf_path", ""),
        "source_file": d.get("source_file", ""),  # PC path
        "contract": contract,
        "won": bool(po_info.get("po_number")),
    }


def _find_quote_for_order(order: dict) -> tuple:
    """Given an orders row, find the (quote_id, quote_type) it relates to.

    Tries quote_number against pcs.reytech_quote_number, then
    rfqs.reytech_quote_number, then exact pcs.id / rfqs.id. Tolerant
    of missing columns (test schemas may lack reytech_quote_number).
    """
    qn = (order.get("quote_number") or "").strip()
    if not qn:
        return ("", "")
    from src.core.db import get_db
    try:
        with get_db() as conn:
            pc_cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(price_checks)")}
            for col in ("reytech_quote_number", "quote_number", "id"):
                if col not in pc_cols:
                    continue
                try:
                    row = conn.execute(
                        f"SELECT id FROM price_checks WHERE {col} = ? LIMIT 1",
                        (qn,)
                    ).fetchone()
                    if row:
                        return (row["id"], "pc")
                except Exception as e:
                    log.debug("pc lookup col=%s failed: %s", col, e)
            rfq_cols = {r[1] for r in conn.execute(
                "PRAGMA table_info(rfqs)")}
            for col in ("reytech_quote_number", "rfq_number", "id"):
                if col not in rfq_cols:
                    continue
                try:
                    row = conn.execute(
                        f"SELECT id FROM rfqs WHERE {col} = ? LIMIT 1",
                        (qn,)
                    ).fetchone()
                    if row:
                        return (row["id"], "rfq")
                except Exception as e:
                    log.debug("rfq lookup col=%s failed: %s", col, e)
    except Exception as e:
        log.debug("_find_quote_for_order error: %s", e)
    return ("", "")


def _find_order_for_quote(quote_row: dict) -> dict:
    """Reverse lookup: given a PC/RFQ row, find the matching order if any."""
    qn = (quote_row.get("reytech_quote_number") or
          quote_row.get("quote_number") or "")
    if not qn:
        return {}
    from src.core.db import get_db
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT po_number, po_date, po_pdf_path FROM orders "
                "WHERE quote_number = ? ORDER BY po_date DESC LIMIT 1",
                (qn,)
            ).fetchone()
            return dict(row) if row else {}
    except Exception:
        return {}


def _copy_incoming_blanks(record: dict, out_dir: str) -> list:
    """Copy buyer-supplied blank PDFs into the training-pair dir."""
    files = []
    incoming_dir = os.path.join(out_dir, "incoming")
    qid = record["quote_id"]
    qtype = record["quote_type"]

    # PC source_file (single)
    src = record.get("source_file", "") or ""
    if src and os.path.isfile(src):
        os.makedirs(incoming_dir, exist_ok=True)
        dst = os.path.join(incoming_dir, os.path.basename(src))
        if not os.path.exists(dst):
            try:
                shutil.copy2(src, dst)
            except OSError as e:
                log.debug("source_file copy failed: %s", e)
        files.append({"filename": os.path.basename(src),
                      "stored_at": dst, "source": "source_file"})

    # RFQ rfq_files BLOBs
    if qtype == "rfq":
        try:
            from src.core.db import get_db
            with get_db() as conn:
                rows = conn.execute(
                    "SELECT id, filename, data, file_size, file_type "
                    "FROM rfq_files WHERE rfq_id = ? AND category = 'template'",
                    (qid,)
                ).fetchall()
                for r in rows:
                    name = r["filename"] or f"file_{r['id']}.pdf"
                    if not name.lower().endswith(".pdf"):
                        continue
                    os.makedirs(incoming_dir, exist_ok=True)
                    dst = os.path.join(incoming_dir, name)
                    if not os.path.exists(dst) and r["data"]:
                        try:
                            with open(dst, "wb") as f:
                                f.write(r["data"])
                        except OSError as e:
                            log.debug("rfq_files write failed: %s", e)
                            continue
                    files.append({"filename": name, "stored_at": dst,
                                  "source": "rfq_files", "file_id": r["id"]})
        except Exception as e:
            log.debug("rfq_files scan failed: %s", e)

    return files


def _copy_outgoing_fill(record: dict, out_dir: str) -> list:
    """Copy the shipped package PDF if available."""
    files = []
    out_pdf = record.get("po_pdf_path", "") or ""
    if out_pdf and os.path.isfile(out_pdf):
        outgoing_dir = os.path.join(out_dir, "outgoing")
        os.makedirs(outgoing_dir, exist_ok=True)
        dst = os.path.join(outgoing_dir, os.path.basename(out_pdf))
        if not os.path.exists(dst):
            try:
                shutil.copy2(out_pdf, dst)
            except OSError as e:
                log.debug("po_pdf_path copy failed: %s", e)
        files.append({"filename": os.path.basename(out_pdf),
                      "stored_at": dst, "source": "orders.po_pdf_path"})
    return files
