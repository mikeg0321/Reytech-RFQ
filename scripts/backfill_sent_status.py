"""Backfill sent-status for PCs/RFQs stuck mid-workflow.

Audit item A (session_audit 2026-04-22): the triage queue surfaces records
Mike quoted months ago because their status never flipped to "sent". Root
causes:
  * Quote was emailed out-of-band (manual Gmail UI, merge-script workflows
    that do not call the in-app send route — see audit AA).
  * Historical records pre-date current send-path status writes.

This script finds "likely-sent" records still stuck in active statuses and
flips them to sent. A record is "likely sent" when it has BOTH:
  1. A reytech quote number assigned (`reytech_quote_number` non-empty).
  2. Evidence of a generated package — either a `reytech_quote_pdf` /
     `output_pdf` path, or `status` already in a post-generate value
     ("generated", "quoted", "completed").

`sent_at` is approximated from the most recent timestamp available on the
record (generated_at → updated_at → created_at). A `backfilled_sent_at`
flag records that this flip was synthetic so downstream analytics can
exclude it from "real send time" math.

Safe defaults:
  * Dry-run by default. Prints a report, writes nothing.
  * `--apply` required to commit changes.
  * Exits 0 on empty DB (no-op), 1 on runtime error, 2 on missing DB.
  * Never deletes. Never re-fires email/on_sent hooks — those should have
    fired at original send time; replaying risks double-sending.

Usage:
  # Dry-run (default):
  python scripts/backfill_sent_status.py
  # Apply:
  python scripts/backfill_sent_status.py --apply
  # Limit to PCs or RFQs:
  python scripts/backfill_sent_status.py --only pc
  python scripts/backfill_sent_status.py --only rfq
  # Override DB path (CI / test):
  python scripts/backfill_sent_status.py --db /tmp/test.db
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Iterable


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_PC_ACTIVE = {"new", "draft", "parsed", "parse_error", "priced", "ready",
              "auto_drafted", "quoted", "generated", "enriching", "enriched",
              "completed", "converted"}
_RFQ_ACTIVE = {"new", "received", "parsed", "draft", "ready", "priced",
               "generated", "quoted"}
_TERMINAL = {"sent", "won", "lost", "dismissed", "archived", "expired",
             "no_response", "not_responding", "duplicate", "reclassified",
             "pending_award"}


def _resolve_db_path(override: str | None) -> str | None:
    if override:
        return override
    for p in ("/data/reytech.db", "data/reytech.db"):
        if os.path.exists(p):
            return p
    return None


def _load_pc_blob(row: sqlite3.Row) -> dict:
    raw = row["data_json"] or "{}"
    try:
        d = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (ValueError, TypeError):
        d = {}
    if not isinstance(d, dict):
        d = {}
    return d


def _pc_likely_sent(status: str, blob: dict) -> bool:
    if status in _TERMINAL:
        return False
    if status not in _PC_ACTIVE:
        return False
    qn = (blob.get("reytech_quote_number") or "").strip()
    if not qn:
        return False
    has_pdf = bool(blob.get("reytech_quote_pdf") or blob.get("output_pdf"))
    if has_pdf:
        return True
    return status in ("generated", "quoted", "completed")


def _rfq_likely_sent(status: str, blob: dict) -> bool:
    if status in _TERMINAL:
        return False
    if status not in _RFQ_ACTIVE:
        return False
    qn = (blob.get("reytech_quote_number") or "").strip()
    if not qn:
        return False
    has_pdf = bool(blob.get("reytech_quote_pdf") or blob.get("output_pdf")
                   or blob.get("package_pdf"))
    if has_pdf:
        return True
    return status in ("generated", "quoted")


def _best_sent_at(blob: dict) -> str:
    for k in ("generated_at", "package_generated_at", "updated_at",
              "received_at", "created_at"):
        v = blob.get(k)
        if v:
            return str(v)
    return _utc_iso()


def _scan(conn: sqlite3.Connection, kind: str) -> list[dict]:
    conn.row_factory = sqlite3.Row
    candidates: list[dict] = []
    if kind == "pc":
        rows = conn.execute(
            "SELECT id, status, data_json, pc_number, institution, created_at "
            "FROM price_checks"
        ).fetchall()
        for row in rows:
            blob = _load_pc_blob(row)
            status = (row["status"] or "").strip()
            if not _pc_likely_sent(status, blob):
                continue
            candidates.append({
                "kind": "pc",
                "id": row["id"],
                "status": status,
                "pc_number": row["pc_number"] or blob.get("pc_number", ""),
                "institution": row["institution"] or blob.get("institution", ""),
                "quote_number": blob.get("reytech_quote_number", ""),
                "created_at": row["created_at"] or blob.get("created_at", ""),
                "blob": blob,
            })
    elif kind == "rfq":
        rows = conn.execute(
            "SELECT id, status, data_json, rfq_number, institution, received_at "
            "FROM rfqs"
        ).fetchall()
        for row in rows:
            blob = _load_pc_blob(row)
            status = (row["status"] or "").strip()
            if not _rfq_likely_sent(status, blob):
                continue
            candidates.append({
                "kind": "rfq",
                "id": row["id"],
                "status": status,
                "pc_number": row["rfq_number"] or blob.get("rfq_number", ""),
                "institution": row["institution"] or blob.get("institution", ""),
                "quote_number": blob.get("reytech_quote_number", ""),
                "created_at": row["received_at"] or blob.get("received_at", ""),
                "blob": blob,
            })
    return candidates


def _apply_one(conn: sqlite3.Connection, c: dict) -> None:
    blob = dict(c["blob"])
    now_iso = _utc_iso()
    blob["status"] = "sent"
    blob["sent_at"] = _best_sent_at(c["blob"])
    blob["backfilled_sent_at"] = now_iso
    blob["backfill_prior_status"] = c["status"]
    table = "price_checks" if c["kind"] == "pc" else "rfqs"
    conn.execute(
        f"UPDATE {table} SET status=?, data_json=?, updated_at=? WHERE id=?",
        ("sent", json.dumps(blob, default=str), now_iso, c["id"])
    )


def _fmt_row(c: dict) -> str:
    return (f"  {c['kind'].upper():3s} {c['id'][:12]:12s} "
            f"num={c['pc_number'] or '—':14s} "
            f"qn={c['quote_number'] or '—':12s} "
            f"status={c['status']:12s} "
            f"institution={(c['institution'] or '—')[:30]}")


def run(db_path: str | None, *, apply: bool = False,
        only: str | None = None) -> int:
    resolved = _resolve_db_path(db_path)
    if not resolved:
        print("ERROR: no reytech.db found (tried /data/reytech.db, data/reytech.db)",
              file=sys.stderr)
        return 2
    if not os.path.exists(resolved):
        print(f"ERROR: DB not found: {resolved}", file=sys.stderr)
        return 2

    kinds: Iterable[str] = ("pc", "rfq")
    if only:
        if only not in ("pc", "rfq"):
            print(f"ERROR: --only must be 'pc' or 'rfq', got {only!r}",
                  file=sys.stderr)
            return 1
        kinds = (only,)

    print(f"{'APPLY' if apply else 'DRY-RUN'} backfill_sent_status on {resolved}")
    conn = sqlite3.connect(resolved)
    try:
        total = 0
        for kind in kinds:
            try:
                candidates = _scan(conn, kind)
            except sqlite3.OperationalError as e:
                print(f"  skip {kind}: {e}")
                continue
            print(f"\n[{kind.upper()}] {len(candidates)} stuck record(s):")
            for c in candidates:
                print(_fmt_row(c))
            if apply and candidates:
                for c in candidates:
                    _apply_one(conn, c)
            total += len(candidates)
        if apply:
            conn.commit()
            print(f"\n✓ Applied: flipped {total} record(s) to status=sent.")
        else:
            print(f"\nDry-run: would flip {total} record(s). Pass --apply to commit.")
        return 0
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--apply", action="store_true",
                   help="Commit changes. Default is dry-run.")
    p.add_argument("--only", choices=("pc", "rfq"),
                   help="Limit scan to PCs or RFQs only.")
    p.add_argument("--db", default=None,
                   help="Override DB path (default auto-detects /data or ./data).")
    args = p.parse_args(argv)
    return run(args.db, apply=args.apply, only=args.only)


if __name__ == "__main__":
    sys.exit(main())
