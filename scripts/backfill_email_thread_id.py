"""Backfill `email_thread_id` on historical PCs / RFQs.

Built 2026-05-01 (PR-B1). The ingest path (email_poller.py) now captures
the Gmail-internal threadId at receive time so the UI can deeplink to the
original buyer thread and the Send flow can reply-on-thread. Records
created BEFORE this change have `email_thread_id == ""`.

Strategy (cheapest path first):
  1. If the record stored `email_uid` (Gmail message ID), call
     `users.messages.get(id=email_uid, format=metadata)` once per record
     and read `threadId` from the response. This is exact and free.
  2. Otherwise, if `email_message_id` (RFC 2822 Message-ID) is set,
     search `rfc822msgid:<id>` and use the single-result threadId.
  3. Otherwise skip — operator will use the in-app "🔍 Locate Gmail
     thread" picker (`/api/rfq/<id>/locate-email`) on a per-record basis.
     Bulk auto-locate via subject/buyer is deliberately NOT done here:
     too easy to bind the wrong thread on ambiguous matches.

Safe defaults:
  * Dry-run by default. Prints a report, writes nothing.
  * `--apply` required to commit changes.
  * Limits Gmail API calls via `--max` (default 200) so a runaway scan
     can't burn quota.
  * Never overwrites an existing `email_thread_id` — only fills empties.

Usage:
  # Dry-run (default):
  python scripts/backfill_email_thread_id.py
  # Apply, capped at 200 records:
  python scripts/backfill_email_thread_id.py --apply
  # PCs only:
  python scripts/backfill_email_thread_id.py --only pc --apply
  # Override DB path:
  python scripts/backfill_email_thread_id.py --db /tmp/test.db
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Iterable, Optional

log = logging.getLogger("backfill_email_thread_id")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_db_path(override: Optional[str]) -> Optional[str]:
    if override:
        return override
    for p in ("/data/reytech.db", "data/reytech.db"):
        if os.path.exists(p):
            return p
    return None


def _load_blob(row: sqlite3.Row) -> dict:
    raw = row["data_json"] or "{}"
    try:
        d = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (ValueError, TypeError):
        d = {}
    if not isinstance(d, dict):
        d = {}
    return d


def _existing_thread_id(blob: dict, row: sqlite3.Row) -> str:
    # Prefer column value if present, else blob — column was added in PR-B1
    try:
        col_val = row["email_thread_id"]
    except (IndexError, KeyError):
        col_val = ""
    return (col_val or blob.get("email_thread_id") or "").strip()


def _fetch_thread_id(service, *, gmail_id: str = "", rfc822_id: str = "") -> str:
    """Return the Gmail threadId for a record, or "" if not findable."""
    from src.core.gmail_api import get_message_metadata, list_message_ids

    if gmail_id:
        try:
            meta = get_message_metadata(service, gmail_id)
            tid = (meta.get("thread_id") or "").strip()
            if tid:
                return tid
        except Exception as e:
            log.debug("messages.get failed for gmail_id=%s: %s", gmail_id, e)

    if rfc822_id:
        # Strip surrounding angle brackets — Gmail's rfc822msgid: search
        # accepts the bare id without <>.
        clean = rfc822_id.strip().strip("<>").strip()
        if clean:
            try:
                ids = list_message_ids(service, query=f"rfc822msgid:{clean}",
                                       max_results=2)
                if len(ids) == 1:
                    meta = get_message_metadata(service, ids[0])
                    return (meta.get("thread_id") or "").strip()
            except Exception as e:
                log.debug("rfc822msgid lookup failed for %s: %s", clean, e)
    return ""


def _scan(conn: sqlite3.Connection, kind: str) -> list[dict]:
    conn.row_factory = sqlite3.Row
    table = "price_checks" if kind == "pc" else "rfqs"
    # Pull email_thread_id column too — it's the new column added in PR-B1.
    # Older snapshots may lack it; SELECT * is safer for that case.
    try:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    except sqlite3.OperationalError as e:
        log.warning("table %s not readable: %s", table, e)
        return []

    out = []
    for row in rows:
        blob = _load_blob(row)
        if _existing_thread_id(blob, row):
            continue
        gmail_id = (blob.get("email_uid") or "").strip()
        rfc822 = (blob.get("email_message_id")
                  or blob.get("message_id") or "").strip()
        if not gmail_id and not rfc822:
            continue
        out.append({
            "kind": kind,
            "id": row["id"],
            "gmail_id": gmail_id,
            "rfc822_id": rfc822,
            "blob": blob,
        })
    return out


def _apply_one(conn: sqlite3.Connection, c: dict, thread_id: str) -> None:
    blob = dict(c["blob"])
    blob["email_thread_id"] = thread_id
    blob["email_thread_id_backfilled_at"] = _utc_iso()
    table = "price_checks" if c["kind"] == "pc" else "rfqs"
    # Update both the JSON blob and (if column exists) the dedicated column.
    # Try column update first; fall back gracefully on older schemas.
    try:
        conn.execute(
            f"UPDATE {table} SET data_json=?, email_thread_id=?, "
            f"updated_at=? WHERE id=?",
            (json.dumps(blob, default=str), thread_id, _utc_iso(), c["id"])
        )
    except sqlite3.OperationalError:
        conn.execute(
            f"UPDATE {table} SET data_json=?, updated_at=? WHERE id=?",
            (json.dumps(blob, default=str), _utc_iso(), c["id"])
        )


def _fmt_row(c: dict, tid: str = "") -> str:
    src = "gmail_id" if c["gmail_id"] else "rfc822msgid"
    val = c["gmail_id"] or c["rfc822_id"]
    tail = f"→ {tid}" if tid else "→ (not found)"
    return (f"  {c['kind'].upper():3s} {c['id'][:14]:14s} "
            f"{src}={val[:40]:40s} {tail}")


def run(db_path: Optional[str], *, apply: bool = False,
        only: Optional[str] = None, max_records: int = 200) -> dict:
    """Run the backfill and return a structured result dict.

    Result keys:
      ok          : bool
      mode        : "apply" | "dry-run"
      db_path     : resolved DB path used
      total_found : number of records that needed a thread_id
      flipped     : number for which a thread_id was found
      not_found   : number for which Gmail had no match
      capped_at   : the --max value (or None if uncapped)
      records     : per-record list of dicts:
        {kind, id, source, value, thread_id, applied}
      error       : populated on hard failure (DB missing, gmail unconfigured)
    """
    resolved = _resolve_db_path(db_path)
    if not resolved or not os.path.exists(resolved):
        return {"ok": False,
                "error": f"DB not found: {resolved or '/data/reytech.db'}",
                "mode": "apply" if apply else "dry-run",
                "records": []}

    kinds: Iterable[str] = ("pc", "rfq")
    if only:
        if only not in ("pc", "rfq"):
            return {"ok": False,
                    "error": f"--only must be 'pc' or 'rfq', got {only!r}",
                    "records": []}
        kinds = (only,)

    try:
        from src.core.gmail_api import get_service, is_configured
    except Exception as e:
        return {"ok": False, "error": f"cannot import gmail_api: {e}",
                "records": []}

    if not is_configured():
        return {"ok": False,
                "error": "Gmail not configured (no refresh token).",
                "records": []}
    service = get_service("sales")

    conn = sqlite3.connect(resolved)
    try:
        all_candidates = []
        for kind in kinds:
            all_candidates.extend(_scan(conn, kind))
        total_found = len(all_candidates)
        capped_at = None
        if max_records and total_found > max_records:
            capped_at = max_records
            all_candidates = all_candidates[:max_records]

        records = []
        flipped = 0
        not_found = 0
        for c in all_candidates:
            tid = _fetch_thread_id(service,
                                   gmail_id=c["gmail_id"],
                                   rfc822_id=c["rfc822_id"])
            applied = False
            if tid:
                if apply:
                    _apply_one(conn, c, tid)
                    applied = True
                flipped += 1
            else:
                not_found += 1
            records.append({
                "kind": c["kind"],
                "id": c["id"],
                "source": "gmail_id" if c["gmail_id"] else (
                    "rfc822msgid" if c["rfc822_id"] else "none"),
                "value": (c["gmail_id"] or c["rfc822_id"] or "")[:60],
                "thread_id": tid or "",
                "applied": applied,
            })

        if apply:
            conn.commit()

        return {
            "ok": True,
            "mode": "apply" if apply else "dry-run",
            "db_path": resolved,
            "total_found": total_found,
            "flipped": flipped,
            "not_found": not_found,
            "capped_at": capped_at,
            "records": records,
        }
    finally:
        conn.close()


def _print_report(result: dict) -> int:
    """Render a run() result as the legacy CLI text output. Returns exit code."""
    if not result.get("ok"):
        print(f"ERROR: {result.get('error', 'unknown')}", file=sys.stderr)
        return 2 if "DB not found" in (result.get("error") or "") else 1
    print(f"{result['mode'].upper()} backfill_email_thread_id "
          f"on {result['db_path']}")
    print(f"Found {result['total_found']} record(s) needing thread_id.")
    if result.get("capped_at"):
        print(f"Capping at --max {result['capped_at']}.")
    for r in result["records"]:
        tail = f"→ {r['thread_id']}" if r["thread_id"] else "→ (not found)"
        print(f"  {r['kind'].upper():3s} {r['id'][:14]:14s} "
              f"{r['source']}={r['value'][:40]:40s} {tail}")
    if result["mode"] == "apply":
        print(f"\n[OK] Backfilled thread_id on {result['flipped']} record(s); "
              f"{result['not_found']} not found.")
    else:
        print(f"\nDry-run: would backfill {result['flipped']} record(s); "
              f"{result['not_found']} not findable. Pass --apply to commit.")
    return 0


def main(argv: Optional[list] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--apply", action="store_true",
                   help="Commit changes. Default is dry-run.")
    p.add_argument("--only", choices=("pc", "rfq"),
                   help="Limit scan to PCs or RFQs only.")
    p.add_argument("--db", default=None,
                   help="Override DB path (default auto-detects).")
    p.add_argument("--max", type=int, default=200, dest="max_records",
                   help="Cap the number of Gmail API calls (default 200).")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    result = run(args.db, apply=args.apply, only=args.only,
                 max_records=args.max_records)
    return _print_report(result)


if __name__ == "__main__":
    sys.exit(main())
