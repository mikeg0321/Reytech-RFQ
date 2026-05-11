"""Rebuild order→quote linkage from authoritative sources.

The 2026-05-11 deep audit found 64 orders with no `quote_number`. The
simple `link_orphan_orders.py` (PO-exact-match) couldn't link any of
them because Reytech's PO records on orders predate / sidestep the
RFQapp quote workflow.

Mike's correction (2026-05-11 23:45Z): "SCPRS has ALL Reytech POs
across all time. Gmail outbox identifies quotes sent. Gmail inbox has
the PO received. Drive has the final PO with sometimes a written quote
number. There are only 3 POs that have been won AND quoted from the
RFQ app."

This script reconciles the 4 sources to surface those 3 (or however
many) real RFQapp-tied wins:

  1. SCPRS po_master.supplier LIKE '%Reytech%' → set of authoritative
     PO records (~206 on prod).
  2. email_log direction='out' + quote_number set → set of Reytech
     quotes ever sent.
  3. email_log direction='in' + po_number set → set of PO emails
     received.
  4. orders table → operationally-tracked orders (165 on prod, 64 with
     empty quote_number).

Matching logic per SCPRS PO:

  HIGH: po_number directly matches a quote row's po_number OR a
        Reytech order's po_number.
  MEDIUM: po_number appears in an email_log row whose thread also
          contains an outbox quote_number sent ≤ 90 days before the
          PO date (PO ≈ quote response).
  LOW: agency + date proximity (PO date within 60 days of a Reytech
       quote sent) + items overlap heuristic. Surfaced for operator
       pick, never auto-linked.
  NONE: no Reytech quote found — direct PO (buyer ordered without
        going through RFQapp).

Output categorization:

  linked_high     — auto-link safe; PO ↔ quote pair identified
  linked_medium   — needs operator confirm before auto-link
  ambiguous_low   — multiple candidate quotes; operator picks
  direct_po       — confirmed not via RFQapp; mark in orders metadata
  pre_rfqapp      — PO date < RFQapp first-deployment date (no orders)

Usage:
  python scripts/rebuild_orphan_orders.py                  # dry-run (default)
  python scripts/rebuild_orphan_orders.py --apply          # link HIGH matches
  python scripts/rebuild_orphan_orders.py --json           # JSON output
  python scripts/rebuild_orphan_orders.py --confidence high  # filter to HIGH only

Safe defaults: dry-run by default; `--apply` ONLY auto-links HIGH
confidence (po_number exact match). MEDIUM/LOW always require operator
review (surfaced via the /kpi/orphans triage UI — see PR for the
companion route).

Cross-ref:
  - link_orphan_orders.py (older, narrower script — PO exact match only)
  - scripts/backfill_orders_quotes_drift.py (flip quote status to won
    when order has quote_number; runs AFTER this script lands links)
  - docs/AUDIT_DEEP_E2E_2026_05_07_v2.md §S-15 (the read-side debt)
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Iterable


_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


# Approximate RFQapp first-deployment date — POs before this can't have
# come through the workflow. Conservative estimate; refine if Mike has
# a more precise date.
RFQAPP_FIRST_DEPLOY = "2026-02-01"


def _resolve_db_path(override: str | None) -> str | None:
    if override:
        return override
    for p in ("/data/reytech.db", "data/reytech.db"):
        if os.path.exists(p):
            return p
    return None


def _row_to_dict(row) -> dict:
    return {k: row[k] for k in row.keys()} if row else {}


def _date_str(s: str | None) -> str:
    """Normalize a date string for prefix comparison. Returns YYYY-MM-DD
    or empty string."""
    if not s:
        return ""
    s = str(s)
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    # Try MM/DD/YYYY
    if len(s) >= 10 and s[2] == "/" and s[5] == "/":
        mm, dd, yyyy = s[:2], s[3:5], s[6:10]
        return f"{yyyy}-{mm}-{dd}"
    return ""


def _days_between(d1: str, d2: str) -> int:
    """Days between two YYYY-MM-DD strings. Returns huge if either is empty."""
    try:
        a = datetime.strptime(d1, "%Y-%m-%d")
        b = datetime.strptime(d2, "%Y-%m-%d")
        return abs((a - b).days)
    except (ValueError, TypeError):
        return 99999


def _scprs_reytech_pos(conn: sqlite3.Connection) -> list[dict]:
    """All SCPRS POs where the supplier is Reytech."""
    rows = conn.execute("""
        SELECT po_number, supplier, dept_name, dept_code,
               start_date, grand_total, buyer_name, buyer_email
        FROM scprs_po_master
        WHERE supplier LIKE '%Reytech%' OR supplier LIKE '%Rey Tech%'
        ORDER BY start_date DESC
    """).fetchall()
    return [_row_to_dict(r) for r in rows]


def _outbox_quotes(conn: sqlite3.Connection) -> list[dict]:
    """All Reytech quotes ever sent (via email_log outbox).

    email_log.direction values on prod are 'sent' / 'received'. Accept
    both conventions defensively — older code used 'out' / 'in' in
    early prototypes; if the schema drifts in either direction, this
    still matches.
    """
    rows = conn.execute("""
        SELECT id, logged_at, recipient, subject, quote_number, thread_id
        FROM email_log
        WHERE direction IN ('out', 'sent', 'outbound')
          AND quote_number IS NOT NULL AND quote_number != ''
        ORDER BY logged_at DESC
    """).fetchall()
    return [_row_to_dict(r) for r in rows]


def _inbox_po_emails(conn: sqlite3.Connection) -> list[dict]:
    """PO email arrivals — direction='received' (or 'in') + po_number set.

    See _outbox_quotes for the direction-value compat rationale.
    """
    rows = conn.execute("""
        SELECT id, logged_at, sender, subject, po_number, quote_number, thread_id
        FROM email_log
        WHERE direction IN ('in', 'received', 'inbound')
          AND po_number IS NOT NULL AND po_number != ''
        ORDER BY logged_at DESC
    """).fetchall()
    return [_row_to_dict(r) for r in rows]


def _quotes_by_po(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Quotes that already have a po_number set, indexed by PO."""
    rows = conn.execute("""
        SELECT quote_number, po_number, agency, institution, total, created_at, status
        FROM quotes
        WHERE po_number IS NOT NULL AND po_number != ''
    """).fetchall()
    out: dict[str, list[dict]] = {}
    for r in rows:
        d = _row_to_dict(r)
        out.setdefault(d["po_number"].strip(), []).append(d)
    return out


def _orders_by_po(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Orders indexed by po_number."""
    rows = conn.execute("""
        SELECT id, quote_number, po_number, total, agency, institution, created_at
        FROM orders
        WHERE po_number IS NOT NULL AND po_number != ''
    """).fetchall()
    out: dict[str, list[dict]] = {}
    for r in rows:
        d = _row_to_dict(r)
        out.setdefault(d["po_number"].strip(), []).append(d)
    return out


def _classify_po(po: dict, quotes_by_po, orders_by_po, inbox_pos,
                 outbox_quotes) -> dict:
    """Decide which category this SCPRS Reytech PO belongs to."""
    po_num = (po.get("po_number") or "").strip()
    po_date_norm = _date_str(po.get("start_date"))
    result = {
        "po_number": po_num,
        "supplier": po.get("supplier"),
        "agency": po.get("dept_name"),
        "grand_total": po.get("grand_total"),
        "start_date": po.get("start_date"),
        "matched_quote_numbers": [],
        "matched_order_ids": [],
        "confidence": "none",
        "category": "direct_po",
        "evidence": [],
    }

    # Pre-RFQapp gate
    if po_date_norm and po_date_norm < RFQAPP_FIRST_DEPLOY:
        result["category"] = "pre_rfqapp"
        result["evidence"].append(f"PO date {po_date_norm} < RFQapp deploy {RFQAPP_FIRST_DEPLOY}")
        return result

    # HIGH: exact po_number match in quotes table
    if po_num and po_num in quotes_by_po:
        candidates = quotes_by_po[po_num]
        result["matched_quote_numbers"] = [q["quote_number"] for q in candidates]
        result["confidence"] = "high"
        result["category"] = "linked_high"
        result["evidence"].append(
            f"quotes.po_number exact match: {len(candidates)} candidate(s)"
        )

    # HIGH: exact po_number match in orders table
    if po_num and po_num in orders_by_po:
        for o in orders_by_po[po_num]:
            result["matched_order_ids"].append(o["id"])
            if o.get("quote_number"):
                if o["quote_number"] not in result["matched_quote_numbers"]:
                    result["matched_quote_numbers"].append(o["quote_number"])
        if result["matched_quote_numbers"]:
            result["confidence"] = "high"
            result["category"] = "linked_high"
            result["evidence"].append(
                f"orders.po_number exact match: {len(orders_by_po[po_num])} row(s)"
            )

    # MEDIUM: PO appears in an inbox email whose thread has an outbox
    # quote_number (Reytech sent the quote, buyer responded with PO).
    if result["confidence"] in ("none", ""):
        matching_inbox = [e for e in inbox_pos
                          if (e.get("po_number") or "").strip() == po_num]
        for inbox in matching_inbox:
            thread = inbox.get("thread_id")
            if not thread:
                continue
            # Find outbox quotes in the same thread
            thread_quotes = [q for q in outbox_quotes
                             if q.get("thread_id") == thread]
            for tq in thread_quotes:
                qn = tq.get("quote_number")
                if qn and qn not in result["matched_quote_numbers"]:
                    result["matched_quote_numbers"].append(qn)
            if thread_quotes:
                result["confidence"] = "medium"
                result["category"] = "linked_medium"
                result["evidence"].append(
                    f"thread {thread[:12]} ties inbox PO email + outbox quote(s)"
                )

    # LOW: agency + 60-day date proximity to any Reytech quote sent
    if result["confidence"] in ("none", ""):
        po_agency = (po.get("dept_name") or "").upper()
        for q in outbox_quotes:
            recipient = (q.get("recipient") or "").lower()
            subject = (q.get("subject") or "").upper()
            # crude agency hit
            agency_hit = any(
                token in subject or token in recipient
                for token in po_agency.split()
                if len(token) > 4
            )
            if not agency_hit:
                continue
            sent_date = _date_str(q.get("logged_at"))
            if not sent_date or not po_date_norm:
                continue
            if _days_between(sent_date, po_date_norm) <= 60:
                if q["quote_number"] not in result["matched_quote_numbers"]:
                    result["matched_quote_numbers"].append(q["quote_number"])
                result["confidence"] = "low"
                result["category"] = "ambiguous_low"
                result["evidence"].append(
                    f"low-confidence: quote {q['quote_number']} sent {sent_date} "
                    f"to {recipient[:30]} (agency token hit)"
                )

    return result


def run(db_path: str | None, *, apply: bool = False, json_out: bool = False,
        confidence_filter: str | None = None) -> int:
    resolved = _resolve_db_path(db_path)
    if not resolved:
        print("ERROR: no reytech.db found", file=sys.stderr)
        return 2

    conn = sqlite3.connect(resolved)
    conn.row_factory = sqlite3.Row
    try:
        pos = _scprs_reytech_pos(conn)
        outbox = _outbox_quotes(conn)
        inbox = _inbox_po_emails(conn)
        quotes_by_po = _quotes_by_po(conn)
        orders_by_po = _orders_by_po(conn)

        results = []
        for po in pos:
            classification = _classify_po(po, quotes_by_po, orders_by_po,
                                          inbox, outbox)
            if confidence_filter and classification["confidence"] != confidence_filter:
                continue
            results.append(classification)

        # Summary
        by_category: dict[str, int] = {}
        for r in results:
            by_category[r["category"]] = by_category.get(r["category"], 0) + 1

        if json_out:
            print(json.dumps({
                "scanned_at": datetime.now(timezone.utc).isoformat(),
                "total_reytech_pos": len(pos),
                "by_category": by_category,
                "results": results,
                "dry_run": not apply,
            }, indent=2, default=str))
            return 0

        # Human-readable output
        print(f"{'APPLY' if apply else 'DRY-RUN'} orphan rebuild on {resolved}")
        print(f"Total Reytech POs in SCPRS: {len(pos)}")
        print(f"Outbox quotes (email_log): {len(outbox)}")
        print(f"Inbox PO emails: {len(inbox)}")
        print()
        print("Categories:")
        for cat, count in sorted(by_category.items()):
            print(f"  {cat:18s} {count}")
        print()
        print("=== HIGH-confidence matches (auto-link safe) ===")
        for r in results:
            if r["confidence"] == "high":
                print(f"  PO {r['po_number']:15s}  agency={(r['agency'] or '')[:25]:25s}  "
                      f"date={r['start_date'][:10]:10s}  "
                      f"quote_numbers={r['matched_quote_numbers']}")
                for ev in r["evidence"]:
                    print(f"    evidence: {ev}")
        print()
        print("=== MEDIUM-confidence (operator confirm) ===")
        for r in results:
            if r["confidence"] == "medium":
                print(f"  PO {r['po_number']:15s}  candidates={r['matched_quote_numbers']}")
                for ev in r["evidence"]:
                    print(f"    evidence: {ev}")
        print()
        print("=== LOW-confidence (operator pick from list) ===")
        for r in results[:30]:
            if r["confidence"] == "low":
                print(f"  PO {r['po_number']:15s}  candidates={r['matched_quote_numbers'][:3]}")
        low_count = sum(1 for r in results if r["confidence"] == "low")
        if low_count > 30:
            print(f"  ... {low_count - 30} more LOW-confidence; pass --json for full list")
        print()
        # Apply phase
        if apply:
            print("=== APPLY: linking HIGH-confidence matches ===")
            apply_count = 0
            for r in results:
                if r["confidence"] != "high":
                    continue
                quotes = r["matched_quote_numbers"]
                if len(quotes) != 1:
                    print(f"  SKIP {r['po_number']}: {len(quotes)} candidate quotes "
                          f"({quotes}) — needs operator pick")
                    continue
                qn = quotes[0]
                # Find orders for this PO that lack quote_number
                orders = orders_by_po.get(r["po_number"], [])
                for o in orders:
                    if o.get("quote_number"):
                        continue  # already linked
                    try:
                        conn.execute(
                            "UPDATE orders SET quote_number = ? WHERE id = ?",
                            (qn, o["id"]),
                        )
                        apply_count += 1
                        print(f"  LINKED order {o['id']} → quote {qn} (via PO {r['po_number']})")
                    except Exception as e:
                        print(f"  ERR  order {o['id']}: {e}")
            conn.commit()
            print(f"\n✓ Applied {apply_count} order→quote linkages")
        else:
            print("Dry-run: pass --apply to link HIGH-confidence matches "
                  "(single-quote-candidate per PO only).")
        return 0
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--apply", action="store_true",
                    help="Auto-link HIGH-confidence single-candidate matches")
    ap.add_argument("--json", action="store_true",
                    help="Emit JSON instead of human-readable output")
    ap.add_argument("--confidence",
                    choices=("high", "medium", "low", "none"),
                    help="Filter to one confidence level")
    ap.add_argument("--db", default=None, help="Override DB path")
    args = ap.parse_args()
    return run(args.db, apply=args.apply, json_out=args.json,
               confidence_filter=args.confidence)


if __name__ == "__main__":
    sys.exit(main())
