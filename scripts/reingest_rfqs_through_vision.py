"""Re-ingest specific RFQs through the Vision-verification pipeline (PR #908).

Mike P000 2026-05-11: PR #908 added Vision AI verification on the
generic-RFQ-PDF shape so multi-page buyer RFQs no longer drop items
silently. RFQs already in the system that were ingested BEFORE the
fix landed still carry their undersurfaced item lists. This script
re-runs the new pipeline against the archived attachments so those
records pick up the items they should have had.

USAGE
=====

    # Re-process a specific RFQ by record-id, sol#, or partial number match
    python scripts/reingest_rfqs_through_vision.py 10846357
    python scripts/reingest_rfqs_through_vision.py rfq_8efe9fae 10845681

    # Dry-run: report what would change without writing
    python scripts/reingest_rfqs_through_vision.py 10846357 --dry-run

    # Pull a list of suspect RFQs (low items vs page count) automatically
    python scripts/reingest_rfqs_through_vision.py --auto-suspect

The "auto-suspect" mode walks all open/draft RFQs, scores each by
(item_count / pdf_page_count), and re-ingests anything below a 3
items/page threshold OR with zero items but a PDF attachment.

SAFETY
======

- Resolves identifiers with FUZZY match against `rfqs.rfq_number`,
  `rfqs.solicitation_number`, and `rfqs.id`. If multiple match it
  prints the candidates and asks you to pick a more specific id.
- Re-ingest goes through `process_buyer_request(existing_record_id=rid,
  existing_record_type="rfq")` — same code path the live upload-parse
  endpoint uses. The Vision verification kicks in automatically.
- Each RFQ's items are REPLACED (not merged) on re-parse — same
  behavior as the live upload-parse-doc endpoint. Existing pricing
  on items is LOST. Use --dry-run first if you've already priced
  some items.
- Skips an RFQ if there's no buyer-RFQ attachment archived in
  `rfq_files`.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from typing import Optional

# Make `src` importable when run as a script
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _resolve_rfq_ids(identifiers: list[str]) -> list[tuple[str, dict]]:
    """Map user-supplied identifiers (sol#, partial number, rfq_id) to
    actual rfq records. Returns list of (rfq_id, rfq_record_dict).

    Multiple matches → print and skip with an explanatory message so
    the operator can re-run with a more specific identifier.
    """
    from src.core.db import get_db
    matches: list[tuple[str, dict]] = []
    seen_ids: set[str] = set()
    with get_db() as conn:
        for ident in identifiers:
            like = f"%{ident}%"
            rows = conn.execute(
                "SELECT id, rfq_number, solicitation_number, status, "
                "received_at, data_json "
                "FROM rfqs "
                "WHERE id = ? OR rfq_number LIKE ? OR solicitation_number LIKE ? "
                "ORDER BY received_at DESC LIMIT 5",
                (ident, like, like),
            ).fetchall()
            if not rows:
                print(f"  ⚠  '{ident}': no match in rfqs table — skipping")
                continue
            if len(rows) > 1 and rows[0]["id"] != ident:
                print(f"  ⚠  '{ident}': {len(rows)} candidates — please re-run "
                      f"with a more specific id:")
                for r in rows:
                    print(f"      {r['id']}  rfq_number={r['rfq_number']}  "
                          f"sol={r['solicitation_number']}  status={r['status']}")
                continue
            row = rows[0]
            rid = row["id"]
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            try:
                r_dict = json.loads(row["data_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                r_dict = {}
            r_dict.setdefault("id", rid)
            r_dict.setdefault("rfq_number", row["rfq_number"] or "")
            r_dict.setdefault("solicitation_number", row["solicitation_number"] or "")
            r_dict.setdefault("status", row["status"] or "")
            matches.append((rid, r_dict))
    return matches


def _list_suspect_rfqs(min_items_per_page: float = 3.0,
                       limit: int = 25) -> list[tuple[str, dict]]:
    """Find RFQs with an attached PDF where items/page < threshold OR
    items=0 with a PDF on file. Returns same shape as _resolve_rfq_ids."""
    from src.core.db import get_db
    from pypdf import PdfReader
    matches: list[tuple[str, dict]] = []
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, rfq_number, solicitation_number, status, data_json "
            "FROM rfqs "
            "WHERE status IN ('new', 'priced', 'draft', 'ready') "
            "ORDER BY received_at DESC LIMIT 200"
        ).fetchall()
        for row in rows:
            rid = row["id"]
            try:
                r = json.loads(row["data_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            items = r.get("line_items") or r.get("items") or []
            item_count = len(items)
            # Find the FIRST PDF attachment in rfq_files
            file_row = conn.execute(
                "SELECT data, filename FROM rfq_files "
                "WHERE rfq_id = ? AND LOWER(filename) LIKE '%.pdf' "
                "ORDER BY created_at ASC LIMIT 1",
                (rid,),
            ).fetchone()
            if not file_row:
                continue
            data = file_row["data"]
            if not data:
                continue
            page_count = 0
            tmp = None
            try:
                with tempfile.NamedTemporaryFile(
                    suffix=".pdf", delete=False
                ) as f:
                    f.write(data)
                    tmp = f.name
                page_count = len(PdfReader(tmp).pages)
            except Exception:
                page_count = 0
            finally:
                if tmp and os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
            if page_count == 0:
                continue
            ratio = item_count / page_count
            suspect = item_count == 0 or ratio < min_items_per_page
            if suspect:
                r.setdefault("id", rid)
                r.setdefault("rfq_number", row["rfq_number"] or "")
                r.setdefault("solicitation_number", row["solicitation_number"] or "")
                r.setdefault("status", row["status"] or "")
                r["_diag_items"] = item_count
                r["_diag_pages"] = page_count
                r["_diag_ratio"] = round(ratio, 2)
                matches.append((rid, r))
            if len(matches) >= limit:
                break
    return matches


def _find_buyer_attachment(rfq_id: str) -> Optional[tuple[bytes, str]]:
    """Return (file_bytes, filename) for the buyer-RFQ attachment.

    Priority:
      1. rfq_files row with category='buyer_rfq' (most precise — what
         the post-PR-#910 ingest stamps onto saved attachments).
      2. rfq_files OLDEST PDF that is NOT a supplier_quote / package /
         template — legacy path for records ingested before the
         category tag was added.
      3. None — caller falls through to Gmail re-fetch.
    """
    from src.core.db import get_db
    with get_db() as conn:
        # Priority 1: explicit buyer_rfq tag
        row = conn.execute(
            "SELECT data, filename FROM rfq_files "
            "WHERE rfq_id = ? AND category = 'buyer_rfq' "
            "  AND LOWER(filename) LIKE '%.pdf' "
            "ORDER BY created_at ASC LIMIT 1",
            (rfq_id,),
        ).fetchone()
        if row:
            return (row["data"], row["filename"])
        # Priority 2: oldest PDF that isn't a supplier quote / package
        row = conn.execute(
            "SELECT data, filename FROM rfq_files "
            "WHERE rfq_id = ? "
            "  AND LOWER(filename) LIKE '%.pdf' "
            "  AND category NOT IN ('supplier_quote', 'package', 'generated') "
            "ORDER BY created_at ASC LIMIT 1",
            (rfq_id,),
        ).fetchone()
        if row:
            return (row["data"], row["filename"])
        return None


def _fetch_buyer_attachments_from_gmail(
    rfq: dict,
) -> list[tuple[bytes, str]]:
    """Re-pull buyer attachments from the original Gmail message.

    Used when rfq_files has no buyer PDF — typically the case for RFQs
    ingested before this script existed, because email_poller did not
    persist the original PDF after parse. Reads `email_uid` /
    `email_message_id` off the RFQ and pulls the raw message from Gmail.
    Returns list of (bytes, filename) for every PDF attachment found;
    empty list if Gmail unreachable, msg_id missing, or no PDFs found.
    """
    import email as _email_pkg
    msg_id = (
        rfq.get("email_message_id")
        or rfq.get("email_uid")
        or rfq.get("gmail_message_id")
        or ""
    )
    if not msg_id:
        return []
    try:
        from src.core.gmail_api import get_service, get_raw_message
        for inbox in ("sales", "mike"):
            try:
                service = get_service(inbox_name=inbox)
            except Exception:
                continue
            try:
                raw = get_raw_message(service, msg_id)
            except Exception:
                continue
            if not raw:
                continue
            try:
                msg = _email_pkg.message_from_bytes(raw)
            except Exception:
                continue
            results: list[tuple[bytes, str]] = []
            for part in msg.walk():
                if part.get_content_maintype() == "multipart":
                    continue
                fname = part.get_filename() or ""
                if not fname.lower().endswith(".pdf"):
                    continue
                try:
                    data = part.get_payload(decode=True)
                except Exception:
                    continue
                if not data:
                    continue
                results.append((data, fname))
            if results:
                return results
        return []
    except Exception as e:
        print(f"   gmail fetch error: {e}")
        return []


def _persist_buyer_attachment(
    rfq_id: str, data: bytes, filename: str,
) -> None:
    """Save the re-fetched buyer attachment to rfq_files with
    category='buyer_rfq' so future re-ingest finds it without Gmail.
    Partial substrate fix for the missing-persist-at-ingest gap."""
    try:
        from src.api.dashboard import save_rfq_file
        save_rfq_file(
            rfq_id, filename, "application/pdf", data,
            category="buyer_rfq", uploaded_by="reingest_script",
        )
    except Exception as e:
        print(f"   warn: could not persist {filename} to rfq_files: {e}")


def _reingest_one(rfq_id: str, rfq: dict, dry_run: bool = False) -> dict:
    """Re-run the ingest pipeline against the buyer-RFQ attachment for
    one RFQ. Returns a status dict."""
    out = {"rfq_id": rfq_id, "rfq_number": rfq.get("rfq_number", ""),
           "sol": rfq.get("solicitation_number", ""),
           "status": rfq.get("status", ""),
           "items_before": len(rfq.get("line_items") or rfq.get("items") or []),
           "items_after": None, "delta": None, "skipped": None,
           "error": None, "dry_run": dry_run}

    attach = _find_buyer_attachment(rfq_id)
    source = "rfq_files"
    if not attach:
        # rfq_files had nothing usable. Try Gmail re-fetch via the
        # stored email_uid / email_message_id. This handles RFQs
        # ingested before the persist-buyer-attachment substrate fix.
        gmail_attachments = _fetch_buyer_attachments_from_gmail(rfq)
        if not gmail_attachments:
            out["skipped"] = "no_pdf_attachment_and_gmail_fetch_failed"
            return out
        # Use the first PDF attachment (buyer RFQs typically have one).
        # If there are multiple, the rest are usually instructions /
        # terms-and-conditions; the buyer-RFQ parser excludes those by
        # filename pattern.
        data, filename = gmail_attachments[0]
        source = "gmail_refetch"
        # Persist for future runs so we don't keep re-pulling from Gmail.
        if not dry_run:
            _persist_buyer_attachment(rfq_id, data, filename)
            print(f"   gmail re-fetch: {filename} ({len(data)} bytes), persisted to rfq_files")
        else:
            print(f"   gmail re-fetch (dry-run): {filename} ({len(data)} bytes), would persist")
    else:
        data, filename = attach
    out["source"] = source

    # Write to temp file for process_buyer_request
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f"reingest_{rfq_id}_", suffix=".pdf", delete=False
        ) as f:
            f.write(data)
            tmp_path = f.name

        if dry_run:
            # Run the parse only, don't update the record
            from src.forms.vision_parser import parse_with_vision, is_available
            if is_available():
                parsed = parse_with_vision(tmp_path)
                vitems = (parsed or {}).get("line_items") or []
                out["items_after"] = len(vitems)
                out["delta"] = out["items_after"] - out["items_before"]
                out["dry_run_parser"] = "vision"
            else:
                out["skipped"] = "vision_unavailable_in_dry_run"
            return out

        # Live re-ingest through the pipeline. existing_record_id +
        # existing_record_type ensure we UPDATE this record, not create
        # a duplicate.
        from src.core.ingest_pipeline import process_buyer_request
        result = process_buyer_request(
            files=[tmp_path],
            email_body=rfq.get("body_text", "") or "",
            email_subject=rfq.get("email_subject", "") or "",
            email_sender=rfq.get("requestor_email", "") or "",
            existing_record_id=rfq_id,
            existing_record_type="rfq",
        )
        result_dict = result.to_dict() if hasattr(result, "to_dict") else result
        out["items_after"] = result_dict.get("items_parsed")
        out["delta"] = (out["items_after"] or 0) - out["items_before"]
        out["parser"] = result_dict.get("parser", "?")
        out["warnings"] = result_dict.get("warnings", [])
        out["errors"] = result_dict.get("errors", [])
        return out
    except Exception as e:
        out["error"] = str(e)
        return out
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def main():
    ap = argparse.ArgumentParser(description="Re-ingest RFQs through PR #908 Vision pipeline")
    ap.add_argument("identifiers", nargs="*",
                    help="RFQ identifiers (rfq_id, sol#, rfq_number substring)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Report what would happen without updating records")
    ap.add_argument("--auto-suspect", action="store_true",
                    help="Auto-discover undersurfaced RFQs (items/page < 3 OR 0 items)")
    ap.add_argument("--min-ratio", type=float, default=3.0,
                    help="Items-per-page threshold for --auto-suspect (default 3.0)")
    args = ap.parse_args()

    if args.auto_suspect:
        print(f"Scanning RFQs for items/page ratio < {args.min_ratio}...")
        targets = _list_suspect_rfqs(min_items_per_page=args.min_ratio)
        print(f"Found {len(targets)} suspect RFQs:")
        for rid, r in targets:
            print(f"  - {rid}  sol={r.get('solicitation_number','?'):<22} "
                  f"status={r.get('status','?'):<10} "
                  f"items={r.get('_diag_items',0)}  pages={r.get('_diag_pages',0)}  "
                  f"ratio={r.get('_diag_ratio',0)}")
        if not targets:
            print("  (nothing to do)")
            return 0
    else:
        if not args.identifiers:
            ap.print_help()
            return 1
        print(f"Resolving {len(args.identifiers)} identifier(s)...")
        targets = _resolve_rfq_ids(args.identifiers)
        if not targets:
            print("  (no matches resolved)")
            return 1
        print(f"Resolved {len(targets)} RFQ(s):")
        for rid, r in targets:
            print(f"  - {rid}  sol={r.get('solicitation_number','?')}  "
                  f"items_before={len(r.get('line_items') or r.get('items') or [])}")

    if args.dry_run:
        print("\n[DRY RUN] No records will be modified.")
    print(f"\nRe-ingesting {len(targets)} RFQ(s)...\n")

    results = []
    for rid, r in targets:
        print(f">> {rid}  ({r.get('solicitation_number','?')})")
        out = _reingest_one(rid, r, dry_run=args.dry_run)
        results.append(out)
        if out.get("skipped"):
            print(f"   SKIPPED: {out['skipped']}")
        elif out.get("error"):
            print(f"   ERROR: {out['error']}")
        else:
            delta = out.get("delta")
            arrow = "↑" if (delta or 0) > 0 else ("↓" if (delta or 0) < 0 else "·")
            print(f"   {arrow} items: {out['items_before']} → {out['items_after']}  "
                  f"(Δ {delta:+d})" if delta is not None else
                  f"   items: {out['items_before']} → {out['items_after']}")

    print("\n=== Summary ===")
    upgraded = [r for r in results if r.get("delta", 0) and r["delta"] > 0]
    skipped = [r for r in results if r.get("skipped")]
    errored = [r for r in results if r.get("error")]
    print(f"  Upgraded: {len(upgraded)} (Vision found more items)")
    print(f"  Skipped:  {len(skipped)}")
    print(f"  Errored:  {len(errored)}")
    if upgraded:
        print("  Top upgrades:")
        for r in sorted(upgraded, key=lambda x: -x["delta"])[:10]:
            print(f"    + {r['rfq_id']:<14} sol={r['sol']:<22}  "
                  f"{r['items_before']:>2} → {r['items_after']:>2}")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
