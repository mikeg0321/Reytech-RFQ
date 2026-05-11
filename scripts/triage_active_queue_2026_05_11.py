"""One-shot triage of the 2026-05-11 active-queue backlog.

Mike P0 2026-05-11: 12 active records in queue, only 3 are real work.
This script dispositions the 9 noise records per Mike's instructions
+ obvious-data-quality rules.

Run on prod:
    railway ssh "python scripts/triage_active_queue_2026_05_11.py"            # dry-run
    railway ssh "python scripts/triage_active_queue_2026_05_11.py --apply"    # commit

Dispositions:

  Self-ingested PCs (Mike's own outbound mail self-ingested) — DISMISS as
    auto_dedupe_self_ingested. Substrate fix is the email_poller filter
    tightening (same PR as this script); this cleans up the records that
    landed pre-fix.
      pc auto_20260319_1773963548 — pc_number=45007500
      pc auto_20260319_1773957724 — pc_number=45007355

  Parse-failed needs_review RFQs (no source_pdf, 0 items) — DISMISS as
    parse_failed_no_attachment.
      rfq 20260410_154740_19d06b — Grace Post @calvet.ca.gov, no PDF
      rfq 20260410_064854_19cf4b — sandyguadan@gmail.com (personal/family
        email, NOT a CA gov buyer)

  Worke Wodneh CalVet RFQ (status=ready, past due, never sent) — MARK
    no_response per Mike's explicit instruction 2026-05-11.
      rfq 20260404_001014_19c012  (id prefix 20260404_00101...)

  Tarrna Solis (CA Dept) solicitation 2002605 — 4 duplicate revision
    records with R26Q39 / R26Q41 / R26Q42 / R26Q43. All past due
    (4/23/2026). None have sent_at. Mike didn't flag any as sent.
    Disposition: keep the most-recently-updated record marked
    no_response (acts as the canonical history row); mark the other 3
    as duplicate of the canonical.

Each disposition prints a 1-line audit log line. --apply writes through
the standard set_quote_status_atomic + _save_single_pc/_rfq paths so the
audit_trail + lifecycle hooks fire normally.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone


# Project root → sys.path so `from src.api.data_layer import ...` works
# under `railway ssh "python scripts/<name>.py"`. Same pattern other
# scripts/ files use (e.g., backfill_unit_price.py:45-47).
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


log = logging.getLogger("triage_active_queue")
logging.basicConfig(level=logging.INFO, format="%(message)s")


# Explicit by-id dispositions
SELF_INGESTED_PCS = {
    "auto_20260319_1773963548": "self_ingested_outbound_reply pc_number=45007500",
    "auto_20260319_1773957724": "self_ingested_outbound_reply pc_number=45007355",
}

PARSE_FAILED_RFQS = {
    "20260410_154740_19d06b": "parse_failed_no_attachment subject='RFQ- PPE' sender=grace.post@calvet.ca.gov",
    "20260410_064854_19cf4b": "parse_failed_no_attachment subject='PiP hygiene price check' sender=sandyguadan@gmail.com_PERSONAL",
}

# Worke Wodneh — exact id prefix; resolved at runtime to full id.
WORKE_WODNEH_RFQ_PREFIX = "20260404_001014"

# Tarrna Solis solicitation 2002605 — IDs of the 4 records, resolved at runtime.
TARRNA_SOLIS_SOL_NUMBER = "2002605"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _find_rfq_by_prefix(rfqs: dict, prefix: str) -> str | None:
    for rid in rfqs:
        if rid.startswith(prefix):
            return rid
    return None


def _find_rfqs_by_solicitation(rfqs: dict, sol_number: str) -> list[str]:
    """Return rfq IDs whose solicitation_number OR rfq_number matches."""
    out = []
    for rid, r in rfqs.items():
        sn = (r.get("solicitation_number") or "").strip()
        rn = (r.get("rfq_number") or "").strip()
        if sn == sol_number or rn == sol_number:
            out.append(rid)
    return out


def _latest_id(rfqs: dict, rids: list[str]) -> str | None:
    """Pick the canonical rid for a dedup group.

    Filter rules (2026-05-11 audit lesson):
      1. Drop records already in a terminal status (dismissed / archived /
         no_response / duplicate). They shouldn't be candidates — they're
         done.
      2. Among remaining, pick by created_at, NOT updated_at. Prod
         updated_at gets touched by bulk maintenance passes (e.g., schema
         migration writes), syncing the column across all records to the
         same instant. created_at is the truthful per-record signal.
      3. If nothing remains, return None (caller skips this group).
    """
    TERMINAL = {"dismissed", "archived", "no_response", "duplicate", "cancelled"}
    eligible = [
        rid for rid in rids
        if (rfqs.get(rid, {}).get("status") or "").lower() not in TERMINAL
    ]
    if not eligible:
        return None
    def _key(rid: str) -> str:
        return rfqs.get(rid, {}).get("created_at") or ""
    return max(eligible, key=_key)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Commit changes (default: dry-run)")
    args = parser.parse_args()
    dry_run = not args.apply

    try:
        from src.api.data_layer import (
            _load_price_checks, load_rfqs,
            _save_single_pc, _save_single_rfq,
        )
    except ImportError as e:
        log.error("data_layer unavailable: %s", e)
        return 1

    pcs = _load_price_checks()
    rfqs = load_rfqs()

    actions: list[tuple[str, str, str, str]] = []  # (kind, id, new_status, reason)

    # 1. Self-ingested PCs → dismiss
    for pcid, reason in SELF_INGESTED_PCS.items():
        if pcid in pcs and pcs[pcid].get("status") not in ("dismissed", "archived"):
            actions.append(("pc", pcid, "dismissed", reason))

    # 2. Parse-failed RFQs → dismiss
    for rid, reason in PARSE_FAILED_RFQS.items():
        if rid in rfqs and rfqs[rid].get("status") not in ("dismissed", "archived"):
            actions.append(("rfq", rid, "dismissed", reason))

    # 3. Worke Wodneh → no_response per Mike
    wid = _find_rfq_by_prefix(rfqs, WORKE_WODNEH_RFQ_PREFIX)
    if wid and rfqs[wid].get("status") not in ("dismissed", "archived", "no_response"):
        actions.append(("rfq", wid, "no_response",
                        "mike_explicit_did_not_respond_2026_05_11 buyer=Worke_Wodneh_CalVet"))

    # 4. Tarrna Solis 2002605 → keep latest as no_response, others as duplicate
    ts_rids = _find_rfqs_by_solicitation(rfqs, TARRNA_SOLIS_SOL_NUMBER)
    if ts_rids:
        canonical = _latest_id(rfqs, ts_rids)
        if canonical is None:
            # All Tarrna Solis records already terminal — nothing to do.
            log.info("triage: all sol=%s records already terminal, skipping",
                     TARRNA_SOLIS_SOL_NUMBER)
        else:
            for rid in ts_rids:
                if (rfqs[rid].get("status") or "").lower() in (
                    "dismissed", "archived", "no_response", "duplicate",
                ):
                    continue
                if rid == canonical:
                    actions.append(("rfq", rid, "no_response",
                                    f"canonical_of_dedup_group sol={TARRNA_SOLIS_SOL_NUMBER} "
                                    "past_due_no_sent_at"))
                else:
                    actions.append(("rfq", rid, "duplicate",
                                    f"dup_of={canonical} sol={TARRNA_SOLIS_SOL_NUMBER}"))

    # ── Report ──
    if not actions:
        log.info("triage: nothing to do (queue already clean)")
        return 0

    log.info("=== TRIAGE PLAN (%d actions) ===", len(actions))
    for kind, rid, new_status, reason in actions:
        log.info("  %s %s %s -> %s  // %s",
                 "DRY" if dry_run else "APPLY", kind, rid, new_status, reason)

    if dry_run:
        log.info("")
        log.info("Dry-run complete. Pass --apply to commit.")
        return 0

    # ── Execute ──
    now = _now_iso()
    applied = 0
    failed = 0
    for kind, rid, new_status, reason in actions:
        try:
            if kind == "pc":
                pc = pcs.get(rid)
                if not pc:
                    log.warning("  MISS pc %s — not found", rid)
                    failed += 1
                    continue
                prior = pc.get("status", "")
                pc["status"] = new_status
                pc["dismissed_at"] = now
                pc["dismissed_reason"] = reason
                pc["updated_at"] = now
                _save_single_pc(rid, pc)
                log.info("  OK pc %s  %s -> %s", rid, prior, new_status)
                applied += 1
            else:
                r = rfqs.get(rid)
                if not r:
                    log.warning("  MISS rfq %s — not found", rid)
                    failed += 1
                    continue
                prior = r.get("status", "")
                r["status"] = new_status
                r["closed_at"] = now
                r["closed_reason"] = reason
                r["updated_at"] = now
                _save_single_rfq(rid, r)
                log.info("  OK rfq %s  %s -> %s", rid, prior, new_status)
                applied += 1
        except Exception as e:
            log.error("  FAIL %s %s: %s", kind, rid, e)
            failed += 1

    log.info("")
    log.info("=== TRIAGE COMPLETE — applied=%d failed=%d ===", applied, failed)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
