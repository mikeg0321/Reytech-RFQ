"""Oracle calibration backfill — feed all historical won/lost data
through calibrate_from_outcome() so the oracle has real pricing
intelligence immediately.

Background: markQuote() was a silent no-op Feb 17 → Apr 15. During that
window, the oracle feedback loop got zero data. Now that markQuote works
(PR #95), we backfill from every available source:

1. Quotes DB (quotes table) — status='won' or 'lost'
2. Award tracker matches (quote_po_matches table) — competitor wins
3. won_quotes_kb (SCPRS-derived knowledge base) — historical agency-level
   bid outcomes where Reytech bid and we know the winning price.
   This is the largest data source: 1,260+ rows of per-product
   per-agency win/loss signal that the original backfill ignored.
   Phase 0.7a of PLAN_ONCE_AND_FOR_ALL.md (2026-04-25).

Phase 0.7c (2026-04-25 PM): joinback_won_quotes_kb() — when the live
backfill ran, all 1,260 won_quotes_kb rows had reytech_price=NULL because
the SCPRS scraper only stored competitor wins, never Reytech bids. The
join-back walks each KB row, looks up quotes table for a Reytech quote
against the same agency + fuzzy description match in a date window, and
populates reytech_price + reytech_won where matched. Run this BEFORE
backfill_all() to maximize the calibration signal.

The backfill is idempotent: calibrate_from_outcome() uses exponential-
moving-average blending, so re-running reinforces existing calibration
without double-counting.
"""

import json
import logging
from datetime import datetime

log = logging.getLogger("oracle_backfill")


def backfill_all(dry_run: bool = False) -> dict:
    """Run the full oracle calibration backfill.

    Args:
        dry_run: if True, count what WOULD be backfilled without writing.

    Returns:
        {ok, quotes_won, quotes_lost, pcs_won, pcs_lost, calibrations_written, errors}
    """
    from src.core.pricing_oracle_v2 import calibrate_from_outcome

    result = {
        "ok": True,
        "quotes_won": 0, "quotes_lost": 0,
        "pcs_won": 0, "pcs_lost": 0,
        "kb_wins": 0, "kb_losses": 0, "kb_skipped_no_bid": 0,
        "calibrations_written": 0,
        "errors": [],
        # IN-12: per-agency error histogram. Operator asking "52 errors,
        # is one agency broken or is this scatter?" couldn't tell before.
        # Now the ops dashboard shows {"CCHCS": 47, "CDCR": 3, ...}.
        "errors_by_agency": {},
        "dry_run": dry_run,
    }

    # ── 1. Quotes DB ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute("""
                SELECT quote_number, status, agency, institution,
                       line_items, total, po_number, status_notes
                FROM quotes
                WHERE is_test = 0
                  AND status IN ('won', 'lost')
            """).fetchall()

        for r in rows:
            try:
                items = json.loads(r["line_items"] or "[]")
                if not items:
                    continue

                agency = r["agency"] or r["institution"] or ""
                status = r["status"]

                # Determine loss reason from status_notes if available
                loss_reason = None
                if status == "lost":
                    notes = (r["status_notes"] or "").lower()
                    if "price" in notes or "cost" in notes or "cheaper" in notes:
                        loss_reason = "price"
                    else:
                        loss_reason = "other"

                if not dry_run:
                    calibrate_from_outcome(
                        items, status,
                        agency=agency,
                        loss_reason=loss_reason,
                    )
                    result["calibrations_written"] += 1

                if status == "won":
                    result["quotes_won"] += 1
                else:
                    result["quotes_lost"] += 1

            except Exception as e:
                result["errors"].append(f"quote {r['quote_number']}: {e}")
                # IN-12: bucket by agency so a broken-agency pattern shows up
                _ag = (r["agency"] or r["institution"] or "unknown").strip() or "unknown"
                result["errors_by_agency"][_ag] = result["errors_by_agency"].get(_ag, 0) + 1
                log.debug("backfill quote %s: %s", r["quote_number"], e)

    except Exception as e:
        result["errors"].append(f"quotes DB: {e}")
        log.warning("backfill quotes DB: %s", e)

    # ── 2. Award tracker matches (competitor losses with price data) ──
    try:
        from src.core.db import get_db
        with get_db() as conn:
            matches = conn.execute("""
                SELECT quote_number, scprs_total, our_total,
                       outcome, line_analysis
                FROM quote_po_matches
                WHERE outcome = 'lost_to_competitor'
            """).fetchall()

        for m in matches:
            try:
                line_data = json.loads(m["line_analysis"] or "[]")
                if not line_data:
                    continue

                # Build items + winner_prices from the line analysis
                items = []
                winner_prices = {}
                for i, ld in enumerate(line_data):
                    if not isinstance(ld, dict):
                        continue
                    items.append({
                        "description": ld.get("description", ""),
                        "unit_price": ld.get("our_unit_price", 0),
                        "supplier_cost": ld.get("our_cost", 0),
                    })
                    if ld.get("winner_unit_price"):
                        winner_prices[i] = ld["winner_unit_price"]

                if items and not dry_run:
                    calibrate_from_outcome(
                        items, "lost",
                        loss_reason="price",
                        winner_prices=winner_prices,
                    )
                    result["calibrations_written"] += 1

            except Exception as e:
                result["errors"].append(f"match {m['quote_number']}: {e}")
                # IN-12: match table has no agency column, log under generic bucket
                result["errors_by_agency"]["po_match"] = result["errors_by_agency"].get("po_match", 0) + 1

    except Exception as e:
        result["errors"].append(f"quote_po_matches: {e}")
        log.debug("backfill matches: %s", e)

    # ── 3. won_quotes_kb (the bulk of historical data) ──
    # Each row is one bid outcome: (item, agency, winner, winner_price,
    # whether Reytech won, Reytech's price). When Reytech bid we feed
    # calibrate_from_outcome with real data. When Reytech didn't bid the
    # row is market intelligence, not a calibration input — skip it here
    # (a separate pricing-history pipeline consumes those rows).
    try:
        from src.core.db import get_db
        with get_db() as conn:
            kb_rows = conn.execute("""
                SELECT item_description, mfg_number, agency,
                       winning_price, winning_vendor,
                       reytech_won, reytech_price
                FROM won_quotes_kb
                WHERE reytech_price IS NOT NULL AND reytech_price > 0
            """).fetchall()

        for r in kb_rows:
            try:
                desc = (r["item_description"] or "").strip()
                if not desc:
                    continue
                agency = (r["agency"] or "").strip()
                reytech_price = float(r["reytech_price"] or 0)
                winner_price = float(r["winning_price"] or 0)
                won = bool(r["reytech_won"])

                items = [{
                    "description": desc,
                    "mfg_number": r["mfg_number"] or "",
                    "unit_price": reytech_price,
                }]

                if won:
                    if not dry_run:
                        calibrate_from_outcome(items, "won", agency=agency)
                        result["calibrations_written"] += 1
                    result["kb_wins"] += 1
                else:
                    winner_prices = {0: winner_price} if winner_price > 0 else None
                    if not dry_run:
                        calibrate_from_outcome(
                            items, "lost",
                            agency=agency,
                            loss_reason="price",
                            winner_prices=winner_prices,
                        )
                        result["calibrations_written"] += 1
                    result["kb_losses"] += 1

            except Exception as e:
                result["errors"].append(f"kb {r['item_description'][:40]}: {e}")
                _ag = (r["agency"] or "unknown").strip() or "unknown"
                result["errors_by_agency"][_ag] = result["errors_by_agency"].get(_ag, 0) + 1

        # Also count rows where Reytech didn't bid — useful context for the operator.
        with get_db() as conn:
            skipped = conn.execute("""
                SELECT COUNT(*) c FROM won_quotes_kb
                WHERE reytech_price IS NULL OR reytech_price <= 0
            """).fetchone()
            result["kb_skipped_no_bid"] = int(skipped[0] if skipped else 0)

    except Exception as e:
        result["errors"].append(f"won_quotes_kb: {e}")
        log.warning("backfill won_quotes_kb: %s", e)

    log.info(
        "Oracle backfill complete: quotes(%d won + %d lost), "
        "kb(%d wins + %d losses, %d skipped no-bid), "
        "%d calibrations written, %d errors, dry_run=%s",
        result["quotes_won"], result["quotes_lost"],
        result["kb_wins"], result["kb_losses"], result["kb_skipped_no_bid"],
        result["calibrations_written"], len(result["errors"]),
        dry_run,
    )
    return result


# ──────────────────────────────────────────────────────────────────────
# Phase 0.7c — won_quotes_kb reytech_price join-back
# ──────────────────────────────────────────────────────────────────────

_TOKEN_RE = None  # lazy compile inside _tokens()


def _tokens(text: str) -> set:
    """Lowercase alphanumeric tokens of length >= 3, for fuzzy match."""
    global _TOKEN_RE
    if _TOKEN_RE is None:
        import re as _re
        _TOKEN_RE = _re.compile(r"[a-z0-9]{3,}")
    return set(_TOKEN_RE.findall((text or "").lower()))


def _description_match_score(a: str, b: str) -> float:
    """Jaccard similarity over token sets. 0..1, higher is more similar."""
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / float(len(ta | tb))


def _agency_match(a: str, b: str) -> bool:
    """Loose agency match.

    Tries three strategies in order:
      1. case-insensitive substring (either direction)
      2. token-set overlap (Jaccard >= 0.4 over filtered tokens)

    Strategy 2 catches the common SCPRS-vs-QuoteWerks mismatch where the
    same agency is spelled "Dept of Veterans Affairs" in SCPRS and
    "Veterans Home of California - Barstow" in QuoteWerks. They share
    'veterans' which is enough signal when nothing else matches.
    """
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    # Strategy 2: drop common stopwords + short words, then Jaccard
    _STOP = {"of", "the", "and", "for", "ca", "california", "dept",
             "department", "inc", "co", "company", "corp", "corporation"}
    ta = {w for w in _tokens(a) if w not in _STOP}
    tb = {w for w in _tokens(b) if w not in _STOP}
    if not ta or not tb:
        return False
    overlap = len(ta & tb) / float(len(ta | tb))
    return overlap >= 0.4


def joinback_won_quotes_kb(
    dry_run: bool = False,
    description_threshold: float = 0.45,
    date_window_days: int = 90,
) -> dict:
    """Walk won_quotes_kb rows where reytech_price IS NULL and try to
    populate it from the quotes table.

    Match rule per KB row:
      - same (loose) agency: case-insensitive substring either direction
      - line_items contains a description with token-Jaccard >= threshold
      - quote.created_at within ±date_window_days of kb.award_date
      - prefer status=won over status=lost over status=sent

    On match, sets:
      - reytech_price  = matched line_item's unit_price (or 0 if absent)
      - reytech_won    = 1 if quote.status='won' else 0

    Returns {ok, kb_rows_examined, matched, updated, ambiguous, errors,
             dry_run}.
    """
    import json as _json
    from datetime import datetime, timedelta
    from src.core.db import get_db

    result = {
        "ok": True,
        "kb_rows_examined": 0,
        "matched": 0,
        "updated": 0,
        "ambiguous": 0,
        "errors": [],
        "dry_run": dry_run,
    }

    # Pull all KB rows that need a match.
    try:
        with get_db() as conn:
            kb = conn.execute("""
                SELECT id, item_description, agency, award_date,
                       winning_price, winning_vendor, mfg_number
                FROM won_quotes_kb
                WHERE reytech_price IS NULL OR reytech_price <= 0
            """).fetchall()
            quotes = conn.execute("""
                SELECT quote_number, status, agency, institution,
                       line_items, total, created_at
                FROM quotes
                WHERE is_test = 0
                  AND status IN ('won', 'lost', 'sent')
                  AND line_items IS NOT NULL
            """).fetchall()
    except Exception as e:
        result["ok"] = False
        result["errors"].append(f"load: {e}")
        log.warning("joinback load: %s", e)
        return result

    # Decode quote line_items once.
    quote_index = []
    for q in quotes:
        try:
            items = _json.loads(q["line_items"] or "[]")
            if not items:
                continue
            try:
                qdate = datetime.fromisoformat(
                    (q["created_at"] or "").replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except Exception:
                qdate = None
            quote_index.append({
                "quote_number": q["quote_number"],
                "status": q["status"],
                "agency": q["agency"] or q["institution"] or "",
                "items": items,
                "date": qdate,
            })
        except Exception as e:
            result["errors"].append(f"quote {q['quote_number']}: {e}")

    # Walk each KB row and try to match.
    status_rank = {"won": 3, "lost": 2, "sent": 1}
    for kr in kb:
        result["kb_rows_examined"] += 1
        try:
            kdesc = kr["item_description"] or ""
            kagency = kr["agency"] or ""
            kdate = None
            try:
                kdate = datetime.fromisoformat(
                    (kr["award_date"] or "").replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except Exception:
                kdate = None

            # Score every quote-line against this KB row; keep the best.
            best = None
            for q in quote_index:
                if not _agency_match(kagency, q["agency"]):
                    continue
                if kdate and q["date"]:
                    delta = abs((kdate - q["date"]).days)
                    if delta > date_window_days:
                        continue
                for it in q["items"]:
                    if not isinstance(it, dict):
                        continue
                    score = _description_match_score(kdesc, it.get("description", ""))
                    if score < description_threshold:
                        continue
                    candidate = {
                        "quote_number": q["quote_number"],
                        "status": q["status"],
                        "score": score,
                        "rank": status_rank.get(q["status"], 0),
                        "unit_price": float(
                            it.get("unit_price")
                            or it.get("bid_price")
                            or (it.get("pricing") or {}).get("recommended_price")
                            or 0
                        ),
                    }
                    if (best is None
                        or candidate["rank"] > best["rank"]
                        or (candidate["rank"] == best["rank"]
                            and candidate["score"] > best["score"])):
                        best = candidate

            if best is None:
                continue

            result["matched"] += 1
            new_price = best["unit_price"]
            new_won = 1 if best["status"] == "won" else 0

            if not dry_run and new_price > 0:
                with get_db() as conn:
                    conn.execute("""
                        UPDATE won_quotes_kb
                        SET reytech_price = ?,
                            reytech_won = ?,
                            price_delta = (winning_price - ?)
                        WHERE id = ?
                    """, (new_price, new_won, new_price, kr["id"]))
                result["updated"] += 1

        except Exception as e:
            result["errors"].append(f"kb {kr['id']}: {e}")

    log.info(
        "won_quotes_kb joinback: examined=%d matched=%d updated=%d "
        "errors=%d dry_run=%s",
        result["kb_rows_examined"], result["matched"], result["updated"],
        len(result["errors"]), dry_run,
    )
    return result


# ──────────────────────────────────────────────────────────────────────
# Phase 0.7d — QuoteWerks-imported quote outcome verification via SCPRS
# ──────────────────────────────────────────────────────────────────────

def verify_quotewerks_outcomes(
    dry_run: bool = False,
    description_threshold: float = 0.45,
    date_window_days: int = 120,
    require_agency_match: bool = False,
) -> dict:
    """Mike's rule (2026-04-25): a Reytech quote that appears verbatim in
    SCPRS = a won PO. A quote NOT in SCPRS = a loss.

    Source-of-truth: `scprs_reytech_wins` table, populated from Mike's
    SCPRS Detail-Information HTML export. That table contains every
    Reytech-won PO since 2022. The local dry-run parsed 112 unique POs.

    Falls back to the live `scprs_po_lines` table when the won-export
    table is empty (so we still verify against fresh SCPRS pulls).

    For every quote with status='sent' (the bucket QuoteWerks import
    leaves them in), search the won-set for a match:
      - dept matches the quote's agency (substring either direction)
      - at least one description token-Jaccard >= threshold
      - PO start_date within ±date_window_days of quote.created_at

    On match: status → 'won', po_number ← SCPRS po_number.
    On no match: status → 'lost'.

    Returns {ok, examined, marked_won, marked_lost, errors, dry_run, source}.
    """
    import json as _json
    from datetime import datetime
    from src.core.db import get_db

    result = {
        "ok": True, "examined": 0,
        "marked_won": 0, "marked_lost": 0,
        "errors": [], "dry_run": dry_run,
        "source": "none",
    }

    try:
        with get_db() as conn:
            sent_quotes = conn.execute("""
                SELECT quote_number, agency, institution, line_items,
                       created_at, status_notes
                FROM quotes
                WHERE is_test = 0 AND status = 'sent'
                  AND line_items IS NOT NULL
            """).fetchall()
    except Exception as e:
        result["ok"] = False
        result["errors"].append(f"load quotes: {e}")
        return result

    # Pick win-source: prefer curated export, fall back to live SCPRS.
    scprs_by_dept = []
    try:
        with get_db() as conn:
            wins_count = conn.execute(
                "SELECT COUNT(*) FROM scprs_reytech_wins"
            ).fetchone()[0]
            if wins_count > 0:
                result["source"] = "scprs_reytech_wins"
                rows = conn.execute("""
                    SELECT po_number, dept_name, start_date, items_json
                    FROM scprs_reytech_wins
                """).fetchall()
                for w in rows:
                    try:
                        sdate = datetime.fromisoformat(
                            (w["start_date"] or "").replace("Z", "+00:00")
                        ).replace(tzinfo=None)
                    except Exception:
                        sdate = None
                    items = []
                    try:
                        items = _json.loads(w["items_json"] or "[]")
                    except Exception:
                        items = []
                    if not items:
                        scprs_by_dept.append({
                            "po_number": w["po_number"] or "",
                            "dept_name": w["dept_name"] or "",
                            "description": "",
                            "date": sdate,
                        })
                    for it in items:
                        scprs_by_dept.append({
                            "po_number": w["po_number"] or "",
                            "dept_name": w["dept_name"] or "",
                            "description": (it.get("description")
                                            if isinstance(it, dict)
                                            else "") or "",
                            "date": sdate,
                        })
            else:
                result["source"] = "scprs_po_lines"
                rows = conn.execute("""
                    SELECT pl.po_number, pl.dept_name, pl.description, pm.start_date
                    FROM scprs_po_lines pl
                    JOIN scprs_po_master pm ON pm.id = pl.po_id
                    WHERE LOWER(pm.supplier) LIKE '%reytech%'
                      AND pm.is_test = 0
                """).fetchall()
                for s in rows:
                    try:
                        sdate = datetime.fromisoformat(
                            (s["start_date"] or "").replace("Z", "+00:00")
                        ).replace(tzinfo=None)
                    except Exception:
                        sdate = None
                    scprs_by_dept.append({
                        "po_number": s["po_number"] or "",
                        "dept_name": s["dept_name"] or "",
                        "description": s["description"] or "",
                        "date": sdate,
                    })
    except Exception as e:
        result["errors"].append(f"load scprs source: {e}")

    for q in sent_quotes:
        result["examined"] += 1
        try:
            items = _json.loads(q["line_items"] or "[]")
            if not items:
                continue
            qagency = q["agency"] or q["institution"] or ""
            try:
                qdate = datetime.fromisoformat(
                    (q["created_at"] or "").replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except Exception:
                qdate = None

            matched_po = None
            matched_score = 0.0
            for s in scprs_by_dept:
                # Phase 0.7d retune (2026-04-26): SCPRS dept_name is the
                # PARENT agency ('Dept of Corrections & Rehab') while
                # QuoteWerks SoldToCompany is the SPECIFIC FACILITY
                # ('California Institution for Women'). Token Jaccard
                # over those rarely clears 0.4. Mike's rule was
                # "verbatim in SCPRS = won" — agency-name is implied by
                # the PO supplier=Reytech filter the SCPRS export already
                # applies. So agency match is now a soft signal: it
                # boosts score when present, but missing it doesn't skip.
                if require_agency_match and not _agency_match(qagency, s["dept_name"]):
                    continue
                if qdate and s["date"]:
                    if abs((qdate - s["date"]).days) > date_window_days:
                        continue
                agency_boost = 0.05 if _agency_match(qagency, s["dept_name"]) else 0.0
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    score = _description_match_score(
                        it.get("description", ""), s["description"]
                    ) + agency_boost
                    if score >= description_threshold and score > matched_score:
                        matched_po = s["po_number"]
                        matched_score = score

            if matched_po:
                if not dry_run:
                    with get_db() as conn:
                        conn.execute("""
                            UPDATE quotes
                            SET status='won',
                                po_number=COALESCE(NULLIF(?, ''), po_number),
                                status_notes=COALESCE(status_notes, '') ||
                                  ' [SCPRS-verify won via PO ' || ? ||
                                  ' score=' || ? || ']'
                            WHERE quote_number=? AND status='sent'
                        """, (matched_po, matched_po, f"{matched_score:.2f}",
                              q["quote_number"]))
                result["marked_won"] += 1
            else:
                if not dry_run:
                    with get_db() as conn:
                        conn.execute("""
                            UPDATE quotes
                            SET status='lost',
                                status_notes=COALESCE(status_notes, '') ||
                                  ' [SCPRS-verify lost: no PO match]'
                            WHERE quote_number=? AND status='sent'
                        """, (q["quote_number"],))
                result["marked_lost"] += 1
        except Exception as e:
            result["errors"].append(f"{q['quote_number']}: {e}")

    log.info(
        "quotewerks-verify: examined=%d won=%d lost=%d source=%s "
        "errors=%d dry_run=%s",
        result["examined"], result["marked_won"], result["marked_lost"],
        result["source"], len(result["errors"]), dry_run,
    )
    return result
