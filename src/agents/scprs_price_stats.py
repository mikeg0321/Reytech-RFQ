"""SCPRS per-SKU price rollup — Phase 1.5-A.

Replaces ad-hoc per-pricing-call scans of `scprs_po_lines` with a
pre-computed `(match_key_type, match_key, agency, year, qty_band)`
table of unit-price statistics. The oracle reads from here first
when an MFG# or UNSPSC code is known; ad-hoc search becomes the
fallback path.

Mike's design answers (project_pricing_oracle_scprs_prior_plan_2026_05_11):
  • Q1 — match key order MFG# → UNSPSC → desc tokens → McKesson#.
    This rollup carries MFG# and UNSPSC. Desc-token rollups stay
    ad-hoc; McKesson# is a separate table.
  • Q5 — all bids all-or-none; n_lines posterior is a different table.

This module ships the rollup BUILDER. The oracle WIRE-UP is a
follow-up PR — safe to deploy alone since no consumer reads the
table yet.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime

log = logging.getLogger("reytech.scprs_price_stats")


# ── MFG# extraction from line description ────────────────────────


# Match common MFG# shapes inside scprs_po_lines.description.
# Order matters — labeled patterns first, positional fallback last.
# Re-uses the same priority as src.forms.price_check._PN_PATTERNS but
# narrower here because SCPRS descriptions are buyer-typed catalog
# copy and rarely carry the OCR noise that motivated the labeled
# patterns in PR #937.
# Order matters: longer label tokens FIRST so `Mfg Part #` matches before
# `Mfg #` would greedily capture `Part` as the value.
_MFG_PATTERNS_FOR_ROLLUP = [
    re.compile(r'Mfg(?:r)?\.?\s+Part\s*#?\s*[:\-]?\s*([A-Z0-9][A-Z0-9\-./]{2,30})', re.IGNORECASE),
    re.compile(r'Mfg(?:r)?\s*#?\s*[:\-]?\s*([A-Z0-9][A-Z0-9\-./]{2,30})', re.IGNORECASE),
    re.compile(r'Part\s*#\s*[:\-]?\s*([A-Z0-9][A-Z0-9\-./]{2,30})', re.IGNORECASE),
    re.compile(r'Item\s*#\s*[:\-]?\s*([A-Z0-9][A-Z0-9\-./]{2,30})', re.IGNORECASE),
    re.compile(r'Item\s*[:\-]\s*([A-Z0-9][A-Z0-9\-./]{2,30})', re.IGNORECASE),
    re.compile(r'SKU\s*#?\s*[:\-]?\s*([A-Z0-9][A-Z0-9\-./]{2,30})', re.IGNORECASE),
]


def _normalize_mfg(s: str) -> str:
    """Canonical form for an MFG# join key — uppercase, strip whitespace,
    drop dots. Preserves dashes and slashes (those distinguish real SKUs).
    """
    if not s:
        return ""
    return re.sub(r'[\s.]+', '', str(s)).upper().strip("-/")


def extract_mfg_from_scprs_line(description: str) -> str:
    """Pull a MFG# from a SCPRS line description, or '' if none found.

    SCPRS descriptions are inconsistent — sometimes the buyer types
    `Mfg # 12345`, sometimes `Item: ABC-123`, often just product name
    + spec with no part identifier. When no labeled pattern hits, we
    return empty rather than guess from positional regex.
    """
    if not description:
        return ""
    for pat in _MFG_PATTERNS_FOR_ROLLUP:
        m = pat.search(description)
        if m:
            return _normalize_mfg(m.group(1))
    return ""


# ── Quantity-band bucketing ──────────────────────────────────────


def qty_band(qty) -> str:
    """Bucket a quantity into one of the rollup's discrete bands.

    Bands chosen to match typical procurement order shapes:
      "1"      — single-unit demonstration / pilot
      "2-9"    — small office order
      "10-49"  — mid-size institution order (most common)
      "50-499" — large institution / system-wide
      "500+"   — bulk contract"""
    try:
        q = float(qty or 0)
    except (TypeError, ValueError):
        q = 0
    if q < 1:
        return "1"  # treat unknown/zero as singleton
    if q < 2:
        return "1"
    if q < 10:
        return "2-9"
    if q < 50:
        return "10-49"
    if q < 500:
        return "50-499"
    return "500+"


# ── Year + agency normalization ──────────────────────────────────


def _line_year(start_date, end_date) -> str:
    """Best-effort year inference from PO master fields. Both stored as
    text in the SCPRS schema. Falls back to "*" (all-time) if neither
    parses."""
    for s in (start_date, end_date):
        if not s:
            continue
        s = str(s)[:4]
        if s.isdigit() and 2018 <= int(s) <= 2099:
            return s
    return "*"


def _normalize_agency(agency_key, dept_code, dept_name) -> str:
    """Map SCPRS PO master agency fields to a canonical agency key.

    Returns the lowercase agency_key when present; falls back to
    dept_name token match (CDCR / CCHCS / CALVET / DSH / etc). Returns
    "*" when no signal — those rows aggregate into the cross-agency
    rollup.
    """
    if agency_key:
        return str(agency_key).strip().lower() or "*"
    blob = " ".join(str(x or "") for x in (dept_code, dept_name)).upper()
    if "CDCR" in blob or "CCHCS" in blob or "CORRECTIONS" in blob:
        return "cchcs"
    if "CALVET" in blob or "VETERANS" in blob or "DVA" in blob:
        return "calvet"
    if "STATE HOSPITAL" in blob or "DSH" in blob:
        return "dsh"
    if "GENERAL SERVICES" in blob or "DGS" in blob:
        return "dgs"
    if "FORESTRY" in blob or "FIRE" in blob:
        return "calfire"
    return "*"


# ── Rollup builder ───────────────────────────────────────────────


def _percentile(sorted_vals, p: float) -> float:
    """Linear-interpolation percentile (numpy-free)."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return float(d0 + d1)


def rebuild_scprs_price_stats(conn=None, *, max_rows: int = 0) -> dict:
    """Rebuild the `scprs_price_stats` rollup table from `scprs_po_lines`.

    Args:
        conn: optional sqlite connection. When None, opens one via
              `src.core.db.get_db` and closes at the end.
        max_rows: cap on source rows scanned. 0 = unlimited. Test hook.

    Returns:
        {"lines_scanned": N, "stats_written": M, "skipped_no_key": K,
         "duration_sec": S}

    Builds aggregations across both `mfg` and `unspsc` match keys, and
    across both per-agency rows and cross-agency `*` rows. A single
    SCPRS line that has BOTH a MFG# and an UNSPSC contributes to up to
    4 buckets (mfg×agency, mfg×*, unspsc×agency, unspsc×*).

    Idempotent — runs as DELETE + bulk insert in one transaction so the
    table never goes empty during rebuild.
    """
    from src.core.db import get_db
    started = datetime.now()
    close_after = False
    if conn is None:
        conn = get_db().__enter__()
        close_after = True

    stats = {
        "lines_scanned": 0,
        "stats_written": 0,
        "skipped_no_key": 0,
        "duration_sec": 0.0,
    }

    try:
        # Pull joined PO lines + master. SCPRS data is one-shot enough
        # that we hold prices in memory; ~150K rows × small dict each
        # fits comfortably in a Railway B1 box.
        sql = """
            SELECT pl.description, pl.unspsc, pl.unit_price, pl.quantity,
                   pm.agency_key, pm.dept_code, pm.dept_name,
                   pm.start_date, pm.end_date
            FROM scprs_po_lines pl
            LEFT JOIN scprs_po_master pm ON pm.id = pl.po_id
            WHERE pl.unit_price IS NOT NULL AND pl.unit_price > 0
              AND COALESCE(pl.line_status, '') != 'cancelled'
        """
        if max_rows > 0:
            sql += f" LIMIT {int(max_rows)}"
        rows = conn.execute(sql).fetchall()
        stats["lines_scanned"] = len(rows)

        # buckets: {(key_type, key, agency, year, qty_band) → [unit_prices...]}
        buckets: dict[tuple, list[float]] = {}

        for r in rows:
            try:
                desc, unspsc, unit_price, qty = r[0], r[1], r[2], r[3]
                agency_key, dept_code, dept_name = r[4], r[5], r[6]
                start_date, end_date = r[7], r[8]

                price = float(unit_price or 0)
                if price <= 0:
                    continue
                qband = qty_band(qty)
                # Per-EA — SCPRS unit_price IS already per-EA in this
                # schema (line_total = unit_price * quantity).
                # Verified 2026-05-11 audit.
                year = _line_year(start_date, end_date)
                agency = _normalize_agency(agency_key, dept_code, dept_name)

                mfg = extract_mfg_from_scprs_line(desc or "")
                unspsc_norm = str(unspsc or "").strip()

                added = False
                if mfg:
                    # Per-agency, per-year, per-qty bucket
                    for a in (agency, "*"):
                        for y in (year, "*"):
                            for q in (qband, "*"):
                                buckets.setdefault(
                                    ("mfg", mfg, a, y, q), []
                                ).append(price)
                    added = True
                if unspsc_norm and len(unspsc_norm) >= 4:
                    # Roll up at full code AND at the 4-digit family
                    # (UNSPSC is hierarchical: 42143000 = "Bandages,
                    # surgical"; 4214 = "Wound care"). Family rollups
                    # give the oracle a fallback when the exact code
                    # has too few samples.
                    keys_to_add = {unspsc_norm}
                    if len(unspsc_norm) >= 8:
                        keys_to_add.add(unspsc_norm[:4])
                    for uk in keys_to_add:
                        for a in (agency, "*"):
                            for y in (year, "*"):
                                for q in (qband, "*"):
                                    buckets.setdefault(
                                        ("unspsc", uk, a, y, q), []
                                    ).append(price)
                    added = True
                if not added:
                    stats["skipped_no_key"] += 1
            except Exception as e:
                log.debug("skip scprs row: %s", e)
                stats["skipped_no_key"] += 1

        # Build the rollup rows
        now_ts = datetime.now().isoformat(timespec="seconds")
        out_rows = []
        for (kt, k, a, y, q), prices in buckets.items():
            if not prices:
                continue
            sp = sorted(prices)
            out_rows.append((
                kt, k, a, y, q,
                len(sp),
                round(sum(sp) / len(sp), 4),
                round(_percentile(sp, 0.50), 4),
                round(_percentile(sp, 0.75), 4),
                round(_percentile(sp, 0.90), 4),
                now_ts,
            ))

        # Atomic swap: DELETE then INSERT inside one transaction. If
        # the INSERT fails, the DELETE rolls back and the prior rollup
        # survives — readers never see an empty table.
        conn.execute("BEGIN")
        try:
            conn.execute("DELETE FROM scprs_price_stats")
            conn.executemany(
                """INSERT INTO scprs_price_stats
                   (match_key_type, match_key, agency, year, qty_band,
                    count, mean, p50, p75, p90, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                out_rows,
            )
            conn.execute("COMMIT")
            stats["stats_written"] = len(out_rows)
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        if close_after:
            try:
                conn.__exit__(None, None, None)
            except Exception:
                pass

    stats["duration_sec"] = round(
        (datetime.now() - started).total_seconds(), 3,
    )
    log.info(
        "scprs_price_stats rebuild: scanned=%d, written=%d, skipped=%d, took=%.2fs",
        stats["lines_scanned"], stats["stats_written"],
        stats["skipped_no_key"], stats["duration_sec"],
    )
    return stats


def lookup_price_stat(
    *,
    mfg_number: str = "",
    unspsc: str = "",
    agency: str = "",
    qty_band_filter: str = "",
) -> dict | None:
    """Best-bucket lookup against the rollup. Honors Mike's Q1 priority
    order: MFG# > UNSPSC, agency-specific > cross-agency, qty-band match
    > qty-band wildcard. Returns None when no bucket has count >= 1.

    The oracle wire-up PR uses this. Shipping it now (alongside the
    builder) so the contract is locked before pricing_oracle_v2 starts
    depending on it.
    """
    from src.core.db import get_db
    agency_norm = (agency or "").strip().lower() or "*"
    qbf = qty_band_filter or "*"

    # Build prioritized lookup list. First hit wins.
    probes = []
    for kt, key in (("mfg", mfg_number), ("unspsc", unspsc)):
        if not key:
            continue
        key_norm = _normalize_mfg(key) if kt == "mfg" else str(key).strip()
        if not key_norm:
            continue
        for a in (agency_norm, "*"):
            for q in ([qbf, "*"] if qbf != "*" else ["*"]):
                probes.append((kt, key_norm, a, "*", q))
                # If a family-level UNSPSC was supplied, the rollup
                # builder already wrote both full + 4-digit rows; no
                # extra family probe needed here.

    with get_db() as conn:
        for kt, k, a, y, q in probes:
            row = conn.execute(
                """SELECT count, mean, p50, p75, p90, updated_at
                   FROM scprs_price_stats
                   WHERE match_key_type=? AND match_key=? AND agency=?
                     AND year=? AND qty_band=?""",
                (kt, k, a, y, q),
            ).fetchone()
            if row and (row["count"] if hasattr(row, "__getitem__") else row[0]) >= 1:
                return {
                    "match_key_type": kt,
                    "match_key": k,
                    "agency": a,
                    "year": y,
                    "qty_band": q,
                    "count": row[0],
                    "mean": row[1],
                    "p50": row[2],
                    "p75": row[3],
                    "p90": row[4],
                    "updated_at": row[5],
                }
    return None
