"""Spine product catalog — buyer-supplied + validated product data.

Closes Mike's 5/17 directive: "this data should be cataloged in the
table". Every ingest with a non-empty mfg_number emits a catalog
observation. The catalog becomes the durable record of what buyers
ask for + what we've priced before, and is the input for:
- The auto-link substrate (PR #1044) — provides MFG# evidence over
  time even when prior Spine quotes have aged out of recent reads.
- The auto-price substrate (PR #1044) — fall-back source when no
  linked PC has the MFG#.
- Stale-cost recheck signal (task #22) — flags catalog entries whose
  last_priced_at is older than the freshness window.
- URL + photo enrichment (task #23) — background fetcher walks
  rows with enrichment_status='pending' and fills source_url +
  photo_url + photo_path.

DESIGN
- One row per normalized MFG# (upper-cased, trimmed). Repeat
  observations of the same MFG# update last_seen_at + seen_count
  and union the descriptions / uoms / unspsc lists.
- catalog_id = "cat_" + sha256(normalized_mfg)[:16] — deterministic,
  so observe() is idempotent on identity.
- Pure substrate — no I/O outside the SQLite DB. The enrichment
  fetcher lives in a separate module that calls record_enrichment().
- All writes go through `_persist_catalog_entry` (single-writer
  substrate convention; mirrors db._persist_state /
  _persist_snapshot / _persist_contract / _persist_rejection /
  _persist_counter / _persist_link).
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.spine.model import SpineValidationError


# The catalog's stale-cost threshold — entries whose last_priced_at is
# older than this need re-validation before being trusted for carry.
# Mirrors LineItem.COST_VALIDATION_FRESHNESS_DAYS (=30) so the catalog
# and the per-line cost gate stay in sync.
CATALOG_STALENESS_DAYS = 30

# Enrichment status sentinels — written by record_enrichment() and
# read by the queued background fetcher (task #23 follow-on).
ENRICHMENT_PENDING = "pending"
ENRICHMENT_FETCHED = "fetched"
ENRICHMENT_FAILED = "failed"

_WRITE_LOCK = threading.Lock()


# ──────────────────────────────────────────────────────────────────────
# Connection helpers — mirror db._connect to keep WAL + foreign_keys ON.
# ──────────────────────────────────────────────────────────────────────


def _connect(db_path: str | Path) -> sqlite3.Connection:
    """Local mirror so we don't import from db.py and create a cycle."""
    conn = sqlite3.connect(str(db_path), isolation_level=None, timeout=10.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _norm_mfg(s: str | None) -> str | None:
    """Same rule as quote_matcher._norm_mfg / auto_pricer._norm_mfg.

    Intentionally duplicated: each substrate module owns its own
    normalization so a future divergence (e.g., the matcher
    becoming more aggressive than the catalog) doesn't silently
    break invariants.
    """
    if s is None:
        return None
    out = s.strip().upper().rstrip(".,;:")
    return out or None


def _catalog_id_for(mfg: str) -> str:
    """Deterministic ID derived from the normalized MFG#. Idempotent."""
    digest = hashlib.sha256(mfg.encode("utf-8")).hexdigest()[:16]
    return f"cat_{digest}"


# ──────────────────────────────────────────────────────────────────────
# observe — UPSERT on every ingest seeing a MFG#.
# ──────────────────────────────────────────────────────────────────────


def observe(
    db_path: str | Path,
    *,
    mfg_number: str,
    description: str,
    uom: str | None = None,
    unspsc: str | None = None,
    quote_id: str | None = None,
    cost_cents: int | None = None,
    actor: str,
) -> dict:
    """Record one observation of a MFG# from a buyer worksheet.

    First observation creates a row. Subsequent observations:
    - increment seen_count
    - update last_seen_at + last_seen_quote_id
    - union the description into descriptions_json (preserving order
      of first-seen)
    - update canonical_description to the latest non-empty value
    - union uom into uoms_seen_json
    - union unspsc into unspsc_codes_json (if provided)
    - if cost_cents > 0, update last_priced_at + last_priced_cents
      + last_priced_quote_id

    The first observation marks the row enrichment_status=PENDING so
    the background fetcher (task #23) picks it up.

    Returns metadata dict {catalog_id, mfg_number, seen_count,
    created (bool), updated_at}.

    Raises SpineValidationError on bad inputs.
    """
    norm = _norm_mfg(mfg_number)
    if norm is None:
        raise SpineValidationError("observe requires non-empty mfg_number.")
    if not description or not description.strip():
        raise SpineValidationError("observe requires non-empty description.")
    if not actor or not actor.strip():
        raise SpineValidationError("observe requires non-empty actor.")
    if cost_cents is not None:
        if not isinstance(cost_cents, int) or cost_cents < 0:
            raise SpineValidationError(
                f"observe cost_cents must be non-negative int; got {cost_cents!r}"
            )

    catalog_id = _catalog_id_for(norm)
    now_iso = datetime.now(timezone.utc).isoformat()
    desc_clean = description.strip()
    uom_clean = (uom or "").strip().upper() or None
    unspsc_clean = (unspsc or "").strip() or None

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT * FROM spine_catalog WHERE catalog_id = ?",
                (catalog_id,),
            ).fetchone()

            if row is None:
                descriptions = [desc_clean]
                uoms = [uom_clean] if uom_clean else []
                unspscs = [unspsc_clean] if unspsc_clean else []
                seen_count = 1
                first_seen_at = now_iso
                last_priced_at = now_iso if (cost_cents and cost_cents > 0) else None
                last_priced_quote_id = (
                    quote_id if (cost_cents and cost_cents > 0 and quote_id) else None
                )
                last_priced_cents = cost_cents if (cost_cents and cost_cents > 0) else None
                created = True
            else:
                descriptions = json.loads(row["descriptions_json"])
                if desc_clean not in descriptions:
                    descriptions.append(desc_clean)
                uoms = json.loads(row["uoms_seen_json"])
                if uom_clean and uom_clean not in uoms:
                    uoms.append(uom_clean)
                unspscs = json.loads(row["unspsc_codes_json"])
                if unspsc_clean and unspsc_clean not in unspscs:
                    unspscs.append(unspsc_clean)
                seen_count = int(row["seen_count"]) + 1
                first_seen_at = row["first_seen_at"]
                if cost_cents is not None and cost_cents > 0:
                    last_priced_at = now_iso
                    last_priced_quote_id = quote_id or row["last_priced_quote_id"]
                    last_priced_cents = cost_cents
                else:
                    last_priced_at = row["last_priced_at"]
                    last_priced_quote_id = row["last_priced_quote_id"]
                    last_priced_cents = row["last_priced_cents"]
                created = False

            _persist_catalog_entry(
                conn,
                catalog_id=catalog_id,
                mfg_number=norm,
                canonical_description=desc_clean,
                descriptions_json=json.dumps(descriptions),
                uoms_seen_json=json.dumps(uoms),
                unspsc_codes_json=json.dumps(unspscs),
                seen_count=seen_count,
                first_seen_at=first_seen_at,
                last_seen_at=now_iso,
                last_seen_quote_id=quote_id,
                last_priced_at=last_priced_at,
                last_priced_quote_id=last_priced_quote_id,
                last_priced_cents=last_priced_cents,
                source_url=row["source_url"] if row else None,
                source_url_checked_at=row["source_url_checked_at"] if row else None,
                photo_url=row["photo_url"] if row else None,
                photo_path=row["photo_path"] if row else None,
                enrichment_status=(
                    row["enrichment_status"] if row else ENRICHMENT_PENDING
                ),
            )

    return {
        "catalog_id": catalog_id,
        "mfg_number": norm,
        "seen_count": seen_count,
        "created": created,
        "updated_at": now_iso,
    }


def _persist_catalog_entry(
    conn: sqlite3.Connection,
    *,
    catalog_id: str,
    mfg_number: str,
    canonical_description: str,
    descriptions_json: str,
    uoms_seen_json: str,
    unspsc_codes_json: str,
    seen_count: int,
    first_seen_at: str,
    last_seen_at: str,
    last_seen_quote_id: str | None,
    last_priced_at: str | None,
    last_priced_quote_id: str | None,
    last_priced_cents: int | None,
    source_url: str | None,
    source_url_checked_at: str | None,
    photo_url: str | None,
    photo_path: str | None,
    enrichment_status: str | None,
) -> None:
    """THE ONLY function in src/spine/ that writes spine_catalog.

    Mirror of _persist_state / _persist_snapshot / _persist_contract /
    _persist_rejection / _persist_counter / _persist_link. Adding any
    other INSERT/UPDATE/DELETE against spine_catalog inside src/spine/
    is a substrate regression.
    """
    conn.execute(
        "INSERT OR REPLACE INTO spine_catalog "
        "(catalog_id, mfg_number, canonical_description, descriptions_json, "
        " uoms_seen_json, unspsc_codes_json, seen_count, "
        " first_seen_at, last_seen_at, last_seen_quote_id, "
        " last_priced_at, last_priced_quote_id, last_priced_cents, "
        " source_url, source_url_checked_at, photo_url, photo_path, "
        " enrichment_status) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (catalog_id, mfg_number, canonical_description, descriptions_json,
         uoms_seen_json, unspsc_codes_json, seen_count,
         first_seen_at, last_seen_at, last_seen_quote_id,
         last_priced_at, last_priced_quote_id, last_priced_cents,
         source_url, source_url_checked_at, photo_url, photo_path,
         enrichment_status),
    )


# ──────────────────────────────────────────────────────────────────────
# Read path
# ──────────────────────────────────────────────────────────────────────


def get_entry(db_path: str | Path, mfg_number: str) -> dict | None:
    """Look up a catalog row by MFG#. Returns a plain dict with
    descriptions / uoms / unspsc decoded as lists, or None when
    nothing matches."""
    norm = _norm_mfg(mfg_number)
    if norm is None:
        return None
    catalog_id = _catalog_id_for(norm)
    with _connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM spine_catalog WHERE catalog_id = ?",
            (catalog_id,),
        ).fetchone()
    if row is None:
        return None
    return _row_to_dict(row)


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    for key in ("descriptions_json", "uoms_seen_json", "unspsc_codes_json"):
        try:
            d[key.removesuffix("_json")] = json.loads(d[key])
        except Exception:
            d[key.removesuffix("_json")] = []
    return d


def iter_entries(
    db_path: str | Path,
    *,
    limit: int = 1000,
    enrichment_status: str | None = None,
) -> list[dict]:
    """Return catalog rows ordered by last_seen_at DESC. Optional
    enrichment_status filter for the background fetcher's queue
    ("pending"). Defaults to 1000 rows; raises on absurd values."""
    if not isinstance(limit, int) or limit < 1 or limit > 100_000:
        raise SpineValidationError(f"iter_entries limit must be 1..100000; got {limit!r}")
    sql = "SELECT * FROM spine_catalog"
    params: tuple = ()
    if enrichment_status is not None:
        sql += " WHERE enrichment_status = ?"
        params = (enrichment_status,)
    sql += " ORDER BY last_seen_at DESC LIMIT ?"
    params = params + (limit,)
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]


# ──────────────────────────────────────────────────────────────────────
# Stale-cost recheck signal (task #22)
# ──────────────────────────────────────────────────────────────────────


def find_stale_priced_entries(
    db_path: str | Path,
    *,
    days: int = CATALOG_STALENESS_DAYS,
    limit: int = 500,
) -> list[dict]:
    """Return catalog entries whose last_priced_at is older than
    `days` (defaults to CATALOG_STALENESS_DAYS). Used by the
    operator surface to flag "the price you carried for this is
    stale — re-validate".

    Entries with last_priced_at=NULL are NOT returned — they have no
    price to be stale about. Entries newer than `days` aren't
    returned either.

    Result is sorted oldest-first so the operator can attack the
    worst staleness first.
    """
    if not isinstance(days, int) or days < 0:
        raise SpineValidationError(f"find_stale_priced_entries days must be >= 0; got {days!r}")
    if not isinstance(limit, int) or limit < 1 or limit > 100_000:
        raise SpineValidationError(
            f"find_stale_priced_entries limit must be 1..100000; got {limit!r}"
        )
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM spine_catalog "
            "WHERE last_priced_at IS NOT NULL "
            "AND last_priced_at < ? "
            "ORDER BY last_priced_at ASC LIMIT ?",
            (cutoff, limit),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


# ──────────────────────────────────────────────────────────────────────
# Enrichment write path (task #23 substrate; fetcher lives elsewhere)
# ──────────────────────────────────────────────────────────────────────


def record_enrichment(
    db_path: str | Path,
    *,
    mfg_number: str,
    source_url: str | None = None,
    photo_url: str | None = None,
    photo_path: str | None = None,
    status: str = ENRICHMENT_FETCHED,
    actor: str,
) -> dict:
    """Record the result of a URL / photo enrichment attempt.

    Called by the background fetcher (separate module/process) after
    it tries to look up a catalog entry's product page + thumbnail.
    Updates source_url / photo_url / photo_path and sets
    enrichment_status to FETCHED or FAILED.

    Returns the updated entry's metadata. Raises on missing entry
    or invalid status.
    """
    if status not in (ENRICHMENT_FETCHED, ENRICHMENT_FAILED, ENRICHMENT_PENDING):
        raise SpineValidationError(
            f"record_enrichment status must be one of "
            f"{ENRICHMENT_PENDING}/{ENRICHMENT_FETCHED}/{ENRICHMENT_FAILED}; "
            f"got {status!r}"
        )
    if not actor or not actor.strip():
        raise SpineValidationError("record_enrichment requires non-empty actor.")
    norm = _norm_mfg(mfg_number)
    if norm is None:
        raise SpineValidationError("record_enrichment requires non-empty mfg_number.")

    catalog_id = _catalog_id_for(norm)
    now_iso = datetime.now(timezone.utc).isoformat()

    with _WRITE_LOCK:
        with _connect(db_path) as conn:
            row = conn.execute(
                "SELECT * FROM spine_catalog WHERE catalog_id = ?",
                (catalog_id,),
            ).fetchone()
            if row is None:
                raise SpineValidationError(
                    f"record_enrichment: no catalog entry for {norm!r}"
                )
            _persist_catalog_entry(
                conn,
                catalog_id=catalog_id,
                mfg_number=row["mfg_number"],
                canonical_description=row["canonical_description"],
                descriptions_json=row["descriptions_json"],
                uoms_seen_json=row["uoms_seen_json"],
                unspsc_codes_json=row["unspsc_codes_json"],
                seen_count=row["seen_count"],
                first_seen_at=row["first_seen_at"],
                last_seen_at=row["last_seen_at"],
                last_seen_quote_id=row["last_seen_quote_id"],
                last_priced_at=row["last_priced_at"],
                last_priced_quote_id=row["last_priced_quote_id"],
                last_priced_cents=row["last_priced_cents"],
                source_url=source_url if source_url is not None else row["source_url"],
                source_url_checked_at=now_iso,
                photo_url=photo_url if photo_url is not None else row["photo_url"],
                photo_path=photo_path if photo_path is not None else row["photo_path"],
                enrichment_status=status,
            )

    return {
        "catalog_id": catalog_id,
        "mfg_number": norm,
        "enrichment_status": status,
        "source_url_checked_at": now_iso,
    }
