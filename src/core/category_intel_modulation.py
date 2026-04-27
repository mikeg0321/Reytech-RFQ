"""Phase 4.7: Wire category-intel danger signal into the pricing engine.

After PRs #573-#580 the danger/WIN signal is *available* via API and
*visible* in the modal. This module is the next compounding step:
makes the engine USE the signal instead of just displaying it.

Three flavors (selected via env `CATEGORY_INTEL_FLAVOR` or runtime flag):

  A — auto_lower: when danger=true, multiply engine markup by 0.5x
                  (capped at +5% over cost). Highest leverage, riskiest.
                  No operator interaction; the engine just returns a
                  tighter price.

  B — suggest:    [DEFAULT] return engine rec unchanged + add a
                  `category_intel.suggested_alternative` field with the
                  damped markup. UI/operator chooses whether to swap.
                  Additive, reversible, lowest risk.

  C — block:      when danger=true and rate < 8%, set block=True so
                  the caller can render a hard "DO NOT BID" gate.
                  Engine still returns the original price for visibility.

`apply_category_intel()` is idempotent. Calling it on a result that
was already modulated does nothing (the `category_intel` field's
presence is the marker).

Live thresholds match `routes_oracle_category_intel.py`:
  - danger when rate < 15% AND quotes >= 5
  - WIN BUCKET when rate >= 50% AND quotes >= 5
  - hard-block (flavor C only) when rate < 8% AND quotes >= 10
"""

import json
import logging
import os

from src.core.intel_categories import intel_category

log = logging.getLogger("reytech.category_intel_modulation")


# ── Tunables ──────────────────────────────────────────────────────

DAMP_FACTOR = 0.5
"""Multiplier applied to engine markup_pct when bucket is a known
loser. 22% → 11%. Hand-tuned starting point — telemetry on operator
acceptance will inform whether to learn this from data."""

MIN_MARKUP_OVER_COST_PCT = 5.0
"""Floor: even on the worst loss buckets, never recommend dropping
below cost+5%. Otherwise we just bleed money on every sale."""

DANGER_RATE_THRESHOLD = 15.0
WIN_RATE_THRESHOLD = 50.0
HARD_BLOCK_RATE_THRESHOLD = 8.0
MIN_QUOTES_FOR_DANGER = 5
MIN_QUOTES_FOR_BLOCK = 10


# ── Flavor selection ──────────────────────────────────────────────

FLAG_KEY = "oracle.category_intel_flavor"
"""Runtime feature flag key — overrides env var. Set via
POST /api/admin/flags with body {"key": "oracle.category_intel_flavor",
"value": "A"|"B"|"C"|"OFF"} to flip in <60s without a redeploy."""


def _get_active_flavor() -> str:
    """Resolve flavor with this precedence:
        1. Runtime feature flag `oracle.category_intel_flavor`
        2. Env var `CATEGORY_INTEL_FLAVOR`
        3. Default 'B' (suggest only — safest)

    Unknown values fall through to 'B'.
    """
    # Try runtime flag first (defensive — flags layer is best-effort)
    try:
        from src.core.flags import get_flag
        flag_val = get_flag(FLAG_KEY, "")
        if flag_val:
            raw = str(flag_val).strip().upper()
            if raw in ("A", "B", "C", "OFF"):
                return raw
    except Exception as e:
        log.debug("flag lookup skipped: %s", e)

    # Fall back to env
    raw = (os.environ.get("CATEGORY_INTEL_FLAVOR") or "B").strip().upper()
    if raw not in ("A", "B", "C", "OFF"):
        return "B"
    return raw


def _get_flavor_source() -> tuple[str, str]:
    """Return (flavor, source). source is 'flag'|'env'|'default'."""
    try:
        from src.core.flags import get_flag
        flag_val = get_flag(FLAG_KEY, "")
        if flag_val:
            raw = str(flag_val).strip().upper()
            if raw in ("A", "B", "C", "OFF"):
                return (raw, "flag")
    except Exception:
        pass
    env_val = os.environ.get("CATEGORY_INTEL_FLAVOR")
    if env_val:
        raw = env_val.strip().upper()
        if raw in ("A", "B", "C", "OFF"):
            return (raw, "env")
    return ("B", "default")


# ── Public entry point ────────────────────────────────────────────

def apply_category_intel(rec: dict, description: str, agency: str,
                         db, cost: float | None = None) -> dict:
    """Augment engine recommendation `rec` with category-intel signal.

    Args:
      rec:         the dict returned by `_calculate_recommendation()`.
                   Mutated in-place if flavor=A; otherwise just gets
                   a new `category_intel` sub-dict.
      description: the line-item description being priced.
      agency:      buyer/agency name (used to filter category lookup).
      db:          live sqlite connection (won_quotes_kb / quotes table).
      cost:        unit cost (used to floor the suggested markup).

    Returns:
      The modified `rec`. Always includes `rec["category_intel"]` —
      even when no signal fires (`category: 'uncategorized'` etc).
    """
    flavor = _get_active_flavor()
    if flavor == "OFF":
        rec["category_intel"] = {"category": None, "active": False,
                                 "flavor": "OFF"}
        return rec

    # Idempotent guard
    if isinstance(rec.get("category_intel"), dict) \
       and rec["category_intel"].get("processed"):
        return rec

    cat_id, cat_label = intel_category(description)
    intel = {
        "category": cat_id,
        "category_label": cat_label,
        "flavor": flavor,
        "processed": True,
        "danger": False,
        "win_bucket": False,
        "warning_text": None,
        "quotes": 0, "wins": 0, "losses": 0, "win_rate_pct": None,
    }

    if cat_id == "uncategorized":
        rec["category_intel"] = intel
        return rec

    # Look up the bucket's historical stats
    try:
        stats = _bucket_stats(db, cat_id, agency)
    except Exception as e:
        log.debug("bucket lookup failed: %s", e)
        rec["category_intel"] = intel
        return rec

    intel.update(stats)
    rate = stats.get("win_rate_pct")
    quotes = stats.get("quotes", 0)

    # Decide signal
    if quotes >= MIN_QUOTES_FOR_DANGER and rate is not None:
        if rate < DANGER_RATE_THRESHOLD:
            intel["danger"] = True
            intel["warning_text"] = (
                f"LOSS BUCKET: {stats['wins']}/{quotes} wins on "
                f"{cat_label}. Recalibrate markup before bidding."
            )
        elif rate >= WIN_RATE_THRESHOLD:
            intel["win_bucket"] = True
            intel["warning_text"] = (
                f"WIN BUCKET: {stats['wins']}/{quotes} wins on "
                f"{cat_label}. Confident territory."
            )

    # Apply flavor
    if intel["danger"]:
        if flavor == "A":
            _apply_flavor_a(rec, intel, cost)
        elif flavor == "B":
            _apply_flavor_b(rec, intel, cost)
        elif flavor == "C":
            _apply_flavor_c(rec, intel, cost, rate)

    rec["category_intel"] = intel
    return rec


# ── Bucket lookup ─────────────────────────────────────────────────

def _bucket_stats(db, cat_id: str, agency: str) -> dict:
    """Return {quotes, wins, losses, win_rate_pct} for the category
    bucket. Mirrors the rollup in routes_oracle_category_intel.py
    so danger thresholds fire identically across read paths."""
    sql = """
        SELECT status, agency, institution, line_items
        FROM quotes
        WHERE is_test = 0
          AND status IN ('won', 'lost')
          AND line_items IS NOT NULL
    """
    rows = db.execute(sql).fetchall()
    agency_lc = (agency or "").lower()

    quotes = wins = losses = 0
    for r in rows:
        if agency_lc:
            row_a = ((r[1] if isinstance(r, (tuple, list)) else r["agency"]) or
                     (r[2] if isinstance(r, (tuple, list)) else r["institution"]) or "")
            if agency_lc not in row_a.lower():
                continue
        try:
            if isinstance(r, (tuple, list)):
                items = json.loads(r[3] or "[]")
                status = r[0]
            else:
                items = json.loads(r["line_items"] or "[]")
                status = r["status"]
        except (TypeError, ValueError):
            continue
        if not isinstance(items, list):
            continue
        # Per-quote bucket dedup: a quote with 5 footwear items counts once
        for it in items:
            if not isinstance(it, dict):
                continue
            rid, _ = intel_category(it.get("description") or "")
            if rid == cat_id:
                quotes += 1
                if status == "won":
                    wins += 1
                elif status == "lost":
                    losses += 1
                break
    decided = wins + losses
    rate = round(100.0 * wins / decided, 1) if decided else None
    return {"quotes": quotes, "wins": wins, "losses": losses,
            "win_rate_pct": rate}


# ── Flavor implementations ────────────────────────────────────────

def _apply_flavor_a(rec: dict, intel: dict, cost: float | None) -> None:
    """A: silently lower the engine's markup. Mutates rec in-place."""
    current = rec.get("markup_pct")
    if current is None:
        return
    new_markup = max(MIN_MARKUP_OVER_COST_PCT, float(current) * DAMP_FACTOR)
    rec["markup_pct_pre_intel"] = current
    rec["markup_pct"] = round(new_markup, 1)
    if cost and cost > 0:
        rec["quote_price_pre_intel"] = rec.get("quote_price")
        rec["quote_price"] = round(cost * (1 + new_markup / 100.0), 2)
    rec["rationale"] = (rec.get("rationale", "") +
                        f" [Auto-lowered from {current}% — "
                        f"{intel['category_label']} bucket has "
                        f"{intel['wins']}/{intel['quotes']} wins.]").strip()
    intel["action"] = "auto_lowered"
    intel["damped_to_pct"] = round(new_markup, 1)


def _apply_flavor_b(rec: dict, intel: dict, cost: float | None) -> None:
    """B: don't change rec; attach a `suggested_alternative` field that
    the UI can render as a 'swap to this price' option."""
    current = rec.get("markup_pct")
    if current is None:
        intel["action"] = "no_alternative_no_baseline"
        return
    suggested = max(MIN_MARKUP_OVER_COST_PCT, float(current) * DAMP_FACTOR)
    alt = {
        "markup_pct": round(suggested, 1),
        "rationale": (
            f"Bucket has {intel['wins']}/{intel['quotes']} wins "
            f"({intel['win_rate_pct']}%). "
            f"Damping engine's {current}% by {int((1-DAMP_FACTOR)*100)}% to compete."
        ),
    }
    if cost and cost > 0:
        alt["quote_price"] = round(cost * (1 + suggested / 100.0), 2)
    intel["suggested_alternative"] = alt
    intel["action"] = "suggested_only"


def _apply_flavor_c(rec: dict, intel: dict, cost: float | None,
                    rate: float | None) -> None:
    """C: hard-block when bucket is *severely* loss (< 8%, n >= 10)."""
    if (rate is not None and rate < HARD_BLOCK_RATE_THRESHOLD
            and intel.get("quotes", 0) >= MIN_QUOTES_FOR_BLOCK):
        intel["action"] = "blocked"
        intel["block"] = True
        intel["block_reason"] = (
            f"DO NOT BID — bucket has {intel['wins']}/{intel['quotes']} "
            f"wins ({rate}%). Override required."
        )
    else:
        # Below the hard-block bar but still danger; fall through to B.
        _apply_flavor_b(rec, intel, cost)
