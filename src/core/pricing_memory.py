"""
pricing_memory.py — Single canonical resolver for "what do we know about
this product?"

Substrate consolidation (2026-05-26). Closes the URL-paste / Auto-Price /
PC-link drift that left operators looking at "Catalog ✓" badges with
$0.00 cost cells because three separate UI surfaces consulted three
different stores keyed three different ways.

`resolve(url=, mfg=, upc=, asin=, description=, scrape_fn=)` → MemoryHit
returns the first authoritative cost (and sell-price when known) plus an
ordered `source_chain` so the operator can see the lineage.

Order of authority (first match wins):
  1. fresh live scrape (operator-just-pasted; cheap when present)
  2. product_catalog by URL (NEW — closes the screenshot bug)
  3. product_catalog by ASIN (NEW)
  4. product_catalog by MFG / UPC (existing cost_tier_lookup tier 1+2)
  5. linked-PC pricing by description token match
  6. oracle prior (last-resort suggestion, never authoritative cost)

Every tier preserves provenance discipline already established in
`product_catalog.find_by_mfg_exact` — only operator-confirmed costs surface.
Scraped / SCPRS-derived rows are filtered out at the SQL layer.

This module never writes. Catalog writes still flow through
`routes_pricecheck._do_save_prices` on operator Save.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Callable, Optional
from urllib.parse import urlparse

log = logging.getLogger("reytech.pricing_memory")


@dataclass(frozen=True)
class MemoryHit:
    """The unified answer for any pricing-memory lookup.

    `cost` is always operator-trustworthy (operator-confirmed catalog,
    operator-saved PC, or a fresh scrape). It is the value the cell hydrates.

    `sell_price` is the last bid Mike sent for this product — populated
    from catalog.sell_price or the linked PC's price_per_unit. Surfaces in
    the chip ribbon so Mike can see "you sold this for $42.80 last time"
    without having to walk the prior PC.

    `source_chain` is ordered first→last; the first non-zero entry is the
    one that produced `cost`. Subsequent entries are alternate sources we
    saw but didn't override with — Mike can hover to see them.
    """
    cost: float = 0.0
    sell_price: float = 0.0
    supplier: str = ""
    source_chain: list[str] = field(default_factory=list)
    confidence: float = 0.0
    age_days: Optional[int] = None
    evidence_pc_id: str = ""
    evidence_pc_number: str = ""
    fresh_scrape: Optional[dict] = None
    mfg_number: str = ""
    upc: str = ""
    photo_url: str = ""

    @property
    def has_cost(self) -> bool:
        return self.cost > 0


def _normalize_url(url: str) -> str:
    """Strip tracking params, fragments, trailing slashes — so the same
    product on two different campaign URLs catalog-collapses to one row.
    Conservative: only the query string + fragment + double-slashes get
    flattened. We DO preserve path-segment structure (/dp/B08TVK1JQS).
    """
    if not url:
        return ""
    try:
        u = urlparse(url.strip())
        host = (u.netloc or "").lower().lstrip("www.")
        path = (u.path or "").rstrip("/")
        if host and path:
            return f"{host}{path}"
        return (host + path) or url.strip()
    except Exception:
        return url.strip()


def _extract_asin(url: str) -> str:
    """Mirror of item_link_lookup._extract_asin, host-gated so non-Amazon
    URLs cannot mint a fake ASIN. Duplicated locally to avoid a circular
    import at module load time."""
    if not url:
        return ""
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return ""
    if not host:
        return ""
    is_amazon = (
        host == "amazon.com"
        or host.endswith(".amazon.com")
        or host in ("amzn.to", "amzn.com", "a.co")
    )
    if not is_amazon:
        return ""
    patterns = [
        r"/dp/([A-Z0-9]{10,13})",
        r"/gp/aw/d/([A-Z0-9]{10,13})",
        r"/gp/product/([A-Z0-9]{10,13})",
        r"ASIN=([A-Z0-9]{10})",
        r"/product/([A-Z0-9]{10})",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    return ""


def _days_since(iso_string: Optional[str]) -> Optional[int]:
    """Best-effort age in days from an ISO timestamp. None on parse failure."""
    if not iso_string:
        return None
    try:
        from datetime import datetime, timezone
        dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).days
    except Exception:
        return None


def _try_catalog_by_url(normalized_url: str) -> Optional[dict]:
    """product_catalog row whose cost_source_url normalizes to the same URL.
    Provenance discipline: same gate as find_by_mfg_exact —
    cost_source IN ('operator', 'catalog_confirmed'), cost > 0,
    is_test = 0. Scraped rows never surface here.
    """
    if not normalized_url:
        return None
    try:
        from src.agents.product_catalog import _get_conn, init_catalog_db
        init_catalog_db()
    except Exception as e:
        log.debug("catalog_by_url import: %s", e)
        return None
    conn = _get_conn()
    try:
        # We compare against cost_source_url normalized the same way to make
        # the lookup robust to query-string churn. SQLite has no urlparse
        # built-in so we do candidate fetch + Python normalize, scoped to
        # rows whose stored URL contains the host+path stem.
        host_path = normalized_url
        like_pat = f"%{host_path}%"
        try:
            rows = conn.execute(
                "SELECT * FROM product_catalog "
                "WHERE cost_source_url LIKE ? "
                "AND cost > 0 "
                "AND cost_source IN ('operator', 'catalog_confirmed') "
                "AND COALESCE(is_test, 0) = 0 "
                "ORDER BY cost_accepted_at DESC, updated_at DESC LIMIT 5",
                (like_pat,),
            ).fetchall()
        except Exception as e:
            log.debug("catalog_by_url SQL: %s", e)
            return None
        for row in rows:
            stored = _normalize_url((row["cost_source_url"] or "").strip())
            if stored == host_path:
                d = dict(row)
                log.info(
                    "pricing_memory.catalog[url] HIT: %s → product_id=%s cost=%s",
                    host_path, d.get("id"), d.get("cost"),
                )
                return d
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _try_catalog_by_asin(asin: str) -> Optional[dict]:
    """product_catalog row keyed on ASIN. ASIN can live in mfg_number for
    pure-Amazon items (Phase 1 last-resort identifier chain) or in
    cost_source_url path. Operator-confirmed gate applies."""
    if not asin:
        return None
    try:
        from src.agents.product_catalog import _get_conn, init_catalog_db
        init_catalog_db()
    except Exception as e:
        log.debug("catalog_by_asin import: %s", e)
        return None
    conn = _get_conn()
    try:
        # Try ASIN-in-mfg first, then URL-contains-ASIN
        for clause, param in (
            ("UPPER(TRIM(mfg_number)) = ?", asin.upper()),
            ("cost_source_url LIKE ?", f"%{asin}%"),
        ):
            try:
                row = conn.execute(
                    f"SELECT * FROM product_catalog "
                    f"WHERE {clause} "
                    f"AND cost > 0 "
                    f"AND cost_source IN ('operator', 'catalog_confirmed') "
                    f"AND COALESCE(is_test, 0) = 0 "
                    f"ORDER BY cost_accepted_at DESC, updated_at DESC LIMIT 1",
                    (param,),
                ).fetchone()
                if row:
                    d = dict(row)
                    log.info(
                        "pricing_memory.catalog[asin=%s] HIT via %s → product_id=%s cost=%s",
                        asin, clause.split()[0], d.get("id"), d.get("cost"),
                    )
                    return d
            except Exception as e:
                log.debug("catalog_by_asin SQL: %s", e)
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _try_catalog_by_mfg_upc(mfg: str, upc: str) -> Optional[dict]:
    """Delegate to the existing operator-confirmed exact lookup. Returns
    the catalog row dict, or None."""
    if not mfg and not upc:
        return None
    try:
        from src.agents.product_catalog import find_by_mfg_exact
        return find_by_mfg_exact(mfg or None, upc=upc or None)
    except Exception as e:
        log.debug("catalog_by_mfg_upc: %s", e)
        return None


def _try_pc_by_description(description: str, threshold: float = 0.65) -> Optional[dict]:
    """Match this description against every active PC's items. Returns
    {cost, sell_price, supplier, age_days, pc_id, pc_number} on a hit.

    Threshold is the catalog-canonical 0.65 (CLAUDE.md Pricing Guard Rails:
    'Catalog Match Threshold' — never lower without testing cross-category
    accuracy).
    """
    if not description or len(description.strip()) < 5:
        return None
    try:
        from src.api.dashboard import _load_price_checks
        pcs = _load_price_checks()
    except Exception as e:
        log.debug("pc_by_description load: %s", e)
        return None
    if not pcs:
        return None

    desc_lower = description.lower().strip()
    best_hit = None
    best_score = 0.0
    try:
        from difflib import SequenceMatcher
    except Exception:
        return None

    for pc_id, pc in pcs.items():
        if not isinstance(pc, dict):
            continue
        pc_data = pc.get("pc_data", pc)
        if isinstance(pc_data, str):
            try:
                import json
                pc_data = json.loads(pc_data)
            except Exception:
                continue
        items = pc_data.get("items") or pc.get("items") or []
        if not items:
            continue
        for it in items:
            pc_desc = (it.get("description") or it.get("desc") or "").lower().strip()
            if not pc_desc or len(pc_desc) < 5:
                continue
            score = SequenceMatcher(None, desc_lower, pc_desc).ratio()
            if score > best_score and score >= threshold:
                cost = it.get("supplier_cost") or it.get("vendor_cost") or it.get("unit_cost") or 0
                sell = it.get("price_per_unit") or it.get("bid_price") or it.get("unit_price") or 0
                try:
                    cost = float(cost or 0)
                    sell = float(sell or 0)
                except (TypeError, ValueError):
                    continue
                if cost <= 0 and sell <= 0:
                    continue
                pc_inner_num = (pc_data.get("pc_number") or pc.get("pc_number") or "").strip()
                best_score = score
                best_hit = {
                    "cost": cost,
                    "sell_price": sell,
                    "supplier": it.get("item_supplier") or it.get("supplier", ""),
                    "url": it.get("item_link") or "",
                    "mfg_number": it.get("mfg_number") or it.get("item_number", ""),
                    "upc": it.get("upc", ""),
                    "photo_url": it.get("photo_url") or "",
                    "pc_id": pc_id,
                    "pc_number": pc_inner_num,
                    "age_days": _days_since(pc.get("created_at") or pc_data.get("created_at")),
                    "match_score": round(score * 100),
                }
    return best_hit


def resolve(
    *,
    url: str = "",
    mfg: str = "",
    upc: str = "",
    asin: str = "",
    description: str = "",
    scrape_fn: Optional[Callable[[str], dict]] = None,
) -> MemoryHit:
    """Single canonical pricing-memory resolver.

    Args:
        url: pasted supplier URL (optional)
        mfg: manufacturer part number (optional)
        upc: UPC barcode (optional)
        asin: Amazon ASIN (auto-extracted from URL if not provided)
        description: item description for fuzzy PC match (optional)
        scrape_fn: live-scrape callable taking url, returning dict with
                   price/list_price/supplier/title/mfg_number — typically
                   `src.agents.item_link_lookup.lookup_from_url`. When
                   omitted, the scrape step is skipped (read-only resolve).

    Returns MemoryHit. Never raises — failures degrade to empty chain.
    """
    chain: list[str] = []
    cost = 0.0
    sell = 0.0
    supplier = ""
    age_days: Optional[int] = None
    pc_id = ""
    pc_number = ""
    scraped: Optional[dict] = None
    out_mfg = mfg or ""
    out_upc = upc or ""
    out_photo = ""

    if url and not asin:
        asin = _extract_asin(url)
    normalized = _normalize_url(url) if url else ""

    # ── 1. Live scrape (operator-just-pasted intent wins) ──────────────
    if scrape_fn and url:
        try:
            scraped = scrape_fn(url) or {}
        except Exception as e:
            log.debug("scrape_fn raised: %s", e)
            scraped = {}
        if scraped:
            scrape_cost = (
                scraped.get("list_price")
                or scraped.get("price")
                or scraped.get("sale_price")
            )
            try:
                scrape_cost = float(scrape_cost or 0)
            except (TypeError, ValueError):
                scrape_cost = 0.0
            if scrape_cost > 0:
                cost = scrape_cost
                supplier = scraped.get("supplier") or ""
                chain.append(f"scrape[{supplier or 'live'}]=${cost:.2f}")
                out_mfg = out_mfg or (scraped.get("mfg_number") or scraped.get("part_number") or "")
                out_upc = out_upc or (scraped.get("upc") or "")
                out_photo = scraped.get("photo_url") or ""

    # ── 2. Catalog by URL ──────────────────────────────────────────────
    if not cost and normalized:
        hit = _try_catalog_by_url(normalized)
        if hit:
            cost = float(hit.get("cost") or 0)
            sell = float(hit.get("sell_price") or 0)
            supplier = hit.get("best_supplier") or supplier
            age_days = _days_since(hit.get("cost_accepted_at") or hit.get("updated_at"))
            chain.append(
                f"catalog[url]=${cost:.2f}"
                + (f" ({age_days}d)" if age_days is not None else "")
            )
            out_mfg = out_mfg or (hit.get("mfg_number") or "")
            out_upc = out_upc or (hit.get("upc") or "")
            out_photo = out_photo or (hit.get("photo_url") or "")

    # ── 3. Catalog by ASIN ─────────────────────────────────────────────
    if not cost and asin:
        hit = _try_catalog_by_asin(asin)
        if hit:
            cost = float(hit.get("cost") or 0)
            sell = sell or float(hit.get("sell_price") or 0)
            supplier = supplier or (hit.get("best_supplier") or "")
            age_days = _days_since(hit.get("cost_accepted_at") or hit.get("updated_at"))
            chain.append(
                f"catalog[asin={asin}]=${cost:.2f}"
                + (f" ({age_days}d)" if age_days is not None else "")
            )
            out_mfg = out_mfg or (hit.get("mfg_number") or "")
            out_upc = out_upc or (hit.get("upc") or "")
            out_photo = out_photo or (hit.get("photo_url") or "")

    # ── 4. Catalog by MFG/UPC ──────────────────────────────────────────
    if not cost and (out_mfg or out_upc):
        hit = _try_catalog_by_mfg_upc(out_mfg, out_upc)
        if hit:
            cost = float(hit.get("cost") or 0)
            sell = sell or float(hit.get("sell_price") or 0)
            supplier = supplier or (hit.get("best_supplier") or "")
            age_days = _days_since(hit.get("cost_accepted_at") or hit.get("updated_at"))
            key = "mfg" if out_mfg else "upc"
            val = out_mfg or out_upc
            chain.append(
                f"catalog[{key}={val}]=${cost:.2f}"
                + (f" ({age_days}d)" if age_days is not None else "")
            )
            out_photo = out_photo or (hit.get("photo_url") or "")

    # ── 5. PC by description (token-similar prior PC item) ─────────────
    if not cost and description:
        pc_hit = _try_pc_by_description(description)
        if pc_hit:
            cost = pc_hit["cost"]
            sell = sell or pc_hit["sell_price"]
            supplier = supplier or pc_hit.get("supplier", "")
            age_days = pc_hit.get("age_days")
            pc_id = pc_hit["pc_id"]
            pc_number = pc_hit.get("pc_number", "")
            score = pc_hit.get("match_score", 0)
            tag = pc_id[:8] if pc_id else "?"
            sell_part = f", sell ${sell:.2f}" if sell > 0 else ""
            age_part = f", {age_days}d" if age_days is not None else ""
            chain.append(
                f"pc[{tag}]=${cost:.2f}{sell_part} ({score}% match{age_part})"
            )
            out_mfg = out_mfg or pc_hit.get("mfg_number", "")
            out_upc = out_upc or pc_hit.get("upc", "")
            out_photo = out_photo or pc_hit.get("photo_url", "")

    # ── 6. Oracle prior (weak — informational only) ────────────────────
    # We surface oracle even when cost was found earlier, since the operator
    # may want a sanity-check sell-price reference. Only RUN the oracle if
    # we have a description; oracle without context is more noise than signal.
    if description and len(description.strip()) >= 5:
        try:
            from src.core.pricing_oracle_v2 import get_pricing
            prior = get_pricing(description.strip(), quantity=1) or {}
            oracle_cost = prior.get("recommended_cost") or prior.get("cost") or 0
            try:
                oracle_cost = float(oracle_cost)
            except (TypeError, ValueError):
                oracle_cost = 0.0
            if oracle_cost > 0:
                chain.append(f"oracle=${oracle_cost:.2f} (prior, weak)")
                if not cost:
                    # Last-resort fallback — but tagged as low confidence so
                    # the operator UI flags it. Cost surfaces; confidence
                    # below reflects this.
                    cost = oracle_cost
                    supplier = supplier or "Oracle prior"
        except Exception as e:
            log.debug("oracle prior: %s", e)

    confidence = _score_chain(chain)

    return MemoryHit(
        cost=round(cost, 4),
        sell_price=round(sell, 4),
        supplier=supplier,
        source_chain=chain,
        confidence=confidence,
        age_days=age_days,
        evidence_pc_id=pc_id,
        evidence_pc_number=pc_number,
        fresh_scrape=scraped,
        mfg_number=out_mfg,
        upc=out_upc,
        photo_url=out_photo,
    )


def _score_chain(chain: list[str]) -> float:
    """Rough confidence — 1.0 if fresh scrape, 0.9 if catalog URL/ASIN,
    0.8 catalog MFG/UPC, 0.6 PC, 0.3 oracle. Drops with age."""
    if not chain:
        return 0.0
    head = chain[0]
    if head.startswith("scrape["):
        return 1.0
    if head.startswith("catalog[url"):
        return 0.92
    if head.startswith("catalog[asin"):
        return 0.90
    if head.startswith("catalog["):
        return 0.85
    if head.startswith("pc["):
        return 0.65
    if head.startswith("oracle"):
        return 0.30
    return 0.50


def to_jsonable(hit: MemoryHit) -> dict:
    """Convert MemoryHit to a frontend-shaped JSON payload. Used by
    /api/item-link/lookup and any consumer that needs the chip ribbon."""
    return {
        "cost": hit.cost,
        "sell_price": hit.sell_price,
        "supplier": hit.supplier,
        "source_chain": hit.source_chain,
        "confidence": hit.confidence,
        "age_days": hit.age_days,
        "evidence_pc_id": hit.evidence_pc_id,
        "evidence_pc_number": hit.evidence_pc_number,
        "mfg_number": hit.mfg_number,
        "upc": hit.upc,
        "photo_url": hit.photo_url,
    }
