"""Spine → legacy CCHCS packet adapter.

The Spine does NOT render the CCHCS Non-Cloud RFQ Packet from scratch.
It delegates to the legacy filler ``src/forms/cchcs_packet_filler.py``
(shipped 2026-04-13, verified 2026-05-20) which fills THE BUYER'S OWN
packet PDF — Path B: ``PdfReader(buyer_pdf) → PdfWriter(clone_from=...)``.
The buyer's packet is the per-quote template; the filler overlays only
the supplier-side fields (prices, identity, signature, checkboxes) and
splices the real attachments inline.

This module is the adapter: it maps a Spine ``Quote`` + ``EmailContract``
onto the legacy filler's ``(source_pdf, parsed, price_overrides)`` call
shape, and back to bytes.

Why an adapter and not a Spine renderer
---------------------------------------
As the Spine was built out (2026-05-16+) it grew its OWN from-scratch
agency-form renderers — ``src/spine/agency_forms/cchcs_703b.py`` /
``cchcs_704b.py`` / ``cchcs_bidpkg.py``. They reimplemented document
filling starting from blank templates and produced packets that failed
CCHCS responsiveness review (the 2026-05-18 "trash" output + Mike's
21-minute hand-finish). They did not need to exist — the legacy filler
already filled CCHCS packets correctly. Per
``handoff-2026-05-20-legacy-adapter-build`` the fix is this one adapter;
the Spine renderers were DELETED in PR-Job1-D (2026-05-27). The
operator-facing ``/forms/{703b,704b,bidpkg,packet}/pdf`` endpoints all
serve the output of this adapter (and its sibling ``forms_render.py``
for the standalone form set) — see ``src/api/modules/routes_spine.py``.

Dependency boundary
-------------------
The adapter is, by design, a boundary-crosser: it imports the legacy
``src/forms/cchcs_packet_{parser,filler}`` modules. That import is the
adapter's whole purpose and is the ONLY sanctioned legacy import from
``src/spine/``. The tokenize/Jaccard match helpers are inlined here
(mirroring ``src/agents/cchcs_pc_matcher``) so the adapter stays
self-contained and does not depend on legacy private symbols.

Hard requirement
----------------
The adapter fills the buyer's actual packet PDF. It therefore REQUIRES
``contract.attachment_refs`` to resolve to the buyer's CCHCS packet on
disk. If no contract is bound, or no packet PDF can be located, the
adapter fails loudly with ``ok=False`` and an actionable error — it
never silently produces a blank or wrong document.
"""
from __future__ import annotations

import logging
import os
import re
import tempfile
from typing import TYPE_CHECKING, Any

log = logging.getLogger("reytech.spine.packet_render")

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.spine.email_contract import EmailContract
    from src.spine.model import Quote


# ──────────────────────────────────────────────────────────────────────
# Source-PDF resolution — locate the buyer's CCHCS packet on disk.
# ──────────────────────────────────────────────────────────────────────


def _search_roots() -> list[str]:
    """Directories an ``attachment_refs`` entry may be relative to.

    A ref is stored by Spine ingest as a path or a bare filename; it may
    be relative to the repo root, the persistent ``DATA_DIR`` volume, the
    generated-output dir, or the legacy uploads dir. Resolution tries all
    of them so a packet ingested via any path is still locatable here.
    """
    roots: list[str] = []
    try:
        repo_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        roots.append(repo_root)
    except Exception as e:  # pragma: no cover - defensive
        log.debug("packet_render: repo-root resolution failed: %s", e)
    try:
        from src.core.paths import DATA_DIR, OUTPUT_DIR

        roots.append(str(DATA_DIR))
        roots.append(str(OUTPUT_DIR))
        roots.append(os.path.join(str(DATA_DIR), "uploads"))
    except Exception as e:  # pragma: no cover - paths optional in tests
        log.debug("packet_render: src.core.paths unavailable: %s", e)
    # De-dup while preserving order.
    seen: set[str] = set()
    uniq: list[str] = []
    for r in roots:
        if r and r not in seen:
            seen.add(r)
            uniq.append(r)
    return uniq


def _candidate_paths(ref: str) -> list[str]:
    """Every on-disk path an ``attachment_refs`` entry might denote."""
    if not ref:
        return []
    out = [ref]
    base = os.path.basename(ref)
    for root in _search_roots():
        out.append(os.path.join(root, ref))
        if base and base != ref:
            out.append(os.path.join(root, base))
    # De-dup, preserve order.
    seen: set[str] = set()
    uniq: list[str] = []
    for p in out:
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq


def _resolve_source_pdf(contract: "EmailContract") -> str | None:
    """Locate the buyer's CCHCS Non-Cloud RFQ packet PDF.

    Walks ``contract.attachment_refs``, resolving each against the known
    search roots. Prefers an attachment whose filename matches the CCHCS
    packet pattern (``looks_like_cchcs_packet``); among the candidates it
    returns the first that actually parses as a real packet (parser ok +
    at least one line item). Returns ``None`` when nothing qualifies.
    """
    refs = list(getattr(contract, "attachment_refs", None) or [])
    if not refs:
        return None

    try:
        from src.forms.cchcs_packet_parser import (
            looks_like_cchcs_packet,
            parse_cchcs_packet,
        )
    except Exception as e:  # pragma: no cover - import guard
        log.error("packet_render: cannot import cchcs_packet_parser: %s", e)
        return None

    existing: list[str] = []
    for ref in refs:
        for cand in _candidate_paths(ref):
            if (
                cand.lower().endswith(".pdf")
                and os.path.isfile(cand)
                and cand not in existing
            ):
                existing.append(cand)

    if not existing:
        return None

    # Pattern-matching packets first, then any other PDF as a fallback —
    # a buyer may name the packet oddly, but verify-by-parse is the real
    # gate either way.
    pattern_hits = [
        p for p in existing if looks_like_cchcs_packet(filename=os.path.basename(p))
    ]
    ordered = pattern_hits + [p for p in existing if p not in pattern_hits]

    for cand in ordered:
        try:
            parsed = parse_cchcs_packet(cand)
        except Exception as e:  # pragma: no cover - defensive
            log.debug("packet_render: parse probe failed for %s: %s", cand, e)
            continue
        if parsed.get("ok") and parsed.get("line_items"):
            return cand

    log.warning(
        "packet_render: %d attachment PDF(s) resolved but none parsed as a "
        "CCHCS packet: %s",
        len(existing), existing,
    )
    return None


# ──────────────────────────────────────────────────────────────────────
# Line-item matching — Spine Quote prices → packet rows.
# Mirrors src/agents/cchcs_pc_matcher (token/Jaccard), inlined so the
# adapter stays self-contained.
# ──────────────────────────────────────────────────────────────────────

_STOP_WORDS = {
    "the", "and", "for", "with", "pack", "of", "per", "ea", "each", "box",
    "pk", "set", "in", "by", "to", "is", "it", "at", "on", "or", "an", "as",
    "a", "new", "used", "size", "color",
}

# Token-overlap confidence floor. Mirrors cchcs_pc_matcher's 0.60 — the
# packet row and the Spine quote line both descend from the SAME buyer
# email, so descriptions are near-identical and 0.60 is conservative.
_DESC_MATCH_FLOOR = 0.60


def _tokens(text: str) -> set[str]:
    if not text:
        return set()
    cleaned = re.sub(r"[^\w\s]", " ", str(text).lower())
    return {t for t in cleaned.split() if len(t) >= 2 and t not in _STOP_WORDS}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _normalize_mfg(s: str | None) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", "", str(s)).upper()


def _match_quote_lines_to_packet(
    quote: "Quote",
    parsed: dict[str, Any],
) -> tuple[dict[int, dict[str, float]], list[dict[str, Any]]]:
    """Map Spine quote line prices onto the parsed packet's rows.

    Returns ``(price_overrides, report)`` where ``price_overrides`` is the
    shape ``fill_cchcs_packet`` expects — ``{row_index: {"unit_price":
    float}}`` keyed by the packet row's 1-based ``row_index`` — and
    ``report`` is a per-row audit trail (strategy + confidence).

    Matching is description-primary per CLAUDE.md ("Match items by
    description … positional fallback"):
      1. MFG# exact (normalized, len ≥ 3) — confidence 1.0.
      2. Description Jaccard ≥ 0.60 — best unused quote line.
      3. Positional — packet ``row_index`` == quote ``line_no``.

    Each quote line is consumed at most once. Unmatched rows and rows
    whose matched quote line has ``unit_price_cents == 0`` are OMITTED
    from ``price_overrides`` — per Reytech standard the filler leaves
    those packet cells blank so the operator sees the gap before send.
    """
    packet_items = list(parsed.get("line_items") or [])
    quote_lines = list(quote.line_items)
    used: set[int] = set()  # quote line_no values already consumed
    overrides: dict[int, dict[str, float]] = {}
    report: list[dict[str, Any]] = []

    def _commit(row: int, li, strategy: str, confidence: float) -> None:
        used.add(li.line_no)
        entry: dict[str, Any] = {
            "row_index": row,
            "quote_line_no": li.line_no,
            "strategy": strategy,
            "confidence": round(confidence, 2),
            "unit_price": li.unit_price_cents / 100.0,
        }
        if li.unit_price_cents > 0:
            overrides[row] = {"unit_price": li.unit_price_cents / 100.0}
        else:
            entry["strategy"] = f"{strategy}+unpriced_skip"
        report.append(entry)

    unmatched: list[dict[str, Any]] = []

    # Pass 1 — MFG# exact.
    for item in packet_items:
        row = int(item.get("row_index", 0) or 0)
        if row < 1:
            continue
        target = _normalize_mfg(item.get("mfg_number") or item.get("part_number"))
        hit = None
        if target and len(target) >= 3:
            for li in quote_lines:
                if li.line_no in used:
                    continue
                if _normalize_mfg(li.mfg_number) == target:
                    hit = li
                    break
        if hit is not None:
            _commit(row, hit, "mfg_number", 1.0)
        else:
            unmatched.append(item)

    # Pass 2 — description Jaccard.
    still: list[dict[str, Any]] = []
    for item in unmatched:
        row = int(item.get("row_index", 0) or 0)
        target_tokens = _tokens(item.get("description") or "")
        best_li = None
        best_score = 0.0
        if len(target_tokens) >= 2:
            for li in quote_lines:
                if li.line_no in used:
                    continue
                score = _jaccard(target_tokens, _tokens(li.description))
                if score > best_score:
                    best_score, best_li = score, li
        if best_li is not None and best_score >= _DESC_MATCH_FLOOR:
            _commit(row, best_li, "description", best_score)
        else:
            still.append(item)

    # Pass 3 — positional (packet row_index == quote line_no).
    for item in still:
        row = int(item.get("row_index", 0) or 0)
        hit = next(
            (li for li in quote_lines if li.line_no == row and li.line_no not in used),
            None,
        )
        if hit is not None:
            _commit(row, hit, "positional", 0.50)
        else:
            report.append({
                "row_index": row,
                "quote_line_no": None,
                "strategy": "unmatched",
                "confidence": 0.0,
                "unit_price": 0.0,
            })

    report.sort(key=lambda r: r["row_index"])
    return overrides, report


# ──────────────────────────────────────────────────────────────────────
# Public API — the adapter.
# ──────────────────────────────────────────────────────────────────────


def render_cchcs_packet_via_legacy(
    quote: "Quote",
    contract: "EmailContract | None",
    *,
    output_dir: str | None = None,
    strict: bool = True,
) -> dict[str, Any]:
    """Render the filled CCHCS Non-Cloud RFQ packet for a Spine quote.

    Delegates to the legacy ``fill_cchcs_packet`` after locating the
    buyer's packet PDF and mapping the Spine quote's operator-validated
    prices onto its rows.

    Args:
        quote:      Validated Spine ``Quote`` (source of truth for prices,
                    quantities, the tax rate, and the display number).
        contract:   The ``EmailContract`` that drove ingest. REQUIRED —
                    its ``attachment_refs`` locate the buyer's packet PDF.
        output_dir: Where the legacy filler writes ``<base>_Reytech.pdf``.
                    ``None`` → a private temp dir (the bytes are returned
                    in ``pdf_bytes`` regardless).
        strict:     Forwarded to ``fill_cchcs_packet``. ``True`` makes a
                    gate failure set ``ok=False``; ``False`` (preview)
                    keeps ``ok=True`` with the gate report attached so
                    the operator can still eyeball the rendered packet.

    Returns:
        ``{
            "ok": bool,                # adapter + fill + (strict) gate ok
            "error": str,              # actionable detail when not ok
            "source_pdf": str,         # the buyer packet PDF that was filled
            "output_path": str,        # filled <base>_Reytech.pdf on disk
            "pdf_bytes": bytes,        # the filled packet bytes (may be
                                       #   present even on gate failure)
            "match_report": [...],     # per-row price-match audit trail
            "fill_result": {...},      # the raw fill_cchcs_packet dict
        }``
    """
    result: dict[str, Any] = {
        "ok": False,
        "error": "",
        "source_pdf": "",
        "output_path": "",
        "pdf_bytes": b"",
        "match_report": [],
        "fill_result": {},
    }

    if contract is None:
        result["error"] = (
            "no EmailContract is bound to this quote. The CCHCS packet "
            "adapter fills the BUYER'S packet PDF and needs "
            "contract.attachment_refs to locate it. Ingest the quote via "
            "the Spine email-contract path, or POST /contract-override "
            "with the packet attachment, before requesting the packet."
        )
        return result

    source_pdf = _resolve_source_pdf(contract)
    if not source_pdf:
        result["error"] = (
            "could not locate the buyer's CCHCS packet PDF. "
            f"contract.attachment_refs={list(contract.attachment_refs)!r} "
            "resolved to no parseable CCHCS packet on disk. The packet "
            "PDF the buyer emailed must be stored and referenced in "
            "attachment_refs — the filler fills that exact document."
        )
        return result
    result["source_pdf"] = source_pdf

    try:
        from src.forms.cchcs_packet_parser import parse_cchcs_packet
        from src.forms.cchcs_packet_filler import fill_cchcs_packet
    except Exception as e:  # pragma: no cover - import guard
        result["error"] = f"legacy filler import failed: {e}"
        return result

    parsed = parse_cchcs_packet(source_pdf)
    if not parsed.get("ok"):
        result["error"] = (
            f"CCHCS packet parse failed for {os.path.basename(source_pdf)}: "
            f"{parsed.get('error')}"
        )
        return result

    overrides, report = _match_quote_lines_to_packet(quote, parsed)
    result["match_report"] = report

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="spine_packet_")

    # The Spine quote's tax_rate_bps is operator-validated at ingest and
    # is the substrate's source of truth — flow it INTO the filler so the
    # packet's totals page can't drift to a different (zip-derived) rate
    # than the rest of the response. tax_rate_bps is basis points; the
    # filler wants a fraction (875 bps → 0.0875). It is ALWAYS passed —
    # including 0 — so the substrate rate wins unconditionally over the
    # filler's CDTFA fallback (a priced/finalized quote always has
    # tax_rate_bps > 0 by the Quote model validator; a parsed-status
    # preview with 0 honestly renders $0 tax rather than a zip guess).
    tax_rate = quote.tax_rate_bps / 10_000.0

    try:
        fill_result = fill_cchcs_packet(
            source_pdf=source_pdf,
            parsed=parsed,
            output_dir=output_dir,
            price_overrides=overrides,
            quote_number=quote.display_number or quote.quote_id,
            tax_rate=tax_rate,
            strict=strict,
        )
    except Exception as e:  # pragma: no cover - filler is defensive itself
        log.exception("packet_render: fill_cchcs_packet crashed for %s", quote.quote_id)
        result["error"] = f"fill_cchcs_packet crashed: {e}"
        return result

    result["fill_result"] = fill_result
    result["ok"] = bool(fill_result.get("ok"))
    result["output_path"] = fill_result.get("output_path") or ""
    if not result["ok"]:
        result["error"] = fill_result.get("error", "") or "fill returned ok=False"

    # Read the bytes back even on a gate failure — a gate-flagged packet
    # is exactly what the operator needs to SEE to diagnose. The hard
    # gate lives at snapshot/send, not at this preview render.
    out_path = result["output_path"]
    if out_path and os.path.isfile(out_path):
        try:
            with open(out_path, "rb") as fh:
                result["pdf_bytes"] = fh.read()
        except Exception as e:  # pragma: no cover - defensive
            log.warning("packet_render: could not read filled packet %s: %s",
                        out_path, e)

    log.info(
        "packet_render: quote=%s sol=%s source=%s ok=%s rows_priced=%s "
        "matched=%d/%d total=$%.2f",
        quote.quote_id,
        quote.solicitation_number,
        os.path.basename(source_pdf),
        result["ok"],
        fill_result.get("rows_priced"),
        len(overrides),
        len(report),
        float(fill_result.get("grand_total") or 0.0),
    )
    return result


__all__ = ["render_cchcs_packet_via_legacy"]
