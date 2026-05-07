"""Auto-pricing TP/FP rate telemetry (#18, 2026-05-07).

The auto-pricer stamps `auto_priced_value` + `auto_priced_at` on every
line item it sets. Operators (Mike) then routinely override that value
before sending. This module measures how often the auto-priced value
"stuck" (TP) vs. was overridden (FP), so we know whether the pricer is
worth keeping.

Decision rule per line item:
  * TP (true positive)  — auto-priced and current `price_per_unit` ==
    `auto_priced_value` within 1¢ tolerance. Operator kept it.
  * FP (false positive) — auto-priced and current `price_per_unit` is
    different (or zero / missing). Operator overrode it.
  * SKIP — no `auto_priced_value` stamp on the item (not auto-priced).

Aggregated per RFQ via `compute_record_tp_fp(record)` and across many
RFQs via `summarise_jsonl(path)`.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Tuple


_TOLERANCE = 0.01  # treat sub-penny diffs as "kept"


def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def classify_item(item: Dict[str, Any]) -> str:
    """Classify a single line item as 'tp', 'fp', or 'skip'."""
    if not isinstance(item, dict):
        return "skip"
    auto = _coerce_float(item.get("auto_priced_value"))
    if auto is None or auto <= 0:
        return "skip"
    cur = _coerce_float(item.get("price_per_unit"))
    if cur is None or cur <= 0:
        return "fp"  # cleared = overridden
    if abs(cur - auto) <= _TOLERANCE:
        return "tp"
    return "fp"


def compute_record_tp_fp(record: Dict[str, Any]) -> Dict[str, Any]:
    """Walk a record's line_items and return per-source TP/FP counts.

    Returns:
        {
          "rid": "...",
          "status": "...",
          "auto_priced_count": int,
          "tp": int,
          "fp": int,
          "by_source": {"catalog_url": {"tp": ..., "fp": ...}, ...},
          "tp_rate": float | None,  # None when auto_priced_count == 0
        }
    """
    items = record.get("line_items") or record.get("items") or []
    tp = fp = 0
    by_source: Dict[str, Dict[str, int]] = {}
    for item in items:
        verdict = classify_item(item)
        if verdict == "skip":
            continue
        src = item.get("auto_priced_source") or "unknown"
        bucket = by_source.setdefault(src, {"tp": 0, "fp": 0})
        if verdict == "tp":
            tp += 1
            bucket["tp"] += 1
        else:
            fp += 1
            bucket["fp"] += 1
    total = tp + fp
    return {
        "rid": record.get("id", ""),
        "status": record.get("status", ""),
        "auto_priced_count": total,
        "tp": tp,
        "fp": fp,
        "by_source": by_source,
        "tp_rate": (tp / total) if total > 0 else None,
    }


def summarise_jsonl(path: str) -> Dict[str, Any]:
    """Aggregate a JSONL log of `compute_record_tp_fp` rows."""
    if not os.path.exists(path):
        return {"records": 0, "tp": 0, "fp": 0, "tp_rate": None,
                "by_source": {}}
    tp = fp = 0
    by_source: Dict[str, Dict[str, int]] = {}
    records = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except (ValueError, json.JSONDecodeError):
                continue
            records += 1
            tp += int(row.get("tp", 0) or 0)
            fp += int(row.get("fp", 0) or 0)
            for src, counts in (row.get("by_source") or {}).items():
                bucket = by_source.setdefault(src, {"tp": 0, "fp": 0})
                bucket["tp"] += int(counts.get("tp", 0) or 0)
                bucket["fp"] += int(counts.get("fp", 0) or 0)
    total = tp + fp
    return {
        "records": records,
        "tp": tp,
        "fp": fp,
        "tp_rate": (tp / total) if total > 0 else None,
        "by_source": by_source,
    }


def scan_records(records: Iterable[Dict[str, Any]],
                 status_allowlist: Tuple[str, ...] = ("sent", "won", "lost")
                 ) -> List[Dict[str, Any]]:
    """Run TP/FP over an iterable of records, keeping only those whose
    status is in `status_allowlist` AND that have at least one auto-priced
    item. Pre-send drafts are excluded so we don't count work-in-progress."""
    out: List[Dict[str, Any]] = []
    allow = {s.lower() for s in status_allowlist}
    for r in records:
        if not isinstance(r, dict):
            continue
        status = (r.get("status") or "").lower()
        if status not in allow:
            continue
        result = compute_record_tp_fp(r)
        if result["auto_priced_count"] > 0:
            out.append(result)
    return out
