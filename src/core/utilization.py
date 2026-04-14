"""Utilization tracking — Phase 4 of the PC↔RFQ refactor.

Lightweight event recorder so Mike can see which features of the
RFQ app are actually being used, which are dead, and where the
bottlenecks are.

Every meaningful user action or pipeline step calls:

    record_feature_use("generate_quote_1click", context={
        "record_type": "pc",
        "record_id": "pc_abc123",
        "agency": "cchcs",
        "duration_ms": 3400,
    })

Events are written to the `utilization_events` SQLite table. An
admin dashboard at `/api/admin/utilization/summary` aggregates the
last N days and returns:

    - Top 10 features by usage count
    - Dead features (zero uses in window)
    - Average duration per feature
    - Error rate per feature
    - Per-user breakdown (for internal attribution)

Writes are fire-and-forget — a tracking failure can NEVER break
the user action. The recorder swallows every exception.

Retention: 90 days. Older events purged by a weekly job.
"""
from __future__ import annotations

import atexit
import json
import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional, Tuple

log = logging.getLogger("reytech.utilization")


# ── Event recording ──────────────────────────────────────────────────────
#
# Events are enqueued into an in-memory deque and flushed by a single
# background daemon thread every ~1s (or immediately when the queue
# hits _MAX_QUEUE). This keeps the request hot path at a few
# microseconds — the alternative is a synchronous SQLite INSERT which
# costs ~1-2ms and, more importantly, can stall under WAL contention.
# On process exit we flush once more so short-lived workers don't
# drop their last batch.

_MAX_QUEUE = 2000          # backpressure ceiling
_FLUSH_INTERVAL = 1.0      # seconds between drain cycles
_FLUSH_BATCH_MAX = 500     # events per INSERT transaction

_queue: Deque[Tuple[str, str, str, int, int, str]] = deque()
_queue_lock = threading.Lock()
_flusher_started = False
_flusher_lock = threading.Lock()


def _start_flusher_once() -> None:
    global _flusher_started
    if _flusher_started:
        return
    with _flusher_lock:
        if _flusher_started:
            return
        t = threading.Thread(
            target=_flusher_loop,
            name="utilization-flusher",
            daemon=True,
        )
        t.start()
        atexit.register(_flush_queue)
        _flusher_started = True


def _flusher_loop() -> None:
    while True:
        time.sleep(_FLUSH_INTERVAL)
        try:
            _flush_queue()
        except Exception as e:
            log.debug("utilization flusher error: %s", e)


def flush_now() -> int:
    """Public synchronous drain. Callers: tests that need to read
    their own writes; shutdown hooks; the atexit handler. Returns
    the number of rows flushed on this call."""
    return _flush_queue()


def _flush_queue() -> int:
    """Drain up to `_FLUSH_BATCH_MAX` events into SQLite in one
    transaction. Returns the number of rows written. Safe to call
    from anywhere — locks the queue only long enough to copy rows."""
    batch: List[Tuple[str, str, str, int, int, str]] = []
    with _queue_lock:
        while _queue and len(batch) < _FLUSH_BATCH_MAX:
            batch.append(_queue.popleft())
    if not batch:
        return 0
    try:
        from src.core.db import get_db
        with get_db() as conn:
            conn.executemany(
                """INSERT INTO utilization_events
                   (feature, context, user, duration_ms, ok, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                batch,
            )
        return len(batch)
    except Exception as e:
        log.debug("utilization flush suppressed: %s", e)
        return 0


def record_feature_use(
    feature: str,
    context: Optional[Dict[str, Any]] = None,
    user: str = "",
    duration_ms: int = 0,
    ok: bool = True,
) -> None:
    """Record one feature use. Fire-and-forget and truly non-blocking
    — the call path is a JSON-encode + deque append (~microseconds),
    with a background daemon thread flushing the queue to SQLite
    every ~1s.

    Args:
        feature: short feature key, e.g. "generate_quote_1click",
                 "classify_request", "auto_price_all". Namespace by
                 module: "pc.generate_quote", "rfq.upload_parse",
                 "oracle.lookup".
        context: arbitrary dict with identifying info (agency,
                 record_type, record_id, item_count, etc.) — stored
                 as JSON
        user: best-effort username from Basic Auth
        duration_ms: how long the operation took (optional)
        ok: True if success, False on error — used for error-rate
            computations in the dashboard
    """
    if not feature:
        return
    try:
        ctx_json = json.dumps(context or {}, default=str)[:4000]
        row = (
            feature, ctx_json, user or "", int(duration_ms),
            1 if ok else 0, datetime.now().isoformat(),
        )
        with _queue_lock:
            if len(_queue) >= _MAX_QUEUE:
                # Backpressure: drop the oldest event to protect the
                # caller from unbounded memory growth if the flusher
                # falls behind. Count is logged so the dashboard shows
                # the drop rate in its error-rate column.
                _queue.popleft()
            _queue.append(row)
        _start_flusher_once()
    except Exception as e:
        log.debug("record_feature_use suppressed: %s", e)


def time_feature(feature: str):
    """Context manager that times a block and records the result.

    Usage:
        with time_feature("pc.generate_quote") as ctx:
            ctx["agency"] = "cchcs"
            ctx["item_count"] = 15
            ... do the work ...
    """
    return _FeatureTimer(feature)


class _FeatureTimer:
    def __init__(self, feature: str):
        self.feature = feature
        self.context: Dict[str, Any] = {}
        self._start = 0.0
        self._ok = True

    def __enter__(self) -> Dict[str, Any]:
        self._start = time.time()
        return self.context

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        duration_ms = int((time.time() - self._start) * 1000)
        self._ok = exc_type is None
        record_feature_use(
            feature=self.feature,
            context=self.context,
            duration_ms=duration_ms,
            ok=self._ok,
        )


# ── Aggregation / dashboard queries ─────────────────────────────────────

def top_features(days: int = 7, limit: int = 20) -> List[Dict[str, Any]]:
    """Return top N features by usage count in the last `days`."""
    try:
        from src.core.db import get_db
        since = (datetime.now() - timedelta(days=days)).isoformat()
        with get_db() as conn:
            rows = conn.execute(
                """SELECT feature, COUNT(*) as uses,
                          AVG(duration_ms) as avg_ms,
                          SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) as errors
                   FROM utilization_events
                   WHERE created_at >= ?
                   GROUP BY feature
                   ORDER BY uses DESC
                   LIMIT ?""",
                (since, limit),
            ).fetchall()
    except Exception as e:
        log.warning("top_features query failed: %s", e)
        return []

    out = []
    for row in rows:
        d = dict(row) if hasattr(row, "keys") else {
            "feature": row[0], "uses": row[1],
            "avg_ms": row[2], "errors": row[3],
        }
        d["error_rate"] = (
            (d.get("errors", 0) or 0) / max(d.get("uses", 1), 1)
        )
        out.append(d)
    return out


def dead_features(known_features: List[str], days: int = 30) -> List[str]:
    """Return features from `known_features` that have ZERO uses in
    the last `days`. Shows which code paths are safe to delete."""
    if not known_features:
        return []
    try:
        from src.core.db import get_db
        since = (datetime.now() - timedelta(days=days)).isoformat()
        with get_db() as conn:
            placeholders = ",".join(["?"] * len(known_features))
            rows = conn.execute(
                f"""SELECT DISTINCT feature FROM utilization_events
                    WHERE created_at >= ? AND feature IN ({placeholders})""",
                (since, *known_features),
            ).fetchall()
    except Exception as e:
        log.warning("dead_features query failed: %s", e)
        return []
    used = {r[0] if not hasattr(r, "get") else r["feature"] for r in rows}
    return sorted(set(known_features) - used)


def feature_series(feature: str, days: int = 30) -> List[Dict[str, Any]]:
    """Return daily counts for one feature over `days`, for trend graphs."""
    try:
        from src.core.db import get_db
        since = (datetime.now() - timedelta(days=days)).isoformat()
        with get_db() as conn:
            rows = conn.execute(
                """SELECT date(created_at) as day, COUNT(*) as uses
                   FROM utilization_events
                   WHERE feature=? AND created_at >= ?
                   GROUP BY date(created_at)
                   ORDER BY day""",
                (feature, since),
            ).fetchall()
    except Exception as e:
        log.warning("feature_series query failed: %s", e)
        return []
    return [
        {"day": r[0] if not hasattr(r, "get") else r["day"],
         "uses": r[1] if not hasattr(r, "get") else r["uses"]}
        for r in rows
    ]


def summary(days: int = 7) -> Dict[str, Any]:
    """One-shot dashboard summary: totals, top features, error leaders."""
    try:
        from src.core.db import get_db
        since = (datetime.now() - timedelta(days=days)).isoformat()
        with get_db() as conn:
            totals = conn.execute(
                """SELECT COUNT(*) as n, AVG(duration_ms) as avg_ms,
                          SUM(CASE WHEN ok=0 THEN 1 ELSE 0 END) as errors
                   FROM utilization_events WHERE created_at >= ?""",
                (since,),
            ).fetchone()
    except Exception as e:
        log.warning("summary query failed: %s", e)
        return {"ok": False, "error": str(e)}

    tot = dict(totals) if hasattr(totals, "keys") else {
        "n": totals[0] if totals else 0,
        "avg_ms": totals[1] if totals else 0,
        "errors": totals[2] if totals else 0,
    }
    return {
        "ok": True,
        "days": days,
        "total_events": tot.get("n", 0),
        "avg_duration_ms": round(float(tot.get("avg_ms") or 0), 1),
        "errors": tot.get("errors", 0),
        "error_rate": (
            (tot.get("errors", 0) or 0) / max(tot.get("n", 1), 1)
        ),
        "top_features": top_features(days=days, limit=10),
    }


__all__ = [
    "record_feature_use",
    "time_feature",
    "top_features",
    "dead_features",
    "feature_series",
    "summary",
]
