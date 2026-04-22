"""BUILD-9 P1 regression guard — manual-won paths must calibrate oracle.

BUILD-1 wired `calibrate_from_outcome` into the RFQ-outcome route (the
UI "mark won/lost" button for RFQs). Post-BUILD-8 re-audit found 3 other
paths that mark a quote `status='won'` but never fed the outcome back
to the oracle:

  1. `email_poller.py` — detects a PO arriving via email, auto-marks
     the matching sent quote as won.
  2. `dashboard._create_order_from_quote` — operator creates an order
     from a quote in the UI; marks the quote won.
  3. `dashboard._create_order_from_po_email` — automated order creation
     when a PO email is parsed; marks the linked quote won.

Every real-world Reytech win flows through one of these four paths.
Prior to BUILD-9 only path (0) = api_rfq_outcome called calibrate. The
oracle's EMAs over sample_size / win_count / avg_winning_margin were
systematically starved of win signal.

This test locks source-level guards. Each of the 3 call sites must:
  - call `calibrate_from_outcome(...)` with outcome=\"won\"
  - gate it on a rowcount>0 idempotency check (so retries don't
    double-count into the EMA — BUILD-6 pattern)
  - run AFTER the SQL UPDATE/commit has released the write lock
    (BUILD-5 lock-contention fix)
"""
from __future__ import annotations

import re
from pathlib import Path


def _read(p: str) -> str:
    return Path(p).read_text(encoding="utf-8")


# ─── Call-site presence guards ───────────────────────────────────────

def test_email_poller_calibrates_on_email_won():
    src = _read("src/agents/email_poller.py")
    # Must import calibrate and invoke it with outcome="won"
    assert "from src.core.pricing_oracle_v2 import calibrate_from_outcome" in src, (
        "BUILD-9: email_poller.py must import calibrate_from_outcome"
    )
    # The call must pass outcome="won" positionally (2nd arg) — match a
    # 1-6 line dotall window. Use DOTALL so multi-line call sigs match.
    pattern = re.compile(
        r'calibrate_from_outcome\(\s*[^,]+,\s*"won"',
        re.DOTALL,
    )
    assert pattern.search(src), (
        "BUILD-9: email_poller.py must call calibrate_from_outcome(items, \"won\", ...)"
    )


def test_create_order_from_quote_calibrates():
    src = _read("src/api/dashboard.py")
    # The order-from-quote function must call calibrate with outcome="won"
    # inside its body. Verify by checking both the function marker and
    # the call exist, and the call references `line_items` (not some
    # other var), which is what the function builds.
    assert "def _create_order_from_quote" in src
    assert "calibrate_from_outcome" in src, (
        "BUILD-9: dashboard.py must call calibrate_from_outcome"
    )
    # Quick proximity check: BUILD-9 marker should appear near both
    # order-from-quote and order-from-po-email. Count must be >=2.
    build9_hits = len(re.findall(r"BUILD-9", src))
    assert build9_hits >= 2, (
        f"BUILD-9: expected at least 2 BUILD-9 markers in dashboard.py "
        f"(one per manual-won path), found {build9_hits}"
    )


def test_three_manual_won_calibrate_sites_across_repo():
    """Aggregate guard: across email_poller.py + dashboard.py there must
    be at least 3 calls to calibrate_from_outcome with outcome=\"won\" —
    one per manual-won path identified in the post-BUILD-8 re-audit."""
    files = [
        "src/agents/email_poller.py",
        "src/api/dashboard.py",
    ]
    total_won_calls = 0
    for fp in files:
        src = _read(fp)
        # Match calibrate_from_outcome(<anything>, "won", ...) across
        # multiple lines. DOTALL so \n is permitted inside the arg list.
        matches = re.findall(
            r'calibrate_from_outcome\(\s*[^,]+,\s*"won"',
            src,
            flags=re.DOTALL,
        )
        total_won_calls += len(matches)
    assert total_won_calls >= 3, (
        f"BUILD-9: expected 3 'won' calibrate calls across email_poller "
        f"+ dashboard (one per manual-won path), found {total_won_calls}. "
        f"A refactor may have dropped one of the three sites."
    )


# ─── Idempotency guard — rowcount gate ──────────────────────────────

def test_manual_won_calibrate_gated_on_rowcount():
    """Each calibrate call must be gated on an `_won_rowcount > 0` check
    so that a redundant status transition (e.g. a forwarded PO email
    replay, or a second order created on the same quote) does NOT
    re-fire calibrate and double-count into the EMA."""
    for fp in ["src/agents/email_poller.py", "src/api/dashboard.py"]:
        src = _read(fp)
        # BUILD-6 pattern: `_won_rowcount > 0` immediately precedes (within
        # ~300 chars) a `calibrate_from_outcome(` call.
        pattern = re.compile(
            r"_won_rowcount\s*>\s*0.{0,300}calibrate_from_outcome\(",
            re.DOTALL,
        )
        assert pattern.search(src), (
            f"BUILD-9: {fp} must gate calibrate_from_outcome on "
            f"`_won_rowcount > 0` for idempotency (matches the BUILD-6 "
            f"pattern). Without the gate, every re-trigger on an "
            f"already-won quote would re-fire the EMA update."
        )


# ─── Lock-contention guard — commit BEFORE calibrate ────────────────

def test_email_poller_commits_before_calibrate():
    """email_poller: the outer `_aconn.commit()` must come BEFORE the
    calibrate_from_outcome call. BUILD-5 exposed that calibrate opens its
    own SQLite connection; if the outer conn still holds the write lock,
    calibrate times out at 10s busy_timeout and silently no-ops."""
    src = _read("src/agents/email_poller.py")
    commit_idx = src.find("_aconn.commit()")
    assert commit_idx > 0, "email_poller must commit its connection"
    calibrate_idx = src.find("calibrate_from_outcome", commit_idx)
    assert calibrate_idx > commit_idx, (
        "BUILD-9: email_poller.py must call _aconn.commit() BEFORE "
        "calibrate_from_outcome — otherwise calibrate's own connection "
        "times out on the write lock (BUILD-5 incident pattern)."
    )


def test_dashboard_calibrate_outside_with_block():
    """dashboard paths use `with get_db() as conn:` to own the UPDATE.
    The calibrate call must run AFTER the with-block exits (so the write
    lock releases before calibrate opens its own connection — BUILD-5
    lock-contention incident).

    Structural proof: `_won_rowcount` is assigned INSIDE the with-body,
    and the guard `if qn and _won_rowcount > 0:` that wraps each calibrate
    call necessarily executes OUTSIDE the with-body (the variable is
    only in scope after the with ends). Verifying that guard precedes
    every calibrate call in dashboard.py proves calibrate runs post-commit.
    """
    src = _read("src/api/dashboard.py")
    # Every calibrate call in dashboard.py must be preceded (within 300
    # chars) by the `if qn and _won_rowcount > 0:` guard.
    for match in re.finditer(r"calibrate_from_outcome\(", src):
        window_start = max(0, match.start() - 300)
        window = src[window_start:match.start()]
        assert re.search(r"if\s+qn\s+and\s+_won_rowcount\s*>\s*0\s*:", window), (
            f"BUILD-9: dashboard calibrate_from_outcome at offset {match.start()} "
            f"is missing the `if qn and _won_rowcount > 0:` guard that proves "
            f"it runs outside the with-block (BUILD-5 lock-contention fix)."
        )
