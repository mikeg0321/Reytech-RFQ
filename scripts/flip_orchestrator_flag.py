"""Flip `rfq.orchestrator_pipeline` ON/OFF — 2026-04-20.

The observer-style orchestrator wrapper landed flag-gated OFF (PRs #259-#263).
Once the inbox-ingest verification has proved the pipeline can drive the 5
currently-in-inbox RFQs cleanly, this script lets the operator flip the flag
without hand-crafting a SQL statement or curl-ing `/api/admin/flags`.

Usage:
    python scripts/flip_orchestrator_flag.py on       # turn pipeline ON
    python scripts/flip_orchestrator_flag.py off      # revert to observer-only
    python scripts/flip_orchestrator_flag.py status   # show current value

The flag is read via `get_flag("rfq.orchestrator_pipeline", False)` in
QuoteOrchestrator.run_legacy_package; when False the wrapper returns a
no-op StageResult and legacy routes finish via their original path.
"""
from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

FLAG_KEY = "rfq.orchestrator_pipeline"


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in ("on", "off", "status"):
        print("usage: flip_orchestrator_flag.py [on|off|status]", file=sys.stderr)
        return 2

    action = sys.argv[1]
    from src.core.flags import get_flag, set_flag

    current = get_flag(FLAG_KEY, False)
    if action == "status":
        print(f"{FLAG_KEY} = {current}")
        return 0

    want = action == "on"
    ok = set_flag(FLAG_KEY, "true" if want else "false",
                  updated_by="flip_orchestrator_flag.py",
                  description="Observer-style orchestrator wrapper on legacy RFQ routes")
    if not ok:
        print(f"ERROR: set_flag failed for {FLAG_KEY}", file=sys.stderr)
        return 1
    new_val = get_flag(FLAG_KEY, False)
    print(f"{FLAG_KEY}: {current} → {new_val}")
    return 0 if new_val == want else 1


if __name__ == "__main__":
    raise SystemExit(main())
