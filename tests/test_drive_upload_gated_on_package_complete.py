"""Pins the substrate fix shipped 2026-05-27 after the rfq_0124647e
split-brain caught by Mr. Wolf's deploy-log audit:

  04:24:37 [err] PACKAGE INCOMPLETE rfq_0124647e: Pricing alignment:
                 quote (quote) TAX $0.00 ≠ canonical $70.22 ($-70.22)
  04:24:37 [wrn] NOTIFY SUPPRESSED rfq_0124647e: package incomplete
  04:24:39 [inf] Drive trigger: package_generated → 4 files queued
                 for 10847395                           ← !!!
  04:24:43 [inf] Updated Drive file: 10847395_703B_Reytech.pdf
  04:24:50 [inf] Updated Drive file: 10847395_Quote_Reytech.pdf
                 (the $0.00 TAX one)

Operator notification was correctly suppressed for the incomplete
package, but the Drive upload fired unconditionally — putting the
broken $0-tax quote PDF in the Pending folder Mike pulls from.

Fix at `src/api/modules/routes_rfq_gen.py` ~line 3395: wrap the
`on_package_generated` call in `if _package_complete:`, matching the
existing gates on `notify_package_ready` (line 3370) and
`create_draft_email` (line 3405). Incomplete packages still write to
local disk at `out_dir` for operator review via the review-package
route — only Drive (operator's send-from folder) is gated.
"""
from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TARGET = REPO_ROOT / "src" / "api" / "modules" / "routes_rfq_gen.py"


def test_drive_upload_call_is_gated_on_package_complete():
    """The `on_package_generated` Drive trigger MUST be inside an
    `if _package_complete:` block. Pre-fix it fired unconditionally
    after the gate, so PACKAGE INCOMPLETE rfqs still hit Drive."""
    src = TARGET.read_text(encoding="utf-8")
    idx = src.find("on_package_generated(r, out_dir, final_output_files)")
    assert idx > 0, "Drive trigger call not found"

    # Walk backward to the nearest line-start; the line ABOVE the
    # `from src.agents.drive_triggers import ...` line must be a
    # `try:` whose enclosing block is `if _package_complete:`.
    window = src[max(0, idx - 600):idx]
    assert "if _package_complete:" in window, (
        "on_package_generated must be inside an `if _package_complete:` "
        "block — see split-brain incident 2026-05-27 rfq_0124647e where "
        "Drive uploaded 4 broken files for a PACKAGE INCOMPLETE rfq."
    )


def test_drive_upload_skip_path_logs_the_block():
    """When `_package_complete` is False, the operator deserves a log
    line explaining why Drive was skipped — mirrors the existing
    `NOTIFY SUPPRESSED` log for the notify gate at line 3378.
    Otherwise it's invisible that Drive WOULD have fired but didn't."""
    src = TARGET.read_text(encoding="utf-8")
    assert "DRIVE UPLOAD SKIPPED" in src, (
        "Incomplete-package branch must log `DRIVE UPLOAD SKIPPED %s: "
        "package incomplete — %s` (same shape as `NOTIFY SUPPRESSED` "
        "next to it) so the operator can see in logs that Drive was "
        "intentionally bypassed, not silently broken."
    )


def test_drive_gate_matches_notify_and_draft_gates():
    """All three operator-visible side-effects (notify, Drive upload,
    draft email) MUST share the same `_package_complete` predicate.
    This is the substrate-singleness fix: one gate, three writes."""
    src = TARGET.read_text(encoding="utf-8")
    notify_idx = src.find("notify_package_ready(r, result)")
    drive_idx = src.find("on_package_generated(r, out_dir, final_output_files)")
    draft_idx = src.find("sender.create_draft_email(r, all_paths)")
    assert notify_idx > 0 and drive_idx > 0 and draft_idx > 0

    # Each must have `if _package_complete:` within the preceding ~600 chars.
    for label, call_idx in (
        ("notify", notify_idx),
        ("drive", drive_idx),
        ("draft", draft_idx),
    ):
        window = src[max(0, call_idx - 600):call_idx]
        assert "if _package_complete:" in window, (
            f"{label} call must be gated on `_package_complete` — "
            "all three operator-facing side-effects share one gate."
        )
