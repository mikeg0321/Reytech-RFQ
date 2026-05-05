"""Mike Phase 3.5 follow-up (2026-05-06): quarter-mismatch fallback in
find_po_folder.

Background: 2026-05-06 prod smoke after PR #757 returned 42/100 archived
folders found, 58/100 missed. Investigation showed most misses were
real legacy-tree POs filed in a DIFFERENT quarter than the order's
`created_at` derives. Mike files based on PO award/receipt date; the
audit derives quarter from order creation date.

Fix: if the requested quarter has no match under the legacy year folder,
scan all 4 quarters before giving up. Same pattern for the app-tree
fossil branch.

Performance: only kicks in when the exact-quarter match fails. Worst
case = 4 list calls per miss (each ~100ms), so a 100-row audit pays
~40s in the cold-cache path. Acceptable for a one-shot audit; the live
on_po_received path always passes the current quarter (matches on the
first try).
"""
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── source-level guards ───────────────────────────────────────────


def test_find_po_folder_legacy_quarter_fallback_exists():
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    fn_start = body.find("def find_po_folder(")
    fn_body = body[fn_start:fn_start + 6000]
    # All 4 quarters must be referenced as fallback options
    assert "Q1" in fn_body and "Q2" in fn_body and "Q3" in fn_body and "Q4" in fn_body, (
        "find_po_folder must scan Q1/Q2/Q3/Q4 as fallback when the "
        "audit-derived quarter doesn't match. Pre-fix the prod smoke "
        "showed 58/100 audit rows missed because of this exact case."
    )
    # Loop construct that iterates fallback quarters
    assert "for fallback_q" in fn_body or "quarters_to_try" in fn_body, (
        "Quarter-fallback loop must be present in find_po_folder."
    )


def test_find_po_folder_logs_quarter_mismatch_match():
    """When a fallback-quarter match succeeds, log it so operations can
    spot the audit-vs-filing-date drift."""
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    fn_start = body.find("def find_po_folder(")
    fn_body = body[fn_start:fn_start + 6000]
    assert "audited quarter was" in fn_body, (
        "Quarter-mismatch matches must be logged with both the actual "
        "and audited quarters, so we can spot orders where the order's "
        "created_at quarter consistently doesn't match Mike's filing."
    )


# ── behavioral integration tests ──────────────────────────────────


def test_find_po_folder_falls_back_to_other_quarter(monkeypatch):
    """Order's created_at says Q1; folder is actually filed under Q3.
    Without the fallback we'd return None. With the fallback, we
    return the Q3 folder ID."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    # Legacy year folder lookup
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                {"id": "Y2025", "name": "2025 - Purchase Orders"},
                            ],
                            # Q1 folder exists but has different POs
                            "Q1_LEGACY": [
                                {"id": "WRONG_PO", "name": "9999-9999999999"},
                            ],
                            # Q3 folder is where the target lives
                            "Q3_LEGACY": [
                                {"id": "TARGET_PO", "name": "4500693412"},
                            ],
                        }.get(parent, []))

    def _ff(name, parent):
        if parent == "Y2025":
            return {"Q1": "Q1_LEGACY", "Q2": None, "Q3": "Q3_LEGACY",
                    "Q4": None}.get(name)
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    # Audit thinks Q1 (per order's created_at), but file is in Q3
    found = gdrive.find_po_folder("2025", "Q1", "4500693412")
    assert found == "TARGET_PO", (
        f"Quarter-fallback failed — expected TARGET_PO from Q3, got {found!r}"
    )


def test_find_po_folder_exact_quarter_short_circuits(monkeypatch):
    """When the requested quarter HAS the PO, we return it without
    walking the other quarters (perf optimization)."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    list_calls = []

    def _list_sub(parent):
        list_calls.append(parent)
        return {
            "ROOT_ID": [{"id": "Y2025", "name": "2025 - Purchase Orders"}],
            "Q2_LEGACY": [{"id": "TARGET", "name": "4500700000"}],
        }.get(parent, [])

    monkeypatch.setattr(gdrive, "_list_subfolders", _list_sub)
    monkeypatch.setattr(gdrive, "find_folder",
                        lambda name, parent:
                        "Q2_LEGACY" if (name == "Q2" and parent == "Y2025") else None)

    found = gdrive.find_po_folder("2025", "Q2", "4500700000")
    assert found == "TARGET"
    # ROOT_ID list (year resolution) + Q2_LEGACY list (po match) = 2 calls
    # NOT 5 (would be 5 if we walked Q1/Q2/Q3/Q4 + Q2 again).
    assert len(list_calls) == 2, (
        f"Exact quarter match should short-circuit. Got {len(list_calls)} "
        f"list calls: {list_calls}"
    )


def test_find_po_folder_fallback_handles_missing_quarter_folders(monkeypatch):
    """If only some quarters exist (e.g., Q1 and Q3 created but Q2/Q4
    never were), fallback gracefully skips the missing ones."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [{"id": "Y2024", "name": "2024 - Purchase Orders"}],
                            "Q3_2024": [{"id": "PO_HIT", "name": "4500685000"}],
                        }.get(parent, []))

    def _ff(name, parent):
        if parent == "Y2024":
            # Q1, Q2, Q4 don't exist
            return "Q3_2024" if name == "Q3" else None
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    # Order says Q4, real folder is Q3, Q1/Q2/Q4 don't exist as folders
    found = gdrive.find_po_folder("2024", "Q4", "4500685000")
    assert found == "PO_HIT"


def test_find_po_folder_app_tree_quarter_fallback(monkeypatch):
    """The app tree gets the same quarter-fallback treatment so the
    lone fossil PO (8955-0000076737, lives at 2026/Q2 even though
    its order shows Q1) is findable."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    # Legacy tree empty so we fall through to app tree
    monkeypatch.setattr(gdrive, "_list_subfolders", lambda parent: [])

    def _ff(name, parent):
        # No legacy year folder
        if parent == "ROOT_ID" and name == "2026":
            return "Y2026_APP"
        # App tree has Q2 only
        if parent == "Y2026_APP" and name == "Q2":
            return "Q2_APP"
        if parent == "Q2_APP" and name == "8955-0000076737":
            return "FOSSIL"
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    # Audit says Q1; actual fossil is in Q2
    found = gdrive.find_po_folder("2026", "Q1", "8955-0000076737")
    assert found == "FOSSIL", (
        f"App-tree quarter fallback failed for fossil PO; got {found!r}"
    )


def test_find_po_folder_returns_none_when_truly_unarchived(monkeypatch):
    """No quarter has the PO. Confirm we still return None and don't
    accidentally match a different PO."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [{"id": "Y", "name": "2025 - Purchase Orders"}],
                            # Other POs exist in Q1/Q2/Q3/Q4 but not the target
                            "Q1": [{"id": "PO1", "name": "4500111111"}],
                            "Q2": [{"id": "PO2", "name": "4500222222"}],
                            "Q3": [{"id": "PO3", "name": "4500333333"}],
                            "Q4": [{"id": "PO4", "name": "4500444444"}],
                        }.get(parent, []))

    def _ff(name, parent):
        if parent == "Y":
            return {"Q1": "Q1", "Q2": "Q2", "Q3": "Q3", "Q4": "Q4"}.get(name)
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    found = gdrive.find_po_folder("2025", "Q1", "4500999999")
    assert found is None, "Returned a folder ID for an unarchived PO"
