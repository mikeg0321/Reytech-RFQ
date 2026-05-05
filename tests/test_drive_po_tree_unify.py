"""Mike Phase 3.5 Option C (2026-05-06): Drive PO archive two-tree
divergence — hybrid lookup that reads BOTH trees and writes to legacy.

Background: prior to this PR, `find_po_folder` searched only the
app-tree path `{year}/{quarter}/PO-{po}/`. ~94% of prod POs (89/90)
live in Mike's legacy tree `{year} - Purchase Orders/{quarter}/{po}/`,
so the audit returned no_folder for nearly everything.

Per scope doc PR #756 — Option C ("hybrid: read both, write to legacy"):
  - `find_po_folder` searches legacy tree FIRST, falls back to app tree
    for the lone fossil PO that ever wrote there.
  - `_create_po_folder_with_contents` writes new POs into the legacy
    tree with bare PO names (matches Mike's filing convention).
  - `drive_triggers.on_po_received` no longer prepends 'PO-'.

Tests in this file are unit + source-level — they exercise the pure
helpers (`_normalize_po_name`) and pin the wiring with grep-against-
function-body assertions. Drive-API integration coverage requires a
mock service object; see comments below.
"""
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── Pure-function unit tests on _normalize_po_name ────────────────


def test_normalize_po_strips_PO_dash_prefix():
    from src.core.gdrive import _normalize_po_name
    assert _normalize_po_name("PO-8955-0000063707") == "8955-0000063707"


def test_normalize_po_strips_PO_space_prefix():
    """Mike's manual filing variant: 'PO 4500736218'."""
    from src.core.gdrive import _normalize_po_name
    assert _normalize_po_name("PO 4500736218") == "4500736218"


def test_normalize_po_strips_trailing_whitespace():
    """Drive preserves trailing whitespace in folder names."""
    from src.core.gdrive import _normalize_po_name
    assert _normalize_po_name("8955-0000071826 ") == "8955-0000071826"
    assert _normalize_po_name(" 4500750017 ") == "4500750017"


def test_normalize_po_passes_bare_po_through():
    from src.core.gdrive import _normalize_po_name
    assert _normalize_po_name("4500750017") == "4500750017"
    assert _normalize_po_name("8955-0000063707") == "8955-0000063707"


def test_normalize_po_handles_empty_and_whitespace_only():
    from src.core.gdrive import _normalize_po_name
    assert _normalize_po_name("") == ""
    assert _normalize_po_name("   ") == ""
    assert _normalize_po_name(None) == ""


def test_normalize_po_case_insensitive_PO_prefix():
    """'po-' and 'po ' should also strip (defensive — observed forms
    are uppercase but Drive UI may auto-correct case)."""
    from src.core.gdrive import _normalize_po_name
    assert _normalize_po_name("po-8955-0000063707") == "8955-0000063707"
    assert _normalize_po_name("Po 4500736218") == "4500736218"


def test_normalize_po_preserves_internal_PO_substring():
    """A PO containing 'PO' mid-string (unlikely but possible) must NOT
    be stripped — only leading 'PO-' or 'PO ' prefixes."""
    from src.core.gdrive import _normalize_po_name
    assert _normalize_po_name("4500POEXAMPLE") == "4500POEXAMPLE"


# ── Source-level wiring guards ────────────────────────────────────


def test_find_po_folder_searches_legacy_tree():
    """find_po_folder must call _find_legacy_year_folder — pre-fix it
    only searched the app-tree path which holds 1/90 archived POs."""
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    fn_start = body.find("def find_po_folder(")
    assert fn_start > 0, "find_po_folder not found"
    fn_end = body.find("\ndef ", fn_start + 10)
    fn_body = body[fn_start:fn_end if fn_end > 0 else fn_start + 4000]
    assert "_find_legacy_year_folder" in fn_body, (
        "find_po_folder must search the legacy tree via "
        "_find_legacy_year_folder. Without it, ~94%% of archived POs "
        "(89/90 as of 2026-04-28) are unfindable."
    )
    assert "_normalize_po_name" in fn_body, (
        "find_po_folder must normalize PO names so legacy variants "
        "(bare, PO- prefix, PO space prefix, trailing whitespace) all "
        "resolve to the same folder."
    )


def test_find_po_folder_keeps_app_tree_fallback():
    """The app-tree branch is intentionally preserved as a fallback
    for the lone fossil PO that wrote there (8955-0000076737 in
    2026/Q2). A future PR can drop it once we confirm no more app-tree
    writes — that's Option A's end state per scope doc."""
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    fn_start = body.find("def find_po_folder(")
    fn_body = body[fn_start:fn_start + 8000]
    # The function should still call `find_folder(year, ROOT)` for the
    # bare-year app-tree branch
    assert "find_folder(year, GOOGLE_DRIVE_ROOT_FOLDER_ID)" in fn_body, (
        "App-tree fallback removed prematurely. It must stay until the "
        "lone fossil PO (8955-0000076737) is migrated to legacy or until "
        "a separate cleanup PR explicitly drops it."
    )


def test_create_po_folder_writes_to_legacy_tree():
    """_create_po_folder_with_contents must use the legacy-year helper.
    Pre-fix it called get_folder_path(year, ...) which built the app
    tree."""
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    fn_start = body.find("def _create_po_folder_with_contents(")
    assert fn_start > 0
    fn_body = body[fn_start:fn_start + 3000]
    assert "_get_or_create_legacy_year_folder" in fn_body, (
        "_create_po_folder_with_contents must write to the legacy tree "
        "via _get_or_create_legacy_year_folder. Pre-fix it wrote to the "
        "app tree (`{year}/{quarter}/PO-{po}/`) which Mike doesn't use."
    )
    # The old broken call signature must be gone
    assert "get_folder_path(year, quarter, po_number)" not in fn_body, (
        "Pre-fix call to get_folder_path(year, quarter, po_number) "
        "still present — that builds the app-tree path. Use "
        "_get_or_create_legacy_year_folder + _get_or_create_folder."
    )


def test_drive_triggers_no_longer_prepends_PO_prefix():
    """drive_triggers.on_po_received must pass the bare po_number to
    enqueue. Pre-fix it sent 'PO-{po}' which doesn't match Mike's
    legacy filing convention (bare PO, no prefix)."""
    body = (REPO / "src/agents/drive_triggers.py").read_text(encoding="utf-8")
    fn_start = body.find("def on_po_received(")
    assert fn_start > 0
    fn_body = body[fn_start:fn_start + 2000]
    bad_pattern = '"po_number": f"PO-{po}"'
    assert bad_pattern not in fn_body, (
        "drive_triggers.on_po_received still prepends 'PO-' to po_number. "
        "Mike's legacy tree uses bare PO names. The downstream "
        "_create_po_folder_with_contents strips the prefix anyway, but "
        "fix the source for clarity."
    )


def test_legacy_year_folder_helper_handles_variants():
    """_find_legacy_year_folder must compare folder names case-
    insensitively after stripping whitespace. The audit observed three
    name patterns: '2024 - Purchase Orders' (canonical),
    '2024 - Purchase Orders ' (trailing space), '2023 - Purchase orders'
    (lowercase 'orders')."""
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    fn_start = body.find("def _find_legacy_year_folder(")
    assert fn_start > 0
    fn_body = body[fn_start:fn_start + 1500]
    # Must do a case-insensitive comparison
    assert ".lower()" in fn_body, (
        "_find_legacy_year_folder must compare names case-insensitively "
        "to handle '2023 - Purchase orders' (lowercase 'orders' variant)."
    )
    # Must strip whitespace to handle the trailing-space variant
    assert ".strip()" in fn_body, (
        "_find_legacy_year_folder must strip whitespace to handle "
        "'2024 - Purchase Orders ' (trailing space — Drive preserves)."
    )


# ── Behavioral integration test: hybrid lookup with mocked Drive ──


def test_find_po_folder_legacy_match(monkeypatch):
    """Simulate a legacy-tree match: the year folder, quarter, and a
    bare PO folder all exist. find_po_folder should return the bare PO
    folder ID without falling through to the app tree."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    # Legacy year folder lookup → returns id
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                {"id": "Y2024", "name": "2024 - Purchase Orders"},
                                {"id": "Y2025_app", "name": "2025"},
                            ],
                            "Q3_LEGACY": [
                                {"id": "PO_BARE", "name": "4500750017"},
                                {"id": "OTHER", "name": "PO-99"},
                            ],
                        }.get(parent, []))
    # Quarter find: legacy quarter exists; app quarter not used in this case
    calls = {"find_folder": []}
    def _ff(name, parent):
        calls["find_folder"].append((name, parent))
        if name == "Q3" and parent == "Y2024":
            return "Q3_LEGACY"
        return None
    monkeypatch.setattr(gdrive, "find_folder", _ff)

    found = gdrive.find_po_folder("2024", "Q3", "4500750017")
    assert found == "PO_BARE"


def test_find_po_folder_legacy_match_with_PO_dash_variant(monkeypatch):
    """Legacy folder named 'PO-8955-0000063707' must still match a
    lookup for bare '8955-0000063707'."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                {"id": "Y2025", "name": "2025 - Purchase Orders"},
                            ],
                            "Q1_LEGACY": [
                                {"id": "PO_DASHED", "name": "PO-8955-0000063707"},
                            ],
                        }.get(parent, []))
    monkeypatch.setattr(gdrive, "find_folder",
                        lambda name, parent: "Q1_LEGACY"
                        if (name == "Q1" and parent == "Y2025") else None)

    found = gdrive.find_po_folder("2025", "Q1", "8955-0000063707")
    assert found == "PO_DASHED"


def test_find_po_folder_app_tree_fallback(monkeypatch):
    """When the legacy tree has no matching year folder, the app-tree
    branch fires. This covers the 1 fossil PO that lives at
    `2026/Q2/8955-0000076737` (bare folder name, no PO- prefix)."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    # Legacy tree empty
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: [])
    # App tree: bare year, then quarter, then bare PO matches
    def _ff(name, parent):
        if name == "2026" and parent == "ROOT_ID":
            return "Y2026_APP"
        if name == "Q2" and parent == "Y2026_APP":
            return "Q2_APP"
        if name == "8955-0000076737" and parent == "Q2_APP":
            return "FOSSIL_PO"
        return None
    monkeypatch.setattr(gdrive, "find_folder", _ff)

    found = gdrive.find_po_folder("2026", "Q2", "8955-0000076737")
    assert found == "FOSSIL_PO"


def test_find_po_folder_returns_none_when_neither_tree_has_it(monkeypatch):
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders", lambda parent: [])
    monkeypatch.setattr(gdrive, "find_folder",
                        lambda name, parent: None)

    assert gdrive.find_po_folder("2027", "Q4", "9999-0000099999") is None


def test_find_po_folder_no_root_returns_none(monkeypatch):
    from src.core import gdrive
    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "")
    assert gdrive.find_po_folder("2025", "Q1", "4500750017") is None


def test_find_po_folder_normalizes_query_input(monkeypatch):
    """A caller passing 'PO-4500750017' should match a folder named
    bare '4500750017' in the legacy tree — both sides normalize."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                {"id": "Y2024", "name": "2024 - Purchase Orders"},
                            ],
                            "Q3_LEGACY": [
                                {"id": "BARE", "name": "4500750017"},
                            ],
                        }.get(parent, []))
    monkeypatch.setattr(gdrive, "find_folder",
                        lambda name, parent: "Q3_LEGACY"
                        if (name == "Q3" and parent == "Y2024") else None)

    found = gdrive.find_po_folder("2024", "Q3", "PO-4500750017")
    assert found == "BARE", (
        "Caller-side normalization failed — passing 'PO-4500750017' as "
        "the query should match bare '4500750017' in the legacy folder."
    )
