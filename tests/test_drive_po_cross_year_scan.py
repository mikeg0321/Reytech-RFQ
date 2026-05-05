"""Mike Phase 3.5 follow-up #2 (2026-05-06): cross-year scan + Q{n} {year}
quarter naming variance in find_po_folder.

Background: PR #758's quarter-fallback got us from 42/100 → 46/100 — well
short of the predicted ~85/100. Smoke breakdown showed two larger drift
sources still uncovered:

1. **Cross-year filing.** Order created 2026 but Mike files PO under 2025
   (e.g. award arrived in a later fiscal year than the order's
   `created_at`). Probe confirmed `8955-0000071826` lives in `2025/Q2/`
   while the audit searched `2026/Q*/`. PR #758 only iterates 4 quarters
   within ONE year — never crosses years.

2. **Q{n} {year} naming variance.** The `2023 - Purchase orders` legacy
   folder has quarter subfolders named `Q1 2023`, `Q2 2023`, `Q3 2023`,
   `Q4 2023` (year suffix), not the canonical `Q1`/`Q2`. PR #758 calls
   `find_folder("Q1", year_id)` which doesn't match `"Q1 2023"`. ~14 stubs
   miss because of this.

Fix:
- `_find_legacy_quarter_folder(year_id, quarter, year)` — tolerates both
  `Q1` and `Q1 2023` naming.
- `find_po_folder` adds a year-fallback loop: target year first, then
  prior 3 years, for the legacy tree only. Bounded for perf.

Performance: live `on_po_received` always passes current year + current
quarter — first match short-circuits. Cross-year only kicks in when the
target year is fully exhausted (= 5 list calls, then prior years tried).
"""
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent


# ── source-level guards ───────────────────────────────────────────


def test_find_po_folder_cross_year_scan_exists():
    """Body must reference a candidate-years list / loop so a PO filed
    in year N-1 is findable when the audit derives year N."""
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    fn_start = body.find("def find_po_folder(")
    fn_body = body[fn_start:fn_start + 8000]
    assert "candidate_years" in fn_body or "try_year" in fn_body, (
        "find_po_folder must iterate candidate years (target + prior) "
        "as cross-year fallback. Pre-fix smoke showed POs filed in "
        "Mike's prior-year folders went unfound."
    )


def test_find_po_folder_q_year_suffix_helper_exists():
    """`_find_legacy_quarter_folder` must be defined to absorb the
    `Q1 2023`-style naming used by the 2023 legacy folder."""
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    assert "def _find_legacy_quarter_folder(" in body, (
        "Helper for Q{n}/Q{n} {year} naming-variant lookup must be "
        "defined; otherwise the 2023 legacy tree is unreachable."
    )


def test_cross_year_match_logged_distinctly():
    """Cross-year + cross-quarter is a distinct drift signal from
    cross-quarter-only — log them differently so we can spot which
    direction Mike's filing convention deviates from the audit."""
    body = (REPO / "src/core/gdrive.py").read_text(encoding="utf-8")
    fn_start = body.find("def find_po_folder(")
    fn_body = body[fn_start:fn_start + 8000]
    assert "audited year=" in fn_body, (
        "Cross-year matches must be logged with both the actual and "
        "audited years (`audited year=Y, quarter=Q`). PR #758's "
        "cross-quarter-only log line ('audited quarter was X') doesn't "
        "surface the year drift signal."
    )


# ── behavioral integration tests ──────────────────────────────────


def test_cross_year_finds_po_in_prior_year(monkeypatch):
    """Order created 2026 (audit year=2026), but PO physically filed in
    2025/Q2. Real prod case: 8955-0000071826 found in 2025/Q2 via probe."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                {"id": "Y2026", "name": "2026 - Purchase Orders"},
                                {"id": "Y2025", "name": "2025 - Purchase Orders"},
                            ],
                            # 2026 has no matching POs
                            "Q1_2026": [],
                            "Q2_2026": [],
                            "Q3_2026": [],
                            "Q4_2026": [],
                            # 2025/Q2 has the target
                            "Q2_2025": [
                                {"id": "TARGET", "name": "8955-0000071826"},
                            ],
                        }.get(parent, []))

    def _ff(name, parent):
        # 2026 quarters all exist (but empty)
        if parent == "Y2026":
            return {"Q1": "Q1_2026", "Q2": "Q2_2026",
                    "Q3": "Q3_2026", "Q4": "Q4_2026"}.get(name)
        # 2025 only has Q2
        if parent == "Y2025":
            return "Q2_2025" if name == "Q2" else None
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    # Audit thinks 2026/Q2; PO actually filed in 2025/Q2
    found = gdrive.find_po_folder("2026", "Q2", "8955-0000071826")
    assert found == "TARGET", (
        f"Cross-year scan failed — 8955-0000071826 lives in 2025/Q2 "
        f"per Drive ground truth, got {found!r}. Without the fallback, "
        f"orders opened in 2026 but awarded under FY 2025 are unfindable."
    )


def test_q_year_suffix_naming_tolerated(monkeypatch):
    """The `2023 - Purchase orders` legacy folder uses quarters named
    `Q1 2023`/`Q2 2023`/etc. instead of canonical `Q1`/`Q2`. Probe
    confirmed this naming variance on prod."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                {"id": "Y2023", "name": "2023 - Purchase orders"},
                            ],
                            "Q3_2023_SUFFIXED": [
                                {"id": "TARGET", "name": "8955-0000049667"},
                            ],
                        }.get(parent, []))

    def _ff(name, parent):
        if parent == "Y2023":
            # Canonical "Q3" doesn't exist, only the year-suffixed form
            if name == "Q3":
                return None
            if name == "Q3 2023":
                return "Q3_2023_SUFFIXED"
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    # Audit-derived quarter is Q3, lives at 'Q3 2023' folder.
    found = gdrive.find_po_folder("2023", "Q3", "8955-0000049667")
    assert found == "TARGET", (
        f"Q{{n}} {{year}} naming variant not tolerated — PR #758 only "
        f"matches canonical Q1/Q2/Q3/Q4. Got {found!r}."
    )


def test_canonical_q_naming_still_short_circuits(monkeypatch):
    """Regression: when canonical Q1/Q2/Q3/Q4 exist (the common case),
    the helper returns immediately without attempting the suffixed form."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                {"id": "Y2025", "name": "2025 - Purchase Orders"},
                            ],
                            "Q2_2025": [
                                {"id": "HIT", "name": "4500700000"},
                            ],
                        }.get(parent, []))

    ff_calls = []

    def _ff(name, parent):
        ff_calls.append((name, parent))
        if parent == "Y2025" and name == "Q2":
            return "Q2_2025"
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    found = gdrive.find_po_folder("2025", "Q2", "4500700000")
    assert found == "HIT"
    # Canonical Q2 hit on first try; we should NOT have queried "Q2 2025".
    suffixed_call = ("Q2 2025", "Y2025")
    assert suffixed_call not in ff_calls, (
        f"Canonical Q2 was found, but `_find_legacy_quarter_folder` still "
        f"queried suffixed form (`Q2 2025`). That's a perf regression — "
        f"helper must short-circuit on canonical match. Calls: {ff_calls}"
    )


def test_cross_year_combined_with_q_year_suffix(monkeypatch):
    """The hardest real case: order created 2026, PO filed in 2023, AND
    the 2023 folder uses `Q1 2023`-style naming. Both fallbacks must
    fire together."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                {"id": "Y2026", "name": "2026 - Purchase Orders"},
                                {"id": "Y2025", "name": "2025 - Purchase Orders"},
                                {"id": "Y2024", "name": "2024 - Purchase Orders"},
                                {"id": "Y2023", "name": "2023 - Purchase orders"},
                            ],
                            "Q4_2023_SUFFIXED": [
                                {"id": "STUB_PO", "name": "4500678548"},
                            ],
                        }.get(parent, []))

    def _ff(name, parent):
        # 2026/2025/2024 — no quarters exist (empty year folders)
        if parent in ("Y2026", "Y2025", "Y2024"):
            return None
        # 2023 has only Q4 with the year-suffixed name
        if parent == "Y2023":
            if name == "Q4 2023":
                return "Q4_2023_SUFFIXED"
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    # Audit thinks 2026/Q1, PO actually at 2023/Q4 2023
    found = gdrive.find_po_folder("2026", "Q1", "4500678548")
    assert found == "STUB_PO", (
        f"Combined cross-year + Q-suffix lookup failed; got {found!r}. "
        f"This is the dominant shape of the 14 STUB-* misses with year=2023."
    )


def test_cross_year_caps_at_3_years_back(monkeypatch):
    """Bound: don't iterate forever. A PO from 2018 should not be found
    even if the legacy folder exists. Caps API call count for unarchived
    POs."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders",
                        lambda parent: {
                            "ROOT_ID": [
                                # Only the 2018 year exists (way back)
                                {"id": "Y2018", "name": "2018 - Purchase Orders"},
                            ],
                            "Q1_2018": [
                                {"id": "ANCIENT_PO", "name": "1234567890"},
                            ],
                        }.get(parent, []))

    def _ff(name, parent):
        if parent == "Y2018" and name == "Q1":
            return "Q1_2018"
        return None

    monkeypatch.setattr(gdrive, "find_folder", _ff)

    # Audit thinks 2026; PO actually in 2018 (>3 years back)
    found = gdrive.find_po_folder("2026", "Q1", "1234567890")
    assert found is None, (
        f"Cross-year scan should cap at 3 prior years; 2018 from a "
        f"2026 audit must not be reached. Got {found!r}."
    )


def test_cross_year_does_not_kick_in_when_found_in_target_year(monkeypatch):
    """Perf: when the PO is found in the target year (the common case),
    we must NOT touch any prior-year folder."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")

    sub_calls = []

    def _list_sub(parent):
        sub_calls.append(parent)
        return {
            "ROOT_ID": [
                {"id": "Y2026", "name": "2026 - Purchase Orders"},
                {"id": "Y2025", "name": "2025 - Purchase Orders"},
            ],
            "Q2_2026": [
                {"id": "HIT", "name": "4500770000"},
            ],
        }.get(parent, [])

    monkeypatch.setattr(gdrive, "_list_subfolders", _list_sub)
    monkeypatch.setattr(gdrive, "find_folder",
                        lambda name, parent:
                        "Q2_2026" if (parent == "Y2026" and name == "Q2") else None)

    found = gdrive.find_po_folder("2026", "Q2", "4500770000")
    assert found == "HIT"
    # ROOT_ID list (year resolution) + Q2_2026 list (po match) = 2 calls.
    # Must NOT include Y2025-* anything.
    assert "Y2025" not in [c for c in sub_calls], (
        f"Prior-year folder was listed even though target year had the "
        f"PO. Sub calls: {sub_calls}"
    )


def test_cross_year_with_non_numeric_year_handled_gracefully(monkeypatch):
    """If `year` arg is non-numeric (e.g. 'Q?' from the audit's parse-
    failure path), cross-year fallback must not crash — just return
    None (or fall through to app tree)."""
    from src.core import gdrive

    monkeypatch.setattr(gdrive, "GOOGLE_DRIVE_ROOT_FOLDER_ID", "ROOT_ID")
    monkeypatch.setattr(gdrive, "_list_subfolders", lambda parent: [])
    monkeypatch.setattr(gdrive, "find_folder", lambda name, parent: None)

    # Should not raise
    found = gdrive.find_po_folder("Q?", "Q?", "4500685000")
    assert found is None
