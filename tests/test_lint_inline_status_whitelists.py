"""Pin behavior of `tools/lint_inline_status_whitelists.py` (Tier 1c
audit follow-on, 2026-05-07).

The lint catches inline `WHERE status [NOT] IN ('a', 'b', ...)` SQL
literals in route modules. PR #832 collapsed 5 Python `valid = {...}`
status whitelists into the canonical `status_taxonomy.is_valid_status_for()`
predicate. This lint covers the SQL flavor of the same regression
class — and surfaces 30 currently-known sites of inline status
literal use as explicit technical debt via the EXEMPTIONS list.

These tests pin:
  1. The detection regex catches the offender shape.
  2. Parameterized SQL (`WHERE status IN ({placeholders})`) is NOT
     a violation.
  3. The exemption mechanism works — adding a known literal to
     EXEMPTIONS suppresses the violation.
  4. Stale exemptions (literal moved or removed) ALSO trip the
     lint, so cleanup PRs naturally remove their exemptions.
  5. The current state of the repo is clean (all matches covered
     by exemptions).
"""
from __future__ import annotations

from pathlib import Path
import importlib.util
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
LINT_PATH = REPO_ROOT / "tools" / "lint_inline_status_whitelists.py"


def _load_lint_module():
    """Load the lint script as a module without executing __main__."""
    spec = importlib.util.spec_from_file_location(
        "_lint_inline_status_whitelists", LINT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_detection_regex_catches_basic_where_clause():
    mod = _load_lint_module()
    sql = "SELECT * FROM rfq_records WHERE status IN ('new', 'sent')"
    matches = list(mod._WHERE_STATUS_IN.finditer(sql))
    assert len(matches) == 1
    assert "WHERE status IN" in matches[0].group(0)


def test_detection_regex_catches_compound_clause():
    """`WHERE x = ? AND status IN (...)` — common in route modules."""
    mod = _load_lint_module()
    sql = ("SELECT * FROM rfq_records WHERE created_at > ? "
           "AND status IN ('won', 'lost')")
    matches = list(mod._WHERE_STATUS_IN.finditer(sql))
    assert len(matches) == 1
    assert "AND status IN" in matches[0].group(0)


def test_detection_regex_catches_not_in():
    mod = _load_lint_module()
    sql = "WHERE due_date < ? AND status NOT IN ('sent', 'won', 'lost')"
    matches = list(mod._WHERE_STATUS_IN.finditer(sql))
    assert len(matches) == 1
    assert "NOT IN" in matches[0].group(0)


def test_detection_regex_ignores_parameterized():
    """`WHERE status IN ({placeholders})` is parameterized — safe."""
    mod = _load_lint_module()
    sql = "WHERE status IN ({placeholders})"
    matches = list(mod._WHERE_STATUS_IN.finditer(sql))
    assert len(matches) == 0, (
        "parameterized form should not be flagged — values come "
        "from the calling code, not literal SQL")


def test_detection_regex_handles_double_quotes():
    """Some routes use double-quoted strings inside SQL."""
    mod = _load_lint_module()
    sql = 'WHERE status IN ("won", "lost")'
    matches = list(mod._WHERE_STATUS_IN.finditer(sql))
    assert len(matches) == 1


def test_detection_regex_stops_at_first_close_paren():
    """`WHERE status IN ('a','b') AND created_at < datetime('now', '-7 days')`
    must capture only `WHERE status IN ('a','b')`, not the trailing
    datetime() function call."""
    mod = _load_lint_module()
    sql = ("WHERE status IN ('new', 'parsed') AND created_at < "
           "datetime('now', '-7 days')")
    matches = list(mod._WHERE_STATUS_IN.finditer(sql))
    assert len(matches) == 1
    snippet = matches[0].group(0)
    assert snippet == "WHERE status IN ('new', 'parsed')"
    assert "datetime" not in snippet


def test_strip_comments_drops_python_line_comments():
    mod = _load_lint_module()
    src = ("x = 1\n"
           "# WHERE status IN ('priced', 'sent')\n"
           "y = 2\n")
    out = mod._strip_comments(src)
    assert "WHERE status" not in out


def test_strip_comments_preserves_inside_string_literals():
    """A `#` inside a string literal must NOT be treated as a comment
    boundary — the SQL might contain `#` legitimately (e.g. as part
    of a CTE comment)."""
    mod = _load_lint_module()
    src = 'sql = "WHERE id = \'#1\' AND status IN (\'won\')"\n'
    out = mod._strip_comments(src)
    assert "WHERE id" in out


def test_current_repo_is_clean():
    """End-to-end: the lint must exit OK on the current repo state.
    Any new violation pushed without an exemption update should
    flip this test red."""
    mod = _load_lint_module()
    violations, stale = mod.find_violations(REPO_ROOT)
    assert violations == [], (
        f"unexempted inline status whitelists found:\n  " +
        "\n  ".join(f"{v['file']}:{v['line']} → {v['snippet']}"
                    for v in violations))
    assert stale == [], (
        f"stale exemptions (literal no longer present in source):\n  " +
        "\n  ".join(f"{f} → {s}" for f, s in stale))


def test_exemptions_are_well_formed():
    """Each exemption must be a (str path, str substring) pair, and
    the path must point to a file under src/api/modules/."""
    mod = _load_lint_module()
    for entry in mod.EXEMPTIONS:
        assert isinstance(entry, tuple) and len(entry) == 2, (
            f"malformed exemption: {entry}")
        path, substr = entry
        assert path.startswith("src/api/modules/"), (
            f"exemption path {path!r} not under src/api/modules/")
        assert substr.strip(), (
            f"empty exemption substring for {path}")


def test_main_returns_zero_on_clean_repo(capsys):
    """Calling `main([])` on the current repo must exit 0."""
    mod = _load_lint_module()
    rc = mod.main(["lint_inline_status_whitelists.py"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "OK" in out


def test_main_returns_one_when_stale_exemption(monkeypatch, capsys):
    """If an EXEMPTIONS entry doesn't match anything in the source
    tree, main() must exit 1 and report the stale exemption — this
    forces cleanup PRs to remove their exemption when the literal
    is migrated to a canonical predicate."""
    mod = _load_lint_module()
    # Inject a fake exemption that won't match any source.
    fake = mod.EXEMPTIONS + [
        ("src/api/modules/routes_analytics.py",
         "WHERE status IN ('this_token_does_not_exist_xyz')")
    ]
    monkeypatch.setattr(mod, "EXEMPTIONS", fake)
    rc = mod.main(["lint_inline_status_whitelists.py"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "stale exemption" in out.lower()
