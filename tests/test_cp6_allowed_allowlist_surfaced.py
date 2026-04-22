"""CP-6 regression guard: update_product_pricing silently dropped any
kwarg not in ALLOWED and still returned True when at least one kwarg
landed. Operators saw "Saved" but msrp / web_lowest_source / other
keys vanished. The API endpoint now returns the rejected-key list so
the UI can show the truth.

This is a source-level guard — the return value and route plumbing
are verified. Runtime DB behavior is covered by existing catalog tests.
"""
from __future__ import annotations

import re
from pathlib import Path


PRODUCT_CATALOG = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "agents"
    / "product_catalog.py"
)

ROUTES_CF = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_catalog_finance.py"
)


def _strip_comment_lines(src: str) -> str:
    kept = []
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def _func_body(path: Path, name: str) -> str:
    src = path.read_text(encoding="utf-8")
    m = re.search(
        rf"def {name}\([^)]*\)[\s\S]*?(?=\ndef [a-zA-Z_]|\nclass |\Z)",
        src,
    )
    assert m, f"{name} body not located in {path.name}"
    return m.group(0)


def test_update_product_pricing_returns_dict_with_rejected():
    """update_product_pricing must now return a dict containing
    `ok`, `written`, `rejected` (not a bare bool)."""
    body = _strip_comment_lines(_func_body(PRODUCT_CATALOG, "update_product_pricing"))
    # Two return paths (empty-allowed and success). Both must surface
    # the rejected list.
    returns = re.findall(r"return\s+\{[^}]*\}", body)
    assert len(returns) >= 2, (
        "CP-6 regression: expected >=2 dict-returning paths in "
        f"update_product_pricing, found {len(returns)}."
    )
    for r in returns:
        assert '"rejected"' in r or "'rejected'" in r, (
            f"CP-6 regression: return `{r[:60]}...` omits rejected key list."
        )
        assert '"written"' in r or "'written'" in r, (
            f"CP-6 regression: return `{r[:60]}...` omits written key list."
        )


def test_rejected_keys_are_warned_at_runtime():
    """A warning log must fire when keys are dropped so that silent
    drops leave a trail in server logs."""
    body = _strip_comment_lines(_func_body(PRODUCT_CATALOG, "update_product_pricing"))
    assert re.search(
        r"log\.warning\([^)]*update_product_pricing",
        body,
    ), (
        "CP-6 regression: the log.warning on rejected keys is missing. "
        "Silent-drop no longer leaves a log trail."
    )


def test_no_bare_true_false_return_from_update():
    """Neither `return True` nor `return False` should survive — both
    paths were the silent bool that hid rejected keys."""
    body = _strip_comment_lines(_func_body(PRODUCT_CATALOG, "update_product_pricing"))
    assert not re.search(r"(?m)^\s*return\s+True\s*$", body), (
        "CP-6 regression: `return True` is back — callers can no "
        "longer see the rejected key list."
    )
    assert not re.search(r"(?m)^\s*return\s+False\s*$", body), (
        "CP-6 regression: `return False` is back — callers can no "
        "longer see the rejected key list."
    )


def test_api_catalog_update_surfaces_full_result():
    """The /api/catalog/<pid>/update route must jsonify the full dict
    returned by update_product_pricing (not wrap it in {'ok': ok})."""
    body = _strip_comment_lines(_func_body(ROUTES_CF, "api_catalog_update"))
    # The fix uses `jsonify(result)` where result is the full dict.
    # The banned shape wrapped a bool: `jsonify({"ok": ok})`.
    banned = re.search(r'jsonify\(\s*\{\s*"ok"\s*:\s*ok\s*\}\s*\)', body)
    assert not banned, (
        "CP-6 regression: the route is back to `jsonify({'ok': ok})`, "
        "which drops the rejected key list."
    )
    assert re.search(r"jsonify\(\s*result\s*\)", body), (
        "CP-6 regression: expected `jsonify(result)` to surface the "
        "full dict from update_product_pricing."
    )


def test_modules_still_compile():
    import py_compile
    py_compile.compile(str(PRODUCT_CATALOG), doraise=True)
    py_compile.compile(str(ROUTES_CF), doraise=True)
