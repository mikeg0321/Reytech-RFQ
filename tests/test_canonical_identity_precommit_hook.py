"""IN-6 regression guard: `.githooks/pre-commit` exists and encodes the
same canonical-identity forbidden-token list as the pre-push test sweep.

The sweep at `tests/test_canonical_identity_sweep.py` is excellent at blocking
bad identity drift before PUSH — but by that point the commit is already in
the branch. The pre-commit hook catches drift before the commit even lands,
shortening the feedback loop from ~90s (pre-push suite) to <1s (fast grep).

Both defences must agree on the *definition* of "wrong". If someone adds a
new wrong-identity variant to the test but forgets the hook (or vice versa)
a leak slips through. This guard keeps the two in lockstep.
"""
from __future__ import annotations

import pathlib
import re

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
HOOK_PATH = REPO_ROOT / ".githooks" / "pre-commit"
SWEEP_TEST = REPO_ROOT / "tests" / "test_canonical_identity_sweep.py"


def _forbidden_from_sweep_test():
    """Parse the FORBIDDEN_IDENTITY_TOKENS list from the sweep test source.

    We parse the file directly (not import) because we want exactly what the
    assertion references — not whatever other code has mutated at import
    time."""
    src = SWEEP_TEST.read_text(encoding="utf-8")
    m = re.search(
        r"FORBIDDEN_IDENTITY_TOKENS\s*=\s*\[(.+?)\]", src, re.DOTALL
    )
    assert m, "test_canonical_identity_sweep.py no longer defines FORBIDDEN_IDENTITY_TOKENS"
    body = m.group(1)
    return set(re.findall(r'"([^"\n]+)"', body))


def test_pre_commit_hook_exists_and_is_executable():
    assert HOOK_PATH.exists(), (
        ".githooks/pre-commit is missing — IN-6 regression. Restore the hook "
        "that grep-guards canonical identity on staged route files."
    )
    # On Windows/NTFS the exec bit is not always meaningful, but git respects
    # the hook file regardless. We just confirm it's a regular file.
    assert HOOK_PATH.is_file()


def test_hook_forbidden_list_matches_sweep_test():
    hook_src = HOOK_PATH.read_text(encoding="utf-8")
    sweep_tokens = _forbidden_from_sweep_test()
    # Each sweep token must appear verbatim in the hook source. We only
    # check direction sweep→hook (the hook must cover everything the test
    # considers forbidden). The hook may legitimately carry additional
    # tokens as a front-line defence, so we don't assert the reverse.
    missing = [t for t in sweep_tokens if t not in hook_src]
    assert not missing, (
        f"Pre-commit hook is missing forbidden tokens that the pre-push "
        f"sweep enforces: {missing}. Both defences must agree — update "
        f".githooks/pre-commit FORBIDDEN array."
    )


def test_hook_scopes_to_routes_modules_only():
    # Sanity: make sure the hook narrows to src/api/modules/routes_*.py and
    # isn't a global ban. A project-wide ban would break legitimate test
    # fixtures and archived email samples.
    hook_src = HOOK_PATH.read_text(encoding="utf-8")
    assert "src/api/modules/routes_" in hook_src, (
        "Hook lost its route-module scope filter — a global ban would break "
        "test fixtures that legitimately carry historical identity strings."
    )
