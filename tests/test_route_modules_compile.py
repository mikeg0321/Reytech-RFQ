"""Guard the Python 3.12 floor (creepy-crawler O6, 2026-05-30).

Five route modules use PEP-701 f-strings (a backslash inside an f-string
expression), which only parse on Python >= 3.12. On an older interpreter they
raise SyntaxError and `dashboard._load_route_module` drops them *silently* —
the whole route surface of that module vanishes with no test failure. Prod is
3.12, so this is latent, but a runner or contributor on <3.12 loses routes.

This test makes a NEW compile/syntax break in any route module fail loudly on
the supported runtime. It is version-guarded: on the supported interpreter
every `routes_*.py` must compile; on an unsupported one it skips with a reason
(rather than failing for an expected, documented cause).
"""
import glob
import os
import py_compile
import sys

import pytest

_ROUTES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "src", "api", "modules"
)
_ROUTE_FILES = sorted(glob.glob(os.path.join(_ROUTES_DIR, "routes_*.py")))


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="App requires Python >= 3.12 (PEP-701 f-strings); skip the compile "
    "floor on unsupported interpreters where the SyntaxError is expected.",
)
@pytest.mark.parametrize("path", _ROUTE_FILES, ids=lambda p: os.path.basename(p))
def test_route_module_compiles(path):
    # doraise=True turns a SyntaxError into a test failure instead of a silent
    # drop at boot.
    py_compile.compile(path, doraise=True)


def test_route_files_discovered():
    # Sanity: the glob actually found the modules (guards against a moved dir
    # making the parametrized test vacuously pass with zero cases).
    assert len(_ROUTE_FILES) >= 50, _ROUTE_FILES
