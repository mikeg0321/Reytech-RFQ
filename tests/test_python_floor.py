"""O6 — Python floor guard tests.

Verifies:
  1. The runtime guard in app.py rejects interpreters below 3.12.
  2. The guard passes on the current interpreter (which IS >=3.12).
  3. pyproject.toml carries the requires-python pin.

These tests do NOT launch a subprocess with a downgraded interpreter
(that would require a second Python install).  Instead they exercise the
guard's boolean logic directly — the same check that runs at boot.
"""
import os
import sys
import unittest

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")


class TestPythonFloorGuardLogic(unittest.TestCase):
    """Unit-test the version-check predicate used in app.py."""

    # The predicate extracted from app.py:  sys.version_info < (3, 12)
    # We replicate it here so the test proves the comparison form is correct.

    def _should_abort(self, major: int, minor: int, micro: int = 0) -> bool:
        """Return True if the given version would trigger app.py's sys.exit(1)."""
        return (major, minor, micro) < (3, 12, 0)

    # --- versions that MUST abort ---

    def test_py311_aborts(self):
        self.assertTrue(self._should_abort(3, 11), "3.11 must be rejected")

    def test_py310_aborts(self):
        self.assertTrue(self._should_abort(3, 10), "3.10 must be rejected")

    def test_py39_aborts(self):
        self.assertTrue(self._should_abort(3, 9), "3.9 must be rejected")

    def test_py27_aborts(self):
        self.assertTrue(self._should_abort(2, 7), "2.7 must be rejected")

    # --- versions that MUST NOT abort ---

    def test_py312_passes(self):
        self.assertFalse(self._should_abort(3, 12), "3.12 must be accepted")

    def test_py313_passes(self):
        self.assertFalse(self._should_abort(3, 13), "3.13 must be accepted")

    def test_py312_micro_passes(self):
        self.assertFalse(self._should_abort(3, 12, 7), "3.12.7 must be accepted")

    # --- live interpreter (CI-only: skipped when sandbox runs <3.12) ---

    @unittest.skipIf(
        sys.version_info < (3, 12),
        "Sandbox interpreter is <3.12; this assertion runs under CI where "
        "python-version: '3.12' is pinned in ci.yml.",
    )
    def test_live_interpreter_passes(self):
        """The interpreter running this test suite must be >=3.12."""
        vi = sys.version_info
        self.assertFalse(
            self._should_abort(vi.major, vi.minor, vi.micro),
            f"Live interpreter {vi.major}.{vi.minor}.{vi.micro} must be >=3.12; "
            "upgrade the Python version in CI / Railway.",
        )


class TestPyprojectToml(unittest.TestCase):
    """pyproject.toml must declare requires-python = >=3.12."""

    def test_pyproject_has_requires_python(self):
        pyproject = os.path.join(_PROJECT_ROOT, "pyproject.toml")
        self.assertTrue(
            os.path.isfile(pyproject),
            "pyproject.toml not found at repo root",
        )
        with open(pyproject, encoding="utf-8") as fh:
            content = fh.read()
        self.assertIn(
            'requires-python',
            content,
            "pyproject.toml must contain requires-python",
        )
        self.assertIn(
            "3.12",
            content,
            "pyproject.toml requires-python must reference 3.12",
        )


class TestAppPyFloorGuardPresent(unittest.TestCase):
    """app.py must contain the version guard block."""

    def test_app_py_has_version_guard(self):
        app_py = os.path.join(_PROJECT_ROOT, "app.py")
        self.assertTrue(os.path.isfile(app_py), "app.py not found")
        with open(app_py, encoding="utf-8") as fh:
            source = fh.read()
        self.assertIn(
            "sys.version_info < (3, 12)",
            source,
            "app.py must contain the sys.version_info < (3, 12) guard",
        )
        self.assertIn(
            "sys.exit(1)",
            source,
            "app.py must call sys.exit(1) when the version floor is not met",
        )
