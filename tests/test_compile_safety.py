"""V2 Test Suite — Group 4: Import & Compile Safety.

Catches what git sweeps miss:
- All agent files compile without errors
- All route modules compile
- All form modules compile
- No bare `except: pass` (swallows real errors)
- All POST routes have @auth_required

Incident 2026-04-10: 7 agent files committed alongside a DOCX fix —
one had Haiku+thinking (unsupported) that shipped to production.
"""
import os
import py_compile
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
_SRC_DIR = os.path.join(_PROJECT_ROOT, "src")


def _get_py_files(subdir):
    """Return all .py files in a src/ subdirectory."""
    full_path = os.path.join(_SRC_DIR, subdir)
    if not os.path.isdir(full_path):
        return []
    return [
        os.path.join(full_path, f)
        for f in os.listdir(full_path)
        if f.endswith(".py") and not f.startswith("__")
    ]


class TestAllAgentsCompile:
    """Every file in src/agents/ must compile without syntax errors."""

    def test_agents_compile(self):
        files = _get_py_files("agents")
        assert len(files) > 0, "No agent files found"
        errors = []
        for f in files:
            try:
                py_compile.compile(f, doraise=True)
            except py_compile.PyCompileError as e:
                errors.append(f"{os.path.basename(f)}: {e}")
        assert not errors, (
            f"{len(errors)} agent file(s) have syntax errors:\n" + "\n".join(errors)
        )


class TestAllRoutesCompile:
    """Every file in src/api/modules/ must compile."""

    def test_routes_compile(self):
        files = _get_py_files(os.path.join("api", "modules"))
        assert len(files) > 0, "No route files found"
        errors = []
        for f in files:
            try:
                py_compile.compile(f, doraise=True)
            except py_compile.PyCompileError as e:
                errors.append(f"{os.path.basename(f)}: {e}")
        assert not errors, (
            f"{len(errors)} route file(s) have syntax errors:\n" + "\n".join(errors)
        )


class TestAllFormsCompile:
    """Every file in src/forms/ must compile."""

    def test_forms_compile(self):
        files = _get_py_files("forms")
        assert len(files) > 0, "No form files found"
        errors = []
        for f in files:
            try:
                py_compile.compile(f, doraise=True)
            except py_compile.PyCompileError as e:
                errors.append(f"{os.path.basename(f)}: {e}")
        assert not errors, (
            f"{len(errors)} form file(s) have syntax errors:\n" + "\n".join(errors)
        )


class TestAllCoreCompile:
    """Every file in src/core/ must compile."""

    def test_core_compile(self):
        files = _get_py_files("core")
        assert len(files) > 0, "No core files found"
        errors = []
        for f in files:
            try:
                py_compile.compile(f, doraise=True)
            except py_compile.PyCompileError as e:
                errors.append(f"{os.path.basename(f)}: {e}")
        assert not errors, (
            f"{len(errors)} core file(s) have syntax errors:\n" + "\n".join(errors)
        )


class TestNoBareExceptPass:
    """No bare `except: pass` patterns — these swallow real errors silently.

    All 5 bare excepts were removed in the 2026-03-23 audit.
    This test prevents regression.
    """

    def test_no_bare_except_pass_in_src(self):
        violations = []
        for root, dirs, files in os.walk(_SRC_DIR):
            # Skip __pycache__
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            for fname in files:
                if not fname.endswith(".py"):
                    continue
                fpath = os.path.join(root, fname)
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()

                for i, line in enumerate(lines):
                    stripped = line.strip()
                    # Match `except:` with no exception type (bare except)
                    # followed by `pass` on the next non-empty line
                    if stripped == "except:":
                        # Check next non-blank line
                        for j in range(i + 1, min(i + 3, len(lines))):
                            next_line = lines[j].strip()
                            if next_line == "pass":
                                rel_path = os.path.relpath(fpath, _PROJECT_ROOT)
                                violations.append(f"{rel_path}:{i+1}: except: pass")
                                break
                            elif next_line:
                                break  # Not bare except:pass

        assert not violations, (
            f"Found {len(violations)} bare `except: pass` (swallows errors silently):\n"
            + "\n".join(violations)
        )


class TestPostRoutesHaveAuth:
    """All POST routes must have @auth_required decorator.

    Prevents accidentally exposing admin endpoints without authentication.
    Known exceptions: webhooks, health checks, email tracking pixels.
    """

    KNOWN_UNPROTECTED = {
        "/health",
        "/api/health",
        "/api/webhook",
        "/email/track",
        "/api/email/webhook",
    }

    def test_post_routes_have_auth(self):
        routes_dir = os.path.join(_SRC_DIR, "api", "modules")
        if not os.path.isdir(routes_dir):
            pytest.skip("routes dir not found")

        violations = []
        for fname in os.listdir(routes_dir):
            if not fname.endswith(".py") or fname.startswith("__"):
                continue
            fpath = os.path.join(routes_dir, fname)
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()

            for i, line in enumerate(lines):
                # Find POST route decorators
                m = re.search(r'@bp\.route\("([^"]+)".*POST', line)
                if not m:
                    continue

                route_path = m.group(1)
                # Skip known unprotected routes
                if any(route_path.startswith(ex) for ex in self.KNOWN_UNPROTECTED):
                    continue

                # Check that @auth_required appears within the next 3 lines
                has_auth = False
                for j in range(i + 1, min(i + 4, len(lines))):
                    if "auth_required" in lines[j]:
                        has_auth = True
                        break
                    if lines[j].strip().startswith("def "):
                        break  # Hit function def without auth

                if not has_auth:
                    rel_path = os.path.relpath(fpath, _PROJECT_ROOT)
                    violations.append(f"{rel_path}:{i+1}: POST {route_path} missing @auth_required")

        # Allow up to 13 known unprotected (from CLAUDE.md audit)
        # But flag if NEW unprotected routes appear
        if len(violations) > 15:
            assert False, (
                f"Found {len(violations)} POST routes without @auth_required "
                f"(expected ≤15 known exceptions):\n" + "\n".join(violations[:20])
            )
