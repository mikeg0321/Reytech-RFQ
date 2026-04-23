"""Contract tests for .githooks/pre-push Chrome-verify gate.

Guards the incident that landed Bundle-5 + Bundle-6 to prod without real
Chrome-MCP visual verification: pytest template-render assertions and
HTTP 200 checks are not enough for user-facing changes. The pre-push
hook now blocks any push that touches HTML / JS / CSS unless the pusher
provides explicit proof via CHROME_VERIFIED=1 env var or a
CHROME-VERIFIED: commit-message trailer.

These tests exercise the hook in an isolated temp git repo so we never
touch the real repo history or network.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
HOOK_PATH = REPO_ROOT / ".githooks" / "pre-push"


def _run(cmd, cwd, env=None, input_text=None):
    full_env = os.environ.copy()
    # Let the hook actually fail rather than inherit CHROME_VERIFIED from the
    # parent shell that's running the test.
    full_env.pop("CHROME_VERIFIED", None)
    full_env.pop("CHROME_VERIFIED_SKIP", None)
    if env:
        full_env.update(env)
    return subprocess.run(
        cmd, cwd=str(cwd), env=full_env, input=input_text,
        capture_output=True, text=True, shell=isinstance(cmd, str),
    )


@pytest.fixture
def git_repo(tmp_path):
    """Init a throwaway git repo with the real hook installed + a fake origin/main."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _run(["git", "init", "--initial-branch=main", "-q"], cwd=repo)
    _run(["git", "config", "user.email", "test@test"], cwd=repo)
    _run(["git", "config", "user.name", "Test"], cwd=repo)
    # Install the hook under .git/hooks so git runs it directly (bypasses
    # core.hooksPath differences across the host repo vs the tmp repo).
    hook_dst = repo / ".git" / "hooks" / "pre-push"
    hook_dst.parent.mkdir(parents=True, exist_ok=True)
    hook_src = HOOK_PATH.read_text(encoding="utf-8")
    # Disable the critical-test gate in the test repo — we're only asserting
    # the UI-change gate here. The pytest block tries to import the real
    # test suite which doesn't exist in the tmp repo. Replace the whole
    # multi-line pytest invocation with a single `true` + synthesized RESULT=0.
    import re as _re
    hook_src = _re.sub(
        r"echo \"Running critical test suite\.\.\.\"\n.*?RESULT=\$\?",
        "echo \"Running critical test suite... (disabled in test)\"\nRESULT=0",
        hook_src,
        count=1,
        flags=_re.DOTALL,
    )
    hook_dst.write_text(hook_src, encoding="utf-8")
    hook_dst.chmod(0o755)

    # Make an initial commit so HEAD exists, and a fake origin/main that
    # points to it — the hook's fallback path diffs against origin/<branch>.
    (repo / "README.md").write_text("seed\n")
    _run(["git", "add", "README.md"], cwd=repo)
    _run(["git", "commit", "-q", "-m", "seed"], cwd=repo)
    _run(["git", "branch", "-f", "origin/main"], cwd=repo)  # not a real remote ref
    # Create a worktree ref name the hook can compare against: origin/<branch>.
    _run(["git", "update-ref", "refs/remotes/origin/main", "HEAD"], cwd=repo)

    # Move to a feature branch so the hook doesn't trip the "no push to main" gate.
    _run(["git", "checkout", "-q", "-b", "feat/test"], cwd=repo)
    return repo


def _commit_ui_change(repo, msg="feat: tweak modal", trailer=None):
    ui = repo / "src" / "templates" / "foo.html"
    ui.parent.mkdir(parents=True, exist_ok=True)
    # Append a byte so each call creates a real change.
    ui.write_text(ui.read_text() + "<x>\n" if ui.exists() else "<x>\n")
    _run(["git", "add", "src/templates/foo.html"], cwd=repo)
    body = msg
    if trailer:
        body += "\n\n" + trailer
    _run(["git", "commit", "-q", "-m", body], cwd=repo)


def _commit_python_change(repo, msg="fix: backend only"):
    p = repo / "src" / "core" / "thing.py"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text((p.read_text() if p.exists() else "") + "# change\n")
    _run(["git", "add", "src/core/thing.py"], cwd=repo)
    _run(["git", "commit", "-q", "-m", msg], cwd=repo)


def _invoke_hook(repo, env=None):
    """Run the hook directly with a feature branch context (no stdin)."""
    result = _run(
        ["bash", ".git/hooks/pre-push", "origin", "https://example.git"],
        cwd=repo, env=env,
    )
    return result


# ── UI change without proof → BLOCKED ───────────────────────────────────────


def test_ui_change_without_proof_blocks_push(git_repo):
    _commit_ui_change(git_repo)
    r = _invoke_hook(git_repo)
    assert r.returncode == 1, (
        f"Hook should BLOCK a UI push with no proof.\n"
        f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )
    assert "PUSH BLOCKED" in r.stdout
    assert "UI file(s) changed without Chrome-MCP proof" in r.stdout
    assert "src/templates/foo.html" in r.stdout


def test_ui_change_with_env_flag_passes(git_repo):
    _commit_ui_change(git_repo)
    r = _invoke_hook(git_repo, env={"CHROME_VERIFIED": "1"})
    assert r.returncode == 0, (
        f"Hook should PASS with CHROME_VERIFIED=1.\n"
        f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )
    assert "CHROME_VERIFIED=1 env flag present" in r.stdout


def test_ui_change_with_commit_trailer_passes(git_repo):
    _commit_ui_change(git_repo,
                      msg="feat: modal wiring",
                      trailer="CHROME-VERIFIED: opened /rfq/x, clicked btn, saw toast.")
    r = _invoke_hook(git_repo)
    assert r.returncode == 0, (
        f"Hook should PASS with a CHROME-VERIFIED: trailer.\n"
        f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )
    assert "CHROME-VERIFIED: commit trailer found" in r.stdout


def test_ui_change_with_skip_flag_passes(git_repo):
    """Escape hatch for chore/revert/docs only — prints a reminder."""
    _commit_ui_change(git_repo, msg="revert: chore")
    r = _invoke_hook(git_repo, env={"CHROME_VERIFIED_SKIP": "1"})
    assert r.returncode == 0, r.stdout + r.stderr
    assert "SKIP flag set" in r.stdout


def test_non_ui_change_does_not_require_proof(git_repo):
    """Pure Python/backend changes bypass the gate."""
    _commit_python_change(git_repo)
    r = _invoke_hook(git_repo)
    assert r.returncode == 0, (
        f"Non-UI push should pass with no proof.\n"
        f"stdout:\n{r.stdout}\nstderr:\n{r.stderr}"
    )
    assert "UI file(s) changed without Chrome-MCP proof" not in r.stdout


def test_css_change_triggers_gate(git_repo):
    p = git_repo / "src" / "static" / "style.css"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(".x{}\n")
    _run(["git", "add", "src/static/style.css"], cwd=git_repo)
    _run(["git", "commit", "-q", "-m", "style tweak"], cwd=git_repo)
    r = _invoke_hook(git_repo)
    assert r.returncode == 1
    assert "src/static/style.css" in r.stdout


def test_js_change_triggers_gate(git_repo):
    p = git_repo / "src" / "static" / "app.js"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("window.x=1;\n")
    _run(["git", "add", "src/static/app.js"], cwd=git_repo)
    _run(["git", "commit", "-q", "-m", "js tweak"], cwd=git_repo)
    r = _invoke_hook(git_repo)
    assert r.returncode == 1
    assert "src/static/app.js" in r.stdout


def test_mixed_commit_ui_plus_backend_requires_proof(git_repo):
    """A commit that touches both a template and a .py still needs proof."""
    # Backend-only in commit 1 (fine).
    _commit_python_change(git_repo)
    # Mixed in commit 2 (UI triggers gate).
    _commit_ui_change(git_repo, msg="feat: ui + backend")
    r = _invoke_hook(git_repo)
    assert r.returncode == 1
    assert "src/templates/foo.html" in r.stdout


def test_trailer_on_any_pushed_commit_satisfies_gate(git_repo):
    """The trailer doesn't have to be on the UI commit itself — any commit
    in the push range can carry it, because the whole range is the 'unit'
    being visually verified."""
    _commit_ui_change(git_repo, msg="feat: add banner")
    _commit_python_change(git_repo,
                          msg="test: backend coverage\n\nCHROME-VERIFIED: full prod re-sweep 2026-04-23")
    r = _invoke_hook(git_repo)
    assert r.returncode == 0, r.stdout + r.stderr
