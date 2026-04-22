"""Regression guard: IMAP backend was ripped out 2026-04-21.

Gmail API is the sole inbound email path. A runtime IMAP import sneaking back
into src/ is a ship-blocker — it would silently reintroduce the old fallback
that hid auth failures and kept the legacy UID code alive.

This test scans src/ for `import imaplib`, `from imaplib import ...`, and
`imaplib.IMAP4_SSL(` calls and fails if any are found.

Comments and docstrings that mention "IMAP" for historical context are fine;
we only forbid the runtime hooks.
"""
from __future__ import annotations

import pathlib
import re


SRC = pathlib.Path(__file__).resolve().parents[1] / "src"

# Patterns that indicate live IMAP use (not just historical commentary)
_FORBIDDEN = [
    re.compile(r"^\s*import\s+imaplib\b", re.MULTILINE),
    re.compile(r"^\s*from\s+imaplib\s+import\b", re.MULTILINE),
    re.compile(r"\bimaplib\.IMAP4(?:_SSL)?\s*\("),
]


def test_no_imap_runtime_refs_in_src():
    offenders: list[str] = []
    for py in SRC.rglob("*.py"):
        text = py.read_text(encoding="utf-8", errors="replace")
        for pat in _FORBIDDEN:
            for m in pat.finditer(text):
                line_no = text.count("\n", 0, m.start()) + 1
                offenders.append(f"{py.relative_to(SRC.parent)}:{line_no}: {m.group(0).strip()}")

    assert not offenders, (
        "IMAP runtime usage leaked back into src/ (Gmail API is the only "
        "supported inbound backend as of 2026-04-21):\n  "
        + "\n  ".join(offenders)
    )
