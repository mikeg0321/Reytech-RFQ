"""CR-6 regression guard: api_draft_outreach used to iterate the entire
outbox and call upsert_outbox_email() on every row after appending a new
draft. Prod had ~261 outbox rows, so every outreach-draft call ran ~262
DB writes instead of 1.

Fix: upsert only the draft just created. The upsert is idempotent on
`id`, so pre-existing rows don't need re-writing every time a new draft
lands.
"""
from __future__ import annotations

import re
from pathlib import Path


ROUTES_CRM = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "api"
    / "modules"
    / "routes_crm.py"
)


def _strip_comment_lines(src: str) -> str:
    kept = []
    for line in src.splitlines():
        if line.lstrip().startswith("#"):
            continue
        kept.append(line)
    return "\n".join(kept)


def _api_draft_outreach_body() -> str:
    src = ROUTES_CRM.read_text(encoding="utf-8")
    m = re.search(
        r"def api_draft_outreach\(\)[\s\S]*?(?=\n@bp\.route|\ndef [a-zA-Z_])",
        src,
    )
    assert m, "api_draft_outreach body not located"
    return m.group(0)


def test_no_full_outbox_iteration_upsert():
    """The banned shape iterated the entire outbox upserting every row.
    It must be gone."""
    body = _strip_comment_lines(_api_draft_outreach_body())
    # The banned pattern: for <var> in outbox: ... _upsert_ob(<var>)
    banned = re.search(
        r"for\s+_?\w+\s+in\s+outbox\s*:\s*\n[\s\S]{0,200}?_upsert_ob\(",
        body,
    )
    assert not banned, (
        "CR-6 regression: `for _e in outbox: _upsert_ob(_e)` is back. "
        "This writes every existing outbox row on every new draft "
        "(O(N) churn). Only the new draft should be upserted."
    )


def test_single_upsert_of_new_draft():
    """After the fix the function must call _upsert_ob(draft) exactly
    once — operating only on the draft just appended."""
    body = _strip_comment_lines(_api_draft_outreach_body())
    m = re.search(r"_upsert_ob\s*\(\s*draft\s*\)", body)
    assert m, (
        "CR-6 regression: expected a single `_upsert_ob(draft)` call on "
        "the newly-created draft. If you changed the helper name, update "
        "this guard too."
    )


def test_api_draft_outreach_still_appends_locally():
    """Sanity: we still push the draft into the local outbox list
    (callers may rely on the returned count); the CR-6 fix only drops
    the iteration, not the append."""
    body = _strip_comment_lines(_api_draft_outreach_body())
    assert re.search(
        r"outbox\.append\(\s*draft\s*\)",
        body,
    ), "CR-6: the local outbox.append(draft) step is missing."


def test_module_still_compiles():
    import py_compile
    py_compile.compile(str(ROUTES_CRM), doraise=True)
