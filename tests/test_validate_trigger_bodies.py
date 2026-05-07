"""Pin the SHA of migration #20's validate_* trigger SQL (issue #415).

When this hash changes, the trigger BODY in migration #20 has been
edited. `CREATE TRIGGER IF NOT EXISTS` in that same migration won't
re-apply the new body to existing prod DBs (they already have the old
body from the prior deploy), so the new body would silently fail to
ship. We caught this once already (2026-04-12) and patched it with a
DROP-on-every-boot preamble in `railway.toml` — a 10-15s deploy tax.

PR for issue #415 removes that preamble. To keep the safety net, this
test fails if migration #20's body SHA changes without:

  1. A new migration AFTER #20 (e.g. `(42, "json_validation_triggers_v2",
     "DROP TRIGGER IF EXISTS validate_*; CREATE TRIGGER ...")`) added to
     the MIGRATIONS list, AND
  2. This `EXPECTED_SHA` updated to the new body's hash.

Both have to happen together. If you skipped (1), prod won't pick up
your changes. If you skipped (2), this test fails CI.
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path


# Update both EXPECTED_SHA and add a new MIGRATIONS entry when changing
# a validate_* trigger body. See module docstring.
EXPECTED_SHA = "fe4d1df2bbb53141ace5499387608f3b611be8393db942352f34dc16c9d63581"


def _migration_20_body() -> str:
    """Pull the SQL block for `(20, "json_validation_triggers", ...)`."""
    text = Path("src/core/migrations.py").read_text(encoding="utf-8")
    m = re.search(
        r'\(20,\s*"json_validation_triggers",\s*"""(.*?)"""\s*\),',
        text, re.DOTALL,
    )
    assert m, "could not locate migration #20 in src/core/migrations.py"
    return m.group(1)


def _normalize(s: str) -> str:
    """Collapse all whitespace runs to a single space — formatter-stable."""
    return re.sub(r"\s+", " ", s).strip()


def test_validate_trigger_bodies_unchanged():
    """Pin SHA so trigger-body edits can't slip past `CREATE IF NOT EXISTS`
    on existing prod DBs without an explicit follow-up DROP migration."""
    body = _migration_20_body()
    sha = hashlib.sha256(_normalize(body).encode()).hexdigest()
    assert sha == EXPECTED_SHA, (
        "Migration #20 body changed.\n"
        f"  expected: {EXPECTED_SHA}\n"
        f"  got:      {sha}\n"
        "If you intentionally edited a validate_* trigger body, you ALSO "
        "need to add a new migration AFTER #20 that DROPs the old + "
        "CREATEs the new — `CREATE TRIGGER IF NOT EXISTS` in #20 will "
        "no-op on existing prod DBs and silently keep the old body. "
        "See `tests/test_validate_trigger_bodies.py` docstring."
    )


def test_migration_list_has_20():
    """Sanity: the regex above only works if #20 still exists."""
    body = _migration_20_body()
    # Must contain all 12 trigger names (6 insert + 6 update)
    for trig in [
        "validate_quotes_items_insert", "validate_quotes_items_update",
        "validate_pc_items_insert", "validate_pc_items_update",
        "validate_rfq_items_insert", "validate_rfq_items_update",
        "validate_order_items_insert", "validate_order_items_update",
        "validate_vo_items_insert", "validate_vo_items_update",
        "validate_wf_errors_insert", "validate_wf_errors_update",
    ]:
        assert trig in body, f"missing trigger: {trig}"
