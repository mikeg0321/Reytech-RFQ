"""CR-3 + CR-4 regression guards for api_expansion_outreach.

CR-3: the fabricated "expansion" PC must carry is_test=True so it is
filtered from deadline alerts, funnels, analytics, etc. The PC is a
CRM lead placeholder, not a real price check.

CR-4: the email-draft write must go through the DAL (upsert_outbox_email).
The old code raw-dumped the whole outbox.json, race-stomping concurrent
writers and bypassing DB persistence.
"""
import re
from pathlib import Path

CRM = Path(__file__).resolve().parents[1] / "src" / "api" / "modules" / "routes_crm.py"


def _expansion_body() -> str:
    """Slice of routes_crm.py covering api_expansion_outreach."""
    src = CRM.read_text(encoding="utf-8")
    start = src.find("def api_expansion_outreach(")
    assert start >= 0, "api_expansion_outreach not found"
    rest = src[start:]
    # Next top-level def starts at col 0
    next_def = re.search(r"\n(?:def |@bp\.route)", rest[len("def api_expansion_outreach("):])
    end = len(rest) if not next_def else len("def api_expansion_outreach(") + next_def.start()
    return rest[:end]


def test_module_compiles():
    import py_compile
    py_compile.compile(str(CRM), doraise=True)


def test_expansion_pc_marked_is_test():
    """CR-3: fabricated PC must carry is_test=True so downstream filters
    (deadlines, funnels, analytics) skip it."""
    body = _expansion_body()
    assert '"is_test": True' in body, \
        "fabricated expansion PC must set \"is_test\": True (CR-3)"


def test_email_draft_uses_dal():
    """CR-4: must call upsert_outbox_email, not raw dump outbox.json."""
    body = _expansion_body()
    assert "upsert_outbox_email" in body, \
        "email draft must go through DAL upsert_outbox_email (CR-4)"


def test_no_raw_outbox_json_write():
    """CR-4: the email branch must not rewrite the whole outbox.json."""
    body = _expansion_body()
    # The old pattern was: with open(outbox_path_local, "w") as f: _json.dump(outbox, ...)
    assert not re.search(r'open\([^)]*outbox[^)]*"w"', body), \
        "must not rewrite outbox.json raw (CR-4)"
    assert "outbox_path_local" not in body, \
        "outbox_path_local variable is a leftover of the raw-write path (CR-4)"


def test_no_get_outbox_then_append_pattern():
    """CR-4: the old get_outbox() → .append → dump pattern is the bug.
    Must use upsert instead."""
    body = _expansion_body()
    # get_outbox → outbox.append → json.dump is the smell
    # We allow the DAL upsert now; ensure outbox.append is gone.
    assert "outbox.append" not in body, \
        "outbox.append is the pre-CR-4 raw-write pattern"
