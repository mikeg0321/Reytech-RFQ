"""CR-4b P0 regression guards — expansion_outreach preserves metadata.

PR #384 (CR-4) switched api_expansion_outreach from raw outbox.json
rewrites to upsert_outbox_email() calls. The draft dict was constructed
with `facility` at the top level, but upsert_outbox_email only persists
fields in its INSERT column list (id, to_address, subject, body, intent,
entities, metadata, etc). `facility` and `agency_type` were silently
dropped — a data-preservation regression from the old path.

Fix: nest facility, agency_type, and pc_id into the `metadata` dict,
which IS persisted (as a JSON TEXT column).
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ROUTES_CRM = ROOT / "src" / "api" / "modules" / "routes_crm.py"
DAL = ROOT / "src" / "core" / "dal.py"


def _body_of(source_file: Path, fn_name: str) -> str:
    src = source_file.read_text(encoding="utf-8")
    start = src.find(f"def {fn_name}(")
    if start < 0:
        return ""
    rest = src[start:]
    m = re.search(r"\n(?:def |@bp\.route)", rest[len(f"def {fn_name}("):])
    end = len(rest) if not m else len(f"def {fn_name}(") + m.start()
    return rest[:end]


def test_upsert_outbox_email_does_not_persist_top_level_facility():
    """Guard: confirm upsert_outbox_email has no `facility` column in its
    INSERT list. If this test starts failing, the DAL grew a facility
    column and the fix for CR-4b might be relaxable. Until then, facility
    MUST be nested inside metadata to survive the round-trip."""
    body = _body_of(DAL, "upsert_outbox_email")
    assert body, "CR-4b: could not locate upsert_outbox_email in dal.py"
    insert_match = re.search(r"INSERT INTO email_outbox\s*\((.*?)\)", body, re.DOTALL)
    assert insert_match, "CR-4b: could not parse INSERT column list"
    cols = insert_match.group(1)
    assert "facility" not in cols.lower(), (
        "CR-4b premise broken: upsert_outbox_email now stores `facility` "
        "directly — the fix can move back to a top-level field. Until then, "
        "keep it in metadata."
    )
    assert "metadata" in cols, (
        "CR-4b relies on the `metadata` JSON column existing in the INSERT"
    )


def test_expansion_outreach_draft_nests_facility_in_metadata():
    body = _body_of(ROUTES_CRM, "api_expansion_outreach")
    assert body, "CR-4b: could not locate api_expansion_outreach"
    # Must have metadata dict with facility key
    assert '"facility": facility_name' in body, (
        "CR-4b: facility must be inside the metadata dict so "
        "upsert_outbox_email persists it"
    )
    # The metadata dict must be passed to the draft
    assert re.search(r'"metadata":\s*\{', body), (
        "CR-4b: draft dict must include a `metadata` key"
    )


def test_expansion_outreach_draft_preserves_agency_type_and_pc_id():
    body = _body_of(ROUTES_CRM, "api_expansion_outreach")
    assert '"agency_type": agency_type' in body, (
        "CR-4b: agency_type must ride along in metadata so downstream "
        "analytics can see what vertical this outreach targeted"
    )
    assert '"pc_id": results.get("pc_id"' in body, (
        "CR-4b: when action=email_and_pc creates a ghost PC, the pc_id "
        "must be preserved in metadata so the email is linkable to "
        "the is_test PC record"
    )


def test_expansion_outreach_drops_stale_top_level_facility():
    """Belt-and-suspenders: confirm we removed the now-useless top-level
    `"facility": facility_name` line that upsert_outbox_email ignored."""
    body = _body_of(ROUTES_CRM, "api_expansion_outreach")
    # The string must not appear outside the metadata dict. We scope by
    # the single `draft = {` occurrence — it must be the metadata one.
    draft_match = re.search(r"draft\s*=\s*\{(.*?)\n            \}", body, re.DOTALL)
    assert draft_match, "CR-4b: could not locate the draft dict"
    draft_body = draft_match.group(1)
    # Count "facility" occurrences — should be exactly 1 (the metadata one)
    facility_count = draft_body.count('"facility"')
    assert facility_count == 1, (
        f"CR-4b: expected exactly 1 `\"facility\"` key in the draft dict "
        f"(inside metadata), found {facility_count}. A top-level copy "
        f"would be silently dropped by upsert_outbox_email."
    )
