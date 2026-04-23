"""Quote Model Adapter — wraps legacy dict access through the Quote model.

Feature-flagged via `quote_model_v2_enabled`. When enabled, route handlers
transparently convert dicts to Quotes and back. When disabled, passthrough.

Usage in route handlers:
    from src.core.quote_adapter import adapt_pc, adapt_rfq

    # In PC detail route:
    pc = pcs.get(pcid)
    pc = adapt_pc(pc, pcid)  # Returns dict (possibly round-tripped through Quote)

    # In RFQ detail route:
    r = rfqs.get(rid)
    r = adapt_rfq(r, rid)
"""
import copy
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from src.core.paths import DATA_DIR

log = logging.getLogger(__name__)

_PST = timezone(timedelta(hours=-8))
_DIFF_LOG = os.path.join(DATA_DIR, "quote_adapter_diffs.jsonl")


def _is_enabled() -> bool:
    """Check if Quote model V2 adapter is enabled."""
    try:
        from src.core.flags import get_flag
        return bool(get_flag("quote_model_v2_enabled", False))
    except Exception:
        return False


# Fields that the Quote pydantic model does not know about but that routes,
# templates, and downstream features depend on. `to_legacy_dict()` builds a
# fresh dict from model fields, so anything not in the model silently vanishes
# on round-trip. These must be copied back from the original dict after
# adaptation. Add new entries here when adding non-model fields — NEVER rely
# on the adapter preserving anything unless it's listed here or in the model.
#
# Incident 2026-04-22: RFQ #9ad8a0ac got linked_pc_id via manual write; the
# detail route called adapt_rfq() and the template rendered "no PC linked"
# because linked_pc_id had been stripped by the adapter round-trip.
_ADAPTER_PASSTHROUGH_FIELDS = (
    # PC link bookkeeping (RFQ→PC triangulated linker + manual overrides)
    "linked_pc_id",
    "linked_pc_number",
    "linked_pc_ids",          # multi-PC bundle case
    "link_reason",
    "link_confidence",
    # Deadline provenance (PR #429/#430/#432)
    "due_date_source",
    # Parse-failure + recovery signals
    "_parse_failed",
    "_recovered_from",
    # Manual RFQ flag used by several status hygiene checks
    "is_manual",
    "is_test",
)


def _preserve_passthrough(original: dict, adapted: dict) -> None:
    """Copy passthrough fields from original onto adapted when adapter dropped them."""
    for k in _ADAPTER_PASSTHROUGH_FIELDS:
        if k in original and not adapted.get(k):
            adapted[k] = original[k]


def adapt_pc(pc_dict: dict, pcid: str = "") -> dict:
    """Adapt a PC dict through the Quote model if flag is enabled.

    Always returns a dict (for backward compatibility with templates).
    When enabled: dict → Quote → dict (validates, normalizes, computes fields),
    then passthrough fields (see _ADAPTER_PASSTHROUGH_FIELDS) are restored
    from the original so the round-trip is lossless for known non-model keys.
    When disabled: returns a deepcopy of the original dict.
    """
    if not pc_dict:
        return pc_dict

    if not _is_enabled():
        return copy.deepcopy(pc_dict)

    try:
        from src.core.quote_model import Quote
        original = copy.deepcopy(pc_dict)
        quote = Quote.from_legacy_dict(original, doc_type="pc")
        adapted = quote.to_legacy_dict()
        _preserve_passthrough(original, adapted)

        # Log any differences for monitoring
        _log_diffs(pcid, "pc", original, adapted)

        return adapted
    except Exception as e:
        log.warning("Quote adapter failed for PC %s, falling back to raw dict: %s", pcid, e)
        return copy.deepcopy(pc_dict)


def adapt_rfq(rfq_dict: dict, rid: str = "") -> dict:
    """Adapt an RFQ dict through the Quote model if flag is enabled.

    Passthrough fields (linked_pc_id, due_date_source, etc.) that the Quote
    model doesn't know about are restored post-adaptation so the detail
    route can render them. See _ADAPTER_PASSTHROUGH_FIELDS.
    """
    if not rfq_dict:
        return rfq_dict

    if not _is_enabled():
        return copy.deepcopy(rfq_dict)

    try:
        from src.core.quote_model import Quote
        original = copy.deepcopy(rfq_dict)
        quote = Quote.from_legacy_dict(original, doc_type="rfq")
        adapted = quote.to_legacy_dict()
        _preserve_passthrough(original, adapted)

        _log_diffs(rid, "rfq", original, adapted)

        return adapted
    except Exception as e:
        log.warning("Quote adapter failed for RFQ %s, falling back to raw dict: %s", rid, e)
        return copy.deepcopy(rfq_dict)


def _log_diffs(doc_id: str, doc_type: str, original: dict, adapted: dict):
    """Log field-level differences between original and adapted dicts."""
    diffs = []
    # Compare top-level string/number fields
    check_keys = [
        "id", "institution", "agency_name", "solicitation_number",
        "rfq_number", "pc_number", "status", "due_date", "ship_to",
        "delivery_location", "requestor_name", "requestor",
    ]
    for key in check_keys:
        orig_val = str(original.get(key, "")).strip()
        adapt_val = str(adapted.get(key, "")).strip()
        if orig_val != adapt_val and (orig_val or adapt_val):
            diffs.append({"field": key, "original": orig_val[:100], "adapted": adapt_val[:100]})

    # Compare item count
    orig_items = original.get("line_items") or original.get("items") or []
    adapt_items = adapted.get("line_items") or adapted.get("items") or []
    if len(orig_items) != len(adapt_items):
        diffs.append({"field": "item_count", "original": str(len(orig_items)), "adapted": str(len(adapt_items))})

    if diffs:
        entry = {
            "timestamp": datetime.now(_PST).isoformat(),
            "doc_id": doc_id,
            "doc_type": doc_type,
            "diff_count": len(diffs),
            "diffs": diffs[:20],
        }
        try:
            os.makedirs(os.path.dirname(_DIFF_LOG), exist_ok=True)
            with open(_DIFF_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            log.debug("Failed to write adapter diff: %s", e)

        log.info("QUOTE_ADAPTER %s %s: %d field diffs", doc_type, doc_id[:8], len(diffs))
