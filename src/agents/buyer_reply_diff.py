"""Claude-driven diff extraction for buyer-reply emails (PR-F1).

PR-E (#813) routes buyer-reply emails to existing primary records and
stores them as `buyer_replies` entries on the record. The natural next
question is *what did the buyer actually ask for*: a price change? a
quantity change? a new item? a deadline shift? Hand-reading every
reply doesn't scale.

This module is the pure-function helper that takes:
  * the reply email body text, and
  * the record's current item list (description / qty / unit_price),

and asks Claude (via tool-use, structured output) to extract a
quote-diff: per-line price changes, qty changes, items added/removed,
plus freeform notes (deadline asks, shipping questions, etc.).

The wrapper returns the same `(diff, skipped_reason)`-shaped contract
as `compliance_validator._run_llm_gap_check` — None skipped_reason
means the LLM ran; a non-None reason carries a short string so the
operator UI shows that the diff was not exercised.

This PR ships the helper only:
  * `extract_quote_diff(reply_text, current_items, current_totals=None)`
  * Anthropic SDK call split into `_invoke_llm_diff` so tests can
    patch the boundary while the surrounding shape checks run.

Wiring (read `record.buyer_replies[i]` → call helper → store the diff
on the record + render in a pending-changes panel) lands in PR-F2 once
PR-E is on main.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
_REPLY_TEXT_MAX = 8000
_ITEM_DESC_MAX = 240


def _normalize_items(items: Optional[List[Dict]]) -> List[Dict]:
    """Strip the items list down to fields the LLM needs. Keeps the
    prompt tight and avoids leaking unrelated catalog fields."""
    out = []
    for idx, it in enumerate(items or []):
        if not isinstance(it, dict):
            continue
        desc = (it.get("description") or "")[:_ITEM_DESC_MAX]
        try:
            qty = float(it.get("qty") or it.get("quantity") or 0)
        except (TypeError, ValueError):
            qty = 0.0
        try:
            unit_price = float(
                it.get("unit_price")
                or it.get("price_per_unit")
                or it.get("bid_price")
                or 0
            )
        except (TypeError, ValueError):
            unit_price = 0.0
        line_no = it.get("line_number") or it.get("line_no") or (idx + 1)
        out.append({
            "line_no": line_no,
            "description": desc,
            "qty": qty,
            "unit_price": unit_price,
        })
    return out


def _build_tool_schema() -> Dict:
    """JSONSchema for the structured-output tool. Each diff bucket is
    a fixed shape so the renderer can iterate predictably."""
    return {
        "name": "record_quote_diff",
        "description": (
            "Record what the buyer's reply email is asking for "
            "compared to the current quote line items."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "price_changes": {
                    "type": "array",
                    "description": (
                        "Items where the buyer is asking for a "
                        "different unit price."
                    ),
                    "items": {
                        "type": "object",
                        "properties": {
                            "line_no":  {"type": "integer"},
                            "description": {"type": "string"},
                            "current_unit_price": {"type": "number"},
                            "requested_unit_price": {"type": "number"},
                            "rationale": {"type": "string"},
                        },
                        "required": ["line_no", "requested_unit_price",
                                     "rationale"],
                    },
                },
                "qty_changes": {
                    "type": "array",
                    "description": (
                        "Items where the buyer is asking for a "
                        "different quantity."
                    ),
                    "items": {
                        "type": "object",
                        "properties": {
                            "line_no":  {"type": "integer"},
                            "description": {"type": "string"},
                            "current_qty": {"type": "number"},
                            "requested_qty": {"type": "number"},
                            "rationale": {"type": "string"},
                        },
                        "required": ["line_no", "requested_qty",
                                     "rationale"],
                    },
                },
                "items_added": {
                    "type": "array",
                    "description": (
                        "Items the buyer is asking to add to the "
                        "quote that are not present in the current "
                        "item list."
                    ),
                    "items": {
                        "type": "object",
                        "properties": {
                            "description": {"type": "string"},
                            "qty":   {"type": "number"},
                            "unit_price_hint": {"type": "number"},
                            "rationale": {"type": "string"},
                        },
                        "required": ["description", "qty", "rationale"],
                    },
                },
                "items_removed": {
                    "type": "array",
                    "description": (
                        "Items the buyer is asking to remove from "
                        "the quote."
                    ),
                    "items": {
                        "type": "object",
                        "properties": {
                            "line_no":  {"type": "integer"},
                            "description": {"type": "string"},
                            "rationale": {"type": "string"},
                        },
                        "required": ["line_no", "rationale"],
                    },
                },
                "notes": {
                    "type": "array",
                    "description": (
                        "Freeform asks the buyer makes that don't "
                        "fit the structured buckets above (deadline "
                        "shifts, shipping questions, addressing "
                        "changes, vendor clarifications)."
                    ),
                    "items": {
                        "type": "object",
                        "properties": {
                            "category": {
                                "type": "string",
                                "enum": [
                                    "deadline", "shipping", "tax",
                                    "address", "approval",
                                    "specification", "other",
                                ],
                            },
                            "note": {"type": "string"},
                        },
                        "required": ["category", "note"],
                    },
                },
            },
            "required": [
                "price_changes", "qty_changes",
                "items_added", "items_removed", "notes",
            ],
        },
    }


def _build_system_prompt() -> str:
    return (
        "You are a procurement-RFQ analyst. The user will give you "
        "(a) a buyer's reply email body and (b) the current line "
        "items on the open quote. Your job is to extract ONLY what "
        "the buyer is actually asking to change.\n\n"
        "Rules:\n"
        " * Match items by description against the current item list. "
        "If the buyer references 'item 3' or 'line 3', use that "
        "line_no.\n"
        " * If the buyer is just thanking, asking a non-binding "
        "question, or restating the existing quote, every diff bucket "
        "should be empty.\n"
        " * Do NOT invent items. If the buyer mentions a SKU you "
        "can't tie to a current line and isn't an obviously new "
        "request, leave it out and surface it as a `notes` entry "
        "with category=specification.\n"
        " * Numbers must be the buyer's requested values, not deltas "
        "(e.g., 'reduce qty by 2 from 10 to 8' → requested_qty=8, "
        "not -2).\n"
        " * Prices in dollars (15.50, not '$15.50').\n"
        " * Keep `rationale` to one short sentence quoting the "
        "buyer's words."
    )


def _invoke_llm_diff(*,
                    api_key: str,
                    reply_text: str,
                    current_items: List[Dict],
                    current_totals: Optional[Dict] = None,
                    ) -> Dict:
    """Make the actual Anthropic call. Split out so tests can patch
    just this boundary while letting the surrounding setup checks
    (api_key, normalize) run normally.

    Returns the parsed tool-input dict (what record_quote_diff said).
    """
    import anthropic

    user_payload = {
        "buyer_reply_body": (reply_text or "")[:_REPLY_TEXT_MAX],
        "current_items": _normalize_items(current_items),
    }
    if current_totals:
        user_payload["current_totals"] = {
            "subtotal": current_totals.get("subtotal"),
            "tax": current_totals.get("tax"),
            "total": current_totals.get("total"),
        }
    user = json.dumps(user_payload, indent=2)

    tool = _build_tool_schema()
    system = _build_system_prompt()

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=2048,
        system=system,
        tools=[tool],
        tool_choice={"type": "tool", "name": "record_quote_diff"},
        messages=[{"role": "user", "content": user}],
    )

    diff: Dict = {
        "price_changes": [],
        "qty_changes": [],
        "items_added": [],
        "items_removed": [],
        "notes": [],
    }
    for block in resp.content:
        if getattr(block, "type", None) == "tool_use" \
                and block.name == "record_quote_diff":
            payload = block.input or {}
            for k in diff.keys():
                v = payload.get(k, [])
                if isinstance(v, list):
                    diff[k] = v
            break
    diff["_response_id"] = getattr(resp, "id", "")
    return diff


def _is_diff_empty(diff: Dict) -> bool:
    return not any(
        bool(diff.get(k))
        for k in ("price_changes", "qty_changes",
                  "items_added", "items_removed", "notes")
    )


def extract_quote_diff(
        reply_text: str,
        current_items: List[Dict],
        *,
        current_totals: Optional[Dict] = None,
        ) -> Tuple[Dict, Optional[str]]:
    """Public entry. Returns `(diff, skipped_reason)`.

    `skipped_reason` is None when Claude actually ran. Non-None values
    carry a short human-readable string the operator UI surfaces:
      * "no reply text"
      * "no current items"
      * "ANTHROPIC_API_KEY not set"
      * "anthropic SDK not installed"
      * "LLM call failed: <type>: <msg>"
    On a successful run the helper guarantees the five bucket keys
    are present (possibly empty lists) so callers never need defaults.
    """
    if not (reply_text or "").strip():
        return _empty_diff(), "no reply text"
    if not current_items:
        return _empty_diff(), "no current items"

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _empty_diff(), "ANTHROPIC_API_KEY not set"

    try:
        raw = _invoke_llm_diff(
            api_key=api_key,
            reply_text=reply_text,
            current_items=current_items,
            current_totals=current_totals,
        )
    except ImportError:
        return _empty_diff(), "anthropic SDK not installed"
    except Exception as e:
        log.debug("buyer-reply diff extraction failed: %s", e)
        return _empty_diff(), f"LLM call failed: {type(e).__name__}: {e}"

    diff = _empty_diff()
    for k in ("price_changes", "qty_changes",
              "items_added", "items_removed", "notes"):
        v = raw.get(k, [])
        if isinstance(v, list):
            # Trim list size so a runaway response can't blow up
            # the operator UI. 50 entries per bucket is a generous
            # ceiling for any single buyer reply.
            diff[k] = v[:50]
    diff["_response_id"] = raw.get("_response_id", "")
    diff["_empty"] = _is_diff_empty(diff)
    return diff, None


def _empty_diff() -> Dict:
    return {
        "price_changes": [],
        "qty_changes": [],
        "items_added": [],
        "items_removed": [],
        "notes": [],
        "_empty": True,
    }
