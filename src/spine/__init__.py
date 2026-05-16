"""The Spine — canonical quote substrate.

See src/spine/SPINE_CHARTER.md for the architectural mandate.

Hard rules enforced by tests/spine/test_spine_architecture.py:
- No legacy imports inside src/spine/ except whitelisted leaf utils.
- No alias fields (bid_price, price_per_unit, our_price, etc.) in the
  Spine model.
- Exactly one writer for the spine_quotes table.
- extra='forbid' on every Spine pydantic model.
"""

__all__ = [
    "Quote",
    "LineItem",
    "QuoteStatus",
    "SpineValidationError",
]

from src.spine.model import (
    Quote,
    LineItem,
    QuoteStatus,
    SpineValidationError,
)
