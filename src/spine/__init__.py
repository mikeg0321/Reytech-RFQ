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
    # Model
    "Quote",
    "LineItem",
    "QuoteStatus",
    "SpineValidationError",
    "SUPPORTED_UOM",
    # DB
    "init_db",
    "read_quote",
    "write_quote",
    "read_event_log",
    "iter_quote_ids",
    "write_snapshot",
    "read_snapshot",
    "iter_snapshots",
    "latest_snapshot",
    # Rendering
    "render_quote_pdf",
    "format_dollars",
    "format_tax_rate",
    "SpineRenderMismatchError",
]

from src.spine.model import (
    Quote,
    LineItem,
    QuoteStatus,
    SpineValidationError,
    SUPPORTED_UOM,
)
from src.spine.db import (
    init_db,
    read_quote,
    write_quote,
    read_event_log,
    iter_quote_ids,
    write_snapshot,
    read_snapshot,
    iter_snapshots,
    latest_snapshot,
)
from src.spine.quote_pdf import (
    render_quote_pdf,
    format_dollars,
    format_tax_rate,
    SpineRenderMismatchError,
)
