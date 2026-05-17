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
    # Email contract — the master ingestion record
    "EmailContract",
    "ContractLineItem",
    "ContractDelta",
    "contract_vs_quote",
    "FormCode",
    "ALL_FORM_CODES",
    "CCHCS_DEFAULT_REQUIRED_FORMS",
    # Ingest rejections — every email considered emits a row
    "IngestRejection",
    "RejectionReason",
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
    "write_email_contract",
    "read_email_contract",
    "find_contract_for_quote",
    "write_ingest_rejection",
    "latest_rejections",
    # Sequential counters — R26PCXXXX / R26R#### / R26Q#### substrate
    "next_value",
    "get_counter",
    "set_counter",
    "COUNTER_MAX_JUMP",
    # Quote ↔ Quote links — PC predecessor ← RFQ rebid (Mike 5/17)
    "write_quote_link",
    "find_links_from",
    "find_links_to",
    "AUTO_LINK_OPERATOR_CONFIDENCE",
    # Quote matcher — scoring + candidate selection
    "score_quote_pair",
    "find_pc_candidates",
    "AUTO_LINK_THRESHOLD",
    # Auto-pricer — carry validated costs from linked PC to new RFQ
    "carry_forward_costs",
    "parse_carry_note",
    "COST_CARRY_PREFIX",
    # Catalog substrate — buyer-supplied product data over time
    "catalog_observe",
    "catalog_get_entry",
    "catalog_iter_entries",
    "catalog_find_stale_priced",
    "catalog_record_enrichment",
    "CATALOG_STALENESS_DAYS",
    "ENRICHMENT_PENDING",
    "ENRICHMENT_FETCHED",
    "ENRICHMENT_FAILED",
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
from src.spine.email_contract import (
    EmailContract,
    ContractLineItem,
    FormCode,
    ALL_FORM_CODES,
    CCHCS_DEFAULT_REQUIRED_FORMS,
)
from src.spine.contract_diff import (
    ContractDelta,
    contract_vs_quote,
)
from src.spine.ingest_rejection import (
    IngestRejection,
    RejectionReason,
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
    write_email_contract,
    read_email_contract,
    find_contract_for_quote,
    write_ingest_rejection,
    latest_rejections,
    next_value,
    get_counter,
    set_counter,
    COUNTER_MAX_JUMP,
    write_quote_link,
    find_links_from,
    find_links_to,
    AUTO_LINK_OPERATOR_CONFIDENCE,
)
from src.spine.quote_matcher import (
    score_quote_pair,
    find_pc_candidates,
    AUTO_LINK_THRESHOLD,
)
from src.spine.auto_pricer import (
    carry_forward_costs,
    parse_carry_note,
    COST_CARRY_PREFIX,
)
from src.spine.catalog import (
    observe as catalog_observe,
    get_entry as catalog_get_entry,
    iter_entries as catalog_iter_entries,
    find_stale_priced_entries as catalog_find_stale_priced,
    record_enrichment as catalog_record_enrichment,
    CATALOG_STALENESS_DAYS,
    ENRICHMENT_PENDING,
    ENRICHMENT_FETCHED,
    ENRICHMENT_FAILED,
)
from src.spine.quote_pdf import (
    render_quote_pdf,
    format_dollars,
    format_tax_rate,
    SpineRenderMismatchError,
)
