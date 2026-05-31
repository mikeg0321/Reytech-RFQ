"""src.spine_bridge — translators between legacy quote shapes and the Spine.

NOT part of src/spine/. The Spine itself is forbidden by Charter rule
#11 to import legacy modules. This package is the bridge: it knows the
legacy dict shape (rfq_files rows, priced_carts items, etc.) and
produces a clean Spine Quote.

When CCHCS migration is complete (Day 30+ on the roadmap) and no
quotes flow through legacy anymore, this package can be retired.
"""

from src.spine_bridge.translator import (
    LegacyTranslationResult,
    TranslationIssue,
    translate_legacy_quote,
)
from src.spine_bridge.ingest import (
    IngestResult,
    NotCchcsError,
    TaxResolver,
    get_cchcs_required_forms,
    ingest_email_contract,
    synthesize_cchcs_email_contract,
)
from src.spine_bridge.oracle_proxy import (
    OracleLineSuggestion,
    OracleSource,
    suggestions_for_quote,
    suggestion_to_dict,
)

__all__ = [
    "LegacyTranslationResult",
    "TranslationIssue",
    "translate_legacy_quote",
    "IngestResult",
    "NotCchcsError",
    "TaxResolver",
    "get_cchcs_required_forms",
    "ingest_email_contract",
    "synthesize_cchcs_email_contract",
    "OracleLineSuggestion",
    "OracleSource",
    "suggestions_for_quote",
    "suggestion_to_dict",
]
