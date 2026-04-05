"""
config.py — Shared configuration for all route modules.

Extracted from dashboard.py to break the exec() injection dependency.
Import this instead of relying on dashboard globals.
"""

import os
import logging

log = logging.getLogger("reytech.config")

# ── Path constants ────────────────────────────────────────────────────────────
try:
    from src.core.paths import PROJECT_ROOT as BASE_DIR, DATA_DIR, UPLOAD_DIR, OUTPUT_DIR
except ImportError:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
    OUTPUT_DIR = os.path.join(BASE_DIR, "output")
    DATA_DIR = os.path.join(BASE_DIR, "data")
    for d in [UPLOAD_DIR, OUTPUT_DIR, DATA_DIR]:
        os.makedirs(d, exist_ok=True)

# ── App configuration (from reytech_config.json) ─────────────────────────────
try:
    from src.forms.reytech_filler_v4 import load_config
    CONFIG = load_config()
except Exception:
    CONFIG = {}

# Override config with env vars if present (for production)
if os.environ.get("GMAIL_PASSWORD"):
    CONFIG.setdefault("email", {})["email_password"] = os.environ["GMAIL_PASSWORD"]
if os.environ.get("GMAIL_ADDRESS"):
    CONFIG.setdefault("email", {})["email"] = os.environ["GMAIL_ADDRESS"]

# ── Feature flags (graceful import detection) ─────────────────────────────────
# Tier 1: Core features
PRICING_ORACLE_AVAILABLE = False
PRODUCT_RESEARCH_AVAILABLE = False
PRICE_CHECK_AVAILABLE = False
QUOTE_GEN_AVAILABLE = False
AUTO_PROCESSOR_AVAILABLE = False

try:
    from src.knowledge.pricing_oracle import recommend_prices_for_rfq, pricing_health_check
    from src.knowledge.won_quotes_db import (ingest_scprs_result, find_similar_items,
                                             get_kb_stats, get_price_history)
    PRICING_ORACLE_AVAILABLE = True
except ImportError:
    pass

try:
    from src.agents.product_research import (research_product, research_rfq_items,
                                             quick_lookup, test_amazon_search,
                                             get_research_cache_stats, RESEARCH_STATUS)
    PRODUCT_RESEARCH_AVAILABLE = True
except ImportError:
    pass

try:
    from src.forms.price_check import (parse_ams704, process_price_check, lookup_prices,
                                       test_parse, REYTECH_INFO, clean_description)
    PRICE_CHECK_AVAILABLE = True
except ImportError:
    pass

try:
    from src.forms.quote_generator import (generate_quote, generate_quote_from_pc,
                                           generate_quote_from_rfq, AGENCY_CONFIGS,
                                           get_all_quotes, search_quotes,
                                           peek_next_quote_number, update_quote_status,
                                           get_quote_stats, set_quote_counter,
                                           _detect_agency)
    QUOTE_GEN_AVAILABLE = True
except ImportError:
    pass

try:
    from src.auto.auto_processor import (auto_process_price_check, detect_document_type,
                                         score_quote_confidence, system_health_check,
                                         get_audit_stats, track_response_time)
    AUTO_PROCESSOR_AVAILABLE = True
except ImportError:
    pass

# Tier 2: Intelligence & agents (set during module loading)
INTEL_AVAILABLE = False
PREDICT_AVAILABLE = False
QB_AVAILABLE = False
GROWTH_AVAILABLE = False
OUTREACH_AVAILABLE = False
VOICE_AVAILABLE = False
CAMPAIGNS_AVAILABLE = False
SCANNER_AVAILABLE = False
LEADGEN_AVAILABLE = False
ITEM_ID_AVAILABLE = False
REPLY_ANALYZER_AVAILABLE = False
QA_AVAILABLE = False
MANAGER_AVAILABLE = False
ORCHESTRATOR_AVAILABLE = False
CATALOG_AVAILABLE = False
_WF_AVAILABLE = False
