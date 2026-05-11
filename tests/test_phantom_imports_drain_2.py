"""Pin the 7 phantom-import drains from batch 2.

Drain rule per PR #862: each fixed phantom site removes its entry from
BASELINE_EXEMPTIONS, and a regression test pins the new state so a
revert can't sneak through.

Batch 2 (this PR) drains:

  email_poller order-win flow (4 sites)
  ────────────────────────────────────
  * update_quote_status: from src.core.db_dal → src.forms.quote_generator
    (also fixes call: adds po_number + actor="email_poller" so the
    won-requires-PO gate doesn't block)
  * log_revenue: from src.core.db_dal → src.core.db
    (also fixes call: adds required `description` parameter)
  * record_price: from src.core.db_dal → src.core.db
  * log_activity: from src.core.db_dal → src.api.data_layer._log_crm_activity
    (different function entirely — broken call used ref_type/ref_id/detail
    which never matched any real signature)

  routes_rfq tp-fp scan (2 sites)
  ───────────────────────────────
  * load_pcs → _load_price_checks as load_pcs (same /health typo class)
  * load_pcs as _load_pcs → _load_price_checks as _load_pcs

  routes_rfq_admin upload-edited-quote audit (1 site)
  ───────────────────────────────────────────────────
  * audit_log.log_event → src.core.security._log_audit_internal
    (also fixes call: actor/target_id folded into metadata)
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return Path(rel).read_text(encoding="utf-8")


# ─── Group A: email_poller (4 drains) ──────────────────────────────────

class TestEmailPollerNoPhantomDbDal:
    """All 4 src.core.db_dal phantom imports must be gone."""

    def test_no_db_dal_anywhere(self):
        src = _read("src/agents/email_poller.py")
        assert "from src.core.db_dal import" not in src, \
            "email_poller regressed: still imports phantom src.core.db_dal"


class TestEmailPollerUpdateQuoteStatus:
    def test_uses_real_module(self):
        src = _read("src/agents/email_poller.py")
        assert "from src.forms.quote_generator import update_quote_status" in src, \
            "update_quote_status must import from quote_generator"

    def test_call_passes_actor_to_bypass_won_gate(self):
        """update_quote_status('won') blocks actor=user without po_number.
        email_poller MUST pass actor != 'user' or the won-mark silently fails."""
        src = _read("src/agents/email_poller.py")
        assert 'actor="email_poller"' in src, \
            "update_quote_status call must pass actor='email_poller' to bypass won-requires-PO gate"


class TestEmailPollerLogRevenue:
    def test_uses_real_module(self):
        src = _read("src/agents/email_poller.py")
        assert "from src.core.db import log_revenue" in src

    def test_call_passes_description(self):
        """log_revenue requires `description` (no default). The pre-drain
        call omitted it — would have crashed even with the right module."""
        src = _read("src/agents/email_poller.py")
        # Find the log_revenue() call body
        idx = src.find("log_revenue(")
        assert idx >= 0
        # Window after the call open paren — must contain `description=`
        window = src[idx:idx + 600]
        assert "description=" in window, \
            "log_revenue call missing required `description=` kwarg"


class TestEmailPollerRecordPrice:
    def test_uses_real_module(self):
        src = _read("src/agents/email_poller.py")
        assert "from src.core.db import record_price" in src


class TestEmailPollerLogActivity:
    def test_uses_crm_activity_writer(self):
        """The semantics in the original broken call (event_type/ref_id/detail)
        match _log_crm_activity, not src.core.db.log_activity (which takes
        contact_id/subject/body)."""
        src = _read("src/agents/email_poller.py")
        assert "from src.api.data_layer import _log_crm_activity" in src

    def test_call_uses_description_not_detail(self):
        src = _read("src/agents/email_poller.py")
        idx = src.find("_log_crm_activity(")
        assert idx >= 0
        window = src[idx:idx + 800]
        assert "description=" in window
        assert "detail=" not in window or "_log_crm_activity" not in window, \
            "_log_crm_activity call must use description=, not detail="


# ─── Group B: routes_rfq tp-fp scan (2 drains) ─────────────────────────

class TestRoutesRfqLoadPcs:
    def test_no_phantom_load_pcs(self):
        src = _read("src/api/modules/routes_rfq.py")
        # Must not import phantom `load_pcs` directly
        assert "from src.api.data_layer import load_pcs\n" not in src, \
            "routes_rfq.py regressed: imports phantom load_pcs"
        # The fix is an alias: `from src.api.data_layer import _load_price_checks as load_pcs`
        assert "_load_price_checks as load_pcs" in src, \
            "routes_rfq.py must alias _load_price_checks"

    def test_second_site_aliased_too(self):
        src = _read("src/api/modules/routes_rfq.py")
        assert "_load_price_checks as _load_pcs" in src, \
            "routes_rfq.py second site must alias _load_price_checks as _load_pcs"


# ─── Group C: routes_rfq_admin upload-edited-quote (1 drain) ───────────

class TestRoutesRfqAdminAuditLog:
    def test_no_phantom_audit_log_module(self):
        src = _read("src/api/modules/routes_rfq_admin.py")
        assert "from src.core.audit_log" not in src, \
            "routes_rfq_admin regressed: imports phantom src.core.audit_log"

    def test_uses_security_audit_internal(self):
        src = _read("src/api/modules/routes_rfq_admin.py")
        assert "from src.core.security import _log_audit_internal" in src

    def test_call_uses_action_and_metadata(self):
        src = _read("src/api/modules/routes_rfq_admin.py")
        idx = src.find("_log_audit_internal(")
        assert idx >= 0
        window = src[idx:idx + 800]
        assert 'action="quote_pdf_edited_externally"' in window
        # actor + target_id folded into metadata dict, not top-level kwargs
        assert "metadata=" in window


# ─── Live-import verification (real names exist + are callable) ────────

class TestRealNamesResolve:
    def test_update_quote_status_resolves(self):
        from src.forms.quote_generator import update_quote_status
        assert callable(update_quote_status)

    def test_log_revenue_resolves(self):
        from src.core.db import log_revenue
        assert callable(log_revenue)

    def test_record_price_resolves(self):
        from src.core.db import record_price
        assert callable(record_price)

    def test_log_crm_activity_resolves(self):
        from src.api.data_layer import _log_crm_activity
        assert callable(_log_crm_activity)

    def test_load_price_checks_resolves(self):
        from src.api.data_layer import _load_price_checks
        assert callable(_load_price_checks)

    def test_log_audit_internal_resolves(self):
        from src.core.security import _log_audit_internal
        assert callable(_log_audit_internal)
