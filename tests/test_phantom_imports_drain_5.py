"""Pin batch 5 drains: queue_background_lookup wrapper, processed-uid
fallback inline, tax_agent CRM source.

Three fixes touching 5 sites:
  * src/agents/scprs_lookup.py — NEW queue_background_lookup() thread
    wrapper (60s dedup); satisfies both callers without changing their
    call shape.
  * src/api/modules/routes_rfq_admin.py — _remove_processed_uid was a
    phantom; the JSON fallback below it was the real impl. Removed the
    try/except wrapper, kept the fallback as the primary path.
  * src/agents/tax_agent.py — load_contacts was phantom; rewired to
    _load_crm_contacts (returns dict) via .values().
"""
from __future__ import annotations

import os
import sys
import time as _time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return Path(rel).read_text(encoding="utf-8")


# ─── Fix 1: queue_background_lookup wrapper ───────────────────────────

class TestQueueBackgroundLookupExists:
    def test_function_exists_with_correct_signature(self):
        from src.agents.scprs_lookup import queue_background_lookup
        assert callable(queue_background_lookup)
        # Signature must accept (description, source=...) — both call sites use it
        import inspect
        sig = inspect.signature(queue_background_lookup)
        assert "description" in sig.parameters
        assert "source" in sig.parameters

    def test_returns_bool(self):
        """Empty description must short-circuit with False."""
        from src.agents.scprs_lookup import queue_background_lookup
        assert queue_background_lookup("") is False
        assert queue_background_lookup("   ") is False

    def test_dedup_blocks_repeat_within_window(self):
        """Same (description, source) within 60s must return False the
        second time. The first call spawns a daemon thread; this test
        does not wait for or join that thread."""
        from src.agents.scprs_lookup import queue_background_lookup
        unique = f"TEST-DEDUP-{int(_time.time())}"
        first = queue_background_lookup(unique, source="pytest")
        second = queue_background_lookup(unique, source="pytest")
        assert first is True
        assert second is False

    def test_gc_prunes_stale_entries_under_pressure(self):
        """When the recent-queue dict grows past the watermark, stale
        entries must be removed (the broken prior impl only cleared
        when zero entries were fresh, so under sustained activity the
        dict grew unbounded)."""
        from src.agents import scprs_lookup as sl
        with sl._BG_QUEUE_LOCK:
            sl._BG_QUEUE_RECENT.clear()
            now = _time.time()
            # 250 stale + 5 fresh = 255 entries, all over the watermark
            for i in range(250):
                sl._BG_QUEUE_RECENT[f"stale-{i}"] = now - sl._BG_QUEUE_DEDUP_SECONDS - 10
            for i in range(5):
                sl._BG_QUEUE_RECENT[f"fresh-{i}"] = now

        # Trigger GC by adding a new entry — only fresh keys should remain
        sl.queue_background_lookup("trigger-gc-pass", source="pytest")
        with sl._BG_QUEUE_LOCK:
            remaining = list(sl._BG_QUEUE_RECENT.keys())
        assert all(not k.startswith("stale-") for k in remaining), \
            f"GC failed to prune stale entries; remaining: {remaining[:10]}"
        # All 5 fresh entries should still be there
        fresh_remaining = [k for k in remaining if k.startswith("fresh-")]
        assert len(fresh_remaining) == 5


class TestDismissedPcUsesRealQueue:
    def test_routes_pricecheck_pricing_imports_real_name(self):
        src = _read("src/api/modules/routes_pricecheck_pricing.py")
        # Phantom is fixed by adding the function, but the call line stays
        assert "from src.agents.scprs_lookup import queue_background_lookup" in src
        assert "log.warning" in src  # not silent


class TestDismissedRfqUsesRealQueue:
    def test_routes_rfq_gen_imports_real_name(self):
        src = _read("src/api/modules/routes_rfq_gen.py")
        assert "from src.agents.scprs_lookup import queue_background_lookup" in src


# ─── Fix 2: _remove_processed_uid phantom removed ─────────────────────

class TestRemoveProcessedUidPhantomGone:
    def test_no_phantom_import(self):
        src = _read("src/api/modules/routes_rfq_admin.py")
        assert "from src.api.modules.routes_pricecheck import _remove_processed_uid" not in src, \
            "routes_rfq_admin regressed: still imports phantom _remove_processed_uid"

    def test_inline_implementation_kept(self):
        """The JSON-file processed_emails cleanup must still exist —
        the inline impl is now the only code path."""
        src = _read("src/api/modules/routes_rfq_admin.py")
        assert "processed_emails.json" in src
        # The list-or-dict branch handling is the real implementation
        assert "isinstance(processed, list)" in src
        assert "isinstance(processed, dict)" in src


# ─── Fix 3: tax_agent rewired to _load_crm_contacts ───────────────────

class TestTaxAgentUsesCrmContactsLoader:
    def test_no_phantom_load_contacts_import(self):
        src = _read("src/agents/tax_agent.py")
        assert "from src.forms.quote_generator import load_contacts" not in src, \
            "tax_agent regressed: still imports phantom load_contacts"

    def test_uses_load_crm_contacts(self):
        src = _read("src/agents/tax_agent.py")
        assert "from src.api.modules.routes_intel_ops import _load_crm_contacts" in src
        # Must iterate .values() since _load_crm_contacts returns a dict
        assert "_load_crm_contacts().values()" in src


# ─── Live resolution checks ──────────────────────────────────────────

class TestRealNamesResolve:
    def test_queue_background_lookup_resolves(self):
        from src.agents.scprs_lookup import queue_background_lookup
        assert callable(queue_background_lookup)

    def test_load_crm_contacts_resolves(self):
        from src.api.modules.routes_intel_ops import _load_crm_contacts
        assert callable(_load_crm_contacts)
