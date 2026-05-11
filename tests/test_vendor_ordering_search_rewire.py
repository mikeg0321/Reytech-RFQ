"""Pin vendor_ordering_agent SCPRS-history lookup uses find_similar_items.

Phantom drain: `search_pricing` never existed in won_quotes_db. The real
function is `find_similar_items` which returns
  [{quote: {...row from won_quotes...}, match_confidence, ...}, ...].
The won_quotes schema uses `supplier` (not `vendor`); caller adapts.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _read(rel: str) -> str:
    return Path(rel).read_text(encoding="utf-8")


class TestVendorOrderingPhantomDrained:
    def test_no_phantom_search_pricing(self):
        src = _read("src/agents/vendor_ordering_agent.py")
        assert "from src.knowledge.won_quotes_db import search_pricing" not in src, \
            "vendor_ordering regressed: still imports phantom search_pricing"

    def test_uses_find_similar_items(self):
        src = _read("src/agents/vendor_ordering_agent.py")
        assert "from src.knowledge.won_quotes_db import find_similar_items" in src

    def test_call_reads_quote_subfield(self):
        """The wrapped result shape requires quote = matches[0]['quote'].
        Reading top-level fields (the broken old shape) would silently
        return empty strings."""
        src = _read("src/agents/vendor_ordering_agent.py")
        assert 'matches[0].get("quote", {})' in src

    def test_reads_supplier_not_vendor(self):
        """won_quotes column is `supplier`, not `vendor`. Reading the
        wrong key would silently produce 'SCPRS ()' display."""
        src = _read("src/agents/vendor_ordering_agent.py")
        # Must read q["supplier"]
        assert 'q.get(\'supplier\', \'\')' in src or 'q.get("supplier", "")' in src


class TestRealNameResolves:
    def test_find_similar_items_resolves(self):
        from src.knowledge.won_quotes_db import find_similar_items
        assert callable(find_similar_items)
