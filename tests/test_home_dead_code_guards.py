"""Regression guards for home.html dead-code removals.

2026-04-20: `batchDismiss()` was a 15-line duplicate of `bulkAction('rfq',
'dismiss')` — zero callers anywhere in the repo. Removed, along with two
orphaned comment headers for deleted "Clear stale quote number" / "Delete
Price Check" sections and a leftover refactor marker.

The live functions that LOOK similar must stay:
  - bulkAction(type, action)    — the canonical bulk handler, called from
                                  inline onclick in the rendered queue tables
  - batchClearSelection()       — wired from partials/_queue_table.html
  - bulkGenerate()              — wired from routes_rfq.py bulk_actions list

If someone re-introduces batchDismiss in a future refactor, this test pings.
If someone accidentally removes one of the live three during a follow-up
cleanup, the "still present" tests ping.
"""
from __future__ import annotations


def _home_html(client) -> str:
    resp = client.get("/")
    assert resp.status_code == 200, f"/ returned {resp.status_code}"
    return resp.get_data(as_text=True)


class TestDeadBatchDismissIsGone:
    def test_batchDismiss_function_is_gone(self, auth_client):
        html = _home_html(auth_client)
        assert "function batchDismiss" not in html, (
            "Dead batchDismiss() is back — removed 2026-04-20 because zero "
            "callers exist and bulkAction('rfq','dismiss') already covers it"
        )

    def test_no_onclick_points_to_batchDismiss(self, auth_client):
        html = _home_html(auth_client)
        assert "batchDismiss(" not in html, (
            "Something re-wired a button to the deleted batchDismiss — use "
            "bulkAction('rfq','dismiss') instead"
        )


class TestLiveBulkHandlersStillPresent:
    """Belt-and-suspenders: if a follow-up cleanup deletes too aggressively,
    these catch it. Each function has at least one visible caller in the
    codebase — removing any of them would break a rendered button."""

    def test_bulkAction_still_defined(self, auth_client):
        html = _home_html(auth_client)
        assert "function bulkAction(type, action)" in html, (
            "bulkAction() got deleted — inline onclick handlers in the "
            "rfq/pc queue tables call this one"
        )

    def test_batchClearSelection_still_defined(self, auth_client):
        html = _home_html(auth_client)
        assert "function batchClearSelection()" in html, (
            "batchClearSelection() got deleted — "
            "partials/_queue_table.html 'Clear selection' button calls it"
        )

    def test_bulkGenerate_still_defined(self, auth_client):
        html = _home_html(auth_client)
        assert "function bulkGenerate()" in html, (
            "bulkGenerate() got deleted — routes_rfq.py bulk_actions "
            "config wires 'Generate All' to this handler"
        )


class TestOrphanCommentsGone:
    """Two comment headers ('Clear stale quote number from PC', 'Delete
    Price Check') had no code under them and were leftover from prior
    deletions. A stale '// dismissItem replaced by global doDismiss()'
    marker was also in the same block. Make sure they don't drift back."""

    def test_orphan_comment_headers_removed(self, auth_client):
        html = _home_html(auth_client)
        assert "Clear stale quote number from PC" not in html
        # The dismissItem marker referenced a function that no longer exists
        assert "dismissItem replaced by global doDismiss" not in html
