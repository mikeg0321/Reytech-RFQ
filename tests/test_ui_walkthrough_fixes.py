"""Regressions for the two small UI issues surfaced by the 2026-04-17
DevTools walkthrough on production:

1. /api/diag returned 500 with "Object of type EmailPoller is not JSON
   serializable" because POLL_STATUS contains a live EmailPoller object
   under "_poller_instance" that jsonify chokes on.
2. /rfqs returned 404 — operators naturally try it (there's /pricechecks,
   /quotes, /orders) but no dedicated RFQ list page exists. Now redirects
   to home where RFQs surface.
"""
import pytest


class TestDiagEndpoint:

    def test_returns_200_not_500(self, client):
        # Before the fix this endpoint 500'd on every home-page load because
        # POLL_STATUS["_poller_instance"] is a live EmailPoller.
        r = client.get("/api/diag")
        assert r.status_code == 200, (
            f"/api/diag returned {r.status_code}. "
            f"If 500 with 'not JSON serializable', the _poller_instance "
            f"filter regressed."
        )
        body = r.get_json()
        # poll_status must be present, but must NOT contain underscore-
        # prefixed keys (those hold live objects, not data)
        assert "poll_status" in body
        for k in body["poll_status"].keys():
            assert not k.startswith("_"), (
                f"poll_status contains a live-object key {k!r}; filter broke"
            )


class TestRfqsRedirect:

    def test_rfqs_redirects_to_home(self, client):
        # Natural URL operators type (matches /pricechecks, /quotes, /orders).
        # Was 404 before the fix.
        r = client.get("/rfqs", follow_redirects=False)
        assert r.status_code in (301, 302, 303, 307, 308), (
            f"/rfqs should redirect, got {r.status_code}"
        )
        loc = r.headers.get("Location", "")
        assert loc in ("/", "") or loc.endswith("/"), (
            f"/rfqs redirect target should be home (/), got {loc!r}"
        )
