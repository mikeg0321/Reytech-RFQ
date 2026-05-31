"""Regression test for routes_crm :: api_vendor_performance.

Incident (2026-05-30, creepy-crawler site sweep): GET /api/vendor/performance
500'd on every call with `NameError: name 'defaultdict' is not defined`.
routes_crm.py used `defaultdict` at the top of the handler but never imported
it (the S11 refactor moved this module to explicit imports, and `defaultdict`
was missing from the injected-globals fallback). The route was dead in prod.

Guard: the endpoint must not raise NameError — it returns 200 with the
`{"ok": True, ...}` envelope even when the catalog is empty.
"""


def test_vendor_performance_does_not_500_on_missing_import(auth_client):
    r = auth_client.get("/api/vendor/performance")
    # Must not be a 500 (NameError) — the import regression.
    assert r.status_code == 200, r.get_data(as_text=True)[:300]
    body = r.get_json()
    assert body["ok"] is True
    assert "vendors" in body
