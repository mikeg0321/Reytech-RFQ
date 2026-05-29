"""Invocation test for /api/qa/regressions.

Why this exists: the endpoint returned HTTP 200 with {"ok": false, "error":
"near \\"drop\\": syntax error"} in prod for an unknown period. The query
selected `"drop" as drop` — but (a) the real column is `score_drop`, not
`drop`, and (b) the alias `as drop` was an unquoted SQL reserved word. Because
the handler catches the exception and returns a 200 with ok:false, neither an
HTTP-status monitor nor a page-render check could see it — only actually
calling the endpoint and inspecting the JSON catches it. (Found 2026-05-29 by
expanding the prod sentinel to functional/JSON-level checks.)

This test boots the app and asserts the query executes (ok:true), which would
have failed pre-fix.
"""


def test_qa_regressions_endpoint_executes(client):
    resp = client.get("/api/qa/regressions")
    assert resp.status_code == 200
    data = resp.get_json()
    # The whole point: the SQL must parse + run. Pre-fix this was ok:false
    # with "near \"drop\": syntax error".
    assert data.get("ok") is True, "regressions query failed: {}".format(data.get("error"))
    assert isinstance(data.get("regressions"), list)
    # When rows exist they expose the score-drop magnitude under the "drop"
    # key (the alias the UI reads); empty table is also valid.
    for row in data["regressions"]:
        assert "drop" in row
