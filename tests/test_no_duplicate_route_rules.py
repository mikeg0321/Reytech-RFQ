"""Guard against shadowed duplicate route handlers (creepy-crawler O1).

Two handlers were registered for `POST /api/pricecheck/<pcid>/auto-price`
(`routes_pricecheck_admin.py`). Werkzeug silently served the first-registered
one (`api_pricecheck_auto_price`) and the second (`api_pc_auto_price`, the
race-safe "PR #778" variant) was DEAD — never reached. Flask raises no error
for this; the only symptom is that editing the shadowed copy changes nothing.

This test fails if any (path, method) pair resolves to more than one endpoint,
so a future duplicate is caught at CI time instead of in production.
"""


def test_no_duplicate_route_rules(app):
    seen: dict[tuple[str, str], list[str]] = {}
    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static":
            continue
        for method in (rule.methods or set()):
            if method in ("HEAD", "OPTIONS"):
                continue
            seen.setdefault((str(rule.rule), method), []).append(rule.endpoint)

    dupes = {k: v for k, v in seen.items() if len(v) > 1}
    assert not dupes, (
        "Duplicate (path, method) → multiple endpoints; one handler is "
        f"shadowed/dead: {dupes}"
    )
