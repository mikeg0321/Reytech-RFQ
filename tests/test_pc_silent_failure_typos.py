"""Regression guards for three silent-failure typos in the PC module.
All three were discovered by the 2026-04-21 PC module audit. Each bug
was swallowed by an exception handler, leaving operators unaware anything
failed.

PC-2. Rename route (`/pricecheck/<pcid>/rename`)
    routes_pricecheck.py:1609 used `_save_single_pc(pcid, pc)` — `pc`
    was never bound in that function; the rename silently NameErrored
    and the name change didn't persist. The `@safe_page` wrapper
    returned a 500 HTML page; JSON clients got the HTML instead of
    `{"ok": true}`.

PC-3. Institution self-heal at `_pricecheck_detail_inner`
    routes_pricecheck.py:1076 used `_save_single_pc(pc_id, pc)` — the
    function param is `pcid`. Bare `except Exception as _e:
    log.debug(...)` at line 1079-1080 swallowed every NameError, so the
    self-heal was theater: every page load rediscovered the resolved
    institution but never persisted it.

PC-4. "View/Edit Sent Document" href
    pc_detail.html:178 had a literal `'+_pcid+'` JS-concat remnant
    embedded inside what is actually a server-rendered Jinja string,
    so the browser navigated to `/pricecheck/'+_pcid+'/document/...` —
    a 404. Both sibling links on that line (View + All Versions) had
    the same typo.
"""
from __future__ import annotations

import json
import os
import pathlib


# ── PC-2: Rename route must persist ──────────────────────────────────────

def test_pc_rename_persists(client, seed_pc):
    """Rename must succeed AND change the display name in storage.

    Before fix: NameError on `pc` (unbound local). `@safe_page` returned
    500 HTML; the rename silently didn't persist.
    """
    resp = client.post(
        f"/pricecheck/{seed_pc}/rename",
        json={"pc_number": "Renamed-OK-2026"},
        content_type="application/json",
    )
    assert resp.status_code == 200, (
        f"rename returned {resp.status_code}; body={resp.get_data(as_text=True)[:200]}"
    )
    body = resp.get_json()
    assert body and body.get("ok") is True, f"expected ok:true, got {body!r}"
    assert body.get("pc_number") == "Renamed-OK-2026"

    # Read back via the authoritative load path.
    from src.api.data_layer import _load_price_checks
    pcs = _load_price_checks()
    assert pcs[seed_pc]["pc_number"] == "Renamed-OK-2026", (
        f"rename did not persist: got {pcs[seed_pc]['pc_number']!r}"
    )


def test_pc_rename_source_uses_pcs_index():
    """Belt-and-suspenders: the call site MUST pass `pcs[pcid]` or
    `pcs.get(pcid)`, not an unbound `pc`.

    routes_pricecheck.py is loaded via exec() into dashboard.py's
    namespace — grep the source directly."""
    src = pathlib.Path("src/api/modules/routes_pricecheck.py").read_text(
        encoding="utf-8"
    )
    # Locate the pricecheck_rename function.
    rename_start = src.index("def pricecheck_rename(")
    rename_end = src.index("\n\n\n", rename_start)
    body = src[rename_start:rename_end]
    # The save call MUST use pcs[pcid] form, NOT a bare `pc` variable.
    assert "_save_single_pc(pcid, pc)" not in body, (
        "pricecheck_rename still calls _save_single_pc(pcid, pc) — "
        "`pc` is unbound in that function, which NameErrors silently."
    )


# ── PC-3: Institution self-heal call site must use pcid ──────────────────

def test_pc_detail_self_heal_uses_pcid():
    """The self-heal block inside `_pricecheck_detail_inner(pcid)` must
    reference the param `pcid`, not the undefined `pc_id`."""
    src = pathlib.Path("src/api/modules/routes_pricecheck.py").read_text(
        encoding="utf-8"
    )
    # Locate _pricecheck_detail_inner.
    fn_start = src.index("def _pricecheck_detail_inner(")
    # Find the next top-level function or route to bound the scan.
    fn_end_candidates = [
        src.index("\n@bp.route", fn_start),
        src.index("\ndef _", fn_start + 1),
    ]
    fn_end = min(c for c in fn_end_candidates if c > fn_start)
    body = src[fn_start:fn_end]
    # The bug: _save_single_pc(pc_id, pc) — `pc_id` is undefined, param is `pcid`.
    assert "_save_single_pc(pc_id," not in body, (
        "_pricecheck_detail_inner still references undefined `pc_id` in "
        "_save_single_pc call — function param is `pcid`. Bare except "
        "swallows the NameError, so the institution self-heal never persists."
    )


# ── PC-4: Sent-document href must not embed literal '+_pcid+' ────────────

def test_pc_detail_sent_doc_href_uses_jinja(client, temp_data_dir, sample_pc):
    """When a PC is sent with a current_doc_id, the 'View/Edit Sent
    Document' link must render a real URL — not a literal string with
    `'+_pcid+'` JS-concat syntax that produces 404s in the browser."""
    pc = dict(sample_pc)
    pc["status"] = "sent"
    pc["current_doc_id"] = "doc_abc123"
    path = os.path.join(temp_data_dir, "price_checks.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({pc["id"]: pc}, f)

    resp = client.get(f"/pricecheck/{pc['id']}")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)

    # Before fix: raw `'+_pcid+'` token appeared INSIDE an href attribute.
    # (The same token inside `<script>` blocks is legitimate JS concat —
    # only hrefs render the literal token as a URL.)
    import re
    bad_hrefs = re.findall(r'href="[^"]*\+_pcid\+[^"]*"', html)
    assert not bad_hrefs, (
        f"pc_detail.html still has literal '+_pcid+' in href: {bad_hrefs[:3]}"
        f" — browser navigates to that literal string and 404s."
    )
    # After fix: server-rendered href must include the actual PC id + doc id.
    assert f"/pricecheck/{pc['id']}/document/doc_abc123" in html, (
        "sent-document link should render with real pcid + doc_id"
    )
    assert f"/pricecheck/{pc['id']}/documents" in html, (
        "All Versions link should render with real pcid"
    )


def test_pc_detail_template_no_href_concat_remnants():
    """Belt-and-suspenders: no `href="..."` attribute in the template
    may contain the JS-concat token `'+_pcid+'`. Inside `<script>`
    blocks the same pattern is legitimate JS and is left alone."""
    import re
    tpl = pathlib.Path("src/templates/pc_detail.html").read_text(encoding="utf-8")
    bad = re.findall(r'href="[^"]*\+_pcid\+[^"]*"', tpl)
    assert not bad, (
        f"pc_detail.html has literal '+_pcid+' inside href: {bad[:3]} "
        f"— use {{{{ pcid }}}} inside Jinja hrefs instead."
    )
