#!/usr/bin/env python3
"""orders_only_loop.py — close the orders-only investigation loop.

Single-command driver that:
  1. POSTs /api/admin/scprs-orders-only-sentinel-cleanup (flips
     sentinel rows like 'TEST' to is_test=1; idempotent + sticky
     since PR #646)
  2. POSTs /api/admin/orders-only-investigate (read-only Gmail API
     batch search per orders_only row, returning canonical-PO
     suggestions extracted from buyer email subjects)
  3. Pretty-prints the suggestions and writes them to
     data/orders_only_findings.json so the operator can review
     and apply rewrites with a follow-up command.

Apply step (per row):
  python scripts/orders_only_loop.py \\
    --apply ORDER_ID --new-po '8955-0000076737' \\
    --reason 'investigator confirmed PO from buyer email m1xxx'

Env required (loaded from `.env` at repo root or already exported):
  REYTECH_USER / REYTECH_PASS  (or DASH_USER / DASH_PASS)

Defaults to prod (https://web-production-dcee9.up.railway.app); override
with --base-url for staging.

This is a glue script, not a test — it intentionally does no DB
work locally and never holds creds in memory longer than the HTTP
call.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import urllib.error
import urllib.parse
import urllib.request
import base64

DEFAULT_BASE = "https://web-production-dcee9.up.railway.app"
FINDINGS_PATH_DEFAULT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "orders_only_findings.json",
)


def _load_dotenv(path: str) -> None:
    """Best-effort .env loader. Reads KEY=VALUE lines, ignores comments
    and quoted values. Doesn't override pre-existing env. We don't
    pull in python-dotenv to keep this script dependency-free."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def _resolve_creds() -> tuple[str, str]:
    user = os.environ.get("REYTECH_USER") or os.environ.get("DASH_USER", "")
    pw = os.environ.get("REYTECH_PASS") or os.environ.get("DASH_PASS", "")
    if not user or not pw:
        sys.stderr.write(
            "ERROR: REYTECH_USER/REYTECH_PASS (or DASH_USER/DASH_PASS) "
            "not set.\nLoad them from .env in the repo root, or export "
            "them in the shell before running this script.\n"
        )
        sys.exit(2)
    return user, pw


def _post_json(url: str, body: dict | None, user: str, pw: str,
               timeout: int = 60) -> tuple[int, Any]:
    payload = json.dumps(body or {}).encode("utf-8")
    auth = base64.b64encode(f"{user}:{pw}".encode()).decode()
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            pass
        return e.code, body


def _section(title: str) -> None:
    bar = "═" * 72
    print(f"\n{bar}\n  {title}\n{bar}")


def _money(v) -> str:
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return str(v)


def cmd_run(args) -> int:
    """Default: cleanup → investigate → save findings."""
    user, pw = _resolve_creds()
    base = args.base_url.rstrip("/")

    _section(f"1/2 sentinel cleanup ({base})")
    code, data = _post_json(
        f"{base}/api/admin/scprs-orders-only-sentinel-cleanup",
        body={"dry_run": args.dry_run},
        user=user, pw=pw,
    )
    if code != 200:
        sys.stderr.write(f"sentinel-cleanup HTTP {code}: {data!r}\n")
        return 1
    cands = data.get("candidates", 0)
    upd = data.get("rows_updated", 0)
    print(f"  candidates : {cands}")
    print(f"  rows_updated: {upd}  (dry_run={data.get('dry_run')})")
    for s in data.get("samples", [])[:10]:
        print(f"    - id={s.get('id')} po={s.get('po_number')!r} "
              f"agency={s.get('agency')!r} total={_money(s.get('total'))}")

    _section(f"2/2 orders-only Gmail investigator ({base})")
    code, data = _post_json(
        f"{base}/api/admin/orders-only-investigate",
        body={"max_messages": args.max_messages},
        user=user, pw=pw,
        timeout=180,  # Gmail can be slow
    )
    if code != 200:
        sys.stderr.write(f"investigate HTTP {code}: {data!r}\n")
        return 1
    rows = data.get("rows", [])
    print(f"  examined            : {data.get('examined', 0)}")
    print(f"  candidates_count    : {data.get('candidates_count', 0)}")
    print(f"  rows_with_suggestion: {data.get('rows_with_suggestion', 0)}")
    print()
    for r in rows:
        sugg = r.get("suggested_rewrite") or "—"
        print(f"  • order_id={r.get('order_id')!r}")
        print(f"      stored_po    : {r.get('po_number')!r}")
        print(f"      quote_number : {r.get('quote_number')!r}")
        print(f"      class        : {r.get('classification')}")
        print(f"      query        : {r.get('search_query')!r}")
        print(f"      msgs_matched : {r.get('matched_message_count', 0)}")
        print(f"      candidates   : "
              f"{[c.get('canonical') for c in r.get('candidates', [])]}")
        print(f"      → suggested  : {sugg}")
        if r.get("error"):
            print(f"      ⚠ error      : {r['error']}")
        print()

    out_path = args.findings_path or FINDINGS_PATH_DEFAULT
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "base_url": base,
            "rows": rows,
            "summary": {
                "examined": data.get("examined", 0),
                "candidates_count": data.get("candidates_count", 0),
                "rows_with_suggestion": data.get("rows_with_suggestion", 0),
            },
        }, f, indent=2, default=str)
    print(f"  findings saved → {out_path}")
    print("\n  next: review the JSON, then for each row you want to fix:")
    print(f"    python {os.path.basename(sys.argv[0])} \\")
    print("      --apply ORDER_ID --new-po '8955-...' \\")
    print("      --reason 'investigator confirmed'")
    return 0


def cmd_apply(args) -> int:
    user, pw = _resolve_creds()
    base = args.base_url.rstrip("/")
    body = {
        "order_id": args.apply,
        "new_po": args.new_po,
        "reason": args.reason or "",
        "dry_run": args.dry_run,
    }
    _section(
        f"orders-po-rewrite — order_id={args.apply} → {args.new_po} "
        f"(dry_run={args.dry_run})"
    )
    code, data = _post_json(
        f"{base}/api/admin/orders-po-rewrite",
        body=body, user=user, pw=pw,
    )
    print(json.dumps(data, indent=2, default=str))
    if code != 200:
        sys.stderr.write(f"  HTTP {code}\n")
        return 1
    return 0


def main():
    repo_root = os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
    _load_dotenv(os.path.join(repo_root, ".env"))

    p = argparse.ArgumentParser(
        description="Close the orders-only investigation loop "
                    "(cleanup + Gmail investigator + manual rewrites)."
    )
    p.add_argument("--base-url", default=DEFAULT_BASE,
                   help="Override prod URL (e.g. for staging)")
    p.add_argument("--dry-run", action="store_true",
                   help="Don't write — preview only")
    p.add_argument("--max-messages", type=int, default=10,
                   help="Per-row Gmail message cap (default: 10)")
    p.add_argument("--findings-path", default="",
                   help="Override default data/orders_only_findings.json")
    p.add_argument("--apply", default="",
                   help="ORDER_ID — apply a manual PO rewrite for "
                        "this order (requires --new-po).")
    p.add_argument("--new-po", default="",
                   help="Canonical PO to write into orders.po_number")
    p.add_argument("--reason", default="",
                   help="Audit-log free-text reason for the rewrite")

    args = p.parse_args()

    if args.apply:
        if not args.new_po:
            sys.stderr.write("ERROR: --apply requires --new-po\n")
            sys.exit(2)
        sys.exit(cmd_apply(args))
    sys.exit(cmd_run(args))


if __name__ == "__main__":
    main()
