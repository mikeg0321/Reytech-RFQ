"""Local developer helper for the Proofpoint auto-pull v2 effort.

Walks the same login flow as the production /proofpoint/pull endpoint
but captures full diagnostics (URL, title, HTML head, DOM, screenshot)
at each step. Writes:
  - JSON dump with all steps + DOM details
  - PNG screenshot per step
  - HTML head per step

so you can iterate on selectors without burning a scraper code-deploy
cycle for each guess.

Usage:
  # Production scraper (via the web service env or your local .env):
  SCRAPER_SERVICE_URL=http://localhost:8001 \
  SCRAPER_SECRET=... \
  PROOFPOINT_EMAIL=sales@reytechinc.com \
  PROOFPOINT_PASSWORD=... \
  python scripts/dev_proofpoint_inspect.py <portal_url>

  # Or from within the web container, the env vars are already set:
  railway ssh --service web -- /opt/venv/bin/python /app/scripts/dev_proofpoint_inspect.py <portal_url>

  # Optional: --persist-cookies to reuse the prod-side persistent profile
  # (useful when you're trying to debug post-login state without
  # re-authenticating). Default is a fresh ephemeral context.

Output lands under ./tmp/proofpoint_diag_<timestamp>/.
"""
import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("portal_url", help="Proofpoint portal URL")
    ap.add_argument("--persist-cookies", action="store_true",
                    help="Reuse the prod persistent-profile cookie state")
    ap.add_argument("--timeout-s", type=int, default=30,
                    help="Per-stage Playwright timeout (default 30)")
    ap.add_argument("--out", default=None,
                    help="Output directory (default ./tmp/proofpoint_diag_<ts>)")
    args = ap.parse_args()

    import requests

    scraper_url = os.environ.get("SCRAPER_SERVICE_URL", "").rstrip("/")
    secret = os.environ.get("SCRAPER_SECRET", "")
    email = os.environ.get("PROOFPOINT_EMAIL", "")
    password = os.environ.get("PROOFPOINT_PASSWORD", "")

    if not scraper_url:
        print("ERROR: SCRAPER_SERVICE_URL not set", file=sys.stderr)
        sys.exit(1)
    if not email or not password:
        print("ERROR: PROOFPOINT_EMAIL / PROOFPOINT_PASSWORD not set",
              file=sys.stderr)
        sys.exit(1)

    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-Scraper-Secret"] = secret

    payload = {
        "portal_url": args.portal_url,
        "email": email,
        "password": password,
        "timeout_s": args.timeout_s,
        "persist_cookies": args.persist_cookies,
    }

    endpoint = f"{scraper_url}/proofpoint/inspect"
    print(f"POSTing to {endpoint} (timeout {args.timeout_s + 60}s)...")
    resp = requests.post(endpoint, json=payload, headers=headers,
                         timeout=args.timeout_s + 60)
    if resp.status_code != 200:
        print(f"HTTP {resp.status_code}: {resp.text[:500]}", file=sys.stderr)
        sys.exit(1)
    body = resp.json()
    if not body.get("ok"):
        print(f"scraper failure: {body.get('error', 'unknown')}", file=sys.stderr)
        sys.exit(1)
    data = body.get("data") or {}

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_dir = args.out or os.path.join("tmp", f"proofpoint_diag_{ts}")
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    summary = {
        "portal_url": data.get("portal_url"),
        "email_used": data.get("email_used"),
        "persist_cookies": data.get("persist_cookies"),
        "login_form_detected": data.get("login_form_detected"),
        "login_attempted": data.get("login_attempted"),
        "submit_clicked": data.get("submit_clicked"),
        "errors": data.get("errors", []),
        "steps": [],
    }

    for step in data.get("steps", []):
        name = step.get("name", "unknown")
        # Extract screenshot to its own PNG.
        sshot_b64 = step.pop("screenshot_b64", None)
        if sshot_b64:
            png_path = os.path.join(out_dir, f"{name}.png")
            with open(png_path, "wb") as fh:
                fh.write(base64.b64decode(sshot_b64))
            step["screenshot_path"] = png_path
        # Extract HTML head to its own file.
        html_head = step.pop("html_head", None)
        if html_head:
            html_path = os.path.join(out_dir, f"{name}.html.head.txt")
            with open(html_path, "w", encoding="utf-8") as fh:
                fh.write(html_head)
            step["html_path"] = html_path
        summary["steps"].append(step)

    json_path = os.path.join(out_dir, "summary.json")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)

    print()
    print("=" * 60)
    print(f"DIAGNOSTIC CAPTURE — {out_dir}")
    print("=" * 60)
    print(f"login_form_detected: {summary['login_form_detected']}")
    print(f"login_attempted:     {summary['login_attempted']}")
    print(f"submit_clicked:      {summary['submit_clicked']}")
    print(f"errors:              {len(summary['errors'])}")
    for e in summary["errors"]:
        print(f"  - {e}")
    print()
    for step in summary["steps"]:
        name = step.get("name", "?")
        url = step.get("url", "")
        title = step.get("title", "")
        inputs = len(step.get("inputs", []))
        buttons = len(step.get("buttons", []))
        links = len(step.get("links", []))
        forms = len(step.get("forms", []))
        print(f"--- {name} ---")
        print(f"  URL:     {url[:100]}")
        print(f"  title:   {title!r}")
        print(f"  inputs:  {inputs} | buttons: {buttons} | links: {links} | forms: {forms}")
        # Highlight visible inputs (login fields)
        for inp in step.get("inputs", []):
            if inp.get("visible") and inp.get("type") in ("text", "email", "password"):
                print(f"    visible {inp.get('type')}: name={inp.get('name')!r} id={inp.get('id')!r}")
        # Highlight non-empty links
        for lnk in (step.get("links") or [])[:10]:
            txt = lnk.get("text", "")
            if txt and len(txt) < 80:
                print(f"    link: {txt!r} -> {lnk.get('href', '')[:80]}")
        attach_sels = step.get("selectors_tried")
        if attach_sels:
            print("  attachment selector probe:")
            for s in attach_sels:
                print(f"    {s.get('selector')!r}: {s.get('count', s.get('error'))}")
        print()
    print(f"Full JSON: {json_path}")
    print(f"Screenshots + HTML heads under: {out_dir}/")


if __name__ == "__main__":
    main()
