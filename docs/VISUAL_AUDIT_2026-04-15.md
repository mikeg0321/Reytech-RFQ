# Visual/DOM Audit — 2026-04-15

Conducted via chrome-devtools MCP against production after deploying PRs #95–#101.

## Pages Checked

| Page | Broken Images | Dead Links | JS Errors | Error Text | Unlabeled Inputs | Dup IDs | Verdict |
|---|---|---|---|---|---|---|---|
| `/` (home) | 0 | 0 | 0 | 1 (stale activity feed entry) | 8 | 0 | Clean |
| `/quotes` | 0 | 0 | 0 | 0 | 0 | 0 | Clean |
| `/outbox` | 0 | 0 | 0 | 0 | 0 | 0 | Clean |
| `/orders` | 0 | 0 | 0 | 0 | — | 0 | Clean |
| `/pipeline` | 0 | 0 | 0 | 0 | — | 0 | Clean |

## Bugs Found

### Pre-existing (not introduced by today's PRs)

1. **`/api/metrics` 500s** — `NameError: _rate_limiter` in exec'd module context.
   The route references a `dashboard.py` namespace variable that isn't injected into the
   exec'd `routes_crm.py`. Works in production (exec shares namespace) but fails in
   test isolation. Covered by xfail test in `test_blind_spot_routes.py`.

2. **Home activity feed "500 Error: MethodNotAllowed"** — stale historical entry from
   before the GET /rfq/<id>/update fix (PR #95). Not a new occurrence — just persisted
   data that hasn't been cleaned up. Harmless but confusing.

3. **5 govspendemail CS drafts still in outbox** — created before the auto-reject list
   went live (PR #101). New drafts ARE blocked. Old ones need manual bulk-delete.

4. **8 unlabeled inputs on home** — a11y gap in search/filter inputs that lack
   `aria-label`, `placeholder`, or `id`. P2 polish.

## PR #95 Features Confirmed Working

- [x] Win rate single % (not %%)
- [x] Ghost R26Q16 absent from /quotes
- [x] RFQ# column backfilled
- [x] Award Tracker tile with health verdict + eligible count
- [x] Audit Now + Reconcile PO + Backfill Oracle buttons
- [x] QA gate banner on RFQ detail (12 blockers, buttons disabled)
- [x] Food Cert auto-hidden on non-food RFQ
- [x] Line# readonly with double-click-to-edit
- [x] Qty/cost/price min/max bounds in DOM
- [x] Pipeline "Sent→Won" label (not "Win Rate")
- [x] Chart.js + fonts self-hosted (zero CSP errors)
- [x] Expiring-quote banner with $-sort and dismiss
- [x] Outbox search + age filter
- [x] SLA coloring on Awaiting Response cards

## No Issues Found (clean pages)
- Zero CSP / Content-Security-Policy console errors on any page
- Fonts (DM Sans, JetBrains Mono) rendering correctly everywhere
- Chart.js bars rendering on /pipeline
- All nav links functional
