# SESSION_RESUME.md — Instant Context Recovery

**Last updated:** 2026-03-24

## WHAT THIS APP IS

Reytech RFQ — end-to-end RFQ automation for Reytech Inc., a California SB/DVBE government reseller.
- **Stack:** Python 3.12 / Flask / SQLite (WAL) / Jinja2 / Gunicorn
- **Deploy:** Push to `main` → Railway auto-deploys
- **Live:** https://web-production-dcee9.up.railway.app
- **Repo:** https://github.com/mikeg0321/Reytech-RFQ.git
- **Size:** 101K+ lines, 955 routes, 50 templates, 42 background agents

## CURRENT STATE

All commits pushed and deployed. No pending local changes.

### Recent Fixes (2026-03-23, all pushed and deployed)
1. **Vision parser NameError** — `pdf_path` → `file_path` (was crashing all image parsing)
2. **Add-item autosave race condition** — cancel timer + sync flush before add POST
3. **7 audit fixes** — data wipe, XSS, GET-writes, PDF crashes, stale DOM count
4. **Text parser dedup** — 40-char prefix truncation dropped similar items → full description dedup
5. **Catalog boot crash** — bare `_get_db()` call → `with _get_db() as _db:` context manager
6. **Vision key stale** — module-level `ANTHROPIC_API_KEY` → live `_get_api_key()` at call time
7. **Pre-deploy false positives** — UTF-8 encoding + globals whitelist + Windows console fix
8. **Due date reminder crash** — same bare `get_db()` pattern → context manager
9. **Henry Schein URL hang** — login-required domains fast-fail + 15s client-side timeout on all lookups
10. **Quote notes field** — "Notes to Buyer" textarea on RFQ, saved to rfq['quote_notes'], printed on PDF
11. **Tax rate fallback** — CDTFA API fallback table + amber badge for fallback sources
12. **PC upload overwrites** — protected ship_to/delivery fields from PDF merge + onblur auto-save
13. **PC URL paste blocked** — removed lockable-field from link inputs so URLs always pasteable
14. **CRM autocomplete** — buyer name autocomplete on New PC, New RFQ, and RFQ detail from CRM + SCPRS

## KNOWN REMAINING ISSUES

### Railway Env Vars — SET (2026-03-24)
- mike@ inbox: `GMAIL_ADDRESS_2` + `GMAIL_PASSWORD_2` — configured
- Vision AI: `ANTHROPIC_API_KEY` — configured

### Medium Priority — ALL RESOLVED (2026-03-24)
- ~~Race condition in load/modify/save~~ → save locks (_save_rfqs_lock, _save_pcs_lock)
- ~~JSON file writes without locks~~ → _json_write_lock in growth_agent + email_poller
- ~~Global status dicts without locks~~ → _status_lock wrapping all 15 .update() calls
- ~~Upload files disk leak~~ → try/finally cleanup in upload-parse-doc

### Low Priority
- Open redirect in email tracking — routes_prd28.py:149
- Memory leak in unbounded caches — growth_agent.py:3708, dashboard.py:115

## KEY FILES

| Area | File |
|------|------|
| Main app + routes | src/api/dashboard.py |
| RFQ routes | src/api/modules/routes_rfq.py |
| Price check routes | src/api/modules/routes_pricecheck.py |
| Email poller | src/agents/email_poller.py |
| Vision parser | src/forms/vision_parser.py |
| Text parser | src/forms/generic_rfq_parser.py |
| PDF filler | src/forms/reytech_filler_v4.py |
| Database | src/core/db.py |
| Templates | src/templates/*.html |
| Project rules | CLAUDE.md |

## HOW TO RESUME

1. `cd C:\Users\mikeg\Documents\Reytech-RFQ`
2. `git pull origin main`
3. Read CLAUDE.md for project rules
4. Read this file for current state
5. `python tests/pre_deploy_check.py` to verify health
