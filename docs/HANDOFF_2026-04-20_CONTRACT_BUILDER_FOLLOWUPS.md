# Handoff — Contract Builder Follow-ups + Remaining Audit Sweep

**Written:** 2026-04-20 by previous Claude window
**Worktree:** `C:\Users\mikeg\rfq-rfq-contract-builder`
**Branch status at handoff:** `feat/rfq-contract-builder` shipped as PR #240 with auto-merge armed. Base commit on `main` at handoff: `b9776ace` (PR #239 B1 manual-submit emergency route).
**Authority:** Mike reviewed & approved. Auto mode → execute autonomously, course-correct if blocked.

---

## 0. What the previous window already shipped

| PR | Branch | What landed |
|----|--------|-------------|
| #239 | `feat/manual-submit-emergency` | B1 — POST `/rfq/<rid>/manual-submit` emergency route. Operator uploads hand-filled 704B PDF; bypasses broken auto-fill. |
| #240 | `feat/rfq-contract-builder` | Unified Contract Builder: single dropzone on `/rfq/<rid>` auto-classifies uploads → 703B/704B/bidpkg template slots, email screenshots (new `r['email_screenshot']` key), or attachments. New `src/forms/form_classifier.py` + `POST /api/rfq/<rid>/contract-upload` + Contract Builder card in `rfq_detail.html`. 13 new tests all green. |

**First action of the new window:**
```bash
cd C:\Users\mikeg\rfq-rfq-contract-builder
git fetch origin
gh pr view 240 --json state,mergedAt,statusCheckRollup
```
If PR #240 is still open, run `gh pr checks 240 --watch`. If it merged, rebase onto main:
```bash
git checkout main && git pull
make worktree name=feat/ui-contract-followups  # or reuse this worktree after `git checkout main && git pull && git branch -D feat/rfq-contract-builder`
```

---

## 1. "A group" — operator-facing UI consolidation (ship FIRST)

Source: `docs/UX_AUDIT.md` — the five consolidations Mike has been asking about. This is the natural sequel to the Contract Builder work (same theme: cut operator cognitive load on RFQ/PC detail pages). Ship each as its own PR so each can be reverted independently if it lands wrong.

| # | Change | Impact | Est effort |
|---|--------|--------|-----|
| A1 | **Consolidate Save workflow** — merge Save / Save & Fill 704 / Download 704 / Re-fill 704 into one `💾 Save & Download` button. Saves prices, generates PDF, auto-downloads. If no changes since last gen, just re-downloads. | HIGH | 30 min |
| A2 | **Consolidate pricing** — replace 7 buttons (SCPRS, Amazon, Catalog, AI Find, Web Search, Auto-Price, Sweep) with one `🔍 Find Prices` button + dropdown arrow for manual single-source override. | HIGH | 20 min |
| A3 | **Dedupe admin buttons** — adminAction appears 8 times, saveAndGenerate 3x, window.print 2x, generateQuote 2x. Keep each in More menu only; remove toolbar duplicates. | MEDIUM | 15 min |
| A4 | Move `🔍 diagnose` into More menu (rare debug tool). | LOW | 5 min |
| A5 | Promote `📝 Revisions` to the primary toolbar (new feature, currently buried in More). | LOW | 5 min |

For each A-item: ship as `fix/ui-*` or `feat/ui-*`, include a Chrome DevTools MCP walkthrough (see §4), and make sure the existing PC/RFQ tests still pass. **Do not** bundle all five into one PR — too big a blast radius on the highest-traffic page.

---

## 2. Other remaining deliverables, prioritized

After A group ships, work these in order. Verify current state before coding — memory is up to 5 days stale.

### P0 — 704 Rebuild continuation (biggest revenue risk)
- Phase 0 (B1 manual-submit) = SHIPPED PR #239.
- **Phase 1** — Quote model v2 + Form Profile Registry skeleton + boot-time validator, behind `QUOTE_MODEL_V2=true` flag. Read `docs/DESIGN_QUOTE_MODEL_V2.md` + `docs/DESIGN_704_REBUILD.md` first. Do **not** edit the delete-list modules yet (`price_check.py`, `ams704_helpers.py`, `routes_rfq_gen.py`, `ingest_pipeline.py`, `request_classifier.py`, `rfq_parser.py`, `reytech_filler_v4.py`).
- **Phase 2** — parse + fill engines in shadow mode, diff log at `data/shadow_diffs.jsonl`, `/admin/shadow-diffs` dashboard, 20 consecutive real bids with zero divergence before Phase 3.

### P1 — Verification + backfills
- Confirm PR #240 Contract Builder card renders correctly in all four RFQ states: fresh (no files), partial (only email screenshot), partial (only 704b template), complete. Screenshot each via Chrome DevTools MCP and attach to PR comment.
- Golden-path expansion (`tests/test_golden_path.py`): add PC→RFQ deepcopy + 704B fill, package generation, DOCX source flow, email pipeline mock → parse → PC → price → generate. Promote to CI gate in `.github/workflows/ci.yml` once stable.

### P2 — Ops cleanups
- Orders V2: merge `purchase_orders` data into `orders` + `order_line_items` (still split-brain), drop `orders.data_json` blob after confirming zero reads, auto-mark delivered when carrier webhook shows delivered.
- Stale memory cleanup — 8 memory entries listed in `project_rfq_remaining_deliverables.md` need rewrite/delete so the next session doesn't chase ghosts.

### P3 — UI polish (most of Grok Tier 2/3 already shipped)
Check before building: cmd palette (#199), filter chips (#219), PDF preview modal (#200/#220), SVG progress rings (#209), Tailwind utility classes (#209), JSON trail export (#223) all landed recently. Remaining:
- Tailwind+DaisyUI migration of `base.html` (Tier 3 #8).
- Mobile-first bottom nav — **skip unless Mike asks**. He works desktop.

---

## 3. Re-audit protocol (run once A group is done)

Do a fresh top-to-bottom audit before declaring done. This is line-by-line, not sampling.

### 3a. Static audit
1. `git diff main...HEAD --stat` — confirm only intended files changed.
2. For every modified Python file: `python -c "import py_compile; py_compile.compile(PATH, doraise=True)"`.
3. For every modified template: render-test with the full variable set the route passes (open the route handler, copy the context dict into a throwaway test).
4. `grep -rn "TODO\|FIXME\|XXX"` the diff — nothing new should ship.
5. **Audit the commit for sneak-ins** — `git show --stat HEAD` per commit. Any file you didn't mean to touch: read the diff and revert or justify. The 2026-04-10 incident (7 agent files shipped alongside a DOCX fix) is the reason this step is mandatory.

### 3b. Route + data-access audit
- Every new or modified POST route: `@auth_required` present, CSRF covered by session, rate limit decorator appropriate for cost tier.
- Every new SQL query: no f-string interpolation of user input; allowlist or parameterized.
- Every `except:` clause: specific exception type, `log.error(...)` with `exc_info=True`, no bare `except:` and no silent `pass`.

### 3c. UX audit (Chrome DevTools MCP — mandatory for UI changes)
Per `feedback_workflow_ui_chrome_verify.md` + `feedback_visual_verify_always.md`. Backend tests + smoke are **not enough** for workflow/UI changes.

For each A-item that touched UI:
1. `mcp__chrome-devtools__new_page` → `https://web-production-dcee9.up.railway.app` (or local dev — start with `make dev` if not running).
2. Log in, navigate to a real PC (has items) and a real RFQ (has templates).
3. Exercise the golden path: click the button, wait for the network request, verify the response, verify the UI updated.
4. Exercise edge cases: empty state, error state (kill the network mid-request), re-run state.
5. `mcp__chrome-devtools__take_screenshot` before + after each state. Attach to the PR.
6. `mcp__chrome-devtools__list_console_messages` — zero errors. Zero warnings that are new vs baseline.
7. `mcp__chrome-devtools__list_network_requests` — no 4xx/5xx on the golden path.

---

## 4. Button-level E2E sweep (runs last, gates the handoff as complete)

This is the "line by line, button end to end" step Mike asked for. Run against prod (`web-production-dcee9.up.railway.app`) with a real test RFQ. Record pass/fail in `docs/E2E_SWEEP_2026-04-20.md` as you go.

### Pages to sweep (in order)
1. `/` homepage — KPI cards, PC queue, RFQ queue, drag-drop upload, activity feed, Quick Price panel.
2. `/pricechecks` + `/pc/<pcid>` — every toolbar button, More menu, status stepper, item-row actions. Post-A group: confirm the consolidated `💾 Save & Download` and `🔍 Find Prices` buttons work.
3. `/rfq` + `/rfq/<rid>` — Contract Builder dropzone (shipped PR #240), 📧 Email / 📄 703B / 📄 703C / 📋 704B / 📦 BidPkg slot badges, 🆘 Manual 704B (PR #239), per-RFQ actions, status stepper.
4. `/quoting/status` + `/quoting/status/<doc_id>` — stepper, retry modal, PDF preview, filter chips, JSON trail export.
5. `/quotes` — list view, cmd palette (Cmd+K), filter chips, PDF preview.
6. `/admin/flags`, `/admin/locked-costs`, `/admin/shadow-diffs` (if Phase 2 lands).

### Per-button checklist
For each button on each page:
- [ ] Click once — expected result (server 2xx, UI updates, no console errors).
- [ ] Click twice fast — no double-submit (idempotent or disabled mid-request).
- [ ] Click during an error state — graceful message, no silent failure.
- [ ] Keyboard access — can reach via Tab, activates on Enter/Space.
- [ ] Network inspected — no unintended side requests, no stale-cache 304 masking a real failure.

### Regression guardrails
Run these after the sweep is clean:
```bash
python -m pytest tests/ -v --tb=short   # full sandbox, ~90s–3min
make smoke                              # prod synthetic
```
Both green → update `project_rfq_remaining_deliverables.md` with what shipped, write the end-of-session memory entry, open a wrap-up PR with the audit + sweep notes.

---

## 5. Rules that apply (non-negotiable)

- **Worktree protocol** — this window owns `C:\Users\mikeg\rfq-rfq-contract-builder`. If another window is active on this repo, spin a fresh worktree (`make worktree name=feat/...`). See `CLAUDE.md` → "Worktrees Are Required for Parallel Windows."
- **No push to main** — always via `make ship` (auto-mode: `make ship auto=1`).
- **Three-strikes rule** — if the same fix fails 3 times, STOP, revert, write a handoff note, hand it off. Do not attempt a 4th fix.
- **Test before push** — pre-push hook is a floor, not a ceiling. Run the relevant subset proactively.
- **Never use SCPRS or Amazon prices as supplier cost** — they're reference/ceiling only. See `CLAUDE.md` → Pricing Guard Rails.
- **Signature placement** — only sign lower 40% for generic `Signature1`/`Signature` fields; form-specific names (e.g., `Signature_703b`) are safe anywhere.
- **Reytech identity on forms** — "Michael Guadan" + `sales@reytechinc.com`. No variants, even if reference packs show others.
- **No Slack.** Mike does not use it. Do not add webhooks/alerts there.
- **Never push form changes without verifying against real PDF field dumps.** No guessing — use `PdfReader(...).get_fields()` first.

---

## 6. If blocked

- Prod on fire? `make rollback` → `make smoke` → diagnose on a branch, not main.
- PR #240 CI red on a test the new window didn't touch? Check `.github/workflows/ci.yml` for timeout bumps or flaky suites; `gh run view <id> --log-failed`. Don't retry blindly.
- Can't reach consensus on what "A group" means for a given user session? Default to the UX_AUDIT.md items listed in §1 — that's the most recent concrete UI audit and the natural follow-on to the Contract Builder's discoverability theme. If Mike clarifies otherwise mid-session, pivot immediately.

---

## 7. End-of-handoff checklist for the outgoing window

- [x] PR #240 opened with auto-merge armed.
- [x] 13 new tests passing; pre-push gate green.
- [x] `WORKSTREAMS.md` updated — `feat/rfq-contract-builder` row added.
- [x] This handoff written.
- [ ] Task #56 marked completed (done in TaskList after writing this doc).

Good hunting.
