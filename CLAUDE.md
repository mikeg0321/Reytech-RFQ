# CLAUDE.md — Reytech RFQ Project Rules

## §0 — THE WOLFPACK OPERATING MODEL (READ FIRST — GOVERNS EVERYTHING BELOW)

> Added 2026-05-21 after the crossroads review. This section is the
> operating model. Every rule below §0 — the Prime Directive, the guard
> rails, the incident rules — is SUBJECT to it. Memory:
> `project_rfqapp_crossroads_verdict_2026_05_21`.

### The diagnosis — why this section exists

The code is ~80% sound. The Spine data model fixed the quoting math.
The disease is the **operating model**, measured 2026-05-21:

- 140 working directories, 121 git worktrees, 1,665 branches — no
  canonical home.
- 717 commits/30d, 47 reverts — 1 in 15 commits undoes another. The
  rate did not drop after the Spine shipped.
- **Three** quote substrates coexist: the legacy dict path,
  `src/core/quote_contract.py` (`QuoteContract`), and `src/spine/`
  (the Spine). Each "fix" added a layer; none deleted the prior one.

A migration that never deletes what it replaces is not a migration.
That single behavior is what this section exists to stop.

### The cure — seven laws

**LAW 1 — One substrate.** The Spine (`src/spine/`) is the canonical
quote path. `QuoteContract` and the legacy dict path are on a
**deletion schedule, not a coexistence schedule**. No fourth substrate
is ever created without Architect AND Closer sign-off (LAW 4);
disagreement defaults to NO.

**LAW 2 — A migration is DONE only when the replaced legacy code is
DELETED** and the deletion commit is visible in `git log`. "Shadow
mode works" is not done. "Routes repointed" is not done. The legacy
file is gone, or it is not done. No new agency starts until the prior
one is done by this definition. (The Migration Completion Gate.)

**LAW 3 — Convergence is measured by COUNT, not lines.** Net-negative
LOC is banned as a metric — it is gameable (delete 500 legacy lines,
add 480 of a new substrate, "pass"). The real metric is a ratchet:
`tests/spine/convergence_baseline.json` holds the current count of
(a) code paths that can write a quote, (b) distinct quote substrates,
(c) tracked working directories. A test asserts current ≤ baseline.
These numbers may only go DOWN. Lowering one requires a deletion
commit; raising one fails the build.

**LAW 4 — Architecture is authorized, never assumed.** Any change to
`src/spine/model.py`, any DB schema, `SPINE_CHARTER.md`, or the
creation of any new `src/spine/` module requires the Architect's
approval recorded in the PR. A new substrate / agency / migration
requires Architect AND Closer; if they disagree, the answer is NO.
Implementers may not make these changes. Enforced at the boundary by
`tests/spine/test_spine_architecture.py`.

**LAW 5 — One repo, one main.** Worktrees only (never new clones),
capped at 10 live, tracked in `.claude/WORKSTREAMS.md`. The
140-directory sprawl is archived and removed in Job #0.

**LAW 6 — The email contract is the engine.** The inbound RFQ email
and its attachments are the COMPLETE and ONLY specification of the
response. At ingest — once, not incrementally — the `EmailContract`
must resolve **every answer the response needs**: required forms, due
date, solicitation number, buyer and ship-to, delivery and packaging
instructions, every line item with qty / UOM / MFG#, and tax
jurisdiction. Nothing required is discovered later by heuristic, by
operator memory, or by a renderer default. Renderers iterate
`contract.required_forms`; the send-gate refuses when the rendered set
≠ the contract. The single thing the email does NOT carry is **price**
— and price is resolved **catalog-first** (catalog cost is the basis;
SCPRS is a ceiling, Amazon is reference, never cost). When the email is
ambiguous, ingest records the gap as a contract field and **blocks the
quote** — it never papers over it.

**READ THE WHOLE CONTRACT — EVERY TIME, NO EXCEPTIONS. (The hardest
rule in this file.)** Ingest MUST read the full email **body** AND
**every attachment**, in full. No skipping by file type, no capping at
N files, no flag that disables coverage, no "non-RFQ-shaped" heuristic
that silently drops a file. Every attachment ends in exactly one of two
states: **parsed into the contract**, or **explicitly classified as
non-RFQ with a recorded reason**. A file that produced neither is a
bug, not a default. The contract's own cross-references are **mandatory
reading orders**: when any parsed form points elsewhere — "see attached
distribution list", "supplemental", "schedule", "see attachment", or
names another form — ingest MUST locate and parse that target before
the contract is considered complete. Rendering or sending a response
whose own forms reference an attachment ingest never parsed is
forbidden and **BLOCKS the quote**. Incident 2026-05-28 (Coleman
10842771): the 704B row 4 read `***PLEASE SEE ATTACHED DISTRIBUTION
LIST`; the supplemental **AMS 701B** distribution list (21 facilities,
full addresses, per-facility qty) WAS in the contract, but ingest
mis-modeled its 21 rows as 21 phantom line items and **discarded the
facility / address / zip columns** — yielding a single-facility ship-to
and single-jurisdiction tax on a 21-jurisdiction order. The north star
carried the answer; the parser threw the columns away. **Teeth
(non-negotiable):** this law is enforced by a forcing function — ingest
records a per-attachment disposition manifest, and the send-gate +
`test_ingest_reads_all_attachments` FAIL THE BUILD when (a) any
attachment has no recorded disposition, or (b) a parsed form carries a
cross-reference whose target was never parsed. A docs-only restatement
of this rule without that forcing function does NOT satisfy it (see the
"3 months" history under the Prime Directive).

Every agency migration (Job #1
onward) must prove its `EmailContract` carries that agency's full
requirement set before that agency's legacy path is deleted.

**LAW 7 — §0 changes only by PR; verbal directives MOTIVATE a PR,
they don't BECOME one.** §0 is the governance substrate. Any change
to it — any LAW, any Job, any clause — is a PR diff, code-reviewed,
visible in `git log`. A directive from Mike spoken in chat, captured
in a memory file, or relayed through a handoff is the *motivation*
for a §0 PR; it is **not** itself a §0 change. Two parallel windows
reading §0 differently because one captured a verbal "kill Spine" in
memory while the other read the file and saw `LAW 1 — Spine
canonical` is the exact failure mode this rule closes (2026-05-25:
the substrate-wave handoff acted on a verbal-only "§0 reversal"
while the Spine-save-button-fix handoff read the file and built the
opposite plan — same operator, same day, opposite directions). When
a verbal directive conflicts with §0 as written, the file wins until
a PR amends it. If you find yourself about to act on a §0 change that
isn't in `git log` yet, stop and open the PR first.

### The pack — four roles, mission-scoped

Definitions in `.claude/agents/`. The pack exists to converge the
substrate and migrate every agency with deletion — then it shrinks.
It is not a standing committee.

- **Architect** (`architect.md`) — singular. Sole authorizer of
  substrate/schema/migration. Owns convergence. Only role that
  dispatches the pack.
- **Implementer** (`implementer.md`) — 2-3 instances. Scoped tickets,
  worktree + branch each. Forbidden: `model.py`, schema, new
  `src/spine/` modules, new substrates.
- **Inspector** (`inspector.md`) — owns BOTH gates: the Chrome
  walkthrough AND the math reconciliation (every quote's
  subtotal/tax/total/cost-basis verified against source before
  render). No Edit/Write tools — verifies and reports, cannot "fix it."
  Veto: nothing ships without sign-off.
- **Closer** (`closer.md`) — the "no." Kills scope, enforces
  delete-before-add, co-authorizes new substrates. No Edit/Write tools.

### The mandatory reporting block

Every pack task ends its final message with EXACTLY this block:

```
WOLFPACK REPORT
- Task: <ticket id + one line>
- Files changed: <count>  | LOC +<n>/-<n>
- Legacy deleted: <yes: path(s) | no — why>
- Convergence: writers <before>→<after> | substrates <b>→<a> | dirs <b>→<a>
- Tests: <suite> <pass/fail counts>
- Inspector: <walkthrough verdict + math-reconcile verdict | N/A>
- Charter/schema touched: <yes — Architect approval ref | no>
```

On a migration ticket, a report that cannot fill "Legacy deleted" with
a path is not complete.

### Job #0 — Collapse the den  (owner: Architect | due 2026-05-28)

You cannot run a disciplined migration from 140 directories.
1. Designate this repo (`rfq-spine-sequential-numbering`) the canonical
   checkout. One main.
2. Prune merged/dead branches (1,665 → live work only). `git worktree
   prune` + `make worktree-remove` for stale worktrees.
3. Archive the 140 `rfq-*` dirs: no unmerged commits → delete; unmerged
   work → branch it, then delete the directory.
4. Cap live worktrees at 10, tracked in `WORKSTREAMS.md`.
5. Build the LAW 3 ratchet: measure the three counts, commit
   `tests/spine/convergence_baseline.json` + a test in
   `test_spine_architecture.py` asserting current ≤ baseline.
**Acceptance:** branch count < 50, worktrees ≤ 10, baseline committed,
ratchet test green.

### Job #1 — CCHCS migration, with deletion  (owner: Architect | due 2026-06-18)

Make the Spine the ONLY CCHCS quote path, then delete the legacy one.

The Spine must render BOTH CCHCS response formats: the Non-Cloud Packet
(the buyer's single bundled PDF — already covered by `packet_render.py`)
AND the standalone set — AMS 703A, 703B, *or* 703C, AMS 704B, the CDCR
Bid Package, plus the Reytech Quote (the common format, not yet covered).
Which 703 revision and which format applies is declared by the email
contract (LAW 6) — never guessed. (703A added 2026-05-27 after Coleman
solicitation 10842771 surfaced the Rev. 03/2025 form. 703B is the prior
revision; 703C is the IT-RFQ variant.)

**Acceptance — all required:**
- 0 imports from `src/core/` in the CCHCS quote path (extend
  `test_no_legacy_imports` to cover the CCHCS routes).
- CCHCS routes to ZERO legacy quote-write paths — provable by code path.
  The numeric LAW 3 ratchet (writers 9→8, substrates 3→2) is DEFERRED to
  the CalVet/DSH/DGS migrations: the 8 legacy writers and the shared
  files `data_layer.py` / `quote_generator.py` survive Job #1. (The
  Architect's 2026-05-21 Job #1 plan established this — do not re-litigate.)
- The `AGENCY_CONFIGS["CCHCS"]` entry (`src/forms/quote_generator.py`)
  and the `"cchcs"` entry (`src/core/agency_config.py`) DELETED — commit
  in `git log`. (There is no literal "CCHCS branch" in
  `src/core/quote_contract.py`; that file is agency-agnostic — the
  original wording was corrected by the 2026-05-21 plan.)
- Legacy CCHCS quote/package routes DELETED — commit in `git log`.
- The retired CCHCS adapter shims under `src/spine/agency_forms/`
  (the `cchcs_*.py` files) DELETED. **Correction (2026-05-30, LAW 7 —
  Architect+Closer trace of Job #1):** the earlier wording — "the
  retired `src/spine/agency_forms/` renderers DELETED … just remove
  them" — was factually wrong about the tree and would have deleted
  LIVE code. The directory's remaining modules (`std_204.py`,
  `dvbe_843.py`, `darfur.py`, `calrecycle_74.py`, `std_1000.py`,
  `cuf.py`, `_identity.py`, `_template_resolver.py`, and `FORM_REGISTRY`
  in `__init__.py`) are LOAD-BEARING — imported by
  `src/spine/forms_render.py` and consumed by `routes_spine.py`'s
  per-form route surface — and are NOT deleted in Job #1. The only
  retired set was the `cchcs_*.py` adapter shims, which were ALREADY
  removed (they now point at `forms_render.py`; see
  `agency_forms/__init__.py`). This acceptance item is therefore
  **already satisfied** and requires no further deletion — do NOT
  "just remove" the directory.
- 3 consecutive CCHCS quotes shipped through the Spine — covering BOTH
  formats — each with a clean Inspector report (walkthrough + math
  reconcile).

Only when ALL are true does CalVet begin. Same pattern. Same gate.

### Pack checkpoint — 2026-06-20

If, by 2026-06-20, the substrate count and the tracked-directory count
have not DROPPED from the 2026-05-21 baseline, the pack model has
failed. Stop, do not start CalVet, re-assess with Mike. The pack is
not exempt from its own completion gate.

### How §0 relates to the rest of this file

The Prime Directive and the guard rails below remain in force **for the
legacy path** until Job #1 deletes it. Where §0 and a rule below
conflict, §0 wins, and the conflicting rule is rewritten or deleted in
the same PR — the governance does not get to have two sources of truth
either.

---

## Prime Directive (READ BEFORE ANY QUOTE-GEN EDIT)

> ⚠️ **CONTESTED — see §0.** This directive names `QuoteContract` as
> canonical. `src/spine/SPINE_CHARTER.md` names `src/spine/model.py` as
> canonical for CCHCS. **Two canonical sources coexisting is the
> disease itself.** Until Job #1 (§0) deletes the legacy CCHCS path,
> this directive governs the **legacy path only**; the Spine governs
> CCHCS. Job #1 ends this contradiction by deletion, not by edit.

The canonical source of truth for any Quote PDF / Package PDF / form
fill is `src/core/quote_contract.py` :: `QuoteContract`. It is FROZEN.
Renderers receive a contract and read from its frozen fields. Renderers
do NOT call `tax_resolver` / `agency_config` / `facility_registry`
directly — that's how the 7 recurring fix patterns started.

If your PR adds a new `rfq_data.get(...)` read inside a renderer module
(`src/forms/`, `src/api/modules/routes_rfq_gen.py`), it MUST be wired
through QuoteContract first. The architecture-contract test will fail
your PR if you take a shortcut. Do not add to the allowlist without
explicit approval from Mike.

History: this rule was added 2026-04-24 (commit @PR#501). It was not
durably enforced. Mike's "I've been saying the same thing for 3 months"
is a direct quote about the consequence. Finish the migration. Do not
add new consumers outside the contract.

## Multi-Window Development Protocol (MANDATORY)

Multiple Claude Code sessions run in parallel. Follow this protocol to avoid conflicts.

### Worktrees Are Required for Parallel Windows

**Branches alone do not isolate parallel Claude windows.** A `git checkout` in one
window silently overwrites the other window's uncommitted edits on disk, because
both windows share the same working tree at `C:\Users\mikeg\Reytech-RFQ`. Symptoms:
edits "revert themselves" between writes, `make ship` tests a mix of both windows'
files, pre-push hooks pass/fail non-deterministically.

**Rule:** If a second Claude window will be active on this repo at the same time
as yours, one of you MUST work from a separate git worktree. Use the Makefile:

```bash
make worktree name=feat/my-topic       # creates ../rfq-my-topic on feat/my-topic from latest main
cd ../rfq-my-topic                     # launch Claude here — isolated working tree, shared .git
# <do work, commit, make ship as normal>
make worktree-remove name=feat/my-topic  # after merge/abandon
make worktree-list                      # see all active worktrees
```

**Skip worktrees only when:**
- Only one Claude window is active on this repo (no collision risk).
- The edit is a tiny one-off you will finish in under 2 minutes.
- Two tasks both need to edit the same conflict-zone file (see WORKSTREAMS.md) —
  worktrees solve filesystem collisions, not logical merge conflicts. Sequence
  the work instead.

**Update `.claude/WORKSTREAMS.md`** with the `Worktree` column so every window
knows which directory it owns. Worktrees are **additive** to branch protection
and WORKSTREAMS.md coordination — not a replacement.

### Before Starting Any Work
1. **Read `.claude/WORKSTREAMS.md`** — check what branches are active and what files they touch
2. **Never work directly on `main`.** Always create a feature branch: `make branch name=feat/description`
3. If your work overlaps with an active branch in WORKSTREAMS.md, coordinate — don't create a parallel branch that touches the same files

### Branch Naming
- `feat/description` — new feature
- `fix/description` — bug fix
- `refactor/description` — code improvement
- `hotfix/description` — urgent production fix

### The Ship Cycle
```
make branch name=feat/my-feature   # 1. Create branch from latest main
# <do work, commit normally>
make ship                          # 2. Run tests + push + create PR
# <CI runs automatically>
make promote                       # 3. Merge PR + smoke test production
```

### Rules
- **`make ship` is the ONLY way to push code.** It enforces test + check gates.
- **Never `git push origin main` directly.** Branch protection blocks this.
- **Update `.claude/WORKSTREAMS.md`** when you create, merge, or abandon a branch.
- **Check `make status`** to see active PRs and recent CI runs.
- **Conflict zones** (files edited by many windows): see WORKSTREAMS.md. If two windows need the same file, one finishes first.

### Emergency: Production Is Broken
```
git checkout main && git pull
make rollback                      # Reverts last commit, pushes, redeploys
make smoke                         # Verify production recovered
```

## System Context

**What this is:** End-to-end RFQ automation + business intelligence for Reytech Inc., a California SB/DVBE government reseller. 90K+ lines, 955 routes, 50 templates, deployed on Railway.

**Stack:** Python 3.12 / Flask / SQLite (WAL mode) / Jinja2 / Gunicorn. No frontend framework — all server-rendered HTML with inline JS.

**Deploy:** Feature branch → PR → CI passes → merge to `main` → Railway auto-deploys → smoke test. Persistent volume at `/data`. Domain: `web-production-dcee9.up.railway.app`. Use `make ship` / `make promote` — never push main directly.

**Module loading:** Route modules in `src/api/modules/` are loaded via `exec()` into `dashboard.py` namespace. This means all modules share globals. Be aware of name collisions.

## Workflow Orchestration

### 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately — don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Verification Before Done
- Never mark a task complete without proving it works
- **Always compile-check** Python: `python -c "import py_compile; py_compile.compile('file.py', doraise=True)"`
- **Always render-test** templates with all required variables after changes
- Test with realistic data structures — production data may differ from dev assumptions
- Ask yourself: "Would a staff engineer approve this?"

### 3. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests — then resolve them
- Trace the full call chain: route → function → template → data structure
- Check for type mismatches (dict vs list, missing keys, None values)

### 4. Three Strikes Rule — Stop Fixing, Start Diagnosing
If **3 consecutive fix attempts for the same issue** fail (compile error, wrong output,
new bug introduced), you MUST:
1. **STOP coding immediately.** Do not attempt a 4th fix.
2. **Tell the user:** "This fix is not converging. I've failed 3 times and the root cause
   is likely deeper than what I'm patching. I recommend starting a fresh session to
   audit this properly."
3. **Revert to the last known working state** (git stash or revert) so the user isn't
   left with broken code.
4. **Write a handoff note** explaining: what was attempted, what broke each time, and
   what the likely root cause is. Save it so the next session (or a fresh agent) can
   pick up without repeating the same mistakes.

**Why this matters:** The 2026-04-03 multi-page 704 incident had 11 consecutive failed
fix commits because each one patched a symptom without diagnosing the shared root cause
(hardcoded 8 rows vs actual 11). A fresh session with full audit found 7 bugs and fixed
all of them in one change. Incremental patching of multi-bug problems makes things worse.

**Signs you're in a fix-forward spiral:**
- Each "fix" creates a NEW bug you didn't expect
- You're changing coordinates, constants, or thresholds by trial and error
- You're adding flags like `pricing_only=True` to work around your own recent code
- The diff is growing past 200 lines with no test passing yet

### 5. Audit Before Fix — Find ALL Bugs First
For any bug that touches PDF generation, form filling, or multi-page logic:
1. **Read the actual template/data first** — run pdfplumber, dump field names, count rows.
   Never assume structure from code comments or variable names.
2. **List ALL bugs before fixing ANY.** A single root cause often manifests as 3-7 symptoms.
   If you fix symptom #1 without knowing about #2-#7, your fix will break something else.
3. **Write automated tests for each scenario BEFORE pushing.** At minimum: boundary cases
   (exactly N items, N+1 items, 2N items where N is a page capacity).

### 6. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- Skip this for simple, obvious fixes — don't over-engineer
- Challenge your own work before presenting it

## Code Patterns

### Route Pattern
```python
@bp.route("/api/example", methods=["POST"])
@auth_required
def api_example():
    """Docstring with purpose."""
    try:
        # business logic
        return jsonify({"ok": True, "data": result})
    except Exception as e:
        log.error("Example error: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": str(e)}), 500
```

### Template Variable Safety
Always use `|default()` for any variable that might not exist:
```jinja2
{{ value|default(0) }}
{{ obj.key|default('fallback') }}
{% for item in items|default([]) %}
```

### Defensive Data Loading
```python
try:
    data = some_function()
    if not isinstance(data, dict):
        data = {}
except Exception as e:
    log.error("Load error: %s", e)
    data = {}
data.setdefault("required_key", default_value)
```

### Growth Agent Functions
All in `src/agents/growth_agent.py` (104 functions). Key patterns:
- `_load_json(path)` / `_save_json(path, data)` for all JSON file I/O
- `_load_prospects_list()` returns list of prospect dicts
- Status dicts (`PULL_STATUS`, `BUYER_STATUS`, `INTEL_STATUS`) for long-running ops
- Thread-based async for SCPRS scraping — poll status endpoints for progress

## Known Issues (Production Audit — last audited 2026-03-23)

### Resolved
- **SQL Injection (was Critical):** All f-string SQL instances audited — all interpolate
  hardcoded constants, table names from allowlists, or dynamic `LIKE ?` placeholder
  counts. No user input reaches SQL strings. Not injection vectors.
- **Bare `except:` clauses:** All 5 replaced with specific exception types (0 remaining).
- **Duplicate routes:** `/api/pc/convert-to-rfq` and `/api/pricecheck/download` duplicates
  removed. Kept the more thorough implementations.
- **Orphaned templates:** 4 dead templates removed (expand, growth_intel, growth, crm).

### Warning — Unprotected Routes
13 routes lack `@auth_required`. Most are intentional (health check, webhooks, email
tracking pixels). Monitor for new unprotected admin routes.

### Info — Code Quality
- 2 TODO comments remaining (QB line-item search, RFQ Undefined values)
- 230 POST endpoints rely on session auth only (no explicit CSRF tokens)

## File Layout Rules

- **Routes:** `src/api/modules/routes_*.py` — one file per domain area
- **Agents:** `src/agents/*.py` — one file per external integration or intelligence engine
- **Templates:** `src/templates/*.html` — extends `base.html`, uses `render_page()`
- **Data:** `data/*.json` and `data/*.db` — persisted on Railway volume
- **Forms:** `src/forms/*.py` — PDF generation and form filling

## Testing Checklist

Before pushing any change:
1. `python -c "py_compile.compile('changed_file.py', doraise=True)"` for each modified Python file
2. If template changed: render test with all required variables (check for `UndefinedError`)
3. If route changed: verify `@auth_required` decorator is present
4. If data structure changed: check all templates that consume it for type assumptions
5. `git diff --stat` to verify only intended files are modified
6. **AUDIT EVERY FILE IN THE COMMIT.** `git add` sweeps in dirty working tree files.
   Run `git show --stat HEAD` AFTER committing to verify no unintended files snuck in.
   For each unintended file: read the diff, verify it's safe, or revert it.
   Incident 2026-04-10: 7 agent files committed alongside a DOCX fix — one had
   Haiku+thinking (unsupported = 400 errors on every call) that shipped to production.
7. **Run the test sandbox BEFORE pushing.** The pre-push hook blocks pushes with
   failing tests, but run them proactively so you fix issues before committing.

## Test Sandbox (MANDATORY — Built 2026-04-10)

A pytest-based test sandbox exists. Push to `main` auto-deploys to production.
The pre-push git hook (`.githooks/pre-push`) blocks pushes when tests fail.

### Running Tests
```bash
# Full sandbox suite (146 tests, ~90 seconds):
python -m pytest tests/test_ams704_helpers.py tests/test_template_registry.py tests/test_pc_generation.py tests/test_rfq_generation.py tests/test_multipage_704.py tests/test_golden_path.py -v --tb=short

# By area — run the relevant subset:
# Price Check / 704 fill:
python -m pytest tests/test_ams704_helpers.py tests/test_pc_generation.py tests/test_multipage_704.py -v
# Template / PDF introspection:
python -m pytest tests/test_template_registry.py -v
# RFQ routes:
python -m pytest tests/test_rfq_generation.py -v
# Golden path (E2E pricing + email + metrics):
python -m pytest tests/test_golden_path.py -v
# Order lifecycle + pricing pipeline + V5 features:
python -m pytest tests/test_order_lifecycle.py tests/test_quote_counter.py -v
```

### Writing New Tests — Available Fixtures
All fixtures auto-isolate per test (temp DB, temp dirs, no cross-contamination).

**DB seeding** (creates real rows in isolated test DB):
- `seed_db_quote(quote_number, agency=, total=, ...)` — insert a quote
- `seed_db_contact(id, name, email, agency=, ...)` — insert a contact
- `seed_db_price_history(description, price, source=, ...)` — insert price record
- `seed_db_price_check(id, items=, ...)` — insert a price check

**External API mocks** (no real HTTP calls, prevent accidental prod hits):
- `mock_gmail` — `.set_messages([...])`, `.set_configured(bool)`
- `mock_vision_parser` — `.set_result({...})`, `.set_available(bool)`
- `mock_product_research` — `.set_search_results([...])`, `.set_product({...})`
- `mock_scprs` — `.set_price({...})`, `.set_bulk({...})`
- `mock_twilio` — `.sent` list captures all outbound SMS

**PDF assertion helpers** (imported from `tests.conftest`):
- `assert_pdf_fields(pdf_path, {"SUPPLIER NAME": "Reytech Inc."})` — verify field values
- `extract_pdf_text(pdf_path, page_num=)` — pdfplumber text extraction
- `get_pdf_field_names(pdf_path)` — list all form fields
- `get_pdf_page_count(pdf_path)` — page count

**Flask test clients** (auth auto-injected):
- `client` / `auth_client` — authenticated (Basic Auth on every request)
- `anon_client` — unauthenticated (for testing auth gates)

**Sample data factories:**
- `sample_pc`, `sample_pc_items`, `sample_rfq`, `sample_stryker_quote`
- `seed_pc`, `seed_rfq` — write samples to JSON files in temp data dir
- `blank_704_path` — path to blank AMS 704 template in fixtures
- `fixture_json(filename)` — load any JSON from `tests/fixtures/`

### Rules for Test Sandbox
1. **Every session must start with a green baseline.** Run the suite before coding.
2. **Every push must pass tests.** The pre-push hook enforces this automatically.
3. **New features MUST have tests.** If you touch `fill_ams704()`, add/update tests
   in `test_pc_generation.py`. If you add a route, add a test in the relevant file.
4. **Mock ALL external APIs.** Tests must work offline. Never call Gmail, Claude,
   SerpApi, SCPRS, or Twilio in tests. Use the mock fixtures.
5. **Test boundary cases for PDF generation:** 1, 8, 9, 16, 19, 20+ items.
   These are the page boundaries where bugs hide.
6. **Never skip the sandbox.** "It's just a small change" is how production breaks.
   The 2026-04-03 incident was "just" a constant change that caused 11 failed commits.

## Form Filling Guard Rails (CRITICAL — Production Incidents 2026-03-26)

### Package Generation
- **CCHCS package = 703B/C + 704B + Bid Package + Quote ONLY.** DVBE 843, seller's permit, CalRecycle are INSIDE the bid package. Never generate standalone.
- **Optional forms are OPTIONAL.** Never auto-include based on item count or heuristics. Only generate if user explicitly checks them.
- **703C vs 703B:** If buyer provides 703C template, use `fill_703c()`. Never include both.
- **Before changing `agency_config.py` required_forms:** Verify the form isn't already inside the bid package PDF.

### Signature Placement
- **Generic fields (Signature1, Signature):** Only sign if in the lower 40% of the page. Certification sigs are always at the bottom.
- **Never double-sign:** If PDF has `/Sig` form field, `fill_and_sign_pdf` handles it. `_703b_overlay_signature` only runs when NO `/Sig` field exists.
- **New forms:** Use form-specific field names in `SIGN_FIELDS` (e.g., `Signature_formname`), not generic names.

### Quote Counter
- **Stored counter is authoritative.** Scans of existing quotes NEVER override a manual set.
- **Max jump = 5.** Counter blocked if it tries to jump more than 5 from last known value.
- **No nested DB connections inside `BEGIN IMMEDIATE`.** Use single connection with direct SQL. Nested connections cause cascading locks (2+ minute hangs).
- **`set_quote_counter()` must update `quote_counter_last_good`.**

### PC → RFQ Workflow
- **704 (PC)** = market test. Buyer's descriptions unchanged. Only pricing added.
- **704B (RFQ)** = Reytech's response. Use catalog descriptions, proper MFG#, ASIN in description.
- **PC pricing is authoritative** for that quote. Catalog pricing may be older.
- **Match items by description** (should be near-identical), positional fallback.
- **Never import PC items into RFQ.** RFQ items from 704B are authoritative.
- **Catalog provides enrichment** (URLs, ASIN, supplier) but NOT pricing.
- **Cross-queue dedup:** If PC exists for an email, don't also create an RFQ.

### 703C Form Filling
- Read actual PDF field names before filling. Detect prefix (703B_, 703C_, or none).
- Log field names for debugging: `print(f"703C fields: {sorted(field_names)}")`

## UI Changes Must Ship With Chrome-MCP Proof (HARD RULE 2026-04-23)

**Incident that added this rule:** Bundle-5 + Bundle-6 shipped to prod
(PRs #449, #451) with pytest template-render assertions as the only UI
proof. `readonly` lock state, modal submit flow, recalc() live math,
responsive layout of the 6-cell KPI strip — none of it was exercised
in a real browser before merge.

### The rule
When your push touches any `src/templates/**/*.html`, `src/static/**/*.{js,css}`,
or any other `.html`/`.css`/`.js` file in the repo, the `pre-push` hook
refuses to push unless one of these is present:

1. **`CHROME_VERIFIED=1`** env var on the push:
   ```
   CHROME_VERIFIED=1 make ship auto=1
   ```

2. **`CHROME-VERIFIED:`** footer in at least one commit being pushed.
   This preserves the proof in git history forever.
   ```
   git commit --amend
   # add to the body:
   CHROME-VERIFIED: opened /rfq/<id>, clicked Mark Sent, filled modal,
   saw toast, reloaded, confirmed lock banner + readonly dim on inputs.
   ```

### What Chrome-MCP verification actually means
Not "the 200 response contains my data-testid". That's what the Flask
test client gives you. A real visual pass requires loading the page in
Chrome DevTools MCP and taking screenshots of every state Mike will see:

- **Empty state** — no rows, first render
- **Filled state** — realistic data, full content
- **Error state** — what the user sees when the request fails
- **Re-run / reload state** — second visit, idempotent behavior

Then exercise the interactive flows:
- **Modal open/submit/close** — does the dialog actually render + submit + dismiss?
- **Readonly locks** — can you still type into a supposedly-locked input?
- **Live math** — edit a price cell, verify the Subtotal/Tax/Total KPI updates
- **Responsive** — resize the viewport to 1280×720 and 1024×768; does the
  6-cell KPI strip wrap readably, or does the Status cell fall off?

### Escape hatch (RARE)
`CHROME_VERIFIED_SKIP=1 make ship` — only for chore/revert/docs pushes
that genuinely don't change user-visible behavior. Include the reason
in the commit message.

### Everything Operator-Visible Gets a Real Chrome Visual Check (2026-05-29)

**Mike's rule: "everything needs chrome visual checks."** A change that
touches any operator surface is not verified by pytest, an API/curl response,
or a Jinja render-test alone — load the actual page in Chrome-MCP, confirm the
change renders in every state Mike will see, and screenshot it. Do NOT reach
for `CHROME_VERIFIED_SKIP` when a Chrome path exists; the skip hatch is for
genuinely non-visual pushes only.

**When Chrome-MCP can't attach** (`"The browser is already running for
...chrome-devtools-mcp/chrome-profile"`), don't give up and skip — check who
owns the session and how stale it is, and **if idle > 5 minutes, take it over
via bash:**

1. **Owner:** `Get-CimInstance Win32_Process -Filter "Name='chrome.exe'"`,
   filter `CommandLine` for `chrome-devtools-mcp`.
2. **Staleness:** mtime of the most-recent file under
   `~/.cache/chrome-devtools-mcp/chrome-profile` (and/or `SingletonLock`).
   Age > 5 min ⇒ stale.
3. **Stale ⇒** `Stop-Process` the locking chrome.exe (remove `SingletonLock`
   if present), then retry `new_page`. **Active (< 5 min) ⇒ Mike is likely
   using it — do not kill; wait or ask.**

**Credential-free path (never leak prod secrets to Chrome):** prod is HTTP
Basic-Auth and the MCP profile isn't authed; embedding real `REYTECH_USER/PASS`
in a navigation URL is (correctly) blocked. Verify against the LOCAL dev
server instead — `unset PORT; SECRET_KEY=dev-only DASH_USER=reytech
DASH_PASS=changeme python app.py` (dev creds `reytech:changeme` are public
defaults — safe in `http://reytech:changeme@localhost:5000/`). **Gotcha:
setting `PORT` trips shared.py's production guard ("DASH_PASS must be set in
production") — `make run` omits PORT on purpose; app.py defaults to 5000.**

### Related memories
- `feedback_workflow_ui_chrome_verify` — the original principle
- `feedback_visual_verify_always` — PDFs must be verified in real Chrome too
- `feedback_production_ready_definition` — displayed == persisted == delivered

## JavaScript Guard Rails (CRITICAL — Production Incidents 2026-03-31)

### DOM Access Must Be Null-Safe
Every `document.getElementById()` or `querySelector()` call in inline JS MUST
use null checks. The exec() module loading means elements may not exist on all
page variants (manual PC vs parsed PC vs RFQ).
```javascript
// WRONG — kills autosave silently if element missing:
data['tax_enabled'] = document.getElementById('taxToggle').checked;

// RIGHT:
var el = document.getElementById('taxToggle');
data['tax_enabled'] = el ? el.checked : false;
```

### Autosave Must Never Die Silently
- Wrap `collectPrices()` in try-catch inside `doPcAutosave()`
- Log errors to console so they're visible in DevTools
- Never let a single failed save kill the autosave timer
- The autosave timer re-triggers on next user input (change/input events)

### Inline Event Handlers in innerHTML Are Fragile
Never use complex JS in `onkeydown="..."` inside dynamically inserted HTML.
Quote escaping breaks silently. Use `addEventListener` after DOM insertion:
```javascript
// WRONG — nested quotes break:
html += '<input onkeydown="if(event.key===\'Enter\'){...}">';

// RIGHT — attach after insertion:
element.innerHTML = html;
var input = document.getElementById('myInput');
if (input) input.addEventListener('keydown', function(e) { ... });
```

## Pricing Guard Rails (CRITICAL — Production Incidents 2026-03-31)

### SCPRS Prices Are NOT Supplier Costs
SCPRS prices are what the STATE paid another vendor. They are reference
ceilings for your bid price, NEVER your cost basis.
```python
# WRONG — uses SCPRS as cost:
unit_cost = p.get("unit_cost") or amazon_price or scprs_price or 0

# RIGHT — only real supplier costs:
unit_cost = (p.get("unit_cost") or p.get("catalog_cost")
             or p.get("web_cost") or item.get("vendor_cost") or 0)
```

### Amazon Prices Are NOT Supplier Costs
Amazon retail prices are reference data for comparison. Never use as your
wholesale cost. The app marks Amazon data with ASIN badges — informational only.

### Cost Sanity Guardrail (3x Rule)
If unit_cost is >3x the SCPRS or catalog reference price, it's almost certainly
a bad scrape (wrong product matched on Amazon). Auto-correct to the reference
price and show a warning badge.

### S&S Worldwide Pricing
- S&S is Cloudflare-blocked — cannot scrape prices directly
- ALWAYS keep the S&S URL (never override with Amazon link)
- Use LIST price (non-discount) as cost basis — discounts expire in 45-day window
- When price unavailable: show quick-entry field, not $0.00 silently

### Catalog Match Threshold
Token matching threshold = 0.65 (raised from 0.35 → 0.50 → 0.65 across two
audits because cross-category near-misses kept slipping through at 0.50).
Final output filter also follows this threshold.
Never lower it without testing cross-category accuracy.

## PDF Parsing Guard Rails (Production Incidents 2026-03-31)

### Multi-Page AMS 704 Forms — Ground Truth (verified 2026-04-03)
The Reytech blank template (`ams_704_blank.pdf`) has **exactly** this structure:
- **Page 1:** 11 unsuffixed row fields (Row1 through Row11)
- **Page 2:** 8 suffixed row fields (Row1_2 through Row8_2)
- **NO `_3` or `_4` suffix fields exist.** Pages 3+ have zero form fields.
- **Total form field capacity: 19 items** (11 + 8)
- Shared fields (`Page`, `of`, `SUPPLIER NAME`) show same value on ALL pages.

Row mapping in `fill_ams704()`:
- Items 1-11 → unsuffixed fields (Row1..Row11)
- Items 12-19 → `_2` suffix fields (Row1_2..Row8_2)
- Items 20+ → `_append_overflow_pages()` draws via reportlab canvas

**Use `_detect_pg1_rows()` to get the actual count** — some buyer PDFs have 8
rows on page 1 instead of 11. NEVER hardcode the row count.

**NEVER assume row count = 8.** This caused 11 failed commits on 2026-04-03.
Run `tests/test_multipage_704.py` after ANY change to `fill_ams704()`.

### MFG# Extraction Patterns
Must handle: `W12919` (single letter + digits), `FN4368` (2 letter + digits),
`NL304` (2 letter + digits), `16753` (pure 5+ digit codes after " - ").
The `_PN_PATTERNS` list in `price_check.py` covers all these.

### Never Merge Items With Their Own Line Number
If a PDF row has its own `item_number` (distinct line # on the form), NEVER
merge it as a continuation row — even if qty=1 and uom=EA.

### Re-Index After Merge
After continuation merges remove rows, re-index items sequentially (1, 2, 3...)
not the original PDF row numbers (1, 3, 5...).

### Re-Parse Clears Enrichment
When re-parsing from PDF, clear `enrichment_status` and `enrichment_summary`.
Old enrichment data doesn't apply to new item set.

## Agency & Institution Rules

### We Only Sell in CA
Every institution maps to a known CA agency. Default to CDCR (most common),
never "DEFAULT".

### Facility Resolution — One Source of Truth (Updated 2026-05-27)
`src/core/facility_registry.py` is the canonical source for every CA
facility (CDCR prisons, CalVet veterans homes, DSH hospitals). It owns
the address, zip, parent agency, and alias set for each facility, and
its `resolve(text)` refuses to silently guess on ambiguous input
(e.g. bare "Folsom" / "Lancaster" / shared zips return None).

`src/core/institution_resolver.py` is now a **thin facade** over
`facility_registry`. Its `resolve(name, email, ship_to)` is preserved
as a backwards-compatible public API that returns the legacy dict
shape (`{canonical, agency, facility_code, original, source}`) for
the grandfathered callers in `_INSTITUTION_RESOLVER_DIRECT_IMPORT_ALLOWLIST`
(see `tests/test_classify_agency_facade.py`). The 5 heuristic data
tables that previously duplicated facility data here —
`_CDCR_FACILITIES`, `_CALVET_FACILITIES`, `_DSH_FACILITIES`,
`_ADDRESS_FACILITIES`, `_ADDRESS_KEYWORDS` — have been DELETED
(LAW 2 deletion commit). Every facility lookup now goes through
`facility_registry.resolve()`.

What's still resolved locally inside `institution_resolver`:
  - Agency-alias map (`_AGENCY_ALIASES`) — agency-level only, no addresses
  - Email-domain → agency map (`_EMAIL_DOMAINS`)
  - Garbage-form-label filter (`_GARBAGE_NAMES`)
  - The 3-input fallback chain (raw name → ship_to → email)

For NEW code, prefer the `quote_contract` facades:
  - `quote_contract.canonical_name(text)` — label normalizer
  - `quote_contract.same_institution(a, b)` — match comparator
  - `quote_contract.classify_agency(name, email, ship_to)` — full chain
  - `quote_contract.ship_to_for_text(text)` — facility address resolver

### Institution Resolver Returns Lowercase
The resolver returns `"cchcs"`, `"cdcr"`, etc. UI expects `"CCHCS"`, `"CDCR"`.
Always normalize via `src/core/agency_display.agency_display(key)`.

## PC → RFQ Conversion (Updated 2026-03-31)

### Conversion = deepcopy, Not Field Remapping
PC → RFQ conversion is a `copy.deepcopy(pc)` + status change + audit log.
**No field-by-field remapping.** Same items, same prices, same data.
The old approach caused 4 bugs (empty MFG#, 0.00 bid price, "unknown" PC link,
empty subtotals) because field names differed between PC and RFQ schemas.

## Date/Time Rules

### All Dates Must Be PST/PDT
Server runs UTC (Railway). Use `_pst_now()` for any user-facing date:
- AMS 704 signature date
- Price Check expires date (45 days from PST today)
- Quote dates, due dates

### PDF Preview Must Be Inline
Use `?inline=1` query parameter on download URLs for iframe preview.
Without it, browser downloads the PDF instead of rendering it.

## Deploy Guard Rails (Production Incident 2026-04-16)

### Never Bust the Docker Layer Cache
`nixpacks.toml` controls the Railway build. The `[phases.setup]` section
(aptPkgs, nixPkgs) is cached as a Docker layer. **Changing it forces a
full re-download of all system packages.** libreoffice-writer alone is
~200MB — on a slow mirror day, that's a 40+ minute deploy.

**Rules:**
- **NEVER change `[phases.setup]` in `nixpacks.toml` without confirming
  the impact on Docker layer caching.** Read the comments in the file.
- **NEVER switch from `aptPkgs` to `cmds`** for package installation — it
  changes the Dockerfile structure and invalidates the entire apt cache layer.
- **If Ubuntu mirrors are broken** (transient `apt-get update` failures), wait
  and retry. Do NOT restructure nixpacks.toml to "fix" it — that makes it worse.
- **If you must add a new system package**, add it to the existing `aptPkgs` list.
  That preserves the cache for all previously-installed packages.
- **Test deploy changes in a Railway preview environment first**, not production.

Incident: On 2026-04-16, switching from `aptPkgs` to `cmds` in nixpacks.toml
to add apt-get retry logic busted the Docker cache. Combined with slow Ubuntu
mirrors (59kB/s), this caused 3 failed deploys and one 40-minute deploy.
The retry logic was correct but the cache-busting cost was not worth it.

### Deploy Time Budget
- **Normal deploy: 5-8 minutes** (Docker cache hit on apt layer, pip cache hit)
- **After nixpacks.toml change: 15-40 minutes** (full apt re-download)
- **If deploy exceeds 15 minutes**: check Railway dashboard for build progress.
  Likely a cache miss or slow mirror. Do NOT cancel and retry — that restarts
  the download from zero.

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Impact minimal code.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Changes should only touch what's necessary. Avoid introducing bugs.
- **Defensive Programming**: Every data access should handle None, wrong type, missing keys.
- **Production First**: This is a live business system. Every commit deploys automatically.
- **Never Die Silently**: Errors must be logged. Autosave must never stop. Data loss is unacceptable.
- **Prices Have Roles**: SCPRS = ceiling, Amazon = reference, Catalog = cost, S&S = cost.
- **Test With Real Numbers**: Before pushing ANY calculation change, manually verify: input × formula = expected output. "40.0% markup on $82.24 = ?" must equal $115.14, not $411.20. Compile-check is not enough.
- **Test All UI States**: Every new UI element must be tested in all states: empty, filled, error, re-run. If a field appears conditionally, test the condition being true AND false. Never mix.

## Build Quality Rules (Production Incidents 2026-04-03)

### Never Add Logging Without Checking Imports
Adding `log.info()` or `log.warning()` to a module that has NO `import logging`
will crash the entire function silently. Always check the top of the file for
`import logging` and `log = logging.getLogger(...)` BEFORE adding log calls.
Incident: `agency_config.py` had no logging import — `match_agency()` crashed
on every call, fell to CCHCS fallback, wrong agency forms generated.

### Never Reference Variables Across try/except Boundaries
If a variable is set inside a `try:` block, the `except:` block MUST also
set it. Otherwise `UnboundLocalError` crashes downstream code.
```python
# WRONG:
try:
    _key, _cfg = match_agency(r)
except:
    _key = "fallback"
    # _cfg is UNBOUND if match_agency failed!

# RIGHT:
try:
    _key, _cfg = match_agency(r)
except:
    _key = "fallback"
    _cfg = {"name": "Fallback", "required_forms": [...]}
```

### PDF Form Fields Are Shared Across Pages
PDF form fields with the same name (e.g., `Page`, `SUPPLIER NAME`) show
the SAME value on ALL pages. You CANNOT set different values per page.
For multi-page PDFs:
- Pages 1-2: Use form field fill (template has `_2` suffix fields)
- Pages 3+: Use reportlab overlay to draw ALL content (no `_3` fields exist)
- To remove an unused page: strip it from the source BEFORE filling, or
  use the overlay to mask content. Never try to "blank" shared fields.
- The `_fill_pdf_text_overlay` function draws independently per page.

### Measure Before Drawing — Never Guess PDF Coordinates
All PDF overlay coordinates MUST be measured from the actual template:
```python
# Use pdfplumber to measure:
import pdfplumber
pdf = pdfplumber.open("template.pdf")
edges = pdf.pages[1].edges  # horizontal/vertical lines
rects = pdf.pages[1].rects  # cell boundaries
# Convert: reportlab_y = page_height - pdfplumber_y
```
Never extrapolate row positions. Never assume row heights. The AMS 704
has different row heights on page 1 vs continuation pages. Measure both.
Incident: `PG1_ROWS` had 3 rows (from old DocuSign layout) but template
had 8 → `current_row` counter was off → all pages misaligned.

### Test Multi-Page PDFs With 1, 8, 9, 16, 17+ Items
The 704 form has page boundaries at 8 and 16 items:
- 1-8 items: 1 page (strip page 2)
- 9-16 items: 2 pages (form fields with `_2` suffix)
- 17-24 items: 3 pages (page 3 uses overlay, not form fields)
Test ALL three cases before pushing any 704 fill change.

### URL Sanitization Must Preserve Spaces
`re.sub(r'[^a-zA-Z0-9_-]', '', path)` strips spaces from directory names.
Output directories like "RFQ Elastic Bandage" become "RFQElasticBandage"
→ file not found → 404. Only block path traversal: `..`, `/`, `\`.

### Agency Config: Required Forms Always Win
The `_include(form_id)` function must check agency `required_forms` FIRST.
User `package_forms` overrides should NEVER block agency-required forms.
Stale `package_forms` from a previous agency match can silently disable
forms that the current agency requires.

### Oracle Prices Must Be Per-Unit
SCPRS stores line totals in `unit_price` fields. A 5-qty order at $20/ea
shows `unit_price = $100`. Always divide by quantity:
```python
per_unit = price / qty if qty > 1 else price
```
Apply this in ALL search functions: `_search_won_quotes`, `_search_po_lines`,
`_search_scprs_catalog`, `_search_winning_prices`.

### Amazon MSRP vs Sale Price
SerpApi returns `typical_price` (MSRP) and `price` (sale/current).
Always use MSRP as cost basis — it's the stable price. Log the sale price
separately for the discount profit calculator. Never quote from sale prices
that may expire.

### Gmail Handles Signatures
Never add an app-level email signature. Gmail auto-appends the configured
signature. Adding our own creates a double signature. Send plain text body
only — no HTML wrapping, no signature block.

### scrollIntoView Steals Focus
Never call `el.scrollIntoView()` from status messages, link lookups, or
background operations. It yanks the user away from their current position
in the table. Save `window.scrollY` before DOM updates and restore via
`requestAnimationFrame`.
