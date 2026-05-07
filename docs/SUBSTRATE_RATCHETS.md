# Substrate Ratchets

A **substrate ratchet** is a CI test that converts a recurring bug class
into an extinct failure class. Once written, the failure shape can never
re-enter the codebase: the build refuses any PR that introduces a new
instance, and (where applicable) refuses any PR that loosens the
baseline.

This file is the catalog. Each entry tells the next session: *the rule
is already enforced — don't try to write your own.* When a new bug class
recurs three times in a quarter, this is the playbook for closing it.

---

## When to write a new ratchet

You need a ratchet when **all four** are true:

1. **Recurrence.** The same shape of bug has surfaced ≥ 3 times.
   One instance is a bug; three is a class.
2. **Detectable in source.** A regex or AST scan can identify the bad
   shape without running the code.
3. **Mechanical fix.** "Apply pattern X" closes it — no business
   judgment required per instance.
4. **Worth more than the audit cost.** The bug class costs real
   operator time, real lost revenue, or real P0 escalations. A ratchet
   for cosmetic warnings is overhead, not leverage.

If only 1–3 are true, write the fix and a single regression test.
Reach for a ratchet when the *class* is the problem.

---

## Ratchet shapes (pick one)

### A. Frozen-baseline burndown (`KNOWN_VIOLATIONS`)
Best when: many existing instances, can't fix in one PR.

- Audit the codebase, find every instance, freeze it in a `frozenset`.
- CI fails if the live scan finds an instance not in the baseline (new
  violation) OR if the baseline lists an instance the scan no longer
  finds (fix shipped without removing the entry).
- Burn down in batches grouped by file. When the baseline hits zero,
  the test becomes a pure forward-direction guard.

**Reference implementation:** `tests/test_rmw_race_lint.py`.

### B. Shrink-only allowlist (countdown)
Best when: legacy code is allowed, new code is not.

- Maintain `_LEGACY_ALLOWLIST` of files/symbols permitted to use the
  forbidden pattern.
- CI fails if a non-allowlisted file matches the pattern. Adding to
  the allowlist requires a code change visible in the diff.
- A second test pins the allowlist *count* (`EXPECTED_LEGACY_COUNT`)
  so a PR that grows the list is loud in review.

**Reference implementation:** `tests/test_architecture_contract.py`.

### C. Hard ban on new instances (diff-scoped)
Best when: legacy is grandfathered but new code can never add the shape.

- CI scans only lines added since `origin/main`.
- Any new line matching the forbidden regex fails.
- Mirror the pre-push hook so the CI catches what a force-push would
  bypass.

**Reference implementation:** `tests/test_canonical_lint_pr6.py`.

### D. Equality / floor (count-based)
Best when: the bug shape is "something dropped silently."

- Baseline a count (route count, module list, fixture count).
- CI fails if the count drops below floor (silent regression) or if
  set membership diverges (drift).

**Reference implementations:** `tests/test_route_module_registration.py`
(equality), `tests/test_golden_path.py::test_url_map_size_floor` (floor).

### E. Cross-source parity
Best when: two files must stay in lockstep.

- CI fails if file A has a token that file B doesn't (or vice versa).
- Used when a hook + a CI test must both know the same forbidden list.

**Reference implementation:**
`tests/test_canonical_identity_precommit_hook.py`.

---

## Active ratchets in this repo

| Ratchet | Bug class closed | Shape | Status |
|---------|------------------|-------|--------|
| `tests/test_rmw_race_lint.py` | Load → mutate → save without lock (autosave races, lost operator work) | A — frozen baseline burndown | **Burndown complete: 95 → 0.** Forward-direction guard active. Shipped 2026-05-06 PRs #784 #786 #788–#794. |
| `tests/test_architecture_contract.py` | Renderers/agents importing canonical resolvers directly instead of receiving a `QuoteContract` parameter | B — shrink-only allowlist with `EXPECTED_LEGACY_COUNT` countdown | Active. Net countdown 33 → 8 (8 remaining are canonical core modules — intentionally authorized). |
| `tests/test_canonical_lint_pr6.py` | Inline `WHERE status IN (...)` / `WHERE created_at >= ...` filters bypassing `src.core.canonical_state` predicates and views | C — hard ban on new lines added since origin/main, mirrors pre-push hook | Active. Hard ban — no grandfather list. PR #696. |
| `tests/test_route_module_registration.py` | New `routes_*.py` file dropped into `src/api/modules/` without being added to `_ROUTE_MODULES` (silent inert routes) | D — set equality between disk and dispatch list | Active. PR #532. |
| `tests/test_golden_path.py::test_url_map_size_floor` | Silent module load failure that strips routes from the URL map | D — count floor (1,200 floor against ~1,223 baseline) | Active. PR #532. |
| `tests/test_canonical_identity_precommit_hook.py` | Pre-commit hook drifting out of sync with the CI sweep test's forbidden-token list | E — cross-source parity (hook tokens ⊇ sweep tokens) | Active. |

---

## How to write a new ratchet (worked checklist)

1. **Name the bug class in one sentence.** "Handler does load → mutate
   → save without a save lock." If you can't fit it in one sentence, it
   isn't one class.
2. **Audit.** Write a scan (regex over source, or AST visitor) that
   finds every instance. Sanity-check the count matches the rough size
   you expected. False positives at this stage are fine — they get
   filtered by `KNOWN_EXEMPTIONS`.
3. **Pick a shape** from the list above. Most new ratchets are A or C.
4. **Write the test file** at `tests/test_<topic>_lint.py`. Include:
   - Module docstring naming the bug class, why it matters, and the
     PR/incident that motivated the ratchet.
   - The frozen baseline / allowlist / floor as a top-level constant.
   - A short comment explaining how to fix a violation (e.g., "wrap
     load+mutate+save in `with _save_pcs_lock:`").
   - The forward-direction test (no new violations).
   - Where applicable, the symmetric test (no fix shipped without
     updating the baseline).
5. **Burn down (shape A only).** Group violations by file, ship one
   PR per file with mechanical fixes. Don't fan out into a single
   100-handler PR — unreviewable. Each PR decrements the baseline.
6. **Update this catalog.** Add a row to the "Active ratchets" table.

---

## Anti-patterns

- **Don't write a ratchet for cosmetic style.** "Lines should be < 100
  chars" is a linter, not a ratchet. Ratchets close *failure classes
  with operational cost*.
- **Don't write a ratchet for a single bug.** Write a regression test.
- **Don't loosen a ratchet to make CI green.** If the lint flags
  something, either fix it or write a `KNOWN_EXEMPTIONS` entry with a
  one-line justification (and only when the helper genuinely manages
  its own atomicity / handles the invariant differently). Loose
  exemptions hollow the ratchet out.
- **Don't write a ratchet without a measured baseline.** The audit tells
  you whether the class is real. If the audit returns 1–2 hits, it's
  not a class yet.

---

## Candidate ratchets (open ideas, not yet written)

These are bug shapes that have surfaced but haven't met all four "when
to write" criteria yet. Listed so the next session has a head start
when one of them recurs.

- **Status-string drift.** `status="sent"` set on `rfqs` but `quotes`
  table stays at `"draft"` (or vice versa). Cross-table status
  divergence has surfaced ≥ 2 times (project_session_2026_05_06_overnight,
  feedback_pc_items_line_items_alias_drift). One more recurrence and
  this is a ratchet — likely shape C (hard ban on new direct status
  writes outside `src/core/canonical_state`).
- **Alias divergence on dict mutations.** `pc.items` (2) vs
  `pc.line_items` (1) silently diverged because writers updated only
  one alias (memory: `feedback_pc_items_line_items_alias_drift`,
  2026-05-05 23:10Z, pc_177b18e6). Shape A — find every direct
  `pc["items"] =` / `pc["line_items"] =` write that doesn't go through
  a sync helper, baseline them, burn down.
- **Unsafe DOM access in inline JS** (CLAUDE.md JavaScript Guard
  Rails). `document.getElementById(...).<prop>` without null-check
  silently kills autosave. Shape A — regex scan templates for
  unguarded chains.
- **Auto-source overrides operator data.** Already pinned by two
  point-tests (`tests/test_enrichment_respects_operator_cost.py`,
  `tests/test_enrich_step4b_no_link_overwrite.py`) but not yet a
  general ratchet. If a third instance surfaces, write shape C: any
  code path that fills `cost` / `price` / `markup` / `item_link` must
  gate on `if not <field>:`, never on confidence comparisons.

- **Classifier predicates need both positive AND negative markers.**
  Mike P0 2026-05-06 RFQ a5b09b56: `_is_cchcs_it_rfq` page-1 marker
  list contained `"Request For Quotation"` — too generic, matched
  every standard CCHCS 703B header. Filename token `"rfq "` matched
  every RFQ filename including NON-IT 703Bs. Result: standard
  NON-IT 703B Rev 03/2025 was mis-routed to the LPA IT filler →
  blank Bidder Information section → operator hand-fill at deadline.
  Closed in PR #798 by removing the generic positive marker AND
  adding negative markers (`"NON-IT GOODS"` page-1 text disqualifier,
  `"non-it"` filename disqualifier).
  **Doctrine**: every classifier predicate that fires on a generic
  positive marker MUST have a corresponding negative marker for the
  templates that should NOT match. Shape A — when a 3rd
  classifier-marker bug surfaces, this becomes a ratcheting lint:
  scan `src/forms/**/*.py` for `_PAGE1_MARKERS` / `_FILENAME_MARKERS`
  / `_MARKERS` / `*_signal` predicates and require each module to
  also declare a `*_NEGATIVE_MARKERS` set (even if empty) — forces
  the author to think about disqualifiers.

- **Form row capacity invisible to operator.** Mike P0 2026-05-06
  RFQ a5b09b56: CalRecycle 74 form has 6 rows; quote had 8 items;
  items 7-8 silently dropped with only a server-log warning the
  operator never sees. Mike has had 37-item quotes — every form
  whose row capacity is exceeded silently drops on every quote.
  Closed in PR #801 with `src/forms/form_capacity.py` registry +
  pre-fill `check_required_forms` + completeness-gate merge so
  capacity overflow becomes a first-class QA blocker. **Future
  ratchet**: a lint that forbids hardcoded `range(1, N+1)` row
  loops outside the registry — every row-iterating filler must
  look up `FORM_CAPACITY[form_id]` instead. Shape A. Hold until
  3rd instance.

- **Address-construction by string concat.** Mike P0 2026-05-06
  RFQ a5b09b56: quote PDF Ship-to clipped past margin because
  institution + street were jammed into a single ` - ` line and
  city/state/zip arrived as separate single-token lines (4 lines
  for one address). Closed in PR #800 by routing through
  `src/core/address_format.format_address_canonical`. Audit list
  in `project_address_canonical_format_2026_05_07.md` enumerates
  7 more callers that still build addresses inline. **Future
  ratchet**: when 3 callers have been migrated, write a lint
  that forbids `" - ".join([institution, street])` and similar
  string-concat patterns outside `src/core/address_format.py`.
  Shape A.

---

## Operating cadence

Ratchet health is checked weekly by a scheduled agent (Option A from
the 2026-05-06 substrate session conversation). The agent reports:

- Did any ratchet baseline grow (regression)?
- Were any `KNOWN_EXEMPTIONS` entries added in the last 7 days?
- Did any race-class / autosave-class incidents surface in PRs or
  closed issues?

If everything is green, the agent reports a single line. If anything
moved, the agent flags it for human review. The agent does not
modify the codebase.
