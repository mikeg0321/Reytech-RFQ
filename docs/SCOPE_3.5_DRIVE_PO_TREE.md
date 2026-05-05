# Phase 3.5 — Drive PO Archive Tree Divergence

Scope doc for Mike's design call. Built 2026-05-06 from `project_session_2026_04_28_drive_audit_findings` + verification against current code.

---

## TL;DR

Two parallel folder trees hold PO archives in Drive. The app reads/writes only one. Mike uses the other. Result: `find_po_folder` returns None for ~89 of 90 archived POs.

**Decision needed:** which tree wins? Three options below.

---

## Current state (verified 2026-05-06)

### App-side write (automated)
`drive_triggers.on_po_received` (`src/agents/drive_triggers.py:161-187`) enqueues a `create_po_folder` action with this layout:

```
{year}/{quarter}/PO-{po_number}/
```

- Year = bare (e.g. `2026`)
- Quarter = `Q1`–`Q4`
- PO folder = `PO-` prefix prepended

### App-side read
`find_po_folder` (`src/core/gdrive.py:318-334`) searches the **same** layout:

```python
year_id    = find_folder(year, ROOT)               # "2026"
quarter_id = find_folder(quarter, year_id)          # "Q3"
po_id      = find_folder(f"PO-{po_number}", quarter_id)
```

### Drive ground truth (per audit 2026-04-28)

```
ROOT/
├── 2023 - Purchase orders        ← legacy (lower-case "orders")
├── 2024 - Purchase Orders        ← legacy, 51 POs
├── 2025 - Purchase Orders        ← legacy, 38 POs
├── 2026/                         ← app-created (only 1 PO ever)
│   └── Q2/
│       └── 8955-0000076737       ← bare PO, no `PO-` prefix even though
│                                    on_po_received writes `PO-` prefix.
│                                    Either the trigger never fired here
│                                    or it was renamed manually.
├── 2026 - Purchase Orders        ← legacy, 1 PO so far
├── Archive
├── Backups
└── Supplier_Quotes
```

**90 POs total in legacy tree. 1 PO in app tree. 96 prod orders with `po_number`.**
~94% archive coverage in **the legacy tree alone**, which `find_po_folder` doesn't search.

### Folder-name variants observed in legacy tree

- Bare CCHCS: `4500750017`
- Bare CalVet: `8955-0000063707`
- **Trailing space:** `8955-0000071826 ` (Drive preserves trailing whitespace)
- **`PO ` (space) prefix:** `PO 4500736218`
- **`PO-` (dash) prefix:** *not observed in any legacy folder*

So the app's hardcoded `PO-` prefix is **incompatible with how Mike actually files POs**.

---

## Why this matters

- **Audit/find rate:** `/api/admin/po-drive-audit` returned 96/96 → no_folder on first prod run because it searches the wrong tree with the wrong name pattern.
- **Drive triggers:** `on_po_received` keeps writing to a tree Mike doesn't look at. Future automation (PO-receipt confirmations, supplier doc uploads) would land in the wrong place.
- **FAR compliance:** every Drive write is audited. If an auditor asks "where's PO 8955-0000063707?", we'd say "it's archived" but `find_po_folder` would return None, our endpoint would fail to surface it. Confidence-eroding.

---

## Three options

### Option A — Unify on **legacy tree** (Mike's existing convention)

**Change:** Update `find_po_folder` + `on_po_received` to use:
```
{year} - Purchase Orders /Q{n}/{po_number}    # bare PO, no prefix
```
…matching what Mike already does manually. One-shot migration: move the single `2026/Q2/8955-0000076737` folder into `2026 - Purchase Orders /Q2/8955-0000076737`.

**Pros:**
- Aligns with how Mike actually files. Zero training required.
- 89 of 90 archived POs immediately findable.
- Trailing-space + variant tolerance can be added incrementally.

**Cons:**
- Trailing spaces in folder names are fragile (Drive UI strips them on rename, search APIs sometimes trim them). Need a normalization layer.
- The legacy tree has *three* observed name patterns — bare, `PO ` prefix, trailing space. Lookup must try all three.
- Folder names like `2024 - Purchase Orders ` (with literal trailing space) require exact-match queries; `q="name='2024 - Purchase Orders'"` (no space) returns nothing.

**Implementation effort:** Medium. ~1 day of code + 1 day of probing all the variants. Single migration is trivial (move 1 folder).

---

### Option B — Unify on **app tree** (clean canonical)

**Change:** Bulk-migrate all 89 legacy POs into `{year}/{quarter}/PO-{po}/`. Rename `2024 - Purchase Orders /Q1/8955-0000063707` → `2024/Q1/PO-8955-0000063707`. After migration, delete legacy `* - Purchase Orders` parent folders.

**Pros:**
- Single canonical tree forever. No variant logic.
- Existing `find_po_folder` and `on_po_received` work as-is.
- Easier to extend (per-quarter sub-folders, supplier-doc nesting) without legacy quirks.

**Cons:**
- **Breaks Mike's bookmarks/links.** Anyone with a saved Drive URL to `2025 - Purchase Orders /Q3/4500736218` gets a 404.
- Big migration: 89 folder renames across 3-4 year-folders. Each rename is 1 Drive API call but parent reparenting is involved (move file+children). Risk of partial migration if API rate-limits or any single move fails.
- The `PO-` prefix is *only* in code — Mike doesn't actually use it. Forcing it on a legacy folder may feel unnatural.

**Implementation effort:** High. ~3 days of careful migration scripting, dry-run validation, rollback plan, and Mike spot-checking the result. Real risk of data ambiguity if any folder rename clashes.

---

### Option C — **Hybrid: read both, write to legacy** (lowest-risk path)

**Change:** Update `find_po_folder` to search both trees with all name variants. Switch `on_po_received` to write into the legacy tree using Mike's bare-PO convention. Leave existing folders untouched.

**Pros:**
- Zero migration. Zero broken links.
- 100% of legacy POs discoverable from day one (by `find_po_folder`).
- New POs land where Mike expects them.
- Implementable as a single PR with feature-flag rollback.

**Cons:**
- `find_po_folder` becomes more complex (try 2 trees × 3-4 name variants = 6-8 queries per lookup; cache the result).
- Permanent inconsistency: `2026/Q2/8955-0000076737` (the one in the app tree) becomes orphaned. Either move it or it stays as a fossil.
- Trailing-space normalization layer still needed.

**Implementation effort:** Medium. ~1 day. Search-with-variants helper, plus a one-line write-side switch.

---

## Recommended option (mine)

**C, then converge to A over time.** Here's why:

- C is the cheapest path to "100% findable" — the actual KPI gap right now.
- C doesn't ask Mike to change his workflow.
- Once C is live, the app and Mike both write into the legacy tree. The single app-tree PO becomes dead-letter; can be moved manually whenever.
- Future PR: drop the app-tree read path entirely, leaving only the legacy tree (= Option A's end state). No migration needed because no new app-tree POs are being created.

**B is overengineered for this scale (90 POs).** The clean canonical tree is nice in theory but doesn't justify breaking Mike's bookmarks.

---

## Open questions for Mike

1. **Confirm the legacy tree is your canonical filing system going forward?** (i.e., new POs should land in `2026 - Purchase Orders /Q2/{po}/`, not `2026/Q2/PO-{po}/`)
2. **Bare PO numbers, no `PO-` prefix?** (matches what's in the legacy tree)
3. **Trailing-space tolerance:** when you create `2027 - Purchase Orders` next year, will it have a trailing space or not? Tells us whether to normalize on read.
4. **The lone `2026/Q2/8955-0000076737`** — move it manually to the legacy tree, or leave the dead-letter?
5. **Supplier docs / shipping labels** — those land inside the PO folder today. Same expectation if we switch to legacy tree?
6. **`drive_triggers.py:179` writes `PO-{po}`. Want to drop the `PO-` prefix on write?** (yes if matching legacy convention)

---

## What I'll ship after your answers

- **If C:** one PR — `find_po_folder` searches both trees with variants; `on_po_received` writes to legacy with bare PO. ~5 tests.
- **If A:** one PR with the same code as C plus a one-shot migration script for the lone app-tree PO.
- **If B:** plan a migration script + dry-run mode + Mike-confirmation gate before any rename. ~3 PRs (read fix → migration tool → final cleanup).

No work starts until you pick.

---

## References

- Memory: `project_session_2026_04_28_drive_audit_findings`
- Code: `src/core/gdrive.py:318-334`, `src/agents/drive_triggers.py:161-187`, `src/api/modules/routes_po_drive_audit.py`
- Endpoint: `/api/admin/po-drive-audit?probe=1` (read-only, surfaces tree shape)
