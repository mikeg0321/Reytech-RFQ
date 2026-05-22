# The Spine — Charter

> **Authored by Mr. Wolf, 2026-05-15, in response to Mike's
> first-principles assessment ("should I nuke this app?").**
>
> Memory: `project_mr_wolf_first_principles_call_2026_05_15.md`

---

## What The Spine Is

The Spine is a **structurally minimal canonical path** for quoting one
agency (CCHCS to start), built **beside** the existing legacy substrate
under `src/core/quote_model.py` + `src/core/quote_contract.py` +
`src/core/pricing_math.py` + the ~20 mutator routes.

It is **not** a refactor. It is **not** a replacement. It is a
**forcing function** — the discipline ("don't add aliases", "one
canonical reader", "tax math is `subtotal × rate`, period") that failed
for three months as policy is now enforced **by the Pydantic model
boundary** (`extra="forbid"`) and **by the test suite** (architectural
tests that fail the build on alias creep or legacy imports).

If the Spine can render today's CCHCS quote correctly **from its own
state with zero legacy imports**, it gradually cannibalizes the legacy
paths — agency by agency, over the next ~90 days. If it can't, the
failure is contained and legacy keeps shipping.

---

## What The Spine Is Not

- It is **not** a violation of the project's Prime Directive
  (`CLAUDE.md` lines 3–22). That directive applies to the legacy
  renderers reading via `QuoteContract`. The Spine is a separate
  canonical contract for the CCHCS path, opt-in only via an agency
  routing flag. Legacy renderers continue to flow through
  `QuoteContract` for all non-Spine quotes.
- It is **not** a Big Bang rewrite. Shadow-mode runs every CCHCS
  quote through both paths for 14 days minimum before any operator
  ships via Spine.
- It is **not** a place to dump features. Anything that does not
  belong inside the model's `extra="forbid"` boundary stays in legacy
  until v2.

---

## Non-Negotiable Invariants (Enforced By Code, Not Policy)

1. **One canonical model.** `src/spine/model.py` is the only schema
   for Spine quotes. Unknown fields raise.

2. **One write endpoint.** `POST /spine/quotes/{id}/state` accepts a
   full Quote state and stores it atomically. No partial fan-outs.

3. **One read endpoint.** `GET /spine/quotes/{id}` returns the
   canonical Quote. Operator UI, Quote PDF, form fillers, and QA gate
   all read from this one endpoint.

4. **Single field per logical value.** No aliases. `unit_price_cents`
   is the only sell-price field. No `bid_price`. No `price_per_unit`.
   No `our_price`. No stored `markup_pct` (it's a computed display
   property only).

5. **Integer cents, never floats.** Tax math is `(subtotal_cents *
   tax_rate_bps) // 10000`. There is no float-rounding error class.

6. **Tax rate is mandatory at ingest.** A Spine Quote cannot reach
   the `priced` status without a non-zero `tax_rate_bps`.

7. **Shipping is the constant $0.00.** Not a field. Not optional. Not
   conditional. Mike's universal procurement rule (5/15: *"all
   resellers don't include shipping costs and bake those into our
   margins"*) is encoded by the **absence of a shipping field**.

8. **Cost basis must carry source + validation timestamp.** Every
   `cost_cents` over $100 requires either `cost_source_url` or
   `cost_hand_validated_note`, **plus** `cost_validated_at` within 30
   days. The `priced → finalized` transition rejects otherwise.

9. **State machine, not free mutation.** `parsed → priced → finalized
   → sent`. Each transition has hard preconditions. Status transitions
   do not recompute line item values.

10. **Append-only event log.** Every state write also writes an event
    row. The current state is reconstructible from the event log; no
    path silently mutates.

11. **Zero legacy imports inside `src/spine/`.** Tested by
    `test_no_legacy_imports.py`. The Spine's correctness must not
    depend on legacy code being correct. The only whitelisted external
    deps are 3 leaf utilities (Vision parser, CDTFA client, PDF
    writer) — and even those are wrapped at the boundary. The **two
    file-scoped exceptions** are the CCHCS document adapters — see the
    section below.

---

## Sanctioned Boundary — The CCHCS Document Adapters

> Added 2026-05-20; second adapter added 2026-05-21 (Job #1 PR-1).
> Memory: `handoff-2026-05-20-legacy-adapter-build`,
> `rfqapp-crossroads-verdict-2026-05-21`.

Invariant 11 forbids legacy imports so the Spine's correctness does not
depend on legacy correctness. There are **two sanctioned, file-scoped
exceptions** — both CCHCS document adapters: `src/spine/packet_render.py`
(below) and `src/spine/forms_render.py` (the "Second adapter" section).

**Why it exists.** The CCHCS Non-Cloud RFQ Packet is a single buyer-
supplied PDF that already bundles the 703B cover, the 704B line-item
table, and the bid-package attachments. The correct way to respond is to
fill *that* document (Path B: `PdfReader(buyer_pdf) → PdfWriter(clone)`).
The legacy filler `src/forms/cchcs_packet_filler.py` (shipped 2026-04-13,
verified 2026-05-20) already does this correctly.

As the Spine was built out it grew its OWN from-scratch agency-form
renderers (`src/spine/agency_forms/cchcs_{703b,704b,bidpkg}.py`). They
re-implemented document filling from blank templates and produced
packets that failed CCHCS responsiveness review (the 2026-05-18 "trash"
output + a 21-minute operator hand-finish). Re-implementing a verified
renderer to satisfy a purity rule would be **the rule defeating its own
purpose** — the Spine would depend on a *worse, unverified* renderer
instead of `None`.

**The decision.** `packet_render.py` is an adapter, not a renderer. It
maps a Spine `Quote` + `EmailContract` onto the legacy filler's call
shape and delegates. It is permitted to import exactly:

- `src.forms.cchcs_packet_parser` — parse the buyer's packet PDF.
- `src.forms.cchcs_packet_filler` — fill the buyer's packet PDF.
- `src.core.paths` — `DATA_DIR` / `OUTPUT_DIR` path constants only.

**Containment.** The exception is enforced *file-scoped* in
`test_no_legacy_imports` (`_FILE_SCOPED_LEGACY_IMPORTS`): only
`packet_render.py` may import those three modules; every other Spine
file still gets zero legacy imports. The Spine substrate proper —
`model.py`, `email_contract.py`, `db.py`, the counters — remains
import-clean. The adapter is a leaf at the edge of the Spine, not a
dependency of its core.

The Spine's own `agency_forms/` renderers are **retired**: every
operator-facing `/forms/{703b,704b,bidpkg,packet}/pdf` route serves the
adapter's output. They are kept on disk only until a follow-up deletes
them and their tests.

### Second adapter — `forms_render.py` (the standalone form set)

> Added 2026-05-21 (Job #1 PR-1). Memory:
> `rfqapp-crossroads-verdict-2026-05-21`.

The Non-Cloud Packet (above) is the MINORITY CCHCS format. The COMMON
format is a set of separate buyer forms: AMS 703B **or** 703C, AMS 704B,
and the CDCR Bid Package. The from-scratch `agency_forms/` renderers
were built for these and produced the 2026-05-18 "trash" output.

The 2026-05-21 research established cause and fix. **Cause:** the
from-scratch renderers filled *blank templates*. **Fix:** the verified
legacy fillers in `src/forms/reytech_filler_v4.py` (`fill_703b`,
`fill_703c`, `fill_704b`, `fill_bid_package`, et al.) — a 4-month,
120-commit, audited code path that Reytech has shipped real bids on —
fill the buyer's actual documents. The fix-rate on that file dropped
~70% after late April and stayed down. Re-implementing it would discard
that track record to re-attempt the failure.

`src/spine/forms_render.py` is therefore the **second sanctioned
adapter** — the exact sibling of `packet_render.py`. It maps a Spine
`Quote` + `EmailContract` onto the legacy fillers' call shape and
delegates. It is permitted to import only the verified `src.forms`
fillers it delegates to (`reytech_filler_v4` and the helpers those
call) plus `src.core.paths`. Enforced file-scoped in
`test_no_legacy_imports` (`_FILE_SCOPED_LEGACY_IMPORTS`); every other
Spine file still gets zero legacy imports.

Which adapter serves a quote — `packet_render` (Non-Cloud Packet) or
`forms_render` (standalone set), and 703B vs 703C — is decided by the
`EmailContract`, never guessed (LAW 6). The retired `agency_forms/`
renderers are deleted by Job #1.

---

## Cannibalization Roadmap

| Day | Gate |
|-----|------|
| 3 | Render `9e63456e` Quote PDF correctly from Spine state. Tax non-zero. No hand-overlay. |
| 6 | Operator UI POSTs full state — exactly 1 POST per save. |
| 9 | All 5 architectural tests + tax math + cost-validity tests pass. |
| 14 | Shadow-mode: every CCHCS quote runs through both paths. Diff emitted on divergence. Operator still ships via legacy. |
| 21 | 5 consecutive diff-clean CCHCS quotes → operator ships ONE via Spine with legacy backup. |
| 30 | 10 clean Spine ships → CCHCS is the Spine's. |
| 60 | CalVet migration begins. |
| 90+ | CalRecycle, DGS, DSH migrated. Legacy code path shrinks as agencies leave. |

If any day-gate fails, failure is bounded to the Spine codebase.
Legacy keeps shipping. No rollback panic.

---

## What Stays Untouched

- Race substrate (Doheny / CC / SE locks)
- WolfPack cancellation watcher
- Proofpoint auto-pull
- Gmail poller
- Oracle calibration daemon
- Telegram bridge
- All non-CCHCS legacy quote paths (CalVet, DSH, CalRecycle, DGS)

None of these are broken. The Spine is exclusively about the **quote
substrate**, not the operating apparatus around it.

---

## When This Document Is Wrong

If, during build, you discover that an invariant on this charter is
incompatible with reality (e.g., shipping really does need a non-zero
field for one agency), **stop and update this charter first**, before
modifying the model. The architectural tests will fail the build until
the charter and the model agree.

Drift between the charter and the model is the bug class this whole
project was built to prevent.
