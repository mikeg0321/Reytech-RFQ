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
    writer) — and even those are wrapped at the boundary.

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
