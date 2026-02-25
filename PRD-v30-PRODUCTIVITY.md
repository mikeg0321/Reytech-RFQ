# PRD-v30 — 5 Productivity Enhancements
**Date:** 2026-02-25 · **Commit:** fa4a0b0

## Why These 5

These were chosen by auditing where real time is lost in the daily workflow.
The common thread: **reduce clicks between seeing work and completing it.**

---

## Enhancement 1 — Smart Queue Urgency Sort

**Problem:** Queue sorted by quote number (newest first). A PC due tomorrow sits below one due next month. User must scan every row to find what's urgent.

**Solution:** Queue sorts by deadline urgency:
1. **OVERDUE** (past due) — red ⚠️ — always at top
2. **CRITICAL** (≤24h) — orange 🔥
3. **SOON** (≤3 days) — yellow
4. **NORMAL** — default
5. **TERMINAL** (won/lost/sent) — sinks to bottom

Due date column now shows a countdown: `2d overdue`, `Due today`, `Tomorrow`, `3d left`

**Impact:** The thing you need to do next is always the first row.

---

## Enhancement 2 — Quick-Price Panel

**Problem:** To price a PC, you click into detail, scroll through a massive page, enter prices, save, navigate back. For 10 PCs, that's 40+ page loads.

**Solution:** ⚡ button on every queue row opens a slide-out panel:
- All items listed with description, qty, cost sources
- SCPRS / Amazon / catalog prices shown inline
- Won history (past winning prices) shown per item
- One-click "⚡ $98.00" to apply recommendation
- "💾 Save & Close" prices the entity and refreshes queue

**Impact:** Price a full PC in ~15 seconds without ever leaving the home page.

**API:**
- `GET /api/quick-price/{pc|rfq}/{id}` — item data with recommendations
- `POST /api/quick-price/{pc|rfq}/{id}/save` — save prices

---

## Enhancement 3 — Won History Intelligence

**Problem:** When pricing an item, you have no context about what Reytech has won before for similar items. You guess, or manually search the won quotes database.

**Solution:** Every item in quick-price (and margin optimizer) now shows past winning prices:
- Searches `won_quotes` SQLite table for matching descriptions/part numbers
- Also searches PCs marked as "won" for matching item descriptions
- Shows: price, institution, date, quantity, quote number
- Results deduped and sorted by recency

**Impact:** Price with confidence — you see exactly what won before.

**API:** `GET /api/won-history/search?q={description}`

---

## Enhancement 4 — Stale Quote Follow-Up System

**Problem:** Quotes get sent and forgotten. No visibility into which quotes are awaiting response. Manual follow-ups require remembering who to email and when.

**Solution:**
- **Home widget:** Yellow banner shows stale quotes (sent > N days, no response)
- **Follow-ups page:** `/follow-ups` — full table with days-since, one-click email
- **Configurable threshold:** 2/3/5/7/14 day dropdown
- **One-click follow-up:** Sends professional email via Gmail, records on entity
- **Follow-up history:** Stored on RFQ/PC as `follow_ups[]` array

**Impact:** Never lose track of a sent quote. Follow up at exactly the right time.

**API:**
- `GET /api/stale-quotes?days=3` — all stale quotes
- `POST /api/stale-quotes/{rfq|pc}/{id}/follow-up` — send follow-up email

---

## Enhancement 5 — PC→RFQ Linkage & Conversion

**Problem:** PCs are the precursor to RFQs. When a PC becomes a formal solicitation, the data doesn't flow through. User re-enters everything. No audit trail connecting the two.

**Solution:**
- **Link to Existing RFQ:** Auto-matches by solicitation number or by institution + item overlap (≥50% match)
- **Convert to New RFQ:** Creates a full RFQ entity carrying over all items, pricing, cost data, recommendations
- **Bidirectional linking:** RFQ shows `linked_pc_id`, PC shows `linked_rfq_id`
- **Visual banner:** PC detail shows purple "🔗 Linked to RFQ" badge when linked

**Impact:** PC→RFQ conversion is one click. All pricing intelligence carries forward. Audit trail is preserved.

**API:**
- `POST /api/pc/{id}/link-rfq` — link PC to existing RFQ (auto or manual)
- `POST /api/pc/{id}/convert-to-rfq` — create new RFQ from PC data
