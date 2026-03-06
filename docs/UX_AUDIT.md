# RFQ Dashboard — UX Audit & Recommendations
## PC Detail & Home Page Review

---

## Executive Summary

The PC Detail page has **87 clickable elements** across 60 buttons and 29 API endpoints. For a page whose workflow is linear (receive → price → markup → generate → send), this is severe cognitive overload. The user shouldn't need to think about which of 7 pricing buttons to click or distinguish between Save vs Save & Fill vs Download.

**Issues found this session that trace back to UX complexity:**
- Save & Fill silently downgraded "sent" → "draft" 
- MFG# overwritten by Amazon ASIN on paste
- Markup/buffer didn't apply to all items
- SCPRS showed 100% confidence on wrong match
- "PC not found" on save (race condition between threads)
- Source badge said "Amazon" for Walmart links
- 13 duplicate growth/intel pages
- Admin actions (duplicate/archive) didn't work from More menu

Each bug was caused by **too many features with too little integration between them.**

---

## Current State: PC Detail Page

### Toolbar Row 1 (always visible): 8 buttons
| Button | Usage | Recommendation |
|--------|-------|----------------|
| 💾 Save | Saves prices only | **MERGE** into single Save |
| 👁️ Preview | Shows print preview | KEEP |
| 📄 Save & Fill 704 | Saves + generates PDF | **MERGE** → becomes the Save |
| 📄 Download 704 | Downloads last PDF | **MERGE** → auto-downloads after Save |
| 🔍 diagnose | Debug tool | **MOVE** to More menu |
| 🔍 SCPRS | Price lookup | **MERGE** into "Find Prices" |
| 🔬 Amazon | Price lookup | **MERGE** into "Find Prices" |
| 🎯 Catalog | Price lookup | **MERGE** into "Find Prices" |

### Toolbar Row 2: 5 buttons
| Button | Usage | Recommendation |
|--------|-------|----------------|
| 🤖 AI Find | Price lookup | **MERGE** into "Find Prices" |
| 🌐 Web Search | Price lookup | **MERGE** into "Find Prices" |
| 🧠 Auto-Price | Runs all lookups | **REPLACE** with "Find Prices" |
| 🛒 Sweep | Batch price search | **MERGE** into "Find Prices" |
| 📝 Notes | Toggle notes rows | KEEP |
| ⋯ More | Dropdown menu | KEEP |

### More Menu: 12+ items
| Item | Usage | Recommendation |
|------|-------|----------------|
| Re-fill 704 | **Duplicate** of Save & Fill | **REMOVE** |
| Print Page | Low use | KEEP |
| Generate Quote | Formal 704b | KEEP |
| Portfolio Optimizer | Rare | KEEP (low position) |
| Competitor Intel | Rare | KEEP (low position) |
| Re-parse from PDF | Rare admin | KEEP |
| Rescan MFG#s | Rare admin | KEEP |
| 📝 Revisions | New feature | **PROMOTE** to toolbar |
| Mark Won/Lost | Important | **Already in status stepper** → REMOVE from More |
| No Response | Admin | KEEP |
| Duplicate | Admin | KEEP |
| Archive | Admin | KEEP |
| Delete | Admin | KEEP |

---

## Recommended Changes

### 1. CONSOLIDATE SAVE WORKFLOW (highest impact)

**Before:** 4 confusing buttons
- Save (just prices)
- Save & Fill 704 (prices + PDF)  
- Download 704 (get last PDF)
- Re-fill 704 (in More menu — duplicate)

**After:** 1 smart button
- **💾 Save & Download** — saves prices, generates PDF, auto-downloads
- If no changes since last generate, just downloads the existing PDF
- Green flash on success, shows download link

### 2. CONSOLIDATE PRICING (second highest impact)

**Before:** 7 separate buttons
- SCPRS, Amazon, Catalog, AI Find, Web Search, Auto-Price, Sweep

**After:** 1 button with dropdown
- **🔍 Find Prices** — runs Auto-Price (which already calls SCPRS → Catalog → Amazon → Web in sequence)
- Dropdown arrow shows individual sources for manual override
- Results populate inline as they come in

### 3. REMOVE DUPLICATE BUTTONS

- adminAction appears **8 times** — consolidate to More menu only
- saveAndGenerate appears **3 times** — single button
- window.print appears **2 times** — More menu only
- generateQuote appears **2 times** — More menu only

### 4. STATUS STEPPER ALREADY FIXED ✅

Now clickable. No changes needed.

### 5. HOME PAGE ASSESSMENT

The home page is actually well-organized:
- KPI cards (Urgent / Action Needed / Progress)
- Price Checks queue with quick actions
- RFQ queue
- Drag-drop upload
- Activity feed

**One issue:** Quick Price panel opens inline and pushes content down. Should be a modal or sidebar.

---

## Implementation Priority

| # | Change | Impact | Effort |
|---|--------|--------|--------|
| 1 | Consolidate Save workflow | HIGH | 30 min |
| 2 | Consolidate pricing buttons | HIGH | 20 min |
| 3 | Remove duplicate buttons | MEDIUM | 15 min |
| 4 | Move diagnose to More | LOW | 5 min |
| 5 | Promote Revisions to toolbar | LOW | 5 min |
