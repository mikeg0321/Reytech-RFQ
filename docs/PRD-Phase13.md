# Reytech RFQ — Phase 13 PRD
## Autonomous Pipeline + First Two Agents

**Version:** 8.1  
**Owner:** Michael Guadan, Reytech Inc.  
**Branch:** `phase-13` (from `main` @ v8.0.0, commit 6d70265)  
**Date:** February 17, 2026

---

## Vision

Transform Reytech from a tool you operate into a system that operates for you. Phase 13 builds the autonomous pipeline (email → filled 704, zero clicks) and deploys the first two revenue-generating agents: **Pricing Agent** and **Lead Generator**.

Phase 14 (future): Marketing Agent, Developer Agent, Manager Agent, Growth Agent.

---

## Current State (v8.0.0)

| Asset | Status |
|-------|--------|
| Pipeline stages | parse → price → fill 704 (manual triggers) |
| Entity graph | PC ↔ Quote ↔ PO bidirectional linking |
| CRM | 122 QB vendors + customer matching |
| SCPRS lookup | Manual per-item |
| Amazon lookup | Manual per-item |
| Tests | 110/110, 28-point QA |
| Routes | 68, all auth-protected |
| Data layer | JSON files |

---

## Phase 13 Deliverables

### 1. Dynamic Pricing Agent

**What it does:** When a PC is parsed, the pricing agent automatically determines the best price for each line item by checking multiple sources in priority order.

**Pricing waterfall:**
1. **Won Quotes DB** — Have we sold this exact item before? At what price? To this institution?
2. **Vendor check** — Can we source from our 122 QB vendors cheaper than Amazon? (data/vendors.json)
3. **SCPRS current price** — What is the state paying now? Undercut by configurable %
4. **Amazon lookup** — Fallback: find product, apply markup
5. **Margin optimizer** — Given win history for this agency + institution, what price maximizes P(win) × margin?

**Data inputs:**
- `data/vendors.json` — 122 vendors, 12 with contact info
- `data/won_quotes_knowledge.json` — historical wins with prices
- `data/quotes_log.json` — all quotes with win/loss status
- SCPRS API (live lookup)
- Amazon search (live lookup)

**Output:** Each line item gets:
- `recommended_price` — agent's best price
- `price_source` — which waterfall step determined the price
- `confidence` — A/B/C grade
- `alternatives` — other sources checked with their prices
- `reasoning` — "Won at $12.50 for CSP-Sac last month. SCPRS at $18.50. Recommending $14.00 (12% under SCPRS, 12% margin over vendor cost $12.50)"

**Key design decisions:**
- Agent runs automatically when PC status transitions to `parsed`
- Agent writes directly to PC item pricing fields
- Agent transitions PC status from `parsed` → `priced`
- Human reviews on PC page, adjusts if needed, clicks "Save & Fill 704"
- No API keys exposed — agent uses server-side secrets registry

### 2. Lead Generator Agent (SCPRS Scanner)

**What it does:** Monitors SCPRS for new purchase orders where Reytech can compete. Runs on a configurable interval (default: every 5 minutes during business hours).

**Detection logic:**
1. Poll SCPRS for new/updated POs
2. For each PO line item, check:
   - Is this a product category Reytech sells? (match against won_quotes_knowledge)
   - Is the PO price higher than our historical cost + margin?
   - Have we sold to this institution before? (CRM match)
3. Score each opportunity: `opportunity_score = margin_potential × win_probability × urgency`
4. For high-score opportunities, auto-generate outreach

**Outreach (Phase 14 — AI voice/email):**
- Phase 13: Generate lead card in dashboard with recommended action
- Phase 14: Auto-email buyer ("Saw PO #X for [item]. We supply these at [price]. Can we get on the quote list?")
- Phase 14+: AI voice call via Twilio + ElevenLabs

**Dashboard integration:**
- New "Leads" section on homepage
- Each lead shows: institution, PO #, items, SCPRS price, our estimated price, margin potential
- Actions: "Create PC" (auto-populates from lead), "Dismiss", "Contact buyer"

### 3. Secret Management

**Current problem:** All credentials are flat env vars on Railway. Agents sharing the same email password is a security risk.

**Solution:**
```
data/secrets_registry.json (encrypted at rest, never committed)
{
  "agents": {
    "pricing_agent": {
      "scprs_session": "...",
      "amazon_api_key": "..."
    },
    "lead_generator": {
      "scprs_credentials": "...",
      "email_sender": "..."
    },
    "email_poller": {
      "gmail_app_password": "..."
    }
  },
  "master_key_env": "REYTECH_MASTER_KEY"
}
```

- Each agent gets only the secrets it needs
- Master key in Railway env var decrypts the registry
- Secrets never appear in logs (redacted by logging_config)
- API: `get_secret(agent_name, key_name)` — returns decrypted value or raises

### 4. Autonomous Pipeline

**The full zero-touch flow:**

```
Email arrives (704 PDF attached)
    ↓
Email Poller detects new message
    ↓
Parser extracts items, institution, due date
    → Status: PARSED
    → Auto-assign R26Q# 
    ↓
Pricing Agent runs waterfall
    → Checks: won quotes → vendors → SCPRS → Amazon
    → Status: PRICED
    ↓
704 Filler generates completed PDF
    → Status: COMPLETED
    ↓
PC appears on dashboard: "Ready for review"
    → You open, preview 704, adjust if needed
    → Click "Approve & Send" (Phase 14: auto-send)
```

**Dependency chain:**
- Parser must succeed → Pricing Agent
- Pricing Agent must price all items → 704 Filler
- Any failure → PC marked with error status, flagged for manual intervention
- Each step logged with actor/timestamp in status_history

### 5. QuickBooks Integration (Foundation)

**Phase 13 scope:** Read-only vendor sync
- OAuth2 connection to QuickBooks Online
- Pull vendor list + purchase history on demand
- Update `data/vendors.json` with fresh data
- Show last-purchase-price for items when available

**Phase 14 scope:** Write-back
- Create POs in QB when quote is won
- Sync invoice data
- Track actual vs quoted costs

---

## Technical Architecture

### Agent Communication Protocol

```python
class AgentMessage:
    agent_id: str       # "pricing_agent"
    action: str         # "price_pc"  
    payload: dict       # {"pc_id": "abc123"}
    priority: int       # 1=urgent, 5=background
    timestamp: datetime
    
class AgentOrchestrator:
    def dispatch(self, msg: AgentMessage) -> AgentResult
    def get_agent_status(self, agent_id: str) -> dict
    def get_queue_depth(self) -> int
```

### File Structure (new)

```
src/
  agents/
    pricing_agent.py        # Dynamic pricing waterfall
    lead_generator.py       # SCPRS scanner + opportunity scorer  
    orchestrator.py         # Agent message dispatch
  core/
    secrets.py              # Secret management registry
    agent_protocol.py       # AgentMessage, AgentResult types
```

### Database Migration Path

Phase 13 stays on JSON (it works, don't break what works).
Phase 14 migrates to SQLite when:
- Quote count exceeds 500
- Multiple agents writing concurrently causes conflicts
- Query patterns need indexes (e.g., "all wins for CDCR in 2025")

---

## Success Metrics

| Metric | Current | Phase 13 Target |
|--------|---------|-----------------|
| Time: email → filled 704 | 15-30 min (manual) | < 2 min (auto) |
| Pricing accuracy | Manual research | 80%+ items priced without human edit |
| Leads generated | 0 (reactive only) | 5-10 per week |
| Human clicks per PC | 10+ | 1 (approve) |
| Test coverage | 110 tests | 150+ (agents covered) |

---

## Implementation Order

1. **Secret management** (foundation — everything depends on this)
2. **Agent protocol** (AgentMessage, orchestrator skeleton)
3. **Pricing Agent** (biggest value — auto-prices PCs)
4. **Autonomous pipeline wiring** (parser → pricing agent → 704 filler)
5. **Lead Generator** (SCPRS scanner + opportunity cards)
6. **QB vendor sync** (enrich pricing agent with vendor costs)
7. **Dashboard: Leads section** (display opportunities)

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| SCPRS changes HTML structure | Lead gen + pricing break | Parser resilience + fallback to cached data |
| Amazon blocks scraping | Pricing fallback lost | Vendor-first pricing + SCPRS undercut strategy |
| JSON write conflicts | Data corruption | File locking + atomic writes (rename pattern) |
| Agent loops (pricing triggers re-pricing) | CPU burn | Status checks prevent re-processing |
| QB OAuth token expiry | Vendor sync fails | Refresh token + manual re-auth flow |

---

## Not In Scope (Phase 14+)

- AI voice calls (Twilio + ElevenLabs)
- Auto-send emails to buyers
- Marketing agent (LinkedIn/social)
- React frontend
- Multi-user auth / roles
- SQLite migration
- Developer monitoring agent
- Manager orchestration agent
- Growth strategy agent
