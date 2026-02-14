# Reytech RFQ System — Go Live Guide

## What Works Right Now (Ship Today)

### ✅ Price Check Flow (AMS 704) — PRODUCTION READY
Upload 704 → Parse → SCPRS Lookup → Amazon Lookup → Set Pricing → Generate Completed 704 → Download

This is your bread and butter. It works end-to-end:
- Auto-detects AMS 704 on upload
- Parses all header fields + line items from fillable PDF
- Cleans descriptions (strips font specs, dimensions, garbage)
- SCPRS Won Quotes lookup for historical pricing
- Amazon SerpApi pricing with title matching
- Price protection tiers (Current/Light/Standard/Safe)
- Live tier comparison table (profit at every buffer level)
- Generates filled PDF with auto-fit fonts, signature, date
- Filename: PC_{name}_Reytech_.pdf

### ✅ Full RFQ Flow (703B + 704B + Bid Package) — PRODUCTION READY
Upload all RFQ PDFs → Auto-identify forms → Parse 704B line items → SCPRS Lookup → Amazon Lookup → Set Pricing → Generate Filled 703B + 704B + Bid Package → Draft Email → Send

This replaces QuoteWerks for state bids:
- Drag-drop upload of RFQ PDF attachments
- Auto-identifies which PDF is 703B, 704B, Bid Package
- Parses line items from 704B
- Quick Markup buttons (10-30%) + SCPRS undercut (-1% to -5%)
- Generates all three filled forms in one click
- Creates draft response email with attachments
- Send directly or open in mail app

### ✅ Supporting Systems — WORKING
- Won Quotes KB: Stores every SCPRS result + completed PC pricing for future reference
- Product Research: Amazon SerpApi search with caching
- Pricing Oracle: Historical price intelligence from Won Quotes
- Auto-Processor: One-click pipeline with confidence scoring (A/B/C/F)
- Audit trail: Tracks processing times, confidence grades

### ⚠️ Convert PC → Full Quote — PARTIAL
Creates the RFQ record with pricing carried over, but the user
needs to upload blank 704B/703B/Bid Package templates for the RFQ
to generate the full bid package forms. This is because each RFQ
comes with its own copy of these forms.

### ⚠️ Email Poller — NEEDS CONFIG
Code exists and works. Needs GMAIL_ADDRESS and GMAIL_PASSWORD
(App Password) env vars on Railway. Once set, it auto-polls inbox
for RFQ emails, downloads attachments, and processes them.


## Deployment Steps

### 1. Files to Deploy

These files changed and need to be pushed to Railway:

```
dashboard.py        — Main app (2,669 lines) — UI overhaul + tier comparison
price_check.py      — PC engine (898 lines) — description cleaner + PDF overwrite
auto_processor.py   — One-click pipeline (692 lines) — from Phase 7
product_research.py — Amazon search (544 lines) — from Phase 6.1
won_quotes_db.py    — SCPRS KB (589 lines) — from Phase 6.0
pricing_oracle.py   — Price intelligence (595 lines) — from Phase 6.0
```

These files are ALREADY on Railway and should NOT be overwritten:
```
reytech_filler_v4.py — Core form filler (703B, 704B, Bid Package)
email_poller.py      — IMAP poller + email sender
config.json          — Company info, email settings
scprs_lookup.py      — SCPRS web scraper
```

### 2. Railway Deployment

Option A — If you have a Git repo connected:
```bash
cd your-repo
# Copy the updated files into your repo
cp ~/Downloads/dashboard.py .
cp ~/Downloads/price_check.py .
cp ~/Downloads/auto_processor.py .
cp ~/Downloads/product_research.py .
cp ~/Downloads/won_quotes_db.py .
cp ~/Downloads/pricing_oracle.py .

git add -A
git commit -m "Phase 7: auto-processor, tier comparison, description cleaner"
git push
# Railway auto-deploys from push
```

Option B — If using Railway CLI:
```bash
railway up
```

Option C — Manual via Railway dashboard:
1. Go to your Railway project
2. Settings → Deploy from local
3. Upload the updated files

### 3. Environment Variables (Railway Dashboard → Variables)

Required:
```
DASH_USER=reytech           # Dashboard login
DASH_PASS=<your-password>   # Dashboard password
SECRET_KEY=<random-string>  # Flask session key
PORT=5001                   # Railway assigns this
```

For email auto-polling (optional but recommended):
```
GMAIL_ADDRESS=sales@reytechinc.com
GMAIL_PASSWORD=<gmail-app-password>  # NOT your regular password
```
→ Get app password: Google Account → Security → 2FA → App Passwords

For Amazon pricing:
```
# SerpApi key is stored at /app/data/.serpapi_key on the volume
# If it's not there, set it once via the dashboard or SSH:
echo "your-key-here" > /app/data/.serpapi_key
```

### 4. Railway Volume

Your persistent data lives at /app/data (mounted volume):
```
/app/data/
  price_checks.json        — All PC records
  rfqs.json                — All RFQ records  
  won_quotes.json          — SCPRS Won Quotes KB
  product_research_cache.json — Amazon search cache
  auto_process_audit.json  — Processing audit trail
  .serpapi_key             — SerpApi API key
  quote_counter.json       — Quote numbering
  processed_emails.json    — Dedup for email poller
  pc_upload_*.pdf          — Uploaded PC source PDFs
  PC_*_Reytech_.pdf        — Generated completed PCs
```

### 5. Verify After Deploy

Hit these URLs to confirm everything works:

1. `https://your-app.railway.app/` → Should see dashboard with login
2. `https://your-app.railway.app/api/health` → Should show all components green
3. Upload a test AMS 704 PDF → Should parse and show PC detail page
4. Click SCPRS Lookup → Should search Won Quotes KB
5. Click Amazon Lookup → Should find Amazon prices
6. Click Generate Completed 704 → Should produce filled PDF
7. Download PDF → Verify company info, pricing, signature, clean description


## QuoteWerks Replacement — What You Gain & Lose

### What this system does BETTER than QuoteWerks for state bids:
- Reads AMS forms natively (QuoteWerks doesn't understand 704/704B)
- Auto-fills California state form fields in the correct locations
- SCPRS historical pricing lookup (QuoteWerks has no state price database)
- Amazon price matching with SerpApi
- Price protection tiers for volatile Amazon pricing  
- One-click auto-process with confidence scoring
- Generates the exact PDF forms the state expects

### What QuoteWerks has that you'd lose:
- Customer/contact database → Your customers ARE the state agencies, they come on the form
- Quote numbering → Already handled by quote_counter.json
- Product catalog → Won Quotes KB + Amazon search replaces this
- Invoice generation → You don't invoice from quotes, POs come separately
- Multi-format export → You only need PDF (what the state accepts)

### Bottom line:
For California state bids (your business), this system is strictly better
than QuoteWerks. QuoteWerks is general-purpose; this is purpose-built.

You can ditch QuoteWerks for state work immediately. If you have
non-state customers that need generic quotes, keep QuoteWerks for those
until we build a standalone quote generator (Phase 9+).


## Daily Workflow After Go-Live

### Morning (with email poller):
1. Open dashboard — new RFQs/PCs auto-imported overnight
2. Review parsed items and confidence grades
3. For A-grade PCs: verify pricing, click Generate, download, email
4. For lower grades: adjust pricing manually, then generate

### Morning (without email poller):
1. Download PDF attachments from email
2. Upload to dashboard
3. Same review/generate/send flow

### Typical Price Check (2-3 minutes):
1. Upload AMS 704 PDF
2. Click ⚡ Auto-Process
3. Review tier comparison, pick pricing tier
4. Click Generate Completed 704
5. Download, attach to reply email, send

### Typical Full RFQ (5-10 minutes):
1. Upload 703B + 704B + Bid Package PDFs
2. System parses line items
3. Click SCPRS Lookup, then Amazon Lookup
4. Set pricing with markup buttons or manual entry
5. Click Generate Bid Package
6. Review draft email, click Send
