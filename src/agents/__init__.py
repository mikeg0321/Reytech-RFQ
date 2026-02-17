"""External service integrations.

Modules:
    email_poller       — IMAP email monitoring for inbound RFQs
    product_research   — Amazon/SerpApi product lookup
    scprs_lookup       — State SCPRS price database
    tax_agent          — CDTFA tax rate lookup
    item_identifier    — Phase 13: Item ID + search term optimization (LLM-enhanced)
    lead_gen_agent     — Phase 13: SCPRS opportunity scanner + outreach
    scprs_scanner      — Phase 13: Background SCPRS polling loop
    quickbooks_agent   — Phase 13: QuickBooks OAuth2 + vendor/PO sync
    email_outreach     — Phase 14: Auto-draft + approve + send buyer emails
    growth_agent       — Phase 14: Win/loss analysis + strategy recommendations
    voice_agent        — Phase 14: AI voice calls (Twilio + ElevenLabs scaffold)
"""
