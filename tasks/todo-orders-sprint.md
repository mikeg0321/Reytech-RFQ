# Orders Sprint — Full Lifecycle Build-out

## What Exists (verified)
- [x] PO email detection + order creation (email_poller → _create_order_from_po_email)
- [x] Order detail page with line items, supplier links, tracking
- [x] Line item status dropdowns (pending → ordered → shipped → delivered)
- [x] _update_order_status auto-calculates order status from items
- [x] Draft invoice on all-delivered
- [x] CS agent classify + build_cs_response_draft
- [x] Follow-up engine with scheduler
- [x] Notify agent with SMS/email (po_received, all_delivered added)
- [x] Orders page with stats, progress bars

## What's Missing (build now)
- [ ] 1. Line-item level notifications (shipped/delivered per item)
- [ ] 2. Daily digest: items not ordered, delivered but not invoiced
- [ ] 3. Home page orders progress board (what's missing/pending)
- [ ] 4. CS agent order context (feed live order data into draft responses)
- [ ] 5. Shipping tracking auto-import from vendor emails in mike@ inbox
