import json, sys, os
os.chdir("/data") if os.path.exists("/data") else None

rfqs = json.load(open("rfqs.json")) if os.path.exists("rfqs.json") else json.load(open(os.path.join(os.path.dirname(__file__), "data", "rfqs.json")))
r = rfqs.get("20260324_000120_3312", {})
if not r:
    print("RFQ NOT FOUND")
    sys.exit()

print("=== RFQ 20260324_000120_3312 ===")
print("SOL:", r.get("solicitation_number", ""))
print("INST:", r.get("delivery_location", ""), "|", r.get("institution", ""), "|", r.get("institution_name", ""))
print("AGENCY:", r.get("agency", ""), r.get("agency_name", ""))
print("SOURCE:", r.get("source", ""))
print("SUBJ:", r.get("email_subject", ""))
print("SENDER:", r.get("email_sender", ""))
print("FORM:", r.get("form_type", ""))
print("LINKED_PC:", r.get("linked_pc_id", ""), r.get("linked_pc_number", ""))
print("STATUS:", r.get("status", ""))
print("CREATED:", r.get("created_at", ""))

items = r.get("line_items", [])
print(f"ITEMS: {len(items)}")
for i, item in enumerate(items):
    d = (item.get("description", "") or "")[:55]
    q = item.get("qty", "")
    pn = item.get("item_number", "") or item.get("part_number", "")
    cost = item.get("supplier_cost", "") or item.get("vendor_cost", "")
    src = item.get("source_pc", "") or item.get("_from_pc", "")
    print(f"  [{i+1}] q={q} pn={pn} cost={cost} src_pc={src} | {d}")

print("\n=== TEMPLATES ===")
print(json.dumps(r.get("templates", {}), indent=2)[:300])

print("\n=== BODY (first 500) ===")
print((r.get("body_text", "") or "")[:500])

# Check matching PCs
print("\n=== MATCHING PCs ===")
pcs_path = "price_checks.json"
if os.path.exists(pcs_path):
    pcs = json.load(open(pcs_path))
    for pid, pc in pcs.items():
        pc_inst = (pc.get("institution", "") or "").lower()
        rfq_inst = (r.get("delivery_location", "") or r.get("institution_name", "") or "").lower()
        if (pc_inst and rfq_inst and (pc_inst in rfq_inst or rfq_inst in pc_inst)):
            print(f"  PC {pid}: inst={pc.get('institution','')} items={len(pc.get('items',[]))} status={pc.get('status','')} created={pc.get('created_at','')[:10]}")
