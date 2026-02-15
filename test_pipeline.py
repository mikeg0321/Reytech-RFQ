"""
Integration pipeline tests — exercise full workflows end-to-end.
"""
import os
import json
import pytest


class TestPCToQuotePipeline:
    """Full flow: seed PC → save prices → generate Reytech quote → verify PDF + log."""

    def test_full_pipeline(self, client, seed_pc, sample_pc, temp_data_dir):
        # Step 1: PC detail loads
        r = client.get(f"/pricecheck/{seed_pc}")
        assert r.status_code == 200

        # Step 2: Save prices
        r = client.post(f"/pricecheck/{seed_pc}/save-prices",
                        json={"price_0": 15.72, "cost_0": 12.58,
                              "markup_0": 25, "qty_0": 22,
                              "tax_enabled": False},
                        content_type="application/json")
        assert r.get_json()["ok"]

        # Step 3: Generate Reytech quote
        r = client.get(f"/pricecheck/{seed_pc}/generate-quote")
        d = r.get_json()
        assert d["ok"]
        qn = d["quote_number"]
        assert qn.startswith("R")
        # Format: R{YY}Q{seq} e.g. R26Q16
        assert qn[1:3].isdigit()  # year part

        # Step 4: Verify PDF exists
        dl_path = d.get("download", "")
        assert dl_path  # should have a download URL


class TestRFQWorkflow:
    """RFQ: seed → update pricing → generate Reytech quote."""

    def test_rfq_pricing_and_quote(self, client, seed_rfq, sample_rfq):
        rid = seed_rfq

        # Update prices
        r = client.post(f"/rfq/{rid}/update",
                        data={"cost_0": "350.00", "price_0": "454.40"},
                        follow_redirects=True)
        assert r.status_code == 200

        # Generate Reytech quote
        r = client.get(f"/rfq/{rid}/generate-quote", follow_redirects=True)
        assert r.status_code == 200


class TestQuoteNumberSequence:
    """Verify quote numbers lock-in per PC and increment across different PCs."""

    def test_same_pc_reuses_number(self, client, seed_pc, temp_data_dir):
        """Lock-in: regenerating same PC reuses the assigned quote number."""
        r1 = client.get(f"/pricecheck/{seed_pc}/generate-quote")
        d1 = r1.get_json()
        q1 = d1["quote_number"]

        # Second generation on same PC — should get SAME number
        r2 = client.get(f"/pricecheck/{seed_pc}/generate-quote")
        d2 = r2.get_json()
        q2 = d2["quote_number"]
        assert q1 == q2  # locked in

    def test_different_pcs_increment(self, client, seed_pc, temp_data_dir):
        """Different PCs get sequential quote numbers."""
        import json, os
        r1 = client.get(f"/pricecheck/{seed_pc}/generate-quote")
        q1 = r1.get_json()["quote_number"]

        # Create a second PC
        pc2 = {
            "id": "test-pc-002", "pc_number": "Second-PC",
            "institution": "CIM", "ship_to": "CIM, Chino, CA",
            "status": "priced", "tax_enabled": False, "tax_rate": 0,
            "delivery_option": "", "custom_notes": "", "price_buffer": 0,
            "default_markup": 25,
            "parsed": {"header": {"institution": "CIM"}, "line_items": []},
            "items": [{"item_number": "1", "qty": 1, "uom": "EA",
                       "description": "Test item", "no_bid": False,
                       "pricing": {"recommended_price": 10.00}}],
        }
        pc_path = os.path.join(temp_data_dir, "price_checks.json")
        with open(pc_path) as f: pcs = json.load(f)
        pcs["test-pc-002"] = pc2
        with open(pc_path, "w") as f: json.dump(pcs, f)

        r2 = client.get("/pricecheck/test-pc-002/generate-quote")
        q2 = r2.get_json()["quote_number"]

        seq1 = int(q1.split("Q")[1])
        seq2 = int(q2.split("Q")[1])
        assert seq2 == seq1 + 1  # different PC → next number


class TestQuoteFromPCPDFContent:
    """Verify generated PDF contains correct business data."""

    def test_pdf_has_institution_and_prices(self, tmp_path, sample_pc):
        from quote_generator import generate_quote_from_pc
        out = str(tmp_path / "pipeline.pdf")
        r = generate_quote_from_pc(sample_pc, out, quote_number="PIPE1",
                                    include_tax=False)
        assert r["ok"]

        import pdfplumber
        with pdfplumber.open(out) as pdf:
            text = pdf.pages[0].extract_text()

        # Institution
        assert "CSP-Sacramento" in text
        # RFQ number
        assert "OS - Den - Feb" in text
        # Price
        assert "15.72" in text
        # ASIN
        assert "B07TEST123" in text
