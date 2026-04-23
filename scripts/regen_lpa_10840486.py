"""Re-run the LPA fill engine (PR #447) on RFQ 10840486's uploaded template.

The existing 10840486_703B_Reytech.pdf was generated before PR #447 landed,
so it went through fill_703b() which can't name-match LPA fields. This
script forces the new fill_cchcs_it_rfq() path against the same uploaded
template and overwrites the output at the canonical path.
"""
import os, sys
sys.path.insert(0, "/app")

RID = "9ad8a0ac"
SOL = "10840486"


def main():
    from src.api.data_layer import load_rfqs
    from src.forms.reytech_filler_v4 import (
        _classify_703b_slot_template,
        fill_cchcs_it_rfq,
    )
    from src.core.paths import CONFIG_PATH

    r = load_rfqs().get(RID)
    if not r:
        print(f"ERROR: RFQ {RID} not found")
        sys.exit(1)

    tmpl = r.get("templates", {}) or {}
    lpa_path = tmpl.get("703b")
    if not lpa_path or not os.path.exists(lpa_path):
        print(f"ERROR: 703b template missing on record: {lpa_path}")
        sys.exit(1)

    shape = _classify_703b_slot_template(lpa_path)
    print(f"Template classified as: {shape}")
    if shape != "cchcs_it_rfq":
        print(f"ERROR: expected cchcs_it_rfq but got {shape}")
        print("Check _classify_703b_slot_template or the uploaded template.")
        sys.exit(1)

    import json
    with open(CONFIG_PATH) as f:
        config = json.load(f)

    out_dir = f"/data/output/{SOL}"
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{SOL}_703B_Reytech.pdf")

    # Archive previous output
    if os.path.exists(out_path):
        from datetime import datetime
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        prev_dir = os.path.join(out_dir, "_prev")
        os.makedirs(prev_dir, exist_ok=True)
        import shutil
        shutil.move(out_path, os.path.join(prev_dir, f"{stamp}_{SOL}_703B_Reytech.pdf"))
        print(f"Archived prior 703B to _prev/{stamp}_...pdf")

    print(f"Running fill_cchcs_it_rfq()\n  input : {lpa_path}\n  output: {out_path}")
    fill_cchcs_it_rfq(lpa_path, r, config, out_path)
    print(f"\n✓ Regenerated: {os.path.getsize(out_path):,} bytes")
    print(f"Download: https://web-production-dcee9.up.railway.app"
          f"/api/download/{SOL}/{SOL}_703B_Reytech.pdf")


if __name__ == "__main__":
    main()
