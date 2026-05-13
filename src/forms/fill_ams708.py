"""AMS 708 — Generative AI Use Disclosure (standalone filler).

2026-05-12: rfq_0ebe242f (CCWF Ashley) email contract told us
explicitly that CCHCS/CDCR is moving from STD 1000 → AMS 708 for the
GenAI disclosure form, effective immediately. The 708 form fields
already exist inside the bid package PDF (when the buyer ships one
with `708_*` AcroForm fields) and are filled by `fill_bid_package`
— but a STANDALONE AMS 708 file is what buyers expect when the
package goes to them as separate attachments.

The standalone blank template is NOT yet checked into the repo
(`data/templates/ams_708_blank.pdf`). Until it is, this filler is a
typed no-op that logs absence and returns False so the package
generator can detect "AMS 708 was requested but template missing"
and degrade gracefully (operator sees the gap in the QA panel
rather than a silent miss).

Once the template arrives, replace `_template_path()` lookup with
the real path and implement the field map. The values dict already
exists in `reytech_filler_v4.fill_bid_package` (708_Text1..14 +
708_Check Box2 + 708_Signature15 + 708_Text16 for date) — that
contract is the single source of truth for what to fill.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger("reytech.fill_ams708")

# Standalone AMS 708 blank template. Drop the buyer's blank here when
# it arrives. Until then, `os.path.exists()` returns False and the
# filler skips gracefully.
def _template_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..",
        "data", "templates", "ams_708_blank.pdf",
    )


def ams708_template_available() -> bool:
    """Cheap presence check — does the standalone blank PDF exist?"""
    try:
        return os.path.exists(_template_path())
    except Exception:
        return False


def fill_ams708_standalone(rfq_data: dict, config: dict, output_path: str) -> bool:
    """Fill a standalone AMS 708 PDF for the given RFQ + Reytech config.

    Args:
        rfq_data: the RFQ dict (we read `solicitation_number`, `sign_date`)
        config:   the Reytech company config (we read `company.{name,phone,address,
                  city,state,zip,owner,title,fein}`)
        output_path: absolute path to write the filled PDF

    Returns:
        True iff the file was successfully written. False when the
        standalone template isn't checked in yet — caller is
        responsible for surfacing this gap in the package QA panel.

    The function NEVER raises on the "no template" path; it just logs
    a single WARN and returns False. That keeps the package generator
    resilient until the template lands.
    """
    tmpl = _template_path()
    if not os.path.exists(tmpl):
        log.warning(
            "fill_ams708_standalone: blank template not present at %s — "
            "AMS 708 will be missing from this package. Add the blank "
            "PDF and re-deploy.", tmpl,
        )
        return False

    # ── When the template ships, the wiring below mirrors the 708 block
    #    in `reytech_filler_v4.fill_bid_package`. Field map is the canon. ──
    from src.forms.reytech_filler_v4 import (
        fill_and_sign_pdf,
        _sol_display,
        get_pst_date,
    )

    company = config["company"]
    sol = _sol_display(rfq_data.get("solicitation_number", ""))
    sign_date = rfq_data.get("sign_date", get_pst_date())

    values = {
        "708_Text1": sol,
        "708_Text3": company["name"],
        "708_Text4": company.get("phone", ""),
        "708_Text5": company.get("address", ""),
        "708_Text6": company.get("city", ""),
        "708_Text7": company.get("state", "CA"),
        "708_Text8": company.get("zip", ""),
        "708_Check Box2": "/Yes",       # "Do not use GenAI"
        "708_Text11": "N/A",
        # Grid of 3 + 6 N/A cells — same as bid package fill_bid_package
        "708_Text12.0": "N/A", "708_Text12.1": "N/A", "708_Text12.2": "N/A",
        "708_Text12.3.0": "N/A", "708_Text12.3.1": "N/A", "708_Text12.3.2": "N/A",
        "708_Text13.0": "N/A", "708_Text13.1": "N/A", "708_Text13.2": "N/A",
        "708_Text13.3": "N/A", "708_Text13.4": "N/A", "708_Text13.5": "N/A",
        "708_Text13.6": "N/A", "708_Text13.7": "N/A",
        "708_Text14": "N/A",
        "708_Text16": sign_date,
    }

    try:
        fill_and_sign_pdf(tmpl, values, output_path, sign_date=sign_date)
        log.info("AMS 708 standalone filled at %s (sol=%s)", output_path, sol)
        return True
    except Exception as e:
        log.exception("fill_ams708_standalone failed: %s", e)
        return False
