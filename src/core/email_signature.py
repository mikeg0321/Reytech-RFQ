"""
Shared Email Signature — used across all email sending modules.
Supports HTML (with logo image) and plain text variants.

Logo Configuration:
  - Set REYTECH_LOGO_URL env var to a hosted logo image URL
  - Or place logo.png in data/ directory and it will be base64 embedded
  - Falls back to text-only signature if no logo available
"""
import base64
import os
import logging

log = logging.getLogger("reytech.signature")

# ═══════════════════════════════════════════════════════════════════════
# Configuration — override via env vars or config
# ═══════════════════════════════════════════════════════════════════════

COMPANY = os.environ.get("REYTECH_COMPANY", "Reytech Inc.")
NAME = os.environ.get("REYTECH_SIGNER_NAME", "Michael Guadan")
PHONE = os.environ.get("REYTECH_PHONE", "949-229-1575")
EMAIL = os.environ.get("REYTECH_EMAIL", "sales@reytechinc.com")
ADDRESS = os.environ.get("REYTECH_ADDRESS", "30 Carnoustie Way, Trabuco Canyon, CA 92679")
CERT = os.environ.get("REYTECH_CERT", "SB/DVBE Cert #2002605")
WEBSITE = os.environ.get("REYTECH_WEBSITE", "https://reytechinc.com")
LOGO_URL = os.environ.get("REYTECH_LOGO_URL", "")


def _get_logo_src():
    """Get logo image source — URL, base64, or empty."""
    if LOGO_URL:
        return LOGO_URL

    # Try to find logo in data directory
    try:
        from src.core.paths import DATA_DIR
    except ImportError:
        DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

    for fname in ("logo.png", "logo.jpg", "reytech_logo.png", "reytech_logo.jpg"):
        path = os.path.join(DATA_DIR, fname)
        if os.path.exists(path):
            try:
                with open(path, "rb") as f:
                    data = base64.b64encode(f.read()).decode()
                ext = fname.rsplit(".", 1)[-1]
                mime = f"image/{'jpeg' if ext in ('jpg','jpeg') else ext}"
                return f"data:{mime};base64,{data}"
            except Exception as e:
                log.warning("Failed to read logo %s: %s", path, e)

    return ""


def get_plain_signature(closing: str = "Respectfully,") -> str:
    """Plain text email signature."""
    return f"""{closing}

{NAME}
{COMPANY}
{ADDRESS}
{PHONE}
{EMAIL}
{CERT}"""


def get_html_signature(closing: str = "Respectfully,") -> str:
    """HTML email signature with optional logo image."""
    logo_src = _get_logo_src()

    logo_cell = ""
    if logo_src:
        logo_cell = f"""<td style="padding-right:16px;vertical-align:top">
     <img src="{logo_src}" alt="{COMPANY}" style="width:80px;height:auto;border-radius:4px" onerror="this.style.display='none'">
    </td>"""

    return f"""<div style="border-top:1px solid #ddd;padding-top:12px;margin-top:16px">
 <table cellpadding="0" cellspacing="0" style="font-family:'Segoe UI',Arial,sans-serif">
  <tr>
   {logo_cell}
   <td style="vertical-align:top">
    <div style="font-weight:700;font-size:14px;color:#1a1a2e">{NAME}</div>
    <div style="font-size:12px;color:#666">{COMPANY}</div>
    <div style="font-size:12px;color:#666">{ADDRESS}</div>
    <div style="font-size:12px;margin-top:4px">
     <a href="tel:{PHONE.replace('-','')}" style="color:#2563eb;text-decoration:none">{PHONE}</a> |
     <a href="mailto:{EMAIL}" style="color:#2563eb;text-decoration:none">{EMAIL}</a>
    </div>
    <div style="font-size:11px;color:#888;margin-top:2px">{CERT} · <a href="{WEBSITE}" style="color:#2563eb;text-decoration:none">{WEBSITE.replace('https://','')}</a></div>
   </td>
  </tr>
 </table>
</div>"""


def wrap_html_email(body_text: str, closing: str = "Respectfully,") -> str:
    """
    Convert plain text body to full HTML email with signature.
    Use this when upgrading plain text emails to HTML.
    """
    import html as _html
    body_escaped = _html.escape(body_text).replace("\n", "<br>")

    return f"""<div style="font-family:'Segoe UI',Arial,sans-serif;font-size:14px;color:#222;line-height:1.6">
{body_escaped}
<br><br>
{get_html_signature(closing)}
</div>"""
