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

    for fname in ("reytech_logo.png", "reytech_logo_email.png", "logo.png", "logo.jpg", "reytech_logo.jpg", "reytech_logo_white.png"):
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
    """Plain text email signature — matches Gmail signature format."""
    return f"""{closing}

{COMPANY}
Sales Support
{WEBSITE.replace('https://', '').replace('http://', '')}
Trabuco Canyon, CA
{PHONE}
CA MB/SB/SB-PW/DVBE #2002605
NY SDVOB - 221449
DOT - Disadvantaged Business Enterprise DBE #44511
MBE - SC6550
SBA-SDVOB (Unique Entity ID: FWWSKE9113T7)"""


def get_html_signature(closing: str = "Respectfully,") -> str:
    """HTML email signature — matches Gmail signature format exactly.
    Vertical stack: closing → logo → company → details → certifications."""
    logo_html = '<img src="cid:reytech_logo" alt="ReyTech Inc." style="width:140px;height:auto;display:block">'

    return f"""{closing}
<div style="font-family:'Segoe UI',Arial,sans-serif;margin-top:16px;line-height:1.6">
{logo_html}
<br>
<strong style="font-size:14px;color:#1a1a2e">{COMPANY}</strong><br>
<span style="font-size:13px;color:#444">Sales Support</span><br>
<a href="https://www.reytechinc.com" style="color:#2563eb;text-decoration:none;font-size:13px">www.reytechinc.com</a><br>
<span style="font-size:13px;color:#444">Trabuco Canyon, CA</span><br>
<span style="font-size:13px;color:#444">{PHONE}</span><br>
<span style="font-size:12px;color:#666;line-height:1.5">
CA MB/SB/SB-PW/DVBE #2002605<br>
NY SDVOB - 221449<br>
DOT - Disadvantaged Business Enterprise DBE #44511<br>
MBE - SC6550<br>
SBA-SDVOB (Unique Entity ID: FWWSKE9113T7)
</span>
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
