"""
pii_mask.py — Mask personally identifiable information in log output.

Usage:
    from src.core.pii_mask import mask_pii
    log.error("Error processing: %s", mask_pii(str(e)))
"""
import re

# Patterns to mask
_EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
_PHONE_RE = re.compile(r'\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b')
_SSN_RE = re.compile(r'\b\d{3}-\d{2}-\d{4}\b')
_CC_RE = re.compile(r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b')


def mask_pii(text: str) -> str:
    """Mask emails, phones, SSNs, and credit card numbers in text."""
    if not text or not isinstance(text, str):
        return text or ""
    text = _EMAIL_RE.sub('[EMAIL]', text)
    text = _SSN_RE.sub('[SSN]', text)
    text = _CC_RE.sub('[CC]', text)
    text = _PHONE_RE.sub('[PHONE]', text)
    return text
