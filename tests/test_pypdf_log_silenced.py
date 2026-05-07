"""Pin pypdf decrypt-warning silencer (audit Tracked-not-P0, 2026-05-07).

pypdf logs `Ignoring padding error: Invalid padding bytes` at WARNING for
almost every encrypted-PDF read (CCHCS templates etc). The error is
benign and buries real warnings. We bump it to ERROR in
`src/core/structured_log.py:setup_logging`.
"""
import logging


def test_pypdf_crypt_logger_silenced_after_setup():
    """`setup_logging` must lift the pypdf-cryptography logger to ERROR
    so the `Ignoring padding error` WARNINGs stop spamming the console."""
    from src.core.structured_log import setup_structured_logging
    setup_structured_logging()
    lvl = logging.getLogger(
        "pypdf._crypt_providers._cryptography").getEffectiveLevel()
    assert lvl >= logging.ERROR, (
        f"expected pypdf cryptography logger >= ERROR; got {lvl}")
