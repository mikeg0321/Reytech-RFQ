"""Regression guard: no app-level email signature blocks.

Per CLAUDE.md "Gmail Handles Signatures": Gmail auto-appends the configured
signature on every outbound message. Any app-level signature here creates a
double-sig on every send.

Audit 2026-04-21 (OB-3 through OB-9) found 7 sites emitting hardcoded
`Best regards,\\nMichael Guadan\\n...` blocks (or HTML equivalents). All 7
were cleared. This file prevents them from coming back.

If a future change legitimately needs to emit an email signature, add a
comment + pytest mark to document *why* — don't just loosen the guard.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Files that emit outbound email bodies. If you add a new email-emitting
# module, add it here so the regression guard covers it.
EMAIL_EMITTING_FILES = [
    ROOT / "src" / "agents" / "cs_agent.py",
    ROOT / "src" / "agents" / "email_outreach.py",
    ROOT / "src" / "api" / "modules" / "routes_crm.py",
    ROOT / "src" / "api" / "modules" / "routes_analytics.py",
    ROOT / "src" / "api" / "modules" / "routes_rfq_admin.py",
    ROOT / "src" / "api" / "modules" / "routes_pricecheck_admin.py",
    ROOT / "src" / "api" / "modules" / "routes_pricecheck_gen.py",
    ROOT / "src" / "agents" / "notify_agent.py",
]

# Patterns that indicate an app-level signature block. The closing value
# names ("Michael Guadan" / "Mike Guadan") are fine in body prose (e.g.
# "My name is Michael Guadan..."); the violation is when they appear
# immediately after a "Best regards" closing.
SIG_SIGNATURE_PATTERNS = [
    # Plain text sig: "Best regards,\nMichael Guadan" variants
    re.compile(r"Best regards[,\s]+\\n\s*(Michael|Mike)\s+Guadan", re.IGNORECASE),
    re.compile(r"Best regards[,\s]+\\n\s*(Michael|Mike)\s+Guzman", re.IGNORECASE),
    # HTML sig: <p>Best regards,<br> ... Michael Guadan
    re.compile(r"Best regards,<br>.*?Michael\s+Guadan", re.IGNORECASE | re.DOTALL),
    # Multi-line triple-quoted sig: "Best regards,\nMichael"
    re.compile(r"Best regards,\n(Michael|Mike)\s+Guadan", re.IGNORECASE),
]


class TestNoAppLevelSignatures:
    """Audit guard OB-3/4/5/6/7/8/9 — app-level sig blocks must not return."""

    @pytest.mark.parametrize("path", EMAIL_EMITTING_FILES, ids=lambda p: p.name)
    def test_no_best_regards_signature_in_file(self, path: Path):
        assert path.exists(), f"Expected {path} to exist — update guard if moved."
        src = path.read_text(encoding="utf-8")
        hits = []
        for pat in SIG_SIGNATURE_PATTERNS:
            for m in pat.finditer(src):
                # Show the 40-char window around the match for debugging
                start = max(0, m.start() - 20)
                end = min(len(src), m.end() + 20)
                hits.append(f"  pat={pat.pattern!r}  ctx={src[start:end]!r}")
        assert not hits, (
            f"OB-3/4/5/6/7/8/9 REGRESSION: {path.name} emits an app-level "
            f"signature block. Gmail auto-appends the canonical sig — the app "
            f"must not add its own. Matches:\n" + "\n".join(hits)
        )

    def test_cs_signature_constant_is_empty(self):
        """OB-3: `CS_SIGNATURE` must not contain sig content."""
        from src.agents.cs_agent import CS_SIGNATURE
        assert CS_SIGNATURE == "", (
            "OB-3 REGRESSION: CS_SIGNATURE was re-populated. Every CS draft "
            "will double-sig on top of Gmail's auto-sig."
        )

    def test_build_default_signature_returns_empty(self):
        """OB-9: `_build_default_signature` must not auto-generate a sig."""
        from src.api.modules.routes_rfq_admin import _build_default_signature
        assert _build_default_signature() == "", (
            "OB-9 REGRESSION: auto-generated signature is back. First load "
            "of /api/email-signature will stash a hardcoded Reytech sig "
            "into CONFIG — Gmail's auto-sig then stacks on top."
        )


class TestCSResponseBodyHasNoSignature:
    """Hit the real code path: build a CS draft and assert the body is clean."""

    def test_general_cs_draft_no_best_regards(self, tmp_path, monkeypatch):
        from src.agents.cs_agent import build_cs_response_draft
        monkeypatch.chdir(tmp_path)
        result = build_cs_response_draft(
            classification={
                "intent": "general",
                "sender_email": "buyer@cdcr.ca.gov",
                "sender_name": "Real Buyer",
            },
            subject="Pricing question",
            body="Hi Mike, quick question about an order.",
            sender="buyer@cdcr.ca.gov",
        )
        assert result["ok"] is True
        body = result["draft"]["body"]
        assert "Best regards" not in body, (
            "CS draft body contains 'Best regards' — app-level sig leaked back."
        )
        assert "Michael Guadan" not in body, (
            "CS draft body names 'Michael Guadan' — Gmail's auto-sig will "
            "append it; the app must not."
        )
        assert "sales@reytechinc.com" not in body, (
            "CS draft body contains 'sales@reytechinc.com' — that belongs in "
            "the Gmail auto-sig, not the body."
        )

    def test_quote_status_cs_draft_no_sig(self, tmp_path, monkeypatch):
        from src.agents.cs_agent import build_cs_response_draft
        monkeypatch.chdir(tmp_path)
        result = build_cs_response_draft(
            classification={
                "intent": "quote_status",
                "sender_email": "buyer@cdcr.ca.gov",
                "sender_name": "Real Buyer",
                "entities": {"quote_numbers": []},
            },
            subject="Status on my quote?",
            body="Just checking in.",
            sender="buyer@cdcr.ca.gov",
        )
        assert result["ok"] is True
        body = result["draft"]["body"]
        assert "Best regards" not in body
        assert "Michael Guadan" not in body


class TestOutreachBodiesHaveNoSignature:
    """Draft PC + lead outreach emails and check no sig block."""

    def test_draft_pc_email_has_no_signature(self):
        from src.agents.email_outreach import _draft_pc_email
        draft = _draft_pc_email({
            "pc_number": "PC123",
            "institution": "CDCR",
            "requestor": "Real Buyer",
            "requestor_email": "buyer@cdcr.ca.gov",
            "items": [{"description": "Test item", "qty": 1,
                       "pricing": {"recommended_price": 10.0}}],
            "quote_number": "R26Q100",
            "due_date": "2026-05-01",
        }, pdf_path=None)
        body = draft["body"]
        assert "Best regards" not in body
        assert "Michael Guadan" not in body
        assert "sales@reytechinc.com" not in body
        assert "SB/DVBE Cert #2002605" not in body

    def test_draft_lead_email_has_no_signature(self):
        from src.agents.email_outreach import _draft_lead_email
        draft = _draft_lead_email({
            "institution": "CalVet",
            "buyer_name": "Buyer",
            "buyer_email": "buyer@calvet.ca.gov",
            "po_number": "PO123",
            "estimated_savings_pct": 10,
            "matched_items": [],
        })
        body = draft["body"]
        assert "Best regards" not in body
        assert "Michael Guadan" not in body
