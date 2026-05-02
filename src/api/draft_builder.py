"""Pure-logic builders for the Gmail draft that goes out for an RFQ.

Built 2026-05-01 (PR-B3). Mike's directive: "do not direct send, i want
to see everything first" — so the Send flow now creates a Gmail draft
that the operator reviews + sends from Gmail itself (which already has
a perfectly good draft preview UI we don't need to rebuild).

The draft is built from:
  - The RFQ record (recipient, subject, sol#, threading params from
    PR-B1's `email_thread_id` + `email_message_id` capture).
  - The package manifest (which forms were generated, attachments).
  - Resolved attachment paths on disk.

This module is intentionally side-effect free. The route handler does
the IO (Gmail API call, file reads, DB writes) — this just builds the
arguments to feed `gmail_api.save_draft()`.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


def build_recipients(rfq: Dict) -> Tuple[str, str]:
    """Return (to, cc) strings for the draft.

    Reply target priority — see CLAUDE.md "respond only to the initial
    received email" + Mike's PR-B1 directive:
      1. ``original_sender`` — set when ingest detected a forwarded mail
         and extracted the buyer's address from the forward chain.
      2. ``requestor_email`` — primary buyer field on most records.
      3. ``email_sender`` — fallback when neither of the above is set.
    """
    to = ""
    for k in ("original_sender", "requestor_email", "email_sender"):
        v = (rfq.get(k) or "").strip()
        if v and "@" in v:
            to = v
            break
    cc = (rfq.get("cc_emails") or rfq.get("email_cc") or "").strip()
    return to, cc


def build_subject(rfq: Dict) -> str:
    """Re: <original subject> when we have the buyer's subject; else a
    well-formed fallback. Strips any existing Re:/Fwd: prefixes so we
    don't end up with "Re: Re: Re: Quote for X" after a few rounds.
    """
    orig = (rfq.get("email_subject") or rfq.get("subject") or "").strip()
    sol = (rfq.get("solicitation_number") or rfq.get("rfq_number") or "").strip()
    if orig:
        cleaned = re.sub(r'^(re:\s*|fwd?:\s*|fw:\s*)+', '', orig,
                         flags=re.IGNORECASE).strip()
        return f"Re: {cleaned}" if cleaned else f"Re: Quote — Solicitation #{sol}"
    if sol:
        return f"Quote — Solicitation #{sol}"
    return f"Quote — RFQ #{rfq.get('id', '?')}"


def build_body(rfq: Dict) -> str:
    """Build the default email body.

    PR-B3 keeps this simple — pre-fill with a short polite reply that
    references the solicitation. PR-B2 will replace this with a
    smart-drafted body sourced from past Gmail Sent emails (Mike's
    directive: "smart drafting. most responses are simple and say the
    same thing"). For now the operator can still edit in Gmail before
    hitting Send.

    NEVER include a signature block — Gmail auto-appends the configured
    signature on send (per CLAUDE.md "Gmail Handles Signatures" rule).
    """
    sol = (rfq.get("solicitation_number") or rfq.get("rfq_number") or "").strip()
    name = (rfq.get("requestor_name") or "").strip()
    first = ""
    if name:
        # Strip "Last, First" → First, then take the first whitespace token
        if "," in name:
            name = name.split(",", 1)[1].strip()
        first = name.split()[0] if name.split() else ""
    if not first or "@" in first:
        first = "Procurement Officer"
    sol_line = f"Solicitation #{sol}" if sol else "this RFQ"
    return (f"Dear {first},\n\n"
            f"Please find attached our bid response for {sol_line}.\n\n"
            f"Let us know if you have any questions.\n\n"
            f"Thank you for the opportunity.")


def resolve_attachments(rfq: Dict, manifest: Optional[Dict],
                        data_dir: str) -> List[str]:
    """Find the absolute paths of the package PDFs to attach.

    Search order (matches the priority chain that
    routes_analytics.send_quote_email used pre-B3):
      1. Stored paths on the RFQ record (reytech_quote_pdf / output_pdf).
      2. Manifest's generated_forms list — joins file paths under the
         per-RFQ output dir.
      3. r["output_files"] list joined under data/output/<sol> or <rid>.
      4. Scan data/output/<sol> directly.
    """
    paths: List[str] = []
    sol = (rfq.get("solicitation_number") or "").strip()
    rid = rfq.get("id", "")

    # 1. Direct refs on the RFQ — these win when present + on disk
    for k in ("reytech_quote_pdf", "output_pdf"):
        p = (rfq.get(k) or "").strip()
        if p and os.path.exists(p) and p not in paths:
            paths.append(p)

    # 2. Manifest forms — most accurate inventory of what's in this version
    if manifest:
        for f in (manifest.get("generated_forms") or []):
            fname = f.get("filename") or ""
            if not fname:
                continue
            for base in (
                os.path.join(data_dir, "output", sol) if sol else None,
                os.path.join(data_dir, "output", rid) if rid else None,
            ):
                if not base:
                    continue
                fp = os.path.join(base, fname)
                if os.path.exists(fp) and fp not in paths:
                    paths.append(fp)
                    break

    # 3. r["output_files"] — older PCs/RFQs may only have this list
    if rfq.get("output_files"):
        for of in rfq["output_files"]:
            if not of:
                continue
            for base in (
                os.path.join(data_dir, "output", sol) if sol else None,
                os.path.join(data_dir, "output", rid) if rid else None,
            ):
                if not base:
                    continue
                fp = os.path.join(base, of)
                if os.path.exists(fp) and fp not in paths:
                    paths.append(fp)
                    break

    # 4. Last resort — scan output dir for PDFs
    if not paths and sol:
        outdir = os.path.join(data_dir, "output", sol)
        if os.path.isdir(outdir):
            for fn in sorted(os.listdir(outdir)):
                if fn.lower().endswith(".pdf"):
                    fp = os.path.join(outdir, fn)
                    if fp not in paths:
                        paths.append(fp)

    return paths


def build_threading_params(rfq: Dict) -> Dict[str, Optional[str]]:
    """Return in_reply_to / references / thread_id for the draft.

    Per Mike's PR-B1 directive ("respond only to the inintal received
    email"), we anchor on the *first* received email's identifiers, not
    the latest reply in the thread. The locator/bind flow ensures these
    point at the original even when ingest didn't capture them.
    """
    msg_id = (rfq.get("email_message_id") or "").strip() or None
    thread_id = (rfq.get("email_thread_id") or "").strip() or None
    return {
        "in_reply_to": msg_id,
        # References chain seeded from the message-id of the original;
        # Gmail will extend the chain on send if there's a longer history.
        "references": msg_id,
        "thread_id": thread_id,
    }


# Per-agency Gmail label hints — Mike's "buyers require the same thread
# or subject when forwarded to organize their open bids" → group sent
# drafts in Gmail by buyer agency so the Sent folder reads cleanly.
# Operator must create these labels in Gmail UI; if absent, Gmail
# silently ignores the labelIds entry. We never auto-create labels —
# that requires labels.create scope which we don't ask for.
_AGENCY_LABEL_NAMES = {
    "calvet": "CalVet",
    "calvet_barstow": "CalVet",
    "cchcs": "CCHCS",
    "cdcr": "CDCR",
    "dsh": "DSH",
    "dgs": "DGS",
    "calfire": "CalFire",
    "chp": "CHP",
}


def agency_label_name(agency_key: str) -> Optional[str]:
    """Return the Gmail label name for an agency key, or None.

    The route handler resolves this to a labelId via Gmail labels.list.
    Keeping the name → key mapping here makes it easy to override per
    agency without touching draft-creation logic.
    """
    if not agency_key:
        return None
    return _AGENCY_LABEL_NAMES.get(agency_key.lower().strip())


def build_draft_params(rfq: Dict, manifest: Optional[Dict],
                       attachments: List[str]) -> Dict:
    """One-shot composer that returns everything `save_draft()` needs.

    Caller is expected to layer on label_ids (after resolving the agency
    label name to an id) and from_name/from_addr if overriding defaults.
    """
    to, cc = build_recipients(rfq)
    threading = build_threading_params(rfq)
    return {
        "to": to,
        "cc": cc or None,
        "subject": build_subject(rfq),
        "body_plain": build_body(rfq),
        "attachments": attachments or None,
        "in_reply_to": threading["in_reply_to"],
        "references": threading["references"],
        "thread_id": threading["thread_id"],
    }


def gmail_draft_url(draft_response: Dict) -> str:
    """Build the Gmail web URL that opens this draft for the operator.

    Gmail's stable URL for a specific draft is:
        https://mail.google.com/mail/u/0/#drafts/<message_id>
    where message_id is the id of the draft's underlying message
    (response.message.id), NOT the draft id itself. Falls back to the
    Drafts folder when the response shape is unexpected.
    """
    try:
        msg_id = (draft_response or {}).get("message", {}).get("id", "")
        if msg_id:
            return f"https://mail.google.com/mail/u/0/#drafts/{msg_id}"
    except Exception as e:
        log.debug("gmail_draft_url: %s", e)
    return "https://mail.google.com/mail/u/0/#drafts"
