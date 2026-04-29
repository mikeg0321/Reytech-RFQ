"""Production-grade item extractor for email bodies (no parseable attachment).

Built 2026-04-29 per project_email_body_rfq_parser_gap.md. Body-only RFQs
(buyers who paste the request into the email instead of attaching a 704)
were silently landing as zero-item placeholder records. PR-A killed the
buyer-content-derived placeholders; PR-B (this module) actually extracts
items so the operator gets a parsed RFQ instead of a triage shell.

Design decisions (per Mike's scope answers):
  - Production quality — this is a recurring path, not a one-off.
  - Regex first (no LLM dependency on hot path).
  - When extraction yields 0 items, the caller falls back to a manual-
    entry shell with the body text saved for copy-paste reference.
  - Tag every extracted item with source='email_body_regex' so downstream
    surfaces can flag operator review needs.

Preprocessing chain:
  1. strip_html: emails arrive with mixed text/html — drop HTML, keep text
  2. strip_signature: cut at "--", "Sent from", "Get Outlook for", common
     trailing disclaimer / phone / email blocks
  3. normalize whitespace (tabs -> spaces, multi-blank-line collapse)

Pattern coverage:
  - tabular: LINE_NO QTY UOM PART# DESCRIPTION (CalVet, CCHCS-style)
  - tabular_simple: LINE_NO QTY DESCRIPTION
  - inline_qty_x_desc: "5 x widget", "5 widgets", "5 of widget"
  - inline_please_quote: "please quote 3 of widget", "need pricing on 10 widgets"
  - bullet/dash: "- 5 widgets" or "* 5 widgets"

Conservative bias: when in doubt, return fewer items. The kill criterion
for the rollout is "operator deletes >half of body-extracted items per
RFQ" — that means the extractor is a net negative. Bias toward false
negatives rather than false positives.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

log = logging.getLogger("reytech.email_body_extractor")


# ── Preprocessing ───────────────────────────────────────────────────


_HTML_TAG = re.compile(r"<[^>]+>")
_HTML_ENTITY = re.compile(r"&(?:#\d+|#x[0-9a-fA-F]+|[a-zA-Z]+);")
_ENTITY_MAP = {
    "&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">",
    "&quot;": '"', "&#39;": "'", "&apos;": "'", "&rsquo;": "'",
    "&lsquo;": "'", "&rdquo;": '"', "&ldquo;": '"', "&mdash;": "-",
    "&ndash;": "-", "&hellip;": "...",
}


def strip_html(body: str) -> str:
    """Remove HTML tags + decode common entities. Conservative — no
    full HTML parser dep; targeted at email-style HTML where the text
    is still readable inline."""
    if not body:
        return body
    # Tag-stripping only runs when there's HTML
    if "<" in body:
        # Remove style/script blocks entirely (their contents aren't text)
        body = re.sub(r"<(style|script)[^>]*>.*?</\1>", " ", body,
                      flags=re.DOTALL | re.IGNORECASE)
        # Replace block-level closing tags with newlines so paragraphs survive
        body = re.sub(r"</(p|div|tr|li|h[1-6])>", "\n", body, flags=re.IGNORECASE)
        body = re.sub(r"<br\s*/?>", "\n", body, flags=re.IGNORECASE)
        body = _HTML_TAG.sub(" ", body)
    # Always decode entities — buyers paste copy that contains them even
    # when the body is plain-text otherwise (e.g., "Tom&#39;s widgets")
    for ent, repl in _ENTITY_MAP.items():
        body = body.replace(ent, repl)
    body = _HTML_ENTITY.sub("", body)
    return body


_SIG_BOUNDARIES = [
    re.compile(r"^\s*--\s*$", re.MULTILINE),
    re.compile(r"^\s*-{2,}\s*Original Message\s*-{2,}", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*From:\s+.+@.+", re.MULTILINE),  # forwarded-message header
    re.compile(r"^\s*Sent from my (?:iPhone|iPad|Android|mobile)", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Get Outlook for ", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*This (?:e-?mail|message) (?:and any|is intended)", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*CONFIDENTIALITY NOTICE", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\s*Notice:\s+This (?:e-?mail|message)", re.MULTILINE | re.IGNORECASE),
]


def strip_signature(body: str) -> str:
    """Cut the body at the first signature/boilerplate boundary.

    Signature blocks tend to contain phone numbers, addresses, and
    disclaimers that match item-extraction patterns by accident —
    e.g., "Phone: 555-123-4567" can look like a part number. Cutting
    them off avoids those false positives entirely.
    """
    if not body:
        return body
    cut_at = len(body)
    for pat in _SIG_BOUNDARIES:
        m = pat.search(body)
        if m and m.start() < cut_at:
            cut_at = m.start()
    return body[:cut_at].rstrip()


def preprocess(body: str) -> str:
    """Full chain: html-strip -> signature-strip -> whitespace normalize."""
    if not body:
        return ""
    body = strip_html(body)
    body = strip_signature(body)
    # Tabs -> spaces (preserve column alignment for tabular patterns)
    body = body.replace("\t", "  ")
    # Collapse 3+ consecutive blank lines to 2 (preserves paragraph breaks)
    body = re.sub(r"\n{3,}", "\n\n", body)
    return body


# ── Extraction patterns ─────────────────────────────────────────────


# Whitelist of real UOM tokens. Restricting to known UOMs prevents the
# pattern from matching the FIRST word of a description as a UOM (e.g.,
# "GAUZE" in "2  50  GAUZE PAD STERILE" — without this whitelist, GAUZE
# would parse as uom='GAUZE' mfg='PAD' desc='STERILE').
_UOM_TOKENS = (
    "EA|EACH|CS|CASE|BX|BOX|PK|PKG|PACK|BG|BAG|GAL|LB|OZ|FT|YD|"
    "DZ|DOZ|DOZEN|GR|GROSS|CTN|CARTON|ROLL|RL|TUBE|TB|JAR|JR|"
    "PR|PAIR|PRPRS|SET|KIT|UNIT|UN|EAS"
)

# Tabular: "1  20  CS  MCK-123  BANDAGE ELASTIC 6\""
_TABULAR_FULL = re.compile(
    r"^\s*(\d{1,3})\s+"            # line no
    r"(\d{1,6})\s+"                # qty
    r"(" + _UOM_TOKENS + r")\s+"   # uom (whitelist of known tokens)
    r"([\w\-\.#]+(?:\s[\w\-\.#]+)?)\s+"  # part #
    r"(.+?)\s*$",                  # description
    re.IGNORECASE,
)

# Simpler tabular: "1  20  BANDAGE ELASTIC 6\""
_TABULAR_SIMPLE = re.compile(
    r"^\s*(\d{1,3})\s+"
    r"(\d{1,6})\s+"
    r"(.{10,})\s*$",
)

# Inline: "5 x widget", "5 widgets", "5 of widget", "qty: 5 widget"
_INLINE_QTY_DESC = re.compile(
    r"(?:^|[\s,;:.])"                              # boundary
    r"(?:qty[:\s]*)?"                              # optional "qty:" prefix
    r"(\d{1,5})\s*"                                # qty 1-99999
    r"(?:x|of|@|each)?\s+"                         # connector (optional)
    r"([A-Za-z][\w\-/]{2,}(?:[ \-/][\w\-/]{2,}){0,8})"  # description (2-9 tokens)
    r"(?=$|[.,;])",                                # word-boundary end
    re.IGNORECASE,
)

# "Please quote 3 of widget" / "Need pricing on 10 widgets"
_INLINE_PLEASE_QUOTE = re.compile(
    r"(?:please\s+)?(?:quote|provide\s+pricing|need(?:\s+pricing)?(?:\s+on)?|price(?:\s+check)?)\s+"
    r"(?:on\s+|for\s+|us\s+)?"
    r"(\d{1,5})\s*"
    r"(?:x|of|each|units?\s+of)?\s*"
    r"([A-Za-z][\w\-/]{2,}(?:[ \-/][\w\-/]{2,}){0,8})",
    re.IGNORECASE,
)

# Bullet/dash list: "- 5 widgets", "* 5 widgets", "• 5 widgets"
_BULLET_QTY_DESC = re.compile(
    r"^[\s]*[-*•‣⁃]\s*"
    r"(?:qty[:\s]*)?"
    r"(\d{1,5})\s*"
    r"(?:x|of|each)?\s+"
    r"([A-Za-z][\w\-/]{2,}(?:[ \-/][\w\-/]{2,}){0,8})",
    re.IGNORECASE,
)


# Lines that SHOULD NOT match — header rows, disclaimer fragments, etc.
_NEGATIVE_TOKENS = re.compile(
    r"\b(?:line\s+no|item\s+(?:no|#)|qty[/\s]?unit|part\s+#|"
    r"description|page\s+\d|of\s+\d+|"
    r"phone|fax|email|website|url|http)\b",
    re.IGNORECASE,
)

# Description-quality filter: at least one alphabetic token of length >= 3
_DESC_QUALITY = re.compile(r"\b[A-Za-z]{3,}\b")


def _is_real_description(desc: str) -> bool:
    """Filter signature/footer fragments masquerading as descriptions."""
    if not desc or len(desc) < 5:
        return False
    if _NEGATIVE_TOKENS.search(desc):
        return False
    if not _DESC_QUALITY.search(desc):
        return False
    # Reject lines that are mostly punctuation or numbers
    alpha = sum(1 for c in desc if c.isalpha())
    if alpha < len(desc) * 0.3:
        return False
    return True


def _make_item(qty: int, desc: str, *, item_no: int, mfg: str = "",
               uom: str = "EA") -> Dict[str, Any]:
    return {
        "item_number": str(item_no),
        "qty": qty,
        "uom": uom,
        "mfg_number": mfg.strip(),
        "description": desc.strip(),
        "row_index": item_no,
        "pricing": {},
        "source": "email_body_regex",
        "needs_review": True,  # extracted from prose, operator should verify
    }


def extract_items(body_text: str) -> List[Dict[str, Any]]:
    """Run the full extraction chain. Returns a list of item dicts.

    Stages (first non-empty wins, no merging) — tabular patterns run
    before prose patterns because tabular has stronger structure
    (line_no anchor) and should win when present:
      1. Tabular full (line_no qty uom part# desc)
      2. Tabular simple (line_no qty desc)
      3. Bullet/dash list (- 5 widgets)
      4. "Please quote N of widget" pattern
      5. Inline "qty x desc" — most permissive, last resort
    """
    if not body_text:
        return []
    text = preprocess(body_text)
    if len(text) < 15:
        return []

    lines = text.split("\n")

    # Stage 1: tabular full
    items: List[Dict[str, Any]] = []
    for ln in lines:
        ln = ln.strip()
        if not ln or _NEGATIVE_TOKENS.search(ln):
            continue
        m = _TABULAR_FULL.match(ln)
        if m:
            qty = int(m.group(2))
            mfg = m.group(4).strip()
            desc = m.group(5).strip()
            if 1 <= qty <= 99999 and _is_real_description(desc):
                items.append(_make_item(
                    qty, desc, item_no=int(m.group(1)),
                    mfg=mfg, uom=m.group(3).upper(),
                ))
    if items:
        log.info("body_extract: tabular_full found %d items", len(items))
        return items

    # Stage 2: tabular simple (line_no qty desc, no UOM/part#)
    for ln in lines:
        ln = ln.strip()
        if not ln or _NEGATIVE_TOKENS.search(ln):
            continue
        m = _TABULAR_SIMPLE.match(ln)
        if m:
            qty = int(m.group(2))
            desc = m.group(3).strip()
            if 1 <= qty <= 99999 and _is_real_description(desc):
                items.append(_make_item(qty, desc, item_no=int(m.group(1))))
    if items:
        log.info("body_extract: tabular_simple found %d items", len(items))
        return items

    # Stage 3: bullet/dash list
    for ln in lines:
        m = _BULLET_QTY_DESC.match(ln)
        if m:
            qty = int(m.group(1))
            desc = m.group(2).strip()
            if 1 <= qty <= 99999 and _is_real_description(desc):
                items.append(_make_item(qty, desc, item_no=len(items) + 1))
    if items:
        log.info("body_extract: bullet found %d items", len(items))
        return items

    # Stage 4: "please quote / need pricing on N of widget"
    for ln in lines:
        for m in _INLINE_PLEASE_QUOTE.finditer(ln):
            qty = int(m.group(1))
            desc = m.group(2).strip()
            if 1 <= qty <= 99999 and _is_real_description(desc):
                items.append(_make_item(qty, desc, item_no=len(items) + 1))
    if items:
        log.info("body_extract: please_quote found %d items", len(items))
        return items

    # Stage 5: inline qty x desc — most permissive, last resort
    seen_descs = set()
    for ln in lines:
        for m in _INLINE_QTY_DESC.finditer(ln):
            qty = int(m.group(1))
            desc = m.group(2).strip()
            # De-dup on description (case-insensitive)
            key = desc.lower()
            if key in seen_descs:
                continue
            if 1 <= qty <= 99999 and _is_real_description(desc):
                items.append(_make_item(qty, desc, item_no=len(items) + 1))
                seen_descs.add(key)
    if items:
        log.info("body_extract: inline_qty_desc found %d items", len(items))
        return items

    log.info("body_extract: 0 items found in %d-char body", len(text))
    return []
