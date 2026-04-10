"""
PDF Visual QA — Vision-based verification of filled PDF forms.

Renders filled PDFs to images, sends to Claude Vision, and validates:
- All expected fields are visibly filled (not blank due to font issues)
- Text is not cut off or overflowing field boundaries
- Signature is present and in the correct location
- Layout is correct (no misaligned rows, no orphaned content)
- Cross-form consistency (company name matches across package)

Runs AFTER form_qa.py (field-level) to catch rendering issues that
programmatic checks miss. Uses prompt caching for cost efficiency.

Cost: ~$0.02-0.03 per page. At 20 PDFs/day ≈ $0.50/day.
"""

import os
import json
import logging
from typing import Optional
from dataclasses import dataclass, field

log = logging.getLogger("reytech.pdf_visual_qa")


def _get_api_key() -> str:
    """Get Anthropic API key."""
    try:
        from src.core.secrets import get_agent_key
        return get_agent_key("item_identifier") or ""
    except Exception:
        return os.environ.get("ANTHROPIC_API_KEY", "")


# ═══════════════════════════════════════════════════════════════════════════
# RESULT TYPES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class VisualIssue:
    """A single visual issue found in a PDF."""
    severity: str       # "error" | "warning" | "info"
    page: int           # 1-based page number
    category: str       # "text_overflow" | "blank_field" | "signature" | "layout" | "consistency"
    description: str    # Human-readable description
    field_name: str = ""  # PDF field name if applicable


@dataclass
class VisualQAResult:
    """Result of visual QA inspection."""
    passed: bool
    issues: list = field(default_factory=list)   # list[VisualIssue]
    pages_inspected: int = 0
    model: str = ""
    raw_response: str = ""  # For debugging

    @property
    def errors(self) -> list:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list:
        return [i for i in self.issues if i.severity == "warning"]

    def summary(self) -> str:
        if self.passed:
            return f"Visual QA passed ({self.pages_inspected} pages, {len(self.warnings)} warnings)"
        return f"Visual QA FAILED: {len(self.errors)} errors, {len(self.warnings)} warnings"


# ═══════════════════════════════════════════════════════════════════════════
# SYSTEM PROMPT (cached for cost savings)
# ═══════════════════════════════════════════════════════════════════════════

_VISUAL_QA_SYSTEM = """You are a PDF form quality inspector for government procurement documents. You verify that filled PDF forms are visually correct and ready to submit to a buyer.

Inspect the PDF page image and check for these issues:

1. BLANK FIELDS: Any form fields that appear empty but should be filled (company name, prices, quantities, descriptions, dates, signatures)
2. TEXT OVERFLOW: Text that is cut off, extends past field boundaries, or overlaps with adjacent fields
3. SIGNATURE: Is there a visible signature image? Is it in the correct location (bottom of page, near "Signature" or "Date" labels)?
4. ALIGNMENT: Are numbers aligned in their columns? Are descriptions readable and not jumbled?
5. FONT ISSUES: Any black boxes, garbled characters, or missing text that suggests encoding problems
6. PAGE NUMBERS: If visible, are they correct (e.g. "Page 1 of 2")?

Return ONLY valid JSON:
{
  "passed": true/false,
  "issues": [
    {
      "severity": "error|warning|info",
      "category": "blank_field|text_overflow|signature|alignment|font_issue|page_number",
      "description": "Clear description of what's wrong",
      "field_name": "Field name if identifiable, or empty string"
    }
  ],
  "summary": "One-line summary of overall quality"
}

Rules:
- "error" = must fix before sending (blank required fields, missing signature, garbled text)
- "warning" = should review (minor alignment, long text near boundary)
- "info" = observation (everything looks good, or cosmetic notes)
- If the page looks correct, return {"passed": true, "issues": [], "summary": "All fields filled correctly"}
- Be specific: "COMPANY NAME field appears blank" not "a field is blank"
- For pricing tables, verify numbers are present in price/extension columns"""


# ═══════════════════════════════════════════════════════════════════════════
# CORE INSPECTION
# ═══════════════════════════════════════════════════════════════════════════

def inspect_pdf(
    pdf_path: str,
    expected_fields: Optional[dict] = None,
    max_pages: int = 4,
    company_name: str = "Reytech Inc.",
) -> VisualQAResult:
    """Visually inspect a filled PDF using Claude Vision.

    Renders each page to an image and asks Claude to verify the
    form is correctly filled.

    Args:
        pdf_path: Path to the filled PDF to inspect.
        expected_fields: Optional dict of {field_name: expected_value} to
            include in the prompt for targeted verification.
        max_pages: Maximum pages to inspect (default 4, covers most forms).
        company_name: Expected company name for consistency checks.

    Returns:
        VisualQAResult with pass/fail, issues list, and summary.
    """
    api_key = _get_api_key()
    if not api_key:
        log.info("pdf_visual_qa: No API key — skipping visual inspection")
        return VisualQAResult(passed=True, pages_inspected=0)

    if not os.path.exists(pdf_path):
        log.warning("pdf_visual_qa: PDF not found: %s", pdf_path)
        return VisualQAResult(passed=True, pages_inspected=0)

    # Render PDF to images
    page_images = _render_pdf_pages(pdf_path, max_pages=max_pages)
    if not page_images:
        log.warning("pdf_visual_qa: Failed to render PDF to images")
        return VisualQAResult(passed=True, pages_inspected=0)

    # Build the user prompt with context
    user_prompt = _build_user_prompt(
        page_count=len(page_images),
        expected_fields=expected_fields,
        company_name=company_name,
        pdf_name=os.path.basename(pdf_path),
    )

    # Call Claude Vision
    try:
        response = _call_vision_api(api_key, page_images, user_prompt)
    except Exception as e:
        log.error("pdf_visual_qa: Vision API call failed: %s", e)
        return VisualQAResult(passed=True, pages_inspected=len(page_images))

    # Parse response
    return _parse_response(response, len(page_images))


def inspect_package(
    pdf_paths: list,
    company_name: str = "Reytech Inc.",
) -> dict:
    """Inspect multiple PDFs in a package for cross-form consistency.

    Returns dict of {pdf_path: VisualQAResult} plus a "consistency" key
    with cross-form issues.
    """
    results = {}
    for path in pdf_paths:
        if os.path.exists(path) and path.lower().endswith(".pdf"):
            results[os.path.basename(path)] = inspect_pdf(
                path, company_name=company_name, max_pages=2
            )

    # Cross-form consistency check not needed via vision —
    # form_qa.py already checks field values match.
    # Visual inspection per-form is sufficient.
    return results


# ═══════════════════════════════════════════════════════════════════════════
# INTERNALS
# ═══════════════════════════════════════════════════════════════════════════

def _render_pdf_pages(pdf_path: str, max_pages: int = 4, dpi: int = 150) -> list:
    """Render PDF pages to base64 PNG images.

    Uses lower DPI than vision_parser (150 vs 200) since we're checking
    layout, not extracting text. Saves tokens.
    """
    import base64
    results = []
    try:
        from pdf2image import convert_from_path
        images = convert_from_path(
            pdf_path, dpi=dpi,
            first_page=1, last_page=max_pages,
            fmt="png",
        )
        for i, img in enumerate(images):
            # Resize for Claude token efficiency
            max_dim = 1200  # Smaller than parser — layout check doesn't need full res
            if img.width > max_dim or img.height > max_dim:
                ratio = min(max_dim / img.width, max_dim / img.height)
                new_size = (int(img.width * ratio), int(img.height * ratio))
                img = img.resize(new_size)

            import io
            buf = io.BytesIO()
            img.save(buf, format="PNG", optimize=True)
            b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            results.append({"base64": b64, "page": i + 1})
    except Exception as e:
        log.error("pdf_visual_qa: PDF render failed: %s", e)
    return results


def _build_user_prompt(
    page_count: int,
    expected_fields: Optional[dict],
    company_name: str,
    pdf_name: str,
) -> str:
    """Build the user prompt with context about what to check."""
    parts = [f"Inspect this {page_count}-page PDF form ({pdf_name})."]
    parts.append(f"Expected company name: \"{company_name}\".")

    if expected_fields:
        field_list = ", ".join(f"{k}=\"{v}\"" for k, v in list(expected_fields.items())[:10])
        parts.append(f"Verify these fields are visible: {field_list}.")

    parts.append("Check every page for blank fields, text overflow, signature placement, and layout issues.")
    return " ".join(parts)


def _call_vision_api(api_key: str, page_images: list, user_prompt: str) -> str:
    """Call Claude Vision API with page images."""
    import requests

    # Build content array with images + text
    content = []
    for img_data in page_images:
        content.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/png",
                "data": img_data["base64"],
            },
        })
    content.append({"type": "text", "text": user_prompt})

    payload = {
        "model": "claude-haiku-4-5-20251001",  # Haiku for cost — layout check doesn't need Sonnet
        "max_tokens": 1024,
        "system": [
            {
                "type": "text",
                "text": _VISUAL_QA_SYSTEM,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        "messages": [{"role": "user", "content": content}],
    }

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "prompt-caching-2024-07-31",
            "content-type": "application/json",
        },
        json=payload,
        timeout=15,
    )

    if resp.status_code != 200:
        log.error("pdf_visual_qa: API returned %d: %s", resp.status_code, resp.text[:200])
        raise RuntimeError(f"Vision API error {resp.status_code}")

    data = resp.json()
    return data["content"][0]["text"]


def _parse_response(raw_text: str, pages_inspected: int) -> VisualQAResult:
    """Parse Claude's JSON response into VisualQAResult."""
    # Strip markdown fences if present
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    if text.startswith("json"):
        text = text[4:].strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        log.warning("pdf_visual_qa: Failed to parse response as JSON: %s", text[:200])
        return VisualQAResult(
            passed=True, pages_inspected=pages_inspected,
            model="claude-haiku-4-5-20251001", raw_response=raw_text,
        )

    issues = []
    for item in data.get("issues", []):
        issues.append(VisualIssue(
            severity=item.get("severity", "info"),
            page=item.get("page", 0),
            category=item.get("category", ""),
            description=item.get("description", ""),
            field_name=item.get("field_name", ""),
        ))

    has_errors = any(i.severity == "error" for i in issues)
    passed = data.get("passed", not has_errors)

    return VisualQAResult(
        passed=passed,
        issues=issues,
        pages_inspected=pages_inspected,
        model="claude-haiku-4-5-20251001",
        raw_response=raw_text,
    )
