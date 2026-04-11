"""
compliance_extractor.py — AI-powered compliance matrix extraction from solicitation PDFs.

Reads full solicitation PDFs, extracts ALL compliance requirements using Claude Haiku,
and cross-references against the current RFQ package to identify met/unmet requirements.

V1: Extract + cross-reference. V2: Agency templates, auto-suggest forms, trending.
"""
import json
import logging
import os
import re
import time

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from src.core.api_quota import api_quota
except ImportError:
    api_quota = None

log = logging.getLogger("reytech.compliance_extractor")

_MODEL = "claude-haiku-4-5-20251001"
_API_TIMEOUT = 15  # seconds — solicitations are longer than emails
_MAX_PAGES = 100
_MAX_CHUNKS = 4    # max API calls per document
_CHUNK_SIZE = 7500  # chars per chunk (leave room for prompt)

# ═══════════════════════════════════════════════════════════════════════════
# KNOWN FORM MAPPINGS (for cross-reference)
# ═══════════════════════════════════════════════════════════════════════════

FORM_KEYWORDS = {
    "dvbe": "dvbe843",
    "disabled veteran": "dvbe843",
    "843": "dvbe843",
    "sellers permit": "sellers_permit",
    "seller's permit": "sellers_permit",
    "resale certificate": "sellers_permit",
    "std 204": "std204",
    "std204": "std204",
    "payee data": "std204",
    "std 205": "std205",
    "std205": "std205",
    "darfur": "darfur_act",
    "contracting act": "darfur_act",
    "calrecycle": "calrecycle74",
    "recycled content": "calrecycle74",
    "bidder declaration": "bidder_decl",
    "bidder's declaration": "bidder_decl",
    "general terms": "std1000",
    "std 1000": "std1000",
    "w-9": "w9",
    "w9": "w9",
    "taxpayer identification": "w9",
    "food items": "obs_1600",
    "obs 1600": "obs_1600",
    "obs-1600": "obs_1600",
    "703b": "703b",
    "703c": "703c",
    "704": "704b",
    "price quotation": "quote",
    "price quote": "quote",
    "quotation form": "quote",
    "insurance": "insurance_cert",
    "certificate of insurance": "insurance_cert",
    "performance bond": "perf_bond",
    "bid bond": "bid_bond",
    "genai": "genai_708",
    "708": "genai_708",
}

# Severity detection keywords
REQUIRED_KEYWORDS = {"shall", "must", "required", "mandatory", "failure to"}
PREFERRED_KEYWORDS = {"should", "preferred", "desirable", "encouraged"}


# ═══════════════════════════════════════════════════════════════════════════
# PDF TEXT EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

def _extract_text_from_pdf(pdf_path: str, max_pages: int = _MAX_PAGES) -> tuple:
    """Extract text from PDF with page tracking.

    Returns: (full_text: str, page_texts: list[tuple(page_num, text)])
    """
    from pypdf import PdfReader

    reader = PdfReader(pdf_path)
    page_texts = []
    for i, page in enumerate(reader.pages[:max_pages]):
        text = page.extract_text() or ""
        if text.strip():
            page_texts.append((i + 1, text))

    full_text = "\n\n".join(text for _, text in page_texts)
    log.info("Extracted %d pages of text from %s (%d chars)",
             len(page_texts), os.path.basename(pdf_path), len(full_text))
    return full_text, page_texts


# ═══════════════════════════════════════════════════════════════════════════
# CLAUDE API EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

_EXTRACTION_PROMPT = """You are analyzing a government procurement solicitation document.
Extract ALL compliance requirements — things the vendor MUST or SHOULD do/provide.

Return ONLY a valid JSON array. Each requirement:
{
    "text": "exact requirement text or close paraphrase",
    "category": "form|certification|delivery|pricing|insurance|bond|qualification|reporting|other",
    "severity": "required|preferred|informational",
    "source_page": 0
}

Categories:
- form: specific forms to submit (STD 204, DVBE 843, W-9, etc.)
- certification: certifications needed (SB, DVBE, insurance, licenses)
- delivery: delivery terms, timelines, locations, FOB terms
- pricing: pricing format, validity period, discount terms
- insurance: insurance requirements, coverage amounts
- bond: bid bonds, performance bonds
- qualification: experience, references, past performance
- reporting: reporting requirements, status updates
- other: anything else

Rules:
- Extract EVERY requirement, even minor ones
- "shall" and "must" = required severity
- "should" and "preferred" = preferred severity
- Background info with no action = informational
- Include page number if identifiable from context
- Do NOT duplicate requirements — merge similar ones"""


def _get_api_key() -> str:
    for var in ("AGENT_PRICING_KEY", "ANTHROPIC_API_KEY"):
        key = os.environ.get(var, "")
        if key:
            return key
    return ""


def _extract_requirements_claude(text: str) -> list:
    """Extract requirements from text using Claude Haiku. Chunks if needed."""
    api_key = _get_api_key()
    if not api_key or not HAS_REQUESTS:
        log.warning("Compliance extractor: no API key or requests library")
        return []

    all_requirements = []
    chunks = []

    # Split into chunks
    if len(text) <= _CHUNK_SIZE:
        chunks = [text]
    else:
        for i in range(0, len(text), _CHUNK_SIZE):
            chunks.append(text[i:i + _CHUNK_SIZE])
            if len(chunks) >= _MAX_CHUNKS:
                break

    for i, chunk in enumerate(chunks):
        try:
            # Soft quota check
            if api_quota and not api_quota.can_call("claude"):
                log.warning("Claude daily quota exceeded, skipping compliance chunk %d", i + 1)

            user_msg = f"Solicitation text (part {i + 1} of {len(chunks)}):\n\n{chunk}"

            request_body = {
                "model": _MODEL,
                "max_tokens": 2048,
                "system": [{"type": "text", "text": _EXTRACTION_PROMPT,
                            "cache_control": {"type": "ephemeral"}}],
                "messages": [{"role": "user", "content": user_msg}],
            }
            headers = {
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }

            _t0 = time.time()
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=headers, json=request_body, timeout=_API_TIMEOUT,
            )
            _elapsed_ms = int((time.time() - _t0) * 1000)

            if resp.status_code != 200:
                log.warning("Compliance extractor: API error %d on chunk %d",
                            resp.status_code, i + 1)
                if api_quota:
                    api_quota.log_call("claude", agent="compliance_extractor",
                                       error=f"http_{resp.status_code}",
                                       response_time_ms=_elapsed_ms, model=_MODEL)
                continue

            data = resp.json()

            # Log successful API call with token usage
            _usage = data.get("usage", {})
            if api_quota:
                api_quota.log_call("claude", agent="compliance_extractor",
                                   tokens_in=_usage.get("input_tokens", 0),
                                   tokens_out=_usage.get("output_tokens", 0),
                                   response_time_ms=_elapsed_ms, model=_MODEL)

            full_text = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    full_text += block.get("text", "")

            # Parse JSON array
            text_clean = full_text.strip()
            start = text_clean.find("[")
            end = text_clean.rfind("]") + 1
            if start >= 0 and end > start:
                reqs = json.loads(text_clean[start:end])
                if isinstance(reqs, list):
                    all_requirements.extend(reqs)

            log.info("Compliance chunk %d/%d: extracted %d requirements",
                     i + 1, len(chunks), len(reqs) if isinstance(reqs, list) else 0)

        except requests.exceptions.Timeout:
            log.warning("Compliance extractor: timeout on chunk %d", i + 1)
        except (json.JSONDecodeError, ValueError) as e:
            log.warning("Compliance extractor: JSON parse error on chunk %d: %s", i + 1, e)
        except Exception as e:
            log.error("Compliance extractor: error on chunk %d: %s", i + 1, e, exc_info=True)

    return all_requirements


# ═══════════════════════════════════════════════════════════════════════════
# CROSS-REFERENCE ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def _match_form_id(text: str) -> str:
    """Try to match requirement text to a known form ID."""
    lower = text.lower()
    for keyword, form_id in FORM_KEYWORDS.items():
        if keyword in lower:
            return form_id
    return ""


def _cross_reference_package(requirements: list, rfq_data: dict = None,
                             generated_files: list = None) -> list:
    """Check each requirement against the current RFQ package.

    Args:
        requirements: list of requirement dicts from Claude
        rfq_data: RFQ record dict (optional)
        generated_files: list of form_ids in the generated package (optional)

    Returns: requirements list with 'met', 'met_by', 'notes' fields added
    """
    rfq_data = rfq_data or {}
    generated_files = generated_files or []
    gen_lower = {f.lower() for f in generated_files}

    for i, req in enumerate(requirements):
        req["id"] = f"REQ-{i + 1:03d}"
        req.setdefault("met", False)
        req.setdefault("met_by", "")
        req.setdefault("notes", "")

        text = req.get("text", "")
        category = req.get("category", "other")

        # Form requirements — check against generated files
        if category == "form":
            form_id = _match_form_id(text)
            if form_id and form_id.lower() in gen_lower:
                req["met"] = True
                req["met_by"] = form_id
                req["notes"] = f"Form {form_id} included in package"
            elif form_id:
                req["notes"] = f"Form {form_id} NOT found in package"

        # Certification requirements
        elif category == "certification":
            form_id = _match_form_id(text)
            if form_id and form_id.lower() in gen_lower:
                req["met"] = True
                req["met_by"] = form_id
            # Check for known Reytech certifications
            lower_text = text.lower()
            if any(kw in lower_text for kw in ["sb", "small business"]):
                req["met"] = True
                req["met_by"] = "Reytech SB certification"
            if any(kw in lower_text for kw in ["dvbe", "disabled veteran"]):
                req["met"] = True
                req["met_by"] = "Reytech DVBE certification"

        # Delivery requirements
        elif category == "delivery":
            if rfq_data.get("delivery_location") or rfq_data.get("ship_to"):
                req["met"] = True
                req["met_by"] = "Delivery address specified"

        # Pricing requirements
        elif category == "pricing":
            if "quote" in gen_lower or any("quote" in f.lower() for f in generated_files):
                req["met"] = True
                req["met_by"] = "Quote document included"

    return requirements


# ═══════════════════════════════════════════════════════════════════════════
# DATABASE OPERATIONS
# ═══════════════════════════════════════════════════════════════════════════

def save_compliance_matrix(rfq_id: str, requirements: list,
                           source_pdf: str = "", method: str = "claude") -> int:
    """Save compliance matrix to DB. Returns matrix ID or -1."""
    met_count = sum(1 for r in requirements if r.get("met"))
    total = len(requirements)
    score = (met_count / total * 100) if total > 0 else 0

    try:
        from src.core.db import get_db
        with get_db() as conn:
            cursor = conn.execute(
                """INSERT INTO compliance_matrices
                   (rfq_id, requirements_json, met_count, total_count, score,
                    extraction_method, source_pdf_path)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (rfq_id, json.dumps(requirements), met_count, total,
                 round(score, 1), method, source_pdf)
            )
            matrix_id = cursor.lastrowid
            log.info("Saved compliance matrix: id=%d, rfq=%s, %d/%d met (%.0f%%)",
                     matrix_id, rfq_id, met_count, total, score)
            return matrix_id
    except Exception as e:
        log.error("Failed to save compliance matrix for %s: %s", rfq_id, e, exc_info=True)
        return -1


def get_compliance_matrix(rfq_id: str) -> dict:
    """Load the latest compliance matrix for an RFQ."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                """SELECT * FROM compliance_matrices
                   WHERE rfq_id = ? ORDER BY extracted_at DESC LIMIT 1""",
                (rfq_id,)
            ).fetchone()
            if not row:
                return {"ok": False, "error": "No compliance matrix found"}
            d = dict(row)
            if d.get("requirements_json"):
                try:
                    d["requirements"] = json.loads(d["requirements_json"])
                except (json.JSONDecodeError, TypeError):
                    d["requirements"] = []
            else:
                d["requirements"] = []
            d["ok"] = True
            return d
    except Exception as e:
        log.error("Failed to load compliance matrix for %s: %s", rfq_id, e)
        return {"ok": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════

def extract_compliance_matrix(pdf_path: str, rfq_id: str,
                              rfq_data: dict = None,
                              generated_files: list = None) -> dict:
    """Extract compliance matrix from a solicitation PDF.

    Args:
        pdf_path: path to the solicitation PDF
        rfq_id: RFQ ID to associate the matrix with
        rfq_data: optional RFQ record for cross-referencing
        generated_files: optional list of form_ids already in the package

    Returns: {"ok", "requirements", "met_count", "total_count", "score", "matrix_id"}
    """
    if not os.path.isfile(pdf_path):
        return {"ok": False, "error": "PDF file not found"}

    start = time.time()

    # Step 1: Extract text
    try:
        full_text, page_texts = _extract_text_from_pdf(pdf_path)
    except Exception as e:
        log.error("Compliance: PDF text extraction failed: %s", e, exc_info=True)
        return {"ok": False, "error": f"PDF extraction failed: {e}"}

    if not full_text.strip():
        return {"ok": False, "error": "No text found in PDF"}

    # Step 2: Extract requirements via Claude
    requirements = _extract_requirements_claude(full_text)

    if not requirements:
        log.warning("Compliance: no requirements extracted from %s", pdf_path)
        return {"ok": False, "error": "No requirements found in document"}

    # Step 3: Cross-reference against package
    requirements = _cross_reference_package(requirements, rfq_data, generated_files)

    # Step 4: Save to DB
    matrix_id = save_compliance_matrix(rfq_id, requirements, pdf_path)

    met_count = sum(1 for r in requirements if r.get("met"))
    total = len(requirements)
    score = (met_count / total * 100) if total > 0 else 0
    duration_ms = int((time.time() - start) * 1000)

    log.info("Compliance matrix for %s: %d/%d met (%.0f%%) in %dms",
             rfq_id, met_count, total, score, duration_ms)

    return {
        "ok": True,
        "requirements": requirements,
        "met_count": met_count,
        "total_count": total,
        "score": round(score, 1),
        "matrix_id": matrix_id,
        "duration_ms": duration_ms,
    }


# ═══════════════════════════════════════════════════════════════════════════
# V2: AGENCY COMPLIANCE TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════

_AGENCY_TEMPLATE_SEEDS = {
    "cchcs": [
        ("Submit 703B/703C form", "form", "required", "703b"),
        ("Submit 704B pricing form", "form", "required", "704b"),
        ("Include Bid Package", "form", "required", "bid_package"),
        ("Include Reytech price quotation", "form", "required", "quote"),
        ("Vendor must be certified Small Business (SB)", "certification", "required", ""),
        ("Vendor must be certified DVBE", "certification", "preferred", "dvbe843"),
        ("Deliver within 5-7 business days", "delivery", "required", ""),
        ("FOB Destination, Freight Prepaid", "delivery", "required", ""),
    ],
    "calvet": [
        ("Submit STD 204 Payee Data Record", "form", "required", "std204"),
        ("Submit STD 205", "form", "required", "std205"),
        ("Submit CalRecycle 74 form", "form", "required", "calrecycle74"),
        ("Submit DVBE 843 form", "form", "required", "dvbe843"),
        ("Submit Darfur Contracting Act certification", "form", "required", "darfur_act"),
        ("Submit Seller's Permit", "form", "required", "sellers_permit"),
        ("Submit STD 1000 General Terms", "form", "required", "std1000"),
        ("Include price quotation", "form", "required", "quote"),
    ],
    "dgs": [
        ("Submit STD 204 Payee Data Record", "form", "required", "std204"),
        ("Submit W-9 taxpayer identification", "form", "required", "w9"),
        ("Provide certificate of insurance", "certification", "preferred", "insurance_cert"),
        ("Include price quotation", "form", "required", "quote"),
    ],
    "dsh": [
        ("Submit STD 204", "form", "required", "std204"),
        ("Submit DVBE 843 form", "form", "required", "dvbe843"),
        ("Include price quotation", "form", "required", "quote"),
    ],
    "calfire": [
        ("Submit STD 204", "form", "required", "std204"),
        ("Include price quotation", "form", "required", "quote"),
    ],
}


def seed_agency_templates() -> int:
    """Seed default agency compliance templates. Idempotent."""
    try:
        from src.core.db import get_db
        count = 0
        with get_db() as conn:
            for agency_key, reqs in _AGENCY_TEMPLATE_SEEDS.items():
                for text, category, severity, form_id in reqs:
                    existing = conn.execute(
                        "SELECT id FROM agency_compliance_templates WHERE agency_key=? AND requirement_text=?",
                        (agency_key, text)
                    ).fetchone()
                    if existing:
                        continue
                    conn.execute(
                        "INSERT INTO agency_compliance_templates (agency_key, requirement_text, category, severity, form_id) VALUES (?, ?, ?, ?, ?)",
                        (agency_key, text, category, severity, form_id)
                    )
                    count += 1
        log.info("Seeded %d agency compliance templates", count)
        return count
    except Exception as e:
        log.error("Failed to seed agency templates: %s", e, exc_info=True)
        return 0


def get_agency_template(agency_key: str) -> list:
    """Load compliance template for an agency."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                "SELECT requirement_text, category, severity, form_id FROM agency_compliance_templates WHERE agency_key = ? AND active = 1 ORDER BY category, severity",
                (agency_key.lower(),)
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.error("Failed to load agency template for %s: %s", agency_key, e)
        return []


def merge_template_with_extraction(template_reqs: list, extracted_reqs: list) -> list:
    """Merge agency template with PDF-extracted requirements. Deduplicates."""
    merged = []
    used_texts = set()
    for i, req in enumerate(template_reqs):
        text = req.get("requirement_text", "") or req.get("text", "")
        merged.append({
            "id": f"TPL-{i + 1:03d}", "text": text,
            "category": req.get("category", "other"),
            "severity": req.get("severity", "required"),
            "source": "agency_template",
            "form_id": req.get("form_id", ""),
            "met": False, "met_by": "", "notes": "",
        })
        used_texts.add(text.lower()[:60])
    for req in extracted_reqs:
        text = req.get("text", "")
        text_key = text.lower()[:60]
        if text_key in used_texts:
            continue
        form_id = req.get("form_id", "") or _match_form_id(text)
        if form_id and any(m.get("form_id") == form_id for m in merged):
            continue
        merged.append(req)
        used_texts.add(text_key)
    return merged
