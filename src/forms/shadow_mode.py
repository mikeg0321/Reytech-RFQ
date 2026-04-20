"""Shadow Mode — run the new fill engine alongside legacy, diff outputs.

When QUOTE_MODEL_V2 flag is enabled, this module runs the new fill engine
in parallel with the legacy path. The legacy output is always served.
Divergences are logged to data/shadow_diffs.jsonl for operator review.

Usage:
    from src.forms.shadow_mode import shadow_fill

    # Inside the existing generate route, AFTER legacy fill succeeds:
    shadow_fill(
        pc_or_rfq_dict=pc,
        doc_type="pc",
        doc_id=pcid,
        legacy_output_path="/path/to/legacy_output.pdf",
    )
    # Returns immediately. Shadow fill runs in a background thread.
    # Diffs logged to data/shadow_diffs.jsonl.
"""
import hashlib
import io
import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone

from src.core.paths import DATA_DIR

log = logging.getLogger(__name__)

_PST = timezone(timedelta(hours=-8))
_SHADOW_LOG = os.path.join(DATA_DIR, "shadow_diffs.jsonl")


def _is_shadow_enabled() -> bool:
    """Check if shadow mode is enabled via feature flag."""
    try:
        from src.core.flags import get_flag
        return bool(get_flag("quote_model_v2_shadow", True))  # Default ON
    except Exception:
        return True  # Safe default: shadow mode is non-destructive


def shadow_fill(
    pc_or_rfq_dict: dict,
    doc_type: str,
    doc_id: str,
    legacy_output_path: str,
):
    """Run the new fill engine in a background thread, diff against legacy output.

    This is non-blocking. The caller continues with the legacy result.
    Any error in the shadow path is logged but never surfaces to the user.
    """
    if not _is_shadow_enabled():
        return

    # Run in background thread — never block the request
    t = threading.Thread(
        target=_shadow_worker,
        args=(pc_or_rfq_dict, doc_type, doc_id, legacy_output_path),
        daemon=True,
        name=f"shadow-fill-{doc_id[:8]}",
    )
    t.start()


def _shadow_worker(pc_or_rfq_dict: dict, doc_type: str, doc_id: str, legacy_output_path: str):
    """Background worker: fill via new engine, diff, log."""
    try:
        import copy
        from src.core.quote_model import Quote
        from src.forms.profile_registry import load_profiles, match_profile
        from src.forms.fill_engine import fill

        start = datetime.now(_PST)

        # Convert to Quote
        doc = copy.deepcopy(pc_or_rfq_dict)
        quote = Quote.from_legacy_dict(doc, doc_type=doc_type)

        # Pick profile
        profiles = load_profiles()

        # Try to match by fingerprint from source PDF
        source_pdf = doc.get("source_pdf", "")
        profile = None
        if source_pdf and os.path.exists(source_pdf):
            profile = match_profile(source_pdf, profiles)

        # Fallback to default 704A
        if not profile:
            profile = profiles.get("704a_reytech_standard")

        if not profile:
            _log_diff(doc_id, doc_type, "no_profile", "No profile found", 0)
            return

        # Fill via new engine
        new_bytes = fill(quote, profile)
        elapsed_ms = int((datetime.now(_PST) - start).total_seconds() * 1000)

        # Compare against legacy output
        if not os.path.exists(legacy_output_path):
            _log_diff(doc_id, doc_type, "legacy_missing", "Legacy output not found", elapsed_ms)
            return

        with open(legacy_output_path, "rb") as f:
            legacy_bytes = f.read()

        # Field-level comparison using pypdf
        diff_result = _compare_pdfs(legacy_bytes, new_bytes)

        # Log the result
        _log_diff(
            doc_id=doc_id,
            doc_type=doc_type,
            verdict=diff_result["verdict"],
            detail=diff_result["summary"],
            elapsed_ms=elapsed_ms,
            field_diffs=diff_result.get("field_diffs"),
            legacy_sha=hashlib.sha256(legacy_bytes).hexdigest()[:16],
            new_sha=hashlib.sha256(new_bytes).hexdigest()[:16],
            profile_id=profile.id,
            item_count=len(quote.line_items),
        )

        # Save new output for manual comparison
        shadow_dir = os.path.join(DATA_DIR, "shadow_outputs")
        os.makedirs(shadow_dir, exist_ok=True)
        shadow_path = os.path.join(shadow_dir, f"{doc_id}_shadow.pdf")
        with open(shadow_path, "wb") as f:
            f.write(new_bytes)

    except Exception as e:
        log.warning("Shadow fill failed for %s: %s", doc_id, e)
        _log_diff(doc_id, doc_type, "error", str(e), 0)


def _compare_pdfs(legacy_bytes: bytes, new_bytes: bytes) -> dict:
    """Compare two PDFs field-by-field using pypdf.

    Returns:
        {
            "verdict": "match" | "diverge" | "error",
            "summary": "human-readable summary",
            "field_diffs": [{"field": "...", "legacy": "...", "new": "..."}, ...]
        }
    """
    try:
        from pypdf import PdfReader

        legacy_reader = PdfReader(io.BytesIO(legacy_bytes))
        new_reader = PdfReader(io.BytesIO(new_bytes))

        legacy_fields = legacy_reader.get_fields() or {}
        new_fields = new_reader.get_fields() or {}

        # Extract field values
        def _val(field_dict):
            if isinstance(field_dict, dict):
                return str(field_dict.get("/V", "")).strip()
            return str(field_dict).strip()

        # Compare all fields
        all_field_names = set(legacy_fields.keys()) | set(new_fields.keys())
        diffs = []
        match_count = 0
        total_compared = 0

        for name in sorted(all_field_names):
            legacy_val = _val(legacy_fields.get(name, ""))
            new_val = _val(new_fields.get(name, ""))

            # Skip empty-vs-empty
            if not legacy_val and not new_val:
                continue

            total_compared += 1
            if legacy_val == new_val:
                match_count += 1
            else:
                diffs.append({
                    "field": name,
                    "legacy": legacy_val[:100],
                    "new": new_val[:100],
                })

        if not diffs:
            return {
                "verdict": "match",
                "summary": f"{match_count}/{total_compared} fields match, 0 divergences",
                "field_diffs": [],
            }
        else:
            return {
                "verdict": "diverge",
                "summary": f"{match_count}/{total_compared} match, {len(diffs)} divergences",
                "field_diffs": diffs[:50],  # Cap at 50 to avoid huge logs
            }

    except Exception as e:
        return {
            "verdict": "error",
            "summary": f"Comparison error: {e}",
            "field_diffs": [],
        }


def _log_diff(doc_id, doc_type, verdict, detail, elapsed_ms, **extra):
    """Append one line to the shadow diffs JSONL file."""
    entry = {
        "timestamp": datetime.now(_PST).isoformat(),
        "doc_id": doc_id,
        "doc_type": doc_type,
        "verdict": verdict,
        "detail": detail,
        "elapsed_ms": elapsed_ms,
    }
    entry.update(extra)

    try:
        os.makedirs(os.path.dirname(_SHADOW_LOG), exist_ok=True)
        with open(_SHADOW_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning("Failed to write shadow diff log: %s", e)

    level = logging.INFO if verdict == "match" else logging.WARNING
    log.log(level, "SHADOW %s %s: %s — %s (%dms)", doc_type, doc_id[:8], verdict, detail, elapsed_ms)


# ── API for the dashboard ────────────────────────────────────────────────────

def get_recent_diffs(limit: int = 50) -> list[dict]:
    """Read the most recent shadow diffs for the admin dashboard."""
    if not os.path.exists(_SHADOW_LOG):
        return []

    lines = []
    try:
        with open(_SHADOW_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    lines.append(json.loads(line))
    except Exception as e:
        log.warning("Failed to read shadow diffs: %s", e)
        return []

    # Return most recent first
    return list(reversed(lines[-limit:]))


def get_diff_summary() -> dict:
    """Summary stats for the admin dashboard."""
    diffs = get_recent_diffs(limit=1000)
    total = len(diffs)
    matches = sum(1 for d in diffs if d.get("verdict") == "match")
    divergences = sum(1 for d in diffs if d.get("verdict") == "diverge")
    errors = sum(1 for d in diffs if d.get("verdict") == "error")

    return {
        "total": total,
        "matches": matches,
        "divergences": divergences,
        "errors": errors,
        "match_rate": round(matches / total * 100, 1) if total > 0 else 0,
        "consecutive_matches": _count_consecutive_matches(diffs),
    }


def _count_consecutive_matches(diffs: list[dict]) -> int:
    """Count consecutive matches from the most recent entry backward."""
    count = 0
    for d in diffs:  # Already reversed (newest first)
        if d.get("verdict") == "match":
            count += 1
        else:
            break
    return count
