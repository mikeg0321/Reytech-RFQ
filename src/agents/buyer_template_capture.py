"""buyer_template_capture.py — Phase 1.6 PR3c.

Every incoming attached PDF gets fingerprinted and registered in
`buyer_template_candidates`. When the fingerprint matches an existing
FormProfile, we update last_seen_at + seen_count. When it doesn't,
we register a candidate the operator can later promote to a YAML
profile via /settings/forms (PR3f, future).

Read-only from the perspective of the fill engine — registration is a
side-effect that does NOT affect today's profile-matching path. The
panel surfaces "🆕 new variant" via the candidate state.

Cost: one fingerprint compute (SHA-256 of sorted AcroForm field names)
per attachment. Idempotent via UNIQUE(fingerprint, agency_key) — same
blank seen twice from the same buyer is a no-op INSERT and a counter bump.
"""

import logging
import os
from typing import Optional

log = logging.getLogger("reytech.buyer_template_capture")


# ─── Form-type heuristic from filename ─────────────────────────────────────
# When a buyer attaches "703B_Folsom.pdf", we can guess form_type=703b
# even before fingerprinting. Used only as a hint on the candidate row;
# the operator decides on promote.
_FORM_HINTS = [
    ("703a", ["703a", "703-a"]),
    ("703b", ["703b", "703-b", "ams 703"]),
    ("703c", ["703c", "703-c", "fair and reasonable"]),
    ("704a", ["704a", "704-a"]),
    ("704b", ["704b", "704-b", "quote worksheet"]),
    ("dvbe843", ["dvbe", "std 843", "std843", "843"]),
    ("darfur_act", ["darfur"]),
    ("cv012_cuf", ["cv 012", "cv012", "cuf"]),
    ("calrecycle74", ["calrecycle", "recycled-content", "recycle"]),
    ("bidder_decl", ["bidder declaration", "gspd-05-105", "gspd05"]),
    ("std204", ["std 204", "std204", "payee data"]),
    ("std205", ["std 205", "std205", "payee supplemental"]),
    ("std1000", ["std 1000", "std1000", "genai"]),
    ("sellers_permit", ["seller's permit", "sellers permit", "seller permit"]),
    ("w9", ["w-9", "w9 ", "_w9"]),
    ("drug_free", ["drug-free", "drug free"]),
    ("obs_1600", ["obs 1600", "obs-1600", "food cert"]),
    ("cchcs_it_rfq", ["it goods", "it_goods", "non-cloud"]),
]


def register_attachment(quote_id: str, quote_type: str,
                         attachment: dict, agency_key: str = "") -> dict:
    """Fingerprint an attached PDF and register/refresh a candidate.

    Returns a dict describing the registration outcome:
      {
        "ok": bool,
        "fingerprint": str,
        "status": "new_candidate" | "matched_profile" | "existing_candidate" |
                  "skipped_no_fingerprint" | "skipped_no_pdf",
        "profile_id": str (if matched_profile),
        "candidate_id": int (if new/existing candidate),
        "form_type_guess": str,
      }

    Defensive — never raises. Returns {ok: False} on any failure.
    """
    try:
        if not _is_pdf(attachment):
            return {"ok": False, "status": "skipped_no_pdf"}

        fingerprint, field_count, page_count = _fingerprint_attachment(attachment)
        if not fingerprint:
            return {"ok": False, "status": "skipped_no_fingerprint"}

        # If a FormProfile already covers this fingerprint, no candidate
        matched = _match_profile_by_fingerprint(fingerprint)
        if matched:
            return {
                "ok": True, "fingerprint": fingerprint[:16],
                "status": "matched_profile",
                "profile_id": matched,
                "form_type_guess": "",
            }

        form_type_guess = _guess_form_type(attachment.get("filename", ""))
        agency = (agency_key or "").lower().strip()

        candidate_id, was_new = _upsert_candidate(
            fingerprint=fingerprint,
            agency_key=agency,
            form_type_guess=form_type_guess,
            sample_filename=attachment.get("filename", ""),
            sample_quote_id=quote_id,
            sample_quote_type=quote_type,
            field_count=field_count,
            page_count=page_count,
        )

        return {
            "ok": True,
            "fingerprint": fingerprint[:16],
            "status": "new_candidate" if was_new else "existing_candidate",
            "candidate_id": candidate_id,
            "form_type_guess": form_type_guess,
        }
    except Exception as e:
        log.debug("register_attachment suppressed: %s", e)
        return {"ok": False, "error": str(e)}


def list_candidates(status: str = "candidate", limit: int = 200) -> list:
    """List candidate buyer templates pending review/promotion."""
    try:
        from src.core.db import get_db
        with get_db() as conn:
            rows = conn.execute(
                """SELECT id, fingerprint, agency_key, form_type_guess,
                          sample_filename, sample_quote_id, sample_quote_type,
                          field_count, page_count, first_seen_at, last_seen_at,
                          seen_count, status, promoted_profile_id
                   FROM buyer_template_candidates
                   WHERE status = ?
                   ORDER BY seen_count DESC, last_seen_at DESC
                   LIMIT ?""",
                (status, limit),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        log.debug("list_candidates error: %s", e)
        return []


def get_candidate_for_fingerprint(fingerprint: str,
                                  agency_key: str = "") -> Optional[dict]:
    """Look up a candidate by (fingerprint, agency_key). Returns None if absent."""
    if not fingerprint:
        return None
    try:
        from src.core.db import get_db
        with get_db() as conn:
            row = conn.execute(
                """SELECT id, status, form_type_guess, seen_count,
                          promoted_profile_id
                   FROM buyer_template_candidates
                   WHERE fingerprint = ? AND agency_key = ?""",
                (fingerprint, (agency_key or "").lower()),
            ).fetchone()
            return dict(row) if row else None
    except Exception as e:
        log.debug("get_candidate_for_fingerprint error: %s", e)
        return None


# ─── Internals ─────────────────────────────────────────────────────────────

def _is_pdf(att: dict) -> bool:
    name = (att.get("filename", "") or "").lower()
    ftype = (att.get("file_type", "") or "").lower()
    return name.endswith(".pdf") or ftype == "pdf" or "pdf" in ftype


def _fingerprint_attachment(att: dict) -> tuple:
    """Return (fingerprint_hex, field_count, page_count). Empty fingerprint
    means the PDF had no AcroForm fields (flat scan) — handled separately."""
    try:
        from pypdf import PdfReader
    except ImportError:
        return ("", 0, 0)

    reader = _open_reader(att)
    if reader is None:
        return ("", 0, 0)

    try:
        page_count = len(reader.pages)
    except Exception:
        page_count = 0

    try:
        fields = reader.get_fields() or {}
    except Exception:
        fields = {}

    if not fields:
        # Flat PDF — no fingerprint, but we still want to record for
        # PR3l overlay-mode treatment. For now return empty.
        return ("", 0, page_count)

    import hashlib
    names = sorted(fields.keys())
    fp = hashlib.sha256("\n".join(names).encode()).hexdigest()
    return (fp, len(names), page_count)


def _open_reader(att: dict):
    try:
        from pypdf import PdfReader
    except ImportError:
        return None

    path = att.get("file_path")
    if path and os.path.isfile(path):
        try:
            return PdfReader(path)
        except Exception as e:
            log.debug("PdfReader(path=%s) failed: %s", path, e)
            return None

    file_id = att.get("file_id")
    if file_id is not None:
        try:
            from src.core.db import get_db
            import io
            with get_db() as conn:
                row = conn.execute(
                    "SELECT data FROM rfq_files WHERE id = ?", (file_id,)
                ).fetchone()
                if not row or not row["data"]:
                    return None
                return PdfReader(io.BytesIO(row["data"]))
        except Exception as e:
            log.debug("PdfReader(blob=%s) failed: %s", file_id, e)
            return None

    return None


def _match_profile_by_fingerprint(fingerprint: str) -> str:
    """Return profile_id whose fingerprint matches, or empty string."""
    if not fingerprint:
        return ""
    try:
        from src.forms.profile_registry import load_profiles
        profiles = load_profiles() or {}
        for p in profiles.values():
            if getattr(p, "fingerprint", "") == fingerprint:
                return getattr(p, "id", "")
    except Exception as e:
        log.debug("_match_profile_by_fingerprint error: %s", e)
    return ""


def _guess_form_type(filename: str) -> str:
    """Heuristic form_type guess from filename. Empty string when ambiguous."""
    if not filename:
        return ""
    name = filename.lower()
    for form_id, hints in _FORM_HINTS:
        for h in hints:
            if h in name:
                return form_id
    return ""


def _upsert_candidate(fingerprint: str, agency_key: str, form_type_guess: str,
                      sample_filename: str, sample_quote_id: str,
                      sample_quote_type: str, field_count: int,
                      page_count: int) -> tuple:
    """Insert a new candidate or bump seen_count on existing.

    Returns (candidate_id, was_new).
    """
    try:
        from src.core.db import get_db
        with get_db() as conn:
            # Try INSERT first; on UNIQUE conflict, UPDATE
            try:
                cur = conn.execute(
                    """INSERT INTO buyer_template_candidates
                        (fingerprint, agency_key, form_type_guess,
                         sample_filename, sample_quote_id, sample_quote_type,
                         field_count, page_count)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (fingerprint, agency_key, form_type_guess,
                     sample_filename, sample_quote_id, sample_quote_type,
                     field_count, page_count),
                )
                conn.commit()
                return (cur.lastrowid, True)
            except Exception:
                # Existing row — bump counters
                conn.execute(
                    """UPDATE buyer_template_candidates
                       SET last_seen_at = datetime('now'),
                           seen_count = seen_count + 1,
                           sample_filename = COALESCE(NULLIF(sample_filename,''),
                                                      ?)
                       WHERE fingerprint = ? AND agency_key = ?""",
                    (sample_filename, fingerprint, agency_key),
                )
                row = conn.execute(
                    "SELECT id FROM buyer_template_candidates WHERE fingerprint = ? AND agency_key = ?",
                    (fingerprint, agency_key),
                ).fetchone()
                conn.commit()
                return (row["id"] if row else 0, False)
    except Exception as e:
        log.debug("_upsert_candidate error: %s", e)
        return (0, False)
